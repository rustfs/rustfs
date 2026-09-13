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

use super::super::root_recovery::RootHealRecovery;
use super::*;
use crate::heal::RUSTFS_META_BUCKET;
use std::collections::HashSet;

#[cfg(unix)]
struct RestoreDirectoryMode {
    path: std::path::PathBuf,
    mode: u32,
}

#[cfg(unix)]
impl RestoreDirectoryMode {
    fn read_only(path: std::path::PathBuf) -> Self {
        use std::os::unix::fs::PermissionsExt as _;

        let mode = std::fs::metadata(&path)
            .expect("metadata directory mode")
            .permissions()
            .mode();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o555)).expect("make metadata directory read-only");
        Self { path, mode }
    }
}

#[cfg(unix)]
impl Drop for RestoreDirectoryMode {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt as _;

        let _ = std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(self.mode));
    }
}

#[cfg(unix)]
fn ordered_recovery_disks(first: DiskStore, second: DiskStore) -> (DiskStore, DiskStore) {
    if first.endpoint().to_string() <= second.endpoint().to_string() {
        (first, second)
    } else {
        (second, first)
    }
}

async fn recovery_disk() -> (TempDir, DiskStore) {
    let temp = TempDir::new().expect("temporary root recovery disk");
    let endpoint = Endpoint::try_from(temp.path().to_string_lossy().as_ref()).expect("disk endpoint");
    let disk = new_disk(
        &endpoint,
        &DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("local recovery disk");
    match disk.make_volume(RUSTFS_META_BUCKET).await {
        Ok(()) | Err(DiskError::VolumeExists) => {}
        Err(error) => panic!("metadata volume: {error}"),
    }
    (temp, disk)
}

fn recovery_manager(disks: Vec<DiskStore>) -> HealManager {
    let mut manager = HealManager::new(
        Arc::new(MockStorage),
        Some(HealConfig {
            enable_auto_heal: false,
            ..Default::default()
        }),
    );
    manager.root_recovery = Arc::new(RootHealRecovery::with_disks(disks));
    manager
}

fn root_request() -> HealRequest {
    admin_request(HealType::Cluster)
}

fn admin_request(heal_type: HealType) -> HealRequest {
    let mut request = HealRequest::new(heal_type, HealOptions::default(), HealPriority::High);
    request.source = HealRequestSource::Admin;
    request
}

fn completed_admin_status(heal_type: &HealType, completed_at: SystemTime) -> CompletedHealStatus {
    CompletedHealStatus {
        outcome: None,
        heal_type: heal_type.clone(),
        options: HealOptions::default(),
        status: HealTaskStatus::Completed,
        progress: Some(HealProgress {
            objects_scanned: 1,
            objects_healed: 1,
            bytes_processed: 64,
            ..Default::default()
        }),
        retained_bytes: std::sync::OnceLock::new(),
        result_items_truncated: false,
        completed_at,
        seqed_items: Vec::new(),
        next_seq: 0,
        min_seq: 0,
    }
}

#[cfg(unix)]
#[tokio::test]
async fn root_recovery_new_intent_skips_prepublication_read_only_owner() {
    let (first_temp, first_disk) = recovery_disk().await;
    let (second_temp, second_disk) = recovery_disk().await;
    let first_endpoint = first_disk.endpoint().to_string();
    let (read_only_disk, writable_disk) = ordered_recovery_disks(first_disk, second_disk);
    let read_only_root = if read_only_disk.endpoint().to_string() == first_endpoint {
        first_temp.path()
    } else {
        second_temp.path()
    };
    let _restore = RestoreDirectoryMode::read_only(read_only_root.join(RUSTFS_META_BUCKET));
    let manager = recovery_manager(vec![read_only_disk.clone(), writable_disk.clone()]);
    let mut request = admin_request(HealType::Object {
        bucket: "bucket".to_string(),
        object: "object".to_string(),
        version_id: None,
    });

    let receipt = manager
        .submit_heal_request_with_receipt(request.clone())
        .await
        .expect("a writable local disk should own the admin heal intent");
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    let path = format!("root-heal-{}.json", request.id);
    assert!(matches!(
        read_only_disk.read_all(RUSTFS_META_BUCKET, &path).await,
        Err(DiskError::FileNotFound)
    ));
    assert!(writable_disk.read_all(RUSTFS_META_BUCKET, &path).await.is_ok());

    request.retry_attempts = 1;
    manager
        .root_recovery
        .persist(&request)
        .await
        .expect("an existing fallback owner should remain updateable");
    let pending = manager.root_recovery.pending().await.expect("read the single durable owner");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, request.id);
    assert_eq!(pending[0].retry_attempts, 1);
}

#[cfg(unix)]
#[tokio::test]
async fn root_recovery_existing_owner_never_migrates_after_write_rejection() {
    let (first_temp, first_disk) = recovery_disk().await;
    let (second_temp, second_disk) = recovery_disk().await;
    let first_endpoint = first_disk.endpoint().to_string();
    let (owner_disk, alternate_disk) = ordered_recovery_disks(first_disk, second_disk);
    let owner_root = if owner_disk.endpoint().to_string() == first_endpoint {
        first_temp.path()
    } else {
        second_temp.path()
    };
    let manager = recovery_manager(vec![owner_disk.clone(), alternate_disk.clone()]);
    let mut request = root_request();
    manager
        .root_recovery
        .persist(&request)
        .await
        .expect("create the canonical owner");
    let path = format!("root-heal-{}.json", request.id);
    let committed = owner_disk
        .read_all(RUSTFS_META_BUCKET, &path)
        .await
        .expect("canonical owner bytes");
    let _restore = RestoreDirectoryMode::read_only(owner_root.join(RUSTFS_META_BUCKET));

    request.retry_attempts = 1;
    assert!(
        manager.root_recovery.persist(&request).await.is_err(),
        "an existing owner write rejection must fail closed"
    );
    assert_eq!(
        owner_disk
            .read_all(RUSTFS_META_BUCKET, &path)
            .await
            .expect("original owner remains"),
        committed
    );
    assert!(matches!(
        alternate_disk.read_all(RUSTFS_META_BUCKET, &path).await,
        Err(DiskError::FileNotFound)
    ));
}

async fn active_root(manager: &HealManager, request: HealRequest) -> Arc<HealTask> {
    let task = Arc::new(HealTask::from_request(request, manager.storage.clone()));
    *task.status.write().await = HealTaskStatus::Running;
    task.progress.write().await.update_object_progress(1, 1, 0, 0, 128);
    manager.active_heals.lock().await.insert(task.id.clone(), task.clone());
    task
}

#[tokio::test]
async fn root_recovery_shutdown_restart_replays_same_id_and_success_retires_intent() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let mut request = root_request();
    request.options.recursive = true;
    let task = active_root(&manager, request.clone()).await;
    manager.stop().await.expect("durable shutdown handoff");
    assert!(task.cancel_token.is_cancelled());
    drop(manager);

    let restarted = recovery_manager(vec![disk]);
    restarted.replay_root_heals().await.expect("replay durable root");
    restarted.replay_root_heals().await.expect("replay is idempotent");
    assert_eq!(restarted.get_queue_length().await, 1);
    let restored = restarted
        .heal_queue
        .lock()
        .await
        .requests()
        .next()
        .cloned()
        .expect("restored request");
    assert_eq!(restored.id, request.id);
    assert_eq!(restored.options, request.options);
    assert_eq!(restored.priority, request.priority);
    assert_eq!(restored.retry_attempts, request.retry_attempts);
    assert_eq!(restored.created_at, request.created_at);

    process_manager_queue_once(&restarted).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(restarted.get_task_status(&request.id).await, Ok(HealTaskStatus::Completed))
                && !restarted.active_heals.lock().await.contains_key(&request.id)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("restored root executes successfully");
    assert!(restarted.root_recovery.pending().await.expect("read completion").is_empty());
}

#[tokio::test]
async fn root_recovery_admin_start_persists_before_shutdown() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let mut request = root_request();
    request.options.recursive = true;
    let receipt = manager
        .submit_heal_request_with_receipt(request.clone())
        .await
        .expect("root admission should persist");
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    assert_eq!(receipt.task_id, request.id);
    let pending = manager.root_recovery.pending().await.expect("read durable admission");
    assert_eq!(
        pending.iter().map(|request| request.id.as_str()).collect::<Vec<_>>(),
        [request.id.as_str()]
    );
    drop(manager);

    let restarted = recovery_manager(vec![disk]);
    restarted.replay_root_heals().await.expect("replay durable admission");
    let queued = restarted
        .heal_queue
        .lock()
        .await
        .requests()
        .map(|request| request.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(queued, [request.id]);
}

#[tokio::test]
async fn root_recovery_admin_non_root_types_persist_and_replay() {
    for heal_type in [
        HealType::Bucket {
            bucket: "bucket".to_string(),
        },
        HealType::Prefix {
            bucket: "bucket".to_string(),
            prefix: "logs/2026".to_string(),
        },
        HealType::Object {
            bucket: "bucket".to_string(),
            object: "object".to_string(),
            version_id: Some("version-1".to_string()),
        },
        HealType::Metadata {
            bucket: "bucket".to_string(),
            object: "object".to_string(),
        },
        HealType::ECDecode {
            bucket: "bucket".to_string(),
            object: "object".to_string(),
            version_id: None,
        },
        HealType::ErasureSet {
            buckets: vec!["bucket".to_string()],
            set_disk_id: "pool_0_set_1".to_string(),
        },
    ] {
        let (_temp, disk) = recovery_disk().await;
        let manager = recovery_manager(vec![disk.clone()]);
        let mut request = admin_request(heal_type.clone());
        request.options.recursive = true;
        let receipt = manager
            .submit_heal_request_with_receipt(request.clone())
            .await
            .expect("admin heal admission should persist");
        assert_eq!(receipt.result, HealAdmissionResult::Accepted);
        assert_eq!(
            manager
                .root_recovery
                .pending()
                .await
                .expect("read durable admin state")
                .iter()
                .map(|request| (&request.id, &request.heal_type))
                .collect::<Vec<_>>(),
            [(&request.id, &request.heal_type)]
        );
        drop(manager);

        let restarted = recovery_manager(vec![disk]);
        restarted.replay_root_heals().await.expect("replay durable admin heal");
        let queued = restarted.heal_queue.lock().await.requests().cloned().collect::<Vec<_>>();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].id, request.id);
        assert_eq!(queued[0].heal_type, request.heal_type);
        assert_eq!(queued[0].options, request.options);
        assert_eq!(queued[0].source, HealRequestSource::Admin);
    }
}

#[tokio::test]
async fn root_recovery_non_admin_request_is_not_persisted() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let mut request = HealRequest::new(
        HealType::Bucket {
            bucket: "scanner".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    request.source = HealRequestSource::Scanner;
    assert_eq!(
        manager
            .submit_heal_request(request)
            .await
            .expect("scanner request should still queue"),
        HealAdmissionResult::Accepted
    );
    assert!(manager.root_recovery.pending().await.expect("read durable state").is_empty());
    drop(manager);

    let restarted = recovery_manager(vec![disk]);
    restarted.replay_root_heals().await.expect("replay empty durable state");
    assert_eq!(restarted.get_queue_length().await, 0);
}

#[tokio::test]
async fn root_recovery_path_cancel_covers_durable_only_non_root_record() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let mut request = admin_request(HealType::Bucket {
        bucket: "bucket".to_string(),
    });
    request.options.scan_mode = rustfs_heal_contracts::heal_channel::HealScanMode::Deep;
    request.options.dry_run = true;
    request.options.recreate_missing = false;
    manager
        .root_recovery
        .persist(&request)
        .await
        .expect("durable bucket responsibility");
    assert_eq!(
        manager
            .cancel_tasks_for_path("bucket")
            .await
            .expect("cancel durable-only bucket path"),
        1
    );
    drop(manager);

    let restarted = recovery_manager(vec![disk]);
    restarted
        .replay_root_heals()
        .await
        .expect("restart after durable path cancellation");
    assert_eq!(restarted.get_queue_length().await, 0);
    assert_eq!(
        restarted
            .get_task_status_for_path("bucket", &request.id)
            .await
            .expect("durable cancellation remains queryable by path"),
        HealTaskStatus::Cancelled
    );
    assert_eq!(
        restarted
            .get_task_status(&request.id)
            .await
            .expect("durable cancellation remains queryable by id"),
        HealTaskStatus::Cancelled
    );
    assert_eq!(
        restarted
            .get_task_report(&request.id)
            .await
            .expect("durable cancellation report")
            .options,
        Some(request.options)
    );
}

#[tokio::test]
async fn root_recovery_active_cancel_is_queryable_after_restart_for_scoped_admin() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let request = admin_request(HealType::Bucket {
        bucket: "bucket".to_string(),
    });
    active_root(&manager, request.clone()).await;
    manager
        .root_recovery
        .persist(&request)
        .await
        .expect("durable active bucket responsibility");

    manager.cancel_task(&request.id).await.expect("cancel active bucket");
    assert!(
        manager
            .root_recovery
            .pending()
            .await
            .expect("active terminal retires intent")
            .is_empty()
    );
    drop(manager);

    let restarted = recovery_manager(vec![disk]);
    restarted
        .replay_root_heals()
        .await
        .expect("restart after active cancellation");
    assert_eq!(restarted.get_queue_length().await, 0);
    assert_eq!(
        restarted
            .get_task_status(&request.id)
            .await
            .expect("active cancellation remains queryable by id"),
        HealTaskStatus::Cancelled
    );
    assert_eq!(
        restarted
            .get_task_status_for_path("bucket", &request.id)
            .await
            .expect("active cancellation remains queryable by path"),
        HealTaskStatus::Cancelled
    );
}

#[tokio::test]
async fn root_recovery_terminal_receipt_wins_over_stale_pending_scoped_intent_after_restart() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let request = admin_request(HealType::Bucket {
        bucket: "bucket".to_string(),
    });
    manager
        .root_recovery
        .persist(&request)
        .await
        .expect("durable bucket responsibility");
    manager
        .publish_admin_cancelled_terminal(&request.id, &request.heal_type, request.source, &request.options)
        .await
        .expect("publish terminal receipt");
    manager
        .root_recovery
        .persist(&request)
        .await
        .expect("restore stale pending intent after terminal publication");
    assert!(
        manager
            .root_recovery
            .pending()
            .await
            .expect("terminal masks stale pending")
            .is_empty()
    );
    drop(manager);

    let restarted = recovery_manager(vec![disk]);
    restarted
        .replay_root_heals()
        .await
        .expect("restart with terminal and stale pending");
    assert_eq!(restarted.get_queue_length().await, 0);
    assert_eq!(
        restarted
            .get_task_status(&request.id)
            .await
            .expect("terminal status survives stale pending"),
        HealTaskStatus::Cancelled
    );
}

#[tokio::test]
async fn root_recovery_completed_non_root_admin_is_queryable_after_restart() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let mut request = admin_request(HealType::Bucket {
        bucket: "bucket".to_string(),
    });
    request.options.scan_mode = rustfs_heal_contracts::heal_channel::HealScanMode::Deep;
    request.options.dry_run = true;
    request.options.recreate_missing = false;
    manager
        .root_recovery
        .persist(&request)
        .await
        .expect("durable bucket responsibility");
    let completed = CompletedHealStatus {
        outcome: None,
        heal_type: request.heal_type.clone(),
        options: request.options.clone(),
        status: HealTaskStatus::Completed,
        progress: Some(HealProgress {
            objects_scanned: 2,
            objects_healed: 2,
            bytes_processed: 128,
            ..Default::default()
        }),
        retained_bytes: std::sync::OnceLock::new(),
        result_items_truncated: false,
        completed_at: SystemTime::now(),
        seqed_items: Vec::new(),
        next_seq: 0,
        min_seq: 0,
    };
    assert!(
        manager
            .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
            .await
            .expect("publish completed terminal")
    );
    assert!(
        manager
            .root_recovery
            .pending()
            .await
            .expect("completed terminal retires intent")
            .is_empty()
    );
    drop(manager);

    let restarted = recovery_manager(vec![disk]);
    restarted.replay_root_heals().await.expect("restart after completed terminal");
    assert_eq!(restarted.get_queue_length().await, 0);
    assert_eq!(
        restarted
            .get_task_status_for_path("bucket", &request.id)
            .await
            .expect("completed terminal remains queryable by path"),
        HealTaskStatus::Completed
    );
    let progress = restarted
        .get_task_progress(&request.id)
        .await
        .expect("completed terminal exposes progress");
    assert_eq!(progress.objects_scanned, 2);
    assert_eq!(progress.objects_healed, 2);
    assert_eq!(
        restarted
            .get_task_report(&request.id)
            .await
            .expect("completed terminal report")
            .options,
        Some(request.options)
    );
}

#[tokio::test]
async fn root_recovery_legacy_terminal_without_options_uses_defaults() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let request = admin_request(HealType::Bucket {
        bucket: "legacy-bucket".to_string(),
    });
    let completed = completed_admin_status(&request.heal_type, SystemTime::now());
    manager
        .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
        .await
        .expect("publish terminal receipt");

    let path = format!("terminal-root-heal-{}.json", request.id);
    let bytes = disk.read_all(RUSTFS_META_BUCKET, &path).await.expect("read terminal receipt");
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("decode terminal receipt");
    value.as_object_mut().expect("terminal object").remove("options");
    disk.write_all(
        RUSTFS_META_BUCKET,
        &path,
        serde_json::to_vec(&value).expect("encode legacy receipt").into(),
    )
    .await
    .expect("write legacy terminal receipt");
    drop(manager);

    let restarted = recovery_manager(vec![disk]);
    let report = restarted
        .get_task_report(&request.id)
        .await
        .expect("legacy terminal remains queryable");
    assert_eq!(report.status, HealTaskStatus::Completed);
    assert_eq!(report.options, Some(HealOptions::default()));
}

#[tokio::test]
async fn root_recovery_terminal_receipt_ttl_boundary_matches_completed_status_retention() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk]);
    let request = admin_request(HealType::Bucket {
        bucket: "bucket".to_string(),
    });
    let completed_at = SystemTime::now();
    let completed = completed_admin_status(&request.heal_type, completed_at);
    assert!(
        manager
            .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
            .await
            .expect("publish terminal receipt")
    );

    let boundary = completed_at + KEEP_HEAL_TASK_STATUS_DURATION;
    assert_eq!(
        manager
            .root_recovery
            .completed(&request.id)
            .await
            .expect("read retained terminal")
            .expect("terminal retained at exact TTL boundary")
            .status,
        HealTaskStatus::Completed
    );
    let report = manager
        .root_recovery
        .gc_terminal_receipts_once(boundary)
        .await
        .expect("boundary GC");
    assert_eq!(report.terminals_removed, 0);
    assert_eq!(
        manager
            .root_recovery
            .completed(&request.id)
            .await
            .expect("read retained terminal")
            .expect("terminal retained before wall-clock advances")
            .status,
        HealTaskStatus::Completed
    );

    let expired = boundary + Duration::from_nanos(1);
    let report = manager
        .root_recovery
        .gc_terminal_receipts_once(expired)
        .await
        .expect("expired GC");
    assert_eq!(report.terminals_removed, 1);
    assert!(matches!(manager.get_task_status(&request.id).await, Err(Error::TaskNotFound { .. })));
}

#[tokio::test]
async fn root_recovery_terminal_gc_removes_stale_pending_before_expired_receipt() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let request = admin_request(HealType::Bucket {
        bucket: "bucket".to_string(),
    });
    manager
        .root_recovery
        .persist(&request)
        .await
        .expect("durable bucket responsibility");
    let now = SystemTime::now();
    let completed = completed_admin_status(&request.heal_type, now - KEEP_HEAL_TASK_STATUS_DURATION - Duration::from_nanos(1));
    assert!(
        manager
            .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
            .await
            .expect("publish expired terminal receipt")
    );
    manager
        .root_recovery
        .persist(&request)
        .await
        .expect("recreate stale pending intent after terminal publication");
    assert!(
        manager
            .root_recovery
            .pending()
            .await
            .expect("terminal still masks stale pending")
            .is_empty(),
        "an expired receipt must continue masking stale pending until GC retires the pending owner"
    );
    assert!(matches!(manager.get_task_status(&request.id).await, Err(Error::TaskNotFound { .. })));

    let first = manager
        .root_recovery
        .gc_terminal_receipts_once(now)
        .await
        .expect("first GC removes stale pending only");
    assert_eq!(first.pending_removed, 1);
    assert_eq!(first.terminals_removed, 0);
    assert!(
        disk.read_all(RUSTFS_META_BUCKET, &format!("terminal-root-heal-{}.json", request.id))
            .await
            .is_ok()
    );
    assert!(manager.root_recovery.pending().await.expect("pending retired").is_empty());

    let second = manager
        .root_recovery
        .gc_terminal_receipts_once(now)
        .await
        .expect("second GC removes unneeded expired terminal");
    assert_eq!(second.pending_removed, 0);
    assert_eq!(second.terminals_removed, 1);
    assert!(
        disk.read_all(RUSTFS_META_BUCKET, &format!("terminal-root-heal-{}.json", request.id))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn root_recovery_terminal_gc_is_delete_budget_bounded() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let now = SystemTime::now();
    let expired_at = now - KEEP_HEAL_TASK_STATUS_DURATION - Duration::from_nanos(1);
    for _ in 0..=64 {
        let request = admin_request(HealType::Bucket {
            bucket: "bucket".to_string(),
        });
        let completed = completed_admin_status(&request.heal_type, expired_at);
        manager
            .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
            .await
            .expect("publish expired terminal receipt");
    }

    let report = manager
        .root_recovery
        .gc_terminal_receipts_once(now)
        .await
        .expect("budgeted terminal GC");
    assert_eq!(report.terminals_removed, 64);
    assert!(report.budget_exhausted);
    let terminal_entries = disk
        .list_dir("", RUSTFS_META_BUCKET, "", -1)
        .await
        .expect("list remaining terminal receipts")
        .into_iter()
        .filter(|entry| entry.starts_with("terminal-root-heal-"))
        .count();
    assert_eq!(terminal_entries, 1);
}

#[tokio::test]
async fn root_recovery_corrupt_terminal_receipt_retains_pending_fail_closed() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let request = admin_request(HealType::Bucket {
        bucket: "bucket".to_string(),
    });
    manager
        .root_recovery
        .persist(&request)
        .await
        .expect("durable bucket responsibility");
    let completed = completed_admin_status(
        &request.heal_type,
        SystemTime::now() - KEEP_HEAL_TASK_STATUS_DURATION - Duration::from_nanos(1),
    );
    manager
        .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
        .await
        .expect("publish terminal receipt");
    manager
        .root_recovery
        .persist(&request)
        .await
        .expect("restore stale pending intent");
    disk.write_all(
        RUSTFS_META_BUCKET,
        &format!("terminal-root-heal-{}.json", request.id),
        br#"{"schema":1,"task_id":"not-the-same-id"}"#.to_vec().into(),
    )
    .await
    .expect("corrupt terminal receipt");

    assert!(
        manager
            .root_recovery
            .gc_terminal_receipts_once(SystemTime::now())
            .await
            .is_err(),
        "corrupt terminal receipt must fail closed"
    );
    assert!(
        disk.read_all(RUSTFS_META_BUCKET, &format!("root-heal-{}.json", request.id))
            .await
            .is_ok()
    );
    assert!(manager.root_recovery.pending().await.is_err());
}

#[tokio::test]
async fn root_recovery_admin_start_fails_closed_when_owner_is_unavailable() {
    let (_temp, disk) = recovery_disk().await;
    let (unavailable_temp, unavailable) = recovery_disk().await;
    std::fs::remove_dir_all(unavailable_temp.path().join(RUSTFS_META_BUCKET)).expect("make owner volume unavailable");
    let manager = recovery_manager(vec![unavailable, disk.clone()]);
    let request = admin_request(HealType::Bucket {
        bucket: "bucket".to_string(),
    });

    assert!(
        manager.submit_heal_request_with_receipt(request).await.is_err(),
        "admin admission must fail closed when the durable owner cannot be checked"
    );
    assert_eq!(manager.get_queue_length().await, 0);
    assert!(
        RootHealRecovery::with_disks(vec![disk])
            .pending()
            .await
            .expect("other disk remains empty")
            .is_empty()
    );
}

#[tokio::test]
async fn root_recovery_force_start_cancels_only_overlapping_durable_admin_records() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let old = admin_request(HealType::Bucket {
        bucket: "bucket-a".to_string(),
    });
    let disjoint = admin_request(HealType::Bucket {
        bucket: "bucket-b".to_string(),
    });
    manager.root_recovery.persist(&old).await.expect("old bucket owner");
    manager.root_recovery.persist(&disjoint).await.expect("disjoint bucket owner");

    let mut replacement = admin_request(HealType::Prefix {
        bucket: "bucket-a".to_string(),
        prefix: "logs/".to_string(),
    });
    replacement.force_start = true;
    assert_eq!(
        manager
            .submit_heal_request(replacement.clone())
            .await
            .expect("forceStart should replace only the overlapping durable owner"),
        HealAdmissionResult::Accepted
    );

    let mut pending = manager
        .root_recovery
        .pending()
        .await
        .expect("read durable owners")
        .into_iter()
        .map(|request| (request.id, request.heal_type))
        .collect::<Vec<_>>();
    pending.sort_by(|left, right| left.0.cmp(&right.0));
    let mut expected = vec![
        (disjoint.id.clone(), disjoint.heal_type.clone()),
        (replacement.id.clone(), replacement.heal_type.clone()),
    ];
    expected.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(pending, expected);
    drop(manager);

    let restarted = recovery_manager(vec![disk]);
    restarted.replay_root_heals().await.expect("replay surviving owners");
    let queued_ids = restarted
        .heal_queue
        .lock()
        .await
        .requests()
        .map(|request| request.id.clone())
        .collect::<HashSet<_>>();
    assert_eq!(queued_ids, HashSet::from([disjoint.id, replacement.id]));
}

#[tokio::test]
async fn root_recovery_admin_overlap_rejects_durable_only_owners_without_writing_new_intents() {
    for (existing, incoming, reason) in [
        ("scope/", "scope/", HealAdmissionDropReason::AlreadyRunning),
        ("scope/", "scope/child/", HealAdmissionDropReason::OverlappingPaths),
        ("scope/child/", "scope/", HealAdmissionDropReason::OverlappingPaths),
    ] {
        let (_temp, disk) = recovery_disk().await;
        let manager = recovery_manager(vec![disk]);
        let mut owner = admin_prefix_request("bucket", existing);
        owner.options.timeout = Some(Duration::ZERO);
        manager.root_recovery.persist(&owner).await.expect("persist unreplayed owner");
        let receipt = manager
            .submit_heal_request_with_receipt(admin_prefix_request("bucket", incoming))
            .await
            .expect("durable overlap decision");
        assert_eq!(receipt.result, HealAdmissionResult::Dropped(reason));
        assert_eq!(receipt.task_id, owner.id);
        assert!(manager.heal_queue.lock().await.is_empty());
        let pending = manager
            .root_recovery
            .pending()
            .await
            .expect("original durable responsibility remains");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, owner.id);
        assert_eq!(
            pending[0].options.timeout,
            Some(Duration::ZERO),
            "admission must not reset an exhausted budget"
        );
    }
}

#[tokio::test]
async fn root_recovery_admin_overlap_same_id_does_not_overwrite_a_durable_only_budget() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk]);
    let mut owner = admin_prefix_request("bucket", "scope/");
    owner.options.timeout = Some(Duration::ZERO);
    manager.root_recovery.persist(&owner).await.expect("exhausted owner");
    let mut replay = owner.clone();
    replay.options.timeout = Some(Duration::from_secs(60));
    replay.force_start = true;
    let receipt = manager
        .submit_heal_request_with_receipt(replay)
        .await
        .expect("same ID conflicts with durable owner");
    assert_eq!(receipt.result, HealAdmissionResult::Dropped(HealAdmissionDropReason::AlreadyRunning));
    let pending = manager.root_recovery.pending().await.expect("retained owner");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].options.timeout, Some(Duration::ZERO));
    assert!(manager.heal_queue.lock().await.is_empty());
}

#[tokio::test]
async fn root_recovery_admin_overlap_corrupt_preflight_does_not_cancel_a_live_owner() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let owner = admin_prefix_request("bucket", "scope/child/");
    manager
        .submit_heal_request(owner.clone())
        .await
        .expect("admit original owner");
    let corrupt = root_request();
    let path = format!("root-heal-{}.json", corrupt.id);
    disk.write_all(RUSTFS_META_BUCKET, &path, b"{".to_vec().into())
        .await
        .expect("inject corrupt ownership record");
    let mut replacement = admin_prefix_request("bucket", "scope/");
    replacement.force_start = true;
    assert!(
        manager.submit_heal_request(replacement).await.is_err(),
        "unknown ownership must fail before cancellation"
    );
    assert_eq!(
        manager
            .heal_queue
            .lock()
            .await
            .requests()
            .map(|request| request.id.clone())
            .collect::<Vec<_>>(),
        vec![owner.id.clone()]
    );
    assert_eq!(
        manager
            .get_task_status(&owner.id)
            .await
            .expect("original owner remains queryable"),
        HealTaskStatus::Pending
    );
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, &path)
            .await
            .expect("retain corrupt record")
            .as_ref(),
        b"{"
    );
}

#[tokio::test]
async fn root_recovery_admin_overlap_preserves_legacy_owners_and_replays_replacement_cancellations() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let parent = admin_prefix_request("bucket", "scope/");
    let child = admin_prefix_request("bucket", "scope/child/");
    manager
        .root_recovery
        .persist(&parent)
        .await
        .expect("legacy parent responsibility");
    manager
        .root_recovery
        .persist(&child)
        .await
        .expect("legacy child responsibility");
    manager
        .replay_root_heals()
        .await
        .expect("accepted legacy owners must not be discarded");
    assert_eq!(manager.heal_queue.lock().await.len(), 2);
    let rejected = manager
        .submit_heal_request_with_receipt(admin_prefix_request("bucket", "scope/child/deep/"))
        .await
        .expect("new overlap must reject after replay");
    assert_eq!(rejected.result, HealAdmissionResult::Dropped(HealAdmissionDropReason::OverlappingPaths));
    let mut replacement = admin_prefix_request("bucket", "scope/");
    replacement.force_start = true;
    let receipt = manager
        .submit_heal_request_with_receipt(replacement)
        .await
        .expect("replace both recovered owners");
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    drop(manager);
    let restarted = recovery_manager(vec![disk]);
    restarted.replay_root_heals().await.expect("restart replacement");
    assert_eq!(
        restarted
            .heal_queue
            .lock()
            .await
            .requests()
            .map(|request| request.id.clone())
            .collect::<Vec<_>>(),
        vec![receipt.task_id]
    );
    for owner in [&parent.id, &child.id] {
        assert_eq!(
            restarted.get_task_status(owner).await.expect("cancellation survives restart"),
            HealTaskStatus::Cancelled
        );
    }
}

#[tokio::test]
async fn root_recovery_queued_non_root_admin_owner_is_not_priority_displaced() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    manager.config.write().await.queue_size = 1;
    let mut durable = admin_request(HealType::Bucket {
        bucket: "bucket".to_string(),
    });
    durable.priority = HealPriority::Low;
    assert_eq!(
        manager
            .submit_heal_request(durable.clone())
            .await
            .expect("low-priority admin bucket should queue durably"),
        HealAdmissionResult::Accepted
    );

    let mut urgent = HealRequest::new(
        HealType::Object {
            bucket: "other".to_string(),
            object: "object".to_string(),
            version_id: None,
        },
        HealOptions::default(),
        HealPriority::Urgent,
    );
    urgent.source = HealRequestSource::Internal;
    assert_eq!(
        manager
            .submit_heal_request(urgent)
            .await
            .expect("durable admin owner cannot be displaced"),
        HealAdmissionResult::Full
    );
    assert_eq!(
        manager
            .root_recovery
            .pending()
            .await
            .expect("read durable bucket")
            .iter()
            .map(|request| request.id.as_str())
            .collect::<Vec<_>>(),
        [durable.id.as_str()]
    );
}

#[tokio::test]
async fn root_recovery_legacy_schema_replays_as_cluster() {
    #[derive(serde::Serialize)]
    struct LegacyRootHealIntent<'a> {
        schema: u32,
        task_id: &'a str,
        options: &'a HealOptions,
        priority: HealPriority,
        retry_attempts: u32,
        created_at: SystemTime,
    }

    let (_temp, disk) = recovery_disk().await;
    let request = root_request();
    let path = format!("root-heal-{}.json", request.id);
    let bytes = serde_json::to_vec(&LegacyRootHealIntent {
        schema: 1,
        task_id: &request.id,
        options: &request.options,
        priority: request.priority,
        retry_attempts: request.retry_attempts,
        created_at: request.created_at,
    })
    .expect("legacy root recovery JSON");
    disk.write_all(RUSTFS_META_BUCKET, &path, bytes.into())
        .await
        .expect("write legacy root record");

    let restarted = recovery_manager(vec![disk]);
    restarted.replay_root_heals().await.expect("replay legacy root record");
    let queued = restarted.heal_queue.lock().await.requests().cloned().collect::<Vec<_>>();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].id, request.id);
    assert_eq!(queued[0].heal_type, HealType::Cluster);
}

#[tokio::test]
async fn root_recovery_rejected_admin_start_does_not_persist() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    manager.config.write().await.queue_size = 0;
    let request = root_request();

    let receipt = manager
        .submit_heal_request_with_receipt(request.clone())
        .await
        .expect("full admission reports a receipt");
    assert_eq!(receipt.result, HealAdmissionResult::Full);
    assert!(manager.root_recovery.pending().await.expect("read durable state").is_empty());
    drop(manager);

    let restarted = recovery_manager(vec![disk]);
    restarted.replay_root_heals().await.expect("replay empty durable state");
    assert_eq!(restarted.get_queue_length().await, 0);
}

#[tokio::test]
async fn root_recovery_queued_owner_is_not_priority_displaced() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    manager.config.write().await.queue_size = 1;
    let mut root = root_request();
    root.priority = HealPriority::Low;
    assert_eq!(
        manager
            .submit_heal_request(root.clone())
            .await
            .expect("low-priority root should queue"),
        HealAdmissionResult::Accepted
    );

    let mut bucket = HealRequest::new(
        HealType::Bucket {
            bucket: "bucket".to_string(),
        },
        HealOptions::default(),
        HealPriority::Urgent,
    );
    // Internal urgent work has the same displacement eligibility without
    // taking the administrator overlap rejection before the capacity check.
    bucket.source = HealRequestSource::Internal;
    assert_eq!(
        manager
            .submit_heal_request(bucket)
            .await
            .expect("durable root owner cannot be displaced"),
        HealAdmissionResult::Full
    );
    let pending = manager.root_recovery.pending().await.expect("read durable root");
    assert_eq!(pending.iter().map(|request| request.id.as_str()).collect::<Vec<_>>(), [root.id.as_str()]);
    let queued = manager
        .heal_queue
        .lock()
        .await
        .requests()
        .map(|request| request.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(queued, [root.id]);
}

#[tokio::test]
async fn root_recovery_explicit_cancel_covers_active_queued_retrying_and_durable_only() {
    for state in ["active", "queued", "retrying", "durable_only", "root_path"] {
        let (_temp, disk) = recovery_disk().await;
        let manager = recovery_manager(vec![disk.clone()]);
        let request = root_request();
        manager.root_recovery.persist(&request).await.expect("durable responsibility");
        match state {
            "active" => {
                active_root(&manager, request.clone()).await;
            }
            "queued" => {
                manager.replay_root_heals().await.expect("queued recovery");
            }
            "retrying" => {
                insert_retrying_request(&manager, request.clone()).await;
            }
            _ => {}
        }
        if state == "root_path" {
            assert_eq!(manager.cancel_tasks_for_path("").await.expect("cancel durable root path"), 1);
        } else {
            manager.cancel_task(&request.id).await.expect("cancel root responsibility");
        }
        drop(manager);
        let restarted = recovery_manager(vec![disk]);
        restarted
            .replay_root_heals()
            .await
            .expect("restart after explicit cancellation");
        assert_eq!(restarted.get_queue_length().await, 0, "state={state}");
    }
}

#[tokio::test]
async fn root_recovery_force_start_cancels_durable_only_responsibility() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let old = root_request();
    manager
        .root_recovery
        .persist(&old)
        .await
        .expect("old terminal responsibility");
    let mut new = root_request();
    new.force_start = true;
    assert_eq!(
        manager
            .submit_heal_request(new.clone())
            .await
            .expect("force start replacement"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(
        manager
            .root_recovery
            .pending()
            .await
            .expect("new owner retained")
            .iter()
            .map(|request| request.id.as_str())
            .collect::<Vec<_>>(),
        [new.id.as_str()]
    );
    manager.stop().await.expect("persist new root only");
    let restarted = recovery_manager(vec![disk]);
    restarted.replay_root_heals().await.expect("restart replacement");
    let ids = restarted
        .heal_queue
        .lock()
        .await
        .requests()
        .map(|request| request.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, [new.id]);
}

#[tokio::test]
async fn root_recovery_force_start_preserves_disjoint_durable_only_admin_work() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let old_overlap = admin_request(HealType::Bucket {
        bucket: "overlap".to_string(),
    });
    let old_disjoint = admin_request(HealType::Bucket {
        bucket: "disjoint".to_string(),
    });
    manager
        .root_recovery
        .persist(&old_overlap)
        .await
        .expect("durable overlapping bucket");
    manager
        .root_recovery
        .persist(&old_disjoint)
        .await
        .expect("durable disjoint bucket");

    let mut replacement = admin_request(HealType::Bucket {
        bucket: "overlap".to_string(),
    });
    replacement.force_start = true;
    assert_eq!(
        manager
            .submit_heal_request(replacement.clone())
            .await
            .expect("forceStart replaces overlapping durable owner"),
        HealAdmissionResult::Accepted
    );
    drop(manager);

    let restarted = recovery_manager(vec![disk]);
    restarted
        .replay_root_heals()
        .await
        .expect("restart after selective forceStart");
    let mut ids = restarted
        .heal_queue
        .lock()
        .await
        .requests()
        .map(|request| request.id.clone())
        .collect::<Vec<_>>();
    ids.sort();
    let mut expected = vec![old_disjoint.id, replacement.id];
    expected.sort();
    assert_eq!(ids, expected);
}

#[tokio::test]
async fn root_recovery_force_start_replaces_fresh_queued_and_retrying_admin_roots() {
    for retrying in [false, true] {
        let (_temp, disk) = recovery_disk().await;
        let manager = recovery_manager(vec![disk.clone()]);
        let old = root_request();
        if retrying {
            insert_retrying_request(&manager, old.clone()).await;
        } else {
            manager.submit_heal_request(old.clone()).await.expect("queue original root");
        }
        if retrying {
            assert!(
                manager
                    .root_recovery
                    .pending()
                    .await
                    .expect("retrying not handed off yet")
                    .is_empty()
            );
        } else {
            assert_eq!(
                manager
                    .root_recovery
                    .pending()
                    .await
                    .expect("queued root is durable immediately")
                    .iter()
                    .map(|request| request.id.as_str())
                    .collect::<Vec<_>>(),
                [old.id.as_str()]
            );
        }
        let mut new = root_request();
        new.force_start = true;
        assert_eq!(
            manager.submit_heal_request(new.clone()).await.expect("force replacement"),
            HealAdmissionResult::Accepted
        );
        manager.stop().await.expect("handoff only the new responsibility");
        let restarted = recovery_manager(vec![disk]);
        restarted.replay_root_heals().await.expect("restart after forceStart");
        let ids = restarted
            .heal_queue
            .lock()
            .await
            .requests()
            .map(|request| request.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(ids, [new.id], "retrying={retrying}; old={}", old.id);
    }
}

#[tokio::test]
async fn root_recovery_invalid_records_are_retained_without_partial_replay() {
    for kind in ["truncated", "schema", "identity", "option", "no_lock"] {
        let (_temp, disk) = recovery_disk().await;
        let manager = recovery_manager(vec![disk.clone()]);
        let valid = root_request();
        let invalid = root_request();
        manager.root_recovery.persist(&valid).await.expect("valid root record");
        manager
            .root_recovery
            .persist(&invalid)
            .await
            .expect("record before corruption");
        let path = format!("root-heal-{}.json", invalid.id);
        let original = disk.read_all(RUSTFS_META_BUCKET, &path).await.expect("read root record");
        let mut value: serde_json::Value = serde_json::from_slice(&original).expect("record JSON");
        match kind {
            "schema" => value["schema"] = 3.into(),
            "identity" => value["task_id"] = valid.id.clone().into(),
            "option" => value["options"]["future_delete_mode"] = true.into(),
            "no_lock" => value["options"]["no_lock"] = true.into(),
            _ => {}
        }
        let bytes = if kind == "truncated" {
            b"{".to_vec()
        } else {
            serde_json::to_vec(&value).expect("modified record")
        };
        disk.write_all(RUSTFS_META_BUCKET, &path, bytes.clone().into())
            .await
            .expect("inject bad record");
        assert!(manager.replay_root_heals().await.is_err(), "kind={kind}");
        assert_eq!(manager.get_queue_length().await, 0, "no partial admission for {kind}");
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, &path)
                .await
                .expect("bad record retained")
                .as_ref(),
            bytes
        );
        let mut forced = root_request();
        forced.force_start = true;
        assert!(
            manager.submit_heal_request(forced).await.is_err(),
            "forceStart must not discard unknown state"
        );
    }
}

#[tokio::test]
async fn root_recovery_failed_handoff_keeps_runtime_owner_and_does_not_try_another_disk() {
    let (_temp, disk) = recovery_disk().await;
    let (unavailable_temp, unavailable) = recovery_disk().await;
    std::fs::remove_dir_all(unavailable_temp.path().join(RUSTFS_META_BUCKET)).expect("make owner volume unavailable");
    let manager = recovery_manager(vec![unavailable, disk.clone()]);
    let task = active_root(&manager, root_request()).await;
    assert!(manager.stop().await.is_err());
    assert!(
        manager.cancel_task(&task.id).await.is_err(),
        "missing owner cannot acknowledge cancellation"
    );
    assert!(!manager.cancel_token.is_cancelled());
    assert!(!task.cancel_token.is_cancelled());
    assert!(manager.active_heals.lock().await.contains_key(&task.id));
    assert!(
        RootHealRecovery::with_disks(vec![disk])
            .pending()
            .await
            .expect("other disk remains empty")
            .is_empty()
    );
}

#[tokio::test]
async fn root_recovery_shutdown_fences_new_admission_and_preserves_later_cancellation() {
    for operation_kind in ["submit", "force_start", "cancel"] {
        let cancel = operation_kind == "cancel";
        let (_temp, disk) = recovery_disk().await;
        let manager = Arc::new(recovery_manager(vec![disk.clone()]));
        let request = root_request();
        active_root(&manager, request.clone()).await;
        let queue = manager.heal_queue.lock().await;
        let stopping = manager.clone();
        let stop = tokio::spawn(async move { stopping.stop().await });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if manager.active_heals.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown owns active lock while waiting for queue");
        let concurrent = manager.clone();
        let operation = tokio::spawn(async move {
            if cancel {
                concurrent.cancel_task(&request.id).await
            } else {
                let mut new = root_request();
                new.force_start = operation_kind == "force_start";
                concurrent.submit_heal_request(new).await.map(|_| ())
            }
        });
        drop(queue);
        stop.await.expect("shutdown task").expect("durable shutdown");
        let result = operation.await.expect("concurrent operation");
        assert_eq!(result.is_ok(), cancel, "operation={operation_kind}");
        let restarted = recovery_manager(vec![disk]);
        restarted.replay_root_heals().await.expect("read final responsibility");
        assert_eq!(restarted.get_queue_length().await, usize::from(!cancel));
    }
}

#[tokio::test]
async fn root_recovery_exhausted_timeout_is_not_reset_by_restart() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let mut request = root_request();
    request.options.timeout = Some(Duration::from_secs(10));
    let task = active_root(&manager, request.clone()).await;
    task.set_execution_elapsed_for_test(Duration::from_secs(11)).await;
    manager.stop().await.expect("persist exhausted execution budget");
    let restarted = recovery_manager(vec![disk]);
    restarted.replay_root_heals().await.expect("restore bounded request");
    assert_eq!(
        restarted
            .heal_queue
            .lock()
            .await
            .requests()
            .next()
            .expect("restored root")
            .options
            .timeout,
        Some(Duration::ZERO)
    );
    process_manager_queue_once(&restarted).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(restarted.get_task_status(&request.id).await, Ok(HealTaskStatus::Timeout)) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("exhausted request stays timed out");
    restarted
        .cancel_task(&request.id)
        .await
        .expect("timeout responsibility remains cancellable");
    assert!(restarted.root_recovery.pending().await.expect("retired timeout").is_empty());
}

#[tokio::test]
async fn root_recovery_shutdown_preserves_remaining_execution_budget() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let mut request = root_request();
    request.options.timeout = Some(Duration::from_secs(60));
    let task = active_root(&manager, request).await;
    task.set_execution_elapsed_for_test(Duration::from_secs(20)).await;
    manager.stop().await.expect("handoff with consumed execution time");
    let restarted = recovery_manager(vec![disk]);
    restarted.replay_root_heals().await.expect("restore remaining budget");
    let queue = restarted.heal_queue.lock().await;
    let remaining = queue
        .requests()
        .next()
        .expect("restored root")
        .options
        .timeout
        .expect("remaining timeout");
    assert!(remaining <= Duration::from_secs(40), "elapsed execution must not be refunded");
    assert!(
        remaining >= Duration::from_secs(30),
        "shutdown fixture should retain most of its remaining budget"
    );
}

#[tokio::test]
async fn root_recovery_force_start_after_shutdown_does_not_retire_original_owner() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let old = root_request();
    active_root(&manager, old.clone()).await;
    manager.stop().await.expect("handoff original root");
    let mut new = root_request();
    new.force_start = true;
    assert!(manager.submit_heal_request(new).await.is_err());
    let restarted = recovery_manager(vec![disk]);
    restarted.replay_root_heals().await.expect("original responsibility remains");
    let ids = restarted
        .heal_queue
        .lock()
        .await
        .requests()
        .map(|request| request.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, [old.id]);
}

#[tokio::test]
async fn root_recovery_terminal_timeout_updates_only_existing_journal_before_second_restart() {
    for durable in [false, true] {
        let (_temp, disk) = recovery_disk().await;
        let manager = recovery_manager(vec![disk.clone()]);
        let mut request = root_request();
        request.options.timeout = Some(Duration::from_nanos(1));
        if durable {
            manager
                .root_recovery
                .persist(&request)
                .await
                .expect("persist nonzero execution budget");
            manager.replay_root_heals().await.expect("first restart");
        } else {
            manager
                .submit_heal_request(request.clone())
                .await
                .expect("first root execution");
        }
        process_manager_queue_once(&manager).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if matches!(manager.get_task_status(&request.id).await, Ok(HealTaskStatus::Timeout))
                    && !manager.active_heals.lock().await.contains_key(&request.id)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real execution exhausts a nonzero budget");
        assert!(!manager.active_heals.lock().await.contains_key(&request.id));
        let restarted = recovery_manager(vec![disk]);
        restarted
            .replay_root_heals()
            .await
            .expect("second restart after terminal timeout");
        let queue = restarted.heal_queue.lock().await;
        assert_eq!(
            queue
                .requests()
                .next()
                .expect("remaining timeout responsibility")
                .options
                .timeout,
            Some(Duration::ZERO),
            "durable={durable}"
        );
    }
}
