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
    let request = admin_request(HealType::Bucket {
        bucket: "bucket".to_string(),
    });
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
    bucket.source = HealRequestSource::Admin;
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
