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

use super::super::root_recovery::{MAX_ROOT_RECOVERY_BYTES, RootHealRecovery};
use super::*;
use crate::heal::RUSTFS_META_BUCKET;
use std::collections::HashSet;

fn bucket_incarnations() -> &'static std::sync::Mutex<HashMap<String, Option<Uuid>>> {
    static IDS: std::sync::OnceLock<std::sync::Mutex<HashMap<String, Option<Uuid>>>> = std::sync::OnceLock::new();
    IDS.get_or_init(Default::default)
}

pub(super) fn test_bucket_incarnation(bucket: &str) -> Option<Uuid> {
    bucket_incarnations()
        .lock()
        .expect("bucket incarnation fixture")
        .get(bucket)
        .copied()
        .unwrap_or(Some(Uuid::from_u128(42)))
}

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
    if let HealType::Bucket { bucket } = &request.heal_type {
        request.bucket_incarnation_id = test_bucket_incarnation(bucket);
    }
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

#[tokio::test]
async fn root_recovery_terminal_outcome_survives_repeated_restart() {
    use crate::heal::outcome::{HealExecutionOutcome, HealTraversalCoverage};

    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let request = root_request();
    manager.root_recovery.persist(&request).await.expect("admit root heal");
    let mut completed = completed_admin_status(&request.heal_type, SystemTime::now());
    let mut outcome = HealTaskOutcome::default();
    outcome.execution = HealExecutionOutcome::Completed;
    outcome.coverage = HealTraversalCoverage::Complete;
    outcome.counters.processed = 257;
    outcome.counters.healed = 255;
    outcome.counters.unchanged = 2;
    completed.outcome = Some(Arc::new(outcome));
    let expected = serde_json::to_value(completed.outcome.as_deref()).expect("expected outcome");
    manager
        .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
        .await
        .expect("publish root outcome");
    drop(manager);

    for _ in 0..2 {
        let restarted = recovery_manager(vec![disk.clone()]);
        restarted.replay_root_heals().await.expect("replay terminal receipt");
        assert_eq!(restarted.get_queue_length().await, 0);
        let report = restarted.get_task_report(&request.id).await.expect("retained report");
        assert_eq!(report.status, HealTaskStatus::Completed);
        assert_eq!(serde_json::to_value(report.outcome.as_deref()).expect("restored outcome"), expected);
        let retained = restarted
            .root_recovery
            .completed(&request.id)
            .await
            .expect("receipt")
            .expect("retained");
        assert_eq!(retained.completed_at, completed.completed_at, "restart cannot renew retention");
    }
}

fn terminal_with_outcome(heal_type: &HealType, status: HealTaskStatus) -> CompletedHealStatus {
    use crate::heal::outcome::{
        HealAbortReason, HealFailureClass, HealObjectDisposition, HealObjectIdentity, HealObjectKind, HealObjectOutcome,
    };
    let mut completed = completed_admin_status(heal_type, SystemTime::now());
    let mut outcome = HealTaskOutcome::default();
    outcome.start();
    for index in 0..257 {
        let disposition = if index < 255 {
            HealObjectDisposition::Repaired
        } else {
            HealObjectDisposition::VerifiedHealthy
        };
        outcome.record(HealObjectOutcome {
            identity: HealObjectIdentity {
                kind: HealObjectKind::Object,
                bucket: "bucket".to_string(),
                object: format!("object-{index}"),
                version_id: None,
                bucket_incarnation_id: None,
                pool_index: None,
                set_index: None,
            },
            disposition,
            detail: Some("verified repair".to_string()),
        });
    }
    if matches!(status, HealTaskStatus::Failed { .. }) {
        let mut failure = outcome.objects.back().expect("last object").clone();
        failure.disposition = HealObjectDisposition::Failed(HealFailureClass::Permanent);
        outcome.record(failure);
        outcome.attempt_failed();
    }
    outcome.finish((status == HealTaskStatus::Cancelled).then_some(HealAbortReason::Cancelled));
    completed.outcome = Some(Arc::new(outcome));
    completed.status = status;
    completed.seqed_items = vec![(
        9,
        HealResultItem {
            object: "retained-object".to_string(),
            ..Default::default()
        },
    )];
    completed.next_seq = 10;
    completed.min_seq = 9;
    completed.result_items_truncated = true;
    completed
}

#[tokio::test]
async fn root_recovery_terminal_report_preserves_states_windows_and_legacy_marker() {
    for status in [
        HealTaskStatus::Completed,
        HealTaskStatus::Cancelled,
        HealTaskStatus::Failed {
            error: "permanent failure".to_string(),
        },
    ] {
        let (_temp, disk) = recovery_disk().await;
        let manager = recovery_manager(vec![disk.clone()]);
        let request = root_request();
        let completed = terminal_with_outcome(&request.heal_type, status.clone());
        manager.root_recovery.persist(&request).await.expect("admission");
        manager
            .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
            .await
            .expect("terminal report");
        let marker = disk
            .read_all(RUSTFS_META_BUCKET, &format!("terminal-root-heal-{}.json", request.id))
            .await
            .expect("legacy marker");
        let marker_json: serde_json::Value = serde_json::from_slice(&marker).expect("legacy JSON");
        let keys = marker_json
            .as_object()
            .expect("terminal object")
            .keys()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        assert_eq!(
            keys,
            HashSet::from([
                "schema",
                "task_id",
                "heal_type",
                "status",
                "options",
                "progress",
                "completed_at"
            ])
        );
        assert_eq!(marker_json["schema"], 1, "rollback readers retain their original format");
        let expected = serde_json::to_value(completed.outcome.as_deref()).expect("outcome JSON");
        assert_eq!(expected["objectsTruncated"], true);
        assert_eq!(expected["objects"].as_array().expect("window").len(), 128);
        drop(manager);
        for _ in 0..2 {
            let restarted = recovery_manager(vec![disk.clone()]);
            restarted.replay_root_heals().await.expect("restart");
            let report = restarted
                .get_task_report_since(&request.id, Some(8))
                .await
                .expect("incremental report");
            assert_eq!(report.status, status);
            assert_eq!(serde_json::to_value(report.outcome.as_deref()).expect("restored outcome"), expected);
            assert_eq!(report.result_items.len(), 1);
            assert_eq!(report.result_items[0].object, "retained-object");
            assert_eq!((report.next_seq, report.min_seq, report.result_items_truncated), (10, 9, true));
            assert_eq!(
                serde_json::to_value(report.progress).expect("progress"),
                serde_json::to_value(&completed.progress).expect("expected progress")
            );
        }
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, &format!("terminal-root-heal-{}.json", request.id))
                .await
                .expect("unchanged marker"),
            marker
        );
    }
}

#[tokio::test]
async fn root_recovery_legacy_terminal_does_not_invent_outcome() {
    let (_temp, disk) = recovery_disk().await;
    let task_id = "00000000-0000-0000-0000-000000002519";
    let mut marker: serde_json::Value = serde_json::from_str(r#"{"schema":1,"task_id":"00000000-0000-0000-0000-000000002519","heal_type":{"type":"cluster"},"status":"Cancelled","progress":null,"completed_at":{"secs_since_epoch":1,"nanos_since_epoch":0}}"#).expect("pinned legacy terminal");
    marker["completed_at"] = serde_json::to_value(SystemTime::now()).expect("current retention epoch");
    disk.write_all(
        RUSTFS_META_BUCKET,
        &format!("terminal-root-heal-{task_id}.json"),
        serde_json::to_vec(&marker).expect("legacy marker").into(),
    )
    .await
    .expect("write legacy marker");
    let manager = recovery_manager(vec![disk]);
    let report = manager.get_task_report(task_id).await.expect("legacy report");
    assert_eq!(report.status, HealTaskStatus::Cancelled);
    assert!(report.outcome.is_none(), "historical counters are unavailable");
    assert!(report.result_items_truncated, "missing historical detail must be explicit");
}

#[cfg(unix)]
#[tokio::test]
async fn root_recovery_cancelled_worker_publishes_durable_refinement_or_keeps_previous_report() {
    use crate::heal::outcome::{HealAbortReason, HealExecutionOutcome};
    for failure_mode in 0..4 {
        let (temp, disk) = recovery_disk().await;
        let manager = recovery_manager(vec![disk.clone()]);
        let bucket = format!("terminal-report-cancel-{}", Uuid::new_v4());
        let request = admin_request(HealType::Object {
            bucket: bucket.clone(),
            object: "object".to_string(),
            version_id: None,
        });
        let task_id = request.id.clone();
        let hook = Arc::new(CompletedRetentionHook {
            pause_before_publish: true,
            ..Default::default()
        });
        {
            let mut hooks = COMPLETED_RETENTION_HOOKS.lock().await;
            hooks.insert(bucket.clone(), hook.clone());
            hooks.insert(task_id.clone(), hook.clone());
        }
        manager.submit_heal_request(request).await.expect("admit cancellable object");
        process_manager_queue_once(&manager).await;
        tokio::time::timeout(Duration::from_secs(10), hook.started.notified())
            .await
            .expect("worker entered storage");
        manager.cancel_task(&task_id).await.expect("durable cancellation");
        let initial = manager.get_task_report(&task_id).await.expect("initial cancelled report");
        assert_eq!(
            initial.outcome.as_ref().expect("initial outcome").execution,
            HealExecutionOutcome::Aborted(HealAbortReason::Cancelled)
        );
        let completed_at = manager
            .root_recovery
            .completed(&task_id)
            .await
            .expect("receipt")
            .expect("retained")
            .completed_at;
        hook.execute.notify_one();
        tokio::time::timeout(Duration::from_secs(10), hook.before_publish.notified())
            .await
            .expect("worker finalized outcome");
        let read_only =
            matches!(failure_mode, 1 | 3).then(|| RestoreDirectoryMode::read_only(temp.path().join(RUSTFS_META_BUCKET)));
        if failure_mode == 3 {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(temp.path().join(RUSTFS_META_BUCKET), std::fs::Permissions::from_mode(0o0))
                .expect("make the report owner unreadable as well as unwritable");
        }
        manager
            .root_recovery
            .fail_after_terminal_write
            .store(failure_mode == 2, std::sync::atomic::Ordering::SeqCst);
        hook.publish.notify_one();
        tokio::time::timeout(Duration::from_secs(10), hook.handoff.notified())
            .await
            .expect("worker published completion");
        let final_report = manager.get_task_report(&task_id).await.expect("final cancelled report");
        let expected = if failure_mode == 3 {
            assert!(
                final_report.outcome.is_none(),
                "an uncertain unreadable report must be marked unavailable"
            );
            assert!(final_report.result_items_truncated);
            assert!(final_report.result_items.is_empty());
            serde_json::to_value(initial.outcome.as_deref()).expect("last durable report after I/O recovers")
        } else {
            serde_json::to_value(final_report.outcome.as_deref()).expect("final outcome")
        };
        if failure_mode == 1 {
            assert_eq!(
                expected,
                serde_json::to_value(initial.outcome.as_deref()).expect("previous durable outcome")
            );
        } else if failure_mode != 3 {
            assert!(
                final_report.outcome.as_ref().expect("final counters").counters.processed
                    > initial.outcome.as_ref().expect("initial counters").counters.processed
            );
        }
        drop(read_only);
        for _ in 0..2 {
            let restarted = recovery_manager(vec![disk.clone()]);
            restarted.replay_root_heals().await.expect("cancelled restart");
            assert_eq!(restarted.get_queue_length().await, 0);
            let restored = restarted.get_task_report(&task_id).await.expect("restored cancellation");
            assert_eq!(serde_json::to_value(restored.outcome.as_deref()).expect("restored outcome"), expected);
            assert_eq!(
                restarted
                    .root_recovery
                    .completed(&task_id)
                    .await
                    .expect("receipt")
                    .expect("retained")
                    .completed_at,
                completed_at
            );
        }
        hook.finish.notify_one();
        COMPLETED_RETENTION_HOOKS
            .lock()
            .await
            .retain(|key, _| key != &bucket && key != &task_id);
    }
}

#[tokio::test]
async fn root_recovery_retry_cancellation_preserves_previous_attempt_outcome() {
    use crate::heal::outcome::{HealAbortReason, HealExecutionOutcome};
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let request = root_request();
    manager.root_recovery.persist(&request).await.expect("durable retry intent");
    let previous = terminal_with_outcome(
        &request.heal_type,
        HealTaskStatus::Failed {
            error: "retryable".to_string(),
        },
    );
    let counters = previous.outcome.as_ref().expect("attempt outcome").counters.clone();
    manager
        .completed_heals
        .lock()
        .await
        .insert(request.id.clone(), Arc::new(previous));
    manager.retrying_heals.lock().await.insert(
        request.id.clone(),
        RetryingHeal {
            request: request.clone(),
            error: "retryable".to_string(),
            cancel_token: CancellationToken::new(),
        },
    );
    manager.cancel_task(&request.id).await.expect("cancel retry");
    drop(manager);
    let restarted = recovery_manager(vec![disk]);
    let report = restarted
        .get_task_report(&request.id)
        .await
        .expect("retained retry cancellation");
    let outcome = report.outcome.expect("previous attempt retained");
    assert_eq!(outcome.execution, HealExecutionOutcome::Aborted(HealAbortReason::Cancelled));
    assert_eq!(outcome.counters, counters);
    assert_eq!(report.result_items.len(), 1);
}

#[tokio::test]
async fn root_recovery_corrupt_or_oversize_reports_do_not_resurrect_terminal_work() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let request = root_request();
    let completed = terminal_with_outcome(&request.heal_type, HealTaskStatus::Completed);
    manager.root_recovery.persist(&request).await.expect("admission");
    manager
        .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
        .await
        .expect("terminal");
    let path = format!("heal-terminal-report-{}.json", request.id);
    let original = disk.read_all(RUSTFS_META_BUCKET, &path).await.expect("valid report");
    let original_json: serde_json::Value = serde_json::from_slice(&original).expect("report JSON");
    for (pointer, value) in [
        ("/schema", serde_json::json!(2)),
        ("/terminal/task_id", serde_json::json!(Uuid::new_v4().to_string())),
        ("/terminal/completed_at/secs_since_epoch", serde_json::json!(1)),
        ("/outcome/execution", serde_json::json!({"state":"running"})),
        ("/outcome/counters/processed", serde_json::json!(0)),
        ("/outcome/objects/0/detail", serde_json::json!("x".repeat(1025))),
        ("/min_seq", serde_json::json!(11)),
    ] {
        let mut corrupted = original_json.clone();
        *corrupted.pointer_mut(pointer).expect("existing field") = value;
        disk.write_all(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&corrupted).expect("corrupt JSON").into())
            .await
            .expect("inject corruption");
        assert!(manager.get_task_report(&request.id).await.is_err(), "must reject {pointer}");
        assert!(
            manager
                .root_recovery
                .pending()
                .await
                .expect("terminal still fences replay")
                .is_empty()
        );
    }
    disk.write_all(RUSTFS_META_BUCKET, &path, b"{".to_vec().into())
        .await
        .expect("inject malformed JSON");
    assert!(manager.get_task_report(&request.id).await.is_err());
    let mut boundary = original.to_vec();
    boundary.resize(8 * 1024 * 1024, b' ');
    disk.write_all(RUSTFS_META_BUCKET, &path, boundary.clone().into())
        .await
        .expect("exact-size valid JSON");
    assert!(
        manager
            .get_task_report(&request.id)
            .await
            .expect("exact byte limit is accepted")
            .outcome
            .is_some()
    );
    boundary.push(b' ');
    disk.write_all(RUSTFS_META_BUCKET, &path, boundary.into())
        .await
        .expect("limit plus one");
    assert!(
        manager
            .get_task_report(&request.id)
            .await
            .expect_err("oversized otherwise-valid JSON")
            .to_string()
            .contains("size limit")
    );
    disk.write_all(RUSTFS_META_BUCKET, &path, original)
        .await
        .expect("restore valid report");
    assert!(
        manager
            .get_task_report(&request.id)
            .await
            .expect("valid report restored")
            .outcome
            .is_some()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn root_recovery_report_write_failure_preserves_pending_owner() {
    let (temp, disk) = recovery_disk().await;
    let (_other_temp, other) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone(), other.clone()]);
    let request = root_request();
    manager.root_recovery.persist(&request).await.expect("admission");
    let intent_path = format!("root-heal-{}.json", request.id);
    let original = disk
        .read_all(RUSTFS_META_BUCKET, &intent_path)
        .await
        .expect("original responsibility");
    let completed = terminal_with_outcome(&request.heal_type, HealTaskStatus::Completed);
    let read_only = RestoreDirectoryMode::read_only(temp.path().join(RUSTFS_META_BUCKET));
    assert!(
        manager
            .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
            .await
            .is_err()
    );
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, &intent_path)
            .await
            .expect("responsibility retained"),
        original
    );
    for disk in [&disk, &other] {
        assert!(matches!(
            disk.read_all(RUSTFS_META_BUCKET, &format!("terminal-root-heal-{}.json", request.id))
                .await,
            Err(DiskError::FileNotFound)
        ));
        assert!(matches!(
            disk.read_all(RUSTFS_META_BUCKET, &format!("heal-terminal-report-{}.json", request.id))
                .await,
            Err(DiskError::FileNotFound)
        ));
    }
    drop(read_only);
    manager
        .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
        .await
        .expect("retry on original owner");
}

#[tokio::test]
async fn root_recovery_old_cancelled_token_cannot_stop_successor() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let old = root_request();
    let cancelled = terminal_with_outcome(&old.heal_type, HealTaskStatus::Cancelled);
    manager
        .publish_admin_terminal(&old.id, &old.heal_type, old.source, &cancelled)
        .await
        .expect("old cancellation");
    let mut successor = root_request();
    successor.force_start = true;
    manager.submit_heal_request(successor.clone()).await.expect("admit successor");
    let path = format!("root-heal-{}.json", successor.id);
    let original = disk.read_all(RUSTFS_META_BUCKET, &path).await.expect("successor owner");
    manager.cancel_task(&old.id).await.expect("repeat old STOP");
    assert_eq!(
        manager.get_task_status(&successor.id).await.expect("successor still pending"),
        HealTaskStatus::Pending
    );
    assert_eq!(disk.read_all(RUSTFS_META_BUCKET, &path).await.expect("same successor intent"), original);
    drop(manager);
    let restarted = recovery_manager(vec![disk]);
    restarted.replay_root_heals().await.expect("successor restart");
    assert_eq!(
        restarted
            .get_task_status(&successor.id)
            .await
            .expect("successor survives restart"),
        HealTaskStatus::Pending
    );
    assert_eq!(
        serde_json::to_value(
            restarted
                .get_task_report(&old.id)
                .await
                .expect("old report")
                .outcome
                .as_deref()
        )
        .expect("restored"),
        serde_json::to_value(cancelled.outcome.as_deref()).expect("original")
    );
}

#[tokio::test]
async fn root_recovery_uncertain_terminal_publication_preserves_commit_and_pending_fence() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let request = root_request();
    manager
        .root_recovery
        .persist(&request)
        .await
        .expect("original responsibility");
    let completed = terminal_with_outcome(&request.heal_type, HealTaskStatus::Cancelled);
    manager
        .root_recovery
        .fail_after_terminal_write
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(
        manager
            .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
            .await
            .is_err()
    );
    assert!(
        disk.read_all(RUSTFS_META_BUCKET, &format!("root-heal-{}.json", request.id))
            .await
            .is_ok(),
        "uncertain publication must not retire pending ownership"
    );
    let restarted = recovery_manager(vec![disk]);
    restarted
        .replay_root_heals()
        .await
        .expect("commit marker fences stale pending");
    assert_eq!(restarted.get_queue_length().await, 0);
    let report = restarted
        .get_task_report(&request.id)
        .await
        .expect("committed report survives uncertain response");
    assert_eq!(
        serde_json::to_value(report.outcome.as_deref()).expect("restored"),
        serde_json::to_value(completed.outcome.as_deref()).expect("committed")
    );
}

#[tokio::test]
async fn root_recovery_orphan_report_never_commits_or_retires_pending_work() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let request = root_request();
    let mut completed = terminal_with_outcome(&request.heal_type, HealTaskStatus::Completed);
    completed.completed_at = SystemTime::now() - KEEP_HEAL_TASK_STATUS_DURATION - Duration::from_secs(1);
    manager
        .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
        .await
        .expect("report fixture");
    let path = format!("terminal-root-heal-{}.json", request.id);
    let marker = disk.read_all(RUSTFS_META_BUCKET, &path).await.expect("marker");
    assert_eq!(
        crate::heal::storage_api::owner::EcstoreDiskAPI::compare_and_update_file(
            disk.as_ref(),
            RUSTFS_META_BUCKET,
            &path,
            Some(marker),
            None
        )
        .await
        .expect("simulate missing commit marker"),
        crate::heal::storage_api::owner::EcstoreConditionalFileUpdate::Updated
    );
    manager
        .root_recovery
        .persist(&request)
        .await
        .expect("original pending responsibility");
    assert_eq!(
        manager
            .root_recovery
            .pending()
            .await
            .expect("uncommitted report cannot mask pending")
            .len(),
        1
    );
    assert!(matches!(manager.get_task_report(&request.id).await, Err(Error::TaskNotFound { .. })));
    let gc = manager
        .root_recovery
        .gc_terminal_receipts_once(SystemTime::now())
        .await
        .expect("orphan GC");
    assert_eq!(gc.reports_removed, 1);
    assert_eq!((gc.pending_removed, gc.terminals_removed), (0, 0));
    assert_eq!(manager.root_recovery.pending().await.expect("pending survives GC").len(), 1);
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
    assert_eq!(report.terminals_removed, 32);
    assert_eq!(report.reports_removed, 32, "report deletion shares the 64-operation budget");
    assert!(report.budget_exhausted);
    let terminal_entries = disk
        .list_dir("", RUSTFS_META_BUCKET, "", -1)
        .await
        .expect("list remaining terminal receipts")
        .into_iter()
        .filter(|entry| entry.starts_with("terminal-root-heal-"))
        .count();
    assert_eq!(terminal_entries, 33);
}

#[tokio::test]
async fn root_recovery_corrupt_terminal_receipt_is_quarantined_without_blocking_admission() {
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
    assert!(
        manager
            .root_recovery
            .pending()
            .await
            .expect("quarantine corrupt terminal")
            .is_empty()
    );
    assert!(
        disk.read_all(RUSTFS_META_BUCKET, &format!("quarantined-root-heal-terminal-{}.json", request.id))
            .await
            .is_ok()
    );
    let mut replacement = admin_request(request.heal_type.clone());
    replacement.force_start = true;
    assert_eq!(
        manager
            .submit_heal_request(replacement)
            .await
            .expect("admit independent replacement"),
        HealAdmissionResult::Accepted
    );
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
async fn root_recovery_admin_overlap_quarantines_corrupt_owner_before_replacement() {
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
    let replacement_id = replacement.id.clone();
    assert_eq!(
        manager
            .submit_heal_request(replacement)
            .await
            .expect("admit replacement after quarantine"),
        HealAdmissionResult::Accepted
    );
    assert_eq!(
        manager
            .heal_queue
            .lock()
            .await
            .requests()
            .map(|request| request.id.clone())
            .collect::<Vec<_>>(),
        vec![replacement_id]
    );
    assert_eq!(
        manager
            .get_task_status(&owner.id)
            .await
            .expect("original owner remains queryable"),
        HealTaskStatus::Cancelled
    );
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, &path)
            .await
            .expect("retain corrupt record")
            .as_ref(),
        b"{"
    );
    assert!(
        disk.read_all(RUSTFS_META_BUCKET, &format!("quarantined-root-heal-intent-{}.json", corrupt.id))
            .await
            .is_ok()
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
async fn root_recovery_invalid_records_are_quarantined_without_blocking_replay() {
    for kind in ["truncated", "schema", "identity", "option", "no_lock", "oversized"] {
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
            "schema" => value["schema"] = 4.into(),
            "identity" => value["task_id"] = valid.id.clone().into(),
            "option" => value["options"]["future_delete_mode"] = true.into(),
            "no_lock" => value["options"]["no_lock"] = true.into(),
            _ => {}
        }
        let bytes = match kind {
            "truncated" => b"{".to_vec(),
            "oversized" => vec![b' '; MAX_ROOT_RECOVERY_BYTES + 1],
            _ => serde_json::to_vec(&value).expect("modified record"),
        };
        disk.write_all(RUSTFS_META_BUCKET, &path, bytes.clone().into())
            .await
            .expect("inject bad record");
        manager.replay_root_heals().await.expect("quarantine invalid record");
        assert_eq!(manager.get_queue_length().await, 1, "valid record replays for {kind}");
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, &path)
                .await
                .expect("bad record retained")
                .as_ref(),
            bytes
        );
        let marker_path = format!("quarantined-root-heal-intent-{}.json", invalid.id);
        let marker = disk
            .read_all(RUSTFS_META_BUCKET, &marker_path)
            .await
            .expect("durable quarantine marker");
        let marker: serde_json::Value = serde_json::from_slice(&marker).expect("quarantine marker JSON");
        assert_eq!(marker["task_id"], invalid.id, "kind={kind}");
        assert_eq!(marker["record_kind"], "intent", "kind={kind}");
        assert_eq!(marker["source_prefix_len"], bytes.len().min(MAX_ROOT_RECOVERY_BYTES + 1), "kind={kind}");
        assert_eq!(marker["source_oversized"], kind == "oversized", "kind={kind}");

        let mut reused = invalid.clone();
        reused.force_start = true;
        assert!(
            manager.submit_heal_request(reused).await.is_err(),
            "quarantined task ID remains reserved for {kind}"
        );
        let mut replacement = root_request();
        replacement.force_start = true;
        assert_eq!(
            manager
                .submit_heal_request(replacement)
                .await
                .expect("admit independent compensation"),
            HealAdmissionResult::Accepted,
            "kind={kind}"
        );
    }
}

#[tokio::test]
async fn root_recovery_quarantine_source_change_fails_closed() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let request = root_request();
    let path = format!("root-heal-{}.json", request.id);
    disk.write_all(RUSTFS_META_BUCKET, &path, b"{".to_vec().into())
        .await
        .expect("corrupt source");
    manager.root_recovery.pending().await.expect("quarantine source");
    disk.write_all(RUSTFS_META_BUCKET, &path, b"changed".to_vec().into())
        .await
        .expect("change retained source");
    assert!(manager.root_recovery.pending().await.is_err(), "marker must fence a changed source");
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

#[tokio::test]
async fn bucket_incarnation_recovery_retires_old_owner_and_admits_successor() {
    let (_temp, disk) = recovery_disk().await;
    let bucket = format!("incarnation-replay-{}", Uuid::new_v4());
    let old = Uuid::new_v4();
    let new = Uuid::new_v4();
    bucket_incarnations()
        .lock()
        .expect("fixture")
        .insert(bucket.clone(), Some(old));
    let manager = recovery_manager(vec![disk.clone()]);
    let mut request = HealRequest::bucket(bucket.clone());
    request.source = HealRequestSource::Admin;
    request.options.recursive = true;
    let token = request.id.clone();
    manager
        .submit_heal_request_with_receipt(request)
        .await
        .expect("admit original bucket");
    let pending = manager.root_recovery.pending().await.expect("read admission");
    assert_eq!(pending[0].bucket_incarnation_id, Some(old));
    let original = disk
        .read_all(RUSTFS_META_BUCKET, &format!("root-heal-{token}.json"))
        .await
        .expect("raw admission");
    drop(manager);

    let same = recovery_manager(vec![disk.clone()]);
    same.replay_root_heals().await.expect("same generation restart");
    let restored = same
        .heal_queue
        .lock()
        .await
        .requests()
        .next()
        .expect("restored owner")
        .clone();
    assert_eq!(restored.id, token);
    assert_eq!(restored.bucket_incarnation_id, Some(old));
    assert!(restored.options.recursive);
    let retry = HealTask::from_request(restored, Arc::new(MockStorage)).retry_request();
    assert_eq!(retry.bucket_incarnation_id, Some(old));
    drop(same);

    bucket_incarnations()
        .lock()
        .expect("fixture")
        .insert(bucket.clone(), Some(new));
    let restarted = recovery_manager(vec![disk.clone()]);
    restarted.replay_root_heals().await.expect("retire obsolete admission");
    assert_eq!(restarted.heal_queue.lock().await.len(), 0);
    assert!(matches!(restarted.get_task_status(&token).await.expect("old token remains queryable"),
        HealTaskStatus::Failed { error } if error.starts_with("stale_bucket_incarnation:")));
    let mut fresh = HealRequest::bucket(bucket.clone());
    fresh.source = HealRequestSource::Admin;
    let receipt = restarted
        .submit_heal_request_with_receipt(fresh)
        .await
        .expect("new generation admission");
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    assert_ne!(receipt.task_id, token);
    assert_eq!(
        restarted
            .heal_queue
            .lock()
            .await
            .requests()
            .next()
            .expect("new owner")
            .bucket_incarnation_id,
        Some(new)
    );

    // A terminal receipt must dominate a duplicate old journal after another crash.
    disk.write_all(RUSTFS_META_BUCKET, &format!("root-heal-{token}.json"), original)
        .await
        .expect("restore old bytes");
    let again = recovery_manager(vec![disk]);
    again.replay_root_heals().await.expect("restart with duplicate old bytes");
    assert!(again.heal_queue.lock().await.requests().all(|request| request.id != token));
    assert!(matches!(
        again.get_task_status(&token).await.expect("terminal persists"),
        HealTaskStatus::Failed { .. }
    ));
    bucket_incarnations().lock().expect("fixture").remove(&bucket);
}

#[tokio::test]
async fn bucket_incarnation_legacy_admissions_are_queryable_without_rebinding() {
    for identity in [None, Some(Uuid::nil())] {
        let (_temp, disk) = recovery_disk().await;
        let manager = recovery_manager(vec![disk.clone()]);
        let request = admin_request(HealType::Bucket {
            bucket: format!("legacy-{}", Uuid::new_v4()),
        });
        manager
            .root_recovery
            .persist(&request)
            .await
            .expect("capture server admission");
        let path = format!("root-heal-{}.json", request.id);
        let bytes = disk.read_all(RUSTFS_META_BUCKET, &path).await.expect("read JSON");
        let mut legacy: serde_json::Value = serde_json::from_slice(&bytes).expect("decode JSON");
        if let Some(id) = identity {
            legacy["bucket_incarnation_id"] = serde_json::json!(id);
        } else {
            legacy["schema"] = 2.into();
            legacy.as_object_mut().expect("root record").remove("bucket_incarnation_id");
        }
        disk.write_all(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&legacy).expect("legacy bytes").into())
            .await
            .expect("legacy journal");
        let restarted = recovery_manager(vec![disk]);
        restarted.replay_root_heals().await.expect("retire unsafe legacy task");
        assert_eq!(restarted.heal_queue.lock().await.len(), 0);
        assert!(matches!(restarted.get_task_status(&request.id).await.expect("legacy token"),
            HealTaskStatus::Failed { error } if error.starts_with("stale_bucket_incarnation:")));
    }
}

#[tokio::test]
async fn bucket_incarnation_queued_and_retrying_work_does_not_rebind() {
    for successor in [None, Some(Uuid::new_v4())] {
        let bucket = format!("queued-incarnation-{}", Uuid::new_v4());
        let old = Uuid::new_v4();
        bucket_incarnations()
            .lock()
            .expect("fixture")
            .insert(bucket.clone(), Some(old));
        let request = admin_request(HealType::Bucket { bucket: bucket.clone() });
        let retry = HealTask::from_request(request.clone(), Arc::new(MockStorage)).retry_request();
        bucket_incarnations()
            .lock()
            .expect("fixture")
            .insert(bucket.clone(), successor);
        for pending in [request, retry] {
            let task = HealTask::from_request(pending, Arc::new(MockStorage));
            let error = task.execute().await.expect_err("obsolete generation cannot execute");
            assert!(matches!(error, Error::StaleBucketIncarnation { expected: Some(id), .. } if id == old));
            assert!(!error.is_recoverable_heal(), "never retry against a successor");
            assert_eq!(task.get_outcome().await.counters.processed, 0);
        }
        bucket_incarnations().lock().expect("fixture").remove(&bucket);
    }
}

#[tokio::test]
async fn bucket_incarnation_metadata_failure_defers_replay_without_retiring_owner() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk]);
    let request = admin_request(HealType::Bucket {
        bucket: format!("incarnation-metadata-unavailable-{}", Uuid::new_v4()),
    });
    manager
        .root_recovery
        .persist(&request)
        .await
        .expect("accepted responsibility");
    assert!(matches!(manager.replay_root_heals().await, Err(Error::Storage(EcstoreError::SlowDown))));
    assert_eq!(manager.root_recovery.pending().await.expect("pending owner").len(), 1);
    assert!(
        manager
            .root_recovery
            .completed(&request.id)
            .await
            .expect("terminal lookup")
            .is_none()
    );
    assert!(manager.heal_queue.lock().await.requests().next().is_none());
}

#[tokio::test]
async fn root_recovery_all_buckets_erasure_terminal_round_trip() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let request = admin_request(HealType::ErasureSet {
        buckets: Vec::new(),
        set_disk_id: "pool_0_set_0".to_string(),
    });
    manager
        .submit_heal_request_with_receipt(request.clone())
        .await
        .expect("admit all-buckets erasure heal");
    let pending = manager.root_recovery.pending().await.expect("decode admitted scope");
    assert_eq!(pending[0].heal_type, request.heal_type);
    let completed = completed_admin_status(&request.heal_type, SystemTime::now());
    manager
        .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
        .await
        .expect("publish all-buckets terminal");
    let restarted = recovery_manager(vec![disk]);
    restarted
        .replay_root_heals()
        .await
        .expect("restart after all-buckets completion");
    assert_eq!(restarted.get_queue_length().await, 0);
    let report = restarted.get_task_report(&request.id).await.expect("retained same token");
    assert_eq!(report.status, HealTaskStatus::Completed);
}

#[tokio::test]
async fn root_recovery_legacy_empty_erasure_scope_does_not_block_other_owners() {
    let (_temp, disk) = recovery_disk().await;
    let manager = recovery_manager(vec![disk.clone()]);
    let ordinary = root_request();
    manager
        .root_recovery
        .persist(&ordinary)
        .await
        .expect("ordinary pending owner");
    let request = admin_request(HealType::ErasureSet {
        buckets: Vec::new(),
        set_disk_id: "pool_0_set_0".to_string(),
    });
    // Pinned pre-marker shape: production admitted this record before #2539.
    let bytes = serde_json::to_vec(&serde_json::json!({
        "schema": 2, "task_id": request.id,
        "heal_type": { "type": "erasure_set", "buckets": [], "set_disk_id": "pool_0_set_0" },
        "options": request.options, "priority": request.priority, "retry_attempts": 0, "created_at": request.created_at
    }))
    .expect("legacy scoped fixture");
    disk.write_all(RUSTFS_META_BUCKET, &format!("root-heal-{}.json", request.id), bytes.into())
        .await
        .expect("legacy owner");
    let restarted = recovery_manager(vec![disk]);
    restarted
        .replay_root_heals()
        .await
        .expect("restore both accepted responsibilities");
    let ids = restarted
        .heal_queue
        .lock()
        .await
        .requests()
        .map(|request| request.id.clone())
        .collect::<HashSet<_>>();
    assert_eq!(ids, HashSet::from([ordinary.id, request.id.clone()]));
    restarted
        .root_recovery
        .persist(&request)
        .await
        .expect("retry updates legacy scope");
    let completed = completed_admin_status(&request.heal_type, SystemTime::now());
    restarted
        .publish_admin_terminal(&request.id, &request.heal_type, request.source, &completed)
        .await
        .expect("retire legacy scope");
}

#[tokio::test]
async fn root_recovery_erasure_bucket_scope_marker_quarantines_conflicts() {
    for (buckets, marker, valid) in [
        (serde_json::json!([]), serde_json::json!(true), true),
        (serde_json::json!([]), serde_json::json!(false), false),
        (serde_json::json!(["bucket"]), serde_json::json!(false), true),
        (serde_json::json!(["bucket"]), serde_json::json!(true), false),
        (serde_json::json!([]), serde_json::json!("all"), false),
    ] {
        let (_temp, disk) = recovery_disk().await;
        let request = root_request();
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema": 3, "task_id": request.id,
            "heal_type": { "type": "erasure_set", "buckets": buckets, "set_disk_id": "pool_0_set_0", "all_buckets": marker },
            "options": request.options, "priority": request.priority, "retry_attempts": 0, "created_at": request.created_at
        }))
        .expect("scope marker fixture");
        let path = format!("root-heal-{}.json", request.id);
        disk.write_all(RUSTFS_META_BUCKET, &path, bytes.clone().into())
            .await
            .expect("scope marker record");
        let manager = recovery_manager(vec![disk.clone()]);
        let pending = manager.root_recovery.pending().await.expect("inventory scope marker");
        assert_eq!(pending.len(), usize::from(valid), "{marker:?}, {buckets:?}");
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, &format!("quarantined-root-heal-intent-{}.json", request.id))
                .await
                .is_ok(),
            !valid,
            "{marker:?}, {buckets:?}"
        );
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, &path)
                .await
                .expect("retained record")
                .as_ref(),
            bytes.as_slice()
        );
    }
}
