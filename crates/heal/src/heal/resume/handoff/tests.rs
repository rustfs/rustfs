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
use std::sync::LazyLock;

static FAILURES: LazyLock<Mutex<HashMap<String, &'static str>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[tokio::test]
async fn handoff_rebinding_never_resets_an_exhausted_successor_budget() {
    let (_dirs, parent, execution) = fixture(1).await;
    let child = parent
        .prepare_replacement_handoff(&execution, Vec::new())
        .await
        .expect("prepare");
    child
        .record_replacement_failure(&Error::ReplacementRetryBudgetExhausted, 0)
        .await
        .expect("exhausted child");
    let rebound =
        ReplacementExecution::for_test(execution.test_disks().to_vec(), vec![identity("replacement-0", "another-mount:fs:root")]);
    let child = parent
        .prepare_replacement_handoff(&rebound, Vec::new())
        .await
        .expect("rebind pending ownership");
    let state = child.get_state().await;
    assert_eq!(state.retry_count, state.max_retries);
    assert!(matches!(
        ResumeManager::new_replacement_intent(
            child.disk.clone(),
            state.task_id,
            state.set_disk_id,
            state.replacement_buckets,
            state.replacement_targets,
            state.replacement_target_identities,
        )
        .await,
        Err(Error::ReplacementRetryBudgetExhausted)
    ));
}
#[tokio::test]
async fn handoff_to_a_genuinely_new_disk_acquires_an_absent_marker() {
    let (_dirs, parent, old_execution) = fixture(1).await;
    let (_blank_dir, blank) = schema_test_disk().await;
    let mut target_identity = identity("replacement-0", "new-mount:new-fs:new-root");
    target_identity.physical_device_ids = vec!["new-device".to_string()];
    let replacement = ReplacementExecution::for_test(vec![blank], vec![target_identity.clone()]);
    assert_eq!(replacement.markers().await.expect("blank target"), [None]);
    let child = parent
        .prepare_replacement_handoff(&replacement, Vec::new())
        .await
        .expect("new disk successor");
    child
        .acquire_replacement_markers(&replacement)
        .await
        .expect("fresh marker acquisition");
    assert_eq!(child.get_state().await.replacement_target_identities, [target_identity]);
    let parent_id = parent.get_state().await.task_id;
    assert!(
        old_execution.markers().await.expect("old device evidence")[0]
            .as_ref()
            .is_some_and(|marker| marker.ends_with(&parent_id))
    );
}

#[tokio::test]
async fn handoff_never_publishes_a_successor_from_an_unpersisted_decision() {
    let (_dirs, parent, execution) = fixture(1).await;
    let state = parent.get_state().await;
    let successor = Uuid::new_v4().to_string();
    let raw = ResumeManager::read_state_file(&parent.disk, &state.task_id, parent.state_file)
        .await
        .expect("source bytes");
    let expected_markers = execution.markers().await.expect("source markers");
    {
        let mut current = parent.state.write().await;
        current.replacement_phase = ReplacementPhase::HandoffPending;
        current.replacement_handoff = Some(ReplacementHandoff {
            phase: ReplacementHandoffPhase::Prepared,
            link: ReplacementHandoffLink {
                transaction_id: Uuid::new_v4().to_string(),
                predecessor: state.task_id.clone(),
                successor: successor.clone(),
                set_disk_id: state.set_disk_id,
                source_sha256: HashAlgorithm::SHA256.hash_encode(&raw).as_ref().to_vec(),
                targets: execution.identities().to_vec(),
                expected_markers,
                buckets: state.replacement_buckets,
            },
        });
    }
    assert!(parent.prepare_replacement_handoff(&execution, Vec::new()).await.is_err());
    assert!(!ResumeManager::has_replacement_intent(&parent.disk, &successor).await);
    assert_eq!(
        ResumeManager::read_state_file(&parent.disk, &state.task_id, parent.state_file)
            .await
            .expect("source bytes retained"),
        raw
    );
}

pub(super) fn fail_at(task_id: &str, boundary: &str) -> Result<()> {
    let mut failures = FAILURES.lock().expect("handoff failure registry");
    if failures.get(task_id).is_some_and(|stage| *stage == boundary) {
        failures.remove(task_id);
        return Err(Error::Disk(DiskError::other(format!("injected handoff crash at {boundary}"))));
    }
    Ok(())
}

fn identity(endpoint: &str, incarnation: &str) -> ReplacementTargetIdentity {
    ReplacementTargetIdentity {
        endpoint: endpoint.to_string(),
        canonical_path: format!("/mnt/{endpoint}"),
        physical_device_ids: vec![format!("device-{endpoint}")],
        filesystem_identity: incarnation.to_string(),
    }
}

async fn fixture(target_count: usize) -> (Vec<tempfile::TempDir>, ResumeManager, Arc<ReplacementExecution>) {
    let (anchor_dir, anchor) = schema_test_disk().await;
    let mut dirs = vec![anchor_dir];
    let mut disks = Vec::new();
    let mut old = Vec::new();
    let mut new = Vec::new();
    let id = Uuid::new_v4().to_string();
    for index in 0..target_count {
        let (dir, disk) = schema_test_disk().await;
        dirs.push(dir);
        let endpoint = format!("replacement-{index}");
        old.push(identity(&endpoint, "old-mount:fs-1:root-1"));
        new.push(identity(&endpoint, "new-mount:fs-1:root-1"));
        disk.write_all(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, format!("pool_0_set_0:{id}").into())
            .await
            .expect("old marker");
        disks.push(disk);
    }
    let manager = ResumeManager::new_replacement_intent(
        anchor,
        id,
        "pool_0_set_0".to_string(),
        vec!["old-bucket".to_string()],
        old.iter().map(|identity| identity.endpoint.clone()).collect(),
        old,
    )
    .await
    .expect("source intent");
    let execution = ReplacementExecution::for_test(disks, new);
    (dirs, manager, execution)
}

#[tokio::test]
async fn handoff_replays_every_durable_boundary_with_one_fresh_successor() {
    for boundary in [
        "prepared",
        "successor_published",
        "markers_transferred",
        "markers_owned",
        "committed",
    ] {
        let (_dirs, parent, execution) = fixture(2).await;
        let source_id = parent.get_state().await.task_id;
        {
            let mut state = parent.state.write().await;
            state.processed_objects = 99;
            state.resume_cursor = Some("unsafe-old-cursor".to_string());
            state.complete_bucket("old-bucket");
        }
        parent.save_state_strict().await.expect("old progress");
        FAILURES.lock().expect("failure registry").insert(source_id.clone(), boundary);
        let prepared = parent
            .prepare_replacement_handoff(&execution, vec!["new-bucket".to_string()])
            .await;
        if let Ok(child) = prepared {
            assert!(child.acquire_replacement_markers(&execution).await.is_err(), "{boundary}");
        }
        assert!(
            !FAILURES.lock().expect("failure registry").contains_key(&source_id),
            "boundary was reached"
        );
        let parent = ResumeManager::load_replacement_intent(parent.disk.clone(), &source_id)
            .await
            .expect("restart parent");
        let reserved = parent
            .get_state()
            .await
            .replacement_handoff
            .expect("durable authority")
            .link
            .successor;
        let child = parent
            .prepare_replacement_handoff(&execution, Vec::new())
            .await
            .expect("replay handoff");
        let child_state = child.get_state().await;
        assert_eq!(child_state.task_id, reserved, "{boundary}");
        assert_ne!(child_state.task_id, source_id);
        assert_eq!(child_state.replacement_phase, ReplacementPhase::OwnershipPending);
        assert_eq!(child_state.processed_objects, 0);
        assert!(child_state.resume_cursor.is_none());
        assert!(child_state.completed_buckets.is_empty());
        assert_eq!(child_state.pending_buckets, ["new-bucket", "old-bucket"]);
        assert_eq!(child_state.replacement_lineage.len(), 1);
        child
            .acquire_replacement_markers(&execution)
            .await
            .expect("replay marker transfer");
        child
            .mark_replacement_rebuilding(execution.identities().to_vec())
            .await
            .expect("start successor");
        let committed = ResumeManager::load_replacement_intent(parent.disk.clone(), &source_id)
            .await
            .expect("committed parent")
            .get_state()
            .await;
        assert_eq!(committed.replacement_phase, ReplacementPhase::Abandoned);
        assert_eq!(
            committed.replacement_handoff.expect("edge retained").phase,
            ReplacementHandoffPhase::Committed
        );
        assert_eq!(
            execution.markers().await.expect("markers"),
            vec![Some(format!("pool_0_set_0:{reserved}")); 2]
        );
        child
            .mark_replacement_completed_and_verified()
            .await
            .expect("fresh scan verified");
        let proof = child
            .ensure_replacement_completion_proof()
            .await
            .expect("proof includes lineage");
        assert_eq!(proof.replacement_lineage, child_state.replacement_lineage);
        assert_eq!(proof.schema_version, 2);
    }
}

#[tokio::test]
async fn handoff_preserves_partial_transfer_and_rejects_unknown_owner() {
    let (_dirs, parent, execution) = fixture(2).await;
    let source = parent.get_state().await;
    let child = parent
        .prepare_replacement_handoff(&execution, Vec::new())
        .await
        .expect("prepare");
    let rogue = format!("pool_0_set_0:{}", Uuid::new_v4());
    // Simulate a conflicting writer after preparation on the second target.
    // The first target is already durable when this mismatch is discovered.
    let second = &execution.test_disks()[1];
    second
        .write_all(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, rogue.clone().into())
        .await
        .expect("conflict");
    assert!(child.acquire_replacement_markers(&execution).await.is_err());
    let successor = child.get_state().await.task_id;
    assert_eq!(
        execution.markers().await.expect("partial markers"),
        [Some(format!("pool_0_set_0:{successor}")), Some(rogue.clone())]
    );
    assert_eq!(parent.get_state().await.replacement_phase, ReplacementPhase::HandoffPending);
    assert_eq!(child.get_state().await.replacement_phase, ReplacementPhase::OwnershipPending);
    second
        .write_all(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, format!("pool_0_set_0:{}", source.task_id).into())
        .await
        .expect("restore approved owner");
    let resumed = ResumeManager::load_replacement_intent(parent.disk.clone(), &source.task_id)
        .await
        .expect("restart parent");
    let child = resumed
        .prepare_replacement_handoff(&execution, Vec::new())
        .await
        .expect("same child");
    child
        .acquire_replacement_markers(&execution)
        .await
        .expect("finish partial transfer");
    assert_eq!(child.get_state().await.task_id, successor);
}

#[tokio::test]
async fn handoff_unknown_owner_and_exhausted_budget_leave_source_bytes_unchanged() {
    let (_dirs, parent, execution) = fixture(1).await;
    let state = parent.get_state().await;
    let before = ResumeManager::read_state_file(&parent.disk, &state.task_id, parent.state_file)
        .await
        .expect("source");
    execution.test_disks()[0]
        .write_all(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, b"unknown".to_vec().into())
        .await
        .expect("unknown owner");
    assert!(parent.prepare_replacement_handoff(&execution, Vec::new()).await.is_err());
    assert_eq!(
        ResumeManager::read_state_file(&parent.disk, &state.task_id, parent.state_file)
            .await
            .expect("source retained"),
        before
    );
    {
        let mut state = parent.state.write().await;
        state.retry_count = state.max_retries;
    }
    parent.save_state_strict().await.expect("exhausted budget");
    assert!(parent.prepare_replacement_handoff(&execution, Vec::new()).await.is_err());
    assert!(parent.get_state().await.replacement_handoff.is_none());
}

#[tokio::test]
async fn handoff_rebinds_an_unstarted_successor_after_another_reboot() {
    let (_dirs, parent, execution) = fixture(1).await;
    let child = parent
        .prepare_replacement_handoff(&execution, Vec::new())
        .await
        .expect("prepare");
    let reserved = child.get_state().await.task_id;
    let next_mount = ReplacementExecution::for_test(
        execution.test_disks().to_vec(),
        vec![identity("replacement-0", "third-mount:new-fs:new-root")],
    );
    let resumed = parent
        .prepare_replacement_handoff(&next_mount, Vec::new())
        .await
        .expect("rebind unstarted child");
    assert_eq!(resumed.get_state().await.task_id, reserved);
    assert_eq!(resumed.get_state().await.replacement_target_identities, next_mount.identities());
    resumed.state.write().await.processed_objects = 1;
    resumed.save_state_strict().await.expect("unexpected scan progress");
    assert!(
        parent.prepare_replacement_handoff(&execution, Vec::new()).await.is_err(),
        "must not rebind a started child"
    );
}

#[tokio::test]
async fn handoff_revision_cas_rejects_a_stale_state_writer() {
    let (_dirs, first, execution) = fixture(1).await;
    let state = first.get_state().await;
    let stale = ResumeManager::load_replacement_intent(first.disk.clone(), &state.task_id)
        .await
        .expect("second opener");
    first
        .prepare_replacement_handoff(&execution, Vec::new())
        .await
        .expect("durable handoff");
    assert!(
        stale
            .record_replacement_failure(&Error::other("stale failure"), 0)
            .await
            .is_err()
    );
    let actual = ResumeManager::load_replacement_intent(first.disk.clone(), &state.task_id)
        .await
        .expect("read authoritative state")
        .get_state()
        .await;
    assert_eq!(actual.replacement_phase, ReplacementPhase::HandoffPending);
    assert!(actual.error_message.is_none());
}

#[tokio::test]
async fn handoff_gc_retains_pending_and_committed_authority() {
    let (_dirs, parent, execution) = fixture(1).await;
    parent.state.write().await.last_update = 1;
    parent.save_state_strict().await.expect("old intent");
    let child = parent
        .prepare_replacement_handoff(&execution, Vec::new())
        .await
        .expect("prepare");
    ResumeUtils::cleanup_expired_states(&parent.disk, 0)
        .await
        .expect("pending GC");
    assert!(ResumeManager::has_replacement_intent(&parent.disk, &parent.get_state().await.task_id).await);
    child.acquire_replacement_markers(&execution).await.expect("commit");
    ResumeUtils::cleanup_expired_states(&parent.disk, 0)
        .await
        .expect("committed GC");
    assert!(ResumeManager::has_replacement_intent(&parent.disk, &parent.get_state().await.task_id).await);
    assert!(ResumeManager::has_replacement_intent(&parent.disk, &child.get_state().await.task_id).await);
}
