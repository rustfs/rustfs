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
use crate::heal::{
    HEALING_MARKER_PATH, HealObjectInfo,
    resume::tests::schema_test_disk,
    storage::{HealListItem, HealStorageAPI},
    storage_api::storage::{BucketInfo, DiskSetSelector},
};
use rustfs_heal_contracts::heal_channel::HealOpts;
use rustfs_madmin::heal_commands::HealResultItem;
use std::sync::{
    LazyLock,
    atomic::{AtomicUsize, Ordering},
};

static FAILURES: LazyLock<Mutex<HashMap<String, &'static str>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

struct PreformatRecoveryStorage {
    execution: Arc<ReplacementExecution>,
    historical_topology: bool,
    probe_calls: AtomicUsize,
    identity_calls: AtomicUsize,
    change_identity_on_call: usize,
}

#[async_trait::async_trait]
impl HealStorageAPI for PreformatRecoveryStorage {
    async fn replacement_target_identities(&self, _: &[String]) -> Result<Vec<ReplacementTargetIdentity>> {
        let mut identities = self.execution.identities().to_vec();
        if self.identity_calls.fetch_add(1, Ordering::SeqCst) == self.change_identity_on_call {
            identities[0].filesystem_identity = "another-mount:fs:root".to_string();
        }
        Ok(identities)
    }

    async fn replacement_execution(&self, _: &[String]) -> Result<Arc<ReplacementExecution>> {
        Ok(self.execution.clone())
    }

    async fn replacement_targets_ready_for_retry(
        &self,
        _: &str,
        _: &[String],
        expected: &[ReplacementTargetIdentity],
    ) -> Result<bool> {
        if expected != self.execution.identities() {
            return Ok(false);
        }
        for disk in self.execution.test_disks() {
            if !matches!(disk.get_disk_id().await, Ok(Some(id)) if !id.is_nil()) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn replacement_preformat_retry_is_safe(&self, _: &str, _: &[String]) -> Result<bool> {
        self.probe_calls.fetch_add(1, Ordering::SeqCst);
        if !self.historical_topology {
            return Ok(false);
        }
        for disk in self.execution.test_disks() {
            match disk.read_all(RUSTFS_META_BUCKET, "format.json").await {
                Err(DiskError::FileNotFound | DiskError::VolumeNotFound) => {}
                Ok(_) => return Ok(false),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(true)
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        Ok(Vec::new())
    }

    async fn get_object_meta(&self, _: &str, _: &str) -> Result<Option<HealObjectInfo>> {
        Err(Error::other("pre-format recovery must not inspect objects"))
    }

    async fn ec_decode_rebuild(&self, _: &str, _: &str) -> Result<Vec<u8>> {
        Err(Error::other("pre-format recovery must not rebuild objects"))
    }

    async fn get_bucket_info(&self, _: &str) -> Result<Option<BucketInfo>> {
        Err(Error::other("pre-format recovery must not inspect buckets"))
    }

    async fn list_buckets_for_admin_heal(&self, _: Option<DiskSetSelector>) -> Result<Vec<BucketInfo>> {
        Err(Error::other("pre-format recovery must not start admin discovery"))
    }

    async fn object_exists(&self, _: &str, _: &str) -> Result<bool> {
        Err(Error::other("pre-format recovery must not inspect objects"))
    }

    async fn heal_object(&self, _: &str, _: &str, _: Option<&str>, _: &HealOpts) -> Result<(HealResultItem, Option<Error>)> {
        Err(Error::other("pre-format recovery must not heal objects"))
    }

    async fn heal_bucket(&self, _: &str, _: &HealOpts) -> Result<HealResultItem> {
        Err(Error::other("pre-format recovery must not heal buckets"))
    }

    async fn heal_format(&self, _: bool) -> Result<(HealResultItem, Option<Error>)> {
        Err(Error::other("pre-format recovery must not write formats"))
    }

    async fn list_objects_for_heal_page(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: bool,
    ) -> Result<(Vec<HealListItem>, Option<String>, bool)> {
        Err(Error::other("pre-format recovery must not traverse objects"))
    }

    async fn get_disk_for_resume(&self, _: &str) -> Result<DiskStore> {
        Err(Error::other("pre-format recovery must retain its durable anchor"))
    }
}

async fn preformat_recovery_fixture() -> (Vec<tempfile::TempDir>, ResumeManager, PreformatRecoveryStorage) {
    let (dirs, manager, execution) = fixture(1).await;
    execution.test_disks()[0]
        .delete(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, Default::default())
        .await
        .expect("remove the fixture marker from the blank replacement");
    {
        let mut state = manager.state.write().await;
        state.retry_count = state.max_retries;
        state.replacement_preformat_handoff_retry_done = false;
        state.replacement_retry_waiting_for_target = true;
    }
    manager.save_state_strict().await.expect("persist exhausted older intent");
    (
        dirs,
        manager,
        PreformatRecoveryStorage {
            execution,
            historical_topology: true,
            probe_calls: AtomicUsize::new(0),
            identity_calls: AtomicUsize::new(0),
            change_identity_on_call: usize::MAX,
        },
    )
}

#[tokio::test]
async fn preformat_recovery_same_mount_reuses_existing_readiness_rearm() {
    let (_dirs, manager, mut storage) = preformat_recovery_fixture().await;
    let initial = manager.get_state().await;
    storage.execution =
        ReplacementExecution::for_test(storage.execution.test_disks().to_vec(), initial.replacement_target_identities.clone());
    assert!(matches!(
        storage.execution.test_disks()[0].get_disk_id().await,
        Err(DiskError::UnformattedDisk)
    ));
    assert!(
        manager
            .rearm_replacement_recovery_if_needed(&storage)
            .await
            .expect("rearm blank same mount")
    );
    let resumed = manager
        .resolve_replacement_recovery(&storage)
        .await
        .expect("resume the same generation");
    assert_eq!(resumed.task_id, initial.task_id);
    assert_eq!(resumed.retry_count, resumed.max_retries - 1);
    assert!(resumed.replacement_handoff.is_none());
    assert!(!resumed.replacement_preformat_handoff_retry_done);
    assert_eq!(storage.probe_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn preformat_recovery_reserves_one_handoff_and_one_rebuild() {
    let (_dirs, manager, mut storage) = preformat_recovery_fixture().await;
    assert!(
        manager
            .rearm_replacement_recovery_if_needed(&storage)
            .await
            .expect("admit historical pre-format recovery")
    );
    let prepared = manager.get_state().await;
    assert_eq!(prepared.replacement_phase, ReplacementPhase::HandoffPending);
    assert!(prepared.replacement_preformat_handoff_retry_done);
    let child_state = manager
        .resolve_replacement_recovery(&storage)
        .await
        .expect("resolve the reserved successor");
    assert_eq!(child_state.retry_count, child_state.max_retries - 1);
    assert!(child_state.replacement_preformat_handoff_retry_done);
    assert_eq!(child_state.replacement_lineage.len(), 1);
    let child = ResumeManager::load_replacement_intent(manager.disk.clone(), &child_state.task_id)
        .await
        .expect("reload the admitted successor");
    // Unlike a generic schema-test disk, an admitted replacement has an exact
    // set/slot binding that get_disk_id validates against its persisted format.
    let mut endpoint = storage.execution.test_disks()[0].endpoint();
    endpoint.set_pool_index(0);
    endpoint.set_set_index(0);
    endpoint.set_disk_index(0);
    let disk = crate::heal::new_disk(
        &endpoint,
        &crate::heal::DiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("open the replacement with its real erasure-set slot");
    storage.execution = ReplacementExecution::for_test(vec![disk.clone()], storage.execution.identities().to_vec());
    let disk_id = Uuid::new_v4();
    let format = serde_json::json!({
        "version": "1", "format": "xl", "id": Uuid::new_v4(),
        "xl": {"version": "3", "this": disk_id, "sets": [[disk_id]], "distributionAlgo": "SIPMOD+PARITY"}
    });
    storage.execution.test_disks()[0]
        .write_all(
            RUSTFS_META_BUCKET,
            "format.json",
            serde_json::to_vec(&format).expect("serialize target format").into(),
        )
        .await
        .expect("format the admitted target before rebuilding");
    crate::heal::storage_api::EcstoreDiskAPI::set_disk_id(disk.as_ref(), Some(disk_id))
        .await
        .expect("publish the formatted disk identity as save_format_file does");
    assert_eq!(disk.get_disk_id().await.expect("formatted replacement must be ready"), Some(disk_id));
    child
        .acquire_replacement_markers(&storage.execution)
        .await
        .expect("commit successor ownership");
    child
        .mark_replacement_rebuilding(storage.execution.identities().to_vec())
        .await
        .expect("admit actual rebuilding");
    child
        .record_replacement_failure(&Error::other("actual rebuild failed"), child_state.max_retries)
        .await
        .expect("exhaust final attempt");
    assert!(
        !child
            .rearm_replacement_recovery_if_needed(&storage)
            .await
            .expect("never regrant the successor")
    );
    let reloaded = ResumeManager::load_replacement_intent(manager.disk.clone(), &prepared.task_id)
        .await
        .expect("reload predecessor");
    assert!(
        !reloaded
            .rearm_preformat_handoff_retry_if_needed(&storage)
            .await
            .expect("never regrant the predecessor")
    );
    assert_eq!(storage.probe_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn preformat_recovery_replays_prepared_and_published_crashes_without_new_credit() {
    for boundary in ["prepared", "successor_published"] {
        let (_dirs, manager, storage) = preformat_recovery_fixture().await;
        let source = manager.get_state().await.task_id;
        FAILURES
            .lock()
            .expect("failure injection lock")
            .insert(source.clone(), boundary);
        assert!(manager.rearm_replacement_recovery_if_needed(&storage).await.is_err());
        let reloaded = ResumeManager::load_replacement_intent(manager.disk.clone(), &source)
            .await
            .expect("reload crashed grant");
        let prepared = reloaded.get_state().await;
        assert!(prepared.replacement_preformat_handoff_retry_done);
        let successor = prepared
            .replacement_handoff
            .expect("grant and successor are atomic")
            .link
            .successor;
        assert!(
            !reloaded
                .rearm_preformat_handoff_retry_if_needed(&storage)
                .await
                .expect("grant already used")
        );
        let recovered = reloaded
            .resolve_replacement_recovery(&storage)
            .await
            .expect("replay the reserved handoff");
        assert_eq!(recovered.task_id, successor);
        assert_eq!(recovered.retry_count, recovered.max_retries - 1);
        assert_eq!(storage.probe_calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn preformat_recovery_rejects_progress_checkpoints_formats_and_unrelated_topology() {
    for rejection in [
        "progress",
        "checkpoint",
        "blocked",
        "completion_proof",
        "format",
        "topology",
        "new_generation",
        "single_retry",
    ] {
        let (_dirs, manager, mut storage) = preformat_recovery_fixture().await;
        let source = manager.get_state().await.task_id;
        match rejection {
            "progress" => manager.state.write().await.processed_objects = 1,
            "checkpoint" | "blocked" => {
                let suffix = if rejection == "checkpoint" {
                    RESUME_CHECKPOINT_FILE
                } else {
                    RESUME_CHECKPOINT_BLOCKED_FILE
                };
                manager
                    .disk
                    .write_all(RUSTFS_META_BUCKET, &format!("{BUCKET_META_PREFIX}/{source}_{suffix}"), Vec::new().into())
                    .await
                    .expect("persist checkpoint evidence, including an empty file");
            }
            "format" => storage.execution.test_disks()[0]
                .write_all(RUSTFS_META_BUCKET, "format.json", b"existing format".to_vec().into())
                .await
                .expect("persist actual format evidence"),
            "completion_proof" => {
                let proof = super::super::replacement::ReplacementCompletionProof::from_state(&manager.get_state().await, 1)
                    .expect("construct a prior completion proof");
                manager
                    .disk
                    .write_all(
                        RUSTFS_META_BUCKET,
                        path_to_str(&replacement_completion_proof_path(&source)).expect("proof path"),
                        serde_json::to_vec(&proof).expect("serialize completion proof").into(),
                    )
                    .await
                    .expect("persist existing completion proof");
            }
            "topology" => storage.historical_topology = false,
            "new_generation" => manager.state.write().await.replacement_preformat_handoff_retry_done = true,
            "single_retry" => manager.state.write().await.max_retries = 1,
            _ => unreachable!("known test rejection"),
        }
        manager.save_state_strict().await.expect("persist rejection fixture");
        let before = ResumeManager::read_state_file(&manager.disk, &source, manager.state_file)
            .await
            .expect("source bytes before admission");
        assert!(
            !manager
                .rearm_preformat_handoff_retry_if_needed(&storage)
                .await
                .expect("reject unsafe recovery"),
            "{rejection}"
        );
        assert_eq!(
            ResumeManager::read_state_file(&manager.disk, &source, manager.state_file)
                .await
                .expect("source bytes after rejection"),
            before,
            "{rejection}"
        );
    }
}

#[tokio::test]
async fn preformat_recovery_rejects_mount_changes_before_or_during_probe() {
    for change_identity_on_call in [0, 1] {
        let (_dirs, manager, mut storage) = preformat_recovery_fixture().await;
        storage.change_identity_on_call = change_identity_on_call;
        let source = manager.get_state().await.task_id;
        let before = ResumeManager::read_state_file(&manager.disk, &source, manager.state_file)
            .await
            .expect("source bytes before mount change");
        assert!(matches!(
            manager.rearm_preformat_handoff_retry_if_needed(&storage).await,
            Err(Error::ReplacementTargetNotReady(_))
        ));
        assert_eq!(
            ResumeManager::read_state_file(&manager.disk, &source, manager.state_file)
                .await
                .expect("source bytes after mount change"),
            before
        );
    }
}

#[tokio::test]
async fn preformat_recovery_rejects_unknown_marker_and_stale_revision() {
    let (_dirs, manager, storage) = preformat_recovery_fixture().await;
    let source = manager.get_state().await.task_id;
    storage.execution.test_disks()[0]
        .write_all(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, b"unknown".to_vec().into())
        .await
        .expect("publish unknown marker owner");
    let before = ResumeManager::read_state_file(&manager.disk, &source, manager.state_file)
        .await
        .expect("source before owner conflict");
    assert!(matches!(
        manager.rearm_preformat_handoff_retry_if_needed(&storage).await,
        Err(Error::ReplacementOwnershipConflict(_))
    ));
    assert_eq!(
        ResumeManager::read_state_file(&manager.disk, &source, manager.state_file)
            .await
            .expect("source after owner conflict"),
        before
    );
    storage.execution.test_disks()[0]
        .delete(RUSTFS_META_BUCKET, HEALING_MARKER_PATH, Default::default())
        .await
        .expect("remove fixture owner conflict");
    let stale = ResumeManager::load_replacement_intent(manager.disk.clone(), &source)
        .await
        .expect("open stale source");
    manager.save_state_strict().await.expect("advance source revision");
    assert!(stale.rearm_preformat_handoff_retry_if_needed(&storage).await.is_err());
    let unchanged = ResumeManager::load_replacement_intent(manager.disk.clone(), &source)
        .await
        .expect("reload authoritative source")
        .get_state()
        .await;
    assert!(!unchanged.replacement_preformat_handoff_retry_done);
    assert!(unchanged.replacement_handoff.is_none());
}

#[test]
fn preformat_recovery_marker_defaults_only_old_snapshots_to_eligible() {
    let state = ResumeState::replacement_intent(
        Uuid::new_v4().to_string(),
        "erasure_set".into(),
        "pool_0_set_0".into(),
        Vec::new(),
        vec!["replacement-0".into()],
        vec![identity("replacement-0", "mount:fs:root")],
    );
    assert!(state.replacement_preformat_handoff_retry_done);
    let mut wire = serde_json::to_value(state).expect("serialize new intent");
    wire.as_object_mut()
        .expect("intent object")
        .remove("replacement_preformat_handoff_retry_done");
    let old: ResumeState = serde_json::from_value(wire).expect("deserialize older schema-seven intent");
    assert!(!old.replacement_preformat_handoff_retry_done);
}

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

#[tokio::test]
async fn handoff_chain_does_not_consume_the_rebuild_retry_budget() {
    // rustfs/rustfs#8664: every mount change hands the generation off again.
    // Interruptions are not rebuild failures, so three consecutive handoffs
    // must leave the full `max_retries` budget for the eventual rebuild.
    let (_dirs, root, execution) = fixture(1).await;
    let max_retries = root.get_state().await.max_retries;
    let mut current = root;
    let mut mount = 0;
    for interruption in 0..3 {
        mount += 1;
        let next_mount = ReplacementExecution::for_test(
            execution.test_disks().to_vec(),
            vec![identity("replacement-0", &format!("mount-{mount}:fs-1:root-1"))],
        );
        let child = current
            .prepare_replacement_handoff(&next_mount, Vec::new())
            .await
            .unwrap_or_else(|error| panic!("interruption {interruption} must still hand off: {error}"));
        child
            .acquire_replacement_markers(&next_mount)
            .await
            .expect("successor takes the markers");
        let state = child.get_state().await;
        assert_eq!(state.retry_count, 0, "handoff {interruption} must not spend a rebuild attempt");
        assert_eq!(state.max_retries, max_retries);
        assert_eq!(state.replacement_lineage.len(), interruption + 1);
        current = child;
    }
    let final_mount = ReplacementExecution::for_test(
        execution.test_disks().to_vec(),
        vec![identity("replacement-0", &format!("mount-{mount}:fs-1:root-1"))],
    );
    current
        .mark_replacement_rebuilding(final_mount.identities().to_vec())
        .await
        .expect("the last successor rebuilds");
    current
        .mark_replacement_completed_and_verified()
        .await
        .expect("the last successor completes");
    assert_eq!(current.get_state().await.replacement_phase, ReplacementPhase::Verified);
}

#[tokio::test]
async fn committed_predecessor_is_collectible_once_its_successor_is_verified() {
    let (_dirs, parent, execution) = fixture(1).await;
    let parent_id = parent.get_state().await.task_id;
    let child = parent
        .prepare_replacement_handoff(&execution, Vec::new())
        .await
        .expect("prepare");
    child.acquire_replacement_markers(&execution).await.expect("commit");
    let child_id = child.get_state().await.task_id;
    // The commit advanced the predecessor through its own handle; reload the
    // durable revision before aging both records.
    let parent = ResumeManager::load_replacement_intent(parent.disk.clone(), &parent_id)
        .await
        .expect("reload committed predecessor");
    for manager in [&parent, &child] {
        manager.state.write().await.last_update = 1;
        manager.save_state_strict().await.expect("age the record");
    }
    ResumeUtils::cleanup_expired_states(&parent.disk, 0)
        .await
        .expect("GC before verification");
    assert!(
        ResumeManager::has_replacement_intent(&parent.disk, &parent_id).await,
        "a committed authority is retained while its successor may still need it"
    );

    child
        .mark_replacement_rebuilding(execution.identities().to_vec())
        .await
        .expect("rebuild");
    child.mark_replacement_completed_and_verified().await.expect("verify");
    ResumeUtils::cleanup_expired_states(&parent.disk, 0)
        .await
        .expect("GC after verification");
    assert!(
        !ResumeManager::has_replacement_intent(&parent.disk, &parent_id).await,
        "a committed predecessor of a verified successor is collectible"
    );
    assert!(
        ResumeManager::has_replacement_intent(&parent.disk, &child_id).await,
        "the verified successor keeps its own record until cleanup"
    );
}
