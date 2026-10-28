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

//! One local process exercises the scanner, heal manager, and real ECStore.

use rustfs_ecstore::api::object::{ShardIntegrityWriteMode, WriteCommitGuard, WriteCompletion};
#[cfg(feature = "test-util")]
use rustfs_heal::heal::task::{HealRequest, HealTaskStatus};
use rustfs_heal::{
    HealChannelProcessor,
    heal::{
        manager::{HealConfig, HealManager},
        storage::ECStoreHealStorage,
    },
};
#[cfg(feature = "test-util")]
use rustfs_heal_contracts::{
    completion::HealCompletion,
    heal_channel::{HealAdmissionResult, HealChannelResponse, HealRequestSource, subscribe_heal_responses},
};
use rustfs_heal_contracts::{
    completion::subscribe_heal_completions,
    heal_channel::{HealScanMode, init_heal_channels},
    outcome::HealExecutionOutcome,
};
#[cfg(feature = "test-util")]
use rustfs_scanner::{ScannerHealTestAdmission, ScannerHealTestEvent, ScannerObjectInfo, subscribe_scanner_heal_test_events};
use rustfs_scanner::{
    ScannerObjectOptions, ScannerPutObjReader, SizeSummary, scanner_folder::ScannerItem, scanner_heal_observation_status,
};
use rustfs_storage_api::{NamespaceLocking, ObjectIO, ObjectOperations};
use rustfs_test_utils::TestECStoreEnv;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, atomic::AtomicU64, atomic::Ordering},
    time::Duration,
};
use time::OffsetDateTime;
#[cfg(feature = "test-util")]
use tokio::sync::broadcast;
#[cfg(feature = "test-util")]
use uuid::Uuid;

#[derive(Default)]
struct CounterRecorder(Mutex<HashMap<metrics::Key, Arc<AtomicU64>>>);

impl metrics::Recorder for CounterRecorder {
    fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::from_arc(
            self.0
                .lock()
                .expect("counter recorder")
                .entry(key.clone())
                .or_default()
                .clone(),
        )
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

impl CounterRecorder {
    fn total(&self, name: &str) -> u64 {
        self.0
            .lock()
            .expect("counter recorder")
            .iter()
            .filter(|(key, _)| key.name() == name)
            .map(|(_, value)| value.load(Ordering::Relaxed))
            .sum()
    }
}

async fn put_protected(env: &TestECStoreEnv, bucket: &str, object: &str, fill: u8, mod_time: OffsetDateTime) {
    let lock = env.ecstore.new_ns_lock(bucket, object).await.expect("fixture namespace lock");
    let guard = WriteCommitGuard::acquire(&lock, Duration::from_secs(30))
        .await
        .expect("fixture write ownership");
    let mut options = ScannerObjectOptions {
        mod_time: Some(mod_time),
        shard_integrity_write_mode: Some(ShardIntegrityWriteMode::Protected),
        write_completion: WriteCompletion::TailDrained,
        ..Default::default()
    };
    options.add_write_commit_guard(&guard);
    env.ecstore
        .put_object(bucket, object, &mut ScannerPutObjReader::from_vec(vec![fill; 256 * 1024 + 137]), &options)
        .await
        .expect("write protected object to the real erasure set");
}

#[cfg(feature = "test-util")]
async fn wait_for_observer_admission(
    events: &mut broadcast::Receiver<ScannerHealTestEvent>,
    owner: &str,
    data_dir: Uuid,
) -> ScannerHealTestAdmission {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match events.recv().await.expect("observer test event stream") {
                ScannerHealTestEvent::Admission {
                    task_id,
                    candidate_data_dir: Some(observed),
                    outcome,
                } if task_id == owner && observed == data_dir => break outcome,
                _ => {}
            }
        }
    })
    .await
    .expect("observer must process the candidate admission")
}

#[cfg(feature = "test-util")]
async fn wait_for_observer_terminal(
    events: &mut broadcast::Receiver<ScannerHealTestEvent>,
    owner: &str,
    data_dir: Uuid,
) -> (bool, Vec<Option<Uuid>>, Vec<Option<Uuid>>) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match events.recv().await.expect("observer test event stream") {
                ScannerHealTestEvent::Terminal {
                    task_id,
                    terminal_data_dir: Some(observed),
                    duplicate,
                    settled_candidate_data_dirs,
                    remaining_candidate_data_dirs,
                } if task_id == owner && observed == data_dir => {
                    break (duplicate, settled_candidate_data_dirs, remaining_candidate_data_dirs);
                }
                _ => {}
            }
        }
    })
    .await
    .expect("observer must process the requested terminal")
}

#[cfg(feature = "test-util")]
async fn wait_for_idle(manager: &HealManager) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while manager.get_active_tasks_count().await != 0 || manager.get_queue_length().await != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the previous execution must leave the active queue");
}

#[cfg(feature = "test-util")]
async fn run_reused_owner_round(
    manager: &HealManager,
    scanner: &mut ScannerItem,
    admissions: &mut broadcast::Receiver<HealChannelResponse>,
    completions: &mut broadcast::Receiver<Arc<HealCompletion>>,
    observer_events: &mut broadcast::Receiver<ScannerHealTestEvent>,
    owner: &str,
    candidate: ScannerObjectInfo,
    data_dir: Uuid,
) -> (ScannerHealTestAdmission, (bool, Vec<Option<Uuid>>, Vec<Option<Uuid>>)) {
    manager.set_max_concurrent_heals_for_test(0).await;
    let mut request = HealRequest::object(scanner.bucket.clone(), scanner.object_path(), None);
    request.id = owner.to_owned();
    request.source = HealRequestSource::Scanner;
    request.options.scan_mode = HealScanMode::Deep;
    request.options.recreate_missing = false;
    let receipt = manager
        .submit_heal_request_with_receipt(request)
        .await
        .expect("submit new execution with reused task ID");
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    assert_eq!(receipt.task_id, owner);

    temp_env::async_with_vars([("RUSTFS_SCANNER_DEEP_VERIFY_COOLDOWN_SECS", Some("0"))], async {
        scanner
            .apply_actions(vec![candidate], None, Default::default(), &[], &mut SizeSummary::default())
            .await;
    })
    .await;
    let response = tokio::time::timeout(Duration::from_secs(30), admissions.recv())
        .await
        .expect("scanner admission response deadline")
        .expect("scanner admission response delivered");
    assert_eq!(response.request_id, owner, "scanner must merge into the new owner");
    assert!(response.success);
    assert_eq!(response.data.as_deref(), Some(&b"admission=merged,reason=none"[..]));
    assert_eq!(
        manager.get_task_status(owner).await.expect("new owner status"),
        HealTaskStatus::Pending,
        "new owner must remain queued while the observer handles admission"
    );
    let admission = wait_for_observer_admission(observer_events, owner, data_dir).await;
    assert_ne!(admission, ScannerHealTestAdmission::Unaccounted, "observer did not account for admission");

    manager.set_max_concurrent_heals_for_test(1).await;
    let terminal = tokio::time::timeout(Duration::from_secs(30), completions.recv())
        .await
        .expect("new execution terminal deadline")
        .expect("new terminal delivered");
    assert_eq!(terminal.task_id, owner);
    assert_eq!(terminal.execution, Some(HealExecutionOutcome::Completed));
    assert_eq!(terminal.object.as_ref().expect("new storage receipt").resolved_data_dir, Some(data_dir));
    let observed_terminal = wait_for_observer_terminal(observer_events, owner, data_dir).await;
    wait_for_idle(manager).await;
    (admission, observed_terminal)
}

#[cfg(feature = "test-util")]
fn assert_observer_resolution(
    admission: &ScannerHealTestAdmission,
    terminal: &(bool, Vec<Option<Uuid>>, Vec<Option<Uuid>>),
    data_dir: Uuid,
) {
    let (duplicate, settled, remaining) = terminal;
    assert!(
        remaining.is_empty(),
        "the owner's candidate must not remain pending after terminal processing"
    );
    match admission {
        ScannerHealTestAdmission::Pending => {
            assert!(!duplicate, "the new terminal must not be discarded as a duplicate");
            assert_eq!(settled.as_slice(), &[Some(data_dir)], "only the expected candidate must be settled");
        }
        ScannerHealTestAdmission::Unknown => {}
        ScannerHealTestAdmission::CachedTerminal { .. } | ScannerHealTestAdmission::Unaccounted => {
            panic!("unexpected admission state after stale-terminal assertion")
        }
    }
}

#[tokio::test]
async fn overwritten_unversioned_generation_is_not_attributed_to_the_scanned_candidate() {
    let root = tempfile::tempdir().expect("local erasure-set fixture");
    let env = TestECStoreEnv::builder().base_dir(root.path()).build().await;
    let bucket = "scanner-generation";
    let object = "overwritten-object";
    env.make_bucket(bucket, false).await;

    // The old timestamp exercises the scanner's real Deep selection without
    // altering the ObjectInfo returned by storage.
    let now = OffsetDateTime::now_utc();
    put_protected(&env, bucket, object, 0x41, now - time::Duration::minutes(4)).await;
    let discovered = env
        .ecstore
        .get_object_info(bucket, object, &ScannerObjectOptions::default())
        .await
        .expect("read generation A from storage");
    assert!(discovered.version_id.is_none_or(|id| id.is_nil()), "fixture must be unversioned");
    let generation_a = discovered.data_dir.filter(|id| !id.is_nil()).expect("generation A");

    put_protected(&env, bucket, object, 0x42, now - time::Duration::minutes(3)).await;
    let current = env
        .ecstore
        .get_object_info(bucket, object, &ScannerObjectOptions::default())
        .await
        .expect("read generation B from storage");
    assert!(current.version_id.is_none_or(|id| id.is_nil()), "replacement must remain unversioned");
    let generation_b = current.data_dir.filter(|id| !id.is_nil()).expect("generation B");
    assert_ne!(generation_a, generation_b, "the overwrite must change data_dir");

    let manager = Arc::new(HealManager::new(
        Arc::new(ECStoreHealStorage::new(env.ecstore.clone())),
        Some(HealConfig {
            enable_auto_heal: false,
            mainline_throttle_enable: false,
            max_concurrent_heals: 1,
            low_priority_merge_enable: true,
            event_driven_scheduler_enable: true,
            heal_interval: Duration::from_millis(10),
            ..Default::default()
        }),
    ));
    manager.start().await.expect("start production heal manager");
    let (commands, receipts) = init_heal_channels().expect("initialize process-local heal channels");
    let processor_manager = manager.clone();
    let processor = tokio::spawn(async move {
        HealChannelProcessor::new(processor_manager)
            .start_with_receipts(commands, receipts)
            .await
    });
    let mut completions = subscribe_heal_completions();
    #[cfg(feature = "test-util")]
    let mut observer_events = subscribe_scanner_heal_test_events();
    let recorder = CounterRecorder::default();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    let mut scanner = ScannerItem {
        path: String::new(),
        bucket: bucket.into(),
        prefix: String::new(),
        object_name: object.into(),
        file_type: std::fs::metadata(root.path()).expect("fixture root metadata").file_type(),
        lifecycle: None,
        object_lock: None,
        replication: None,
        heal_enabled: true,
        heal_bitrot: true,
        debug: false,
    };
    temp_env::async_with_vars([("RUSTFS_SCANNER_DEEP_VERIFY_COOLDOWN_SECS", Some("0"))], async {
        scanner
            .apply_actions(vec![discovered], None, Default::default(), &[], &mut SizeSummary::default())
            .await;
    })
    .await;

    let terminal = tokio::time::timeout(Duration::from_secs(30), completions.recv())
        .await
        .expect("manager terminal deadline")
        .expect("manager terminal delivered");
    assert_eq!(terminal.execution, Some(HealExecutionOutcome::Completed));
    assert_eq!(terminal.scan_mode, HealScanMode::Deep);
    assert!(!terminal.dry_run);
    let receipt = terminal.object.as_ref().expect("production object receipt");
    assert_eq!((&*receipt.identity.bucket, &*receipt.identity.object), (bucket, object));
    assert_eq!(receipt.resolved_data_dir, Some(generation_b), "terminal must describe generation B");
    assert!(
        receipt.payload_integrity_verified,
        "real protected Deep scan must prove the executed payload"
    );

    let status = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let status = scanner_heal_observation_status();
            if status.terminal_task_observations_total == Some(1) {
                break status;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("scanner must observe the production terminal");
    assert_eq!(status.integrity_state, "unknown");
    assert_eq!(status.retained_pending_candidates, Some(0));
    assert_eq!(status.terminal_candidates.len(), 1);
    assert_eq!(status.terminal_candidates[0].scope, "identity_mismatch");
    assert_eq!(status.terminal_candidates[0].scan_mode, HealScanMode::Deep.as_str());
    assert_eq!(status.terminal_candidates[0].count, 1);
    assert_eq!(recorder.total("rustfs_scanner_heal_candidate_generation_mismatch_total"), 1);
    assert_eq!(recorder.total("rustfs_scanner_heal_task_deep_payload_verified_observations_total"), 1);

    #[cfg(feature = "test-util")]
    {
        let owner = terminal.task_id.clone();
        let initial = scanner_heal_observation_status();
        assert_eq!(initial.state, "active");
        let observer_generation = initial.generation.expect("observer generation");
        let lost_notifications = initial.lost_completion_notifications.expect("observer loss counter");
        let _ = wait_for_observer_terminal(&mut observer_events, &owner, generation_b).await;
        wait_for_idle(&manager).await;
        let mut admissions = subscribe_heal_responses();

        // The same ID first runs again without an overwrite: equal data_dir
        // still does not mean that this is the earlier execution.
        let (same_admission, same_terminal) = run_reused_owner_round(
            &manager,
            &mut scanner,
            &mut admissions,
            &mut completions,
            &mut observer_events,
            &owner,
            current,
            generation_b,
        )
        .await;

        put_protected(&env, bucket, object, 0x43, now - time::Duration::minutes(2)).await;
        let changed = env
            .ecstore
            .get_object_info(bucket, object, &ScannerObjectOptions::default())
            .await
            .expect("read generation C from storage");
        let generation_c = changed.data_dir.filter(|id| !id.is_nil()).expect("generation C");
        assert_ne!(generation_b, generation_c);
        let (changed_admission, changed_terminal) = run_reused_owner_round(
            &manager,
            &mut scanner,
            &mut admissions,
            &mut completions,
            &mut observer_events,
            &owner,
            changed,
            generation_c,
        )
        .await;

        let final_status = scanner_heal_observation_status();
        assert_eq!(final_status.state, "active");
        assert_eq!(final_status.generation, Some(observer_generation));
        assert_eq!(final_status.lost_completion_notifications, Some(lost_notifications));

        let same_stale = matches!(&same_admission, ScannerHealTestAdmission::CachedTerminal { .. });
        let changed_stale = matches!(&changed_admission, ScannerHealTestAdmission::CachedTerminal { .. });
        assert!(
            !same_stale && !changed_stale,
            "HEAL_ID_REUSE_E2E_STALE_TERMINAL: same_data={same_stale} changed_data={changed_stale}"
        );
        assert_observer_resolution(&same_admission, &same_terminal, generation_b);
        assert_observer_resolution(&changed_admission, &changed_terminal, generation_c);
        assert_eq!(recorder.total("rustfs_scanner_heal_candidate_generation_mismatch_total"), 1);
        assert_eq!(final_status.integrity_state, "unknown");
    }

    manager.stop().await.expect("stop production heal manager");
    processor.abort();
    let _ = processor.await;
}
