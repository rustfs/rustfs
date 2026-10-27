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
use rustfs_heal::{
    HealChannelProcessor,
    heal::{
        manager::{HealConfig, HealManager},
        storage::ECStoreHealStorage,
    },
};
use rustfs_heal_contracts::{
    completion::subscribe_heal_completions,
    heal_channel::{HealScanMode, init_heal_channels},
    outcome::HealExecutionOutcome,
};
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

#[tokio::test]
async fn overwritten_unversioned_generation_is_only_a_data_dir_observation() {
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
    assert_eq!(receipt.quorum_selected_data_dir, Some(generation_b), "storage selected generation B");
    assert!(receipt.deep_check_observed, "the protected Deep check path must be observed");

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
    assert_eq!(status.terminal_candidates[0].scope, "identity_unknown");
    assert_eq!(status.terminal_candidates[0].count, 1);
    assert_eq!(
        recorder.total("rustfs_scanner_heal_candidate_selected_data_dir_mismatch_observations_total"),
        1
    );
    assert_eq!(recorder.total("rustfs_scanner_heal_task_deep_check_observations_total"), 1);

    manager.stop().await.expect("stop production heal manager");
    processor.abort();
    let _ = processor.await;
}
