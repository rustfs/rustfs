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
use rustfs_common::mrf_channel::{
    MrfDurableRepairAnchor, MrfScope, MrfUnverifiedLegacyEvent, mrf_durable_repair_events_may_be_pending,
    note_mrf_unverified_legacy, note_mrf_verified_repair,
};
use std::sync::Mutex;

fn isolated(name: &str) -> bool {
    const CHILD: &str = "RUSTFS_MRF_EMPTY_EVENTS_TEST";
    let test = format!("heal::mrf_queue::tests::empty_events_tests::{name}");
    if let Ok(current) = env::var(CHILD) {
        assert_eq!(current, test, "only the selected registry test may run in the child");
        return false;
    }
    let output = std::process::Command::new(env::current_exe().expect("test executable"))
        .args([&test, "--exact", "--test-threads=1"])
        .env(CHILD, &test)
        .output()
        .expect("run isolated MRF consumer test");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed; 0 failed"),
        "consumer child must execute exactly once: {stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    true
}

fn runtime() -> MrfRuntime {
    let config = MrfConsumerConfig::default();
    MrfRuntime {
        partial_writes: PartialWrites::default(),
        queue: MrfQueue::new(config.queue_capacity, config.journal_max_bytes),
        config,
        checkpoint_owner: Uuid::new_v4(),
        next_checkpoint_sequence: 1,
        new_since_flush: 0,
        dirty: false,
        journal_on_disk: false,
        retain_replay_journal: false,
        durable_replay_anchors: Vec::new(),
        replay_cleanup: None,
        runtime_checkpoint: None,
        backoff_until: None,
    }
}

fn partial_intent(bucket: &str) -> MrfIntent {
    MrfIntent {
        kind: MrfKind::PartialWrite,
        scope: Some(MrfScope {
            pool_index: 0,
            set_index: 0,
        }),
        ..intent(bucket, "same-object", 0)
    }
}

fn proof(anchor: &MrfDurableRepairAnchor) -> MrfVerifiedRepairEvent {
    MrfVerifiedRepairEvent {
        kind: anchor.kind,
        bucket: anchor.bucket.clone(),
        object: anchor.object.clone(),
        version_id: anchor.version_id,
        scope: anchor.scope,
        delete_marker_purge: anchor.delete_marker_purge,
        lease: Some(anchor.lease),
        bucket_incarnation_id: anchor.bucket_incarnation_id,
        disposition: MrfVerifiedRepairDisposition::VerifiedHealthy,
    }
}

struct CloneProbe {
    bucket: Arc<str>,
    counts: Mutex<Vec<usize>>,
}

impl metrics::GaugeFn for CloneProbe {
    fn increment(&self, _: f64) {
        panic!("discharge publishes absolute gauges");
    }
    fn decrement(&self, _: f64) {
        panic!("discharge publishes absolute gauges");
    }
    fn set(&self, _: f64) {
        self.counts
            .lock()
            .expect("probe samples")
            .push(Arc::strong_count(&self.bucket));
    }
}

struct ProbeRecorder(Arc<CloneProbe>);

impl metrics::Recorder for ProbeRecorder {
    fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::noop()
    }
    fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        if key.name() == "rustfs_heal_mrf_queue_depth" {
            metrics::Gauge::from_arc(self.0.clone())
        } else {
            metrics::Gauge::noop()
        }
    }
}

#[test]
fn empty_events_avoid_anchor_clones_and_preserve_inner_publication() {
    if isolated("empty_events_avoid_anchor_clones_and_preserve_inner_publication") {
        return;
    }
    assert!(!mrf_durable_repair_events_may_be_pending());
    let mut source = partial_intent("empty-proof-clone-probe");
    assert_eq!(
        rustfs_common::mrf_channel::try_rearm_mrf_replay_intent(&mut source),
        MrfIngressResult::Enqueued
    );
    let anchor = MrfDurableRepairAnchor::from_intent(&source, Uuid::new_v4()).expect("exact fixture anchor");
    let probe = Arc::new(CloneProbe {
        bucket: anchor.bucket.clone(),
        counts: Mutex::new(Vec::new()),
    });
    let recorder = ProbeRecorder(probe.clone());
    let mut runtime = runtime();
    runtime.durable_replay_anchors.push(anchor);
    let baseline = Arc::strong_count(&probe.bucket);
    metrics::with_local_recorder(&recorder, || runtime.discharge_durable_replay_anchors());
    assert_eq!(
        *probe.counts.lock().expect("probe samples"),
        vec![baseline],
        "the real inner publication must occur once without a temporary cloned anchor"
    );
    assert_eq!(runtime.durable_replay_anchors.len(), 1);
    assert!(!runtime.dirty);
    assert!(runtime.retained_replay_journal());
    probe.counts.lock().expect("clear probe").clear();
    runtime.durable_replay_anchors.clear();
    metrics::with_local_recorder(&recorder, || runtime.discharge_durable_replay_anchors());
    assert!(
        probe.counts.lock().expect("probe samples").is_empty(),
        "empty anchors never publish inside discharge"
    );
    runtime
        .admit_partial_write(partial_intent("empty-proof-old-none"), None)
        .expect("retain old unknown");
    let original = runtime.partial_writes.checkpoint_records(intent_digest);
    metrics::with_local_recorder(&recorder, || runtime.discharge_durable_replay_anchors());
    assert!(
        probe.counts.lock().expect("probe samples").is_empty(),
        "unknown-only records do not fabricate an anchor"
    );
    assert_eq!(runtime.partial_writes.checkpoint_records(intent_digest), original);
    rustfs_common::mrf_channel::release_mrf_intent(&source);
}

async fn bound_runtime() -> (rustfs_test_utils::TestECStoreEnv, Arc<HealManager>, MrfRuntime, MrfDurableRepairAnchor) {
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("rustfs_mrf_empty_events")
        .build()
        .await;
    let bucket = "empty-event-responsibility";
    env.make_bucket(bucket, false).await;
    let storage: Arc<dyn HealStorageAPI> = Arc::new(ECStoreHealStorage::new(env.ecstore.clone()));
    let manager = Arc::new(HealManager::new_without_root_recovery_for_test(storage, None));
    let incarnation = env.ecstore.pools[0]
        .get_disks(0)
        .bucket_incarnation_id_from_disk(bucket)
        .await
        .expect("persisted bucket identity");
    let mut runtime = runtime();
    runtime
        .admit_partial_write(partial_intent(bucket), None)
        .expect("retain original source None");
    runtime
        .admit_partial_write(partial_intent(bucket), Some(incarnation))
        .expect("admit adjacent exact source");
    assert!(runtime.flush().await, "both identities need a real committed successor");
    runtime.dispatch(&manager).await;
    let anchor = runtime
        .partial_writes
        .anchors()
        .next()
        .expect("source-bound dispatch creates a real anchor")
        .clone();
    assert_eq!(runtime.partial_writes.depth(), 2);
    (env, manager, runtime, anchor)
}

#[test]
fn empty_events_late_verified_overflow_keeps_real_journal_and_old_none() {
    if isolated("empty_events_late_verified_overflow_keeps_real_journal_and_old_none") {
        return;
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            let (_env, manager, mut runtime, anchor) = bound_runtime().await;
            let unknown = runtime
                .partial_writes
                .checkpoint_records(intent_digest)
                .into_iter()
                .find(|record| record.source_bucket_incarnation_id.is_none())
                .expect("old unknown checkpoint");
            let before = runtime.partial_writes.checkpoint_records(intent_digest);
            assert!(!mrf_durable_repair_events_may_be_pending());
            runtime.discharge_durable_replay_anchors();
            assert_eq!(runtime.partial_writes.checkpoint_records(intent_digest), before);
            note_mrf_verified_repair(proof(&anchor));
            for index in 0..4096 {
                note_mrf_verified_repair(MrfVerifiedRepairEvent {
                    bucket: Arc::from("foreign-overflow"),
                    object: Arc::from(format!("foreign-{index}")),
                    ..proof(&anchor)
                });
            }
            assert!(
                rustfs_common::mrf_channel::take_mrf_verified_repair_events_for(&anchor.bucket).is_empty(),
                "the late proof really was evicted before this consumer opportunity"
            );
            runtime.discharge_durable_replay_anchors();
            assert_eq!(runtime.partial_writes.checkpoint_records(intent_digest), before);
            assert!(runtime.retained_replay_journal());
            assert_eq!(runtime.replay_cleanup_to_delete(), None);
            runtime.dispatch(&manager).await;
            assert_eq!(
                runtime.partial_writes.depth(),
                2,
                "a retry opportunity cannot acknowledge either identity"
            );
            note_mrf_verified_repair(proof(&anchor));
            runtime.discharge_durable_replay_anchors();
            assert_eq!(runtime.partial_writes.checkpoint_records(intent_digest), vec![unknown.clone()]);
            assert!(runtime.dirty);
            assert!(runtime.retained_replay_journal(), "old None still owns its checkpoint");
            assert!(runtime.flush().await);
            let mut queue = MrfQueue::new(runtime.config.queue_capacity, runtime.config.journal_max_bytes);
            let replay = replay_into(&manager, &mut queue, &mut None).await;
            assert_eq!(replay.partial_writes.len(), 1);
            assert_eq!(
                replay.partial_writes[0].1.as_ref(),
                Some(&unknown),
                "identity, None, state, enqueue time and operator audit survive the actual journal replay"
            );
        });
}

#[test]
fn empty_events_late_legacy_only_overflow_parks_only_the_exact_source() {
    if isolated("empty_events_late_legacy_only_overflow_parks_only_the_exact_source") {
        return;
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            let (_env, manager, mut runtime, anchor) = bound_runtime().await;
            let unknown = runtime
                .partial_writes
                .checkpoint_records(intent_digest)
                .into_iter()
                .find(|record| record.source_bucket_incarnation_id.is_none())
                .expect("old unknown checkpoint");
            assert!(!mrf_durable_repair_events_may_be_pending());
            runtime.discharge_durable_replay_anchors();
            note_mrf_unverified_legacy(MrfUnverifiedLegacyEvent { anchor: anchor.clone() });
            for index in 0..4096 {
                note_mrf_unverified_legacy(MrfUnverifiedLegacyEvent {
                    anchor: MrfDurableRepairAnchor {
                        bucket: Arc::from("foreign-legacy-overflow"),
                        object: Arc::from(format!("foreign-{index}")),
                        ..anchor.clone()
                    },
                });
            }
            assert!(
                rustfs_common::mrf_channel::take_mrf_unverified_legacy_events_for(&anchor.bucket).is_empty(),
                "the typed legacy notice was actually evicted"
            );
            runtime.discharge_durable_replay_anchors();
            assert_eq!(runtime.partial_writes.lifecycle_summary().held_count, 0);
            assert_eq!(runtime.partial_writes.depth(), 2);
            assert!(runtime.retained_replay_journal());
            runtime.dispatch(&manager).await;
            note_mrf_unverified_legacy(MrfUnverifiedLegacyEvent { anchor });
            runtime.discharge_durable_replay_anchors();
            assert_eq!(
                runtime.partial_writes.lifecycle_summary().held_count,
                1,
                "legacy-only queue wakes the consumer"
            );
            assert_eq!(runtime.partial_writes.depth(), 2, "parking is never a discharge");
            assert!(runtime.partial_writes.checkpoint_records(intent_digest).contains(&unknown));
            assert!(runtime.dirty);
            assert!(runtime.flush().await, "the held lifecycle transition must be checkpointed");
            let mut queue = MrfQueue::new(runtime.config.queue_capacity, runtime.config.journal_max_bytes);
            let replay = replay_into(&manager, &mut queue, &mut None).await;
            assert_eq!(replay.partial_writes.len(), 2);
            assert!(
                replay
                    .partial_writes
                    .iter()
                    .any(|(_, record)| record.as_ref() == Some(&unknown))
            );
            assert!(replay.partial_writes.iter().any(|(_, record)| {
                record
                    .as_ref()
                    .is_some_and(|record| matches!(record.state, partial_write::ResponsibilityState::HeldUnverifiedLegacy { .. }))
            }));
        });
}

#[test]
fn empty_events_keep_dirty_flush_failure_retry_and_cleanup_gates() {
    if isolated("empty_events_keep_dirty_flush_failure_retry_and_cleanup_gates") {
        return;
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            let (env, manager, mut runtime, _) = bound_runtime().await;
            let original = runtime.partial_writes.checkpoint_records(intent_digest);
            runtime.dirty = true;
            assert!(!mrf_durable_repair_events_may_be_pending());
            runtime.discharge_durable_replay_anchors();
            assert!(matches!(
                tick_action(
                    runtime.dirty,
                    runtime.partial_writes.depth(),
                    runtime.journal_on_disk,
                    runtime.retained_replay_journal()
                ),
                TickAction::Flush
            ));
            let paths = env
                .disk_paths
                .iter()
                .map(|disk| (disk.join(".rustfs.sys"), disk.join(".mrf-empty-sys-backup")))
                .collect::<Vec<_>>();
            for (path, backup) in &paths {
                fs::rename(path, backup).expect("preserve the test's committed metadata before injecting failure");
                fs::write(path, b"block test metadata writes").expect("block the test metadata directory");
            }
            let persisted = runtime.flush().await;
            for (path, backup) in &paths {
                fs::remove_file(path).expect("remove test failure injection");
                fs::rename(backup, path).expect("restore the test's committed metadata");
            }
            assert!(!persisted, "a failed journal write must not become a checkpoint receipt");
            assert!(runtime.dirty);
            assert_eq!(runtime.partial_writes.checkpoint_records(intent_digest), original);
            assert!(runtime.retained_replay_journal());
            assert_eq!(runtime.replay_cleanup_to_delete(), None);
            assert!(runtime.flush().await, "the unchanged dirty path must retry after storage returns");
            assert!(!runtime.dirty);
            runtime.dispatch(&manager).await;
            let mut queue = MrfQueue::new(runtime.config.queue_capacity, runtime.config.journal_max_bytes);
            let replay = replay_into(&manager, &mut queue, &mut None).await;
            let mut restored = replay
                .partial_writes
                .into_iter()
                .map(|(_, record)| record.expect("lifecycle companion"))
                .collect::<Vec<_>>();
            let mut original = original;
            restored.sort_by_key(|record| record.responsibility_id);
            original.sort_by_key(|record| record.responsibility_id);
            assert_eq!(restored, original, "failure and retry preserve both exact persisted responsibilities");
        });
}

#[test]
fn empty_events_real_consumer_stop_retains_unresolved_journal() {
    if isolated("empty_events_real_consumer_stop_retains_unresolved_journal") {
        return;
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            let (_env, manager, mut runtime, anchor) = bound_runtime().await;
            let original = runtime.partial_writes.checkpoint_records(intent_digest);
            assert!(!mrf_durable_repair_events_may_be_pending());
            runtime.discharge_durable_replay_anchors();
            assert_eq!(runtime.partial_writes.checkpoint_records(intent_digest), original);
            drop(runtime);
            let (sender, receiver) = mpsc::channel(4);
            let (durable_sender, durable_receiver) = mpsc::channel(4);
            let (lifecycle_sender, lifecycle_receiver) = mpsc::channel(4);
            let consumer = tokio::spawn(run_mrf_consumer(manager.clone(), receiver, durable_receiver, lifecycle_receiver));
            let (response, listed) = tokio::sync::oneshot::channel();
            lifecycle_sender
                .send(MrfLifecycleCommand::List {
                    after: None,
                    limit: 10,
                    response,
                })
                .await
                .expect("query the actual consumer");
            let listing = tokio::time::timeout(Duration::from_secs(10), listed)
                .await
                .expect("consumer startup finishes")
                .expect("listing receipt");
            assert!(
                listing
                    .responsibilities
                    .iter()
                    .any(|row| row.source_bucket_incarnation_id.is_none()),
                "the actual consumer must restore the unknown source"
            );
            // A prior process's late proof must not be mistaken for a newly replayed
            // lease, even if shutdown wins before the next periodic consumption.
            note_mrf_verified_repair(proof(&anchor));
            drop(sender);
            tokio::time::timeout(Duration::from_secs(10), consumer)
                .await
                .expect("ordinary channel closure returns")
                .expect("consumer task");
            drop(durable_sender);
            drop(lifecycle_sender);
            let mut queue = MrfQueue::new(
                MrfConsumerConfig::default().queue_capacity,
                MrfConsumerConfig::default().journal_max_bytes,
            );
            let replay = replay_into(&manager, &mut queue, &mut None).await;
            let mut restored = replay
                .partial_writes
                .into_iter()
                .map(|(_, record)| record.expect("retained lifecycle companion"))
                .collect::<Vec<_>>();
            let mut original = original;
            restored.sort_by_key(|record| record.responsibility_id);
            original.sort_by_key(|record| record.responsibility_id);
            assert_eq!(
                restored, original,
                "stop returns with exact unresolved data still recoverable, not an event-drain promise"
            );
        });
}
