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

fn empty_runtime(capacity: usize) -> MrfRuntime {
    let config = MrfConsumerConfig::default();
    MrfRuntime {
        queue: MrfQueue::new(capacity, config.journal_max_bytes),
        partial_writes: PartialWrites::default(),
        config,
        checkpoint_owner: Uuid::new_v4(),
        next_checkpoint_sequence: 1,
        lifecycle_format_version: MRF_LIFECYCLE_FORMAT_VERSION,
        new_since_flush: 0,
        dirty: false,
        journal_on_disk: false,
        retain_replay_journal: false,
        replay_readable: true,
        durable_replay_anchors: Vec::new(),
        replay_cleanup: None,
        runtime_checkpoint: None,
        backoff_until: None,
    }
}

fn partial(object: &str) -> MrfIntent {
    let mut item = intent("mrf-owned-shutdown", object, 0);
    item.kind = MrfKind::PartialWrite;
    item.version_id = None;
    item
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn owned_mrf_shutdown_drains_and_persists_before_ack_and_join() {
    let _env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("mrf_owned_shutdown")
        .build()
        .await;
    let mut runtime = empty_runtime(8);
    let (incoming, mut receiver) = mpsc::channel(4);
    let (durable, mut durable_receiver) = mpsc::channel(4);
    let (lifecycle, mut lifecycle_receiver) = mpsc::channel(4);
    incoming
        .send(intent("mrf-owned-shutdown", "ordinary", 0))
        .await
        .expect("queued ordinary intent");
    let (reply, ack) = tokio::sync::oneshot::channel();
    durable
        .send(MrfDurableSubmission {
            intent: partial("durable"),
            source_bucket_incarnation_id: Some(Uuid::new_v4()),
            response: reply,
        })
        .await
        .expect("queued durable submission");
    let (response, listed) = tokio::sync::oneshot::channel();
    lifecycle
        .send(MrfLifecycleCommand::List {
            after: None,
            limit: 1,
            response,
        })
        .await
        .expect("queued lifecycle query");
    tokio::time::timeout(
        StdDuration::from_secs(10),
        finalize_mrf_consumer(&mut runtime, &mut receiver, &mut durable_receiver, &mut lifecycle_receiver),
    )
    .await
    .expect("real final checkpoint completes with live sender handles");
    assert!(ack.await.expect("explicit durable acknowledgement").is_ok());
    assert!(matches!(
        listed.await.expect("explicit query rejection"),
        Err(MrfLifecycleControlError::Unavailable)
    ));
    assert!(incoming.is_closed() && durable.is_closed() && lifecycle.is_closed());
    assert!(!runtime.dirty);
    let saved = snapshot::inspect_local_committed_snapshot(runtime.config.journal_max_bytes)
        .await
        .expect("inspect committed shutdown snapshot")
        .expect("shutdown successor exists");
    let (items, truncated) = decode_journal(saved.payload());
    assert_eq!(truncated, 0);
    assert_eq!(items.len(), 2, "both ingress kinds have durable replay storage before successful receipt");
}

// The blocked paths belong to this fresh test environment. Restore them even
// when an assertion fails so its ordinary disk cleanup can finish.
struct BlockedMetadata(Vec<(PathBuf, PathBuf)>);
impl Drop for BlockedMetadata {
    fn drop(&mut self) {
        for (original, held) in self.0.drain(..) {
            let _ = fs::remove_file(&original);
            let _ = fs::rename(held, original);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn owned_mrf_failed_flush_retains_runtime_until_disk_recovers() {
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("mrf_owned_retry")
        .build()
        .await;
    let mut runtime = empty_runtime(8);
    runtime
        .admit_partial_write(partial("previously-committed"), Some(Uuid::new_v4()))
        .expect("accept resident responsibility");
    assert!(runtime.flush().await, "establish the prior committed responsibility before failure");
    let mut blocked = BlockedMetadata(Vec::new());
    for root in &env.disk_paths {
        let original = root.join(RUSTFS_META_BUCKET);
        let held = root.join("mrf-owned-held-metadata");
        assert!(!held.exists());
        fs::rename(&original, &held).expect("hold only this fixture metadata volume");
        blocked.0.push((original.clone(), held));
        fs::write(&original, b"block directory traversal").expect("force actual disk writes to fail");
    }
    let (incoming, mut receiver) = mpsc::channel(4);
    let (durable, mut durable_receiver) = mpsc::channel(4);
    let (_lifecycle, mut lifecycle_receiver) = mpsc::channel(4);
    let (reply, ack) = tokio::sync::oneshot::channel();
    durable
        .send(MrfDurableSubmission {
            intent: partial("retained-after-failed-flush"),
            source_bucket_incarnation_id: Some(Uuid::new_v4()),
            response: reply,
        })
        .await
        .expect("queue durable responsibility");
    let mut shutdown = tokio::spawn(async move {
        finalize_mrf_consumer(&mut runtime, &mut receiver, &mut durable_receiver, &mut lifecycle_receiver).await;
        runtime
    });
    assert!(matches!(
        tokio::time::timeout(StdDuration::from_secs(10), ack)
            .await
            .expect("failed checkpoint explicitly responds")
            .expect("durable reply is not dropped"),
        Err(MrfDurableAdmissionError::Persistence)
    ));
    assert!(
        tokio::time::timeout(StdDuration::from_millis(100), &mut shutdown)
            .await
            .is_err(),
        "failed flush is not an exited/joined consumer"
    );
    assert!(incoming.is_closed() && durable.is_closed());
    drop(blocked);
    let runtime = tokio::time::timeout(StdDuration::from_secs(10), shutdown)
        .await
        .expect("same live worker retries after volume recovery")
        .expect("real worker joins");
    assert!(!runtime.dirty);
    assert_eq!(
        runtime.partial_writes.depth(),
        2,
        "prior committed and newly unacknowledged responsibilities both stay owned"
    );
    let saved = snapshot::inspect_local_committed_snapshot(runtime.config.journal_max_bytes)
        .await
        .expect("inspect recovered successor")
        .expect("recovered successor exists");
    assert_eq!(decode_journal(saved.payload()).0.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn owned_mrf_full_admission_replies_full_without_false_ack() {
    let _env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("mrf_owned_full")
        .build()
        .await;
    let mut runtime = empty_runtime(1);
    runtime.admit_best_effort(intent("mrf-owned-shutdown", "resident", 0));
    let (_incoming, mut receiver) = mpsc::channel(4);
    let (durable, mut durable_receiver) = mpsc::channel(4);
    let (_lifecycle, mut lifecycle_receiver) = mpsc::channel(4);
    let (reply, ack) = tokio::sync::oneshot::channel();
    durable
        .send(MrfDurableSubmission {
            intent: partial("beyond-resident-limit"),
            source_bucket_incarnation_id: Some(Uuid::new_v4()),
            response: reply,
        })
        .await
        .expect("queue unacknowledged submission");
    tokio::time::timeout(
        StdDuration::from_secs(10),
        finalize_mrf_consumer(&mut runtime, &mut receiver, &mut durable_receiver, &mut lifecycle_receiver),
    )
    .await
    .expect("resident checkpoint completes");
    assert!(matches!(
        ack.await.expect("Full explicitly returned"),
        Err(MrfDurableAdmissionError::Full)
    ));
    assert_eq!(runtime.queue.depth(), 1);
    assert_eq!(runtime.partial_writes.depth(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn owned_mrf_real_consumer_joins_with_live_static_senders() {
    const CHILD: &str = "RUSTFS_MRF_OWNED_CONSUMER_TEST_CHILD";
    const TEST: &str = "heal::mrf_queue::tests::owned_shutdown_tests::owned_mrf_real_consumer_joins_with_live_static_senders";
    if std::env::var_os(CHILD).is_none() {
        // Process-global senders cannot be reinitialized by another unit test.
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
            .env(CHILD, "1")
            .output()
            .expect("isolated MRF consumer test");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success(), "{stdout}{}", String::from_utf8_lossy(&output.stderr));
        assert!(stdout.contains("test result: ok. 1 passed; 0 failed; 0 ignored;"));
        assert!(stdout.contains("MRF_OWNED_CONSUMER_CHILD_OK"));
        return;
    }
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("mrf_owned_loop")
        .build()
        .await;
    let manager = Arc::new(HealManager::new_without_root_recovery_for_test(
        Arc::new(ECStoreHealStorage::new(env.ecstore.clone())),
        None,
    ));
    manager.stop().await.expect("original manager handoff before MRF stop");
    let stop = tokio_util::sync::CancellationToken::new();
    let task = spawn_owned_mrf_consumer(manager, stop.clone()).expect("fresh process owns actual MRF startup");
    tokio::time::timeout(StdDuration::from_secs(10), list_legacy_responsibilities(None, 1))
        .await
        .expect("actual consumer must process a command before the stop probe")
        .expect("empty lifecycle query succeeds");
    assert!(!task.is_finished(), "the ready consumer is still running with static senders retained");
    stop.cancel();
    tokio::time::timeout(StdDuration::from_secs(10), task)
        .await
        .expect("explicit stop must terminate despite process-global sender lifetime")
        .expect("actual consumer joined");
    println!("MRF_OWNED_CONSUMER_CHILD_OK");
}
