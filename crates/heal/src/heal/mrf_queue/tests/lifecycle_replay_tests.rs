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

fn lifecycle_frame(payload: &Value) -> Vec<u8> {
    let payload = serde_json::to_vec(payload).expect("encode lifecycle fixture");
    let mut encoded = Vec::from(MRF_LIFECYCLE_MAGIC.as_slice());
    encoded.push(MRF_LIFECYCLE_FORMAT_VERSION);
    encoded.extend_from_slice(&u64::try_from(payload.len()).expect("fixture size").to_le_bytes());
    encoded.extend_from_slice(&sha2::Sha256::digest(&payload));
    encoded.extend_from_slice(&payload);
    encoded
}

#[test]
fn unknown_source_deleted_bucket_lifecycle_rejects_unknown_state_version_and_fields() {
    assert_eq!(
        serde_json::to_string(&partial_write::ResponsibilityState::Active {}).expect("encode active state"),
        r#"{"state":"active"}"#,
        "strict field validation must preserve the legacy encoding"
    );
    let owner = Uuid::new_v4();
    let intent_digest = [7_u8; 32];
    let mut payload = json!({
        "format_version": MRF_LIFECYCLE_FORMAT_VERSION,
        "checkpoint_owner": owner,
        "checkpoint_sequence": 1,
        "records": [{
            "intent_digest": intent_digest,
            "responsibility_id": Uuid::new_v4(),
            "source_bucket_incarnation_id": null,
            "last_operator_acceptance": null,
            "state": {"state": "active"}
        }]
    });
    let valid = lifecycle_frame(&payload);
    assert!(decode_mrf_lifecycle_checkpoint(&valid, 8192).is_ok());
    let mut future_version = valid;
    future_version[8] = MRF_LIFECYCLE_FORMAT_VERSION_V2 + 1;
    assert!(matches!(
        decode_mrf_lifecycle_checkpoint(&future_version, 8192),
        Err(MrfReplayError::Snapshot(snapshot::SnapshotError::Unsupported))
    ));
    payload["records"][0]["state"] = json!({"state": "source_and_current_generation_unknown", "since_ms": 7});
    assert!(matches!(
        decode_mrf_lifecycle_checkpoint(&lifecycle_frame(&payload), 8192),
        Err(MrfReplayError::Lifecycle(_))
    ));
    payload["records"][0]["state"] = json!({"state": "active", "hidden_source_incarnation": Uuid::new_v4()});
    assert!(matches!(
        decode_mrf_lifecycle_checkpoint(&lifecycle_frame(&payload), 8192),
        Err(MrfReplayError::Lifecycle(_))
    ));
}

fn durable_files(roots: &[PathBuf]) -> Vec<Option<Vec<u8>>> {
    roots
        .iter()
        .flat_map(|root| {
            [
                ".heal-mrf-snapshot.0.bin",
                ".heal-mrf-snapshot.1.bin",
                ".heal-mrf-commit.0.bin",
                ".heal-mrf-commit.1.bin",
                MRF_LIFECYCLE_PATHS[0],
                MRF_LIFECYCLE_PATHS[1],
                MRF_SCOPED_JOURNAL_PATH,
                MRF_JOURNAL_PATH,
            ]
            .map(|path| fs::read(root.join(RUSTFS_META_BUCKET).join(path)).ok())
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn unknown_source_deleted_bucket_unsupported_lifecycle_freezes_all_writes() {
    assert_unreadable_lifecycle_is_retained(|payload| {
        payload["records"][0]["state"] = json!({"state": "future_held", "since_ms": 17});
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn unknown_source_deleted_bucket_mismatched_lifecycle_freezes_all_writes() {
    assert_unreadable_lifecycle_is_retained(|payload| {
        let wrong_digest = [7_u8; 32];
        payload["records"][0]["intent_digest"] = json!(wrong_digest);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn unknown_source_deleted_bucket_missing_lifecycle_record_freezes_all_writes() {
    assert_unreadable_lifecycle_is_retained(|payload| payload["records"] = json!([])).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn unknown_source_deleted_bucket_extra_lifecycle_record_freezes_all_writes() {
    assert_unreadable_lifecycle_is_retained(|payload| {
        let mut extra = payload["records"][0].clone();
        extra["responsibility_id"] = json!(Uuid::new_v4());
        extra["source_bucket_incarnation_id"] = json!(Uuid::new_v4());
        payload["records"].as_array_mut().expect("fixture records").push(extra);
    })
    .await;
}

async fn assert_unreadable_lifecycle_is_retained(corrupt: fn(&mut Value)) {
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("rustfs_mrf_unknown_lifecycle")
        .build()
        .await;
    let config = MrfConsumerConfig::default();
    let owner = Uuid::new_v4();
    let mut record = intent("retained-bucket", "retained-object", 0);
    record.kind = MrfKind::PartialWrite;
    let bytes = encoded_payload(&record);
    let mut lifecycle = json!({
        "format_version": MRF_LIFECYCLE_FORMAT_VERSION,
        "checkpoint_owner": owner,
        "checkpoint_sequence": 1,
        "records": [{
            "intent_digest": intent_digest(&record).expect("fixture digest"),
            "responsibility_id": Uuid::new_v4(),
            "source_bucket_incarnation_id": Uuid::new_v4(),
            "last_operator_acceptance": null,
            "state": {"state": "active"}
        }]
    });
    corrupt(&mut lifecycle);
    let invalid_lifecycle = lifecycle_frame(&lifecycle);
    snapshot::publish_committed_snapshot_with_companion(
        &journal_disks().await,
        owner,
        1,
        &bytes,
        config.journal_max_bytes,
        Some((&MRF_LIFECYCLE_PATHS, &invalid_lifecycle, config.journal_max_bytes)),
    )
    .await
    .expect("commit an unreadable companion with the retained intent");
    let before = durable_files(&env.disk_paths);
    assert!(before.iter().any(Option::is_some), "fixture must contain actual committed files");
    assert!(
        read_replay_source(config.journal_max_bytes, config.journal_max_bytes * 4)
            .await
            .is_err(),
        "an unreadable companion must not be treated as an absent legacy companion"
    );

    let storage: Arc<dyn HealStorageAPI> = Arc::new(ECStoreHealStorage::new(env.ecstore.clone()));
    let manager = Arc::new(HealManager::new_without_root_recovery_for_test(storage, None));
    let mut queue = MrfQueue::new(config.queue_capacity, config.journal_max_bytes);
    let replay = replay_into(&manager, &mut queue, &mut None).await;
    assert!(replay.retain_journal_for_replay);
    assert!(replay.partial_writes.is_empty(), "do not synthesize source-less active responsibilities");
    let mut runtime = MrfRuntime {
        partial_writes: PartialWrites::default(),
        queue,
        config,
        checkpoint_owner: Uuid::new_v4(),
        next_checkpoint_sequence: replay.next_checkpoint_sequence,
        lifecycle_format_version: replay.lifecycle_format_version,
        new_since_flush: 0,
        dirty: true,
        journal_on_disk: replay.journal_on_disk,
        retain_replay_journal: replay.retain_journal_for_replay,
        replay_readable: replay.source_readable,
        durable_replay_anchors: replay.durable_replay_anchors,
        replay_cleanup: replay.cleanup,
        runtime_checkpoint: None,
        backoff_until: None,
    };
    assert!(!runtime.flush().await, "an unreadable owner must fence every writer");
    assert!(matches!(
        runtime.admit_partial_write(record.clone(), Some(Uuid::new_v4())),
        Err(MrfDurableAdmissionError::Persistence)
    ));
    assert!(!runtime.flush().await, "new admission must not overwrite the unreadable owner");
    assert!(!runtime.delete_idle_recovery_anchors().await, "idle cleanup must also remain fenced");
    assert!(matches!(
        runtime.list_unverified_legacy(None, 10),
        Err(MrfLifecycleControlError::CheckpointUnreadable)
    ));
    assert_eq!(
        durable_files(&env.disk_paths),
        before,
        "payloads, manifests, companions and mirrors must stay byte-identical"
    );

    let (incoming, receiver) = mpsc::channel(4);
    let (durable, durable_receiver) = mpsc::channel(4);
    let (lifecycle, lifecycle_receiver) = mpsc::channel(4);
    let consumer = tokio::spawn(run_mrf_consumer(manager.clone(), receiver, durable_receiver, lifecycle_receiver));
    let (response, received) = tokio::sync::oneshot::channel();
    durable
        .send(MrfDurableSubmission {
            intent: record,
            source_bucket_incarnation_id: Some(Uuid::new_v4()),
            response,
        })
        .await
        .expect("submit to the actual consumer");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), received)
            .await
            .expect("admission deadline")
            .expect("admission response"),
        Err(MrfDurableAdmissionError::Persistence)
    ));
    let (response, listed) = tokio::sync::oneshot::channel();
    lifecycle
        .send(MrfLifecycleCommand::List {
            after: None,
            limit: 10,
            response,
        })
        .await
        .expect("query the actual consumer");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), listed)
            .await
            .expect("listing deadline")
            .expect("listing response"),
        Err(MrfLifecycleControlError::CheckpointUnreadable)
    ));
    incoming
        .send(intent("retained-bucket", "new-best-effort", 0))
        .await
        .expect("queue a periodic flush");
    tokio::time::sleep(Duration::from_millis(600)).await;
    drop(incoming);
    tokio::time::timeout(Duration::from_secs(5), consumer)
        .await
        .expect("consumer shutdown deadline")
        .expect("consumer shutdown");
    assert_eq!(manager.get_queue_length().await, 0, "unreadable recovery must not dispatch object work");
    assert_eq!(
        durable_files(&env.disk_paths),
        before,
        "startup, admission, periodic flush and shutdown must preserve every file"
    );
}
