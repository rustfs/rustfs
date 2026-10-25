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

fn lifecycle_reader_frame(payload: &Value, envelope_version: u8) -> Vec<u8> {
    let payload = serde_json::to_vec(payload).expect("encode reader fixture");
    let mut encoded = Vec::from(MRF_LIFECYCLE_MAGIC.as_slice());
    encoded.push(envelope_version);
    encoded.extend_from_slice(&u64::try_from(payload.len()).expect("fixture size").to_le_bytes());
    encoded.extend_from_slice(&sha2::Sha256::digest(&payload));
    encoded.extend_from_slice(&payload);
    encoded
}

fn unknown_generation_payload() -> Value {
    let intent_digest = [7_u8; 32];
    json!({
        "format_version": 2,
        "checkpoint_owner": Uuid::new_v4(),
        "checkpoint_sequence": 1,
        "records": [{
            "intent_digest": intent_digest,
            "responsibility_id": Uuid::new_v4(),
            "source_bucket_incarnation_id": null,
            "last_operator_acceptance": null,
            "state": {
                "state": "source_and_current_generation_unknown",
                "since_ms": 7,
                "next_probe_at_ms": 60_007,
                "probe_attempts": 3
            }
        }]
    })
}

#[test]
fn lifecycle_v2_reader_accepts_retained_unknown_generation_without_a_source_identity() {
    let payload = unknown_generation_payload();
    let decoded = decode_mrf_lifecycle_checkpoint(&lifecycle_reader_frame(&payload, 2), 8192)
        .expect("the reader must understand a valid retained V2 responsibility");
    assert_eq!(decoded.format_version, 2);
    assert_eq!(decoded.records.len(), 1);
    assert!(decoded.records[0].is_valid());
    assert_eq!(decoded.records[0].source_bucket_incarnation_id, None);
    assert_eq!(
        serde_json::to_value(&decoded.records[0].state).expect("serialize restored state"),
        payload["records"][0]["state"],
        "reading must preserve the first hold and bounded retry information"
    );
}

#[test]
fn lifecycle_v2_reader_rejects_mislabeled_and_unsupported_formats() {
    let payload = unknown_generation_payload();
    for (payload_version, envelope_version) in [(1, 1), (1, 2), (2, 1), (3, 3)] {
        let mut invalid = payload.clone();
        invalid["format_version"] = json!(payload_version);
        assert!(decode_mrf_lifecycle_checkpoint(&lifecycle_reader_frame(&invalid, envelope_version), 8192).is_err());
    }
    let decoded = decode_mrf_lifecycle_checkpoint(&lifecycle_reader_frame(&payload, 2), 8192).expect("valid V2 fixture");
    for version in [0, 1, 3] {
        assert!(
            encode_mrf_lifecycle_checkpoint(version, decoded.checkpoint_owner, 1, decoded.records.clone(), 8192).is_none(),
            "V2 state cannot be written under format {version}"
        );
    }
    let encoded = encode_mrf_lifecycle_checkpoint(2, decoded.checkpoint_owner, 1, decoded.records.clone(), 8192)
        .expect("preserve an observed V2 owner");
    assert_eq!(encoded[8], 2);
    assert_eq!(
        decode_mrf_lifecycle_checkpoint(&encoded, 8192)
            .expect("read V2 round trip")
            .records,
        decoded.records
    );
}

#[test]
fn lifecycle_v2_reader_rejects_invalid_identity_retry_fields_and_torn_frames() {
    let payload = unknown_generation_payload();
    for source in [Uuid::nil(), Uuid::new_v4()] {
        let mut invalid = payload.clone();
        invalid["records"][0]["source_bucket_incarnation_id"] = json!(source);
        assert!(decode_mrf_lifecycle_checkpoint(&lifecycle_reader_frame(&invalid, 2), 8192).is_err());
    }
    for field in ["since_ms", "next_probe_at_ms", "probe_attempts"] {
        for replacement in [None, Some(json!(-1)), Some(json!("unknown"))] {
            let mut invalid = payload.clone();
            let state = invalid["records"][0]["state"].as_object_mut().expect("state object");
            match replacement {
                Some(value) => {
                    state.insert(field.to_string(), value);
                }
                None => {
                    state.remove(field);
                }
            }
            assert!(
                decode_mrf_lifecycle_checkpoint(&lifecycle_reader_frame(&invalid, 2), 8192).is_err(),
                "reject {field}"
            );
        }
    }
    let mut invalid = payload.clone();
    invalid["records"][0]["state"]["probe_attempts"] = json!(u64::MAX);
    assert!(decode_mrf_lifecycle_checkpoint(&lifecycle_reader_frame(&invalid, 2), 8192).is_err());
    invalid = payload.clone();
    invalid["records"][0]["state"]["current_incarnation"] = json!(Uuid::new_v4());
    assert!(decode_mrf_lifecycle_checkpoint(&lifecycle_reader_frame(&invalid, 2), 8192).is_err());
    let encoded = lifecycle_reader_frame(&payload, 2);
    for length in [0, 48, encoded.len() - 1] {
        assert!(decode_mrf_lifecycle_checkpoint(&encoded[..length], 8192).is_err());
    }
    assert!(decode_mrf_lifecycle_checkpoint(&encoded, encoded.len() - 1).is_err());
    let mut corrupt = encoded;
    *corrupt.last_mut().expect("frame body") ^= 1;
    assert!(decode_mrf_lifecycle_checkpoint(&corrupt, 8192).is_err());
}

#[test]
#[serial]
fn lifecycle_v2_reader_replays_real_files_without_promoting_v1_or_downgrading_v2() {
    const STACK_SIZE: usize = 8 * 1024 * 1024;
    std::thread::Builder::new()
        .name("mrf-lifecycle-reader".to_owned())
        .stack_size(STACK_SIZE)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(STACK_SIZE)
                .enable_all()
                .build()
                .expect("reader fixture runtime")
                .block_on(async {
                    for version in [1, 2] {
                        check_reader_restart(version).await;
                    }
                });
        })
        .expect("reader fixture thread")
        .join()
        .expect("reader fixture must finish");
}

async fn check_reader_restart(version: u8) {
    let env = rustfs_test_utils::TestECStoreEnv::builder()
        .prefix("rustfs_mrf_lifecycle_reader")
        .build()
        .await;
    let config = MrfConsumerConfig::default();
    let owner = Uuid::new_v4();
    let bucket = "reader-unknown-bucket";
    let state = if version == 2 {
        unknown_generation_payload()["records"][0]["state"].clone()
    } else {
        json!({"state": "active"})
    };
    let mut payload = Vec::new();
    let mut records = Vec::new();
    for index in 0..3 {
        let mut original = intent(bucket, &format!("retained-{index}"), 0);
        original.kind = MrfKind::PartialWrite;
        original.scope = Some(rustfs_common::mrf_channel::MrfScope {
            pool_index: 0,
            set_index: 0,
        });
        payload.extend(encoded_payload(&original));
        records.push(json!({
            "intent_digest": intent_digest(&original).expect("fixture identity"),
            "responsibility_id": Uuid::new_v4(),
            "source_bucket_incarnation_id": null,
            "last_operator_acceptance": null,
            "state": state
        }));
    }
    let lifecycle = lifecycle_reader_frame(
        &json!({
            "format_version": version,
            "checkpoint_owner": owner,
            "checkpoint_sequence": 1,
            "records": records
        }),
        version,
    );
    snapshot::publish_committed_snapshot_with_companion(
        &journal_disks().await,
        owner,
        1,
        &payload,
        config.journal_max_bytes,
        Some((&MRF_LIFECYCLE_PATHS, &lifecycle, config.journal_max_bytes * 4)),
    )
    .await
    .expect("publish the reader fixture on real disks");
    let storage: Arc<dyn HealStorageAPI> = Arc::new(ECStoreHealStorage::new(env.ecstore.clone()));
    let manager = Arc::new(HealManager::new_without_root_recovery_for_test(storage, None));
    let ids: HashSet<_> = records
        .iter()
        .map(|record| serde_json::from_value::<Uuid>(record["responsibility_id"].clone()).expect("fixture id"))
        .collect();
    let (incoming, receiver) = mpsc::channel(4);
    let (_durable, durable_receiver) = mpsc::channel(4);
    let (commands, lifecycle_receiver) = mpsc::channel(4);
    let consumer = tokio::spawn(run_mrf_consumer(manager.clone(), receiver, durable_receiver, lifecycle_receiver));
    let (response, listed) = tokio::sync::oneshot::channel();
    commands
        .send(MrfLifecycleCommand::List {
            after: None,
            limit: 3,
            response,
        })
        .await
        .expect("query the real consumer after its startup replay");
    let listing = tokio::time::timeout(Duration::from_secs(5), listed)
        .await
        .expect("consumer listing deadline")
        .expect("consumer listing response")
        .expect("V1 and V2 owners must be readable");
    let successor_owner = listing.checkpoint_owner.expect("the real consumer must publish a successor");
    let successor_sequence = listing.checkpoint_sequence.expect("successor sequence");
    assert_ne!(successor_owner, owner);
    assert!(successor_sequence > 1);
    assert_eq!(
        listing
            .responsibilities
            .iter()
            .map(|row| row.responsibility_id)
            .collect::<HashSet<_>>(),
        ids
    );
    drop(incoming);
    tokio::time::timeout(Duration::from_secs(5), consumer)
        .await
        .expect("consumer shutdown deadline")
        .expect("consumer shutdown");
    let published = read_mrf_lifecycle_checkpoint(
        successor_owner,
        successor_sequence,
        config.journal_max_bytes,
        config.journal_max_bytes * 4,
    )
    .await
    .expect("read the actual consumer's successor")
    .expect("retained lifecycle companion");
    assert_eq!(published.format_version, version);
    assert_eq!(published.records.len(), 3);
    let mut last_records = None;
    for restart in 0..3 {
        let mut queue = MrfQueue::new(config.queue_capacity, config.journal_max_bytes);
        let replay = replay_into(&manager, &mut queue, &mut None).await;
        assert!(replay.source_readable);
        assert_eq!(replay.partial_writes.len(), 3);
        assert_eq!(replay.lifecycle_format_version, version);
        let mut runtime = MrfRuntime {
            queue,
            partial_writes: PartialWrites::default(),
            config: config.clone(),
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
        runtime.adopt_replayed_partial_writes(replay.partial_writes);
        if let Some(previous) = &last_records {
            let current: HashMap<_, _> = runtime
                .partial_writes
                .checkpoint_records(intent_digest)
                .into_iter()
                .map(|record| (record.responsibility_id, record))
                .collect();
            assert_eq!(&current, previous, "replay must preserve every state and audit field");
        }
        assert!(runtime.flush().await);
        runtime.dispatch(&manager).await;
        let mut cursor = None;
        let mut visible = HashSet::new();
        loop {
            let page = runtime.list_unverified_legacy(cursor, 1).expect("list retained records");
            assert_eq!(page.legacy_generation_unknown_count, 3);
            assert_eq!(page.responsibilities.len(), 1);
            let row = &page.responsibilities[0];
            assert!(visible.insert(row.responsibility_id));
            assert_eq!(row.source_bucket_incarnation_id, None);
            assert_eq!((row.pool_index, row.set_index), (Some(0), Some(0)));
            assert_eq!(row.version_id, Some(Uuid::from_bytes([7; 16]).to_string()));
            if restart < 2 {
                assert_eq!(row.bucket_incarnation_id, None);
                assert_eq!(
                    row.status,
                    if version == 2 {
                        "source_and_current_generation_unknown"
                    } else {
                        "legacy_generation_unobserved"
                    }
                );
                assert_eq!(row.held_since_ms, (version == 2).then_some(7));
                assert!(row.next_identity_probe_at_ms.is_some());
            } else {
                assert_eq!(row.status, "legacy_generation_unknown");
                assert!(row.bucket_incarnation_id.is_some());
                if version == 2 {
                    assert_eq!(row.held_since_ms, Some(7));
                }
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(visible, ids);
        assert_eq!(manager.get_queue_length().await, 0, "unknown source never authorizes object healing");
        if restart == 1 {
            env.make_bucket(bucket, false).await;
            for id in &ids {
                runtime
                    .refresh_legacy_generation(&manager, *id, None)
                    .await
                    .expect("record current identity without rebinding source");
            }
        }
        let (checkpoint_owner, sequence) = runtime.runtime_checkpoint.expect("successor checkpoint");
        let stored =
            read_mrf_lifecycle_checkpoint(checkpoint_owner, sequence, config.journal_max_bytes, config.journal_max_bytes * 4)
                .await
                .expect("read actual successor files")
                .expect("retained companion");
        assert_eq!(
            stored.format_version, version,
            "owner transfer must preserve the format even after observing a bucket"
        );
        last_records = Some(
            stored
                .records
                .into_iter()
                .map(|record| (record.responsibility_id, record))
                .collect::<HashMap<_, _>>(),
        );
    }
}
