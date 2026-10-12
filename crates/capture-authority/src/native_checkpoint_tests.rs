use super::*;
use std::collections::{BTreeMap, BTreeSet};

fn identity() -> NodeIdentity {
    NodeIdentity {
        cluster_id: [7; 16],
        node_id: 0,
    }
}

fn binding() -> CaptureBinding {
    CaptureBinding {
        revision: 9,
        binding_id: [8; 16],
        target_id: [9; 16],
    }
}

fn id(index: u64) -> LogId {
    LogId {
        term: 8,
        leader_node: 0,
        index,
    }
}

fn command(operation: u8) -> DecideCreated {
    DecideCreated {
        operation_id: [operation; 16],
        object: ObjectIdentity {
            bucket_incarnation: [2; 16],
            key: b"checkpoint-object".to_vec(),
        },
        expected_head: None,
        prepared: PreparedIdentity {
            version_id: [3; 16],
            preparation_id: [4; 16],
            content_digest: [5; 32],
            content_length: 4097,
        },
        binding_revision: 9,
    }
}

fn apply(store: &mut Store, index: u64, command: DecideCreated) -> CaptureResponse {
    let entry = CaptureRaftEntry {
        log_id: id(index).into(),
        payload: openraft::EntryPayload::Normal(CaptureCommand::try_created(command).expect("canonical command")),
    };
    store.append_native(&[entry]).expect("actual durable append");
    store.save_committed(id(index)).expect("actual durable commit");
    store.apply_next_native(id(index)).expect("actual native apply")
}

fn fixture() -> (tempfile::TempDir, Store, DecideCreated, CreatedOperationProvenance) {
    let dir = tempfile::tempdir().expect("actual native database directory");
    let mut store = Store::initialize(dir.path().join("db"), identity()).expect("initialize actual redb");
    store.pin_capture_binding(binding()).expect("bind actual store");
    store.enable_native_empty().expect("native schema4");
    store.promote_created_provenance().expect("native schema6 provenance");
    let membership = CaptureMembership::new(
        Some(id(0)),
        vec![BTreeSet::from([0])],
        BTreeMap::from([(0, CaptureNode::new("http://127.0.0.1:7000".into()).expect("native member"))]),
    )
    .expect("full native membership");
    let stored = openraft::StoredMembership::try_from(&membership).expect("OpenRaft membership");
    store
        .append_native(&[CaptureRaftEntry {
            log_id: id(0).into(),
            payload: openraft::EntryPayload::Membership(stored.membership().clone()),
        }])
        .expect("append actual membership");
    store.save_committed(id(0)).expect("commit membership");
    store.apply_next_native(id(0)).expect("apply membership");
    let negative = command(11);
    let mut original = negative.clone();
    original.expected_head = Some([12; 16]);
    apply(&mut store, 1, original.clone());
    let first = store
        .created_operation_provenance(original.operation_id)
        .expect("read first negative")
        .expect("canonical negative operation");
    assert_eq!(first.first_decision_log_id, id(1), "CHECKPOINT_FIRST_NEGATIVE_BOUND");
    assert!(
        matches!(first.operation.result, CreatedResult::HeadMismatch { .. }),
        "CHECKPOINT_FIRST_NEGATIVE_BOUND"
    );
    apply(&mut store, 2, command(1));
    (dir, store, original, first)
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("checkpoint runtime")
}

fn schema(store: &Store) -> u64 {
    let tx = store.database.begin_read().expect("read actual schema");
    let table = tx.open_table(META).expect("schema table");
    number(table.get("schema").expect("schema read").expect("schema exists").value()).expect("schema number")
}

fn assert_negative(store: &Store, original: &DecideCreated, first: &CreatedOperationProvenance) {
    assert_eq!(
        store
            .created_operation_provenance(original.operation_id)
            .expect("canonical read"),
        Some(first.clone()),
        "CHECKPOINT_FIRST_NEGATIVE_BOUND: preserve original bytes/result/full first ID"
    );
}

#[test]
fn native_checkpoint_negative_retry_and_purge_reopen() {
    let (dir, store, original, first) = fixture();
    assert_eq!(schema(&store), 6);
    let writer = NativeLogWriter::new(store);
    let receipt = runtime()
        .block_on(writer.checkpoint_local(CheckpointLimits::default()))
        .expect("publish local checkpoint");
    assert_eq!(receipt.cut(), id(2), "CHECKPOINT_BASELINE_BOUND");
    assert_eq!(receipt.generation(), 1);
    {
        let mut store = writer.store.lock().expect("actual writer store");
        assert_eq!(schema(&store), 7);
        assert_negative(&store, &original, &first);
        apply(&mut store, 3, original.clone());
        assert_negative(&store, &original, &first);
        let mut conflict = original.clone();
        conflict.binding_revision += 1;
        apply(&mut store, 4, conflict);
        assert_eq!(
            store
                .created_apply_result(4)
                .expect("conflicting response")
                .expect("response")
                .result,
            CreatedResult::OperationConflict,
            "CHECKPOINT_RETRY_BOUND"
        );
        assert_negative(&store, &original, &first);
    }
    runtime()
        .block_on(writer.purge_checkpoint(&receipt))
        .expect("purge exact durable baseline");
    drop(writer);
    let reopened = Store::open(dir.path().join("db"), identity()).expect("reopen purged baseline plus suffix");
    assert_negative(&reopened, &original, &first);
    assert_eq!(reopened.last_applied().expect("suffix applied"), Some(id(4)), "CHECKPOINT_SUFFIX_BOUND");
    assert!(reopened.read_entry(2).expect("purged cut").is_none());
    assert!(reopened.read_entry(3).expect("retained suffix").is_some());
}

#[test]
fn native_checkpoint_capture_keeps_concurrent_suffix() {
    let (dir, store, original, first) = fixture();
    let writer = NativeLogWriter::new(store);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let _hook = native_checkpoint::after_local_capture(&writer, move || {
        entered_tx.send(()).expect("capture reached");
        release_rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("release owned capture");
    });
    let worker = writer.clone();
    let thread = std::thread::spawn(move || runtime().block_on(worker.checkpoint_local(CheckpointLimits::default())));
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("actual capture boundary");
    {
        let mut store = writer.store.lock().expect("capture releases live write owner");
        apply(&mut store, 3, original.clone());
    }
    release_tx.send(()).expect("release capture");
    let receipt = thread.join().expect("checkpoint worker").expect("concurrent checkpoint");
    assert_eq!(receipt.cut(), id(2), "CHECKPOINT_SAME_CUT_BOUND");
    runtime()
        .block_on(writer.purge_checkpoint(&receipt))
        .expect("purge captured prefix");
    drop(writer);
    let reopened = Store::open(dir.path().join("db"), identity()).expect("reopen concurrent suffix");
    assert_eq!(reopened.last_applied().expect("actual suffix"), Some(id(3)), "CHECKPOINT_SUFFIX_BOUND");
    assert_negative(&reopened, &original, &first);
}

#[test]
fn native_checkpoint_staging_corruption_preserves_live_generation() {
    let (dir, store, original, first) = fixture();
    let writer = NativeLogWriter::new(store);
    let before = runtime()
        .block_on(writer.checkpoint_local(CheckpointLimits::default()))
        .expect("initial baseline");
    let _hook = native_checkpoint::after_local_stage(&writer, |database| {
        let tx = database.begin_write().expect("actual isolated stage mutation");
        tx.open_table(checkpoint_baseline::OPERATIONS)
            .expect("stage operations")
            .retain(|_, _| false)
            .expect("damage cross-record relationships");
        tx.commit().expect("persist actual damaged stage");
    });
    assert!(
        runtime()
            .block_on(writer.checkpoint_local(CheckpointLimits::default()))
            .is_err(),
        "CHECKPOINT_FAILURE_ATOMIC"
    );
    let store = writer.store.lock().expect("unchanged live store");
    let tx = store.database.begin_read().expect("published generation");
    let meta = tx.open_table(META).expect("published meta");
    assert_eq!(
        checkpoint_baseline::read_metadata(&meta)
            .expect("valid old metadata")
            .expect("old baseline")
            .generation,
        before.generation(),
        "CHECKPOINT_FAILURE_ATOMIC"
    );
    assert_negative(&store, &original, &first);
    drop(meta);
    drop(tx);
    drop(store);
    drop(writer);
    let reopened = Store::open(dir.path().join("db"), identity()).expect("reopen after failed staging validation");
    assert_negative(&reopened, &original, &first);
}

#[test]
fn native_checkpoint_budget_failure_preserves_schema_and_history() {
    for staging in [false, true] {
        let (dir, mut store, original, first) = fixture();
        if staging {
            for index in 0u64..256 {
                let mut large = command(31);
                large.operation_id[..8].copy_from_slice(&index.to_be_bytes());
                large.prepared.version_id[..8].copy_from_slice(&index.to_be_bytes());
                large.object.key = vec![b'x'; MAX_OBJECT_KEY_LENGTH];
                large.object.key[..8].copy_from_slice(&index.to_be_bytes());
                apply(&mut store, index + 3, large);
            }
        }
        let writer = NativeLogWriter::new(store);
        let mut limits = CheckpointLimits::default();
        if staging {
            limits.staging_bytes = 4 * 1024 * 1024;
        } else {
            limits.snapshot.total_bytes = 64;
        }
        let result = runtime().block_on(writer.checkpoint_local(limits));
        if staging {
            assert!(
                matches!(&result, Err(CheckpointError::Store(StoreError::ResourceLimit))),
                "CHECKPOINT_RESOURCE_BOUND: actual 4MiB physical staging limit: {result:?}"
            );
        } else {
            assert!(
                result.is_err(),
                "CHECKPOINT_RESOURCE_BOUND: actual bounded output must refuse: {result:?}"
            );
        }
        {
            let store = writer.store.lock().expect("live after exhausted budget");
            assert_eq!(schema(&store), 6, "CHECKPOINT_FAILURE_ATOMIC");
            assert_negative(&store, &original, &first);
            assert!(store.read_entry(0).expect("prefix intact").is_some());
        }
        drop(writer);
        assert_negative(
            &Store::open(dir.path().join("db"), identity()).expect("reopen budget failure"),
            &original,
            &first,
        );
    }
}

#[test]
fn native_checkpoint_cancelled_capture_cannot_publish_or_purge() {
    let (dir, store, original, first) = fixture();
    let writer = NativeLogWriter::new(store);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let _hook = native_checkpoint::after_local_capture(&writer, move || {
        entered_tx.send(()).expect("capture cancellation boundary");
        release_rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("release pending actual capture");
    });
    let rt = runtime();
    let worker = writer.clone();
    rt.block_on(async {
        let task = tokio::spawn(async move { worker.checkpoint_local(CheckpointLimits::default()).await });
        tokio::task::spawn_blocking(move || entered_rx.recv_timeout(std::time::Duration::from_secs(30)))
            .await
            .expect("capture observer")
            .expect("actual capture owned transaction");
        task.abort();
        assert!(task.await.expect_err("cancelled waiter").is_cancelled(), "CHECKPOINT_CANCEL_BOUND");
        release_tx.send(()).expect("release actual worker after waiter cancellation");
        // A fresh checkpoint waits for the same production admission; completion
        // establishes that the cancelled capture worker released its real resources.
        let receipt = writer
            .checkpoint_local(CheckpointLimits::default())
            .await
            .expect("capture after cancellation");
        assert_eq!(receipt.generation(), 1, "CHECKPOINT_CANCEL_BOUND: abandoned capture never published");
    });
    drop(rt);
    drop(writer);
    let reopened = Store::open(dir.path().join("db"), identity()).expect("reopen after cancellation and successful next capture");
    assert_negative(&reopened, &original, &first);
    assert!(
        reopened.read_entry(0).expect("cancellation cannot purge").is_some(),
        "CHECKPOINT_CANCEL_BOUND"
    );
}

#[test]
fn native_checkpoint_stale_receipt_cannot_remove_history() {
    let (_dir, store, original, first) = fixture();
    let writer = NativeLogWriter::new(store);
    let rt = runtime();
    let old = rt
        .block_on(writer.checkpoint_local(CheckpointLimits::default()))
        .expect("first generation");
    let new = rt
        .block_on(writer.checkpoint_local(CheckpointLimits::default()))
        .expect("next generation");
    assert!(new.generation() > old.generation());
    assert!(
        matches!(
            rt.block_on(writer.purge_checkpoint(&old)),
            Err(NativeWriteError::Store(StoreError::StaleCheckpoint))
        ),
        "CHECKPOINT_PURGE_BOUND"
    );
    let store = writer.store.lock().expect("history after stale purge");
    assert_negative(&store, &original, &first);
    assert!(store.read_entry(1).expect("first decision retained").is_some());
    assert!(store.created_apply_result(1).expect("typed result retained").is_some());
}

#[test]
fn native_checkpoint_reopen_rejects_conflicting_cut_and_suffix_gap() {
    for corruption in 0..4 {
        let gap = corruption == 1;
        let (dir, store, original, _) = fixture();
        let writer = NativeLogWriter::new(store);
        let rt = runtime();
        let receipt = rt
            .block_on(writer.checkpoint_local(CheckpointLimits::default()))
            .expect("valid checkpoint");
        if gap {
            {
                let mut store = writer.store.lock().expect("suffix owner");
                apply(&mut store, 3, original.clone());
                apply(&mut store, 4, original.clone());
            }
            rt.block_on(writer.purge_checkpoint(&receipt))
                .expect("purge before gap corruption");
            {
                let store = writer.store.lock().expect("actual purged metadata owner");
                let tx = store.database.begin_write().expect("actual CPUR mutation");
                let mut meta = tx.open_table(META).expect("actual CPUR table");
                let mut conflict = receipt.cut();
                conflict.term += 1;
                conflict.leader_node += 1;
                let encoded = checkpoint_baseline::encode_purged(conflict);
                meta.insert(checkpoint_baseline::PURGED_KEY, encoded.as_slice())
                    .expect("persist conflicting full purge ID");
                drop(meta);
                tx.commit().expect("durable conflicting CPUR");
                let tx = store.database.begin_read().expect("actual conflicting CPUR read");
                let meta = tx.open_table(META).expect("actual persisted CPUR");
                assert!(
                    matches!(checkpoint_baseline::read_purged(&meta), Err(StoreError::CorruptRecord)),
                    "CHECKPOINT_FULL_CUT_BOUND: same index with different term/leader is corruption"
                );
                drop(meta);
                drop(tx);
                let tx = store
                    .database
                    .begin_write()
                    .expect("restore valid purge boundary for gap scenario");
                let encoded = checkpoint_baseline::encode_purged(receipt.cut());
                tx.open_table(META)
                    .expect("actual purge meta")
                    .insert(checkpoint_baseline::PURGED_KEY, encoded.as_slice())
                    .expect("restore exact cut");
                tx.commit().expect("persist exact purge boundary");
            }
        }
        if corruption >= 2 {
            let mut store = writer.store.lock().expect("actual suffix before tail corruption");
            apply(&mut store, 3, original.clone());
        }
        {
            let store = writer.store.lock().expect("actual corruption owner");
            let tx = store.database.begin_write().expect("actual redb mutation");
            let mut logs = tx.open_table(LOGS).expect("actual log table");
            if gap {
                logs.remove(3).expect("remove required suffix entry");
            } else if corruption >= 2 {
                let mut tail = id(3);
                if corruption == 2 {
                    tail.index += 1;
                } else {
                    tail.term += 1;
                    tail.leader_node += 1;
                }
                let encoded = encode_id(tail);
                tx.open_table(META)
                    .expect("actual metadata")
                    .insert("tail", encoded.as_slice())
                    .expect("corrupt durable tail");
            } else {
                let mut entry = store.read_entry(2).expect("retained cut").expect("cut entry");
                entry.id.term += 1;
                entry.id.leader_node += 1;
                let encoded = encode_log(&entry).expect("valid conflicting full ID encoding");
                logs.insert(2, encoded.as_slice()).expect("replace exact cut identity");
            }
            drop(logs);
            tx.commit().expect("persist actual inconsistent recovery input");
        }
        assert!(
            rt.block_on(writer.purge_checkpoint(&receipt)).is_err(),
            "CHECKPOINT_PURGE_BOUND: invalid recovery cannot authorize cleanup"
        );
        if !gap {
            let store = writer.store.lock().expect("refused purge retains original evidence");
            assert!(store.read_entry(1).expect("original log retained").is_some(), "CHECKPOINT_PURGE_BOUND");
            assert!(
                store.created_apply_result(1).expect("original response retained").is_some(),
                "CHECKPOINT_PURGE_BOUND"
            );
        }
        drop(writer);
        drop(rt);
        assert!(Store::open(dir.path().join("db"), identity()).is_err(), "CHECKPOINT_RECOVERY_BOUND");
    }
}

#[test]
fn native_checkpoint_publication_fault_selects_complete_generation() {
    for (sync, after) in [(1, false), (1, true), (2, false), (2, true)] {
        let (dir, store, original, first) = fixture();
        let vote = VoteRecord {
            term: 9,
            voted_node: 0,
            committed: true,
        };
        let mut store = store;
        store.save_vote(vote).expect("local hard state before checkpoint");
        drop(store);
        let path = dir.path().join("db");
        let backend = tests::CrashBackend::new(&path);
        let store = Store::from_database(backend.database(), identity()).expect("existing simulated durability backend");
        let writer = NativeLogWriter::new(store);
        let (entered, release) = backend.pause_sync(sync, after);
        let worker = writer.clone();
        let task = std::thread::spawn(move || runtime().block_on(worker.checkpoint_local(CheckpointLimits::default())));
        entered.wait();
        backend.cut();
        release.wait();
        let _unknown = task.join().expect("interrupted publication worker");
        drop(writer);
        let reopened = Store::open(&path, identity()).expect("recover complete generation after simulated sync cut");
        assert!(matches!(schema(&reopened), 6 | 7), "CHECKPOINT_CRASH_ATOMIC");
        assert_negative(&reopened, &original, &first);
        assert_eq!(
            reopened.read_vote().expect("preserved local vote"),
            Some(vote),
            "CHECKPOINT_LOCAL_STATE_BOUND"
        );
        assert_eq!(reopened.last_applied().expect("complete applied position"), Some(id(2)));
    }
}

#[test]
fn native_checkpoint_recovery_budget_can_be_raised_without_changing_baseline() {
    use redb::ReadableTableMetadata;
    let (dir, store, original, first) = fixture();
    let mut limits = CheckpointLimits::default();
    let tx = store.database.begin_read().expect("measure real baseline rows");
    limits.snapshot.records = [
        application::HEADS,
        application::VERSIONS,
        application::EVENTS,
        application::OPERATIONS,
    ]
    .into_iter()
    .map(|definition| {
        tx.open_table(definition)
            .expect("actual baseline table")
            .len()
            .expect("actual row count")
    })
    .sum();
    drop(tx);
    let writer = NativeLogWriter::new(store);
    let receipt = runtime()
        .block_on(writer.checkpoint_local(limits))
        .expect("capture exact bounded baseline");
    {
        let mut store = writer.store.lock().expect("actual suffix writer");
        let mut suffix = command(21);
        suffix.object.key = b"suffix-new-key".to_vec();
        suffix.prepared.version_id = [22; 16];
        apply(&mut store, 3, suffix);
    }
    drop(writer);
    let path = dir.path().join("db");
    assert!(
        matches!(Store::open(&path, identity()), Err(StoreError::ResourceLimit)),
        "CHECKPOINT_RESOURCE_BOUND"
    );
    let mut raised = limits;
    raised.snapshot.records += 32;
    let reopened =
        Store::open_with_checkpoint_limits(&path, identity(), raised).expect("explicitly raise bounded recovery budget");
    assert_negative(&reopened, &original, &first);
    assert_eq!(reopened.last_applied().expect("retained suffix position"), Some(id(3)));
    let tx = reopened.database.begin_read().expect("unchanged baseline metadata");
    let meta = tx.open_table(META).expect("actual metadata");
    let baseline = checkpoint_baseline::read_metadata(&meta)
        .expect("baseline decode")
        .expect("baseline selected");
    assert_eq!(baseline.generation, receipt.generation());
    assert_eq!(
        baseline.limits.snapshot.records, limits.snapshot.records,
        "CHECKPOINT_RESOURCE_BOUND: override never rewrites evidence"
    );
}

#[test]
fn native_checkpoint_interrupted_purge_reopens_and_retries_idempotently() {
    for (sync, after) in [(1, false), (1, true), (2, false), (2, true)] {
        let (dir, store, original, first) = fixture();
        let writer = NativeLogWriter::new(store);
        let receipt = runtime()
            .block_on(writer.checkpoint_local(CheckpointLimits::default()))
            .expect("durable baseline before purge");
        drop(writer);
        let path = dir.path().join("db");
        let backend = tests::CrashBackend::new(&path);
        let store = Store::from_database(backend.database(), identity()).expect("simulated purge durability backend");
        let writer = NativeLogWriter::new(store);
        let (entered, release) = backend.pause_sync(sync, after);
        let worker = writer.clone();
        let task = std::thread::spawn(move || runtime().block_on(worker.purge_checkpoint(&receipt)));
        entered.wait();
        backend.cut();
        release.wait();
        let _unknown = task.join().expect("interrupted purge worker");
        drop(writer);
        let reopened = Store::open(&path, identity()).expect("baseline survives simulated interrupted purge");
        assert_negative(&reopened, &original, &first);
        let retained = (0..=2)
            .map(|index| reopened.read_entry(index).expect("atomic purge log state").is_some())
            .collect::<Vec<_>>();
        assert!(
            retained.iter().all(|present| *present) || retained.iter().all(|present| !*present),
            "CHECKPOINT_PURGE_ATOMIC: complete old prefix or complete new purge"
        );
        let writer = NativeLogWriter::new(reopened);
        let rt = runtime();
        rt.block_on(writer.purge_checkpoint(&receipt)).expect("resume exact purge");
        rt.block_on(writer.purge_checkpoint(&receipt))
            .expect("repeat exact purge idempotently");
        drop(writer);
        let reopened = Store::open(&path, identity()).expect("reopen resumed purge");
        assert_negative(&reopened, &original, &first);
        assert!(reopened.read_entry(2).expect("purged cut absent").is_none(), "CHECKPOINT_PURGE_ATOMIC");
    }
}

#[test]
fn native_checkpoint_ack_and_purge_ack_survive_simulated_power_cut() {
    let (dir, store, original, first) = fixture();
    drop(store);
    let path = dir.path().join("db");
    let backend = tests::CrashBackend::new(&path);
    let writer =
        NativeLogWriter::new(Store::from_database(backend.database(), identity()).expect("actual checkpoint fault backend"));
    let receipt = runtime()
        .block_on(writer.checkpoint_local(CheckpointLimits::default()))
        .expect("successful checkpoint ACK");
    backend.cut();
    drop(writer);
    let reopened = Store::open(&path, identity()).expect("ACK checkpoint survives simulated power cut");
    assert_eq!(schema(&reopened), 7, "CHECKPOINT_ACK_DURABLE");
    assert_negative(&reopened, &original, &first);
    let backend = tests::CrashBackend::new(&path);
    drop(reopened);
    let writer = NativeLogWriter::new(Store::from_database(backend.database(), identity()).expect("actual purge fault backend"));
    runtime()
        .block_on(writer.purge_checkpoint(&receipt))
        .expect("successful purge ACK");
    backend.cut();
    drop(writer);
    let reopened = Store::open(&path, identity()).expect("ACK purge survives simulated power cut");
    assert_negative(&reopened, &original, &first);
    assert!(reopened.read_entry(2).expect("ACK purge cut absent").is_none(), "CHECKPOINT_ACK_DURABLE");
}
