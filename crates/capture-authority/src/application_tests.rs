use super::*;
use crate::application::{APPLY_RESULTS, EVENTS, OPERATIONS, VERSIONS};
use crate::tests::CrashBackend;
use redb::ReadableTableMetadata;
use std::{
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
    sync::mpsc,
    time::Duration,
};
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
fn command() -> DecideCreated {
    DecideCreated {
        operation_id: [1; 16],
        object: ObjectIdentity {
            bucket_incarnation: [2; 16],
            key: vec![0, 255, 3],
        },
        expected_head: None,
        prepared: PreparedIdentity {
            version_id: [3; 16],
            preparation_id: [4; 16],
            content_digest: [5; 32],
            content_length: 123,
        },
        binding_revision: 9,
    }
}
fn id(index: u64) -> LogId {
    LogId {
        term: 8,
        leader_node: 0,
        index,
    }
}
fn append(store: &mut Store, index: u64, c: &DecideCreated) {
    store
        .append(&[LogEntry {
            id: id(index),
            payload: c.encode().expect("encode command"),
        }])
        .expect("append command");
}
fn apply(store: &mut Store, index: u64, c: &DecideCreated) -> CreatedResult {
    append(store, index, c);
    store.save_committed(id(index)).expect("commit log");
    store.apply_next_created(id(index)).expect("apply command")
}
fn success(c: &DecideCreated, index: u64) -> CreatedResult {
    CreatedResult::Created {
        version_id: c.prepared.version_id,
        event_id: c.operation_id,
        decision_log_id: id(index),
    }
}
fn complete(store: &Store) {
    let c = command();
    assert_eq!(store.capture_binding().expect("binding"), Some(binding()));
    assert_eq!(
        store.created_head(&c.object).expect("head"),
        Some(c.prepared.version_id),
        "U11 acknowledged head lost"
    );
    assert_eq!(
        store.created_version(&c.object, c.prepared.version_id).expect("version"),
        Some(CreatedVersion {
            prepared: c.prepared.clone(),
            decision_log_id: id(0),
            binding_revision: 9
        })
    );
    assert_eq!(
        store.created_event(c.operation_id).expect("event"),
        Some(CreatedEvent {
            object: c.object.clone(),
            prepared: c.prepared.clone(),
            decision_log_id: id(0),
            binding: binding()
        }),
        "U11 complete event missing"
    );
    assert_eq!(
        store.created_operation(c.operation_id).expect("operation"),
        Some(CreatedOperation {
            canonical_command: c.encode().expect("encode"),
            result: success(&c, 0)
        })
    );
    assert_eq!(
        store.created_apply_result(0).expect("result"),
        Some(CreatedApplyResult {
            log_id: id(0),
            result: success(&c, 0)
        })
    );
    assert_eq!(store.last_applied().expect("last applied"), Some(id(0)));
}
fn fixture() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut store = Store::initialize(dir.path().join("db"), identity()).expect("initialize");
    store.pin_capture_binding(binding()).expect("pin");
    (dir, store)
}
fn reopen(path: &std::path::Path) -> Store {
    let result = Store::open(path, identity());
    assert!(result.is_ok(), "U11 complete reopen: {:?}", result.as_ref().err());
    result.expect("asserted successful reopen")
}
#[test]
fn created_codec_is_canonical_and_strict() {
    let c = command();
    let b = c.encode().expect("encode");
    let mut golden = b"CRTD\x01".to_vec();
    golden.extend([1; 16]);
    golden.extend([2; 16]);
    golden.extend(3u32.to_be_bytes());
    golden.extend([0, 255, 3]);
    golden.push(0);
    golden.extend([3; 16]);
    golden.extend([4; 16]);
    golden.extend([5; 32]);
    golden.extend(123u64.to_be_bytes());
    golden.extend(9u64.to_be_bytes());
    assert_eq!(b, golden);
    assert_eq!(DecideCreated::decode(&b).expect("decode"), c);
    let mut variants = Vec::new();
    let mut v = c.clone();
    v.operation_id = [6; 16];
    variants.push(v);
    let mut v = c.clone();
    v.object.bucket_incarnation = [6; 16];
    variants.push(v);
    let mut v = c.clone();
    v.object.key.push(4);
    variants.push(v);
    let mut v = c.clone();
    v.expected_head = Some([6; 16]);
    variants.push(v);
    let mut v = c.clone();
    v.prepared.version_id = [6; 16];
    variants.push(v);
    let mut v = c.clone();
    v.prepared.preparation_id = [6; 16];
    variants.push(v);
    let mut v = c.clone();
    v.prepared.content_digest = [6; 32];
    variants.push(v);
    let mut v = c.clone();
    v.prepared.content_length += 1;
    variants.push(v);
    let mut v = c.clone();
    v.binding_revision += 1;
    variants.push(v);
    for v in variants {
        assert_ne!(v.encode().expect("variant"), b)
    }
    for n in 0..b.len() {
        assert!(DecideCreated::decode(&b[..n]).is_err())
    }
    let mut bad = b.clone();
    bad[4] = 2;
    assert!(DecideCreated::decode(&bad).is_err());
    let mut bad = b.clone();
    bad[44] = 2;
    assert!(DecideCreated::decode(&bad).is_err());
    let mut bad = b.clone();
    bad.push(0);
    assert!(DecideCreated::decode(&bad).is_err());
    let mut bad = b;
    bad[37..41].copy_from_slice(&4097u32.to_be_bytes());
    assert!(DecideCreated::decode(&bad).is_err());
    let mut bad = c.clone();
    bad.operation_id = [0; 16];
    assert!(bad.encode().is_err());
    bad = c;
    bad.object.key = vec![0; MAX_OBJECT_KEY_LENGTH + 1];
    assert!(bad.encode().is_err());
}
#[test]
fn created_committed_order_and_identity_are_enforced() {
    let (_dir, mut s) = fixture();
    let c = command();
    append(&mut s, 0, &c);
    append(&mut s, 1, &c);
    assert!(s.apply_next_created(id(0)).is_err());
    assert_eq!(s.last_applied().expect("no apply"), None);
    s.save_committed(id(1)).expect("committed");
    assert!(matches!(s.apply_next_created(id(1)), Err(StoreError::InvalidApplySequence)));
    let mut wrong = id(0);
    wrong.term += 1;
    assert!(s.apply_next_created(wrong).is_err());
    wrong = id(0);
    wrong.leader_node += 1;
    assert!(s.apply_next_created(wrong).is_err());
    assert_eq!(s.apply_next_created(id(0)).expect("first"), success(&c, 0));
    complete(&s);
    assert_eq!(s.apply_next_created(id(0)).expect("retry"), success(&c, 0));
    complete(&s);
    assert!(s.apply_next_created(wrong).is_err());
    assert_eq!(s.apply_next_created(id(1)).expect("duplicate log"), success(&c, 0));
    assert_eq!(s.last_applied().expect("last"), Some(id(1)));
    assert_eq!(
        s.created_apply_result(1).expect("result"),
        Some(CreatedApplyResult {
            log_id: id(1),
            result: success(&c, 0)
        })
    );
    let tx = s.database.begin_read().expect("read");
    assert_eq!(tx.open_table(EVENTS).expect("events").len().expect("len"), 1);
}
#[test]
fn created_head_version_event_and_dedup_reopen_together() {
    let (dir, mut s) = fixture();
    let c = command();
    assert_eq!(apply(&mut s, 0, &c), success(&c, 0));
    drop(s);
    let mut s = reopen(&dir.path().join("db"));
    complete(&s);
    assert_eq!(apply(&mut s, 1, &c), success(&c, 0));
    let mut changed = c.clone();
    changed.prepared.content_digest = [6; 32];
    assert_eq!(
        apply(&mut s, 2, &changed),
        CreatedResult::OperationConflict,
        "U11 full canonical operation conflict"
    );
    assert_eq!(
        s.created_operation(c.operation_id)
            .expect("original")
            .expect("operation")
            .result,
        success(&c, 0)
    );
    let mut stale = c.clone();
    stale.operation_id = [6; 16];
    stale.prepared.version_id = [6; 16];
    assert_eq!(
        apply(&mut s, 3, &stale),
        CreatedResult::HeadMismatch {
            actual_head: Some(c.prepared.version_id)
        }
    );
    let mut reuse = c.clone();
    reuse.operation_id = [7; 16];
    reuse.expected_head = Some(c.prepared.version_id);
    assert_eq!(apply(&mut s, 4, &reuse), CreatedResult::VersionConflict);
    let mut replacement = stale.clone();
    replacement.operation_id = [10; 16];
    replacement.expected_head = Some(c.prepared.version_id);
    assert_eq!(apply(&mut s, 5, &replacement), success(&replacement, 5));
    assert_eq!(s.created_head(&c.object).expect("head"), Some(replacement.prepared.version_id));
    assert_eq!(
        s.created_version(&c.object, c.prepared.version_id)
            .expect("old version")
            .expect("version")
            .decision_log_id,
        id(0)
    );
    assert!(s.created_event(c.operation_id).expect("old event").is_some());
    assert!(s.created_event(replacement.operation_id).expect("new event").is_some());
    assert!(s.created_event(stale.operation_id).expect("rejection event").is_none());
    assert_eq!(
        apply(&mut s, 6, &stale),
        CreatedResult::HeadMismatch {
            actual_head: Some(c.prepared.version_id)
        }
    );
    drop(s);
    let s = reopen(&dir.path().join("db"));
    assert_eq!(s.last_applied().expect("last"), Some(id(6)));
    let tx = s.database.begin_read().expect("read");
    assert_eq!(tx.open_table(EVENTS).expect("events").len().expect("len"), 2);
}
#[test]
fn created_binding_is_pinned_and_not_caller_selected() {
    let (dir, mut s) = fixture();
    let mut bad = command();
    bad.binding_revision = 10;
    assert_eq!(apply(&mut s, 0, &bad), CreatedResult::BindingMismatch);
    assert!(s.created_head(&bad.object).expect("head").is_none());
    assert!(s.created_event(bad.operation_id).expect("event").is_none());
    assert_eq!(
        s.created_operation(bad.operation_id)
            .expect("operation")
            .expect("saved")
            .result,
        CreatedResult::BindingMismatch
    );
    s.pin_capture_binding(binding()).expect("idempotent pin");
    let mut changed = binding();
    changed.target_id = [10; 16];
    assert!(matches!(s.pin_capture_binding(changed), Err(StoreError::BindingConflict)));
    drop(s);
    let s = reopen(&dir.path().join("db"));
    assert_eq!(s.capture_binding().expect("binding"), Some(binding()));
}
#[test]
fn created_schema_compatibility_and_corruption_are_checked() {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("db");
    let s = Store::initialize(&path, identity()).expect("schema1");
    assert_eq!(s.capture_binding().expect("unmigrated"), None);
    drop(s);
    let mut s = reopen(&path);
    assert_eq!(s.last_applied().expect("no apply"), None);
    append(&mut s, 0, &command());
    s.save_committed(id(0)).expect("schema2");
    drop(s);
    let mut s = reopen(&path);
    assert_eq!(s.capture_binding().expect("schema2 unchanged"), None);
    s.pin_capture_binding(binding()).expect("explicit upgrade");
    drop(s);
    let mut s = reopen(&path);
    assert_eq!(s.capture_binding().expect("schema3"), Some(binding()));
    s.apply_next_created(id(0)).expect("apply");
    drop(s);
    let db = Database::open(&path).expect("raw db");
    let tx = db.begin_write().expect("write corruption");
    tx.open_table(EVENTS)
        .expect("events")
        .remove(command().operation_id.as_slice())
        .expect("remove reference");
    tx.commit().expect("commit corruption");
    drop(db);
    let result = Store::open(&path, identity());
    assert!(
        matches!(result, Err(StoreError::CorruptRecord)),
        "U11 missing event rejected before expect"
    );
    let (dir, mut s) = fixture();
    assert_eq!(s.read_committed().expect("empty committed"), None);
    append(&mut s, 0, &command());
    s.save_committed(id(0)).expect("committed schema3");
    assert_eq!(s.capture_binding().expect("no downgrade"), Some(binding()));
    drop(s);
    let path = dir.path().join("db");
    let db = Database::open(&path).expect("raw db");
    let tx = db.begin_write().expect("write");
    let mut marker = vec![1];
    marker.extend(encode_id(id(0)));
    tx.open_table(META)
        .expect("meta")
        .insert("last_applied", marker.as_slice())
        .expect("corrupt applied");
    tx.commit().expect("commit");
    drop(db);
    assert!(
        matches!(Store::open(&path, identity()), Err(StoreError::CorruptRecord)),
        "U11 last applied missing result rejected"
    );
    let (dir, s) = fixture();
    drop(s);
    let path = dir.path().join("db");
    let db = Database::open(&path).expect("raw pinned db");
    let tx = db.begin_write().expect("missing binding write");
    tx.open_table(META)
        .expect("meta")
        .remove("capture_binding")
        .expect("remove binding");
    tx.commit().expect("binding corruption commit");
    drop(db);
    assert!(
        matches!(Store::open(&path, identity()), Err(StoreError::CorruptRecord)),
        "U11 missing pinned binding rejected"
    );

    let (dir, mut s) = fixture();
    let c = command();
    assert_eq!(apply(&mut s, 0, &c), success(&c, 0));
    assert_eq!(apply(&mut s, 1, &c), success(&c, 0));
    drop(s);
    let path = dir.path().join("db");
    let db = Database::open(&path).expect("raw duplicate db");
    let tx = db.begin_write().expect("retarget decision write");
    let mut key = created::object_bytes(&c.object).expect("object key");
    key.extend(c.prepared.version_id);
    let mut version = Vec::new();
    created::put_prepared(&mut version, &c.prepared);
    version.extend(encode_id(id(1)));
    version.extend(binding().revision.to_be_bytes());
    tx.open_table(VERSIONS)
        .expect("versions")
        .insert(key.as_slice(), version.as_slice())
        .expect("retarget version");
    let mut event = created::object_bytes(&c.object).expect("event object");
    created::put_prepared(&mut event, &c.prepared);
    event.extend(encode_id(id(1)));
    event.extend(created::binding_bytes(&binding()).expect("binding bytes"));
    tx.open_table(EVENTS)
        .expect("events")
        .insert(c.operation_id.as_slice(), event.as_slice())
        .expect("retarget event");
    let canonical = c.encode().expect("canonical");
    let mut operation = u64::try_from(canonical.len()).expect("length").to_be_bytes().to_vec();
    operation.extend(canonical);
    operation.extend(created::result_bytes(&success(&c, 1)));
    tx.open_table(OPERATIONS)
        .expect("operations")
        .insert(c.operation_id.as_slice(), operation.as_slice())
        .expect("retarget original response");
    for index in 0..=1 {
        let mut result = encode_id(id(index));
        result.extend(created::result_bytes(&success(&c, 1)));
        tx.open_table(APPLY_RESULTS)
            .expect("results")
            .insert(index, result.as_slice())
            .expect("retarget duplicate response");
    }
    tx.commit().expect("coherent retarget commit");
    drop(db);
    assert!(
        matches!(Store::open(&path, identity()), Err(StoreError::CorruptRecord)),
        "U11 original decision cannot retarget duplicate log"
    );
}
#[test]
fn created_process_child() {
    if let Some(path) = std::env::var_os("U11_CHILD_DB") {
        let mut s = Store::initialize(path, identity()).expect("child init");
        s.pin_capture_binding(binding()).expect("child pin");
        assert_eq!(apply(&mut s, 0, &command()), success(&command(), 0));
        complete(&s);
        println!("U11_ACK");
        std::io::stdout().flush().expect("flush ACK");
        loop {
            std::thread::park()
        }
    }
    let (_dir, mut s) = fixture();
    assert_eq!(apply(&mut s, 0, &command()), success(&command(), 0));
    complete(&s);
}
#[test]
fn created_acknowledged_apply_survives_child_kill() {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("db");
    let mut child = Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", "application_tests::created_process_child", "--nocapture"])
        .env("U11_CHILD_DB", &path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn");
    let stdout = child.stdout.take().expect("stdout");
    let (tx, rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let line = line.expect("child line");
            if line.ends_with("U11_ACK") {
                let _ = tx.send(());
                break;
            }
        }
    });
    let acknowledged = rx.recv_timeout(Duration::from_secs(20));
    child.kill().expect("kill child");
    child.wait().expect("wait child");
    reader.join().expect("reader cleanup");
    assert!(acknowledged.is_ok(), "U11 bounded child ACK missing");
    complete(&reopen(&path));
}
#[test]
fn created_power_cut_before_and_after_apply_flush() {
    let (dir, mut s) = fixture();
    append(&mut s, 0, &command());
    s.save_committed(id(0)).expect("committed");
    drop(s);
    let path = dir.path().join("db");
    let backend = CrashBackend::new(&path);
    let mut s = Store::from_database(backend.database(), identity()).expect("backend");
    assert_eq!(s.apply_next_created(id(0)).expect("apply ACK"), success(&command(), 0));
    backend.cut();
    drop(s);
    complete(&reopen(&path));
    for sync in 1..=2 {
        for after in [false, true] {
            let (dir, mut s) = fixture();
            append(&mut s, 0, &command());
            s.save_committed(id(0)).expect("committed");
            drop(s);
            let path = dir.path().join("db");
            let backend = CrashBackend::new(&path);
            let mut s = Store::from_database(backend.database(), identity()).expect("backend");
            let (entered, resume) = backend.pause_sync(sync, after);
            let writer = std::thread::spawn(move || {
                let result = s.apply_next_created(id(0));
                (s, result)
            });
            entered.wait();
            backend.cut();
            resume.wait();
            let (s, result) = writer.join().expect("writer");
            drop(s);
            let reopened = Store::open(&path, identity());
            assert!(
                reopened.is_ok(),
                "U11 apply cut must reopen whole old/new state sync={sync} after={after}: {:?}",
                reopened.as_ref().err()
            );
            let s = reopened.expect("asserted reopen");
            if s.last_applied().expect("last").is_some() {
                complete(&s)
            } else {
                assert!(result.is_err());
                assert_eq!(s.created_head(&command().object).expect("old head"), None);
                assert_eq!(
                    s.created_version(&command().object, command().prepared.version_id)
                        .expect("old version"),
                    None
                );
                assert_eq!(s.created_event(command().operation_id).expect("old event"), None);
                assert_eq!(s.created_operation(command().operation_id).expect("old op"), None);
                assert_eq!(s.created_apply_result(0).expect("old result"), None)
            }
            eprintln!("U11 cut boundary sync={sync} after={after}");
        }
    }
}
