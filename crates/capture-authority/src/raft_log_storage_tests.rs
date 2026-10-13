//! Exercises actual OpenRaft storage traits, not a substitute consensus engine.
use super::*;
use openraft::{
    RaftLogReader,
    storage::{RaftLogStorage, RaftLogStorageExt},
};
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
fn entry(index: u64) -> CaptureRaftEntry {
    let payload = if index == 0 {
        let membership = CaptureMembership::new(
            Some(id(0)),
            vec![BTreeSet::from([0])],
            BTreeMap::from([(0, CaptureNode::new("http://127.0.0.1:7000".into()).expect("test member"))]),
        )
        .expect("native membership");
        let membership = openraft::StoredMembership::try_from(&membership).expect("stored native membership");
        openraft::EntryPayload::Membership(membership.membership().clone())
    } else {
        let byte = u8::try_from(index).expect("small test event index");
        let command = DecideCreated {
            operation_id: [byte; 16],
            object: ObjectIdentity {
                bucket_incarnation: [2; 16],
                key: vec![byte],
            },
            expected_head: None,
            prepared: PreparedIdentity {
                version_id: [byte; 16],
                preparation_id: [4; 16],
                content_digest: [5; 32],
                content_length: 123,
            },
            binding_revision: binding().revision,
        };
        openraft::EntryPayload::Normal(CaptureCommand::try_created(command).expect("native test command"))
    };
    CaptureRaftEntry {
        log_id: id(index).into(),
        payload,
    }
}
fn entries(end: u64) -> Vec<CaptureRaftEntry> {
    (0..=end).map(entry).collect()
}
fn fixture() -> (tempfile::TempDir, NativeLogWriter, NativeRaftLogStore) {
    let directory = tempfile::tempdir().expect("actual native database directory");
    let mut store = Store::initialize(directory.path().join("db"), identity()).expect("actual redb initialization");
    store.pin_capture_binding(binding()).expect("capture binding");
    store.enable_native_empty().expect("native mode");
    store.promote_created_provenance().expect("native provenance");
    let writer = NativeLogWriter::new(store);
    let adapter = NativeRaftLogStore::new(writer.clone());
    (directory, writer, adapter)
}
fn media_fixture() -> (tempfile::TempDir, tests::CrashBackend, NativeLogWriter, NativeRaftLogStore) {
    let (directory, writer, adapter) = fixture();
    drop(adapter);
    drop(writer);
    let backend = tests::CrashBackend::new(&directory.path().join("db"));
    let store = Store::from_database(backend.database(), identity()).expect("existing real disk fault backend");
    let writer = NativeLogWriter::new(store);
    let adapter = NativeRaftLogStore::new(writer.clone());
    (directory, backend, writer, adapter)
}
async fn logs(adapter: &mut NativeRaftLogStore) -> Vec<CaptureRaftEntry> {
    adapter
        .get_log_reader()
        .await
        .try_get_log_entries(..)
        .await
        .expect("actual trait reader")
}
async fn checkpoint(writer: &NativeLogWriter, adapter: &mut NativeRaftLogStore) -> CheckpointReceipt {
    let batch = entries(2);
    adapter
        .blocking_append(batch.clone())
        .await
        .expect("trait append before checkpoint");
    // Prepares genuine applied/checkpoint evidence; this does not claim Raft consensus.
    writer
        .apply_committed(batch)
        .await
        .expect("trusted test owner applies preparation entries");
    writer
        .checkpoint_local(CheckpointLimits::default())
        .await
        .expect("real durable baseline")
}

#[tokio::test]
async fn raft_log_storage_flush_restart() {
    let (directory, backend, writer, mut adapter) = media_fixture();
    let batch = entries(2);
    // blocking_append constructs the real library-private LogFlushed callback.
    adapter
        .blocking_append(batch.clone())
        .await
        .expect("real durable flush callback");
    let state = adapter.get_log_state().await.expect("trait log state");
    assert_eq!(state.last_log_id, Some(id(2).into()), "RAFT_LOG_TAIL_NOT_APPLIED_STATE");
    assert_eq!(state.last_purged_log_id, None);
    assert_eq!(writer.applied_state().await.expect("application state").0, None);
    assert_eq!(logs(&mut adapter).await, batch);
    backend.cut();
    drop(adapter);
    drop(writer);
    let reopened = Store::open(directory.path().join("db"), identity()).expect("reopen after successful callback and power cut");
    let mut adapter = NativeRaftLogStore::new(NativeLogWriter::new(reopened));
    assert_eq!(logs(&mut adapter).await, batch, "RAFT_LOG_FLUSH_CALLBACK_DURABLE");
    assert_eq!(
        adapter.get_log_state().await.expect("reopened trait state").last_log_id,
        Some(id(2).into())
    );
}

#[tokio::test]
async fn raft_log_storage_vote_restart() {
    let (directory, backend, writer, mut adapter) = media_fixture();
    let vote = openraft::Vote::new_committed(9, 0);
    adapter.save_vote(&vote).await.expect("trait durable vote");
    assert_eq!(adapter.read_vote().await.expect("trait vote read"), Some(vote));
    adapter.blocking_append(entries(2)).await.expect("trait append");
    adapter
        .save_committed(Some(id(1).into()))
        .await
        .expect("trait durable commit marker");
    assert!(adapter.save_committed(None).await.is_err(), "RAFT_LOG_COMMITTED_NONE_CANNOT_RESET");
    assert_eq!(adapter.read_committed().await.expect("exact marker"), Some(id(1).into()));
    assert!(
        adapter.save_committed(Some(id(0).into())).await.is_err(),
        "RAFT_LOG_COMMITTED_NO_REGRESSION"
    );
    let mut wrong = id(2);
    wrong.term += 1;
    assert!(
        adapter.save_committed(Some(wrong.into())).await.is_err(),
        "RAFT_LOG_COMMITTED_EXACT_IDENTITY"
    );
    backend.cut();
    drop(adapter);
    drop(writer);
    let mut reopened = NativeRaftLogStore::new(NativeLogWriter::new(
        Store::open(directory.path().join("db"), identity()).expect("durable vote reopen"),
    ));
    assert_eq!(
        reopened.read_vote().await.expect("reopened vote"),
        Some(vote),
        "RAFT_LOG_VOTE_ACK_DURABLE"
    );
    assert_eq!(reopened.read_committed().await.expect("reopened committed marker"), Some(id(1).into()));
}

#[tokio::test]
async fn raft_log_storage_partial_purge_restart() {
    let (directory, writer, mut adapter) = fixture();
    let receipt = checkpoint(&writer, &mut adapter).await;
    adapter
        .blocking_append(vec![entry(3), entry(4)])
        .await
        .expect("uncovered suffix append");
    let events = writer
        .created_events_page(None, 256)
        .await
        .expect("pre-purge authoritative events");
    adapter
        .purge(id(1).into())
        .await
        .expect("checkpoint authorizes strict prefix");
    assert_eq!(
        logs(&mut adapter).await,
        vec![entry(2), entry(3), entry(4)],
        "RAFT_LOG_PARTIAL_PURGE_RETAINS_SUFFIX"
    );
    let state = adapter.get_log_state().await.expect("partially purged state");
    assert_eq!(state.last_purged_log_id, Some(id(1).into()));
    assert_eq!(state.last_log_id, Some(id(4).into()));
    adapter.purge(id(1).into()).await.expect("repeat exact purge is idempotent");
    assert!(adapter.purge(id(0).into()).await.is_err(), "RAFT_LOG_PURGE_NO_REGRESSION");
    {
        let store = writer.store.lock().expect("inspect task-owned store");
        let tx = store.database.begin_read().expect("real persisted prefix inspection");
        let results = tx.open_table(application::APPLY_RESULTS).expect("apply receipts");
        assert!(results.get(0).expect("membership receipt").is_none());
        assert!(results.get(1).expect("first receipt").is_none());
        assert!(
            results.get(2).expect("retained receipt").is_some(),
            "RAFT_LOG_PARTIAL_PURGE_RECEIPT_SUFFIX"
        );
    }
    drop(adapter);
    drop(writer);
    let writer = NativeLogWriter::new(Store::open(directory.path().join("db"), identity()).expect("restart partial purge"));
    let mut adapter = NativeRaftLogStore::new(writer.clone());
    assert_eq!(
        logs(&mut adapter).await,
        vec![entry(2), entry(3), entry(4)],
        "RAFT_LOG_PARTIAL_PURGE_REOPEN"
    );
    adapter
        .blocking_append(vec![entry(5)])
        .await
        .expect("continue append after partial purge");
    writer
        .apply_committed(vec![entry(3), entry(4), entry(5)])
        .await
        .expect("continue application from true applied state");
    assert_eq!(
        writer
            .created_events_page(None, 256)
            .await
            .expect("retained plus new events")
            .events
            .len(),
        5,
        "RAFT_LOG_PURGE_NEVER_DELETES_EVENTS"
    );
    writer
        .purge_checkpoint(&receipt)
        .await
        .expect("existing exact receipt API remains compatible after partial purge");
    assert_eq!(
        adapter
            .get_log_state()
            .await
            .expect("exact compatibility state")
            .last_purged_log_id,
        Some(id(2).into())
    );
    assert_eq!(logs(&mut adapter).await, vec![entry(3), entry(4), entry(5)]);
    assert_eq!(
        writer
            .created_events_page(None, 2)
            .await
            .expect("original events retained")
            .events,
        events.events
    );
    drop(adapter);
    drop(writer);
    let mut reopened = NativeRaftLogStore::new(NativeLogWriter::new(
        Store::open(directory.path().join("db"), identity()).expect("reopen continued apply and exact purge"),
    ));
    assert_eq!(logs(&mut reopened).await, vec![entry(3), entry(4), entry(5)]);
}

#[tokio::test]
async fn raft_log_storage_purge_rejects_uncovered() {
    let (directory, writer, mut adapter) = fixture();
    adapter
        .blocking_append(entries(2))
        .await
        .expect("trait append without snapshot authority");
    writer
        .apply_committed(entries(2))
        .await
        .expect("applied preparation without checkpoint");
    let before = std::fs::read(directory.path().join("db")).expect("stable pre-rejection bytes");
    assert!(
        adapter.purge(id(1).into()).await.is_err(),
        "PURGE_REQUIRES_CHECKPOINT_COVERAGE: application progress is not a durable checkpoint"
    );
    assert_eq!(
        std::fs::read(directory.path().join("db")).expect("rejected purge bytes"),
        before,
        "RAFT_LOG_UNAUTHORIZED_PURGE_NO_MUTATION"
    );
    writer
        .checkpoint_local(CheckpointLimits::default())
        .await
        .expect("checkpoint at applied cut 2");
    adapter
        .blocking_append(vec![entry(3)])
        .await
        .expect("log beyond checkpoint cut");
    let before = std::fs::read(directory.path().join("db")).expect("post-checkpoint bytes");
    assert!(
        adapter.purge(id(3).into()).await.is_err(),
        "PURGE_REQUIRES_CHECKPOINT_COVERAGE: retained target exceeds durable cut"
    );
    assert_eq!(
        std::fs::read(directory.path().join("db")).expect("uncovered target rejection bytes"),
        before,
        "RAFT_LOG_UNCOVERED_PURGE_NO_MUTATION"
    );
    assert_eq!(logs(&mut adapter).await, entries(3));
    assert_eq!(adapter.get_log_state().await.expect("unchanged marker").last_purged_log_id, None);
}

#[tokio::test]
async fn raft_log_storage_purge_identity() {
    let (directory, writer, mut adapter) = fixture();
    checkpoint(&writer, &mut adapter).await;
    for target in [
        LogId { term: 9, ..id(1) },
        LogId { leader_node: 1, ..id(1) },
        LogId { term: 9, ..id(2) },
    ] {
        let before = std::fs::read(directory.path().join("db")).expect("before wrong complete LogId");
        assert!(adapter.purge(target.into()).await.is_err(), "RAFT_LOG_PURGE_EXACT_LOG_ID");
        assert_eq!(std::fs::read(directory.path().join("db")).expect("after identity rejection"), before);
    }
    adapter.purge(id(1).into()).await.expect("valid identity");
    let mut wrong = id(1);
    wrong.term += 1;
    assert!(adapter.purge(wrong.into()).await.is_err(), "RAFT_LOG_PURGED_REPEAT_IDENTITY_BOUND");
    assert_eq!(
        adapter
            .get_log_state()
            .await
            .expect("unchanged identity marker")
            .last_purged_log_id,
        Some(id(1).into())
    );
    adapter.purge(id(2).into()).await.expect("final exact covered target");
    let state = adapter.get_log_state().await.expect("no remaining logs");
    assert_eq!(state.last_purged_log_id, Some(id(2).into()));
    assert_eq!(state.last_log_id, Some(id(2).into()), "RAFT_LOG_EMPTY_TAIL_USES_PURGED_LOG_ID");
    assert!(logs(&mut adapter).await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_log_storage_cancelled_append() {
    let (directory, backend, writer, adapter) = media_fixture();
    let (entered, resume) = backend.pause_sync(1, false);
    let mut first = adapter.clone();
    let task = tokio::spawn(async move { first.blocking_append(entries(2)).await });
    entered.wait(); // Actual redb commit has begun but its first sync is paused.
    assert!(!task.is_finished(), "RAFT_LOG_FLUSH_NOT_BEFORE_DURABILITY");
    task.abort();
    assert!(task.await.expect_err("cancelled blocking append waiter").is_cancelled());
    assert!(writer.ordering.try_lock().is_err(), "RAFT_LOG_CANCELLED_APPEND_RETAINS_ORDERING_OWNER");
    let mut following = adapter.clone();
    let next = tokio::spawn(async move { following.blocking_append(vec![entry(3)]).await });
    assert!(writer.ordering.try_lock().is_err(), "RAFT_LOG_NEXT_APPEND_CANNOT_BYPASS_PENDING_SYNC");
    resume.wait();
    next.await.expect("next trait append task").expect("ordered follow-up append");
    let mut adapter = adapter;
    assert_eq!(
        logs(&mut adapter).await,
        entries(3),
        "RAFT_LOG_CANCELLED_WAITER_BACKGROUND_COMMIT_COMPLETE"
    );
    backend.cut();
    drop(adapter);
    drop(writer);
    let mut reopened = NativeRaftLogStore::new(NativeLogWriter::new(
        Store::open(directory.path().join("db"), identity()).expect("reopen cancelled waiter background commit"),
    ));
    assert_eq!(logs(&mut reopened).await, entries(3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_log_storage_failed_append() {
    let (directory, backend, writer, adapter) = media_fixture();
    let (entered, resume) = backend.pause_sync(1, false);
    let mut appending = adapter.clone();
    let task = tokio::spawn(async move { appending.blocking_append(entries(2)).await });
    entered.wait();
    assert!(!task.is_finished(), "RAFT_LOG_FAILED_FLUSH_NOT_EARLY_SUCCESS");
    backend.cut();
    resume.wait();
    assert!(
        task.await.expect("failed actual trait append task").is_err(),
        "RAFT_LOG_FAILED_APPEND_NEVER_REPORTS_CALLBACK_SUCCESS"
    );
    let mut adapter = adapter;
    assert!(
        adapter.blocking_append(entries(2)).await.is_err(),
        "RAFT_LOG_INDETERMINATE_STORE_REJECTS_LATER_APPEND"
    );
    assert!(
        adapter.save_vote(&openraft::Vote::new_committed(9, 0)).await.is_err(),
        "RAFT_LOG_INDETERMINATE_STORE_REJECTS_LATER_VOTE"
    );
    drop(adapter);
    drop(writer);
    let mut reopened = NativeRaftLogStore::new(NativeLogWriter::new(
        Store::open(directory.path().join("db"), identity()).expect("reopen failed-before-first-sync append"),
    ));
    assert!(logs(&mut reopened).await.is_empty(), "RAFT_LOG_FAILURE_NO_PARTIAL_BATCH");
}

#[tokio::test]
async fn raft_log_storage_truncate_uncommitted_suffix() {
    let (directory, writer, mut adapter) = fixture();
    adapter.blocking_append(entries(4)).await.expect("trait append full log");
    adapter
        .save_committed(Some(id(2).into()))
        .await
        .expect("protect committed prefix");
    assert!(
        adapter.truncate(id(2).into()).await.is_err(),
        "RAFT_LOG_TRUNCATE_PROTECTS_COMMITTED_PREFIX"
    );
    adapter
        .truncate(id(3).into())
        .await
        .expect("truncate includes uncommitted target");
    assert_eq!(logs(&mut adapter).await, entries(2), "RAFT_LOG_TRUNCATE_INCLUSIVE");
    let mut replacement = entry(3);
    replacement.log_id = LogId { term: 9, ..id(3) }.into();
    adapter
        .blocking_append(vec![replacement.clone()])
        .await
        .expect("trait append replacement suffix");
    assert_eq!(
        adapter.get_log_state().await.expect("replacement tail").last_log_id,
        Some(replacement.log_id)
    );
    drop(adapter);
    drop(writer);
    let mut reopened = NativeRaftLogStore::new(NativeLogWriter::new(
        Store::open(directory.path().join("db"), identity()).expect("reopen truncate and replacement"),
    ));
    let mut expected = entries(2);
    expected.push(replacement);
    assert_eq!(logs(&mut reopened).await, expected);
}
