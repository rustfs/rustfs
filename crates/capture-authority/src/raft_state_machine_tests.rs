//! Actual OpenRaft storage traits over real redb; no simulated state machine.
use super::*;
use openraft::{
    RaftLogReader, RaftSnapshotBuilder,
    storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::SeekFrom,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub(super) type Snapshot = openraft::Snapshot<CaptureTypeConfig>;
pub(super) fn identity(node_id: u64) -> NodeIdentity {
    NodeIdentity {
        cluster_id: [7; 16],
        node_id,
    }
}
pub(super) fn binding() -> CaptureBinding {
    CaptureBinding {
        revision: 9,
        binding_id: [8; 16],
        target_id: [9; 16],
    }
}
pub(super) fn id(index: u64) -> LogId {
    LogId {
        term: 8,
        leader_node: 0,
        index,
    }
}
pub(super) fn entry(index: u64) -> CaptureRaftEntry {
    let payload = if index == 0 {
        let membership = CaptureMembership::new(
            Some(id(0)),
            vec![BTreeSet::from([0])],
            BTreeMap::from([(0, CaptureNode::new("http://127.0.0.1:7000".into()).expect("member"))]),
        )
        .expect("native membership");
        let stored = openraft::StoredMembership::try_from(&membership).expect("stored membership");
        openraft::EntryPayload::Membership(stored.membership().clone())
    } else {
        let byte = u8::try_from(index).expect("bounded test event");
        openraft::EntryPayload::Normal(
            CaptureCommand::try_created(DecideCreated {
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
            })
            .expect("native command"),
        )
    };
    CaptureRaftEntry {
        log_id: id(index).into(),
        payload,
    }
}
pub(super) fn entries(end: u64) -> Vec<CaptureRaftEntry> {
    (0..=end).map(entry).collect()
}
pub(super) struct Fixture {
    pub directory: tempfile::TempDir,
    pub writer: NativeLogWriter,
    pub logs: NativeRaftLogStore,
    pub sm: NativeRaftStateMachine,
    pub identity: NodeIdentity,
}
impl Fixture {
    pub fn new(node_id: u64) -> Self {
        Self::with_identity_binding(identity(node_id), binding())
    }
    pub fn with_identity_binding(identity: NodeIdentity, binding: CaptureBinding) -> Self {
        let directory = tempfile::tempdir().expect("real state machine directory");
        let mut store = Store::initialize(directory.path().join("db"), identity).expect("real redb initialization");
        store.pin_capture_binding(binding).expect("native binding");
        store.enable_native_empty().expect("native mode");
        store.promote_created_provenance().expect("provenance");
        let writer = NativeLogWriter::new(store);
        let logs = NativeRaftLogStore::new(writer.clone());
        let sm = NativeRaftStateMachine::new(writer.clone(), CheckpointLimits::default()).expect("real state machine adapter");
        Self {
            directory,
            writer,
            logs,
            sm,
            identity,
        }
    }
    pub async fn apply(&mut self, batch: Vec<CaptureRaftEntry>) -> Vec<CaptureResponse> {
        self.logs
            .blocking_append(batch.clone())
            .await
            .expect("actual trait append and library flush callback");
        self.sm.apply(batch).await.expect("actual state machine trait apply")
    }
    pub async fn build(&mut self) -> Snapshot {
        self.sm
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .expect("actual durable snapshot builder")
    }
    pub async fn page(&self) -> EventPage {
        self.writer
            .created_events_page(None, 256)
            .await
            .expect("authoritative native events")
    }
}
pub(super) async fn snapshot_bytes(snapshot: &mut Snapshot) -> Vec<u8> {
    snapshot.snapshot.seek(SeekFrom::Start(0)).await.expect("snapshot rewind");
    let mut bytes = Vec::new();
    snapshot
        .snapshot
        .read_to_end(&mut bytes)
        .await
        .expect("actual snapshot bytes");
    bytes
}
pub(super) async fn receive(
    sm: &mut NativeRaftStateMachine,
    bytes: &[u8],
) -> Box<<CaptureTypeConfig as openraft::RaftTypeConfig>::SnapshotData> {
    let mut data = sm.begin_receiving_snapshot().await.expect("actual bounded receive handle");
    data.write_all(bytes).await.expect("receive real snapshot bytes");
    data.seek(SeekFrom::Start(0)).await.expect("received snapshot rewind");
    data
}
pub(super) async fn install(sm: &mut NativeRaftStateMachine, snapshot: &mut Snapshot) {
    let bytes = snapshot_bytes(snapshot).await;
    let data = receive(sm, &bytes).await;
    sm.install_snapshot(&snapshot.meta, data)
        .await
        .expect("actual state machine trait install");
}

#[tokio::test]
async fn raft_state_machine_apply_restart() {
    let mut f = Fixture::new(0);
    assert_eq!(f.sm.applied_state().await.expect("empty applied state").0, None);
    assert!(f.sm.get_current_snapshot().await.expect("no published snapshot").is_none());
    let batch = entries(2);
    let responses = f.apply(batch.clone()).await;
    assert_eq!(responses.len(), 3, "RAFT_SM_REAL_RESPONSE_COUNT");
    assert_eq!(responses[0].kind(), CaptureResponseKind::Membership);
    assert!(matches!(responses[1].created_result(), Some(CreatedResult::Created { .. })));
    let state = f.sm.applied_state().await.expect("durable applied state");
    assert_eq!(state.0, Some(id(2).into()));
    assert_eq!(state.1.log_id(), &Some(id(0).into()));
    let before = f.page().await;
    let Fixture {
        directory,
        writer,
        logs,
        sm,
        identity,
    } = f;
    drop(sm);
    drop(logs);
    drop(writer);
    let writer = NativeLogWriter::new(Store::open(directory.path().join("db"), identity).expect("restart real store"));
    let mut sm = NativeRaftStateMachine::new(writer.clone(), CheckpointLimits::default()).expect("reopened state machine");
    assert_eq!(
        sm.applied_state().await.expect("reopened applied state"),
        state,
        "RAFT_SM_APPLY_ACK_DURABLE"
    );
    assert_eq!(writer.created_events_page(None, 256).await.expect("reopened events"), before);
    assert_eq!(
        sm.apply(batch).await.expect("exact replay responses"),
        responses,
        "RAFT_SM_REPLAY_SAME_RESPONSES"
    );
    assert_eq!(
        writer
            .created_events_page(None, 256)
            .await
            .expect("no duplicate replay events"),
        before
    );
}

#[tokio::test]
async fn raft_state_machine_snapshot_build_get_restart() {
    let mut f = Fixture::new(0);
    f.apply(entries(2)).await;
    let mut built = f.build().await;
    let meta = built.meta.clone();
    let bytes = snapshot_bytes(&mut built).await;
    assert_eq!(meta.last_log_id, Some(id(2).into()), "RAFT_SM_SNAPSHOT_META_CUT");
    assert_eq!(meta.last_membership, f.sm.applied_state().await.expect("membership").1);
    f.apply(vec![entry(3)]).await;
    let mut current =
        f.sm.get_current_snapshot()
            .await
            .expect("persisted snapshot")
            .expect("snapshot exists");
    assert_eq!(current.meta, meta, "RAFT_SM_LIVE_APPLY_DOES_NOT_REPIN_SNAPSHOT");
    assert_eq!(snapshot_bytes(&mut current).await, bytes, "RAFT_SM_SNAPSHOT_FROZEN_BYTES");
    let Fixture {
        directory,
        writer,
        logs,
        sm,
        identity,
    } = f;
    drop(current);
    drop(built);
    drop(sm);
    drop(logs);
    drop(writer);
    let writer = NativeLogWriter::new(Store::open(directory.path().join("db"), identity).expect("restart snapshot owner"));
    let mut sm = NativeRaftStateMachine::new(writer, CheckpointLimits::default()).expect("reopen adapter");
    let mut current = sm
        .get_current_snapshot()
        .await
        .expect("restarted current snapshot")
        .expect("durable snapshot exists");
    assert_eq!(current.meta, meta, "RAFT_SM_COMPLETE_META_RECONSTRUCTED");
    assert_eq!(snapshot_bytes(&mut current).await, bytes, "RAFT_SM_RESTART_SNAPSHOT_BYTES");
}

#[tokio::test]
async fn raft_state_machine_receive_bounds() {
    let f = Fixture::new(0);
    let mut limits = CheckpointLimits::default();
    limits.snapshot.total_bytes = 1024;
    let mut sm = NativeRaftStateMachine::new(f.writer.clone(), limits).expect("bounded state machine");
    let mut data = sm.begin_receiving_snapshot().await.expect("bounded receive");
    data.write_all(&[1; 16]).await.expect("small write");
    assert!(data.seek(SeekFrom::Start(u64::MAX)).await.is_err(), "RAFT_SM_RECEIVE_SEEK_BOUND");
    data.seek(SeekFrom::Start(0)).await.expect("restore seek");
    let mut bytes = [0; 16];
    data.read_exact(&mut bytes).await.expect("small received bytes");
    assert_eq!(bytes, [1; 16], "RAFT_SM_BAD_SEEK_PRESERVES_BYTES");
    assert!(data.write_all(&[2; 2048]).await.is_err(), "RAFT_SM_RECEIVE_WRITE_BOUND_BEFORE_INSTALL");
    assert_eq!(sm.applied_state().await.expect("receive is not install").0, None);
    assert!(sm.get_current_snapshot().await.expect("no received publication").is_none());
}

#[tokio::test]
async fn raft_state_machine_install_rejects_meta_identity() {
    let mut source = Fixture::new(0);
    source.apply(entries(2)).await;
    let mut snapshot = source.build().await;
    let bytes = snapshot_bytes(&mut snapshot).await;
    let mut target = Fixture::new(1);
    target.apply(entries(1)).await;
    let old_page = target.page().await;
    let stable = std::fs::read(target.directory.path().join("db")).expect("nonempty target bytes");
    let mut wrong = snapshot.meta.clone();
    wrong.last_log_id = Some(LogId { term: 9, ..id(2) }.into());
    let data = receive(&mut target.sm, &bytes).await;
    assert!(
        target.sm.install_snapshot(&wrong, data).await.is_err(),
        "SNAPSHOT_META_BOUND_TO_FRAME: full LogId term cannot differ from snapshot header"
    );
    assert_eq!(target.sm.applied_state().await.expect("rejection state").0, Some(id(1).into()));
    assert_eq!(target.page().await, old_page, "RAFT_SM_META_REJECTION_RETAINS_EVENTS");
    assert_eq!(
        std::fs::read(target.directory.path().join("db")).expect("unchanged nonempty target"),
        stable,
        "RAFT_SM_META_REJECTION_NO_MUTATION"
    );
    assert!(target.sm.get_current_snapshot().await.expect("rejection snapshot").is_none());
    let mut wrong = snapshot.meta.clone();
    wrong.last_membership = openraft::StoredMembership::default();
    let data = receive(&mut target.sm, &bytes).await;
    assert!(
        target.sm.install_snapshot(&wrong, data).await.is_err(),
        "RAFT_SM_MEMBERSHIP_BOUND_TO_FRAME"
    );
    let mut foreign = Fixture::with_identity_binding(
        NodeIdentity {
            cluster_id: [6; 16],
            ..identity(1)
        },
        binding(),
    );
    let data = receive(&mut foreign.sm, &bytes).await;
    assert!(
        foreign.sm.install_snapshot(&snapshot.meta, data).await.is_err(),
        "RAFT_SM_CLUSTER_BOUND_TO_DURABLE_OWNER"
    );
    let mut foreign = Fixture::with_identity_binding(
        identity(1),
        CaptureBinding {
            target_id: [5; 16],
            ..binding()
        },
    );
    let data = receive(&mut foreign.sm, &bytes).await;
    assert!(
        foreign.sm.install_snapshot(&snapshot.meta, data).await.is_err(),
        "RAFT_SM_CAPTURE_BINDING_BOUND"
    );
}

#[tokio::test]
async fn raft_state_machine_install_preserves_local_identity_vote() {
    let mut source = Fixture::new(0);
    source.apply(entries(2)).await;
    let mut snapshot = source.build().await;
    snapshot.meta.snapshot_id = "foreign-snapshot-id-preserved-verbatim".into();
    let mut target = Fixture::new(1);
    let vote = openraft::Vote::new_committed(10, 1);
    target.logs.save_vote(&vote).await.expect("local election vote");
    install(&mut target.sm, &mut snapshot).await;
    assert_eq!(
        target.writer.durable_identity().await.expect("local identity"),
        identity(1),
        "RAFT_SM_INSTALL_PRESERVES_LOCAL_NODE"
    );
    assert_eq!(
        target.logs.read_vote().await.expect("local vote"),
        Some(vote),
        "RAFT_SM_INSTALL_PRESERVES_LOCAL_VOTE"
    );
    let mut current = target
        .sm
        .get_current_snapshot()
        .await
        .expect("installed snapshot")
        .expect("present");
    assert_eq!(current.meta, snapshot.meta, "RAFT_SM_INSTALL_PRESERVES_COMPLETE_INCOMING_META");
    assert_eq!(snapshot_bytes(&mut current).await, snapshot_bytes(&mut snapshot).await);
    let Fixture {
        directory,
        writer,
        logs,
        sm,
        identity,
    } = target;
    drop(current);
    drop(sm);
    drop(logs);
    drop(writer);
    let writer = NativeLogWriter::new(Store::open(directory.path().join("db"), identity).expect("restart installed target"));
    let mut logs = NativeRaftLogStore::new(writer.clone());
    let mut sm = NativeRaftStateMachine::new(writer.clone(), CheckpointLimits::default()).expect("reopened installed SM");
    assert_eq!(writer.durable_identity().await.expect("restarted local identity"), identity);
    assert_eq!(logs.read_vote().await.expect("restarted vote"), Some(vote));
    assert_eq!(
        sm.get_current_snapshot()
            .await
            .expect("restarted snapshot")
            .expect("present")
            .meta,
        snapshot.meta,
        "RAFT_SM_INCOMING_SNAPSHOT_ID_DURABLE"
    );
}

#[tokio::test]
async fn raft_state_machine_install_suffix() {
    let mut source = Fixture::new(0);
    source.apply(entries(2)).await;
    let mut snapshot = source.build().await;
    let mut target = Fixture::new(1);
    target
        .logs
        .blocking_append(entries(4))
        .await
        .expect("retained local log suffix");
    target.sm.apply(entries(1)).await.expect("old local applied prefix");
    target
        .logs
        .save_committed(Some(id(4).into()))
        .await
        .expect("known committed compatible suffix");
    install(&mut target.sm, &mut snapshot).await;
    assert_eq!(
        target
            .logs
            .get_log_reader()
            .await
            .try_get_log_entries(3..)
            .await
            .expect("compatible suffix"),
        vec![entry(3), entry(4)],
        "RAFT_SM_INSTALL_PRESERVES_COMPATIBLE_COMMITTED_SUFFIX"
    );
    assert_eq!(
        target.logs.read_committed().await.expect("preserved committed marker"),
        Some(id(4).into())
    );
    target
        .sm
        .apply(vec![entry(3), entry(4)])
        .await
        .expect("apply retained committed suffix");
    assert_eq!(target.page().await.events.len(), 4);
    let mut conflict = Fixture::new(2);
    let mut batch = entries(4);
    for entry in &mut batch[2..] {
        entry.log_id = LogId {
            term: 9,
            ..id(entry.log_id.index)
        }
        .into();
    }
    conflict
        .logs
        .blocking_append(batch)
        .await
        .expect("conflicting uncommitted suffix");
    conflict.sm.apply(entries(1)).await.expect("protected applied prefix");
    install(&mut conflict.sm, &mut snapshot).await;
    assert!(
        conflict
            .logs
            .get_log_reader()
            .await
            .try_get_log_entries(3..)
            .await
            .expect("discarded conflict")
            .is_empty(),
        "RAFT_SM_INSTALL_DROPS_ONLY_UNCOMMITTED_CONFLICT"
    );
    conflict.apply(vec![entry(3)]).await;
    assert_eq!(conflict.sm.applied_state().await.expect("new suffix applied").0, Some(id(3).into()));
}

#[tokio::test]
async fn raft_state_machine_install_restart_continue() {
    let mut source = Fixture::new(0);
    source.apply(entries(2)).await;
    let expected = source.page().await;
    let mut snapshot = source.build().await;
    let mut target = Fixture::new(1);
    install(&mut target.sm, &mut snapshot).await;
    assert_eq!(target.page().await, expected, "RAFT_SM_INSTALL_REBUILDS_SOURCE_EVENTS");
    let Fixture {
        directory,
        writer,
        logs,
        sm,
        identity,
    } = target;
    drop(sm);
    drop(logs);
    drop(writer);
    let writer = NativeLogWriter::new(Store::open(directory.path().join("db"), identity).expect("restart installed redb"));
    let mut logs = NativeRaftLogStore::new(writer.clone());
    let mut sm = NativeRaftStateMachine::new(writer.clone(), CheckpointLimits::default()).expect("restart installed adapter");
    assert_eq!(sm.applied_state().await.expect("snapshot applied state").0, Some(id(2).into()));
    logs.blocking_append(vec![entry(3)])
        .await
        .expect("append after install restart");
    sm.apply(vec![entry(3)]).await.expect("apply after install restart");
    let all = writer.created_events_page(None, 256).await.expect("retained plus new event");
    assert_eq!(all.events.len(), 3);
    assert_eq!(all.events[..2], expected.events);
    let next = writer
        .created_events_page(expected.next, 1)
        .await
        .expect("snapshot cursor resumes");
    assert_eq!(next.events[0].event_id, [3; 16], "RAFT_SM_INSTALL_CURSOR_CONTINUITY");
}

#[tokio::test]
async fn raft_state_machine_empty_snapshot_restart_continue() {
    let mut source = Fixture::new(0);
    let mut target = Fixture::new(42);
    let mut snapshot = source.sm.build_snapshot().await.expect("durable empty baseline");
    assert_eq!(snapshot.meta.last_log_id, None);
    let empty_meta = snapshot.meta.clone();
    let received = receive(&mut target.sm, &snapshot_bytes(&mut snapshot).await).await;
    target
        .sm
        .install_snapshot(&empty_meta, received)
        .await
        .expect("install empty frame");
    let mut target = target.reopen();
    let current = target
        .sm
        .get_current_snapshot()
        .await
        .expect("empty after reopen")
        .expect("durable selection");
    assert_eq!(current.meta, empty_meta);
    target
        .logs
        .blocking_append(entries(1))
        .await
        .expect("real append after empty install");
    target.sm.apply(entries(1)).await.expect("real apply after empty install");
    let mut target = target.reopen();
    assert_eq!(
        target.sm.applied_state().await.expect("replay empty baseline + suffix").0,
        Some(id(1).into())
    );
    assert_eq!(
        target
            .writer
            .created_events_page(None, 16)
            .await
            .expect("event after empty baseline")
            .events
            .len(),
        1
    );
    let refreshed = target
        .sm
        .build_snapshot()
        .await
        .expect("nonempty checkpoint supersedes empty");
    assert_eq!(refreshed.meta.last_log_id, Some(id(1).into()));
}

impl Fixture {
    fn reopen(self) -> Self {
        let Self {
            directory,
            writer,
            logs,
            sm,
            identity,
        } = self;
        drop(sm);
        drop(logs);
        drop(writer);
        let writer =
            NativeLogWriter::new(Store::open(directory.path().join("db"), identity).expect("real empty baseline restart"));
        let logs = NativeRaftLogStore::new(writer.clone());
        let sm =
            NativeRaftStateMachine::new(writer.clone(), CheckpointLimits::default()).expect("reopened empty baseline adapter");
        Self {
            directory,
            writer,
            logs,
            sm,
            identity,
        }
    }
}

#[tokio::test]
async fn raft_state_machine_build_competes_with_initial_apply() {
    let mut target = Fixture::new(0);
    target
        .logs
        .blocking_append(entries(1))
        .await
        .expect("real pending initial batch");
    let entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let resume = std::sync::Arc::new(tokio::sync::Notify::new());
    *target.sm.build_before_ordering.lock().expect("build pause") = Some((entered.clone(), resume.clone()));
    let mut builder = target.sm.get_snapshot_builder().await;
    let task = tokio::spawn(async move { builder.build_snapshot().await });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .expect("builder began before initial apply");
    target
        .sm
        .apply(entries(1))
        .await
        .expect("actual initial apply while builder pending");
    resume.notify_one();
    let snapshot = tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .expect("build continues")
        .expect("builder task")
        .expect("initial apply does not make snapshot build a fatal storage error");
    assert_eq!(
        snapshot.meta.last_log_id,
        Some(id(1).into()),
        "SNAPSHOT_BUILD_INITIAL_APPLY_RACE_USES_NONEMPTY_CHECKPOINT"
    );
}
