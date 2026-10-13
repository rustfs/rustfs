//! Atomic installation failures over real native stores and disk fault backend.
use super::raft_state_machine_tests::{Fixture, entries, entry, id, install, receive, snapshot_bytes};
use super::*;
use openraft::{
    RaftLogReader, RaftSnapshotBuilder,
    storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine},
};

async fn media_target() -> (Fixture, tests::CrashBackend) {
    let mut f = Fixture::new(1);
    f.apply(entries(1)).await;
    f.logs
        .save_vote(&openraft::Vote::new_committed(9, 1))
        .await
        .expect("preserved target vote");
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
    let backend = tests::CrashBackend::new(&directory.path().join("db"));
    let writer = NativeLogWriter::new(Store::from_database(backend.database(), identity).expect("actual redb fault target"));
    let logs = NativeRaftLogStore::new(writer.clone());
    let sm = NativeRaftStateMachine::new(writer.clone(), CheckpointLimits::default()).expect("fault target state machine");
    (
        Fixture {
            directory,
            writer,
            logs,
            sm,
            identity,
        },
        backend,
    )
}

#[tokio::test]
async fn native_snapshot_install_corruption_preserves_state() {
    let mut source = Fixture::new(0);
    source.apply(entries(2)).await;
    let mut snapshot = source.build().await;
    let bytes = snapshot_bytes(&mut snapshot).await;
    let mut target = Fixture::new(1);
    target.apply(entries(1)).await;
    let before = target.page().await;
    let state = target.sm.applied_state().await.expect("old applied state");
    let mut corrupted = bytes.clone();
    *corrupted.last_mut().expect("nonempty snapshot") ^= 1;
    for payload in [corrupted, bytes[..bytes.len() - 1].to_vec()] {
        let stable = std::fs::read(target.directory.path().join("db")).expect("old stable bytes");
        let data = receive(&mut target.sm, &payload).await;
        assert!(
            target.sm.install_snapshot(&snapshot.meta, data).await.is_err(),
            "SNAPSHOT_INSTALL_CORRUPT_FRAME_REJECTED"
        );
        assert_eq!(target.sm.applied_state().await.expect("unchanged applied state"), state);
        assert_eq!(target.page().await, before, "SNAPSHOT_INSTALL_CORRUPTION_NO_PARTIAL_TABLES");
        assert_eq!(std::fs::read(target.directory.path().join("db")).expect("unchanged stable bytes"), stable);
        assert!(
            target
                .sm
                .get_current_snapshot()
                .await
                .expect("unpublished corrupt snapshot")
                .is_none()
        );
    }
    install(&mut target.sm, &mut snapshot).await;
    assert_eq!(target.page().await, source.page().await, "SNAPSHOT_INSTALL_RETRY_AFTER_CORRUPT_RECEIVE");
}

#[tokio::test]
async fn native_snapshot_install_stale_cut_rejected() {
    let mut source = Fixture::new(0);
    source.apply(entries(1)).await;
    let mut snapshot = source.build().await;
    let bytes = snapshot_bytes(&mut snapshot).await;
    let mut target = Fixture::new(1);
    target.apply(entries(3)).await;
    let current = target.build().await.meta;
    let events = target.page().await;
    let data = receive(&mut target.sm, &bytes).await;
    assert!(
        target.sm.install_snapshot(&snapshot.meta, data).await.is_err(),
        "SNAPSHOT_INSTALL_NEVER_ROLLS_BACK_APPLIED_STATE"
    );
    assert_eq!(target.sm.applied_state().await.expect("non-regressed applied").0, Some(id(3).into()));
    assert_eq!(
        target
            .sm
            .get_current_snapshot()
            .await
            .expect("old current snapshot")
            .expect("present")
            .meta,
        current
    );
    assert_eq!(target.page().await, events);
    let mut source = Fixture::new(0);
    source.apply(entries(2)).await;
    let mut snapshot = source.build().await;
    let bytes = snapshot_bytes(&mut snapshot).await;
    let mut target = Fixture::new(1);
    let mut batch = entries(3);
    for entry in &mut batch[2..] {
        entry.log_id = LogId {
            term: 9,
            ..id(entry.log_id.index)
        }
        .into();
    }
    target
        .logs
        .blocking_append(batch)
        .await
        .expect("conflicting committed suffix logs");
    target.sm.apply(entries(1)).await.expect("existing applied prefix");
    target
        .logs
        .save_committed(Some(LogId { term: 9, ..id(3) }.into()))
        .await
        .expect("protect conflicting committed suffix");
    let before = target
        .logs
        .get_log_reader()
        .await
        .try_get_log_entries(..)
        .await
        .expect("protected logs");
    let data = receive(&mut target.sm, &bytes).await;
    assert!(
        target.sm.install_snapshot(&snapshot.meta, data).await.is_err(),
        "SNAPSHOT_INSTALL_NEVER_DISCARDS_COMMITTED_CONFLICT"
    );
    assert_eq!(
        target
            .logs
            .get_log_reader()
            .await
            .try_get_log_entries(..)
            .await
            .expect("unchanged protected logs"),
        before
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_snapshot_install_cancelled_owner() {
    let mut source = Fixture::new(0);
    source.apply(entries(2)).await;
    let mut snapshot = source.build().await;
    let bytes = snapshot_bytes(&mut snapshot).await;
    let (mut target, backend) = media_target().await;
    let data = receive(&mut target.sm, &bytes).await;
    let (entered, resume) = backend.pause_sync(1, false);
    let mut installing = target.sm.clone();
    let meta = snapshot.meta.clone();
    let task = tokio::spawn(async move { installing.install_snapshot(&meta, data).await });
    entered.wait();
    assert!(!task.is_finished(), "SNAPSHOT_INSTALL_NOT_ACKED_BEFORE_SYNC");
    task.abort();
    assert!(task.await.expect_err("cancelled install waiter").is_cancelled());
    assert!(
        target.writer.ordering.try_lock().is_err(),
        "SNAPSHOT_INSTALL_CANCELLED_WAITER_RETAINS_OWNER"
    );
    resume.wait();
    // Acquiring the production owner waits for the abandoned blocking commit.
    drop(target.writer.ordering.lock().await);
    assert_eq!(
        target.sm.applied_state().await.expect("completed background install").0,
        Some(id(2).into()),
        "SNAPSHOT_INSTALL_COMMIT_NOT_INTERRUPTED_BY_WAITER_CANCEL"
    );
    let vote = target.logs.read_vote().await.expect("preserved vote");
    assert_eq!(vote, Some(openraft::Vote::new_committed(9, 1)));
    backend.cut();
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
    let writer = NativeLogWriter::new(Store::open(directory.path().join("db"), identity).expect("reopen cancelled installation"));
    let mut sm =
        NativeRaftStateMachine::new(writer.clone(), CheckpointLimits::default()).expect("reopen installed state machine");
    assert_eq!(
        sm.get_current_snapshot()
            .await
            .expect("completed durable snapshot")
            .expect("present")
            .meta,
        snapshot.meta
    );
    assert_eq!(
        writer.created_events_page(None, 256).await.expect("installed events"),
        source.page().await
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_snapshot_install_power_cut_atomic() {
    let mut source = Fixture::new(0);
    source.apply(entries(2)).await;
    let mut snapshot = source.build().await;
    let bytes = snapshot_bytes(&mut snapshot).await;
    let new_events = source.page().await;
    for after_sync in [false, true] {
        let (mut target, backend) = media_target().await;
        let old_events = target.page().await;
        let data = receive(&mut target.sm, &bytes).await;
        let (entered, resume) = backend.pause_sync(1, after_sync);
        let meta = snapshot.meta.clone();
        let mut installing = target.sm.clone();
        let task = tokio::spawn(async move { installing.install_snapshot(&meta, data).await });
        entered.wait();
        backend.cut();
        resume.wait();
        assert!(
            task.await.expect("faulted install task").is_err(),
            "SNAPSHOT_INSTALL_POWER_CUT_NO_FALSE_ACK"
        );
        assert!(
            target.sm.get_current_snapshot().await.is_err(),
            "SNAPSHOT_INSTALL_FENCED_HANDLE_CANNOT_EXPORT"
        );
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
        let writer =
            NativeLogWriter::new(Store::open(directory.path().join("db"), identity).expect("atomic installation recovery"));
        let mut logs = NativeRaftLogStore::new(writer.clone());
        let mut sm = NativeRaftStateMachine::new(writer.clone(), CheckpointLimits::default()).expect("recovered SM");
        let events = writer
            .created_events_page(None, 256)
            .await
            .expect("complete recovered tables");
        let cut = sm.applied_state().await.expect("recovered applied marker").0;
        if events == old_events {
            assert_eq!(cut, Some(id(1).into()), "SNAPSHOT_INSTALL_OLD_TABLES_OLD_MARKER");
            assert!(sm.get_current_snapshot().await.expect("old snapshot selection").is_none());
        } else {
            assert_eq!(events, new_events, "SNAPSHOT_INSTALL_ALL_TABLES_OLD_OR_NEW");
            assert_eq!(cut, Some(id(2).into()), "SNAPSHOT_INSTALL_NEW_TABLES_NEW_MARKER");
            let mut current = sm
                .get_current_snapshot()
                .await
                .expect("new snapshot selection")
                .expect("new snapshot published");
            assert_eq!(current.meta, snapshot.meta);
            assert_eq!(snapshot_bytes(&mut current).await, bytes);
        }
        assert_eq!(writer.durable_identity().await.expect("recovered local identity"), identity);
        assert_eq!(
            logs.read_vote().await.expect("recovered local vote"),
            Some(openraft::Vote::new_committed(9, 1))
        );
        let state = logs.get_log_state().await.expect("coherent recovered log state");
        assert!(
            state
                .last_log_id
                .is_some_and(|last| cut.is_some_and(|applied| last.index >= applied.index)),
            "SNAPSHOT_INSTALL_LOG_STATE_COVERS_APPLIED_CUT"
        );
    }
}

#[tokio::test]
async fn native_snapshot_install_ack_survives_power_cut() {
    let mut source = Fixture::new(0);
    source.apply(entries(2)).await;
    let mut snapshot = source.build().await;
    let bytes = snapshot_bytes(&mut snapshot).await;
    let (mut target, backend) = media_target().await;
    install(&mut target.sm, &mut snapshot).await;
    backend.cut();
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
    let writer = NativeLogWriter::new(
        Store::open(directory.path().join("db"), identity).expect("acknowledged installation survives power cut"),
    );
    let mut logs = NativeRaftLogStore::new(writer.clone());
    let mut sm = NativeRaftStateMachine::new(writer.clone(), CheckpointLimits::default()).expect("reopened durable SM");
    let mut current = sm
        .get_current_snapshot()
        .await
        .expect("durable current snapshot")
        .expect("present");
    assert_eq!(current.meta, snapshot.meta, "SNAPSHOT_INSTALL_ACK_COMPLETE_META_DURABLE");
    assert_eq!(snapshot_bytes(&mut current).await, bytes);
    assert_eq!(
        writer.created_events_page(None, 256).await.expect("durable installed events"),
        source.page().await
    );
    logs.blocking_append(vec![entry(3)]).await.expect("append after ACK recovery");
    sm.apply(vec![entry(3)]).await.expect("apply after ACK recovery");
    assert_eq!(sm.applied_state().await.expect("continued state").0, Some(id(3).into()));
}

#[tokio::test]
async fn native_snapshot_install_purge_without_pending_rejected() {
    let mut target = Fixture::new(1);
    target.apply(entries(1)).await;
    let before = target.page().await;
    let bytes = std::fs::read(target.directory.path().join("db")).expect("old unauthorized purge bytes");
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), target.logs.purge(id(3).into()))
        .await
        .expect("no pending receiver means purge cannot wait for hypothetical installation");
    assert!(result.is_err(), "SNAPSHOT_PURGE_WITHOUT_PENDING_FAILS_IMMEDIATELY");
    assert_eq!(target.page().await, before);
    assert_eq!(
        std::fs::read(target.directory.path().join("db")).expect("rejected purge preserves bytes"),
        bytes
    );
}

async fn wait_for_pending_purge(writer: &NativeLogWriter) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while writer.snapshot_receives.waiters.load(std::sync::atomic::Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual trait purge reached pending-receive wait");
}

#[tokio::test]
async fn native_snapshot_install_pending_purge_waits_for_install() {
    let mut source = Fixture::new(0);
    source.apply(entries(3)).await;
    let mut snapshot = source.build().await;
    let bytes = snapshot_bytes(&mut snapshot).await;
    let mut target = Fixture::new(1);
    target.apply(entries(1)).await;
    let data = receive(&mut target.sm, &bytes).await;
    let mut purging = target.logs.clone();
    let purge = tokio::spawn(async move { purging.purge(id(3).into()).await });
    wait_for_pending_purge(&target.writer).await;
    assert!(!purge.is_finished(), "SNAPSHOT_PURGE_WAITS_FOR_INCOMING_CUT_BEYOND_LOCAL_TAIL");
    target
        .sm
        .install_snapshot(&snapshot.meta, data)
        .await
        .expect("actual install while OpenRaft-style purge waits");
    tokio::time::timeout(std::time::Duration::from_secs(5), purge)
        .await
        .expect("pending purge wakes after install")
        .expect("purge task")
        .expect("installed checkpoint now authorizes exact purge");
    assert_eq!(
        target
            .logs
            .get_log_state()
            .await
            .expect("installed log state")
            .last_purged_log_id,
        Some(id(3).into()),
        "SNAPSHOT_PENDING_PURGE_USES_DURABLE_INSTALLED_CUT"
    );
    assert_eq!(target.page().await, source.page().await);
    assert_eq!(
        target
            .writer
            .snapshot_receives
            .waiters
            .load(std::sync::atomic::Ordering::Acquire),
        0,
        "SNAPSHOT_PURGE_WAIT_OWNER_RELEASED"
    );
}

#[tokio::test]
async fn native_snapshot_install_cancelled_receive_wakes_purge() {
    let mut target = Fixture::new(1);
    target.apply(entries(1)).await;
    let page = target.page().await;
    let before = std::fs::read(target.directory.path().join("db")).expect("stable target before cancelled receive");
    let data = target.sm.begin_receiving_snapshot().await.expect("actual pending receiver");
    let mut purging = target.logs.clone();
    let purge = tokio::spawn(async move { purging.purge(id(3).into()).await });
    wait_for_pending_purge(&target.writer).await;
    assert!(!purge.is_finished(), "SNAPSHOT_CANCELLED_RECEIVE_TEST_HAS_REAL_WAITING_PURGE");
    drop(data);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), purge)
            .await
            .expect("cancelled receive wakes pending purge")
            .expect("purge task")
            .is_err(),
        "SNAPSHOT_CANCELLED_RECEIVE_CANNOT_AUTHORIZE_PURGE"
    );
    assert_eq!(target.page().await, page);
    assert_eq!(
        std::fs::read(target.directory.path().join("db")).expect("retained cancelled-receive state"),
        before
    );
    assert_eq!(
        target
            .writer
            .snapshot_receives
            .waiters
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
}

#[tokio::test]
async fn native_snapshot_install_failed_receive_wakes_purge() {
    let mut source = Fixture::new(0);
    source.apply(entries(3)).await;
    let mut snapshot = source.build().await;
    let bytes = snapshot_bytes(&mut snapshot).await;
    let mut target = Fixture::new(1);
    target.apply(entries(1)).await;
    let page = target.page().await;
    let before = std::fs::read(target.directory.path().join("db")).expect("stable target before failed install");
    let data = receive(&mut target.sm, &bytes).await;
    let mut purging = target.logs.clone();
    let purge = tokio::spawn(async move { purging.purge(id(3).into()).await });
    wait_for_pending_purge(&target.writer).await;
    assert!(!purge.is_finished(), "SNAPSHOT_FAILED_INSTALL_TEST_HAS_REAL_WAITING_PURGE");
    let mut wrong = snapshot.meta.clone();
    wrong.last_log_id = Some(LogId { term: 9, ..id(3) }.into());
    assert!(
        target.sm.install_snapshot(&wrong, data).await.is_err(),
        "SNAPSHOT_INVALID_META_INSTALL_REJECTED"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), purge)
            .await
            .expect("failed install wakes pending purge")
            .expect("purge task")
            .is_err(),
        "SNAPSHOT_FAILED_INSTALL_CANNOT_AUTHORIZE_PURGE"
    );
    assert_eq!(target.page().await, page);
    assert_eq!(
        std::fs::read(target.directory.path().join("db")).expect("retained failed-install state"),
        before
    );
    assert_eq!(
        target
            .writer
            .snapshot_receives
            .waiters
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
}

#[tokio::test]
async fn native_snapshot_install_completed_between_purge_query_and_wait() {
    let mut source = Fixture::new(0);
    source.apply(entries(2)).await;
    let mut snapshot = source.build().await;
    let data_bytes = snapshot_bytes(&mut snapshot).await;
    let mut target = Fixture::new(1);
    let data = receive(&mut target.sm, &data_bytes).await;
    let entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let resume = std::sync::Arc::new(tokio::sync::Notify::new());
    *target.writer.snapshot_receives.purge_recheck.lock().expect("test pause") = Some((entered.clone(), resume.clone()));
    let mut logs = target.logs.clone();
    let purge = tokio::spawn(async move { logs.purge(id(2).into()).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .expect("purge failed coverage under owner before continuation");
    target
        .sm
        .install_snapshot(&snapshot.meta, data)
        .await
        .expect("install completes before purge continuation");
    resume.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(5), purge)
        .await
        .expect("completed install wake retained")
        .expect("purge task")
        .expect("old StaleCheckpoint rechecked after install");
    assert_eq!(
        target.page().await,
        source.page().await,
        "SNAPSHOT_COMPLETED_INSTALL_RECHECKS_STALE_PURGE"
    );
}

#[tokio::test]
async fn native_snapshot_install_empty_baseline_pending_purge() {
    let mut source = Fixture::new(0);
    source.apply(entries(2)).await;
    let mut snapshot = source.build().await;
    let bytes = snapshot_bytes(&mut snapshot).await;
    let mut target = Fixture::new(1);
    target.sm.build_snapshot().await.expect("real local empty baseline");
    let data = receive(&mut target.sm, &bytes).await;
    let mut logs = target.logs.clone();
    let purge = tokio::spawn(async move { logs.purge(id(2).into()).await });
    wait_for_pending_purge(&target.writer).await;
    target
        .sm
        .install_snapshot(&snapshot.meta, data)
        .await
        .expect("install supersedes empty baseline");
    tokio::time::timeout(std::time::Duration::from_secs(5), purge)
        .await
        .expect("empty coverage wake")
        .expect("purge task")
        .expect("new covered purge");
    assert_eq!(
        target.page().await,
        source.page().await,
        "SNAPSHOT_EMPTY_BASELINE_IS_UNCOVERED_NOT_CORRUPT"
    );
}
