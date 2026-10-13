//! Real NativeLogWriter and socket owner is this test process, not a production S3 owner.
use crate::{source_reader::*, *};
use std::{
    collections::{BTreeMap, BTreeSet},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::PathBuf,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
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
fn id(index: u64) -> LogId {
    LogId {
        term: 8,
        leader_node: 0,
        index,
    }
}
fn command(event: u8) -> DecideCreated {
    DecideCreated {
        operation_id: [event; 16],
        object: ObjectIdentity {
            bucket_incarnation: [2; 16],
            key: vec![event],
        },
        expected_head: None,
        prepared: PreparedIdentity {
            version_id: [event; 16],
            preparation_id: [4; 16],
            content_digest: [5; 32],
            content_length: 123,
        },
        binding_revision: 9,
    }
}
fn fixture() -> (tempfile::TempDir, NativeLogWriter) {
    let dir = tempfile::tempdir().expect("real source reader directory");
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).expect("private owner directory");
    let mut store = Store::initialize(dir.path().join("db"), identity()).expect("initialize real redb");
    store.pin_capture_binding(binding()).expect("binding");
    store.enable_native_empty().expect("native schema");
    store.promote_created_provenance().expect("provenance");
    let membership = CaptureMembership::new(
        Some(id(0)),
        vec![BTreeSet::from([0])],
        BTreeMap::from([(0, CaptureNode::new("http://127.0.0.1:7000".into()).expect("member"))]),
    )
    .expect("membership");
    let membership = openraft::StoredMembership::try_from(&membership).expect("stored membership");
    store
        .append_native(&[CaptureRaftEntry {
            log_id: id(0).into(),
            payload: openraft::EntryPayload::Membership(membership.membership().clone()),
        }])
        .expect("membership log");
    store.save_committed(id(0)).expect("membership commit");
    store.apply_next_native(id(0)).expect("membership apply");
    (dir, NativeLogWriter::new(store))
}
async fn apply(writer: &NativeLogWriter, index: u64, event: u8) {
    let entry = CaptureRaftEntry {
        log_id: id(index).into(),
        payload: openraft::EntryPayload::Normal(CaptureCommand::try_created(command(event)).expect("canonical native command")),
    };
    writer
        .append_native(vec![entry.clone()])
        .await
        .expect("live durable log append");
    let response = writer.apply_committed(vec![entry]).await.expect("live committed apply");
    assert!(
        matches!(response[0].created_result(), Some(CreatedResult::Created { .. })),
        "SOURCE_READER_REAL_APPLY"
    );
}
fn start(
    dir: &tempfile::TempDir,
    writer: &NativeLogWriter,
) -> (PathBuf, SourceReaderClient, Shutdown, tokio::task::JoinHandle<Result<(), ReaderError>>) {
    let path = dir.path().join("reader.sock");
    let (server, shutdown) = SourceReaderServer::bind(&path, writer.clone()).expect("owner socket bind");
    let client = SourceReaderClient::new(&path).expect("trusted local client");
    let task = tokio::spawn(server.serve());
    (path, client, shutdown, task)
}
async fn stop(path: PathBuf, shutdown: Shutdown, task: tokio::task::JoinHandle<Result<(), ReaderError>>) {
    shutdown.shutdown();
    task.await.expect("serve task").expect("clean shutdown");
    assert!(!path.exists(), "SOURCE_READER_SOCKET_CLEANUP");
}
async fn raw(path: &std::path::Path, value: serde_json::Value) -> ReadPageReply {
    let mut stream = UnixStream::connect(path).await.expect("real socket connect");
    let bytes = serde_json::to_vec(&value).expect("wire request");
    stream
        .write_u32(bytes.len().try_into().expect("request length"))
        .await
        .expect("frame prefix");
    stream.write_all(&bytes).await.expect("frame bytes");
    let len = stream.read_u32().await.expect("reply prefix");
    let mut bytes = vec![0; usize::try_from(len).expect("reply length")];
    stream.read_exact(&mut bytes).await.expect("reply bytes");
    serde_json::from_slice(&bytes).expect("strict native reply")
}
fn request() -> serde_json::Value {
    serde_json::to_value(ReadPage {
        version: WIRE_VERSION,
        expected_cluster: identity().cluster_id,
        cursor: None,
        limit: 2,
    })
    .expect("native request")
}
#[tokio::test]
async fn source_reader_live_apply() {
    let (dir, writer) = fixture();
    let (path, client, shutdown, task) = start(&dir, &writer);
    assert_eq!(writer.durable_identity().await.expect("durable META identity"), identity());
    assert!(
        client
            .page(identity().cluster_id, None, 2)
            .await
            .expect("empty live page")
            .page
            .events
            .is_empty()
    );
    apply(&writer, 1, 9).await;
    let response = client
        .page(identity().cluster_id, None, 2)
        .await
        .expect("live committed page");
    assert_eq!(response.identity, identity(), "SOURCE_READER_DURABLE_IDENTITY");
    assert_eq!(response.page.events[0].event_id, [9; 16], "SOURCE_READER_LIVE_EVENT");
    assert_eq!(response.page.events[0].captured.binding, binding());
    {
        let mut corrupt = writer.store.lock().expect("test owner");
        let tx = corrupt.write_transaction().expect("corrupt META transaction");
        tx.open_table(META)
            .expect("META")
            .insert("identity", &[0; 24][..])
            .expect("corrupt identity");
        corrupt.commit(tx).expect("persist corruption");
    }
    assert!(writer.durable_identity().await.is_err(), "SOURCE_READER_META_CORRUPTION_REJECTED");
    assert!(
        matches!(
            client.page(identity().cluster_id, None, 2).await,
            Err(ReaderError::Rejected(Rejection::StoreUnavailable))
        ),
        "SOURCE_READER_NO_CALLER_IDENTITY_SUBSTITUTION"
    );
    stop(path, shutdown, task).await;
}
#[tokio::test]
async fn source_reader_restart_cursor() {
    let (dir, writer) = fixture();
    apply(&writer, 1, 9).await;
    apply(&writer, 2, 7).await;
    let (path, client, shutdown, task) = start(&dir, &writer);
    let first = client.page(identity().cluster_id, None, 1).await.expect("first");
    stop(path, shutdown, task).await;
    drop(writer);
    let writer = NativeLogWriter::new(Store::open(dir.path().join("db"), identity()).expect("restart same durable store"));
    let (path, client, shutdown, task) = start(&dir, &writer);
    let next = client
        .page(identity().cluster_id, first.page.next, 1)
        .await
        .expect("resume durable cursor");
    assert_eq!(next.page.events[0].event_id, [7; 16], "SOURCE_READER_RESTART_CONTINUES");
    stop(path, shutdown, task).await;
}
#[tokio::test]
async fn source_reader_page_boundaries() {
    let (dir, writer) = fixture();
    for (index, event) in [(1, 9), (2, 7), (3, 1)] {
        apply(&writer, index, event).await;
    }
    let (path, client, shutdown, task) = start(&dir, &writer);
    let first = client.page(identity().cluster_id, None, 2).await.expect("first two");
    assert_eq!(
        first.page.events.iter().map(|e| e.event_id).collect::<Vec<_>>(),
        vec![[9; 16], [7; 16]],
        "SOURCE_READER_DECISION_ORDER"
    );
    let next = client
        .page(identity().cluster_id, first.page.next, 2)
        .await
        .expect("remaining");
    assert_eq!(next.page.events.len(), 1);
    let end = client
        .page(identity().cluster_id, next.page.next.clone(), 2)
        .await
        .expect("end");
    assert!(end.page.events.is_empty());
    assert_eq!(end.page.next, next.page.next, "SOURCE_READER_CURSOR_RETAINED_AT_END");
    for limit in [0, MAX_EVENT_PAGE + 1] {
        assert!(
            matches!(
                client.page(identity().cluster_id, None, limit).await,
                Err(ReaderError::Rejected(Rejection::InvalidRequest))
            ),
            "SOURCE_READER_LIMIT_REJECTED"
        );
    }
    stop(path, shutdown, task).await;
}
#[tokio::test]
async fn source_reader_wrong_identity() {
    let (dir, writer) = fixture();
    apply(&writer, 1, 9).await;
    let (path, client, shutdown, task) = start(&dir, &writer);
    let mut wrong = request();
    wrong["expected_cluster"] = serde_json::to_value([6; 16]).expect("wrong cluster");
    assert!(
        matches!(
            raw(&path, wrong).await,
            ReadPageReply::Rejected {
                error: Rejection::WrongCluster,
                ..
            }
        ),
        "SOURCE_READER_CLUSTER: reject before reading or returning foreign events"
    );
    let first = client.page(identity().cluster_id, None, 1).await.expect("real cursor");
    let mut wrong = serde_json::to_value(first.page.next.expect("cursor")).expect("cursor wire");
    wrong["event"] = serde_json::to_value([1; 16]).expect("wrong event");
    let wrong: EventCursor = serde_json::from_value(wrong).expect("syntactically valid foreign cursor");
    assert!(
        matches!(
            client.page(identity().cluster_id, Some(wrong), 1).await,
            Err(ReaderError::Rejected(Rejection::InvalidCursor))
        ),
        "SOURCE_READER_CURSOR_BOUND"
    );
    for (field, value) in [
        ("version", serde_json::json!(2)),
        ("unknown", serde_json::json!(true)),
        ("cursor", serde_json::json!({"unknown":true})),
    ] {
        let mut wrong = request();
        wrong[field] = value;
        assert!(
            matches!(
                raw(&path, wrong).await,
                ReadPageReply::Rejected {
                    error: Rejection::InvalidRequest,
                    ..
                }
            ),
            "SOURCE_READER_STRICT_REQUEST"
        );
    }
    stop(path, shutdown, task).await;
}
#[tokio::test]
async fn source_reader_socket_ownership() {
    let (dir, writer) = fixture();
    let path = dir.path().join("reader.sock");
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o750)).expect("insecure directory");
    assert!(
        matches!(SourceReaderServer::bind(&path, writer.clone()), Err(ReaderError::Permission)),
        "SOURCE_READER_PRIVATE_DIRECTORY"
    );
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).expect("restore private directory");
    std::fs::write(&path, b"preserve me").expect("occupied file");
    assert!(
        matches!(SourceReaderServer::bind(&path, writer.clone()), Err(ReaderError::Occupied)),
        "SOURCE_READER_OCCUPIED_PATH"
    );
    assert_eq!(std::fs::read(&path).expect("unchanged file"), b"preserve me");
    std::fs::remove_file(&path).expect("remove own fixture");
    std::os::unix::fs::symlink(dir.path().join("missing"), &path).expect("unknown symlink");
    assert!(matches!(SourceReaderServer::bind(&path, writer.clone()), Err(ReaderError::Occupied)));
    assert!(SourceReaderClient::new(&path).is_err());
    std::fs::remove_file(&path).expect("remove own symlink");
    let long = dir.path().join("x".repeat(300));
    assert!(SourceReaderServer::bind(&long, writer.clone()).is_err(), "SOURCE_READER_LONG_SOCKET_PATH");
    let parent_link = dir.path().join("parent-link");
    std::os::unix::fs::symlink(dir.path(), &parent_link).expect("parent symlink");
    assert!(
        matches!(
            SourceReaderServer::bind(parent_link.join("socket"), writer.clone()),
            Err(ReaderError::Permission)
        ),
        "SOURCE_READER_PARENT_SYMLINK_DENIED"
    );
    let (path, client, shutdown, task) = start(&dir, &writer);
    let meta = std::fs::symlink_metadata(&path).expect("socket metadata");
    assert_eq!(meta.mode() & 0o777, 0o600);
    assert_eq!(meta.uid(), rustix::process::geteuid().as_raw());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660)).expect("insecure socket");
    assert!(
        matches!(client.page(identity().cluster_id, None, 1).await, Err(ReaderError::Permission)),
        "SOURCE_READER_CLIENT_VALIDATES_SOCKET"
    );
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("restore socket");
    std::fs::remove_file(&path).expect("test owner replaces socket");
    std::fs::write(&path, b"replacement").expect("replacement file");
    shutdown.shutdown();
    assert!(task.await.expect("serve exit").is_err(), "SOURCE_READER_REPLACEMENT_CLEANUP_REJECTED");
    assert_eq!(std::fs::read(&path).expect("replacement preserved"), b"replacement");
}
#[tokio::test]
async fn source_reader_transport_bounds() {
    let (dir, writer) = fixture();
    let (path, client, shutdown, task) = start(&dir, &writer);
    let mut stream = UnixStream::connect(&path).await.expect("oversized input peer");
    stream
        .write_u32(u32::try_from(MAX_REQUEST_BYTES + 1).expect("length"))
        .await
        .expect("overlong prefix");
    let mut byte = [0];
    assert_eq!(
        stream.read(&mut byte).await.expect("bounded rejection closes"),
        0,
        "SOURCE_READER_INPUT_BOUND"
    );
    let mut idle = UnixStream::connect(&path).await.expect("idle connection");
    let start = std::time::Instant::now();
    assert_eq!(
        idle.read(&mut byte).await.expect("deadline closes connection"),
        0,
        "SOURCE_READER_READ_DEADLINE"
    );
    assert!(start.elapsed() >= IO_TIMEOUT, "SOURCE_READER_REAL_READ_DEADLINE");
    assert!(
        client.page(identity().cluster_id, None, 1).await.is_ok(),
        "SOURCE_READER_TIMEOUT_RELEASES_CONNECTION"
    );
    stop(path.clone(), shutdown, task).await;
    // A same-UID peer still cannot supply unbounded responses to the client.
    let listener = tokio::net::UnixListener::bind(&path).expect("controlled response peer");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("private response socket");
    let client = SourceReaderClient::new(&path).expect("response client");
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("client connects");
        stream
            .write_u32(u32::try_from(MAX_RESPONSE_BYTES + 1).expect("oversized response"))
            .await
            .expect("response prefix");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    });
    assert!(
        matches!(client.page(identity().cluster_id, None, 1).await, Err(ReaderError::Protocol)),
        "SOURCE_READER_RESPONSE_BOUND"
    );
    peer.await.expect("peer task");
}
fn hold_store(writer: &NativeLogWriter) -> (std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>) {
    let owner = writer.clone();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        let _store = owner.store.lock().expect("actual Store held by test owner");
        locked_tx.send(()).expect("lock visible");
        let _ = release_rx.recv();
    });
    locked_rx.recv().expect("store locked");
    (release_tx, thread)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_reader_shutdown() {
    let (dir, writer) = fixture();
    apply(&writer, 1, 9).await;
    let (path, _client, shutdown, task) = start(&dir, &writer);
    let (release_tx, blocker) = hold_store(&writer);
    let mut streams = Vec::new();
    for _ in 0..MAX_CONNECTIONS {
        let mut stream = UnixStream::connect(&path).await.expect("active read socket");
        let bytes = serde_json::to_vec(&request()).expect("request");
        stream
            .write_u32(bytes.len().try_into().expect("length"))
            .await
            .expect("prefix");
        stream.write_all(&bytes).await.expect("request bytes");
        streams.push(stream);
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while writer.source_reader_admission.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all admissions active");
    drop(streams);
    tokio::time::sleep(IO_TIMEOUT + std::time::Duration::from_millis(100)).await;
    assert_eq!(
        writer.source_reader_admission.available_permits(),
        0,
        "SOURCE_READER_CLIENT_TIMEOUT_KEEPS_WORKER_ADMISSION"
    );
    shutdown.shutdown();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!task.is_finished(), "SOURCE_READER_SHUTDOWN_DRAINS_READERS");
    assert_eq!(
        writer.source_reader_admission.available_permits(),
        0,
        "SOURCE_READER_CANCELLED_CLIENTS_KEEP_ADMISSION"
    );
    release_tx.send(()).expect("release real blocking reads");
    blocker.join().expect("test lock owner exits");
    task.await.expect("serve completes").expect("shutdown clean");
    assert!(!path.exists());
    assert_eq!(
        writer.source_reader_admission.available_permits(),
        MAX_CONNECTIONS,
        "SOURCE_READER_ADMISSION_RELEASED_AFTER_WORKERS"
    );
    let (release, blocker) = hold_store(&writer);
    let (path, _, _shutdown, task) = start(&dir, &writer);
    let bytes = serde_json::to_vec(&request()).expect("native request");
    let mut sockets = Vec::new();
    for _ in 0..MAX_CONNECTIONS {
        let mut stream = UnixStream::connect(&path).await.expect("cancelled service peer");
        stream
            .write_u32(bytes.len().try_into().expect("length"))
            .await
            .expect("prefix");
        stream.write_all(&bytes).await.expect("request");
        sockets.push(stream);
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while writer.source_reader_admission.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled service admits bounded workers");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    task.abort();
    assert!(task.await.expect_err("serve cancellation").is_cancelled());
    drop(sockets);
    assert_eq!(
        writer.source_reader_admission.available_permits(),
        0,
        "SOURCE_READER_CANCELLED_SERVE_RETAINS_WORKER_ADMISSION"
    );
    let (path, client, shutdown, task) = start(&dir, &writer);
    let pending = tokio::spawn(async move { client.page(identity().cluster_id, None, 1).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!pending.is_finished(), "SOURCE_READER_REBIND_CANNOT_BYPASS_ADMISSION");
    assert_eq!(writer.source_reader_admission.available_permits(), 0);
    release.send(()).expect("release cancelled worker store lock");
    blocker.join().expect("lock owner completes");
    assert_eq!(
        pending
            .await
            .expect("new service client")
            .expect("new service waits for old workers")
            .page
            .events
            .len(),
        1
    );
    stop(path, shutdown, task).await;
}
#[tokio::test]
async fn source_reader_checkpoint_replay() {
    let (dir, writer) = fixture();
    apply(&writer, 1, 9).await;
    apply(&writer, 2, 7).await;
    let (path, client, shutdown, task) = start(&dir, &writer);
    let first = client
        .page(identity().cluster_id, None, 1)
        .await
        .expect("pre-checkpoint cursor");
    let checkpoint = writer
        .checkpoint_local(CheckpointLimits::default())
        .await
        .expect("real checkpoint");
    assert_eq!(checkpoint.cut(), id(2));
    let next = client
        .page(identity().cluster_id, first.page.next.clone(), 1)
        .await
        .expect("checkpoint retained events");
    assert_eq!(next.page.events[0].event_id, [7; 16], "SOURCE_READER_CHECKPOINT_REPLAY");
    stop(path, shutdown, task).await;
    drop(writer);
    let writer = NativeLogWriter::new(Store::open(dir.path().join("db"), identity()).expect("reopen checkpointed store"));
    let (path, client, shutdown, task) = start(&dir, &writer);
    assert_eq!(
        client
            .page(identity().cluster_id, first.page.next, 1)
            .await
            .expect("replay after restart"),
        next,
        "SOURCE_READER_CHECKPOINT_RESTART"
    );
    stop(path, shutdown, task).await;
}

struct PublicationFixture {
    writer: NativeLogWriter,
    ready_through: std::sync::atomic::AtomicU64,
    unavailable: std::sync::atomic::AtomicBool,
}
impl PublicationReadiness for PublicationFixture {
    fn check_ready(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ReaderError>> + Send + '_>> {
        Box::pin(async move {
            assert!(self.writer.store.try_lock().is_ok(), "READINESS_CHECK_RELEASES_STORE_LOCK");
            if self.unavailable.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(ReaderError::PublicationUnavailable);
            }
            Ok(())
        })
    }

    fn is_published(
        &self,
        event: SourceEvent,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, ReaderError>> + Send + '_>> {
        Box::pin(async move {
            assert!(self.writer.store.try_lock().is_ok(), "PUBLICATION_CHECK_RELEASES_STORE_LOCK");
            if self.unavailable.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(ReaderError::PublicationUnavailable);
            }
            Ok(event.captured.decision_log_id.index == 3
                || event.captured.decision_log_id.index <= self.ready_through.load(std::sync::atomic::Ordering::SeqCst))
        })
    }
}

#[tokio::test]
async fn source_reader_publication_prefix_resumes_without_skipping() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };
    let (dir, writer) = fixture();
    for (index, event) in [(1, 9), (2, 10), (3, 11)] {
        apply(&writer, index, event).await;
    }
    let publication = Arc::new(PublicationFixture {
        writer: writer.clone(),
        ready_through: AtomicU64::new(0),
        unavailable: AtomicBool::new(false),
    });
    let path = dir.path().join("reader.sock");
    let (server, shutdown) = SourceReaderServer::bind(&path, writer).expect("bind reader");
    let task = tokio::spawn(server.with_publication_readiness(publication.clone()).serve());
    let client = SourceReaderClient::new(&path).expect("reader client");
    let held = client.page(identity().cluster_id, None, 3).await.expect("held page").page;
    assert!(held.events.is_empty(), "UNPUBLISHED_EVENT_MUST_NOT_ESCAPE");
    assert_eq!(held.next, None, "UNPUBLISHED_EVENT_MUST_NOT_ADVANCE_CURSOR");
    publication.ready_through.store(1, Ordering::SeqCst);
    let first = client
        .page(identity().cluster_id, None, 3)
        .await
        .expect("published prefix")
        .page;
    assert_eq!(first.events.iter().map(|event| event.event_id).collect::<Vec<_>>(), vec![[9; 16]]);
    let cursor = first.next.expect("first published cursor");
    let held = client
        .page(identity().cluster_id, Some(cursor.clone()), 3)
        .await
        .expect("held suffix")
        .page;
    assert!(held.events.is_empty());
    assert_eq!(held.next, Some(cursor.clone()), "HELD_SUFFIX_PRESERVES_INPUT_CURSOR");
    publication.ready_through.store(3, Ordering::SeqCst);
    let rest = client
        .page(identity().cluster_id, Some(cursor), 3)
        .await
        .expect("publication recovered")
        .page;
    assert_eq!(
        rest.events.iter().map(|event| event.event_id).collect::<Vec<_>>(),
        vec![[10; 16], [11; 16]],
        "PUBLICATION_RECOVERY_MUST_NOT_SKIP"
    );
    stop(path, shutdown, task).await;
}

#[tokio::test]
async fn source_reader_publication_error_is_retryable_without_cursor_loss() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };
    let (dir, writer) = fixture();
    apply(&writer, 1, 9).await;
    let publication = Arc::new(PublicationFixture {
        writer: writer.clone(),
        ready_through: AtomicU64::new(1),
        unavailable: AtomicBool::new(true),
    });
    let path = dir.path().join("reader.sock");
    let (server, shutdown) = SourceReaderServer::bind(&path, writer).expect("bind reader");
    let task = tokio::spawn(server.with_publication_readiness(publication.clone()).serve());
    let client = SourceReaderClient::new(&path).expect("reader client");
    assert!(
        matches!(
            client.page(identity().cluster_id, None, 3).await,
            Err(ReaderError::Rejected(Rejection::StoreUnavailable))
        ),
        "PUBLICATION_ERROR_MUST_NOT_RELEASE_EVENT"
    );
    publication.unavailable.store(false, Ordering::SeqCst);
    let page = client
        .page(identity().cluster_id, None, 3)
        .await
        .expect("retry publication check")
        .page;
    assert_eq!(page.events[0].event_id, [9; 16]);
    stop(path, shutdown, task).await;
}

#[tokio::test]
async fn source_reader_empty_page_checks_instance_readiness() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };
    let (dir, writer) = fixture();
    let publication = Arc::new(PublicationFixture {
        writer: writer.clone(),
        ready_through: AtomicU64::new(0),
        unavailable: AtomicBool::new(true),
    });
    let path = dir.path().join("reader.sock");
    let (server, shutdown) = SourceReaderServer::bind(&path, writer).expect("bind reader");
    let task = tokio::spawn(server.with_publication_readiness(publication.clone()).serve());
    let client = SourceReaderClient::new(&path).expect("reader client");
    for unavailable in [true, false, true] {
        publication.unavailable.store(unavailable, Ordering::SeqCst);
        let response = client.page(identity().cluster_id, None, 3).await;
        if unavailable {
            assert!(
                matches!(response, Err(ReaderError::Rejected(Rejection::StoreUnavailable))),
                "EMPTY_PAGE_MUST_CHECK_INSTANCE_READINESS"
            );
        } else {
            let page = response.expect("ready empty instance").page;
            assert!(page.events.is_empty());
            assert_eq!(page.next, None);
        }
    }
    stop(path, shutdown, task).await;
}

struct StalledPublication;
impl PublicationReadiness for StalledPublication {
    fn is_published(
        &self,
        _event: SourceEvent,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, ReaderError>> + Send + '_>> {
        Box::pin(std::future::pending())
    }
}
#[tokio::test]
async fn source_reader_publication_timeout_releases_admission_and_drains() {
    let (dir, writer) = fixture();
    apply(&writer, 1, 9).await;
    let path = dir.path().join("reader.sock");
    let (server, shutdown) = SourceReaderServer::bind(&path, writer.clone()).expect("bind reader");
    let task = tokio::spawn(
        server
            .with_publication_readiness(std::sync::Arc::new(StalledPublication))
            .serve(),
    );
    let reply = tokio::time::timeout(IO_TIMEOUT + std::time::Duration::from_secs(2), raw(&path, request()))
        .await
        .expect("publication check is bounded");
    assert!(
        matches!(
            reply,
            ReadPageReply::Rejected {
                error: Rejection::StoreUnavailable,
                ..
            }
        ),
        "STALLED_PUBLICATION_MUST_NOT_RELEASE_EVENT"
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), stop(path, shutdown, task))
        .await
        .expect("reader drains after publication timeout");
    assert_eq!(
        writer.source_reader_admission.available_permits(),
        MAX_CONNECTIONS,
        "PUBLICATION_TIMEOUT_RELEASES_ADMISSION"
    );
}
