//! Startup tests exercise the installed native owner and its real source reader.
use super::*;
use rustfs_capture_authority::{
    CaptureBinding, CaptureCommand, CaptureNode, DecideCreated, NodeIdentity, ObjectIdentity, PreparedIdentity,
    source_reader::SourceReaderClient,
};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
struct DurableCaptureState {
    identity: NodeIdentity,
    binding: Option<CaptureBinding>,
    vote: Option<rustfs_capture_authority::VoteRecord>,
    committed: Option<rustfs_capture_authority::LogId>,
    logs: Vec<rustfs_capture_authority::LogEntry>,
    applied_membership: (Option<rustfs_capture_authority::LogId>, rustfs_capture_authority::CaptureMembership),
    events_cursor: rustfs_capture_authority::EventPage,
    purged: Option<rustfs_capture_authority::LogId>,
    tail: Option<rustfs_capture_authority::LogId>,
}

async fn durable_state(database: &std::path::Path, expected: NodeIdentity) -> DurableCaptureState {
    use openraft::storage::RaftLogStorage;
    let store = Store::open(database, expected).expect("inspect retained store with original identity");
    let binding = store.capture_binding().expect("binding");
    let vote = store.read_vote().expect("vote");
    let committed = store.read_committed().expect("committed");
    let logs = store.read_entries(0, u64::MAX).expect("every retained log payload");
    let applied_membership = store.native_application_state().expect("applied and membership");
    let events_cursor = store
        .created_events_page(None, 256)
        .expect("complete fixture events and cursor");
    let writer = NativeLogWriter::new(store);
    let identity = writer.durable_identity().await.expect("actual durable identity");
    let mut storage = rustfs_capture_authority::NativeRaftLogStore::new(writer.clone());
    let state = storage.get_log_state().await.expect("durable log boundaries");
    DurableCaptureState {
        identity,
        binding,
        vote,
        committed,
        logs,
        applied_membership,
        events_cursor,
        purged: state.last_purged_log_id.map(Into::into),
        tail: state.last_log_id.map(Into::into),
    }
}

fn capture_config(directory: &std::path::Path, node_id: u64) -> config::CaptureConfig {
    config::CaptureConfig {
        identity: NodeIdentity {
            cluster_id: [1; 16],
            node_id,
        },
        database: directory.join("capture.redb"),
        reader_socket: directory.join("capture.sock"),
        peers: BTreeMap::from([(
            node_id,
            CaptureNode::new(format!("http://127.0.0.1:{}", 19000 + node_id)).expect("fixed local peer"),
        )]),
        capture_binding: CaptureBinding {
            revision: 9,
            binding_id: [8; 16],
            target_id: [9; 16],
        },
        mode: config::CaptureMode::Create,
        bootstrap: true,
    }
}

fn command(value: u8) -> CaptureCommand {
    CaptureCommand::try_created(DecideCreated {
        operation_id: [value; 16],
        object: ObjectIdentity {
            bucket_incarnation: [2; 16],
            key: vec![value],
        },
        expected_head: None,
        prepared: PreparedIdentity {
            version_id: [value; 16],
            preparation_id: [4; 16],
            content_digest: [5; 32],
            content_length: 123,
        },
        binding_revision: 9,
    })
    .expect("real native command")
}

fn private_directory() -> tempfile::TempDir {
    let _ = rustfs_credentials::set_global_rpc_secret("native-capture-startup-test-secret".into());
    rustfs_credentials::try_get_rpc_token().expect("configured internal RPC secret");
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().expect("startup storage directory");
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).expect("private reader parent");
    directory
}

fn server_config(capture: &config::CaptureConfig) -> crate::config::Config {
    let path = capture.database.with_extension("json");
    let value = serde_json::json!({
        "identity": capture.identity,
        "database": capture.database,
        "reader_socket": capture.reader_socket,
        "peers": capture.peers,
        "capture_binding": capture.capture_binding,
        "mode": match capture.mode { config::CaptureMode::Create => "create", config::CaptureMode::Recover => "recover" },
        "bootstrap": capture.bootstrap,
    });
    std::fs::write(&path, serde_json::to_vec(&value).expect("capture JSON")).expect("write configuration");
    let mut config = crate::config::Config::new(format!("127.0.0.1:{}", 19000 + capture.identity.node_id), Vec::new());
    config.native_capture_config = Some(path);
    config
}

async fn leader(runtime: &NativeCaptureRuntime) {
    let mut metrics = runtime.owner().metrics();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if metrics.borrow().current_leader == Some(runtime.owner().identity().node_id) {
                break;
            }
            metrics.changed().await.expect("live owner metrics");
        }
    })
    .await
    .expect("real single-node election");
}

#[tokio::test]
async fn native_capture_startup_disabled() {
    let directory = private_directory();
    let config = crate::config::Config::new("127.0.0.1:19000", Vec::new());
    let slot = ServerContextSlot::new();
    assert!(start_capture(&config, &slot).await.expect("disabled startup").is_none());
    assert!(slot.installed_capture_service().is_none());
    assert_eq!(std::fs::read_dir(directory.path()).expect("directory").count(), 0);
}

#[tokio::test]
async fn native_capture_startup_create_recover() {
    let directory = private_directory();
    let mut capture = capture_config(directory.path(), 0);
    let slot = ServerContextSlot::new();
    let mut runtime = start_capture(&server_config(&capture), &slot)
        .await
        .expect("create")
        .expect("enabled");
    runtime.finish_startup("127.0.0.1:19000").await.expect("bootstrap");
    leader(&runtime).await;
    let result = runtime.owner().client_write(command(1)).await.expect("actual Raft write");
    let client = SourceReaderClient::new(&capture.reader_socket).expect("shared source reader");
    let before = client
        .page(capture.identity.cluster_id, None, 1)
        .await
        .expect("committed page");
    assert_eq!(before.identity, capture.identity);
    assert_eq!(before.page.events.len(), 1);
    assert_eq!(
        before.page.events[0].captured.decision_log_id,
        rustfs_capture_authority::LogId::from(result.log_id)
    );
    runtime
        .owner()
        .raft()
        .trigger()
        .snapshot()
        .await
        .expect("actual startup owner snapshot trigger");
    runtime
        .owner()
        .raft()
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| metrics.snapshot.is_some_and(|cut| cut.index >= result.log_id.index),
            "startup create produces a real durable snapshot",
        )
        .await
        .expect("NATIVE_CAPTURE_STARTUP_PROVENANCE_SUPPORTS_SNAPSHOT");
    runtime.shutdown().await.expect("full shutdown");
    drop(slot);
    capture.mode = config::CaptureMode::Recover;
    capture.bootstrap = false;
    let slot = ServerContextSlot::new();
    let mut runtime = start_capture(&server_config(&capture), &slot)
        .await
        .expect("recover")
        .expect("enabled");
    runtime
        .finish_startup("127.0.0.1:19000")
        .await
        .expect("recover without bootstrap");
    let client = SourceReaderClient::new(&capture.reader_socket).expect("recovered reader");
    assert_eq!(
        client
            .page(capture.identity.cluster_id, None, 1)
            .await
            .expect("retained page"),
        before
    );
    leader(&runtime).await;
    runtime.owner().client_write(command(1)).await.expect("idempotent real write");
    assert_eq!(
        client
            .page(capture.identity.cluster_id, None, 10)
            .await
            .expect("deduplicated page")
            .page
            .events
            .len(),
        1
    );
    runtime.shutdown().await.expect("recovered shutdown");
}

#[tokio::test]
async fn native_capture_startup_rejects_invalid_identity() {
    let directory = private_directory();
    let mut capture = capture_config(directory.path(), 0);
    let slot = ServerContextSlot::new();
    let mut runtime = start_capture(&server_config(&capture), &slot)
        .await
        .expect("create")
        .expect("enabled");
    runtime.finish_startup("127.0.0.1:19000").await.expect("bootstrap");
    leader(&runtime).await;
    runtime.owner().client_write(command(2)).await.expect("retained native event");
    runtime.shutdown().await.expect("shutdown");
    drop(slot);
    let original_identity = capture.identity;
    let stable = durable_state(&capture.database, original_identity).await;
    capture.mode = config::CaptureMode::Recover;
    capture.bootstrap = false;
    let vote_before = Store::open(&capture.database, capture.identity)
        .expect("retained identity")
        .read_vote()
        .expect("retained vote");
    let pending_slot = ServerContextSlot::new();
    let mut pending = start_capture(&server_config(&capture), &pending_slot)
        .await
        .expect("recover before audience publication")
        .expect("enabled");
    assert!(pending.owner.is_none(), "audience validation must precede actual Raft construction");
    let defaults = openraft::Config::default();
    tokio::time::sleep(Duration::from_millis(defaults.election_timeout_max + 2 * defaults.heartbeat_interval)).await;
    use rustfs_protos::proto_gen::node_service::capture_raft_service_server::CaptureRaftService;
    let pending_service = pending_slot.installed_capture_service().expect("installed early holder");
    let error = pending_service
        .vote(tonic::Request::new(rustfs_protos::proto_gen::node_service::CaptureRaftRequest {
            body: bytes::Bytes::new(),
        }))
        .await
        .expect_err("RPC admission must wait for actual Ping identity");
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(pending.finish_startup("127.0.0.1:19001").await.is_err());
    pending.shutdown().await.expect("wrong audience recovery cleanup");
    assert_eq!(durable_state(&capture.database, original_identity).await, stable);
    assert_eq!(
        Store::open(&capture.database, capture.identity)
            .expect("immediate reopen")
            .read_vote()
            .expect("vote"),
        vote_before
    );
    capture.identity.cluster_id = [7; 16];
    let slot = ServerContextSlot::new();
    assert!(
        start_capture(&server_config(&capture), &slot).await.is_err(),
        "STARTUP_RECOVER_REQUIRES_DURABLE_IDENTITY"
    );
    assert_eq!(durable_state(&capture.database, original_identity).await, stable);
    assert!(slot.installed_capture_service().is_none());
    capture.identity.cluster_id = [1; 16];
    capture.capture_binding.binding_id = [7; 16];
    assert!(
        start_capture(&server_config(&capture), &slot).await.is_err(),
        "STARTUP_RECOVER_REQUIRES_DURABLE_BINDING"
    );
    assert_eq!(durable_state(&capture.database, original_identity).await, stable);
    assert!(slot.installed_capture_service().is_none());
    capture.capture_binding.binding_id = [8; 16];

    let foreign_listener = std::os::unix::net::UnixListener::bind(&capture.reader_socket).expect("foreign socket");
    use std::os::unix::fs::MetadataExt;
    let foreign_inode = std::fs::symlink_metadata(&capture.reader_socket)
        .expect("foreign socket metadata")
        .ino();
    assert!(start_capture(&server_config(&capture), &slot).await.is_err(), "occupied reader socket");
    assert_eq!(durable_state(&capture.database, original_identity).await, stable);
    assert_eq!(
        std::fs::symlink_metadata(&capture.reader_socket)
            .expect("foreign socket preserved")
            .ino(),
        foreign_inode
    );
    assert!(slot.installed_capture_service().is_none());
    drop(foreign_listener);
    std::fs::remove_file(&capture.reader_socket).expect("remove our foreign socket");

    capture.database = directory.path().join("missing.redb");
    assert!(start_capture(&server_config(&capture), &slot).await.is_err());
    assert!(!capture.database.exists());

    capture.mode = config::CaptureMode::Create;
    let config = server_config(&capture);
    let config_path = config.native_capture_config.as_ref().expect("configuration path");
    let mut invalid: serde_json::Value =
        serde_json::from_slice(&std::fs::read(config_path).expect("configuration bytes")).expect("configuration JSON");
    invalid["unexpected"] = serde_json::json!(true);
    std::fs::write(config_path, serde_json::to_vec(&invalid).expect("unknown field JSON")).expect("write unknown field");
    assert!(start_capture(&config, &slot).await.is_err(), "strict configuration rejects unknown field");
    assert!(!capture.database.exists());
    assert!(!capture.reader_socket.exists());
    invalid.as_object_mut().expect("configuration object").remove("unexpected");
    invalid["peers"]["0"]["rpc_uri"] = serde_json::json!("http://127.0.0.1:0");
    std::fs::write(config_path, serde_json::to_vec(&invalid).expect("port zero JSON")).expect("write port zero");
    assert!(start_capture(&config, &slot).await.is_err(), "fixed peer port required");
    assert!(!capture.database.exists());
    assert!(!capture.reader_socket.exists());
    assert_eq!(durable_state(&directory.path().join("capture.redb"), original_identity).await, stable);
}

#[tokio::test]
async fn native_capture_startup_failure_drains_owner() {
    let directory = private_directory();
    let mut capture = capture_config(directory.path(), 0);
    let slot = ServerContextSlot::new();
    let mut runtime = start_capture(&server_config(&capture), &slot)
        .await
        .expect("create")
        .expect("enabled");
    runtime
        .finish_startup("127.0.0.1:19000")
        .await
        .expect("actual owner started before later failure");
    assert!(runtime.owner.is_some());
    assert!(
        runtime.finish_startup("127.0.0.1:19001").await.is_err(),
        "startup must reject wrong Ping audience"
    );
    runtime.shutdown().await.expect("startup error cleanup");
    drop(slot);
    assert!(!capture.reader_socket.exists());
    capture.mode = config::CaptureMode::Recover;
    capture.bootstrap = false;
    let slot = ServerContextSlot::new();
    let runtime = start_capture(&server_config(&capture), &slot)
        .await
        .expect("immediate same-identity reopen")
        .expect("enabled");
    runtime.shutdown().await.expect("shutdown reopened runtime");
}

#[tokio::test]
async fn native_capture_startup_instance_isolation() {
    let first_directory = private_directory();
    let second_directory = private_directory();
    let first_config = capture_config(first_directory.path(), 0);
    let mut second_config = capture_config(second_directory.path(), 1);
    second_config.identity.cluster_id = [6; 16];
    let first = ServerContextSlot::new();
    let second = ServerContextSlot::new();
    let empty = ServerContextSlot::new();
    assert!(first.installed_capture_service().is_none());
    let mut a = start_capture(&server_config(&first_config), &first)
        .await
        .expect("first")
        .expect("enabled");
    let mut b = start_capture(&server_config(&second_config), &second)
        .await
        .expect("second")
        .expect("enabled");
    a.finish_startup("127.0.0.1:19000").await.expect("first real owner");
    b.finish_startup("127.0.0.1:19001").await.expect("second real owner");
    let a_service = first.installed_capture_service().expect("first service");
    let b_service = second.installed_capture_service().expect("second service");
    assert!(!Arc::ptr_eq(&a_service, &b_service));
    assert!(empty.installed_capture_service().is_none());
    let a_client = SourceReaderClient::new(&first_config.reader_socket).expect("first reader");
    let b_client = SourceReaderClient::new(&second_config.reader_socket).expect("second reader");
    assert_eq!(
        a_client
            .page(first_config.identity.cluster_id, None, 1)
            .await
            .expect("first page")
            .identity,
        first_config.identity
    );
    assert_eq!(
        b_client
            .page(second_config.identity.cluster_id, None, 1)
            .await
            .expect("second page")
            .identity,
        second_config.identity
    );
    assert!(a_client.page(second_config.identity.cluster_id, None, 1).await.is_err());
    a.shutdown().await.expect("first shutdown");
    let reopened = Store::open(&first_config.database, first_config.identity)
        .expect("idle HTTP service holder must release real Store after shutdown");
    drop(reopened);
    use rustfs_protos::proto_gen::node_service::capture_raft_service_server::CaptureRaftService;
    let error = a_service
        .vote(tonic::Request::new(rustfs_protos::proto_gen::node_service::CaptureRaftRequest {
            body: bytes::Bytes::new(),
        }))
        .await
        .expect_err("old HTTP service is unmounted");
    assert_eq!(error.code(), tonic::Code::Unavailable, "CAPTURE_IDLE_ROUTES_DO_NOT_RETAIN_STORE");
    drop(a_service);
    drop(b_service);
    b.shutdown().await.expect("second shutdown");
}

#[tokio::test]
async fn native_capture_startup_defers_committed_replay_until_identity() {
    use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
    use rustfs_capture_authority::{CaptureRaftEntry, LogId, NativeRaftLogStore, NativeRaftStateMachine};
    let directory = private_directory();
    let mut capture = capture_config(directory.path(), 0);
    let id = |index| LogId {
        term: 1,
        leader_node: 0,
        index,
    };
    let mut store = Store::initialize(&capture.database, capture.identity).expect("fixture database");
    store
        .pin_capture_binding(capture.capture_binding.clone())
        .expect("fixture binding");
    store.enable_native_empty().expect("native mode");
    store.promote_created_provenance().expect("native provenance");
    let writer = NativeLogWriter::new(store);
    let mut logs = NativeRaftLogStore::new(writer.clone());
    let mut sm = NativeRaftStateMachine::new(writer.clone(), CheckpointLimits::default()).expect("real SM");
    logs.save_vote(&openraft::Vote::new_committed(1, 0))
        .await
        .expect("actual durable leader vote");
    let membership = CaptureRaftEntry {
        log_id: id(0).into(),
        payload: openraft::EntryPayload::Membership(openraft::Membership::new(
            vec![std::collections::BTreeSet::from([0])],
            capture.peers.clone(),
        )),
    };
    logs.blocking_append(vec![membership.clone()])
        .await
        .expect("persist membership");
    logs.save_committed(Some(id(0).into())).await.expect("commit membership");
    sm.apply(vec![membership]).await.expect("apply membership prefix");
    let pending = CaptureRaftEntry {
        log_id: id(1).into(),
        payload: openraft::EntryPayload::Normal(command(3)),
    };
    logs.blocking_append(vec![pending]).await.expect("persist pending Created");
    logs.save_committed(Some(id(1).into()))
        .await
        .expect("persist commit before apply");
    // Prepare the real committed-before-apply crash boundary, not a consensus result.
    drop(sm);
    drop(logs);
    drop(writer);
    let retained = Store::open(&capture.database, capture.identity).expect("inspect fixture");
    let before_page = retained.created_events_page(None, 256).expect("initial source page");
    let before_state = retained.native_application_state().expect("initial applied/membership");
    assert_eq!(before_state.0, Some(id(0)));
    assert!(before_page.events.is_empty());
    drop(retained);
    let before_durable = durable_state(&capture.database, capture.identity).await;
    capture.mode = config::CaptureMode::Recover;
    capture.bootstrap = false;
    let slot = ServerContextSlot::new();
    let mut runtime = start_capture(&server_config(&capture), &slot)
        .await
        .expect("prepare recovery")
        .expect("enabled");
    assert!(runtime.owner.is_none());
    assert!(runtime.finish_startup("127.0.0.1:19001").await.is_err());
    runtime.shutdown().await.expect("failed startup cleanup");
    drop(slot);
    assert_eq!(
        durable_state(&capture.database, capture.identity).await,
        before_durable,
        "WRONG_AUDIENCE_MUST_NOT_START_COMMITTED_REPLAY"
    );
    let retained = Store::open(&capture.database, capture.identity).expect("inspect rejection");
    assert_eq!(retained.created_events_page(None, 256).expect("retained page"), before_page);
    assert_eq!(retained.native_application_state().expect("retained applied/membership"), before_state);
    drop(retained);
    let slot = ServerContextSlot::new();
    let mut runtime = start_capture(&server_config(&capture), &slot)
        .await
        .expect("prepare valid recovery")
        .expect("enabled");
    runtime
        .finish_startup("127.0.0.1:19000")
        .await
        .expect("validate then start actual owner");
    let client = SourceReaderClient::new(&capture.reader_socket).expect("real reader");
    let replayed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let page = client
                .page(capture.identity.cluster_id, None, 256)
                .await
                .expect("source page");
            if page.page.events.len() == 1 {
                break page;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual committed replay completes");
    assert_eq!(replayed.page.events[0].captured.decision_log_id, id(1));
    runtime.shutdown().await.expect("shutdown valid recovery");
    drop(slot);
    let retained = Store::open(&capture.database, capture.identity).expect("inspect actual replay");
    assert!(
        retained
            .native_application_state()
            .expect("applied")
            .0
            .is_some_and(|value| value.index >= 1)
    );
    assert_eq!(retained.created_events_page(None, 256).expect("durable replay page"), replayed.page);
}
