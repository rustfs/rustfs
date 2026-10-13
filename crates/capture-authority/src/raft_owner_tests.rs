//! Real Raft engines and real redb; direct RPC routing exists only inside these tests.
use super::*;
use openraft::{
    Raft, RaftNetwork, RaftNetworkFactory, ServerState,
    error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable},
    network::{Backoff, RPCOption},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

type Engine = Raft<CaptureTypeConfig>;
type RpcError = RPCError<u64, CaptureNode, RaftError<u64>>;
type SnapshotRpcError = RPCError<u64, CaptureNode, RaftError<u64, InstallSnapshotError>>;
type PausedLinks = Mutex<BTreeMap<(u64, u64), (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>;
#[derive(Default)]
struct Routes {
    nodes: Mutex<BTreeMap<u64, Engine>>,
    blocked: Mutex<BTreeSet<(u64, u64)>>,
    snapshot_installs: AtomicUsize,
    pause_append: PausedLinks,
}
impl Routes {
    fn register(&self, id: u64, raft: Engine) {
        self.nodes.lock().expect("routes").insert(id, raft);
    }
    fn remove(&self, id: u64) {
        self.nodes.lock().expect("routes").remove(&id);
    }
    fn isolate(&self, id: u64, members: &[u64]) {
        let mut blocked = self.blocked.lock().expect("partition routes");
        for peer in members {
            if *peer != id {
                blocked.insert((id, *peer));
                blocked.insert((*peer, id));
            }
        }
    }
    fn heal(&self) {
        self.blocked.lock().expect("restore routes").clear();
    }
    fn endpoint(&self, source: u64, target: u64) -> Result<Engine, Unreachable> {
        if self.blocked.lock().expect("partition check").contains(&(source, target)) {
            return Err(Unreachable::new(&std::io::Error::other("test partition")));
        }
        self.nodes
            .lock()
            .expect("endpoint registry")
            .get(&target)
            .cloned()
            .ok_or_else(|| Unreachable::new(&std::io::Error::other("test endpoint unavailable")))
    }
}
#[derive(Clone)]
struct DirectFactory {
    source: u64,
    routes: Arc<Routes>,
}
struct DirectClient {
    source: u64,
    target: u64,
    node: CaptureNode,
    routes: Arc<Routes>,
}
impl RaftNetworkFactory<CaptureTypeConfig> for DirectFactory {
    type Network = DirectClient;
    async fn new_client(&mut self, target: u64, node: &CaptureNode) -> DirectClient {
        DirectClient {
            source: self.source,
            target,
            node: node.clone(),
            routes: self.routes.clone(),
        }
    }
}
impl RaftNetwork<CaptureTypeConfig> for DirectClient {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<CaptureTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RpcError> {
        let pause = self
            .routes
            .pause_append
            .lock()
            .expect("actual RPC pause hook")
            .remove(&(self.source, self.target));
        if let Some((entered, resume)) = pause {
            entered.notify_one();
            resume.notified().await;
        }
        let raft = self.routes.endpoint(self.source, self.target)?;
        raft.append_entries(request)
            .await
            .map_err(|error| RemoteError::new_with_node(self.target, self.node.clone(), error).into())
    }
    async fn vote(&mut self, request: VoteRequest<u64>, _option: RPCOption) -> Result<VoteResponse<u64>, RpcError> {
        let raft = self.routes.endpoint(self.source, self.target)?;
        raft.vote(request)
            .await
            .map_err(|error| RemoteError::new_with_node(self.target, self.node.clone(), error).into())
    }
    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest<CaptureTypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<u64>, SnapshotRpcError> {
        let raft = self.routes.endpoint(self.source, self.target)?;
        let done = request.done;
        let result = raft
            .install_snapshot(request)
            .await
            .map_err(|error| RemoteError::new_with_node(self.target, self.node.clone(), error).into());
        if done && result.is_ok() {
            self.routes.snapshot_installs.fetch_add(1, Ordering::AcqRel);
        }
        result
    }
    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(Duration::from_millis(25)))
    }
}
fn identity(node_id: u64) -> NodeIdentity {
    NodeIdentity {
        cluster_id: [7; 16],
        node_id,
    }
}
fn binding() -> CaptureBinding {
    CaptureBinding {
        revision: 9,
        binding_id: [8; 16],
        target_id: [9; 16],
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
    .expect("native client command")
}
fn config(snapshot_every: u64) -> Arc<openraft::Config> {
    Arc::new(
        openraft::Config {
            cluster_name: "native-owner-tests".into(),
            heartbeat_interval: 100,
            election_timeout_min: 500,
            election_timeout_max: 1000,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(snapshot_every),
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            ..openraft::Config::default()
        }
        .validate()
        .expect("valid real Raft test configuration"),
    )
}
fn members(ids: &[u64]) -> BTreeMap<u64, CaptureNode> {
    ids.iter()
        .map(|id| {
            (
                *id,
                CaptureNode::new(format!("http://127.0.0.1:{}", 7000 + id)).expect("bounded test endpoint"),
            )
        })
        .collect()
}
struct Node {
    directory: tempfile::TempDir,
    writer: NativeLogWriter,
    owner: Option<NativeRaftOwner>,
    id: u64,
}
impl Node {
    async fn start(id: u64, routes: Arc<Routes>, config: Arc<openraft::Config>) -> Self {
        let directory = tempfile::tempdir().expect("real Raft owner directory");
        let mut store = Store::initialize(directory.path().join("db"), identity(id)).expect("real owner redb");
        store.pin_capture_binding(binding()).expect("captured binding");
        store.enable_native_empty().expect("native storage mode");
        store.promote_created_provenance().expect("native provenance");
        let writer = NativeLogWriter::new(store);
        let owner = NativeRaftOwner::start(
            writer.clone(),
            id,
            config,
            CheckpointLimits::default(),
            DirectFactory {
                source: id,
                routes: routes.clone(),
            },
        )
        .await
        .expect("actual owner start");
        routes.register(id, owner.raft().clone());
        Self {
            directory,
            writer,
            owner: Some(owner),
            id,
        }
    }
    fn owner(&self) -> &NativeRaftOwner {
        self.owner.as_ref().expect("live owner")
    }
    async fn page(&self) -> EventPage {
        self.writer
            .created_events_page(None, 256)
            .await
            .expect("actual committed events")
    }
    async fn stop(&mut self, routes: &Routes) {
        self.owner
            .take()
            .expect("running owner")
            .shutdown()
            .await
            .expect("real Raft shutdown");
        routes.remove(self.id);
    }
}
struct Cluster {
    nodes: Vec<Node>,
    routes: Arc<Routes>,
}
impl Cluster {
    async fn start(ids: &[u64], snapshot_every: u64) -> Self {
        let routes = Arc::new(Routes::default());
        let mut nodes = Vec::new();
        for id in ids {
            nodes.push(Node::start(*id, routes.clone(), config(snapshot_every)).await);
        }
        nodes[0]
            .owner()
            .bootstrap(members(ids))
            .await
            .expect("explicit initial cluster bootstrap");
        let cluster = Self { nodes, routes };
        cluster.leader(ids).await;
        cluster
    }
    fn node(&self, id: u64) -> &Node {
        self.nodes.iter().find(|node| node.id == id).expect("cluster node")
    }
    async fn leader(&self, eligible: &[u64]) -> u64 {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                for id in eligible {
                    let node = self.node(*id);
                    let Some(owner) = &node.owner else {
                        continue;
                    };
                    let metrics = owner.metrics().borrow().clone();
                    assert!(metrics.running_state.is_ok(), "real Raft owner fatal state: {:?}", metrics.running_state);
                    if metrics.state == ServerState::Leader && metrics.current_leader == Some(*id) {
                        return *id;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real leader election readiness")
    }
    async fn applied(&self, ids: &[u64], index: u64) {
        for id in ids {
            self.node(*id)
                .owner()
                .raft()
                .wait(Some(Duration::from_secs(15)))
                .applied_index_at_least(Some(index), "real replicated apply readiness")
                .await
                .expect("committed log applied on real node");
        }
    }
    async fn stop(mut self) {
        for node in &mut self.nodes {
            if node.owner.is_some() {
                node.stop(&self.routes).await;
            }
        }
    }
}

#[tokio::test]
async fn raft_owner_single_node_bootstrap() {
    let cluster = Cluster::start(&[0], 100).await;
    let response = cluster
        .node(0)
        .owner()
        .client_write(command(1))
        .await
        .expect("real client_write");
    assert!(
        matches!(response.data.created_result(),Some(CreatedResult::Created { event_id,.. }) if *event_id==[1;16]),
        "OWNER_TRUE_RAFT_CREATED_RESPONSE"
    );
    let page = cluster.node(0).page().await;
    assert_eq!(page.events.len(), 1);
    assert_eq!(
        page.events[0].captured.decision_log_id,
        LogId::from(response.log_id),
        "OWNER_COMMITTED_EVENT_LOG_ID"
    );
    assert!(
        cluster.node(0).owner().bootstrap(members(&[0])).await.is_err(),
        "OWNER_BOOTSTRAP_NOT_REINITIALIZATION"
    );
    cluster.stop().await;
}

#[tokio::test]
async fn raft_owner_single_node_restart() {
    let routes = Arc::new(Routes::default());
    let mut node = Node::start(0, routes.clone(), config(100)).await;
    node.owner().bootstrap(members(&[0])).await.expect("first bootstrap only");
    node.owner()
        .raft()
        .wait(Some(Duration::from_secs(15)))
        .state(ServerState::Leader, "single leader")
        .await
        .expect("leader ready");
    node.owner().client_write(command(1)).await.expect("first genuine write");
    let before = node.page().await;
    node.stop(&routes).await;
    let Node {
        directory,
        writer,
        owner: _,
        id,
    } = node;
    let vote = writer.read_vote().await.expect("durable vote before reopen");
    drop(writer);
    let store = Store::open(directory.path().join("db"), identity(id)).expect("restart same durable identity");
    assert_eq!(store.read_vote().expect("persisted vote"), vote.map(Into::into));
    let writer = NativeLogWriter::new(store);
    let owner = NativeRaftOwner::start(
        writer.clone(),
        id,
        config(100),
        CheckpointLimits::default(),
        DirectFactory {
            source: id,
            routes: routes.clone(),
        },
    )
    .await
    .expect("restart existing Raft");
    routes.register(id, owner.raft().clone());
    assert!(
        owner.raft().is_initialized().await.expect("restored cluster membership"),
        "OWNER_RESTART_DOES_NOT_REQUIRE_BOOTSTRAP"
    );
    owner
        .raft()
        .wait(Some(Duration::from_secs(15)))
        .state(ServerState::Leader, "restored leader")
        .await
        .expect("restarted leader ready");
    owner
        .client_write(command(2))
        .await
        .expect("continue after restart without initialize");
    let after = writer.created_events_page(None, 256).await.expect("restart continued events");
    assert_eq!(after.events.len(), 2);
    assert_eq!(after.events[0], before.events[0], "OWNER_RESTART_PRESERVES_FIRST_DECISION");
    owner.shutdown().await.expect("restart owner shutdown");
    routes.remove(id);
}

#[tokio::test]
async fn raft_owner_idempotent_retry() {
    let cluster = Cluster::start(&[0], 100).await;
    let owner = cluster.node(0).owner();
    let first = owner.client_write(command(1)).await.expect("first write");
    let replay = owner.client_write(command(1)).await.expect("same operation retry");
    assert_eq!(
        replay.data.created_result(),
        first.data.created_result(),
        "OWNER_RETRY_PRESERVES_FIRST_DECISION_RESULT"
    );
    assert!(replay.log_id.index > first.log_id.index, "real retry can append another Raft log");
    assert_eq!(cluster.node(0).page().await.events.len(), 1, "OWNER_RETRY_NO_DUPLICATE_EVENT");
    cluster.stop().await;
}

#[tokio::test]
async fn raft_owner_identity_binding() {
    let routes = Arc::new(Routes::default());
    let mut node = Node::start(0, routes.clone(), config(100)).await;
    node.owner().bootstrap(members(&[0])).await.expect("real durable membership");
    node.owner()
        .raft()
        .wait(Some(Duration::from_secs(15)))
        .state(ServerState::Leader, "single leader")
        .await
        .expect("leader");
    node.owner().client_write(command(1)).await.expect("nonempty durable store");
    node.stop(&routes).await;
    let before = std::fs::read(node.directory.path().join("db")).expect("stable identity-bound store");
    let result = NativeRaftOwner::start(
        node.writer.clone(),
        1,
        config(100),
        CheckpointLimits::default(),
        DirectFactory {
            source: 1,
            routes: routes.clone(),
        },
    )
    .await;
    assert!(
        matches!(result, Err(NativeRaftOwnerError::IdentityMismatch { runtime: 1, durable: 0 })),
        "OWNER_REQUIRES_DURABLE_NODE_ID: runtime node cannot reinterpret stored membership/identity"
    );
    assert_eq!(
        std::fs::read(node.directory.path().join("db")).expect("unchanged identity-bound store"),
        before
    );
    let invalid_config = Arc::new(openraft::Config {
        election_timeout_min: 1000,
        election_timeout_max: 500,
        ..openraft::Config::default()
    });
    let invalid = NativeRaftOwner::start(
        node.writer.clone(),
        0,
        invalid_config,
        CheckpointLimits::default(),
        DirectFactory {
            source: 0,
            routes: routes.clone(),
        },
    )
    .await;
    assert!(
        matches!(invalid, Err(NativeRaftOwnerError::Config(_))),
        "OWNER_REJECTS_INVALID_CONFIG_BEFORE_START"
    );
    let mut limits = CheckpointLimits::default();
    limits.snapshot.total_bytes = 0;
    let invalid = NativeRaftOwner::start(node.writer.clone(), 0, config(100), limits, DirectFactory { source: 0, routes }).await;
    assert!(
        matches!(invalid, Err(NativeRaftOwnerError::Snapshot(_))),
        "OWNER_REJECTS_INVALID_LIMITS_BEFORE_START"
    );
    assert_eq!(
        std::fs::read(node.directory.path().join("db")).expect("invalid start leaves stable bytes"),
        before
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn raft_owner_shared_reader() {
    use source_reader::SourceReaderClient;
    use std::os::unix::fs::PermissionsExt;
    let cluster = Cluster::start(&[0], 100).await;
    let node = cluster.node(0);
    std::fs::set_permissions(node.directory.path(), std::fs::Permissions::from_mode(0o700)).expect("private reader parent");
    let socket = node.directory.path().join("reader.sock");
    let (server, shutdown) = node
        .owner()
        .bind_reader(&socket)
        .expect("owner shares live writer with reader");
    let task = tokio::spawn(server.serve());
    let client = SourceReaderClient::new(&socket).expect("live reader");
    assert!(
        client
            .page(identity(0).cluster_id, None, 1)
            .await
            .expect("initial reader page")
            .page
            .events
            .is_empty()
    );
    let response = node
        .owner()
        .client_write(command(1))
        .await
        .expect("real write while reader runs");
    let page = client
        .page(identity(0).cluster_id, None, 1)
        .await
        .expect("live committed event");
    assert_eq!(page.identity, identity(0));
    assert_eq!(
        page.page.events[0].captured.decision_log_id,
        LogId::from(response.log_id),
        "OWNER_READER_USES_REAL_COMMITTED_WRITER"
    );
    shutdown.shutdown();
    task.await.expect("reader serve task").expect("reader drains");
    cluster.stop().await;
}

#[tokio::test]
async fn raft_owner_shutdown() {
    let cluster = Cluster::start(&[0], 100).await;
    let owner = cluster.node(0).owner();
    owner.client_write(command(1)).await.expect("accepted before shutdown");
    owner.shutdown().await.expect("actual owner shutdown");
    assert!(owner.client_write(command(2)).await.is_err(), "OWNER_SHUTDOWN_REJECTS_NEW_WRITES");
    assert_eq!(cluster.node(0).page().await.events.len(), 1);
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_owner_three_node_majority() {
    let cluster = Cluster::start(&[0, 1, 2], 100).await;
    let leader = cluster.leader(&[0, 1, 2]).await;
    let first = cluster
        .node(leader)
        .owner()
        .client_write(command(1))
        .await
        .expect("real quorum write");
    cluster.applied(&[0, 1, 2], first.log_id.index).await;
    let follower = [0, 1, 2].into_iter().find(|id| *id != leader).expect("nonleader");
    let rejected = cluster.node(follower).owner().client_write(command(254)).await;
    match rejected {
        Err(NativeRaftOwnerError::Write(error)) => {
            assert!(
                matches!(error.api_error(), Some(openraft::error::ClientWriteError::ForwardToLeader(_))),
                "OWNER_PRESERVES_TYPED_FORWARD_TO_LEADER"
            );
        }
        other => panic!("OWNER_NONLEADER_WRITE_MUST_NOT_SUCCEED: {other:?}"),
    }
    cluster.routes.isolate(leader, &[0, 1, 2]);
    let uncertain =
        tokio::time::timeout(Duration::from_millis(1500), cluster.node(leader).owner().client_write(command(2))).await;
    assert!(!matches!(uncertain, Ok(Ok(_))), "OWNER_MAJORITY_REQUIRED_FOR_SUCCESS");
    let others: Vec<_> = [0, 1, 2].into_iter().filter(|id| *id != leader).collect();
    let next = cluster.leader(&others).await;
    let response = cluster
        .node(next)
        .owner()
        .client_write(command(2))
        .await
        .expect("majority-owned retry of exact same operation");
    cluster.routes.heal();
    cluster.applied(&[0, 1, 2], response.log_id.index).await;
    for node in &cluster.nodes {
        assert_eq!(node.page().await.events.len(), 2, "OWNER_QUORUM_RETRY_NO_DUPLICATE_EVENT");
    }
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_owner_leader_failover() {
    let mut cluster = Cluster::start(&[0, 1, 2], 100).await;
    let leader = cluster.leader(&[0, 1, 2]).await;
    let first = cluster
        .node(leader)
        .owner()
        .client_write(command(1))
        .await
        .expect("first confirmed write");
    cluster.applied(&[0, 1, 2], first.log_id.index).await;
    let before = cluster.node(leader).page().await.events[0].clone();
    cluster
        .nodes
        .iter_mut()
        .find(|node| node.id == leader)
        .expect("old leader")
        .stop(&cluster.routes)
        .await;
    let survivors: Vec<_> = [0, 1, 2].into_iter().filter(|id| *id != leader).collect();
    let next = cluster.leader(&survivors).await;
    let response = cluster
        .node(next)
        .owner()
        .client_write(command(2))
        .await
        .expect("new leader actual quorum write");
    cluster.applied(&survivors, response.log_id.index).await;
    for id in survivors {
        let page = cluster.node(id).page().await;
        assert_eq!(page.events.len(), 2);
        assert_eq!(page.events[0], before, "OWNER_FAILOVER_PRESERVES_CONFIRMED_DECISION");
    }
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_owner_snapshot_catchup() {
    let cluster = Cluster::start(&[0, 1, 2], 1000).await;
    let leader = cluster.leader(&[0, 1, 2]).await;
    let follower = [0, 1, 2].into_iter().find(|id| *id != leader).expect("lagging follower");
    let first = cluster
        .node(leader)
        .owner()
        .client_write(command(1))
        .await
        .expect("initial confirmed event");
    cluster.applied(&[0, 1, 2], first.log_id.index).await;
    // Keep the catch-up test focused on replication rather than an isolated follower's election.
    cluster.node(follower).owner().raft().runtime_config().elect(false);
    cluster.routes.isolate(follower, &[0, 1, 2]);
    let mut last = first;
    for value in 2..=12 {
        last = cluster
            .node(leader)
            .owner()
            .client_write(command(value))
            .await
            .expect("majority writes while follower offline");
    }
    cluster
        .node(leader)
        .owner()
        .raft()
        .trigger()
        .snapshot()
        .await
        .expect("true Raft snapshot trigger");
    cluster
        .node(leader)
        .owner()
        .raft()
        .wait(Some(Duration::from_secs(15)))
        .metrics(
            |metrics| metrics.purged.is_some_and(|cut| cut.index >= last.log_id.index),
            "snapshot durably purged lagging log prefix",
        )
        .await
        .expect("real snapshot and purge");
    let before = cluster.routes.snapshot_installs.load(Ordering::Acquire);
    cluster.routes.heal();
    cluster.applied(&[0, 1, 2], last.log_id.index).await;
    cluster
        .node(follower)
        .owner()
        .raft()
        .wait(Some(Duration::from_secs(15)))
        .metrics(
            |metrics| metrics.snapshot.is_some_and(|cut| cut.index >= last.log_id.index),
            "follower installed actual transferred snapshot",
        )
        .await
        .expect("real snapshot catch-up");
    tokio::time::timeout(Duration::from_secs(15), async {
        while cluster.routes.snapshot_installs.load(Ordering::Acquire) <= before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("snapshot readiness includes actual successful done-RPC return");
    assert!(
        cluster.routes.snapshot_installs.load(Ordering::Acquire) > before,
        "OWNER_CATCHUP_USED_ACTUAL_SNAPSHOT_RPC"
    );
    let expected = cluster.node(leader).page().await;
    for node in &cluster.nodes {
        assert_eq!(node.page().await, expected, "OWNER_SNAPSHOT_CATCHUP_FULL_SOURCE_EVENTS");
    }
    cluster.node(follower).owner().raft().runtime_config().elect(true);
    let current = cluster.leader(&[0, 1, 2]).await;
    let next = cluster
        .node(current)
        .owner()
        .client_write(command(13))
        .await
        .expect("continue after real snapshot install");
    cluster.applied(&[0, 1, 2], next.log_id.index).await;
    for node in &cluster.nodes {
        assert_eq!(node.page().await.events.len(), 13);
    }
    cluster.stop().await;
}

async fn wait_for_shutdown_drain(waiters: &AtomicUsize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while waiters.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("OWNER_SHUTDOWN_DRAINS_INFLIGHT_WORK: owner reached real worker drain wait");
}

#[tokio::test]
async fn raft_owner_shutdown_inflight_snapshot() {
    let routes = Arc::new(Routes::default());
    let mut node = Node::start(0, routes.clone(), config(1000)).await;
    node.owner()
        .bootstrap(members(&[0]))
        .await
        .expect("real single-node bootstrap");
    node.owner()
        .raft()
        .wait(Some(Duration::from_secs(15)))
        .state(ServerState::Leader, "leader before snapshot")
        .await
        .expect("leader readiness");
    node.owner()
        .client_write(command(1))
        .await
        .expect("real committed snapshot input");
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *node.owner().snapshot_build_pause.lock().expect("real builder hook") = Some((entered.clone(), resume.clone()));
    node.owner()
        .raft()
        .trigger()
        .snapshot()
        .await
        .expect("real Raft snapshot request");
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("real detached snapshot builder paused");
    let owner = node.owner.take().expect("running owner");
    let waiters = owner.shutdown_waiters();
    let observer = owner.raft().clone();
    let shutdown = tokio::spawn(async move {
        let result = owner.shutdown().await;
        (owner, result)
    });
    wait_for_shutdown_drain(&waiters).await;
    assert!(
        observer.metrics().borrow().running_state.is_err(),
        "OWNER_CORE_STOPPED_WHILE_SNAPSHOT_DRAIN_PENDING"
    );
    assert!(!shutdown.is_finished(), "OWNER_SHUTDOWN_DRAINS_INFLIGHT_SNAPSHOT");
    resume.notify_one();
    let (owner, result) = tokio::time::timeout(Duration::from_secs(10), shutdown)
        .await
        .expect("released snapshot lets shutdown finish")
        .expect("shutdown task");
    result.expect("snapshot work drained");
    routes.remove(0);
    drop(observer);
    drop(owner);
    drop(waiters);
    let Node {
        directory,
        writer,
        owner: _,
        id,
    } = node;
    drop(writer);
    let store = Store::open(directory.path().join("db"), identity(id)).expect("OWNER_SHUTDOWN_SNAPSHOT_RELEASES_LIVE_DATABASE");
    assert_eq!(
        store
            .created_events_page(None, 256)
            .expect("retained committed source events")
            .events
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_owner_shutdown_inflight_replication() {
    // OpenRaft times append_entries by heartbeat_interval. Keep the real deadline
    // longer than readiness checks so only explicit RPC release ends this pause.
    let slow = Arc::new(
        openraft::Config {
            heartbeat_interval: 10_000,
            election_timeout_min: 20_000,
            election_timeout_max: 30_000,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1000),
            ..openraft::Config::default()
        }
        .validate()
        .expect("long real RPC deadline"),
    );
    let routes = Arc::new(Routes::default());
    let mut nodes = Vec::new();
    for id in [0, 1, 2] {
        nodes.push(Node::start(id, routes.clone(), slow.clone()).await);
    }
    nodes[0]
        .owner()
        .bootstrap(members(&[0, 1, 2]))
        .await
        .expect("real three-node bootstrap");
    nodes[0]
        .owner()
        .raft()
        .trigger()
        .elect()
        .await
        .expect("explicit test election readiness");
    let mut cluster = Cluster { nodes, routes };
    let leader = cluster.leader(&[0, 1, 2]).await;
    let response = cluster
        .node(leader)
        .owner()
        .client_write(command(1))
        .await
        .expect("real replicated write before pause");
    cluster.applied(&[0, 1, 2], response.log_id.index).await;
    let follower = [0, 1, 2].into_iter().find(|id| *id != leader).expect("replication target");
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    cluster
        .routes
        .pause_append
        .lock()
        .expect("actual request pause")
        .insert((leader, follower), (entered.clone(), resume.clone()));
    cluster
        .node(leader)
        .owner()
        .raft()
        .trigger()
        .heartbeat()
        .await
        .expect("real replication RPC");
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("actual AppendEntries in flight");
    let node = cluster.nodes.iter_mut().find(|node| node.id == leader).expect("leader node");
    let owner = node.owner.take().expect("leader owner");
    let waiters = owner.shutdown_waiters();
    let observer = owner.raft().clone();
    let shutdown = tokio::spawn(async move {
        let result = owner.shutdown().await;
        (owner, result)
    });
    wait_for_shutdown_drain(&waiters).await;
    assert!(
        observer.metrics().borrow().running_state.is_err(),
        "OWNER_CORE_STOPPED_WHILE_REPLICATION_DRAIN_PENDING"
    );
    assert!(!shutdown.is_finished(), "OWNER_SHUTDOWN_DRAINS_INFLIGHT_REPLICATION");
    resume.notify_one();
    let (owner, result) = tokio::time::timeout(Duration::from_secs(10), shutdown)
        .await
        .expect("released real RPC lets shutdown finish")
        .expect("replication shutdown task");
    result.expect("actual replication reader drained");
    cluster.routes.remove(leader);
    drop(observer);
    drop(owner);
    drop(waiters);
    let position = cluster
        .nodes
        .iter()
        .position(|node| node.id == leader)
        .expect("stopped leader");
    let Node {
        directory,
        writer,
        owner: _,
        id,
    } = cluster.nodes.remove(position);
    drop(writer);
    let store =
        Store::open(directory.path().join("db"), identity(id)).expect("OWNER_SHUTDOWN_REPLICATION_RELEASES_LIVE_DATABASE");
    assert_eq!(
        store
            .created_events_page(None, 256)
            .expect("real quorum event remains")
            .events
            .len(),
        1
    );
    drop(store);
    cluster.stop().await;
}
