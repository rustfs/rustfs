//! Real TCP/tonic admission and genuine Raft/redb. Ping is the only test-only route.
use super::http_auth::{
    tonic_boot_epoch_challenge, tonic_boot_epoch_response_headers, verify_tonic_rpc_signature_with_bootstrap,
};
use rustfs_protos::proto_gen::node_service::{PingRequest, PingResponse};
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tower::Service;

#[derive(Clone)]
struct PingOnly {
    audience: Arc<str>,
    pings: Arc<std::sync::atomic::AtomicUsize>,
}
struct PingUnary {
    audience: Arc<str>,
    pings: Arc<std::sync::atomic::AtomicUsize>,
}
impl tonic::server::UnaryService<PingRequest> for PingUnary {
    type Response = PingResponse;
    type Future = Pin<Box<dyn Future<Output = Result<tonic::Response<PingResponse>, tonic::Status>> + Send>>;
    fn call(&mut self, request: tonic::Request<PingRequest>) -> Self::Future {
        let audience = self.audience.clone();
        let pings = self.pings.clone();
        Box::pin(async move {
            verify_tonic_rpc_signature_with_bootstrap(
                &audience,
                "/node_service.NodeService/Ping",
                request.metadata().as_ref(),
                true,
            )
            .map_err(|_| tonic::Status::unauthenticated("test Ping authentication rejected"))?;
            let challenge = tonic_boot_epoch_challenge(request.metadata().as_ref())
                .map_err(|_| tonic::Status::unauthenticated("invalid boot challenge"))?
                .ok_or_else(|| tonic::Status::unauthenticated("boot challenge required"))?;
            pings.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            let mut response = tonic::Response::new(PingResponse {
                version: 1,
                body: bytes::Bytes::new(),
            });
            response.metadata_mut().as_mut().extend(
                tonic_boot_epoch_response_headers(&audience, challenge)
                    .map_err(|_| tonic::Status::unavailable("boot proof unavailable"))?,
            );
            Ok(response)
        })
    }
}
impl tonic::server::NamedService for PingOnly {
    const NAME: &'static str = "node_service.NodeService";
}
impl Service<http::Request<tonic::body::Body>> for PingOnly {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let audience = self.audience.clone();
        let pings = self.pings.clone();
        Box::pin(async move {
            if request.uri().path() != "/node_service.NodeService/Ping" {
                return Ok(http::Response::builder()
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "12")
                    .body(tonic::body::Body::empty())
                    .expect("unimplemented Ping-only route"));
            }
            let codec = tonic_prost::ProstCodec::<PingResponse, PingRequest>::default();
            let mut grpc = tonic::server::Grpc::new(codec).apply_max_message_size_config(Some(4096), Some(4096));
            Ok(grpc.unary(PingUnary { audience, pings }, request).await)
        })
    }
}

use super::capture_raft::{CaptureRaftLimits, CaptureRaftNetworkFactory, CaptureRaftRpc};
use openraft::{RaftNetwork, RaftNetworkFactory, ServerState};
use rustfs_capture_authority::{
    CaptureBinding, CaptureCommand, CaptureNode, CheckpointLimits, DecideCreated, EventPage, NativeLogWriter, NativeRaftOwner,
    NodeIdentity, ObjectIdentity, PreparedIdentity, Store,
};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::net::TcpListener;

fn identity(id: u64) -> NodeIdentity {
    NodeIdentity {
        cluster_id: [7; 16],
        node_id: id,
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
    .expect("actual native client command")
}
fn config() -> Arc<openraft::Config> {
    Arc::new(
        openraft::Config {
            cluster_name: "capture-raft-tcp-tests".into(),
            heartbeat_interval: 300,
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1000),
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            ..openraft::Config::default()
        }
        .validate()
        .expect("valid TCP Raft test config"),
    )
}
struct TcpNode {
    id: u64,
    directory: tempfile::TempDir,
    writer: NativeLogWriter,
    owner: Option<Arc<NativeRaftOwner>>,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    server_stop: Option<tokio::sync::oneshot::Sender<()>>,
    pings: Arc<AtomicUsize>,
    faults: Arc<FaultState>,
    rpc: CaptureRaftRpc,
}
impl TcpNode {
    fn owner(&self) -> &Arc<NativeRaftOwner> {
        self.owner.as_ref().expect("running native owner")
    }
    async fn page(&self) -> EventPage {
        self.writer
            .created_events_page(None, 256)
            .await
            .expect("true committed source page")
    }
}
struct Cluster {
    nodes: Vec<TcpNode>,
    peers: BTreeMap<u64, CaptureNode>,
}
impl Cluster {
    async fn start(count: u64, bootstrap: bool) -> Self {
        Self::start_with(count, bootstrap, config(), CaptureRaftLimits::default()).await
    }
    async fn start_with(count: u64, bootstrap: bool, runtime: Arc<openraft::Config>, limits: CaptureRaftLimits) -> Self {
        crate::runtime::sources::ensure_test_rpc_secret();
        let mut listeners = Vec::new();
        let mut peers = BTreeMap::new();
        for id in 0..count {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("real TCP node listener");
            peers.insert(
                id,
                CaptureNode::new(format!("http://{}", listener.local_addr().expect("bound TCP address")))
                    .expect("actual node URI"),
            );
            listeners.push((id, listener));
        }
        let mut nodes = Vec::new();
        for (id, listener) in listeners {
            let directory = tempfile::tempdir().expect("separate real native redb");
            let mut store = Store::initialize(directory.path().join("db"), identity(id)).expect("actual owner store");
            store.pin_capture_binding(binding()).expect("capture binding");
            store.enable_native_empty().expect("native mode");
            store.promote_created_provenance().expect("native provenance");
            let writer = NativeLogWriter::new(store);
            let factory = CaptureRaftNetworkFactory::new(identity(id), peers.clone(), runtime.clone(), limits)
                .expect("production network factory");
            let owner = Arc::new(
                NativeRaftOwner::start(writer.clone(), id, runtime.clone(), CheckpointLimits::default(), factory)
                    .await
                    .expect("true Raft owner"),
            );
            let rpc = CaptureRaftRpc::new(owner.clone(), peers.clone(), limits).expect("production RPC service");
            let service = rpc.clone().into_service();
            let audience = super::http_auth::normalize_tonic_rpc_audience(
                peers[&id]
                    .rpc_uri()
                    .parse::<http::Uri>()
                    .expect("peer URI")
                    .authority()
                    .expect("peer authority")
                    .as_str(),
            )
            .expect("actual target audience");
            let pings = Arc::new(AtomicUsize::new(0));
            let ping = PingOnly {
                audience: audience.into(),
                pings: pings.clone(),
            };
            let incoming = futures::stream::unfold(listener, |listener| async move {
                let accepted = listener.accept().await.map(|(stream, _)| stream);
                Some((accepted, listener))
            });
            let faults = Arc::new(FaultState::default());
            let service = FaultyCapture {
                inner: service,
                state: faults.clone(),
            };
            let (server_stop, stopped) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(ping)
                    .add_service(service)
                    .serve_with_incoming_shutdown(incoming, async {
                        let _ = stopped.await;
                    }),
            );
            nodes.push(TcpNode {
                id,
                directory,
                writer,
                owner: Some(owner),
                server,
                server_stop: Some(server_stop),
                pings,
                faults,
                rpc,
            });
        }
        let cluster = Self { nodes, peers };
        if bootstrap {
            cluster
                .node(0)
                .owner()
                .bootstrap(cluster.peers.clone())
                .await
                .expect("explicit true cluster bootstrap");
            let ids = cluster.ids();
            cluster.leader(&ids).await;
        }
        cluster
    }
    fn ids(&self) -> Vec<u64> {
        self.nodes.iter().map(|node| node.id).collect()
    }
    fn node(&self, id: u64) -> &TcpNode {
        self.nodes.iter().find(|node| node.id == id).expect("TCP cluster node")
    }
    async fn leader(&self, ids: &[u64]) -> u64 {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                for id in ids {
                    let node = self.node(*id);
                    let Some(owner) = &node.owner else {
                        continue;
                    };
                    let metrics = owner.metrics().borrow().clone();
                    assert!(metrics.running_state.is_ok(), "actual Raft fatal: {:?}", metrics.running_state);
                    if metrics.state == ServerState::Leader && metrics.current_leader == Some(*id) {
                        return *id;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual TCP election readiness")
    }
    async fn applied(&self, ids: &[u64], index: u64) {
        for id in ids {
            self.node(*id)
                .owner()
                .raft()
                .wait(Some(Duration::from_secs(20)))
                .applied_index_at_least(Some(index), "actual authenticated TCP replication")
                .await
                .expect("real committed apply readiness");
        }
    }
    async fn stop(mut self) {
        for node in &self.nodes {
            node.rpc.shutdown().await.expect("actual transport task drain");
        }
        for node in &mut self.nodes {
            if let Some(owner) = node.owner.take() {
                owner.shutdown().await.expect("actual owner drain");
            }
        }
        for node in &mut self.nodes {
            if let Some(signal) = node.server_stop.take() {
                let _ = signal.send(());
            }
        }
        for node in &mut self.nodes {
            let result = tokio::time::timeout(Duration::from_secs(10), &mut node.server)
                .await
                .expect("true TCP listener graceful shutdown");
            match result {
                Ok(result) => result.expect("TCP service shutdown result"),
                Err(error) => assert!(error.is_cancelled(), "unexpected TCP server failure: {error}"),
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_raft_tcp_three_node_majority() {
    let mut cluster = Cluster::start(3, true).await;
    let ids = cluster.ids();
    let leader = cluster.leader(&ids).await;
    let first = cluster
        .node(leader)
        .owner()
        .client_write(command(1))
        .await
        .expect("actual production TCP majority write");
    cluster.applied(&ids, first.log_id.index).await;
    assert!(
        cluster
            .nodes
            .iter()
            .map(|node| node.pings.load(Ordering::Acquire))
            .sum::<usize>()
            >= 2,
        "CAPTURE_RAFT_REAL_PING_AUTH_HANDSHAKE"
    );
    // Remove both peer listeners: the production factory must observe real TCP loss.
    for node in &cluster.nodes {
        if node.id != leader {
            node.server.abort();
        }
    }
    for node in &mut cluster.nodes {
        if node.id != leader
            && let Some(owner) = node.owner.take()
        {
            owner.shutdown().await.expect("offline peer drains");
        }
    }
    let result = tokio::time::timeout(Duration::from_secs(2), cluster.node(leader).owner().client_write(command(2))).await;
    assert!(!matches!(result, Ok(Ok(_))), "CAPTURE_RAFT_MAJORITY_REQUIRED_FOR_ACCEPTANCE");
    assert_eq!(
        cluster.node(leader).page().await.events.len(),
        1,
        "CAPTURE_RAFT_UNKNOWN_WRITE_NOT_EMITTED"
    );
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_raft_tcp_leader_failover() {
    let mut cluster = Cluster::start(3, true).await;
    let ids = cluster.ids();
    let leader = cluster.leader(&ids).await;
    let first = cluster
        .node(leader)
        .owner()
        .client_write(command(1))
        .await
        .expect("confirmed before leader loss");
    cluster.applied(&ids, first.log_id.index).await;
    let retained = cluster.node(leader).page().await.events[0].clone();
    let old = cluster.nodes.iter_mut().find(|node| node.id == leader).expect("leader node");
    old.owner
        .take()
        .expect("leader owner")
        .shutdown()
        .await
        .expect("real stopped leader drain");
    old.server.abort();
    let survivors: Vec<_> = ids.into_iter().filter(|id| *id != leader).collect();
    let next = cluster.leader(&survivors).await;
    let response = cluster
        .node(next)
        .owner()
        .client_write(command(2))
        .await
        .expect("production TCP new-leader quorum write");
    cluster.applied(&survivors, response.log_id.index).await;
    for id in survivors {
        let page = cluster.node(id).page().await;
        assert_eq!(page.events.len(), 2);
        assert_eq!(page.events[0], retained, "CAPTURE_RAFT_FAILOVER_PRESERVES_CONFIRMED_EVENT");
    }
    cluster.stop().await;
}

use http_body_util::BodyExt;
use rustfs_protos::proto_gen::node_service::{
    CaptureRaftRequest, CaptureRaftResponse, capture_raft_service_server::CaptureRaftServiceServer,
};
use std::sync::Mutex;
use tonic_prost::prost::Message;
const NORMAL: usize = 0;
const UNAVAILABLE: usize = 1;
const BAD_PROOF: usize = 2;
const OLD_RESPONSE: usize = 3;
const DELAY: usize = 4;
#[derive(Default)]
struct FaultState {
    mode: AtomicUsize,
    captured: Mutex<Option<(http::HeaderMap, CaptureRaftRequest)>>,
    response: Mutex<Option<bytes::Bytes>>,
    snapshots: AtomicUsize,
    delay_entered: tokio::sync::Notify,
}
#[derive(Clone)]
struct FaultyCapture {
    inner: CaptureRaftServiceServer<CaptureRaftRpc>,
    state: Arc<FaultState>,
}
impl tonic::server::NamedService for FaultyCapture {
    const NAME: &'static str = <CaptureRaftServiceServer<CaptureRaftRpc> as tonic::server::NamedService>::NAME;
}
impl Service<http::Request<tonic::body::Body>> for FaultyCapture {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <CaptureRaftServiceServer<CaptureRaftRpc> as Service<http::Request<tonic::body::Body>>>::poll_ready(&mut self.inner, cx)
    }
    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let mut inner = self.inner.clone();
        let state = self.state.clone();
        Box::pin(async move {
            let mode = state.mode.load(Ordering::Acquire);
            if mode == UNAVAILABLE {
                return Ok(http::Response::builder()
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "14")
                    .body(tonic::body::Body::empty())
                    .expect("network unavailable fixture"));
            }
            if mode == DELAY {
                state.delay_entered.notify_one();
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            let path = request.uri().path().to_owned();
            let (parts, body) = request.into_parts();
            let bytes = body.collect().await.expect("test request transport body").to_bytes();
            if bytes.len() >= 5
                && bytes[0] == 0
                && let Ok(request) = CaptureRaftRequest::decode(&bytes[5..])
            {
                *state.captured.lock().expect("captured authenticated request") = Some((parts.headers.clone(), request));
            }
            let request = http::Request::from_parts(parts, tonic::body::Body::new(http_body_util::Full::new(bytes)));
            // The production handler performs all authentication and actual Raft work.
            let response = inner.call(request).await?;
            let (parts, body) = response.into_parts();
            let collected = body.collect().await.expect("production response transport");
            let trailers = collected.trailers().cloned();
            let mut bytes = collected.to_bytes();
            let ok = trailers
                .as_ref()
                .and_then(|headers| headers.get("grpc-status"))
                .is_some_and(|status| status == "0");
            if ok && path.ends_with("/InstallSnapshot") {
                state.snapshots.fetch_add(1, Ordering::AcqRel);
            }
            if ok && bytes.len() >= 5 && bytes[0] == 0 {
                if mode == BAD_PROOF {
                    let mut response = CaptureRaftResponse::decode(&bytes[5..]).expect("valid production response protobuf");
                    let mut proof = response.response_proof.to_vec();
                    assert!(!proof.is_empty(), "production response really carries proof");
                    proof[0] ^= 1;
                    response.response_proof = proof.into();
                    let payload = response.encode_to_vec();
                    let mut changed = vec![0];
                    changed.extend(u32::try_from(payload.len()).expect("bounded test response").to_be_bytes());
                    changed.extend(payload);
                    bytes = changed.into();
                } else if mode == OLD_RESPONSE {
                    bytes = state
                        .response
                        .lock()
                        .expect("prior actual response")
                        .clone()
                        .expect("previous successful real response");
                } else if mode == NORMAL {
                    *state.response.lock().expect("remember actual response") = Some(bytes.clone());
                }
            }
            let mut frames: Vec<Result<hyper::body::Frame<bytes::Bytes>, Infallible>> = vec![Ok(hyper::body::Frame::data(bytes))];
            if let Some(trailers) = trailers {
                frames.push(Ok(hyper::body::Frame::trailers(trailers)));
            }
            Ok(http::Response::from_parts(
                parts,
                tonic::body::Body::new(http_body_util::StreamBody::new(futures::stream::iter(frames))),
            ))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_raft_tcp_snapshot_catchup() {
    let cluster = Cluster::start(3, true).await;
    let ids = cluster.ids();
    let leader = cluster.leader(&ids).await;
    let follower = ids.iter().copied().find(|id| *id != leader).expect("lagging TCP follower");
    let first = cluster
        .node(leader)
        .owner()
        .client_write(command(1))
        .await
        .expect("first authenticated quorum event");
    cluster.applied(&ids, first.log_id.index).await;
    cluster.node(follower).owner().raft().runtime_config().elect(false);
    cluster.node(follower).faults.mode.store(UNAVAILABLE, Ordering::Release);
    let mut last = first;
    for value in 2..=12 {
        last = cluster
            .node(leader)
            .owner()
            .client_write(command(value))
            .await
            .expect("remaining true TCP majority commits");
    }
    cluster
        .node(leader)
        .owner()
        .raft()
        .trigger()
        .snapshot()
        .await
        .expect("actual Raft snapshot build");
    cluster
        .node(leader)
        .owner()
        .raft()
        .wait(Some(Duration::from_secs(20)))
        .metrics(
            |metrics| metrics.purged.is_some_and(|cut| cut.index >= last.log_id.index),
            "true durable prefix purge before recovery",
        )
        .await
        .expect("real snapshot/purge prerequisite");
    let before = cluster.node(follower).faults.snapshots.load(Ordering::Acquire);
    cluster.node(follower).faults.mode.store(NORMAL, Ordering::Release);
    cluster.applied(&ids, last.log_id.index).await;
    cluster
        .node(follower)
        .owner()
        .raft()
        .wait(Some(Duration::from_secs(20)))
        .metrics(
            |metrics| metrics.snapshot.is_some_and(|cut| cut.index >= last.log_id.index),
            "production InstallSnapshot RPC applied",
        )
        .await
        .expect("actual transferred snapshot readiness");
    tokio::time::timeout(Duration::from_secs(10), async {
        while cluster.node(follower).faults.snapshots.load(Ordering::Acquire) <= before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual production snapshot RPC completion");
    let expected = cluster.node(leader).page().await;
    for node in &cluster.nodes {
        assert_eq!(node.page().await, expected, "CAPTURE_RAFT_TCP_SNAPSHOT_PRESERVES_ALL_EVENTS");
    }
    cluster.node(follower).owner().raft().runtime_config().elect(true);
    let leader = cluster.leader(&ids).await;
    let next = cluster
        .node(leader)
        .owner()
        .client_write(command(13))
        .await
        .expect("continue after real TCP snapshot install");
    cluster.applied(&ids, next.log_id.index).await;
    cluster.stop().await;
}

use openraft::{network::RPCOption, raft::VoteRequest};
use rustfs_capture_authority::CaptureTypeConfig;
type ProductionClient = <CaptureRaftNetworkFactory as RaftNetworkFactory<CaptureTypeConfig>>::Network;
async fn network(cluster: &Cluster, local: NodeIdentity, target: u64, limits: CaptureRaftLimits) -> ProductionClient {
    let mut factory =
        CaptureRaftNetworkFactory::new(local, cluster.peers.clone(), config(), limits).expect("production factory config");
    factory.new_client(target, &cluster.peers[&target]).await
}
fn vote(source: u64, term: u64) -> VoteRequest<u64> {
    VoteRequest {
        vote: openraft::Vote::new(term, source),
        last_log_id: None,
    }
}
fn option() -> RPCOption {
    RPCOption::new(Duration::from_secs(3))
}

#[tokio::test]
async fn capture_raft_tcp_identity_binding() {
    let cluster = Cluster::start(2, false).await;
    let target = cluster.node(1);
    let before = std::fs::read(target.directory.path().join("db")).expect("real target bytes before identity attacks");
    let mut wrong_cluster = network(
        &cluster,
        NodeIdentity {
            cluster_id: [6; 16],
            node_id: 0,
        },
        1,
        CaptureRaftLimits::default(),
    )
    .await;
    assert!(
        wrong_cluster.vote(vote(0, 5), option()).await.is_err(),
        "CAPTURE_RAFT_DURABLE_CLUSTER_REQUIRED"
    );
    let mut client = network(&cluster, identity(0), 1, CaptureRaftLimits::default()).await;
    assert!(
        client.vote(vote(1, 5), option()).await.is_err(),
        "CAPTURE_RAFT_RPC_CANDIDATE_MUST_MATCH_SOURCE"
    );
    let mut factory = CaptureRaftNetworkFactory::new(identity(0), cluster.peers.clone(), config(), CaptureRaftLimits::default())
        .expect("legitimate factory");
    let mut wrong_address = factory.new_client(1, &cluster.peers[&0]).await;
    assert!(
        wrong_address.vote(vote(0, 5), option()).await.is_err(),
        "CAPTURE_RAFT_TARGET_ADDRESS_BOUND_TO_MEMBERSHIP"
    );
    let mut unknown = factory.new_client(99, &cluster.peers[&1]).await;
    assert!(unknown.vote(vote(0, 5), option()).await.is_err(), "CAPTURE_RAFT_UNKNOWN_TARGET_REJECTED");
    assert_eq!(
        std::fs::read(target.directory.path().join("db")).expect("unchanged rejected target"),
        before,
        "CAPTURE_RAFT_IDENTITY_REJECTION_NO_STORE_MUTATION"
    );
    assert!(
        target.pings.load(Ordering::Acquire) > 0,
        "identity attacks used actual Ping authentication"
    );
    let epoch = authenticated_epoch(&cluster).await;
    let protected = std::fs::read(target.directory.path().join("db")).expect("stable genuinely authenticated target");
    for (source, target_id, cluster_id) in [(99, 1, [7; 16]), (0, 0, [7; 16]), (0, 1, [6; 16])] {
        let body = encode_request(identity(0), 1, Method::Vote, &vote(source, 6), CaptureRaftLimits::default())
            .expect("production request codec");
        let mut wire: WireRequest = serde_json::from_slice(&body.body).expect("strict production envelope");
        wire.source = source;
        wire.target = target_id;
        wire.cluster = cluster_id;
        let body = CaptureRaftRequest {
            body: serde_json::to_vec(&wire)
                .expect("alter identity without payload recoding")
                .into(),
        };
        let error = raw_vote(&cluster, signed_request(&cluster, body, epoch, false))
            .await
            .expect_err("authenticated wrong capture identity");
        assert_eq!(
            error.code(),
            tonic::Code::PermissionDenied,
            "CAPTURE_RAFT_AUTHENTICATED_IDENTITY_REJECTED"
        );
    }
    assert_eq!(
        std::fs::read(target.directory.path().join("db")).expect("identity attacks retain actual target"),
        protected
    );
    cluster.stop().await;
}

#[tokio::test]
async fn capture_raft_tcp_typed_errors() {
    let mut cluster = Cluster::start(2, false).await;
    let mut client = network(&cluster, identity(0), 1, CaptureRaftLimits::default()).await;
    client.vote(vote(0, 5), option()).await.expect("real admitted Vote RPC");
    let target = cluster.nodes.iter_mut().find(|node| node.id == 1).expect("actual target");
    target
        .owner
        .take()
        .expect("real owner")
        .shutdown()
        .await
        .expect("actual Raft now stopped");
    let error = client
        .vote(vote(0, 6), option())
        .await
        .expect_err("true remote Raft fatal is not success");
    match error {
        openraft::error::RPCError::RemoteError(remote) => assert!(
            matches!(remote.source, openraft::error::RaftError::Fatal(openraft::error::Fatal::Stopped)),
            "CAPTURE_RAFT_PRESERVES_REMOTE_FATAL"
        ),
        other => panic!("CAPTURE_RAFT_REMOTE_ERROR_NOT_FLATTENED: {other:?}"),
    }
    cluster.stop().await;
}

use super::capture_raft::{Method, WireRequest, canonical_request, encode_request};
use rustfs_protos::proto_gen::node_service::capture_raft_service_client::CaptureRaftServiceClient;
fn signed_request(
    cluster: &Cluster,
    body: CaptureRaftRequest,
    epoch: uuid::Uuid,
    unsigned: bool,
) -> tonic::Request<CaptureRaftRequest> {
    let uri: http::Uri = cluster.peers[&1].rpc_uri().parse().expect("actual peer URI");
    let audience = super::http_auth::normalize_tonic_rpc_audience(uri.authority().expect("actual target authority").as_str())
        .expect("normalized actual audience");
    let mut request = tonic::Request::new(body);
    let canonical = canonical_request(&request.get_ref().body);
    super::http_auth::set_tonic_canonical_body_digest(&mut request, &canonical).expect("production canonical request digest");
    let digest = if unsigned {
        "UNSIGNED-PAYLOAD".to_owned()
    } else {
        request
            .metadata()
            .get(super::http_auth::RPC_CONTENT_SHA256_HEADER)
            .expect("digest")
            .to_str()
            .expect("digest text")
            .to_owned()
    };
    let headers =
        super::http_auth::gen_tonic_signature_headers(&audience, "node_service.CaptureRaftService", "Vote", Some(&digest))
            .expect("production v2 signing");
    let timestamp = headers
        .get(super::http_auth::TIMESTAMP_HEADER)
        .expect("signed timestamp")
        .to_str()
        .expect("timestamp text")
        .to_owned();
    request.metadata_mut().as_mut().extend(headers);
    request.metadata_mut().as_mut().extend(
        super::http_auth::gen_tonic_replay_scope_headers(&audience, Method::Vote.path(), &timestamp, &digest, epoch)
            .expect("production boot-scoped signing"),
    );
    request
}
async fn raw_vote(
    cluster: &Cluster,
    mut request: tonic::Request<CaptureRaftRequest>,
) -> Result<tonic::Response<CaptureRaftResponse>, tonic::Status> {
    request.set_timeout(Duration::from_secs(3));
    let endpoint = tonic::transport::Endpoint::from_shared(cluster.peers[&1].rpc_uri().to_owned())
        .expect("actual TCP target")
        .connect_timeout(Duration::from_secs(3));
    let channel = endpoint.connect().await.expect("raw negative request real TCP connection");
    tokio::time::timeout(Duration::from_secs(5), CaptureRaftServiceClient::new(channel).vote(request))
        .await
        .expect("bounded raw negative RPC")
}
async fn authenticated_epoch(cluster: &Cluster) -> uuid::Uuid {
    let mut client = network(cluster, identity(0), 1, CaptureRaftLimits::default()).await;
    client
        .vote(vote(0, 5), option())
        .await
        .expect("actual production factory handshake then Vote");
    assert!(
        cluster.node(1).pings.load(Ordering::Acquire) > 0,
        "CAPTURE_RAFT_BOOT_EPOCH_COMES_FROM_REAL_PING"
    );
    let captured = cluster
        .node(1)
        .faults
        .captured
        .lock()
        .expect("captured genuine request")
        .clone()
        .expect("signed production request");
    captured
        .0
        .get(super::http_auth::RPC_BOOT_EPOCH_HEADER)
        .expect("genuine request authenticated boot epoch")
        .to_str()
        .expect("epoch text")
        .parse()
        .expect("verified real epoch")
}

#[tokio::test]
async fn capture_raft_tcp_auth_binding() {
    let cluster = Cluster::start(2, false).await;
    let epoch = authenticated_epoch(&cluster).await;
    let body =
        encode_request(identity(0), 1, Method::Vote, &vote(0, 6), CaptureRaftLimits::default()).expect("production wire codec");
    assert_eq!(
        raw_vote(&cluster, tonic::Request::new(body.clone()))
            .await
            .expect_err("unsigned network request")
            .code(),
        tonic::Code::Unauthenticated,
        "CAPTURE_RAFT_MISSING_AUTH_REJECTED"
    );
    let mut legacy = signed_request(&cluster, body.clone(), epoch, false);
    let names: Vec<_> = legacy
        .metadata()
        .as_ref()
        .keys()
        .filter(|name| name.as_str() != "x-rustfs-signature" && name.as_str() != super::http_auth::TIMESTAMP_HEADER)
        .cloned()
        .collect();
    for name in names {
        legacy.metadata_mut().as_mut().remove(name);
    }
    assert_eq!(
        raw_vote(&cluster, legacy).await.expect_err("v1 downgrade").code(),
        tonic::Code::Unauthenticated,
        "CAPTURE_RAFT_LEGACY_AUTH_REJECTED"
    );
    let unsigned = signed_request(&cluster, body.clone(), epoch, true);
    assert_eq!(
        raw_vote(&cluster, unsigned)
            .await
            .expect_err("bodyless signing downgrade")
            .code(),
        tonic::Code::Unauthenticated,
        "CAPTURE_RAFT_UNSIGNED_BODY_REJECTED"
    );
    let mut tampered = signed_request(&cluster, body, epoch, false);
    tampered.get_mut().body = encode_request(identity(0), 1, Method::Vote, &vote(0, 7), CaptureRaftLimits::default())
        .expect("different valid wire body")
        .body;
    assert_eq!(
        raw_vote(&cluster, tampered)
            .await
            .expect_err("request changed after genuine signing")
            .code(),
        tonic::Code::Unauthenticated,
        "CAPTURE_RAFT_REQUEST_BOUND_TO_SIGNATURE"
    );
    let mut client = network(&cluster, identity(0), 1, CaptureRaftLimits::default()).await;
    cluster.node(1).faults.mode.store(BAD_PROOF, Ordering::Release);
    assert!(client.vote(vote(0, 8), option()).await.is_err(), "CAPTURE_RAFT_RESPONSE_BOUND_TO_REQUEST");
    cluster.node(1).faults.mode.store(NORMAL, Ordering::Release);
    client.vote(vote(0, 9), option()).await.expect("fresh actual signed response");
    cluster.node(1).faults.mode.store(OLD_RESPONSE, Ordering::Release);
    assert!(
        client.vote(vote(0, 10), option()).await.is_err(),
        "CAPTURE_RAFT_OLD_RESPONSE_NOT_REUSABLE"
    );
    cluster.node(1).faults.mode.store(NORMAL, Ordering::Release);
    cluster.stop().await;
}

#[tokio::test]
async fn capture_raft_tcp_replay_and_boot_epoch() {
    let cluster = Cluster::start(2, false).await;
    let epoch = authenticated_epoch(&cluster).await;
    let body =
        encode_request(identity(0), 1, Method::Vote, &vote(0, 6), CaptureRaftLimits::default()).expect("actual encoded vote");
    let request = signed_request(&cluster, body.clone(), epoch, false);
    let headers = request.metadata().as_ref().clone();
    raw_vote(&cluster, request)
        .await
        .expect("first actually authenticated unique request");
    let before = std::fs::read(cluster.node(1).directory.path().join("db")).expect("stable accepted vote bytes");
    let mut replay = tonic::Request::new(body);
    replay.metadata_mut().as_mut().extend(headers);
    assert_eq!(
        raw_vote(&cluster, replay)
            .await
            .expect_err("same signed request replay")
            .code(),
        tonic::Code::Unauthenticated,
        "CAPTURE_RAFT_NONCE_REPLAY_REJECTED"
    );
    let stale =
        encode_request(identity(0), 1, Method::Vote, &vote(0, 20), CaptureRaftLimits::default()).expect("stale epoch message");
    let stale = signed_request(&cluster, stale, uuid::Uuid::new_v4(), false);
    assert_eq!(
        raw_vote(&cluster, stale)
            .await
            .expect_err("valid signature for wrong server boot")
            .code(),
        tonic::Code::Unauthenticated,
        "CAPTURE_RAFT_STALE_BOOT_REJECTED"
    );
    assert_eq!(
        std::fs::read(cluster.node(1).directory.path().join("db")).expect("rejects retain stable vote"),
        before,
        "CAPTURE_RAFT_REPLAY_NO_STORE_MUTATION"
    );
    cluster.stop().await;
}

#[tokio::test]
async fn capture_raft_tcp_limits_timeout_shutdown() {
    let mut cluster = Cluster::start(2, false).await;
    let epoch = authenticated_epoch(&cluster).await;
    let limits = CaptureRaftLimits::default();
    let oversized = CaptureRaftRequest {
        body: vec![b'0'; limits.max_request_bytes + 1].into(),
    };
    let error = raw_vote(&cluster, tonic::Request::new(oversized))
        .await
        .expect_err("oversized actual TCP request");
    assert_eq!(error.code(), tonic::Code::ResourceExhausted, "CAPTURE_RAFT_TCP_REQUEST_LIMIT");
    let body = encode_request(identity(0), 1, Method::Vote, &vote(0, 6), limits).expect("normal wire request");
    for changed in [
        {
            let mut wire: WireRequest = serde_json::from_slice(&body.body).expect("valid wire");
            wire.version = 2;
            serde_json::to_vec(&wire).expect("wrong-version wire")
        },
        {
            let mut bytes = body.body.to_vec();
            bytes.push(0);
            bytes
        },
        {
            let mut bytes = body.body[..body.body.len() - 1].to_vec();
            bytes.extend_from_slice(b",\"unknown\":true}");
            bytes
        },
    ] {
        let changed = CaptureRaftRequest { body: changed.into() };
        assert_eq!(
            raw_vote(&cluster, signed_request(&cluster, changed, epoch, false))
                .await
                .expect_err("strict actual envelope admission")
                .code(),
            tonic::Code::InvalidArgument,
            "CAPTURE_RAFT_STRICT_WIRE_BOUNDARY"
        );
    }
    let mut client = network(&cluster, identity(0), 1, limits).await;
    client
        .vote(vote(0, 7), option())
        .await
        .expect("warm authenticated transport before controlled timeout");
    let faults = cluster.node(1).faults.clone();
    faults.mode.store(DELAY, Ordering::Release);
    let request = tokio::spawn(async move { client.vote(vote(0, 8), RPCOption::new(Duration::from_millis(300))).await });
    tokio::time::timeout(Duration::from_secs(2), faults.delay_entered.notified())
        .await
        .expect("actual delayed TCP RPC reached service");
    assert!(
        tokio::time::timeout(Duration::from_secs(1), request)
            .await
            .expect("production RPCOption bounds actual request")
            .expect("request task")
            .is_err(),
        "CAPTURE_RAFT_RPC_OPTION_DEADLINE"
    );
    faults.mode.store(NORMAL, Ordering::Release);
    for node in &mut cluster.nodes {
        if let Some(owner) = node.owner.take() {
            owner.shutdown().await.expect("real owner shutdown including workers");
        }
    }
    for node in &mut cluster.nodes {
        if let Some(signal) = node.server_stop.take() {
            let _ = signal.send(());
        }
    }
    for node in &mut cluster.nodes {
        tokio::time::timeout(Duration::from_secs(10), &mut node.server)
            .await
            .expect("production TCP listener shutdown")
            .expect("server task")
            .expect("server result");
    }
    let node = cluster.nodes.pop().expect("target node");
    let TcpNode {
        id,
        directory,
        writer,
        owner: _,
        server: _,
        server_stop: _,
        pings: _,
        faults: _,
        rpc,
    } = node;
    drop(faults);
    drop(rpc);
    drop(writer);
    let reopened = Store::open(directory.path().join("db"), identity(id)).expect("CAPTURE_RAFT_SHUTDOWN_RELEASES_REAL_REDB");
    let vote = reopened
        .read_vote()
        .expect("genuine vote survives transport shutdown")
        .expect("durable vote");
    assert!(
        (7..=8).contains(&vote.term),
        "CAPTURE_RAFT_SHUTDOWN_RETAINS_REAL_VOTE: timeout leaves remote outcome uncertain"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_raft_tcp_encoded_batch_hint_catchup() {
    let limits = CaptureRaftLimits {
        max_request_bytes: 20_480,
        max_snapshot_chunk_bytes: 1024,
        ..CaptureRaftLimits::default()
    };
    let mut selected = (*config()).clone();
    selected.snapshot_max_chunk_size = 1024;
    let config = Arc::new(selected.validate().expect("small valid snapshot chunk"));
    let mut incompatible = (*config).clone();
    incompatible.max_payload_entries = 301;
    let peers = BTreeMap::from([(0, CaptureNode::new("http://127.0.0.1:7000".into()).expect("peer"))]);
    assert!(
        CaptureRaftNetworkFactory::new(identity(0), peers, Arc::new(incompatible), limits).is_err(),
        "CAPTURE_RAFT_BATCH_CONFIG_MUST_FIT_COUNT_LIMIT"
    );
    let cluster = Cluster::start_with(3, true, config.clone(), limits).await;
    let ids = cluster.ids();
    let leader = cluster.leader(&ids).await;
    let follower = ids.iter().copied().find(|id| *id != leader).expect("lagging follower");
    cluster.node(follower).owner().raft().runtime_config().elect(false);
    cluster.node(follower).faults.mode.store(UNAVAILABLE, Ordering::Release);
    let mut last = None;
    for value in 1..=12 {
        let mut key = vec![120; 4096];
        key[4095] = value;
        let value = CaptureCommand::try_created(DecideCreated {
            operation_id: [value; 16],
            object: ObjectIdentity {
                bucket_incarnation: [2; 16],
                key,
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
        .expect("maximal valid native key");
        last = Some(
            cluster
                .node(leader)
                .owner()
                .client_write(value)
                .await
                .expect("actual majority accepts backlog"),
        );
    }
    let last = last.expect("committed backlog");
    assert!(
        cluster.node(follower).page().await.events.is_empty(),
        "follower actually missed large batch"
    );
    cluster.node(follower).faults.mode.store(NORMAL, Ordering::Release);
    cluster.applied(&ids, last.log_id.index).await;
    assert_eq!(
        cluster.node(follower).page().await,
        cluster.node(leader).page().await,
        "CAPTURE_RAFT_BYTE_LIMIT_HINT_COMPLETES_REAL_CATCHUP"
    );
    assert_eq!(cluster.node(follower).page().await.events.len(), 12);
    cluster.node(follower).owner().raft().runtime_config().elect(true);
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_raft_tcp_cancelled_waiter_retains_admission() {
    let limits = CaptureRaftLimits {
        max_inflight: 1,
        ..CaptureRaftLimits::default()
    };
    let cluster = Cluster::start_with(2, false, config(), limits).await;
    let epoch = authenticated_epoch(&cluster).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *cluster.node(1).rpc.worker_pause.lock().expect("true worker pause") = Some((entered.clone(), resume.clone()));
    let raw = encode_request(identity(0), 1, Method::Vote, &vote(0, 8), limits).expect("actual valid request");
    let request = signed_request(&cluster, raw, epoch, false);
    let channel = tonic::transport::Endpoint::from_shared(cluster.peers[&1].rpc_uri().to_owned())
        .expect("endpoint")
        .connect()
        .await
        .expect("actual TCP channel");
    let mut client = CaptureRaftServiceClient::new(channel);
    let waiter = tokio::spawn(async move { client.vote(request).await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("actual accepted service worker retains permit");
    waiter.abort();
    assert!(waiter.await.expect_err("cancelled HTTP waiter").is_cancelled());
    let next = encode_request(identity(0), 1, Method::Vote, &vote(0, 9), limits).expect("second valid request");
    assert_eq!(
        raw_vote(&cluster, signed_request(&cluster, next, epoch, false))
            .await
            .expect_err("cancel cannot release real worker permit")
            .code(),
        tonic::Code::ResourceExhausted,
        "CAPTURE_RAFT_CANCELLED_WAITER_RETAINS_REAL_ADMISSION"
    );
    let rpc = cluster.node(1).rpc.clone();
    let drain = tokio::spawn(async move { rpc.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), async {
            while !drain.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_err(),
        "service drain waits for its actual accepted RPC task"
    );
    resume.notify_one();
    tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .expect("resumed actual worker drains")
        .expect("drain task")
        .expect("transport drain result");
    assert_eq!(
        cluster
            .node(1)
            .writer
            .read_vote()
            .await
            .expect("actual vote storage")
            .expect("accepted worker still ran real Raft")
            .leader_id
            .term,
        8,
        "CAPTURE_RAFT_CANCELLED_HTTP_WORKER_EXECUTES_REAL_RPC"
    );
    cluster.stop().await;
}
