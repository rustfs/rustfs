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
use prost::Message;
use rustfs_lock::distributed_lock::DistributedLock;
use rustfs_lock::{DistributedLockGuard, LockClient, ObjectKey};
use rustfs_protos::proto_gen::node_service::{BatchGenerallyLockResponse, LockAcquireStatus};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::watch;

const TEST: &str = "server::http::tests::production_batch_lock_rpc_tests::production_batch_lock_rpc_quorum_and_cleanup";
const ROLE: &str = "RUSTFS_BATCH_LOCK_WIRE_TEST_ROLE";
const ROOT: &str = "RUSTFS_BATCH_LOCK_WIRE_TEST_ROOT";
const PEER: &str = "RUSTFS_BATCH_LOCK_WIRE_TEST_PEER";
const TTL: Duration = Duration::from_secs(30);
const DEADLINE: Duration = Duration::from_secs(5);

type ObservedRpcFuture = futures::future::BoxFuture<'static, std::io::Result<Response<tonic::body::Body>>>;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
enum ReplyMode {
    #[default]
    Production,
    Legacy,
    UnknownStatus,
    MissingItem,
    ContradictoryStatus,
    UnavailableAfterGrant,
    TerminalRejection,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
struct Fault {
    hold_before: bool,
    hold_after: bool,
    reply: ReplyMode,
}

#[derive(Deserialize, Serialize)]
enum Control {
    Snapshot,
    Configure(Fault),
    Stop,
}

#[derive(Debug, Deserialize, Serialize)]
struct Snapshot {
    batch_calls: usize,
    completed_batches: usize,
    single_calls: usize,
    cleanup_calls: usize,
    refresh_calls: usize,
    active_batches: usize,
    peak_batches: usize,
    batch_sizes: Vec<usize>,
    leases_by_owner: BTreeMap<String, usize>,
}

#[derive(Default)]
struct RpcCounts {
    batch_calls: AtomicUsize,
    completed_batches: AtomicUsize,
    single_calls: AtomicUsize,
    cleanup_calls: AtomicUsize,
    refresh_calls: AtomicUsize,
    active_batches: AtomicUsize,
    peak_batches: AtomicUsize,
    batch_sizes: Mutex<Vec<usize>>,
}

struct BatchCall(Arc<RpcCounts>);

impl Drop for BatchCall {
    fn drop(&mut self) {
        self.0.active_batches.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Deserialize, Serialize)]
struct Ready {
    rpc: String,
    control: String,
    process_id: u32,
}

async fn snapshot(counts: &RpcCounts, local: &Arc<dyn LockClient>) -> Snapshot {
    let mut leases_by_owner = BTreeMap::new();
    for lease in local.list_lock_leases().await {
        *leases_by_owner.entry(lease.owner).or_default() += 1;
    }
    Snapshot {
        batch_calls: counts.batch_calls.load(Ordering::SeqCst),
        completed_batches: counts.completed_batches.load(Ordering::SeqCst),
        single_calls: counts.single_calls.load(Ordering::SeqCst),
        cleanup_calls: counts.cleanup_calls.load(Ordering::SeqCst),
        refresh_calls: counts.refresh_calls.load(Ordering::SeqCst),
        active_batches: counts.active_batches.load(Ordering::SeqCst),
        peak_batches: counts.peak_batches.load(Ordering::SeqCst),
        batch_sizes: counts.batch_sizes.lock().expect("batch sizes lock").clone(),
        leases_by_owner,
    }
}

fn endpoint(address: &str, local: bool) -> Endpoint {
    let mut endpoint = Endpoint::try_from(format!("http://{address}/wire-test").as_str()).expect("lock endpoint");
    endpoint.is_local = local;
    endpoint
}

fn initialize_clients(endpoints: Vec<Endpoint>) {
    storage::init_lock_clients(EndpointServerPools::from(vec![PoolEndpoints {
        legacy: false,
        set_count: 1,
        drives_per_set: endpoints.len(),
        endpoints: Endpoints::from(endpoints),
        cmd_line: "batch-lock-wire-test".into(),
        platform: "test".into(),
    }]));
}

async fn rewrite_reply(
    response: Response<tonic::body::Body>,
    mode: ReplyMode,
    counts: &RpcCounts,
) -> (Response<tonic::body::Body>, bool) {
    // Only compatibility/negative cases alter a response, after the real
    // authentication, decoder, NodeService, and LocalClient have completed.
    let (mut parts, body) = response.into_parts();
    let collected = body.collect().await.expect("production gRPC response body");
    let trailers = collected.trailers().cloned();
    let bytes = collected.to_bytes();
    if bytes.is_empty() {
        // Authentication/bootstrap failures have no lock reply. Preserve
        // them and let the real client perform its normal handshake.
        let frames = trailers
            .into_iter()
            .map(|trailers| Ok::<_, Status>(Frame::trailers(trailers)));
        return (
            Response::from_parts(
                parts,
                tonic::body::Body::new(http_body_util::StreamBody::new(futures::stream::iter(frames))),
            ),
            false,
        );
    }
    assert!(bytes.len() >= 5, "one uncompressed unary response frame");
    assert_eq!(bytes[0], 0, "fixture does not negotiate response compression");
    let size = u32::from_be_bytes(bytes[1..5].try_into().expect("gRPC frame length"));
    assert_eq!(usize::try_from(size).expect("frame fits usize"), bytes.len() - 5);
    let mut reply = BatchGenerallyLockResponse::decode(&bytes[5..]).expect("production lock reply protobuf");
    {
        let mut sizes = counts.batch_sizes.lock().expect("batch sizes lock");
        assert!(sizes.len() < 4096, "a bounded suite cannot issue an unbounded RPC stream");
        // Production NodeService preserves one result slot per request arg.
        // Record before a compatibility fault can remove or alter a slot.
        sizes.push(reply.results.len());
    }
    if matches!(mode, ReplyMode::UnavailableAfterGrant) {
        let authenticated_headers = [
            "x-rustfs-rpc-boot-epoch",
            "x-rustfs-rpc-boot-epoch-proof",
            "x-rustfs-rpc-replay-cache-capability",
            "x-rustfs-rpc-replay-cache-capability-proof",
        ]
        .map(|name| {
            (
                name,
                parts
                    .headers
                    .get(name)
                    .expect("production reply carries authenticated peer proof")
                    .clone(),
            )
        });
        // Lose the acquisition result without revoking the peer's authenticated
        // replay capability: cleanup must still traverse production RPC auth.
        parts.headers.remove(CONTENT_LENGTH);
        parts.headers.remove(Status::GRPC_STATUS_DETAILS);
        Status::unavailable("test response lost after production lock acquisition")
            .add_header(&mut parts.headers)
            .expect("injected unavailable status headers");
        let response = Response::from_parts(parts, tonic::body::Body::empty());
        for (name, expected) in authenticated_headers {
            assert_eq!(response.headers().get(name), Some(&expected), "fault injection must preserve {name}");
        }
        assert_eq!(response.headers().get(Status::GRPC_STATUS), Some(&HeaderValue::from_static("14")));
        return (response, true);
    }
    match mode {
        ReplyMode::Legacy => reply.results.iter_mut().for_each(|result| result.acquire_status = 0),
        ReplyMode::UnknownStatus => reply.results.iter_mut().for_each(|result| result.acquire_status = i32::MAX),
        ReplyMode::MissingItem => {
            reply.results.pop().expect("fixture batch is nonempty");
        }
        ReplyMode::ContradictoryStatus => {
            for result in &mut reply.results {
                if result.success {
                    result.acquire_status = LockAcquireStatus::RetryableNoGrant.into();
                }
            }
        }
        ReplyMode::TerminalRejection => {
            for result in &mut reply.results {
                if !result.success {
                    assert_eq!(
                        result.acquire_status,
                        i32::from(LockAcquireStatus::RetryableNoGrant),
                        "terminal fault may only refine a real no-grant response"
                    );
                    result.acquire_status = LockAcquireStatus::RejectedNoGrant.into();
                }
            }
        }
        ReplyMode::Production => {}
        ReplyMode::UnavailableAfterGrant => unreachable!("handled above"),
    }
    let payload = reply.encode_to_vec();
    let mut framed = Vec::with_capacity(5 + payload.len());
    framed.push(0);
    framed.extend_from_slice(&u32::try_from(payload.len()).expect("small test protobuf").to_be_bytes());
    framed.extend_from_slice(&payload);
    parts.headers.remove(CONTENT_LENGTH);
    let mut frames = vec![Ok::<_, Status>(Frame::data(Bytes::from(framed)))];
    if let Some(trailers) = trailers {
        frames.push(Ok(Frame::trailers(trailers)));
    }
    (
        Response::from_parts(
            parts,
            tonic::body::Body::new(http_body_util::StreamBody::new(futures::stream::iter(frames))),
        ),
        true,
    )
}

async fn receiver() {
    let root = PathBuf::from(std::env::var_os(ROOT).expect("receiver control directory"));
    let peer = std::env::var(PEER).expect("receiver index");
    let rpc_listener = TcpListener::bind("127.0.0.1:0").await.expect("production RPC listener");
    let rpc = rpc_listener.local_addr().expect("RPC listener address").to_string();
    let control_listener = TcpListener::bind("127.0.0.1:0").await.expect("fixture control listener");
    let control = control_listener.local_addr().expect("control address").to_string();
    let _ = rustfs_credentials::set_global_rpc_secret("batch-lock-wire-test-secret".to_owned());
    rustfs_common::set_global_local_node_name(&rpc).await;
    initialize_clients(vec![endpoint(&rpc, true)]);
    let local = storage::get_global_lock_client().expect("one production receiver LocalClient");
    let counts = Arc::new(RpcCounts::default());
    let (faults, _) = watch::channel(Fault::default());
    let node = InterceptedService::new(NodeServiceServer::new(make_server()), check_auth);
    let fallback = tower::service_fn(|_| async { Ok::<_, Infallible>(Response::new(storage::rpc::Body::empty())) });
    let production = hybrid(
        InternodeRpcService::new(fallback),
        RpcRequestPathService::new(Routes::new(node).prepare()),
    );
    let observed_counts = Arc::clone(&counts);
    let observed_faults = faults.clone();
    let service = tower::service_fn(move |request: HttpRequest<hyper::body::Incoming>| -> ObservedRpcFuture {
        let mut production = production.clone();
        let counts = Arc::clone(&observed_counts);
        let mut faults = observed_faults.subscribe();
        Box::pin(async move {
            let path = request.uri().path();
            let is_batch = path.ends_with("/LockBatch");
            let _active = if is_batch {
                counts.batch_calls.fetch_add(1, Ordering::SeqCst);
                let active = counts.active_batches.fetch_add(1, Ordering::SeqCst) + 1;
                counts.peak_batches.fetch_max(active, Ordering::SeqCst);
                Some(BatchCall(Arc::clone(&counts)))
            } else {
                if path.ends_with("/Lock") {
                    counts.single_calls.fetch_add(1, Ordering::SeqCst);
                } else if path.ends_with("/UnLock") || path.ends_with("/UnLockBatch") {
                    counts.cleanup_calls.fetch_add(1, Ordering::SeqCst);
                } else if path.ends_with("/Refresh") {
                    counts.refresh_calls.fetch_add(1, Ordering::SeqCst);
                }
                None
            };
            let initial = *faults.borrow();
            if is_batch {
                while faults.borrow().hold_before {
                    faults.changed().await.expect("receiver control remains alive");
                }
            }
            let response = production
                .call(request)
                .await
                .map_err(|error: Box<dyn std::error::Error + Send + Sync + 'static>| std::io::Error::other(error))?
                .map(tonic::body::Body::new);
            if is_batch {
                let (response, executed) = rewrite_reply(response, initial.reply, &counts).await;
                if executed {
                    counts.completed_batches.fetch_add(1, Ordering::SeqCst);
                    while faults.borrow().hold_after {
                        faults.changed().await.expect("receiver control remains alive");
                    }
                }
                Ok(response)
            } else {
                Ok(response)
            }
        })
    });
    let server = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let (socket, _) = rpc_listener.accept().await.expect("receiver RPC connection");
            let service = service.clone();
            connections.spawn(async move {
                let _ = ConnBuilder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(socket), TowerToHyperService::new(service))
                    .await;
            });
        }
    });
    let ready = Ready {
        rpc,
        control,
        process_id: std::process::id(),
    };
    let temporary = root.join(format!("peer-{peer}.tmp"));
    tokio::fs::write(&temporary, serde_json::to_vec(&ready).expect("ready JSON"))
        .await
        .expect("write ready record");
    tokio::fs::rename(temporary, root.join(format!("peer-{peer}.json")))
        .await
        .expect("publish ready record");
    let (control, _) = control_listener.accept().await.expect("coordinator control connection");
    let mut control = BufReader::new(control);
    loop {
        let mut line = String::new();
        if control.read_line(&mut line).await.expect("control command") == 0 {
            break;
        }
        let command: Control = serde_json::from_str(&line).expect("bounded test command");
        let stop = matches!(command, Control::Stop);
        if let Control::Configure(fault) = command {
            faults.send_replace(fault);
        }
        let mut response = serde_json::to_vec(&snapshot(&counts, &local).await).expect("snapshot JSON");
        response.push(b'\n');
        control.get_mut().write_all(&response).await.expect("control response");
        if stop {
            break;
        }
    }
    server.abort();
    let _ = server.await;
    assert!(local.list_lock_leases().await.is_empty(), "receiver has no leftover leases");
    println!("BATCH_LOCK_RECEIVER_OK");
}

struct Receiver {
    child: Child,
    control: BufReader<TcpStream>,
    rpc: String,
    log: PathBuf,
}

impl Receiver {
    async fn start(root: &Path, index: usize) -> Self {
        let log = root.join(format!("receiver-{index}.log"));
        let stdout = std::fs::File::create(&log).expect("receiver output");
        let mut child = Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
            .env(ROLE, "receiver")
            .env(ROOT, root)
            .env(PEER, index.to_string())
            .env_remove("HTTP_PROXY")
            .env_remove("HTTPS_PROXY")
            .env_remove("ALL_PROXY")
            .env_remove("http_proxy")
            .env_remove("https_proxy")
            .env_remove("all_proxy")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost")
            .stdout(Stdio::from(stdout.try_clone().expect("receiver output clone")))
            .stderr(Stdio::from(stdout))
            .kill_on_drop(true)
            .spawn()
            .expect("isolated receiver process");
        let ready = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Ok(bytes) = tokio::fs::read(root.join(format!("peer-{index}.json"))).await {
                    break serde_json::from_slice::<Ready>(&bytes).expect("receiver ready JSON");
                }
                assert!(
                    child.try_wait().expect("receiver status").is_none(),
                    "receiver exited: {}",
                    std::fs::read_to_string(&log).unwrap_or_default()
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("receiver startup deadline");
        assert_ne!(
            ready.process_id,
            std::process::id(),
            "each node owns a distinct process/global LocalClient"
        );
        let control = BufReader::new(TcpStream::connect(&ready.control).await.expect("control channel"));
        Self {
            child,
            control,
            rpc: ready.rpc,
            log,
        }
    }

    async fn command(&mut self, command: Control) -> Snapshot {
        tokio::time::timeout(DEADLINE, async {
            let mut bytes = serde_json::to_vec(&command).expect("command JSON");
            bytes.push(b'\n');
            self.control.get_mut().write_all(&bytes).await.expect("send control command");
            let mut line = String::new();
            assert!(
                self.control.read_line(&mut line).await.expect("read snapshot") > 0,
                "receiver control EOF"
            );
            serde_json::from_str(&line).expect("receiver snapshot JSON")
        })
        .await
        .expect("control command deadline")
    }

    async fn stop(mut self) {
        let final_state = self.command(Control::Stop).await;
        assert!(final_state.leases_by_owner.is_empty());
        let status = tokio::time::timeout(DEADLINE, self.child.wait())
            .await
            .expect("receiver exit deadline")
            .expect("receiver status");
        let output = tokio::fs::read_to_string(&self.log).await.expect("receiver execution log");
        assert!(status.success(), "receiver failed: {output}");
        assert!(output.contains("BATCH_LOCK_RECEIVER_OK"), "missing real receiver execution marker");
    }
}

struct Cluster {
    peers: Vec<Receiver>,
    clients: Vec<Arc<dyn LockClient>>,
    local: Arc<dyn LockClient>,
    locks: Arc<DistributedLock>,
    _root: tempfile::TempDir,
}

impl Cluster {
    async fn start() -> Self {
        let root = tempfile::tempdir().expect("isolated lock receiver fixture");
        let mut peers = Vec::new();
        for index in 0..3 {
            peers.push(Receiver::start(root.path(), index).await);
        }
        let local_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("coordinator identity reservation");
        let local_address = local_listener.local_addr().expect("coordinator address").to_string();
        let _ = rustfs_credentials::set_global_rpc_secret("batch-lock-wire-test-secret".to_owned());
        rustfs_common::set_global_local_node_name(&local_address).await;
        let endpoints = std::iter::once(endpoint(&local_address, true))
            .chain(peers.iter().map(|peer| endpoint(&peer.rpc, false)))
            .collect();
        initialize_clients(endpoints);
        let published = storage::get_global_lock_clients().expect("production client map");
        let local = Arc::clone(published.get(&local_address).expect("coordinator LocalClient"));
        assert!(local.is_local().await);
        let mut clients = vec![Arc::clone(&local)];
        for peer in &peers {
            let client = Arc::clone(published.get(&peer.rpc).expect("production RemoteClient"));
            assert!(!client.is_local().await);
            clients.push(client);
        }
        let locks = Arc::new(DistributedLock::new("wire-test".to_owned(), clients.clone(), 3));
        let mut cluster = Self {
            peers,
            clients,
            local,
            locks,
            _root: root,
        };
        for (index, client) in cluster.clients.iter().enumerate().skip(1).take(3) {
            let peer = DistributedLock::new("warmup".to_owned(), vec![Arc::clone(client)], 1);
            drop(holder(&peer, keys(&format!("warmup-{index}"), 1).remove(0), "warmup-holder").await);
        }
        cluster.idle().await;
        cluster
    }

    async fn states(&mut self) -> Vec<Snapshot> {
        let mut states = Vec::new();
        for peer in &mut self.peers {
            states.push(peer.command(Control::Snapshot).await);
        }
        states
    }

    async fn idle(&mut self) {
        let mut last_observation = None;
        let convergence = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let states = self.states().await;
                let mut local_leases_by_owner = BTreeMap::<String, usize>::new();
                for lease in self.local.list_lock_leases().await {
                    *local_leases_by_owner.entry(lease.owner).or_default() += 1;
                }
                let idle = local_leases_by_owner.is_empty()
                    && states
                        .iter()
                        .all(|state| state.leases_by_owner.is_empty() && state.active_batches == 0);
                last_observation = Some((local_leases_by_owner, states));
                if idle {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            convergence.is_ok(),
            "all four physical lease owners and RPCs must converge; last local owners and peer snapshots: {last_observation:?}"
        );
    }

    async fn wait_calls(&mut self, peer: usize, calls: usize, completed: bool) {
        tokio::time::timeout(DEADLINE, async {
            loop {
                let state = self.peers[peer].command(Control::Snapshot).await;
                let count = if completed {
                    state.completed_batches
                } else {
                    state.batch_calls
                };
                if count >= calls {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("observed production batch RPC entry/completion");
    }

    async fn stop(mut self) {
        self.idle().await;
        for peer in self.peers {
            peer.stop().await;
        }
    }
}

fn keys(prefix: &str, count: usize) -> Vec<ObjectKey> {
    (0..count)
        .map(|index| ObjectKey::new("wire-bucket", format!("{prefix}-{index:04}")))
        .collect()
}

fn assert_grants(results: &[rustfs_lock::Result<DistributedLockGuard>], expected: usize) {
    assert_eq!(results.len(), expected);
    for (index, result) in results.iter().enumerate() {
        let guard = result.as_ref().unwrap_or_else(|error| panic!("key {index} failed: {error}"));
        assert!(!guard.is_lock_lost(), "cannot return a lost guard for key {index}");
        assert!(guard.granted_lock_ids().count() >= 3, "original write quorum retained");
    }
}

async fn holder(locks: &DistributedLock, key: ObjectKey, owner: &str) -> DistributedLockGuard {
    locks
        .lock_guard_quiet(key, owner, DEADLINE, TTL)
        .await
        .expect("distributed holder request")
        .expect("legal distributed holder quorum")
}

async fn healthy_and_contended(cluster: &mut Cluster) {
    let resources = keys("healthy", 1000);
    let before = cluster.states().await;
    let results = cluster
        .locks
        .acquire_guard_batch(&resources, "healthy-batch", DEADLINE, TTL)
        .await;
    assert_grants(&results, 1000);
    for (index, state) in before.iter().enumerate() {
        cluster.wait_calls(index, state.batch_calls + 1, false).await;
    }
    let after = cluster.states().await;
    for (before, after) in before.iter().zip(&after) {
        assert_eq!(
            after.batch_calls - before.batch_calls,
            1,
            "healthy 1000-key batch uses one RPC per remote peer"
        );
    }
    drop(results);
    cluster.idle().await;

    let resources = keys("hot", 1000);
    let held = holder(&cluster.locks, resources[0].clone(), "hot-holder").await;
    let before = cluster.states().await;
    let started = Instant::now();
    let results = cluster
        .locks
        .acquire_guard_batch(&resources, "hot-contender", Duration::from_secs(2), TTL)
        .await;
    assert!(results[0].is_err(), "live holder must exclude a competing write quorum");
    assert_grants(&results[1..], 999);
    assert!(!held.is_lock_lost(), "contention must not release the holder");
    assert!(started.elapsed() < Duration::from_secs(4), "foreground total budget remains bounded");
    let after = cluster.states().await;
    for (before, after) in before.iter().zip(&after) {
        let sizes = &after.batch_sizes[before.batch_sizes.len()..];
        assert!(
            sizes.first().is_none_or(|size| *size == 1000),
            "first attempt retains the requested batch"
        );
        assert!(
            sizes.iter().skip(1).all(|size| *size == 1),
            "only the unresolved hot key can be retried: {sizes:?}"
        );
        assert!(
            after.batch_calls - before.batch_calls <= 8,
            "bounded hot-key retries cannot become per-key RPC fanout"
        );
    }
    drop(results);
    drop(held);
    cluster.idle().await;
}

async fn release_within_budget(cluster: &mut Cluster, wait_for_retry: bool) {
    let resources = keys(if wait_for_retry { "retry" } else { "first-round" }, 32);
    let held = holder(&cluster.locks, resources[0].clone(), "release-holder").await;
    let before = cluster.states().await;
    let locks = Arc::clone(&cluster.locks);
    let pending = tokio::spawn(async move {
        locks
            .acquire_guard_batch(&resources, "release-contender", DEADLINE, TTL)
            .await
    });
    // At least two remote peers must have the holder because q=3 and
    // there is only one local vote. Wait on actual RPC observations.
    let conflicted = before
        .iter()
        .position(|state| state.leases_by_owner.contains_key("release-holder"))
        .expect("distributed holder has a remote lease");
    cluster
        .wait_calls(conflicted, before[conflicted].batch_calls + if wait_for_retry { 2 } else { 1 }, false)
        .await;
    assert!(!pending.is_finished(), "contender cannot succeed while holder owns a quorum");
    drop(held);
    let results = tokio::time::timeout(DEADLINE, pending)
        .await
        .expect("holder release remains inside total budget")
        .expect("contender task");
    assert_grants(&results, 32);
    drop(results);
    cluster.idle().await;
}

async fn distinct_peer_contention(cluster: &mut Cluster) {
    let resources = keys("distinct-peer", 1000);
    let one_peer = DistributedLock::new("peer-one".to_owned(), vec![Arc::clone(&cluster.clients[1])], 1);
    let another_peer = DistributedLock::new("peer-two".to_owned(), vec![Arc::clone(&cluster.clients[2])], 1);
    let first = holder(&one_peer, resources[0].clone(), "peer-one-holder").await;
    let second = holder(&another_peer, resources[1].clone(), "peer-two-holder").await;
    let results = cluster
        .locks
        .acquire_guard_batch(&resources, "distinct-peer-contender", DEADLINE, TTL)
        .await;
    assert_grants(&results, 1000);
    assert!(!first.is_lock_lost() && !second.is_lock_lost());
    drop(results);
    drop(first);
    drop(second);
    cluster.idle().await;
}

async fn reset_degraded_peer(cluster: &mut Cluster, label: &str) {
    cluster.peers[2].command(Control::Configure(Fault::default())).await;
    cluster.idle().await;
    let remote = DistributedLock::new(label.to_owned(), vec![Arc::clone(&cluster.clients[3])], 1);
    drop(holder(&remote, keys(label, 1).remove(0), "recovered-peer-holder").await);
    cluster.idle().await;
}

async fn ambiguous_peer_subset_retry(cluster: &mut Cluster, terminal: bool) {
    let resources = keys(if terminal { "terminal-fourth" } else { "retryable-fourth" }, 1);
    let fourth = DistributedLock::new("fourth-peer".to_owned(), vec![Arc::clone(&cluster.clients[2])], 1);
    let held = holder(&fourth, resources[0].clone(), "fourth-peer-holder").await;
    cluster.peers[1]
        .command(Control::Configure(Fault {
            reply: if terminal {
                ReplyMode::TerminalRejection
            } else {
                ReplyMode::Production
            },
            ..Fault::default()
        }))
        .await;
    cluster.peers[2]
        .command(Control::Configure(Fault {
            reply: ReplyMode::UnavailableAfterGrant,
            ..Fault::default()
        }))
        .await;
    let before = cluster.states().await;
    let locks = Arc::clone(&cluster.locks);
    let pending = tokio::spawn(async move {
        locks
            .acquire_guard_batch(&resources, "subset-retry-owner", DEADLINE, TTL)
            .await
    });
    let results = if terminal {
        let results = tokio::time::timeout(DEADLINE, pending)
            .await
            .expect("terminal no-grant ends the unresolved key")
            .expect("terminal contender task");
        assert!(results.iter().all(|result| result.is_err()));
        assert!(!held.is_lock_lost(), "terminal rejection cannot release the legal holder");
        drop(held);
        results
    } else {
        // Local + the first remote are the two usable grants. Their next
        // acquire can start only after the old partial grants were released.
        cluster.wait_calls(0, before[0].batch_calls + 2, false).await;
        assert!(!pending.is_finished(), "the fourth peer remains legally held during retry");
        drop(held);
        let results = tokio::time::timeout(DEADLINE, pending)
            .await
            .expect("three usable peers complete inside the total budget")
            .expect("subset retry task");
        assert_grants(&results, 1);
        results
    };
    cluster.wait_calls(2, before[2].batch_calls + 1, false).await;
    let after = cluster.states().await;
    assert_eq!(
        after[2].batch_calls - before[2].batch_calls,
        1,
        "an ambiguous peer must not receive another acquire for the same key"
    );
    if terminal {
        assert_eq!(after[0].batch_calls - before[0].batch_calls, 1);
        assert_eq!(after[1].batch_calls - before[1].batch_calls, 1);
    } else {
        assert!(
            after[0].cleanup_calls > before[0].cleanup_calls,
            "partial grant rollback crossed the real RPC path"
        );
        assert!(after[0].batch_calls - before[0].batch_calls <= 8);
        assert!(after[1].batch_calls - before[1].batch_calls <= 8);
    }
    drop(results);
    cluster.peers[1].command(Control::Configure(Fault::default())).await;
    reset_degraded_peer(cluster, if terminal { "after-terminal" } else { "after-subset-retry" }).await;
}

async fn local_contention_with_degraded_remote(cluster: &mut Cluster) {
    let resources = keys("local-and-degraded", 32);
    let holder_locks = DistributedLock::new("local-holder-quorum".to_owned(), cluster.clients[..3].to_vec(), 3);
    let held = holder(&holder_locks, resources[0].clone(), "local-degraded-holder").await;
    assert!(
        cluster
            .local
            .list_lock_leases()
            .await
            .iter()
            .any(|lease| lease.owner == "local-degraded-holder"),
        "the contention must include the production LocalClient"
    );
    cluster.peers[2]
        .command(Control::Configure(Fault {
            reply: ReplyMode::UnavailableAfterGrant,
            ..Fault::default()
        }))
        .await;
    let before = cluster.states().await;
    let locks = Arc::clone(&cluster.locks);
    let started = Instant::now();
    let pending = tokio::spawn(async move {
        locks
            .acquire_guard_batch(&resources, "local-degraded-contender", DEADLINE, TTL)
            .await
    });
    // Wait for a second real no-grant round instead of using a sleep to
    // presume that the first local wait has finished.
    cluster.wait_calls(0, before[0].completed_batches + 2, true).await;
    assert!(started.elapsed() > Duration::from_secs(1), "the holder outlives one peer wait window");
    assert!(!pending.is_finished(), "a live local quorum still excludes the contender");
    assert!(!held.is_lock_lost());
    drop(held);
    let results = tokio::time::timeout(DEADLINE, pending)
        .await
        .expect("local retry preserves the remaining total budget")
        .expect("local contender task");
    assert_grants(&results, 32);
    assert!(started.elapsed() < DEADLINE, "the caller must not receive a new total budget");
    let after = cluster.states().await;
    assert_eq!(after[2].batch_calls - before[2].batch_calls, 1, "the degraded peer is retired");
    for peer in 0..2 {
        let sizes = &after[peer].batch_sizes[before[peer].batch_sizes.len()..];
        assert_eq!(sizes.first(), Some(&32));
        assert!(sizes.iter().skip(1).all(|size| *size == 1), "cold keys must not be acquired again");
        assert!(after[peer].batch_calls - before[peer].batch_calls <= 8);
    }
    drop(results);
    reset_degraded_peer(cluster, "after-local-degraded").await;
}

async fn overlapping_batches_survive_repeated_cancellation(cluster: &mut Cluster) {
    for iteration in 0..3 {
        let first_keys = keys(&format!("overlap-{iteration}"), 32);
        let second_keys = first_keys[16..]
            .iter()
            .cloned()
            .chain(keys(&format!("overlap-extra-{iteration}"), 16))
            .collect::<Vec<_>>();
        for peer in &mut cluster.peers[1..] {
            peer.command(Control::Configure(Fault {
                hold_after: true,
                ..Fault::default()
            }))
            .await;
        }
        let before = cluster.states().await;
        let locks = Arc::clone(&cluster.locks);
        let first = tokio::spawn(async move {
            locks
                .acquire_guard_batch(&first_keys, "cancelled-overlap-owner", DEADLINE, TTL)
                .await
        });
        for (peer, state) in before.iter().enumerate().skip(1) {
            cluster.wait_calls(peer, state.completed_batches + 1, true).await;
        }
        assert!(!first.is_finished(), "two held replies prevent a completed quorum");
        let locks = Arc::clone(&cluster.locks);
        let second = tokio::spawn(async move {
            locks
                .acquire_guard_batch(&second_keys, "surviving-overlap-owner", DEADLINE, TTL)
                .await
        });
        for (peer, state) in before.iter().enumerate() {
            cluster.wait_calls(peer, state.batch_calls + 2, false).await;
        }
        assert!(!second.is_finished(), "overlapping acquisition reaches the same held peers");
        first.abort();
        assert!(first.await.expect_err("first caller is cancelled").is_cancelled());
        for peer in &mut cluster.peers[1..] {
            peer.command(Control::Configure(Fault::default())).await;
        }
        let results = tokio::time::timeout(DEADLINE, second)
            .await
            .expect("surviving overlapping batch completes after exact-ID cleanup")
            .expect("surviving contender task");
        assert_grants(&results, 32);
        drop(results);
        cluster.idle().await;
        let after = cluster.states().await;
        for (before, after) in before.iter().zip(&after) {
            assert!(after.batch_calls - before.batch_calls <= 12, "overlapping retries remain bounded");
            assert!(
                after.cleanup_calls > before.cleanup_calls,
                "cancelled and completed grants are released over RPC"
            );
            assert!(after.leases_by_owner.is_empty(), "repeated cancellation cannot accumulate leases");
        }
    }
}

async fn pending_third_vote(cluster: &mut Cluster, ambiguous_fourth: bool) {
    let resources = keys(
        if ambiguous_fourth {
            "ambiguous-fourth"
        } else {
            "pending-fourth"
        },
        32,
    );
    let before = cluster.states().await;
    cluster.peers[1]
        .command(Control::Configure(Fault {
            hold_before: true,
            ..Fault::default()
        }))
        .await;
    cluster.peers[2]
        .command(Control::Configure(if ambiguous_fourth {
            Fault {
                reply: ReplyMode::UnavailableAfterGrant,
                ..Fault::default()
            }
        } else {
            Fault {
                hold_before: true,
                ..Fault::default()
            }
        }))
        .await;
    let locks = Arc::clone(&cluster.locks);
    let pending = tokio::spawn(async move {
        locks
            .acquire_guard_batch(&resources, "third-vote-contender", DEADLINE, TTL)
            .await
    });
    cluster.wait_calls(1, before[1].batch_calls + 1, false).await;
    cluster.wait_calls(2, before[2].batch_calls + 1, ambiguous_fourth).await;
    assert!(!pending.is_finished(), "two votes plus a pending third must remain pending");
    cluster.peers[1].command(Control::Configure(Fault::default())).await;
    let results = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .expect("third valid vote completes without waiting for the fourth peer")
        .expect("third-vote contender task");
    assert_grants(&results, 32);
    drop(results);
    cluster.peers[2].command(Control::Configure(Fault::default())).await;
    cluster.idle().await;
    // Reset request-breaker history with a real successful operation.
    let remote = DistributedLock::new("peer-probe".to_owned(), vec![Arc::clone(&cluster.clients[3])], 1);
    drop(holder(&remote, keys("post-fault-probe", 1).remove(0), "probe-holder").await);
    cluster.idle().await;
}

async fn late_rpc_cleanup(cluster: &mut Cluster) {
    use rustfs_lock::{BatchAcquireOutcome, LockRequest, LockType};

    let before = cluster.peers[2]
        .command(Control::Configure(Fault {
            hold_after: true,
            ..Fault::default()
        }))
        .await;
    let resource = keys("late-rpc", 1).remove(0);
    let request = LockRequest::new(resource.clone(), LockType::Exclusive, "late-rpc-owner")
        .with_acquire_timeout(DEADLINE)
        .with_ttl(TTL);
    let original_id = request.lock_id.clone();
    let remote = Arc::clone(&cluster.clients[3]);
    let pending = tokio::spawn(async move {
        remote
            .acquire_locks_batch_until(&[request], tokio::time::Instant::now() + Duration::from_secs(1))
            .await
    });
    cluster.wait_calls(2, before.completed_batches + 1, true).await;
    let outcomes = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .expect("remote total deadline")
        .expect("timed-out remote task");
    assert!(
        matches!(outcomes.as_slice(), [BatchAcquireOutcome::Ambiguous { .. }]),
        "lost reply cannot prove no grant"
    );
    cluster.peers[2].command(Control::Configure(Fault::default())).await;
    cluster.idle().await;
    let successor_locks = DistributedLock::new("late-successor".to_owned(), vec![Arc::clone(&cluster.clients[3])], 1);
    let successor = holder(&successor_locks, resource, "late-successor-holder").await;
    let outcome = cluster.clients[3]
        .cleanup_grant(&original_id)
        .await
        .expect("late exact-ID cleanup");
    assert!(matches!(outcome, rustfs_lock::LockCleanupOutcome::AlreadyAbsent));
    for id in successor.granted_lock_ids() {
        assert!(cluster.clients[3].refresh(id).await.expect("successor lease remains present"));
    }
    assert!(!successor.is_lock_lost(), "old-ID cleanup must not unlock the successor");
    drop(successor);
    cluster.idle().await;
}

async fn queued_rpc_deadline_never_reaches_backend(cluster: &mut Cluster) {
    use rustfs_lock::{BatchAcquireOutcome, LockRequest, LockType};

    // Preserve the production RemoteClient and authenticated request path while
    // making the real tonic Channel queue observable with one occupied permit.
    let peer = 2;
    let address = endpoint(&cluster.peers[peer].rpc, false).url.to_string();
    let original_channel = rustfs_common::cached_connection(&address)
        .await
        .expect("warmup cached the production control channel");
    let queued_channel = tonic::transport::Endpoint::from_shared(address.clone())
        .expect("receiver endpoint")
        .concurrency_limit(1)
        .connect()
        .await
        .expect("real single-permit control channel");
    rustfs_common::cache_connection(address.clone(), queued_channel).await;

    let before = cluster.peers[peer]
        .command(Control::Configure(Fault {
            hold_after: true,
            ..Fault::default()
        }))
        .await;
    let first = LockRequest::new(keys("queue-permit-holder", 1).remove(0), LockType::Exclusive, "queue-permit-holder")
        .with_acquire_timeout(DEADLINE)
        .with_ttl(TTL);
    let first_id = first.lock_id.clone();
    let remote = Arc::clone(&cluster.clients[peer + 1]);
    let occupied = tokio::spawn(async move {
        remote
            .acquire_locks_batch_until(&[first], tokio::time::Instant::now() + DEADLINE)
            .await
    });
    cluster.wait_calls(peer, before.completed_batches + 1, true).await;
    let held = cluster.peers[peer].command(Control::Snapshot).await;
    assert_eq!(held.leases_by_owner.get("queue-permit-holder"), Some(&1));
    assert_eq!(held.active_batches, 1, "the first response still occupies the channel permit");
    assert!(!occupied.is_finished());

    let queued = LockRequest::new(
        keys("expired-in-channel-queue", 1).remove(0),
        LockType::Exclusive,
        "expired-in-channel-queue",
    )
    .with_acquire_timeout(DEADLINE)
    .with_ttl(TTL);
    let remote = Arc::clone(&cluster.clients[peer + 1]);
    let outcomes = remote
        .acquire_locks_batch_until(&[queued], tokio::time::Instant::now() + Duration::from_millis(200))
        .await;
    assert!(
        matches!(outcomes.as_slice(), [BatchAcquireOutcome::RetryableNoGrant { .. }]),
        "an expired request still inside the Channel queue has no submitted payload: {outcomes:?}"
    );
    assert!(!occupied.is_finished(), "queue expiry must not complete the permit holder");

    cluster.peers[peer].command(Control::Configure(Fault::default())).await;
    let first_outcomes = tokio::time::timeout(DEADLINE, occupied)
        .await
        .expect("permit holder completes after gate release")
        .expect("permit holder task");
    assert!(matches!(first_outcomes.as_slice(), [BatchAcquireOutcome::Granted { .. }]));
    assert!(matches!(
        remote
            .cleanup_grant(&first_id)
            .await
            .expect("release the real permit holder grant"),
        rustfs_lock::LockCleanupOutcome::Released
    ));
    // This real follow-up RPC has crossed the same single-permit queue. Once it
    // completes, a previously enqueued request cannot be hidden behind the gate.
    cluster.idle().await;
    let after = cluster.peers[peer].command(Control::Snapshot).await;
    assert_eq!(
        after.completed_batches - before.completed_batches,
        1,
        "releasing the channel permit cannot execute the expired queued batch"
    );
    assert_eq!(
        &after.batch_sizes[before.batch_sizes.len()..],
        &[1],
        "only the permit holder reached production NodeService lock processing"
    );
    assert!(!after.leases_by_owner.contains_key("expired-in-channel-queue"));
    rustfs_common::cache_connection(address, original_channel).await;
}

async fn quorum_deadline_cleans_late_votes(cluster: &mut Cluster) {
    for peer in &mut cluster.peers[1..] {
        peer.command(Control::Configure(Fault {
            hold_after: true,
            ..Fault::default()
        }))
        .await;
    }
    let resources = keys("quorum-deadline", 32);
    let results = cluster
        .locks
        .acquire_guard_batch(&resources, "quorum-deadline-owner", Duration::from_millis(600), TTL)
        .await;
    assert_eq!(results.len(), 32);
    assert!(
        results.iter().all(|result| result.is_err()),
        "two timely votes cannot become a valid write quorum after the deadline"
    );
    for peer in &mut cluster.peers[1..] {
        peer.command(Control::Configure(Fault::default())).await;
    }
    cluster.idle().await;
}

async fn short_rpc_budget_and_exhausted_deadline(cluster: &mut Cluster) {
    use rustfs_lock::{BatchAcquireOutcome, LockRequest, LockType};

    let remote = Arc::clone(&cluster.clients[3]);
    let remote_locks = DistributedLock::new("short-rpc".to_owned(), vec![Arc::clone(&remote)], 1);
    let resources = keys("short-rpc", 32);
    let held = holder(&remote_locks, resources[0].clone(), "short-rpc-holder").await;
    let requests = resources
        .into_iter()
        .map(|key| {
            LockRequest::new(key, LockType::Exclusive, "short-rpc-contender")
                .with_acquire_timeout(DEADLINE)
                .with_ttl(TTL)
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    let outcomes = temp_env::async_with_vars([("RUSTFS_OBJECT_LOCK_RPC_TIMEOUT_MS", Some("250"))], async {
        remote
            .acquire_locks_batch_until(&requests, tokio::time::Instant::now() + DEADLINE)
            .await
    })
    .await;
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "backend wait preserves short-RPC response headroom"
    );
    assert_eq!(outcomes.len(), 32);
    assert!(
        matches!(outcomes[0], BatchAcquireOutcome::RetryableNoGrant { .. }),
        "a held key returns a typed no-grant before RPC expiry"
    );
    for (request, outcome) in requests.iter().zip(&outcomes).skip(1) {
        let BatchAcquireOutcome::Granted { lock_info, .. } = outcome else {
            panic!("an uncontended key was lost with its peer batch: {outcome:?}");
        };
        let id = lock_info.as_ref().map(|info| &info.id).unwrap_or(&request.lock_id);
        assert!(matches!(
            remote.cleanup_grant(id).await.expect("release confirmed remote grant"),
            rustfs_lock::LockCleanupOutcome::Released
        ));
    }
    assert!(!held.is_lock_lost());
    drop(held);
    cluster.idle().await;

    let before = cluster.peers[2].command(Control::Snapshot).await;
    let outcomes = remote.acquire_locks_batch_until(&requests, tokio::time::Instant::now()).await;
    assert!(
        outcomes
            .iter()
            .all(|outcome| !matches!(outcome, BatchAcquireOutcome::Granted { .. }))
    );
    let after = cluster.peers[2].command(Control::Snapshot).await;
    assert_eq!(
        before.batch_calls, after.batch_calls,
        "an exhausted absolute deadline cannot dispatch another RPC"
    );
    cluster.idle().await;
}

async fn wire_compatibility_and_unknowns(cluster: &mut Cluster) {
    for peer in &mut cluster.peers {
        peer.command(Control::Configure(Fault {
            reply: ReplyMode::Legacy,
            ..Fault::default()
        }))
        .await;
    }
    let resources = keys("legacy-success", 32);
    let results = cluster
        .locks
        .acquire_guard_batch(&resources, "legacy-success-owner", DEADLINE, TTL)
        .await;
    assert_grants(&results, 32);
    drop(results);
    cluster.idle().await;
    let resources = keys("legacy-false", 1);
    let held = holder(&cluster.locks, resources[0].clone(), "legacy-false-holder").await;
    let before = cluster.states().await;
    let results = cluster
        .locks
        .acquire_guard_batch(&resources, "legacy-false-contender", Duration::from_secs(2), TTL)
        .await;
    assert!(
        results.iter().all(|result| result.is_err()),
        "legacy false cannot become a usable no-grant proof"
    );
    assert!(!held.is_lock_lost(), "uncertain cleanup must not release the legal legacy holder");
    for (peer, state) in before.iter().enumerate() {
        cluster.wait_calls(peer, state.batch_calls + 1, false).await;
    }
    let after = cluster.states().await;
    for (before, after) in before.iter().zip(&after) {
        assert_eq!(after.batch_calls - before.batch_calls, 1, "legacy false cannot authorize another acquire");
    }
    drop(results);
    drop(held);
    for peer in &mut cluster.peers {
        peer.command(Control::Configure(Fault::default())).await;
    }
    cluster.idle().await;

    for (index, mode) in [
        ReplyMode::UnknownStatus,
        ReplyMode::MissingItem,
        ReplyMode::ContradictoryStatus,
    ]
    .into_iter()
    .enumerate()
    {
        let before = cluster.peers[2]
            .command(Control::Configure(Fault {
                reply: mode,
                ..Fault::default()
            }))
            .await;
        let resources = keys(&format!("unknown-wire-{index}"), 32);
        let results = cluster
            .locks
            .acquire_guard_batch(&resources, "unknown-wire-owner", DEADLINE, TTL)
            .await;
        assert_grants(&results, 32);
        cluster.wait_calls(2, before.batch_calls + 1, false).await;
        let after = cluster.peers[2].command(Control::Snapshot).await;
        assert_eq!(
            after.batch_calls - before.batch_calls,
            1,
            "ambiguous peer cannot receive an overlapping retry"
        );
        drop(results);
        cluster.peers[2].command(Control::Configure(Fault::default())).await;
        cluster.idle().await;
        let probe = DistributedLock::new("wire-probe".to_owned(), vec![Arc::clone(&cluster.clients[3])], 1);
        drop(holder(&probe, keys(&format!("wire-probe-{index}"), 1).remove(0), "wire-probe-holder").await);
        cluster.idle().await;
    }
}

async fn coordinator() {
    println!("BATCH_LOCK_WIRE_STAGE start");
    let mut cluster = Cluster::start().await;
    println!("BATCH_LOCK_WIRE_STAGE healthy_and_contended");
    healthy_and_contended(&mut cluster).await;
    println!("BATCH_LOCK_WIRE_STAGE release_within_budget_first_round");
    release_within_budget(&mut cluster, false).await;
    println!("BATCH_LOCK_WIRE_STAGE release_within_budget_retry");
    release_within_budget(&mut cluster, true).await;
    println!("BATCH_LOCK_WIRE_STAGE distinct_peer_contention");
    distinct_peer_contention(&mut cluster).await;
    println!("BATCH_LOCK_WIRE_STAGE ambiguous_peer_subset_retry_retryable");
    ambiguous_peer_subset_retry(&mut cluster, false).await;
    println!("BATCH_LOCK_WIRE_STAGE ambiguous_peer_subset_retry_terminal");
    ambiguous_peer_subset_retry(&mut cluster, true).await;
    println!("BATCH_LOCK_WIRE_STAGE local_contention_with_degraded_remote");
    local_contention_with_degraded_remote(&mut cluster).await;
    println!("BATCH_LOCK_WIRE_STAGE overlapping_batches_survive_repeated_cancellation");
    overlapping_batches_survive_repeated_cancellation(&mut cluster).await;
    println!("BATCH_LOCK_WIRE_STAGE pending_third_vote_pending_fourth");
    pending_third_vote(&mut cluster, false).await;
    println!("BATCH_LOCK_WIRE_STAGE pending_third_vote_ambiguous_fourth");
    pending_third_vote(&mut cluster, true).await;
    println!("BATCH_LOCK_WIRE_STAGE queued_rpc_deadline_never_reaches_backend");
    queued_rpc_deadline_never_reaches_backend(&mut cluster).await;
    println!("BATCH_LOCK_WIRE_STAGE late_rpc_cleanup");
    late_rpc_cleanup(&mut cluster).await;
    println!("BATCH_LOCK_WIRE_STAGE quorum_deadline_cleans_late_votes");
    quorum_deadline_cleans_late_votes(&mut cluster).await;
    println!("BATCH_LOCK_WIRE_STAGE short_rpc_budget_and_exhausted_deadline");
    short_rpc_budget_and_exhausted_deadline(&mut cluster).await;
    println!("BATCH_LOCK_WIRE_STAGE wire_compatibility_and_unknowns");
    wire_compatibility_and_unknowns(&mut cluster).await;
    let statistics = cluster.states().await;
    assert!(
        statistics.iter().all(|state| state.peak_batches <= 4),
        "batch RPCs cannot grow without bound: {statistics:?}"
    );
    println!(
        "BATCH_LOCK_WIRE_STATISTICS {}",
        serde_json::to_string(&statistics).expect("peer statistics JSON")
    );
    println!("BATCH_LOCK_WIRE_STAGE stop");
    cluster.stop().await;
    println!("BATCH_LOCK_PRODUCTION_WIRE_OK");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn production_batch_lock_rpc_quorum_and_cleanup() {
    match std::env::var(ROLE).as_deref() {
        Ok("receiver") => receiver().await,
        Ok("coordinator") => coordinator().await,
        _ => {
            let output = tokio::time::timeout(
                Duration::from_secs(180),
                Command::new(std::env::current_exe().expect("test executable"))
                    .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
                    .env(ROLE, "coordinator")
                    .env("RUSTFS_INTERNODE_PREWARM", "false")
                    .env("RUSTFS_OBJECT_LOCK_RPC_TIMEOUT_MS", "3000")
                    .env_remove("HTTP_PROXY")
                    .env_remove("HTTPS_PROXY")
                    .env_remove("ALL_PROXY")
                    .env_remove("http_proxy")
                    .env_remove("https_proxy")
                    .env_remove("all_proxy")
                    .env("NO_PROXY", "127.0.0.1,localhost")
                    .env("no_proxy", "127.0.0.1,localhost")
                    .kill_on_drop(true)
                    .output(),
            )
            .await
            .expect("production lock wire suite deadline")
            .expect("isolated coordinator execution");
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(output.status.success(), "production lock wire regression failed: {stdout}{stderr}");
            assert!(
                stdout.contains("BATCH_LOCK_PRODUCTION_WIRE_OK"),
                "missing production execution evidence: {stdout}"
            );
            for line in stdout
                .lines()
                .filter(|line| line.starts_with("BATCH_LOCK_WIRE_STATISTICS ") || *line == "BATCH_LOCK_PRODUCTION_WIRE_OK")
            {
                println!("{line}");
            }
        }
    }
}
