//! Bounded authenticated Raft RPC, mountable without a second storage owner.
use super::{client, http_auth};
use openraft::{
    DefensiveError, ErrorSubject, ErrorVerb, RaftNetwork, RaftNetworkFactory, StorageError, StorageIOError, Violation,
    error::{Fatal, InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable},
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use rustfs_capture_authority::{CaptureMembership, CaptureNode, CaptureTypeConfig, NativeRaftOwner, NodeIdentity};
use rustfs_protos::proto_gen::node_service::{
    CaptureRaftRequest, CaptureRaftResponse,
    capture_raft_service_server::{CaptureRaftService, CaptureRaftServiceServer},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use std::{collections::BTreeMap, io::Write, sync::Arc, time::Duration};
use tonic::{Request, Response, Status};

#[derive(Clone, Copy, Debug)]
pub struct CaptureRaftLimits {
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub max_entries: usize,
    pub max_snapshot_chunk_bytes: usize,
    pub max_inflight: usize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}
impl Default for CaptureRaftLimits {
    fn default() -> Self {
        Self {
            max_request_bytes: 16 * 1024 * 1024,
            max_response_bytes: 1024 * 1024,
            max_entries: 300,
            max_snapshot_chunk_bytes: 3 * 1024 * 1024,
            max_inflight: 16,
            connect_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(5),
        }
    }
}
impl CaptureRaftLimits {
    fn validate(&self) -> Result<(), CaptureRaftError> {
        let chunk = self
            .max_snapshot_chunk_bytes
            .checked_mul(4)
            .and_then(|n| n.checked_add(16384))
            .ok_or(CaptureRaftError::Limits)?;
        if self.max_request_bytes < chunk
            || self.max_request_bytes > 64 * 1024 * 1024
            || self.max_response_bytes < 4096
            || self.max_response_bytes > 4 * 1024 * 1024
            || self.max_entries == 0
            || self.max_entries > 1024
            || self.max_snapshot_chunk_bytes == 0
            || self.max_inflight == 0
            || self.max_inflight > 128
            || self.connect_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.request_timeout > Duration::from_secs(60)
            || self.connect_timeout > Duration::from_secs(60)
        {
            return Err(CaptureRaftError::Limits);
        }
        Ok(())
    }
}
#[derive(Debug, thiserror::Error)]
pub enum CaptureRaftError {
    #[error("capture Raft message limit exceeded")]
    Limits,
    #[error("capture Raft request requires a smaller append batch")]
    RequestTooLarge,
    #[error("invalid capture Raft message")]
    Protocol,
    #[error("capture Raft identity mismatch")]
    Identity,
    #[error("capture Raft authentication failed")]
    Authentication,
    #[error("capture Raft channel unavailable")]
    ChannelUnavailable,
    #[error("capture Raft deadline exceeded")]
    Deadline,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Node(#[from] rustfs_capture_authority::CaptureNodeError),
    #[error(transparent)]
    Status(Box<Status>),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Method {
    AppendEntries,
    Vote,
    InstallSnapshot,
}
impl Method {
    pub(crate) fn path(self) -> &'static str {
        match self {
            Self::AppendEntries => "/node_service.CaptureRaftService/AppendEntries",
            Self::Vote => "/node_service.CaptureRaftService/Vote",
            Self::InstallSnapshot => "/node_service.CaptureRaftService/InstallSnapshot",
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WireRequest {
    pub version: u32,
    pub cluster: [u8; 16],
    pub source: u64,
    pub target: u64,
    pub method: Method,
    pub nonce: [u8; 16],
    pub payload: Box<RawValue>,
}
struct Bounded {
    bytes: Vec<u8>,
    max: usize,
}
impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let end = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("capture RPC limit"))?;
        if end > self.max {
            return Err(std::io::Error::other("capture RPC limit"));
        }
        self.bytes.try_reserve(bytes.len()).map_err(std::io::Error::other)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encoded<T: Serialize>(value: &T, max: usize) -> Result<Vec<u8>, CaptureRaftError> {
    let mut out = Bounded { bytes: Vec::new(), max };
    serde_json::to_writer(&mut out, value).map_err(|error| {
        if error.is_io() {
            CaptureRaftError::Limits
        } else {
            CaptureRaftError::Json(error)
        }
    })?;
    Ok(out.bytes)
}
fn raw<T: Serialize>(value: &T, max: usize) -> Result<Box<RawValue>, CaptureRaftError> {
    Ok(RawValue::from_string(
        String::from_utf8(encoded(value, max)?).map_err(|_| CaptureRaftError::Protocol)?,
    )?)
}
pub(crate) fn encode_request<P: Serialize>(
    local: NodeIdentity,
    target: u64,
    method: Method,
    payload: &P,
    limits: CaptureRaftLimits,
) -> Result<CaptureRaftRequest, CaptureRaftError> {
    let wire = WireRequest {
        version: 1,
        cluster: local.cluster_id,
        source: local.node_id,
        target,
        method,
        nonce: *uuid::Uuid::new_v4().as_bytes(),
        payload: raw(payload, limits.max_request_bytes)?,
    };
    Ok(CaptureRaftRequest {
        body: encoded(&wire, limits.max_request_bytes)?.into(),
    })
}
pub(crate) fn canonical_request(body: &[u8]) -> Vec<u8> {
    let mut out = b"rustfs.capture-raft.request.v1\0".to_vec();
    out.extend_from_slice(body);
    out
}
fn decode_wire(body: &[u8], max: usize) -> Result<WireRequest, CaptureRaftError> {
    if body.len() > max {
        return Err(CaptureRaftError::Limits);
    }
    let wire: WireRequest = serde_json::from_slice(body)?;
    if wire.version != 1 || wire.cluster == [0; 16] || wire.nonce == [0; 16] || encoded(&wire, max)? != body {
        return Err(CaptureRaftError::Protocol);
    }
    Ok(wire)
}
fn decode_payload<P: Serialize + DeserializeOwned>(wire: &WireRequest, max: usize) -> Result<P, CaptureRaftError> {
    let value: P = serde_json::from_str(wire.payload.get())?;
    if encoded(&value, max)?.as_slice() != wire.payload.get().as_bytes() {
        return Err(CaptureRaftError::Protocol);
    }
    Ok(value)
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub(crate) enum RemoteFailure {
    Stopped,
    Panicked,
    StorageIo {
        operation: Option<IoOperation>,
    },
    StorageDefensive {
        subject: ErrorSubject<u64>,
        violation: Violation<u64>,
    },
    SnapshotMismatch {
        error: InstallSnapshotError,
    },
}
#[derive(Serialize, Deserialize)]
pub(crate) struct IoOperation {
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
}
fn fatal(error: Fatal<u64>) -> RemoteFailure {
    match error {
        Fatal::Stopped => RemoteFailure::Stopped,
        Fatal::Panicked => RemoteFailure::Panicked,
        Fatal::StorageError(StorageError::IO { source }) => RemoteFailure::StorageIo {
            operation: encoded(&source, 16384)
                .ok()
                .and_then(|bytes| serde_json::from_slice(&bytes).ok()),
        },
        Fatal::StorageError(StorageError::Defensive { source }) => RemoteFailure::StorageDefensive {
            subject: source.subject,
            violation: source.violation,
        },
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum WireReply {
    Success { payload: Box<RawValue> },
    Failure { error: RemoteFailure },
}
fn response_context(request: &CaptureRaftRequest, body: &[u8]) -> Result<Vec<u8>, CaptureRaftError> {
    let mut out = b"rustfs.capture-raft.response.v1\0".to_vec();
    out.try_reserve(
        request
            .body
            .len()
            .checked_add(body.len())
            .and_then(|n| n.checked_add(16))
            .ok_or(CaptureRaftError::Limits)?,
    )
    .map_err(std::io::Error::other)?;
    out.extend(
        u64::try_from(request.body.len())
            .map_err(|_| CaptureRaftError::Limits)?
            .to_be_bytes(),
    );
    out.extend_from_slice(&request.body);
    out.extend(u64::try_from(body.len()).map_err(|_| CaptureRaftError::Limits)?.to_be_bytes());
    out.extend_from_slice(body);
    Ok(out)
}
fn reply<P: Serialize>(
    request: &CaptureRaftRequest,
    result: Result<P, RemoteFailure>,
    limits: CaptureRaftLimits,
) -> Result<CaptureRaftResponse, CaptureRaftError> {
    let wire = match result {
        Ok(value) => WireReply::Success {
            payload: raw(&value, limits.max_response_bytes)?,
        },
        Err(error) => WireReply::Failure { error },
    };
    let body = encoded(&wire, limits.max_response_bytes)?;
    let response_proof = http_auth::sign_tonic_rpc_response_proof(&response_context(request, &body)?)?;
    Ok(CaptureRaftResponse {
        body: body.into(),
        response_proof: response_proof.into(),
    })
}
pub(crate) fn verify_response<P: Serialize + DeserializeOwned>(
    request: &CaptureRaftRequest,
    response: CaptureRaftResponse,
    limits: CaptureRaftLimits,
) -> Result<Result<P, RemoteFailure>, CaptureRaftError> {
    if request.body.len() > limits.max_request_bytes
        || response.body.len() > limits.max_response_bytes
        || response.response_proof.len() != 32
    {
        return Err(CaptureRaftError::Limits);
    }
    http_auth::verify_tonic_rpc_response_proof(&response_context(request, &response.body)?, &response.response_proof)?;
    let wire: WireReply = serde_json::from_slice(&response.body)?;
    if encoded(&wire, limits.max_response_bytes)?.as_slice() != response.body {
        return Err(CaptureRaftError::Protocol);
    }
    match wire {
        WireReply::Failure { error } => Ok(Err(error)),
        WireReply::Success { payload } => {
            let value: P = serde_json::from_str(payload.get())?;
            if encoded(&value, limits.max_response_bytes)?.as_slice() != payload.get().as_bytes() {
                return Err(CaptureRaftError::Protocol);
            }
            Ok(Ok(value))
        }
    }
}
fn restored_fatal(error: RemoteFailure) -> Result<Fatal<u64>, CaptureRaftError> {
    Ok(match error {
        RemoteFailure::Stopped => Fatal::Stopped,
        RemoteFailure::Panicked => Fatal::Panicked,
        RemoteFailure::StorageIo { operation } => {
            let (subject, verb) = operation
                .map(|op| (op.subject, op.verb))
                .unwrap_or((ErrorSubject::None, ErrorVerb::Read));
            Fatal::StorageError(
                StorageIOError::new(
                    subject,
                    verb,
                    openraft::AnyError::new(&std::io::Error::other("remote Raft storage I/O failed; detailed source redacted")),
                )
                .into(),
            )
        }
        RemoteFailure::StorageDefensive { subject, violation } => Fatal::StorageError(StorageError::Defensive {
            source: DefensiveError {
                subject,
                violation,
                backtrace: None,
            },
        }),
        RemoteFailure::SnapshotMismatch { .. } => return Err(CaptureRaftError::Protocol),
    })
}
fn peers_valid(local: NodeIdentity, peers: &BTreeMap<u64, CaptureNode>) -> Result<(), CaptureRaftError> {
    if local.cluster_id == [0; 16] || !peers.contains_key(&local.node_id) {
        return Err(CaptureRaftError::Identity);
    }
    let mut addresses = std::collections::BTreeSet::new();
    for node in peers.values() {
        node.validate()?;
        if !addresses.insert(node.rpc_uri()) {
            return Err(CaptureRaftError::Identity);
        }
    }
    Ok(())
}

#[cfg(test)]
type WorkerPause = Arc<std::sync::Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>>;
#[derive(Clone)]
pub struct CaptureRaftRpc {
    owner: Arc<NativeRaftOwner>,
    peers: Arc<BTreeMap<u64, CaptureNode>>,
    audience: String,
    limits: CaptureRaftLimits,
    admission: Arc<tokio::sync::Semaphore>,
    accepting: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    pub(crate) worker_pause: WorkerPause,
}
impl CaptureRaftRpc {
    pub fn new(
        owner: Arc<NativeRaftOwner>,
        peers: BTreeMap<u64, CaptureNode>,
        limits: CaptureRaftLimits,
    ) -> Result<Self, CaptureRaftError> {
        limits.validate()?;
        let identity = owner.identity();
        peers_valid(identity, &peers)?;
        let node = peers.get(&identity.node_id).ok_or(CaptureRaftError::Identity)?;
        let uri: http::Uri = node.rpc_uri().parse().map_err(|_| CaptureRaftError::Identity)?;
        let audience = http_auth::normalize_tonic_rpc_audience(uri.authority().ok_or(CaptureRaftError::Identity)?.as_str())?;
        Ok(Self {
            owner,
            peers: Arc::new(peers),
            audience,
            limits,
            #[cfg(test)]
            worker_pause: Arc::new(std::sync::Mutex::new(None)),
            accepting: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            admission: Arc::new(tokio::sync::Semaphore::new(limits.max_inflight)),
        })
    }
    pub fn into_service(self) -> CaptureRaftServiceServer<Self> {
        let request = self.limits.max_request_bytes + 64;
        let response = self.limits.max_response_bytes + 128;
        CaptureRaftServiceServer::new(self)
            .max_decoding_message_size(request)
            .max_encoding_message_size(response)
    }
    fn prepare<P: Serialize + DeserializeOwned>(
        &self,
        request: &Request<CaptureRaftRequest>,
        method: Method,
    ) -> Result<(WireRequest, P, tokio::sync::OwnedSemaphorePermit), Status> {
        if !self.accepting.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Status::unavailable("capture Raft service stopping"));
        }
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("capture Raft admission full"))?;
        if !self.accepting.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Status::unavailable("capture Raft service stopping"));
        }
        if request.get_ref().body.len() > self.limits.max_request_bytes {
            return Err(Status::resource_exhausted("capture Raft request limit"));
        }
        if request
            .metadata()
            .get(http_auth::RPC_REPLAY_SCOPE_VERSION_HEADER)
            .and_then(|v| v.to_str().ok())
            != Some("3")
        {
            return Err(auth_rejected("missing_replay_scope"));
        }
        http_auth::verify_tonic_rpc_signature_with_bootstrap(&self.audience, method.path(), request.metadata().as_ref(), false)
            .map_err(|_| auth_rejected("invalid_signature"))?;
        http_auth::verify_tonic_canonical_body_digest(request, &canonical_request(&request.get_ref().body))
            .map_err(|_| auth_rejected("body_digest_mismatch"))?;
        let wire = decode_wire(&request.get_ref().body, self.limits.max_request_bytes)
            .map_err(|_| Status::invalid_argument("capture Raft wire rejected"))?;
        let local = self.owner.identity();
        if wire.cluster != local.cluster_id
            || wire.target != local.node_id
            || wire.method != method
            || !self.peers.contains_key(&wire.source)
        {
            return Err(Status::permission_denied("capture Raft identity rejected"));
        }
        let payload = decode_payload(&wire, self.limits.max_request_bytes)
            .map_err(|_| Status::invalid_argument("capture Raft payload rejected"))?;
        Ok((wire, payload, permit))
    }
    #[cfg(test)]
    async fn paused(pause: WorkerPause) {
        let gate = pause.lock().expect("worker pause lock").take();
        if let Some((entered, resume)) = gate {
            entered.notify_one();
            resume.notified().await;
        }
    }
    /// Stop admission and wait for all actual RPC tasks, including cancelled HTTP waiters.
    pub async fn shutdown(&self) -> Result<(), CaptureRaftError> {
        self.accepting.store(false, std::sync::atomic::Ordering::Release);
        let count = u32::try_from(self.limits.max_inflight).map_err(|_| CaptureRaftError::Limits)?;
        let _all = self
            .admission
            .clone()
            .acquire_many_owned(count)
            .await
            .map_err(|_| CaptureRaftError::ChannelUnavailable)?;
        Ok(())
    }
}
fn signed_response<P: Serialize>(
    raw: &CaptureRaftRequest,
    result: Result<P, RemoteFailure>,
    limits: CaptureRaftLimits,
    audience: &str,
    challenge: Option<uuid::Uuid>,
) -> Result<Response<CaptureRaftResponse>, Status> {
    let mut response =
        Response::new(reply(raw, result, limits).map_err(|_| Status::internal("capture Raft response unavailable"))?);
    if let Some(challenge) = challenge {
        response.metadata_mut().as_mut().extend(
            http_auth::tonic_boot_epoch_response_headers(audience, challenge)
                .map_err(|_| Status::internal("capture Raft epoch proof unavailable"))?,
        );
    }
    Ok(response)
}
#[tonic::async_trait]
impl CaptureRaftService for CaptureRaftRpc {
    async fn append_entries(&self, request: Request<CaptureRaftRequest>) -> Result<Response<CaptureRaftResponse>, Status> {
        let (wire, payload, permit) = self.prepare::<AppendEntriesRequest<CaptureTypeConfig>>(&request, Method::AppendEntries)?;
        validate_append(&payload, wire.source, self.limits)
            .map_err(|_| Status::invalid_argument("capture Raft append rejected"))?;
        let challenge = http_auth::tonic_boot_epoch_challenge(request.metadata().as_ref())
            .map_err(|_| Status::unauthenticated("capture Raft challenge rejected"))?;
        let raw = request.into_inner();
        let owner = self.owner.clone();
        let audience = self.audience.clone();
        let limits = self.limits;
        #[cfg(test)]
        let pause = self.worker_pause.clone();
        tokio::spawn(async move {
            let _permit = permit;
            #[cfg(test)]
            Self::paused(pause).await;
            let result = owner.raft().append_entries(payload).await.map_err(|error| match error {
                RaftError::Fatal(error) => fatal(error),
                RaftError::APIError(error) => match error {},
            });
            drop(owner);
            signed_response(&raw, result, limits, &audience, challenge)
        })
        .await
        .map_err(|_| Status::internal("capture Raft worker failed"))?
    }
    async fn vote(&self, request: Request<CaptureRaftRequest>) -> Result<Response<CaptureRaftResponse>, Status> {
        let (wire, payload, permit) = self.prepare::<VoteRequest<u64>>(&request, Method::Vote)?;
        if payload.vote.leader_id.node_id != wire.source {
            return Err(Status::invalid_argument("capture Raft candidate rejected"));
        }
        let challenge = http_auth::tonic_boot_epoch_challenge(request.metadata().as_ref())
            .map_err(|_| Status::unauthenticated("capture Raft challenge rejected"))?;
        let raw = request.into_inner();
        let owner = self.owner.clone();
        let audience = self.audience.clone();
        let limits = self.limits;
        #[cfg(test)]
        let pause = self.worker_pause.clone();
        tokio::spawn(async move {
            let _permit = permit;
            #[cfg(test)]
            Self::paused(pause).await;
            let result = owner.raft().vote(payload).await.map_err(|error| match error {
                RaftError::Fatal(error) => fatal(error),
                RaftError::APIError(error) => match error {},
            });
            drop(owner);
            signed_response(&raw, result, limits, &audience, challenge)
        })
        .await
        .map_err(|_| Status::internal("capture Raft worker failed"))?
    }
    async fn install_snapshot(&self, request: Request<CaptureRaftRequest>) -> Result<Response<CaptureRaftResponse>, Status> {
        let (wire, payload, permit) =
            self.prepare::<InstallSnapshotRequest<CaptureTypeConfig>>(&request, Method::InstallSnapshot)?;
        validate_snapshot(&payload, wire.source, self.limits)
            .map_err(|_| Status::invalid_argument("capture Raft snapshot rejected"))?;
        let challenge = http_auth::tonic_boot_epoch_challenge(request.metadata().as_ref())
            .map_err(|_| Status::unauthenticated("capture Raft challenge rejected"))?;
        let raw = request.into_inner();
        let owner = self.owner.clone();
        let audience = self.audience.clone();
        let limits = self.limits;
        #[cfg(test)]
        let pause = self.worker_pause.clone();
        tokio::spawn(async move {
            let _permit = permit;
            #[cfg(test)]
            Self::paused(pause).await;
            let result = owner.raft().install_snapshot(payload).await.map_err(|error| match error {
                RaftError::Fatal(error) => fatal(error),
                RaftError::APIError(error) => RemoteFailure::SnapshotMismatch { error },
            });
            drop(owner);
            signed_response(&raw, result, limits, &audience, challenge)
        })
        .await
        .map_err(|_| Status::internal("capture Raft worker failed"))?
    }
}
fn validate_append(
    request: &AppendEntriesRequest<CaptureTypeConfig>,
    source: u64,
    limits: CaptureRaftLimits,
) -> Result<(), CaptureRaftError> {
    if request.vote.leader_id.node_id != source {
        return Err(CaptureRaftError::Identity);
    }
    if request.entries.len() > limits.max_entries {
        return Err(CaptureRaftError::RequestTooLarge);
    }
    for entry in &request.entries {
        if let openraft::EntryPayload::Membership(value) = &entry.payload {
            CaptureMembership::try_from(&openraft::StoredMembership::new(Some(entry.log_id), value.clone()))
                .map_err(|_| CaptureRaftError::Protocol)?;
        }
    }
    Ok(())
}
fn validate_snapshot(
    request: &InstallSnapshotRequest<CaptureTypeConfig>,
    source: u64,
    limits: CaptureRaftLimits,
) -> Result<(), CaptureRaftError> {
    if request.vote.leader_id.node_id != source
        || request.data.len() > limits.max_snapshot_chunk_bytes
        || request
            .offset
            .checked_add(u64::try_from(request.data.len()).map_err(|_| CaptureRaftError::Limits)?)
            .is_none()
        || request.meta.snapshot_id.is_empty()
        || request.meta.snapshot_id.len() > 1024
    {
        return Err(CaptureRaftError::Limits);
    }
    CaptureMembership::try_from(&request.meta.last_membership).map_err(|_| CaptureRaftError::Protocol)?;
    Ok(())
}

#[derive(Clone)]
pub struct CaptureRaftNetworkFactory {
    local: NodeIdentity,
    peers: Arc<BTreeMap<u64, CaptureNode>>,
    limits: CaptureRaftLimits,
    admission: Arc<tokio::sync::Semaphore>,
}
impl CaptureRaftNetworkFactory {
    pub fn new(
        local: NodeIdentity,
        peers: BTreeMap<u64, CaptureNode>,
        config: Arc<openraft::Config>,
        limits: CaptureRaftLimits,
    ) -> Result<Self, CaptureRaftError> {
        limits.validate()?;
        peers_valid(local, &peers)?;
        if usize::try_from(config.max_payload_entries).map_err(|_| CaptureRaftError::Limits)? > limits.max_entries {
            return Err(CaptureRaftError::Limits);
        }
        if usize::try_from(config.snapshot_max_chunk_size).map_err(|_| CaptureRaftError::Limits)?
            > limits.max_snapshot_chunk_bytes
        {
            return Err(CaptureRaftError::Limits);
        }
        Ok(Self {
            local,
            peers: Arc::new(peers),
            limits,
            admission: Arc::new(tokio::sync::Semaphore::new(limits.max_inflight)),
        })
    }
}
pub struct CaptureRaftPeer {
    factory: CaptureRaftNetworkFactory,
    target: u64,
    node: CaptureNode,
    valid: bool,
}
impl RaftNetworkFactory<CaptureTypeConfig> for CaptureRaftNetworkFactory {
    type Network = CaptureRaftPeer;
    async fn new_client(&mut self, target: u64, node: &CaptureNode) -> Self::Network {
        CaptureRaftPeer {
            factory: self.clone(),
            target,
            node: node.clone(),
            valid: self.peers.get(&target) == Some(node),
        }
    }
}
impl CaptureRaftPeer {
    async fn exchange<P: Serialize, R: Serialize + DeserializeOwned>(
        &self,
        method: Method,
        payload: &P,
        option: &RPCOption,
    ) -> Result<Result<R, RemoteFailure>, CaptureRaftError> {
        if !self.valid {
            return Err(CaptureRaftError::Identity);
        }
        let ttl = self.factory.limits.request_timeout.min(option.soft_ttl());
        tokio::time::timeout(ttl, async {
            let _permit = self
                .factory
                .admission
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| CaptureRaftError::ChannelUnavailable)?;
            let raw =
                encode_request(self.factory.local, self.target, method, payload, self.factory.limits).map_err(
                    |error| match error {
                        CaptureRaftError::Limits => CaptureRaftError::RequestTooLarge,
                        error => error,
                    },
                )?;
            let mut request = Request::new(raw.clone());
            http_auth::set_tonic_canonical_body_digest(&mut request, &canonical_request(&raw.body))?;
            request.set_timeout(ttl);
            let mut client = client::capture_raft_time_out_client(
                self.node.rpc_uri(),
                self.factory.limits.connect_timeout.min(ttl),
                self.factory.limits.max_request_bytes + 64,
                self.factory.limits.max_response_bytes + 128,
            )
            .await?;
            let response = match method {
                Method::AppendEntries => client.append_entries(request).await,
                Method::Vote => client.vote(request).await,
                Method::InstallSnapshot => client.install_snapshot(request).await,
            }
            .map_err(|status| CaptureRaftError::Status(Box::new(status)))?;
            verify_response(&raw, response.into_inner(), self.factory.limits)
        })
        .await
        .map_err(|_| CaptureRaftError::Deadline)?
    }
    fn network_error<E: std::error::Error>(
        &self,
        error: CaptureRaftError,
        method: Method,
        option: &RPCOption,
    ) -> RPCError<u64, CaptureNode, E> {
        match error {
            CaptureRaftError::Deadline => RPCError::Timeout(openraft::error::Timeout {
                action: match method {
                    Method::AppendEntries => openraft::RPCTypes::AppendEntries,
                    Method::Vote => openraft::RPCTypes::Vote,
                    Method::InstallSnapshot => openraft::RPCTypes::InstallSnapshot,
                },
                id: self.factory.local.node_id,
                target: self.target,
                timeout: self.factory.limits.request_timeout.min(option.soft_ttl()),
            }),
            error @ CaptureRaftError::ChannelUnavailable => RPCError::Unreachable(Unreachable::new(&error)),
            error => RPCError::Network(openraft::error::NetworkError::new(&error)),
        }
    }
    fn append_error<E: std::error::Error>(
        &self,
        error: CaptureRaftError,
        count: usize,
        option: &RPCOption,
    ) -> RPCError<u64, CaptureNode, E> {
        if matches!(error, CaptureRaftError::RequestTooLarge) && count > 1 {
            let hint = u64::try_from((count / 2).max(1).min(self.factory.limits.max_entries)).unwrap_or(1);
            return RPCError::PayloadTooLarge(openraft::error::PayloadTooLarge::new_entries_hint(hint));
        }
        self.network_error(error, Method::AppendEntries, option)
    }
    fn remote<E: std::error::Error>(&self, error: E) -> RPCError<u64, CaptureNode, E> {
        RPCError::RemoteError(RemoteError::new_with_node(self.target, self.node.clone(), error))
    }
}
impl RaftNetwork<CaptureTypeConfig> for CaptureRaftPeer {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<CaptureTypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, CaptureNode, RaftError<u64>>> {
        let count = request.entries.len();
        validate_append(&request, self.factory.local.node_id, self.factory.limits)
            .map_err(|error| self.append_error(error, count, &option))?;
        match self
            .exchange(Method::AppendEntries, &request, &option)
            .await
            .map_err(|error| self.append_error(error, count, &option))?
        {
            Ok(value) => Ok(value),
            Err(error) => Err(self.remote(RaftError::Fatal(
                restored_fatal(error).map_err(|error| self.network_error(error, Method::AppendEntries, &option))?,
            ))),
        }
    }
    async fn vote(
        &mut self,
        request: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, CaptureNode, RaftError<u64>>> {
        if request.vote.leader_id.node_id != self.factory.local.node_id {
            return Err(self.network_error(CaptureRaftError::Identity, Method::Vote, &option));
        }
        match self
            .exchange(Method::Vote, &request, &option)
            .await
            .map_err(|error| self.network_error(error, Method::Vote, &option))?
        {
            Ok(value) => Ok(value),
            Err(error) => Err(self.remote(RaftError::Fatal(
                restored_fatal(error).map_err(|error| self.network_error(error, Method::Vote, &option))?,
            ))),
        }
    }
    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest<CaptureTypeConfig>,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse<u64>, RPCError<u64, CaptureNode, RaftError<u64, InstallSnapshotError>>> {
        validate_snapshot(&request, self.factory.local.node_id, self.factory.limits)
            .map_err(|error| self.network_error(error, Method::InstallSnapshot, &option))?;
        match self
            .exchange(Method::InstallSnapshot, &request, &option)
            .await
            .map_err(|error| self.network_error(error, Method::InstallSnapshot, &option))?
        {
            Ok(value) => Ok(value),
            Err(error) => {
                let source = match error {
                    RemoteFailure::SnapshotMismatch { error } => RaftError::APIError(error),
                    error => RaftError::Fatal(
                        restored_fatal(error).map_err(|error| self.network_error(error, Method::InstallSnapshot, &option))?,
                    ),
                };
                Err(self.remote(source))
            }
        }
    }
}

fn auth_rejected(reason: &'static str) -> Status {
    use rustfs_io_metrics::internode_metrics::{
        INTERNODE_OPERATION_GRPC_OTHER, INTERNODE_TRANSPORT_BACKEND_GRPC, global_internode_metrics,
    };
    global_internode_metrics().record_rpc_auth_failure_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_OTHER,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
        reason,
    );
    Status::unauthenticated("capture Raft authentication rejected")
}
