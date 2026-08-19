// Copyright 2024 RustFS Team
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

#[cfg(test)]
use crate::cluster::rpc::http_auth::RPC_REPLAY_SCOPE_VERSION_HEADER;
use crate::cluster::rpc::http_auth::{
    AuthenticatedPeerReplayCapabilities, RPC_AUTH_VERSION_HEADER, RPC_AUTH_VERSION_V2, RPC_BOOT_EPOCH_CHALLENGE_HEADER,
    RPC_CONTENT_SHA256_HEADER, RPC_REPLAY_CACHE_CAPABILITY_HEADER, RPC_REPLAY_CACHE_CAPABILITY_PROOF_HEADER,
    RollingMutationBodyDigest, TIMESTAMP_HEADER, internode_rpc_body_digest_strict,
    verify_tonic_peer_replay_capabilities_response,
};
use crate::cluster::rpc::{gen_tonic_replay_scope_headers, gen_tonic_signature_headers, normalize_tonic_rpc_audience};
#[cfg(test)]
use crate::cluster::rpc::{tonic_boot_epoch_challenge, tonic_boot_epoch_response_headers};
use crate::disk::error::{DiskError, Error as DiskErrorType, RpcStatusError};
use crate::runtime::sources as runtime_sources;
use http::{Request as HttpRequest, Response as HttpResponse, Uri};
use rustfs_protos::{
    ChannelClass, create_new_channel, get_channel_for_class,
    proto_gen::node_service::{
        heal_control_service_client::HealControlServiceClient, node_service_client::NodeServiceClient,
        tier_mutation_control_service_client::TierMutationControlServiceClient,
    },
};
use std::{
    collections::HashMap,
    error::Error,
    future::Future,
    io::ErrorKind,
    pin::Pin,
    sync::{LazyLock, Mutex},
    task::{Context, Poll},
};
use tonic::{service::interceptor::InterceptedService, transport::Channel};
use tower::Service;
use tracing::debug;
use uuid::Uuid;

use super::context_propagation::{inject_request_id_into_metadata, inject_trace_context_into_metadata};

/// 3. Subsequent calls will attempt fresh connections
/// 4. If node is still down, connection will fail fast (3s timeout)
pub async fn node_service_time_out_client(
    addr: &String,
    interceptor: TonicInterceptor,
) -> Result<NodeServiceClient<InterceptedService<AuthenticatedChannel, TonicInterceptor>>, Box<dyn Error>> {
    // Default to the latency-sensitive control channel; bulk `bytes` RPCs opt in via the
    // `_for_class` variant below (grpc-optimization P1).
    node_service_time_out_client_for_class(addr, interceptor, ChannelClass::Control).await
}

pub async fn heal_control_time_out_client(
    addr: &str,
    interceptor: TonicInterceptor,
) -> Result<HealControlServiceClient<InterceptedService<AuthenticatedChannel, TonicInterceptor>>, Box<dyn Error>> {
    let interceptor = interceptor.with_rpc_audience(addr)?;
    let channel = match runtime_sources::cached_node_channel(addr).await {
        Some(channel) => channel,
        None => create_new_channel(addr).await?,
    };
    let max_message_size = rustfs_protos::HEAL_CONTROL_RPC_MAX_MESSAGE_SIZE;
    let channel = ReplayScopeChannel::new(channel, interceptor.replay_scope_audience());
    Ok(HealControlServiceClient::with_interceptor(channel, interceptor)
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size))
}

pub async fn tier_mutation_control_time_out_client(
    addr: &str,
    interceptor: TonicInterceptor,
) -> Result<TierMutationControlServiceClient<InterceptedService<AuthenticatedChannel, TonicInterceptor>>, Box<dyn Error>> {
    let interceptor = interceptor.with_rpc_audience(addr)?;
    let channel = match runtime_sources::cached_node_channel(addr).await {
        Some(channel) => channel,
        None => create_new_channel(addr).await?,
    };
    let max_message_size = rustfs_protos::TIER_MUTATION_RPC_MAX_MESSAGE_SIZE;
    let channel = ReplayScopeChannel::new(channel, interceptor.replay_scope_audience());
    Ok(TierMutationControlServiceClient::with_interceptor(channel, interceptor)
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size))
}

/// Build a `NodeServiceClient` bound to the [`ChannelClass`]-appropriate channel for `addr`.
///
/// Bulk `bytes`-carrying RPCs (ReadAll/WriteAll/ReadMultiple/BatchReadVersion) pass
/// [`ChannelClass::Bulk`] so, when channel isolation is enabled, they are physically isolated
/// from lock/health RPCs; everything else uses [`ChannelClass::Control`]. When isolation is
/// disabled the two classes resolve to the same cached channel, i.e. legacy behavior.
pub async fn node_service_time_out_client_for_class(
    addr: &String,
    interceptor: TonicInterceptor,
    class: ChannelClass,
) -> Result<NodeServiceClient<InterceptedService<AuthenticatedChannel, TonicInterceptor>>, Box<dyn Error>> {
    let interceptor = interceptor.with_rpc_audience(addr)?;
    let channel = match class {
        ChannelClass::Control => match runtime_sources::cached_node_channel(addr).await {
            Some(channel) => {
                debug!("Using cached gRPC channel for: {}", addr);
                channel
            }
            // No cached connection, create new one.
            None => create_new_channel(addr).await?,
        },
        ChannelClass::Bulk => get_channel_for_class(addr, ChannelClass::Bulk).await?,
    };

    let max_message_size = rustfs_protos::internode_rpc_max_message_size();
    let channel = ReplayScopeChannel::new(channel, interceptor.replay_scope_audience());
    Ok(NodeServiceClient::with_interceptor(channel, interceptor)
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size))
}

pub async fn node_service_time_out_client_no_auth(
    addr: &String,
) -> Result<NodeServiceClient<InterceptedService<AuthenticatedChannel, TonicInterceptor>>, Box<dyn Error>> {
    node_service_time_out_client(addr, TonicInterceptor::NoOp(NoOpInterceptor)).await
}

/// The typed `tonic::Status` an internode RPC failure was converted from, if
/// this error carries one.
pub(crate) fn embedded_tonic_status(io_err: &std::io::Error) -> Option<&tonic::Status> {
    io_err.get_ref()?.downcast_ref::<RpcStatusError>().map(RpcStatusError::status)
}

/// Decide whether a gRPC status reports a peer we cannot currently reach,
/// rather than an application outcome from a live peer.
///
/// `Unavailable` is the one code that means "no service behind this channel":
/// the client transport raises it when the connection is broken, and the
/// server's own not-ready gates use it deliberately.
///
/// `Unknown` is the client transport's escape hatch for a cause it could not
/// map to a code — tower's "Service was not ready: <cause>", an h2 error with
/// no gRPC mapping. Our handlers never return it, so there its message is the
/// only evidence available and the anchored needles decide.
///
/// Every other code is an answer from a live peer and is never a transport
/// failure, whatever its message says. That distinction is the point of
/// classifying by code: a peer relaying its own downstream trouble as
/// `Internal("connection refused ...")`, or a handler interpolating a local
/// `io::Error` into `Status::internal`, answered us perfectly well. Marking it
/// offline over that text is the bug this classification replaces. Likewise a
/// `Cancelled` "Timeout expired" from the per-RPC channel deadline means the
/// peer is slow, not gone; gating it would turn load into a partition.
pub(crate) fn is_network_like_status(status: &tonic::Status) -> bool {
    match status.code() {
        tonic::Code::Unavailable => true,
        tonic::Code::Unknown => message_has_network_needle(&status.to_string()),
        _ => false,
    }
}

/// Substring fallback for failures that only exist as text: dial errors
/// wrapped by `get_client`, remote `error_info` payloads, and statuses
/// flattened through `format!`. Needles must stay anchored to transport
/// context — a bare word like "unavailable" also matches application text
/// (e.g. a bucket named "unavailable-logs") and would take a healthy peer
/// offline.
pub(crate) fn message_has_network_needle(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "temporarily offline",
        "transport error",
        // tonic >= 0.14 renders Code::Unavailable as
        // `code: 'The service is currently unavailable'`.
        "code: 'the service is currently unavailable'",
        // RUSTFS_COMPAT_TODO(tonic-013-status-render): releases up to 1.0.0-alpha.38 shipped tonic 0.13, which rendered the same status as `status: Unavailable`, and peers relay that text in error_info. Remove after the minimum supported RustFS peer version ships tonic >= 0.14.
        "status: unavailable",
        "error trying to connect",
        "connection refused",
        "connection reset",
        "broken pipe",
        "not connected",
        "unexpected eof",
        "timed out",
        "deadline has elapsed",
        "connection closed",
        "connection aborted",
        "tcp connect error",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

pub(crate) fn is_network_like_disk_error(err: &DiskErrorType) -> bool {
    match err {
        DiskError::Timeout => true,
        DiskError::Io(io_err) => {
            if let Some(status) = embedded_tonic_status(io_err) {
                return is_network_like_status(status);
            }
            if matches!(
                io_err.kind(),
                ErrorKind::TimedOut
                    | ErrorKind::ConnectionRefused
                    | ErrorKind::ConnectionReset
                    | ErrorKind::BrokenPipe
                    | ErrorKind::NotConnected
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::UnexpectedEof
            ) {
                return true;
            }

            message_has_network_needle(&io_err.to_string())
        }
        _ => false,
    }
}

/// The transport service that learns an authenticated peer boot epoch and adds the replay-scoped
/// signature only after one has been observed. The v1/v2 interceptor stays inside this wrapper so
/// old servers continue receiving precisely the metadata they understand.
#[derive(Clone, Debug)]
pub struct ReplayScopeChannel<S> {
    inner: S,
    audience: Option<String>,
}

/// The channel type used by internode clients after v2 authentication and replay-scope handling.
pub type AuthenticatedChannel = ReplayScopeChannel<Channel>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PeerReplayCapability {
    Capable { boot_epoch: Uuid },
    Revoked,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct PeerReplayState {
    boot_epoch: Option<Uuid>,
    cache_capability: Option<PeerReplayCapability>,
}

#[derive(Clone, Copy, Debug)]
struct PeerReplayStateSnapshot(PeerReplayState);

static PEER_REPLAY_STATES: LazyLock<Mutex<HashMap<String, PeerReplayState>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

impl<S> ReplayScopeChannel<S> {
    fn new(inner: S, audience: Option<String>) -> Self {
        Self { inner, audience }
    }
}

#[allow(dead_code, reason = "replay-state probe asserted by this file's tests (backlog#1823)")]
fn peer_replay_state(audience: &str) -> PeerReplayState {
    PEER_REPLAY_STATES
        .lock()
        .ok()
        .and_then(|states| states.get(audience).copied())
        .unwrap_or_default()
}

fn apply_peer_replay_response(
    audience: String,
    sent_state: PeerReplayState,
    response: std::io::Result<AuthenticatedPeerReplayCapabilities>,
) {
    if let Ok(mut states) = PEER_REPLAY_STATES.lock() {
        let current_state = states.get(&audience).copied().unwrap_or_default();
        let mut next_state = current_state;
        if let Ok(response) = &response
            && sent_state.boot_epoch == current_state.boot_epoch
        {
            next_state.boot_epoch = Some(response.boot_epoch);
        }

        if sent_state.boot_epoch == current_state.boot_epoch {
            let response_capability = response
                .as_ref()
                .ok()
                .filter(|response| response.dynamic_replay_cache)
                .map(|response| response.boot_epoch);
            match (sent_state.cache_capability, current_state.cache_capability, response_capability) {
                (None, None, Some(boot_epoch))
                | (Some(PeerReplayCapability::Revoked), Some(PeerReplayCapability::Revoked), Some(boot_epoch)) => {
                    next_state.cache_capability = Some(PeerReplayCapability::Capable { boot_epoch });
                }
                (
                    Some(PeerReplayCapability::Capable {
                        boot_epoch: sent_boot_epoch,
                    }),
                    Some(PeerReplayCapability::Capable {
                        boot_epoch: current_boot_epoch,
                    }),
                    Some(response_boot_epoch),
                ) if sent_boot_epoch == current_boot_epoch => {
                    next_state.cache_capability = Some(PeerReplayCapability::Capable {
                        boot_epoch: response_boot_epoch,
                    });
                }
                (
                    Some(PeerReplayCapability::Capable {
                        boot_epoch: sent_boot_epoch,
                    }),
                    Some(PeerReplayCapability::Capable {
                        boot_epoch: current_boot_epoch,
                    }),
                    None,
                ) if sent_boot_epoch == current_boot_epoch => {
                    next_state.cache_capability = Some(PeerReplayCapability::Revoked);
                }
                _ => {}
            }
        }
        states.insert(audience, next_state);
    }
}

impl<S, ReqBody, ResBody> Service<HttpRequest<ReqBody>> for ReplayScopeChannel<S>
where
    S: Service<HttpRequest<ReqBody>, Response = HttpResponse<ResBody>>,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = HttpResponse<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: HttpRequest<ReqBody>) -> Self::Future {
        let authenticated = self.audience.as_ref().is_some_and(|_| {
            request
                .headers()
                .get(RPC_AUTH_VERSION_HEADER)
                .and_then(|value| value.to_str().ok())
                == Some(RPC_AUTH_VERSION_V2)
        });
        let challenge = authenticated.then(Uuid::new_v4);
        let sent_state = request
            .extensions()
            .get::<PeerReplayStateSnapshot>()
            .map(|snapshot| snapshot.0)
            .unwrap_or_default();
        if let (Some(audience), Some(challenge)) = (self.audience.as_deref(), challenge) {
            // The challenge is independently HMAC-authenticated by the response proof. It is not
            // part of v2 so old peers ignore it, while a new peer can safely advertise its epoch.
            request.headers_mut().insert(
                RPC_BOOT_EPOCH_CHALLENGE_HEADER,
                challenge.to_string().parse().expect("UUID must be a valid header value"),
            );
            if let (Some(boot_epoch), Some(timestamp), Some(content_sha256)) = (
                sent_state.boot_epoch,
                request.headers().get(TIMESTAMP_HEADER).and_then(|value| value.to_str().ok()),
                request
                    .headers()
                    .get(RPC_CONTENT_SHA256_HEADER)
                    .and_then(|value| value.to_str().ok()),
            ) {
                match gen_tonic_replay_scope_headers(audience, request.uri().path(), timestamp, content_sha256, boot_epoch) {
                    Ok(headers) => request.headers_mut().extend(headers),
                    Err(error) => debug!(error = %error, "could not attach replay-scoped RPC signature"),
                }
            }
        }

        let audience = self.audience.clone();
        let future = self.inner.call(request);
        Box::pin(async move {
            let response = future.await?;
            if let (Some(audience), Some(challenge)) = (audience, challenge) {
                let response_state = verify_tonic_peer_replay_capabilities_response(&audience, challenge, response.headers());
                if let Err(error) = &response_state
                    && (response.headers().contains_key(RPC_REPLAY_CACHE_CAPABILITY_HEADER)
                        || response.headers().contains_key(RPC_REPLAY_CACHE_CAPABILITY_PROOF_HEADER))
                {
                    debug!(
                        event = "internode_rpc_capability_proof_rejected",
                        component = "ecstore",
                        subsystem = "rpc_client",
                        result = "rejected",
                        error = %error,
                        "internode RPC capability proof rejected"
                    )
                }
                apply_peer_replay_response(audience, sent_state, response_state);
            }
            Ok(response)
        })
    }
}

pub struct TonicSignatureInterceptor {
    audience: Option<String>,
    body_digest_strict: bool,
}

impl tonic::service::Interceptor for TonicSignatureInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let method = req
            .extensions()
            .get::<tonic::GrpcMethod<'_>>()
            .ok_or_else(|| tonic::Status::unauthenticated("Missing gRPC method metadata"))?;
        let audience = self
            .audience
            .as_deref()
            .ok_or_else(|| tonic::Status::unauthenticated("Missing gRPC audience"))?;
        let content_sha256 = req
            .metadata()
            .get(RPC_CONTENT_SHA256_HEADER)
            .and_then(|value| value.to_str().ok());
        // RUSTFS_COMPAT_TODO(disk-mutation-body-digest): use cache-free v2 for peers without an authenticated boot epoch. Remove after every supported peer advertises the authenticated dynamic replay-cache capability and body-digest strict mode is the default.
        // beta.11 verifies v2 body digests but stores their nonces in a fixed-size cache.
        let rolling_mutation = req.extensions().get::<RollingMutationBodyDigest>().is_some();
        let peer_state = PEER_REPLAY_STATES
            .lock()
            .map_err(|_| tonic::Status::unauthenticated("RPC peer capability state unavailable"))?
            .get(audience)
            .copied()
            .unwrap_or_default();
        let content_sha256 = if content_sha256.is_some() {
            if peer_state.cache_capability == Some(PeerReplayCapability::Revoked) {
                return Err(tonic::Status::unauthenticated("RPC peer replay capability changed"));
            }
            if rolling_mutation && !self.body_digest_strict && peer_state.boot_epoch.is_none() {
                None
            } else {
                content_sha256
            }
        } else {
            content_sha256
        };
        let headers = gen_tonic_signature_headers(audience, method.service(), method.method(), content_sha256)
            .map_err(|_| tonic::Status::unauthenticated("No valid auth token"))?;
        req.metadata_mut().as_mut().extend(headers);
        req.extensions_mut().insert(PeerReplayStateSnapshot(peer_state));
        inject_trace_context_into_metadata(req.metadata_mut());
        inject_request_id_into_metadata(req.metadata_mut());
        Ok(req)
    }
}

pub fn gen_tonic_signature_interceptor() -> TonicSignatureInterceptor {
    TonicSignatureInterceptor {
        audience: None,
        body_digest_strict: internode_rpc_body_digest_strict(),
    }
}

pub struct NoOpInterceptor;

impl tonic::service::Interceptor for NoOpInterceptor {
    fn call(&mut self, req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        Ok(req)
    }
}

pub enum TonicInterceptor {
    Signature(TonicSignatureInterceptor),
    NoOp(NoOpInterceptor),
}

impl TonicInterceptor {
    fn with_rpc_audience(mut self, addr: &str) -> std::io::Result<Self> {
        if let Self::Signature(interceptor) = &mut self {
            let uri = addr
                .parse::<Uri>()
                .map_err(|_| std::io::Error::other("Invalid gRPC peer URI"))?;
            let audience = uri
                .authority()
                .map(|authority| normalize_tonic_rpc_audience(authority.as_str()))
                .ok_or_else(|| std::io::Error::other("Missing gRPC peer authority"))?;
            interceptor.audience = Some(audience?);
        }
        Ok(self)
    }

    fn replay_scope_audience(&self) -> Option<String> {
        match self {
            Self::Signature(interceptor) => interceptor.audience.clone(),
            Self::NoOp(_) => None,
        }
    }
}

impl tonic::service::Interceptor for TonicInterceptor {
    fn call(&mut self, req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        match self {
            TonicInterceptor::Signature(interceptor) => interceptor.call(req),
            TonicInterceptor::NoOp(interceptor) => interceptor.call(req),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::global;
    use opentelemetry::trace::{SpanContext, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider as _};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tonic::service::Interceptor;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::{Registry, layer::SubscriberExt};

    #[derive(Clone)]
    struct EpochProofService {
        audience: String,
        include_capability: bool,
        seen_headers: std::sync::Arc<Mutex<Vec<http::HeaderMap>>>,
    }

    impl Service<HttpRequest<()>> for EpochProofService {
        type Response = HttpResponse<()>;
        type Error = std::convert::Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: HttpRequest<()>) -> Self::Future {
            self.seen_headers
                .lock()
                .expect("test header capture lock must not be poisoned")
                .push(request.headers().clone());
            let challenge = tonic_boot_epoch_challenge(request.headers())
                .expect("client challenge must be syntactically valid")
                .expect("authenticated client request must carry a boot epoch challenge");
            let mut response = HttpResponse::new(());
            let mut headers = tonic_boot_epoch_response_headers(&self.audience, challenge)
                .expect("test server must be able to sign an epoch proof");
            if !self.include_capability {
                headers.remove(RPC_REPLAY_CACHE_CAPABILITY_HEADER);
                headers.remove(RPC_REPLAY_CACHE_CAPABILITY_PROOF_HEADER);
            }
            response.headers_mut().extend(headers);
            std::future::ready(Ok(response))
        }
    }

    #[derive(Clone)]
    struct MissingProofService;

    impl Service<HttpRequest<()>> for MissingProofService {
        type Response = HttpResponse<()>;
        type Error = std::convert::Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: HttpRequest<()>) -> Self::Future {
            std::future::ready(Ok(HttpResponse::new(())))
        }
    }

    fn ensure_test_rpc_secret() {
        runtime_sources::ensure_test_rpc_secret();
    }

    fn test_request() -> tonic::Request<()> {
        test_request_for("Ping")
    }

    fn test_request_for(method: &'static str) -> tonic::Request<()> {
        let mut request = tonic::Request::new(());
        request
            .extensions_mut()
            .insert(tonic::GrpcMethod::new("node_service.NodeService", method));
        request
    }

    fn test_interceptor() -> TonicSignatureInterceptor {
        test_interceptor_for("node-a:9000", false)
    }

    fn test_interceptor_for(audience: &str, body_digest_strict: bool) -> TonicSignatureInterceptor {
        TonicSignatureInterceptor {
            audience: Some(audience.to_string()),
            body_digest_strict,
        }
    }

    fn clear_peer_capability(audience: &str) {
        PEER_REPLAY_STATES
            .lock()
            .expect("peer capability cache lock must not be poisoned")
            .remove(audience);
    }

    fn rolling_mutation_request(method: &'static str) -> tonic::Request<()> {
        let mut request = tonic::Request::new(rustfs_protos::proto_gen::node_service::GenerallyLockRequest {
            args: "canonical mutation request".to_string(),
        });
        request
            .extensions_mut()
            .insert(tonic::GrpcMethod::new("node_service.NodeService", method));
        crate::cluster::rpc::set_tonic_rolling_mutation_body_digest(&mut request).expect("test mutation digest must be attached");
        request.map(|_| ())
    }

    fn replay_scope_request(audience: &str, method: &'static str) -> HttpRequest<()> {
        let mut request = HttpRequest::builder()
            .uri(format!("/node_service.NodeService/{method}"))
            .body(())
            .expect("test RPC request must build");
        request.headers_mut().extend(
            gen_tonic_signature_headers(audience, "node_service.NodeService", method, None).expect("v2 test headers must mint"),
        );
        request
            .extensions_mut()
            .insert(PeerReplayStateSnapshot(peer_replay_state(audience)));
        request
    }

    fn authenticated_peer_response(boot_epoch: Uuid, dynamic_replay_cache: bool) -> AuthenticatedPeerReplayCapabilities {
        AuthenticatedPeerReplayCapabilities {
            boot_epoch,
            dynamic_replay_cache,
        }
    }

    fn with_trace_parent<F>(trace_id_hex: &str, f: F)
    where
        F: FnOnce(),
    {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let provider = SdkTracerProvider::builder().build();
        let tracer = provider.tracer("rpc-client-tests");
        let subscriber = Registry::default().with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("rpc-client-test-span");
            let trace_id = TraceId::from_hex(trace_id_hex).expect("trace id should be valid hex");
            let span_id = opentelemetry::trace::SpanId::from_hex("0102030405060708").expect("span id should be valid hex");
            let parent = SpanContext::new(trace_id, span_id, TraceFlags::SAMPLED, true, TraceState::default());
            span.set_parent(opentelemetry::Context::new().with_remote_span_context(parent))
                .expect("failed to set parent context");
            let _guard = span.enter();

            f();
        });
        let _ = provider.shutdown();
    }

    #[test]
    fn network_like_disk_error_uses_typed_status_code() {
        // Transport-level Unavailable statuses justify retry/eviction.
        assert!(is_network_like_disk_error(&DiskError::from(tonic::Status::unavailable(
            "storage layer is not initialized"
        ))));
        // Application statuses from a live peer must not look network-like,
        // even when their message contains transport-sounding words.
        assert!(!is_network_like_disk_error(&DiskError::from(tonic::Status::internal(
            "failed to heal bucket \"unavailable-logs\""
        ))));
        assert!(!is_network_like_disk_error(&DiskError::from(tonic::Status::unauthenticated(
            "No valid auth token"
        ))));
        // A slow peer that blew the per-RPC deadline is still answering.
        assert!(!is_network_like_disk_error(&DiskError::from(tonic::Status::cancelled("Timeout expired"))));
    }

    #[test]
    fn embedded_tonic_status_is_recovered_across_error_conversions() {
        // DiskError and StorageError share one wrapper, so a status keeps its
        // typed classification whichever error it was converted into first.
        let from_storage: DiskErrorType = crate::error::Error::from(tonic::Status::unavailable("peer gone")).into();
        let DiskError::Io(io_err) = &from_storage else {
            panic!("status-derived disk error should stay an Io error");
        };
        assert_eq!(embedded_tonic_status(io_err).map(|status| status.code()), Some(tonic::Code::Unavailable));

        let from_disk = crate::error::Error::from(DiskError::from(tonic::Status::unavailable("peer gone")));
        let crate::error::Error::Io(io_err) = &from_disk else {
            panic!("status-derived storage error should stay an Io error");
        };
        assert_eq!(embedded_tonic_status(io_err).map(|status| status.code()), Some(tonic::Code::Unavailable));
    }

    #[test]
    fn network_like_disk_error_ignores_transport_words_in_application_statuses() {
        // Same contract as the peer client: a status the peer answered with
        // is not a transport failure, so it must not drive a reconnect even
        // when its message describes one.
        assert!(!is_network_like_disk_error(&DiskError::from(tonic::Status::internal(
            "connection refused while dialing downstream backend"
        ))));
        assert!(!is_network_like_disk_error(&DiskError::from(tonic::Status::unauthenticated(
            "connection reset while validating token"
        ))));
    }

    #[test]
    fn network_like_disk_error_requires_anchored_unavailable_needle() {
        // Regression: a bare "unavailable" needle used to match application
        // text such as a bucket name.
        assert!(!is_network_like_disk_error(&DiskError::other("bucket \"unavailable-logs\" not found")));
        // Anchored renderings of a flattened Unavailable status still match.
        assert!(is_network_like_disk_error(&DiskError::other(
            "code: 'The service is currently unavailable', message: \"peer gone\""
        )));
        assert!(is_network_like_disk_error(&DiskError::other(
            "status: Unavailable, message: \"peer gone\""
        )));
        assert!(is_network_like_disk_error(&DiskError::other("connection refused")));
        assert!(!is_network_like_disk_error(&DiskError::FileNotFound));
    }

    #[test]
    fn test_signature_interceptor_keeps_auth_headers() {
        ensure_test_rpc_secret();
        let mut interceptor = test_interceptor();
        let req = test_request();

        let req = interceptor.call(req).expect("interceptor call should succeed");

        assert!(req.metadata().contains_key("x-rustfs-signature"));
        assert!(req.metadata().contains_key("x-rustfs-timestamp"));
        assert!(req.metadata().contains_key("x-rustfs-rpc-signature-v2"));
        assert!(req.metadata().contains_key("x-rustfs-rpc-nonce"));
        assert!(
            crate::cluster::rpc::verify_tonic_rpc_signature(
                "node-a:9000",
                "/node_service.NodeService/Ping",
                req.metadata().as_ref(),
            )
            .is_ok(),
            "interceptor signature should bind the configured peer audience and generated method"
        );
    }

    #[test]
    fn unknown_peer_mutations_use_cache_free_unsigned_v2() {
        ensure_test_rpc_secret();
        let audience = "legacy-body-digest-client-test:9000";
        clear_peer_capability(audience);
        let mut interceptor = test_interceptor_for(audience, false);
        for method in ["Lock", "WriteAll"] {
            let request = interceptor
                .call(rolling_mutation_request(method))
                .expect("interceptor call should succeed");

            assert_eq!(
                request
                    .metadata()
                    .get("x-rustfs-content-sha256")
                    .and_then(|value| value.to_str().ok()),
                Some("UNSIGNED-PAYLOAD")
            );
            assert_eq!(
                request
                    .metadata()
                    .get("x-rustfs-rpc-nonce")
                    .and_then(|value| value.to_str().ok()),
                Some("unsigned")
            );
            assert!(
                crate::cluster::rpc::verify_tonic_rpc_signature(
                    audience,
                    &format!("/node_service.NodeService/{method}"),
                    request.metadata().as_ref(),
                )
                .is_ok(),
                "the cache-free request must retain valid audience- and method-bound v2 authentication"
            );
        }
    }

    #[test]
    fn unknown_peer_exact_body_contract_remains_body_bound() {
        ensure_test_rpc_secret();
        let audience = "exact-body-contract-client-test:9000";
        clear_peer_capability(audience);
        let mut interceptor = test_interceptor_for(audience, false);
        let mut request = test_request_for("ScannerActivity");
        crate::cluster::rpc::set_tonic_canonical_body_digest(&mut request, b"exact scanner activity body")
            .expect("test exact body digest must be attached");
        let expected_digest = request
            .metadata()
            .get("x-rustfs-content-sha256")
            .and_then(|value| value.to_str().ok())
            .expect("test request must carry its digest")
            .to_string();

        let request = interceptor.call(request).expect("interceptor call should succeed");

        assert_eq!(
            request
                .metadata()
                .get("x-rustfs-content-sha256")
                .and_then(|value| value.to_str().ok()),
            Some(expected_digest.as_str())
        );
        assert!(
            crate::cluster::rpc::verify_tonic_rpc_signature(
                audience,
                "/node_service.NodeService/ScannerActivity",
                request.metadata().as_ref(),
            )
            .is_ok()
        );
    }

    #[test]
    fn unknown_peer_iam_mutation_helper_remains_body_bound() {
        ensure_test_rpc_secret();
        let audience = "exact-iam-mutation-client-test:9000";
        clear_peer_capability(audience);
        let mut interceptor = test_interceptor_for(audience, false);
        let mut request = tonic::Request::new(rustfs_protos::proto_gen::node_service::DeleteUserRequest {
            access_key: "target-access-key".to_string(),
        });
        request
            .extensions_mut()
            .insert(tonic::GrpcMethod::new("node_service.NodeService", "DeleteUser"));
        crate::cluster::rpc::set_tonic_mutation_body_digest(&mut request).expect("test IAM mutation digest must be attached");
        let request = request.map(|_| ());
        let expected_digest = request
            .metadata()
            .get("x-rustfs-content-sha256")
            .and_then(|value| value.to_str().ok())
            .expect("test IAM mutation must carry its digest")
            .to_string();

        let request = interceptor.call(request).expect("interceptor call should succeed");

        assert_eq!(
            request
                .metadata()
                .get("x-rustfs-content-sha256")
                .and_then(|value| value.to_str().ok()),
            Some(expected_digest.as_str())
        );
        assert!(
            crate::cluster::rpc::verify_tonic_rpc_signature(
                audience,
                "/node_service.NodeService/DeleteUser",
                request.metadata().as_ref(),
            )
            .is_ok(),
            "IAM mutations must remain body-bound before capability discovery"
        );
    }

    #[test]
    fn authenticated_replay_cache_capability_enables_body_binding() {
        ensure_test_rpc_secret();
        let audience = "body-digest-capable-client-test:9000";
        clear_peer_capability(audience);
        let seen_headers = std::sync::Arc::new(Mutex::new(Vec::new()));
        let service = EpochProofService {
            audience: audience.to_string(),
            include_capability: true,
            seen_headers,
        };
        let mut channel = ReplayScopeChannel::new(service, Some(audience.to_string()));
        futures::executor::block_on(channel.call(replay_scope_request(audience, "Ping")))
            .expect("authenticated capability probe must complete");

        let mut interceptor = test_interceptor_for(audience, false);
        let request = rolling_mutation_request("Lock");
        let expected_digest = request
            .metadata()
            .get("x-rustfs-content-sha256")
            .and_then(|value| value.to_str().ok())
            .expect("test mutation must carry its digest")
            .to_string();

        let request = interceptor.call(request).expect("interceptor call should succeed");

        assert_eq!(
            request
                .metadata()
                .get("x-rustfs-content-sha256")
                .and_then(|value| value.to_str().ok()),
            Some(expected_digest.as_str())
        );
        let nonce = request
            .metadata()
            .get("x-rustfs-rpc-nonce")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| Uuid::parse_str(value).ok())
            .expect("capable peer body-bound mutation must carry a UUID nonce");
        assert!(!nonce.is_nil());
        assert!(
            crate::cluster::rpc::verify_tonic_rpc_signature(
                audience,
                "/node_service.NodeService/Lock",
                request.metadata().as_ref(),
            )
            .is_ok(),
            "the body-bound request must retain valid audience- and method-bound v2 authentication"
        );
        clear_peer_capability(audience);
    }

    #[test]
    fn invalid_capability_proof_does_not_enable_body_binding() {
        ensure_test_rpc_secret();
        let audience = "invalid-capability-client-test:9000";
        clear_peer_capability(audience);
        let service = EpochProofService {
            audience: "wrong-capability-audience:9000".to_string(),
            include_capability: true,
            seen_headers: std::sync::Arc::new(Mutex::new(Vec::new())),
        };
        let mut channel = ReplayScopeChannel::new(service, Some(audience.to_string()));
        futures::executor::block_on(channel.call(replay_scope_request(audience, "Ping")))
            .expect("invalid capability response must still complete");

        let mut interceptor = test_interceptor_for(audience, false);
        let request = interceptor
            .call(rolling_mutation_request("Lock"))
            .expect("legacy-compatible mutation must still be signed");

        assert_eq!(
            request
                .metadata()
                .get("x-rustfs-content-sha256")
                .and_then(|value| value.to_str().ok()),
            Some("UNSIGNED-PAYLOAD")
        );
    }

    #[test]
    fn legacy_boot_proof_keeps_mutations_body_bound_and_enables_non_ping_v3() {
        ensure_test_rpc_secret();
        let audience = "legacy-boot-proof-client-test:9000";
        clear_peer_capability(audience);
        let seen_headers = std::sync::Arc::new(Mutex::new(Vec::new()));
        let service = EpochProofService {
            audience: audience.to_string(),
            include_capability: false,
            seen_headers: seen_headers.clone(),
        };
        let mut channel = ReplayScopeChannel::new(service, Some(audience.to_string()));
        futures::executor::block_on(channel.call(replay_scope_request(audience, "Ping")))
            .expect("legacy boot proof response must complete");

        let state = peer_replay_state(audience);
        assert!(state.boot_epoch.is_some(), "authenticated legacy proof must enable replay-scoped v3");
        assert_eq!(state.cache_capability, None);

        let mut interceptor = test_interceptor_for(audience, false);
        let request = rolling_mutation_request("Lock");
        let expected_digest = request
            .metadata()
            .get("x-rustfs-content-sha256")
            .and_then(|value| value.to_str().ok())
            .expect("test mutation must carry its digest")
            .to_string();
        let request = interceptor.call(request).expect("legacy-compatible mutation must be signed");
        assert_eq!(
            request
                .metadata()
                .get("x-rustfs-content-sha256")
                .and_then(|value| value.to_str().ok()),
            Some(expected_digest.as_str())
        );
        let (metadata, extensions, body) = request.into_parts();
        let mut request = HttpRequest::new(body);
        *request.uri_mut() = "/node_service.NodeService/Lock".parse().expect("test RPC URI must parse");
        *request.headers_mut() = metadata.into_headers();
        *request.extensions_mut() = extensions;
        futures::executor::block_on(channel.call(request)).expect("legacy strict-compatible lock request must complete");

        let headers = seen_headers.lock().expect("test header capture lock must not be poisoned");
        assert!(
            headers[1].contains_key(RPC_REPLAY_SCOPE_VERSION_HEADER),
            "authenticated legacy boot proof must enable v3 on a non-Ping request"
        );
    }

    #[test]
    fn reordered_capability_responses_cannot_undo_newer_state() {
        let audience = "reordered-capability-client-test:9000";
        let epoch_one = Uuid::new_v4();
        let epoch_two = Uuid::new_v4();
        clear_peer_capability(audience);

        let unknown = PeerReplayState::default();
        apply_peer_replay_response(audience.to_string(), unknown, Ok(authenticated_peer_response(epoch_one, true)));
        apply_peer_replay_response(audience.to_string(), unknown, Err(std::io::Error::other("delayed legacy response")));
        let epoch_one_state = PeerReplayState {
            boot_epoch: Some(epoch_one),
            cache_capability: Some(PeerReplayCapability::Capable { boot_epoch: epoch_one }),
        };
        assert_eq!(peer_replay_state(audience), epoch_one_state);

        apply_peer_replay_response(audience.to_string(), epoch_one_state, Err(std::io::Error::other("rollback response")));
        apply_peer_replay_response(audience.to_string(), epoch_one_state, Ok(authenticated_peer_response(epoch_one, true)));
        assert_eq!(
            peer_replay_state(audience),
            PeerReplayState {
                boot_epoch: Some(epoch_one),
                cache_capability: Some(PeerReplayCapability::Revoked),
            }
        );

        let revoked = peer_replay_state(audience);
        apply_peer_replay_response(audience.to_string(), revoked, Ok(authenticated_peer_response(epoch_two, true)));
        apply_peer_replay_response(audience.to_string(), epoch_one_state, Ok(authenticated_peer_response(epoch_one, true)));
        assert_eq!(
            peer_replay_state(audience),
            PeerReplayState {
                boot_epoch: Some(epoch_two),
                cache_capability: Some(PeerReplayCapability::Capable { boot_epoch: epoch_two }),
            }
        );
        clear_peer_capability(audience);
    }

    #[test]
    fn stale_capability_response_cannot_cross_a_new_boot_epoch() {
        let audience = "cross-epoch-capability-client-test:9000";
        let epoch_one = Uuid::new_v4();
        let epoch_two = Uuid::new_v4();
        let epoch_three = Uuid::new_v4();
        clear_peer_capability(audience);

        let revoked_epoch_one = PeerReplayState {
            boot_epoch: Some(epoch_one),
            cache_capability: Some(PeerReplayCapability::Revoked),
        };
        PEER_REPLAY_STATES
            .lock()
            .expect("peer replay state lock must not be poisoned")
            .insert(audience.to_string(), revoked_epoch_one);

        apply_peer_replay_response(
            audience.to_string(),
            revoked_epoch_one,
            Ok(authenticated_peer_response(epoch_three, false)),
        );
        apply_peer_replay_response(audience.to_string(), revoked_epoch_one, Ok(authenticated_peer_response(epoch_two, true)));

        assert_eq!(
            peer_replay_state(audience),
            PeerReplayState {
                boot_epoch: Some(epoch_three),
                cache_capability: Some(PeerReplayCapability::Revoked),
            },
            "a stale dynamic-cache proof must not cross a newer authenticated boot epoch"
        );
        clear_peer_capability(audience);
    }

    #[test]
    fn interceptor_snapshot_prevents_delayed_legacy_response_from_revoking_capability() {
        ensure_test_rpc_secret();
        let audience = "capability-snapshot-client-test:9000";
        clear_peer_capability(audience);
        let boot_epoch = Uuid::new_v4();
        let mut interceptor = test_interceptor_for(audience, false);
        let request = interceptor
            .call(rolling_mutation_request("Lock"))
            .expect("legacy-compatible request must pass the interceptor");
        assert_eq!(
            request
                .extensions()
                .get::<PeerReplayStateSnapshot>()
                .map(|snapshot| snapshot.0),
            Some(PeerReplayState::default()),
            "interceptor must preserve its unknown-state admission snapshot"
        );

        let capable_state = PeerReplayState {
            boot_epoch: Some(boot_epoch),
            cache_capability: Some(PeerReplayCapability::Capable { boot_epoch }),
        };
        PEER_REPLAY_STATES
            .lock()
            .expect("peer capability cache lock must not be poisoned")
            .insert(audience.to_string(), capable_state);
        let (metadata, extensions, body) = request.into_parts();
        let mut request = HttpRequest::new(body);
        *request.uri_mut() = "/node_service.NodeService/Lock".parse().expect("test RPC URI must parse");
        *request.headers_mut() = metadata.into_headers();
        *request.extensions_mut() = extensions;
        let mut channel = ReplayScopeChannel::new(MissingProofService, Some(audience.to_string()));

        futures::executor::block_on(channel.call(request)).expect("in-flight request response must complete");

        assert_eq!(peer_replay_state(audience), capable_state);
        clear_peer_capability(audience);
    }

    #[test]
    fn strict_mode_keeps_unknown_peer_mutations_body_bound() {
        ensure_test_rpc_secret();
        let audience = "strict-body-digest-client-test:9000";
        clear_peer_capability(audience);
        let mut interceptor = test_interceptor_for(audience, true);
        let request = rolling_mutation_request("WriteAll");
        let expected_digest = request
            .metadata()
            .get("x-rustfs-content-sha256")
            .and_then(|value| value.to_str().ok())
            .expect("test mutation must carry its digest")
            .to_string();

        let request = interceptor.call(request).expect("interceptor call should succeed");

        assert_eq!(
            request
                .metadata()
                .get("x-rustfs-content-sha256")
                .and_then(|value| value.to_str().ok()),
            Some(expected_digest.as_str())
        );
        let nonce = request
            .metadata()
            .get("x-rustfs-rpc-nonce")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| Uuid::parse_str(value).ok())
            .expect("strict body-bound mutation must carry a UUID nonce");
        assert!(!nonce.is_nil());
        assert!(
            crate::cluster::rpc::verify_tonic_rpc_signature(
                audience,
                "/node_service.NodeService/WriteAll",
                request.metadata().as_ref(),
            )
            .is_ok()
        );
    }

    #[test]
    fn missing_capability_after_pin_fails_closed() {
        ensure_test_rpc_secret();
        let audience = "revoked-capability-client-test:9000";
        let boot_epoch = Uuid::new_v4();
        PEER_REPLAY_STATES
            .lock()
            .expect("peer capability cache lock must not be poisoned")
            .insert(
                audience.to_string(),
                PeerReplayState {
                    boot_epoch: Some(boot_epoch),
                    cache_capability: Some(PeerReplayCapability::Capable { boot_epoch }),
                },
            );
        let mut channel = ReplayScopeChannel::new(MissingProofService, Some(audience.to_string()));
        futures::executor::block_on(channel.call(replay_scope_request(audience, "Ping")))
            .expect("legacy response must complete before capability rejection");

        let mut interceptor = test_interceptor_for(audience, false);
        let error = interceptor
            .call(rolling_mutation_request("Lock"))
            .expect_err("a peer that loses its pinned capability must fail closed");

        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert_eq!(error.message(), "RPC peer replay capability changed");
        clear_peer_capability(audience);
    }

    #[test]
    fn test_signature_interceptor_binds_audience_from_peer_uri() {
        let interceptor = TonicInterceptor::Signature(gen_tonic_signature_interceptor())
            .with_rpc_audience("http://node-a:9000")
            .expect("peer URI should provide an audience");
        let TonicInterceptor::Signature(interceptor) = interceptor else {
            panic!("signature interceptor variant should be preserved");
        };

        assert_eq!(interceptor.audience.as_deref(), Some("node-a:9000"));
    }

    #[test]
    fn replay_scope_channel_uses_epoch_proof_before_sending_v3() {
        ensure_test_rpc_secret();
        let audience = "replay-scope-client-test:9000";
        clear_peer_capability(audience);
        let seen_headers = std::sync::Arc::new(Mutex::new(Vec::new()));
        let service = EpochProofService {
            audience: audience.to_string(),
            include_capability: true,
            seen_headers: seen_headers.clone(),
        };
        let mut channel = ReplayScopeChannel::new(service, Some(audience.to_string()));
        let make_request = || replay_scope_request(audience, "Ping");

        futures::executor::block_on(channel.call(make_request())).expect("first request must complete");
        futures::executor::block_on(channel.call(make_request())).expect("second request must complete");

        let headers = seen_headers.lock().expect("test header capture lock must not be poisoned");
        assert_eq!(headers.len(), 2);
        assert!(headers[0].contains_key(RPC_BOOT_EPOCH_CHALLENGE_HEADER));
        assert!(
            !headers[0].contains_key(RPC_REPLAY_SCOPE_VERSION_HEADER),
            "the first request must remain v2-compatible until the peer proves its epoch"
        );
        assert!(
            headers[1].contains_key(RPC_REPLAY_SCOPE_VERSION_HEADER),
            "the second request must carry the replay-scoped v3 signature"
        );
        clear_peer_capability(audience);
    }

    #[test]
    fn test_signature_interceptor_requires_generated_method_metadata() {
        ensure_test_rpc_secret();
        let mut interceptor = test_interceptor();
        let error = interceptor
            .call(tonic::Request::new(()))
            .expect_err("requests without an exact generated method must fail closed");

        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert_eq!(error.message(), "Missing gRPC method metadata");
    }

    #[test]
    fn test_signature_interceptor_may_inject_request_id() {
        ensure_test_rpc_secret();
        let mut interceptor = test_interceptor();
        let req = test_request();

        let span = tracing::info_span!("grpc-rpc-test-span");
        let _guard = span.enter();
        let req = interceptor.call(req).expect("interceptor call should succeed");

        if let Some(v) = req.metadata().get("x-request-id") {
            assert!(!v.as_encoded_bytes().is_empty());
        }
    }

    #[test]
    fn test_signature_interceptor_injects_traceparent_metadata() {
        ensure_test_rpc_secret();
        let mut interceptor = test_interceptor();
        let req = test_request();

        with_trace_parent("4bf92f3577b34da6a3ce929d0e0e4736", || {
            let req = interceptor.call(req).expect("interceptor call should succeed");
            let traceparent = req
                .metadata()
                .get("traceparent")
                .and_then(|v| v.to_str().ok())
                .expect("traceparent metadata should be injected");
            assert!(traceparent.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"));
        });
    }
}
