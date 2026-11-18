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

//! The control-plane host: every route the admin router claims, served on the gateway stack.
//!
//! Responsible for: [`assemble`], a gateway service whose dialect hands each claimed request,
//! once its signature is verified, to the admin router's own access rule and route table
//! (`S3Router::admit` and `S3Router::dispatch`) — the admin, table-catalog and profiling prefixes
//! as one claimed operation, the STS form at `/`, and the query-discriminated extension and
//! Object Lambda rows the gateway's RustFS dialect declares.
//! NOT responsible for: which requests reach it (`server/stack.rs`), any access decision or route
//! (the admin router), or the ten system reads `admin.rs` serves natively.
//! Upstream: `server/stack.rs`. Downstream: the gateway signature verifier and the admin router.

use super::{GatewayAssembly, GatewayAssemblyError, GatewayStack};
use crate::admin::operation::AdminResponse;
use crate::admin::router::{AdminOperation, S3Router, dispatch, object_lambda_target, serve_object_lambda};
use crate::app::RequestEnvelope;
use crate::runtime_sources::ServerContextSlot;
use crate::server::gateway_handlers::handler_error;
use crate::storage_api::server::http::access::{ReqInfo, VerifiedSignature};
use bytes::Bytes;
use http_body::Body as _;
use rustfs_gateway::OperationFloor;
use rustfs_gateway::sig::SigService;
use rustfs_gateway::{
    Decision, Handler, HandlerResult, OWNED_RESPONSE_HEADERS, Operation, Req, Resp, SigV4Authenticator, decide_with,
};
use rustfs_gateway_adapter::{IamKeyLookup, RootSource};
use rustfs_gateway_core::codec::{CodecError, EncodedResponse, MetaView, OperationCodec, RequestBody, RequestBodyMode};
use rustfs_gateway_core::dialect::{
    ClaimedRoute, ClaimedRow, Dialect, DialectOverlay, FormRoute, OverlayRow, render_claimed_route,
};
use rustfs_gateway_core::op::{AuthRequirement, ResourceShape};
use rustfs_gateway_core::registry::{HandlerDeadlineClass, OperationSpec};
use rustfs_gateway_core::route::FormClaim;
use rustfs_gateway_core::{DerivedResourceError, NoDerived};
use rustfs_gateway_dialect_rustfs_admin::ops::{
    check_replication::CheckReplication, get_replication_metrics::GetReplicationMetrics,
    get_replication_metrics_v2::GetReplicationMetricsV2, get_replication_reset_status::GetReplicationResetStatus,
    invoke_object_lambda::InvokeObjectLambda, listen_bucket_notification::ListenBucketNotification,
    listen_notification::ListenNotification, reset_bucket_replication::ResetBucketReplication,
};
use rustfs_gateway_dialect_rustfs_admin::{
    AdminResponse as WireResponse, CLAIMS, ExtensionOperation, OVERLAY, TABLE_CATALOG_PREFIXES, admin,
};
use rustfs_gateway_stream::{ByteStream, PayloadCaps, PayloadRead, PayloadStream, StreamError, TrailingHeaders};
use rustfs_s3_types::{Body, s3_error};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

/// The router whose behaviour this host serves.
const ROUTER: &str = "https://github.com/rustfs/rustfs/blob/main/rustfs/src/admin/router.rs";

/// The one operation every admin, table-catalog and profiling request is routed to.
pub(super) const NAME: &str = "rustfs:ControlPlane";

/// Each claim exactly, with a trailing slash, and every descendant; every method.
static ROWS: &[ClaimedRow] = &[
    row("/rustfs/admin"),
    row("/rustfs/admin/"),
    row("/rustfs/admin/{*remainder}"),
    row("/minio/admin"),
    row("/minio/admin/"),
    row("/minio/admin/{*remainder}"),
    row("/_iceberg/v1"),
    row("/_iceberg/v1/"),
    row("/_iceberg/v1/{*remainder}"),
    row("/iceberg/v1"),
    row("/iceberg/v1/"),
    row("/iceberg/v1/{*remainder}"),
    row("/profile/cpu"),
    row("/profile/cpu/"),
    row("/profile/cpu/{*remainder}"),
    row("/profile/memory"),
    row("/profile/memory/"),
    row("/profile/memory/{*remainder}"),
];

const fn row(template: &'static str) -> ClaimedRow {
    ClaimedRow { template, selector: &[] }
}

const ROUTE: ClaimedRoute = ClaimedRoute {
    precedence: 65535,
    rows: ROWS,
    shadows: &[],
    bucket_param: None,
};

static SPEC: OperationSpec = OperationSpec::builder(NAME, 200, None)
    .handler_deadline_class(HandlerDeadlineClass::Standard)
    .required_params(&[])
    .auth(AuthRequirement::new(NAME, ResourceShape::Service))
    .build();

/// Header-signed, presigned or anonymous: the gateway verifies whatever signature is presented,
/// and `S3Router::admit` decides, exactly as on the legacy stack.
static FLOOR: OperationFloor =
    OperationFloor::builtin_presigned(NAME, SigService::S3).allow_anonymous_after_listing_in_the_posture_report();

/// Every claimed admin request, its body buffered: legacy RustFS bounds a claimed route's body at
/// 1 MiB, which the RustFS profile's claimed-route ceiling repeats, and a handler that answers
/// without reading its body must not leave the gateway a body it never finished.
#[derive(Debug)]
pub(super) struct ControlPlane;

impl Operation for ControlPlane {
    const NAME: &'static str = NAME;
    type Input = Bytes;
    type Output = WireResponse;
    type DerivedResources = NoDerived;
    fn derive_resources(_input: &Self::Input) -> Result<NoDerived, DerivedResourceError> {
        Ok(NoDerived)
    }
    fn seal_derived_input(_input: &mut Self::Input) {}
    fn spec() -> &'static OperationSpec {
        &SPEC
    }
    fn floor() -> &'static OperationFloor {
        &FLOOR
    }
}

impl OperationCodec for ControlPlane {
    const REQUEST_BODY: RequestBodyMode = RequestBodyMode::Full;
    fn decode(_request: &MetaView<'_>, body: RequestBody) -> Result<Bytes, CodecError> {
        admin::buffered(body)
    }
    fn encode(output: WireResponse, _request: &MetaView<'_>, status: u16) -> Result<EncodedResponse, CodecError> {
        admin::encode(output, status)
    }
}

/// The STS endpoint: a form `POST /`, on every host and whatever the query.
const STS_NAME: &str = "rustfs:StsForm";

static STS_CLAIM: FormClaim = FormClaim {
    path: "/",
    reason: "RustFS's admin router takes a form POST of / as its STS endpoint before its S3 path parser runs.",
    evidence: &[ROUTER],
};

const STS_ROUTE: FormRoute = FormRoute {
    precedence: 99,
    claim: &STS_CLAIM,
};

static STS_SPEC: OperationSpec = OperationSpec::builder(STS_NAME, 200, None)
    .handler_deadline_class(HandlerDeadlineClass::Standard)
    .required_params(&[])
    .auth(AuthRequirement::new(STS_NAME, ResourceShape::Service))
    .build();

/// As [`FLOOR`], for the STS signing service: the STS handler decides which actions need which
/// signature.
static STS_FLOOR: OperationFloor =
    OperationFloor::builtin_presigned(STS_NAME, SigService::Sts).allow_anonymous_after_listing_in_the_posture_report();

/// The STS form, its body buffered.
#[derive(Debug)]
pub(super) struct StsForm;

impl Operation for StsForm {
    const NAME: &'static str = STS_NAME;
    type Input = Bytes;
    type Output = WireResponse;
    type DerivedResources = NoDerived;
    fn derive_resources(_input: &Self::Input) -> Result<NoDerived, DerivedResourceError> {
        Ok(NoDerived)
    }
    fn seal_derived_input(_input: &mut Self::Input) {}
    fn spec() -> &'static OperationSpec {
        &STS_SPEC
    }
    fn floor() -> &'static OperationFloor {
        &STS_FLOOR
    }
}

impl OperationCodec for StsForm {
    const REQUEST_BODY: RequestBodyMode = RequestBodyMode::Full;
    fn decode(_request: &MetaView<'_>, body: RequestBody) -> Result<Bytes, CodecError> {
        admin::buffered(body)
    }
    fn encode(output: WireResponse, _request: &MetaView<'_>, status: u16) -> Result<EncodedResponse, CodecError> {
        admin::encode(output, status)
    }
}

/// The gateway's query-discriminated extension rows this host serves, recorded in the gateway's
/// own overlay.
const BORROWED: [&str; 8] = [
    ResetBucketReplication::NAME,
    GetReplicationResetStatus::NAME,
    GetReplicationMetricsV2::NAME,
    GetReplicationMetrics::NAME,
    CheckReplication::NAME,
    InvokeObjectLambda::NAME,
    ListenNotification::NAME,
    ListenBucketNotification::NAME,
];

fn dialect() -> Result<Dialect, GatewayAssemblyError> {
    static ROWS: OnceLock<Vec<OverlayRow>> = OnceLock::new();
    static SELECTOR: OnceLock<String> = OnceLock::new();
    static ACTION: OnceLock<String> = OnceLock::new();
    static STS_SELECTOR: OnceLock<String> = OnceLock::new();
    static STS_ACTION: OnceLock<String> = OnceLock::new();
    static RECORD: OnceLock<DialectOverlay> = OnceLock::new();
    let mut rows = vec![OverlayRow {
        name: NAME,
        precedence: ROUTE.precedence,
        selector: SELECTOR.get_or_init(|| render_claimed_route(&ROUTE)),
        action: ACTION.get_or_init(|| SPEC.auth.map(|auth| auth.render()).unwrap_or_default()),
        resource: ResourceShape::Service,
        success_status: 200,
        anonymous: true,
        evidence: &[ROUTER],
    }];
    rows.push(OverlayRow {
        name: STS_NAME,
        precedence: STS_ROUTE.precedence,
        selector: STS_SELECTOR.get_or_init(|| STS_CLAIM.render()),
        action: STS_ACTION.get_or_init(|| STS_SPEC.auth.map(|auth| auth.render()).unwrap_or_default()),
        resource: ResourceShape::Service,
        success_status: 200,
        anonymous: true,
        evidence: &[ROUTER],
    });
    for name in BORROWED {
        rows.push(OVERLAY.row(name).copied().ok_or(GatewayAssemblyError::AdminOverlayMissing)?);
    }
    let record = RECORD.get_or_init(|| DialectOverlay {
        name: "rustfs-control-plane",
        vendor: OVERLAY.vendor,
        claims: CLAIMS,
        operations: ROWS.get_or_init(|| rows),
    });
    Dialect::assemble(record)
        .declare_claimed::<ControlPlane>(ROUTE)
        .declare_form::<StsForm>(STS_ROUTE)
        .declare::<ResetBucketReplication>(ResetBucketReplication::ROUTE)
        .declare::<GetReplicationResetStatus>(GetReplicationResetStatus::ROUTE)
        .declare::<GetReplicationMetricsV2>(GetReplicationMetricsV2::ROUTE)
        .declare::<GetReplicationMetrics>(GetReplicationMetrics::ROUTE)
        .declare::<CheckReplication>(CheckReplication::ROUTE)
        .declare::<InvokeObjectLambda>(InvokeObjectLambda::ROUTE)
        .declare::<ListenNotification>(ListenNotification::ROUTE)
        .declare::<ListenBucketNotification>(ListenBucketNotification::ROUTE)
        .build()
        .map_err(GatewayAssemblyError::AdminDialect)
}

/// Assembles the host over `router`, the admin router the legacy stack mounts.
pub(super) fn assemble(
    assembly: GatewayAssembly,
    iam: Arc<dyn IamKeyLookup>,
    root: Arc<dyn RootSource>,
    router: Arc<S3Router<AdminOperation>>,
) -> Result<GatewayStack, GatewayAssemblyError> {
    // Every decision is `S3Router::admit`'s. A claimed request no row of this host takes — a
    // console path while the console is off, a health path while the probe is off — routes to an
    // operation with no handler here: a signed one is answered `501`, as the legacy route table
    // answers it, and an anonymous one is refused.
    let gate = decide_with(|request| {
        if [NAME, STS_NAME].contains(&request.operation) || BORROWED.contains(&request.operation) || request.identity.is_some() {
            Decision::Allow
        } else {
            Decision::Deny
        }
    });
    // Admin handlers read the caller's secret as the legacy stack hands it to every custom route:
    // the MinIO alias seals bodies with it and Object Lambda presigns its source URL with it.
    let authenticator = |authenticator: SigV4Authenticator| {
        authenticator
            .hand_caller_secret_to_handlers()
            .verify_paths_double_encoded_under(TABLE_CATALOG_PREFIXES)
    };
    let host = Arc::new(Host {
        router,
        server_ctx: Arc::clone(&assembly.server_ctx),
    });
    let service = GatewayStack::configured_builder(&assembly, iam, root, gate, authenticator)?
        .hand_caller_secret_to_every_operation_after_listing_in_the_posture_report()
        .dialect(&dialect()?)
        .register::<ControlPlane, _>(Arc::clone(&host))
        .register::<StsForm, _>(Arc::clone(&host))
        .register::<ResetBucketReplication, _>(Arc::clone(&host))
        .register::<GetReplicationResetStatus, _>(Arc::clone(&host))
        .register::<GetReplicationMetricsV2, _>(Arc::clone(&host))
        .register::<GetReplicationMetrics, _>(Arc::clone(&host))
        .register::<CheckReplication, _>(Arc::clone(&host))
        .register::<InvokeObjectLambda, _>(Arc::clone(&host))
        .register::<ListenNotification, _>(Arc::clone(&host))
        .register::<ListenBucketNotification, _>(host)
        .build()
        .map_err(GatewayAssemblyError::Service)?;
    Ok(GatewayStack {
        service,
        keystone_auth: assembly.keystone_auth,
    })
}

/// The admin router behind every operation of the host, and the server its requests run on.
struct Host {
    router: Arc<S3Router<AdminOperation>>,
    server_ctx: Arc<ServerContextSlot>,
}

/// The request body an operation's codec decoded, as the admin handlers read it.
trait AdminBody {
    fn into_admin_body(self) -> Body;
}

impl AdminBody for () {
    fn into_admin_body(self) -> Body {
        Body::empty()
    }
}

impl AdminBody for Bytes {
    fn into_admin_body(self) -> Body {
        Body::from(self)
    }
}

impl<O> Handler<O> for Host
where
    O: Operation<Output = WireResponse>,
    O::Input: AdminBody,
{
    async fn call(&self, request: Req<O>) -> HandlerResult<O> {
        let router = &self.router;
        // The legacy stack marks no custom-route request as SigV4-verified, so neither does this.
        let mut envelope = RequestEnvelope::from_gateway(request.context(), &self.server_ctx, VerifiedSignature::default())
            .map_err(handler_error)?;
        if let Some(info) = request.context().transport_extensions().get::<ReqInfo>() {
            envelope.install_req_info(info.clone());
        }
        router
            .admit(envelope.method(), envelope.uri(), envelope.headers(), envelope.credentials().is_some())
            .map_err(handler_error)?;
        let body = request.into_input().into_admin_body();
        let response = match object_lambda_target(envelope.method(), envelope.uri()) {
            Some((bucket, object)) => serve_object_lambda(envelope, &bucket, &object).await,
            None => dispatch(router, envelope, body).await,
        }
        .map_err(handler_error)?;
        respond(response)
    }
}

/// The handler's answer as the gateway writes it: its status, its headers except those the
/// gateway frames itself, and its body streamed with the length it declared.
fn respond<O: Operation<Output = WireResponse>>(response: AdminResponse) -> HandlerResult<O> {
    let (status, mut headers, body) = response.into_parts();
    let declared = headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    for name in OWNED_RESPONSE_HEADERS {
        headers.remove(*name);
    }
    let remaining = body.size_hint().exact().or(declared);
    let stream = ByteStream::new(Box::pin(AdminPayload { body, remaining }))
        .map_err(|_| handler_error(s3_error!(InternalError, "failed to stream admin response")))?;
    Ok(Resp::with_status(WireResponse::stream("", stream), status.as_u16()).with_extra_headers(headers))
}

/// An admin response body read as the gateway's payload.
struct AdminPayload {
    body: Body,
    remaining: Option<u64>,
}

impl PayloadStream for AdminPayload {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<PayloadRead, StreamError>> {
        loop {
            match Pin::new(&mut self.body).poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    return Poll::Ready(Ok(PayloadRead::Eof {
                        trailers: TrailingHeaders::empty(),
                    }));
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(StreamError::upstream(error))),
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(bytes) if !bytes.is_empty() => {
                        let read = bytes.len() as u64;
                        self.remaining = self.remaining.map(|remaining| remaining.saturating_sub(read));
                        return Poll::Ready(Ok(PayloadRead::Chunk(bytes)));
                    }
                    _ => {}
                },
            }
        }
    }

    fn caps(&self) -> PayloadCaps {
        match self.remaining {
            Some(_) => PayloadCaps::PUSH | PayloadCaps::KNOWN_LENGTH,
            None => PayloadCaps::PUSH,
        }
    }

    fn len_hint(&self) -> Option<u64> {
        self.remaining
    }
}
