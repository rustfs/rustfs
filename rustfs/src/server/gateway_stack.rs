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
//! The gateway S3 stack of one server: its assembly, and the facts the host hands every request
//! (rustfs/backlog#2734 T2.5).
//!
//! Responsible for: [`GatewayStack::assemble`], the one place the `rustfs-gateway` service is
//! built for RustFS — the RustFS profile, signature verification against RustFS IAM, the early
//! gate, explicit deadlines, and the registered handlers — and [`GatewayStack::call`], which
//! attaches the host's facts to a request before the gateway accepts it.
//! NOT responsible for: choosing the stack (`server/stack.rs`), the middleware around it
//! (`server/http.rs`), or any S3 behaviour (`server/gateway_handlers` and the use cases).
//! Upstream: `server/http.rs`. Downstream: `rustfs-gateway`, `rustfs-gateway-adapter`.
//!
//! # What the host keeps
//!
//! RustFS's own layers stay in front of the gateway and the gateway's counterparts stay off
//! (rustfs/backlog#2771): CORS is answered by `ConditionalCorsLayer`, so no CORS source is
//! installed and the gateway writes no CORS header; the SSE-C transport layer runs first and the
//! gateway gate follows the same deployment setting; and the request identifier is the one
//! `ExternalRequestContextLayer` assigned, handed over as the gateway's `HostRequestId`, so an
//! answer and RustFS's logs name a request identically.
//!
//! # Authorization
//!
//! The gateway's `Authorizer` is an early gate in front of the handler, not a second policy
//! engine. An operation the table does not serve is denied. A served operation is decided by the
//! same core the legacy access hook calls (`server/gateway_authorizer`): the gate may deny only
//! what that hook would deny, and the handler still runs the hook. Body-derived conditions and
//! any check that would take the table publication fence stay in the handler. A backend failure
//! is indeterminate. No policy source is installed: the gate reads the server's own IAM and
//! bucket policy through that core.
//!
//! # Expected bucket owner
//!
//! The gateway checks `x-amz-expected-bucket-owner` after the early gate allowed a request,
//! against the one owner id RustFS reports. RustFS stores no per-bucket owner, so
//! the answer is that id for every bucket, with no lookup: a missing bucket is the handler's own
//! `NoSuchBucket`.

mod admin;
mod control_plane;
pub(in crate::server) mod indeterminate_audit;

#[cfg(test)]
pub(crate) static CONFIG_HANDLER_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

use crate::app::dto_values::owner;
use crate::runtime_sources::{ServerContextSlot, current_action_credentials};
use crate::server::gateway_handlers::{GatewayBackend, OPERATIONS, OperationTable, metadata_listing, register};
use crate::server::ssec_transport::{is_secure_transport, require_tls};
use crate::shared_types::{NativeTlsTransport, RemoteAddr};
use crate::storage_api::server::http::request_context::RequestContext;
use http::{Extensions, Request, Response};
use rustfs_config::RUSTFS_REGION;
use rustfs_gateway::{
    AssemblyError, Authorizer, BoxFuture, BucketName, BucketOwnerError, BucketOwnerSource, ClientAddr, ConnectionIntent,
    DEFAULT_MAX_BUFFERED_BODY_BYTES, HandlerDeadlineClass, HandlerDeadlineConfig, MintedTraces, PlaintextCustomerKeyAck,
    RegionSet, RequestBodyDeadlineConfig, ServiceBuilder, ServiceConfig, SigV4Authenticator, SseConfig, TransportSecurity,
    connection_intent_of,
};
#[cfg(test)]
use rustfs_gateway::{Decision, decide_with};
use rustfs_gateway_adapter::{
    IamCredentials, IamKeyLookup, KeystoneEstablished, KeystoneVerifier, RequestIdCarrier, RootCredentials, RootCredentialsError,
    RootSource, establish_keystone_identity, host_request_id, keystone_header_token_claim, rustfs_host_resolver,
};
use rustfs_iam::error::Error as IamError;
use rustfs_iam::manager::get_token_signing_key;
use rustfs_iam::store::object::ObjectStore;
use rustfs_iam::sys::IamSys;
use rustfs_policy::auth::UserIdentity;
use rustfs_trusted_proxies::ClientInfo;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// What the assembly takes from the server it is built for.
#[derive(Clone)]
pub(crate) struct GatewayAssembly {
    pub(crate) server_ctx: Arc<ServerContextSlot>,
    /// The configured root credentials, checked once here. Requests read the live ones.
    pub(crate) root_access_key: String,
    pub(crate) root_secret_key: String,
    /// The configured region; the default region signs when none is configured.
    pub(crate) region: Option<String>,
    /// The virtual-hosted-style base domains of the S3 listener, as the legacy stack reads them.
    pub(crate) server_domains: Vec<String>,
    /// `RUSTFS_HTTP_REQUEST_BODY_READ_TIMEOUT`: the longest silence between two body reads. Zero
    /// disables the bound, as it does on the legacy stack.
    pub(crate) body_read_timeout: Duration,
    /// The same initialized provider installed in the outer Keystone middleware.
    pub(crate) keystone_auth: Option<Arc<rustfs_keystone::KeystoneAuthProvider>>,
}

/// Reads and validates the configuration before enabling the gateway identity bridge.
pub(crate) fn keystone_provider_in_configuration()
-> Result<Option<Arc<rustfs_keystone::KeystoneAuthProvider>>, GatewayAssemblyError> {
    let config = rustfs_keystone::KeystoneConfig::from_env().map_err(GatewayAssemblyError::KeystoneConfig)?;
    if !config.enable {
        return Ok(None);
    }
    config.validate().map_err(GatewayAssemblyError::KeystoneConfig)?;
    crate::auth_keystone::get_keystone_auth()
        .map(Some)
        .ok_or(GatewayAssemblyError::KeystoneNotInitialized)
}

/// Sanitized authorization facts paired with the identity established at this gateway edge.
/// Only consuming the middleware's validation can construct this request payload.
#[derive(Clone)]
pub(crate) struct OwnedKeystoneIdentity {
    credentials: rustfs_credentials::Credentials,
    had_session_token: bool,
}

impl OwnedKeystoneIdentity {
    pub(crate) fn credentials(&self) -> &rustfs_credentials::Credentials {
        &self.credentials
    }
    pub(crate) fn had_session_token(&self) -> bool {
        self.had_session_token
    }
}

/// Why the gateway stack could not be assembled. Startup fails with it; the legacy stack is never
/// started in its place.
#[derive(Debug, thiserror::Error)]
pub(crate) enum GatewayAssemblyError {
    #[error("the root credentials cannot verify signatures on the gateway stack: {0}")]
    RootCredentials(RootCredentialsError),
    #[error("region {region:?} cannot be a signing region on the gateway stack")]
    Region { region: String },
    #[error("the server domains cannot be served by the gateway stack: {0:?}")]
    ServerDomains(rustfs_gateway::LegacyDomainError),
    #[error("the gateway service refused its assembly: {0:?}")]
    Service(AssemblyError),
    #[error("the native admin overlay has no server-info operation")]
    AdminOverlayMissing,
    #[error("the native admin dialect refused its assembly: {0:?}")]
    AdminDialect(Vec<rustfs_gateway_core::dialect::DialectError>),
    #[error("the metadata listing dialect refused its assembly: {0:?}")]
    MetadataListingDialect(Vec<rustfs_gateway_core::dialect::DialectError>),
    #[error("RUSTFS_KEYSTONE_ENABLE configuration is invalid: {0}")]
    KeystoneConfig(rustfs_keystone::KeystoneError),
    #[error("RUSTFS_KEYSTONE_ENABLE requires an initialized Keystone provider before the gateway stack is assembled")]
    KeystoneNotInitialized,
    #[error("the Keystone header claim is invalid")]
    KeystoneClaim,
}

/// The assembled gateway service.
#[derive(Clone)]
pub(crate) struct GatewayStack {
    service: rustfs_gateway::S3Service,
    keystone_auth: Option<Arc<rustfs_keystone::KeystoneAuthProvider>>,
    /// Whether this is the control-plane host, which reads each request's head before the gateway.
    control_plane: bool,
    /// Companion control-plane service without GET native admin routes, used for Object Lambda requests.
    object_lambda_fallback: Option<rustfs_gateway::S3Service>,
}

impl GatewayStack {
    /// Assembles the gateway stack for one server.
    ///
    /// # Errors
    ///
    /// [`GatewayAssemblyError`] naming the input the gateway refused. The secret key is never
    /// part of it.
    pub(crate) fn assemble(assembly: GatewayAssembly) -> Result<Self, GatewayAssemblyError> {
        let iam = Arc::new(ServerIam {
            server_ctx: Arc::clone(&assembly.server_ctx),
        });
        let server_ctx = Arc::clone(&assembly.server_ctx);
        let root = Arc::new(live_root(Arc::clone(&assembly.server_ctx)));
        Self::assemble_with(assembly, iam, root, early_gate(OPERATIONS, server_ctx))
    }

    /// Assembles the gateway service for the two signed server-info aliases.
    pub(crate) fn assemble_admin(assembly: GatewayAssembly) -> Result<Self, GatewayAssemblyError> {
        let iam = Arc::new(ServerIam {
            server_ctx: Arc::clone(&assembly.server_ctx),
        });
        let root = Arc::new(live_root(Arc::clone(&assembly.server_ctx)));
        admin::assemble(assembly, iam, root)
    }

    /// Assembles the control-plane host over `router`: every other route the admin router claims.
    pub(crate) fn assemble_control_plane(
        assembly: GatewayAssembly,
        router: Arc<crate::admin::router::S3Router<crate::admin::router::AdminOperation>>,
    ) -> Result<Self, GatewayAssemblyError> {
        let iam = Arc::new(ServerIam {
            server_ctx: Arc::clone(&assembly.server_ctx),
        });
        let root = Arc::new(live_root(Arc::clone(&assembly.server_ctx)));
        control_plane::assemble(assembly, iam, root, router)
    }

    /// The stack as [`GatewayStack::assemble`] configures it, admitting every caller to the
    /// handlers `register` adds instead of RustFS's: for tests of the host layers around it.
    #[cfg(test)]
    pub(crate) fn assemble_serving(
        assembly: GatewayAssembly,
        register: impl FnOnce(ServiceBuilder) -> ServiceBuilder,
    ) -> Result<Self, GatewayAssemblyError> {
        let iam = Arc::new(ServerIam {
            server_ctx: Arc::clone(&assembly.server_ctx),
        });
        let root = Arc::new(live_root(Arc::clone(&assembly.server_ctx)));
        let builder = Self::configured_builder(&assembly, iam, root, decide_with(|_| Decision::Allow), std::convert::identity)?;
        let service = register(builder).build().map_err(GatewayAssemblyError::Service)?;
        Ok(Self {
            service,
            keystone_auth: assembly.keystone_auth,
            control_plane: false,
            object_lambda_fallback: None,
        })
    }

    fn assemble_with(
        assembly: GatewayAssembly,
        iam: Arc<dyn IamKeyLookup>,
        root: Arc<dyn RootSource>,
        authorizer: impl Authorizer,
    ) -> Result<Self, GatewayAssemblyError> {
        let listings = metadata_listing::dialect().map_err(GatewayAssemblyError::MetadataListingDialect)?;
        let builder = Self::configured_builder(&assembly, iam, root, authorizer, std::convert::identity)?
            .answer_nul_torrent_keys_as_missing()
            .dialect(&listings);
        let backend = Arc::new(GatewayBackend::new(Arc::clone(&assembly.server_ctx)));
        let builder = metadata_listing::register(register(OPERATIONS, builder, &backend), &assembly.server_ctx);
        let service = builder.build().map_err(GatewayAssemblyError::Service)?;
        Ok(Self {
            service,
            keystone_auth: assembly.keystone_auth,
            control_plane: false,
            object_lambda_fallback: None,
        })
    }

    fn configured_builder(
        assembly: &GatewayAssembly,
        iam: Arc<dyn IamKeyLookup>,
        root: Arc<dyn RootSource>,
        authorizer: impl Authorizer,
        authenticator: impl FnOnce(SigV4Authenticator) -> SigV4Authenticator,
    ) -> Result<ServiceBuilder, GatewayAssemblyError> {
        // A root the gateway could not verify a signature with is a startup error, never a
        // server that silently has no root. Requests read the live root from `root`.
        RootCredentials::new(&assembly.root_access_key, &assembly.root_secret_key, &assembly.root_secret_key)
            .map_err(GatewayAssemblyError::RootCredentials)?;
        // `RUSTFS_REGION` may list several regions; a request may be signed for any of them.
        let region = assembly.region.as_deref().unwrap_or(RUSTFS_REGION);
        let regions = RegionSet::new(region.split(',').map(str::trim)).map_err(|_| GatewayAssemblyError::Region {
            region: region.to_owned(),
        })?;
        let hosts = rustfs_host_resolver(&assembly.server_domains).map_err(GatewayAssemblyError::ServerDomains)?;
        // Every legacy RustFS reading of a request, a credential scope and an answer, in the
        // gateway's one reviewable preset; its other half is on the authenticator.
        let credentials = Arc::new(IamCredentials::new(iam, root));
        let builder = ServiceBuilder::new()
            .rustfs_profile()
            // Shipped RustFS authorizes before it judges an SSE header; the handler entry answers
            // the refusal once authorization succeeds.
            .defer_encryption_header_refusals_to_handlers()
            .authenticator(authenticator(SigV4Authenticator::new(credentials, regions).rustfs_profile()))
            .authorizer(authorizer)
            .authz_audit(indeterminate_audit::IndeterminateAudit)
            .observer(indeterminate_audit::IndeterminateAudit)
            .bucket_owner_source(ReportedOwner(owner().id.map(Arc::from)))
            // Both gates follow the deployment setting; transport facts remain per request.
            .sse_config(if require_tls() {
                SseConfig::strict()
            } else {
                SseConfig::allowing_customer_keys_over_plaintext(
                    PlaintextCustomerKeyAck::i_understand_customer_keys_will_be_sent_in_the_clear(),
                )
            })
            .host_resolver(hosts)
            // The fallback only: `attach_host_facts` hands RustFS's own identifier over.
            .trace_source(MintedTraces::with_uuid_request_ids());
        let builder = if assembly.keystone_auth.is_some() {
            builder.host_verified_header_token(
                keystone_header_token_claim().map_err(|_| GatewayAssemblyError::KeystoneClaim)?,
                KeystoneVerifier,
            )
        } else {
            builder
        };
        let (builder, _settings) = builder.config(service_config(assembly.body_read_timeout, DEFAULT_MAX_BUFFERED_BODY_BYTES));
        Ok(builder)
    }

    /// Answers one request on the gateway stack, with the host's facts attached.
    pub(crate) fn call<B>(&self, mut request: Request<B>) -> impl Future<Output = Response<rustfs_gateway::Body>> + Send + 'static
    where
        B: http_body::Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let unsigned_extension_refusal =
            crate::auth::reject_unsigned_extension_headers_on_signed_request(request.headers(), request.uri().query())
                .err()
                .map(|err| err.message().unwrap_or(crate::auth::UNSIGNED_HEADERS_MESSAGE).to_owned());
        if self.control_plane {
            control_plane::attach_head(&mut request);
        }
        let extensions = request.extensions_mut();
        extensions.remove::<KeystoneEstablished>();
        extensions.remove::<OwnedKeystoneIdentity>();
        let validation = extensions.remove::<rustfs_keystone::KeystoneRequestIdentity>();
        let valid = match (validation, self.keystone_auth.as_ref()) {
            (Some(validation), Some(provider)) => {
                validation
                    .take_for(provider)
                    .ok()
                    .is_some_and(|(proof, credentials, had_session_token)| {
                        if establish_keystone_identity(extensions, proof).is_err() {
                            return false;
                        }
                        extensions.insert(OwnedKeystoneIdentity {
                            credentials,
                            had_session_token,
                        });
                        true
                    })
            }
            (Some(_), None) => false,
            (None, _) => true,
        };
        attach_host_facts(extensions);
        let refusal_request_id = unsigned_extension_refusal.as_ref().map(|_| {
            extensions
                .get::<RequestContext>()
                .and_then(host_request_id)
                .map(|id| id.request_id().as_str().to_owned())
                .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string())
        });
        // `Connection` is an HTTP/1 header; HTTP/2 and HTTP/3 forbid it and end only the stream.
        let http1 = request.version() < http::Version::HTTP_2;
        let is_head = request.method() == http::Method::HEAD;
        let service = match self.object_lambda_fallback.as_ref() {
            Some(fallback) if crate::admin::router::object_lambda_target(request.method(), request.uri()).is_some() => {
                fallback.clone()
            }
            _ => self.service.clone(),
        };
        async move {
            if !valid {
                let mut response = Response::new(rustfs_gateway::Body::empty());
                *response.status_mut() = http::StatusCode::FORBIDDEN;
                return response;
            }
            if let Some(message) = unsigned_extension_refusal {
                let request_id = refusal_request_id.unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
                return unsigned_extension_header_response(&message, &request_id, is_head, http1);
            }
            // The service's own entry, not its tower adapter: the adapter reads a body the handler
            // left unread in a detached task of its own, which takes the body away from
            // `EarlyResponseBodyService`, the drain the HTTP/1-2 shutdown accounting owns. The
            // adapter's connection verdict is kept.
            let mut response = service.call(request).await;
            if http1 && connection_intent_of(&response).is_some_and(ConnectionIntent::must_close) {
                response
                    .headers_mut()
                    .insert(http::header::CONNECTION, http::HeaderValue::from_static("close"));
            }
            response
        }
    }
}

/// Renders an S3 `403 AccessDenied` refusal matching the gateway's header guard format when a
/// signed request carries an unsigned `x-rustfs-*` or `x-minio-*` extension header.
fn unsigned_extension_header_response(
    message: &str,
    request_id: &str,
    is_head: bool,
    http1: bool,
) -> Response<rustfs_gateway::Body> {
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>AccessDenied</Code><Message>{message}</Message><RequestId>{request_id}</RequestId></Error>"
    );
    let content_length = xml.len();
    let body = if is_head {
        rustfs_gateway::Body::empty()
    } else {
        rustfs_gateway::Body::from_bytes(bytes::Bytes::from(xml))
    };
    let mut response = Response::new(body);
    *response.status_mut() = http::StatusCode::FORBIDDEN;
    let headers = response.headers_mut();
    headers.insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/xml"));
    headers.insert(http::header::CONTENT_LENGTH, http::HeaderValue::from(content_length));
    if let Ok(value) = http::HeaderValue::from_str(request_id) {
        headers.insert(http::header::HeaderName::from_static("x-amz-request-id"), value);
    }
    if http1 {
        headers.insert(http::header::CONNECTION, http::HeaderValue::from_static("close"));
    }
    response
}

/// The gateway stack over a server context, with requests signed by a root it publishes on that
/// context: for tests that drive an operation through the wire codec, authentication and
/// authorization instead of calling its use case.
#[cfg(test)]
pub(crate) struct TestGateway(GatewayStack);

#[cfg(test)]
impl TestGateway {
    pub(crate) const ACCESS_KEY: &str = "gatewaytestroot";
    pub(crate) const SECRET_KEY: &str = "gateway-test-root-secret";

    pub(crate) async fn call(&self, request: Request<rustfs_gateway::Body>) -> Response<rustfs_gateway::Body> {
        self.0.call(request).await
    }

    /// Publishes the root on `context` and assembles the stack over `slot`, which holds `context`.
    pub(crate) fn over(context: &crate::runtime_sources::AppContext, slot: Arc<ServerContextSlot>) -> Self {
        assert!(
            context.publish_action_credentials(rustfs_credentials::Credentials {
                access_key: Self::ACCESS_KEY.to_owned(),
                secret_key: Self::SECRET_KEY.to_owned(),
                status: "on".to_owned(),
                ..Default::default()
            }),
            "the test context has no root yet"
        );
        let stack = GatewayStack::assemble(GatewayAssembly {
            server_ctx: slot,
            root_access_key: Self::ACCESS_KEY.to_owned(),
            root_secret_key: Self::SECRET_KEY.to_owned(),
            region: None,
            server_domains: Vec::new(),
            body_read_timeout: Duration::from_secs(300),
            keystone_auth: None,
        })
        .expect("the gateway stack assembles");
        Self(stack)
    }

    /// `method target` with `headers` and `body`, header-signed by the root over an unsigned
    /// payload. A `Result`, so a call site reads as a service call does.
    pub(crate) async fn signed(
        &self,
        method: http::Method,
        target: &str,
        headers: Vec<(&str, String)>,
        body: bytes::Bytes,
    ) -> Result<Response<rustfs_gateway::Body>, std::convert::Infallible> {
        use crate::server::gateway_handlers::test_support::{Signed, signed_request_with_headers};
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        // The signer takes the canonical spelling of a subresource, `uploads=` for `uploads`.
        let query = query
            .split('&')
            .filter(|pair| !pair.is_empty())
            .map(|pair| {
                if pair.contains('=') {
                    pair.to_owned()
                } else {
                    format!("{pair}=")
                }
            })
            .collect::<Vec<_>>()
            .join("&");
        let mut signed_headers = http::HeaderMap::new();
        // The length is the body's own, set below; the signer does not sign it.
        for (name, value) in headers
            .into_iter()
            .filter(|(name, _)| !name.eq_ignore_ascii_case("content-length"))
        {
            signed_headers.append(
                http::HeaderName::from_bytes(name.as_bytes()).expect("a header name"),
                value.parse().expect("a header value"),
            );
        }
        let request =
            signed_request_with_headers(Signed::Header, method, path, &query, Self::ACCESS_KEY, Self::SECRET_KEY, signed_headers);
        let mut request = request.map(|_| http_body_util::Full::new(body.clone()));
        request
            .headers_mut()
            .insert(http::header::CONTENT_LENGTH, http::HeaderValue::from(body.len()));
        Ok(self.0.call(request).await)
    }
}

/// The early gate over `table` and the two metadata listings. An operation the table does not
/// serve is denied. A served operation is decided by [`crate::server::gateway_authorizer`], which
/// may deny only what the handler's legacy hook would deny.
fn early_gate(table: OperationTable, server_ctx: Arc<ServerContextSlot>) -> impl Authorizer {
    crate::server::gateway_authorizer::early_gate(table, server_ctx)
}

/// The gateway's request settings for RustFS, every deadline stated instead of inherited.
///
/// No handler runs under a total deadline, object data included: the legacy stack has none, and a
/// large transfer must not be cut off for its duration. A committed answer is not bounded either.
/// The request body keeps RustFS's own idle bound and no throughput floor, and at most
/// `max_buffered_body_bytes` of it is held.
fn service_config(body_read_timeout: Duration, max_buffered_body_bytes: u64) -> ServiceConfig {
    let handlers = HandlerDeadlineConfig::default()
        .without_deadline(HandlerDeadlineClass::Standard)
        .without_deadline(HandlerDeadlineClass::Extended)
        .without_commit_progress_deadline();
    ServiceConfig::new(max_buffered_body_bytes)
        .with_handler_deadlines(handlers)
        .with_request_body_deadlines(body_deadlines(body_read_timeout))
}

/// RustFS's body read timeout as the gateway's idle deadlines: the same silence is allowed before
/// the first byte and between reads, and zero lifts both.
fn body_deadlines(read_timeout: Duration) -> RequestBodyDeadlineConfig {
    RequestBodyDeadlineConfig::new(read_timeout, read_timeout)
        .unwrap_or(RequestBodyDeadlineConfig::S3.without_idle_deadlines())
        .without_throughput_floor()
}

/// Attaches what the gateway reads about a request from its host: whether the transport was
/// secure, the client address after RustFS's trusted-proxy correction, and RustFS's request
/// identifier. Each is derived from what RustFS's own layers already attached; no header is read.
fn attach_host_facts(extensions: &mut Extensions) {
    let client_info = extensions.get::<ClientInfo>();
    let native_tls = extensions.get::<NativeTlsTransport>().is_some_and(|transport| transport.0);
    let security = if is_secure_transport(native_tls, client_info) {
        TransportSecurity::Encrypted
    } else {
        TransportSecurity::Plaintext
    };
    let client_ip = client_info.map(|info| info.real_ip).or_else(|| {
        extensions
            .get::<Option<RemoteAddr>>()
            .and_then(|peer| peer.as_ref().map(|peer| peer.0.ip()))
    });
    // An identifier outside the gateway's alphabet is not handed over; the gateway mints its own.
    let request_id = extensions.get::<RequestContext>().and_then(host_request_id);

    extensions.insert(security);
    if let Some(client_ip) = client_ip {
        extensions.insert(ClientAddr::from_peer(client_ip));
    }
    if let Some(request_id) = request_id {
        extensions.insert(request_id);
    }
}

/// The identifier the outer layer assigned is the one the gateway is handed.
impl RequestIdCarrier for RequestContext {
    fn request_id(&self) -> &str {
        &self.request_id
    }
}

/// The root credentials of one server as they are now, read on every call from where the legacy
/// stack reads them, so a root change reaches both stacks at once. `None` while they are not
/// initialized or cannot verify a signature, which the adapters answer as unavailable.
fn live_root(server_ctx: Arc<ServerContextSlot>) -> impl Fn() -> Option<Arc<RootCredentials>> + Send + Sync + 'static {
    move || {
        let root = match server_ctx.app_context() {
            Some(context) => context.action_credentials().get(),
            None => current_action_credentials(),
        }?;
        // IAM signs STS session tokens with its token-signing key, which is the root secret
        // until IAM has published one of its own.
        let signing_key = get_token_signing_key().unwrap_or_else(|| root.secret_key.clone());
        RootCredentials::new(&root.access_key, &root.secret_key, &signing_key)
            .ok()
            .map(Arc::new)
    }
}

/// The owner id RustFS reports, answered for every bucket. Without one, every owner check refuses.
struct ReportedOwner(Option<Arc<str>>);

impl BucketOwnerSource for ReportedOwner {
    fn owner<'a>(&'a self, _bucket: &'a BucketName) -> BoxFuture<'a, Result<Arc<str>, BucketOwnerError>> {
        let answer = self.0.clone().ok_or_else(BucketOwnerError::unavailable);
        Box::pin(async move { answer })
    }
}

/// The IAM system of one server, looked up when a credential is: IAM is installed after the
/// listener starts, so the assembly cannot hold a handle.
struct ServerIam {
    server_ctx: Arc<ServerContextSlot>,
}

impl ServerIam {
    fn ready(&self) -> Option<Arc<IamSys<ObjectStore>>> {
        self.server_ctx
            .app_context()
            .filter(|context| context.iam().is_ready())
            .map(|context| context.iam().handle())
    }
}

impl IamKeyLookup for ServerIam {
    fn is_ready(&self) -> bool {
        self.ready().is_some()
    }

    fn check_key<'a>(&'a self, access_key: &'a str) -> BoxFuture<'a, Result<(Option<UserIdentity>, bool), IamError>> {
        Box::pin(async move {
            let Some(iam) = self.ready() else {
                return Err(IamError::IamSysNotInitialized);
            };
            iam.check_key(access_key).await
        })
    }
}

#[cfg(test)]
mod snowball_tests {
    use super::*;
    use crate::app::storage_api::error::is_err_object_not_found;
    use crate::app::storage_api::test::StorageObjectOptions;
    use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
    use crate::app::storage_api::test::contract::object::ObjectOperations as _;
    use bytes::Bytes;
    use http_body_util::BodyExt as _;
    use rustfs_gateway_stream::Body;
    use tokio_tar::Header;
    use uuid::Uuid;

    #[tokio::test]
    #[serial_test::serial]
    async fn snowball_extract_preserves_sha256_failure_after_sniffing() {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        if crate::runtime_sources::current_app_context().is_none() {
            crate::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let ambient = crate::runtime_sources::current_app_context().expect("Snowball gateway test requires an AppContext");
        let context = Arc::new(crate::runtime_sources::AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        let bucket = format!("extract-body-error-{}", Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create extract test bucket");
        let mut header = Header::new_gnu();
        header.set_path("member.txt").expect("valid member path");
        header.set_size(0);
        header.set_mode(0o644);
        header.set_cksum();
        let mut archive = header.as_bytes().to_vec();
        archive.extend_from_slice(&[0; 1024]);
        let archive = Bytes::from(archive);
        let slot = ServerContextSlot::new();
        assert!(slot.install(Arc::clone(&context)));
        let gateway = TestGateway::over(&context, slot);
        for valid in [false, true] {
            let digest = if valid {
                rustfs_utils::crypto::hex_sha256(&archive, str::to_owned)
            } else {
                "00".repeat(32)
            };
            // The first frame supplies the sniff prefix. The gateway compares the signed
            // payload digest when it reaches the end of the second frame.
            let body = Body::from_segments([archive.slice(..16), archive.slice(16..)]);
            let request = http::Request::put(format!("http://localhost/{bucket}/archive.tar"))
                .header(http::header::HOST, "localhost")
                .header(http::header::CONTENT_LENGTH, archive.len())
                .header("x-amz-meta-snowball-auto-extract", "true")
                .header("x-amz-content-sha256", digest)
                .body(body)
                .expect("build Snowball upload");
            let request = rustfs_signer::try_sign_v4(
                request,
                i64::try_from(archive.len()).expect("archive length fits i64"),
                TestGateway::ACCESS_KEY,
                TestGateway::SECRET_KEY,
                "",
                "us-east-1",
            )
            .expect("sign Snowball upload");
            let response = gateway.call(request).await;
            let status = response.status();
            let response = response
                .into_body()
                .collect()
                .await
                .expect("collect Snowball response")
                .to_bytes();
            if valid {
                assert_eq!(status, http::StatusCode::OK, "{response:?}");
                assert_eq!(
                    store
                        .get_object_info(&bucket, "member.txt", &StorageObjectOptions::default())
                        .await
                        .expect("matching digest must extract the member")
                        .size,
                    0
                );
            } else {
                assert_eq!(status, http::StatusCode::BAD_REQUEST, "{response:?}");
                assert!(
                    std::str::from_utf8(&response)
                        .expect("S3 error response is UTF-8")
                        .contains("<Code>XAmzContentSHA256Mismatch</Code>"),
                    "{response:?}"
                );
                let missing = store
                    .get_object_info(&bucket, "member.txt", &StorageObjectOptions::default())
                    .await
                    .expect_err("rejected archive cannot publish a member");
                assert!(is_err_object_not_found(&missing), "{missing}");
            }
            let missing = store
                .get_object_info(&bucket, "archive.tar", &StorageObjectOptions::default())
                .await
                .expect_err("the archive itself must not be stored");
            assert!(is_err_object_not_found(&missing), "{missing}");
        }
    }
}

#[cfg(test)]
mod object_lock_tests {
    use super::*;
    use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
    use bytes::Bytes;
    use http::{Method, StatusCode};
    use http_body_util::BodyExt as _;
    use md5::Digest as _;
    use rustfs_gateway_types::persistence::parse_object_lock;
    use uuid::Uuid;

    #[tokio::test]
    #[serial_test::serial]
    async fn object_lock_round_trips_the_default_retention() {
        let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let context = Arc::new(crate::runtime_sources::AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        let slot = ServerContextSlot::new();
        assert!(slot.install(Arc::clone(&context)));
        let gateway = TestGateway::over(&context, slot);
        let bucket = format!("lock-config-{}", Uuid::new_v4());
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    lock_enabled: true,
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create object-lock bucket");

        let xml = b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
            <Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>30</Days></DefaultRetention></Rule></ObjectLockConfiguration>";
        let target = format!("/{bucket}?object-lock");
        let checksum = base64_simd::STANDARD.encode_to_string(md5::Md5::digest(xml));
        let put = gateway
            .signed(Method::PUT, &target, vec![("content-md5", checksum)], Bytes::from_static(xml))
            .await
            .expect("PUT Object Lock configuration");
        let put_status = put.status();
        let put_body = put
            .into_body()
            .collect()
            .await
            .expect("collect PUT Object Lock response")
            .to_bytes();
        assert_eq!(put_status, StatusCode::OK, "{}", String::from_utf8_lossy(&put_body));

        let (stored, _) = crate::storage_api::server::gateway_stack::get_object_lock_config(&bucket)
            .await
            .expect("stored lock config");
        assert_eq!(stored.object_lock_enabled.as_deref(), Some("Enabled"));

        let get = gateway
            .signed(Method::GET, &target, vec![], Bytes::new())
            .await
            .expect("GET Object Lock configuration");
        assert_eq!(get.status(), StatusCode::OK);
        let body = get
            .into_body()
            .collect()
            .await
            .expect("collect GET Object Lock response")
            .to_bytes();
        let returned = parse_object_lock(&body).expect("get lock configuration");
        assert_eq!(returned, stored);
        let retention = returned
            .rule
            .expect("stored Object Lock rule")
            .default_retention
            .expect("stored default retention");
        assert_eq!(retention.mode.as_deref(), Some("GOVERNANCE"));
        assert_eq!(retention.days, Some(30));
        assert_eq!(retention.years, None);
    }
}

#[cfg(test)]
mod tests;
