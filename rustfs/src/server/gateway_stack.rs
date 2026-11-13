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
//! gateway's own strict gate stays behind it; and the request identifier is the one
//! `ExternalRequestContextLayer` assigned, handed over as the gateway's `HostRequestId`, so an
//! answer and RustFS's logs name a request identically.
//!
//! # Authorization
//!
//! The gateway's `Authorizer` is only the early gate: it allows an operation the table serves and
//! denies every other. Every authorization decision, an anonymous caller's included, is the
//! handler's, made by the function the legacy access hook calls (`server/gateway_handlers`), which
//! also writes its denial audit record. No policy source and no authorization audit sink is
//! installed: nothing reads a policy snapshot, and the gate's only refusal is an operation without
//! a handler, which the gateway answers `501 NotImplemented`.

// The stack is under construction, so a build without debug assertions must not carry it. The
// message is checked by `scripts/check_connect_release_boundary.py`.
#[cfg(not(debug_assertions))]
compile_error!("gateway-stack is restricted to debug builds");

use crate::runtime_sources::{ServerContextSlot, current_action_credentials};
use crate::server::gateway_handlers::{GatewayBackend, OPERATIONS, OperationTable, register, serves};
use crate::server::ssec_transport::is_secure_transport;
use crate::shared_types::{NativeTlsTransport, RemoteAddr};
use crate::storage_api::server::http::request_context::RequestContext;
use http::{Extensions, Request, Response};
use rustfs_config::RUSTFS_REGION;
use rustfs_gateway::{
    AssemblyError, Authorizer, BoxFuture, ClientAddr, DEFAULT_MAX_BUFFERED_BODY_BYTES, Decision, HandlerDeadlineClass,
    HandlerDeadlineConfig, MintedTraces, RegionSet, RequestBodyDeadlineConfig, ServiceBuilder, ServiceConfig, SigV4Authenticator,
    SseConfig, TransportSecurity, decide_with,
};
use rustfs_gateway_adapter::{
    IamCredentials, IamKeyLookup, RequestIdCarrier, RootCredentials, RootCredentialsError, RootSource, host_request_id,
    rustfs_host_resolver,
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
    /// Whether Keystone authentication is enabled in configuration (`RUSTFS_KEYSTONE_ENABLE`).
    pub(crate) keystone_enabled: bool,
}

/// Whether Keystone authentication is switched on in configuration. A Keystone configuration
/// that cannot be read counts as switched on: only a readable configuration can say it is off.
pub(crate) fn keystone_enabled_in_configuration() -> bool {
    rustfs_keystone::KeystoneConfig::from_env().map_or(true, |config| config.enable)
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
    #[error(
        "RUSTFS_S3_STACK=gateway cannot be served while RUSTFS_KEYSTONE_ENABLE is on: the gateway admits a request that \
         carries only X-Auth-Token as anonymous while RustFS reads the Keystone identity behind it; Keystone support on the \
         gateway stack is rustfs/gateway#1586"
    )]
    KeystoneEnabled,
}

/// The assembled gateway service.
#[derive(Clone)]
pub(crate) struct GatewayStack {
    service: rustfs_gateway::S3Service,
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
        let root = Arc::new(live_root(Arc::clone(&assembly.server_ctx)));
        Self::assemble_with(assembly, iam, root, early_gate(OPERATIONS))
    }

    fn assemble_with(
        assembly: GatewayAssembly,
        iam: Arc<dyn IamKeyLookup>,
        root: Arc<dyn RootSource>,
        authorizer: impl Authorizer,
    ) -> Result<Self, GatewayAssemblyError> {
        // The pinned gateway admits a request that carries only `X-Auth-Token` as anonymous and
        // runs no authenticator for it, while the RustFS code behind the gateway reads the
        // Keystone identity: the request would be authorized as one caller and served as
        // another. Refused until the gateway authenticates the token (rustfs/gateway#1586).
        if assembly.keystone_enabled {
            return Err(GatewayAssemblyError::KeystoneEnabled);
        }
        // A root the gateway could not verify a signature with is a startup error, never a
        // server that silently has no root. Requests read the live root from `root`.
        RootCredentials::new(&assembly.root_access_key, &assembly.root_secret_key, &assembly.root_secret_key)
            .map_err(GatewayAssemblyError::RootCredentials)?;
        let region = assembly.region.as_deref().unwrap_or(RUSTFS_REGION);
        let regions = RegionSet::new([region]).map_err(|_| GatewayAssemblyError::Region {
            region: region.to_owned(),
        })?;
        let hosts = rustfs_host_resolver(&assembly.server_domains).map_err(GatewayAssemblyError::ServerDomains)?;
        let backend = Arc::new(GatewayBackend::new(Arc::clone(&assembly.server_ctx)));

        // Every legacy RustFS reading of a request, a credential scope and an answer, in the
        // gateway's one reviewable preset; its other half is on the authenticator.
        let credentials = Arc::new(IamCredentials::new(iam, root));
        let builder = ServiceBuilder::new()
            .rustfs_profile()
            .authenticator(SigV4Authenticator::new(credentials, regions).rustfs_profile())
            .authorizer(authorizer)
            // The outer SSE-C transport layer answers first; the gateway's own gate stays strict
            // behind it.
            .sse_config(SseConfig::strict())
            .host_resolver(hosts)
            // The fallback only: `attach_host_facts` hands RustFS's own identifier over.
            .trace_source(MintedTraces::with_uuid_request_ids());
        let (builder, _settings) = builder.config(service_config(assembly.body_read_timeout));
        let service = register(OPERATIONS, builder, &backend)
            .build()
            .map_err(GatewayAssemblyError::Service)?;
        Ok(Self { service })
    }

    /// Answers one request on the gateway stack, with the host's facts attached.
    pub(crate) fn call<B>(&self, mut request: Request<B>) -> impl Future<Output = Response<rustfs_gateway::Body>> + Send + 'static
    where
        B: http_body::Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        attach_host_facts(request.extensions_mut());
        let mut service = self.service.clone();
        let answer = tower::Service::call(&mut service, request);
        async move {
            match answer.await {
                Ok(response) => response,
                Err(never) => match never {},
            }
        }
    }
}

/// The early gate over `table`: an operation it serves is allowed on to its handler, which decides;
/// every other operation is denied.
fn early_gate(table: OperationTable) -> impl Authorizer {
    decide_with(move |request| {
        if serves(table, request.operation) {
            Decision::Allow
        } else {
            Decision::Deny
        }
    })
}

/// The gateway's request settings for RustFS, every deadline stated instead of inherited.
///
/// No handler runs under a total deadline, object data included: the legacy stack has none, and a
/// large transfer must not be cut off for its duration. A committed answer is not bounded either.
/// The request body keeps RustFS's own idle bound and no throughput floor.
fn service_config(body_read_timeout: Duration) -> ServiceConfig {
    let handlers = HandlerDeadlineConfig::default()
        .without_deadline(HandlerDeadlineClass::Standard)
        .without_deadline(HandlerDeadlineClass::Extended)
        .without_commit_progress_deadline();
    ServiceConfig::new(DEFAULT_MAX_BUFFERED_BODY_BYTES)
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
mod tests;
