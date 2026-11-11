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
//! built for RustFS — the RustFS profile, signature verification against RustFS IAM, the RustFS
//! authorizer, explicit deadlines, and the registered handlers — and [`GatewayStack::call`],
//! which attaches the host's facts to a request before the gateway accepts it.
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

// The stack is under construction, so a build without debug assertions must not carry it. The
// message is checked by `scripts/check_gateway_stack_release_boundary.sh`.
#[cfg(not(debug_assertions))]
compile_error!("gateway-stack is restricted to debug builds");

mod authorizer;

use self::authorizer::{AUTHORIZATION_TABLE, RustfsAuthorizer, UnwiredDecisionCore};
use crate::runtime_sources::ServerContextSlot;
use crate::server::gateway_handlers::{GatewayBackend, register};
use crate::server::ssec_transport::is_secure_transport;
use crate::shared_types::{NativeTlsTransport, RemoteAddr};
use crate::storage_api::server::http::request_context::RequestContext;
use http::{Extensions, Request, Response};
use rustfs_config::RUSTFS_REGION;
use rustfs_gateway::{
    AssemblyError, Authorizer, BoxFuture, ClientAddr, CredentialsError, DEFAULT_MAX_BUFFERED_BODY_BYTES, HandlerDeadlineClass,
    HandlerDeadlineConfig, HostRequestId, LegacyRustfsVirtualHosts, MintedTraces, RegionSet, RequestBodyDeadlineConfig,
    ServiceBuilder, ServiceConfig, SigV4Authenticator, SseConfig, TransportSecurity,
};
use rustfs_gateway_adapter::{IamCredentials, IamKeyLookup};
use rustfs_iam::error::Error as IamError;
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
    pub(crate) root_access_key: String,
    pub(crate) root_secret_key: String,
    /// The configured region; the default region signs when none is configured.
    pub(crate) region: Option<String>,
    /// The virtual-hosted-style base domains of the S3 listener, as the legacy stack reads them.
    pub(crate) server_domains: Vec<String>,
    /// `RUSTFS_HTTP_REQUEST_BODY_READ_TIMEOUT`: the longest silence between two body reads. Zero
    /// disables the bound, as it does on the legacy stack.
    pub(crate) body_read_timeout: Duration,
}

/// Why the gateway stack could not be assembled. Startup fails with it; the legacy stack is never
/// started in its place.
#[derive(Debug, thiserror::Error)]
pub(crate) enum GatewayAssemblyError {
    #[error("the root access key cannot verify signatures on the gateway stack: {0:?}")]
    RootCredentials(CredentialsError),
    #[error("region {region:?} cannot be a signing region on the gateway stack")]
    Region { region: String },
    #[error("the server domains cannot be served by the gateway stack: {0:?}")]
    ServerDomains(rustfs_gateway::LegacyDomainError),
    #[error("the gateway service refused its assembly: {0:?}")]
    Service(AssemblyError),
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
        // WIRED BY rustfs/rustfs#9281: the owned authorization entry point replaces the unwired core.
        let authorizer = RustfsAuthorizer::new(AUTHORIZATION_TABLE, Arc::new(UnwiredDecisionCore));
        Self::assemble_with(assembly, iam, authorizer)
    }

    fn assemble_with(
        assembly: GatewayAssembly,
        iam: Arc<dyn IamKeyLookup>,
        authorizer: impl Authorizer,
    ) -> Result<Self, GatewayAssemblyError> {
        let credentials = IamCredentials::new(iam)
            .with_root(&assembly.root_access_key, assembly.root_secret_key.as_bytes())
            .map_err(GatewayAssemblyError::RootCredentials)?;
        let region = assembly.region.as_deref().unwrap_or(RUSTFS_REGION);
        let regions = RegionSet::new([region]).map_err(|_| GatewayAssemblyError::Region {
            region: region.to_owned(),
        })?;
        let hosts = LegacyRustfsVirtualHosts::new(&assembly.server_domains).map_err(GatewayAssemblyError::ServerDomains)?;
        let backend = Arc::new(GatewayBackend::new(Arc::clone(&assembly.server_ctx)));

        let builder = ServiceBuilder::new()
            // Every legacy RustFS reading of a request, a credential scope and an answer, in the
            // gateway's one reviewable preset; its other half is on the authenticator below.
            .rustfs_profile()
            .authenticator(SigV4Authenticator::new(Arc::new(credentials), regions).rustfs_profile())
            .authorizer(authorizer)
            // The outer SSE-C transport layer answers first; the gateway's own gate stays strict
            // behind it.
            .sse_config(SseConfig::strict())
            .host_resolver(hosts)
            // The fallback only: `attach_host_facts` hands RustFS's own identifier over.
            .trace_source(MintedTraces::with_uuid_request_ids());
        let (builder, _settings) = builder.config(service_config(assembly.body_read_timeout));
        let service = register(builder, &backend).build().map_err(GatewayAssemblyError::Service)?;
        Ok(Self { service })
    }

    /// The operations the assembled service answers, read from its registry. Everything else is
    /// answered `501 NotImplemented`.
    pub(crate) fn registered_operations(&self) -> Vec<&'static str> {
        self.service.operations().collect()
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
    let request_id = extensions
        .get::<RequestContext>()
        .and_then(|context| HostRequestId::new(&context.request_id).ok());

    extensions.insert(security);
    if let Some(client_ip) = client_ip {
        extensions.insert(ClientAddr::from_peer(client_ip));
    }
    if let Some(request_id) = request_id {
        extensions.insert(request_id);
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
