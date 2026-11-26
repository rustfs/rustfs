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

//! Independent S3-signed gateway host for reserved console paths when the console is disabled on
//! this listener (for example, the S3 listener while a dedicated console listener serves assets).
//!
//! Using `ServiceBuilder::for_service(SigService::S3)` with `Dialect::assemble_for_service` lets
//! both multi-segment defaults (`/rustfs/console`) and single-segment custom prefixes (`/ui`,
//! `/UI_assets`, `/favicon.ico`) bypass S3 bucket-name parsing while keeping original-URI SigV4
//! verification intact: anonymous requests receive `403 AccessDenied` and validly signed requests
//! receive `501 NotImplemented`, matching RustFS 1.0.5 (rustfs/backlog#2771).

use super::control_plane::ClaimedHead;
use super::{GatewayAssembly, GatewayAssemblyError, GatewayStack};
use crate::admin::router::admit_reserved_console;
use crate::runtime_sources::ServerContextSlot;
use crate::server::gateway_handlers::handler_error;
use crate::server::{APPLE_TOUCH_ICON_PATH, APPLE_TOUCH_ICON_PRECOMPOSED_PATH, FAVICON_PATH, console_prefix};
use bytes::Bytes;
use rustfs_gateway::sig::SigService;
use rustfs_gateway::{
    Authorizer, AuthzRequest, BoxFuture, Decision, Handler, HandlerError, HandlerResult, InputAuthzRequest, InputDecisions,
    Operation, OperationFloor, Req, RequestContext as AuthzContext, ServiceBuilder,
};
use rustfs_gateway_adapter::{IamKeyLookup, RootSource};
use rustfs_gateway_core::codec::{CodecError, EncodedResponse, MetaView, OperationCodec, RequestBody, RequestBodyMode};
use rustfs_gateway_core::dialect::{ClaimedRoute, ClaimedRow, Dialect, DialectOverlay, OverlayRow, render_claimed_route};
use rustfs_gateway_core::op::{AuthRequirement, ResourceShape};
use rustfs_gateway_core::registry::{HandlerDeadlineClass, OperationSpec};
use rustfs_gateway_core::route::PathClaim;
use rustfs_gateway_core::{DerivedResourceError, NoDerived};
use rustfs_gateway_dialect_rustfs_admin::{AdminResponse as WireResponse, OVERLAY, admin};
use rustfs_s3_types::s3_error;
use std::sync::{Arc, OnceLock};

const ROUTER: &str = "https://github.com/rustfs/rustfs/blob/main/rustfs/src/admin/router.rs";
const CLAIM_REASON: &str =
    "Reserved console paths on a listener without the native console verify SigV4 before returning 501 NotImplemented.";
const NAME: &str = "rustfs:ReservedConsole";
const CLAIMED_ROUTE_BODY_BYTES: u64 = 1024 * 1024;

static SPEC: OperationSpec = OperationSpec::builder(NAME, 200, None)
    .handler_deadline_class(HandlerDeadlineClass::Standard)
    .required_params(&[])
    .auth(AuthRequirement::new(NAME, ResourceShape::Service))
    .build();

/// Header-signed or presigned SigV4 only: anonymous and SigV2 requests are refused at the floor
/// with `403 AccessDenied`, matching legacy RustFS `S3Router::admit` on a console-disabled port.
static FLOOR: OperationFloor = OperationFloor::builtin_presigned(NAME, SigService::S3).require_sigv4_under_every_policy();

#[derive(Debug)]
struct ReservedConsole;

impl Operation for ReservedConsole {
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

impl OperationCodec for ReservedConsole {
    const REQUEST_BODY: RequestBodyMode = RequestBodyMode::Full;

    fn decode(_request: &MetaView<'_>, body: RequestBody) -> Result<Bytes, CodecError> {
        admin::buffered(body)
    }

    fn encode(output: WireResponse, _request: &MetaView<'_>, status: u16) -> Result<EncodedResponse, CodecError> {
        admin::encode(output, status)
    }
}

struct ReservedConsoleTable {
    dialect: Dialect,
}

fn build_dialect(prefix: &'static str) -> Result<Dialect, GatewayAssemblyError> {
    let slash: &'static str = Box::leak(format!("{prefix}/").into_boxed_str());
    let remainder: &'static str = Box::leak(format!("{prefix}/{{*remainder}}").into_boxed_str());
    let claims: &'static [PathClaim] = Box::leak(Box::new([
        PathClaim {
            prefix,
            reason: CLAIM_REASON,
            evidence: &[ROUTER],
        },
        PathClaim {
            prefix: FAVICON_PATH,
            reason: CLAIM_REASON,
            evidence: &[ROUTER],
        },
        PathClaim {
            prefix: APPLE_TOUCH_ICON_PATH,
            reason: CLAIM_REASON,
            evidence: &[ROUTER],
        },
        PathClaim {
            prefix: APPLE_TOUCH_ICON_PRECOMPOSED_PATH,
            reason: CLAIM_REASON,
            evidence: &[ROUTER],
        },
    ]));
    let rows: &'static [ClaimedRow] = Box::leak(Box::new([
        ClaimedRow {
            template: prefix,
            selector: &[],
        },
        ClaimedRow {
            template: slash,
            selector: &[],
        },
        ClaimedRow {
            template: remainder,
            selector: &[],
        },
        ClaimedRow {
            template: FAVICON_PATH,
            selector: &[],
        },
        ClaimedRow {
            template: APPLE_TOUCH_ICON_PATH,
            selector: &[],
        },
        ClaimedRow {
            template: APPLE_TOUCH_ICON_PRECOMPOSED_PATH,
            selector: &[],
        },
    ]));
    let route = ClaimedRoute {
        precedence: 65535,
        rows,
        shadows: &[],
        bucket_param: None,
    };
    let selector: &'static str = Box::leak(render_claimed_route(&route).into_boxed_str());
    let action: &'static str = Box::leak(SPEC.auth.map(|auth| auth.render()).unwrap_or_default().into_boxed_str());
    let operations: &'static [OverlayRow] = Box::leak(Box::new([OverlayRow {
        name: NAME,
        precedence: route.precedence,
        selector,
        action,
        resource: ResourceShape::Service,
        success_status: 200,
        anonymous: false,
        evidence: &[ROUTER],
    }]));
    let record: &'static DialectOverlay = Box::leak(Box::new(DialectOverlay {
        name: "rustfs-reserved-console",
        vendor: OVERLAY.vendor,
        claims,
        operations,
    }));
    Dialect::assemble_for_service(record, SigService::S3)
        .declare_claimed::<ReservedConsole>(route)
        .build()
        .map_err(GatewayAssemblyError::AdminDialect)
}

fn production_dialect() -> Result<&'static Dialect, GatewayAssemblyError> {
    static TABLE: OnceLock<Result<ReservedConsoleTable, GatewayAssemblyError>> = OnceLock::new();
    match TABLE.get_or_init(|| build_dialect(console_prefix()).map(|dialect| ReservedConsoleTable { dialect })) {
        Ok(table) => Ok(&table.dialect),
        Err(err) => Err(GatewayAssemblyError::AdminDialect(match err {
            GatewayAssemblyError::AdminDialect(errors) => errors.clone(),
            _ => Vec::new(),
        })),
    }
}

#[cfg(test)]
fn test_dialect_for_prefix(prefix: &'static str) -> Result<Dialect, GatewayAssemblyError> {
    use std::collections::HashMap;
    use std::sync::Mutex;

    static CACHE: OnceLock<Mutex<HashMap<&'static str, Dialect>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cache.lock().expect("reserved console dialect cache lock");
    if let Some(dialect) = guard.get(prefix) {
        return Ok(dialect.clone());
    }
    let dialect = build_dialect(prefix)?;
    guard.insert(prefix, dialect.clone());
    Ok(dialect)
}

struct Admission {
    server_ctx: Option<Arc<ServerContextSlot>>,
}

impl Admission {
    fn route(&self, context: &AuthzContext<'_>, request: &AuthzRequest<'_>) -> Result<Decision, HandlerError> {
        let head = context
            .server_extensions()
            .get::<ClaimedHead>()
            .ok_or_else(|| handler_error(s3_error!(InternalError, "the claimed request head was not attached")))?;
        let signed = request.identity.is_some() && !head.repeated_authorization;
        admit_reserved_console(self.server_ctx.as_deref(), &head.method, &head.uri, &head.headers, signed)
            .map_err(handler_error)?;
        if head.repeated_authorization {
            return Ok(Decision::Deny);
        }
        Ok(Decision::Allow)
    }
}

impl Authorizer for Admission {
    fn authorize_route<'a>(&'a self, context: &'a AuthzContext<'a>, request: &'a AuthzRequest<'a>) -> BoxFuture<'a, Decision> {
        let decision = self.route(context, request).unwrap_or(Decision::Deny);
        Box::pin(async move { decision })
    }

    fn authorize_route_result<'a>(
        &'a self,
        context: &'a AuthzContext<'a>,
        request: &'a AuthzRequest<'a>,
    ) -> BoxFuture<'a, Result<Decision, HandlerError>> {
        let result = self.route(context, request);
        Box::pin(async move { result })
    }

    fn authorize_input<'a>(
        &'a self,
        _context: &'a AuthzContext<'a>,
        request: &'a InputAuthzRequest<'a>,
    ) -> BoxFuture<'a, InputDecisions> {
        let stage = if request.route().identity.is_some() {
            Decision::Allow
        } else {
            Decision::Deny
        };
        let decisions = request.decide_all(stage, |_| stage);
        Box::pin(async move { decisions })
    }
}

struct NotImplementedHost;

impl Handler<ReservedConsole> for NotImplementedHost {
    async fn call(&self, _request: Req<ReservedConsole>) -> HandlerResult<ReservedConsole> {
        Err(handler_error(s3_error!(NotImplemented)))
    }
}

pub(super) fn assemble(
    assembly: GatewayAssembly,
    iam: Arc<dyn IamKeyLookup>,
    root: Arc<dyn RootSource>,
) -> Result<GatewayStack, GatewayAssemblyError> {
    let server_ctx = Some(Arc::clone(&assembly.server_ctx));
    let dialect = production_dialect()?;
    assemble_with_dialect(assembly, iam, root, dialect, server_ctx)
}

#[cfg(test)]
pub(super) fn assemble_for_prefix(
    assembly: GatewayAssembly,
    iam: Arc<dyn IamKeyLookup>,
    root: Arc<dyn RootSource>,
    prefix: &'static str,
    server_ctx: Option<Arc<ServerContextSlot>>,
) -> Result<GatewayStack, GatewayAssemblyError> {
    let dialect = test_dialect_for_prefix(prefix)?;
    assemble_with_dialect(assembly, iam, root, &dialect, server_ctx)
}

fn assemble_with_dialect(
    mut assembly: GatewayAssembly,
    iam: Arc<dyn IamKeyLookup>,
    root: Arc<dyn RootSource>,
    dialect: &Dialect,
    server_ctx: Option<Arc<ServerContextSlot>>,
) -> Result<GatewayStack, GatewayAssemblyError> {
    // Claimed console routes reject Keystone header tokens in `S3StackService` before reaching
    // this host, and independent SigV4-only services do not accept header-token authentication.
    assembly.keystone_auth = None;
    let gate = Admission { server_ctx };
    let (builder, _settings) = GatewayStack::configured_builder_on(
        ServiceBuilder::for_service(SigService::S3),
        &assembly,
        iam,
        root,
        gate,
        std::convert::identity,
    )?
    .config(super::service_config(assembly.body_read_timeout, CLAIMED_ROUTE_BODY_BYTES));
    let service = builder
        .dialect(dialect)
        .register::<ReservedConsole, _>(Arc::new(NotImplementedHost))
        .build()
        .map_err(GatewayAssemblyError::Service)?;
    Ok(GatewayStack {
        service,
        keystone_auth: None,
        control_plane: true,
        object_lambda_fallback: None,
    })
}
