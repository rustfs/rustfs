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

//! Bridges the two signed server-info aliases to the owned admin handler.
//! It owns neither IAM policy decisions nor other admin routes. Upstream: the edge selector;
//! downstream: the gateway signature verifier and the real ServerInfoHandler.

use super::{GatewayAssembly, GatewayAssemblyError, GatewayStack};
use crate::admin::handlers::system::ServerInfoHandler;
use crate::admin::operation::{AdminRequest, AdminResponse, Operation as _};
use crate::app::RequestEnvelope;
use crate::runtime_sources::ServerContextSlot;
use crate::server::gateway_handlers::{handler_error, verified_signature};
use crate::storage_api::server::http::access::ReqInfo;
use http_body_util::BodyExt as _;
use rustfs_gateway::{Decision, Handler, HandlerResult, Operation, Req, RequestContextView, Resp, decide_with};
use rustfs_gateway_adapter::{IamKeyLookup, RootSource};
use rustfs_gateway_core::dialect::{ClaimedRoute, Dialect, DialectOverlay};
use rustfs_gateway_dialect_rustfs_admin::ops::get_v3_info::GetV3Info;
use rustfs_gateway_dialect_rustfs_admin::{AdminOperation, AdminResponse as WireResponse, CLAIMS, OVERLAY};
use rustfs_s3_types::{Body, S3Result, s3_error};
use std::sync::{Arc, OnceLock};

pub(super) fn dialect() -> Result<Dialect, GatewayAssemblyError> {
    static OVERLAY_INFO: OnceLock<DialectOverlay> = OnceLock::new();
    let row = OVERLAY
        .row(GetV3Info::NAME)
        .ok_or(GatewayAssemblyError::AdminOverlayMissing)?;
    let overlay = OVERLAY_INFO.get_or_init(|| DialectOverlay {
        name: "rustfs-server-info",
        vendor: OVERLAY.vendor,
        claims: &CLAIMS[..2],
        operations: std::slice::from_ref(row),
    });
    Dialect::assemble(overlay)
        .declare_claimed::<GetV3Info>(ClaimedRoute {
            precedence: GetV3Info::PRECEDENCE,
            rows: GetV3Info::rows(),
            shadows: &[],
            bucket_param: GetV3Info::BUCKET,
        })
        .build()
        .map_err(GatewayAssemblyError::AdminDialect)
}

pub(super) fn assemble(
    assembly: GatewayAssembly,
    iam: Arc<dyn IamKeyLookup>,
    root: Arc<dyn RootSource>,
) -> Result<GatewayStack, GatewayAssemblyError> {
    let gate = decide_with(|request| {
        if request.operation == GetV3Info::NAME {
            Decision::Allow
        } else {
            Decision::Deny
        }
    });
    let builder = GatewayStack::configured_builder(&assembly, iam, root, gate)?;
    let service = builder
        .dialect(&dialect()?)
        .register::<GetV3Info, _>(Arc::new(ServerInfoBridge {
            server_ctx: assembly.server_ctx,
        }))
        .build()
        .map_err(GatewayAssemblyError::Service)?;
    Ok(GatewayStack {
        service,
        keystone_auth: assembly.keystone_auth,
    })
}

struct ServerInfoBridge {
    server_ctx: Arc<ServerContextSlot>,
}

pub(super) fn envelope(context: &RequestContextView, server_ctx: &Arc<ServerContextSlot>) -> S3Result<RequestEnvelope> {
    let mut envelope = RequestEnvelope::from_gateway(context, server_ctx, verified_signature(context))?;
    if let Some(info) = context.transport_extensions().get::<ReqInfo>() {
        envelope.install_req_info(info.clone());
    }
    Ok(envelope)
}

impl Handler<GetV3Info> for ServerInfoBridge {
    async fn call(&self, request: Req<GetV3Info>) -> HandlerResult<GetV3Info> {
        if self.server_ctx.installed_app_context().is_none() {
            return Err(handler_error(s3_error!(ServiceUnavailable, "server context is not ready")));
        }
        let envelope = envelope(request.context(), &self.server_ctx).map_err(handler_error)?;
        let response = ServerInfoHandler {}
            .call(AdminRequest::new(envelope, Body::empty()), matchit::Params::new())
            .await
            .map_err(handler_error)?;
        complete_response(response).await
    }
}

// ServerInfoHandler already constructs the complete JSON in memory. This bridge does not serve streams.
pub(super) async fn complete_response(response: AdminResponse) -> HandlerResult<GetV3Info> {
    let (status, mut headers, body) = response.into_parts();
    let bytes = body
        .collect()
        .await
        .map_err(|_| handler_error(s3_error!(InternalError, "failed to read server info response")))?
        .to_bytes();
    headers.remove(http::header::CONTENT_LENGTH);
    Ok(Resp::with_status(WireResponse::bytes("", bytes.to_vec()), status.as_u16()).with_extra_headers(headers))
}
