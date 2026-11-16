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

//! Bridges signed system-status reads to their owned admin handlers.
//! It owns neither IAM policy decisions nor other admin routes. Upstream: the edge selector;
//! downstream: the gateway signature verifier and the real admin handlers.

use super::{GatewayAssembly, GatewayAssemblyError, GatewayStack};
use crate::admin::handlers::module_switch::GetModuleSwitchesHandler;
use crate::admin::handlers::object_data_cache::ObjectDataCacheStatsHandler;
use crate::admin::handlers::plugins_catalog::GetPluginCatalogHandler;
use crate::admin::handlers::system::{DataUsageInfoHandler, RuntimeCapabilitiesHandler, ServerInfoHandler, StorageInfoHandler};
use crate::admin::operation::{AdminRequest, AdminResponse, Operation as AdminHandler};
use crate::app::RequestEnvelope;
use crate::runtime_sources::ServerContextSlot;
use crate::server::gateway_handlers::{handler_error, verified_signature};
use crate::storage_api::server::http::access::ReqInfo;
use http_body_util::BodyExt as _;
use rustfs_gateway::{Decision, Handler, HandlerResult, Operation, Req, RequestContextView, Resp, decide_with};
use rustfs_gateway_adapter::{IamKeyLookup, RootSource};
use rustfs_gateway_core::dialect::{ClaimedRoute, Dialect, DialectBuilder, DialectOverlay, OverlayRow};
use rustfs_gateway_dialect_rustfs_admin::ops::{
    get_v3_datausageinfo::GetV3Datausageinfo, get_v3_info::GetV3Info, get_v3_module_switches::GetV3ModuleSwitches,
    get_v3_object_data_cache_stats::GetV3ObjectDataCacheStats, get_v3_storageinfo::GetV3Storageinfo,
    get_v4_plugins_catalog::GetV4PluginsCatalog, get_v4_runtime_capabilities::GetV4RuntimeCapabilities,
};
use rustfs_gateway_dialect_rustfs_admin::{AdminOperation, AdminResponse as WireResponse, CLAIMS, OVERLAY};
use rustfs_s3_types::{Body, S3Result, s3_error};
use std::sync::{Arc, OnceLock};

const OPERATIONS: [&str; 7] = [
    GetV3Info::NAME,
    GetV3Datausageinfo::NAME,
    GetV4PluginsCatalog::NAME,
    GetV3ModuleSwitches::NAME,
    GetV3Storageinfo::NAME,
    GetV4RuntimeCapabilities::NAME,
    GetV3ObjectDataCacheStats::NAME,
];

fn declare<O: AdminOperation>(builder: DialectBuilder) -> DialectBuilder {
    builder.declare_claimed::<O>(ClaimedRoute {
        precedence: O::PRECEDENCE,
        rows: O::rows(),
        shadows: &[],
        bucket_param: O::BUCKET,
    })
}

pub(super) fn dialect() -> Result<Dialect, GatewayAssemblyError> {
    static ROWS: OnceLock<Vec<OverlayRow>> = OnceLock::new();
    static OVERLAY_READS: OnceLock<DialectOverlay> = OnceLock::new();
    let rows = OPERATIONS
        .iter()
        .map(|name| OVERLAY.row(name).copied().ok_or(GatewayAssemblyError::AdminOverlayMissing))
        .collect::<Result<Vec<_>, _>>()?;
    let overlay = OVERLAY_READS.get_or_init(|| DialectOverlay {
        name: "rustfs-system-reads",
        vendor: OVERLAY.vendor,
        claims: &CLAIMS[..2],
        operations: ROWS.get_or_init(|| rows),
    });
    let builder = declare::<GetV3Info>(Dialect::assemble(overlay));
    let builder = declare::<GetV3Storageinfo>(builder);
    let builder = declare::<GetV4RuntimeCapabilities>(builder);
    let builder = declare::<GetV3ObjectDataCacheStats>(builder);
    let builder = declare::<GetV3Datausageinfo>(builder);
    let builder = declare::<GetV4PluginsCatalog>(builder);
    declare::<GetV3ModuleSwitches>(builder)
        .build()
        .map_err(GatewayAssemblyError::AdminDialect)
}

pub(super) fn assemble(
    assembly: GatewayAssembly,
    iam: Arc<dyn IamKeyLookup>,
    root: Arc<dyn RootSource>,
) -> Result<GatewayStack, GatewayAssemblyError> {
    let gate = decide_with(|request| {
        if OPERATIONS.contains(&request.operation) {
            Decision::Allow
        } else {
            Decision::Deny
        }
    });
    let builder = GatewayStack::configured_builder(&assembly, iam, root, gate)?;
    let service = builder
        .dialect(&dialect()?)
        .register::<GetV3Info, _>(Arc::new(AdminBridge {
            server_ctx: assembly.server_ctx.clone(),
            handler: &ServerInfoHandler {},
        }))
        .register::<GetV3Storageinfo, _>(Arc::new(AdminBridge {
            server_ctx: assembly.server_ctx.clone(),
            handler: &StorageInfoHandler {},
        }))
        .register::<GetV4RuntimeCapabilities, _>(Arc::new(AdminBridge {
            server_ctx: assembly.server_ctx.clone(),
            handler: &RuntimeCapabilitiesHandler {},
        }))
        .register::<GetV3ObjectDataCacheStats, _>(Arc::new(AdminBridge {
            server_ctx: assembly.server_ctx.clone(),
            handler: &ObjectDataCacheStatsHandler {},
        }))
        .register::<GetV3Datausageinfo, _>(Arc::new(AdminBridge {
            server_ctx: assembly.server_ctx.clone(),
            handler: &DataUsageInfoHandler {},
        }))
        .register::<GetV4PluginsCatalog, _>(Arc::new(AdminBridge {
            server_ctx: assembly.server_ctx.clone(),
            handler: &GetPluginCatalogHandler {},
        }))
        .register::<GetV3ModuleSwitches, _>(Arc::new(AdminBridge {
            server_ctx: assembly.server_ctx,
            handler: &GetModuleSwitchesHandler {},
        }))
        .build()
        .map_err(GatewayAssemblyError::Service)?;
    Ok(GatewayStack {
        service,
        keystone_auth: assembly.keystone_auth,
    })
}

struct AdminBridge {
    handler: &'static dyn AdminHandler,
    server_ctx: Arc<ServerContextSlot>,
}

pub(super) fn envelope(context: &RequestContextView, server_ctx: &Arc<ServerContextSlot>) -> S3Result<RequestEnvelope> {
    let mut envelope = RequestEnvelope::from_gateway(context, server_ctx, verified_signature(context))?;
    if let Some(info) = context.transport_extensions().get::<ReqInfo>() {
        envelope.install_req_info(info.clone());
    }
    Ok(envelope)
}

impl<O: Operation<Output = WireResponse>> Handler<O> for AdminBridge {
    async fn call(&self, request: Req<O>) -> HandlerResult<O> {
        if self.server_ctx.installed_app_context().is_none() {
            return Err(handler_error(s3_error!(ServiceUnavailable, "server context is not ready")));
        }
        let envelope = envelope(request.context(), &self.server_ctx).map_err(handler_error)?;
        let response = self
            .handler
            .call(AdminRequest::new(envelope, Body::empty()), matchit::Params::new())
            .await
            .map_err(handler_error)?;
        complete_response(response).await
    }
}

// These handlers construct complete JSON in memory. This bridge does not serve streams.
pub(super) async fn complete_response<O: Operation<Output = WireResponse>>(response: AdminResponse) -> HandlerResult<O> {
    let (status, mut headers, body) = response.into_parts();
    let bytes = body
        .collect()
        .await
        .map_err(|_| handler_error(s3_error!(InternalError, "failed to read admin response")))?
        .to_bytes();
    headers.remove(http::header::CONTENT_LENGTH);
    Ok(Resp::with_status(WireResponse::bytes("", bytes.to_vec()), status.as_u16()).with_extra_headers(headers))
}
