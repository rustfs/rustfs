// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Assembles S3 Vectors with the server's live IAM and managed storage use case.

use super::{GatewayAssembly, GatewayAssemblyError, GatewayStack, ServerIam, live_root, service_config};
use crate::app::vector_usecase::DefaultVectorUsecase;
use crate::vector::{
    lance::ManifestIoLimits,
    protocol::{
        ProtocolError,
        gateway::{Backend, register, serves},
        gateway_error_bridge::{map_gateway_error, typed_error_filter},
    },
};
use rustfs_gateway::{Decision, MintedTraces, RegionSet, ServiceBuilder, SigService, SigV4Authenticator, decide_with};
use rustfs_gateway_adapter::{IamCredentials, RootCredentials};
use std::{num::NonZeroUsize, sync::Arc};

#[derive(Debug, thiserror::Error)]
pub(crate) enum VectorAssemblyError {
    #[error("Vector authentication assembly failed")]
    Authentication(#[from] GatewayAssemblyError),
    #[error("Vector application context is unavailable")]
    Application(#[from] ProtocolError),
    #[error("Vector dialect assembly failed: {0:?}")]
    Dialect(Vec<rustfs_gateway_core::DialectError>),
}

pub(crate) fn assemble(
    assembly: GatewayAssembly,
    limits: ManifestIoLimits,
    max_write_attempts: NonZeroUsize,
) -> Result<GatewayStack, VectorAssemblyError> {
    RootCredentials::new(&assembly.root_access_key, &assembly.root_secret_key, &assembly.root_secret_key)
        .map_err(GatewayAssemblyError::RootCredentials)?;
    let region = assembly.region.as_deref().unwrap_or(rustfs_config::RUSTFS_REGION);
    let regions = RegionSet::new([region]).map_err(|_| GatewayAssemblyError::Region {
        region: region.to_owned(),
    })?;
    let iam = Arc::new(ServerIam {
        server_ctx: assembly.server_ctx.clone(),
    });
    let root = Arc::new(live_root(assembly.server_ctx.clone()));
    let authenticator =
        SigV4Authenticator::new(Arc::new(IamCredentials::new(iam, root)), regions).verify_paths_double_encoded_under(&["/tags"]);
    let builder = ServiceBuilder::for_service(SigService::S3Vectors)
        .authenticator(authenticator)
        .authorizer(decide_with(|request| {
            if serves(request.operation) {
                Decision::Allow
            } else {
                Decision::Deny
            }
        }))
        .trace_source(MintedTraces::with_uuid_request_ids())
        .stage_filter(typed_error_filter(map_gateway_error));
    let (builder, _) = builder.config(service_config(assembly.body_read_timeout));
    let usecase = DefaultVectorUsecase::new(assembly.server_ctx, limits, max_write_attempts)?;
    let service = register(builder, Arc::new(Backend::new(usecase, region.to_owned())))
        .map_err(VectorAssemblyError::Dialect)?
        .build()
        .map_err(GatewayAssemblyError::Service)?;
    Ok(GatewayStack {
        service,
        keystone_auth: None,
    })
}
