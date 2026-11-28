// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Independent S3 Vectors Gateway assembly and live application readiness.

use super::gateway_stack::{ServerIam, attach_host_facts, live_root, service_config};
use crate::app::vector_usecase::{AuthorizedVectorRequest, DefaultVectorUsecase};
use crate::runtime_sources::ServerContextSlot;
use crate::vector::{
    lance::ManifestIoLimits,
    protocol::{
        ProtocolError, Request, Response,
        gateway::{Backend, VectorGatewayService, register, serves},
        gateway_error_bridge::{map_gateway_error, typed_error_filter},
    },
};
use rustfs_gateway::{BoxFuture, Decision, MintedTraces, RegionSet, ServiceBuilder, SigService, SigV4Authenticator, decide_with};
use rustfs_gateway_adapter::{IamCredentials, RootCredentials};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};

pub(crate) struct VectorAssembly {
    pub(crate) server_ctx: Arc<ServerContextSlot>,
    pub(crate) root_access_key: String,
    pub(crate) root_secret_key: String,
    pub(crate) region: Option<String>,
    pub(crate) body_read_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum VectorAssemblyError {
    #[error("Vector root credentials cannot verify signatures: {0}")]
    RootCredentials(#[from] rustfs_gateway_adapter::RootCredentialsError),
    #[error("region {region:?} cannot be a Vector signing region")]
    Region { region: String },
    #[error("Vector service refused its assembly: {0:?}")]
    Service(rustfs_gateway::AssemblyError),
    #[error("Vector dialect assembly failed: {0:?}")]
    Dialect(Vec<rustfs_gateway_core::DialectError>),
}

#[derive(Clone)]
pub(crate) struct VectorGateway {
    service: rustfs_gateway::S3Service,
    server_ctx: Arc<ServerContextSlot>,
}

impl VectorGateway {
    pub(crate) fn call<B>(
        &self,
        mut request: http::Request<B>,
    ) -> impl Future<Output = http::Response<rustfs_gateway::Body>> + Send + 'static
    where
        B: http_body::Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let extensions = request.extensions_mut();
        extensions.remove::<rustfs_gateway_adapter::KeystoneEstablished>();
        extensions.remove::<super::gateway_stack::OwnedKeystoneIdentity>();
        let unsupported_identity = extensions.remove::<rustfs_keystone::KeystoneRequestIdentity>().is_some();
        attach_host_facts(extensions);
        let mut service = self.service.clone();
        let server_ctx = self.server_ctx.clone();
        async move {
            if !server_ctx
                .installed_app_context()
                .is_some_and(|context| context.iam().is_ready())
            {
                return ProtocolError::new(
                    crate::vector::protocol::ErrorCode::ServiceUnavailable,
                    "Vector service is unavailable",
                )
                .into_response()
                .map(|body| rustfs_gateway::Body::from(bytes::Bytes::from(body)));
            }
            if unsupported_identity {
                let mut response = http::Response::new(rustfs_gateway::Body::empty());
                *response.status_mut() = http::StatusCode::FORBIDDEN;
                return response;
            }
            match tower::Service::call(&mut service, request).await {
                Ok(response) => response,
                Err(never) => match never {},
            }
        }
    }
}

pub(crate) fn assemble(
    assembly: VectorAssembly,
    limits: ManifestIoLimits,
    max_write_attempts: NonZeroUsize,
) -> Result<VectorGateway, VectorAssemblyError> {
    RootCredentials::new(&assembly.root_access_key, &assembly.root_secret_key, &assembly.root_secret_key)?;
    let region = assembly.region.as_deref().unwrap_or(rustfs_config::RUSTFS_REGION);
    let regions = RegionSet::new([region]).map_err(|_| VectorAssemblyError::Region {
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
    let (builder, _) = builder.config(service_config(
        assembly.body_read_timeout,
        rustfs_gateway::DEFAULT_MAX_BUFFERED_BODY_BYTES,
    ));
    let usecase = LiveVectorUsecase {
        server: assembly.server_ctx.clone(),
        limits,
        max_write_attempts,
    };
    let service = register(builder, Arc::new(Backend::new(usecase, region.to_owned())))
        .map_err(VectorAssemblyError::Dialect)?
        .build()
        .map_err(VectorAssemblyError::Service)?;
    Ok(VectorGateway {
        service,
        server_ctx: assembly.server_ctx,
    })
}

/// HTTP listeners exist before IAM installation. Resolve the application for each request,
/// and retain that same use case together with its authorization for execution.
struct LiveVectorUsecase {
    server: Arc<ServerContextSlot>,
    limits: ManifestIoLimits,
    max_write_attempts: NonZeroUsize,
}

impl VectorGatewayService for LiveVectorUsecase {
    type Authorization = (DefaultVectorUsecase, AuthorizedVectorRequest);

    fn authorize<'a>(
        &'a self,
        context: &'a rustfs_gateway::RequestContextView,
        request: &'a Request,
    ) -> BoxFuture<'a, Result<Self::Authorization, ProtocolError>> {
        Box::pin(async move {
            let usecase = DefaultVectorUsecase::new(self.server.clone(), self.limits, self.max_write_attempts)?;
            let authorization = usecase.authorize(context, request).await?;
            Ok((usecase, authorization))
        })
    }

    fn execute<'a>(
        &'a self,
        (usecase, authorization): Self::Authorization,
        request: Request,
    ) -> BoxFuture<'a, Result<Response, ProtocolError>> {
        Box::pin(async move { usecase.execute(authorization, request).await })
    }
}

/// Keeps the ordinary S3 service and its readiness/backpressure behavior intact.
/// Vector activation is closed until the distributed and lifecycle acceptance gates pass.
#[derive(Clone)]
pub(crate) struct VectorDispatch<S> {
    s3: S,
    vector: Result<VectorGateway, Arc<VectorAssemblyError>>,
    enabled: bool,
}

impl<S> VectorDispatch<S> {
    pub(crate) fn new(s3: S, vector: Result<VectorGateway, VectorAssemblyError>) -> Self {
        Self {
            s3,
            vector: vector.map_err(Arc::new),
            enabled: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn enabled_for_test(s3: S, vector: VectorGateway) -> Self {
        Self {
            s3,
            vector: Ok(vector),
            enabled: true,
        }
    }
}

impl<S, B> tower::Service<http::Request<B>> for VectorDispatch<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<super::http::EdgeBody>, Error = super::http::EdgeError>,
    S::Future: Send + 'static,
    B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    type Response = http::Response<super::http::EdgeBody>;
    type Error = super::http::EdgeError;
    type Future = std::pin::Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.s3.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if !claims_vector_service(&request) {
            let response = self.s3.call(request);
            return Box::pin(response);
        }
        if !self.enabled {
            let response =
                ProtocolError::new(crate::vector::protocol::ErrorCode::ServiceUnavailable, "Vector service is unavailable")
                    .into_response()
                    .map(|body| super::http::EdgeBody::from(bytes::Bytes::from(body)));
            return Box::pin(async move { Ok(response) });
        }
        let vector = match &self.vector {
            Ok(vector) => vector,
            Err(_) => {
                let response =
                    ProtocolError::new(crate::vector::protocol::ErrorCode::ServiceUnavailable, "Vector service is unavailable")
                        .into_response()
                        .map(|body| super::http::EdgeBody::from(bytes::Bytes::from(body)));
                return Box::pin(async move { Ok(response) });
            }
        };
        let response = vector.call(request);
        Box::pin(async move { Ok(response.await.map(super::http::EdgeBody::new)) })
    }
}

/// The credential scope is an untrusted routing hint; Gateway verifies it before dispatch.
/// Tag paths alone cannot identify the service because `tags` is a valid S3 bucket name.
fn claims_vector_service<B>(request: &http::Request<B>) -> bool {
    fn vector_scope(credential: &str) -> bool {
        let mut components = credential.split('/');
        matches!(
            (
                components.next(),
                components.next(),
                components.next(),
                components.next(),
                components.next(),
                components.next()
            ),
            (Some(_), Some(_), Some(_), Some("s3vectors"), Some("aws4_request"), None)
        )
    }
    request.headers().get_all(http::header::AUTHORIZATION).iter().any(|value| {
        value
            .to_str()
            .ok()
            .and_then(|value| value.strip_prefix("AWS4-HMAC-SHA256 "))
            .is_some_and(|value| {
                value
                    .split(',')
                    .any(|field| field.trim().strip_prefix("Credential=").is_some_and(vector_scope))
            })
    }) || request.uri().query().is_some_and(|query| {
        url::form_urlencoded::parse(query.as_bytes()).any(|(key, value)| key == "X-Amz-Credential" && vector_scope(&value))
    })
}

#[cfg(test)]
mod tests;
