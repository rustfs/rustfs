// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Private application admission for verified S3 Vectors requests.

mod admission;
mod bucket;
mod dataset;
mod encryption;
pub(crate) mod maintenance;
mod target;
#[cfg(test)]
mod tests;

use crate::{
    app::{
        RequestEnvelope,
        bucket_usecase::DefaultBucketUsecase,
        storage_api::managed::{
            AccountNamespaceId, BucketHandle, BucketIncarnation, BucketIncarnationFenceGuard, BucketKind, BucketMetadata,
            BucketPublicName,
        },
    },
    runtime_sources::ServerContextSlot,
    table_catalog::{
        CatalogName, CatalogResourceIdentity, EcStoreTableCatalogObjectBackend, Namespace, ObjectTableCatalogStore,
        TableCatalogStoreError, VectorIndexDefinitionV1,
    },
    vector::{
        access_core::{BoundVectorRequest, BucketAuthorizationFacts, VectorAccessError, VectorTarget},
        lance::ManifestIoLimits,
        protocol::{ErrorCode, Operation, ProtocolError, Request, Response, dto, gateway::VectorGatewayService},
        service::{VectorExecution, VectorRequestId, VectorService, VectorServiceError},
    },
};
use admission::VerifiedVectorRequest;
use std::{num::NonZeroUsize, sync::Arc};
use target::RequestTarget;
use uuid::Uuid;

type Backend = EcStoreTableCatalogObjectBackend<crate::app::storage_api::bucket_usecase::ECStore>;
type Catalog = ObjectTableCatalogStore<Backend>;

#[derive(Clone)]
pub(crate) struct DefaultVectorUsecase {
    server: Arc<ServerContextSlot>,
    region: String,
    limits: ManifestIoLimits,
    max_write_attempts: NonZeroUsize,
    numeric_proof_cache: Arc<crate::vector::lance::LanceNumericProofCache>,
}

pub(crate) struct AuthorizedVectorRequest {
    verified: VerifiedVectorRequest,
    target: RequestTarget,
    bucket: Option<BucketHandle>,
    catalog: Option<Arc<Catalog>>,
    resource: Option<CatalogResourceIdentity>,
    bound: BoundVectorRequest,
    reader: Arc<dataset::CandidateReader>,
    request_id: VectorRequestId,
    audit: Option<crate::app::storage_api::vector_usecase::OperationHelper>,
}

impl DefaultVectorUsecase {
    pub(crate) fn new(
        server: Arc<ServerContextSlot>,
        limits: ManifestIoLimits,
        max_write_attempts: NonZeroUsize,
    ) -> Result<Self, ProtocolError> {
        let app = server.installed_app_context().ok_or_else(unavailable)?;
        let region = app
            .region()
            .get()
            .map(|region| region.to_string())
            .unwrap_or_else(|| rustfs_config::RUSTFS_REGION.to_owned());
        Ok(Self {
            server,
            region,
            limits,
            max_write_attempts,
            numeric_proof_cache: app.vector_numeric_proof_cache(),
        })
    }

    fn buckets(&self) -> DefaultBucketUsecase {
        DefaultBucketUsecase::with_context(self.server.installed_app_context())
    }

    async fn admit(&self, verified: VerifiedVectorRequest) -> Result<AuthorizedVectorRequest, ProtocolError> {
        let current = verified.current_envelope().await;
        let envelope = match &current {
            Ok(envelope) => envelope.fork_for_authorization(),
            Err(_) => verified.revoked_identity_audit_envelope()?,
        };
        let helper = crate::app::storage_api::vector_usecase::OperationHelper::audit_only_named_from_view(
            &envelope.audit_view(),
            verified.request().operation().name(),
        );
        if let Err(error) = current {
            complete_audit(helper, Some(&error));
            return Err(error);
        }
        match self.admit_inner(verified).await {
            Ok(mut authorization) => {
                authorization.audit = Some(helper);
                Ok(authorization)
            }
            Err(error) => {
                complete_audit(helper, Some(&error));
                Err(error)
            }
        }
    }

    async fn admit_inner(&self, verified: VerifiedVectorRequest) -> Result<AuthorizedVectorRequest, ProtocolError> {
        let request = verified.request();
        let target = RequestTarget::resolve(request, &self.region)?;
        let reader = Arc::new(dataset::CandidateReader::default());
        let mut catalog = None;
        let mut resource = None;
        let bucket = match &target {
            RequestTarget::Service => {
                verified.authorize(request, &VectorTarget::Service).await?;
                None
            }
            RequestTarget::NewBucket(public_name) => {
                verified.authorize(request, &VectorTarget::NewBucket { public_name }).await?;
                None
            }
            RequestTarget::Bucket(name)
            | RequestTarget::NewIndex { bucket: name, .. }
            | RequestTarget::Index { bucket: name, .. } => {
                let usecase = self.buckets();
                let handle = usecase
                    .resolve_bucket_handle(
                        BucketKind::Vector,
                        &AccountNamespaceId::try_new(String::new()).map_err(|_| unavailable())?,
                        &self.region,
                        &BucketPublicName::try_new(name.clone()).map_err(|_| denied())?,
                    )
                    .await
                    .map_err(bucket_error)?;
                match &target {
                    RequestTarget::Index { index, .. } => {
                        let store = self.catalog(&handle, reader.clone()).await?;
                        let (operation, entry) = store
                            .begin_vector_operation(&handle, &index_namespace()?, index)
                            .await
                            .map_err(catalog_error)?;
                        let guard = operation.lifecycle_guard().map_err(catalog_error)?;
                        let snapshot = usecase
                            .read_managed_bucket_metadata(&handle, &guard)
                            .await
                            .map_err(bucket_error)?;
                        let facts = bucket::authorization_facts(&handle, &snapshot)?;
                        verified
                            .authorize(
                                request,
                                &VectorTarget::Index {
                                    bucket: &facts,
                                    index_name: index,
                                    resource: operation.resource_identity(),
                                    definition: entry.definition(),
                                },
                            )
                            .await?;
                        operation.check_held().map_err(catalog_error)?;
                        resource = Some(operation.resource_identity().clone());
                        catalog = Some(store);
                    }
                    RequestTarget::Bucket(_) | RequestTarget::NewIndex { .. } => {
                        let guard = usecase.acquire_bucket_handle_guard(&handle).await.map_err(bucket_error)?;
                        let snapshot = usecase
                            .read_managed_bucket_metadata(&handle, &guard)
                            .await
                            .map_err(bucket_error)?;
                        let facts = bucket::authorization_facts(&handle, &snapshot)?;
                        let target = match &target {
                            RequestTarget::NewIndex { index, .. } => VectorTarget::NewIndex {
                                bucket: &facts,
                                index_name: index,
                            },
                            _ => VectorTarget::Bucket(&facts),
                        };
                        verified.authorize(request, &target).await?;
                    }
                    _ => return Err(denied()),
                }
                Some(handle)
            }
        };
        if matches!(target, RequestTarget::NewIndex { .. })
            && let Some(bucket) = &bucket
        {
            catalog = Some(self.catalog(bucket, reader.clone()).await?);
        }
        let bound = BoundVectorRequest::new(request.clone(), resource.clone());
        let request_id = VectorRequestId::try_new(Uuid::new_v4()).map_err(service_error)?;
        Ok(AuthorizedVectorRequest {
            verified,
            target,
            bucket,
            catalog,
            resource,
            bound,
            reader,
            request_id,
            audit: None,
        })
    }

    async fn catalog(&self, bucket: &BucketHandle, reader: Arc<dataset::CandidateReader>) -> Result<Arc<Catalog>, ProtocolError> {
        let app = self.server.installed_app_context().ok_or_else(unavailable)?;
        Ok(Arc::new(
            Catalog::for_vector_scope(Backend::new_with_managed_candidate_reader(app.object_store(), reader), bucket.clone())
                .await
                .map_err(catalog_error)?,
        ))
    }

    async fn execute_authorized(
        &self,
        authorization: AuthorizedVectorRequest,
        request: Request,
    ) -> Result<Response, ProtocolError> {
        let owner = self.clone();
        crate::app::storage_api::vector_usecase::run_owned_mutation(async move {
            let mut authorization = authorization;
            let helper = authorization.audit.take().ok_or_else(|| OwnedExecutionError(unavailable()))?;
            let result = owner.execute_owned(authorization, request).await;
            complete_audit(helper, result.as_ref().err());
            result.map_err(OwnedExecutionError)
        })
        .await
        .map_err(|error| error.0)
    }

    async fn execute_owned(&self, authorization: AuthorizedVectorRequest, request: Request) -> Result<Response, ProtocolError> {
        authorization
            .bound
            .require_same(&request, authorization.resource.as_ref())
            .map_err(access_error)?;
        match &authorization.target {
            RequestTarget::Index { index, .. } | RequestTarget::NewIndex { index, .. } => {
                let catalog = authorization.catalog.as_ref().ok_or_else(unavailable)?.clone();
                if matches!(request, Request::CreateIndex(_)) {
                    self.initialize_index_catalog(&authorization, index).await?;
                }
                let context = dataset::IndexRequest {
                    owner: self,
                    authorization: &authorization,
                    index,
                };
                let execution = VectorService::new(catalog, self.limits, self.max_write_attempts)
                    .with_numeric_proof_cache(self.numeric_proof_cache.clone())
                    .execute(&context)
                    .await;
                #[cfg(test)]
                if let Err(error) = &execution {
                    crate::vector::fault_test::observe_sdk_request_failure(
                        crate::vector::fault_test::SdkRequestFailureContext {
                            instance_id: self.server.installed_app_context().map(|app| app.object_store().id),
                            resource: authorization.resource.as_ref(),
                            request_id: authorization.request_id.as_uuid(),
                            http_request_id: authorization.verified.fault_http_request_id(),
                            account: authorization.verified.fault_account(),
                        },
                        error,
                    );
                }
                match execution.map_err(|error| service_error_for_operation(request.operation(), error))? {
                    VectorExecution::Complete(response) => Ok(response),
                    VectorExecution::GetVectors(prepared) => Ok(Response::GetVectors(dto::GetVectorsOutput {
                        vectors: prepared.vectors,
                    })),
                    VectorExecution::QueryVectors(prepared) => {
                        Ok(Response::QueryVectors(prepared.into_output().map_err(service_error)?))
                    }
                }
            }
            _ => self.execute_bucket(authorization, &request).await,
        }
    }
}

#[cfg(test)]
pub(crate) async fn catalog_worker_test_identity() -> (Arc<crate::runtime_sources::AppContext>, rustfs_credentials::Credentials) {
    let fixture = tests::Fixture::new().await;
    (fixture.app, fixture.credentials)
}

struct OwnedExecutionError(ProtocolError);

fn complete_audit(helper: crate::app::storage_api::vector_usecase::OperationHelper, error: Option<&ProtocolError>) {
    let result: rustfs_s3_types::S3Result<()> = match error {
        None => Ok(()),
        Some(error) => {
            let mut audit_error = rustfs_s3_types::S3Error::with_message(
                rustfs_s3_types::S3ErrorCode::Custom(error.code().name().into()),
                error.to_string(),
            );
            audit_error.set_status_code(error.status_code());
            Err(audit_error)
        }
    };
    drop(helper.complete_owned(&result, http::StatusCode::OK, &http::HeaderMap::new()));
}

impl From<rustfs_s3_types::S3Error> for OwnedExecutionError {
    fn from(error: rustfs_s3_types::S3Error) -> Self {
        Self(s3_error(error))
    }
}

impl VectorGatewayService for DefaultVectorUsecase {
    type Authorization = AuthorizedVectorRequest;
    fn authorize<'a>(
        &'a self,
        context: &'a rustfs_gateway::RequestContextView,
        request: &'a Request,
    ) -> rustfs_gateway::BoxFuture<'a, Result<Self::Authorization, ProtocolError>> {
        Box::pin(async move {
            self.admit(VerifiedVectorRequest::from_gateway(self.server.clone(), context, request, &self.region).await?)
                .await
        })
    }
    fn execute<'a>(
        &'a self,
        authorization: Self::Authorization,
        request: Request,
    ) -> rustfs_gateway::BoxFuture<'a, Result<Response, ProtocolError>> {
        Box::pin(self.execute_authorized(authorization, request))
    }
}

fn index_namespace() -> Result<Namespace, ProtocolError> {
    Namespace::parse("indexes").map_err(|_| unavailable())
}
fn denied() -> ProtocolError {
    ProtocolError::new(ErrorCode::AccessDenied, "Vector access is denied")
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(ErrorCode::ServiceUnavailable, "Vector service is unavailable")
}
fn pagination_unavailable() -> ProtocolError {
    ProtocolError::new(ErrorCode::ServiceUnavailable, "Vector pagination is unavailable")
}
fn access_error(error: VectorAccessError) -> ProtocolError {
    match error {
        VectorAccessError::Denied | VectorAccessError::ResourceMismatch => denied(),
        VectorAccessError::Iam(_) => unavailable(),
        VectorAccessError::Policy(_) => ProtocolError::validation("policy", "invalid Vector policy"),
    }
}
fn s3_error(error: rustfs_s3_types::S3Error) -> ProtocolError {
    api_code(error.code().as_str())
}
fn api_code(code: &str) -> ProtocolError {
    ProtocolError::new(
        match code {
            "AccessDenied" | "InvalidAccessKeyId" | "ExpiredToken" | "InvalidToken" | "SignatureDoesNotMatch" => {
                ErrorCode::AccessDenied
            }
            "NoSuchBucket" | "NoSuchKey" | "NoSuchVersion" => ErrorCode::NotFound,
            "BucketAlreadyExists" | "BucketAlreadyOwnedByYou" | "BucketNotEmpty" | "PreconditionFailed" => ErrorCode::Conflict,
            "InvalidRequest" | "InvalidArgument" => ErrorCode::Validation,
            "SlowDown" | "ServiceUnavailable" => ErrorCode::ServiceUnavailable,
            _ => ErrorCode::InternalServer,
        },
        "Vector operation failed",
    )
}
fn bucket_error(error: crate::app::bucket_usecase::BucketResolveError) -> ProtocolError {
    use crate::app::bucket_usecase::BucketResolveError as E;
    match error {
        E::MissingIdentity => ProtocolError::new(ErrorCode::NotFound, "Vector bucket does not exist"),
        E::CapabilityUnavailable => unavailable(),
        E::Storage(error) => {
            if let crate::app::storage_api::vector_usecase::StorageError::Io(context) = &error
                && let Some(catalog) = context
                    .get_ref()
                    .and_then(|source| source.downcast_ref::<TableCatalogStoreError>())
            {
                return catalog_error_ref(catalog);
            }
            api_code(crate::error::ApiError::from(error).code.as_str())
        }
        _ => denied(),
    }
}
fn catalog_error(error: TableCatalogStoreError) -> ProtocolError {
    catalog_error_ref(&error)
}
fn catalog_error_ref(error: &TableCatalogStoreError) -> ProtocolError {
    use TableCatalogStoreError as E;
    let code = match error {
        E::NotFound(_) | E::NamespaceNotFound(_) | E::TableNotFound(_) => ErrorCode::NotFound,
        E::AlreadyExists(_) | E::Conflict(_) => ErrorCode::Conflict,
        E::Unsupported(_) | E::Unavailable(_) | E::LockUnavailable { .. } => ErrorCode::ServiceUnavailable,
        E::Io {
            classification: crate::table_catalog::CatalogIoErrorKind::Denied,
            ..
        } => ErrorCode::AccessDenied,
        _ => ErrorCode::InternalServer,
    };
    ProtocolError::new(code, "Vector Catalog operation failed")
}
fn service_error_for_operation(operation: Operation, error: VectorServiceError) -> ProtocolError {
    if matches!(operation, Operation::CreateIndex | Operation::PutVectors) {
        let mut source: &(dyn std::error::Error + 'static) = &error;
        loop {
            if matches!(
                source.downcast_ref::<crate::app::storage_api::vector_usecase::StorageError>(),
                Some(crate::app::storage_api::vector_usecase::StorageError::QuotaExceeded { .. })
            ) {
                return ProtocolError::new(ErrorCode::ServiceQuotaExceeded, "Vector storage quota exceeded");
            }
            let next = if let Some(error) = source.downcast_ref::<std::io::Error>() {
                error.get_ref().map(|inner| inner as &(dyn std::error::Error + 'static))
            } else {
                source.source()
            };
            let Some(next) = next else {
                break;
            };
            source = next;
        }
    }
    service_error(error)
}
fn service_error(error: VectorServiceError) -> ProtocolError {
    let mut source: &(dyn std::error::Error + 'static) = &error;
    loop {
        if let Some(error) = source.downcast_ref::<rustfs_s3_types::S3Error>() {
            return api_code(error.code().as_str());
        }
        if let Some(error) = source.downcast_ref::<crate::error::ApiError>() {
            return api_code(error.code.as_str());
        }
        let Some(next) = source.source() else {
            break;
        };
        source = next;
    }
    match error {
        VectorServiceError::Protocol(error) => error,
        VectorServiceError::Catalog(error) => catalog_error(error),
        VectorServiceError::Input(_) | VectorServiceError::Filter(_) | VectorServiceError::InvalidMetadataShape { .. } => {
            ProtocolError::validation("vectors", "invalid Vector data or filter")
        }
        VectorServiceError::PendingIntegration(_) | VectorServiceError::OperationUnavailable(_) => unavailable(),
        VectorServiceError::Dataset(
            crate::vector::lance::DatasetError::InvalidVector(_)
            | crate::vector::lance::DatasetError::DuplicateInputKey { .. }
            | crate::vector::lance::DatasetError::InvalidQueryLimit
            | crate::vector::lance::DatasetError::InvalidOffset,
        ) => ProtocolError::validation("vectors", "invalid Vector data or query"),
        VectorServiceError::Manifest(crate::vector::lance::LanceManifestError::ExpectedHeadConflict) => {
            ProtocolError::new(ErrorCode::Conflict, "Vector Index changed")
        }
        _ => ProtocolError::new(ErrorCode::InternalServer, "Vector operation failed"),
    }
}
