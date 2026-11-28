// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use serde_json::Value;
use uuid::Uuid;

use crate::{
    storage_api::vector::{BucketHandle, ManagedObjectAccess, ManagedTableId},
    table_catalog::{
        CatalogCommitId, CatalogCommitParts, CatalogFormat, CatalogName, CatalogResourceIdentity, Namespace,
        ObjectTableCatalogStore, TableCatalogObjectBackend, TableCatalogStoreError, VectorCatalogEntry,
        VectorCatalogOperationGuard, VectorEffectiveEncryptionV1, VectorIndexCreateRequest, VectorIndexCreateReservation,
        VectorIndexDefinitionV1,
    },
};

use super::{
    filter::{FilterError, ValidatedFilter},
    lance::{
        DatasetError, DatasetSnapshot, LanceManifestAdapter, LanceManifestError, LanceOperationContext, ManagedManifestObjects,
        ManifestIoLimits, ManifestPrepareRequest, ProjectedVector, QueryMatch, VectorIndexManifestSpec, VectorProjection,
        VectorRecord, decode_initialization_payload, encode_initialization_payload, plan_initial_manifest, validate_record_keys,
    },
    model::{VectorDistanceMetric, VectorInputError, validate_non_filterable_keys},
    pagination::{
        self, PaginationError, PaginationSession, ParsedToken, SessionBinding, SessionResults, SessionState, StoredMatch,
    },
    protocol::{Operation, ProtocolError, Request, Response, dto},
};

mod pages;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorRequestId(Uuid);

impl VectorRequestId {
    pub(crate) fn try_new(value: Uuid) -> Result<Self, VectorServiceError> {
        if value.is_nil() {
            return Err(VectorServiceError::InvalidContext("request ID must be non-nil"));
        }
        Ok(Self(value))
    }
    pub(crate) fn as_uuid(self) -> Uuid {
        self.0
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum VectorServiceError {
    #[error("Vector request validation failed")]
    Protocol(#[from] ProtocolError),
    #[error("Vector Catalog operation failed")]
    Catalog(#[from] TableCatalogStoreError),
    #[error("Vector manifest operation failed")]
    Manifest(#[from] LanceManifestError),
    #[error("Vector Dataset operation failed")]
    Dataset(#[from] DatasetError),
    #[error("Vector value validation failed")]
    Input(#[from] VectorInputError),
    #[error("Vector filter validation failed")]
    Filter(#[from] FilterError),
    #[error("Vector pagination failed")]
    Pagination(#[from] PaginationError),
    #[error("Vector metadata at position {position} must be an object")]
    InvalidMetadataShape { position: usize },
    #[error("Vector request context is invalid: {0}")]
    InvalidContext(&'static str),
    #[error("Vector {operation} bridge failed")]
    Bridge {
        operation: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Vector operation {0:?} has no connected service path")]
    OperationUnavailable(Operation),
}

pub(crate) struct ManagedDatasetAccess {
    pub access: ManagedObjectAccess,
    pub objects: Arc<dyn ManagedManifestObjects>,
}

/// Supplies the authenticated request, identity resolution, authorization and Managed I/O.
#[async_trait::async_trait]
pub(crate) trait VectorRequestAccess: Send + Sync {
    fn request(&self) -> &Request;
    fn request_id(&self) -> VectorRequestId;
    fn bucket(&self) -> &BucketHandle;
    fn index_name(&self) -> &CatalogName;

    fn pagination_principal(&self) -> Result<[u8; 32], VectorServiceError>;

    fn pagination_now(&self) -> i64 {
        time::OffsetDateTime::now_utc().unix_timestamp()
    }

    /// Check the main and additional actions against this guarded current definition.
    /// QueryVectors with a filter or returnMetadata also requires GetVectors.
    /// Finish these checks before granting Managed dataset access.
    async fn authorize_index(
        &self,
        request: &Request,
        resource: &CatalogResourceIdentity,
        definition: &VectorIndexDefinitionV1,
        operation: Option<&VectorCatalogOperationGuard>,
    ) -> Result<(), VectorServiceError>;

    /// Resolve approved inheritance and explicit encryption using the actual bucket configuration.
    async fn resolve_index_definition(
        &self,
        input: &dto::CreateIndexInput,
    ) -> Result<VectorIndexDefinitionV1, VectorServiceError>;

    /// Accepted owned writes must retain this entire guard until their storage tail drains.
    async fn managed_dataset(
        &self,
        guard: Arc<VectorCatalogOperationGuard>,
        definition: &VectorIndexDefinitionV1,
    ) -> Result<ManagedDatasetAccess, VectorServiceError>;

    /// Grant the query-results prefix independently from Dataset access, under the same owner.
    async fn managed_query_results(
        &self,
        guard: Arc<VectorCatalogOperationGuard>,
        definition: &VectorIndexDefinitionV1,
    ) -> Result<ManagedDatasetAccess, VectorServiceError>;
}

pub(crate) struct VectorService<B: TableCatalogObjectBackend> {
    catalog: Arc<ObjectTableCatalogStore<B>>,
    limits: ManifestIoLimits,
    max_write_attempts: NonZeroUsize,
    numeric_proof_cache: Arc<super::lance::LanceNumericProofCache>,
}

pub(crate) enum VectorExecution<B: TableCatalogObjectBackend> {
    Complete(Response),
    QueryVectors(PreparedVectorQuery<B>),
}

pub(crate) struct PreparedVectorQuery<B: TableCatalogObjectBackend> {
    pub snapshot: DatasetSnapshot<B>,
    /// Preserve exact scores and ordering until conversion to the protocol Float.
    pub candidates: Vec<QueryMatch>,
    pub metadata_rows: Option<Vec<ProjectedVector>>,
    pub return_distance: bool,
    pub next_token: Option<String>,
}

impl<B: TableCatalogObjectBackend> PreparedVectorQuery<B> {
    pub(crate) fn into_output(self) -> Result<dto::QueryVectorsOutput, VectorServiceError> {
        if self.candidates.len() > pagination::QUERY_PAGE_SIZE {
            return Err(VectorServiceError::InvalidContext("prepared query exceeds its page size"));
        }
        if let Some(rows) = &self.metadata_rows
            && (rows.len() != self.candidates.len()
                || rows
                    .iter()
                    .zip(&self.candidates)
                    .any(|(row, candidate)| row.key != candidate.key))
        {
            return Err(VectorServiceError::InvalidContext("query metadata differs from its fixed candidates"));
        }
        let distance_metric = public_metric(self.snapshot.info().index.distance_metric);
        let mut metadata_rows = self.metadata_rows.into_iter().flatten();
        let vectors = self
            .candidates
            .into_iter()
            .map(|candidate| dto::QueryOutputVector {
                key: candidate.key,
                distance: self.return_distance.then(|| {
                    if candidate.distance_f64 > f64::from(f32::MAX) {
                        f32::INFINITY
                    } else {
                        candidate.distance_f64 as f32
                    }
                }),
                metadata: metadata_rows.next().and_then(|row| row.metadata).map(Value::Object),
            })
            .collect();
        Ok(dto::QueryVectorsOutput {
            vectors,
            distance_metric,
            next_token: self.next_token,
        })
    }
}

struct BoundIndex<B: TableCatalogObjectBackend> {
    entry: VectorCatalogEntry,
    guard: Arc<VectorCatalogOperationGuard>,
    access: ManagedObjectAccess,
    adapter: LanceManifestAdapter<B>,
}

impl<B: TableCatalogObjectBackend> VectorService<B> {
    pub(crate) fn new(
        catalog: Arc<ObjectTableCatalogStore<B>>,
        limits: ManifestIoLimits,
        max_write_attempts: NonZeroUsize,
    ) -> Self {
        Self {
            catalog,
            limits,
            max_write_attempts,
            numeric_proof_cache: Arc::new(super::lance::LanceNumericProofCache::default()),
        }
    }

    pub(crate) fn with_numeric_proof_cache(mut self, cache: Arc<super::lance::LanceNumericProofCache>) -> Self {
        self.numeric_proof_cache = cache;
        self
    }

    pub(crate) async fn execute(&self, context: &dyn VectorRequestAccess) -> Result<VectorExecution<B>, VectorServiceError> {
        let response = match context.request() {
            Request::CreateIndex(input) => Response::CreateIndex(self.create_index(context, input).await?),
            Request::DeleteIndex(_) => {
                self.delete_index(context).await?;
                Response::DeleteIndex(dto::DeleteIndexOutput {})
            }
            Request::GetIndex(_) => Response::GetIndex(self.get_index(context).await?),
            Request::PutVectors(input) => {
                self.put_vectors(context, input).await?;
                Response::PutVectors(dto::PutVectorsOutput {})
            }
            Request::DeleteVectors(input) => {
                self.delete_vectors(context, &input.keys).await?;
                Response::DeleteVectors(dto::DeleteVectorsOutput {})
            }
            Request::GetVectors(input) => Response::GetVectors(self.get_vectors(context, input).await?),
            Request::ListVectors(input) => Response::ListVectors(self.list_vectors(context, input).await?),
            Request::QueryVectors(input) => return Ok(VectorExecution::QueryVectors(self.query_vectors(context, input).await?)),
            Request::ListTagsForResource(_) => {
                let (guard, entry) = self.begin_index(context, None).await?;
                guard.check_held()?;
                Response::ListTagsForResource(dto::ListTagsForResourceOutput {
                    tags: entry.definition().tags.clone(),
                })
            }
            Request::TagResource(input) => {
                self.update_index_tags(context, crate::table_catalog::VectorTagUpdate::Set(input.tags.clone()))
                    .await?;
                Response::TagResource(dto::TagResourceOutput {})
            }
            Request::UntagResource(input) => {
                self.update_index_tags(
                    context,
                    crate::table_catalog::VectorTagUpdate::Remove(input.tag_keys.iter().cloned().collect()),
                )
                .await?;
                Response::UntagResource(dto::UntagResourceOutput {})
            }
            Request::UpdateIndexMode(input) => {
                if input.index_mode != dto::IndexMode::Enhanced {
                    return Err(ProtocolError::validation("indexMode", "only ENHANCED is supported").into());
                }
                let (guard, entry) = self.begin_index(context, None).await?;
                // Decoding the current descriptor proves this is a supported Index.
                LanceManifestAdapter::<B>::index_from_catalog(&entry)?;
                guard.check_held()?;
                Response::UpdateIndexMode(dto::UpdateIndexModeOutput {})
            }
            request => return Err(VectorServiceError::OperationUnavailable(request.operation())),
        };
        Ok(VectorExecution::Complete(response))
    }

    async fn begin_index(
        &self,
        context: &dyn VectorRequestAccess,
        expected_resource: Option<&CatalogResourceIdentity>,
    ) -> Result<(Arc<VectorCatalogOperationGuard>, VectorCatalogEntry), VectorServiceError> {
        let namespace = index_namespace()?;
        let (guard, entry) = self
            .catalog
            .begin_vector_operation(context.bucket(), &namespace, context.index_name())
            .await?;
        if expected_resource.is_some_and(|expected| expected != guard.resource_identity()) {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "Vector retry resource identity changed",
            }
            .into());
        }
        context
            .authorize_index(context.request(), guard.resource_identity(), entry.definition(), Some(&guard))
            .await?;
        guard.check_held()?;
        Ok((Arc::new(guard), entry))
    }

    async fn update_index_tags(
        &self,
        context: &dyn VectorRequestAccess,
        update: crate::table_catalog::VectorTagUpdate,
    ) -> Result<(), VectorServiceError> {
        let (guard, entry) = self.begin_index(context, None).await?;
        if let crate::table_catalog::VectorTagUpdate::Set(tags) = &update {
            let existing = &entry.definition().tags;
            let additional = tags.keys().filter(|key| !existing.contains_key(*key)).count();
            if existing.len() + additional > 50 {
                return Err(ProtocolError::validation("tags", "a resource may have at most 50 tags").into());
            }
        }
        // Catalog metadata commits validate the referenced manifest through this owner.
        let managed = context.managed_dataset(guard.clone(), entry.definition()).await?;
        self.catalog
            .update_vector_tags_with_guard(&guard, &entry, update, new_commit_id()?)
            .await?;
        drop(managed);
        guard.check_held()?;
        Ok(())
    }

    async fn bind_index(
        &self,
        context: &dyn VectorRequestAccess,
        expected_resource: Option<&CatalogResourceIdentity>,
    ) -> Result<BoundIndex<B>, VectorServiceError> {
        let (guard, entry) = self.begin_index(context, expected_resource).await?;
        let index = LanceManifestAdapter::<B>::index_from_catalog(&entry)?;
        let managed = context.managed_dataset(guard.clone(), entry.definition()).await?;
        let access = managed.access;
        let adapter = LanceManifestAdapter::new_guarded(
            LanceOperationContext {
                catalog: self.catalog.clone(),
                objects: managed.objects,
                access: access.clone(),
                guard: guard.clone(),
                definition: entry.definition().clone(),
            },
            context.index_name().clone(),
            index,
            self.limits,
        )?
        .with_numeric_proof_cache(self.numeric_proof_cache.clone())?;
        Ok(BoundIndex {
            entry,
            guard,
            access,
            adapter,
        })
    }

    async fn create_index(
        &self,
        context: &dyn VectorRequestAccess,
        input: &dto::CreateIndexInput,
    ) -> Result<dto::CreateIndexOutput, VectorServiceError> {
        if context.index_name().as_str() != input.index_name {
            return Err(VectorServiceError::InvalidContext("resolved Index name differs from the request"));
        }
        let definition = context.resolve_index_definition(input).await?;
        definition.validate()?;
        if definition.tags != input.tags.clone().unwrap_or_default() {
            return Err(VectorServiceError::InvalidContext("initial tags differ from the authorized request"));
        }
        let resource = CatalogResourceIdentity::try_new(
            context.bucket().clone(),
            ManagedTableId::try_new(Uuid::new_v4().to_string()).map_err(|source| VectorServiceError::Bridge {
                operation: "allocate Index identity",
                source: Box::new(source),
            })?,
            CatalogFormat::Lance,
        )?;
        context
            .authorize_index(context.request(), &resource, &definition, None)
            .await?;
        let index = create_index_spec(input, &definition.effective_encryption)?;
        let create_id = new_commit_id()?;
        let plan = plan_initial_manifest(&resource, &index, &create_id).await?;
        let request = VectorIndexCreateRequest {
            resource_identity: resource,
            namespace: index_namespace()?,
            name: context.index_name().clone(),
            create_id,
            request_id: context.request_id().as_uuid(),
            definition,
            initialization_payload: encode_initialization_payload(&index)?,
            planned_manifest: plan.reference.clone(),
        };
        let (reservation, guard) = self.catalog.reserve_vector_index(context.bucket(), request).await?;
        self.publish_reserved_index(context, &reservation, Arc::new(guard)).await?;
        Ok(dto::CreateIndexOutput {
            index_arn: index_arn(context.bucket(), context.index_name()),
        })
    }

    async fn publish_reserved_index(
        &self,
        context: &dyn VectorRequestAccess,
        reservation: &VectorIndexCreateReservation,
        guard: Arc<VectorCatalogOperationGuard>,
    ) -> Result<(), VectorServiceError> {
        guard.check_held()?;
        if reservation.published_entry().is_some() {
            return Ok(());
        }
        let index =
            decode_initialization_payload(reservation.initialization_payload(), &reservation.definition().effective_encryption)?;
        let plan = plan_initial_manifest(reservation.resource_identity(), &index, reservation.create_id()).await?;
        plan.verify_reserved_reference(reservation.planned_manifest())?;
        let managed = context.managed_dataset(guard.clone(), reservation.definition()).await?;
        let adapter = LanceManifestAdapter::new_guarded(
            LanceOperationContext {
                catalog: self.catalog.clone(),
                objects: managed.objects,
                access: managed.access.clone(),
                guard: guard.clone(),
                definition: reservation.definition().clone(),
            },
            reservation.name().clone(),
            index.clone(),
            self.limits,
        )?
        .with_numeric_proof_cache(self.numeric_proof_cache.clone())?;
        let prepared = adapter
            .prepare_initial_manifest(
                &managed.access,
                ManifestPrepareRequest {
                    parts: CatalogCommitParts {
                        resource_identity: reservation.resource_identity().clone(),
                        namespace: reservation.namespace().clone(),
                        name: reservation.name().clone(),
                        expected_head: reservation.expected_head().clone(),
                        commit_id: reservation.create_id().clone(),
                        operation: "vector-create".into(),
                        idempotency_key: None,
                        writer: None,
                    },
                    index,
                },
                &plan,
                reservation.planned_manifest(),
            )
            .await?;
        self.catalog
            .publish_vector_index_initial_head(&guard, reservation, prepared)
            .await?;
        Ok(())
    }

    async fn get_index(&self, context: &dyn VectorRequestAccess) -> Result<dto::GetIndexOutput, VectorServiceError> {
        let (guard, entry) = self.begin_index(context, None).await?;
        let index = LanceManifestAdapter::<B>::index_from_catalog(&entry)?;
        let created = entry.created_at()?;
        guard.check_held()?;
        Ok(dto::GetIndexOutput {
            index: dto::Index {
                vector_bucket_name: context.bucket().public_name().as_str().into(),
                index_name: context.index_name().as_str().into(),
                index_arn: index_arn(context.bucket(), context.index_name()),
                creation_time: created.unix_timestamp_nanos() as f64 / 1_000_000_000.0,
                data_type: dto::DataType::Float32,
                dimension: i32::try_from(index.dimension.get())
                    .map_err(|_| VectorServiceError::InvalidContext("validated dimension exceeds protocol integer"))?,
                distance_metric: public_metric(index.distance_metric),
                metadata_configuration: (!index.non_filterable_metadata_keys.is_empty()).then(|| dto::MetadataConfiguration {
                    non_filterable_metadata_keys: index.non_filterable_metadata_keys.into_iter().collect(),
                }),
                encryption_configuration: public_encryption(&entry.definition().effective_encryption),
                index_mode: Some(dto::IndexMode::Enhanced),
            },
        })
    }

    async fn delete_index(&self, context: &dyn VectorRequestAccess) -> Result<(), VectorServiceError> {
        let (guard, entry) = self.begin_index(context, None).await?;
        let managed = context.managed_dataset(guard.clone(), entry.definition()).await?;
        self.catalog
            .delete_vector_index_with_guard(&guard, &entry, new_commit_id()?)
            .await?;
        drop(managed);
        Ok(())
    }

    async fn put_vectors(
        &self,
        context: &dyn VectorRequestAccess,
        input: &dto::PutVectorsInput,
    ) -> Result<(), VectorServiceError> {
        let records = input
            .vectors
            .iter()
            .enumerate()
            .map(|(position, vector)| {
                let metadata = match &vector.metadata {
                    None => None,
                    Some(Value::Object(metadata)) => Some(metadata.clone()),
                    Some(_) => return Err(VectorServiceError::InvalidMetadataShape { position }),
                };
                Ok(VectorRecord {
                    key: vector.key.clone(),
                    data: vector.data.float32.clone(),
                    metadata,
                })
            })
            .collect::<Result<Vec<_>, VectorServiceError>>()?;
        validate_record_keys(&records)?;
        let mut expected_resource = None;
        for attempt in 0..self.max_write_attempts.get() {
            let bound = self.bind_index(context, expected_resource.as_ref()).await?;
            expected_resource.get_or_insert_with(|| bound.guard.resource_identity().clone());
            let request = mutation_request(&bound, "put-vectors")?;
            match bound.adapter.put_vectors(&bound.access, request, &records).await {
                Ok(_) => return Ok(()),
                Err(error) if error.is_definite_conflict() && attempt + 1 < self.max_write_attempts.get() => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(VectorServiceError::InvalidContext("write attempt budget must be positive"))
    }

    async fn delete_vectors(&self, context: &dyn VectorRequestAccess, keys: &[String]) -> Result<(), VectorServiceError> {
        let mut expected_resource = None;
        for attempt in 0..self.max_write_attempts.get() {
            let bound = self.bind_index(context, expected_resource.as_ref()).await?;
            expected_resource.get_or_insert_with(|| bound.guard.resource_identity().clone());
            let request = mutation_request(&bound, "delete-vectors")?;
            match bound.adapter.delete_vectors(&bound.access, request, keys).await {
                Ok(_) => return Ok(()),
                Err(error) if error.is_definite_conflict() && attempt + 1 < self.max_write_attempts.get() => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(VectorServiceError::InvalidContext("write attempt budget must be positive"))
    }

    async fn list_vectors(
        &self,
        context: &dyn VectorRequestAccess,
        input: &dto::ListVectorsInput,
    ) -> Result<dto::ListVectorsOutput, VectorServiceError> {
        let issued_at = context.pagination_now();
        let bound = self.bind_index(context, None).await?;
        let binding = self.page_binding(context, &bound)?;
        let max_results = usize::try_from(input.max_results.unwrap_or(500))
            .ok()
            .filter(|count| (1..=1000).contains(count))
            .ok_or_else(|| ProtocolError::validation("maxResults", "must be between 1 and 1000"))?;
        let (segments, segment) = match (input.segment_count, input.segment_index) {
            (None, None) => (1_u64, 0_u64),
            (Some(count), Some(index)) if (1..=16).contains(&count) && (0..count).contains(&index) => (
                u64::try_from(count).map_err(|_| VectorServiceError::InvalidContext("invalid segment count"))?,
                u64::try_from(index).map_err(|_| VectorServiceError::InvalidContext("invalid segment index"))?,
            ),
            _ => {
                return Err(
                    ProtocolError::validation("segmentIndex", "segmentCount and segmentIndex must form a valid pair").into(),
                );
            }
        };
        let (session, snapshot, mut offset) = if let Some(token) = &input.next_token {
            let (session, snapshot, position) = self.resume_page_session(context, &bound, token, &binding).await?;
            if !matches!(session.state.results, SessionResults::List) || position == 0 {
                return Err(PaginationError::invalid_token().into());
            }
            (Some(session), snapshot, position)
        } else {
            (None, bound.adapter.open_catalog_entry(&bound.access, &bound.entry).await?, 0)
        };
        let batch_size = NonZeroUsize::new(64).ok_or(VectorServiceError::InvalidContext("invalid scan batch size"))?;
        let start_offset = offset;
        let mut processed_bytes = 0_usize;
        let mut vectors = Vec::with_capacity(max_results);
        let more = 'scan: loop {
            let batch = snapshot
                .list_batch(
                    offset,
                    batch_size,
                    VectorProjection {
                        data: true,
                        metadata: true,
                    },
                )
                .await?;
            let exhausted = batch.len() < batch_size.get();
            for row in batch {
                let selected = offset % segments == segment;
                let mut vector = dto::ListOutputVector {
                    key: row.key,
                    data: row.data.map(|float32| dto::VectorData { float32 }),
                    metadata: row.metadata.map(Value::Object),
                };
                let row_bytes = serde_json::to_vec(&vector)
                    .map_err(|source| VectorServiceError::Bridge {
                        operation: "measure ListVectors processed data",
                        source: Box::new(source),
                    })?
                    .len();
                let next_bytes = processed_bytes.checked_add(row_bytes).ok_or(DatasetError::InvalidOffset)?;
                if next_bytes > 1024 * 1024 || (selected && vectors.len() == max_results) {
                    if offset == start_offset {
                        return Err(VectorServiceError::InvalidContext("one vector exceeds the ListVectors processing budget"));
                    }
                    break 'scan true;
                }
                processed_bytes = next_bytes;
                offset = offset.checked_add(1).ok_or(DatasetError::InvalidOffset)?;
                if selected {
                    if !input.return_data.unwrap_or(false) {
                        vector.data = None;
                    }
                    if !input.return_metadata.unwrap_or(false) {
                        vector.metadata = None;
                    }
                    vectors.push(vector);
                }
            }
            if exhausted {
                break false;
            }
        };
        let next_token = if more {
            let session = match session {
                Some(session) => session,
                None => {
                    let session = self.new_page_session(binding, &snapshot, issued_at, SessionResults::List)?;
                    self.persist_page_session(context, &bound, &session).await?;
                    session
                }
            };
            Some(session.token(offset)?)
        } else {
            None
        };
        bound.guard.check_held()?;
        Ok(dto::ListVectorsOutput { vectors, next_token })
    }

    async fn get_vectors(
        &self,
        context: &dyn VectorRequestAccess,
        input: &dto::GetVectorsInput,
    ) -> Result<dto::GetVectorsOutput, VectorServiceError> {
        let bound = self.bind_index(context, None).await?;
        let snapshot = bound.adapter.open_catalog_entry(&bound.access, &bound.entry).await?;
        let records = snapshot
            .get(
                &input.keys,
                VectorProjection {
                    data: input.return_data.unwrap_or(false),
                    metadata: input.return_metadata.unwrap_or(false),
                },
            )
            .await?;
        bound.guard.check_held()?;
        Ok(dto::GetVectorsOutput {
            vectors: records
                .into_iter()
                .map(|record| dto::GetOutputVector {
                    key: record.key,
                    data: record.data.map(|float32| dto::VectorData { float32 }),
                    metadata: record.metadata.map(Value::Object),
                })
                .collect(),
        })
    }

    async fn query_vectors(
        &self,
        context: &dyn VectorRequestAccess,
        input: &dto::QueryVectorsInput,
    ) -> Result<PreparedVectorQuery<B>, VectorServiceError> {
        let issued_at = context.pagination_now();
        let bound = self.bind_index(context, None).await?;
        let binding = self.page_binding(context, &bound)?;
        let (snapshot, candidates, next_token) = if let Some(token) = &input.next_token {
            let (session, snapshot, position) = self.resume_page_session(context, &bound, token, &binding).await?;
            let SessionResults::Query { matches } = &session.state.results else {
                return Err(PaginationError::invalid_token().into());
            };
            let start = usize::try_from(position).map_err(|_| PaginationError::invalid_token())?;
            if start == 0 || start >= matches.len() || !start.is_multiple_of(pagination::QUERY_PAGE_SIZE) {
                return Err(PaginationError::invalid_token().into());
            }
            let end = start.saturating_add(pagination::QUERY_PAGE_SIZE).min(matches.len());
            let candidates = matches[start..end]
                .iter()
                .map(|item| QueryMatch {
                    key: item.key.clone(),
                    row_id: item.row_id,
                    distance_f64: f64::from_bits(item.distance_bits),
                })
                .collect();
            let next = if end < matches.len() {
                Some(session.token(end as u64)?)
            } else {
                None
            };
            (snapshot, candidates, next)
        } else {
            let index = LanceManifestAdapter::<B>::index_from_catalog(&bound.entry)?;
            let filter = input
                .filter
                .as_ref()
                .map(|filter| ValidatedFilter::parse(filter, &index.non_filterable_metadata_keys))
                .transpose()?;
            let top_k = usize::try_from(input.top_k)
                .ok()
                .filter(|count| *count <= 10_000)
                .and_then(NonZeroUsize::new)
                .ok_or_else(|| ProtocolError::validation("topK", "must be between 1 and 10000"))?;
            let snapshot = bound.adapter.open_catalog_entry(&bound.access, &bound.entry).await?;
            #[cfg(test)]
            bound
                .adapter
                .pause_query_snapshot(&snapshot, &bound.entry.head().expected_head())
                .await;
            let mut candidates = snapshot.query_matches(&input.query_vector.float32, top_k, filter).await?;
            let next = if candidates.len() > pagination::QUERY_PAGE_SIZE {
                let matches = candidates
                    .iter()
                    .map(|candidate| StoredMatch {
                        key: candidate.key.clone(),
                        row_id: candidate.row_id,
                        distance_bits: candidate.distance_f64.to_bits(),
                    })
                    .collect();
                let session = self.new_page_session(binding, &snapshot, issued_at, SessionResults::Query { matches })?;
                self.persist_page_session(context, &bound, &session).await?;
                candidates.truncate(pagination::QUERY_PAGE_SIZE);
                Some(session.token(pagination::QUERY_PAGE_SIZE as u64)?)
            } else {
                None
            };
            (snapshot, candidates, next)
        };
        let metadata_rows = if input.return_metadata.unwrap_or(false) {
            let rows = snapshot
                .take_rows(
                    &candidates.iter().map(|candidate| candidate.row_id).collect::<Vec<_>>(),
                    VectorProjection {
                        data: false,
                        metadata: true,
                    },
                )
                .await?;
            if rows.len() != candidates.len() || rows.iter().zip(&candidates).any(|(row, candidate)| row.key != candidate.key) {
                return Err(VectorServiceError::InvalidContext("query row lookup differs from its fixed candidates"));
            }
            Some(rows)
        } else {
            None
        };
        bound.guard.check_held()?;
        Ok(PreparedVectorQuery {
            snapshot,
            candidates,
            metadata_rows,
            return_distance: input.return_distance.unwrap_or(false),
            next_token,
        })
    }
}

fn mutation_request<B: TableCatalogObjectBackend>(
    bound: &BoundIndex<B>,
    operation: &str,
) -> Result<ManifestPrepareRequest, VectorServiceError> {
    Ok(ManifestPrepareRequest {
        parts: CatalogCommitParts {
            resource_identity: bound.guard.resource_identity().clone(),
            namespace: bound.guard.namespace().clone(),
            name: bound.guard.name().clone(),
            expected_head: bound.entry.head().expected_head(),
            commit_id: new_commit_id()?,
            operation: operation.into(),
            idempotency_key: None,
            writer: None,
        },
        index: LanceManifestAdapter::<B>::index_from_catalog(&bound.entry)?,
    })
}

fn index_namespace() -> Result<Namespace, VectorServiceError> {
    Namespace::parse("indexes").map_err(|source| VectorServiceError::Bridge {
        operation: "construct Index namespace",
        source: Box::new(source),
    })
}

fn new_commit_id() -> Result<CatalogCommitId, VectorServiceError> {
    Ok(CatalogCommitId::try_new(Uuid::new_v4().to_string())?)
}

fn index_arn(bucket: &BucketHandle, name: &CatalogName) -> String {
    format!(
        "arn:aws:s3vectors:{}::bucket/{}/index/{}",
        bucket.region(),
        bucket.public_name().as_str(),
        name.as_str()
    )
}

fn create_index_spec(
    input: &dto::CreateIndexInput,
    encryption: &VectorEffectiveEncryptionV1,
) -> Result<VectorIndexManifestSpec, VectorServiceError> {
    let dimension = u32::try_from(input.dimension)
        .ok()
        .and_then(NonZeroU32::new)
        .filter(|value| value.get() <= 4096)
        .ok_or_else(|| ProtocolError::validation("dimension", "must be between 1 and 4096"))?;
    let keys = input
        .metadata_configuration
        .as_ref()
        .map(|config| validate_non_filterable_keys(&config.non_filterable_metadata_keys))
        .transpose()?
        .unwrap_or_default();
    Ok(VectorIndexManifestSpec {
        schema_version: 1,
        dimension,
        distance_metric: match input.distance_metric {
            dto::DistanceMetric::Cosine => VectorDistanceMetric::Cosine,
            dto::DistanceMetric::Euclidean => VectorDistanceMetric::Euclidean,
        },
        non_filterable_metadata_keys: keys,
        encryption_descriptor_sha256: encryption.descriptor_sha256()?,
    })
}

fn public_metric(metric: VectorDistanceMetric) -> dto::DistanceMetric {
    match metric {
        VectorDistanceMetric::Cosine => dto::DistanceMetric::Cosine,
        VectorDistanceMetric::Euclidean => dto::DistanceMetric::Euclidean,
    }
}

fn public_encryption(encryption: &VectorEffectiveEncryptionV1) -> Option<dto::EncryptionConfiguration> {
    match encryption {
        VectorEffectiveEncryptionV1::Unencrypted => None,
        VectorEffectiveEncryptionV1::Aes256 => Some(dto::EncryptionConfiguration {
            sse_type: Some(dto::SseType::Aes256),
            kms_key_arn: None,
        }),
        VectorEffectiveEncryptionV1::AwsKms { kms_key_arn, .. } => Some(dto::EncryptionConfiguration {
            sse_type: Some(dto::SseType::AwsKms),
            kms_key_arn: Some(kms_key_arn.clone()),
        }),
    }
}
