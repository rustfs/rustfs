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

use super::super::*;
use crate::table_catalog::store::TableCommitLockPublication;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableCommitRequest {
    pub table_bucket: String,
    pub namespace: String,
    pub table: String,
    pub commit_id: String,
    pub idempotency_key: Option<String>,
    pub operation: String,
    pub expected_version_token: String,
    pub expected_metadata_location: String,
    pub new_metadata_location: String,
    #[serde(default)]
    pub requirements: Vec<serde_json::Value>,
    pub writer: Option<String>,
}

pub(crate) fn commit_log_matches_request(commit_log: &CommitLogEntry, request: &TableCommitRequest, table_id: &str) -> bool {
    commit_log.version == TABLE_CATALOG_ENTRY_VERSION
        && commit_log.commit_id == request.commit_id
        && commit_log.idempotency_key == request.idempotency_key
        && commit_log.table_id == table_id
        && commit_log.operation == request.operation
        && commit_log.expected_version_token == request.expected_version_token
        && commit_log.previous_metadata_location == request.expected_metadata_location
        && commit_log.new_metadata_location == request.new_metadata_location
        && commit_log.requirements == request.requirements
        && commit_log.writer == request.writer
}

pub(crate) fn table_catalog_recovery_summary(
    metadata_status: &TableMetadataPointerStatus,
    commit_recovery: &TableCommitRecoveryReport,
) -> (TableCatalogRecoveryStatus, Vec<TableCatalogRecoveryAction>) {
    let mut actions = Vec::new();
    let metadata_status = match metadata_status {
        TableMetadataPointerStatus::Valid => None,
        TableMetadataPointerStatus::MissingObject => {
            actions.push(TableCatalogRecoveryAction::RestoreCurrentMetadataObject);
            Some(TableCatalogRecoveryStatus::ReadOnlyRecommended)
        }
        TableMetadataPointerStatus::InvalidJson => {
            actions.push(TableCatalogRecoveryAction::FixCurrentMetadataJson);
            Some(TableCatalogRecoveryStatus::ReadOnlyRecommended)
        }
        TableMetadataPointerStatus::InvalidLocation => {
            actions.push(TableCatalogRecoveryAction::MoveCurrentMetadataInsideTable);
            Some(TableCatalogRecoveryStatus::ReadOnlyRecommended)
        }
    };

    if commit_recovery.manual_review_count > 0 {
        actions.push(TableCatalogRecoveryAction::ReviewCommitLog);
        return (metadata_status.unwrap_or(TableCatalogRecoveryStatus::ManualReviewRequired), actions);
    }
    if commit_recovery.finalization_required_count > 0 || commit_recovery.idempotency_repair_required_count > 0 {
        actions.push(TableCatalogRecoveryAction::RunCommitRecovery);
        return (metadata_status.unwrap_or(TableCatalogRecoveryStatus::Recoverable), actions);
    }
    if commit_recovery.staged_before_table_update_count > 0 {
        actions.push(TableCatalogRecoveryAction::RetryCommit);
        return (metadata_status.unwrap_or(TableCatalogRecoveryStatus::Recoverable), actions);
    }

    (metadata_status.unwrap_or(TableCatalogRecoveryStatus::Healthy), actions)
}

/// Validate the exact base, candidate, and graph evidence under publication protection.
async fn prepare_iceberg_commit<B: TableCatalogObjectBackend>(
    backend: &B,
    reader: &dyn CatalogObjectReader,
    graph: Option<(&ValidatedIcebergSnapshotGraph, &(dyn IcebergGraphPublication + Sync))>,
    current: &TableEntry,
    bucket_incarnation: Uuid,
    request: TableCommitRequest,
) -> TableCatalogStoreResult<PreparedCatalogCommit> {
    if current.version != TABLE_CATALOG_ENTRY_VERSION
        || current.vector_head_binding.is_some()
        || !current.format.eq_ignore_ascii_case("iceberg")
        || request.table_bucket != current.table_bucket
        || request.namespace != current.namespace
        || request.table != current.table
    {
        return Err(TableCatalogStoreError::Invalid(
            "Iceberg preparation requires the matching Table entry".to_string(),
        ));
    }
    if !is_valid_table_metadata_location_for_entry(current, &request.new_metadata_location)
        || (!request.expected_metadata_location.is_empty()
            && !is_valid_table_metadata_location_for_entry(current, &request.expected_metadata_location))
    {
        return Err(TableCatalogStoreError::Invalid(
            "metadata location must be inside the table metadata directory".to_string(),
        ));
    }
    let candidate_object = reader
        .read_object_limited(&request.table_bucket, &request.new_metadata_location, TABLE_METADATA_JSON_MAX_SIZE)
        .await?
        .ok_or_else(|| TableCatalogStoreError::NotFound(format!("new metadata object {}", request.new_metadata_location)))?;
    let base_object = if request.expected_metadata_location.is_empty() {
        None
    } else {
        Some(
            reader
                .read_object_limited(&request.table_bucket, &request.expected_metadata_location, TABLE_METADATA_JSON_MAX_SIZE)
                .await?
                .ok_or_else(|| {
                    TableCatalogStoreError::NotFound(format!("base metadata object {}", request.expected_metadata_location))
                })?,
        )
    };
    let candidate_ref =
        CatalogMetadataRef::from_read(CatalogObjectKey::try_new(request.new_metadata_location.clone())?, &candidate_object)?;
    let base_ref = base_object
        .as_ref()
        .map(|object| {
            CatalogMetadataRef::from_read(CatalogObjectKey::try_new(request.expected_metadata_location.clone())?, object)
        })
        .transpose()?;
    let parse_request = request.clone();
    let (base, candidate, state) = tokio::task::spawn_blocking(move || {
        validate_commit_metadata_digest(&parse_request, &candidate_object)?;
        let candidate = decode_table_metadata_json(&parse_request.new_metadata_location, &candidate_object.data)?;
        validate_supported_table_metadata(&candidate)?;
        let state =
            table_metadata_commit_state(&parse_request.table_bucket, &parse_request.new_metadata_location, &candidate_object)?;
        let base = base_object
            .map(|object| decode_table_metadata_json(&parse_request.expected_metadata_location, &object.data))
            .transpose()?;
        Ok::<_, TableCatalogStoreError>((base, candidate, state))
    })
    .await
    .map_err(|err| TableCatalogStoreError::Internal(format!("Iceberg preparation parser task failed: {err}")))??;
    let client_requirements = request
        .requirements
        .iter()
        .filter(|requirement| {
            requirement.get("type").and_then(serde_json::Value::as_str) != Some(TABLE_METADATA_DIGEST_REQUIREMENT_TYPE)
        })
        .cloned()
        .collect::<Vec<_>>();
    if let Some(base) = &base {
        validate_supported_table_metadata(base)?;
        validate_table_commit_requirements(base, &client_requirements)?;
        validate_table_metadata_transition(base, &candidate)?;
        if table_metadata_uuid(base)? != table_metadata_uuid(&candidate)? {
            return Err(TableCatalogStoreError::Conflict("table uuid changed across metadata commit".to_string()));
        }
    } else if !client_requirements.is_empty() {
        return Err(TableCatalogStoreError::Invalid("commit requirements need base metadata".to_string()));
    }
    if table_metadata_uuid(&candidate)? != current.table_uuid {
        return Err(TableCatalogStoreError::Conflict(
            "metadata table uuid does not match catalog identity".to_string(),
        ));
    }
    let mut target = current.clone();
    if let Some(warehouse) = &state.warehouse_location {
        target.warehouse_location.clone_from(warehouse);
    }
    if let Some(version) = state.format_version {
        target.format_version = version;
    }
    if let Some((graph, publication)) = graph {
        graph.validate_for_commit(&target, base.as_ref(), &candidate, publication)?;
    } else {
        validate_table_snapshot_changes(
            &TableSnapshotGraphValidationContext::with_reader(backend, &current.table_bucket, &target, reader),
            base.as_ref(),
            &candidate,
        )
        .await?;
    }
    let descriptor = iceberg_format_descriptor(candidate_ref.clone(), &candidate, &request.requirements)?;
    let base_descriptor = base_ref
        .as_ref()
        .zip(base.as_ref())
        .map(|(reference, metadata)| iceberg_format_descriptor(reference.clone(), metadata, &request.requirements))
        .transpose()?;
    let mut expected = current.clone();
    expected.version_token.clone_from(&request.expected_version_token);
    expected.metadata_location.clone_from(&request.expected_metadata_location);
    let parts = CatalogCommitParts {
        resource_identity: CatalogResourceIdentity::try_table(
            current.table_bucket.clone(),
            bucket_incarnation,
            crate::storage_api::table::managed::ManagedTableId::try_new(current.table_id.clone())
                .map_err(|err| TableCatalogStoreError::Invalid(err.to_string()))?,
        )?,
        namespace: parse_namespace_for_store(&request.namespace)?,
        name: CatalogName::from(&parse_table_for_store(&request.table)?),
        expected_head: CatalogExpectedHead::from_entry(&expected, base_ref, base_descriptor)?,
        commit_id: CatalogCommitId::try_new(request.commit_id)?,
        operation: request.operation,
        idempotency_key: request.idempotency_key,
        writer: request.writer,
    };
    PreparedCatalogCommit::try_from_validated_parts(
        parts,
        candidate_ref,
        descriptor,
        CatalogEntryUpdates::try_new(state.warehouse_location, state.format_version)?,
        serde_json::to_vec(&request.requirements).map_err(|err| {
            TableCatalogStoreError::Invalid(format!("failed to encode Iceberg compatibility requirements: {err}"))
        })?,
    )
}

fn iceberg_format_descriptor(
    reference: CatalogMetadataRef,
    metadata: &serde_json::Value,
    requirements: &[serde_json::Value],
) -> TableCatalogStoreResult<CatalogFormatDescriptor> {
    let version = table_metadata_format_version(metadata)?;
    let payload = serde_json::to_vec(&serde_json::json!({
        "canonical_json_sha256": canonical_json_sha256(metadata)?,
        "requirements": requirements,
        "table_uuid": table_metadata_uuid(metadata)?,
        "format_version": version,
    }))
    .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to encode Iceberg descriptor: {err}")))?;
    CatalogFormatDescriptor::try_new(1, CatalogFormat::Iceberg, version, reference, payload)
}

#[async_trait::async_trait]
pub(crate) trait IcebergCatalogCommit: Send + Sync {
    #[cfg(test)]
    async fn commit_table(&self, request: TableCommitRequest) -> TableCatalogStoreResult<TableCommitResult>;
    async fn commit_table_with_publication(
        &self,
        request: TableCommitRequest,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<TableCommitResult>;
    async fn commit_table_with_validated_graph(
        &self,
        request: TableCommitRequest,
        graph: ValidatedIcebergSnapshotGraph,
        publication: &(dyn IcebergGraphPublication + Sync),
    ) -> TableCatalogStoreResult<TableCommitResult>;

    async fn replace_view(&self, request: ViewCommitRequest) -> TableCatalogStoreResult<ViewCommitResult>;

    async fn replace_view_with_publication(
        &self,
        request: ViewCommitRequest,
        table_bucket_fence_required: bool,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<ViewCommitResult> {
        if table_bucket_fence_required {
            publication.begin_table_bucket(&request.table_bucket).await?;
            if !publication.holds_table_bucket(&request.table_bucket) {
                return Err(TableCatalogStoreError::Internal(
                    "view replacement requires a table-bucket publication fence".to_string(),
                ));
            }
        }
        publication
            .prepare(&request.table_bucket, &request.namespace, &request.view)
            .await?;
        if !publication.holds_table(&request.table_bucket, &request.namespace, &request.view) {
            return Err(TableCatalogStoreError::Internal(
                "view replacement requires a view publication fence".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(publication);
        self.replace_view(request).await
    }
}

struct IcebergCommitPreparation<'a, B> {
    backend: &'a B,
    request: TableCommitRequest,
    graph: Option<(&'a ValidatedIcebergSnapshotGraph, &'a (dyn IcebergGraphPublication + Sync))>,
}

#[async_trait::async_trait]
impl<B: TableCatalogObjectBackend> CatalogCommitPreparation for IcebergCommitPreparation<'_, B> {
    async fn prepare(
        &self,
        current: &TableEntry,
        reader: &dyn CatalogObjectReader,
    ) -> TableCatalogStoreResult<PreparedCatalogCommit> {
        let incarnation = self.backend.read_table_bucket_incarnation(&self.request.table_bucket).await?;
        prepare_iceberg_commit(self.backend, reader, self.graph, current, incarnation, self.request.clone()).await
    }
}

#[async_trait::async_trait]
impl<B: TableCatalogObjectBackend> IcebergCatalogCommit for ObjectTableCatalogStore<B> {
    #[cfg(test)]
    async fn commit_table(&self, request: TableCommitRequest) -> TableCatalogStoreResult<TableCommitResult> {
        let publication = crate::table_catalog::store::TableCommitLockPublication::new(&self.backend, &self.paths);
        self.commit_table_with_publication(request, &publication).await
    }
    async fn commit_table_with_publication(
        &self,
        request: TableCommitRequest,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<TableCommitResult> {
        self.commit_iceberg_with_graph(request, publication, None).await
    }

    async fn commit_table_with_validated_graph(
        &self,
        request: TableCommitRequest,
        graph: ValidatedIcebergSnapshotGraph,
        publication: &(dyn IcebergGraphPublication + Sync),
    ) -> TableCatalogStoreResult<TableCommitResult> {
        self.commit_iceberg_with_graph(request, publication, Some((&graph, publication)))
            .await
    }

    async fn replace_view(&self, request: ViewCommitRequest) -> TableCatalogStoreResult<ViewCommitResult> {
        let publication = TableCommitLockPublication::new(&self.backend, &self.paths);
        self.replace_view_with_publication(request, true, &publication).await
    }

    async fn replace_view_with_publication(
        &self,
        request: ViewCommitRequest,
        table_bucket_fence_required: bool,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<ViewCommitResult> {
        let namespace = parse_namespace_for_store(&request.namespace)?;
        let view = parse_table_for_store(&request.view)?;
        let _migration_guard = if table_bucket_fence_required {
            self.begin_table_bucket_with_migration_permit(&request.table_bucket, publication)
                .await?
        } else {
            match publication.catalog_migration_read_permit_status() {
                Some(true) => {
                    self.ensure_object_backed_catalog_write_permit_after_lock(&request.table_bucket)
                        .await?;
                    None
                }
                Some(false) => {
                    return Err(TableCatalogStoreError::Conflict(
                        "table-bucket catalog migration read permit was lost".to_string(),
                    ));
                }
                None => Some(self.acquire_object_backed_catalog_write_permit(&request.table_bucket).await?),
            }
        };
        if table_bucket_fence_required && !publication.holds_table_bucket(&request.table_bucket) {
            return Err(TableCatalogStoreError::Internal(
                "view replacement requires a table-bucket publication fence".to_string(),
            ));
        }
        if publication.holds_table_bucket(&request.table_bucket) {
            self.recover_active_table_rename(&request.table_bucket, publication).await?;
        } else {
            self.ensure_no_active_table_rename(&request.table_bucket).await?;
        }
        let namespace_path = self.paths.namespace_entry_path(&request.table_bucket, &namespace);
        let _namespace_guard = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &namespace_path)
            .await?;
        let view_path = self.paths.view_entry_path(&request.table_bucket, &namespace, &view);
        let _guard = self.backend.acquire_write_lock(self.catalog_bucket(), &view_path).await?;
        // Preserve catalog -> publication -> object lock order across rolling upgrades.
        publication
            .prepare(&request.table_bucket, &request.namespace, &request.view)
            .await?;
        if !publication.holds_table(&request.table_bucket, &request.namespace, &request.view) {
            return Err(TableCatalogStoreError::Internal(
                "view replacement requires a table publication fence".to_string(),
            ));
        }
        let _publication_completion = TableCommitPublicationCompletion::new(publication);
        let Some((current, current_etag)) = self
            .read_view_with_etag_unlocked(&request.table_bucket, &namespace, &view)
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "view {}/{}/{}",
                request.table_bucket, request.namespace, request.view
            )));
        };
        if current.state != TableCatalogEntryState::Active {
            return Err(TableCatalogStoreError::NotFound(format!(
                "view {}/{}/{}",
                request.table_bucket, request.namespace, request.view
            )));
        }
        if current.version_token != request.expected_version_token {
            return Err(TableCatalogStoreError::Conflict(
                "current view version token does not match expected token".to_string(),
            ));
        }
        if current.metadata_location != request.expected_metadata_location {
            return Err(TableCatalogStoreError::Conflict(
                "current view metadata location does not match expected location".to_string(),
            ));
        }
        if !is_valid_view_metadata_location(&namespace, &view, &request.new_metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "new metadata location must be inside the view metadata directory".to_string(),
            ));
        }
        let Some(new_metadata_object) = self
            .backend
            .read_object_limited(&request.table_bucket, &request.new_metadata_location, TABLE_METADATA_JSON_MAX_SIZE)
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "new view metadata object {}",
                request.new_metadata_location
            )));
        };
        let table_bucket = request.table_bucket.clone();
        let metadata_location = request.new_metadata_location.clone();
        let next_warehouse_location = tokio::task::spawn_blocking(move || {
            view_metadata_warehouse_location(&table_bucket, &metadata_location, &new_metadata_object)
        })
        .await
        .map_err(|err| TableCatalogStoreError::Internal(format!("view metadata parser task failed: {err}")))??;
        let warehouse_relocation = next_warehouse_location
            .as_deref()
            .is_some_and(|location| location != current.warehouse_location);
        if warehouse_relocation && !publication.holds_table_bucket(&request.table_bucket) {
            return Err(TableCatalogStoreError::Internal(
                "view warehouse relocation requires a table-bucket publication fence".to_string(),
            ));
        }

        let mut next = current;
        next.metadata_location = request.new_metadata_location;
        if let Some(warehouse_location) = next_warehouse_location {
            next.warehouse_location = warehouse_location;
        }
        next.version_token = format!("token-{}", Uuid::new_v4());
        next.generation = next.generation.saturating_add(1);
        if !publication.holds_table(&request.table_bucket, &request.namespace, &request.view)
            || ((table_bucket_fence_required || warehouse_relocation) && !publication.holds_table_bucket(&request.table_bucket))
        {
            return Err(TableCatalogStoreError::Internal(
                "view replacement publication fence was lost before catalog update".to_string(),
            ));
        }
        let write_result = self
            .write_entry_unlocked(
                self.catalog_bucket(),
                &view_path,
                &next,
                TableCatalogPutPrecondition::IfMatch(current_etag),
                _guard.write_commit_guards(),
            )
            .await;
        if let Err(err) = write_result {
            match self
                .read_view_with_etag_unlocked(&request.table_bucket, &namespace, &view)
                .await
            {
                Ok(Some((persisted, _))) if persisted == next => {}
                Ok(_) => return Err(err),
                Err(read_err) => {
                    tracing::warn!(
                        table_bucket = %request.table_bucket,
                        namespace = %request.namespace,
                        view = %request.view,
                        error = %read_err,
                        "failed to verify view state after an ambiguous catalog update"
                    );
                    return Err(err);
                }
            }
        }
        Ok(ViewCommitResult { view: next })
    }
}

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    async fn commit_iceberg_with_graph(
        &self,
        request: TableCommitRequest,
        publication: &(dyn TableCommitPublication + Sync),
        graph: Option<(&ValidatedIcebergSnapshotGraph, &(dyn IcebergGraphPublication + Sync))>,
    ) -> TableCatalogStoreResult<TableCommitResult> {
        let compatibility =
            CatalogJsonArray::from_canonical_bytes(&serde_json::to_vec(&request.requirements).map_err(|err| {
                TableCatalogStoreError::Invalid(format!("failed to encode Iceberg compatibility requirements: {err}"))
            })?)?;
        let fields = CatalogCommitFields {
            table_bucket: request.table_bucket.clone(),
            namespace: request.namespace.clone(),
            table: request.table.clone(),
            commit_id: request.commit_id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            operation: request.operation.clone(),
            expected_version_token: request.expected_version_token.clone(),
            expected_metadata_location: request.expected_metadata_location.clone(),
            new_metadata_location: request.new_metadata_location.clone(),
            writer: request.writer.clone(),
        };
        let preparation = IcebergCommitPreparation {
            backend: &self.backend,
            request,
            graph,
        };
        self.commit_catalog_with_preparation(
            CatalogCommitInput::Pending {
                fields: Box::new(fields),
                compatibility,
                preparation: &preparation,
            },
            publication,
        )
        .await
    }
}

#[async_trait::async_trait]
impl<B: TableCatalogObjectBackend> IcebergCatalogCommit for ConfiguredTableCatalogStore<B> {
    #[cfg(test)]
    async fn commit_table(&self, request: TableCommitRequest) -> TableCatalogStoreResult<TableCommitResult> {
        match self {
            Self::ObjectBacked(store) => store.commit_table(request).await,
        }
    }
    async fn commit_table_with_publication(
        &self,
        request: TableCommitRequest,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<TableCommitResult> {
        match self {
            Self::ObjectBacked(store) => store.commit_table_with_publication(request, publication).await,
        }
    }
    async fn commit_table_with_validated_graph(
        &self,
        request: TableCommitRequest,
        graph: ValidatedIcebergSnapshotGraph,
        publication: &(dyn IcebergGraphPublication + Sync),
    ) -> TableCatalogStoreResult<TableCommitResult> {
        match self {
            Self::ObjectBacked(store) => store.commit_table_with_validated_graph(request, graph, publication).await,
        }
    }
    async fn replace_view(&self, request: ViewCommitRequest) -> TableCatalogStoreResult<ViewCommitResult> {
        match self {
            Self::ObjectBacked(store) => store.replace_view(request).await,
        }
    }

    async fn replace_view_with_publication(
        &self,
        request: ViewCommitRequest,
        table_bucket_fence_required: bool,
        publication: &(dyn TableCommitPublication + Sync),
    ) -> TableCatalogStoreResult<ViewCommitResult> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .replace_view_with_publication(request, table_bucket_fence_required, publication)
                    .await
            }
        }
    }
}

#[cfg(test)]
impl PreparedCatalogCommit {
    pub(in crate::table_catalog) fn legacy_request(&self) -> TableCatalogStoreResult<TableCommitRequest> {
        let fields = CatalogCommitFields::from_prepared(self);
        Ok(TableCommitRequest {
            table_bucket: fields.table_bucket,
            namespace: fields.namespace,
            table: fields.table,
            commit_id: fields.commit_id,
            idempotency_key: fields.idempotency_key,
            operation: fields.operation,
            expected_version_token: fields.expected_version_token,
            expected_metadata_location: fields.expected_metadata_location,
            new_metadata_location: fields.new_metadata_location,
            requirements: CatalogJsonArray::from_canonical_bytes(self.legacy_requirements())?.into_values(),
            writer: fields.writer,
        })
    }
}
