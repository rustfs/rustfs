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

use super::object::{
    ObjectTableCatalogStore, validate_namespace_entry_object, validate_table_entry_object, validate_view_entry_object,
};
use super::strong::{
    StrongCommitSnapshotRecord, StrongTableCatalogBucketSnapshot, StrongTableCatalogState, TableCatalogBackingMigrationFence,
    TableCatalogBackingMigrationFenceStatus, TableCatalogBackingMigrationGlobalFence, table_catalog_bucket_snapshot_fingerprint,
};
use super::*;

pub(super) fn table_catalog_backing_manifest(
    paths: &TableCatalogObjectPaths,
    namespace: &Namespace,
    table: &IdentifierSegment,
    entry: &TableEntry,
    commit_recovery: &TableCommitRecoveryReport,
) -> TableCatalogBackingManifest {
    let recovery_required = commit_recovery.staged_before_table_update_count > 0
        || commit_recovery.finalization_required_count > 0
        || commit_recovery.idempotency_repair_required_count > 0;
    let manual_review_required = commit_recovery.manual_review_count > 0;
    let wal_status = if manual_review_required {
        TableCatalogWalStatus::ManualReviewRequired
    } else if recovery_required {
        TableCatalogWalStatus::RecoveryRequired
    } else {
        TableCatalogWalStatus::Recoverable
    };
    let migration_status = if manual_review_required {
        TableCatalogBackingMigrationStatus::ManualReviewRequired
    } else if recovery_required {
        TableCatalogBackingMigrationStatus::RecoveryRequired
    } else {
        TableCatalogBackingMigrationStatus::ReadyToSnapshot
    };
    let mut blockers = Vec::new();
    if recovery_required {
        blockers.push(TableCatalogBackingMigrationBlocker::CommitRecoveryRequired);
    }
    if manual_review_required {
        blockers.push(TableCatalogBackingMigrationBlocker::CommitManualReviewRequired);
    }

    TableCatalogBackingManifest {
        version: TABLE_CATALOG_BACKING_MANIFEST_VERSION,
        current: TableCatalogBackingProfile {
            kind: TableCatalogBackingKind::ObjectBacked,
            authority: TableCatalogAuthority::RustfsSysObject,
            consistency: TableCatalogConsistencyMode::ConditionalObjectCas,
            durability: TableCatalogDurabilityMode::StagedCommitLogBeforePointerUpdate,
            current_pointer_path: paths.table_entry_path(&entry.table_bucket, namespace, table),
            wal: TableCatalogWalState {
                status: wal_status,
                commit_log_prefix: paths.commit_log_entries_prefix(&entry.table_bucket, &entry.table_id),
                idempotency_index_prefix: paths.commit_idempotency_entries_prefix(&entry.table_bucket, &entry.table_id),
                committed_generation: entry.generation,
                staged_before_table_update_count: commit_recovery.staged_before_table_update_count,
                finalization_required_count: commit_recovery.finalization_required_count,
                idempotency_repair_required_count: commit_recovery.idempotency_repair_required_count,
                manual_review_count: commit_recovery.manual_review_count,
            },
            snapshot: TableCatalogSnapshotState {
                export_api: "GET /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/export".to_string(),
                includes_table_bucket: true,
                includes_namespace: true,
                includes_table_pointer: true,
                includes_backing_manifest: true,
            },
        },
        migration: TableCatalogBackingMigrationPlan {
            source_kind: TableCatalogBackingKind::ObjectBacked,
            target_kind: TableCatalogBackingKind::StrongKvWal,
            status: migration_status,
            required_steps: vec![
                TableCatalogBackingMigrationStep::SnapshotCatalogExport,
                TableCatalogBackingMigrationStep::ReplayCommitLog,
                TableCatalogBackingMigrationStep::VerifyCurrentPointer,
                TableCatalogBackingMigrationStep::EnableSingleWriterFencing,
                TableCatalogBackingMigrationStep::CutOverLinearizableReads,
            ],
            blockers,
        },
        ha: TableCatalogHaPolicy {
            writer_region_model: TableCatalogHaWriterModel::SingleActiveWriterRegion,
            read_replica_strategy: TableCatalogReadReplicaStrategy::ReadOnlyReplicasForListAndLoad,
            commit_read_requirement: TableCatalogCommitReadRequirement::LinearizableLeaderRead,
            active_active_supported: false,
            failover_requires_operator_promotion: true,
        },
        scale_validation: TableCatalogScaleValidation {
            status: TableCatalogScaleValidationStatus::MatrixPublished,
            benchmark_required: true,
            required_scenarios: vec![
                TableCatalogScaleValidationScenario::ConcurrentCommitCas,
                TableCatalogScaleValidationScenario::CommitLogRecoveryReplay,
                TableCatalogScaleValidationScenario::MigrationSnapshotReplay,
                TableCatalogScaleValidationScenario::ReadReplicaStaleReadGuard,
                TableCatalogScaleValidationScenario::ClientConformanceMatrix,
            ],
        },
    }
}

impl<B> ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    async fn read_backing_migration_fence(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Option<(TableCatalogBackingMigrationFence, Option<String>)>> {
        self.read_entry(self.catalog_bucket(), &self.paths.backing_migration_fence_path(table_bucket))
            .await
    }

    pub(super) async fn acquire_table_bucket_registry_write_permit(&self) -> TableCatalogStoreResult<Box<dyn Send>> {
        let fence_path = self.paths.backing_migration_global_fence_path();
        let lock_path = self.paths.backing_migration_global_fence_lock_path();
        let guard = self.backend.acquire_read_lock(self.catalog_bucket(), &lock_path).await?;
        if self
            .read_entry::<TableCatalogBackingMigrationGlobalFence>(self.catalog_bucket(), &fence_path)
            .await?
            .is_some()
        {
            return Err(TableCatalogStoreError::Conflict(
                "table bucket registry writes are fenced while durable strong migration is in progress".to_string(),
            ));
        }
        Ok(guard)
    }

    pub(super) async fn acquire_object_backed_catalog_write_permit(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Box<dyn Send>> {
        let lock_path = self.paths.backing_migration_fence_lock_path(table_bucket);
        let guard = self.backend.acquire_read_lock(self.catalog_bucket(), &lock_path).await?;
        if self.read_backing_migration_fence(table_bucket).await?.is_some() {
            return Err(TableCatalogStoreError::Conflict(format!(
                "object-backed catalog writes are fenced while table bucket {table_bucket} is prepared for durable strong cutover"
            )));
        }
        Ok(guard)
    }

    async fn ensure_global_backing_migration_fence(
        &self,
        fence_path: &str,
    ) -> TableCatalogStoreResult<TableCatalogBackingMigrationGlobalFence> {
        if let Some((fence, _)) = self
            .read_entry::<TableCatalogBackingMigrationGlobalFence>(self.catalog_bucket(), fence_path)
            .await?
        {
            if fence.version != TABLE_CATALOG_MIGRATION_VERSION {
                return Err(TableCatalogStoreError::Invalid(
                    "invalid durable strong global migration fence".to_string(),
                ));
            }
            return Ok(fence);
        }
        let fence = TableCatalogBackingMigrationGlobalFence {
            version: TABLE_CATALOG_MIGRATION_VERSION,
            migration_id: Uuid::new_v4().to_string(),
        };
        self.write_entry(self.catalog_bucket(), fence_path, &fence, TableCatalogPutPrecondition::IfAbsent)
            .await?;
        Ok(fence)
    }

    async fn clear_global_backing_migration_fence_if_unused(&self, fence_path: &str) -> TableCatalogStoreResult<()> {
        let bucket_objects = self
            .backend
            .list_objects(self.catalog_bucket(), &self.paths.table_bucket_entries_prefix())
            .await?;
        if bucket_objects
            .iter()
            .any(|object| object.ends_with(TABLE_CATALOG_MIGRATION_FENCE_FILE))
        {
            return Ok(());
        }
        self.backend.delete_object(self.catalog_bucket(), fence_path).await
    }

    pub(super) async fn ensure_object_backed_writes_allowed(&self, table_bucket: &str) -> TableCatalogStoreResult<()> {
        if self
            .backend
            .object_exists(self.catalog_bucket(), &self.paths.backing_migration_fence_path(table_bucket))
            .await?
        {
            return Err(TableCatalogStoreError::Conflict(format!(
                "object-backed catalog writes are fenced while table bucket {table_bucket} is prepared for durable strong cutover"
            )));
        }
        Ok(())
    }

    async fn collect_bucket_snapshot_with_locks(
        &self,
        table_bucket: &str,
        guards: &mut Vec<Box<dyn Send>>,
    ) -> TableCatalogStoreResult<StrongTableCatalogBucketSnapshot> {
        let bucket_path = self.paths.table_bucket_entry_path(table_bucket);
        guards.push(self.backend.acquire_write_lock(self.catalog_bucket(), &bucket_path).await?);
        let Some((table_bucket_entry, _)) = self
            .read_entry_unlocked::<TableBucketEntry>(self.catalog_bucket(), &bucket_path)
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!("table bucket {table_bucket}")));
        };
        if table_bucket_entry.table_bucket != table_bucket {
            return Err(TableCatalogStoreError::Invalid(format!(
                "table bucket entry does not match migration target {table_bucket}"
            )));
        }

        let mut namespaces = Vec::new();
        let mut tables = Vec::new();
        let mut views = Vec::new();
        let mut commits = Vec::new();
        let mut idempotency = Vec::new();
        let namespace_objects = self
            .backend
            .list_objects(self.catalog_bucket(), &self.paths.namespace_entries_prefix(table_bucket))
            .await?;
        for namespace_object in namespace_objects
            .iter()
            .filter(|object| object.ends_with(NAMESPACE_ENTRY_FILE))
        {
            guards.push(
                self.backend
                    .acquire_write_lock(self.catalog_bucket(), namespace_object)
                    .await?,
            );
            let Some((namespace_entry, _)) = self
                .read_entry_unlocked::<NamespaceEntry>(self.catalog_bucket(), namespace_object)
                .await?
            else {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "namespace changed while preparing durable strong snapshot: {namespace_object}"
                )));
            };
            validate_namespace_entry_object(&self.paths, namespace_object, &namespace_entry)?;
            namespaces.push(namespace_entry);
        }

        for table_object in namespace_objects.iter().filter(|object| object.ends_with(TABLE_ENTRY_FILE)) {
            guards.push(self.backend.acquire_write_lock(self.catalog_bucket(), table_object).await?);
            let Some((table_entry, _)) = self
                .read_entry_unlocked::<TableEntry>(self.catalog_bucket(), table_object)
                .await?
            else {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "table changed while preparing durable strong snapshot: {table_object}"
                )));
            };
            validate_table_entry_object(&self.paths, table_object, &table_entry)?;

            for commit_object in self
                .backend
                .list_objects(
                    self.catalog_bucket(),
                    &self.paths.commit_log_entries_prefix(table_bucket, &table_entry.table_id),
                )
                .await?
                .into_iter()
                .filter(|object| object.ends_with(".json"))
            {
                let Some((commit, _)) = self
                    .read_entry_unlocked::<CommitLogEntry>(self.catalog_bucket(), &commit_object)
                    .await?
                else {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "commit log changed while preparing durable strong snapshot: {commit_object}"
                    )));
                };
                commits.push(StrongCommitSnapshotRecord {
                    table_bucket: table_bucket.to_string(),
                    table_id: table_entry.table_id.clone(),
                    lookup_key: commit.commit_id.clone(),
                    commit,
                });
            }
            for idempotency_object in self
                .backend
                .list_objects(
                    self.catalog_bucket(),
                    &self
                        .paths
                        .commit_idempotency_entries_prefix(table_bucket, &table_entry.table_id),
                )
                .await?
                .into_iter()
                .filter(|object| object.ends_with(".json"))
            {
                let Some((commit, _)) = self
                    .read_entry_unlocked::<CommitLogEntry>(self.catalog_bucket(), &idempotency_object)
                    .await?
                else {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "idempotency index changed while preparing durable strong snapshot: {idempotency_object}"
                    )));
                };
                let lookup_key = commit.idempotency_key.clone().ok_or_else(|| {
                    TableCatalogStoreError::Invalid(format!("idempotency index {idempotency_object} has no idempotency key"))
                })?;
                idempotency.push(StrongCommitSnapshotRecord {
                    table_bucket: table_bucket.to_string(),
                    table_id: table_entry.table_id.clone(),
                    lookup_key,
                    commit,
                });
            }
            tables.push(table_entry);
        }

        for view_object in namespace_objects.iter().filter(|object| object.ends_with(VIEW_ENTRY_FILE)) {
            guards.push(self.backend.acquire_write_lock(self.catalog_bucket(), view_object).await?);
            let Some((view_entry, _)) = self
                .read_entry_unlocked::<ViewEntry>(self.catalog_bucket(), view_object)
                .await?
            else {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "view changed while preparing durable strong snapshot: {view_object}"
                )));
            };
            validate_view_entry_object(&self.paths, view_object, &view_entry)?;
            views.push(view_entry);
        }

        namespaces.sort_by(|left, right| left.namespace.cmp(&right.namespace));
        tables.sort_by(|left, right| (&left.namespace, &left.table).cmp(&(&right.namespace, &right.table)));
        views.sort_by(|left, right| (&left.namespace, &left.view).cmp(&(&right.namespace, &right.view)));
        commits.sort_by(|left, right| (&left.table_id, &left.lookup_key).cmp(&(&right.table_id, &right.lookup_key)));
        idempotency.sort_by(|left, right| (&left.table_id, &left.lookup_key).cmp(&(&right.table_id, &right.lookup_key)));

        let snapshot = StrongTableCatalogBucketSnapshot {
            table_bucket: table_bucket_entry,
            namespaces,
            tables,
            views,
            commits,
            idempotency,
        };
        self.validate_bucket_snapshot_for_migration(&snapshot)?;
        Ok(snapshot)
    }

    fn validate_bucket_snapshot_for_migration(&self, snapshot: &StrongTableCatalogBucketSnapshot) -> TableCatalogStoreResult<()> {
        let table_bucket = &snapshot.table_bucket.table_bucket;
        let tables_by_id = snapshot
            .tables
            .iter()
            .map(|table| (table.table_id.as_str(), table))
            .collect::<BTreeMap<_, _>>();
        if tables_by_id.len() != snapshot.tables.len() {
            return Err(TableCatalogStoreError::Invalid(
                "migration snapshot contains duplicate table ids".to_string(),
            ));
        }
        let commits_by_key = snapshot
            .commits
            .iter()
            .map(|record| ((record.table_id.as_str(), record.lookup_key.as_str()), &record.commit))
            .collect::<BTreeMap<_, _>>();
        if commits_by_key.len() != snapshot.commits.len() {
            return Err(TableCatalogStoreError::Invalid(
                "migration snapshot contains duplicate commit lookup keys".to_string(),
            ));
        }
        let idempotency_by_key = snapshot
            .idempotency
            .iter()
            .map(|record| ((record.table_id.as_str(), record.lookup_key.as_str()), &record.commit))
            .collect::<BTreeMap<_, _>>();
        if idempotency_by_key.len() != snapshot.idempotency.len() {
            return Err(TableCatalogStoreError::Invalid(
                "migration snapshot contains duplicate idempotency lookup keys".to_string(),
            ));
        }
        for record in &snapshot.commits {
            let table = tables_by_id.get(record.table_id.as_str()).ok_or_else(|| {
                TableCatalogStoreError::Invalid(format!("commit {} has no table in migration snapshot", record.commit.commit_id))
            })?;
            if record.table_bucket != *table_bucket
                || record.commit.table_id != record.table_id
                || record.lookup_key != record.commit.commit_id
            {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "commit {} does not match its migration snapshot owner",
                    record.commit.commit_id
                )));
            }
            let indexed = record
                .commit
                .idempotency_key
                .as_deref()
                .and_then(|idempotency_key| idempotency_by_key.get(&(record.table_id.as_str(), idempotency_key)).copied());
            let recovery = table_commit_recovery_entry(table, &record.commit, indexed);
            if recovery.recovery_state != TableCommitRecoveryState::Committed {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "commit {} requires catalog recovery before durable strong migration",
                    record.commit.commit_id
                )));
            }
        }
        for record in &snapshot.idempotency {
            let _table = tables_by_id.get(record.table_id.as_str()).ok_or_else(|| {
                TableCatalogStoreError::Invalid(format!(
                    "idempotency index {} has no table in migration snapshot",
                    record.lookup_key
                ))
            })?;
            if record.table_bucket != *table_bucket || record.commit.table_id != record.table_id {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "idempotency index {} does not match its migration snapshot owner",
                    record.lookup_key
                )));
            }
            if record.commit.idempotency_key.as_deref() != Some(record.lookup_key.as_str()) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "idempotency index {} does not match its commit payload",
                    record.lookup_key
                )));
            }
            let committed = commits_by_key
                .get(&(record.table_id.as_str(), record.commit.commit_id.as_str()))
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid(format!(
                        "idempotency index {} has no commit record in migration snapshot",
                        record.lookup_key
                    ))
                })?;
            if *committed != &record.commit {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "idempotency index {} requires catalog recovery before durable strong migration",
                    record.lookup_key
                )));
            }
        }

        let mut state = StrongTableCatalogState::default();
        state.hydrated = true;
        StrongTableCatalogStore::<B>::insert_bucket_snapshot_locked(&mut state, snapshot.clone())?;
        if state.namespaces.len() != snapshot.namespaces.len()
            || state.tables.len() != snapshot.tables.len()
            || state.views.len() != snapshot.views.len()
            || state.commits.len() != snapshot.commits.len()
            || state.idempotency.len() != snapshot.idempotency.len()
        {
            return Err(TableCatalogStoreError::Invalid(
                "migration snapshot contains duplicate catalog identities".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn plan_durable_strong_backing_migration(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<TableCatalogBackingMigrationDryRunReport> {
        if self.get_table_bucket(table_bucket).await?.is_none() {
            return Err(TableCatalogStoreError::NotFound(format!("table bucket {table_bucket}")));
        }

        let namespace_objects = self
            .backend
            .list_objects(self.catalog_bucket(), &self.paths.namespace_entries_prefix(table_bucket))
            .await?;
        let mut namespace_count: usize = 0;
        let mut table_count: usize = 0;
        let mut view_count: usize = 0;
        let mut commit_log_count: usize = 0;
        let mut idempotency_index_count: usize = 0;
        let mut recovery_required_count: usize = 0;
        let mut manual_review_count: usize = 0;
        let mut warehouse_prefix_owners = BTreeMap::<String, usize>::new();

        for object in namespace_objects {
            if object.ends_with(NAMESPACE_ENTRY_FILE) {
                let Some((entry, _)) = self.read_entry::<NamespaceEntry>(self.catalog_bucket(), &object).await? else {
                    continue;
                };
                validate_namespace_entry_object(&self.paths, &object, &entry)?;
                namespace_count = namespace_count.saturating_add(1);
                continue;
            }
            if object.ends_with(VIEW_ENTRY_FILE) {
                let Some((entry, _)) = self.read_entry::<ViewEntry>(self.catalog_bucket(), &object).await? else {
                    continue;
                };
                validate_view_entry_object(&self.paths, &object, &entry)?;
                view_count = view_count.saturating_add(1);
                continue;
            }
            if !object.ends_with(TABLE_ENTRY_FILE) {
                continue;
            }

            let Some((table, _)) = self.read_entry::<TableEntry>(self.catalog_bucket(), &object).await? else {
                continue;
            };
            validate_table_entry_object(&self.paths, &object, &table)?;
            table_count = table_count.saturating_add(1);
            if table.state == TableCatalogEntryState::Active {
                let warehouse_prefix = table_warehouse_object_prefix(&table)?;
                warehouse_prefix_owners
                    .entry(warehouse_prefix)
                    .and_modify(|count| *count = count.saturating_add(1))
                    .or_insert(1);
            }

            let recovery = self.table_commit_recovery_report_for_entry(&table, 0).await?;
            commit_log_count = commit_log_count.saturating_add(recovery.commits.len());
            idempotency_index_count = idempotency_index_count.saturating_add(
                self.backend
                    .list_objects(
                        self.catalog_bucket(),
                        &self.paths.commit_idempotency_entries_prefix(table_bucket, &table.table_id),
                    )
                    .await?
                    .into_iter()
                    .filter(|object| object.ends_with(".json"))
                    .count(),
            );
            recovery_required_count = recovery_required_count
                .saturating_add(recovery.staged_before_table_update_count)
                .saturating_add(recovery.finalization_required_count)
                .saturating_add(recovery.idempotency_repair_required_count);
            manual_review_count = manual_review_count.saturating_add(recovery.manual_review_count);
        }

        let warehouse_index_ready = self.warehouse_index_ready(table_bucket).await?;
        let duplicate_warehouse_prefix_count = warehouse_prefix_owners.values().filter(|count| **count > 1).count();
        let mut blockers = Vec::new();
        let mut recommended_actions = Vec::new();
        if recovery_required_count > 0 {
            blockers.push(TableCatalogBackingMigrationBlocker::CommitRecoveryRequired);
        }
        if manual_review_count > 0 {
            blockers.push(TableCatalogBackingMigrationBlocker::CommitManualReviewRequired);
        }
        if recovery_required_count > 0 || manual_review_count > 0 {
            recommended_actions.push(TableCatalogBackingMigrationAction::RunCatalogRecovery);
        }
        if !warehouse_index_ready {
            blockers.push(TableCatalogBackingMigrationBlocker::WarehouseIndexBackfillRequired);
            recommended_actions.push(TableCatalogBackingMigrationAction::BackfillWarehouseIndex);
        }
        if duplicate_warehouse_prefix_count > 0 {
            blockers.push(TableCatalogBackingMigrationBlocker::DuplicateWarehousePrefix);
            recommended_actions.push(TableCatalogBackingMigrationAction::ReviewDuplicateWarehousePrefixes);
        }

        let mut status = if manual_review_count > 0 || duplicate_warehouse_prefix_count > 0 {
            TableCatalogBackingMigrationStatus::ManualReviewRequired
        } else if recovery_required_count > 0 || !warehouse_index_ready {
            TableCatalogBackingMigrationStatus::RecoveryRequired
        } else {
            TableCatalogBackingMigrationStatus::ReadyToSnapshot
        };

        let strong_store = StrongTableCatalogStore::new(self.backend.clone());
        let migration_fence = self.read_backing_migration_fence(table_bucket).await?.map(|(fence, _)| fence);
        let object_backed_writes_fenced = migration_fence.is_some();
        if status == TableCatalogBackingMigrationStatus::ReadyToSnapshot
            && let Some(fence) = migration_fence.as_ref()
            && fence.status == TableCatalogBackingMigrationFenceStatus::Materialized
            && let Some(source_fingerprint) = fence.source_fingerprint.as_deref()
        {
            if strong_store.bucket_snapshot_fingerprint(table_bucket).await?.as_deref() == Some(source_fingerprint) {
                status = TableCatalogBackingMigrationStatus::SnapshotMaterialized;
            } else {
                status = TableCatalogBackingMigrationStatus::ManualReviewRequired;
                blockers.push(TableCatalogBackingMigrationBlocker::DurableStrongSnapshotChanged);
                recommended_actions.push(TableCatalogBackingMigrationAction::ReviewDurableStrongSnapshot);
            }
        }

        let ready_to_enable_durable_strong = status == TableCatalogBackingMigrationStatus::SnapshotMaterialized
            && self.all_table_buckets_materialized(&strong_store).await?;
        if status == TableCatalogBackingMigrationStatus::ReadyToSnapshot {
            recommended_actions.extend([
                TableCatalogBackingMigrationAction::SnapshotObjectBackedCatalog,
                TableCatalogBackingMigrationAction::KeepObjectBackedRollbackConfig,
            ]);
        } else if status == TableCatalogBackingMigrationStatus::SnapshotMaterialized {
            recommended_actions.push(TableCatalogBackingMigrationAction::VerifyDurableStrongSnapshot);
            if ready_to_enable_durable_strong {
                recommended_actions.push(TableCatalogBackingMigrationAction::EnableDurableStrongBacking);
            } else {
                recommended_actions.push(TableCatalogBackingMigrationAction::SnapshotRemainingTableBuckets);
            }
            recommended_actions.push(TableCatalogBackingMigrationAction::KeepObjectBackedRollbackConfig);
        }

        Ok(TableCatalogBackingMigrationDryRunReport {
            table_bucket: table_bucket.to_string(),
            source_kind: TableCatalogBackingKind::ObjectBacked,
            target_kind: TableCatalogBackingKind::StrongKvWal,
            status,
            namespace_count,
            table_count,
            view_count,
            commit_log_count,
            idempotency_index_count,
            warehouse_prefix_count: warehouse_prefix_owners.len(),
            warehouse_index_ready,
            object_backed_writes_fenced,
            ready_to_enable_durable_strong,
            blockers,
            recommended_actions,
            rollback: TableCatalogBackingRollbackPlan {
                backing_config_key: ENV_TABLE_CATALOG_BACKING,
                current_backing_value: TABLE_CATALOG_BACKING_DURABLE_STRONG,
                rollback_backing_value: TABLE_CATALOG_BACKING_OBJECT,
                preserves_object_backed_catalog: true,
                requires_operator_restart: true,
            },
        })
    }

    pub(crate) async fn materialize_durable_strong_backing_migration(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<TableCatalogBackingMigrationExecutionReport> {
        let fence_path = self.paths.backing_migration_fence_path(table_bucket);
        let fence_lock_path = self.paths.backing_migration_fence_lock_path(table_bucket);
        let global_fence_path = self.paths.backing_migration_global_fence_path();
        let global_fence_lock_path = self.paths.backing_migration_global_fence_lock_path();
        if self.read_backing_migration_fence(table_bucket).await?.is_none() {
            let preflight = self.plan_durable_strong_backing_migration(table_bucket).await?;
            if preflight.status != TableCatalogBackingMigrationStatus::ReadyToSnapshot {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "table bucket {table_bucket} is not ready for durable strong snapshot materialization"
                )));
            }
        }

        let _global_fence_guard = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &global_fence_lock_path)
            .await?;
        let _fence_guard = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &fence_lock_path)
            .await?;
        let existing_fence = self.read_backing_migration_fence(table_bucket).await?;
        let strong_store = StrongTableCatalogStore::new(self.backend.clone());
        if let Some((fence, _)) = existing_fence.as_ref()
            && (fence.version != TABLE_CATALOG_MIGRATION_VERSION || fence.table_bucket != table_bucket)
        {
            return Err(TableCatalogStoreError::Invalid(format!(
                "invalid durable strong migration fence for table bucket {table_bucket}"
            )));
        }

        if !self.warehouse_index_ready(table_bucket).await? {
            return Err(TableCatalogStoreError::Conflict(format!(
                "table bucket {table_bucket} warehouse index must be backfilled before durable strong migration"
            )));
        }

        let mut source_guards = Vec::new();
        let source = self
            .collect_bucket_snapshot_with_locks(table_bucket, &mut source_guards)
            .await?;
        let source_fingerprint = table_catalog_bucket_snapshot_fingerprint(&source)?;
        if let Some((fence, _)) = existing_fence.as_ref()
            && fence.status == TableCatalogBackingMigrationFenceStatus::Materialized
            && fence.source_fingerprint.as_deref() != Some(source_fingerprint.as_str())
        {
            return Err(TableCatalogStoreError::Conflict(format!(
                "object-backed catalog state no longer matches the materialized snapshot for table bucket {table_bucket}"
            )));
        }

        self.ensure_global_backing_migration_fence(&global_fence_path).await?;
        let (migration_id, target_bucket_existed) = if let Some((fence, _)) = existing_fence.as_ref() {
            (fence.migration_id.clone(), fence.target_bucket_existed)
        } else {
            let target_bucket_existed = strong_store.bucket_snapshot_fingerprint(table_bucket).await?.is_some();
            let fence = TableCatalogBackingMigrationFence {
                version: TABLE_CATALOG_MIGRATION_VERSION,
                table_bucket: table_bucket.to_string(),
                migration_id: Uuid::new_v4().to_string(),
                status: TableCatalogBackingMigrationFenceStatus::Preparing,
                target_bucket_existed,
                source_fingerprint: None,
                target_snapshot_etag: None,
            };
            self.write_entry(self.catalog_bucket(), &fence_path, &fence, TableCatalogPutPrecondition::IfAbsent)
                .await?;
            (fence.migration_id, target_bucket_existed)
        };

        let (target_snapshot_etag, created) = strong_store.materialize_bucket_snapshot(source.clone()).await?;
        let completed_fence = TableCatalogBackingMigrationFence {
            version: TABLE_CATALOG_MIGRATION_VERSION,
            table_bucket: table_bucket.to_string(),
            migration_id,
            status: TableCatalogBackingMigrationFenceStatus::Materialized,
            target_bucket_existed,
            source_fingerprint: Some(source_fingerprint.clone()),
            target_snapshot_etag: Some(target_snapshot_etag.clone()),
        };
        self.write_entry(self.catalog_bucket(), &fence_path, &completed_fence, TableCatalogPutPrecondition::Any)
            .await?;

        drop(source_guards);
        drop(_fence_guard);
        let ready_to_enable_durable_strong = self.all_table_buckets_materialized(&strong_store).await?;
        Ok(TableCatalogBackingMigrationExecutionReport {
            table_bucket: table_bucket.to_string(),
            source_kind: TableCatalogBackingKind::ObjectBacked,
            target_kind: TableCatalogBackingKind::StrongKvWal,
            status: if created {
                TableCatalogBackingMigrationExecutionStatus::SnapshotMaterialized
            } else {
                TableCatalogBackingMigrationExecutionStatus::SnapshotAlreadyMaterialized
            },
            namespace_count: source.namespaces.len(),
            table_count: source.tables.len(),
            view_count: source.views.len(),
            commit_log_count: source.commits.len(),
            idempotency_index_count: source.idempotency.len(),
            source_fingerprint,
            target_snapshot_etag,
            object_backed_writes_fenced: true,
            ready_to_enable_durable_strong,
        })
    }

    pub(crate) async fn cancel_durable_strong_backing_migration(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<TableCatalogBackingMigrationCancelReport> {
        let fence_path = self.paths.backing_migration_fence_path(table_bucket);
        let fence_lock_path = self.paths.backing_migration_fence_lock_path(table_bucket);
        let global_fence_path = self.paths.backing_migration_global_fence_path();
        let global_fence_lock_path = self.paths.backing_migration_global_fence_lock_path();
        let _global_fence_guard = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &global_fence_lock_path)
            .await?;
        let _fence_guard = self
            .backend
            .acquire_write_lock(self.catalog_bucket(), &fence_lock_path)
            .await?;
        let Some((fence, _)) = self.read_backing_migration_fence(table_bucket).await? else {
            self.clear_global_backing_migration_fence_if_unused(&global_fence_path)
                .await?;
            return Ok(TableCatalogBackingMigrationCancelReport {
                table_bucket: table_bucket.to_string(),
                status: TableCatalogBackingMigrationCancelStatus::NoMigrationFence,
                object_backed_writes_fenced: false,
            });
        };
        self.ensure_global_backing_migration_fence(&global_fence_path).await?;
        if fence.version != TABLE_CATALOG_MIGRATION_VERSION || fence.table_bucket != table_bucket {
            return Err(TableCatalogStoreError::Invalid(format!(
                "invalid durable strong migration fence for table bucket {table_bucket}"
            )));
        }

        let mut source_guards = Vec::new();
        let source = self
            .collect_bucket_snapshot_with_locks(table_bucket, &mut source_guards)
            .await?;
        let source_fingerprint = table_catalog_bucket_snapshot_fingerprint(&source)?;
        if fence.status == TableCatalogBackingMigrationFenceStatus::Materialized
            && fence.source_fingerprint.as_deref() != Some(source_fingerprint.as_str())
        {
            return Err(TableCatalogStoreError::Conflict(format!(
                "object-backed catalog state changed after materializing table bucket {table_bucket}"
            )));
        }

        let strong_store = StrongTableCatalogStore::new(self.backend.clone());
        if fence.status == TableCatalogBackingMigrationFenceStatus::Materialized
            && strong_store.bucket_snapshot_fingerprint(table_bucket).await?.as_deref() != Some(&source_fingerprint)
        {
            return Err(TableCatalogStoreError::Conflict(format!(
                "durable strong catalog state changed after materializing table bucket {table_bucket}"
            )));
        }
        if !fence.target_bucket_existed {
            strong_store
                .remove_bucket_snapshot_if_unchanged(table_bucket, &source_fingerprint)
                .await?;
        }
        self.backend.delete_object(self.catalog_bucket(), &fence_path).await?;
        self.clear_global_backing_migration_fence_if_unused(&global_fence_path)
            .await?;
        Ok(TableCatalogBackingMigrationCancelReport {
            table_bucket: table_bucket.to_string(),
            status: TableCatalogBackingMigrationCancelStatus::FenceReleased,
            object_backed_writes_fenced: false,
        })
    }

    async fn all_table_buckets_materialized(&self, strong_store: &StrongTableCatalogStore<B>) -> TableCatalogStoreResult<bool> {
        if self
            .read_entry::<TableCatalogBackingMigrationGlobalFence>(
                self.catalog_bucket(),
                &self.paths.backing_migration_global_fence_path(),
            )
            .await?
            .is_none()
        {
            return Ok(false);
        }
        let table_bucket_objects = self
            .backend
            .list_objects(self.catalog_bucket(), &self.paths.table_bucket_entries_prefix())
            .await?;
        for table_bucket_object in table_bucket_objects
            .iter()
            .filter(|object| object.ends_with(TABLE_BUCKET_ENTRY_FILE))
        {
            let Some((entry, _)) = self
                .read_entry::<TableBucketEntry>(self.catalog_bucket(), table_bucket_object)
                .await?
            else {
                return Ok(false);
            };
            let Some((fence, _)) = self.read_backing_migration_fence(&entry.table_bucket).await? else {
                return Ok(false);
            };
            if fence.status != TableCatalogBackingMigrationFenceStatus::Materialized {
                return Ok(false);
            }
            let Some(source_fingerprint) = fence.source_fingerprint.as_deref() else {
                return Ok(false);
            };
            if strong_store
                .bucket_snapshot_fingerprint(&entry.table_bucket)
                .await?
                .as_deref()
                != Some(source_fingerprint)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
