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

use crate::table_catalog::store::TableCommitLockPublication;
use crate::table_catalog::store::object::{
    TABLE_REGISTRATION_READ_CONCURRENCY, ensure_table_maintenance_publication_held, table_maintenance_mutation_fence,
    validate_namespace_entry_object, validate_table_bucket_entry_object, validate_table_entry_object,
    validate_table_maintenance_report_owner,
};
use crate::table_catalog::*;

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn plan_table_snapshot_expiration(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        config: TableSnapshotExpirationConfig,
    ) -> TableCatalogStoreResult<TableSnapshotExpirationReport> {
        self.plan_table_snapshot_expiration_with_backend(&self.backend, table_bucket, namespace, table, config)
            .await
    }

    pub(crate) async fn plan_table_snapshot_expiration_with_backend<P>(
        &self,
        metadata_backend: &P,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        config: TableSnapshotExpirationConfig,
    ) -> TableCatalogStoreResult<TableSnapshotExpirationReport>
    where
        P: TableCatalogObjectBackend,
    {
        self.with_table_maintenance_read(table_bucket, async {
            validate_table_snapshot_expiration_config(&config)?;
            let namespace = parse_namespace_for_store(namespace)?;
            let table = parse_table_for_store(table)?;
            let Some((entry, _)) = self.read_table_with_etag(table_bucket, &namespace, &table).await? else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    table_bucket,
                    namespace.public_name(),
                    table.as_str()
                )));
            };
            if !is_valid_table_metadata_location(&namespace, &table, &entry.metadata_location) {
                return Err(TableCatalogStoreError::Invalid(
                    "current metadata location must be inside the table metadata directory".to_string(),
                ));
            }

            self.plan_table_snapshot_expiration_for_entry_with_backend(metadata_backend, &entry, config)
                .await
        })
        .await
    }

    pub(in crate::table_catalog) async fn plan_table_snapshot_expiration_for_entry_with_backend<P>(
        &self,
        metadata_backend: &P,
        entry: &TableEntry,
        config: TableSnapshotExpirationConfig,
    ) -> TableCatalogStoreResult<TableSnapshotExpirationReport>
    where
        P: TableCatalogObjectBackend,
    {
        validate_table_snapshot_expiration_config(&config)?;
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        if !is_valid_table_metadata_location_for_entry(entry, &entry.metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "current metadata location must be inside the table metadata directory".to_string(),
            ));
        }

        let Some(current_metadata) =
            read_table_metadata_value(metadata_backend, &entry.table_bucket, &entry.metadata_location).await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "current metadata object {}",
                entry.metadata_location
            )));
        };

        Ok(table_snapshot_expiration_report(
            &entry.table_bucket,
            &namespace,
            &table,
            entry,
            &current_metadata,
            config,
            OffsetDateTime::now_utc(),
        ))
    }

    pub(crate) async fn plan_table_compaction(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        config: TableCompactionPlanningConfig,
    ) -> TableCatalogStoreResult<TableCompactionPlanningReport> {
        self.with_table_maintenance_read(table_bucket, async {
            validate_table_compaction_planning_config(&config)?;
            let namespace = parse_namespace_for_store(namespace)?;
            let table = parse_table_for_store(table)?;
            let Some((entry, _)) = self.read_table_with_etag(table_bucket, &namespace, &table).await? else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    table_bucket,
                    namespace.public_name(),
                    table.as_str()
                )));
            };
            if !is_valid_table_metadata_location(&namespace, &table, &entry.metadata_location) {
                return Err(TableCatalogStoreError::Invalid(
                    "current metadata location must be inside the table metadata directory".to_string(),
                ));
            }

            self.plan_table_compaction_for_entry(&entry, config).await
        })
        .await
    }

    pub(in crate::table_catalog) async fn plan_table_compaction_for_entry(
        &self,
        entry: &TableEntry,
        config: TableCompactionPlanningConfig,
    ) -> TableCatalogStoreResult<TableCompactionPlanningReport> {
        validate_table_compaction_planning_config(&config)?;
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        if !is_valid_table_metadata_location_for_entry(entry, &entry.metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "current metadata location must be inside the table metadata directory".to_string(),
            ));
        }

        let Some(current_metadata) =
            read_table_metadata_value(&self.backend, &entry.table_bucket, &entry.metadata_location).await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "current metadata object {}",
                entry.metadata_location
            )));
        };

        table_compaction_planning_report(&self.backend, &entry.table_bucket, &namespace, &table, entry, &current_metadata, config)
            .await
    }

    pub(crate) async fn export_table_catalog_entry(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<TableCatalogExport> {
        self.with_catalog_restore_read(table_bucket, async {
            self.table_catalog_export_with_recovery(table_bucket, namespace, table)
                .await
                .map(|(catalog, _)| catalog)
        })
        .await
    }

    pub(in crate::table_catalog) async fn table_catalog_export_with_recovery(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
    ) -> TableCatalogStoreResult<(TableCatalogExport, TableCommitRecoveryReport)> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;

        let table_bucket_path = self.paths.table_bucket_entry_path(table_bucket);
        let Some((table_bucket_entry, _)) = self
            .read_entry::<TableBucketEntry>(self.catalog_bucket(), &table_bucket_path)
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!("table bucket {table_bucket}")));
        };
        validate_table_bucket_entry_object(&self.paths, &table_bucket_path, &table_bucket_entry)?;
        let namespace_path = self.paths.namespace_entry_path(table_bucket, &namespace);
        let Some((namespace_entry, _)) = self
            .read_entry::<NamespaceEntry>(self.catalog_bucket(), &namespace_path)
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "namespace {}/{}",
                table_bucket,
                namespace.public_name()
            )));
        };
        validate_namespace_entry_object(&self.paths, &namespace_path, &namespace_entry)?;
        let Some((table_entry, _)) = self.read_table_with_etag(table_bucket, &namespace, &table).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        let commit_recovery = self.table_commit_recovery_report_for_entry(&table_entry, 0).await?;
        let backing_manifest = table_catalog_backing_manifest(&self.paths, &namespace, &table, &table_entry, &commit_recovery);

        Ok((
            TableCatalogExport {
                table_bucket: table_bucket_entry,
                namespace: namespace_entry,
                table: table_entry,
                backing_manifest,
            },
            commit_recovery,
        ))
    }

    pub(crate) async fn diagnose_table_catalog(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        retain_recent_metadata_files: usize,
    ) -> TableCatalogStoreResult<TableCatalogDiagnosticsReport> {
        self.with_catalog_restore_read(table_bucket, async {
            let (catalog, commit_recovery) = self
                .table_catalog_export_with_recovery(table_bucket, namespace, table)
                .await?;
            diagnose_table_catalog_from_export(&self.backend, catalog, commit_recovery, retain_recent_metadata_files).await
        })
        .await
    }

    pub(in crate::table_catalog) async fn ensure_exclusive_maintenance_roots(
        &self,
        entry: &TableEntry,
        locked_table_entry: Option<&str>,
    ) -> TableCatalogStoreResult<()> {
        let roots = maintenance_cleanup_roots(entry)?;
        for other in self
            .list_all_table_entries_with_limit(&entry.table_bucket, None, locked_table_entry, TABLE_REGISTRATION_READ_CONCURRENCY)
            .await?
        {
            if other.state == TableCatalogEntryState::Deleted
                || (other.namespace == entry.namespace && other.table == entry.table && other.table_id == entry.table_id)
            {
                continue;
            }
            let other_roots = maintenance_cleanup_roots(&other)?;
            if roots.iter().any(|root| {
                other_roots
                    .iter()
                    .any(|other_root| warehouse_object_prefixes_overlap(root, other_root))
            }) {
                return Err(TableCatalogStoreError::Conflict(
                    "table maintenance requires exclusive metadata and warehouse roots".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) async fn plan_table_metadata_maintenance(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        retain_recent_metadata_files: usize,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        self.with_table_maintenance_read(table_bucket, async {
            let namespace = parse_namespace_for_store(namespace)?;
            let table = parse_table_for_store(table)?;
            let Some((entry, _)) = self.read_table_with_etag(table_bucket, &namespace, &table).await? else {
                return Err(TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    table_bucket,
                    namespace.public_name(),
                    table.as_str()
                )));
            };
            self.plan_table_metadata_maintenance_for_entry(&entry, retain_recent_metadata_files)
                .await
        })
        .await
    }

    pub(in crate::table_catalog) async fn ensure_exclusive_metadata_cleanup_scope(
        &self,
        entry: &TableEntry,
        publication_fenced: bool,
    ) -> TableCatalogStoreResult<()> {
        let roots = maintenance_cleanup_roots(entry)?;
        let owners = if publication_fenced {
            // Cleanup already holds the bucket publication permit. Every table
            // entry mutation needs its write side, so these entries are stable.
            // Acquiring other entry locks while holding this table's write lock
            // would deadlock two concurrent cleanups in opposite scan order.
            let objects = self
                .list_table_entry_objects_for_data_plane_scan(
                    &entry.table_bucket,
                    TABLE_DATA_PLANE_INDEX_MISS_SCAN_MAX_CATALOG_OBJECTS,
                )
                .await?;
            let mut owners = Vec::with_capacity(objects.len());
            for object in objects {
                let Some((owner, _)) = self.read_entry_unlocked::<TableEntry>(self.catalog_bucket(), &object).await? else {
                    return Err(TableCatalogStoreError::Conflict(
                        "metadata ownership entry disappeared during the publication-fenced scan".to_string(),
                    ));
                };
                validate_table_entry_object(&self.paths, &object, &owner)?;
                owners.push(owner);
            }
            owners
        } else {
            self.list_all_table_entries_with_limit(
                &entry.table_bucket,
                Some(TABLE_DATA_PLANE_INDEX_MISS_SCAN_MAX_CATALOG_OBJECTS),
                None,
                1,
            )
            .await?
        };
        for owner in owners {
            if owner.state == TableCatalogEntryState::Deleted
                || (owner.namespace == entry.namespace && owner.table == entry.table && owner.table_id == entry.table_id)
            {
                continue;
            }
            let other_roots = maintenance_cleanup_roots(&owner)?;
            if roots.iter().any(|root| {
                other_roots
                    .iter()
                    .any(|other_root| warehouse_object_prefixes_overlap(root, other_root))
            }) {
                return Err(TableCatalogStoreError::Conflict(
                    "table maintenance requires exclusive metadata and warehouse roots".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(in crate::table_catalog) async fn plan_table_metadata_maintenance_for_entry(
        &self,
        entry: &TableEntry,
        retain_recent_metadata_files: usize,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let table_bucket = &entry.table_bucket;
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        let is_valid_location = |location: &str| is_valid_table_metadata_location_for_entry(entry, location);
        if !is_valid_location(&entry.metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "current metadata location must be inside the table metadata directory".to_string(),
            ));
        }

        self.ensure_exclusive_metadata_cleanup_scope(entry, false).await?;
        let current_object = table_catalog_object_key_from_location(table_bucket, &entry.metadata_location).ok_or_else(|| {
            TableCatalogStoreError::Invalid("current metadata location is not a same-bucket object".to_string())
        })?;
        let Some(current_metadata) = read_table_metadata_value(&self.backend, table_bucket, &current_object).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "current metadata object {}",
                entry.metadata_location
            )));
        };

        let mut retained = BTreeSet::new();
        let mut maintenance_reasons = BTreeMap::<String, BTreeSet<TableMetadataMaintenanceReason>>::new();
        for metadata_location in metadata_log_locations_matching(&current_metadata, table_bucket, is_valid_location) {
            retained.insert(metadata_location.clone());
            insert_metadata_maintenance_reason(
                &mut maintenance_reasons,
                metadata_location,
                TableMetadataMaintenanceReason::MetadataLog,
            );
        }
        retained.insert(current_object.clone());
        insert_metadata_maintenance_reason(
            &mut maintenance_reasons,
            current_object.clone(),
            TableMetadataMaintenanceReason::CurrentMetadata,
        );

        let mut metadata_locations = Vec::new();
        let metadata_dir = table_metadata_dir_path_for_entry(entry)?;
        let metadata_prefix = format!("{metadata_dir}/");
        for object in self.backend.list_objects(table_bucket, &metadata_prefix).await? {
            if is_valid_location(&object) {
                metadata_locations.push(object);
            }
        }
        metadata_locations.sort();
        metadata_locations.dedup();
        let planned_metadata_file_count = metadata_locations.len();

        for metadata_location in metadata_locations.iter().rev().take(retain_recent_metadata_files) {
            retained.insert(metadata_location.clone());
            if metadata_location != &current_object {
                insert_metadata_maintenance_reason(
                    &mut maintenance_reasons,
                    metadata_location.clone(),
                    TableMetadataMaintenanceReason::RecentMetadata,
                );
            }
        }
        let protected_refs = metadata_locations_for_protected_snapshot_refs_matching(
            &self.backend,
            table_bucket,
            &current_metadata,
            &metadata_locations,
            is_valid_location,
        )
        .await?;
        for metadata_location in protected_refs {
            retained.insert(metadata_location.clone());
            insert_metadata_maintenance_reason(
                &mut maintenance_reasons,
                metadata_location,
                TableMetadataMaintenanceReason::ProtectedSnapshotRef,
            );
        }

        let cleanup_candidate_locations = metadata_locations
            .iter()
            .filter(|metadata_location| !retained.contains(metadata_location.as_str()))
            .cloned()
            .collect::<Vec<_>>();

        let now = OffsetDateTime::now_utc();
        let mut deletable_metadata_locations = Vec::new();
        for metadata_location in &cleanup_candidate_locations {
            insert_metadata_maintenance_reason(
                &mut maintenance_reasons,
                metadata_location.clone(),
                TableMetadataMaintenanceReason::NoCurrentReachability,
            );
            let Some(candidate_object) = self.backend.object_metadata(table_bucket, metadata_location).await? else {
                insert_metadata_maintenance_reason(
                    &mut maintenance_reasons,
                    metadata_location.clone(),
                    TableMetadataMaintenanceReason::SafetyWindowPending,
                );
                continue;
            };
            if metadata_candidate_is_past_safety_window(candidate_object.mod_time, now) {
                deletable_metadata_locations.push(metadata_location.clone());
                insert_metadata_maintenance_reason(
                    &mut maintenance_reasons,
                    metadata_location.clone(),
                    TableMetadataMaintenanceReason::SafetyWindowSatisfied,
                );
            } else {
                insert_metadata_maintenance_reason(
                    &mut maintenance_reasons,
                    metadata_location.clone(),
                    TableMetadataMaintenanceReason::SafetyWindowPending,
                );
            }
        }
        let warehouse_object_prefix = table_warehouse_object_prefix(entry).ok();
        let current_metadata_location = entry.metadata_location.clone();
        let retained_metadata_locations = retained.into_iter().collect::<Vec<_>>();
        let object_reports = metadata_maintenance_object_reports(maintenance_reasons);
        let referenced_object_reports = metadata_maintenance_referenced_object_reports_for_entry(
            &self.backend,
            entry,
            warehouse_object_prefix.as_deref(),
            &current_metadata,
            &retained_metadata_locations,
        )
        .await?;
        let reachability_graph =
            metadata_maintenance_reachability_graph_report(planned_metadata_file_count, &referenced_object_reports);
        let (planned_object_file_count, cleanup_object_candidate_locations, deletable_object_locations, object_cleanup_reports) =
            metadata_maintenance_object_cleanup_reports_for_entry(
                &self.backend,
                entry,
                warehouse_object_prefix.as_deref(),
                &referenced_object_reports,
                now,
            )
            .await?;

        let mut report = table_maintenance_report_with_recommended_actions(TableMetadataMaintenanceReport {
            job: TableMetadataMaintenanceJob {
                job_id: Uuid::new_v4().to_string(),
                table_bucket: table_bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                table_id: entry.table_id.clone(),
                operation: TableMetadataMaintenanceOperation::DryRun,
                status: TableMetadataMaintenanceJobStatus::Successful,
                failure_reason: None,
                recommended_actions: Vec::new(),
                config_source: TableMaintenanceConfigSource::Default,
                scheduler_id: None,
                scheduler_lease_id: String::new(),
                scheduled_at: None,
                worker_id: None,
                lease_id: String::new(),
                attempt: 0,
                max_retry_attempts: 0,
                next_retry_after: None,
                quarantine_enabled: false,
                quarantine_retention_seconds: 0,
                heartbeat_at: None,
                started_at: None,
                finished_at: None,
                current_metadata_location: current_metadata_location.clone(),
                current_generation: entry.generation,
                retain_recent_metadata_files,
                safety_window_seconds: TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS,
                cleanup_watermark_unix_seconds: (now - Duration::seconds(TABLE_METADATA_CLEANUP_SAFETY_WINDOW_SECONDS))
                    .unix_timestamp(),
                planned_metadata_file_count,
                retained_metadata_file_count: retained_metadata_locations.len(),
                cleanup_candidate_count: cleanup_candidate_locations.len(),
                deletable_metadata_file_count: deletable_metadata_locations.len(),
                deleted_metadata_file_count: 0,
                planned_object_file_count,
                cleanup_candidate_object_count: cleanup_object_candidate_locations.len(),
                deletable_object_count: deletable_object_locations.len(),
                deleted_object_count: 0,
                quarantined_object_count: 0,
            },
            current_metadata_location,
            retained_metadata_locations,
            cleanup_candidate_locations,
            deletable_metadata_locations,
            cleanup_object_candidate_locations,
            deletable_object_locations,
            object_reports,
            object_cleanup_reports,
            referenced_object_reports,
            reachability_graph,
            snapshot_expiration: None,
            compaction: None,
            audit_events: Vec::new(),
        });
        push_table_maintenance_audit_event(
            &mut report,
            now,
            TableMaintenanceAuditActor::Scheduler,
            TableMaintenanceAuditAction::Planned,
            None,
            None,
            None,
        );
        Ok(report)
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub(crate) async fn delete_table_metadata_maintenance_candidates(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        retain_recent_metadata_files: usize,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let report = self
            .plan_table_metadata_maintenance(table_bucket, namespace, table, retain_recent_metadata_files)
            .await?;
        self.delete_table_metadata_maintenance_report(table_bucket, namespace, table, report)
            .await
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub(crate) async fn run_table_metadata_maintenance(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        delete: bool,
        worker_id: Option<String>,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let effective = self
            .get_effective_table_maintenance_config(table_bucket, namespace, table)
            .await?;
        self.run_table_metadata_maintenance_with_config(table_bucket, namespace, table, delete, worker_id, effective)
            .await
    }

    pub(crate) async fn run_table_metadata_maintenance_with_retention(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        delete: bool,
        worker_id: Option<String>,
        retain_recent_metadata_files: usize,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let mut effective = self
            .get_effective_table_maintenance_config(table_bucket, namespace, table)
            .await?;
        effective.config.retain_recent_metadata_files = retain_recent_metadata_files;
        self.run_table_metadata_maintenance_with_config(table_bucket, namespace, table, delete, worker_id, effective)
            .await
    }

    pub(in crate::table_catalog) async fn run_table_metadata_maintenance_with_config(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        delete: bool,
        worker_id: Option<String>,
        effective: TableMaintenanceEffectiveConfig,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let publication = self.metadata_maintenance_publication(table_bucket).await?;
        let _completion = TableCommitPublicationCompletion::new(&publication);
        let mut report = self
            .plan_table_metadata_maintenance(table_bucket, namespace, table, effective.config.retain_recent_metadata_files)
            .await?;

        let started_at_time = OffsetDateTime::now_utc();
        let started_at = maintenance_timestamp(started_at_time);
        report.job.operation = if delete {
            TableMetadataMaintenanceOperation::Delete
        } else {
            TableMetadataMaintenanceOperation::DryRun
        };
        report.job.status = TableMetadataMaintenanceJobStatus::Running;
        report.job.failure_reason = None;
        report.job.config_source = effective.source;
        report.job.worker_id = worker_id;
        report.job.lease_id = Uuid::new_v4().to_string();
        report.job.attempt = 1;
        report.job.max_retry_attempts = effective.config.max_retry_attempts;
        report.job.next_retry_after = None;
        report.job.quarantine_enabled = effective.config.quarantine_enabled;
        report.job.quarantine_retention_seconds = effective.config.quarantine_retention_seconds;
        report.job.heartbeat_at = Some(started_at.clone());
        report.job.started_at = Some(started_at);
        report.job.finished_at = None;
        refresh_table_maintenance_report_recommended_actions(&mut report);
        push_table_maintenance_audit_event(
            &mut report,
            started_at_time,
            TableMaintenanceAuditActor::Worker,
            TableMaintenanceAuditAction::WorkerStarted,
            None,
            Some(TableMetadataMaintenanceJobStatus::Successful),
            Some(0),
        );
        self.put_table_metadata_maintenance_report_unfenced(&report).await?;

        self.finish_table_metadata_maintenance_run(table_bucket, namespace, table, &publication, &effective, report)
            .await
    }

    pub(in crate::table_catalog) async fn finish_table_metadata_maintenance_run(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        publication: &TableCommitLockPublication<'_, B>,
        effective: &TableMaintenanceEffectiveConfig,
        mut report: TableMetadataMaintenanceReport,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let delete = report.job.operation == TableMetadataMaintenanceOperation::Delete;
        if delete && !effective.config.delete_enabled {
            let finished_at = OffsetDateTime::now_utc();
            let before_status = Some(report.job.status.clone());
            let before_quarantined_object_count = Some(report.job.quarantined_object_count);
            report.job.status = TableMetadataMaintenanceJobStatus::Failed;
            report.job.failure_reason = Some(TABLE_MAINTENANCE_DELETE_DISABLED_REASON.to_string());
            apply_maintenance_retry_after(&mut report.job, &effective.config, finished_at);
            report.job.finished_at = Some(maintenance_timestamp(finished_at));
            refresh_table_maintenance_report_recommended_actions(&mut report);
            push_table_maintenance_audit_event(
                &mut report,
                finished_at,
                TableMaintenanceAuditActor::Worker,
                TableMaintenanceAuditAction::WorkerFailed,
                Some(TABLE_MAINTENANCE_DELETE_DISABLED_REASON.to_string()),
                before_status,
                before_quarantined_object_count,
            );
            self.put_table_metadata_maintenance_report_unfenced(&report).await?;
            return Ok(report);
        }

        if delete {
            let running_report = report.clone();
            let mut deleted = match self
                .delete_table_metadata_maintenance_report_with_bucket_permit(table_bucket, namespace, table, report, publication)
                .await
            {
                Ok(report) => report,
                Err(err) => {
                    let finished_at = OffsetDateTime::now_utc();
                    let mut failed = running_report;
                    let before_status = Some(failed.job.status.clone());
                    let before_quarantined_object_count = Some(failed.job.quarantined_object_count);
                    let reason = err.to_string();
                    failed.job.status = TableMetadataMaintenanceJobStatus::Failed;
                    failed.job.failure_reason = Some(reason.clone());
                    apply_maintenance_retry_after(&mut failed.job, &effective.config, finished_at);
                    failed.job.finished_at = Some(maintenance_timestamp(finished_at));
                    refresh_table_maintenance_report_recommended_actions(&mut failed);
                    push_table_maintenance_audit_event(
                        &mut failed,
                        finished_at,
                        TableMaintenanceAuditActor::Worker,
                        TableMaintenanceAuditAction::WorkerFailed,
                        Some(reason),
                        before_status,
                        before_quarantined_object_count,
                    );
                    self.put_table_metadata_maintenance_report_unfenced(&failed).await?;
                    return Err(err);
                }
            };
            let finished_at = OffsetDateTime::now_utc();
            let before_status = Some(TableMetadataMaintenanceJobStatus::Running);
            let before_quarantined_object_count = Some(0);
            deleted.job.finished_at = Some(maintenance_timestamp(finished_at));
            refresh_table_maintenance_report_recommended_actions(&mut deleted);
            push_table_maintenance_audit_event(
                &mut deleted,
                finished_at,
                TableMaintenanceAuditActor::Worker,
                TableMaintenanceAuditAction::WorkerSucceeded,
                None,
                before_status,
                before_quarantined_object_count,
            );
            self.put_table_metadata_maintenance_report_unfenced(&deleted).await?;
            return Ok(deleted);
        }

        let finished_at = OffsetDateTime::now_utc();
        let before_status = Some(report.job.status.clone());
        let before_quarantined_object_count = Some(report.job.quarantined_object_count);
        report.job.status = TableMetadataMaintenanceJobStatus::Successful;
        report.job.finished_at = Some(maintenance_timestamp(finished_at));
        refresh_table_maintenance_report_recommended_actions(&mut report);
        push_table_maintenance_audit_event(
            &mut report,
            finished_at,
            TableMaintenanceAuditActor::Worker,
            TableMaintenanceAuditAction::WorkerSucceeded,
            None,
            before_status,
            before_quarantined_object_count,
        );
        self.put_table_metadata_maintenance_report_unfenced(&report).await?;
        Ok(report)
    }

    pub(in crate::table_catalog) async fn finish_table_metadata_maintenance_run_for_entry_with_optional_publication(
        &self,
        entry: &TableEntry,
        delete: bool,
        effective: &TableMaintenanceEffectiveConfig,
        mut report: TableMetadataMaintenanceReport,
        publication: Option<&(dyn TableCommitPublication + Sync)>,
        preserve_candidates: bool,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        if delete && !effective.config.delete_enabled {
            let finished_at = OffsetDateTime::now_utc();
            let before_status = Some(report.job.status.clone());
            let before_quarantined_object_count = Some(report.job.quarantined_object_count);
            report.job.status = TableMetadataMaintenanceJobStatus::Failed;
            report.job.failure_reason = Some(TABLE_MAINTENANCE_DELETE_DISABLED_REASON.to_string());
            apply_maintenance_retry_after(&mut report.job, &effective.config, finished_at);
            report.job.finished_at = Some(maintenance_timestamp(finished_at));
            refresh_table_maintenance_report_recommended_actions(&mut report);
            push_table_maintenance_audit_event(
                &mut report,
                finished_at,
                TableMaintenanceAuditActor::Worker,
                TableMaintenanceAuditAction::WorkerFailed,
                Some(TABLE_MAINTENANCE_DELETE_DISABLED_REASON.to_string()),
                before_status,
                before_quarantined_object_count,
            );
            self.put_table_metadata_maintenance_report_for_entry_with_publication(&report, entry, publication)
                .await?;
            return Ok(report);
        }

        if delete {
            let running_report = report.clone();
            let mut deleted = match self
                .delete_table_metadata_maintenance_report_for_entry_with_publication(entry, report, publication)
                .await
            {
                Ok(report) => report,
                Err(err) => {
                    let finished_at = OffsetDateTime::now_utc();
                    let mut failed = running_report;
                    let before_status = Some(failed.job.status.clone());
                    let before_quarantined_object_count = Some(failed.job.quarantined_object_count);
                    let reason = err.to_string();
                    failed.job.status = TableMetadataMaintenanceJobStatus::Failed;
                    failed.job.failure_reason = Some(reason.clone());
                    apply_maintenance_retry_after(&mut failed.job, &effective.config, finished_at);
                    failed.job.finished_at = Some(maintenance_timestamp(finished_at));
                    refresh_table_maintenance_report_recommended_actions(&mut failed);
                    push_table_maintenance_audit_event(
                        &mut failed,
                        finished_at,
                        TableMaintenanceAuditActor::Worker,
                        TableMaintenanceAuditAction::WorkerFailed,
                        Some(reason),
                        before_status,
                        before_quarantined_object_count,
                    );
                    self.put_table_metadata_maintenance_report_for_entry_with_publication(&failed, entry, publication)
                        .await?;
                    return Err(err);
                }
            };
            if preserve_candidates {
                deleted.cleanup_candidate_locations = running_report.cleanup_candidate_locations;
                deleted.deletable_metadata_locations = running_report.deletable_metadata_locations;
                deleted.cleanup_object_candidate_locations = running_report.cleanup_object_candidate_locations;
                deleted.deletable_object_locations = running_report.deletable_object_locations;
            }
            let finished_at = OffsetDateTime::now_utc();
            let before_status = Some(TableMetadataMaintenanceJobStatus::Running);
            let before_quarantined_object_count = Some(0);
            deleted.job.finished_at = Some(maintenance_timestamp(finished_at));
            refresh_table_maintenance_report_recommended_actions(&mut deleted);
            push_table_maintenance_audit_event(
                &mut deleted,
                finished_at,
                TableMaintenanceAuditActor::Worker,
                TableMaintenanceAuditAction::WorkerSucceeded,
                None,
                before_status,
                before_quarantined_object_count,
            );
            self.put_table_metadata_maintenance_report_for_entry_with_publication(&deleted, entry, publication)
                .await?;
            return Ok(deleted);
        }

        let finished_at = OffsetDateTime::now_utc();
        let before_status = Some(report.job.status.clone());
        let before_quarantined_object_count = Some(report.job.quarantined_object_count);
        report.job.status = TableMetadataMaintenanceJobStatus::Successful;
        report.job.finished_at = Some(maintenance_timestamp(finished_at));
        refresh_table_maintenance_report_recommended_actions(&mut report);
        push_table_maintenance_audit_event(
            &mut report,
            finished_at,
            TableMaintenanceAuditActor::Worker,
            TableMaintenanceAuditAction::WorkerSucceeded,
            None,
            before_status,
            before_quarantined_object_count,
        );
        self.put_table_metadata_maintenance_report_for_entry_with_publication(&report, entry, publication)
            .await?;
        Ok(report)
    }

    #[allow(
        dead_code,
        reason = "exercised by table_catalog/tests.rs; the lib target cannot see test-only consumers (backlog#1823)"
    )]
    pub(in crate::table_catalog) async fn delete_table_metadata_maintenance_report(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        report: TableMetadataMaintenanceReport,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let publication = self.metadata_maintenance_publication(table_bucket).await?;
        let _completion = TableCommitPublicationCompletion::new(&publication);
        self.delete_table_metadata_maintenance_report_with_bucket_permit(table_bucket, namespace, table, report, &publication)
            .await
    }

    pub(in crate::table_catalog) async fn delete_table_metadata_maintenance_report_with_bucket_permit(
        &self,
        table_bucket: &str,
        namespace: &str,
        table: &str,
        report: TableMetadataMaintenanceReport,
        publication: &TableCommitLockPublication<'_, B>,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let namespace = parse_namespace_for_store(namespace)?;
        let table = parse_table_for_store(table)?;
        self.ensure_no_active_table_rename(table_bucket).await?;
        let table_path = self.paths.table_entry_path(table_bucket, &namespace, &table);
        let guard = self.backend.acquire_write_lock(self.catalog_bucket(), &table_path).await?;
        publication.attach_catalog_write_guard(guard)?;
        publication
            .prepare(table_bucket, &namespace.public_name(), table.as_str())
            .await?;
        let Some((entry, _)) = self.read_table_with_etag_unlocked(table_bucket, &namespace, &table).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "table {}/{}/{}",
                table_bucket,
                namespace.public_name(),
                table.as_str()
            )));
        };
        self.delete_table_metadata_maintenance_report_for_entry_with_publication(&entry, report, Some(publication))
            .await
    }

    pub(in crate::table_catalog) async fn delete_table_metadata_maintenance_report_for_entry_with_publication(
        &self,
        entry: &TableEntry,
        report: TableMetadataMaintenanceReport,
        publication: Option<&(dyn TableCommitPublication + Sync)>,
    ) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
        let mutation_fence = table_maintenance_mutation_fence(publication, entry)?;
        let table_bucket = &entry.table_bucket;
        let namespace = parse_namespace_for_store(&entry.namespace)?;
        let table = parse_table_for_store(&entry.table)?;
        validate_table_maintenance_report_owner(&report, table_bucket, &namespace, &table, &entry.table_id)?;
        let is_valid_location = |location: &str| {
            if publication.is_some() {
                is_valid_table_metadata_location_for_entry(entry, location)
            } else {
                is_valid_table_metadata_location(&namespace, &table, location)
            }
        };
        if !is_valid_location(&report.current_metadata_location) {
            return Err(TableCatalogStoreError::Invalid(
                "maintenance report current metadata location must be inside the table metadata directory".to_string(),
            ));
        }

        if entry.metadata_location != report.current_metadata_location {
            return Err(TableCatalogStoreError::Conflict(
                "current metadata location changed before maintenance delete".to_string(),
            ));
        }
        let warehouse_object_prefix = table_warehouse_object_prefix(entry).ok();

        self.ensure_exclusive_metadata_cleanup_scope(entry, true).await?;
        let current_object = table_catalog_object_key_from_location(table_bucket, &entry.metadata_location).ok_or_else(|| {
            TableCatalogStoreError::Invalid("current metadata location is not a same-bucket object".to_string())
        })?;
        let Some(current_metadata) = read_table_metadata_value(&self.backend, table_bucket, &current_object).await? else {
            return Err(TableCatalogStoreError::NotFound(format!(
                "current metadata object {}",
                entry.metadata_location
            )));
        };

        let mut protected = metadata_log_locations_matching(&current_metadata, table_bucket, is_valid_location);
        protected.insert(current_object);
        protected.extend(report.retained_metadata_locations.iter().cloned());
        protected.extend(
            metadata_locations_for_protected_snapshot_refs_matching(
                &self.backend,
                table_bucket,
                &current_metadata,
                &report.cleanup_candidate_locations,
                is_valid_location,
            )
            .await?,
        );

        let cleanup_candidate_count = report.cleanup_candidate_locations.len();
        let planned_deletable_locations = report.deletable_metadata_locations.iter().cloned().collect::<BTreeSet<_>>();
        let mut cleanup_candidate_locations = BTreeSet::new();
        let mut delete_etags = BTreeMap::new();
        let now = OffsetDateTime::now_utc();
        for metadata_location in &report.cleanup_candidate_locations {
            if !is_valid_location(metadata_location) {
                return Err(TableCatalogStoreError::Invalid(format!(
                    "cleanup candidate {metadata_location} must be inside the table metadata directory"
                )));
            }
            if protected.contains(metadata_location.as_str()) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "cleanup candidate {metadata_location} is retained by current metadata"
                )));
            }
            let Some(candidate_object) = self.backend.object_metadata(table_bucket, metadata_location).await? else {
                continue;
            };
            if !planned_deletable_locations.contains(metadata_location.as_str()) {
                continue;
            }
            if !metadata_candidate_is_past_safety_window(candidate_object.mod_time, now) {
                continue;
            }
            if mutation_fence.is_some() {
                if candidate_object
                    .mod_time
                    .is_none_or(|modified| modified.unix_timestamp() > report.job.cleanup_watermark_unix_seconds)
                {
                    continue;
                }
                let etag = candidate_object.etag.ok_or_else(|| {
                    TableCatalogStoreError::Conflict(format!(
                        "maintenance candidate has no conditional-delete ETag: {metadata_location}"
                    ))
                })?;
                delete_etags.insert(metadata_location.clone(), etag);
            }
            cleanup_candidate_locations.insert(metadata_location.clone());
        }

        let cleanup_candidate_locations = cleanup_candidate_locations.into_iter().collect::<Vec<_>>();
        let deleted_locations = cleanup_candidate_locations.iter().cloned().collect::<BTreeSet<_>>();
        for metadata_location in &cleanup_candidate_locations {
            ensure_table_maintenance_publication_held(publication, entry, "before metadata deletion")?;
            if let Some(fence) = &mutation_fence {
                self.backend
                    .delete_object_if_match_fenced(table_bucket, metadata_location, &delete_etags[metadata_location], fence)
                    .await?;
            } else {
                self.backend.delete_object(table_bucket, metadata_location).await?;
            }
            ensure_table_maintenance_publication_held(publication, entry, "after metadata deletion")?;
        }

        let referenced_object_reports = if publication.is_some() {
            metadata_maintenance_referenced_object_reports_for_entry(
                &self.backend,
                entry,
                warehouse_object_prefix.as_deref(),
                &current_metadata,
                &report.retained_metadata_locations,
            )
            .await?
        } else {
            metadata_maintenance_referenced_object_reports(
                &self.backend,
                table_bucket,
                &namespace,
                &table,
                warehouse_object_prefix.as_deref(),
                &current_metadata,
                &report.retained_metadata_locations,
            )
            .await?
        };
        let referenced_object_locations = if referenced_object_reports
            .iter()
            .any(|report| report.state == TableMetadataMaintenanceObjectState::ManualReviewRequired)
        {
            BTreeSet::new()
        } else {
            referenced_object_reports
                .iter()
                .filter_map(|report| table_catalog_object_key_from_location(table_bucket, &report.object_location))
                .collect::<BTreeSet<_>>()
        };
        let planned_deletable_object_locations = report.deletable_object_locations.iter().cloned().collect::<BTreeSet<_>>();
        let mut cleanup_object_candidate_locations = BTreeSet::new();
        if !referenced_object_reports
            .iter()
            .any(|report| report.state == TableMetadataMaintenanceObjectState::ManualReviewRequired)
        {
            for object_location in &report.cleanup_object_candidate_locations {
                let object_kind = if publication.is_some() {
                    table_maintenance_object_kind_for_entry(entry, warehouse_object_prefix.as_deref(), object_location)
                } else {
                    table_maintenance_object_kind(&namespace, &table, warehouse_object_prefix.as_deref(), object_location)
                };
                if object_kind.is_none() {
                    return Err(TableCatalogStoreError::Invalid(format!(
                        "cleanup object candidate {object_location} must be inside table metadata, data, or delete directories"
                    )));
                }
                if referenced_object_locations.contains(object_location.as_str()) {
                    return Err(TableCatalogStoreError::Conflict(format!(
                        "cleanup object candidate {object_location} is retained by current metadata"
                    )));
                }
                let Some(candidate_object) = self.backend.object_metadata(table_bucket, object_location).await? else {
                    continue;
                };
                if !planned_deletable_object_locations.contains(object_location.as_str()) {
                    continue;
                }
                if !metadata_candidate_is_past_safety_window(candidate_object.mod_time, now) {
                    continue;
                }
                if mutation_fence.is_some() {
                    if candidate_object
                        .mod_time
                        .is_none_or(|modified| modified.unix_timestamp() > report.job.cleanup_watermark_unix_seconds)
                    {
                        continue;
                    }
                    let etag = candidate_object.etag.ok_or_else(|| {
                        TableCatalogStoreError::Conflict(format!(
                            "maintenance candidate has no conditional-delete ETag: {object_location}"
                        ))
                    })?;
                    delete_etags.insert(object_location.clone(), etag);
                }
                cleanup_object_candidate_locations.insert(object_location.clone());
            }
        }

        let cleanup_object_candidate_locations = cleanup_object_candidate_locations.into_iter().collect::<Vec<_>>();
        let deleted_object_locations = cleanup_object_candidate_locations.iter().cloned().collect::<BTreeSet<_>>();
        for object_location in &cleanup_object_candidate_locations {
            ensure_table_maintenance_publication_held(publication, entry, "before object deletion")?;
            if let Some(fence) = &mutation_fence {
                self.backend
                    .delete_object_if_match_fenced(table_bucket, object_location, &delete_etags[object_location], fence)
                    .await?;
            } else {
                self.backend.delete_object(table_bucket, object_location).await?;
            }
            ensure_table_maintenance_publication_held(publication, entry, "after object deletion")?;
        }

        let retained_metadata_locations = protected.into_iter().collect::<Vec<_>>();
        let mut job = report.job;
        job.operation = TableMetadataMaintenanceOperation::Delete;
        job.status = TableMetadataMaintenanceJobStatus::Successful;
        job.failure_reason = None;
        job.retained_metadata_file_count = retained_metadata_locations.len();
        job.cleanup_candidate_count = cleanup_candidate_count;
        job.deletable_metadata_file_count = planned_deletable_locations.len();
        job.deleted_metadata_file_count = cleanup_candidate_locations.len();
        job.cleanup_candidate_object_count = report.cleanup_object_candidate_locations.len();
        job.deletable_object_count = planned_deletable_object_locations.len();
        job.deleted_object_count = cleanup_object_candidate_locations.len();
        let mut object_reports = report.object_reports;
        mark_deleted_metadata_object_reports(&mut object_reports, &deleted_locations);
        let mut object_cleanup_reports = report.object_cleanup_reports;
        mark_deleted_object_cleanup_reports(&mut object_cleanup_reports, &deleted_object_locations);

        Ok(table_maintenance_report_with_recommended_actions(TableMetadataMaintenanceReport {
            job,
            current_metadata_location: entry.metadata_location.clone(),
            retained_metadata_locations,
            cleanup_candidate_locations: cleanup_candidate_locations.clone(),
            deletable_metadata_locations: cleanup_candidate_locations,
            cleanup_object_candidate_locations: cleanup_object_candidate_locations.clone(),
            deletable_object_locations: cleanup_object_candidate_locations,
            object_reports,
            object_cleanup_reports,
            referenced_object_reports,
            reachability_graph: report.reachability_graph,
            snapshot_expiration: report.snapshot_expiration,
            compaction: report.compaction,
            audit_events: report.audit_events,
        }))
    }
}
