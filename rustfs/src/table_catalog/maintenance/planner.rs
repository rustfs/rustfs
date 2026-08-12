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

#[derive(Debug)]
struct TableSnapshotExpirationDraft {
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    timestamp_ms: Option<i64>,
    manifest_list: Option<String>,
    reasons: BTreeSet<TableSnapshotExpirationReason>,
}

pub(crate) fn table_snapshot_expiration_report(
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    entry: &TableEntry,
    current_metadata: &serde_json::Value,
    config: TableSnapshotExpirationConfig,
    now: OffsetDateTime,
) -> TableSnapshotExpirationReport {
    let current_snapshot_id = current_metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64);
    let expiration_watermark_ms = unix_timestamp_millis(now).saturating_sub(config.max_snapshot_age_ms);
    let (protected_ref_snapshot_ids, user_defined_ref_snapshot_ids, ref_retention_conflict_snapshot_ids) =
        snapshot_expiration_ref_state(current_metadata, current_snapshot_id);
    let table_retention_property_conflict = snapshot_expiration_table_property_conflicts(current_metadata, &config);

    let mut drafts = snapshot_expiration_drafts(current_metadata, current_snapshot_id);
    mark_recent_snapshots_to_keep(&mut drafts, config.min_snapshots_to_keep);

    let mut snapshot_reports = Vec::with_capacity(drafts.len());
    for mut draft in drafts {
        if let Some(snapshot_id) = draft.snapshot_id {
            if protected_ref_snapshot_ids.contains(&snapshot_id) {
                draft.reasons.insert(TableSnapshotExpirationReason::ProtectedSnapshotRef);
            }
            if user_defined_ref_snapshot_ids.contains(&snapshot_id) {
                draft.reasons.insert(TableSnapshotExpirationReason::UserDefinedSnapshotRef);
            }
            if ref_retention_conflict_snapshot_ids.contains(&snapshot_id) {
                draft
                    .reasons
                    .insert(TableSnapshotExpirationReason::SnapshotRefRetentionConflict);
            }
        }
        if table_retention_property_conflict {
            draft
                .reasons
                .insert(TableSnapshotExpirationReason::TableRetentionPropertyConflict);
        }

        let state = if snapshot_expiration_requires_manual_review(&draft.reasons) {
            TableSnapshotExpirationSnapshotState::ManualReviewRequired
        } else if snapshot_expiration_is_retained(&draft.reasons) {
            TableSnapshotExpirationSnapshotState::Retained
        } else if let Some(timestamp_ms) = draft.timestamp_ms {
            if timestamp_ms <= expiration_watermark_ms {
                draft.reasons.insert(TableSnapshotExpirationReason::SnapshotAgeExpired);
                TableSnapshotExpirationSnapshotState::ExpirationCandidate
            } else {
                draft
                    .reasons
                    .insert(TableSnapshotExpirationReason::SnapshotAgeWithinRetention);
                TableSnapshotExpirationSnapshotState::Retained
            }
        } else {
            draft.reasons.insert(TableSnapshotExpirationReason::MissingSnapshotTimestamp);
            TableSnapshotExpirationSnapshotState::ManualReviewRequired
        };

        snapshot_reports.push(TableSnapshotExpirationSnapshotReport {
            snapshot_id: draft.snapshot_id,
            sequence_number: draft.sequence_number,
            timestamp_ms: draft.timestamp_ms,
            manifest_list: draft.manifest_list,
            state,
            reasons: draft.reasons.into_iter().collect(),
        });
    }

    let retained_snapshot_count = snapshot_reports
        .iter()
        .filter(|snapshot| snapshot.state == TableSnapshotExpirationSnapshotState::Retained)
        .count();
    let expiration_candidate_count = snapshot_reports
        .iter()
        .filter(|snapshot| snapshot.state == TableSnapshotExpirationSnapshotState::ExpirationCandidate)
        .count();
    let manual_review_count = snapshot_reports
        .iter()
        .filter(|snapshot| snapshot.state == TableSnapshotExpirationSnapshotState::ManualReviewRequired)
        .count();

    TableSnapshotExpirationReport {
        table_bucket: table_bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: entry.table_id.clone(),
        current_metadata_location: entry.metadata_location.clone(),
        current_snapshot_id,
        config,
        expiration_watermark_ms,
        retained_snapshot_count,
        expiration_candidate_count,
        manual_review_count,
        expired_snapshot_ids: Vec::new(),
        committed_metadata_location: None,
        snapshot_reports,
    }
}

#[derive(Debug, Clone)]
struct TableCompactionDataFileCandidate {
    location: String,
    size_bytes: u64,
    rewrite_prefix: String,
    sort_order_id: Option<i32>,
}

#[derive(Debug, Default)]
struct CompactionManifestPlanning {
    candidates: Vec<TableCompactionDataFileCandidate>,
    row_level_planning: TableRowLevelMaintenancePlanningReport,
}

pub(crate) struct CompactedParquetFile {
    pub(crate) data: Vec<u8>,
    pub(crate) record_count: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct CompactedDataFile {
    pub(crate) object_key: String,
    pub(crate) file_path: String,
    pub(crate) file_size_bytes: u64,
    pub(crate) record_count: u64,
    pub(crate) partition_spec_id: i32,
    pub(crate) partition: Vec<(String, apache_avro::types::Value)>,
    pub(crate) sort_order_id: Option<i32>,
    pub(crate) status: i32,
    pub(crate) snapshot_id: i64,
    pub(crate) sequence_number: i64,
    pub(crate) file_sequence_number: i64,
}

pub(crate) struct CompactionManifestListSummary<'a> {
    pub(crate) manifest_path: &'a str,
    pub(crate) manifest_length: u64,
    pub(crate) partition_spec_id: i32,
    pub(crate) snapshot_id: i64,
    pub(crate) sequence_number: i64,
    pub(crate) added_files_count: usize,
    pub(crate) existing_files_count: usize,
    pub(crate) added_rows_count: u64,
    pub(crate) existing_rows_count: u64,
}

pub(crate) async fn table_compaction_planning_report<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    entry: &TableEntry,
    current_metadata: &serde_json::Value,
    config: TableCompactionPlanningConfig,
) -> TableCatalogStoreResult<TableCompactionPlanningReport>
where
    B: TableCatalogObjectBackend,
{
    let current_snapshot_id = current_metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64);
    let warehouse_object_prefix = table_warehouse_object_prefix(entry).ok();
    let mut snapshot_reports = Vec::new();
    let mut candidates = Vec::new();
    let mut rewrite_groups = Vec::new();
    let mut row_level_planning = TableRowLevelMaintenancePlanningReport::default();

    if let Some(current_snapshot_id) = current_snapshot_id {
        let current_snapshot = current_metadata
            .get("snapshots")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .find(|snapshot| {
                snapshot
                    .get("snapshot-id")
                    .and_then(serde_json::Value::as_i64)
                    .is_some_and(|snapshot_id| snapshot_id == current_snapshot_id)
            });
        match current_snapshot {
            Some(snapshot) => {
                let manifest_list = snapshot
                    .get("manifest-list")
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string);
                match manifest_list.as_deref() {
                    Some(manifest_list) => {
                        let planning = match compaction_data_file_candidates(
                            backend,
                            table_bucket,
                            namespace,
                            table,
                            warehouse_object_prefix.as_deref(),
                            manifest_list,
                            &config,
                        )
                        .await
                        {
                            Ok(planning) => planning,
                            Err(_) => {
                                snapshot_reports.push(TableCompactionSnapshotReport {
                                    snapshot_id: Some(current_snapshot_id),
                                    manifest_list: Some(manifest_list.to_string()),
                                    status: TableCompactionPlanningStatus::ManualReviewRequired,
                                    reasons: vec![
                                        TableCompactionPlanningReason::ManifestList,
                                        TableCompactionPlanningReason::ManifestAvroReaderUnavailable,
                                    ],
                                });
                                CompactionManifestPlanning::default()
                            }
                        };
                        row_level_planning = planning.row_level_planning;
                        candidates = planning.candidates;
                        if row_level_planning.status == TableRowLevelMaintenancePlanningStatus::ManualReviewRequired
                            && snapshot_reports.is_empty()
                        {
                            snapshot_reports.push(TableCompactionSnapshotReport {
                                snapshot_id: Some(current_snapshot_id),
                                manifest_list: Some(manifest_list.to_string()),
                                status: TableCompactionPlanningStatus::ManualReviewRequired,
                                reasons: compaction_row_level_planning_reasons(&row_level_planning),
                            });
                        } else if !candidates.is_empty() && snapshot_reports.is_empty() {
                            rewrite_groups = compaction_rewrite_groups(&candidates, &config);
                            let (status, reasons) = if rewrite_groups.is_empty() {
                                (
                                    TableCompactionPlanningStatus::NoCandidates,
                                    vec![
                                        TableCompactionPlanningReason::ManifestList,
                                        TableCompactionPlanningReason::ManifestFile,
                                    ],
                                )
                            } else {
                                (
                                    TableCompactionPlanningStatus::RewriteCandidates,
                                    vec![
                                        TableCompactionPlanningReason::ManifestList,
                                        TableCompactionPlanningReason::ManifestFile,
                                        TableCompactionPlanningReason::SmallDataFile,
                                        TableCompactionPlanningReason::RewriteGroup,
                                    ],
                                )
                            };
                            snapshot_reports.push(TableCompactionSnapshotReport {
                                snapshot_id: Some(current_snapshot_id),
                                manifest_list: Some(manifest_list.to_string()),
                                status,
                                reasons,
                            });
                        }
                    }
                    None => snapshot_reports.push(TableCompactionSnapshotReport {
                        snapshot_id: Some(current_snapshot_id),
                        manifest_list: None,
                        status: TableCompactionPlanningStatus::ManualReviewRequired,
                        reasons: vec![TableCompactionPlanningReason::MissingManifestList],
                    }),
                }
            }
            None => snapshot_reports.push(TableCompactionSnapshotReport {
                snapshot_id: Some(current_snapshot_id),
                manifest_list: None,
                status: TableCompactionPlanningStatus::ManualReviewRequired,
                reasons: vec![TableCompactionPlanningReason::MissingCurrentSnapshot],
            }),
        }
    }

    let manual_review_count = snapshot_reports
        .iter()
        .filter(|snapshot| snapshot.status == TableCompactionPlanningStatus::ManualReviewRequired)
        .count();
    let status = if manual_review_count > 0 {
        TableCompactionPlanningStatus::ManualReviewRequired
    } else if rewrite_groups.is_empty() {
        TableCompactionPlanningStatus::NoCandidates
    } else {
        TableCompactionPlanningStatus::RewriteCandidates
    };

    Ok(TableCompactionPlanningReport {
        table_bucket: table_bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: entry.table_id.clone(),
        current_metadata_location: entry.metadata_location.clone(),
        current_snapshot_id,
        config,
        status,
        candidate_file_count: candidates.len(),
        rewrite_group_count: rewrite_groups.len(),
        manual_review_count,
        committed_metadata_location: None,
        row_level_planning,
        rewrite_groups,
        snapshot_reports,
    })
}

async fn compaction_data_file_candidates<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    manifest_list: &str,
    config: &TableCompactionPlanningConfig,
) -> TableCatalogStoreResult<CompactionManifestPlanning>
where
    B: TableCatalogObjectBackend,
{
    let Some(manifest_list_key) = table_catalog_object_key_from_location(table_bucket, manifest_list) else {
        return Err(TableCatalogStoreError::Invalid(
            "compaction manifest list must be inside the table bucket".to_string(),
        ));
    };
    if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &manifest_list_key)
        != Some(TableMetadataMaintenanceObjectKind::ManifestList)
    {
        return Err(TableCatalogStoreError::Invalid(
            "compaction manifest list must be inside the table metadata directory".to_string(),
        ));
    }
    let Some(manifest_list_object) = backend
        .read_object_limited(table_bucket, &manifest_list_key, TABLE_MANIFEST_AVRO_MAX_SIZE)
        .await?
    else {
        return Err(TableCatalogStoreError::NotFound(format!("compaction manifest list {manifest_list_key}")));
    };
    let manifest_paths = decode_manifest_list_avro_async(manifest_list_object.data)
        .await?
        .references
        .into_iter()
        .map(|reference| reference.manifest_path)
        .collect::<Vec<_>>();
    let mut planning = CompactionManifestPlanning::default();
    for manifest_location in manifest_paths {
        let Some(manifest_key) = table_catalog_object_key_from_location(table_bucket, &manifest_location) else {
            return Err(TableCatalogStoreError::Invalid(
                "compaction manifest must be inside the table bucket".to_string(),
            ));
        };
        if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &manifest_key)
            != Some(TableMetadataMaintenanceObjectKind::ManifestFile)
        {
            return Err(TableCatalogStoreError::Invalid(
                "compaction manifest must be inside the table metadata directory".to_string(),
            ));
        }
        let Some(manifest_object) = backend
            .read_object_limited(table_bucket, &manifest_key, TABLE_MANIFEST_AVRO_MAX_SIZE)
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!("compaction manifest {manifest_key}")));
        };
        for reference in decode_manifest_avro_async(manifest_object.data).await?.references {
            if reference.object_kind != TableMetadataMaintenanceObjectKind::DataFile {
                record_compaction_row_level_delete_file(
                    backend,
                    table_bucket,
                    namespace,
                    table,
                    warehouse_object_prefix,
                    &mut planning.row_level_planning,
                    &reference,
                )
                .await?;
                continue;
            }
            validate_compaction_manifest_entry_status(reference.entry_status)?;
            let Some(data_key) = table_catalog_object_key_from_location(table_bucket, &reference.location) else {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction data file must be inside the table bucket".to_string(),
                ));
            };
            if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &data_key)
                != Some(TableMetadataMaintenanceObjectKind::DataFile)
            {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction data file must be inside the table data directory".to_string(),
                ));
            }
            let Some(data_object) = backend.read_object(table_bucket, &data_key).await? else {
                return Err(TableCatalogStoreError::NotFound(format!("compaction data file {data_key}")));
            };
            let size_bytes = u64::try_from(data_object.data.len()).unwrap_or(u64::MAX);
            if size_bytes <= config.small_file_threshold_bytes {
                planning.candidates.push(TableCompactionDataFileCandidate {
                    rewrite_prefix: compaction_data_file_rewrite_prefix(namespace, table, warehouse_object_prefix, &data_key)
                        .unwrap_or_else(|| data_key.clone()),
                    location: data_key,
                    size_bytes,
                    sort_order_id: reference.sort_order_id,
                });
            }
        }
    }
    Ok(planning)
}

async fn record_compaction_row_level_delete_file<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    planning: &mut TableRowLevelMaintenancePlanningReport,
    reference: &ManifestDataFileReference,
) -> TableCatalogStoreResult<()>
where
    B: TableCatalogObjectBackend,
{
    let Some(content) = row_level_delete_file_content(reference.content) else {
        return Ok(());
    };
    let Some(delete_key) = table_catalog_object_key_from_location(table_bucket, &reference.location) else {
        return Err(TableCatalogStoreError::Invalid(
            "compaction delete file must be inside the table bucket".to_string(),
        ));
    };
    if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &delete_key)
        != Some(TableMetadataMaintenanceObjectKind::DeleteFile)
    {
        return Err(TableCatalogStoreError::Invalid(
            "compaction delete file must be inside the table delete directory".to_string(),
        ));
    }

    let object_exists = backend.read_object(table_bucket, &delete_key).await?.is_some();
    planning.status = TableRowLevelMaintenancePlanningStatus::ManualReviewRequired;
    planning.delete_file_count = planning.delete_file_count.saturating_add(1);
    planning.manual_review_count = planning.manual_review_count.saturating_add(1);
    push_row_level_planning_reason(planning, TableRowLevelMaintenancePlanningReason::DeleteFileRewriteUnsupported);
    match content {
        TableRowLevelDeleteFileContent::PositionDelete => {
            planning.position_delete_file_count = planning.position_delete_file_count.saturating_add(1);
            push_row_level_planning_reason(planning, TableRowLevelMaintenancePlanningReason::PositionDeleteFile);
        }
        TableRowLevelDeleteFileContent::EqualityDelete => {
            planning.equality_delete_file_count = planning.equality_delete_file_count.saturating_add(1);
            push_row_level_planning_reason(planning, TableRowLevelMaintenancePlanningReason::EqualityDeleteFile);
        }
    }
    if !object_exists {
        push_row_level_planning_reason(planning, TableRowLevelMaintenancePlanningReason::MissingDeleteFile);
    }
    planning.delete_files.push(TableRowLevelDeleteFilePlanningReport {
        file_location: delete_key,
        content,
        object_exists,
        record_count: reference.record_count,
        file_size_bytes: reference.file_size_bytes,
        sequence_number: reference.sequence_number,
        file_sequence_number: reference.file_sequence_number,
    });
    Ok(())
}

fn row_level_delete_file_content(content: ManifestDataFileContent) -> Option<TableRowLevelDeleteFileContent> {
    match content {
        ManifestDataFileContent::Data => None,
        ManifestDataFileContent::PositionDelete => Some(TableRowLevelDeleteFileContent::PositionDelete),
        ManifestDataFileContent::EqualityDelete => Some(TableRowLevelDeleteFileContent::EqualityDelete),
    }
}

fn push_row_level_planning_reason(
    planning: &mut TableRowLevelMaintenancePlanningReport,
    reason: TableRowLevelMaintenancePlanningReason,
) {
    if !planning.reasons.contains(&reason) {
        planning.reasons.push(reason);
    }
}

fn compaction_row_level_planning_reasons(
    planning: &TableRowLevelMaintenancePlanningReport,
) -> Vec<TableCompactionPlanningReason> {
    let mut reasons = vec![
        TableCompactionPlanningReason::ManifestList,
        TableCompactionPlanningReason::ManifestFile,
        TableCompactionPlanningReason::DeleteFile,
        TableCompactionPlanningReason::RowLevelRewriteUnsupported,
    ];
    if planning.position_delete_file_count > 0 {
        reasons.push(TableCompactionPlanningReason::PositionDeleteFile);
    }
    if planning.equality_delete_file_count > 0 {
        reasons.push(TableCompactionPlanningReason::EqualityDeleteFile);
    }
    reasons
}

pub(crate) async fn compaction_current_data_files<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    entry: &TableEntry,
    current_metadata: &serde_json::Value,
) -> TableCatalogStoreResult<Vec<CompactedDataFile>>
where
    B: TableCatalogObjectBackend,
{
    let current_snapshot_id = current_metadata
        .get("current-snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| TableCatalogStoreError::Invalid("compaction requires current snapshot metadata".to_string()))?;
    let current_snapshot = current_metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .find(|snapshot| {
            snapshot
                .get("snapshot-id")
                .and_then(serde_json::Value::as_i64)
                .is_some_and(|snapshot_id| snapshot_id == current_snapshot_id)
        })
        .ok_or_else(|| TableCatalogStoreError::Invalid("compaction requires current snapshot entry".to_string()))?;
    let manifest_list = current_snapshot
        .get("manifest-list")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| TableCatalogStoreError::Invalid("compaction requires current snapshot manifest list".to_string()))?;

    let warehouse_object_prefix = table_warehouse_object_prefix(entry).ok();
    let Some(manifest_list_key) = table_catalog_object_key_from_location(table_bucket, manifest_list) else {
        return Err(TableCatalogStoreError::Invalid(
            "compaction manifest list must be inside the table bucket".to_string(),
        ));
    };
    if table_maintenance_object_kind(namespace, table, warehouse_object_prefix.as_deref(), &manifest_list_key)
        != Some(TableMetadataMaintenanceObjectKind::ManifestList)
    {
        return Err(TableCatalogStoreError::Invalid(
            "compaction manifest list must be inside the table metadata directory".to_string(),
        ));
    }
    let Some(manifest_list_object) = backend
        .read_object_limited(table_bucket, &manifest_list_key, TABLE_MANIFEST_AVRO_MAX_SIZE)
        .await?
    else {
        return Err(TableCatalogStoreError::NotFound(format!("compaction manifest list {manifest_list_key}")));
    };

    let mut data_files = Vec::new();
    for manifest_reference in decode_manifest_list_avro_async(manifest_list_object.data).await?.references {
        let Some(manifest_key) = table_catalog_object_key_from_location(table_bucket, &manifest_reference.manifest_path) else {
            return Err(TableCatalogStoreError::Invalid(
                "compaction manifest must be inside the table bucket".to_string(),
            ));
        };
        if table_maintenance_object_kind(namespace, table, warehouse_object_prefix.as_deref(), &manifest_key)
            != Some(TableMetadataMaintenanceObjectKind::ManifestFile)
        {
            return Err(TableCatalogStoreError::Invalid(
                "compaction manifest must be inside the table metadata directory".to_string(),
            ));
        }
        let Some(manifest_object) = backend
            .read_object_limited(table_bucket, &manifest_key, TABLE_MANIFEST_AVRO_MAX_SIZE)
            .await?
        else {
            return Err(TableCatalogStoreError::NotFound(format!("compaction manifest {manifest_key}")));
        };
        for reference in decode_manifest_avro_async(manifest_object.data).await?.references {
            if reference.object_kind != TableMetadataMaintenanceObjectKind::DataFile {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction currently does not support delete files".to_string(),
                ));
            }
            validate_compaction_manifest_entry_status(reference.entry_status)?;
            let Some(data_key) = table_catalog_object_key_from_location(table_bucket, &reference.location) else {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction data file must be inside the table bucket".to_string(),
                ));
            };
            if table_maintenance_object_kind(namespace, table, warehouse_object_prefix.as_deref(), &data_key)
                != Some(TableMetadataMaintenanceObjectKind::DataFile)
            {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction data file must be inside the table data directory".to_string(),
                ));
            }
            let Some(data_object) = backend.read_object(table_bucket, &data_key).await? else {
                return Err(TableCatalogStoreError::NotFound(format!("compaction data file {data_key}")));
            };
            let snapshot_id = reference
                .snapshot_id
                .or(manifest_reference.added_snapshot_id)
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid("compaction manifest data file missing snapshot id".to_string())
                })?;
            let sequence_number = reference
                .sequence_number
                .or(manifest_reference.sequence_number)
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid("compaction manifest data file missing sequence number".to_string())
                })?;
            let file_sequence_number = reference
                .file_sequence_number
                .or(manifest_reference.sequence_number)
                .ok_or_else(|| {
                    TableCatalogStoreError::Invalid("compaction manifest data file missing file sequence number".to_string())
                })?;
            data_files.push(CompactedDataFile {
                object_key: data_key,
                file_path: reference.location,
                file_size_bytes: reference
                    .file_size_bytes
                    .unwrap_or_else(|| u64::try_from(data_object.data.len()).unwrap_or(u64::MAX)),
                record_count: match reference.record_count {
                    Some(record_count) => record_count,
                    None => parquet_record_count(&data_object.data)?,
                },
                partition_spec_id: manifest_reference.partition_spec_id.unwrap_or(0),
                partition: reference.partition,
                sort_order_id: reference.sort_order_id,
                status: 0,
                snapshot_id,
                sequence_number,
                file_sequence_number,
            });
        }
    }

    Ok(data_files)
}

fn compaction_rewrite_groups(
    candidates: &[TableCompactionDataFileCandidate],
    config: &TableCompactionPlanningConfig,
) -> Vec<TableCompactionRewriteGroup> {
    let mut groups = Vec::new();
    let mut candidates_by_prefix = BTreeMap::<(&str, Option<i32>), Vec<&TableCompactionDataFileCandidate>>::new();
    for candidate in candidates {
        candidates_by_prefix
            .entry((candidate.rewrite_prefix.as_str(), candidate.sort_order_id))
            .or_default()
            .push(candidate);
    }

    for ((_, sort_order_id), prefix_candidates) in candidates_by_prefix {
        push_compaction_rewrite_groups_for_prefix(&mut groups, prefix_candidates.as_slice(), sort_order_id, config);
    }
    groups
}

fn push_compaction_rewrite_groups_for_prefix(
    groups: &mut Vec<TableCompactionRewriteGroup>,
    candidates: &[&TableCompactionDataFileCandidate],
    sort_order_id: Option<i32>,
    config: &TableCompactionPlanningConfig,
) {
    let mut current_locations = Vec::new();
    let mut current_bytes = 0_u64;
    for candidate in candidates {
        let next_bytes = current_bytes.saturating_add(candidate.size_bytes);
        if !current_locations.is_empty() && next_bytes > config.max_rewrite_bytes_per_job {
            push_compaction_rewrite_group(groups, &mut current_locations, &mut current_bytes, sort_order_id, config);
        }
        current_locations.push(candidate.location.clone());
        current_bytes = current_bytes.saturating_add(candidate.size_bytes);
    }
    push_compaction_rewrite_group(groups, &mut current_locations, &mut current_bytes, sort_order_id, config);
}

pub(crate) fn compaction_data_file_rewrite_prefix(
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    location: &str,
) -> Option<String> {
    let warehouse_data_prefix = warehouse_object_prefix
        .map(|prefix| format!("{prefix}{DATA_DIR}"))
        .unwrap_or_else(|| default_table_data_dir_path(namespace, table));
    let default_data_prefix = format!("{}/", default_table_data_dir_path(namespace, table));
    if let Some(relative_path) = location.strip_prefix(&default_data_prefix) {
        return Some(compaction_data_file_output_prefix(&warehouse_data_prefix, relative_path));
    }
    if let Some(warehouse_object_prefix) = warehouse_object_prefix {
        let warehouse_input_prefix = format!("{warehouse_object_prefix}{DATA_DIR}/");
        if let Some(relative_path) = location.strip_prefix(&warehouse_input_prefix) {
            return Some(compaction_data_file_output_prefix(&warehouse_data_prefix, relative_path));
        }
    }
    None
}

fn compaction_data_file_output_prefix(output_data_prefix: &str, relative_path: &str) -> String {
    relative_path
        .rsplit_once('/')
        .map(|(partition_path, _)| format!("{output_data_prefix}/{partition_path}"))
        .unwrap_or_else(|| output_data_prefix.to_string())
}

fn push_compaction_rewrite_group(
    groups: &mut Vec<TableCompactionRewriteGroup>,
    current_locations: &mut Vec<String>,
    current_bytes: &mut u64,
    sort_order_id: Option<i32>,
    config: &TableCompactionPlanningConfig,
) {
    if current_locations.len() >= config.min_input_files {
        let input_file_count = current_locations.len();
        groups.push(TableCompactionRewriteGroup {
            group_id: format!("{:04}", groups.len() + 1),
            sort_order_id,
            input_file_locations: std::mem::take(current_locations),
            input_file_count,
            input_bytes: *current_bytes,
            output_file_location: None,
            output_bytes: None,
        });
    } else {
        current_locations.clear();
    }
    *current_bytes = 0;
}

pub(crate) fn compaction_rewrite_group_partition(
    data_files_by_key: &BTreeMap<&str, &CompactedDataFile>,
    rewrite_group: &TableCompactionRewriteGroup,
) -> TableCatalogStoreResult<(i32, Vec<(String, apache_avro::types::Value)>)> {
    let mut partition_spec_id = None;
    let mut partition = None;
    for input in &rewrite_group.input_file_locations {
        let Some(data_file) = data_files_by_key.get(input.as_str()) else {
            return Err(TableCatalogStoreError::Invalid(
                "compaction rewrite input is missing from current manifest".to_string(),
            ));
        };
        match (partition_spec_id, partition.as_ref()) {
            (None, None) => {
                partition_spec_id = Some(data_file.partition_spec_id);
                partition = Some(data_file.partition.clone());
            }
            (Some(expected_spec_id), Some(expected_partition))
                if expected_spec_id == data_file.partition_spec_id && expected_partition == &data_file.partition => {}
            _ => {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction rewrite group must contain a single partition tuple".to_string(),
                ));
            }
        }
    }
    Ok((partition_spec_id.unwrap_or(0), partition.unwrap_or_default()))
}

pub(crate) fn compaction_rewrite_group_sort_order(
    data_files_by_key: &BTreeMap<&str, &CompactedDataFile>,
    rewrite_group: &TableCompactionRewriteGroup,
) -> TableCatalogStoreResult<Option<i32>> {
    let mut sort_order_id = None;
    let mut initialized = false;
    for input in &rewrite_group.input_file_locations {
        let Some(data_file) = data_files_by_key.get(input.as_str()) else {
            return Err(TableCatalogStoreError::Invalid(
                "compaction rewrite input is missing from current manifest".to_string(),
            ));
        };
        if !initialized {
            sort_order_id = data_file.sort_order_id;
            initialized = true;
        } else if sort_order_id != data_file.sort_order_id {
            return Err(TableCatalogStoreError::Invalid(
                "compaction rewrite group must contain a single sort order".to_string(),
            ));
        }
    }
    if rewrite_group.sort_order_id != sort_order_id {
        return Err(TableCatalogStoreError::Invalid(
            "compaction rewrite group sort order changed after planning".to_string(),
        ));
    }
    Ok(sort_order_id)
}

pub(crate) fn compaction_manifest_partition_spec_id(data_files: &[CompactedDataFile]) -> TableCatalogStoreResult<i32> {
    let Some(first) = data_files.first() else {
        return Ok(0);
    };
    let partition_spec_id = first.partition_spec_id;
    if data_files
        .iter()
        .any(|data_file| data_file.partition_spec_id != partition_spec_id)
    {
        return Err(TableCatalogStoreError::Invalid(
            "compaction manifest cannot mix partition spec ids".to_string(),
        ));
    }
    Ok(partition_spec_id)
}

pub(crate) fn compact_parquet_data_files(input_files: &[(String, Vec<u8>)]) -> TableCatalogStoreResult<CompactedParquetFile> {
    let mut schema: Option<SchemaRef> = None;
    let mut batches = Vec::new();
    let mut record_count = 0_u64;

    for (location, data) in input_files {
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data.clone())).map_err(|err| {
            TableCatalogStoreError::Invalid(format!("failed to read compaction input parquet {location}: {err}"))
        })?;
        let file_schema = builder.schema().clone();
        match schema.as_ref() {
            Some(expected_schema) if expected_schema.as_ref() != file_schema.as_ref() => {
                return Err(TableCatalogStoreError::Invalid("compaction input parquet schemas must match".to_string()));
            }
            Some(_) => {}
            None => schema = Some(file_schema),
        }

        let reader = builder.build().map_err(|err| {
            TableCatalogStoreError::Invalid(format!("failed to build compaction parquet reader {location}: {err}"))
        })?;
        for batch in reader {
            let batch = batch.map_err(|err| {
                TableCatalogStoreError::Invalid(format!("failed to read compaction parquet batch {location}: {err}"))
            })?;
            record_count = record_count.saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
            batches.push(batch);
        }
    }

    let Some(schema) = schema else {
        return Err(TableCatalogStoreError::Invalid(
            "compaction requires at least one parquet input file".to_string(),
        ));
    };
    let mut data = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut data, schema, None)
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to build compaction parquet writer: {err}")))?;
        for batch in batches {
            writer
                .write(&batch)
                .map_err(|err| TableCatalogStoreError::Internal(format!("failed to write compaction parquet batch: {err}")))?;
        }
        writer
            .close()
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to close compaction parquet writer: {err}")))?;
    }

    Ok(CompactedParquetFile { data, record_count })
}

fn validate_compaction_manifest_entry_status(entry_status: Option<i32>) -> TableCatalogStoreResult<()> {
    match entry_status {
        Some(0 | 1) => Ok(()),
        Some(2) => Err(TableCatalogStoreError::Invalid(
            "compaction currently does not support deleted manifest entries".to_string(),
        )),
        Some(_) => Err(TableCatalogStoreError::Invalid(
            "compaction manifest entry status is unsupported".to_string(),
        )),
        None => Err(TableCatalogStoreError::Invalid("compaction manifest entry missing status".to_string())),
    }
}

fn parquet_record_count(data: &[u8]) -> TableCatalogStoreResult<u64> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(data))
        .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to read compaction parquet metadata: {err}")))?;
    u64::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| TableCatalogStoreError::Invalid("compaction parquet record count must not be negative".to_string()))
}

pub(crate) fn compacted_manifest_list_avro_bytes(summary: CompactionManifestListSummary<'_>) -> TableCatalogStoreResult<Vec<u8>> {
    let schema = apache_avro::Schema::parse_str(
        r#"
        {
          "type": "record",
          "name": "manifest_file",
          "fields": [
            {"name": "manifest_path", "type": "string"},
            {"name": "manifest_length", "type": "long"},
            {"name": "partition_spec_id", "type": "int"},
            {"name": "content", "type": "int"},
            {"name": "sequence_number", "type": "long"},
            {"name": "min_sequence_number", "type": "long"},
            {"name": "added_snapshot_id", "type": "long"},
            {"name": "added_files_count", "type": "int"},
            {"name": "existing_files_count", "type": "int"},
            {"name": "deleted_files_count", "type": "int"},
            {"name": "added_rows_count", "type": "long"},
            {"name": "existing_rows_count", "type": "long"},
            {"name": "deleted_rows_count", "type": "long"},
            {"name": "partitions", "type": ["null", {"type": "array", "items": {"type": "record", "name": "field_summary", "fields": [
              {"name": "contains_null", "type": "boolean"},
              {"name": "lower_bound", "type": ["null", "bytes"], "default": null},
              {"name": "upper_bound", "type": ["null", "bytes"], "default": null}
            ]}}], "default": null}
          ]
        }
        "#,
    )
    .map_err(|err| TableCatalogStoreError::Internal(format!("failed to build compaction manifest list schema: {err}")))?;
    let mut writer = apache_avro::Writer::new(&schema, Vec::new()).map_err(|err| {
        TableCatalogStoreError::Internal(format!("failed to initialize compaction manifest list writer: {err}"))
    })?;
    writer
        .append_value(apache_avro::types::Value::Record(vec![
            (
                "manifest_path".to_string(),
                apache_avro::types::Value::String(summary.manifest_path.to_string()),
            ),
            (
                "manifest_length".to_string(),
                apache_avro::types::Value::Long(i64::try_from(summary.manifest_length).unwrap_or(i64::MAX)),
            ),
            ("partition_spec_id".to_string(), apache_avro::types::Value::Int(summary.partition_spec_id)),
            ("content".to_string(), apache_avro::types::Value::Int(0)),
            ("sequence_number".to_string(), apache_avro::types::Value::Long(summary.sequence_number)),
            (
                "min_sequence_number".to_string(),
                apache_avro::types::Value::Long(summary.sequence_number),
            ),
            ("added_snapshot_id".to_string(), apache_avro::types::Value::Long(summary.snapshot_id)),
            (
                "added_files_count".to_string(),
                apache_avro::types::Value::Int(i32::try_from(summary.added_files_count).unwrap_or(i32::MAX)),
            ),
            (
                "existing_files_count".to_string(),
                apache_avro::types::Value::Int(i32::try_from(summary.existing_files_count).unwrap_or(i32::MAX)),
            ),
            ("deleted_files_count".to_string(), apache_avro::types::Value::Int(0)),
            (
                "added_rows_count".to_string(),
                apache_avro::types::Value::Long(i64::try_from(summary.added_rows_count).unwrap_or(i64::MAX)),
            ),
            (
                "existing_rows_count".to_string(),
                apache_avro::types::Value::Long(i64::try_from(summary.existing_rows_count).unwrap_or(i64::MAX)),
            ),
            ("deleted_rows_count".to_string(), apache_avro::types::Value::Long(0)),
            (
                "partitions".to_string(),
                apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
            ),
        ]))
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to write compaction manifest list: {err}")))?;
    writer
        .into_inner()
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to flush compaction manifest list: {err}")))
}

fn compacted_manifest_avro_schema(data_files: &[CompactedDataFile]) -> TableCatalogStoreResult<apache_avro::Schema> {
    let partition_fields = compaction_partition_schema_fields(data_files)?;
    let partition_schema_fields = partition_fields
        .into_iter()
        .map(|(name, field_type)| {
            serde_json::json!({
                "name": name,
                "type": field_type
            })
        })
        .collect::<Vec<_>>();
    let schema = serde_json::json!({
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int"},
            {"name": "snapshot_id", "type": "long"},
            {"name": "sequence_number", "type": "long"},
            {"name": "file_sequence_number", "type": "long"},
            {
                "name": "data_file",
                "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                        {"name": "content", "type": "int"},
                        {"name": "file_path", "type": "string"},
                        {"name": "file_format", "type": "string"},
                        {
                            "name": "partition",
                            "type": {
                                "type": "record",
                                "name": "partition",
                                "fields": partition_schema_fields
                            }
                        },
                        {"name": "record_count", "type": "long"},
                        {"name": "file_size_in_bytes", "type": "long"},
                        {"name": "column_sizes", "type": ["null", {"type": "map", "values": "long"}], "default": null},
                        {"name": "value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
                        {"name": "null_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
                        {"name": "nan_value_counts", "type": ["null", {"type": "map", "values": "long"}], "default": null},
                        {"name": "lower_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
                        {"name": "upper_bounds", "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
                        {"name": "key_metadata", "type": ["null", "bytes"], "default": null},
                        {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long"}], "default": null},
                        {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int"}], "default": null},
                        {"name": "sort_order_id", "type": ["null", "int"], "default": null}
                    ]
                }
            }
        ]
    });
    apache_avro::Schema::parse_str(&schema.to_string())
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to build compaction manifest schema: {err}")))
}

fn compaction_partition_schema_fields(
    data_files: &[CompactedDataFile],
) -> TableCatalogStoreResult<Vec<(String, serde_json::Value)>> {
    let Some(first) = data_files.first() else {
        return Ok(Vec::new());
    };
    let mut expected = Vec::with_capacity(first.partition.len());
    for (field_name, field_value) in &first.partition {
        let Some(field_type) = compaction_partition_field_schema(field_value) else {
            return Err(TableCatalogStoreError::Invalid(
                "compaction partition value type is unsupported".to_string(),
            ));
        };
        expected.push((field_name.clone(), field_type));
    }

    for data_file in data_files.iter().skip(1) {
        if data_file.partition.len() != expected.len() {
            return Err(TableCatalogStoreError::Invalid(
                "compaction manifest partition schemas must match".to_string(),
            ));
        }
        for ((expected_name, expected_type), (field_name, field_value)) in expected.iter().zip(&data_file.partition) {
            let Some(field_type) = compaction_partition_field_schema(field_value) else {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction partition value type is unsupported".to_string(),
                ));
            };
            if expected_name != field_name || expected_type != &field_type {
                return Err(TableCatalogStoreError::Invalid(
                    "compaction manifest partition schemas must match".to_string(),
                ));
            }
        }
    }
    Ok(expected)
}

fn compaction_partition_field_schema(value: &apache_avro::types::Value) -> Option<serde_json::Value> {
    match avro_non_union_value(value) {
        apache_avro::types::Value::Boolean(_) => Some(serde_json::json!("boolean")),
        apache_avro::types::Value::Int(_) => Some(serde_json::json!("int")),
        apache_avro::types::Value::Long(_) => Some(serde_json::json!("long")),
        apache_avro::types::Value::Float(_) => Some(serde_json::json!("float")),
        apache_avro::types::Value::Double(_) => Some(serde_json::json!("double")),
        apache_avro::types::Value::Bytes(_) => Some(serde_json::json!("bytes")),
        apache_avro::types::Value::String(_) => Some(serde_json::json!("string")),
        apache_avro::types::Value::Date(_) => Some(serde_json::json!({"type": "int", "logicalType": "date"})),
        apache_avro::types::Value::TimeMillis(_) => Some(serde_json::json!({"type": "int", "logicalType": "time-millis"})),
        apache_avro::types::Value::TimeMicros(_) => Some(serde_json::json!({"type": "long", "logicalType": "time-micros"})),
        apache_avro::types::Value::TimestampMillis(_) => {
            Some(serde_json::json!({"type": "long", "logicalType": "timestamp-millis"}))
        }
        apache_avro::types::Value::TimestampMicros(_) => {
            Some(serde_json::json!({"type": "long", "logicalType": "timestamp-micros"}))
        }
        apache_avro::types::Value::LocalTimestampMillis(_) => {
            Some(serde_json::json!({"type": "long", "logicalType": "local-timestamp-millis"}))
        }
        apache_avro::types::Value::LocalTimestampMicros(_) => {
            Some(serde_json::json!({"type": "long", "logicalType": "local-timestamp-micros"}))
        }
        apache_avro::types::Value::Uuid(_) => Some(serde_json::json!({"type": "string", "logicalType": "uuid"})),
        _ => None,
    }
}

pub(crate) fn compacted_manifest_avro_bytes(data_files: &[CompactedDataFile]) -> TableCatalogStoreResult<Vec<u8>> {
    let schema = compacted_manifest_avro_schema(data_files)?;
    let mut writer = apache_avro::Writer::new(&schema, Vec::new())
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to initialize compaction manifest writer: {err}")))?;
    for data_file in data_files {
        let sort_order_id = match data_file.sort_order_id {
            Some(sort_order_id) => apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::Int(sort_order_id))),
            None => apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
        };
        writer
            .append_value(apache_avro::types::Value::Record(vec![
                ("status".to_string(), apache_avro::types::Value::Int(data_file.status)),
                ("snapshot_id".to_string(), apache_avro::types::Value::Long(data_file.snapshot_id)),
                ("sequence_number".to_string(), apache_avro::types::Value::Long(data_file.sequence_number)),
                (
                    "file_sequence_number".to_string(),
                    apache_avro::types::Value::Long(data_file.file_sequence_number),
                ),
                (
                    "data_file".to_string(),
                    apache_avro::types::Value::Record(vec![
                        ("content".to_string(), apache_avro::types::Value::Int(0)),
                        ("file_path".to_string(), apache_avro::types::Value::String(data_file.file_path.clone())),
                        ("file_format".to_string(), apache_avro::types::Value::String("PARQUET".to_string())),
                        ("partition".to_string(), apache_avro::types::Value::Record(data_file.partition.clone())),
                        (
                            "record_count".to_string(),
                            apache_avro::types::Value::Long(i64::try_from(data_file.record_count).unwrap_or(i64::MAX)),
                        ),
                        (
                            "file_size_in_bytes".to_string(),
                            apache_avro::types::Value::Long(i64::try_from(data_file.file_size_bytes).unwrap_or(i64::MAX)),
                        ),
                        (
                            "column_sizes".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "value_counts".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "null_value_counts".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "nan_value_counts".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "lower_bounds".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "upper_bounds".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "key_metadata".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "split_offsets".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        (
                            "equality_ids".to_string(),
                            apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
                        ),
                        ("sort_order_id".to_string(), sort_order_id),
                    ]),
                ),
            ]))
            .map_err(|err| TableCatalogStoreError::Internal(format!("failed to write compaction manifest: {err}")))?;
    }
    writer
        .into_inner()
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to flush compaction manifest: {err}")))
}

pub(crate) fn compaction_snapshot_id(current_metadata: &serde_json::Value, entry: &TableEntry, now: OffsetDateTime) -> i64 {
    let generation = i64::try_from(entry.generation).unwrap_or(i64::MAX);
    let mut snapshot_id = unix_timestamp_millis(now).saturating_mul(1000).saturating_add(generation);
    let existing_snapshot_ids = current_metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|snapshot| snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64))
        .collect::<BTreeSet<_>>();
    while existing_snapshot_ids.contains(&snapshot_id) {
        snapshot_id = snapshot_id.saturating_add(1);
    }
    snapshot_id
}

pub(crate) fn next_compaction_sequence_number(current_metadata: &serde_json::Value) -> i64 {
    current_metadata
        .get("last-sequence-number")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0)
        .saturating_add(1)
}

pub(crate) fn compaction_metadata_json(
    current_metadata: &serde_json::Value,
    entry: &TableEntry,
    snapshot_id: i64,
    sequence_number: i64,
    manifest_list: &str,
    previous_metadata_location: &str,
    now: OffsetDateTime,
) -> TableCatalogStoreResult<Vec<u8>> {
    let mut metadata = current_metadata.clone();
    let now_ms = unix_timestamp_millis(now);
    let Some(metadata_object) = metadata.as_object_mut() else {
        return Err(TableCatalogStoreError::Invalid(
            "compaction metadata source must be a JSON object".to_string(),
        ));
    };
    metadata_object.insert("last-sequence-number".to_string(), serde_json::json!(sequence_number));
    metadata_object.insert("last-updated-ms".to_string(), serde_json::json!(now_ms));
    metadata_object.insert("current-snapshot-id".to_string(), serde_json::json!(snapshot_id));

    let snapshots = metadata_object
        .entry("snapshots".to_string())
        .or_insert_with(|| serde_json::json!([]));
    let Some(snapshots) = snapshots.as_array_mut() else {
        return Err(TableCatalogStoreError::Invalid(
            "compaction metadata snapshots must be an array".to_string(),
        ));
    };
    snapshots.push(serde_json::json!({
        "snapshot-id": snapshot_id,
        "sequence-number": sequence_number,
        "timestamp-ms": now_ms,
        "manifest-list": manifest_list,
        "summary": {
            "operation": "rewrite",
            "rustfs.maintenance": "compaction"
        }
    }));

    let snapshot_log = metadata_object
        .entry("snapshot-log".to_string())
        .or_insert_with(|| serde_json::json!([]));
    let Some(snapshot_log) = snapshot_log.as_array_mut() else {
        return Err(TableCatalogStoreError::Invalid(
            "compaction metadata snapshot log must be an array".to_string(),
        ));
    };
    snapshot_log.push(serde_json::json!({
        "timestamp-ms": now_ms,
        "snapshot-id": snapshot_id
    }));

    let metadata_log = metadata_object
        .entry("metadata-log".to_string())
        .or_insert_with(|| serde_json::json!([]));
    let Some(metadata_log) = metadata_log.as_array_mut() else {
        return Err(TableCatalogStoreError::Invalid("compaction metadata log must be an array".to_string()));
    };
    metadata_log.push(serde_json::json!({
        "timestamp-ms": now_ms,
        "metadata-file": previous_metadata_location
    }));

    let refs = metadata_object
        .entry("refs".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let Some(refs) = refs.as_object_mut() else {
        return Err(TableCatalogStoreError::Invalid("compaction metadata refs must be an object".to_string()));
    };
    refs.insert(
        ICEBERG_MAIN_REF.to_string(),
        serde_json::json!({
            "snapshot-id": snapshot_id,
            "type": "branch"
        }),
    );
    metadata_object
        .entry("location".to_string())
        .or_insert_with(|| serde_json::json!(entry.warehouse_location));

    serde_json::to_vec(&metadata)
        .map_err(|err| TableCatalogStoreError::Internal(format!("failed to serialize compaction metadata: {err}")))
}

fn snapshot_expiration_drafts(
    current_metadata: &serde_json::Value,
    current_snapshot_id: Option<i64>,
) -> Vec<TableSnapshotExpirationDraft> {
    let Some(snapshots) = current_metadata.get("snapshots").and_then(serde_json::Value::as_array) else {
        return Vec::new();
    };

    snapshots
        .iter()
        .map(|snapshot| {
            let snapshot_id = snapshot.get("snapshot-id").and_then(serde_json::Value::as_i64);
            let timestamp_ms = snapshot.get("timestamp-ms").and_then(serde_json::Value::as_i64);
            let mut reasons = BTreeSet::new();
            if snapshot_id.is_none() {
                reasons.insert(TableSnapshotExpirationReason::MissingSnapshotId);
            }
            if timestamp_ms.is_none() {
                reasons.insert(TableSnapshotExpirationReason::MissingSnapshotTimestamp);
            }
            if snapshot_id.is_some() && snapshot_id == current_snapshot_id {
                reasons.insert(TableSnapshotExpirationReason::CurrentSnapshot);
            }

            TableSnapshotExpirationDraft {
                snapshot_id,
                sequence_number: snapshot.get("sequence-number").and_then(serde_json::Value::as_i64),
                timestamp_ms,
                manifest_list: snapshot
                    .get("manifest-list")
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string),
                reasons,
            }
        })
        .collect()
}

fn mark_recent_snapshots_to_keep(drafts: &mut [TableSnapshotExpirationDraft], min_snapshots_to_keep: usize) {
    let mut snapshots_by_time = drafts
        .iter()
        .enumerate()
        .filter_map(|(index, draft)| Some((draft.timestamp_ms?, index)))
        .collect::<Vec<_>>();
    snapshots_by_time.sort_by(|(left_timestamp, left_index), (right_timestamp, right_index)| {
        right_timestamp.cmp(left_timestamp).then_with(|| left_index.cmp(right_index))
    });

    for (_, index) in snapshots_by_time.into_iter().take(min_snapshots_to_keep) {
        drafts[index]
            .reasons
            .insert(TableSnapshotExpirationReason::MinSnapshotsToKeep);
    }
}

fn snapshot_expiration_ref_state(
    current_metadata: &serde_json::Value,
    current_snapshot_id: Option<i64>,
) -> (BTreeSet<i64>, BTreeSet<i64>, BTreeSet<i64>) {
    let mut protected_ref_snapshot_ids = BTreeSet::new();
    let mut user_defined_ref_snapshot_ids = BTreeSet::new();
    let mut ref_retention_conflict_snapshot_ids = BTreeSet::new();
    let Some(refs) = current_metadata.get("refs").and_then(serde_json::Value::as_object) else {
        return (
            protected_ref_snapshot_ids,
            user_defined_ref_snapshot_ids,
            ref_retention_conflict_snapshot_ids,
        );
    };

    for (name, reference) in refs {
        let Some(snapshot_id) = reference.get("snapshot-id").and_then(serde_json::Value::as_i64) else {
            continue;
        };
        if name != ICEBERG_MAIN_REF || Some(snapshot_id) != current_snapshot_id {
            protected_ref_snapshot_ids.insert(snapshot_id);
        }
        if name != ICEBERG_MAIN_REF {
            user_defined_ref_snapshot_ids.insert(snapshot_id);
        }
        if snapshot_ref_has_retention_policy(reference) {
            ref_retention_conflict_snapshot_ids.insert(snapshot_id);
        }
    }

    (
        protected_ref_snapshot_ids,
        user_defined_ref_snapshot_ids,
        ref_retention_conflict_snapshot_ids,
    )
}

fn snapshot_ref_has_retention_policy(reference: &serde_json::Value) -> bool {
    reference.get(ICEBERG_REF_MIN_SNAPSHOTS_TO_KEEP_FIELD).is_some()
        || reference.get(ICEBERG_REF_MAX_SNAPSHOT_AGE_MS_FIELD).is_some()
        || reference.get(ICEBERG_REF_MAX_REF_AGE_MS_FIELD).is_some()
}

fn snapshot_expiration_table_property_conflicts(
    current_metadata: &serde_json::Value,
    config: &TableSnapshotExpirationConfig,
) -> bool {
    let Some(properties) = current_metadata.get("properties").and_then(serde_json::Value::as_object) else {
        return false;
    };

    if properties.contains_key(ICEBERG_MAX_REF_AGE_MS_PROPERTY) {
        return true;
    }
    if retention_property_conflicts_usize(properties, ICEBERG_MIN_SNAPSHOTS_TO_KEEP_PROPERTY, config.min_snapshots_to_keep) {
        return true;
    }
    retention_property_conflicts_i64(properties, ICEBERG_MAX_SNAPSHOT_AGE_MS_PROPERTY, config.max_snapshot_age_ms)
}

fn retention_property_conflicts_usize(
    properties: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    expected: usize,
) -> bool {
    let Some(value) = properties.get(key) else {
        return false;
    };
    serde_json_i64(value).and_then(|value| usize::try_from(value).ok()) != Some(expected)
}

fn retention_property_conflicts_i64(properties: &serde_json::Map<String, serde_json::Value>, key: &str, expected: i64) -> bool {
    let Some(value) = properties.get(key) else {
        return false;
    };
    serde_json_i64(value) != Some(expected)
}

fn serde_json_i64(value: &serde_json::Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse::<i64>().ok())
}

fn snapshot_expiration_requires_manual_review(reasons: &BTreeSet<TableSnapshotExpirationReason>) -> bool {
    reasons.contains(&TableSnapshotExpirationReason::MissingSnapshotId)
        || reasons.contains(&TableSnapshotExpirationReason::MissingSnapshotTimestamp)
        || reasons.contains(&TableSnapshotExpirationReason::UserDefinedSnapshotRef)
        || reasons.contains(&TableSnapshotExpirationReason::SnapshotRefRetentionConflict)
        || reasons.contains(&TableSnapshotExpirationReason::TableRetentionPropertyConflict)
}

fn snapshot_expiration_is_retained(reasons: &BTreeSet<TableSnapshotExpirationReason>) -> bool {
    reasons.contains(&TableSnapshotExpirationReason::CurrentSnapshot)
        || reasons.contains(&TableSnapshotExpirationReason::MinSnapshotsToKeep)
        || reasons.contains(&TableSnapshotExpirationReason::ProtectedSnapshotRef)
}

fn unix_timestamp_millis(now: OffsetDateTime) -> i64 {
    now.unix_timestamp()
        .saturating_mul(1000)
        .saturating_add(i64::from(now.millisecond()))
}
