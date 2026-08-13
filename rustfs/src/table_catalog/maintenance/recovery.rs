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

pub(crate) fn insert_metadata_maintenance_reason(
    reasons_by_location: &mut BTreeMap<String, BTreeSet<TableMetadataMaintenanceReason>>,
    metadata_location: String,
    reason: TableMetadataMaintenanceReason,
) {
    reasons_by_location.entry(metadata_location).or_default().insert(reason);
}

pub(crate) fn metadata_maintenance_object_reports(
    reasons_by_location: BTreeMap<String, BTreeSet<TableMetadataMaintenanceReason>>,
) -> Vec<TableMetadataMaintenanceObjectReport> {
    reasons_by_location
        .into_iter()
        .map(|(metadata_location, reasons)| {
            let reasons = reasons.into_iter().collect::<Vec<_>>();
            let state = if reasons.contains(&TableMetadataMaintenanceReason::SafetyWindowSatisfied) {
                TableMetadataMaintenanceObjectState::Deletable
            } else if reasons.contains(&TableMetadataMaintenanceReason::SafetyWindowPending) {
                TableMetadataMaintenanceObjectState::PendingSafetyWindow
            } else {
                TableMetadataMaintenanceObjectState::Retained
            };
            TableMetadataMaintenanceObjectReport {
                metadata_location,
                state,
                reasons,
            }
        })
        .collect()
}

#[derive(Debug, Clone)]
struct TableMetadataMaintenanceReferencedObjectAccumulator {
    object_kind: TableMetadataMaintenanceObjectKind,
    state: TableMetadataMaintenanceObjectState,
    reasons: BTreeSet<TableMetadataMaintenanceReason>,
}

fn insert_referenced_object_report(
    reports: &mut BTreeMap<String, TableMetadataMaintenanceReferencedObjectAccumulator>,
    object_location: String,
    object_kind: TableMetadataMaintenanceObjectKind,
    state: TableMetadataMaintenanceObjectState,
    reason: TableMetadataMaintenanceReason,
) {
    let report = reports
        .entry(object_location)
        .or_insert_with(|| TableMetadataMaintenanceReferencedObjectAccumulator {
            object_kind,
            state: TableMetadataMaintenanceObjectState::Retained,
            reasons: BTreeSet::new(),
        });
    if state == TableMetadataMaintenanceObjectState::ManualReviewRequired {
        report.state = TableMetadataMaintenanceObjectState::ManualReviewRequired;
    }
    report.reasons.insert(reason);
}

pub(crate) async fn metadata_maintenance_referenced_object_reports<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    current_metadata: &serde_json::Value,
    retained_metadata_locations: &[String],
) -> TableCatalogStoreResult<Vec<TableMetadataMaintenanceReferencedObjectReport>>
where
    B: TableCatalogObjectBackend,
{
    let mut reports = BTreeMap::<String, TableMetadataMaintenanceReferencedObjectAccumulator>::new();
    metadata_maintenance_referenced_object_reports_for_metadata(
        backend,
        table_bucket,
        namespace,
        table,
        warehouse_object_prefix,
        current_metadata,
        &mut reports,
    )
    .await?;

    for metadata_location in retained_metadata_locations {
        let metadata = match read_table_metadata_value(backend, table_bucket, metadata_location).await {
            Ok(Some(metadata)) => metadata,
            Ok(None) | Err(TableCatalogStoreError::Invalid(_)) => {
                insert_referenced_object_report(
                    &mut reports,
                    metadata_location.clone(),
                    TableMetadataMaintenanceObjectKind::MetadataFile,
                    TableMetadataMaintenanceObjectState::ManualReviewRequired,
                    TableMetadataMaintenanceReason::UnreadableMetadata,
                );
                continue;
            }
            Err(err) => return Err(err),
        };
        metadata_maintenance_referenced_object_reports_for_metadata(
            backend,
            table_bucket,
            namespace,
            table,
            warehouse_object_prefix,
            &metadata,
            &mut reports,
        )
        .await?;
    }

    Ok(reports
        .into_iter()
        .map(|(object_location, report)| TableMetadataMaintenanceReferencedObjectReport {
            object_location,
            object_kind: report.object_kind,
            state: report.state,
            reasons: report.reasons.into_iter().collect(),
        })
        .collect())
}

async fn metadata_maintenance_referenced_object_reports_for_metadata<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    metadata: &serde_json::Value,
    reports: &mut BTreeMap<String, TableMetadataMaintenanceReferencedObjectAccumulator>,
) -> TableCatalogStoreResult<()>
where
    B: TableCatalogObjectBackend,
{
    let Some(snapshots) = metadata.get("snapshots").and_then(serde_json::Value::as_array) else {
        return Ok(());
    };

    for snapshot in snapshots {
        if let Some(manifest_list_location) = snapshot.get("manifest-list").and_then(serde_json::Value::as_str) {
            metadata_maintenance_referenced_manifest_list(
                backend,
                table_bucket,
                namespace,
                table,
                warehouse_object_prefix,
                manifest_list_location,
                reports,
            )
            .await?;
            continue;
        }

        let Some(manifests) = snapshot.get("manifests").and_then(serde_json::Value::as_array) else {
            continue;
        };
        for manifest in manifests {
            let Some(manifest_location) = manifest.as_str() else {
                insert_referenced_object_report(
                    reports,
                    "snapshots[].manifests".to_string(),
                    TableMetadataMaintenanceObjectKind::ManifestFile,
                    TableMetadataMaintenanceObjectState::ManualReviewRequired,
                    TableMetadataMaintenanceReason::UnsupportedManifestAvro,
                );
                continue;
            };
            metadata_maintenance_referenced_manifest_file(
                backend,
                table_bucket,
                namespace,
                table,
                warehouse_object_prefix,
                manifest_location,
                reports,
            )
            .await?;
        }
    }

    Ok(())
}

async fn metadata_maintenance_referenced_manifest_list<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    manifest_list_location: &str,
    reports: &mut BTreeMap<String, TableMetadataMaintenanceReferencedObjectAccumulator>,
) -> TableCatalogStoreResult<()>
where
    B: TableCatalogObjectBackend,
{
    let Some(manifest_list_key) = table_catalog_object_key_from_location(table_bucket, manifest_list_location) else {
        insert_referenced_object_report(
            reports,
            manifest_list_location.to_string(),
            TableMetadataMaintenanceObjectKind::ManifestList,
            TableMetadataMaintenanceObjectState::ManualReviewRequired,
            TableMetadataMaintenanceReason::UnsupportedManifestAvro,
        );
        return Ok(());
    };
    if !table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &manifest_list_key).is_some_and(|kind| {
        table_maintenance_object_kind_matches_reference(&kind, &TableMetadataMaintenanceObjectKind::ManifestList)
    }) {
        insert_referenced_object_report(
            reports,
            manifest_list_key,
            TableMetadataMaintenanceObjectKind::ManifestList,
            TableMetadataMaintenanceObjectState::ManualReviewRequired,
            TableMetadataMaintenanceReason::UnsupportedManifestAvro,
        );
        return Ok(());
    }
    insert_referenced_object_report(
        reports,
        manifest_list_key.clone(),
        TableMetadataMaintenanceObjectKind::ManifestList,
        TableMetadataMaintenanceObjectState::Retained,
        TableMetadataMaintenanceReason::ManifestList,
    );

    let manifest_list_object = match backend
        .read_object_limited(table_bucket, &manifest_list_key, TABLE_MANIFEST_AVRO_MAX_SIZE)
        .await
    {
        Ok(Some(manifest_list_object)) => manifest_list_object,
        Ok(None) | Err(TableCatalogStoreError::Invalid(_)) => {
            mark_referenced_object_manual_review(
                reports,
                &manifest_list_key,
                TableMetadataMaintenanceReason::UnsupportedManifestAvro,
            );
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    let manifest_paths = match decode_manifest_list_avro_async(manifest_list_object.data).await {
        Ok(decoded) => decoded
            .references
            .into_iter()
            .map(|reference| reference.manifest_path)
            .collect::<Vec<_>>(),
        Err(TableCatalogStoreError::Invalid(_)) => {
            mark_referenced_object_manual_review(
                reports,
                &manifest_list_key,
                TableMetadataMaintenanceReason::UnsupportedManifestAvro,
            );
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    for manifest_location in manifest_paths {
        metadata_maintenance_referenced_manifest_file(
            backend,
            table_bucket,
            namespace,
            table,
            warehouse_object_prefix,
            &manifest_location,
            reports,
        )
        .await?;
    }

    Ok(())
}

async fn metadata_maintenance_referenced_manifest_file<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    manifest_location: &str,
    reports: &mut BTreeMap<String, TableMetadataMaintenanceReferencedObjectAccumulator>,
) -> TableCatalogStoreResult<()>
where
    B: TableCatalogObjectBackend,
{
    let Some(manifest_key) = table_catalog_object_key_from_location(table_bucket, manifest_location) else {
        insert_referenced_object_report(
            reports,
            manifest_location.to_string(),
            TableMetadataMaintenanceObjectKind::ManifestFile,
            TableMetadataMaintenanceObjectState::ManualReviewRequired,
            TableMetadataMaintenanceReason::UnsupportedManifestAvro,
        );
        return Ok(());
    };
    if !table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &manifest_key).is_some_and(|kind| {
        table_maintenance_object_kind_matches_reference(&kind, &TableMetadataMaintenanceObjectKind::ManifestFile)
    }) {
        insert_referenced_object_report(
            reports,
            manifest_key,
            TableMetadataMaintenanceObjectKind::ManifestFile,
            TableMetadataMaintenanceObjectState::ManualReviewRequired,
            TableMetadataMaintenanceReason::UnsupportedManifestAvro,
        );
        return Ok(());
    }
    insert_referenced_object_report(
        reports,
        manifest_key.clone(),
        TableMetadataMaintenanceObjectKind::ManifestFile,
        TableMetadataMaintenanceObjectState::Retained,
        TableMetadataMaintenanceReason::ManifestFile,
    );

    let manifest_object = match backend
        .read_object_limited(table_bucket, &manifest_key, TABLE_MANIFEST_AVRO_MAX_SIZE)
        .await
    {
        Ok(Some(manifest_object)) => manifest_object,
        Ok(None) | Err(TableCatalogStoreError::Invalid(_)) => {
            mark_referenced_object_manual_review(reports, &manifest_key, TableMetadataMaintenanceReason::UnsupportedManifestAvro);
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    let file_references = match decode_manifest_avro_async(manifest_object.data).await {
        Ok(decoded) => decoded
            .references
            .into_iter()
            .map(|reference| (reference.location, reference.object_kind))
            .collect::<Vec<_>>(),
        Err(TableCatalogStoreError::Invalid(_)) => {
            mark_referenced_object_manual_review(reports, &manifest_key, TableMetadataMaintenanceReason::UnsupportedManifestAvro);
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    for (file_location, object_kind) in file_references {
        let Some(file_key) = table_catalog_object_key_from_location(table_bucket, &file_location) else {
            insert_referenced_object_report(
                reports,
                file_location,
                object_kind,
                TableMetadataMaintenanceObjectState::ManualReviewRequired,
                TableMetadataMaintenanceReason::UnsupportedManifestAvro,
            );
            continue;
        };
        if !table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &file_key)
            .is_some_and(|kind| table_maintenance_object_kind_matches_reference(&kind, &object_kind))
        {
            insert_referenced_object_report(
                reports,
                file_key,
                object_kind,
                TableMetadataMaintenanceObjectState::ManualReviewRequired,
                TableMetadataMaintenanceReason::UnsupportedManifestAvro,
            );
            continue;
        }
        insert_referenced_object_report(
            reports,
            file_key,
            object_kind.clone(),
            TableMetadataMaintenanceObjectState::Retained,
            table_metadata_maintenance_reason_for_object_kind(&object_kind),
        );
    }

    Ok(())
}

fn mark_referenced_object_manual_review(
    reports: &mut BTreeMap<String, TableMetadataMaintenanceReferencedObjectAccumulator>,
    object_location: &str,
    reason: TableMetadataMaintenanceReason,
) {
    if let Some(report) = reports.get_mut(object_location) {
        report.state = TableMetadataMaintenanceObjectState::ManualReviewRequired;
        report.reasons.insert(reason);
    }
}

pub(crate) fn table_catalog_object_key_from_location(table_bucket: &str, location: &str) -> Option<String> {
    let object = if let Some(location) = location.strip_prefix("s3://") {
        let (bucket, object) = location.split_once('/')?;
        if bucket != table_bucket {
            return None;
        }
        object
    } else {
        location
    };

    if object.is_empty()
        || object.starts_with('/')
        || object.contains("..")
        || object.contains('\\')
        || object.bytes().any(|byte| byte.is_ascii_control())
    {
        return None;
    }

    Some(object.to_string())
}

pub(crate) fn table_maintenance_object_kind(
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    object_location: &str,
) -> Option<TableMetadataMaintenanceObjectKind> {
    let metadata_prefix = format!("{}/", default_table_metadata_dir_path(namespace, table));
    if let Some(kind) = table_maintenance_metadata_object_kind(&metadata_prefix, object_location) {
        return Some(kind);
    }

    let data_prefix = format!("{}/", default_table_data_dir_path(namespace, table));
    if object_location
        .strip_prefix(&data_prefix)
        .is_some_and(is_valid_table_maintenance_nested_object)
    {
        return Some(TableMetadataMaintenanceObjectKind::DataFile);
    }

    let delete_prefix = format!("{}/", default_table_delete_dir_path(namespace, table));
    if object_location
        .strip_prefix(&delete_prefix)
        .is_some_and(is_valid_table_maintenance_nested_object)
    {
        return Some(TableMetadataMaintenanceObjectKind::DeleteFile);
    }

    if let Some(warehouse_object_prefix) = warehouse_object_prefix {
        let metadata_prefix = format!("{warehouse_object_prefix}{METADATA_DIR}/");
        if let Some(kind) = table_maintenance_metadata_object_kind(&metadata_prefix, object_location) {
            return Some(kind);
        }

        let data_prefix = format!("{warehouse_object_prefix}{DATA_DIR}/");
        if object_location
            .strip_prefix(&data_prefix)
            .is_some_and(is_valid_table_maintenance_nested_object)
        {
            return Some(TableMetadataMaintenanceObjectKind::DataFile);
        }

        let delete_prefix = format!("{warehouse_object_prefix}{DELETE_DIR}/");
        if object_location
            .strip_prefix(&delete_prefix)
            .is_some_and(is_valid_table_maintenance_nested_object)
        {
            return Some(TableMetadataMaintenanceObjectKind::DeleteFile);
        }
    }

    None
}

pub(crate) fn table_maintenance_object_kind_for_entry(
    entry: &TableEntry,
    warehouse_object_prefix: Option<&str>,
    object_location: &str,
) -> Option<TableMetadataMaintenanceObjectKind> {
    let metadata_dir = table_metadata_dir_path_for_entry(entry).ok()?;
    let metadata_prefix = format!("{metadata_dir}/");
    if let Some(kind) = table_maintenance_metadata_object_kind(&metadata_prefix, object_location) {
        return Some(kind);
    }

    let table_root = metadata_dir.strip_suffix(&format!("/{METADATA_DIR}"))?;
    let data_prefix = format!("{table_root}/{DATA_DIR}/");
    if object_location
        .strip_prefix(&data_prefix)
        .is_some_and(is_valid_table_maintenance_nested_object)
    {
        return Some(TableMetadataMaintenanceObjectKind::DataFile);
    }
    let delete_prefix = format!("{table_root}/{DELETE_DIR}/");
    if object_location
        .strip_prefix(&delete_prefix)
        .is_some_and(is_valid_table_maintenance_nested_object)
    {
        return Some(TableMetadataMaintenanceObjectKind::DeleteFile);
    }

    if let Some(warehouse_object_prefix) = warehouse_object_prefix {
        let metadata_prefix = format!("{warehouse_object_prefix}{METADATA_DIR}/");
        if let Some(kind) = table_maintenance_metadata_object_kind(&metadata_prefix, object_location) {
            return Some(kind);
        }
        let data_prefix = format!("{warehouse_object_prefix}{DATA_DIR}/");
        if object_location
            .strip_prefix(&data_prefix)
            .is_some_and(is_valid_table_maintenance_nested_object)
        {
            return Some(TableMetadataMaintenanceObjectKind::DataFile);
        }
        let delete_prefix = format!("{warehouse_object_prefix}{DELETE_DIR}/");
        if object_location
            .strip_prefix(&delete_prefix)
            .is_some_and(is_valid_table_maintenance_nested_object)
        {
            return Some(TableMetadataMaintenanceObjectKind::DeleteFile);
        }
    }

    None
}

pub(crate) fn table_maintenance_object_kind_matches_reference(
    actual: &TableMetadataMaintenanceObjectKind,
    referenced: &TableMetadataMaintenanceObjectKind,
) -> bool {
    match referenced {
        TableMetadataMaintenanceObjectKind::ManifestList | TableMetadataMaintenanceObjectKind::ManifestFile => matches!(
            actual,
            TableMetadataMaintenanceObjectKind::ManifestList | TableMetadataMaintenanceObjectKind::ManifestFile
        ),
        TableMetadataMaintenanceObjectKind::DeleteFile => matches!(
            actual,
            TableMetadataMaintenanceObjectKind::DataFile | TableMetadataMaintenanceObjectKind::DeleteFile
        ),
        _ => actual == referenced,
    }
}

fn table_maintenance_metadata_object_kind(
    metadata_prefix: &str,
    object_location: &str,
) -> Option<TableMetadataMaintenanceObjectKind> {
    let file_name = object_location.strip_prefix(metadata_prefix)?;
    if file_name.is_empty()
        || file_name.contains('/')
        || file_name.contains('\\')
        || file_name.contains("..")
        || file_name.bytes().any(|byte| byte.is_ascii_control())
        || !file_name.ends_with(".avro")
    {
        return None;
    }
    if file_name.starts_with("snap-") {
        return Some(TableMetadataMaintenanceObjectKind::ManifestList);
    }
    Some(TableMetadataMaintenanceObjectKind::ManifestFile)
}

fn is_valid_table_maintenance_nested_object(suffix: &str) -> bool {
    !suffix.is_empty()
        && !suffix.starts_with('/')
        && !suffix.contains("..")
        && !suffix.contains('\\')
        && !suffix.bytes().any(|byte| byte.is_ascii_control())
}

fn table_metadata_maintenance_reason_for_object_kind(
    object_kind: &TableMetadataMaintenanceObjectKind,
) -> TableMetadataMaintenanceReason {
    match object_kind {
        TableMetadataMaintenanceObjectKind::MetadataFile => TableMetadataMaintenanceReason::CurrentMetadata,
        TableMetadataMaintenanceObjectKind::ManifestList => TableMetadataMaintenanceReason::ManifestList,
        TableMetadataMaintenanceObjectKind::ManifestFile => TableMetadataMaintenanceReason::ManifestFile,
        TableMetadataMaintenanceObjectKind::DataFile => TableMetadataMaintenanceReason::DataFile,
        TableMetadataMaintenanceObjectKind::DeleteFile => TableMetadataMaintenanceReason::DeleteFile,
    }
}

pub(crate) fn metadata_maintenance_reachability_graph_report(
    metadata_file_count: usize,
    referenced_object_reports: &[TableMetadataMaintenanceReferencedObjectReport],
) -> TableMaintenanceReachabilityGraphReport {
    let manifest_list_count = referenced_object_reports
        .iter()
        .filter(|report| report.object_kind == TableMetadataMaintenanceObjectKind::ManifestList)
        .count();
    let manifest_file_count = referenced_object_reports
        .iter()
        .filter(|report| report.object_kind == TableMetadataMaintenanceObjectKind::ManifestFile)
        .count();
    let data_file_count = referenced_object_reports
        .iter()
        .filter(|report| report.object_kind == TableMetadataMaintenanceObjectKind::DataFile)
        .count();
    let delete_file_count = referenced_object_reports
        .iter()
        .filter(|report| report.object_kind == TableMetadataMaintenanceObjectKind::DeleteFile)
        .count();
    let manual_review_count = referenced_object_reports
        .iter()
        .filter(|report| report.state == TableMetadataMaintenanceObjectState::ManualReviewRequired)
        .count();
    let mut reasons = BTreeSet::from([TableMaintenanceReachabilityGraphReason::MetadataJsonParsed]);
    if manifest_list_count > 0 {
        reasons.insert(TableMaintenanceReachabilityGraphReason::ManifestListAvroReferenced);
    }
    if referenced_object_reports.iter().any(|report| {
        report
            .reasons
            .contains(&TableMetadataMaintenanceReason::UnsupportedManifestAvro)
    }) {
        reasons.insert(TableMaintenanceReachabilityGraphReason::ManifestAvroReaderUnavailable);
    }

    TableMaintenanceReachabilityGraphReport {
        status: if manual_review_count == 0 {
            TableMaintenanceReachabilityGraphStatus::Complete
        } else {
            TableMaintenanceReachabilityGraphStatus::ManualReviewRequired
        },
        metadata_file_count,
        manifest_list_count,
        manifest_file_count,
        data_file_count,
        delete_file_count,
        manual_review_count,
        reasons: reasons.into_iter().collect(),
    }
}

pub(crate) async fn metadata_maintenance_object_cleanup_reports<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
    referenced_object_reports: &[TableMetadataMaintenanceReferencedObjectReport],
    now: OffsetDateTime,
) -> TableCatalogStoreResult<(usize, Vec<String>, Vec<String>, Vec<TableMetadataMaintenanceObjectCleanupReport>)>
where
    B: TableCatalogObjectBackend,
{
    let scanned_objects =
        table_maintenance_cleanup_objects(backend, table_bucket, namespace, table, warehouse_object_prefix).await?;
    if referenced_object_reports
        .iter()
        .any(|report| report.state == TableMetadataMaintenanceObjectState::ManualReviewRequired)
    {
        return Ok((scanned_objects.len(), Vec::new(), Vec::new(), Vec::new()));
    }

    let referenced_locations = referenced_object_reports
        .iter()
        .filter_map(|report| table_catalog_object_key_from_location(table_bucket, &report.object_location))
        .collect::<BTreeSet<_>>();
    let mut cleanup_candidate_locations = Vec::new();
    let mut deletable_object_locations = Vec::new();
    let mut cleanup_reports = Vec::new();

    for (object_location, object_kind) in scanned_objects {
        if referenced_locations.contains(&object_location) {
            continue;
        }
        let mut reasons = BTreeSet::from([
            table_metadata_maintenance_reason_for_object_kind(&object_kind),
            TableMetadataMaintenanceReason::NoCurrentReachability,
        ]);
        let state = match backend.read_object(table_bucket, &object_location).await? {
            Some(object) if metadata_candidate_is_past_safety_window(object.mod_time, now) => {
                reasons.insert(TableMetadataMaintenanceReason::SafetyWindowSatisfied);
                cleanup_candidate_locations.push(object_location.clone());
                deletable_object_locations.push(object_location.clone());
                TableMetadataMaintenanceObjectState::Deletable
            }
            _ => {
                reasons.insert(TableMetadataMaintenanceReason::SafetyWindowPending);
                cleanup_candidate_locations.push(object_location.clone());
                TableMetadataMaintenanceObjectState::PendingSafetyWindow
            }
        };
        cleanup_reports.push(TableMetadataMaintenanceObjectCleanupReport {
            object_location,
            object_kind,
            state,
            reasons: reasons.into_iter().collect(),
        });
    }

    Ok((
        referenced_locations.len() + cleanup_reports.len(),
        cleanup_candidate_locations,
        deletable_object_locations,
        cleanup_reports,
    ))
}

async fn table_maintenance_cleanup_objects<B>(
    backend: &B,
    table_bucket: &str,
    namespace: &Namespace,
    table: &IdentifierSegment,
    warehouse_object_prefix: Option<&str>,
) -> TableCatalogStoreResult<BTreeMap<String, TableMetadataMaintenanceObjectKind>>
where
    B: TableCatalogObjectBackend,
{
    let mut objects = BTreeMap::new();
    let mut metadata_prefixes = vec![format!("{}/", default_table_metadata_dir_path(namespace, table))];
    let mut data_prefixes = vec![format!("{}/", default_table_data_dir_path(namespace, table))];
    let mut delete_prefixes = vec![format!("{}/", default_table_delete_dir_path(namespace, table))];
    if let Some(warehouse_object_prefix) = warehouse_object_prefix {
        metadata_prefixes.push(format!("{warehouse_object_prefix}{METADATA_DIR}/"));
        data_prefixes.push(format!("{warehouse_object_prefix}{DATA_DIR}/"));
        delete_prefixes.push(format!("{warehouse_object_prefix}{DELETE_DIR}/"));
    }
    metadata_prefixes.sort();
    metadata_prefixes.dedup();
    data_prefixes.sort();
    data_prefixes.dedup();
    delete_prefixes.sort();
    delete_prefixes.dedup();

    for metadata_prefix in metadata_prefixes {
        for object in backend.list_objects(table_bucket, &metadata_prefix).await? {
            if let Some(kind) = table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &object)
                && matches!(
                    kind,
                    TableMetadataMaintenanceObjectKind::ManifestList | TableMetadataMaintenanceObjectKind::ManifestFile
                )
            {
                objects.insert(object, kind);
            }
        }
    }

    for data_prefix in data_prefixes {
        for object in backend.list_objects(table_bucket, &data_prefix).await? {
            if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &object)
                == Some(TableMetadataMaintenanceObjectKind::DataFile)
            {
                objects.insert(object, TableMetadataMaintenanceObjectKind::DataFile);
            }
        }
    }

    for delete_prefix in delete_prefixes {
        for object in backend.list_objects(table_bucket, &delete_prefix).await? {
            if table_maintenance_object_kind(namespace, table, warehouse_object_prefix, &object)
                == Some(TableMetadataMaintenanceObjectKind::DeleteFile)
            {
                objects.insert(object, TableMetadataMaintenanceObjectKind::DeleteFile);
            }
        }
    }

    Ok(objects)
}

pub(crate) fn mark_deleted_metadata_object_reports(
    object_reports: &mut [TableMetadataMaintenanceObjectReport],
    deleted_locations: &BTreeSet<String>,
) {
    for object_report in object_reports {
        if !deleted_locations.contains(&object_report.metadata_location) {
            continue;
        }
        object_report.state = TableMetadataMaintenanceObjectState::Deleted;
        if !object_report
            .reasons
            .contains(&TableMetadataMaintenanceReason::DeletedByMaintenance)
        {
            object_report
                .reasons
                .push(TableMetadataMaintenanceReason::DeletedByMaintenance);
        }
    }
}

pub(crate) fn mark_deleted_object_cleanup_reports(
    object_reports: &mut [TableMetadataMaintenanceObjectCleanupReport],
    deleted_locations: &BTreeSet<String>,
) {
    for object_report in object_reports {
        if !deleted_locations.contains(&object_report.object_location) {
            continue;
        }
        object_report.state = TableMetadataMaintenanceObjectState::Deleted;
        if !object_report
            .reasons
            .contains(&TableMetadataMaintenanceReason::DeletedByMaintenance)
        {
            object_report
                .reasons
                .push(TableMetadataMaintenanceReason::DeletedByMaintenance);
        }
    }
}
