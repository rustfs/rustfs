// Copyright 2026 RustFS Team
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

use crate::table_catalog::store::object::validate_table_entry_object;
use crate::table_catalog::store::{
    OBJECT_CATALOG_BACKUP_MAX_SIZE, OBJECT_CATALOG_BACKUP_REFERENCE_MAX_OBJECTS, ObjectCatalogBackup, ObjectCatalogBackupObject,
    ObjectCatalogBackupReference, ObjectCatalogBackupReferenceKind, append_backup_object, backup_sha256, safe_object_key,
};
use crate::table_catalog::*;

const BACKUP_PREPARATION_MAX_HISTORY_SCOPES: usize = 1024;

impl ObjectCatalogBackupReferenceKind {
    fn from_maintenance_kind(kind: &TableMetadataMaintenanceObjectKind) -> Self {
        match kind {
            TableMetadataMaintenanceObjectKind::MetadataFile => Self::TableMetadata,
            TableMetadataMaintenanceObjectKind::ManifestList => Self::ManifestList,
            TableMetadataMaintenanceObjectKind::ManifestFile => Self::ManifestFile,
            TableMetadataMaintenanceObjectKind::DataFile => Self::DataFile,
            TableMetadataMaintenanceObjectKind::DeleteFile => Self::DeleteFile,
        }
    }
    fn from_statistics_kind(kind: IcebergStatisticsFileKind) -> Self {
        match kind {
            IcebergStatisticsFileKind::Table => Self::TableStatistics,
            IcebergStatisticsFileKind::Partition => Self::PartitionStatistics,
        }
    }
}

fn insert_backup_reference_kind(
    keys: &mut BTreeMap<(String, String), ObjectCatalogBackupReferenceKind>,
    bucket: &str,
    object: &str,
    kind: ObjectCatalogBackupReferenceKind,
) -> TableCatalogStoreResult<()> {
    if !safe_object_key(object) {
        return Err(TableCatalogStoreError::Invalid(
            "catalog backup reference has an invalid object key".to_string(),
        ));
    }
    let key = (bucket.to_string(), object.to_string());
    if let Some(existing) = keys.get(&key) {
        if *existing != kind {
            return Err(TableCatalogStoreError::Invalid(format!(
                "catalog backup reference has conflicting kinds: {bucket}/{object}"
            )));
        }
        return Ok(());
    }
    if keys.len() == OBJECT_CATALOG_BACKUP_REFERENCE_MAX_OBJECTS {
        return Err(TableCatalogStoreError::Invalid(
            "catalog backup reference count exceeds its limit".to_string(),
        ));
    }
    keys.insert(key, kind);
    Ok(())
}

pub(super) fn backup_reference_metadata_log_locations(
    metadata: &serde_json::Value,
    entry: &TableEntry,
) -> TableCatalogStoreResult<BTreeSet<String>> {
    let Some(metadata_log) = metadata.get("metadata-log") else {
        return Ok(BTreeSet::new());
    };
    let entries = metadata_log
        .as_array()
        .ok_or_else(|| TableCatalogStoreError::Invalid("table metadata-log must be an array for a catalog backup".to_string()))?;
    if entries.len() > OBJECT_CATALOG_BACKUP_REFERENCE_MAX_OBJECTS {
        return Err(TableCatalogStoreError::Invalid(
            "catalog backup metadata history exceeds its limit".to_string(),
        ));
    }
    let mut locations = BTreeSet::new();
    for log_entry in entries {
        let location = log_entry
            .get("metadata-file")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                TableCatalogStoreError::Invalid("catalog backup metadata-log entry requires a metadata-file".to_string())
            })?;
        let key = table_catalog_object_key_from_location(&entry.table_bucket, location).ok_or_else(|| {
            TableCatalogStoreError::Invalid("catalog backup metadata history is outside its table bucket".to_string())
        })?;
        if !is_valid_table_metadata_location_for_entry(entry, &key) {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup metadata history is outside its metadata directory".to_string(),
            ));
        }
        locations.insert(key);
    }
    Ok(locations)
}

pub(super) fn validate_backup_reference_table_metadata(
    entry: &TableEntry,
    object: &str,
    metadata: &serde_json::Value,
    current: bool,
) -> TableCatalogStoreResult<()> {
    validate_supported_table_metadata(metadata)?;
    if table_metadata_uuid(metadata)? != entry.table_uuid {
        return Err(TableCatalogStoreError::Invalid(
            "catalog backup metadata UUID does not match its table".to_string(),
        ));
    }
    let warehouse_location = table_metadata_location(metadata)?;
    validate_table_warehouse_location(&entry.table_bucket, warehouse_location)?;
    if current && warehouse_location != entry.warehouse_location {
        return Err(TableCatalogStoreError::Invalid(
            "catalog backup current metadata does not match its warehouse".to_string(),
        ));
    }
    if current && table_metadata_format_version(metadata)? != entry.format_version {
        return Err(TableCatalogStoreError::Invalid(
            "catalog backup current metadata does not match its table format version".to_string(),
        ));
    }
    if !is_valid_table_metadata_location_for_entry(entry, object) {
        return Err(TableCatalogStoreError::Invalid(
            "catalog backup metadata is outside its metadata directory".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn validate_backup_reference_view_metadata(
    entry: &ViewEntry,
    namespace: &Namespace,
    view: &IdentifierSegment,
    object: &str,
    metadata: &serde_json::Value,
) -> TableCatalogStoreResult<()> {
    validate_supported_view_metadata(metadata)?;
    if metadata.get("view-uuid").and_then(serde_json::Value::as_str) != Some(entry.view_uuid.as_str())
        || metadata.get("location").and_then(serde_json::Value::as_str) != Some(entry.warehouse_location.as_str())
        || metadata.get("format-version").and_then(serde_json::Value::as_u64) != Some(u64::from(entry.format_version))
    {
        return Err(TableCatalogStoreError::Invalid(
            "catalog backup metadata does not match its view identity".to_string(),
        ));
    }
    validate_view_warehouse_location(&entry.table_bucket, &entry.warehouse_location)?;
    if !is_valid_view_metadata_location(namespace, view, object) {
        return Err(TableCatalogStoreError::Invalid(
            "catalog backup view metadata is outside its metadata directory".to_string(),
        ));
    }
    Ok(())
}

fn preparation_graph_scope_matches(
    entry: &TableEntry,
    metadata: &serde_json::Value,
    reference: &ObjectCatalogBackupReference,
) -> TableCatalogStoreResult<bool> {
    let prefix = table_warehouse_object_prefix(entry)?;
    let expected_kind = match reference.kind {
        ObjectCatalogBackupReferenceKind::ManifestList => TableMetadataMaintenanceObjectKind::ManifestList,
        ObjectCatalogBackupReferenceKind::ManifestFile => TableMetadataMaintenanceObjectKind::ManifestFile,
        ObjectCatalogBackupReferenceKind::DataFile => TableMetadataMaintenanceObjectKind::DataFile,
        ObjectCatalogBackupReferenceKind::DeleteFile => TableMetadataMaintenanceObjectKind::DeleteFile,
        ObjectCatalogBackupReferenceKind::TableStatistics | ObjectCatalogBackupReferenceKind::PartitionStatistics => {
            for statistics in table_statistics_object_references(metadata)? {
                let kind_matches = matches!(
                    (reference.kind, statistics.kind),
                    (ObjectCatalogBackupReferenceKind::TableStatistics, IcebergStatisticsFileKind::Table)
                        | (
                            ObjectCatalogBackupReferenceKind::PartitionStatistics,
                            IcebergStatisticsFileKind::Partition
                        )
                );
                if kind_matches
                    && reference.object.starts_with(&prefix)
                    && table_catalog_object_key_from_location(&entry.table_bucket, &statistics.location).as_deref()
                        == Some(reference.object.as_str())
                {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        ObjectCatalogBackupReferenceKind::TableMetadata | ObjectCatalogBackupReferenceKind::ViewMetadata => return Ok(false),
    };
    Ok(table_maintenance_object_kind_for_entry(entry, Some(&prefix), &reference.object)
        .is_some_and(|actual| table_maintenance_object_kind_matches_reference(&actual, &expected_kind)))
}

fn validate_preparation_warehouse_owner(
    entry: &TableEntry,
    tables: &[(TableEntry, serde_json::Value)],
) -> TableCatalogStoreResult<()> {
    let prefix = table_warehouse_object_prefix(entry)?;
    for (other, _) in tables {
        if other.table_id != entry.table_id && warehouse_object_prefixes_overlap(&prefix, &table_warehouse_object_prefix(other)?)
        {
            return Err(TableCatalogStoreError::Conflict(
                "backup object preparation scope overlaps another active table".to_string(),
            ));
        }
    }
    Ok(())
}

impl<B> ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    pub(in crate::table_catalog) fn validate_catalog_backup_metadata_identities(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
    ) -> TableCatalogStoreResult<Vec<TableEntry>> {
        self.validated_backup_table_metadata(table_bucket, objects)
            .map(|tables| tables.into_iter().map(|(entry, _)| entry).collect())
    }

    fn validated_backup_table_metadata(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
    ) -> TableCatalogStoreResult<Vec<(TableEntry, serde_json::Value)>> {
        let mut tables = Vec::new();
        for object in objects
            .iter()
            .filter(|object| object.bucket == RUSTFS_META_BUCKET && object.object.ends_with(TABLE_ENTRY_FILE))
        {
            let entry: TableEntry = serde_json::from_slice(&object.data)
                .map_err(|error| TableCatalogStoreError::Invalid(format!("table catalog entry is invalid: {error}")))?;
            if entry.state != TableCatalogEntryState::Active {
                continue;
            }
            if entry.table_bucket != table_bucket {
                return Err(TableCatalogStoreError::Invalid(
                    "catalog backup contains an entry for another table bucket".to_string(),
                ));
            }
            let metadata_object = table_catalog_object_key_from_location(&entry.table_bucket, &entry.metadata_location)
                .ok_or_else(|| TableCatalogStoreError::Invalid("table metadata location is not an object path".to_string()))?;
            let metadata = objects
                .iter()
                .find(|candidate| candidate.bucket == entry.table_bucket && candidate.object == metadata_object)
                .ok_or_else(|| {
                    TableCatalogStoreError::NotFound(format!(
                        "catalog backup is missing current metadata object: {}/{}",
                        entry.table_bucket, metadata_object
                    ))
                })?;
            let metadata = decode_table_metadata_json(&entry.metadata_location, &metadata.data)?;
            validate_supported_table_metadata(&metadata)?;
            if metadata.get("table-uuid").and_then(serde_json::Value::as_str) != Some(entry.table_uuid.as_str())
                || metadata.get("location").and_then(serde_json::Value::as_str) != Some(entry.warehouse_location.as_str())
                || metadata
                    .get("format-version")
                    .and_then(serde_json::Value::as_u64)
                    .is_none_or(|version| version < u64::from(entry.format_version))
            {
                return Err(TableCatalogStoreError::Invalid(
                    "catalog backup metadata does not match its table identity".to_string(),
                ));
            }
            tables.push((entry, metadata));
        }
        Ok(tables)
    }

    pub(in crate::table_catalog) async fn validate_catalog_backup_snapshot_graph(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
    ) -> TableCatalogStoreResult<()> {
        for (entry, metadata) in self.validated_backup_table_metadata(table_bucket, objects)? {
            let context = TableSnapshotGraphValidationContext::new(&self.backend, table_bucket, &entry);
            validate_table_snapshot_changes(&context, None, &metadata).await?;
        }
        Ok(())
    }

    pub(in crate::table_catalog) async fn collect_object_catalog_backup_reference_keys(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
    ) -> TableCatalogStoreResult<BTreeMap<(String, String), ObjectCatalogBackupReferenceKind>> {
        let views = self.backup_reference_view_entries(table_bucket, objects)?;
        let tables = self.validated_backup_table_metadata(table_bucket, objects)?;
        let mut keys = BTreeMap::new();
        for (entry, current_metadata) in &tables {
            let current_object =
                table_catalog_object_key_from_location(table_bucket, &entry.metadata_location).ok_or_else(|| {
                    TableCatalogStoreError::Invalid("catalog backup current metadata location is invalid".to_string())
                })?;
            validate_backup_reference_table_metadata(entry, &current_object, current_metadata, true)?;
            let mut metadata_locations = backup_reference_metadata_log_locations(current_metadata, entry)?;
            metadata_locations.insert(current_object.clone());
            for object in metadata_locations {
                insert_backup_reference_kind(&mut keys, table_bucket, &object, ObjectCatalogBackupReferenceKind::TableMetadata)?;
                let historical;
                let metadata = if object == current_object {
                    current_metadata
                } else {
                    historical = read_table_metadata_value(&self.backend, table_bucket, &object)
                        .await?
                        .ok_or_else(|| TableCatalogStoreError::NotFound(format!("catalog backup metadata history {object}")))?;
                    &historical
                };
                validate_backup_reference_table_metadata(entry, &object, metadata, object == current_object)?;
                let mut metadata_entry = entry.clone();
                metadata_entry.warehouse_location = table_metadata_location(metadata)?.to_string();
                let warehouse_prefix = table_warehouse_object_prefix(&metadata_entry)?;
                for (other, _) in &tables {
                    if other.table_id != entry.table_id
                        && warehouse_object_prefixes_overlap(&warehouse_prefix, &table_warehouse_object_prefix(other)?)
                    {
                        return Err(TableCatalogStoreError::Conflict(
                            "catalog backup metadata warehouse overlaps another active table".to_string(),
                        ));
                    }
                }
                let context = TableSnapshotGraphValidationContext::new(&self.backend, table_bucket, &metadata_entry);
                let graph = validated_table_snapshot_graph_inventory(&context, metadata).await?;
                for reference in table_statistics_object_references(metadata)? {
                    let statistics_object = table_catalog_object_key_from_location(table_bucket, &reference.location)
                        .ok_or_else(|| {
                            TableCatalogStoreError::Invalid(
                                "catalog backup statistics are outside their table bucket".to_string(),
                            )
                        })?;
                    if !statistics_object.starts_with(&warehouse_prefix) {
                        return Err(TableCatalogStoreError::Invalid(
                            "catalog backup statistics are outside their table warehouse".to_string(),
                        ));
                    }
                    insert_backup_reference_kind(
                        &mut keys,
                        table_bucket,
                        &statistics_object,
                        ObjectCatalogBackupReferenceKind::from_statistics_kind(reference.kind),
                    )?;
                }
                for (graph_object, kind) in graph {
                    insert_backup_reference_kind(
                        &mut keys,
                        table_bucket,
                        &graph_object,
                        ObjectCatalogBackupReferenceKind::from_maintenance_kind(&kind),
                    )?;
                }
            }
        }
        for entry in views {
            let namespace = parse_namespace_for_store(&entry.namespace)?;
            let view = parse_table_for_store(&entry.view)?;
            let object = table_catalog_object_key_from_location(table_bucket, &entry.metadata_location)
                .ok_or_else(|| TableCatalogStoreError::Invalid("catalog backup view metadata location is invalid".to_string()))?;
            let metadata = read_table_metadata_value(&self.backend, table_bucket, &object)
                .await?
                .ok_or_else(|| TableCatalogStoreError::NotFound(format!("catalog backup view metadata {object}")))?;
            validate_backup_reference_view_metadata(&entry, &namespace, &view, &object, &metadata)?;
            insert_backup_reference_kind(&mut keys, table_bucket, &object, ObjectCatalogBackupReferenceKind::ViewMetadata)?;
        }
        Ok(keys)
    }

    pub(in crate::table_catalog) async fn validate_backup_object_preparation_scope(
        &self,
        table_bucket: &str,
        backup: &ObjectCatalogBackup,
        references: &[ObjectCatalogBackupReference],
        reference: &ObjectCatalogBackupReference,
        data: &[u8],
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<()> {
        let views = self.backup_reference_view_entries(table_bucket, &backup.objects)?;
        let tables = self.validated_backup_table_metadata(table_bucket, &backup.objects)?;
        for (entry, metadata) in &tables {
            let current = table_catalog_object_key_from_location(table_bucket, &entry.metadata_location).ok_or_else(|| {
                TableCatalogStoreError::Invalid("backup table current metadata location is invalid".to_string())
            })?;
            validate_backup_reference_table_metadata(entry, &current, metadata, true)?;
            validate_preparation_warehouse_owner(entry, &tables)?;
            if reference.kind == ObjectCatalogBackupReferenceKind::TableMetadata {
                let historical = backup_reference_metadata_log_locations(metadata, entry)?;
                if reference.object == current || historical.contains(&reference.object) {
                    if reference.object == current {
                        let embedded = backup
                            .objects
                            .iter()
                            .find(|object| object.bucket == table_bucket && object.object == current)
                            .ok_or_else(|| {
                                TableCatalogStoreError::Invalid("backup is missing its current metadata payload".to_string())
                            })?;
                        if embedded.data.as_slice() != data {
                            return Err(TableCatalogStoreError::Invalid(
                                "current metadata preparation differs from the embedded backup payload".to_string(),
                            ));
                        }
                    }
                    let prepared = decode_table_metadata_json(&reference.object, data)?;
                    validate_backup_reference_table_metadata(entry, &reference.object, &prepared, reference.object == current)?;
                    let mut prepared_entry = entry.clone();
                    prepared_entry.warehouse_location = table_metadata_location(&prepared)?.to_string();
                    validate_preparation_warehouse_owner(&prepared_entry, &tables)?;
                    return Ok(());
                }
            } else if preparation_graph_scope_matches(entry, metadata, reference)? {
                return Ok(());
            }
        }
        if reference.kind == ObjectCatalogBackupReferenceKind::ViewMetadata {
            for entry in views {
                if table_catalog_object_key_from_location(table_bucket, &entry.metadata_location).as_deref()
                    == Some(reference.object.as_str())
                {
                    let metadata = decode_table_metadata_json(&reference.object, data)?;
                    validate_backup_reference_view_metadata(
                        &entry,
                        &parse_namespace_for_store(&entry.namespace)?,
                        &parse_table_for_store(&entry.view)?,
                        &reference.object,
                        &metadata,
                    )?;
                    return Ok(());
                }
            }
        }
        if matches!(
            reference.kind,
            ObjectCatalogBackupReferenceKind::TableMetadata | ObjectCatalogBackupReferenceKind::ViewMetadata
        ) {
            return Err(TableCatalogStoreError::Invalid(
                "backup metadata preparation is outside an active entry's direct metadata scope".to_string(),
            ));
        }

        let mut inspected = 0usize;
        let mut read_bytes = 0u64;
        let mut missing_history = false;
        let max_read_bytes = u64::try_from(OBJECT_CATALOG_BACKUP_MAX_SIZE)
            .map_err(|_| TableCatalogStoreError::Internal("backup preparation history budget is invalid".to_string()))?;
        for (entry, metadata) in &tables {
            let current = table_catalog_object_key_from_location(table_bucket, &entry.metadata_location).ok_or_else(|| {
                TableCatalogStoreError::Invalid("backup table current metadata location is invalid".to_string())
            })?;
            for object in backup_reference_metadata_log_locations(metadata, entry)? {
                fence.ensure_held()?;
                if object == current {
                    continue;
                }
                inspected += 1;
                if inspected > BACKUP_PREPARATION_MAX_HISTORY_SCOPES {
                    return Err(TableCatalogStoreError::Unavailable(
                        "historical preparation scope lookup exceeds its 1024-object budget".to_string(),
                    ));
                }
                let saved = references
                    .binary_search_by(|candidate| {
                        candidate
                            .bucket
                            .as_str()
                            .cmp(table_bucket)
                            .then_with(|| candidate.object.cmp(&object))
                    })
                    .ok()
                    .and_then(|index| references.get(index))
                    .ok_or_else(|| {
                        TableCatalogStoreError::Invalid(
                            "direct history metadata is missing from the backup reference inventory".to_string(),
                        )
                    })?;
                if saved.kind != ObjectCatalogBackupReferenceKind::TableMetadata {
                    return Err(TableCatalogStoreError::Invalid(
                        "direct history has a conflicting backup reference kind".to_string(),
                    ));
                }
                read_bytes = read_bytes
                    .checked_add(saved.size_bytes)
                    .filter(|bytes| *bytes <= max_read_bytes)
                    .ok_or_else(|| {
                        TableCatalogStoreError::Unavailable(
                            "historical preparation scope lookup exceeds its byte budget".to_string(),
                        )
                    })?;
                let Some(stored) = self.read_stable_preparation_object(table_bucket, &object).await? else {
                    missing_history = true;
                    continue;
                };
                if u64::try_from(stored.data.len()).ok() != Some(saved.size_bytes)
                    || saved.sha256.as_deref() != Some(backup_sha256(&stored.data).as_str())
                {
                    return Err(TableCatalogStoreError::Conflict(
                        "prepared historical metadata does not match its backup watermark".to_string(),
                    ));
                }
                let metadata = decode_table_metadata_json(&object, &stored.data)?;
                validate_backup_reference_table_metadata(entry, &object, &metadata, false)?;
                let mut historical_entry = entry.clone();
                historical_entry.warehouse_location = table_metadata_location(&metadata)?.to_string();
                validate_preparation_warehouse_owner(&historical_entry, &tables)?;
                if preparation_graph_scope_matches(&historical_entry, &metadata, reference)? {
                    return Ok(());
                }
            }
        }
        if missing_history {
            return Err(TableCatalogStoreError::Unavailable(
                "prepare the referenced direct history metadata before preparing an object outside current warehouse scope"
                    .to_string(),
            ));
        }
        Err(TableCatalogStoreError::Invalid(
            "backup object preparation is outside its active entries and prepared direct history scopes".to_string(),
        ))
    }

    pub(in crate::table_catalog) async fn validate_backup_object_format(
        kind: ObjectCatalogBackupReferenceKind,
        data: &[u8],
    ) -> TableCatalogStoreResult<()> {
        match kind {
            ObjectCatalogBackupReferenceKind::ManifestList => {
                decode_manifest_list_avro_async(data.to_vec()).await?;
            }
            ObjectCatalogBackupReferenceKind::ManifestFile => {
                decode_manifest_avro_async(data.to_vec()).await?;
            }
            _ => {}
        }
        Ok(())
    }
    pub(in crate::table_catalog) async fn capture_object_catalog_backup_objects(
        &self,
        table_bucket: &str,
    ) -> TableCatalogStoreResult<Vec<ObjectCatalogBackupObject>> {
        let prefix = self.paths.table_bucket_root_prefix(table_bucket);
        let mut objects = Vec::new();
        let limit = NonZeroUsize::new(TABLE_CATALOG_LIST_MAX_KEYS)
            .ok_or_else(|| TableCatalogStoreError::Internal("catalog backup page limit must be positive".to_string()))?;
        let mut cursor: Option<String> = None;
        let mut encoded_bytes = 0usize;
        loop {
            let page = self
                .backend
                .list_objects_page(self.catalog_bucket(), &prefix, cursor.as_deref(), limit)
                .await?;
            if page.objects.len() > limit.get() || (page.is_truncated && page.objects.is_empty()) {
                return Err(TableCatalogStoreError::Invalid("invalid catalog backup listing page".to_string()));
            }
            for object in page.objects {
                if !object.starts_with(&prefix) || cursor.as_ref().is_some_and(|previous| object <= *previous) {
                    return Err(TableCatalogStoreError::Invalid(
                        "catalog backup listing escaped its prefix or did not advance".to_string(),
                    ));
                }
                if !self.is_catalog_directory_sidecar(table_bucket, &object) {
                    let payload = self.capture_backup_object(self.catalog_bucket(), &object).await?;
                    append_backup_object(&mut objects, &mut encoded_bytes, payload)?;
                }
                cursor = Some(object);
            }
            if !page.is_truncated {
                break;
            }
        }
        let mut entries = Vec::new();
        for object in objects.iter().filter(|object| object.object.ends_with(TABLE_ENTRY_FILE)) {
            let entry: TableEntry = serde_json::from_slice(&object.data)
                .map_err(|error| TableCatalogStoreError::Invalid(format!("table catalog entry is invalid: {error}")))?;
            validate_table_entry_object(&self.paths, &object.object, &entry)?;
            if entry.table_bucket != table_bucket {
                return Err(TableCatalogStoreError::Invalid(
                    "catalog backup table owner belongs to another bucket".to_string(),
                ));
            }
            entries.push(entry);
        }
        for entry in entries {
            if entry.state != TableCatalogEntryState::Active {
                continue;
            }
            let metadata_object = table_catalog_object_key_from_location(&entry.table_bucket, &entry.metadata_location)
                .ok_or_else(|| TableCatalogStoreError::Invalid("table metadata location is not an object path".to_string()))?;
            if !is_valid_table_metadata_location_for_entry(&entry, &metadata_object) {
                return Err(TableCatalogStoreError::Invalid(
                    "catalog backup metadata is outside its owner's metadata scope".to_string(),
                ));
            }
            if !objects
                .iter()
                .any(|candidate| candidate.bucket == entry.table_bucket && candidate.object == metadata_object)
            {
                let payload = self.capture_backup_object(&entry.table_bucket, &metadata_object).await?;
                append_backup_object(&mut objects, &mut encoded_bytes, payload)?;
            }
        }
        objects.sort_by(|left, right| left.bucket.cmp(&right.bucket).then_with(|| left.object.cmp(&right.object)));
        if objects.is_empty() {
            return Err(TableCatalogStoreError::NotFound(format!("table bucket catalog {table_bucket}")));
        }
        Ok(objects)
    }

    pub(in crate::table_catalog) async fn validate_catalog_backup_reference_graph(
        &self,
        backup: &ObjectCatalogBackup,
    ) -> TableCatalogStoreResult<()> {
        if let Some(references) = &backup.references {
            self.validate_object_catalog_backup_references(&backup.table_bucket, &backup.objects, references)
                .await
        } else {
            self.validate_catalog_backup_snapshot_graph(&backup.table_bucket, &backup.objects)
                .await
        }
    }
}
