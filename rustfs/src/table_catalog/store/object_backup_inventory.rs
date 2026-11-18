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

use super::*;

const OBJECT_CATALOG_BACKUP_REFERENCE_MAX_OBJECTS: usize = 1_000_000;
const OBJECT_CATALOG_BACKUP_REFERENCE_HASH_MAX_SIZE: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ObjectCatalogBackupReference {
    pub(super) bucket: String,
    pub(super) object: String,
    pub(super) kind: ObjectCatalogBackupReferenceKind,
    pub(super) size_bytes: u64,
    pub(super) etag: Option<String>,
    pub(super) sha256: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) enum ObjectCatalogBackupReferenceKind {
    TableMetadata,
    ViewMetadata,
    ManifestList,
    ManifestFile,
    DataFile,
    DeleteFile,
    TableStatistics,
    PartitionStatistics,
}

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

pub(super) fn validate_object_catalog_backup_reference_shape(
    table_bucket: &str,
    references: &[ObjectCatalogBackupReference],
) -> TableCatalogStoreResult<()> {
    if references.len() > OBJECT_CATALOG_BACKUP_REFERENCE_MAX_OBJECTS {
        return Err(TableCatalogStoreError::Invalid(
            "catalog backup reference count exceeds its limit".to_string(),
        ));
    }
    if references
        .windows(2)
        .any(|pair| (&pair[0].bucket, &pair[0].object) >= (&pair[1].bucket, &pair[1].object))
    {
        return Err(TableCatalogStoreError::Invalid(
            "catalog backup references must be sorted and unique".to_string(),
        ));
    }
    for reference in references {
        if reference.bucket != table_bucket || !safe_object_key(&reference.object) {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup reference is outside its table bucket".to_string(),
            ));
        }
        if reference.size_bytes <= OBJECT_CATALOG_BACKUP_REFERENCE_HASH_MAX_SIZE {
            if reference.sha256.as_deref().is_none_or(|digest| {
                digest.len() != 64
                    || !digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            }) {
                return Err(TableCatalogStoreError::Invalid(
                    "small catalog backup reference requires a SHA-256 watermark".to_string(),
                ));
            }
        } else if reference.sha256.is_some() || reference.etag.as_deref().is_none_or(str::is_empty) {
            return Err(TableCatalogStoreError::Invalid(
                "large catalog backup reference requires an ETag watermark and no inline digest".to_string(),
            ));
        }
    }
    Ok(())
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

impl<B> ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    pub(super) fn backup_reference_view_entries(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
    ) -> TableCatalogStoreResult<Vec<ViewEntry>> {
        if objects.is_empty() || objects.len() > OBJECT_CATALOG_BACKUP_REFERENCE_MAX_OBJECTS {
            return Err(TableCatalogStoreError::Invalid("catalog backup object count is invalid".to_string()));
        }
        let mut paths = BTreeSet::new();
        let mut views = Vec::new();
        for object in objects.iter().filter(|object| object.bucket == self.catalog_bucket()) {
            if object.object.ends_with(TABLE_ENTRY_FILE) {
                let entry: TableEntry = serde_json::from_slice(&object.data).map_err(|_| {
                    TableCatalogStoreError::Invalid("invalid table entry in catalog backup references".to_string())
                })?;
                validate_table_entry_object(&self.paths, &object.object, &entry)?;
                if entry.table_bucket != table_bucket || !paths.insert(object.object.as_str()) {
                    return Err(TableCatalogStoreError::Invalid(
                        "catalog backup table identity is invalid or duplicated".to_string(),
                    ));
                }
                if matches!(entry.state, TableCatalogEntryState::Renaming | TableCatalogEntryState::Deleting) {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog backup requires a stable table operation".to_string(),
                    ));
                }
            } else if object.object.ends_with(VIEW_ENTRY_FILE) {
                let entry: ViewEntry = serde_json::from_slice(&object.data).map_err(|_| {
                    TableCatalogStoreError::Invalid("invalid view entry in catalog backup references".to_string())
                })?;
                validate_view_entry_object(&self.paths, &object.object, &entry)?;
                if entry.table_bucket != table_bucket || !paths.insert(object.object.as_str()) {
                    return Err(TableCatalogStoreError::Invalid(
                        "catalog backup view identity is invalid or duplicated".to_string(),
                    ));
                }
                if matches!(entry.state, TableCatalogEntryState::Renaming | TableCatalogEntryState::Deleting) {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog backup requires a stable view operation".to_string(),
                    ));
                }
                if entry.state == TableCatalogEntryState::Active {
                    views.push(entry);
                }
            }
        }
        Ok(views)
    }

    async fn collect_object_catalog_backup_reference_keys(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
    ) -> TableCatalogStoreResult<BTreeMap<(String, String), ObjectCatalogBackupReferenceKind>> {
        let views = self.backup_reference_view_entries(table_bucket, objects)?;
        let tables = self.validate_catalog_backup_metadata_identities(table_bucket, objects)?;
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

    async fn object_catalog_backup_reference_fingerprint(
        &self,
        bucket: &str,
        object: &str,
        kind: ObjectCatalogBackupReferenceKind,
    ) -> TableCatalogStoreResult<ObjectCatalogBackupReference> {
        let metadata = self
            .backend
            .object_metadata(bucket, object)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound(format!("catalog backup reference {bucket}/{object}")))?;
        let etag = metadata.etag.as_deref().filter(|etag| !etag.is_empty()).ok_or_else(|| {
            TableCatalogStoreError::Invalid(format!("catalog backup reference has no stable ETag: {bucket}/{object}"))
        })?;
        let sha256 = if metadata.size <= OBJECT_CATALOG_BACKUP_REFERENCE_HASH_MAX_SIZE {
            let max_size = usize::try_from(OBJECT_CATALOG_BACKUP_REFERENCE_HASH_MAX_SIZE).map_err(|_| {
                TableCatalogStoreError::Internal("catalog backup hash limit exceeds addressable size".to_string())
            })?;
            let stored = self
                .backend
                .read_object_limited(bucket, object, max_size)
                .await?
                .ok_or_else(|| {
                    TableCatalogStoreError::Conflict(format!("catalog backup reference disappeared: {bucket}/{object}"))
                })?;
            if u64::try_from(stored.data.len()).ok() != Some(metadata.size) || stored.etag.as_deref() != Some(etag) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "catalog backup reference changed during its read: {bucket}/{object}"
                )));
            }
            Some(backup_sha256(&stored.data))
        } else {
            None
        };
        let final_metadata = self.backend.object_metadata(bucket, object).await?.ok_or_else(|| {
            TableCatalogStoreError::Conflict(format!("catalog backup reference disappeared: {bucket}/{object}"))
        })?;
        if final_metadata.size != metadata.size || final_metadata.etag != metadata.etag {
            return Err(TableCatalogStoreError::Conflict(format!(
                "catalog backup reference changed during verification: {bucket}/{object}"
            )));
        }
        Ok(ObjectCatalogBackupReference {
            bucket: bucket.to_string(),
            object: object.to_string(),
            kind,
            size_bytes: metadata.size,
            etag: metadata.etag,
            sha256,
        })
    }

    /// Captures the finite object inventory required by one stable catalog payload.
    ///
    /// The caller retains its publication and restore fences throughout capture. The inventory includes current
    /// metadata, its direct metadata-log history, each corresponding snapshot graph and statistics, and current view
    /// metadata. It contains watermarks, not payload copies. Cancellation makes no storage changes.
    pub(super) async fn capture_object_catalog_backup_references(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
    ) -> TableCatalogStoreResult<Vec<ObjectCatalogBackupReference>> {
        let keys = self
            .collect_object_catalog_backup_reference_keys(table_bucket, objects)
            .await?;
        let mut references = Vec::with_capacity(keys.len());
        for ((bucket, object), kind) in keys {
            references.push(
                self.object_catalog_backup_reference_fingerprint(&bucket, &object, kind)
                    .await?,
            );
        }
        // Current metadata embedded in the catalog payload must be exactly the object whose graph was inventoried.
        for object in objects.iter().filter(|object| object.bucket == table_bucket) {
            let index = references
                .binary_search_by(|reference| {
                    reference
                        .bucket
                        .cmp(&object.bucket)
                        .then_with(|| reference.object.cmp(&object.object))
                })
                .map_err(|_| TableCatalogStoreError::Invalid("catalog backup embeds an unreferenced table object".to_string()))?;
            let reference = &references[index];
            if u64::try_from(object.data.len()).ok() != Some(reference.size_bytes)
                || reference.sha256.as_deref() != Some(backup_sha256(&object.data).as_str())
            {
                return Err(TableCatalogStoreError::Conflict(
                    "catalog backup embedded metadata differs from the prepared object".to_string(),
                ));
            }
        }
        Ok(references)
    }

    /// Verifies the closed inventory without publishing any catalog state.
    ///
    /// The caller retains its restore/publication fences. Small objects compare size and SHA-256 across targets; their
    /// individual reads still require stable target ETags. An absent current metadata object may be supplied by its
    /// validated inline restore payload; existing different bytes are never accepted. All external objects must
    /// already be prepared. Large objects retain the recorded size and ETag. Invalid references fail closed.
    pub(super) async fn validate_object_catalog_backup_references(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
        references: &[ObjectCatalogBackupReference],
    ) -> TableCatalogStoreResult<()> {
        validate_object_catalog_backup_reference_shape(table_bucket, references)?;
        let keys = self
            .collect_object_catalog_backup_reference_keys(table_bucket, objects)
            .await?;
        if keys.len() != references.len() {
            return Err(TableCatalogStoreError::Conflict(
                "catalog backup reference graph does not match the prepared target".to_string(),
            ));
        }
        let embedded = objects
            .iter()
            .filter(|object| object.bucket == table_bucket)
            .map(|object| ((object.bucket.as_str(), object.object.as_str()), object))
            .collect::<BTreeMap<_, _>>();
        for (expected, ((bucket, object), kind)) in references.iter().zip(&keys) {
            if expected.bucket != *bucket || expected.object != *object || expected.kind != *kind {
                return Err(TableCatalogStoreError::Conflict(
                    "catalog backup reference identity does not match its graph".to_string(),
                ));
            }
            if let Some(payload) = embedded.get(&(bucket.as_str(), object.as_str()))
                && self.backend.object_metadata(bucket, object).await?.is_none()
            {
                if u64::try_from(payload.data.len()).ok() != Some(expected.size_bytes)
                    || expected.sha256.as_deref() != Some(backup_sha256(&payload.data).as_str())
                {
                    return Err(TableCatalogStoreError::Conflict(
                        "inline restore metadata does not match its saved inventory".to_string(),
                    ));
                }
                continue;
            }
            let actual = self
                .object_catalog_backup_reference_fingerprint(bucket, object, *kind)
                .await?;
            if expected.size_bytes != actual.size_bytes
                || if expected.size_bytes <= OBJECT_CATALOG_BACKUP_REFERENCE_HASH_MAX_SIZE {
                    expected.sha256 != actual.sha256
                } else {
                    expected.etag != actual.etag
                }
            {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "catalog backup reference watermark does not match the prepared target: {}/{}",
                    expected.bucket, expected.object
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "object_backup_inventory_tests.rs"]
mod tests;
