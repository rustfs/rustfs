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

pub(in crate::table_catalog) const OBJECT_CATALOG_BACKUP_REFERENCE_MAX_OBJECTS: usize = 1_000_000;
pub(in crate::table_catalog) const OBJECT_CATALOG_BACKUP_REFERENCE_HASH_MAX_SIZE: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::table_catalog) struct ObjectCatalogBackupReference {
    pub(in crate::table_catalog) bucket: String,
    pub(in crate::table_catalog) object: String,
    pub(in crate::table_catalog) kind: ObjectCatalogBackupReferenceKind,
    pub(in crate::table_catalog) size_bytes: u64,
    pub(in crate::table_catalog) etag: Option<String>,
    pub(in crate::table_catalog) sha256: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(in crate::table_catalog) enum ObjectCatalogBackupReferenceKind {
    TableMetadata,
    ViewMetadata,
    ManifestList,
    ManifestFile,
    DataFile,
    DeleteFile,
    TableStatistics,
    PartitionStatistics,
}

pub(in crate::table_catalog) fn validate_object_catalog_backup_reference_shape(
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

impl<B> ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    pub(in crate::table_catalog) fn backup_reference_view_entries(
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

    pub(in crate::table_catalog) async fn object_catalog_backup_reference_fingerprint(
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
}

#[cfg(test)]
#[path = "object_backup_inventory_tests.rs"]
mod tests;
