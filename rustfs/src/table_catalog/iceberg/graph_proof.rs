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

#[derive(Clone)]
pub(crate) struct IcebergGraphObserverId(Arc<()>);

impl IcebergGraphObserverId {
    pub(crate) fn new() -> Self {
        Self(Arc::new(()))
    }
    pub(crate) fn same_observer(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergGraphObjectIdentity {
    Missing,
    Metadata {
        body_revision: Option<CatalogBodyRevision>,
        etag: Option<String>,
        mod_time: Option<OffsetDateTime>,
        size: u64,
    },
    ContentSha256(CatalogContentSha256),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergGraphObjectObservation {
    bucket: String,
    object: String,
    body_revision: Option<CatalogBodyRevision>,
    content_size: usize,
    identity: IcebergGraphObjectIdentity,
    max_size: Option<usize>,
}

impl IcebergGraphObjectObservation {
    pub(crate) fn try_new(
        bucket: String,
        object: String,
        body_revision: Option<CatalogBodyRevision>,
        content_size: usize,
        identity: IcebergGraphObjectIdentity,
        max_size: Option<usize>,
    ) -> TableCatalogStoreResult<Self> {
        if bucket.is_empty() || object.is_empty() || max_size.is_some_and(|limit| content_size > limit) {
            return Err(TableCatalogStoreError::Invalid("invalid graph object observation or read budget".into()));
        }
        if matches!(identity, IcebergGraphObjectIdentity::Missing) && (body_revision.is_some() || content_size != 0) {
            return Err(TableCatalogStoreError::Invalid("missing graph object carries content identity".into()));
        }
        if let IcebergGraphObjectIdentity::Metadata {
            body_revision: identity_revision,
            ..
        } = &identity
            && body_revision.is_some()
            && *identity_revision != body_revision
        {
            return Err(TableCatalogStoreError::Invalid("graph metadata revisions disagree".into()));
        }
        Ok(Self {
            bucket,
            object,
            body_revision,
            content_size,
            identity,
            max_size,
        })
    }
    pub(crate) fn location(&self) -> (&str, &str) {
        (&self.bucket, &self.object)
    }
    pub(crate) fn identity(&self) -> &IcebergGraphObjectIdentity {
        &self.identity
    }
    pub(crate) fn body_revision(&self) -> Option<CatalogBodyRevision> {
        self.body_revision
    }
    pub(crate) fn content_size(&self) -> usize {
        self.content_size
    }
    pub(crate) fn max_size(&self) -> Option<usize> {
        self.max_size
    }

    pub(crate) fn merge(&self, incoming: &Self) -> TableCatalogStoreResult<Self> {
        if self.location() != incoming.location()
            || self.identity != incoming.identity
            || self.content_size != incoming.content_size
            || self.body_revision.zip(incoming.body_revision).is_some_and(|(a, b)| a != b)
        {
            return Err(TableCatalogStoreError::Conflict("graph object changed between observations".into()));
        }
        let max_size = match (self.max_size, incoming.max_size) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        Self::try_new(
            self.bucket.clone(),
            self.object.clone(),
            self.body_revision.or(incoming.body_revision),
            self.content_size,
            self.identity.clone(),
            max_size,
        )
    }

    pub(crate) fn validate_supports(&self, captured: &Self) -> TableCatalogStoreResult<()> {
        if self.location() != captured.location()
            || self.identity != captured.identity
            || self.content_size != captured.content_size
            || captured
                .body_revision
                .is_some_and(|revision| self.body_revision != Some(revision))
            || captured
                .max_size
                .is_some_and(|limit| self.max_size.is_none_or(|current| current > limit))
        {
            return Err(TableCatalogStoreError::Conflict(
                "prepared graph observation does not retain captured evidence".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) struct IcebergGraphReadSet {
    observer: IcebergGraphObserverId,
    objects: Vec<Arc<IcebergGraphObjectObservation>>,
}

impl IcebergGraphReadSet {
    pub(crate) fn try_new(
        observer: IcebergGraphObserverId,
        mut objects: Vec<Arc<IcebergGraphObjectObservation>>,
    ) -> TableCatalogStoreResult<Self> {
        objects.sort_by(|a, b| a.location().cmp(&b.location()));
        if objects.windows(2).any(|pair| pair[0].location() == pair[1].location()) {
            return Err(TableCatalogStoreError::Invalid("duplicate graph object observation".into()));
        }
        Ok(Self { observer, objects })
    }
    pub(crate) fn observer(&self) -> &IcebergGraphObserverId {
        &self.observer
    }
    pub(crate) fn objects(&self) -> &[Arc<IcebergGraphObjectObservation>] {
        &self.objects
    }
}

pub(crate) trait IcebergGraphPublication: TableCommitPublication {
    fn capture_graph_read_set(&self) -> TableCatalogStoreResult<IcebergGraphReadSet>;
    fn validate_prepared_graph_read_set(&self, read_set: &IcebergGraphReadSet) -> TableCatalogStoreResult<()>;
}

enum IcebergGraphValidationMode {
    FullGraph,
    ChangesFrom { base_metadata_sha256: CatalogContentSha256 },
}

pub(crate) struct ValidatedIcebergSnapshotGraph {
    table_bucket: String,
    table_id: String,
    table_uuid: String,
    warehouse_location: String,
    mode: IcebergGraphValidationMode,
    candidate_metadata_sha256: CatalogContentSha256,
    retry_equivalent_metadata_sha256: CatalogContentSha256,
    reads: IcebergGraphReadSet,
}

impl ValidatedIcebergSnapshotGraph {
    pub(super) fn from_validated_parts(
        entry: &TableEntry,
        current_metadata: Option<&serde_json::Value>,
        metadata: &serde_json::Value,
        reads: IcebergGraphReadSet,
    ) -> TableCatalogStoreResult<Self> {
        validate_supported_table_metadata(metadata)?;
        if table_metadata_uuid(metadata)? != entry.table_uuid {
            return Err(TableCatalogStoreError::Conflict(
                "graph metadata table uuid does not match catalog identity".into(),
            ));
        }
        Ok(Self {
            table_bucket: entry.table_bucket.clone(),
            table_id: entry.table_id.clone(),
            table_uuid: entry.table_uuid.clone(),
            warehouse_location: entry.warehouse_location.clone(),
            mode: match current_metadata {
                Some(base) => IcebergGraphValidationMode::ChangesFrom {
                    base_metadata_sha256: metadata_sha256(base)?,
                },
                None => IcebergGraphValidationMode::FullGraph,
            },
            candidate_metadata_sha256: metadata_sha256(metadata)?,
            retry_equivalent_metadata_sha256: retry_equivalent_metadata_sha256(metadata)?,
            reads,
        })
    }

    pub(super) fn validate_for_commit(
        &self,
        entry: &TableEntry,
        base_metadata: Option<&serde_json::Value>,
        candidate_metadata: &serde_json::Value,
        publication: &(dyn IcebergGraphPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        if self.table_bucket != entry.table_bucket
            || self.table_id != entry.table_id
            || self.table_uuid != entry.table_uuid
            || self.warehouse_location != entry.warehouse_location
            || self.candidate_metadata_sha256 != metadata_sha256(candidate_metadata)?
        {
            return Err(TableCatalogStoreError::Conflict("validated graph resource or metadata changed".into()));
        }
        if let IcebergGraphValidationMode::ChangesFrom { base_metadata_sha256 } = self.mode
            && base_metadata.map(metadata_sha256).transpose()? != Some(base_metadata_sha256)
        {
            return Err(TableCatalogStoreError::Conflict("validated graph base metadata changed".into()));
        }
        publication.validate_prepared_graph_read_set(&self.reads)
    }

    #[cfg(test)]
    pub(in crate::table_catalog) fn validate_test_commit(
        &self,
        entry: &TableEntry,
        base_metadata: Option<&serde_json::Value>,
        candidate_metadata: &serde_json::Value,
        publication: &(dyn IcebergGraphPublication + Sync),
    ) -> TableCatalogStoreResult<()> {
        self.validate_for_commit(entry, base_metadata, candidate_metadata, publication)
    }

    pub(crate) fn rebind_after_matching_commit(mut self, final_metadata: &serde_json::Value) -> TableCatalogStoreResult<Self> {
        validate_supported_table_metadata(final_metadata)?;
        if table_metadata_uuid(final_metadata)? != self.table_uuid
            || retry_equivalent_metadata_sha256(final_metadata)? != self.retry_equivalent_metadata_sha256
        {
            return Err(TableCatalogStoreError::Conflict("matching commit changed validated graph inputs".into()));
        }
        self.candidate_metadata_sha256 = metadata_sha256(final_metadata)?;
        Ok(self)
    }
}

fn metadata_sha256(metadata: &serde_json::Value) -> TableCatalogStoreResult<CatalogContentSha256> {
    serde_json::to_vec(metadata)
        .map(|bytes| CatalogContentSha256::digest(&bytes))
        .map_err(|err| TableCatalogStoreError::Invalid(format!("failed to encode graph metadata: {err}")))
}

fn retry_equivalent_metadata_sha256(metadata: &serde_json::Value) -> TableCatalogStoreResult<CatalogContentSha256> {
    let mut normalized = metadata.clone();
    if let Some(object) = normalized.as_object_mut() {
        object.remove("last-updated-ms");
    }
    if let Some(log) = normalized.get_mut("snapshot-log").and_then(serde_json::Value::as_array_mut) {
        for item in log {
            if let Some(object) = item.as_object_mut() {
                object.remove("timestamp-ms");
            }
        }
    }
    metadata_sha256(&normalized)
}

#[cfg(test)]
mod tests;
