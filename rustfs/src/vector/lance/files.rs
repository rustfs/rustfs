// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeSet, num::NonZeroU64};

use lance_file::version::ConcreteFileVersion;
use lance_table::format::{IndexMetadata, Manifest, pb};
use object_store_lance::path::Path;
use prost::Message;
use uuid::Uuid;

use crate::table_catalog::{CatalogObjectKey, CatalogResourceIdentity};

use super::{LanceManifestError, VectorIndexManifestSpec};

pub(super) const ANN_INDEX_NAME: &str = "rustfs_vector_ann";

pub(super) struct ManifestObjectReference {
    pub key: CatalogObjectKey,
    pub expected_size: Option<NonZeroU64>,
}

pub(super) fn validate_fragment_protobuf(fragments: &[pb::DataFragment]) -> Result<(), LanceManifestError> {
    let mut ids = BTreeSet::new();
    for fragment in fragments {
        if fragment.id > u64::from(u32::MAX)
            || !ids.insert(fragment.id)
            || fragment.physical_rows == 0
            || fragment.physical_rows > u64::from(u32::MAX)
            || fragment.files.is_empty()
        {
            return Err(LanceManifestError::CorruptManifest {
                reason: "fragment identity or physical row count is invalid",
            });
        }
        if !fragment.overlays.is_empty()
            || fragment.row_id_sequence.is_some()
            || fragment.last_updated_at_version_sequence.is_some()
            || fragment.created_at_version_sequence.is_some()
            || fragment.clustering_id.is_some()
            || !fragment.metadata.is_empty()
        {
            return Err(LanceManifestError::UnsupportedFormat {
                reason: "Vector fragments require ordinary rows without overlays or row lineage",
            });
        }
        if fragment
            .deletion_file
            .as_ref()
            .is_some_and(|deletion| deletion.num_deleted_rows > fragment.physical_rows)
        {
            return Err(LanceManifestError::CorruptManifest {
                reason: "deleted row count exceeds physical rows",
            });
        }
    }
    Ok(())
}

pub(super) fn manifest_objects(
    manifest: &Manifest,
    resource: &CatalogResourceIdentity,
) -> Result<Vec<ManifestObjectReference>, LanceManifestError> {
    let base = Path::from(resource.dataset_prefix().trim_end_matches('/'));
    let schema_fields = manifest.schema.field_ids().into_iter().collect::<BTreeSet<_>>();
    let mut objects = Vec::new();
    let mut paths = BTreeSet::new();
    for fragment in manifest.fragments.iter() {
        let mut fields = BTreeSet::new();
        for file in &fragment.files {
            file.validate(&base)
                .map_err(|source| LanceManifestError::engine("validate fragment file", source))?;
            if file
                .file_version()
                .map_err(|source| LanceManifestError::engine("validate data file version", source))?
                != ConcreteFileVersion::V2_2
                || file.base_id.is_some()
            {
                return Err(LanceManifestError::UnsupportedFormat {
                    reason: "Vector data files require local Lance 2.2 storage",
                });
            }
            let name = file
                .path
                .strip_suffix(".lance")
                .ok_or(LanceManifestError::ReferenceMismatch {
                    reason: "data file name has an invalid suffix",
                })?;
            if name.len() != 50
                || !name.as_bytes()[..24].iter().all(|byte| matches!(byte, b'0' | b'1'))
                || !name.as_bytes()[24..]
                    .iter()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
            {
                return Err(LanceManifestError::ReferenceMismatch {
                    reason: "data file name differs from the native Lance writer format",
                });
            }
            if file
                .fields
                .iter()
                .any(|field| !schema_fields.contains(field) || !fields.insert(*field))
            {
                return Err(LanceManifestError::SchemaMismatch {
                    reason: "fragment data files contain unexpected or overlapping fields",
                });
            }
            let key = CatalogObjectKey::try_new(format!("{}data/{}", resource.dataset_prefix(), file.path))?;
            if !paths.insert(key.as_str().to_string()) {
                return Err(LanceManifestError::ReferenceMismatch {
                    reason: "data file is reused by multiple fragments",
                });
            }
            let expected_size = file.file_size_bytes.get().ok_or(LanceManifestError::ReferenceMismatch {
                reason: "data file has no exact byte length",
            })?;
            objects.push(ManifestObjectReference {
                key,
                expected_size: Some(expected_size),
            });
        }
        if fields != schema_fields {
            return Err(LanceManifestError::SchemaMismatch {
                reason: "fragment data files do not cover the Vector schema",
            });
        }
        if let Some(deletion) = &fragment.deletion_file {
            if deletion.base_id.is_some() || deletion.read_version == 0 || deletion.read_version >= manifest.version {
                return Err(LanceManifestError::ReferenceMismatch {
                    reason: "deletion file identifies an invalid read version or external base",
                });
            }
            let key = CatalogObjectKey::try_new(
                lance_table::io::deletion::deletion_file_path(&base, fragment.id, deletion).to_string(),
            )?;
            if !paths.insert(key.as_str().to_string()) {
                return Err(LanceManifestError::ReferenceMismatch {
                    reason: "deletion file appears more than once",
                });
            }
            objects.push(ManifestObjectReference {
                key,
                expected_size: None,
            });
        }
    }
    if let Some(transaction) = &manifest.transaction_file {
        let name = transaction
            .strip_suffix(".txn")
            .ok_or(LanceManifestError::ReferenceMismatch {
                reason: "transaction file name has an invalid suffix",
            })?;
        let (read_version, id) = name.split_once('-').ok_or(LanceManifestError::ReferenceMismatch {
            reason: "transaction file name is incomplete",
        })?;
        let read_version = read_version
            .parse::<u64>()
            .map_err(|_| LanceManifestError::ReferenceMismatch {
                reason: "transaction read version is invalid",
            })?;
        canonical_uuid(id)?;
        if read_version >= manifest.version || format!("{read_version}-{id}.txn") != *transaction {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "transaction file does not identify a predecessor",
            });
        }
        objects.push(ManifestObjectReference {
            key: CatalogObjectKey::try_new(format!("{}_transactions/{transaction}", resource.dataset_prefix()))?,
            expected_size: None,
        });
    }
    Ok(objects)
}

fn canonical_uuid(value: &str) -> Result<Uuid, LanceManifestError> {
    let parsed = Uuid::parse_str(value).map_err(|_| LanceManifestError::ReferenceMismatch {
        reason: "data object name requires a canonical UUID",
    })?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(LanceManifestError::ReferenceMismatch {
            reason: "data object name requires a canonical non-nil UUID",
        });
    }
    Ok(parsed)
}

pub(super) fn validate_writable_relative_path(relative: &str) -> Result<(), LanceManifestError> {
    let invalid = || LanceManifestError::ReferenceMismatch {
        reason: "Dataset write path is not a native immutable data object",
    };
    if let Some(name) = relative.strip_prefix("data/").and_then(|name| name.strip_suffix(".lance")) {
        if name.len() == 50
            && name.as_bytes()[..24].iter().all(|byte| matches!(byte, b'0' | b'1'))
            && name.as_bytes()[24..]
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        {
            return Ok(());
        }
        return Err(invalid());
    }
    if let Some(name) = relative
        .strip_prefix("_transactions/")
        .and_then(|name| name.strip_suffix(".txn"))
    {
        let (version, id) = name.split_once('-').ok_or_else(invalid)?;
        let parsed = version.parse::<u64>().map_err(|_| invalid())?;
        if parsed.to_string() != version {
            return Err(invalid());
        }
        canonical_uuid(id)?;
        return Ok(());
    }
    if let Some(name) = relative.strip_prefix("_indices/") {
        let (id, file) = name.split_once('/').ok_or_else(invalid)?;
        canonical_uuid(id)?;
        return if matches!(file, "index.idx" | "auxiliary.idx") {
            Ok(())
        } else {
            Err(invalid())
        };
    }
    if let Some(name) = relative.strip_prefix("_deletions/") {
        let name = name
            .strip_suffix(".arrow")
            .or_else(|| name.strip_suffix(".bin"))
            .ok_or_else(invalid)?;
        let parts = name.split('-').collect::<Vec<_>>();
        let [fragment, version, id] = parts.as_slice() else {
            return Err(invalid());
        };
        for part in [fragment, version, id] {
            let parsed = part.parse::<u64>().map_err(|_| invalid())?;
            if parsed.to_string() != *part {
                return Err(invalid());
            }
        }
        return Ok(());
    }
    Err(invalid())
}

pub(super) fn validate_transaction(transaction: &pb::Transaction, manifest: &Manifest) -> Result<(), LanceManifestError> {
    use pb::transaction::Operation;
    canonical_uuid(&transaction.uuid)?;
    if transaction.read_version == 0
        || (transaction.read_version.checked_add(1) != Some(manifest.version)
            && !(matches!(transaction.operation, Some(Operation::Rewrite(_)))
                && transaction.read_version.checked_add(2) == Some(manifest.version)))
    {
        return Err(LanceManifestError::ReferenceMismatch {
            reason: "transaction read version differs from the manifest predecessor",
        });
    }
    if !transaction.tag.is_empty() || !transaction.transaction_properties.is_empty() {
        return Err(LanceManifestError::UnsupportedFormat {
            reason: "Vector transactions cannot set tags or arbitrary properties",
        });
    }
    if is_maintenance_transaction(transaction) {
        return validate_maintenance_transaction(transaction, manifest);
    }
    let (updated, new, removed) = match transaction.operation.as_ref() {
        Some(Operation::Update(update)) => {
            let fields = manifest.schema.field_ids().into_iter().collect::<BTreeSet<_>>();
            if update.update_mode != 0
                || !update.compacted_sstables.is_empty()
                || !update.updated_fragment_offsets.is_empty()
                || !update.updated_fragment_offset_bitmaps.is_empty()
                || update
                    .fields_modified
                    .iter()
                    .chain(&update.fields_for_preserving_frag_bitmap)
                    .any(|field| i32::try_from(*field).ok().is_none_or(|field| !fields.contains(&field)))
            {
                return Err(LanceManifestError::UnsupportedFormat {
                    reason: "Vector update transaction requires full rows and the fixed schema",
                });
            }
            (&update.updated_fragments[..], &update.new_fragments[..], &update.removed_fragment_ids[..])
        }
        Some(Operation::Delete(delete)) => (&delete.updated_fragments[..], &[][..], &delete.deleted_fragment_ids[..]),
        _ => {
            return Err(LanceManifestError::UnsupportedFormat {
                reason: "Vector data transactions support key updates and deletions",
            });
        }
    };
    validate_fragment_protobuf(updated)?;
    let mut removed_ids = BTreeSet::new();
    if removed.iter().any(|id| *id > u64::from(u32::MAX) || !removed_ids.insert(*id)) {
        return Err(LanceManifestError::CorruptManifest {
            reason: "transaction removed fragment identities are invalid",
        });
    }
    let mut matched_ids = BTreeSet::new();
    for (fragment, is_new) in updated
        .iter()
        .map(|fragment| (fragment, false))
        .chain(new.iter().map(|fragment| (fragment, true)))
    {
        validate_fragment_protobuf(std::slice::from_ref(fragment))?;
        let found = manifest
            .fragments
            .iter()
            .find(|current| {
                let mut current = pb::DataFragment::from(*current);
                if is_new {
                    current.id = fragment.id;
                }
                &current == fragment
            })
            .ok_or(LanceManifestError::ReferenceMismatch {
                reason: "transaction fragment is absent from the resulting manifest",
            })?;
        if !matched_ids.insert(found.id) {
            return Err(LanceManifestError::CorruptManifest {
                reason: "transaction repeats a resulting fragment",
            });
        }
    }
    if removed
        .iter()
        .any(|removed| manifest.fragments.iter().any(|fragment| fragment.id == *removed))
    {
        return Err(LanceManifestError::ReferenceMismatch {
            reason: "transaction retained a removed fragment",
        });
    }
    if manifest
        .transaction_file
        .as_ref()
        .is_some_and(|name| *name != format!("{}-{}.txn", transaction.read_version, transaction.uuid))
    {
        return Err(LanceManifestError::ReferenceMismatch {
            reason: "transaction file name differs from the transaction identity",
        });
    }
    Ok(())
}

pub(super) fn decode_indices(indices: Vec<pb::IndexMetadata>) -> Result<Vec<IndexMetadata>, LanceManifestError> {
    indices
        .into_iter()
        .map(|index| {
            if index.clustering_id.is_some()
                || !index.metadata.is_empty()
                || index.created_at.is_some_and(|value| {
                    i64::try_from(value)
                        .ok()
                        .and_then(chrono::DateTime::from_timestamp_millis)
                        .is_none()
                })
            {
                return Err(LanceManifestError::UnsupportedFormat {
                    reason: "unsupported index metadata or timestamp",
                });
            }
            let bitmap_length = index.fragment_bitmap.len();
            let decoded =
                IndexMetadata::try_from(index).map_err(|source| LanceManifestError::engine("decode index metadata", source))?;
            if decoded
                .fragment_bitmap
                .as_ref()
                .is_some_and(|bitmap| bitmap.serialized_size() != bitmap_length)
            {
                return Err(LanceManifestError::CorruptManifest {
                    reason: "ANN fragment bitmap contains trailing or unsupported bytes",
                });
            }
            Ok(decoded)
        })
        .collect()
}

pub(super) fn validate_indices(
    indices: &[IndexMetadata],
    manifest: &Manifest,
    definition: &VectorIndexManifestSpec,
) -> Result<(), LanceManifestError> {
    if indices.len() > 1 {
        return Err(LanceManifestError::UnsupportedFormat {
            reason: "only one logical ANN index is supported",
        });
    }
    let vector = manifest.schema.field("vector").ok_or(LanceManifestError::SchemaMismatch {
        reason: "vector field is absent",
    })?;
    for index in indices {
        if index.uuid.is_nil()
            || index.name != ANN_INDEX_NAME
            || index.fields != [vector.id]
            || !index.covering_fields.is_empty()
            || index.base_id.is_some()
            || index.index_version != 1
            || index.dataset_version == 0
            || index.dataset_version > manifest.version
            || index.fragment_bitmap.is_none()
        {
            return Err(LanceManifestError::UnsupportedFormat {
                reason: "unsupported ANN identity, fields, version or coverage",
            });
        }
        let details = index.index_details.as_ref().ok_or(LanceManifestError::CorruptManifest {
            reason: "ANN details are absent",
        })?;
        if details.type_url != "/lance.index.pb.VectorIndexDetails"
            && details.type_url != "type.googleapis.com/lance.index.pb.VectorIndexDetails"
        {
            return Err(LanceManifestError::UnsupportedFormat {
                reason: "unsupported ANN details type",
            });
        }
        let details = lance_index::pb::VectorIndexDetails::decode(details.value.as_slice())
            .map_err(|source| LanceManifestError::Protobuf { source })?;
        let metric = match definition.distance_metric {
            super::schema::VectorDistanceMetric::Euclidean => 0,
            super::schema::VectorDistanceMetric::Cosine => 1,
        };
        if details.metric_type != metric
            || details.target_partition_size != 0
            || details.hnsw_index_config.is_some()
            || !matches!(details.compression, Some(lance_index::pb::vector_index_details::Compression::Flat(_)))
        {
            return Err(LanceManifestError::UnsupportedFormat {
                reason: "ANN requires the configured metric and native IVF_FLAT",
            });
        }
        for (key, value) in &details.runtime_hints {
            let expected = match key.as_str() {
                "lance.ivf.max_iters" => Some("20"),
                "lance.ivf.sample_rate" => Some("64"),
                "lance.ivf.shuffle_partition_batches" => Some("10240"),
                "lance.ivf.shuffle_partition_concurrency" => Some("2"),
                "lance.skip_transpose" => Some("false"),
                _ => None,
            };
            if expected.is_some_and(|expected| expected != value) {
                return Err(LanceManifestError::UnsupportedFormat {
                    reason: "ANN runtime hint is unsupported",
                });
            }
        }
        index_objects(index, None)?;
    }
    Ok(())
}

fn index_objects(
    index: &IndexMetadata,
    resource: Option<&CatalogResourceIdentity>,
) -> Result<Vec<ManifestObjectReference>, LanceManifestError> {
    let files = index.files.as_ref().ok_or(LanceManifestError::ReferenceMismatch {
        reason: "ANN file size table is absent",
    })?;
    if files.len() != 2 {
        return Err(LanceManifestError::ReferenceMismatch {
            reason: "IVF_FLAT requires exactly two files",
        });
    }
    let mut names = BTreeSet::new();
    let mut references = Vec::new();
    for file in files {
        if !matches!(file.path.as_str(), "index.idx" | "auxiliary.idx")
            || !names.insert(file.path.as_str())
            || file.size_bytes == 0
        {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "ANN file name, size or uniqueness is invalid",
            });
        }
        if let Some(resource) = resource {
            references.push(ManifestObjectReference {
                key: CatalogObjectKey::try_new(format!("{}_indices/{}/{}", resource.dataset_prefix(), index.uuid, file.path))?,
                expected_size: NonZeroU64::new(file.size_bytes),
            });
        }
    }
    Ok(references)
}

pub(super) fn index_references(
    indices: &[IndexMetadata],
    resource: &CatalogResourceIdentity,
) -> Result<Vec<ManifestObjectReference>, LanceManifestError> {
    indices.iter().try_fold(Vec::new(), |mut references, index| {
        references.extend(index_objects(index, Some(resource))?);
        Ok(references)
    })
}

pub(super) fn is_maintenance_transaction(transaction: &pb::Transaction) -> bool {
    matches!(
        transaction.operation,
        Some(
            pb::transaction::Operation::CreateIndex(_)
                | pb::transaction::Operation::ReserveFragments(_)
                | pb::transaction::Operation::Rewrite(_)
        )
    )
}

fn validate_maintenance_transaction(transaction: &pb::Transaction, manifest: &Manifest) -> Result<(), LanceManifestError> {
    use pb::transaction::Operation;
    if manifest
        .transaction_file
        .as_ref()
        .is_some_and(|name| *name != format!("{}-{}.txn", transaction.read_version, transaction.uuid))
    {
        return Err(LanceManifestError::ReferenceMismatch {
            reason: "maintenance transaction file identity differs",
        });
    }
    match transaction.operation.as_ref() {
        Some(Operation::CreateIndex(create)) => {
            if create.new_indices.len() != 1 || create.removed_indices.len() > 1 {
                return Err(LanceManifestError::UnsupportedFormat {
                    reason: "ANN rebuild must replace the complete logical index",
                });
            }
            decode_indices(create.new_indices.clone())?;
            decode_indices(create.removed_indices.clone())?;
        }
        Some(Operation::ReserveFragments(_)) => {}
        Some(Operation::Rewrite(rewrite)) => {
            if !rewrite.old_fragments.is_empty() || !rewrite.new_fragments.is_empty() || rewrite.groups.is_empty() {
                return Err(LanceManifestError::UnsupportedFormat {
                    reason: "compaction requires explicit rewrite groups",
                });
            }
            let mut old_ids = BTreeSet::new();
            let mut new_ids = BTreeSet::new();
            for group in &rewrite.groups {
                if group.old_fragments.is_empty() {
                    return Err(LanceManifestError::CorruptManifest {
                        reason: "rewrite source group is empty",
                    });
                }
                validate_fragment_protobuf(&group.old_fragments)?;
                validate_fragment_protobuf(&group.new_fragments)?;
                for old in &group.old_fragments {
                    if !old_ids.insert(old.id) || manifest.fragments.iter().any(|fragment| fragment.id == old.id) {
                        return Err(LanceManifestError::ReferenceMismatch {
                            reason: "rewrite source is repeated or retained",
                        });
                    }
                }
                for new in &group.new_fragments {
                    if !new_ids.insert(new.id)
                        || !manifest
                            .fragments
                            .iter()
                            .any(|fragment| pb::DataFragment::from(fragment) == *new)
                    {
                        return Err(LanceManifestError::ReferenceMismatch {
                            reason: "rewrite output is repeated or absent",
                        });
                    }
                }
                let live = |fragments: &[pb::DataFragment]| {
                    fragments.iter().try_fold(0u64, |sum, fragment| {
                        sum.checked_add(
                            fragment.physical_rows
                                - fragment
                                    .deletion_file
                                    .as_ref()
                                    .map_or(0, |deletion| deletion.num_deleted_rows),
                        )
                    })
                };
                if live(&group.old_fragments) != live(&group.new_fragments) {
                    return Err(LanceManifestError::ReferenceMismatch {
                        reason: "rewrite changes the live row count",
                    });
                }
            }
        }
        _ => {
            return Err(LanceManifestError::UnsupportedFormat {
                reason: "unsupported maintenance operation",
            });
        }
    }
    Ok(())
}
