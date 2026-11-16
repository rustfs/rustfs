// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeSet, num::NonZeroU64};

use lance_file::version::ConcreteFileVersion;
use lance_table::format::{Manifest, pb};
use object_store_lance::path::Path;
use uuid::Uuid;

use crate::table_catalog::{CatalogObjectKey, CatalogResourceIdentity};

use super::LanceManifestError;

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
    if transaction.read_version == 0 || transaction.read_version.checked_add(1) != Some(manifest.version) {
        return Err(LanceManifestError::ReferenceMismatch {
            reason: "transaction read version differs from the manifest predecessor",
        });
    }
    if !transaction.tag.is_empty() || !transaction.transaction_properties.is_empty() {
        return Err(LanceManifestError::UnsupportedFormat {
            reason: "Vector transactions cannot set tags or arbitrary properties",
        });
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
