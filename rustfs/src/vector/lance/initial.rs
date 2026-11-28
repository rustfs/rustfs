// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroU32;

use bytes::Bytes;
use lance_table::io::commit::write_manifest_file_to_path;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::table_catalog::{CatalogCommitId, CatalogMetadataRef, CatalogObjectKey, CatalogResourceIdentity, TableCatalogObject};

use super::{
    LanceManifestError, VectorIndexManifestSpec,
    manifest::{empty_manifest, encode_manifest, manifest_metadata},
};

pub(crate) struct InitialManifestPlan {
    pub bytes: Bytes,
    pub reference: CatalogMetadataRef,
}

pub(crate) async fn plan_initial_manifest(
    resource: &CatalogResourceIdentity,
    index: &VectorIndexManifestSpec,
    create_id: &CatalogCommitId,
) -> Result<InitialManifestPlan, LanceManifestError> {
    let id = Uuid::parse_str(create_id.as_str()).map_err(|_| LanceManifestError::ReferenceMismatch {
        reason: "initial manifest create ID must be a canonical non-nil UUID",
    })?;
    if id.is_nil() || id.to_string() != create_id.as_str() {
        return Err(LanceManifestError::ReferenceMismatch {
            reason: "initial manifest create ID must be a canonical non-nil UUID",
        });
    }
    let mut manifest = empty_manifest(index.dimension, 1, manifest_metadata(resource, index)?)?;
    let bytes = encode_manifest(&mut manifest, None, None, write_manifest_file_to_path).await?;
    let key = CatalogObjectKey::try_new(format!("{}_versions/{}/1.manifest", resource.dataset_prefix(), create_id.as_str()))?;
    let reference = CatalogMetadataRef::from_read(
        key,
        &TableCatalogObject {
            data: bytes.to_vec(),
            body_revision: None,
            etag: None,
            mod_time: None,
        },
    )?;
    Ok(InitialManifestPlan { bytes, reference })
}

impl InitialManifestPlan {
    pub(crate) fn verify_reserved_reference(&self, reserved: &CatalogMetadataRef) -> Result<(), LanceManifestError> {
        if reserved != &self.reference
            || self.reference.body_revision().is_some()
            || self.reference.etag().is_some()
            || self.reference.content_length() != self.bytes.len() as u64
            || self.reference.content_sha256() != crate::table_catalog::CatalogContentSha256::digest(&self.bytes)
        {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "initial manifest bytes differ from the reserved creation plan",
            });
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InitializationPayloadV1 {
    encoding_version: u16,
    data_type: String,
    dimension: u32,
    distance_metric: crate::vector::model::VectorDistanceMetric,
    non_filterable_metadata_keys: Vec<String>,
}

pub(crate) fn encode_initialization_payload(index: &VectorIndexManifestSpec) -> Result<Vec<u8>, LanceManifestError> {
    index.validate()?;
    let keys = index.non_filterable_metadata_keys.iter().cloned().collect::<Vec<_>>();
    if !keys.is_empty() {
        crate::vector::model::validate_non_filterable_keys(&keys).map_err(|source| LanceManifestError::Storage {
            operation: "validate initial non-filterable keys",
            source: Box::new(source),
        })?;
    }
    Ok(serde_json::to_vec(&InitializationPayloadV1 {
        encoding_version: 1,
        data_type: "float32".into(),
        dimension: index.dimension.get(),
        distance_metric: index.distance_metric,
        non_filterable_metadata_keys: keys,
    })?)
}

pub(crate) fn decode_initialization_payload(
    bytes: &[u8],
    encryption: &crate::table_catalog::VectorEffectiveEncryptionV1,
) -> Result<VectorIndexManifestSpec, LanceManifestError> {
    let payload: InitializationPayloadV1 = serde_json::from_slice(bytes)?;
    if payload.encoding_version != 1 || payload.data_type != "float32" {
        return Err(LanceManifestError::UnsupportedFormat {
            reason: "unsupported initialization payload version or data type",
        });
    }
    let dimension = NonZeroU32::new(payload.dimension).ok_or(LanceManifestError::SchemaMismatch {
        reason: "initial dimension must be positive",
    })?;
    let index = VectorIndexManifestSpec {
        schema_version: 1,
        dimension,
        distance_metric: payload.distance_metric,
        non_filterable_metadata_keys: payload.non_filterable_metadata_keys.into_iter().collect(),
        encryption_descriptor_sha256: encryption.descriptor_sha256()?,
    };
    if encode_initialization_payload(&index)? != bytes {
        return Err(LanceManifestError::ReferenceMismatch {
            reason: "initialization payload must have its canonical encoding",
        });
    }
    Ok(index)
}
