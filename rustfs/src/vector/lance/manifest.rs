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

use std::{collections::HashMap, num::NonZeroU32, sync::Arc};

use bytes::Bytes;
use lance_file::version::ConcreteFileVersion;
use lance_io::object_store::ObjectStore;
use lance_table::{
    feature_flags::{apply_feature_flags, ensure_can_read_manifest, ensure_can_write_manifest},
    format::{DataStorageFormat, IndexMetadata, Manifest, Transaction, is_detached_version},
    io::{commit::ManifestWriter, manifest::read_manifest},
};
use object_store_lance::{ObjectStoreExt, path::Path};
use prost::Message;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::table_catalog::{
    CatalogCommitParts, CatalogEntryUpdates, CatalogFormat, CatalogFormatDescriptor, CatalogMetadataRef, CatalogResourceIdentity,
    PreparedCatalogCommit,
};

use super::schema::{VectorDistanceMetric, VectorIndexManifestSpec};
use super::{LanceManifestError, schema};

pub(super) const VECTOR_BINDING_KEY: &str = "rustfs.vector.binding.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct VectorManifestBindingV1 {
    encoding_version: u16,
    #[serde(deserialize_with = "deserialize_uuid")]
    bucket_incarnation: Uuid,
    table_id: String,
    index_schema_version: u16,
    dimension: u32,
    distance_metric: VectorDistanceMetric,
    non_filterable_metadata_keys: Vec<String>,
    #[serde(deserialize_with = "deserialize_sha256")]
    encryption_descriptor_sha256: String,
}

impl VectorManifestBindingV1 {
    fn new(resource: &CatalogResourceIdentity, index: &VectorIndexManifestSpec) -> Result<Self, LanceManifestError> {
        index.validate()?;
        let bucket = resource.vector_bucket().ok_or(LanceManifestError::ResourceMismatch {
            reason: "Lance requires a Vector bucket identity",
        })?;
        if resource.format() != CatalogFormat::Lance {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "resource format must be Lance",
            });
        }
        Ok(Self {
            encoding_version: 1,
            bucket_incarnation: bucket.incarnation().as_uuid(),
            table_id: resource.table_id().as_str().to_string(),
            index_schema_version: index.schema_version,
            dimension: index.dimension.get(),
            distance_metric: index.distance_metric,
            non_filterable_metadata_keys: index.non_filterable_metadata_keys.iter().cloned().collect(),
            encryption_descriptor_sha256: index.encryption_descriptor_sha256.to_hex(),
        })
    }

    pub(super) fn decode(
        json: &str,
        resource: &CatalogResourceIdentity,
        index: &VectorIndexManifestSpec,
    ) -> Result<Self, LanceManifestError> {
        let binding: Self = serde_json::from_str(json)?;
        binding.validate(resource, index)?;
        Ok(binding)
    }

    fn validate(&self, resource: &CatalogResourceIdentity, index: &VectorIndexManifestSpec) -> Result<(), LanceManifestError> {
        if self != &Self::new(resource, index)? || self.non_filterable_metadata_keys.windows(2).any(|keys| keys[0] >= keys[1]) {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "manifest binding differs from resource identity or Index configuration",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LanceMetadataReferenceV1 {
    object_key: String,
    content_length: u64,
    #[serde(deserialize_with = "deserialize_sha256")]
    content_sha256: String,
    #[serde(deserialize_with = "deserialize_optional_uuid")]
    body_revision: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_required_optional")]
    etag: Option<String>,
}

impl From<&CatalogMetadataRef> for LanceMetadataReferenceV1 {
    fn from(reference: &CatalogMetadataRef) -> Self {
        Self {
            object_key: reference.object_key().as_str().to_string(),
            content_length: reference.content_length(),
            content_sha256: reference.content_sha256().to_hex(),
            body_revision: reference.body_revision().map(|revision| revision.as_uuid()),
            etag: reference.etag().map(str::to_string),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LanceManifestDescriptorV1 {
    descriptor_version: u16,
    library_version: String,
    manifest_version: u64,
    footer_major: u16,
    footer_minor: u16,
    data_storage_format: String,
    data_storage_version: String,
    reader_feature_flags: u64,
    writer_feature_flags: u64,
    binding: VectorManifestBindingV1,
    metadata: LanceMetadataReferenceV1,
}

impl LanceManifestDescriptorV1 {
    pub(super) fn decode(
        descriptor: &CatalogFormatDescriptor,
        resource: &CatalogResourceIdentity,
        index: &VectorIndexManifestSpec,
    ) -> Result<Self, LanceManifestError> {
        if descriptor.encoding_version() != 1 || descriptor.format() != CatalogFormat::Lance || descriptor.format_version() != 1 {
            return Err(LanceManifestError::UnsupportedFormat {
                reason: "unsupported Catalog Lance descriptor envelope",
            });
        }
        let decoded: Self = serde_json::from_slice(descriptor.payload())?;
        if decoded.descriptor_version != 1
            || decoded.library_version != "14.0.2"
            || decoded.footer_major != 0
            || decoded.footer_minor != 2
            || decoded.data_storage_format != "lance"
            || decoded.data_storage_version != "2.2"
        {
            return Err(LanceManifestError::UnsupportedFormat {
                reason: "unsupported Lance descriptor or file format version",
            });
        }
        if !lance_table::feature_flags::can_read_dataset(decoded.reader_feature_flags)
            || !lance_table::feature_flags::can_write_dataset(decoded.writer_feature_flags)
        {
            return Err(LanceManifestError::UnsupportedFormat {
                reason: "descriptor requires unsupported Lance features",
            });
        }
        decoded.binding.validate(resource, index)?;
        if decoded.metadata != LanceMetadataReferenceV1::from(descriptor.metadata()) || decoded.metadata.body_revision.is_none() {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "descriptor payload differs from its exact Catalog reference",
            });
        }
        validate_manifest_path(resource, descriptor.metadata(), decoded.manifest_version, None)?;
        Ok(decoded)
    }

    pub(super) fn index_from_catalog(
        head: &crate::table_catalog::CatalogHead,
        definition: &crate::table_catalog::VectorIndexDefinitionV1,
    ) -> Result<VectorIndexManifestSpec, LanceManifestError> {
        definition.validate()?;
        let decoded: Self = serde_json::from_slice(head.format_descriptor().payload())?;
        let index = VectorIndexManifestSpec {
            schema_version: decoded.binding.index_schema_version,
            dimension: NonZeroU32::new(decoded.binding.dimension).ok_or(LanceManifestError::SchemaMismatch {
                reason: "descriptor dimension must be positive",
            })?,
            distance_metric: decoded.binding.distance_metric,
            non_filterable_metadata_keys: decoded.binding.non_filterable_metadata_keys.iter().cloned().collect(),
            encryption_descriptor_sha256: definition.effective_encryption.descriptor_sha256()?,
        };
        Self::decode(head.format_descriptor(), head.resource_identity(), &index)?;
        index.validate()?;
        super::initial::encode_initialization_payload(&index)?;
        Ok(index)
    }

    pub(super) fn manifest_version(&self) -> u64 {
        self.manifest_version
    }

    pub(super) fn verify_manifest(&self, validated: &ValidatedLanceManifest) -> Result<(), LanceManifestError> {
        let manifest = validated.manifest();
        if self.manifest_version != manifest.version
            || self.reader_feature_flags != manifest.reader_feature_flags
            || self.writer_feature_flags != manifest.writer_feature_flags
        {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "descriptor flags or version differ from the stored manifest",
            });
        }
        Ok(())
    }
}

fn deserialize_sha256<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let value = String::deserialize(deserializer)?;
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(serde::de::Error::custom("SHA-256 must contain 64 lowercase hexadecimal characters"));
    }
    Ok(value)
}

fn parse_uuid<E: serde::de::Error>(value: String) -> Result<Uuid, E> {
    let parsed = Uuid::parse_str(&value).map_err(E::custom)?;
    if parsed.is_nil() || parsed.to_string() != value {
        return Err(E::custom("UUID must be a non-nil canonical hyphenated value"));
    }
    Ok(parsed)
}

fn deserialize_uuid<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Uuid, D::Error> {
    parse_uuid(String::deserialize(deserializer)?)
}

fn deserialize_optional_uuid<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Uuid>, D::Error> {
    Option::<String>::deserialize(deserializer)?.map(parse_uuid).transpose()
}

fn deserialize_required_optional<'de, D: Deserializer<'de>, T: Deserialize<'de>>(deserializer: D) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)
}

pub(super) fn manifest_metadata(
    resource: &CatalogResourceIdentity,
    index: &VectorIndexManifestSpec,
) -> Result<HashMap<String, String>, LanceManifestError> {
    Ok([(
        VECTOR_BINDING_KEY.to_string(),
        serde_json::to_string(&VectorManifestBindingV1::new(resource, index)?)?,
    )]
    .into())
}

pub(super) fn validate_manifest_path(
    resource: &CatalogResourceIdentity,
    reference: &CatalogMetadataRef,
    version: u64,
    commit_id: Option<&str>,
) -> Result<(), LanceManifestError> {
    let prefix = format!("{}_versions/", resource.dataset_prefix());
    let invalid = || LanceManifestError::ReferenceMismatch {
        reason: "manifest path differs from resource, commit identity or positive version",
    };
    let suffix = reference.object_key().as_str().strip_prefix(&prefix).ok_or_else(invalid)?;
    let (actual_commit_id, basename) = suffix.split_once('/').ok_or_else(invalid)?;
    let id = Uuid::parse_str(actual_commit_id).map_err(|_| invalid())?;
    if version == 0
        || is_detached_version(version)
        || id.is_nil()
        || actual_commit_id != id.to_string()
        || basename != format!("{version}.manifest")
        || commit_id.is_some_and(|expected| actual_commit_id != expected)
    {
        return Err(invalid());
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ManifestContentIdentity {
    content_length: u64,
    content_sha256: [u8; 32],
}

impl ManifestContentIdentity {
    pub(super) fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            content_length: bytes.len() as u64,
            content_sha256: Sha256::digest(bytes).into(),
        }
    }

    pub(super) fn content_length(&self) -> u64 {
        self.content_length
    }
}

#[derive(Debug)]
pub(super) struct ValidatedLanceManifest {
    manifest: Manifest,
    exact_file: Bytes,
    content_identity: ManifestContentIdentity,
    bound_reference: Option<CatalogMetadataRef>,
    inline_transaction: Option<lance_table::format::pb::Transaction>,
}

impl ValidatedLanceManifest {
    pub(super) async fn read_bound(
        bytes: Bytes,
        reference: &CatalogMetadataRef,
        resource: &CatalogResourceIdentity,
        index: &VectorIndexManifestSpec,
        version: u64,
        max_manifest_bytes: u64,
    ) -> Result<Self, LanceManifestError> {
        validate_manifest_path(resource, reference, version, None)?;
        if reference.body_revision().is_none() {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "Vector manifest requires a non-nil body revision",
            });
        }
        let identity = ManifestContentIdentity {
            content_length: reference.content_length(),
            content_sha256: *reference.content_sha256().as_bytes(),
        };
        let metadata = manifest_metadata(resource, index)?;
        let mut validated =
            Self::read_and_validate(bytes, &identity, index.dimension, version, &metadata, max_manifest_bytes).await?;
        VectorManifestBindingV1::decode(
            validated
                .manifest
                .table_metadata
                .get(VECTOR_BINDING_KEY)
                .ok_or(LanceManifestError::ResourceMismatch {
                    reason: "manifest resource binding is absent",
                })?,
            resource,
            index,
        )?;
        super::files::manifest_objects(validated.manifest(), resource)?;
        validated.bound_reference = Some(reference.clone());
        Ok(validated)
    }

    pub(super) fn format_descriptor(
        &self,
        resource: &CatalogResourceIdentity,
        index: &VectorIndexManifestSpec,
        reference: &CatalogMetadataRef,
    ) -> Result<CatalogFormatDescriptor, LanceManifestError> {
        if self.bound_reference.as_ref() != Some(reference)
            || self.exact_file().len() as u64 != reference.content_length()
            || &self.content_identity().content_sha256 != reference.content_sha256().as_bytes()
            || reference.body_revision().is_none()
        {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "descriptor reference does not identify the validated manifest bytes",
            });
        }
        let binding = VectorManifestBindingV1::decode(
            self.manifest
                .table_metadata
                .get(VECTOR_BINDING_KEY)
                .ok_or(LanceManifestError::ResourceMismatch {
                    reason: "manifest resource binding is absent",
                })?,
            resource,
            index,
        )?;
        validate_manifest_path(resource, reference, self.manifest.version, None)?;
        let payload = LanceManifestDescriptorV1 {
            descriptor_version: 1,
            library_version: "14.0.2".into(),
            manifest_version: self.manifest.version,
            footer_major: 0,
            footer_minor: 2,
            data_storage_format: "lance".into(),
            data_storage_version: "2.2".into(),
            reader_feature_flags: self.manifest.reader_feature_flags,
            writer_feature_flags: self.manifest.writer_feature_flags,
            binding,
            metadata: reference.into(),
        };
        Ok(CatalogFormatDescriptor::try_new(
            1,
            CatalogFormat::Lance,
            1,
            reference.clone(),
            serde_json::to_vec(&payload)?,
        )?)
    }

    pub(super) fn into_prepared(
        self,
        parts: CatalogCommitParts,
        index: &VectorIndexManifestSpec,
        reference: CatalogMetadataRef,
    ) -> Result<PreparedCatalogCommit, LanceManifestError> {
        validate_manifest_path(
            &parts.resource_identity,
            &reference,
            self.manifest.version,
            Some(parts.commit_id.as_str()),
        )?;
        let descriptor = self.format_descriptor(&parts.resource_identity, index, &reference)?;
        let warehouse = format!(
            "s3://{}/{}",
            parts.resource_identity.storage_bucket_id(),
            parts.resource_identity.dataset_prefix().trim_end_matches('/')
        );
        let updates = CatalogEntryUpdates::try_new(Some(warehouse), Some(1))?;
        Ok(PreparedCatalogCommit::try_from_validated_parts(
            parts,
            reference,
            descriptor,
            updates,
            Vec::new(),
        )?)
    }
    pub(super) async fn read_and_validate(
        bytes: Bytes,
        expected_content: &ManifestContentIdentity,
        dimension: NonZeroU32,
        expected_version: u64,
        expected_metadata: &HashMap<String, String>,
        max_manifest_bytes: u64,
    ) -> Result<Self, LanceManifestError> {
        let actual_length = bytes.len() as u64;
        if actual_length > max_manifest_bytes {
            return Err(LanceManifestError::ReadLimitExceeded {
                length: actual_length,
                limit: max_manifest_bytes,
            });
        }
        if actual_length != expected_content.content_length() {
            return Err(LanceManifestError::LengthMismatch {
                expected: expected_content.content_length,
                actual: actual_length,
            });
        }
        let content_identity = ManifestContentIdentity::from_bytes(&bytes);
        if &content_identity != expected_content {
            return Err(LanceManifestError::DigestMismatch);
        }
        let manifest_offset = validate_footer(&bytes)?;
        validate_protobuf(&bytes, manifest_offset, dimension)?;
        let store = ObjectStore::memory();
        let path = Path::from("validated.manifest");
        store.inner.put(&path, bytes.clone().into()).await?;
        let manifest = read_manifest(&store, &path, Some(actual_length))
            .await
            .map_err(|source| LanceManifestError::engine("read manifest", source))?;
        ensure_can_read_manifest(&manifest).map_err(|source| LanceManifestError::engine("check reader features", source))?;
        ensure_can_write_manifest(&manifest).map_err(|source| LanceManifestError::engine("check writer features", source))?;
        if manifest.version != expected_version {
            return Err(LanceManifestError::VersionMismatch {
                expected: expected_version,
                actual: manifest.version,
            });
        }
        if manifest.version == 0 || is_detached_version(manifest.version) || manifest.branch.is_some() {
            return Err(LanceManifestError::UnsupportedFormat {
                reason: "only positive main-branch versions are supported",
            });
        }
        if manifest.data_storage_format != DataStorageFormat::new(ConcreteFileVersion::V2_2) {
            return Err(LanceManifestError::UnsupportedFormat {
                reason: "data storage format must be lance 2.2",
            });
        }
        schema::validate_schema(&manifest.schema, dimension)?;
        if &manifest.table_metadata != expected_metadata {
            return Err(LanceManifestError::ResourceMismatch {
                reason: "table metadata differs from the expected resource and index configuration",
            });
        }
        for position in [manifest.index_section, manifest.transaction_section].into_iter().flatten() {
            validate_section(&bytes, position, manifest_offset)?;
        }
        if !manifest.base_paths.is_empty() || manifest.index_section.is_some() || manifest.version_aux_data != 0 {
            return Err(LanceManifestError::UnsupportedFormat {
                reason: "Vector manifests cannot contain external bases or unsupported auxiliary sections",
            });
        }
        let inline_transaction = manifest
            .transaction_section
            .map(|position| {
                let end = validate_section(&bytes, position, manifest_offset)?;
                let transaction = lance_table::format::pb::Transaction::decode(&bytes[position + 4..end])
                    .map_err(|source| LanceManifestError::Protobuf { source })?;
                super::files::validate_transaction(&transaction, &manifest)?;
                Ok::<_, LanceManifestError>(transaction)
            })
            .transpose()?;
        if !manifest.fragments.is_empty() && inline_transaction.is_none() && manifest.transaction_file.is_none() {
            return Err(LanceManifestError::CorruptManifest {
                reason: "nonempty Dataset manifest has no transaction",
            });
        }
        Ok(Self {
            manifest,
            exact_file: bytes,
            content_identity,
            bound_reference: None,
            inline_transaction,
        })
    }

    pub(super) fn inline_transaction(&self) -> Option<&lance_table::format::pb::Transaction> {
        self.inline_transaction.as_ref()
    }

    pub(super) fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub(super) fn exact_file(&self) -> &Bytes {
        &self.exact_file
    }

    pub(super) fn content_identity(&self) -> &ManifestContentIdentity {
        &self.content_identity
    }
}

pub(super) fn empty_manifest(
    dimension: NonZeroU32,
    version: u64,
    table_metadata: HashMap<String, String>,
) -> Result<Manifest, LanceManifestError> {
    let mut manifest = Manifest::new(
        schema::vector_schema(dimension)?,
        Arc::new(Vec::new()),
        DataStorageFormat::new(ConcreteFileVersion::V2_2),
        HashMap::new(),
    );
    manifest.version = version;
    manifest.table_metadata = table_metadata;
    apply_feature_flags(&mut manifest, false, true)
        .map_err(|source| LanceManifestError::engine("apply manifest features", source))?;
    Ok(manifest)
}

pub(super) async fn encode_manifest(
    manifest: &mut Manifest,
    indices: Option<Vec<IndexMetadata>>,
    transaction: Option<Transaction>,
    writer: ManifestWriter,
) -> Result<Bytes, LanceManifestError> {
    let store = ObjectStore::memory();
    let path = Path::from("candidate.manifest");
    writer(&store, manifest, indices, &path, transaction)
        .await
        .map_err(|source| LanceManifestError::engine("write manifest", source))?;
    store
        .read_one_all(&path)
        .await
        .map_err(|source| LanceManifestError::engine("read encoded manifest", source))
}

fn validate_footer(bytes: &[u8]) -> Result<usize, LanceManifestError> {
    let corrupt = |reason| LanceManifestError::CorruptManifest { reason };
    if bytes.len() < 20 {
        return Err(corrupt("file is shorter than the length prefix and footer"));
    }
    let footer = &bytes[bytes.len() - 16..];
    if &footer[12..] != b"LANC" {
        return Err(corrupt("footer magic is invalid"));
    }
    if footer[8..12] != [0, 0, 2, 0] {
        return Err(LanceManifestError::UnsupportedFormat {
            reason: "manifest footer version must be 0.2",
        });
    }
    let raw_offset = i64::from_le_bytes(footer[..8].try_into().map_err(|_| corrupt("footer offset is incomplete"))?);
    let offset = usize::try_from(raw_offset).map_err(|_| corrupt("manifest offset is negative or exceeds the address space"))?;
    let end = validate_section(bytes, offset, bytes.len() - 16)?;
    if end != bytes.len() - 16 {
        return Err(corrupt("manifest protobuf length does not end at the footer"));
    }
    Ok(offset)
}

fn validate_protobuf(bytes: &[u8], manifest_offset: usize, dimension: NonZeroU32) -> Result<(), LanceManifestError> {
    let message = lance_table::format::pb::Manifest::decode(&bytes[manifest_offset + 4..bytes.len() - 16])
        .map_err(|source| LanceManifestError::Protobuf { source })?;
    let expected = empty_manifest(dimension, 1, HashMap::new())?;
    let expected: lance_table::format::pb::Manifest = (&expected).into();
    // Check the native field representation before the reader reaches Arrow conversions.
    if message.fields != expected.fields || message.schema_metadata != expected.schema_metadata {
        return Err(LanceManifestError::SchemaMismatch {
            reason: "serialized fields or schema metadata differ",
        });
    }
    if message
        .timestamp
        .as_ref()
        .is_some_and(|timestamp| timestamp.seconds < 0 || !(0..1_000_000_000).contains(&timestamp.nanos))
    {
        return Err(LanceManifestError::CorruptManifest {
            reason: "manifest timestamp is invalid",
        });
    }
    for position in [message.index_section, message.transaction_section].into_iter().flatten() {
        let position = usize::try_from(position).map_err(|_| LanceManifestError::CorruptManifest {
            reason: "section offset exceeds the address space",
        })?;
        validate_section(bytes, position, manifest_offset)?;
    }
    super::files::validate_fragment_protobuf(&message.fragments)?;
    if message.fragment_tree.is_some()
        || !message.clustering.is_empty()
        || !message.config.is_empty()
        || !message.base_paths.is_empty()
        || message.index_section.is_some()
        || message.version_aux_data != 0
    {
        return Err(LanceManifestError::UnsupportedFormat {
            reason: "Vector manifests cannot contain external bases or unsupported auxiliary sections",
        });
    }
    Ok(())
}

fn validate_section(bytes: &[u8], offset: usize, boundary: usize) -> Result<usize, LanceManifestError> {
    let corrupt = || LanceManifestError::CorruptManifest {
        reason: "section offset or length exceeds its enclosing boundary",
    };
    let prefix_end = offset.checked_add(4).filter(|end| *end <= boundary).ok_or_else(corrupt)?;
    let prefix = bytes.get(offset..prefix_end).ok_or_else(corrupt)?;
    let length = u32::from_le_bytes(prefix.try_into().map_err(|_| corrupt())?) as usize;
    prefix_end
        .checked_add(length)
        .filter(|end| *end <= boundary)
        .ok_or_else(corrupt)
}
