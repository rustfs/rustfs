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

use std::{collections::HashMap, error::Error, num::NonZeroU32, sync::Arc};

use bytes::Bytes;
use lance::deps::arrow_schema::{DataType, Field, Schema as ArrowSchema};
use lance_core::datatypes::Schema;
use lance_file::version::ConcreteFileVersion;
use lance_io::object_store::ObjectStore;
use lance_table::{
    format::Manifest,
    io::{commit::write_manifest_file_to_path, manifest::read_manifest},
};
use object_store_lance::{ObjectStoreExt, path::Path};
use prost::Message;

use super::{LanceManifestError, manifest::*, schema};

fn dimension() -> NonZeroU32 {
    NonZeroU32::new(3).unwrap()
}

async fn fixture() -> (Manifest, Bytes) {
    let mut manifest = empty_manifest(dimension(), 1, HashMap::new()).unwrap();
    manifest.timestamp_nanos = 1_700_000_000_000_000_000;
    let bytes = encode_manifest(&mut manifest, None, None, write_manifest_file_to_path)
        .await
        .unwrap();
    (manifest, bytes)
}

async fn validate(bytes: Bytes) -> Result<ValidatedLanceManifest, LanceManifestError> {
    let identity = ManifestContentIdentity::from_bytes(&bytes);
    ValidatedLanceManifest::read_and_validate(bytes, &identity, dimension(), 1, &HashMap::new(), 1024 * 1024).await
}

#[tokio::test]
async fn binary_manifest_roundtrip() {
    let (manifest, bytes) = fixture().await;
    assert_eq!(&bytes[bytes.len() - 8..], &[0, 0, 2, 0, b'L', b'A', b'N', b'C']);
    assert!(serde_json::from_slice::<serde_json::Value>(&bytes).is_err());
    let store = ObjectStore::memory();
    let path = Path::from("fixture/1.manifest");
    store.inner.put(&path, bytes.clone().into()).await.unwrap();
    let decoded = read_manifest(&store, &path, Some(bytes.len() as u64)).await.unwrap();
    assert_eq!(decoded, manifest);
    assert_eq!(decoded.data_storage_format.version, ConcreteFileVersion::V2_2);
    let validated = validate(bytes.clone()).await.unwrap();
    assert_eq!(validated.manifest(), &manifest);
    assert_eq!(validated.exact_file(), &bytes);
    assert_eq!(validated.content_identity().content_length(), bytes.len() as u64);
    assert!(lance_arrow::json::is_json_field(ArrowSchema::from(&validated.manifest().schema).field(2)));
}

#[tokio::test]
async fn wrong_schema_rejected() {
    let valid = ArrowSchema::from(&schema::vector_schema(dimension()).unwrap());
    let wrong_fields = [
        vec![
            Field::new("key", DataType::Utf8, true),
            valid.field(1).clone(),
            valid.field(2).clone(),
        ],
        vec![
            valid.field(0).clone(),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 4),
                false,
            ),
            valid.field(2).clone(),
        ],
        vec![
            valid.field(0).clone(),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, false)), 3),
                false,
            ),
            valid.field(2).clone(),
        ],
        vec![
            valid.field(0).clone(),
            valid.field(1).clone(),
            Field::new("metadata", DataType::LargeBinary, true),
        ],
        vec![valid.field(1).clone(), valid.field(0).clone(), valid.field(2).clone()],
    ];
    for fields in wrong_fields {
        let (mut manifest, _) = fixture().await;
        manifest.schema = Schema::try_from(&ArrowSchema::new(fields)).unwrap();
        let bytes = encode_manifest(&mut manifest, None, None, write_manifest_file_to_path)
            .await
            .unwrap();
        assert!(matches!(validate(bytes).await, Err(LanceManifestError::SchemaMismatch { .. })));
    }
}

#[tokio::test]
async fn corrupt_footer_and_length_rejected() {
    let (_, original) = fixture().await;
    for size in [0, 15, 19, original.len() - 1] {
        assert!(matches!(
            validate(original.slice(..size)).await,
            Err(LanceManifestError::CorruptManifest { .. })
        ));
    }
    for offset in [-1_i64, i64::MAX] {
        let mut bytes = original.to_vec();
        let start = bytes.len() - 16;
        bytes[start..start + 8].copy_from_slice(&offset.to_le_bytes());
        assert!(matches!(validate(bytes.into()).await, Err(LanceManifestError::CorruptManifest { .. })));
    }
    let mut wrong_magic = original.to_vec();
    *wrong_magic.last_mut().unwrap() = b'X';
    assert!(matches!(
        validate(wrong_magic.into()).await,
        Err(LanceManifestError::CorruptManifest { .. })
    ));
    let mut wrong_length = original.to_vec();
    wrong_length[..4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        validate(wrong_length.into()).await,
        Err(LanceManifestError::CorruptManifest { .. })
    ));
}

#[tokio::test]
async fn corrupt_protobuf_preserves_decode_source() {
    let (_, original) = fixture().await;
    let mut bytes = original.to_vec();
    bytes[4] = 0x0f;
    let error = validate(bytes.into()).await.unwrap_err();
    assert!(matches!(error, LanceManifestError::Protobuf { .. }));
    assert!(error.source().is_some());
}

#[tokio::test]
async fn unsupported_footer_and_data_version_rejected() {
    let (mut manifest, original) = fixture().await;
    let mut wrong_footer = original.to_vec();
    let index = wrong_footer.len() - 6;
    wrong_footer[index] = 3;
    assert!(matches!(
        validate(wrong_footer.into()).await,
        Err(LanceManifestError::UnsupportedFormat { .. })
    ));
    manifest.data_storage_format.version = ConcreteFileVersion::V2_1;
    let bytes = encode_manifest(&mut manifest, None, None, write_manifest_file_to_path)
        .await
        .unwrap();
    assert!(matches!(validate(bytes).await, Err(LanceManifestError::UnsupportedFormat { .. })));
}

#[tokio::test]
async fn version_and_metadata_mismatch_rejected() {
    let (mut manifest, _) = fixture().await;
    manifest.version = 2;
    let bytes = encode_manifest(&mut manifest, None, None, write_manifest_file_to_path)
        .await
        .unwrap();
    assert!(matches!(validate(bytes).await, Err(LanceManifestError::VersionMismatch { .. })));
    manifest.version = 1;
    manifest.table_metadata.insert("resource".into(), "different".into());
    let bytes = encode_manifest(&mut manifest, None, None, write_manifest_file_to_path)
        .await
        .unwrap();
    assert!(matches!(validate(bytes).await, Err(LanceManifestError::ResourceMismatch { .. })));
}

#[tokio::test]
async fn same_size_candidate_replacement_rejected() {
    let (mut manifest, original) = fixture().await;
    let identity = ManifestContentIdentity::from_bytes(&original);
    manifest.version = 2;
    let replacement = encode_manifest(&mut manifest, None, None, write_manifest_file_to_path)
        .await
        .unwrap();
    assert_eq!(original.len(), replacement.len());
    assert_ne!(original, replacement);
    let result =
        ValidatedLanceManifest::read_and_validate(replacement, &identity, dimension(), 2, &HashMap::new(), 1024 * 1024).await;
    assert!(matches!(result, Err(LanceManifestError::DigestMismatch)));
}

#[tokio::test]
async fn exact_length_and_read_budget_enforced() {
    let (_, bytes) = fixture().await;
    let identity = ManifestContentIdentity::from_bytes(&bytes);
    let result = ValidatedLanceManifest::read_and_validate(
        bytes.slice(..bytes.len() - 1),
        &identity,
        dimension(),
        1,
        &HashMap::new(),
        1024 * 1024,
    )
    .await;
    assert!(matches!(result, Err(LanceManifestError::LengthMismatch { .. })));
    let result = ValidatedLanceManifest::read_and_validate(bytes, &identity, dimension(), 1, &HashMap::new(), 1).await;
    assert!(matches!(result, Err(LanceManifestError::ReadLimitExceeded { .. })));
}

#[tokio::test]
async fn unsupported_features_and_external_paths_rejected() {
    let (mut manifest, _) = fixture().await;
    manifest.reader_feature_flags = 1 << 63;
    let bytes = encode_manifest(&mut manifest, None, None, write_manifest_file_to_path)
        .await
        .unwrap();
    let error = validate(bytes).await.unwrap_err();
    assert!(matches!(error, LanceManifestError::Engine { .. }));
    assert!(error.source().is_some());
    manifest.reader_feature_flags = 0;
    manifest.branch = Some("other".into());
    let bytes = encode_manifest(&mut manifest, None, None, write_manifest_file_to_path)
        .await
        .unwrap();
    assert!(matches!(validate(bytes).await, Err(LanceManifestError::UnsupportedFormat { .. })));
}

#[test]
fn vector_dimension_limits_enforced() {
    assert!(schema::vector_schema(NonZeroU32::new(4096).unwrap()).is_ok());
    assert!(matches!(
        schema::vector_schema(NonZeroU32::new(4097).unwrap()),
        Err(LanceManifestError::SchemaMismatch { .. })
    ));
}

fn replace_manifest_message(original: &[u8], mutate: impl FnOnce(&mut lance_table::format::pb::Manifest)) -> Bytes {
    let footer = &original[original.len() - 16..];
    let offset = usize::try_from(i64::from_le_bytes(footer[..8].try_into().unwrap())).unwrap();
    let mut message = lance_table::format::pb::Manifest::decode(&original[offset + 4..original.len() - 16]).unwrap();
    mutate(&mut message);
    let encoded = message.encode_to_vec();
    let mut bytes = original[..offset].to_vec();
    bytes.extend_from_slice(&u32::try_from(encoded.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(&encoded);
    bytes.extend_from_slice(footer);
    bytes.into()
}

#[tokio::test]
async fn malformed_native_fields_rejected_before_arrow_conversion() {
    let (_, original) = fixture().await;
    for logical_type in [
        "list",
        "fixed_size_list:struct:invalid",
        "fixed_size_list:float:0",
        "unknown_type",
    ] {
        let bytes = replace_manifest_message(&original, |message| message.fields[1].logical_type = logical_type.into());
        assert!(matches!(validate(bytes).await, Err(LanceManifestError::SchemaMismatch { .. })));
    }
}

#[tokio::test]
async fn negative_timestamp_rejected_before_manifest_conversion() {
    let (_, original) = fixture().await;
    let bytes = replace_manifest_message(&original, |message| message.timestamp.as_mut().unwrap().seconds = -1);
    assert!(matches!(validate(bytes).await, Err(LanceManifestError::CorruptManifest { .. })));
}

#[tokio::test]
async fn auxiliary_section_outside_manifest_rejected() {
    let (_, original) = fixture().await;
    let bytes = replace_manifest_message(&original, |message| message.index_section = Some(u64::MAX));
    assert!(matches!(validate(bytes).await, Err(LanceManifestError::CorruptManifest { .. })));
}

#[tokio::test]
async fn lance_primitive_list_retains_outer_nullability() {
    let (manifest, _) = fixture().await;
    let arrow = ArrowSchema::from(&manifest.schema);
    assert!(!arrow.field(0).is_nullable());
    assert!(!arrow.field(1).is_nullable());
    let DataType::FixedSizeList(item, size) = arrow.field(1).data_type() else {
        panic!("vector must be a fixed-size list");
    };
    assert_eq!(*size, 3);
    assert_eq!(item.data_type(), &DataType::Float32);
    assert!(item.is_nullable());
}

#[derive(Default, Clone)]
struct TestManagedObjects {
    objects: Arc<std::sync::Mutex<HashMap<String, crate::table_catalog::TableCatalogObject>>>,
    gets: Arc<std::sync::atomic::AtomicUsize>,
    deny_create: Arc<std::sync::atomic::AtomicBool>,
    ranges: Arc<std::sync::Mutex<Vec<Option<std::ops::Range<u64>>>>>,
}

impl TestManagedObjects {
    fn metadata(object: &crate::table_catalog::TableCatalogObject) -> crate::table_catalog::TableCatalogObjectMetadata {
        crate::table_catalog::TableCatalogObjectMetadata {
            body_revision: object.body_revision,
            etag: object.etag.clone(),
            mod_time: object.mod_time,
            size: object.data.len() as u64,
        }
    }
}

#[async_trait::async_trait]
impl super::ManagedManifestObjects for TestManagedObjects {
    async fn create_multipart(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
    ) -> Result<Arc<dyn super::ManagedDatasetMultipart>, LanceManifestError> {
        access.check(key.as_str(), crate::storage_api::vector::ManagedObjectOperation::CreateMultipart)?;
        Ok(Arc::new(TestDatasetMultipart {
            objects: Arc::new(self.clone()),
            access: access.clone(),
            key: key.clone(),
            state: tokio::sync::Mutex::new(TestDatasetMultipartState::default()),
        }))
    }

    async fn create_only(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
        content: Bytes,
    ) -> Result<crate::table_catalog::TableCatalogObjectMetadata, LanceManifestError> {
        access.check(key.as_str(), crate::storage_api::vector::ManagedObjectOperation::CreateOnly)?;
        if self.deny_create.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(LanceManifestError::Storage {
                operation: "test create denial",
                source: Box::new(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
            });
        }
        let mut objects = self.objects.lock().unwrap();
        if objects.contains_key(key.as_str()) {
            return Err(LanceManifestError::CandidateAlreadyExists {
                key: key.as_str().into(),
            });
        }
        let object = crate::table_catalog::TableCatalogObject {
            data: content.to_vec(),
            body_revision: Some(uuid::Uuid::new_v4()),
            etag: Some("test-etag".into()),
            mod_time: None,
        };
        let metadata = Self::metadata(&object);
        objects.insert(key.as_str().into(), object);
        Ok(metadata)
    }

    async fn get(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
        range: Option<std::ops::Range<u64>>,
        expected_body_revision: Option<uuid::Uuid>,
    ) -> Result<crate::table_catalog::TableCatalogObject, LanceManifestError> {
        access.check(key.as_str(), crate::storage_api::vector::ManagedObjectOperation::Get)?;
        self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.ranges.lock().unwrap().push(range.clone());
        let objects = self.objects.lock().unwrap();
        let object = objects.get(key.as_str()).ok_or_else(|| LanceManifestError::ObjectNotFound {
            key: key.as_str().into(),
        })?;
        if expected_body_revision.is_some() && object.body_revision != expected_body_revision {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "test body revision changed",
            });
        }
        Ok(crate::table_catalog::TableCatalogObject {
            data: range.map_or_else(
                || object.data.clone(),
                |range| object.data[range.start as usize..range.end as usize].to_vec(),
            ),
            body_revision: object.body_revision,
            etag: object.etag.clone(),
            mod_time: object.mod_time,
        })
    }

    async fn head(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
    ) -> Result<crate::table_catalog::TableCatalogObjectMetadata, LanceManifestError> {
        access.check(key.as_str(), crate::storage_api::vector::ManagedObjectOperation::Head)?;
        let objects = self.objects.lock().unwrap();
        let object = objects.get(key.as_str()).ok_or_else(|| LanceManifestError::ObjectNotFound {
            key: key.as_str().into(),
        })?;
        Ok(Self::metadata(object))
    }
}

fn managed_access() -> crate::storage_api::vector::ManagedObjectAccess {
    use crate::storage_api::vector::*;
    let bucket = BucketHandle::try_from_parts(
        BucketKind::Vector,
        AccountNamespaceId::empty(),
        "us-east-1".into(),
        BucketPublicName::try_new("vectors".into()).unwrap(),
        StorageBucketId::try_new("internal-vectors".into()).unwrap(),
        BucketIncarnation::try_new(uuid::Uuid::from_u128(1)).unwrap(),
    )
    .unwrap();
    ManagedObjectAccess::try_from_parts(
        bucket,
        ManagedTableId::try_new(uuid::Uuid::from_u128(2).to_string()).unwrap(),
        ".rustfs-table/00000000-0000-0000-0000-000000000002/dataset/".into(),
        [
            ManagedObjectOperation::CreateOnly,
            ManagedObjectOperation::Get,
            ManagedObjectOperation::Head,
            ManagedObjectOperation::Range,
        ]
        .into(),
    )
    .unwrap()
}

fn io_limits() -> super::ManifestIoLimits {
    super::ManifestIoLimits {
        max_manifest_bytes: std::num::NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        max_concurrent_reads: std::num::NonZeroUsize::new(8).unwrap(),
    }
}

fn candidate_key() -> crate::table_catalog::CatalogObjectKey {
    crate::table_catalog::CatalogObjectKey::try_new(format!(
        "{}_versions/{}/1.manifest",
        managed_access().allowed_prefix(),
        uuid::Uuid::from_u128(3)
    ))
    .unwrap()
}

#[tokio::test]
async fn managed_candidate_create_only_and_exact_read() {
    let objects = Arc::new(TestManagedObjects::default());
    let store = super::object_store::ManagedObjectStore::new(objects.clone(), managed_access(), io_limits());
    let (_, bytes) = fixture().await;
    let key = candidate_key();
    let reference = store.create_candidate(&key, bytes.clone()).await.unwrap();
    assert!(reference.body_revision().is_some());
    assert_eq!(reference.content_length(), bytes.len() as u64);
    let object = store.read_exact(&reference).await.unwrap();
    assert_eq!(object.data, bytes);
    let duplicate = store.create_candidate(&key, bytes).await;
    assert!(matches!(duplicate, Err(LanceManifestError::CandidateAlreadyExists { .. })));
}

#[tokio::test]
async fn managed_same_size_replacement_and_revision_change_rejected() {
    let objects = Arc::new(TestManagedObjects::default());
    let store = super::object_store::ManagedObjectStore::new(objects.clone(), managed_access(), io_limits());
    let (mut manifest, bytes) = fixture().await;
    let key = candidate_key();
    let reference = store.create_candidate(&key, bytes.clone()).await.unwrap();
    manifest.version = 2;
    let replacement = encode_manifest(&mut manifest, None, None, write_manifest_file_to_path)
        .await
        .unwrap();
    assert_eq!(bytes.len(), replacement.len());
    objects.objects.lock().unwrap().get_mut(key.as_str()).unwrap().data = replacement.to_vec();
    assert!(matches!(store.read_exact(&reference).await, Err(LanceManifestError::DigestMismatch)));
    {
        let mut stored = objects.objects.lock().unwrap();
        let object = stored.get_mut(key.as_str()).unwrap();
        object.data = bytes.to_vec();
        object.body_revision = Some(uuid::Uuid::from_u128(99));
    }
    assert!(matches!(
        store.read_exact(&reference).await,
        Err(LanceManifestError::ReferenceMismatch { .. })
    ));
}

#[tokio::test]
async fn managed_budget_and_scope_reject_before_get() {
    let objects = Arc::new(TestManagedObjects::default());
    let store = super::object_store::ManagedObjectStore::new(objects.clone(), managed_access(), io_limits());
    let (_, bytes) = fixture().await;
    let reference = store.create_candidate(&candidate_key(), bytes).await.unwrap();
    objects.gets.store(0, std::sync::atomic::Ordering::SeqCst);
    let limits = super::ManifestIoLimits {
        max_manifest_bytes: std::num::NonZeroU64::new(1).unwrap(),
        ..io_limits()
    };
    let limited = super::object_store::ManagedObjectStore::new(objects.clone(), managed_access(), limits);
    assert!(matches!(
        limited.read_exact(&reference).await,
        Err(LanceManifestError::ReadLimitExceeded { .. })
    ));
    let mut access = managed_access();
    access = crate::storage_api::vector::ManagedObjectAccess::try_from_parts(
        access.bucket().clone(),
        access.table_id().clone(),
        "another/prefix/".into(),
        [crate::storage_api::vector::ManagedObjectOperation::Get].into(),
    )
    .unwrap();
    let restricted = super::object_store::ManagedObjectStore::new(objects.clone(), access, io_limits());
    assert!(matches!(
        restricted.read_exact(&reference).await,
        Err(LanceManifestError::ManagedAccessDenied(_))
    ));
    assert_eq!(objects.gets.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn managed_object_store_ranges_use_exact_manifest_reference() {
    let objects = Arc::new(TestManagedObjects::default());
    let store = super::object_store::ManagedObjectStore::new(objects, managed_access(), io_limits());
    let (_, bytes) = fixture().await;
    let key = candidate_key();
    store.create_candidate(&key, bytes.clone()).await.unwrap();
    let path = Path::from(key.as_str());
    let range = store.get_range(&path, 4..16).await.unwrap();
    assert_eq!(range, bytes.slice(4..16));
    assert_eq!(store.head(&path).await.unwrap().size, bytes.len() as u64);
    let unknown = Path::from(format!("{}_versions/999.manifest", managed_access().allowed_prefix()));
    assert!(store.get(&unknown).await.is_err());
}

#[tokio::test]
async fn managed_manifest_requires_non_nil_body_revision() {
    use crate::table_catalog::{CatalogMetadataRef, TableCatalogObject};
    let objects = Arc::new(TestManagedObjects::default());
    let store = super::object_store::ManagedObjectStore::new(objects.clone(), managed_access(), io_limits());
    let (_, bytes) = fixture().await;
    let object = TableCatalogObject {
        data: bytes.to_vec(),
        body_revision: None,
        etag: None,
        mod_time: None,
    };
    let reference = CatalogMetadataRef::from_read(candidate_key(), &object).unwrap();
    assert!(matches!(
        store.read_exact(&reference).await,
        Err(LanceManifestError::ReferenceMismatch { .. })
    ));
    assert_eq!(objects.gets.load(std::sync::atomic::Ordering::SeqCst), 0);
    let nil = TableCatalogObject {
        body_revision: Some(uuid::Uuid::nil()),
        ..object
    };
    assert!(CatalogMetadataRef::from_read(candidate_key(), &nil).is_err());
}

fn index_spec() -> super::schema::VectorIndexManifestSpec {
    super::schema::VectorIndexManifestSpec {
        schema_version: 1,
        dimension: dimension(),
        distance_metric: super::schema::VectorDistanceMetric::Cosine,
        non_filterable_metadata_keys: ["private".to_string()].into(),
        encryption_descriptor_sha256: crate::table_catalog::CatalogContentSha256::digest(b"AES256"),
    }
}

fn resource_identity() -> crate::table_catalog::CatalogResourceIdentity {
    let access = managed_access();
    crate::table_catalog::CatalogResourceIdentity::try_new(
        access.bucket().clone(),
        access.table_id().clone(),
        crate::table_catalog::CatalogFormat::Lance,
    )
    .unwrap()
}

#[test]
fn manifest_binding_rejects_duplicate_unknown_and_mismatched_configuration() {
    let resource = resource_identity();
    let index = index_spec();
    let metadata = manifest_metadata(&resource, &index).unwrap();
    let binding = metadata.get(VECTOR_BINDING_KEY).unwrap();
    assert!(VectorManifestBindingV1::decode(binding, &resource, &index).is_ok());
    let duplicate = binding.replacen("{", "{\"dimension\":3,", 1);
    assert!(VectorManifestBindingV1::decode(&duplicate, &resource, &index).is_err());
    let unknown = binding.replacen("{", "{\"unexpected\":true,", 1);
    assert!(VectorManifestBindingV1::decode(&unknown, &resource, &index).is_err());
    let mut missing: serde_json::Value = serde_json::from_str(binding).unwrap();
    missing.as_object_mut().unwrap().remove("encryption_descriptor_sha256");
    assert!(VectorManifestBindingV1::decode(&missing.to_string(), &resource, &index).is_err());
    for (key, value) in [
        ("dimension", serde_json::json!(4)),
        ("encoding_version", serde_json::json!(2)),
        ("bucket_incarnation", serde_json::json!(uuid::Uuid::nil())),
        ("distance_metric", serde_json::json!("dot")),
        ("encryption_descriptor_sha256", serde_json::json!("F".repeat(64))),
        ("non_filterable_metadata_keys", serde_json::json!(["private", "private"])),
    ] {
        let mut modified: serde_json::Value = serde_json::from_str(binding).unwrap();
        modified[key] = value;
        assert!(
            VectorManifestBindingV1::decode(&modified.to_string(), &resource, &index).is_err(),
            "{key}"
        );
    }
}

#[tokio::test]
async fn descriptor_rejects_missing_optional_fields_and_reference_substitution() {
    let resource = resource_identity();
    let index = index_spec();
    let metadata = manifest_metadata(&resource, &index).unwrap();
    let mut manifest = empty_manifest(dimension(), 1, metadata.clone()).unwrap();
    let bytes = encode_manifest(&mut manifest, None, None, write_manifest_file_to_path)
        .await
        .unwrap();
    let object = crate::table_catalog::TableCatalogObject {
        data: bytes.to_vec(),
        body_revision: Some(uuid::Uuid::from_u128(10)),
        etag: None,
        mod_time: None,
    };
    let reference = crate::table_catalog::CatalogMetadataRef::from_read(candidate_key(), &object).unwrap();
    let validated =
        ValidatedLanceManifest::read_bound(bytes, &reference, &resource, &index, 1, io_limits().max_manifest_bytes.get())
            .await
            .unwrap();
    let descriptor = validated.format_descriptor(&resource, &index, &reference).unwrap();
    assert!(LanceManifestDescriptorV1::decode(&descriptor, &resource, &index).is_ok());
    let other_key = crate::table_catalog::CatalogObjectKey::try_new(format!(
        "{}_versions/{}/1.manifest",
        resource.dataset_prefix(),
        uuid::Uuid::from_u128(11)
    ))
    .unwrap();
    let substituted = crate::table_catalog::CatalogMetadataRef::from_read(other_key, &object).unwrap();
    assert!(matches!(
        validated.format_descriptor(&resource, &index, &substituted),
        Err(LanceManifestError::ReferenceMismatch { .. })
    ));
    for missing in ["body_revision", "etag"] {
        let mut payload: serde_json::Value = serde_json::from_slice(descriptor.payload()).unwrap();
        payload["metadata"].as_object_mut().unwrap().remove(missing);
        let changed = crate::table_catalog::CatalogFormatDescriptor::try_new(
            1,
            crate::table_catalog::CatalogFormat::Lance,
            1,
            reference.clone(),
            serde_json::to_vec(&payload).unwrap(),
        )
        .unwrap();
        assert!(LanceManifestDescriptorV1::decode(&changed, &resource, &index).is_err());
    }
    for (field, value) in [
        ("descriptor_version", serde_json::json!(2)),
        ("library_version", serde_json::json!("15.0.0")),
        ("footer_minor", serde_json::json!(3)),
        ("data_storage_version", serde_json::json!("2.1")),
        ("unknown_field", serde_json::json!(true)),
    ] {
        let mut payload: serde_json::Value = serde_json::from_slice(descriptor.payload()).unwrap();
        payload[field] = value;
        let changed = crate::table_catalog::CatalogFormatDescriptor::try_new(
            1,
            crate::table_catalog::CatalogFormat::Lance,
            1,
            reference.clone(),
            serde_json::to_vec(&payload).unwrap(),
        )
        .unwrap();
        assert!(LanceManifestDescriptorV1::decode(&changed, &resource, &index).is_err(), "{field}");
    }
    let duplicate = String::from_utf8(descriptor.payload().to_vec())
        .unwrap()
        .replacen("{", "{\"descriptor_version\":1,", 1);
    let changed = crate::table_catalog::CatalogFormatDescriptor::try_new(
        1,
        crate::table_catalog::CatalogFormat::Lance,
        1,
        reference.clone(),
        duplicate.into_bytes(),
    )
    .unwrap();
    assert!(LanceManifestDescriptorV1::decode(&changed, &resource, &index).is_err());
    let mut payload: serde_json::Value = serde_json::from_slice(descriptor.payload()).unwrap();
    payload["metadata"]["content_sha256"] = serde_json::json!("0".repeat(64));
    let changed = crate::table_catalog::CatalogFormatDescriptor::try_new(
        1,
        crate::table_catalog::CatalogFormat::Lance,
        1,
        reference,
        serde_json::to_vec(&payload).unwrap(),
    )
    .unwrap();
    assert!(LanceManifestDescriptorV1::decode(&changed, &resource, &index).is_err());
}

#[derive(Clone)]
struct CatalogManagedObjects(crate::table_catalog::test_support::TestCatalogObjectBackend);

#[async_trait::async_trait]
impl super::ManagedManifestObjects for CatalogManagedObjects {
    async fn create_multipart(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
    ) -> Result<Arc<dyn super::ManagedDatasetMultipart>, LanceManifestError> {
        access.check(key.as_str(), crate::storage_api::vector::ManagedObjectOperation::CreateMultipart)?;
        Ok(Arc::new(TestDatasetMultipart {
            objects: Arc::new(self.clone()),
            access: access.clone(),
            key: key.clone(),
            state: tokio::sync::Mutex::new(TestDatasetMultipartState::default()),
        }))
    }

    async fn create_only(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
        content: Bytes,
    ) -> Result<crate::table_catalog::TableCatalogObjectMetadata, LanceManifestError> {
        use crate::table_catalog::TableCatalogObjectBackend;
        access.check(key.as_str(), crate::storage_api::vector::ManagedObjectOperation::CreateOnly)?;
        self.0.revalidate_bucket_handle(access.bucket()).await?;
        self.0
            .put_object(
                access.bucket().storage_bucket_id().as_str(),
                key.as_str(),
                content.to_vec(),
                crate::table_catalog::TableCatalogPutPrecondition::IfAbsent,
            )
            .await?;
        self.head(access, key).await
    }

    async fn get(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
        range: Option<std::ops::Range<u64>>,
        expected_body_revision: Option<uuid::Uuid>,
    ) -> Result<crate::table_catalog::TableCatalogObject, LanceManifestError> {
        use crate::table_catalog::TableCatalogObjectBackend;
        access.check(key.as_str(), crate::storage_api::vector::ManagedObjectOperation::Get)?;
        self.0.revalidate_bucket_handle(access.bucket()).await?;
        let mut object = self
            .0
            .read_object(access.bucket().storage_bucket_id().as_str(), key.as_str())
            .await?
            .ok_or_else(|| LanceManifestError::ObjectNotFound {
                key: key.as_str().to_string(),
            })?;
        if expected_body_revision.is_some() && object.body_revision != expected_body_revision {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "test backend returned a different body revision",
            });
        }
        if let Some(range) = range {
            object.data = object.data[range.start as usize..range.end as usize].to_vec();
        }
        Ok(object)
    }

    async fn head(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
    ) -> Result<crate::table_catalog::TableCatalogObjectMetadata, LanceManifestError> {
        use crate::table_catalog::TableCatalogObjectBackend;
        access.check(key.as_str(), crate::storage_api::vector::ManagedObjectOperation::Head)?;
        self.0.revalidate_bucket_handle(access.bucket()).await?;
        self.0
            .object_metadata(access.bucket().storage_bucket_id().as_str(), key.as_str())
            .await?
            .ok_or_else(|| LanceManifestError::ObjectNotFound {
                key: key.as_str().to_string(),
            })
    }
}

struct CatalogFixture {
    backend: crate::table_catalog::test_support::TestCatalogObjectBackend,
    store: Arc<crate::table_catalog::ObjectTableCatalogStore<crate::table_catalog::test_support::TestCatalogObjectBackend>>,
    adapter: super::LanceManifestAdapter<crate::table_catalog::test_support::TestCatalogObjectBackend>,
}

impl CatalogFixture {
    async fn request(&self, attempt: u128) -> super::ManifestPrepareRequest {
        use crate::table_catalog::*;
        let access = managed_access();
        let head = self
            .store
            .read_catalog_head(
                access.bucket(),
                &Namespace::parse("indexes").unwrap(),
                &CatalogName::parse("index.v1").unwrap(),
            )
            .await
            .unwrap();
        super::ManifestPrepareRequest {
            parts: CatalogCommitParts {
                resource_identity: resource_identity(),
                namespace: Namespace::parse("indexes").unwrap(),
                name: CatalogName::parse("index.v1").unwrap(),
                expected_head: head.expected_head(),
                commit_id: CatalogCommitId::try_new(uuid::Uuid::from_u128(attempt).to_string()).unwrap(),
                operation: "update".into(),
                idempotency_key: None,
                writer: None,
            },
            index: index_spec(),
        }
    }
}

async fn catalog_fixture() -> CatalogFixture {
    use crate::table_catalog::*;
    let backend = test_support::TestCatalogObjectBackend::default();
    let access = managed_access();
    let bucket_id = access.bucket().storage_bucket_id().as_str();
    backend
        .state
        .lock()
        .await
        .bucket_handles
        .insert(bucket_id.into(), access.bucket().clone());
    let store = Arc::new(
        ObjectTableCatalogStore::for_vector_scope(backend.clone(), access.bucket().clone())
            .await
            .unwrap(),
    );
    store
        .put_table_bucket(TableBucketEntry {
            vector_bucket_binding: Some(VectorBucketBindingV1::from_handle(access.bucket()).unwrap()),
            version: 2,
            table_bucket: bucket_id.into(),
            catalog_type: "s3vectors".into(),
            warehouse_root: format!("s3://{bucket_id}/"),
            state: TableCatalogEntryState::Active,
            properties: Default::default(),
            active_rename_id: None,
            created_at: None,
            updated_at: None,
        })
        .await
        .unwrap();
    let namespace = Namespace::parse("indexes").unwrap();
    store
        .create_namespace(NamespaceEntry {
            version: 1,
            table_bucket: bucket_id.into(),
            namespace: namespace.public_name(),
            namespace_id: namespace.storage_id(),
            state: TableCatalogEntryState::Active,
            properties: Default::default(),
            created_at: None,
            updated_at: None,
        })
        .await
        .unwrap();
    let name = CatalogName::parse("index.v1").unwrap();
    let adapter = super::LanceManifestAdapter::new(
        store.clone(),
        Arc::new(CatalogManagedObjects(backend.clone())),
        access.clone(),
        name.clone(),
        index_spec(),
        io_limits(),
    )
    .unwrap();
    let resource = resource_identity();
    let mut entry = TableEntry {
        vector_head_binding: None,
        vector_index_definition: None,
        version: 2,
        table_bucket: bucket_id.into(),
        namespace: "indexes".into(),
        table: name.as_str().into(),
        table_id: access.table_id().as_str().into(),
        table_uuid: access.table_id().as_str().into(),
        format: "lance".into(),
        format_version: 1,
        warehouse_location: format!("s3://{bucket_id}/{}", resource.dataset_prefix().trim_end_matches('/')),
        metadata_location: String::new(),
        version_token: "initial-token".into(),
        generation: 1,
        state: TableCatalogEntryState::Active,
        properties: Default::default(),
        created_at: None,
        updated_at: None,
    };
    let request = super::ManifestPrepareRequest {
        parts: CatalogCommitParts {
            resource_identity: resource.clone(),
            namespace,
            name,
            expected_head: CatalogExpectedHead::from_entry(&entry, None, None).unwrap(),
            commit_id: CatalogCommitId::try_new(uuid::Uuid::from_u128(3).to_string()).unwrap(),
            operation: "create".into(),
            idempotency_key: None,
            writer: None,
        },
        index: index_spec(),
    };
    let prepared = adapter.prepare_empty_index(&access, request).await.unwrap();
    entry.metadata_location = prepared.candidate_metadata().object_key().as_str().into();
    entry.vector_head_binding =
        Some(VectorHeadBindingV1::try_new(&resource, prepared.candidate_metadata(), prepared.format_descriptor()).unwrap());
    store.create_table(entry).await.unwrap();
    CatalogFixture { backend, store, adapter }
}

#[tokio::test]
async fn catalog_prepared_lance_commit_publishes_real_manifest() {
    use crate::table_catalog::TableCatalogObjectBackend;
    let fixture = catalog_fixture().await;
    let access = managed_access();
    let initial = fixture.adapter.load_current(&access, &resource_identity()).await.unwrap();
    assert_eq!(initial.manifest_version, 1);
    let committed = fixture
        .adapter
        .commit_empty_index(&access, fixture.request(4).await)
        .await
        .unwrap();
    assert_eq!(committed.manifest_version, 2);
    assert_eq!(committed.index, index_spec());
    let stored = fixture
        .backend
        .read_object(access.bucket().storage_bucket_id().as_str(), committed.reference.object_key().as_str())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.body_revision, committed.reference.body_revision().map(|value| value.as_uuid()));
    assert_eq!(stored.data.len() as u64, committed.reference.content_length());
    let validated = ValidatedLanceManifest::read_bound(
        stored.data.into(),
        &committed.reference,
        &resource_identity(),
        &index_spec(),
        2,
        io_limits().max_manifest_bytes.get(),
    )
    .await
    .unwrap();
    assert_eq!(validated.manifest().version, 2);
    assert_eq!(
        fixture
            .adapter
            .load_committed_version(&access, &resource_identity(), std::num::NonZeroU64::new(1).unwrap())
            .await
            .unwrap()
            .reference,
        initial.reference
    );
}

#[tokio::test]
async fn catalog_vector_history_ignores_uncommitted_manifest() {
    use crate::table_catalog::TableCatalogObjectBackend;
    use futures::TryStreamExt;
    use lance_table::io::commit::{CommitHandler, ManifestNamingScheme};
    let fixture = catalog_fixture().await;
    let access = managed_access();
    fixture
        .adapter
        .commit_empty_index(&access, fixture.request(4).await)
        .await
        .unwrap();
    let mut unpublished =
        empty_manifest(dimension(), 999, manifest_metadata(&resource_identity(), &index_spec()).unwrap()).unwrap();
    let bytes = encode_manifest(&mut unpublished, None, None, write_manifest_file_to_path)
        .await
        .unwrap();
    let ordinary_path = format!("{}_versions/999.manifest", access.allowed_prefix());
    let candidate_path = format!("{}_versions/{}/999.manifest", access.allowed_prefix(), uuid::Uuid::from_u128(999));
    for path in [&ordinary_path, &candidate_path] {
        fixture
            .backend
            .seed_object(access.bucket().storage_bucket_id().as_str(), path, bytes.to_vec())
            .await;
    }
    let listed = fixture
        .backend
        .list_objects(access.bucket().storage_bucket_id().as_str(), access.allowed_prefix())
        .await
        .unwrap();
    assert!(listed.contains(&ordinary_path) && listed.contains(&candidate_path));
    let handler = fixture.adapter.handler(None);
    let base = Path::from(access.allowed_prefix().trim_end_matches('/'));
    let store = ObjectStore::memory();
    assert_eq!(handler.resolve_latest_location(&base, &store).await.unwrap().version, 2);
    assert!(
        handler
            .resolve_version_location(&base, 999, store.inner.as_ref())
            .await
            .is_err()
    );
    assert!(
        !handler
            .version_exists(&base, 999, store.inner.as_ref(), ManifestNamingScheme::V1)
            .await
            .unwrap()
    );
    assert!(handler.resolve_identity(&base, &store, 999).await.unwrap().is_none());
    let versions: Vec<_> = handler
        .list_manifest_locations(&base, &store, true)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(versions.iter().map(|location| location.version).collect::<Vec<_>>(), vec![2, 1]);
    let since: Vec<_> = handler
        .list_manifest_locations_since(&base, &store, 1)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(since.iter().map(|location| location.version).collect::<Vec<_>>(), vec![2]);
    let detached: Vec<_> = handler
        .list_detached_manifest_locations(&base, &store)
        .try_collect()
        .await
        .unwrap();
    assert!(detached.is_empty());
}

#[tokio::test]
async fn catalog_prepared_manifest_rejects_stale_head_and_replacement() {
    use crate::table_catalog::{TableCatalogStore, TableCatalogStoreError};
    let fixture = catalog_fixture().await;
    let access = managed_access();
    let first = fixture
        .adapter
        .prepare_empty_index(&access, fixture.request(4).await)
        .await
        .unwrap();
    let second = fixture
        .adapter
        .prepare_empty_index(&access, fixture.request(5).await)
        .await
        .unwrap();
    fixture.store.commit_prepared(first).await.unwrap();
    assert!(matches!(
        fixture.store.commit_prepared(second).await,
        Err(TableCatalogStoreError::Conflict(_))
    ));
    let current = fixture.adapter.load_current(&access, &resource_identity()).await.unwrap();
    assert_eq!(current.manifest_version, 2);
    let third = fixture
        .adapter
        .prepare_empty_index(&access, fixture.request(6).await)
        .await
        .unwrap();
    let key = third.candidate_metadata().object_key().as_str();
    let mut replacement =
        empty_manifest(dimension(), 4, manifest_metadata(&resource_identity(), &index_spec()).unwrap()).unwrap();
    let bytes = encode_manifest(&mut replacement, None, None, write_manifest_file_to_path)
        .await
        .unwrap();
    assert_eq!(bytes.len() as u64, third.candidate_metadata().content_length());
    fixture
        .backend
        .seed_object(access.bucket().storage_bucket_id().as_str(), key, bytes.to_vec())
        .await;
    assert!(fixture.store.commit_prepared(third).await.is_err());
    assert_eq!(
        fixture
            .adapter
            .load_current(&access, &resource_identity())
            .await
            .unwrap()
            .reference,
        current.reference
    );
}

fn timestamped_manifest_writer<'a>(
    store: &'a ObjectStore,
    manifest: &'a mut Manifest,
    indices: Option<Vec<lance_table::format::IndexMetadata>>,
    path: &'a Path,
    transaction: Option<lance_table::format::Transaction>,
) -> futures::future::BoxFuture<'a, lance_core::Result<lance_io::object_writer::WriteResult>> {
    manifest.timestamp_nanos = 42;
    write_manifest_file_to_path(store, manifest, indices, path, transaction)
}

#[tokio::test]
async fn commit_after_checks_stable_predecessor_and_uses_writer() {
    use lance_table::io::commit::{CommitHandler, ManifestNamingScheme, PredecessorIdentity, write_manifest_file_to_path};
    let fixture = catalog_fixture().await;
    let access = managed_access();
    let base = Path::from(access.allowed_prefix().trim_end_matches('/'));
    let store = ObjectStore::memory();
    let handler = fixture.adapter.handler(Some(fixture.request(4).await));
    let predecessor = handler.resolve_latest_identity(&base, &store).await.unwrap().unwrap();
    let wrong = PredecessorIdentity {
        version: predecessor.version,
        identity: "different-resource".into(),
    };
    let mut manifest = empty_manifest(dimension(), 2, manifest_metadata(&resource_identity(), &index_spec()).unwrap()).unwrap();
    let result = handler
        .commit_after(
            &mut manifest,
            None,
            &base,
            &store,
            write_manifest_file_to_path,
            ManifestNamingScheme::V1,
            None,
            &wrong,
        )
        .await;
    assert!(matches!(
        result,
        Err(lance_table::io::commit::CommitError::OtherError(
            lance_core::Error::PrerequisiteFailed { .. }
        ))
    ));
    handler
        .commit_after(
            &mut manifest,
            None,
            &base,
            &store,
            timestamped_manifest_writer,
            ManifestNamingScheme::V1,
            None,
            &predecessor,
        )
        .await
        .unwrap();
    assert_eq!(manifest.timestamp_nanos, 42);
    assert_eq!(handler.resolve_identity(&base, &store, 1).await.unwrap(), Some(predecessor.clone()));
    assert!(
        handler
            .commit_after(
                &mut manifest,
                None,
                &base,
                &store,
                write_manifest_file_to_path,
                ManifestNamingScheme::V1,
                None,
                &predecessor
            )
            .await
            .is_err()
    );
    assert!(handler.forget_version(&base, 1, &predecessor.identity).await.is_err());
    assert!(handler.delete(&base).await.is_err());
}

#[tokio::test]
async fn same_attempt_reuses_exact_candidate_and_catalog_result() {
    use crate::table_catalog::TableCatalogStore;
    let fixture = catalog_fixture().await;
    let access = managed_access();
    let first = fixture
        .adapter
        .prepare_empty_index(&access, fixture.request(4).await)
        .await
        .unwrap();
    let repeated = fixture
        .adapter
        .prepare_empty_index(&access, fixture.request(4).await)
        .await
        .unwrap();
    assert_eq!(first.candidate_metadata(), repeated.candidate_metadata());
    assert_eq!(first.request_digest(), repeated.request_digest());
    let committed = fixture.store.commit_prepared(first).await.unwrap();
    let retried = fixture.store.commit_prepared(repeated).await.unwrap();
    assert_eq!(committed.table, retried.table);
    assert_eq!(
        fixture
            .adapter
            .load_current(&access, &resource_identity())
            .await
            .unwrap()
            .manifest_version,
        2
    );
}

#[test]
fn manifest_path_requires_attempt_uuid_and_exact_version() {
    use crate::table_catalog::{CatalogMetadataRef, CatalogObjectKey, TableCatalogObject};
    let resource = resource_identity();
    for suffix in [
        "1.manifest",
        "not-a-uuid/1.manifest",
        "00000000-0000-0000-0000-000000000003/01.manifest",
        "00000000-0000-0000-0000-000000000003/2.manifest",
    ] {
        let key = CatalogObjectKey::try_new(format!("{}_versions/{suffix}", resource.dataset_prefix())).unwrap();
        let reference = CatalogMetadataRef::from_read(
            key,
            &TableCatalogObject {
                data: Vec::new(),
                body_revision: Some(uuid::Uuid::from_u128(5)),
                etag: None,
                mod_time: None,
            },
        )
        .unwrap();
        assert!(validate_manifest_path(&resource, &reference, 1, None).is_err());
    }
}

#[tokio::test]
async fn create_storage_denial_preserves_source_without_candidate_readback() {
    let objects = Arc::new(TestManagedObjects::default());
    let store = super::object_store::ManagedObjectStore::new(objects.clone(), managed_access(), io_limits());
    let (_, bytes) = fixture().await;
    store.create_candidate(&candidate_key(), bytes.clone()).await.unwrap();
    objects.deny_create.store(true, std::sync::atomic::Ordering::SeqCst);
    objects.gets.store(0, std::sync::atomic::Ordering::SeqCst);
    let retry = super::object_store::ManagedObjectStore::new(objects.clone(), managed_access(), io_limits());
    let error = retry.prepare_candidate(&candidate_key(), bytes).await.unwrap_err();
    assert!(matches!(error, LanceManifestError::Storage { .. }));
    let source = error.source().unwrap().downcast_ref::<std::io::Error>().unwrap();
    assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(objects.gets.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn catalog_io_errors_preserve_classification_and_source_through_lance() {
    use crate::table_catalog::{CatalogIoErrorKind, TableCatalogStoreError};
    for classification in [
        CatalogIoErrorKind::Denied,
        CatalogIoErrorKind::Conflict,
        CatalogIoErrorKind::Unavailable,
        CatalogIoErrorKind::Internal,
    ] {
        let error = TableCatalogStoreError::Io {
            classification,
            context: "test managed read",
            source: Box::new(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        };
        let result = super::commit::to_commit_error(LanceManifestError::Catalog(error));
        let lance_table::io::commit::CommitError::OtherError(engine_error) = result else {
            panic!("I/O failure must retain its typed source");
        };
        let mut source: &(dyn Error + 'static) = &engine_error;
        let mut retained_kind = None;
        let mut retained_io = None;
        loop {
            if let Some(TableCatalogStoreError::Io { classification, .. }) = source.downcast_ref::<TableCatalogStoreError>() {
                retained_kind = Some(*classification);
            }
            if let Some(io) = source.downcast_ref::<std::io::Error>() {
                retained_io = Some(io.kind());
            }
            let Some(next) = source.source() else {
                break;
            };
            source = next;
        }
        assert_eq!(retained_kind, Some(classification));
        assert_eq!(retained_io, Some(std::io::ErrorKind::PermissionDenied));
    }
}

#[derive(Default)]
struct TestDatasetMultipartState {
    parts: std::collections::BTreeMap<u16, Vec<Bytes>>,
    completed: Option<crate::table_catalog::TableCatalogObjectMetadata>,
    aborted: bool,
}

struct TestDatasetMultipart {
    objects: Arc<dyn super::ManagedManifestObjects>,
    access: crate::storage_api::vector::ManagedObjectAccess,
    key: crate::table_catalog::CatalogObjectKey,
    state: tokio::sync::Mutex<TestDatasetMultipartState>,
}

#[async_trait::async_trait]
impl super::ManagedDatasetMultipart for TestDatasetMultipart {
    async fn put_part(&self, part_number: u16, content: Vec<Bytes>, size: u64) -> Result<(), LanceManifestError> {
        self.access
            .check(self.key.as_str(), crate::storage_api::vector::ManagedObjectOperation::UploadPart)?;
        assert_eq!(content.iter().map(|part| part.len() as u64).sum::<u64>(), size);
        assert!((1..=10_000).contains(&part_number));
        let mut state = self.state.lock().await;
        assert!(!state.aborted && state.completed.is_none());
        assert!(state.parts.insert(part_number, content).is_none());
        Ok(())
    }

    async fn complete(
        &self,
        expected_parts: u16,
    ) -> Result<crate::table_catalog::TableCatalogObjectMetadata, LanceManifestError> {
        self.access
            .check(self.key.as_str(), crate::storage_api::vector::ManagedObjectOperation::CompleteMultipart)?;
        let mut state = self.state.lock().await;
        if let Some(completed) = &state.completed {
            return Ok(completed.clone());
        }
        if state.aborted || state.parts.len() != usize::from(expected_parts) || expected_parts == 0 {
            return Err(LanceManifestError::ReferenceMismatch {
                reason: "multipart fixture is incomplete",
            });
        }
        let mut data = Vec::new();
        for number in 1..=expected_parts {
            let parts = state.parts.get(&number).ok_or(LanceManifestError::ReferenceMismatch {
                reason: "multipart fixture has a missing part",
            })?;
            for part in parts {
                data.extend_from_slice(part);
            }
        }
        let metadata = self.objects.create_only(&self.access, &self.key, Bytes::from(data)).await?;
        state.completed = Some(metadata.clone());
        Ok(metadata)
    }

    async fn abort(&self) -> Result<(), LanceManifestError> {
        self.access
            .check(self.key.as_str(), crate::storage_api::vector::ManagedObjectOperation::AbortMultipart)?;
        let mut state = self.state.lock().await;
        if state.completed.is_none() {
            state.aborted = true;
            state.parts.clear();
        }
        Ok(())
    }
}

fn dataset_access() -> crate::storage_api::vector::ManagedObjectAccess {
    use crate::storage_api::vector::{ManagedObjectAccess, ManagedObjectOperation};
    let access = managed_access();
    ManagedObjectAccess::try_from_parts(
        access.bucket().clone(),
        access.table_id().clone(),
        access.allowed_prefix().to_string(),
        [
            ManagedObjectOperation::CreateOnly,
            ManagedObjectOperation::Get,
            ManagedObjectOperation::Head,
            ManagedObjectOperation::Range,
            ManagedObjectOperation::CreateMultipart,
            ManagedObjectOperation::UploadPart,
            ManagedObjectOperation::CompleteMultipart,
            ManagedObjectOperation::AbortMultipart,
        ]
        .into(),
    )
    .unwrap()
}

fn native_data_path(suffix: u8) -> Path {
    Path::from(format!(
        "{}data/{}{:026x}.lance",
        dataset_access().allowed_prefix(),
        "0".repeat(24),
        suffix
    ))
}

#[tokio::test]
async fn managed_data_put_and_streamed_ranges_preserve_exact_identity() {
    use object_store_lance::{ObjectStore as _, PutOptions};
    let objects = Arc::new(TestManagedObjects::default());
    let store = super::object_store::ManagedObjectStore::new(objects.clone(), dataset_access(), io_limits());
    let path = native_data_path(1);
    let data = Bytes::from(
        (0..12 * 1024 * 1024)
            .map(|position| (position % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let created = store
        .put_opts(&path, data.clone().into(), PutOptions::default())
        .await
        .unwrap();
    assert!(created.version.is_some());
    let range = store.get_range(&path, 13..(data.len() as u64 - 17)).await.unwrap();
    assert_eq!(range, data.slice(13..data.len() - 17));
    {
        let ranges = objects.ranges.lock().unwrap();
        assert!(ranges.len() > 1);
        assert!(
            ranges
                .iter()
                .all(|range| range.as_ref().is_some_and(|range| range.end - range.start <= 1024 * 1024))
        );
    }
    assert!(
        store
            .put_opts(&path, Bytes::from_static(b"replacement").into(), PutOptions::default())
            .await
            .is_err()
    );
    assert_eq!(store.get(&path).await.unwrap().bytes().await.unwrap(), data);
}

#[tokio::test]
async fn managed_data_stream_rejects_revision_change_between_chunks() {
    use futures::StreamExt;
    use object_store_lance::{GetResultPayload, ObjectStore as _, PutOptions};
    let objects = Arc::new(TestManagedObjects::default());
    let store = super::object_store::ManagedObjectStore::new(objects.clone(), dataset_access(), io_limits());
    let path = native_data_path(2);
    store
        .put_opts(&path, Bytes::from(vec![7; 3 * 1024 * 1024]).into(), PutOptions::default())
        .await
        .unwrap();
    let response = store.get(&path).await.unwrap();
    let GetResultPayload::Stream(mut stream) = response.payload else {
        panic!("managed reads must stream");
    };
    assert_eq!(stream.next().await.unwrap().unwrap().len(), 1024 * 1024);
    objects.objects.lock().unwrap().get_mut(path.as_ref()).unwrap().body_revision = Some(uuid::Uuid::new_v4());
    assert!(stream.next().await.unwrap().is_err());
}

#[tokio::test]
async fn managed_multipart_complete_and_abort_preserve_completed_object() {
    let objects = Arc::new(TestManagedObjects::default());
    let store = super::object_store::ManagedObjectStore::new(objects, dataset_access(), io_limits());
    let path = native_data_path(3);
    let mut upload = store.put_multipart(&path).await.unwrap();
    let first = upload.put_part(Bytes::from(vec![1; 5 * 1024 * 1024]).into());
    let second = upload.put_part(Bytes::from_static(b"tail").into());
    second.await.unwrap();
    assert!(upload.complete().await.is_err());
    first.await.unwrap();
    let complete = upload.complete().await.unwrap();
    assert_eq!(upload.complete().await.unwrap().version, complete.version);
    upload.abort().await.unwrap();
    assert_eq!(store.head(&path).await.unwrap().size, 5 * 1024 * 1024 + 4);
    assert_eq!(
        store.get_range(&path, 5 * 1024 * 1024..5 * 1024 * 1024 + 4).await.unwrap(),
        Bytes::from_static(b"tail")
    );
}

#[tokio::test]
async fn managed_data_access_requires_registered_reference_and_native_write_path() {
    use object_store_lance::{ObjectStore as _, PutOptions};
    let objects = Arc::new(TestManagedObjects::default());
    let store = super::object_store::ManagedObjectStore::new(objects.clone(), dataset_access(), io_limits());
    assert!(store.get(&native_data_path(4)).await.is_err());
    for relative in [
        "data/../foreign.lance",
        "data/http://host/object",
        "_versions/999.manifest",
        "query-results/result",
        "_transactions/01-00000000-0000-0000-0000-000000000001.txn",
    ] {
        let path = Path::from(format!("{}{relative}", dataset_access().allowed_prefix()));
        assert!(
            store
                .put_opts(&path, Bytes::from_static(b"bad").into(), PutOptions::default())
                .await
                .is_err()
        );
    }
    assert!(objects.objects.lock().unwrap().is_empty());
}

use std::num::{NonZeroU64, NonZeroUsize};

fn dataset_record(key: &str, data: [f32; 3], metadata: Option<serde_json::Value>) -> super::dataset::VectorRecord {
    super::dataset::VectorRecord {
        key: key.to_string(),
        data: data.into(),
        metadata: metadata.map(|value| value.as_object().unwrap().clone()),
    }
}

fn all_vector_fields() -> super::dataset::VectorProjection {
    super::dataset::VectorProjection {
        data: true,
        metadata: true,
    }
}

#[tokio::test]
async fn dataset_put_replaces_exact_key_and_preserves_old_catalog_snapshot() {
    let fixture = catalog_fixture().await;
    let access = managed_access();
    let old = fixture.adapter.open_current(&access, &resource_identity()).await.unwrap();
    let created = fixture
        .adapter
        .put_vectors(
            &access,
            fixture.request(101).await,
            &[
                dataset_record("first", [1.0, 2.0, 3.0], Some(serde_json::json!({"genre":"drama","year":2020}))),
                dataset_record("second", [3.0, 2.0, 1.0], None),
            ],
        )
        .await
        .unwrap();
    assert_eq!(created.info().manifest_version, 2);
    assert!(old.get(&["first".to_string()], all_vector_fields()).await.unwrap().is_empty());
    let updated = fixture
        .adapter
        .put_vectors(
            &access,
            fixture.request(102).await,
            &[dataset_record(
                "first",
                [9.0, 8.0, 7.0],
                Some(serde_json::json!({"genre":"comedy"})),
            )],
        )
        .await
        .unwrap();
    let current = updated.get(&["first".to_string()], all_vector_fields()).await.unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].data, Some(vec![9.0, 8.0, 7.0]));
    assert_eq!(
        current[0].metadata,
        Some(serde_json::json!({"genre":"comedy"}).as_object().unwrap().clone())
    );
    let previous = created.get(&["first".to_string()], all_vector_fields()).await.unwrap();
    assert_eq!(previous[0].data, Some(vec![1.0, 2.0, 3.0]));
    let reopened = fixture
        .adapter
        .open_committed_version(&access, &resource_identity(), NonZeroU64::new(2).unwrap())
        .await
        .unwrap();
    assert_eq!(reopened.get(&["first".to_string()], all_vector_fields()).await.unwrap(), previous);
}

#[tokio::test]
async fn dataset_delete_uses_literal_keys_and_retains_unselected_rows() {
    let fixture = catalog_fixture().await;
    let access = managed_access();
    let quoted = "' OR true --";
    let before = fixture
        .adapter
        .put_vectors(
            &access,
            fixture.request(111).await,
            &[
                dataset_record(quoted, [1.0, 2.0, 3.0], None),
                dataset_record("retained", [4.0, 5.0, 6.0], None),
            ],
        )
        .await
        .unwrap();
    let after = fixture
        .adapter
        .delete_vectors(&access, fixture.request(112).await, &[quoted.to_string()])
        .await
        .unwrap();
    assert!(
        after
            .get(&[quoted.to_string()], all_vector_fields())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(after.get(&["retained".to_string()], all_vector_fields()).await.unwrap().len(), 1);
    assert_eq!(before.get(&[quoted.to_string()], all_vector_fields()).await.unwrap().len(), 1);
}

#[tokio::test]
async fn dataset_prefilter_applies_before_top_k_and_returns_unindexed_rows() {
    let fixture = catalog_fixture().await;
    let access = managed_access();
    let snapshot = fixture
        .adapter
        .put_vectors(
            &access,
            fixture.request(121).await,
            &[
                dataset_record("near-denied", [1.0, 0.0, 0.0], Some(serde_json::json!({"genre":"drama"}))),
                dataset_record("far-allowed", [0.0, 1.0, 0.0], Some(serde_json::json!({"genre":"comedy"}))),
            ],
        )
        .await
        .unwrap();
    let filter =
        crate::vector::filter::ValidatedFilter::parse(&serde_json::json!({"genre":"comedy"}), &Default::default()).unwrap();
    let matches = snapshot
        .query_candidates(&[1.0, 0.0, 0.0], NonZeroUsize::new(1).unwrap(), Some(filter))
        .await
        .unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].key, "far-allowed");
    assert_eq!(matches[0].engine_distance, 1.0);
    let selected = snapshot.take_rows(&[matches[0].row_id], all_vector_fields()).await.unwrap();
    assert_eq!(selected[0].key, "far-allowed");
}

#[tokio::test]
async fn dataset_ordered_batches_retain_snapshot_after_later_commit() {
    let fixture = catalog_fixture().await;
    let access = managed_access();
    let snapshot = fixture
        .adapter
        .put_vectors(
            &access,
            fixture.request(131).await,
            &[
                dataset_record("alpha", [1.0, 0.0, 0.0], None),
                dataset_record("beta", [0.0, 1.0, 0.0], None),
                dataset_record("gamma", [0.0, 0.0, 1.0], None),
            ],
        )
        .await
        .unwrap();
    let one = NonZeroUsize::new(1).unwrap();
    let first = snapshot.list_batch(0, one, all_vector_fields()).await.unwrap();
    fixture
        .adapter
        .delete_vectors(&access, fixture.request(132).await, &[first[0].key.clone()])
        .await
        .unwrap();
    let second = snapshot.list_batch(1, one, all_vector_fields()).await.unwrap();
    let third = snapshot.list_batch(2, one, all_vector_fields()).await.unwrap();
    let mut keys = [first[0].key.clone(), second[0].key.clone(), third[0].key.clone()];
    keys.sort();
    assert_eq!(keys, ["alpha", "beta", "gamma"]);
    assert!(snapshot.list_batch(3, one, all_vector_fields()).await.unwrap().is_empty());
}

#[tokio::test]
async fn dataset_stale_key_update_fails_without_lance_automatic_rebase() {
    let fixture = catalog_fixture().await;
    let access = managed_access();
    let stale = fixture.request(141).await;
    fixture
        .adapter
        .put_vectors(&access, fixture.request(142).await, &[dataset_record("same", [1.0, 0.0, 0.0], None)])
        .await
        .unwrap();
    let result = fixture
        .adapter
        .put_vectors(&access, stale, &[dataset_record("same", [0.0, 1.0, 0.0], None)])
        .await;
    assert!(matches!(
        result,
        Err(super::dataset::DatasetError::Manifest(LanceManifestError::ExpectedHeadConflict))
    ));
    let current = fixture.adapter.open_current(&access, &resource_identity()).await.unwrap();
    let records = current.get(&["same".to_string()], all_vector_fields()).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].data, Some(vec![1.0, 0.0, 0.0]));
}

#[tokio::test]
async fn dataset_repeated_keys_fail_before_any_catalog_or_object_io() {
    let fixture = catalog_fixture().await;
    let access = managed_access();
    for (attempt, existing) in [(161, false), (163, true)] {
        if existing {
            fixture
                .adapter
                .put_vectors(&access, fixture.request(162).await, &[dataset_record("same", [1.0, 0.0, 0.0], None)])
                .await
                .unwrap();
        }
        let request = fixture.request(attempt).await;
        let before = {
            let state = fixture.backend.state.lock().await;
            (state.read_calls, state.metadata_calls, state.put_attempts.clone(), state.objects.clone())
        };
        let result = fixture
            .adapter
            .put_vectors(
                &access,
                request,
                &[
                    dataset_record("same", [0.0, 1.0, 0.0], None),
                    dataset_record("same", [0.0, 0.0, 1.0], None),
                ],
            )
            .await;
        assert!(
            matches!(&result, Err(super::dataset::DatasetError::DuplicateInputKey { position: 1 })),
            "unexpected Dataset result: {:?}",
            result.err()
        );
        let state = fixture.backend.state.lock().await;
        assert_eq!(
            (state.read_calls, state.metadata_calls, state.put_attempts.clone(), state.objects.clone()),
            before
        );
    }
}

#[tokio::test]
async fn dataset_concurrent_candidates_publish_only_the_winning_catalog_head() {
    use crate::table_catalog::TableCatalogObjectBackend;
    let fixture = catalog_fixture().await;
    let access = managed_access();
    let delayed_request = fixture.request(171).await;
    let winner_request = fixture.request(172).await;
    let candidate_key = format!(
        "{}_versions/{}/2.manifest",
        resource_identity().dataset_prefix(),
        delayed_request.parts.commit_id.as_str()
    );
    let pause = fixture
        .backend
        .pause_next_put(access.bucket().storage_bucket_id().as_str(), &candidate_key)
        .await;
    let delayed_records = [dataset_record("same", [0.0, 1.0, 0.0], None)];
    let delayed = fixture.adapter.put_vectors(&access, delayed_request, &delayed_records);
    tokio::pin!(delayed);
    tokio::select! {
        biased;
        result = &mut delayed => panic!("Dataset write returned before candidate upload pause: {:?}", result.err()),
        entered = tokio::time::timeout(std::time::Duration::from_secs(10), pause.wait_started()) => {
            entered.expect("Dataset write reaches the candidate upload pause");
        }
    }
    let winner = async {
        let result = fixture
            .adapter
            .put_vectors(&access, winner_request, &[dataset_record("same", [1.0, 0.0, 0.0], None)])
            .await;
        pause.release();
        result
    };
    let (delayed, winner) = tokio::time::timeout(std::time::Duration::from_secs(30), async { tokio::join!(delayed, winner) })
        .await
        .unwrap();
    assert!(matches!(
        delayed,
        Err(super::dataset::DatasetError::Engine(lance_core::Error::CommitConflict { .. }))
    ));
    let winner = winner.unwrap();
    assert_eq!(winner.info().manifest_version, 2);
    assert!(
        fixture
            .backend
            .read_object(access.bucket().storage_bucket_id().as_str(), &candidate_key)
            .await
            .unwrap()
            .is_some()
    );
    let current = fixture.adapter.open_current(&access, &resource_identity()).await.unwrap();
    assert_eq!(current.info().reference, winner.info().reference);
    let records = current.get(&["same".to_string()], all_vector_fields()).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].data, Some(vec![1.0, 0.0, 0.0]));
}

#[tokio::test]
async fn dataset_missing_referenced_data_fails_exact_catalog_open() {
    let fixture = catalog_fixture().await;
    let access = managed_access();
    let committed = fixture
        .adapter
        .put_vectors(&access, fixture.request(181).await, &[dataset_record("present", [1.0, 0.0, 0.0], None)])
        .await
        .unwrap();
    let removed = {
        let mut state = fixture.backend.state.lock().await;
        let key = state
            .objects
            .keys()
            .find(|(_, key)| key.starts_with(access.allowed_prefix()) && key.ends_with(".lance"))
            .unwrap()
            .clone();
        state.objects.remove(&key).unwrap()
    };
    assert!(!removed.data.is_empty());
    assert!(matches!(
        fixture.adapter.open_current(&access, &resource_identity()).await,
        Err(LanceManifestError::ObjectNotFound { .. })
    ));
    assert_eq!(
        fixture.request(182).await.parts.expected_head.metadata().unwrap(),
        &committed.info().reference
    );
}

#[tokio::test]
async fn dataset_nonempty_manifest_rejects_foreign_files_and_invalid_fragment_counts() {
    use crate::table_catalog::{CatalogMetadataRef, TableCatalogObjectBackend};
    let fixture = catalog_fixture().await;
    let access = managed_access();
    let committed = fixture
        .adapter
        .put_vectors(&access, fixture.request(191).await, &[dataset_record("present", [1.0, 0.0, 0.0], None)])
        .await
        .unwrap();
    let reference = &committed.info().reference;
    let original = fixture
        .backend
        .read_object(access.bucket().storage_bucket_id().as_str(), reference.object_key().as_str())
        .await
        .unwrap()
        .unwrap();
    for mutation in 0..3 {
        let data = replace_manifest_message(&original.data, |message| {
            assert!(!message.fragments.is_empty());
            match mutation {
                0 => message.fragments[0].files[0].path = "../../another/data.lance".into(),
                1 => message.fragments[0].physical_rows = u64::from(u32::MAX) + 1,
                2 => message.fragments.push(message.fragments[0].clone()),
                _ => unreachable!(),
            }
        });
        let changed = crate::table_catalog::TableCatalogObject {
            data: data.to_vec(),
            ..original.clone()
        };
        let reference = CatalogMetadataRef::from_read(reference.object_key().clone(), &changed).unwrap();
        let result = ValidatedLanceManifest::read_bound(
            data,
            &reference,
            &resource_identity(),
            &index_spec(),
            2,
            io_limits().max_manifest_bytes.get(),
        )
        .await;
        assert!(matches!(
            result,
            Err(LanceManifestError::ReferenceMismatch { .. }
                | LanceManifestError::CorruptManifest { .. }
                | LanceManifestError::Engine { .. })
        ));
    }
}

#[tokio::test]
async fn initial_manifest_plan_is_repeatable_without_object_io() {
    use crate::table_catalog::CatalogCommitId;
    let create_id = CatalogCommitId::try_new(uuid::Uuid::from_u128(151).to_string()).unwrap();
    let mut index = index_spec();
    index.non_filterable_metadata_keys = ["private".to_string(), "notes".to_string(), "unicode-\u{4e2d}".to_string()].into();
    index.encryption_descriptor_sha256 = crate::table_catalog::VectorEffectiveEncryptionV1::Aes256
        .descriptor_sha256()
        .unwrap();
    let first = super::initial::plan_initial_manifest(&resource_identity(), &index, &create_id)
        .await
        .unwrap();
    assert_eq!(first.reference.body_revision(), None);
    assert_eq!(first.reference.etag(), None);
    println!(
        "initial-manifest-fixture-v1 length={} sha256={}",
        first.reference.content_length(),
        first.reference.content_sha256().to_hex()
    );
    for _ in 0..32 {
        let repeated = super::initial::plan_initial_manifest(&resource_identity(), &index, &create_id)
            .await
            .unwrap();
        assert_eq!(repeated.bytes, first.bytes);
        repeated.verify_reserved_reference(&first.reference).unwrap();
    }
    let another_id = CatalogCommitId::try_new(uuid::Uuid::from_u128(152).to_string()).unwrap();
    let other = super::initial::plan_initial_manifest(&resource_identity(), &index, &another_id)
        .await
        .unwrap();
    assert!(other.verify_reserved_reference(&first.reference).is_err());
    assert_eq!(other.bytes, first.bytes);
}

#[test]
fn initialization_payload_uses_approved_canonical_fields_and_order() {
    let mut index = index_spec();
    index.non_filterable_metadata_keys = ["zeta".into(), "alpha".into()].into();
    let bytes = super::initial::encode_initialization_payload(&index).unwrap();
    assert_eq!(bytes, br#"{"encoding_version":1,"data_type":"float32","dimension":3,"distance_metric":"cosine","non_filterable_metadata_keys":["alpha","zeta"]}"#);
    let decoded =
        super::initial::decode_initialization_payload(&bytes, &crate::table_catalog::VectorEffectiveEncryptionV1::Unencrypted)
            .unwrap();
    assert_eq!(decoded.dimension, index.dimension);
    assert_eq!(decoded.non_filterable_metadata_keys, index.non_filterable_metadata_keys);
    assert_eq!(
        decoded.encryption_descriptor_sha256,
        crate::table_catalog::VectorEffectiveEncryptionV1::Unencrypted
            .descriptor_sha256()
            .unwrap()
    );
}

#[test]
fn initialization_payload_rejects_noncanonical_and_invalid_records() {
    let encryption = crate::table_catalog::VectorEffectiveEncryptionV1::Aes256;
    let bytes = super::initial::encode_initialization_payload(&index_spec()).unwrap();
    let mut padded = bytes.clone();
    padded.push(b' ');
    assert!(super::initial::decode_initialization_payload(&padded, &encryption).is_err());
    let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    for (field, value) in [
        ("encoding_version", serde_json::json!(2)),
        ("dimension", serde_json::json!(0)),
        ("dimension", serde_json::json!(4097)),
        ("data_type", serde_json::json!("float64")),
        ("non_filterable_metadata_keys", serde_json::json!(["same", "same"])),
        ("extra", serde_json::json!(true)),
    ] {
        let mut changed = original.clone();
        changed[field] = value;
        assert!(super::initial::decode_initialization_payload(&serde_json::to_vec(&changed).unwrap(), &encryption).is_err());
    }
}

#[tokio::test]
async fn initial_manifest_plan_detects_changed_bytes_before_upload() {
    let create_id = crate::table_catalog::CatalogCommitId::try_new(uuid::Uuid::from_u128(153).to_string()).unwrap();
    let mut plan = super::initial::plan_initial_manifest(&resource_identity(), &index_spec(), &create_id)
        .await
        .unwrap();
    let reserved = plan.reference.clone();
    plan.bytes = Bytes::from_static(b"changed initial content");
    assert!(matches!(
        plan.verify_reserved_reference(&reserved),
        Err(LanceManifestError::ReferenceMismatch { .. })
    ));
}

#[derive(Debug, Default, Clone)]
struct SnapshotHeadObservations {
    active: usize,
    maximum_active: usize,
    entered: std::collections::BTreeSet<String>,
    completed: std::collections::BTreeMap<String, crate::table_catalog::TableCatalogObjectMetadata>,
}

struct SnapshotHeadPause {
    expected: std::collections::BTreeSet<String>,
    observations: std::sync::Mutex<SnapshotHeadObservations>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

impl SnapshotHeadPause {
    fn new(expected: std::collections::BTreeSet<String>) -> Self {
        Self {
            expected,
            observations: std::sync::Mutex::new(SnapshotHeadObservations::default()),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        }
    }

    async fn enter(self: &Arc<Self>, key: &str) -> Option<SnapshotHeadCall> {
        {
            let mut observations = self.observations.lock().unwrap();
            // Native fragment validation can HEAD an object again after its initial binding.
            if !self.expected.contains(key) || !observations.entered.insert(key.to_string()) {
                return None;
            }
            observations.active += 1;
            observations.maximum_active = observations.maximum_active.max(observations.active);
        }
        let call = SnapshotHeadCall(self.clone());
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
        Some(call)
    }

    async fn wait_entered(&self, count: usize) {
        loop {
            let entered = self.entered.notified();
            if self.observations.lock().unwrap().active >= count {
                return;
            }
            entered.await;
        }
    }

    fn release(&self) {
        self.release.add_permits(self.expected.len());
    }
}

struct SnapshotHeadCall(Arc<SnapshotHeadPause>);

impl Drop for SnapshotHeadCall {
    fn drop(&mut self) {
        self.0.observations.lock().unwrap().active -= 1;
    }
}

struct PausedCatalogHeads {
    inner: CatalogManagedObjects,
    pause: std::sync::Mutex<Option<Arc<SnapshotHeadPause>>>,
}

#[async_trait::async_trait]
impl super::ManagedManifestObjects for PausedCatalogHeads {
    async fn create_multipart(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
    ) -> Result<Arc<dyn super::ManagedDatasetMultipart>, LanceManifestError> {
        super::ManagedManifestObjects::create_multipart(&self.inner, access, key).await
    }

    async fn create_only(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
        content: Bytes,
    ) -> Result<crate::table_catalog::TableCatalogObjectMetadata, LanceManifestError> {
        super::ManagedManifestObjects::create_only(&self.inner, access, key, content).await
    }

    async fn get(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
        range: Option<std::ops::Range<u64>>,
        expected_body_revision: Option<uuid::Uuid>,
    ) -> Result<crate::table_catalog::TableCatalogObject, LanceManifestError> {
        super::ManagedManifestObjects::get(&self.inner, access, key, range, expected_body_revision).await
    }

    async fn head(
        &self,
        access: &crate::storage_api::vector::ManagedObjectAccess,
        key: &crate::table_catalog::CatalogObjectKey,
    ) -> Result<crate::table_catalog::TableCatalogObjectMetadata, LanceManifestError> {
        let pause = self.pause.lock().unwrap().clone();
        let call = if let Some(pause) = pause {
            pause.enter(key.as_str()).await
        } else {
            None
        };
        let result = super::ManagedManifestObjects::head(&self.inner, access, key).await;
        if let (Some(call), Ok(metadata)) = (&call, &result) {
            call.0
                .observations
                .lock()
                .unwrap()
                .completed
                .insert(key.as_str().to_string(), metadata.clone());
        }
        drop(call);
        result
    }
}

async fn load_snapshot_at_head_barrier<B: crate::table_catalog::TableCatalogObjectBackend>(
    adapter: &super::LanceManifestAdapter<B>,
    objects: &PausedCatalogHeads,
    expected_keys: &std::collections::BTreeSet<String>,
    concurrency: NonZeroUsize,
) -> (Result<super::CommittedManifestInfo, LanceManifestError>, SnapshotHeadObservations) {
    let pause = Arc::new(SnapshotHeadPause::new(expected_keys.clone()));
    *objects.pause.lock().unwrap() = Some(pause.clone());
    let access = managed_access();
    let resource = resource_identity();
    let opening = adapter.load_current(&access, &resource);
    tokio::pin!(opening);
    tokio::select! {
        biased;
        result = &mut opening => panic!("snapshot open returned before the concurrent HEAD barrier: {result:?}"),
        entered = tokio::time::timeout(std::time::Duration::from_secs(10), pause.wait_entered(concurrency.get())) => {
            let observed = pause.observations.lock().unwrap().clone();
            assert!(
                entered.is_ok(),
                "snapshot file HEAD checks remained serial: expected {} simultaneous callbacks, observed {:?}",
                concurrency,
                observed
            );
        }
    }
    {
        let observed = pause.observations.lock().unwrap().clone();
        assert_eq!(observed.active, concurrency.get(), "HEAD callbacks exceeded the configured concurrency");
        assert!(observed.maximum_active <= concurrency.get());
    }
    pause.release();
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), &mut opening)
        .await
        .expect("snapshot open finishes after releasing HEAD callbacks");
    let observed = pause.observations.lock().unwrap().clone();
    assert_eq!(observed.active, 0, "all paused HEAD callbacks must finish or be cancelled");
    assert!(
        observed.maximum_active <= concurrency.get(),
        "HEAD concurrency exceeded the configured limit"
    );
    (result, observed)
}

#[tokio::test]
async fn dataset_snapshot_file_heads_use_bounded_concurrency_and_preserve_exact_reads() {
    use crate::table_catalog::{CatalogName, TableCatalogObjectBackend};
    let fixture = catalog_fixture().await;
    let access = managed_access();
    for (attempt, key) in [(201, "head-alpha"), (202, "head-beta"), (203, "head-gamma")] {
        fixture
            .adapter
            .put_vectors(&access, fixture.request(attempt).await, &[dataset_record(key, [1.0, 0.0, 0.0], None)])
            .await
            .expect("the HEAD concurrency fixture writes native Dataset fragments");
    }
    let expected = fixture
        .adapter
        .load_current(&access, &resource_identity())
        .await
        .expect("the native multi-fragment fixture loads before installing HEAD pauses");
    let object = fixture
        .backend
        .read_object(access.bucket().storage_bucket_id().as_str(), expected.reference.object_key().as_str())
        .await
        .unwrap()
        .unwrap();
    let validated = ValidatedLanceManifest::read_bound(
        Bytes::from(object.data),
        &expected.reference,
        &resource_identity(),
        &index_spec(),
        expected.manifest_version,
        io_limits().max_manifest_bytes.get(),
    )
    .await
    .unwrap();
    let manifest = validated.manifest();
    assert_eq!(manifest.fragments.len(), 3);
    let mut expected_keys = std::collections::BTreeSet::new();
    for fragment in manifest.fragments.iter() {
        assert!(fragment.deletion_file.is_none());
        for file in &fragment.files {
            assert!(expected_keys.insert(format!("{}data/{}", access.allowed_prefix(), file.path)));
        }
    }
    assert_eq!(expected_keys.len(), 3);
    let missing_key = expected_keys.first().unwrap().clone();
    expected_keys.insert(format!(
        "{}_transactions/{}",
        access.allowed_prefix(),
        manifest
            .transaction_file
            .as_ref()
            .expect("native commits retain their transaction file")
    ));
    let mut expected_metadata = std::collections::BTreeMap::new();
    for key in &expected_keys {
        expected_metadata.insert(
            key.clone(),
            fixture
                .backend
                .object_metadata(access.bucket().storage_bucket_id().as_str(), key)
                .await
                .unwrap()
                .unwrap(),
        );
    }

    for concurrency in [2, 3] {
        let concurrency = NonZeroUsize::new(concurrency).unwrap();
        let objects = Arc::new(PausedCatalogHeads {
            inner: CatalogManagedObjects(fixture.backend.clone()),
            pause: std::sync::Mutex::new(None),
        });
        let adapter = super::LanceManifestAdapter::new(
            fixture.store.clone(),
            objects.clone(),
            access.clone(),
            CatalogName::parse("index.v1").unwrap(),
            index_spec(),
            super::ManifestIoLimits {
                max_concurrent_reads: concurrency,
                ..io_limits()
            },
        )
        .unwrap();
        for _ in 0..2 {
            let (result, observed) = load_snapshot_at_head_barrier(&adapter, &objects, &expected_keys, concurrency).await;
            let opened = result.expect("bounded HEAD validation opens the exact native snapshot");
            assert_eq!(opened.reference, expected.reference);
            assert_eq!(opened.manifest_version, expected.manifest_version);
            assert_eq!(observed.entered, expected_keys);
            assert_eq!(observed.completed, expected_metadata);
            assert_eq!(observed.maximum_active, concurrency.get());
        }
        let removed = fixture
            .backend
            .state
            .lock()
            .await
            .objects
            .remove(&(access.bucket().storage_bucket_id().as_str().to_string(), missing_key.clone()))
            .unwrap();
        let (result, observed) = load_snapshot_at_head_barrier(&adapter, &objects, &expected_keys, concurrency).await;
        assert!(
            matches!(&result, Err(LanceManifestError::ObjectNotFound { key }) if key == &missing_key),
            "missing referenced file must retain its exact error after the HEAD barrier: {result:?}"
        );
        assert!(observed.entered.contains(&missing_key));
        assert!(!observed.completed.contains_key(&missing_key));
        assert_eq!(fixture.request(204).await.parts.expected_head.metadata().unwrap(), &expected.reference);
        fixture
            .backend
            .state
            .lock()
            .await
            .objects
            .insert((access.bucket().storage_bucket_id().as_str().to_string(), missing_key.clone()), removed);
    }
}

// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

// Append to vector::lance::tests. All data files are written by native Lance.
async fn distance_catalog_fixture(metric: super::VectorDistanceMetric, dimensions: u32) -> CatalogFixture {
    let mut spec = index_spec();
    spec.distance_metric = metric;
    spec.dimension = std::num::NonZeroU32::new(dimensions).unwrap();
    use crate::table_catalog::*;
    let backend = test_support::TestCatalogObjectBackend::default();
    let access = managed_access();
    let bucket_id = access.bucket().storage_bucket_id().as_str();
    backend
        .state
        .lock()
        .await
        .bucket_handles
        .insert(bucket_id.into(), access.bucket().clone());
    let store = Arc::new(
        ObjectTableCatalogStore::for_vector_scope(backend.clone(), access.bucket().clone())
            .await
            .unwrap(),
    );
    store
        .put_table_bucket(TableBucketEntry {
            vector_bucket_binding: Some(VectorBucketBindingV1::from_handle(access.bucket()).unwrap()),
            version: 2,
            table_bucket: bucket_id.into(),
            catalog_type: "s3vectors".into(),
            warehouse_root: format!("s3://{bucket_id}/"),
            state: TableCatalogEntryState::Active,
            properties: Default::default(),
            active_rename_id: None,
            created_at: None,
            updated_at: None,
        })
        .await
        .unwrap();
    let namespace = Namespace::parse("indexes").unwrap();
    store
        .create_namespace(NamespaceEntry {
            version: 1,
            table_bucket: bucket_id.into(),
            namespace: namespace.public_name(),
            namespace_id: namespace.storage_id(),
            state: TableCatalogEntryState::Active,
            properties: Default::default(),
            created_at: None,
            updated_at: None,
        })
        .await
        .unwrap();
    let name = CatalogName::parse("index.v1").unwrap();
    let adapter = super::LanceManifestAdapter::new(
        store.clone(),
        Arc::new(CatalogManagedObjects(backend.clone())),
        access.clone(),
        name.clone(),
        spec.clone(),
        io_limits(),
    )
    .unwrap();
    let resource = resource_identity();
    let mut entry = TableEntry {
        vector_head_binding: None,
        vector_index_definition: None,
        version: 2,
        table_bucket: bucket_id.into(),
        namespace: "indexes".into(),
        table: name.as_str().into(),
        table_id: access.table_id().as_str().into(),
        table_uuid: access.table_id().as_str().into(),
        format: "lance".into(),
        format_version: 1,
        warehouse_location: format!("s3://{bucket_id}/{}", resource.dataset_prefix().trim_end_matches('/')),
        metadata_location: String::new(),
        version_token: "initial-token".into(),
        generation: 1,
        state: TableCatalogEntryState::Active,
        properties: Default::default(),
        created_at: None,
        updated_at: None,
    };
    let request = super::ManifestPrepareRequest {
        parts: CatalogCommitParts {
            resource_identity: resource.clone(),
            namespace,
            name,
            expected_head: CatalogExpectedHead::from_entry(&entry, None, None).unwrap(),
            commit_id: CatalogCommitId::try_new(uuid::Uuid::from_u128(3).to_string()).unwrap(),
            operation: "create".into(),
            idempotency_key: None,
            writer: None,
        },
        index: spec.clone(),
    };
    let prepared = adapter.prepare_empty_index(&access, request).await.unwrap();
    entry.metadata_location = prepared.candidate_metadata().object_key().as_str().into();
    entry.vector_head_binding =
        Some(VectorHeadBindingV1::try_new(&resource, prepared.candidate_metadata(), prepared.format_descriptor()).unwrap());
    store.create_table(entry).await.unwrap();
    CatalogFixture { backend, store, adapter }
}

async fn distance_request(fixture: &CatalogFixture, attempt: u128) -> super::ManifestPrepareRequest {
    let mut request = fixture.request(attempt).await;
    request.index = fixture
        .adapter
        .load_current(&managed_access(), &resource_identity())
        .await
        .unwrap()
        .index;
    request
}

fn distance_record(key: &str, data: Vec<f32>) -> super::VectorRecord {
    super::VectorRecord {
        key: key.into(),
        data,
        metadata: None,
    }
}

#[tokio::test]
async fn dataset_distance_euclidean_returns_three_four_five() {
    let fixture = distance_catalog_fixture(super::VectorDistanceMetric::Euclidean, 2).await;
    let snapshot = fixture
        .adapter
        .put_vectors(
            &managed_access(),
            distance_request(&fixture, 501).await,
            &[distance_record("triangle", vec![3.0, 4.0])],
        )
        .await
        .unwrap();
    let found = snapshot
        .query_matches(&[0.0, 0.0], NonZeroUsize::new(1).unwrap(), None)
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].key, "triangle");
    assert_eq!(found[0].distance_f64, 5.0);
    let rows = snapshot.take_rows(&[found[0].row_id], all_vector_fields()).await.unwrap();
    assert_eq!(rows[0].data.as_deref(), Some([3.0, 4.0].as_slice()));
}

#[tokio::test]
async fn dataset_distance_cosine_returns_zero_one_two() {
    let fixture = distance_catalog_fixture(super::VectorDistanceMetric::Cosine, 2).await;
    let snapshot = fixture
        .adapter
        .put_vectors(
            &managed_access(),
            distance_request(&fixture, 502).await,
            &[
                distance_record("opposite", vec![-1.0, 0.0]),
                distance_record("orthogonal", vec![0.0, 1.0]),
                distance_record("same", vec![1.0, 0.0]),
            ],
        )
        .await
        .unwrap();
    let found = snapshot
        .query_matches(&[1.0, 0.0], NonZeroUsize::new(3).unwrap(), None)
        .await
        .unwrap();
    assert_eq!(
        found
            .iter()
            .map(|row| (row.key.as_str(), row.distance_f64))
            .collect::<Vec<_>>(),
        vec![("same", 0.0), ("orthogonal", 1.0), ("opposite", 2.0)]
    );
    assert_eq!(found[0].distance_f64.to_bits(), 0.0_f64.to_bits());
}

#[tokio::test]
async fn dataset_distance_extreme_euclidean_serializes_smithy_infinity() {
    let fixture = distance_catalog_fixture(super::VectorDistanceMetric::Euclidean, 2).await;
    let snapshot = fixture
        .adapter
        .put_vectors(
            &managed_access(),
            distance_request(&fixture, 503).await,
            &[
                distance_record("far", vec![-f32::MAX, 0.0]),
                distance_record("middle", vec![0.0, 0.0]),
                distance_record("near", vec![f32::MAX, 0.0]),
            ],
        )
        .await
        .unwrap();
    let found = snapshot
        .query_matches(&[f32::MAX, 0.0], NonZeroUsize::new(3).unwrap(), None)
        .await
        .unwrap();
    assert_eq!(found.iter().map(|row| row.key.as_str()).collect::<Vec<_>>(), ["near", "middle", "far"]);
    assert_eq!(found[0].distance_f64, 0.0);
    assert_eq!(found[1].distance_f64, f64::from(f32::MAX));
    assert_eq!(found[2].distance_f64, 2.0 * f64::from(f32::MAX));
    assert!(found.iter().all(|row| row.distance_f64.is_finite()));
    let output = crate::vector::protocol::dto::QueryOutputVector {
        key: found[2].key.clone(),
        distance: Some(found[2].distance_f64 as f32),
        metadata: None,
    };
    assert_eq!(
        serde_json::to_value(output).unwrap(),
        serde_json::json!({"key":"far","distance":"Infinity"})
    );
}

#[tokio::test]
async fn dataset_distance_subnormal_cosine_keeps_all_visible_rows() {
    let tiny = f32::from_bits(1);
    let fixture = distance_catalog_fixture(super::VectorDistanceMetric::Cosine, 2).await;
    let snapshot = fixture
        .adapter
        .put_vectors(
            &managed_access(),
            distance_request(&fixture, 504).await,
            &[
                distance_record("opposite", vec![-tiny, 0.0]),
                distance_record("orthogonal", vec![0.0, tiny]),
                distance_record("same", vec![tiny, 0.0]),
            ],
        )
        .await
        .unwrap();
    let found = snapshot
        .query_matches(&[tiny, 0.0], NonZeroUsize::new(3).unwrap(), None)
        .await
        .unwrap();
    assert_eq!(
        found
            .iter()
            .map(|row| (row.key.as_str(), row.distance_f64))
            .collect::<Vec<_>>(),
        vec![("same", 0.0), ("orthogonal", 1.0), ("opposite", 2.0)]
    );
    let rows = snapshot
        .take_rows(&found.iter().map(|row| row.row_id).collect::<Vec<_>>(), all_vector_fields())
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].data.as_deref(), Some([tiny, 0.0].as_slice()));
}

#[tokio::test]
async fn dataset_distance_equal_results_order_by_utf8_key_before_top_k() {
    let fixture = distance_catalog_fixture(super::VectorDistanceMetric::Euclidean, 2).await;
    let snapshot = fixture
        .adapter
        .put_vectors(
            &managed_access(),
            distance_request(&fixture, 505).await,
            &[
                distance_record("é", vec![0.0, -1.0]),
                distance_record("z", vec![-1.0, 0.0]),
                distance_record("a", vec![1.0, 0.0]),
            ],
        )
        .await
        .unwrap();
    for _ in 0..2 {
        let found = snapshot
            .query_matches(&[0.0, 0.0], NonZeroUsize::new(2).unwrap(), None)
            .await
            .unwrap();
        assert_eq!(found.iter().map(|row| row.key.as_str()).collect::<Vec<_>>(), ["a", "z"]);
        assert!(found.iter().all(|row| row.distance_f64 == 1.0));
    }
}

#[tokio::test]
async fn dataset_distance_prefilter_and_projection_keep_original_snapshot() {
    let fixture = distance_catalog_fixture(super::VectorDistanceMetric::Euclidean, 3).await;
    let old = fixture
        .adapter
        .put_vectors(
            &managed_access(),
            distance_request(&fixture, 506).await,
            &[
                dataset_record("near-denied", [0.0, 0.0, 0.0], Some(serde_json::json!({"genre":"drama"}))),
                dataset_record("old-allowed", [3.0, 4.0, 0.0], Some(serde_json::json!({"genre":"comedy","generation":1}))),
            ],
        )
        .await
        .unwrap();
    let original = old.info().reference.clone();
    let new = fixture
        .adapter
        .put_vectors(
            &managed_access(),
            distance_request(&fixture, 507).await,
            &[
                dataset_record(
                    "old-allowed",
                    [30.0, 40.0, 0.0],
                    Some(serde_json::json!({"genre":"comedy","generation":2})),
                ),
                dataset_record("new-allowed", [1.0, 0.0, 0.0], Some(serde_json::json!({"genre":"comedy"}))),
            ],
        )
        .await
        .unwrap();
    assert_ne!(new.info().reference, original);
    let filter =
        crate::vector::filter::ValidatedFilter::parse(&serde_json::json!({"genre":"comedy"}), &Default::default()).unwrap();
    let found = old
        .query_matches(&[0.0, 0.0, 0.0], NonZeroUsize::new(1).unwrap(), Some(filter))
        .await
        .unwrap();
    assert_eq!(old.info().reference, original);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].key, "old-allowed");
    assert_eq!(found[0].distance_f64, 5.0);
    let rows = old.take_rows(&[found[0].row_id], all_vector_fields()).await.unwrap();
    assert_eq!(rows[0].data.as_deref(), Some([3.0, 4.0, 0.0].as_slice()));
    assert_eq!(rows[0].metadata.as_ref().unwrap().get("generation"), Some(&serde_json::json!(1)));
}

#[tokio::test]
async fn dataset_distance_unproven_candidates_scan_all_native_fragments() {
    let fixture = distance_catalog_fixture(super::VectorDistanceMetric::Euclidean, 2).await;
    for (attempt, key, value) in [
        (508, "a-far", -f32::MAX),
        (509, "m-middle", 0.0),
        (510, "z-near", f32::MAX / 2.0),
    ] {
        fixture
            .adapter
            .put_vectors(
                &managed_access(),
                distance_request(&fixture, attempt).await,
                &[distance_record(key, vec![value, 0.0])],
            )
            .await
            .unwrap();
    }
    // A fresh adapter has no data-file admission evidence or ANN training proof.
    let adapter = super::LanceManifestAdapter::new(
        fixture.store.clone(),
        Arc::new(CatalogManagedObjects(fixture.backend.clone())),
        managed_access(),
        crate::table_catalog::CatalogName::parse("index.v1").unwrap(),
        distance_request(&fixture, 511).await.index,
        io_limits(),
    )
    .unwrap();
    let snapshot = adapter.open_current(&managed_access(), &resource_identity()).await.unwrap();
    let reference = snapshot.info().reference.clone();
    let before = fixture.backend.state.lock().await.read_attempts.clone();
    let data_objects = fixture
        .backend
        .state
        .lock()
        .await
        .objects
        .keys()
        .filter(|(bucket, key)| bucket == managed_access().bucket().storage_bucket_id().as_str() && key.ends_with(".lance"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(data_objects.len(), 3, "fixture must contain three distinct native fragments");
    let found = snapshot
        .query_matches(&[f32::MAX, 0.0], NonZeroUsize::new(1).unwrap(), None)
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].key, "z-near");
    assert_eq!(found[0].distance_f64, f64::from(f32::MAX) - f64::from(f32::MAX / 2.0));
    assert_eq!(snapshot.info().reference, reference);
    let after = fixture.backend.state.lock().await.read_attempts.clone();
    for object in data_objects {
        assert!(
            after.get(&object).copied().unwrap_or_default() > before.get(&object).copied().unwrap_or_default(),
            "exact fallback must read each native fragment through the real Managed get callback: {object:?}"
        );
    }
}

#[tokio::test]
async fn dataset_distance_4096_dimensions_preserve_f64_extreme_results() {
    for metric in [super::VectorDistanceMetric::Euclidean, super::VectorDistanceMetric::Cosine] {
        let fixture = distance_catalog_fixture(metric, 4096).await;
        let snapshot = fixture
            .adapter
            .put_vectors(
                &managed_access(),
                distance_request(&fixture, 512).await,
                &[distance_record("opposite", vec![-f32::MAX; 4096])],
            )
            .await
            .unwrap();
        let found = snapshot
            .query_matches(&[f32::MAX; 4096], NonZeroUsize::new(1).unwrap(), None)
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        let expected = match metric {
            super::VectorDistanceMetric::Euclidean => 128.0 * f64::from(f32::MAX),
            super::VectorDistanceMetric::Cosine => 2.0,
        };
        assert!((found[0].distance_f64 - expected).abs() <= expected * 1e-12);
        assert!(found[0].distance_f64.is_finite());
    }
}

#[derive(Clone)]
struct ServiceRequestFixture {
    request: crate::vector::protocol::Request,
    bucket: crate::storage_api::vector::BucketHandle,
    name: crate::table_catalog::CatalogName,
    backend: crate::table_catalog::test_support::TestCatalogObjectBackend,
    deny: bool,
    managed_calls: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl crate::vector::service::VectorRequestAccess for ServiceRequestFixture {
    fn request(&self) -> &crate::vector::protocol::Request {
        &self.request
    }
    fn request_id(&self) -> crate::vector::service::VectorRequestId {
        crate::vector::service::VectorRequestId::try_new(uuid::Uuid::from_u128(301)).unwrap()
    }
    fn bucket(&self) -> &crate::storage_api::vector::BucketHandle {
        &self.bucket
    }
    fn index_name(&self) -> &crate::table_catalog::CatalogName {
        &self.name
    }

    async fn authorize_index(
        &self,
        request: &crate::vector::protocol::Request,
        resource: &crate::table_catalog::CatalogResourceIdentity,
        definition: &crate::table_catalog::VectorIndexDefinitionV1,
        operation: Option<&crate::table_catalog::VectorCatalogOperationGuard>,
    ) -> Result<(), crate::vector::service::VectorServiceError> {
        if let Some(operation) = operation {
            operation.check_held()?;
            assert_eq!(operation.resource_identity(), resource);
        }
        assert_eq!(request, &self.request);
        assert_eq!(resource.vector_bucket(), Some(&self.bucket));
        definition.validate()?;
        if self.deny {
            return Err(crate::vector::protocol::ProtocolError::new(
                crate::vector::protocol::ErrorCode::AccessDenied,
                "fixture denial",
            )
            .into());
        }
        Ok(())
    }

    async fn resolve_index_definition(
        &self,
        input: &crate::vector::protocol::dto::CreateIndexInput,
    ) -> Result<crate::table_catalog::VectorIndexDefinitionV1, crate::vector::service::VectorServiceError> {
        assert!(input.encryption_configuration.is_none());
        Ok(crate::table_catalog::VectorIndexDefinitionV1 {
            version: 1,
            effective_encryption: crate::table_catalog::VectorEffectiveEncryptionV1::Unencrypted,
            tags: input.tags.clone().unwrap_or_default(),
        })
    }

    async fn managed_dataset(
        &self,
        guard: Arc<crate::table_catalog::VectorCatalogOperationGuard>,
        definition: &crate::table_catalog::VectorIndexDefinitionV1,
    ) -> Result<crate::vector::service::ManagedDatasetAccess, crate::vector::service::VectorServiceError> {
        use crate::storage_api::vector::{ManagedObjectAccess, ManagedObjectOperation};
        guard.check_held()?;
        definition.validate()?;
        self.managed_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let resource = guard.resource_identity();
        let access = ManagedObjectAccess::try_from_parts(
            self.bucket.clone(),
            resource.table_id().clone(),
            resource.dataset_prefix(),
            [
                ManagedObjectOperation::CreateOnly,
                ManagedObjectOperation::Get,
                ManagedObjectOperation::Head,
                ManagedObjectOperation::Range,
                ManagedObjectOperation::CreateMultipart,
                ManagedObjectOperation::UploadPart,
                ManagedObjectOperation::CompleteMultipart,
                ManagedObjectOperation::AbortMultipart,
            ]
            .into(),
        )
        .unwrap();
        Ok(crate::vector::service::ManagedDatasetAccess {
            access,
            objects: Arc::new(CatalogManagedObjects(self.backend.clone())),
        })
    }
}

fn service_request(fixture: &CatalogFixture) -> ServiceRequestFixture {
    use crate::vector::protocol::{Request, dto};
    ServiceRequestFixture {
        request: Request::CreateIndex(dto::CreateIndexInput {
            vector_bucket_name: Some("vectors".into()),
            vector_bucket_arn: None,
            index_name: "service-index".into(),
            data_type: dto::DataType::Float32,
            dimension: 3,
            distance_metric: dto::DistanceMetric::Cosine,
            metadata_configuration: Some(dto::MetadataConfiguration {
                non_filterable_metadata_keys: vec!["private".into()],
            }),
            encryption_configuration: None,
            tags: Some([("owner".into(), "research".into())].into()),
        }),
        bucket: managed_access().bucket().clone(),
        name: crate::table_catalog::CatalogName::parse("service-index").unwrap(),
        backend: fixture.backend.clone(),
        deny: false,
        managed_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    }
}

fn index_service(
    fixture: &CatalogFixture,
) -> crate::vector::service::VectorService<crate::table_catalog::test_support::TestCatalogObjectBackend> {
    crate::vector::service::VectorService::new(fixture.store.clone(), io_limits(), std::num::NonZeroUsize::new(3).unwrap())
}

#[tokio::test]
async fn service_creation_publishes_real_initial_manifest_and_restores_reserved_identity() {
    use crate::vector::{protocol::Response, service::VectorExecution};
    let fixture = catalog_fixture().await;
    let context = service_request(&fixture);
    let service = index_service(&fixture);
    let result = service.execute(&context).await.unwrap();
    assert!(
        matches!(result, VectorExecution::Complete(Response::CreateIndex(output)) if output.index_arn == "arn:aws:s3vectors:us-east-1::bucket/vectors/index/service-index")
    );
    let (guard, entry) = fixture
        .store
        .begin_vector_operation(
            &context.bucket,
            &crate::table_catalog::Namespace::parse("indexes").unwrap(),
            &context.name,
        )
        .await
        .unwrap();
    let original_identity = guard.resource_identity().clone();
    assert_eq!(entry.head().generation().get(), 1);
    assert_eq!(entry.definition().tags.get("owner").map(String::as_str), Some("research"));
    assert!(entry.head().metadata().body_revision().is_some());
    let original_reference = entry.head().metadata().clone();
    drop(guard);
    let calls = context.managed_calls.load(std::sync::atomic::Ordering::SeqCst);
    service.resume_index_creation(&context).await.unwrap();
    assert_eq!(context.managed_calls.load(std::sync::atomic::Ordering::SeqCst), calls);
    let (guard, entry) = fixture
        .store
        .begin_vector_operation(
            &context.bucket,
            &crate::table_catalog::Namespace::parse("indexes").unwrap(),
            &context.name,
        )
        .await
        .unwrap();
    assert_eq!(guard.resource_identity(), &original_identity);
    assert_eq!(entry.head().metadata(), &original_reference);
}

#[tokio::test]
async fn service_crud_and_prefilter_use_published_catalog_data() {
    use crate::vector::{
        protocol::{Request, dto},
        service::VectorExecution,
    };
    let fixture = catalog_fixture().await;
    let mut context = service_request(&fixture);
    let service = index_service(&fixture);
    service.execute(&context).await.unwrap();
    context.request = Request::PutVectors(dto::PutVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        vectors: vec![
            dto::PutInputVector {
                key: "near-denied".into(),
                data: dto::VectorData {
                    float32: vec![1.0, 0.0, 0.0],
                },
                metadata: Some(serde_json::json!({"genre":"drama"})),
            },
            dto::PutInputVector {
                key: "far-allowed".into(),
                data: dto::VectorData {
                    float32: vec![0.0, 1.0, 0.0],
                },
                metadata: Some(serde_json::json!({"genre":"comedy"})),
            },
        ],
    });
    service.execute(&context).await.unwrap();
    context.request = Request::QueryVectors(dto::QueryVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        top_k: 1,
        query_vector: dto::VectorData {
            float32: vec![1.0, 0.0, 0.0],
        },
        filter: Some(serde_json::json!({"genre":"comedy"})),
        query_mode: Some(dto::IndexMode::Enhanced),
        return_metadata: Some(true),
        return_distance: Some(true),
        next_token: None,
    });
    let VectorExecution::QueryVectors(query) = service.execute(&context).await.unwrap() else {
        panic!("expected prepared query");
    };
    assert_eq!(query.candidates[0].key, "far-allowed");
    assert_eq!(query.candidates[0].distance_f64, 1.0);
    assert_eq!(query.metadata_rows.as_ref().unwrap()[0].metadata.as_ref().unwrap()["genre"], "comedy");
    context.request = Request::DeleteVectors(dto::DeleteVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        keys: vec!["far-allowed".into()],
    });
    service.execute(&context).await.unwrap();
    assert_eq!(
        query
            .snapshot
            .get(&["far-allowed".into()], all_vector_fields())
            .await
            .unwrap()
            .len(),
        1
    );
    context.request = Request::GetVectors(dto::GetVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        keys: vec!["near-denied".into()],
        return_data: Some(true),
        return_metadata: Some(true),
    });
    let VectorExecution::GetVectors(get) = service.execute(&context).await.unwrap() else {
        panic!("expected prepared get");
    };
    assert_eq!(get.vectors.len(), 1);
    assert_eq!(get.vectors[0].data.as_ref().unwrap().float32, vec![1.0, 0.0, 0.0]);
}

#[tokio::test]
async fn service_list_vectors_projects_current_catalog_rows_and_partitions_segments() {
    use crate::vector::{
        protocol::{Request, Response, dto},
        service::VectorExecution,
    };
    let fixture = catalog_fixture().await;
    let mut context = service_request(&fixture);
    let service = index_service(&fixture);
    service.execute(&context).await.unwrap();
    context.request = Request::PutVectors(dto::PutVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        vectors: (0..20)
            .map(|index| dto::PutInputVector {
                key: format!("key-{index:02}"),
                data: dto::VectorData {
                    float32: vec![1.0, 0.0, 0.0],
                },
                metadata: Some(serde_json::json!({"genre": "drama"})),
            })
            .collect(),
    });
    service.execute(&context).await.unwrap();
    let mut input = dto::ListVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        max_results: Some(20),
        next_token: None,
        segment_count: None,
        segment_index: None,
        return_data: None,
        return_metadata: None,
    };
    context.request = Request::ListVectors(input.clone());
    let VectorExecution::Complete(Response::ListVectors(output)) = service.execute(&context).await.unwrap() else {
        panic!("expected listed vectors");
    };
    assert!(output.next_token.is_none());
    assert_eq!(output.vectors.len(), 20);
    assert!(output.vectors.iter().all(|row| row.data.is_none() && row.metadata.is_none()));
    let all_keys: std::collections::BTreeSet<_> = output.vectors.into_iter().map(|row| row.key).collect();
    let mut segmented_keys = std::collections::BTreeSet::new();
    for segment in 0..3 {
        input.segment_count = Some(3);
        input.segment_index = Some(segment);
        input.return_data = Some(true);
        input.return_metadata = Some(true);
        context.request = Request::ListVectors(input.clone());
        let VectorExecution::Complete(Response::ListVectors(output)) = service.execute(&context).await.unwrap() else {
            panic!("expected segment output");
        };
        assert!(output.next_token.is_none());
        assert!(!output.vectors.is_empty());
        for row in output.vectors {
            assert!(segmented_keys.insert(row.key), "segments must be disjoint");
            assert_eq!(row.data.unwrap().float32, vec![1.0, 0.0, 0.0]);
            assert_eq!(row.metadata.unwrap(), serde_json::json!({"genre":"drama"}));
        }
    }
    assert_eq!(segmented_keys, all_keys);
}

#[tokio::test]
async fn service_list_vectors_does_not_return_incomplete_success_without_a_token() {
    use crate::vector::{
        protocol::{Request, dto},
        service::VectorServiceError,
    };
    let fixture = catalog_fixture().await;
    let mut context = service_request(&fixture);
    let service = index_service(&fixture);
    service.execute(&context).await.unwrap();
    context.request = Request::PutVectors(dto::PutVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        vectors: (0..34)
            .map(|index| dto::PutInputVector {
                key: format!("large-{index:02}"),
                data: dto::VectorData {
                    float32: vec![1.0, 0.0, 0.0],
                },
                metadata: Some(serde_json::json!({"private": "x".repeat(32 * 1024)})),
            })
            .collect(),
    });
    service.execute(&context).await.unwrap();
    let mut input = dto::ListVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        max_results: Some(1),
        next_token: None,
        segment_count: None,
        segment_index: None,
        return_data: Some(false),
        return_metadata: Some(false),
    };
    context.request = Request::ListVectors(input.clone());
    assert!(matches!(service.execute(&context).await, Err(VectorServiceError::PendingIntegration(_))));
    input.max_results = Some(1000);
    context.request = Request::ListVectors(input.clone());
    assert!(
        matches!(service.execute(&context).await, Err(VectorServiceError::PendingIntegration(_))),
        "processed data over 1 MiB needs pagination even when fields are not returned"
    );
    input.next_token = Some("unverified-token".into());
    context.request = Request::ListVectors(input);
    assert!(matches!(service.execute(&context).await, Err(VectorServiceError::PendingIntegration(_))));
}

#[tokio::test]
async fn service_list_vectors_denial_prevents_managed_object_access() {
    use crate::vector::{
        protocol::{Request, dto},
        service::VectorServiceError,
    };
    let fixture = catalog_fixture().await;
    let mut context = service_request(&fixture);
    let service = index_service(&fixture);
    service.execute(&context).await.unwrap();
    let calls = context.managed_calls.load(std::sync::atomic::Ordering::SeqCst);
    context.deny = true;
    context.request = Request::ListVectors(dto::ListVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        max_results: None,
        next_token: None,
        segment_count: None,
        segment_index: None,
        return_data: None,
        return_metadata: None,
    });
    assert!(matches!(service.execute(&context).await, Err(VectorServiceError::Protocol(error))
        if error.code() == crate::vector::protocol::ErrorCode::AccessDenied));
    assert_eq!(context.managed_calls.load(std::sync::atomic::Ordering::SeqCst), calls);
}

#[tokio::test]
async fn service_denial_prevents_managed_object_access() {
    use crate::vector::protocol::{Request, dto};
    let fixture = catalog_fixture().await;
    let mut context = service_request(&fixture);
    let service = index_service(&fixture);
    service.execute(&context).await.unwrap();
    context.deny = true;
    context.request = Request::GetVectors(dto::GetVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        keys: vec!["anything".into()],
        return_data: Some(true),
        return_metadata: Some(true),
    });
    let before = context.managed_calls.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        matches!(service.execute(&context).await, Err(crate::vector::service::VectorServiceError::Protocol(error)) if error.code() == crate::vector::protocol::ErrorCode::AccessDenied)
    );
    assert_eq!(context.managed_calls.load(std::sync::atomic::Ordering::SeqCst), before);
}

#[tokio::test]
async fn service_delete_index_blocks_new_vector_operations() {
    use crate::vector::protocol::{Request, dto};
    let fixture = catalog_fixture().await;
    let mut context = service_request(&fixture);
    let service = index_service(&fixture);
    service.execute(&context).await.unwrap();
    context.request = Request::DeleteIndex(dto::DeleteIndexInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
    });
    service.execute(&context).await.unwrap();
    let result = fixture
        .store
        .begin_vector_operation(
            &context.bucket,
            &crate::table_catalog::Namespace::parse("indexes").unwrap(),
            &context.name,
        )
        .await;
    assert!(matches!(result, Err(crate::table_catalog::TableCatalogStoreError::NotFound(_))));
}

#[tokio::test]
async fn service_repeated_put_keys_fail_before_catalog_and_object_io() {
    use crate::vector::protocol::{Request, dto};
    let fixture = catalog_fixture().await;
    let mut context = service_request(&fixture);
    let service = index_service(&fixture);
    service.execute(&context).await.unwrap();
    context.request = Request::PutVectors(dto::PutVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        vectors: vec![
            dto::PutInputVector {
                key: "same".into(),
                data: dto::VectorData {
                    float32: vec![1.0, 0.0, 0.0],
                },
                metadata: None,
            },
            dto::PutInputVector {
                key: "same".into(),
                data: dto::VectorData {
                    float32: vec![0.0, 1.0, 0.0],
                },
                metadata: None,
            },
        ],
    });
    let before = {
        let state = fixture.backend.state.lock().await;
        (state.read_calls, state.metadata_calls, state.put_attempts.clone(), state.objects.clone())
    };
    assert!(matches!(
        service.execute(&context).await,
        Err(crate::vector::service::VectorServiceError::Dataset(
            super::dataset::DatasetError::DuplicateInputKey { position: 1 }
        ))
    ));
    let state = fixture.backend.state.lock().await;
    assert_eq!(
        (state.read_calls, state.metadata_calls, state.put_attempts.clone(), state.objects.clone()),
        before
    );
}

#[tokio::test]
async fn service_index_tags_merge_remove_and_preserve_dataset_identity() {
    use crate::vector::{
        protocol::{Request, Response, dto},
        service::VectorExecution,
    };
    let fixture = catalog_fixture().await;
    let mut context = service_request(&fixture);
    let service = index_service(&fixture);
    service.execute(&context).await.unwrap();
    let namespace = crate::table_catalog::Namespace::parse("indexes").unwrap();
    let (guard, before) = fixture
        .store
        .begin_vector_operation(&context.bucket, &namespace, &context.name)
        .await
        .unwrap();
    let identity = guard.resource_identity().clone();
    drop(guard);
    let managed_calls = context.managed_calls.load(std::sync::atomic::Ordering::SeqCst);
    let storage_bucket = context.bucket.storage_bucket_id().as_str().to_owned();
    let managed_state = |state: &crate::table_catalog::test_support::TestCatalogObjectState| {
        (
            state
                .objects
                .iter()
                .filter(|((bucket, _), _)| bucket == &storage_bucket)
                .map(|(key, object)| (key.clone(), object.clone()))
                .collect::<std::collections::BTreeMap<_, _>>(),
            state
                .put_attempts
                .iter()
                .filter(|((bucket, _), _)| bucket == &storage_bucket)
                .map(|(_, count)| count)
                .sum::<usize>(),
            state
                .delete_attempts
                .iter()
                .filter(|((bucket, _), _)| bucket == &storage_bucket)
                .map(|(_, count)| count)
                .sum::<usize>(),
        )
    };
    let managed_before = {
        let state = fixture.backend.state.lock().await;
        managed_state(&state)
    };
    let resource_arn = "arn:aws:s3vectors:us-east-1::bucket/vectors/index/service-index".to_owned();
    context.request = Request::TagResource(dto::TagResourceInput {
        resource_arn: resource_arn.clone(),
        tags: [("owner".into(), "updated".into()), ("keep".into(), "yes".into())].into(),
    });
    assert!(matches!(
        service.execute(&context).await.unwrap(),
        VectorExecution::Complete(Response::TagResource(_))
    ));
    assert_eq!(context.managed_calls.load(std::sync::atomic::Ordering::SeqCst), managed_calls + 1);
    context.request = Request::UntagResource(dto::UntagResourceInput {
        resource_arn: resource_arn.clone(),
        tag_keys: vec!["owner".into(), "missing".into()],
    });
    assert!(matches!(
        service.execute(&context).await.unwrap(),
        VectorExecution::Complete(Response::UntagResource(_))
    ));
    context.request = Request::ListTagsForResource(dto::ListTagsForResourceInput { resource_arn });
    let VectorExecution::Complete(Response::ListTagsForResource(output)) = service.execute(&context).await.unwrap() else {
        panic!("expected tags response")
    };
    assert_eq!(output.tags, [("keep".into(), "yes".into())].into());
    let (guard, after) = fixture
        .store
        .begin_vector_operation(&context.bucket, &namespace, &context.name)
        .await
        .unwrap();
    assert_eq!(guard.resource_identity(), &identity);
    assert_eq!(after.head().metadata(), before.head().metadata());
    assert_eq!(after.head().generation().get(), before.head().generation().get() + 2);
    assert_eq!(after.definition().effective_encryption, before.definition().effective_encryption);
    assert_eq!(context.managed_calls.load(std::sync::atomic::Ordering::SeqCst), managed_calls + 2);
    let state = fixture.backend.state.lock().await;
    assert_eq!(managed_state(&state), managed_before);
}

#[tokio::test]
async fn service_index_tag_denial_preserves_authoritative_entry() {
    use crate::vector::{
        protocol::{Request, dto},
        service::VectorServiceError,
    };
    let fixture = catalog_fixture().await;
    let mut context = service_request(&fixture);
    let service = index_service(&fixture);
    service.execute(&context).await.unwrap();
    let namespace = crate::table_catalog::Namespace::parse("indexes").unwrap();
    let (guard, before) = fixture
        .store
        .begin_vector_operation(&context.bucket, &namespace, &context.name)
        .await
        .unwrap();
    drop(guard);
    context.deny = true;
    context.request = Request::TagResource(dto::TagResourceInput {
        resource_arn: "arn:aws:s3vectors:us-east-1::bucket/vectors/index/service-index".into(),
        tags: [("owner".into(), "denied".into())].into(),
    });
    assert!(
        matches!(service.execute(&context).await, Err(VectorServiceError::Protocol(error)) if error.code() == crate::vector::protocol::ErrorCode::AccessDenied)
    );
    let (_, after) = fixture
        .store
        .begin_vector_operation(&context.bucket, &namespace, &context.name)
        .await
        .unwrap();
    assert_eq!(after.head().resource_identity(), before.head().resource_identity());
    assert_eq!(after.head().namespace(), before.head().namespace());
    assert_eq!(after.head().name(), before.head().name());
    assert_eq!(after.head().version_token(), before.head().version_token());
    assert_eq!(after.head().generation(), before.head().generation());
    assert_eq!(after.head().metadata(), before.head().metadata());
    assert_eq!(after.head().format_descriptor(), before.head().format_descriptor());
    assert_eq!(after.definition(), before.definition());
}

#[tokio::test]
async fn service_enhanced_mode_checks_live_index_without_new_head() {
    use crate::vector::{
        protocol::{Request, Response, dto},
        service::{VectorExecution, VectorServiceError},
    };
    let fixture = catalog_fixture().await;
    let mut context = service_request(&fixture);
    let service = index_service(&fixture);
    service.execute(&context).await.unwrap();
    let namespace = crate::table_catalog::Namespace::parse("indexes").unwrap();
    let (guard, before) = fixture
        .store
        .begin_vector_operation(&context.bucket, &namespace, &context.name)
        .await
        .unwrap();
    drop(guard);
    context.request = Request::UpdateIndexMode(dto::UpdateIndexModeInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        index_mode: dto::IndexMode::Enhanced,
    });
    assert!(matches!(
        service.execute(&context).await.unwrap(),
        VectorExecution::Complete(Response::UpdateIndexMode(_))
    ));
    let (guard, after) = fixture
        .store
        .begin_vector_operation(&context.bucket, &namespace, &context.name)
        .await
        .unwrap();
    assert_eq!(after.head().resource_identity(), before.head().resource_identity());
    assert_eq!(after.head().namespace(), before.head().namespace());
    assert_eq!(after.head().name(), before.head().name());
    assert_eq!(after.head().version_token(), before.head().version_token());
    assert_eq!(after.head().generation(), before.head().generation());
    assert_eq!(after.head().metadata(), before.head().metadata());
    assert_eq!(after.head().format_descriptor(), before.head().format_descriptor());
    drop(guard);
    context.deny = true;
    assert!(
        matches!(service.execute(&context).await, Err(VectorServiceError::Protocol(error)) if error.code() == crate::vector::protocol::ErrorCode::AccessDenied)
    );
    context.deny = false;
    if let Request::UpdateIndexMode(input) = &mut context.request {
        input.index_mode = dto::IndexMode::Classic;
    }
    assert!(
        matches!(service.execute(&context).await, Err(VectorServiceError::Protocol(error)) if error.code() == crate::vector::protocol::ErrorCode::Validation)
    );
}

#[tokio::test]
async fn service_query_output_preserves_f64_order_and_encodes_float_boundaries() {
    use crate::vector::{
        protocol::{Request, Response, dto},
        service::VectorExecution,
    };
    let fixture = catalog_fixture().await;
    let mut context = service_request(&fixture);
    let service = index_service(&fixture);
    let Request::CreateIndex(input) = &mut context.request else {
        panic!("expected the Index creation fixture");
    };
    input.distance_metric = dto::DistanceMetric::Euclidean;
    service.execute(&context).await.expect("create the Euclidean Index");
    let below_max = f32::from_bits(f32::MAX.to_bits() - 1);
    let records = [
        ("a-overflow", [f32::MAX, f32::MAX, 0.0]),
        ("b-just-over", [f32::MAX, f32::MAX * 0.0001_f32, 0.0]),
        ("y-maximum", [f32::MAX, 0.0, 0.0]),
        ("z-below", [below_max, 0.0, 0.0]),
    ];
    context.request = Request::PutVectors(dto::PutVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        vectors: records
            .into_iter()
            .map(|(key, data)| dto::PutInputVector {
                key: key.into(),
                data: dto::VectorData { float32: data.into() },
                metadata: Some(serde_json::json!({"source": key})),
            })
            .collect(),
    });
    service.execute(&context).await.expect("publish the finite boundary vectors");
    let expected_keys = ["z-below", "y-maximum", "b-just-over", "a-overflow"];
    for (return_metadata, return_distance) in [(true, true), (false, true), (true, false), (false, false)] {
        context.request = Request::QueryVectors(dto::QueryVectorsInput {
            vector_bucket_name: Some("vectors".into()),
            index_name: Some("service-index".into()),
            index_arn: None,
            top_k: 4,
            query_vector: dto::VectorData { float32: vec![0.0; 3] },
            filter: None,
            query_mode: Some(dto::IndexMode::Enhanced),
            return_metadata: Some(return_metadata),
            return_distance: Some(return_distance),
            next_token: None,
        });
        let VectorExecution::QueryVectors(query) = service.execute(&context).await.expect("query the published boundary vectors")
        else {
            panic!("expected the prepared boundary query");
        };
        assert_eq!(
            query
                .candidates
                .iter()
                .map(|candidate| candidate.key.as_str())
                .collect::<Vec<_>>(),
            expected_keys
        );
        assert!(query.candidates[2].distance_f64 > f64::from(f32::MAX));
        assert_eq!(query.candidates[2].distance_f64 as f32, f32::MAX);
        assert!(query.candidates[3].distance_f64 > query.candidates[2].distance_f64);
        let output = query.into_output().expect("convert the fixed query result");
        assert_eq!(output.distance_metric, dto::DistanceMetric::Euclidean);
        assert!(output.next_token.is_none());
        assert_eq!(output.vectors.iter().map(|vector| vector.key.as_str()).collect::<Vec<_>>(), expected_keys);
        if return_distance {
            assert_eq!(output.vectors[0].distance, Some(below_max));
            assert_eq!(output.vectors[1].distance, Some(f32::MAX));
            assert_eq!(output.vectors[2].distance, Some(f32::INFINITY));
            assert_eq!(output.vectors[3].distance, Some(f32::INFINITY));
        } else {
            assert!(output.vectors.iter().all(|vector| vector.distance.is_none()));
        }
        let body = Response::QueryVectors(output)
            .body_bytes()
            .expect("serialize the protocol response");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("decode the actual response JSON");
        assert_eq!(json["distanceMetric"], "euclidean");
        assert!(json.get("nextToken").is_none());
        for (position, key) in expected_keys.iter().enumerate() {
            let vector = &json["vectors"][position];
            assert_eq!(vector["key"].as_str(), Some(*key));
            if return_metadata {
                assert_eq!(vector["metadata"], serde_json::json!({"source": key}));
            } else {
                assert!(vector.get("metadata").is_none());
            }
            if !return_distance {
                assert!(vector.get("distance").is_none());
            } else if position < 2 {
                assert!(vector["distance"].is_number());
            } else {
                assert_eq!(vector["distance"], "Infinity");
            }
        }
    }
}

#[tokio::test]
async fn service_query_output_retains_snapshot_metadata_and_cosine_metric() {
    use crate::vector::{
        protocol::{Request, Response, dto},
        service::VectorExecution,
    };
    let fixture = catalog_fixture().await;
    let mut context = service_request(&fixture);
    let service = index_service(&fixture);
    service.execute(&context).await.expect("create the cosine Index");
    context.request = Request::PutVectors(dto::PutVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        vectors: vec![
            dto::PutInputVector {
                key: "z-first".into(),
                data: dto::VectorData {
                    float32: vec![1.0, 0.0, 0.0],
                },
                metadata: Some(serde_json::json!({"edition": "original"})),
            },
            dto::PutInputVector {
                key: "a-second".into(),
                data: dto::VectorData {
                    float32: vec![0.0, 1.0, 0.0],
                },
                metadata: Some(serde_json::json!({"edition": "second"})),
            },
        ],
    });
    service.execute(&context).await.expect("publish the original metadata");
    context.request = Request::QueryVectors(dto::QueryVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        top_k: 2,
        query_vector: dto::VectorData {
            float32: vec![1.0, 0.0, 0.0],
        },
        filter: None,
        query_mode: Some(dto::IndexMode::Enhanced),
        return_metadata: Some(true),
        return_distance: Some(true),
        next_token: None,
    });
    let VectorExecution::QueryVectors(query) = service.execute(&context).await.expect("prepare the original snapshot result")
    else {
        panic!("expected the prepared cosine query");
    };
    context.request = Request::PutVectors(dto::PutVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        vectors: vec![dto::PutInputVector {
            key: "z-first".into(),
            data: dto::VectorData {
                float32: vec![1.0, 0.0, 0.0],
            },
            metadata: Some(serde_json::json!({"edition": "replacement"})),
        }],
    });
    service
        .execute(&context)
        .await
        .expect("publish replacement metadata in a new snapshot");
    let output = query.into_output().expect("convert the original fixed snapshot");
    assert_eq!(output.distance_metric, dto::DistanceMetric::Cosine);
    assert_eq!(output.vectors[0].key, "z-first");
    assert_eq!(output.vectors[0].distance, Some(0.0));
    assert_eq!(output.vectors[0].metadata, Some(serde_json::json!({"edition": "original"})));
    assert_eq!(output.vectors[1].key, "a-second");
    assert_eq!(output.vectors[1].distance, Some(1.0));
    assert_eq!(output.vectors[1].metadata, Some(serde_json::json!({"edition": "second"})));
    let body = Response::QueryVectors(output)
        .body_bytes()
        .expect("serialize the cosine response");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("decode the actual cosine response");
    assert_eq!(json["distanceMetric"], "cosine");
    assert_eq!(json["vectors"][0]["metadata"]["edition"], "original");

    context.request = Request::GetVectors(dto::GetVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        keys: vec!["z-first".into()],
        return_data: Some(false),
        return_metadata: Some(true),
    });
    let VectorExecution::GetVectors(get) = service.execute(&context).await.expect("read the current snapshot metadata") else {
        panic!("expected the prepared current GetVectors result");
    };
    assert_eq!(get.requested_keys, vec!["z-first".to_owned()]);
    assert_eq!(get.vectors.len(), 1);
    assert_eq!(get.vectors[0].key, "z-first");
    assert!(get.vectors[0].data.is_none());
    assert_eq!(get.vectors[0].metadata, Some(serde_json::json!({"edition": "replacement"})));
}

#[tokio::test]
async fn service_query_output_requires_pagination_only_above_one_hundred_matches() {
    use crate::vector::{
        protocol::{Request, Response, dto},
        service::{VectorExecution, VectorServiceError},
    };
    let fixture = catalog_fixture().await;
    let mut context = service_request(&fixture);
    let service = index_service(&fixture);
    let Request::CreateIndex(input) = &mut context.request else {
        panic!("expected the Index creation fixture");
    };
    input.distance_metric = dto::DistanceMetric::Euclidean;
    service.execute(&context).await.expect("create the pagination boundary Index");
    context.request = Request::PutVectors(dto::PutVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        vectors: (0..101_u16)
            .map(|ordinal| dto::PutInputVector {
                key: format!("item-{ordinal:03}"),
                data: dto::VectorData {
                    float32: vec![f32::from(ordinal), 0.0, 0.0],
                },
                metadata: Some(serde_json::json!({"ordinal": ordinal, "hundred": ordinal < 100, "short": ordinal < 3})),
            })
            .collect(),
    });
    service.execute(&context).await.expect("publish 101 actual Dataset rows");
    for (top_k, filter, expected_rows) in [
        (101, Some(serde_json::json!({"hundred": true})), 100_usize),
        (101, None, 101),
        (1000, Some(serde_json::json!({"short": true})), 3),
    ] {
        context.request = Request::QueryVectors(dto::QueryVectorsInput {
            vector_bucket_name: Some("vectors".into()),
            index_name: Some("service-index".into()),
            index_arn: None,
            top_k,
            query_vector: dto::VectorData { float32: vec![0.0; 3] },
            filter,
            query_mode: Some(dto::IndexMode::Enhanced),
            return_metadata: Some(true),
            return_distance: Some(true),
            next_token: None,
        });
        let VectorExecution::QueryVectors(query) = service.execute(&context).await.expect("query the fixed Dataset snapshot")
        else {
            panic!("expected the prepared pagination boundary query");
        };
        assert_eq!(query.candidates.len(), expected_rows);
        assert_eq!(query.metadata_rows.as_ref().expect("requested metadata").len(), expected_rows);
        for (ordinal, candidate) in query.candidates.iter().enumerate() {
            assert_eq!(candidate.key, format!("item-{ordinal:03}"));
            assert_eq!(
                candidate.distance_f64,
                f64::from(u16::try_from(ordinal).expect("bounded fixture ordinal"))
            );
        }
        let result = query.into_output();
        if expected_rows == 101 {
            assert!(matches!(result, Err(VectorServiceError::PendingIntegration("query result pagination"))));
            continue;
        }
        let output = result.expect("at most 100 actual matches fit one complete page");
        assert_eq!(output.vectors.len(), expected_rows);
        assert!(output.next_token.is_none());
        assert_eq!(output.distance_metric, dto::DistanceMetric::Euclidean);
        for (ordinal, vector) in output.vectors.iter().enumerate() {
            assert_eq!(vector.key, format!("item-{ordinal:03}"));
            assert_eq!(vector.distance, Some(f32::from(u16::try_from(ordinal).expect("bounded fixture ordinal"))));
            assert_eq!(
                vector.metadata.as_ref().expect("same-snapshot metadata")["ordinal"],
                serde_json::json!(ordinal)
            );
        }
        let body = Response::QueryVectors(output)
            .body_bytes()
            .expect("serialize the complete single page");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("decode the actual single-page response");
        assert_eq!(json["vectors"].as_array().expect("response vectors").len(), expected_rows);
        assert!(json.get("nextToken").is_none());
    }
}

struct ServiceRetryRequestFixture {
    inner: ServiceRequestFixture,
    first_managed_entered: tokio::sync::Notify,
    first_managed_release: tokio::sync::Notify,
    managed_attempts: std::sync::atomic::AtomicUsize,
    authorized_resources: std::sync::Mutex<Vec<crate::table_catalog::CatalogResourceIdentity>>,
}

#[async_trait::async_trait]
impl crate::vector::service::VectorRequestAccess for ServiceRetryRequestFixture {
    fn request(&self) -> &crate::vector::protocol::Request {
        &self.inner.request
    }

    fn request_id(&self) -> crate::vector::service::VectorRequestId {
        crate::vector::service::VectorRequestAccess::request_id(&self.inner)
    }

    fn bucket(&self) -> &crate::storage_api::vector::BucketHandle {
        &self.inner.bucket
    }

    fn index_name(&self) -> &crate::table_catalog::CatalogName {
        &self.inner.name
    }

    async fn authorize_index(
        &self,
        request: &crate::vector::protocol::Request,
        resource: &crate::table_catalog::CatalogResourceIdentity,
        definition: &crate::table_catalog::VectorIndexDefinitionV1,
        operation: Option<&crate::table_catalog::VectorCatalogOperationGuard>,
    ) -> Result<(), crate::vector::service::VectorServiceError> {
        crate::vector::service::VectorRequestAccess::authorize_index(&self.inner, request, resource, definition, operation)
            .await?;
        self.authorized_resources
            .lock()
            .expect("record retry authorization")
            .push(resource.clone());
        Ok(())
    }

    async fn resolve_index_definition(
        &self,
        input: &crate::vector::protocol::dto::CreateIndexInput,
    ) -> Result<crate::table_catalog::VectorIndexDefinitionV1, crate::vector::service::VectorServiceError> {
        crate::vector::service::VectorRequestAccess::resolve_index_definition(&self.inner, input).await
    }

    async fn managed_dataset(
        &self,
        guard: Arc<crate::table_catalog::VectorCatalogOperationGuard>,
        definition: &crate::table_catalog::VectorIndexDefinitionV1,
    ) -> Result<crate::vector::service::ManagedDatasetAccess, crate::vector::service::VectorServiceError> {
        if self.managed_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            self.first_managed_entered.notify_one();
            self.first_managed_release.notified().await;
        }
        crate::vector::service::VectorRequestAccess::managed_dataset(&self.inner, guard, definition).await
    }
}

async fn check_service_retry_resource_identity(delete: bool, recreate: bool) {
    use crate::vector::{
        protocol::{Request, dto},
        service::{VectorExecution, VectorServiceError},
    };
    use std::{sync::atomic::Ordering, time::Duration};

    let fixture = catalog_fixture().await;
    let creation = service_request(&fixture);
    let mut control = creation.clone();
    let service = index_service(&fixture);
    service.execute(&creation).await.expect("create the original Index");
    let seed_request = |edition: &str, target: [f32; 3]| {
        Request::PutVectors(dto::PutVectorsInput {
            vector_bucket_name: Some("vectors".into()),
            index_name: Some("service-index".into()),
            index_arn: None,
            vectors: vec![
                dto::PutInputVector {
                    key: "target".into(),
                    data: dto::VectorData { float32: target.into() },
                    metadata: Some(serde_json::json!({"edition": edition})),
                },
                dto::PutInputVector {
                    key: "keep".into(),
                    data: dto::VectorData {
                        float32: vec![1.0, 0.0, 0.0],
                    },
                    metadata: Some(serde_json::json!({"edition": "keep"})),
                },
            ],
        })
    };
    control.request = seed_request("original", [1.0, 0.0, 0.0]);
    service.execute(&control).await.expect("publish the original target");
    let namespace = crate::table_catalog::Namespace::parse("indexes").expect("Index namespace");
    let (original_guard, original_entry) = fixture
        .store
        .begin_vector_operation(&control.bucket, &namespace, &control.name)
        .await
        .expect("read the original identity and head");
    let original_resource = original_guard.resource_identity().clone();
    drop(original_guard);
    let (catalog_bucket, table_path) = {
        let state = fixture.backend.state.lock().await;
        let entries = state
            .objects
            .iter()
            .filter_map(|(key, record)| {
                let entry = serde_json::from_slice::<crate::table_catalog::TableEntry>(&record.data).ok()?;
                (entry.table_id == original_resource.table_id().as_str() && entry.table == control.name.as_str())
                    .then(|| key.clone())
            })
            .collect::<Vec<_>>();
        assert_eq!(entries.len(), 1, "locate the authoritative entry for the original Index");
        entries[0].clone()
    };

    let mut delayed_context = control.clone();
    delayed_context.request = if delete {
        Request::DeleteVectors(dto::DeleteVectorsInput {
            vector_bucket_name: Some("vectors".into()),
            index_name: Some("service-index".into()),
            index_arn: None,
            keys: vec!["target".into()],
        })
    } else {
        Request::PutVectors(dto::PutVectorsInput {
            vector_bucket_name: Some("vectors".into()),
            index_name: Some("service-index".into()),
            index_arn: None,
            vectors: vec![dto::PutInputVector {
                key: "target".into(),
                data: dto::VectorData {
                    float32: vec![0.0, 1.0, 0.0],
                },
                metadata: Some(serde_json::json!({"edition": "delayed"})),
            }],
        })
    };
    let delayed_context = ServiceRetryRequestFixture {
        inner: delayed_context,
        first_managed_entered: tokio::sync::Notify::new(),
        first_managed_release: tokio::sync::Notify::new(),
        managed_attempts: std::sync::atomic::AtomicUsize::new(0),
        authorized_resources: std::sync::Mutex::new(Vec::new()),
    };
    let mut delayed = Box::pin(service.execute(&delayed_context));
    tokio::select! {
        result = &mut delayed => panic!("write returned before the first Managed bind: {:?}", result.err()),
        entered = tokio::time::timeout(Duration::from_secs(10), delayed_context.first_managed_entered.notified()) => {
            entered.expect("write captures its original Catalog identity before pausing");
        },
    }
    control.request = Request::TagResource(dto::TagResourceInput {
        resource_arn: "arn:aws:s3vectors:us-east-1::bucket/vectors/index/service-index".into(),
        tags: [("conflict".into(), "winner".into())].into(),
    });
    service
        .execute(&control)
        .await
        .expect("publish a competing head for the same identity");
    let (tagged_guard, tagged_entry) = fixture
        .store
        .begin_vector_operation(&control.bucket, &namespace, &control.name)
        .await
        .expect("read the competing head");
    assert_eq!(tagged_guard.resource_identity(), &original_resource);
    assert_ne!(tagged_entry.head().expected_head(), original_entry.head().expected_head());
    drop(tagged_guard);

    // Capture the competing head before allowing deletion and name reuse.
    let head_pause = fixture.backend.pause_next_read(&catalog_bucket, &table_path).await;
    delayed_context.first_managed_release.notify_one();
    tokio::select! {
        result = &mut delayed => panic!("write returned before the captured conflicting head: {:?}", result.err()),
        entered = tokio::time::timeout(Duration::from_secs(10), head_pause.wait_started()) => {
            entered.expect("write reads the definite head conflict before retrying");
        },
    }

    let replacement = if recreate {
        tokio::time::timeout(Duration::from_secs(30), async {
            control.request = Request::DeleteIndex(dto::DeleteIndexInput {
                vector_bucket_name: Some("vectors".into()),
                index_name: Some("service-index".into()),
                index_arn: None,
            });
            service.execute(&control).await.expect("logically delete the original Index");
            service.execute(&creation).await.expect("recreate the same Index name");
            control.request = seed_request("replacement", [0.0, 0.0, 1.0]);
            service.execute(&control).await.expect("publish the replacement Index target");
        })
        .await
        .expect("Index deletion and recreation complete while the original read is paused");
        let (guard, entry) = fixture
            .store
            .begin_vector_operation(&control.bucket, &namespace, &control.name)
            .await
            .expect("capture the replacement Index");
        assert_ne!(guard.resource_identity(), &original_resource);
        drop(guard);
        Some(entry)
    } else {
        None
    };
    let before_retry = {
        let state = fixture.backend.state.lock().await;
        (state.objects.clone(), state.put_attempts.clone())
    };
    head_pause.release();
    let result = tokio::time::timeout(Duration::from_secs(30), &mut delayed)
        .await
        .expect("the write finishes its conflict retry");
    drop(delayed);
    if let Some(replacement) = replacement {
        assert!(matches!(
            result,
            Err(VectorServiceError::Manifest(LanceManifestError::ResourceMismatch {
                reason: "Vector retry resource identity changed"
            }))
        ));
        assert_eq!(delayed_context.managed_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(
            *delayed_context
                .authorized_resources
                .lock()
                .expect("retry authorization observations"),
            vec![original_resource.clone()]
        );
        let state = fixture.backend.state.lock().await;
        assert_eq!((state.objects.clone(), state.put_attempts.clone()), before_retry);
        drop(state);
        let (guard, after) = fixture
            .store
            .begin_vector_operation(&control.bucket, &namespace, &control.name)
            .await
            .expect("read the unchanged replacement head");
        assert_eq!(after.head().resource_identity(), replacement.head().resource_identity());
        assert_eq!(after.head().namespace(), replacement.head().namespace());
        assert_eq!(after.head().name(), replacement.head().name());
        assert_eq!(after.head().expected_head(), replacement.head().expected_head());
        drop(guard);
    } else {
        result.expect("a definite conflict retries successfully within the original identity");
        assert_eq!(delayed_context.managed_attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            *delayed_context
                .authorized_resources
                .lock()
                .expect("retry authorization observations"),
            vec![original_resource.clone(), original_resource.clone()]
        );
    }

    control.request = Request::QueryVectors(dto::QueryVectorsInput {
        vector_bucket_name: Some("vectors".into()),
        index_name: Some("service-index".into()),
        index_arn: None,
        top_k: 10,
        query_vector: dto::VectorData {
            float32: vec![1.0, 0.0, 0.0],
        },
        filter: None,
        query_mode: Some(dto::IndexMode::Enhanced),
        return_metadata: Some(true),
        return_distance: Some(true),
        next_token: None,
    });
    let VectorExecution::QueryVectors(query) = service
        .execute(&control)
        .await
        .expect("read the authoritative Dataset after retry")
    else {
        panic!("expected the actual Dataset query result");
    };
    assert!(query.candidates.iter().any(|candidate| candidate.key == "keep"));
    if delete && !recreate {
        assert_eq!(query.candidates.len(), 1);
        assert!(query.candidates.iter().all(|candidate| candidate.key != "target"));
    } else {
        assert_eq!(query.candidates.len(), 2);
        let target = query
            .snapshot
            .get(&["target".into()], all_vector_fields())
            .await
            .expect("read the actual target bytes");
        assert_eq!(target.len(), 1);
        assert_eq!(target[0].key, "target");
        assert_eq!(target[0].data, Some(if recreate { vec![0.0, 0.0, 1.0] } else { vec![0.0, 1.0, 0.0] }));
        assert_eq!(
            target[0].metadata.as_ref().expect("target metadata")["edition"],
            if recreate { "replacement" } else { "delayed" }
        );
    }
}

#[tokio::test]
async fn service_put_retry_rejects_recreated_index() {
    check_service_retry_resource_identity(false, true).await;
}

#[tokio::test]
async fn service_delete_retry_rejects_recreated_index() {
    check_service_retry_resource_identity(true, true).await;
}

#[tokio::test]
async fn service_put_retry_succeeds_for_same_identity() {
    check_service_retry_resource_identity(false, false).await;
}

#[tokio::test]
async fn service_delete_retry_succeeds_for_same_identity() {
    check_service_retry_resource_identity(true, false).await;
}

#[test]
fn metadata_byte_limits_validate_complete_records_before_batch_creation() {
    use super::dataset::{DatasetError, VectorRecord, record_batch};

    let index = index_spec();
    for (key, maximum) in [("private", 40_960), ("group", 2_048)] {
        for (unit, encoded_bytes) in [("x", 1), ("é", 2), ("\"\\\n", 6)] {
            for size in [maximum - 1, maximum, maximum + 1] {
                // The ASCII key plus braces, quotes and colon has seven bytes of JSON overhead.
                let payload_bytes = size - key.len() - 7;
                let value = unit.repeat(payload_bytes / encoded_bytes) + &"x".repeat(payload_bytes % encoded_bytes);
                let metadata = serde_json::json!({(key): value}).as_object().unwrap().clone();
                assert_eq!(serde_json::to_vec(&metadata).unwrap().len(), size);
                let records = [
                    VectorRecord {
                        key: "first-valid".into(),
                        data: vec![1.0, 2.0, 3.0],
                        metadata: None,
                    },
                    VectorRecord {
                        key: "sized".into(),
                        data: vec![1.0, 2.0, 3.0],
                        metadata: Some(metadata),
                    },
                ];
                let result = record_batch(&records, &index);
                if size <= maximum {
                    assert_eq!(result.unwrap().num_rows(), 2);
                } else {
                    let error = result.expect_err("one oversized record must reject the complete batch");
                    assert!(matches!(error, DatasetError::InvalidVector(_)), "{error:?}");
                }
            }
        }
    }
}

#[test]
fn metadata_non_filterable_exclusion_only_removes_configured_root_fields() {
    use super::dataset::{DatasetError, VectorRecord, record_batch};

    let index = index_spec();
    // Combining the two single-field JSON objects replaces two braces with one comma.
    let both_limits = serde_json::json!({
        "group": "g".repeat(2_048 - 12),
        "private": "p".repeat(40_960 - 2_048 - 13),
    });
    assert_eq!(serde_json::to_vec(&both_limits).unwrap().len(), 40_960);
    let mut record = VectorRecord {
        key: "both-limits".into(),
        data: vec![1.0, 2.0, 3.0],
        metadata: Some(both_limits.as_object().unwrap().clone()),
    };
    assert_eq!(record_batch(&[record.clone()], &index).unwrap().num_rows(), 1);
    record
        .metadata
        .as_mut()
        .unwrap()
        .insert("group".into(), serde_json::json!("g".repeat(2_048 - 11)));
    record
        .metadata
        .as_mut()
        .unwrap()
        .insert("private".into(), serde_json::json!("p"));
    let error = record_batch(&[record.clone()], &index).expect_err("filterable limit applies independently of total size");
    assert!(matches!(error, DatasetError::InvalidVector(_)), "{error:?}");
    record.metadata = Some(
        serde_json::json!({"group":{"private":"x".repeat(2_048)}})
            .as_object()
            .unwrap()
            .clone(),
    );
    let error = record_batch(&[record], &index).expect_err("nested names do not inherit the root non-filterable exclusion");
    assert!(matches!(error, DatasetError::InvalidVector(_)), "{error:?}");
}
