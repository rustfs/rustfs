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

#[test]
fn catalog_prepared_metadata_binds_bytes_and_revision() {
    let key = CatalogObjectKey::try_new(
        ".rustfs-table/8b35cb4c-5b76-40ba-bb83-112be429b79c/dataset/_versions/attempt.manifest".to_string(),
    )
    .unwrap();
    let object = TableCatalogObject {
        data: b"manifest-content".to_vec(),
        body_revision: Some(Uuid::new_v4()),
        etag: Some("etag".to_string()),
        mod_time: None,
    };
    let reference = CatalogMetadataRef::from_read(key.clone(), &object).unwrap();
    assert_eq!(reference.content_length(), object.data.len() as u64);
    assert_eq!(reference.object_key(), &key);
    assert_eq!(reference.body_revision().unwrap().as_uuid(), object.body_revision.unwrap());
    let mut replacement = object.clone();
    replacement.data[0] ^= 1;
    assert_ne!(reference, CatalogMetadataRef::from_read(key.clone(), &replacement).unwrap());
    replacement = object;
    replacement.body_revision = Some(Uuid::new_v4());
    assert_ne!(reference, CatalogMetadataRef::from_read(key.clone(), &replacement).unwrap());
    replacement.body_revision = Some(Uuid::nil());
    assert_matches!(CatalogMetadataRef::from_read(key, &replacement), Err(TableCatalogStoreError::Invalid(_)));
}

#[test]
fn catalog_prepared_descriptor_rejects_unsupported_envelope() {
    let reference = CatalogMetadataRef::from_read(
        CatalogObjectKey::try_new(
            ".rustfs-table/8b35cb4c-5b76-40ba-bb83-112be429b79c/dataset/_versions/attempt.manifest".to_string(),
        )
        .unwrap(),
        &TableCatalogObject {
            data: vec![1],
            body_revision: None,
            etag: None,
            mod_time: None,
        },
    )
    .unwrap();
    assert_matches!(
        CatalogFormatDescriptor::try_new(2, CatalogFormat::Lance, 1, reference.clone(), vec![1]),
        Err(TableCatalogStoreError::Unsupported(_))
    );
    assert_matches!(
        CatalogFormatDescriptor::try_new(1, CatalogFormat::Lance, 1, reference.clone(), Vec::new()),
        Err(TableCatalogStoreError::Invalid(_))
    );
    let descriptor = CatalogFormatDescriptor::try_new(1, CatalogFormat::Lance, 1, reference.clone(), vec![1]).unwrap();
    assert_eq!(descriptor.metadata(), &reference);
}

#[test]
fn catalog_names_preserve_table_rules_and_accept_safe_vector_names() {
    assert_eq!(CatalogName::parse("index.v1").unwrap().as_str(), "index.v1");
    assert!(IdentifierSegment::parse("index.v1").is_err());
    for name in ["", ".", "..", "a/b", "a\\b", "a%2fb", "a\0b"] {
        assert!(CatalogName::parse(name).is_err(), "accepted {name:?}");
    }
}

fn vector_bucket() -> BucketHandle {
    use crate::storage_api::table::managed::{AccountNamespaceId, BucketIncarnation, BucketPublicName, StorageBucketId};
    BucketHandle::try_from_parts(
        BucketKind::Vector,
        AccountNamespaceId::try_new(String::new()).unwrap(),
        "us-east-1".to_string(),
        BucketPublicName::try_new("vectors".to_string()).unwrap(),
        StorageBucketId::try_new("vector-storage".to_string()).unwrap(),
        BucketIncarnation::try_new(Uuid::from_u128(0x8b35cb4c5b7640babb83112be429b79c)).unwrap(),
    )
    .unwrap()
}

struct PreparedFixture {
    backend: TestCatalogObjectBackend,
    store: ObjectTableCatalogStore<TestCatalogObjectBackend>,
    bucket: BucketHandle,
    entry: TableEntry,
    initial: CatalogMetadataRef,
    initial_descriptor: CatalogFormatDescriptor,
}

async fn prepared_fixture() -> PreparedFixture {
    use crate::storage_api::table::managed::ManagedTableId;
    let backend = TestCatalogObjectBackend::default();
    let bucket = vector_bucket();
    let bucket_id = bucket.storage_bucket_id().as_str();
    backend
        .state
        .lock()
        .await
        .bucket_handles
        .insert(bucket_id.to_string(), bucket.clone());
    let store = ObjectTableCatalogStore::for_vector_scope(backend.clone(), bucket.clone())
        .await
        .unwrap();
    let resource = CatalogResourceIdentity::try_new(
        bucket.clone(),
        ManagedTableId::try_new("ea3a1301-903d-4c58-a26c-aae0a4a42f20".to_string()).unwrap(),
        CatalogFormat::Lance,
    )
    .unwrap();
    let key = format!("{}_versions/71bbbc9f-d8e0-436f-8076-526320dd93d8/1.manifest", resource.dataset_prefix());
    // Algorithm fixtures exercise byte binding; official writer/reader fixtures live in vector::lance.
    backend.seed_object(bucket_id, &key, b"first-content".to_vec()).await;
    let object = backend.read_object(bucket_id, &key).await.unwrap().unwrap();
    let initial = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(key.clone()).unwrap(), &object).unwrap();
    let initial_descriptor =
        CatalogFormatDescriptor::try_new(1, CatalogFormat::Lance, 1, initial.clone(), b"opaque-initial-descriptor".to_vec())
            .unwrap();
    let mut bucket_entry = test_bucket_entry(bucket_id);
    bucket_entry.version = 2;
    bucket_entry.catalog_type = "s3vectors".to_string();
    bucket_entry.vector_bucket_binding = Some(VectorBucketBindingV1::from_handle(&bucket).unwrap());
    store.put_table_bucket(bucket_entry).await.unwrap();
    let namespace = Namespace::parse("indexes").unwrap();
    store
        .create_namespace(test_namespace_entry(bucket_id, &namespace))
        .await
        .unwrap();
    let entry = TableEntry {
        version: 2,
        table_bucket: bucket_id.to_string(),
        namespace: "indexes".to_string(),
        table: "index.v1".to_string(),
        table_id: resource.table_id().as_str().to_string(),
        table_uuid: resource.table_id().as_str().to_string(),
        format: "lance".to_string(),
        format_version: 1,
        warehouse_location: format!("s3://{bucket_id}/{}", resource.dataset_prefix().trim_end_matches('/')),
        metadata_location: key,
        version_token: "initial-token".to_string(),
        generation: 1,
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
        vector_index_definition: None,
        vector_head_binding: Some(VectorHeadBindingV1::try_new(&resource, &initial, &initial_descriptor).unwrap()),
    };
    store.create_table(entry.clone()).await.unwrap();
    PreparedFixture {
        backend,
        store,
        bucket,
        entry,
        initial,
        initial_descriptor,
    }
}

async fn prepared_candidate(fixture: &PreparedFixture, commit_id: &str, bytes: &[u8]) -> PreparedCatalogCommit {
    use crate::storage_api::table::managed::ManagedTableId;
    let resource = CatalogResourceIdentity::try_new(
        fixture.bucket.clone(),
        ManagedTableId::try_new(fixture.entry.table_id.clone()).unwrap(),
        CatalogFormat::Lance,
    )
    .unwrap();
    let key = format!("{}_versions/{commit_id}/2.manifest", resource.dataset_prefix());
    fixture
        .backend
        .seed_object(fixture.bucket.storage_bucket_id().as_str(), &key, bytes.to_vec())
        .await;
    let object = fixture
        .backend
        .read_object(fixture.bucket.storage_bucket_id().as_str(), &key)
        .await
        .unwrap()
        .unwrap();
    let candidate = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(key).unwrap(), &object).unwrap();
    let descriptor =
        CatalogFormatDescriptor::try_new(1, CatalogFormat::Lance, 1, candidate.clone(), b"opaque-candidate-descriptor".to_vec())
            .unwrap();
    PreparedCatalogCommit::try_from_validated_parts(
        CatalogCommitParts {
            resource_identity: resource,
            namespace: Namespace::parse("indexes").unwrap(),
            name: CatalogName::parse("index.v1").unwrap(),
            expected_head: CatalogExpectedHead::from_entry(
                &fixture.entry,
                Some(fixture.initial.clone()),
                Some(fixture.initial_descriptor.clone()),
            )
            .unwrap(),
            commit_id: CatalogCommitId::try_new(commit_id.to_string()).unwrap(),
            operation: "append".to_string(),
            idempotency_key: Some(commit_id.to_string()),
            writer: None,
        },
        candidate,
        descriptor,
        CatalogEntryUpdates::try_new(Some(fixture.entry.warehouse_location.clone()), Some(1)).unwrap(),
        Vec::new(),
    )
    .unwrap()
}

#[tokio::test]
async fn catalog_prepared_commit_rejects_candidate_replacement() {
    let fixture = prepared_fixture().await;
    let prepared = prepared_candidate(&fixture, "ec1c1a6d-e352-4c3f-b14f-b40ad3a4a448", b"second-content").await;
    fixture
        .backend
        .seed_object(
            fixture.bucket.storage_bucket_id().as_str(),
            prepared.candidate_metadata().object_key().as_str(),
            b"changed-bytes!".to_vec(),
        )
        .await;
    assert_matches!(fixture.store.commit_prepared(prepared).await, Err(TableCatalogStoreError::Conflict(_)));
    let head = fixture
        .store
        .read_catalog_head(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head.metadata(), &fixture.initial);
    assert_eq!(head.generation().get(), 1);
}

#[tokio::test]
async fn catalog_prepared_commit_rejects_stale_head_and_reads_committed_history() {
    let fixture = prepared_fixture().await;
    let first = prepared_candidate(&fixture, "ec1c1a6d-e352-4c3f-b14f-b40ad3a4a448", b"second-content").await;
    let stale = prepared_candidate(&fixture, "e7734ca3-3c7e-4cbd-bf49-dab90eec4b9e", b"uncommitted-content").await;
    let candidate = first.candidate_metadata().clone();
    let committed = fixture.store.commit_prepared(first.clone()).await.unwrap();
    assert_eq!(committed.table.generation, 2);
    assert_matches!(fixture.store.commit_prepared(stale).await, Err(TableCatalogStoreError::Conflict(_)));
    assert_eq!(fixture.store.commit_prepared(first).await.unwrap(), committed);
    let head = fixture
        .store
        .read_catalog_head(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head.metadata(), &candidate);
    let history = fixture.store.read_committed_history(&head).await.unwrap();
    assert_eq!(history.versions().len(), 2);
    assert_eq!(history.versions()[0].metadata(), &candidate);
    assert_eq!(history.versions()[1].metadata(), &fixture.initial);
    assert!(history.versions()[1].commit_id().is_none());
}

#[tokio::test]
async fn catalog_vector_scope_rejects_table_rename_recovery() {
    let fixture = prepared_fixture().await;
    fixture.backend.reset_call_counts().await;
    let mut cursor = None;
    assert_matches!(
        fixture.store.next_table_bucket_for_rename_recovery(&mut cursor).await,
        Err(TableCatalogStoreError::Unsupported(_))
    );
    assert_matches!(
        fixture
            .store
            .recover_pending_table_rename(fixture.bucket.storage_bucket_id().as_str())
            .await,
        Err(TableCatalogStoreError::Unsupported(_))
    );
    assert!(cursor.is_none());
    assert_eq!(fixture.backend.list_call_count().await, 0);
    assert_eq!(fixture.backend.read_call_count().await, 0);
}

#[tokio::test]
async fn catalog_rename_recovery_discovery_preserves_scope_and_bucket_type() {
    let fixture = prepared_fixture().await;
    let table_store = ObjectTableCatalogStore::new(fixture.backend.clone());
    let table_bucket = test_bucket_entry("analytics");
    table_store.put_table_bucket(table_bucket.clone()).await.unwrap();
    let mut cursor = None;
    assert_eq!(
        table_store.next_table_bucket_for_rename_recovery(&mut cursor).await.unwrap(),
        Some(table_bucket)
    );
    assert!(
        table_store
            .next_table_bucket_for_rename_recovery(&mut cursor)
            .await
            .unwrap()
            .is_none()
    );
    assert!(cursor.is_none());

    let bucket_id = fixture.bucket.storage_bucket_id().as_str();
    let vector_object = fixture.store.paths.table_bucket_entry_path(bucket_id);
    let vector_bytes = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &vector_object)
        .await
        .unwrap()
        .unwrap();
    fixture
        .backend
        .seed_object(
            RUSTFS_META_BUCKET,
            &table_store.paths.table_bucket_entry_path(bucket_id),
            vector_bytes.data.clone(),
        )
        .await;
    let mut invalid = 0;
    let mut found = 0;
    for _ in 0..3 {
        match table_store.next_table_bucket_for_rename_recovery(&mut cursor).await {
            Ok(Some(entry)) => {
                assert_eq!(entry.table_bucket, "analytics");
                found += 1;
            }
            Ok(None) => break,
            Err(TableCatalogStoreError::Invalid(_)) => invalid += 1,
            Err(error) => panic!("unexpected discovery failure: {error}"),
        }
    }
    assert_eq!(found, 1);
    assert_eq!(invalid, 1);
    assert!(cursor.is_none());
    assert_eq!(
        fixture.backend.read_object(RUSTFS_META_BUCKET, &vector_object).await.unwrap(),
        Some(vector_bytes)
    );
}

#[tokio::test]
async fn catalog_vector_scope_rejects_table_backup() {
    let fixture = prepared_fixture().await;
    assert_matches!(
        fixture
            .store
            .create_object_catalog_backup(fixture.bucket.storage_bucket_id().as_str(), None)
            .await,
        Err(TableCatalogStoreError::Unsupported(_))
    );
    let paths = TableCatalogObjectPaths::default();
    assert!(
        fixture
            .backend
            .state
            .lock()
            .await
            .objects
            .keys()
            .all(|(bucket, key)| bucket != RUSTFS_META_BUCKET
                || !key.starts_with(&paths.namespace_entries_prefix(fixture.bucket.storage_bucket_id().as_str())))
    );
}

#[tokio::test]
async fn catalog_prepared_commit_rejects_body_revision_replacement() {
    let fixture = prepared_fixture().await;
    let commit_id = "ec1c1a6d-e352-4c3f-b14f-b40ad3a4a448";
    let first = prepared_candidate(&fixture, commit_id, b"second-content").await;
    let key = first.candidate_metadata().object_key().as_str().to_string();
    let bucket = fixture.bucket.storage_bucket_id().as_str().to_string();
    let revision = Uuid::new_v4();
    fixture
        .backend
        .state
        .lock()
        .await
        .objects
        .get_mut(&(bucket.clone(), key.clone()))
        .unwrap()
        .body_revision = Some(revision);
    let object = fixture.backend.read_object(&bucket, &key).await.unwrap().unwrap();
    let metadata = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(key.clone()).unwrap(), &object).unwrap();
    let descriptor = CatalogFormatDescriptor::try_new(
        1,
        CatalogFormat::Lance,
        1,
        metadata.clone(),
        first.format_descriptor().payload().to_vec(),
    )
    .unwrap();
    let prepared = PreparedCatalogCommit::try_from_validated_parts(
        CatalogCommitParts {
            resource_identity: first.resource_identity().clone(),
            namespace: first.namespace().clone(),
            name: first.name().clone(),
            expected_head: first.expected_head().clone(),
            commit_id: first.commit_id().clone(),
            operation: first.operation().to_string(),
            idempotency_key: first.idempotency_key().map(str::to_string),
            writer: None,
        },
        metadata,
        descriptor,
        first.entry_updates().clone(),
        Vec::new(),
    )
    .unwrap();
    let before = fixture
        .backend
        .state
        .lock()
        .await
        .objects
        .get(&(bucket.clone(), key.clone()))
        .unwrap()
        .clone();
    fixture
        .backend
        .state
        .lock()
        .await
        .objects
        .get_mut(&(bucket.clone(), key.clone()))
        .unwrap()
        .body_revision = Some(Uuid::new_v4());
    let after = fixture
        .backend
        .state
        .lock()
        .await
        .objects
        .get(&(bucket, key))
        .unwrap()
        .clone();
    assert_eq!(before.data, after.data);
    assert_eq!(before.etag, after.etag);
    assert_matches!(fixture.store.commit_prepared(prepared).await, Err(TableCatalogStoreError::Conflict(_)));
    assert_eq!(
        fixture
            .store
            .load_table(fixture.bucket.storage_bucket_id().as_str(), "indexes", "index.v1")
            .await
            .unwrap()
            .unwrap(),
        fixture.entry
    );
}

#[tokio::test]
async fn catalog_vector_scope_is_independent_of_table_backup_fence() {
    let fixture = prepared_fixture().await;
    let prepared = prepared_candidate(&fixture, "ec1c1a6d-e352-4c3f-b14f-b40ad3a4a448", b"second-content").await;
    let table_paths = TableCatalogObjectPaths::default();
    let guard = fixture
        .backend
        .acquire_write_lock(
            RUSTFS_META_BUCKET,
            &table_paths.catalog_backup_fence_lock_path(fixture.bucket.storage_bucket_id().as_str()),
        )
        .await
        .unwrap();
    let committed = tokio::time::timeout(StdDuration::from_secs(5), fixture.store.commit_prepared(prepared))
        .await
        .expect("Vector commit must not wait for the Table backup fence")
        .unwrap();
    assert_eq!(committed.table.generation, 2);
    drop(guard);
}

#[tokio::test]
async fn catalog_vector_history_rejects_ambiguous_chain() {
    let fixture = prepared_fixture().await;
    let prepared = prepared_candidate(&fixture, "ec1c1a6d-e352-4c3f-b14f-b40ad3a4a448", b"second-content").await;
    let committed = fixture.store.commit_prepared(prepared).await.unwrap();
    let mut duplicate = committed.commit_log;
    duplicate.commit_id = "e7734ca3-3c7e-4cbd-bf49-dab90eec4b9e".to_string();
    duplicate.idempotency_key = None;
    let paths = super::super::store::TableCatalogObjectPaths::for_vector();
    let object =
        paths.commit_log_entry_path(fixture.bucket.storage_bucket_id().as_str(), &fixture.entry.table_id, &duplicate.commit_id);
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &object, serde_json::to_vec(&duplicate).unwrap())
        .await;
    let head = fixture
        .store
        .read_catalog_head(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .unwrap();
    assert_matches!(fixture.store.read_committed_history(&head).await, Err(TableCatalogStoreError::Invalid(_)));
}

#[tokio::test]
async fn catalog_prepared_commit_rejects_recreated_bucket_without_catalog_writes() {
    use crate::storage_api::table::managed::BucketIncarnation;
    let fixture = prepared_fixture().await;
    let prepared = prepared_candidate(&fixture, "ec1c1a6d-e352-4c3f-b14f-b40ad3a4a448", b"second-content").await;
    let recreated = BucketHandle::try_from_parts(
        fixture.bucket.kind(),
        fixture.bucket.account_namespace_id().clone(),
        fixture.bucket.region().to_string(),
        fixture.bucket.public_name().clone(),
        fixture.bucket.storage_bucket_id().clone(),
        BucketIncarnation::try_new(Uuid::new_v4()).unwrap(),
    )
    .unwrap();
    let before = {
        let mut state = fixture.backend.state.lock().await;
        state
            .bucket_handles
            .insert(recreated.storage_bucket_id().as_str().to_string(), recreated);
        state.put_attempts.clone()
    };
    assert_matches!(fixture.store.commit_prepared(prepared).await, Err(TableCatalogStoreError::Conflict(_)));
    assert_eq!(fixture.backend.state.lock().await.put_attempts, before);
}

#[tokio::test]
async fn catalog_vector_head_rejects_nonempty_account_and_content_mismatch() {
    for mutation in ["account", "sha256", "descriptor_metadata"] {
        let fixture = prepared_fixture().await;
        let mut entry = fixture.entry.clone();
        let binding = entry.vector_head_binding.as_mut().unwrap();
        match mutation {
            "account" => binding.bucket.account_namespace_id = "123456789012".to_string(),
            "sha256" => binding.metadata.content_sha256 = "invalid-digest".to_string(),
            "descriptor_metadata" => binding.descriptor.metadata.content_length += 1,
            _ => unreachable!(),
        }
        let namespace = Namespace::parse("indexes").unwrap();
        let name = CatalogName::parse("index.v1").unwrap();
        let paths = super::super::store::TableCatalogObjectPaths::for_vector();
        let key = paths.table_entry_path(fixture.bucket.storage_bucket_id().as_str(), &namespace, &name);
        fixture
            .backend
            .seed_object(RUSTFS_META_BUCKET, &key, serde_json::to_vec(&entry).unwrap())
            .await;
        assert_matches!(
            fixture.store.read_catalog_head(&fixture.bucket, &namespace, &name).await,
            Err(TableCatalogStoreError::Invalid(_))
        );
    }
}

#[test]
fn catalog_managed_io_preserves_classification_and_source() {
    use std::error::Error;
    let error = TableCatalogStoreError::Io {
        classification: CatalogIoErrorKind::Unavailable,
        context: "read candidate",
        source: Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "upstream timeout")),
    };
    assert!(error.is_unavailable());
    assert_eq!(
        error.source().unwrap().downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::TimedOut
    );
    assert!(!error.to_string().contains("upstream timeout"));
}

#[tokio::test]
async fn catalog_vector_replay_rejects_detached_committed_record() {
    let fixture = prepared_fixture().await;
    let loser = prepared_candidate(&fixture, "ec1c1a6d-e352-4c3f-b14f-b40ad3a4a448", b"loser-content").await;
    let winner = prepared_candidate(&fixture, "e7734ca3-3c7e-4cbd-bf49-dab90eec4b9e", b"winner-content").await;
    let paths = super::super::store::TableCatalogObjectPaths::for_vector();
    let bucket = fixture.bucket.storage_bucket_id().as_str();
    let table_path = paths.table_entry_path(bucket, loser.namespace(), loser.name());
    fixture.backend.fail_next_put(RUSTFS_META_BUCKET, &table_path).await;
    assert_matches!(
        fixture.store.commit_prepared(loser.clone()).await,
        Err(TableCatalogStoreError::Internal(_))
    );
    let committed = fixture.store.commit_prepared(winner).await.unwrap();
    let mut detached = fixture
        .store
        .get_commit_by_id(bucket, &fixture.entry.table_id, loser.commit_id().as_str())
        .await
        .unwrap()
        .unwrap();
    detached.status = CommitLogStatus::Committed;
    let bytes = serde_json::to_vec(&detached).unwrap();
    for key in [
        paths.commit_log_entry_path(bucket, &fixture.entry.table_id, loser.commit_id().as_str()),
        paths.commit_idempotency_entry_path(bucket, &fixture.entry.table_id, loser.idempotency_key().unwrap()),
    ] {
        fixture.backend.seed_object(RUSTFS_META_BUCKET, &key, bytes.clone()).await;
    }
    assert_matches!(fixture.store.commit_prepared(loser).await, Err(TableCatalogStoreError::Invalid(_)));
    assert_eq!(
        fixture
            .store
            .load_table(bucket, "indexes", "index.v1")
            .await
            .unwrap()
            .unwrap(),
        committed.table
    );
}

#[tokio::test]
async fn vector_commit_recovery_plan_accepts_index_name_with_period() {
    let fixture = prepared_fixture().await;
    let prepared = prepared_candidate(&fixture, "391bc806-79bf-4a42-afc8-2f573b877cc1", b"recovery-plan-content").await;
    let committed = fixture.store.commit_prepared(prepared).await.unwrap();
    let report = fixture
        .store
        .plan_table_commit_recovery(fixture.bucket.storage_bucket_id().as_str(), "indexes", "index.v1")
        .await
        .expect("Vector recovery plan accepts the persisted Catalog name");
    assert_eq!(report.table_id, fixture.entry.table_id);
    assert_eq!(report.current_metadata_location, committed.table.metadata_location);
    assert_eq!(report.current_generation, committed.table.generation);
    assert_eq!(report.commits.len(), 1);
    assert_eq!(report.commits[0].commit_id, committed.commit_log.commit_id);
    assert_eq!(report.commits[0].recovery_state, TableCommitRecoveryState::Committed);
    assert_eq!(report.manual_review_count, 0);
}

#[tokio::test]
async fn vector_commit_recovery_accepts_index_name_with_period() {
    let fixture = prepared_fixture().await;
    let prepared = prepared_candidate(&fixture, "3cfb3596-0aa4-429d-819b-6e7147488fb4", b"recovery-content").await;
    let committed = fixture.store.commit_prepared(prepared).await.unwrap();
    let report = fixture
        .store
        .recover_table_commits(fixture.bucket.storage_bucket_id().as_str(), "indexes", "index.v1")
        .await
        .expect("Vector recovery accepts the persisted Catalog name");
    assert_eq!(report.table_id, fixture.entry.table_id);
    assert_eq!(report.current_metadata_location, committed.table.metadata_location);
    assert_eq!(report.current_generation, committed.table.generation);
    assert_eq!(report.commits.len(), 1);
    assert_eq!(report.commits[0].commit_id, committed.commit_log.commit_id);
    assert_eq!(report.commits[0].recovery_state, TableCommitRecoveryState::Committed);
    assert_eq!(report.finalized_count, 0);
    assert_eq!(report.manual_review_count, 0);
}

fn v3_definition_json(tags: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "effective_encryption": {"kind": "unencrypted"},
        "tags": tags,
    })
}

fn v3_entry_json(entry: &TableEntry, definition: serde_json::Value) -> serde_json::Value {
    let mut value = serde_json::to_value(entry).unwrap();
    value["version"] = 3.into();
    value["vector_index_definition"] = definition;
    value
}

fn v3_mutation_json(kind: &str, expected_definition: serde_json::Value, next_definition: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "kind": kind,
        "expected_definition": expected_definition,
        "next_definition": next_definition,
        "expected_state": "ACTIVE",
        "next_state": "ACTIVE",
    })
}

#[tokio::test]
async fn catalog_vector_v3_reads_complete_definition_and_exact_head() {
    let fixture = prepared_fixture().await;
    let definition = v3_definition_json(serde_json::json!({"owner": "research"}));
    let value = v3_entry_json(&fixture.entry, definition.clone());
    let entry: TableEntry = serde_json::from_value(value).expect("approved Vector v3 entry must decode");
    let encoded = serde_json::to_value(&entry).unwrap();
    assert_eq!(encoded["vector_index_definition"], definition);
    assert!(entry.properties.is_empty());
    let path = fixture.store.paths.table_entry_path(
        fixture.bucket.storage_bucket_id().as_str(),
        &Namespace::parse("indexes").unwrap(),
        &CatalogName::parse("index.v1").unwrap(),
    );
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&entry).unwrap())
        .await;
    let head = fixture
        .store
        .read_catalog_head(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .expect("Vector v3 head must use its complete bound entry");
    assert_eq!(head.metadata(), &fixture.initial);
    assert_eq!(head.format_descriptor(), &fixture.initial_descriptor);
    assert_eq!(head.generation().get(), 1);
}

#[tokio::test]
async fn catalog_vector_v3_rejects_definition_version_fields_and_duplicate_tags() {
    let fixture = prepared_fixture().await;
    let valid = v3_entry_json(&fixture.entry, v3_definition_json(serde_json::json!({})));
    let entry: TableEntry = serde_json::from_value(valid.clone()).expect("validate the positive v3 control");
    CatalogHead::from_vector_entry(&entry).expect("validate the positive v3 head");
    for field in ["version", "effective_encryption", "tags"] {
        let mut absent = valid.clone();
        absent["vector_index_definition"].as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<TableEntry>(absent).is_err(), "accepted missing {field}");
    }
    let mut unknown = valid.clone();
    unknown["vector_index_definition"]["unknown"] = true.into();
    assert!(serde_json::from_value::<TableEntry>(unknown).is_err());
    let mut unknown_version = valid.clone();
    unknown_version["vector_index_definition"]["version"] = 2.into();
    let decoded = serde_json::from_value::<TableEntry>(unknown_version);
    assert!(decoded.is_err() || CatalogHead::from_vector_entry(&decoded.unwrap()).is_err());
    let duplicate = serde_json::to_string(&valid)
        .unwrap()
        .replace("\"tags\":{}", "\"tags\":{\"owner\":\"first\",\"owner\":\"second\"}");
    assert!(duplicate.contains("\"owner\":\"first\",\"owner\":\"second\""));
    assert!(serde_json::from_str::<TableEntry>(&duplicate).is_err());
}

#[tokio::test]
async fn catalog_vector_v3_requires_definition_without_changing_legacy_v2_bytes() {
    let fixture = prepared_fixture().await;
    let legacy = serde_json::to_value(&fixture.entry).unwrap();
    assert!(legacy.get("vector_index_definition").is_none());
    let decoded: TableEntry = serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(decoded, fixture.entry);
    CatalogHead::from_vector_entry(&decoded).expect("M1 v2 remains a validated exact head");
    let mut missing = legacy;
    missing["version"] = 3.into();
    let missing: TableEntry = serde_json::from_value(missing).unwrap();
    assert!(CatalogHead::from_vector_entry(&missing).is_err());
    let mut forbidden = v3_entry_json(&fixture.entry, v3_definition_json(serde_json::json!({})));
    forbidden["version"] = 2.into();
    if let Ok(entry) = serde_json::from_value::<TableEntry>(forbidden) {
        assert!(CatalogHead::from_vector_entry(&entry).is_err());
    }
}

async fn v3_tags_history_fixture() -> (TableEntry, Vec<CommitLogEntry>, CatalogMetadataRef, CatalogMetadataRef) {
    let fixture = prepared_fixture().await;
    let prepared = prepared_candidate(&fixture, "78b2dab1-7e92-44c0-9cd6-85300fb5387c", b"dataset-version-two").await;
    let committed = fixture.store.commit_prepared(prepared).await.unwrap();
    let manifest = CatalogHead::from_vector_entry(&committed.table).unwrap().metadata().clone();
    let initial_definition = v3_definition_json(serde_json::json!({}));
    let tagged_definition = v3_definition_json(serde_json::json!({"owner": "research"}));
    let mut data_log = serde_json::to_value(&committed.commit_log).unwrap();
    data_log["version"] = 3.into();
    data_log["vector_resource_mutation"] = v3_mutation_json("DATASET", initial_definition.clone(), initial_definition.clone());
    let mut tag_log = serde_json::to_value(&committed.commit_log).unwrap();
    tag_log["version"] = 3.into();
    tag_log["commit_id"] = "3f171b6d-0821-42fe-a9e9-eb870f30edcb".into();
    tag_log["idempotency_key"] = serde_json::Value::Null;
    tag_log["operation"] = "vector-tags".into();
    tag_log["expected_version_token"] = committed.table.version_token.clone().into();
    tag_log["new_version_token"] = "099b8e2b-3482-47fe-9766-0e0569b6dd82".into();
    tag_log["previous_metadata_location"] = committed.table.metadata_location.clone().into();
    tag_log["vector_commit_binding"]["expected_generation"] = committed.table.generation.into();
    tag_log["vector_commit_binding"]["expected_metadata"] = tag_log["vector_commit_binding"]["candidate_metadata"].clone();
    tag_log["vector_commit_binding"]["expected_descriptor"] = tag_log["vector_commit_binding"]["descriptor"].clone();
    tag_log["vector_resource_mutation"] = v3_mutation_json("TAGS", initial_definition, tagged_definition.clone());
    let mut head = v3_entry_json(&committed.table, tagged_definition);
    head["generation"] = 3.into();
    head["version_token"] = tag_log["new_version_token"].clone();
    (
        serde_json::from_value(head).expect("v3 tagged head"),
        vec![
            serde_json::from_value(data_log).expect("v3 dataset log"),
            serde_json::from_value(tag_log).expect("v3 tags log"),
        ],
        fixture.initial,
        manifest,
    )
}

#[tokio::test]
async fn catalog_vector_v3_tags_history_keeps_manifest_identity_across_generations() {
    let (head, logs, initial, manifest) = v3_tags_history_fixture().await;
    let history = vector_committed_history_versions(&head, &logs).expect("tags must retain a complete Catalog chain");
    assert_eq!(head.generation, 3);
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].metadata(), &manifest);
    assert_eq!(history[1].metadata(), &manifest);
    assert_eq!(history[2].metadata(), &initial);
    assert_eq!(history[0].format_descriptor(), history[1].format_descriptor());
    assert_ne!(history[0].commit_id(), history[1].commit_id());
}

#[tokio::test]
async fn catalog_vector_v3_receipt_and_history_compare_visible_definition() {
    let (head, logs, _, _) = v3_tags_history_fixture().await;
    assert!(table_matches_committed_log(&head, &logs[1]));
    let mut altered = serde_json::to_value(&head).unwrap();
    altered["vector_index_definition"]["tags"]["owner"] = "different".into();
    let altered: TableEntry = serde_json::from_value(altered).unwrap();
    assert!(!table_matches_committed_log(&altered, &logs[1]));
    assert!(vector_committed_history_versions(&altered, &logs).is_err());
    let mut damaged = serde_json::to_value(&logs[1]).unwrap();
    damaged["vector_resource_mutation"]["expected_definition"]["tags"]["unexpected"] = "value".into();
    let damaged: CommitLogEntry = serde_json::from_value(damaged).unwrap();
    assert!(vector_committed_history_versions(&head, &[logs[0].clone(), damaged]).is_err());
}

#[tokio::test]
async fn catalog_vector_v3_receipt_compares_published_state() {
    let (head, logs, _, _) = v3_tags_history_fixture().await;
    let mut deleted = head.clone();
    deleted.state = TableCatalogEntryState::Deleted;
    assert!(!table_matches_committed_log(&deleted, &logs[1]));
    let mut deletion = serde_json::to_value(&logs[1]).unwrap();
    deletion["vector_resource_mutation"]["kind"] = "DELETE".into();
    deletion["vector_resource_mutation"]["expected_definition"] = deletion["vector_resource_mutation"]["next_definition"].clone();
    deletion["vector_resource_mutation"]["next_state"] = "DELETED".into();
    let deletion: CommitLogEntry = serde_json::from_value(deletion).unwrap();
    assert!(table_matches_committed_log(&deleted, &deletion));
    assert!(!table_matches_committed_log(&head, &deletion));
}

#[test]
fn catalog_vector_v3_encryption_uses_exact_approved_canonical_bytes() {
    let cases = [
        (VectorEffectiveEncryptionV1::Unencrypted, b"null".as_slice()),
        (VectorEffectiveEncryptionV1::Aes256, br#"{"sseType":"AES256"}"#.as_slice()),
        (
            VectorEffectiveEncryptionV1::AwsKms {
                key_id: "resolved-provider-key".into(),
                kms_key_arn: "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012".into(),
            },
            br#"{"sseType":"aws:kms","kmsKeyArn":"arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"}"#
                .as_slice(),
        ),
    ];
    for (encryption, bytes) in cases {
        assert_eq!(encryption.canonical_bytes().unwrap(), bytes);
        assert_eq!(encryption.descriptor_sha256().unwrap(), CatalogContentSha256::digest(bytes));
        let stored = serde_json::to_vec(&encryption).unwrap();
        assert_eq!(serde_json::from_slice::<VectorEffectiveEncryptionV1>(&stored).unwrap(), encryption);
    }
}

#[tokio::test]
async fn catalog_vector_v3_complete_digest_binds_tags_state_and_kms_identity() {
    let fixture = prepared_fixture().await;
    let prepared = prepared_candidate(&fixture, "e1e8c31d-e220-4c38-bdda-ea20212eb072", b"candidate").await;
    let definition = VectorIndexDefinitionV1 {
        version: 1,
        effective_encryption: VectorEffectiveEncryptionV1::Unencrypted,
        tags: BTreeMap::new(),
    };
    let mutation = VectorResourceMutationBindingV1 {
        version: 1,
        kind: VectorResourceMutationKind::Tags,
        expected_definition: definition.clone(),
        next_definition: definition,
        expected_state: TableCatalogEntryState::Active,
        next_state: TableCatalogEntryState::Active,
    };
    let base = mutation.request_digest(&prepared).unwrap();
    assert_ne!(base, prepared.request_digest());
    let mut tagged = mutation.clone();
    tagged.next_definition.tags.insert("owner".into(), "research".into());
    assert_ne!(tagged.request_digest(&prepared).unwrap(), base);
    let mut deletion = mutation.clone();
    deletion.kind = VectorResourceMutationKind::Delete;
    deletion.next_state = TableCatalogEntryState::Deleted;
    assert_ne!(deletion.request_digest(&prepared).unwrap(), base);
    let mut changed_encryption = mutation.clone();
    changed_encryption.next_definition.effective_encryption = VectorEffectiveEncryptionV1::Aes256;
    assert!(changed_encryption.request_digest(&prepared).is_err());
    let mut dataset = tagged;
    dataset.kind = VectorResourceMutationKind::Dataset;
    assert!(dataset.request_digest(&prepared).is_err());
    let mut kms = mutation.clone();
    kms.expected_definition.effective_encryption = VectorEffectiveEncryptionV1::AwsKms {
        key_id: "provider-key-one".into(),
        kms_key_arn: "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012".into(),
    };
    kms.next_definition = kms.expected_definition.clone();
    let kms_digest = kms.request_digest(&prepared).unwrap();
    assert_ne!(kms_digest, base);
    if let VectorEffectiveEncryptionV1::AwsKms { key_id, .. } = &mut kms.expected_definition.effective_encryption {
        *key_id = "provider-key-two".into();
    }
    kms.next_definition = kms.expected_definition.clone();
    assert_ne!(kms.request_digest(&prepared).unwrap(), kms_digest);
    let mut first_order = mutation.clone();
    first_order.next_definition.tags.insert("owner".into(), "research".into());
    first_order.next_definition.tags.insert("purpose".into(), "search".into());
    let mut second_order = mutation;
    second_order.next_definition.tags.insert("purpose".into(), "search".into());
    second_order.next_definition.tags.insert("owner".into(), "research".into());
    assert_eq!(
        first_order.request_digest(&prepared).unwrap(),
        second_order.request_digest(&prepared).unwrap()
    );
}

#[test]
fn catalog_vector_v3_configuration_rejects_missing_encryption_and_mutation_fields() {
    for input in [
        r#"{"version":1,"tags":{}}"#,
        r#"{"version":1,"effective_encryption":{"kind":"aws-kms","key_id":"provider"},"tags":{}}"#,
        r#"{"version":1,"effective_encryption":{"kind":"aws-kms","kms_key_arn":"arn"},"tags":{}}"#,
        r#"{"version":1,"effective_encryption":{"kind":"unencrypted","key_id":"provider"},"tags":{}}"#,
        r#"{"version":1,"effective_encryption":{"kind":"unsupported"},"tags":{}}"#,
        r#"{"version":1,"effective_encryption":{"kind":"unencrypted"},"tags":{},"mode":"ENHANCED"}"#,
    ] {
        assert!(serde_json::from_str::<VectorIndexDefinitionV1>(input).is_err());
    }
    let complete = v3_mutation_json(
        "TAGS",
        v3_definition_json(serde_json::json!({})),
        v3_definition_json(serde_json::json!({})),
    );
    for field in [
        "version",
        "kind",
        "expected_definition",
        "next_definition",
        "expected_state",
        "next_state",
    ] {
        let mut missing = complete.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<VectorResourceMutationBindingV1>(missing).is_err(),
            "accepted missing {field}"
        );
    }
}

#[tokio::test]
async fn catalog_vector_v3_rejects_positional_entry_and_definition_arrays() {
    let fixture = prepared_fixture().await;
    let valid = v3_entry_json(&fixture.entry, v3_definition_json(serde_json::json!({})));
    let entry: TableEntry = serde_json::from_value(valid.clone()).expect("positive v3 object control");
    CatalogHead::from_vector_entry(&entry).unwrap();
    let positional_definition = serde_json::json!([1, {"kind": "unencrypted"}, {}]);
    let mut altered = valid.clone();
    altered["vector_index_definition"] = positional_definition;
    assert!(serde_json::from_value::<TableEntry>(altered).is_err());
    let existing_order = [
        "vector_head_binding",
        "version",
        "table_bucket",
        "namespace",
        "table",
        "table_id",
        "table_uuid",
        "format",
        "format_version",
        "warehouse_location",
        "metadata_location",
        "version_token",
        "generation",
        "state",
        "properties",
        "created_at",
        "updated_at",
    ];
    // Probe both trailing optional-field compatibility and adjacent binding placement.
    for definition_position in [1, existing_order.len()] {
        let mut positional = existing_order.iter().map(|field| valid[*field].clone()).collect::<Vec<_>>();
        positional.insert(definition_position, valid["vector_index_definition"].clone());
        assert!(serde_json::from_value::<TableEntry>(serde_json::Value::Array(positional)).is_err());
    }
}

#[test]
fn catalog_vector_v3_rejects_positional_mutation_and_enum_object_representations() {
    let definition = v3_definition_json(serde_json::json!({}));
    let valid = v3_mutation_json("TAGS", definition.clone(), definition.clone());
    serde_json::from_value::<VectorResourceMutationBindingV1>(valid.clone()).expect("positive mutation object control");
    let positional = serde_json::json!([1, "TAGS", definition.clone(), definition, "ACTIVE", "ACTIVE"]);
    assert!(serde_json::from_value::<VectorResourceMutationBindingV1>(positional).is_err());
    for field in ["kind", "expected_state", "next_state"] {
        let mut object_enum = valid.clone();
        let value = object_enum[field].as_str().unwrap().to_string();
        object_enum[field] = serde_json::json!({(value): null});
        assert!(
            serde_json::from_value::<VectorResourceMutationBindingV1>(object_enum).is_err(),
            "accepted enum object for {field}"
        );
    }
    for invalid in [
        serde_json::json!([]),
        serde_json::json!(["unencrypted"]),
        serde_json::json!({"unencrypted": null}),
    ] {
        assert!(serde_json::from_value::<VectorEffectiveEncryptionV1>(invalid).is_err());
    }
}

#[tokio::test]
async fn catalog_vector_v3_rejects_outer_state_enum_object() {
    let fixture = prepared_fixture().await;
    let mut entry = v3_entry_json(&fixture.entry, v3_definition_json(serde_json::json!({})));
    entry["state"] = serde_json::json!({"ACTIVE": null});
    assert!(serde_json::from_value::<TableEntry>(entry).is_err());
}

#[tokio::test]
async fn catalog_vector_v3_preserves_legacy_entry_array_and_enum_input() {
    let fixture = prepared_fixture().await;
    let legacy_fields = [
        "vector_head_binding",
        "version",
        "table_bucket",
        "namespace",
        "table",
        "table_id",
        "table_uuid",
        "format",
        "format_version",
        "warehouse_location",
        "metadata_location",
        "version_token",
        "generation",
        "state",
        "properties",
        "created_at",
        "updated_at",
    ];
    let table = test_table_entry(
        "table-storage",
        &Namespace::parse("research").unwrap(),
        &IdentifierSegment::parse("sample").unwrap(),
        "s3://table-storage/tables/table-id/metadata/v1.json".into(),
    );
    for entry in [fixture.entry, table] {
        let mut value = serde_json::to_value(&entry).unwrap();
        let array = serde_json::Value::Array(legacy_fields.iter().map(|field| value[*field].clone()).collect());
        assert_eq!(serde_json::from_value::<TableEntry>(array).unwrap(), entry);
        value["state"] = serde_json::json!({"ACTIVE": null});
        assert_eq!(serde_json::from_value::<TableEntry>(value).unwrap(), entry);
    }
}

fn positional_record(value: &serde_json::Value, fields: &[&str]) -> serde_json::Value {
    serde_json::Value::Array(fields.iter().map(|field| value[*field].clone()).collect())
}

#[tokio::test]
async fn catalog_vector_v3_rejects_nested_positional_bindings_without_changing_v2() {
    let fixture = prepared_fixture().await;
    let v2 = serde_json::to_value(&fixture.entry).unwrap();
    let v3 = v3_entry_json(&fixture.entry, v3_definition_json(serde_json::json!({})));
    let bucket_fields = [
        "version",
        "storage_bucket_id",
        "bucket_incarnation",
        "account_namespace_id",
        "region",
        "public_name",
    ];
    let metadata_fields = ["object_key", "content_length", "content_sha256", "body_revision", "etag"];
    let descriptor_fields = ["encoding_version", "format", "format_version", "metadata", "payload_base64"];
    let head_fields = ["version", "bucket", "table_id", "metadata", "descriptor"];
    for (pointer, fields) in [
        ("/vector_head_binding", head_fields.as_slice()),
        ("/vector_head_binding/bucket", bucket_fields.as_slice()),
        ("/vector_head_binding/metadata", metadata_fields.as_slice()),
        ("/vector_head_binding/descriptor", descriptor_fields.as_slice()),
        ("/vector_head_binding/descriptor/metadata", metadata_fields.as_slice()),
    ] {
        let mut legacy = v2.clone();
        *legacy.pointer_mut(pointer).unwrap() = positional_record(v2.pointer(pointer).unwrap(), fields);
        assert_eq!(serde_json::from_value::<TableEntry>(legacy).unwrap(), fixture.entry);
        let mut strict = v3.clone();
        *strict.pointer_mut(pointer).unwrap() = positional_record(v3.pointer(pointer).unwrap(), fields);
        assert!(serde_json::from_value::<TableEntry>(strict).is_err(), "accepted positional {pointer}");
    }
    let prepared = prepared_candidate(&fixture, "a1de2047-f0a5-40a0-aed5-94b0619ec91e", b"metadata-v2").await;
    let result = fixture.store.commit_prepared(prepared).await.unwrap();
    let legacy = serde_json::to_value(&result.commit_log).unwrap();
    let mut strict = legacy.clone();
    strict["version"] = 3.into();
    strict["vector_resource_mutation"] = v3_mutation_json(
        "DATASET",
        v3_definition_json(serde_json::json!({})),
        v3_definition_json(serde_json::json!({})),
    );
    let commit_fields = [
        "version",
        "bucket",
        "table_id",
        "expected_generation",
        "expected_metadata",
        "expected_descriptor",
        "candidate_metadata",
        "descriptor",
        "request_digest_sha256",
    ];
    for (pointer, fields) in [
        ("/vector_commit_binding", commit_fields.as_slice()),
        ("/vector_commit_binding/bucket", bucket_fields.as_slice()),
        ("/vector_commit_binding/expected_metadata", metadata_fields.as_slice()),
        ("/vector_commit_binding/expected_descriptor", descriptor_fields.as_slice()),
        ("/vector_commit_binding/expected_descriptor/metadata", metadata_fields.as_slice()),
        ("/vector_commit_binding/candidate_metadata", metadata_fields.as_slice()),
        ("/vector_commit_binding/descriptor", descriptor_fields.as_slice()),
        ("/vector_commit_binding/descriptor/metadata", metadata_fields.as_slice()),
    ] {
        let mut compatible = legacy.clone();
        *compatible.pointer_mut(pointer).unwrap() = positional_record(legacy.pointer(pointer).unwrap(), fields);
        assert_eq!(serde_json::from_value::<CommitLogEntry>(compatible).unwrap(), result.commit_log);
        let mut invalid = strict.clone();
        *invalid.pointer_mut(pointer).unwrap() = positional_record(strict.pointer(pointer).unwrap(), fields);
        assert!(
            serde_json::from_value::<CommitLogEntry>(invalid).is_err(),
            "accepted positional {pointer}"
        );
    }
}

#[tokio::test]
async fn catalog_vector_v3_recovery_rejects_definition_and_state_mismatched_ancestry() {
    let (head, mut logs, _, _) = v3_tags_history_fixture().await;
    logs[0].status = CommitLogStatus::Staged;
    let valid = TableCommitHistoryIndex::new(&head, &logs);
    assert!(valid.proves_committed(&logs[0]));
    for alter_state in [false, true] {
        let mut altered = head.clone();
        if alter_state {
            altered.state = TableCatalogEntryState::Deleted;
        } else {
            altered
                .vector_index_definition
                .as_mut()
                .unwrap()
                .tags
                .insert("owner".into(), "different".into());
        }
        let history = TableCommitHistoryIndex::new(&altered, &logs);
        assert!(!history.proves_committed(&logs[0]));
        let recovery = table_commit_recovery_entry_from_history(&altered, &logs[0], None, &history);
        assert_eq!(recovery.recovery_state, TableCommitRecoveryState::ManualReview);
    }
    let mut contradictory = logs.clone();
    contradictory[1]
        .vector_resource_mutation
        .as_mut()
        .unwrap()
        .expected_definition
        .tags
        .insert("unexpected".into(), "value".into());
    let history = TableCommitHistoryIndex::new(&head, &contradictory);
    assert!(!history.proves_committed(&contradictory[0]));
}

#[tokio::test]
async fn catalog_vector_v3_superseded_recovery_requires_exact_resource_predecessor() {
    let (head, logs, _, _) = v3_tags_history_fixture().await;
    let mut loser = logs[1].clone();
    loser.commit_id = "73df611a-4c09-4251-87f8-b506ad9a1adc".into();
    loser.new_version_token = "unpublished-loser-token".into();
    loser.status = CommitLogStatus::Staged;
    let mut records = logs.clone();
    records.push(loser.clone());
    let history = TableCommitHistoryIndex::new(&head, &records);
    let valid = table_commit_recovery_entry_from_history(&head, &loser, None, &history);
    assert_eq!(valid.recovery_state, TableCommitRecoveryState::Superseded);
    for corrupt_generation in [false, true] {
        let mut contradictory = loser.clone();
        if corrupt_generation {
            contradictory.vector_commit_binding.as_mut().unwrap().expected_generation += 1;
        } else {
            contradictory
                .vector_resource_mutation
                .as_mut()
                .unwrap()
                .expected_definition
                .tags
                .insert("unknown".into(), "base".into());
        }
        let mut records = logs.clone();
        records.push(contradictory.clone());
        let history = TableCommitHistoryIndex::new(&head, &records);
        let recovery = table_commit_recovery_entry_from_history(&head, &contradictory, None, &history);
        assert_eq!(recovery.recovery_state, TableCommitRecoveryState::ManualReview);
    }
}

#[tokio::test]
async fn catalog_vector_v3_recovery_proof_checks_exact_binding_and_generation() {
    let (head, logs, _, _) = v3_tags_history_fixture().await;
    for corrupt_generation in [false, true] {
        let mut records = logs.clone();
        let binding = records[1].vector_commit_binding.as_mut().unwrap();
        if corrupt_generation {
            binding.expected_generation += 1;
        } else {
            binding.bucket.bucket_incarnation = Uuid::new_v4();
        }
        let history = TableCommitHistoryIndex::new(&head, &records);
        assert!(!history.proves_committed(&records[0]));
        assert!(vector_committed_history_versions(&head, &records).is_err());
    }
}

async fn prepared_v3_fixture() -> PreparedFixture {
    let mut fixture = prepared_fixture().await;
    fixture.entry = serde_json::from_value(v3_entry_json(
        &fixture.entry,
        v3_definition_json(serde_json::json!({"owner": "research"})),
    ))
    .unwrap();
    let path = fixture.store.paths.table_entry_path(
        fixture.bucket.storage_bucket_id().as_str(),
        &Namespace::parse("indexes").unwrap(),
        &CatalogName::parse("index.v1").unwrap(),
    );
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&fixture.entry).unwrap())
        .await;
    fixture
}

#[tokio::test]
async fn catalog_vector_v3_cancelled_publication_retains_gc_until_receipts_finish() {
    let fixture = prepared_v3_fixture().await;
    let (guard, entry) = fixture
        .store
        .begin_vector_operation(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .unwrap();
    let prepared = prepared_candidate(&fixture, "db6f6bd0-75fa-4c58-b9f8-c7c2d89fcabb", b"cancelled-caller-content").await;
    let commit_id = prepared.commit_id().as_str().to_owned();
    let table_path =
        fixture
            .store
            .paths
            .table_entry_path(fixture.bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name());
    let pause = fixture.backend.pause_next_put(RUSTFS_META_BUCKET, &table_path).await;
    let store = fixture.store.clone();
    let caller_guard = guard.clone();
    let definition = entry.definition().clone();
    let caller = tokio::spawn(async move {
        store
            .commit_vector_prepared_with_guard(&caller_guard, prepared, &definition)
            .await
    });
    pause.wait_started().await;
    let gc_path = fixture
        .store
        .paths
        .vector_gc_lock_path(fixture.bucket.storage_bucket_id().as_str(), &fixture.entry.table_id);
    let before = fixture
        .backend
        .write_lock_acquisition_count(RUSTFS_META_BUCKET, &gc_path)
        .await;
    let writer_backend = fixture.backend.clone();
    let writer_path = gc_path.clone();
    let writer = tokio::spawn(async move { writer_backend.acquire_write_lock(RUSTFS_META_BUCKET, &writer_path).await });
    tokio::time::timeout(StdDuration::from_secs(5), async {
        while fixture
            .backend
            .write_lock_acquisition_count(RUSTFS_META_BUCKET, &gc_path)
            .await
            == before
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("GC writer must reach the lock boundary");
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    drop(guard);
    assert!(!writer.is_finished(), "the admitted publication still owns lifecycle and GC protection");
    pause.release();
    let exclusive = tokio::time::timeout(StdDuration::from_secs(5), writer)
        .await
        .expect("GC must proceed after the complete mutation task finishes")
        .unwrap()
        .unwrap();
    let current = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &table_path)
        .await
        .unwrap()
        .unwrap();
    let current: TableEntry = serde_json::from_slice(&current.data).unwrap();
    assert_eq!(current.generation, 2);
    let log_path = fixture.store.paths.commit_log_entry_path(
        fixture.bucket.storage_bucket_id().as_str(),
        &fixture.entry.table_id,
        &commit_id,
    );
    let log = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &log_path)
        .await
        .unwrap()
        .unwrap();
    let log: CommitLogEntry = serde_json::from_slice(&log.data).unwrap();
    assert_eq!(log.status, CommitLogStatus::Committed);
    assert!(table_matches_committed_log(&current, &log));
    drop(exclusive);
}

#[tokio::test]
async fn catalog_vector_v3_operation_guard_covers_snapshot_until_last_read() {
    let fixture = prepared_v3_fixture().await;
    let (guard, entry) = fixture
        .store
        .begin_vector_operation(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(entry.head().metadata(), &fixture.initial);
    let guard = Arc::new(guard);
    let io_guard = guard.clone();
    let io_backend = fixture.backend.clone();
    let bucket = fixture.bucket.storage_bucket_id().as_str().to_owned();
    let reference = fixture.initial.clone();
    let pause = fixture
        .backend
        .pause_next_read(&bucket, reference.object_key().as_str())
        .await;
    let read = tokio::spawn(async move {
        io_guard.check_held()?;
        let object = io_backend
            .read_object(&bucket, reference.object_key().as_str())
            .await?
            .unwrap();
        io_guard.check_held()?;
        reference.verify_read(&object)?;
        Ok::<_, TableCatalogStoreError>(object.data)
    });
    pause.wait_started().await;
    let gc_path = fixture
        .store
        .paths
        .vector_gc_lock_path(fixture.bucket.storage_bucket_id().as_str(), fixture.entry.table_id.as_str());
    let writer_backend = fixture.backend.clone();
    let writer_path = gc_path.clone();
    let before = fixture
        .backend
        .write_lock_acquisition_count(RUSTFS_META_BUCKET, &gc_path)
        .await;
    let writer = tokio::spawn(async move { writer_backend.acquire_write_lock(RUSTFS_META_BUCKET, &writer_path).await });
    tokio::time::timeout(StdDuration::from_secs(5), async {
        while fixture
            .backend
            .write_lock_acquisition_count(RUSTFS_META_BUCKET, &gc_path)
            .await
            == before
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("GC writer must reach the lock boundary");
    drop(guard);
    assert!(!writer.is_finished(), "the file read still owns the request's GC guard");
    pause.release();
    assert_eq!(read.await.unwrap().unwrap(), b"first-content");
    let writer_guard = tokio::time::timeout(StdDuration::from_secs(5), writer)
        .await
        .expect("GC writer should proceed when the final reader releases protection")
        .unwrap()
        .unwrap();
    drop(writer_guard);
}

#[tokio::test]
async fn catalog_vector_v3_protected_commit_reuses_lifecycle_with_waiting_delete() {
    let fixture = prepared_v3_fixture().await;
    let (guard, entry) = fixture
        .store
        .begin_vector_operation(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .unwrap();
    let prepared = prepared_candidate(&fixture, "83ff48ac-f610-44cd-8304-f96930a0789d", b"new-dataset").await;
    let bucket = fixture.bucket.storage_bucket_id().as_str().to_owned();
    let reads = fixture.backend.read_lock_acquisition_count(&bucket, "").await;
    let writers = fixture.backend.write_lock_acquisition_count(&bucket, "").await;
    let writer_backend = fixture.backend.clone();
    let writer_bucket = bucket.clone();
    let deletion = tokio::spawn(async move { writer_backend.acquire_write_lock(&writer_bucket, "").await });
    tokio::time::timeout(StdDuration::from_secs(5), async {
        while fixture.backend.write_lock_acquisition_count(&bucket, "").await == writers {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("bucket deletion must reach lifecycle exclusive acquisition");
    let committed = tokio::time::timeout(
        StdDuration::from_secs(5),
        fixture
            .store
            .commit_vector_prepared_with_guard(&guard, prepared, entry.definition()),
    )
    .await
    .expect("the same lifecycle guard must cover publication behind a queued exclusive waiter")
    .unwrap();
    assert_eq!(committed.table.version, 3);
    assert_eq!(committed.table.generation, 2);
    assert_eq!(fixture.backend.read_lock_acquisition_count(&bucket, "").await, reads);
    assert!(!deletion.is_finished());
    drop(guard);
    drop(
        tokio::time::timeout(StdDuration::from_secs(5), deletion)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    );
}

#[tokio::test]
async fn catalog_vector_v3_guard_rejects_another_store_and_old_bucket_identity() {
    let fixture = prepared_v3_fixture().await;
    let (guard, _) = fixture
        .store
        .begin_vector_operation(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .unwrap();
    let other_store = ObjectTableCatalogStore::for_vector_scope(fixture.backend.clone(), fixture.bucket.clone())
        .await
        .unwrap();
    assert!(other_store.read_catalog_head_with_guard(&guard).await.is_err());
    let mut identity = VectorBucketBindingV1::from_handle(&fixture.bucket).unwrap();
    identity.bucket_incarnation = Uuid::new_v4();
    fixture
        .backend
        .state
        .lock()
        .await
        .bucket_handles
        .insert(fixture.bucket.storage_bucket_id().as_str().to_owned(), identity.to_handle().unwrap());
    assert!(fixture.store.read_catalog_head_with_guard(&guard).await.is_err());
}

#[tokio::test]
async fn catalog_vector_v3_legacy_read_does_not_admit_formal_operation() {
    let fixture = prepared_fixture().await;
    fixture
        .store
        .read_catalog_head(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .expect("M1 exact v2 reads remain supported");
    assert_matches!(
        fixture
            .store
            .begin_vector_operation(
                &fixture.bucket,
                &Namespace::parse("indexes").unwrap(),
                &CatalogName::parse("index.v1").unwrap(),
            )
            .await,
        Err(TableCatalogStoreError::Unsupported(_))
    );
}

#[tokio::test]
async fn catalog_vector_v3_guarded_history_checks_exact_head_and_entry_changes() {
    let fixture = prepared_v3_fixture().await;
    let (guard, entry) = fixture
        .store
        .begin_vector_operation(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .unwrap();
    let prepared = prepared_candidate(&fixture, "a8d991c7-66eb-4e25-aeef-1d3bfd1d8cd1", b"history-content").await;
    let candidate = prepared.candidate_metadata().clone();
    let commit_id = prepared.commit_id().clone();
    let committed = fixture
        .store
        .commit_vector_prepared_with_guard(&guard, prepared, entry.definition())
        .await
        .unwrap();
    let head = fixture.store.read_catalog_head_with_guard(&guard).await.unwrap();
    let history = fixture.store.read_committed_history_with_guard(&guard, &head).await.unwrap();
    assert_eq!(history.versions().len(), 2);
    assert_eq!(history.versions()[0].metadata(), &candidate);
    assert_eq!(history.versions()[0].format_descriptor(), head.format_descriptor());
    assert_eq!(history.versions()[0].commit_id(), Some(&commit_id));
    assert_eq!(history.versions()[1].metadata(), &fixture.initial);
    assert_eq!(history.versions()[1].format_descriptor(), &fixture.initial_descriptor);
    assert!(history.versions()[1].commit_id().is_none());
    assert_matches!(
        fixture.store.read_committed_history_with_guard(&guard, entry.head()).await,
        Err(TableCatalogStoreError::Conflict(message)) if message == "Vector head changed before history read"
    );

    let log_path = fixture.store.paths.commit_log_entry_path(
        fixture.bucket.storage_bucket_id().as_str(),
        &fixture.entry.table_id,
        commit_id.as_str(),
    );
    let pause = fixture.backend.pause_next_read(RUSTFS_META_BUCKET, &log_path).await;
    let store = fixture.store.clone();
    let read_guard = guard.clone();
    let read_head = head.clone();
    let read = tokio::spawn(async move { store.read_committed_history_with_guard(&read_guard, &read_head).await });
    tokio::time::timeout(StdDuration::from_secs(5), pause.wait_started())
        .await
        .expect("history must reach the selected commit log read");

    let mut changed = committed.table;
    changed
        .vector_index_definition
        .as_mut()
        .unwrap()
        .tags
        .insert("owner".into(), "history-observer".into());
    let table_path =
        fixture
            .store
            .paths
            .table_entry_path(fixture.bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name());
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &table_path, serde_json::to_vec(&changed).unwrap())
        .await;
    let after = fixture.store.read_catalog_head_with_guard(&guard).await.unwrap();
    assert_eq!(after.expected_head(), head.expected_head());
    pause.release();
    let result = tokio::time::timeout(StdDuration::from_secs(5), read)
        .await
        .expect("history must finish after the log read resumes")
        .unwrap();
    assert_matches!(
        result,
        Err(TableCatalogStoreError::Conflict(message)) if message == "Vector entry changed during history read"
    );
}

#[tokio::test]
async fn catalog_vector_v3_cancelled_receipt_write_retains_gc_until_both_receipts_finish() {
    let fixture = prepared_v3_fixture().await;
    let (guard, entry) = fixture
        .store
        .begin_vector_operation(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .unwrap();
    let prepared = prepared_candidate(&fixture, "695a4823-8b2c-455d-8f6c-78425879dc68", b"receipt-content").await;
    let candidate = prepared.candidate_metadata().clone();
    let commit_id = prepared.commit_id().as_str().to_owned();
    let log_path = fixture.store.paths.commit_log_entry_path(
        fixture.bucket.storage_bucket_id().as_str(),
        &fixture.entry.table_id,
        &commit_id,
    );
    let receipt_path = fixture.store.paths.commit_idempotency_entry_path(
        fixture.bucket.storage_bucket_id().as_str(),
        &fixture.entry.table_id,
        prepared.idempotency_key().unwrap(),
    );
    let attempt = fixture.backend.put_attempt_count(RUSTFS_META_BUCKET, &receipt_path).await + 2;
    let pause = fixture
        .backend
        .pause_put_attempt(RUSTFS_META_BUCKET, &receipt_path, attempt)
        .await;
    let store = fixture.store.clone();
    let caller_guard = guard.clone();
    let definition = entry.definition().clone();
    let caller = tokio::spawn(async move {
        store
            .commit_vector_prepared_with_guard(&caller_guard, prepared, &definition)
            .await
    });
    tokio::time::timeout(StdDuration::from_secs(5), pause.wait_started())
        .await
        .expect("publication must reach the committed idempotency receipt write");
    let head = fixture.store.read_catalog_head_with_guard(&guard).await.unwrap();
    assert_eq!(head.generation().get(), 2);
    assert_eq!(head.metadata(), &candidate);
    let log = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &log_path)
        .await
        .unwrap()
        .unwrap();
    let log: CommitLogEntry = serde_json::from_slice(&log.data).unwrap();
    assert_eq!(log.status, CommitLogStatus::Committed);
    assert_eq!(log.commit_id, commit_id);
    let receipt = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &receipt_path)
        .await
        .unwrap()
        .unwrap();
    let receipt: CommitLogEntry = serde_json::from_slice(&receipt.data).unwrap();
    assert_eq!(receipt.status, CommitLogStatus::Staged);
    assert!(commit_logs_share_recovery_payload(&log, &receipt));

    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    drop(guard);
    let gc_path = fixture
        .store
        .paths
        .vector_gc_lock_path(fixture.bucket.storage_bucket_id().as_str(), &fixture.entry.table_id);
    let gc_lock = fixture
        .backend
        .locks
        .lock()
        .await
        .get(&(RUSTFS_META_BUCKET.to_string(), gc_path.clone()))
        .unwrap()
        .clone();
    assert!(
        gc_lock.try_write().is_err(),
        "the receipt worker must retain GC protection after caller cancellation"
    );
    let before = fixture
        .backend
        .write_lock_acquisition_count(RUSTFS_META_BUCKET, &gc_path)
        .await;
    let writer_backend = fixture.backend.clone();
    let writer_path = gc_path.clone();
    let writer = tokio::spawn(async move { writer_backend.acquire_write_lock(RUSTFS_META_BUCKET, &writer_path).await });
    tokio::time::timeout(StdDuration::from_secs(5), async {
        while fixture
            .backend
            .write_lock_acquisition_count(RUSTFS_META_BUCKET, &gc_path)
            .await
            == before
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("GC writer must reach exclusive acquisition while receipt finalization is paused");
    assert!(!writer.is_finished(), "receipt finalization still owns the operation's GC protection");
    pause.release();
    let exclusive = tokio::time::timeout(StdDuration::from_secs(5), writer)
        .await
        .expect("GC must proceed after both receipts finish")
        .unwrap()
        .unwrap();
    let table_path = fixture.store.paths.table_entry_path(
        fixture.bucket.storage_bucket_id().as_str(),
        &Namespace::parse("indexes").unwrap(),
        &CatalogName::parse("index.v1").unwrap(),
    );
    let current = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &table_path)
        .await
        .unwrap()
        .unwrap();
    let current: TableEntry = serde_json::from_slice(&current.data).unwrap();
    assert_eq!(current.generation, 2);
    for path in [&log_path, &receipt_path] {
        let object = fixture.backend.read_object(RUSTFS_META_BUCKET, path).await.unwrap().unwrap();
        let record: CommitLogEntry = serde_json::from_slice(&object.data).unwrap();
        assert_eq!(record, log);
        assert!(table_matches_committed_log(&current, &record));
    }
    drop(exclusive);
}

fn vector_create_intent_value(fixture: &PreparedFixture) -> serde_json::Value {
    let mut planned = serde_json::to_value(CatalogMetadataRefRecordV1::from(&fixture.initial)).unwrap();
    planned["body_revision"] = serde_json::Value::Null;
    planned["etag"] = serde_json::Value::Null;
    serde_json::json!({
        "version": 1,
        "bucket": VectorBucketBindingV1::from_handle(&fixture.bucket).unwrap(),
        "namespace": "indexes",
        "name": fixture.entry.table,
        "table_id": fixture.entry.table_id,
        "create_id": "71bbbc9f-d8e0-436f-8076-526320dd93d8",
        "request_id": "c8daaf1d-7e4b-45a4-96fd-d8ea76ed8958",
        "request_sha256": "00".repeat(32),
        "expected_parent": {
            "bucket_entry_etag": "parent-etag",
            "bucket": VectorBucketBindingV1::from_handle(&fixture.bucket).unwrap(),
            "state": "ACTIVE"
        },
        "expected_name": null,
        "definition": v3_definition_json(serde_json::json!({"owner": "research"})),
        "initialization_payload_base64": base64_simd::STANDARD.encode_to_string(
            br#"{"encoding_version":1,"data_type":"float32","dimension":3,"distance_metric":"cosine","non_filterable_metadata_keys":[]}"#
        ),
        "planned_manifest": planned,
        "candidate": null,
        "phase": "RESERVED",
        "published_head": null,
        "created_at": "2026-11-16T00:00:00Z",
        "updated_at": "2026-11-16T00:00:00Z"
    })
}

#[tokio::test]
async fn catalog_vector_v3_create_intent_requires_every_nullable_field_and_object_shape() {
    let fixture = prepared_fixture().await;
    let value = vector_create_intent_value(&fixture);
    let mut intent: VectorIndexCreateIntentV1 = serde_json::from_value(value).expect("complete reserved create record");
    intent.request_sha256 = intent.canonical_request_digest().unwrap().to_hex();
    intent.validate().unwrap();
    let value = serde_json::to_value(&intent).unwrap();
    for field in value.as_object().unwrap().keys() {
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<VectorIndexCreateIntentV1>(missing).is_err(),
            "accepted missing {field}"
        );
    }
    for pointer in ["", "/expected_parent", "/definition", "/planned_manifest"] {
        let mut invalid = value.clone();
        *invalid.pointer_mut(pointer).unwrap() = serde_json::json!([]);
        assert!(
            serde_json::from_value::<VectorIndexCreateIntentV1>(invalid).is_err(),
            "accepted array at {pointer}"
        );
    }
    let mut invalid = value.clone();
    invalid["phase"] = serde_json::json!({"RESERVED": null});
    assert!(serde_json::from_value::<VectorIndexCreateIntentV1>(invalid).is_err());
    let mut unknown = value;
    unknown["unknown"] = true.into();
    assert!(serde_json::from_value::<VectorIndexCreateIntentV1>(unknown).is_err());
}

#[tokio::test]
async fn catalog_vector_v3_create_intent_digest_and_planned_bytes_survive_reconstruction() {
    let fixture = prepared_fixture().await;
    let mut intent: VectorIndexCreateIntentV1 = serde_json::from_value(vector_create_intent_value(&fixture)).unwrap();
    intent.request_sha256 = intent.canonical_request_digest().unwrap().to_hex();
    intent.validate().unwrap();
    let encoded = serde_json::to_vec(&intent).unwrap();
    let reconstructed: VectorIndexCreateIntentV1 = serde_json::from_slice(&encoded).unwrap();
    reconstructed.validate().unwrap();
    assert_eq!(reconstructed, intent);
    for field in ["definition", "initialization_payload_base64", "planned_manifest"] {
        let mut changed = serde_json::to_value(&reconstructed).unwrap();
        match field {
            "definition" => changed[field]["tags"]["owner"] = "changed".into(),
            "initialization_payload_base64" => changed[field] = base64_simd::STANDARD.encode_to_string(b"changed").into(),
            "planned_manifest" => changed[field]["content_length"] = (fixture.initial.content_length() + 1).into(),
            _ => unreachable!(),
        }
        let changed: VectorIndexCreateIntentV1 = serde_json::from_value(changed).unwrap();
        assert!(changed.validate().is_err(), "accepted changed {field} with the original digest");
    }
    let mut changed = reconstructed;
    changed.planned_manifest.body_revision = Some(Uuid::new_v4());
    changed.request_sha256 = changed.canonical_request_digest().unwrap().to_hex();
    assert!(changed.validate().is_err(), "planned bytes must precede an object body revision");
}

#[tokio::test]
async fn catalog_vector_v3_delete_intent_binds_terminal_entry_to_exact_resource() {
    let (head, _, _, _) = v3_tags_history_fixture().await;
    let mut deleted = head.clone();
    deleted.state = TableCatalogEntryState::Deleted;
    deleted.generation += 1;
    deleted.version_token = "deletion-token".into();
    let mut intent: VectorIndexDeleteIntentV1 = serde_json::from_value(serde_json::json!({
        "version": 1,
        "bucket": head.vector_head_binding.as_ref().unwrap().bucket,
        "namespace": head.namespace,
        "name": head.table,
        "table_id": head.table_id,
        "delete_id": "606132d4-47ca-4537-bc53-716a4982d077",
        "phase": "PREPARED",
        "expected_entry": head,
        "deleted_entry": deleted,
        "request_sha256": "00".repeat(32),
        "created_at": "2026-11-16T00:00:00Z",
        "updated_at": "2026-11-16T00:00:00Z"
    }))
    .unwrap();
    intent.request_sha256 = intent.canonical_request_digest().unwrap().to_hex();
    intent.validate().unwrap();
    let old_id = intent.table_id.clone();
    let reconstructed: VectorIndexDeleteIntentV1 = serde_json::from_slice(&serde_json::to_vec(&intent).unwrap()).unwrap();
    assert_eq!(reconstructed.deleted_entry.table_id, old_id);
    assert_eq!(
        reconstructed.deleted_entry.vector_head_binding,
        reconstructed.expected_entry.vector_head_binding
    );
    let mut altered = reconstructed;
    altered.table_id = Uuid::new_v4().to_string();
    assert!(altered.validate().is_err());
    assert_eq!(altered.deleted_entry.table_id, old_id);
}

fn vector_creation_request(fixture: &PreparedFixture, table_id: &str, create_id: &str) -> VectorIndexCreateRequest {
    use crate::storage_api::table::managed::ManagedTableId;
    let resource = CatalogResourceIdentity::try_new(
        fixture.bucket.clone(),
        ManagedTableId::try_new(table_id.to_string()).unwrap(),
        CatalogFormat::Lance,
    )
    .unwrap();
    let planned = CatalogMetadataRef::from_read(
        CatalogObjectKey::try_new(format!("{}_versions/{create_id}/1.manifest", resource.dataset_prefix())).unwrap(),
        &TableCatalogObject {
            data: b"reserved-initial-content".to_vec(),
            body_revision: None,
            etag: None,
            mod_time: None,
        },
    )
    .unwrap();
    VectorIndexCreateRequest {
        resource_identity: resource,
        namespace: Namespace::parse("indexes").unwrap(),
        name: CatalogName::parse("new-index").unwrap(),
        create_id: CatalogCommitId::try_new(create_id.to_string()).unwrap(),
        request_id: Uuid::from_u128(0xa2b1304c66544cdd9a641aeec48df216),
        definition: VectorIndexDefinitionV1 {
            version: 1,
            effective_encryption: VectorEffectiveEncryptionV1::Unencrypted,
            tags: BTreeMap::from([("owner".into(), "research".into())]),
        },
        initialization_payload: br#"{"encoding_version":1,"data_type":"float32","dimension":3,"distance_metric":"cosine","non_filterable_metadata_keys":[]}"#.to_vec(),
        planned_manifest: planned,
    }
}

fn create_intent_path(fixture: &PreparedFixture, request: &VectorIndexCreateRequest) -> String {
    let entry_path =
        fixture
            .store
            .paths
            .table_entry_path(fixture.bucket.storage_bucket_id().as_str(), &request.namespace, &request.name);
    format!("{}create-intent.json", entry_path.strip_suffix("table-entry.json").unwrap())
}

async fn prepare_reserved_initial(
    fixture: &PreparedFixture,
    request: &VectorIndexCreateRequest,
    reservation: &VectorIndexCreateReservation,
) -> PreparedCatalogCommit {
    let bucket = fixture.bucket.storage_bucket_id().as_str();
    let key = request.planned_manifest.object_key().as_str();
    fixture
        .backend
        .seed_object(bucket, key, b"reserved-initial-content".to_vec())
        .await;
    let object = fixture.backend.read_object(bucket, key).await.unwrap().unwrap();
    let metadata = CatalogMetadataRef::from_read(request.planned_manifest.object_key().clone(), &object).unwrap();
    let descriptor =
        CatalogFormatDescriptor::try_new(1, CatalogFormat::Lance, 1, metadata.clone(), b"opaque-initial-descriptor".to_vec())
            .unwrap();
    PreparedCatalogCommit::try_from_validated_parts(
        CatalogCommitParts {
            resource_identity: request.resource_identity.clone(),
            namespace: request.namespace.clone(),
            name: request.name.clone(),
            expected_head: reservation.expected_head().clone(),
            commit_id: request.create_id.clone(),
            operation: "vector-create".into(),
            idempotency_key: None,
            writer: None,
        },
        metadata,
        descriptor,
        CatalogEntryUpdates::try_new(
            Some(format!(
                "s3://{bucket}/{}",
                request.resource_identity.dataset_prefix().trim_end_matches('/')
            )),
            Some(1),
        )
        .unwrap(),
        Vec::new(),
    )
    .unwrap()
}

#[tokio::test]
async fn catalog_vector_v3_creation_reserves_name_before_managed_upload() {
    let fixture = prepared_fixture().await;
    let request =
        vector_creation_request(&fixture, "058815dc-c8f4-4408-a04e-e2ff6fd43f24", "4afafc70-b99b-46a3-873f-b17199c6a027");
    let (reservation, guard) = fixture
        .store
        .reserve_vector_index(&fixture.bucket, request.clone())
        .await
        .unwrap();
    assert_eq!(guard.resource_identity(), &request.resource_identity);
    assert_eq!(reservation.expected_head().generation().get(), 0);
    let persisted = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &create_intent_path(&fixture, &request))
        .await
        .unwrap()
        .unwrap();
    let intent: VectorIndexCreateIntentV1 = serde_json::from_slice(&persisted.data).unwrap();
    assert_eq!(serde_json::to_value(&intent).unwrap()["phase"], "RESERVED");
    assert_eq!(intent.planned_manifest, CatalogMetadataRefRecordV1::from(&request.planned_manifest));
    assert!(intent.candidate.is_none());
    assert!(intent.published_head.is_none());
    assert!(
        fixture
            .store
            .read_table_with_etag(fixture.bucket.storage_bucket_id().as_str(), &request.namespace, &request.name)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .backend
            .read_object(
                fixture.bucket.storage_bucket_id().as_str(),
                request.planned_manifest.object_key().as_str()
            )
            .await
            .unwrap()
            .is_none()
    );
    let competitor =
        vector_creation_request(&fixture, "54d5f8ac-1d66-4075-a374-d618a450c0bc", "46366ef2-0d2a-40f8-9d5a-76d54a0e9669");
    assert_matches!(
        fixture.store.reserve_vector_index(&fixture.bucket, competitor).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
    let after = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &create_intent_path(&fixture, &request))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.data, persisted.data);
    guard.check_held().unwrap();
}

#[tokio::test]
async fn catalog_vector_v3_creation_reservation_recovers_after_lost_put_response() {
    let fixture = prepared_fixture().await;
    let request =
        vector_creation_request(&fixture, "c6731d91-7735-4db3-98dc-508bc7093c67", "bda2df3a-bbc3-4f45-ae6b-b245dd8c61b3");
    let path = create_intent_path(&fixture, &request);
    fixture.backend.fail_after_next_put(RUSTFS_META_BUCKET, &path).await;
    assert_matches!(
        fixture.store.reserve_vector_index(&fixture.bucket, request.clone()).await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("injected post-commit put failure")
    );
    let persisted = fixture.backend.read_object(RUSTFS_META_BUCKET, &path).await.unwrap().unwrap();
    let restarted = ObjectTableCatalogStore::for_vector_scope(fixture.backend.clone(), fixture.bucket.clone())
        .await
        .unwrap();
    let (reservation, guard) = restarted
        .reserve_vector_index(&fixture.bucket, request.clone())
        .await
        .unwrap();
    assert_eq!(guard.resource_identity(), &request.resource_identity);
    assert_eq!(reservation.expected_head().generation().get(), 0);
    let after = fixture.backend.read_object(RUSTFS_META_BUCKET, &path).await.unwrap().unwrap();
    assert_eq!(after.data, persisted.data);
    let mut changed = request;
    changed.definition.tags.insert("owner".into(), "changed".into());
    assert_matches!(
        restarted.reserve_vector_index(&fixture.bucket, changed).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
}

#[tokio::test]
async fn catalog_vector_v3_initial_publication_recovers_exact_head_after_lost_response() {
    let fixture = prepared_fixture().await;
    let request =
        vector_creation_request(&fixture, "e9d2c8ea-9595-4622-9a44-b12c6da10170", "37bf6089-5ad8-4057-b9ba-0c9bfd8d994b");
    let (reservation, guard) = fixture
        .store
        .reserve_vector_index(&fixture.bucket, request.clone())
        .await
        .unwrap();
    let prepared = prepare_reserved_initial(&fixture, &request, &reservation).await;
    let entry_path =
        fixture
            .store
            .paths
            .table_entry_path(fixture.bucket.storage_bucket_id().as_str(), &request.namespace, &request.name);
    fixture.backend.fail_after_next_put(RUSTFS_META_BUCKET, &entry_path).await;
    assert_matches!(
        fixture.store.publish_vector_index_initial_head(&guard, &reservation, prepared.clone()).await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("injected post-commit put failure")
    );
    let initial = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &entry_path)
        .await
        .unwrap()
        .unwrap();
    let visible: TableEntry = serde_json::from_slice(&initial.data).unwrap();
    assert_eq!(visible.version, 3);
    assert_eq!(visible.generation, 1);
    assert_eq!(visible.vector_index_definition.as_ref(), Some(&request.definition));
    assert_eq!(
        CatalogHead::from_vector_entry(&visible).unwrap().metadata(),
        prepared.candidate_metadata()
    );
    drop(guard);
    let restarted = ObjectTableCatalogStore::for_vector_scope(fixture.backend.clone(), fixture.bucket.clone())
        .await
        .unwrap();
    let (reservation, guard) = restarted
        .reserve_vector_index(&fixture.bucket, request.clone())
        .await
        .unwrap();
    let result = restarted
        .publish_vector_index_initial_head(&guard, &reservation, prepared)
        .await
        .unwrap();
    assert_eq!(result.head().generation().get(), 1);
    assert_eq!(result.head().version_token().as_str(), visible.version_token);
    let after = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &entry_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.data, initial.data);
    assert_eq!(after.etag, initial.etag);
    let persisted = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &create_intent_path(&fixture, &request))
        .await
        .unwrap()
        .unwrap();
    let intent: VectorIndexCreateIntentV1 = serde_json::from_slice(&persisted.data).unwrap();
    assert_eq!(serde_json::to_value(&intent).unwrap()["phase"], "PUBLISHED");
    assert_eq!(intent.published_head, Some(visible));
}

#[tokio::test]
async fn catalog_vector_v3_cancelled_creation_keeps_gc_until_published_intent() {
    let fixture = prepared_fixture().await;
    let request =
        vector_creation_request(&fixture, "336d218b-296c-4c7d-97d2-f3a124af871f", "6363a657-27c6-4d49-b9f8-b279e9425b28");
    let (reservation, guard) = fixture
        .store
        .reserve_vector_index(&fixture.bucket, request.clone())
        .await
        .unwrap();
    let prepared = prepare_reserved_initial(&fixture, &request, &reservation).await;
    let table_path =
        fixture
            .store
            .paths
            .table_entry_path(fixture.bucket.storage_bucket_id().as_str(), &request.namespace, &request.name);
    let pause = fixture.backend.pause_next_put(RUSTFS_META_BUCKET, &table_path).await;
    let store = fixture.store.clone();
    let operation_guard = guard.clone();
    let mut caller = tokio::spawn(async move {
        store
            .publish_vector_index_initial_head(&operation_guard, &reservation, prepared)
            .await
    });
    tokio::time::timeout(StdDuration::from_secs(5), async {
        tokio::select! {
            _ = pause.wait_started() => {}
            result = &mut caller => panic!("publication ended before the selected head write: {result:?}"),
        }
    })
    .await
    .expect("publication must reach the selected head write");
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    drop(guard);
    let gc_path = fixture
        .store
        .paths
        .vector_gc_lock_path(fixture.bucket.storage_bucket_id().as_str(), request.resource_identity.table_id().as_str());
    let before = fixture
        .backend
        .write_lock_acquisition_count(RUSTFS_META_BUCKET, &gc_path)
        .await;
    let backend = fixture.backend.clone();
    let writer_path = gc_path.clone();
    let writer = tokio::spawn(async move { backend.acquire_write_lock(RUSTFS_META_BUCKET, &writer_path).await });
    tokio::time::timeout(StdDuration::from_secs(5), async {
        while fixture
            .backend
            .write_lock_acquisition_count(RUSTFS_META_BUCKET, &gc_path)
            .await
            == before
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!writer.is_finished());
    pause.release();
    let exclusive = tokio::time::timeout(StdDuration::from_secs(5), writer)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let saved = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &create_intent_path(&fixture, &request))
        .await
        .unwrap()
        .unwrap();
    let saved: VectorIndexCreateIntentV1 = serde_json::from_slice(&saved.data).unwrap();
    assert_eq!(saved.phase, VectorIndexCreatePhase::Published);
    let head = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &table_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(serde_json::from_slice::<TableEntry>(&head.data).unwrap(), saved.published_head.unwrap());
    drop(exclusive);
}

#[tokio::test]
async fn catalog_vector_v3_tags_advance_generation_without_changing_manifest() {
    let fixture = prepared_v3_fixture().await;
    let (guard, entry) = fixture
        .store
        .begin_vector_operation(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .unwrap();
    let tagged = fixture
        .store
        .update_vector_tags_with_guard(
            &guard,
            &entry,
            VectorTagUpdate::Set(BTreeMap::from([("team".into(), "search".into())])),
            CatalogCommitId::try_new("bf7ba729-365e-4c2c-893a-1b78d2e340d2".into()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(tagged.head().generation().get(), 2);
    assert_eq!(tagged.head().metadata(), entry.head().metadata());
    assert_eq!(tagged.head().format_descriptor(), entry.head().format_descriptor());
    assert_eq!(tagged.definition().tags["team"], "search");
    let history = fixture
        .store
        .read_committed_history_with_guard(&guard, tagged.head())
        .await
        .unwrap();
    assert_eq!(history.versions().len(), 2);
    assert_eq!(history.versions()[0].metadata(), history.versions()[1].metadata());
    assert_eq!(history.versions()[0].format_descriptor(), history.versions()[1].format_descriptor());
    assert_matches!(
        fixture
            .store
            .update_vector_tags_with_guard(
                &guard,
                &entry,
                VectorTagUpdate::Remove(BTreeSet::from(["team".into()])),
                CatalogCommitId::try_new("6bdff6de-eaeb-4b85-a048-321fd2df5994".into()).unwrap()
            )
            .await,
        Err(TableCatalogStoreError::Conflict(_))
    );
}

#[tokio::test]
async fn catalog_vector_v3_old_delete_recovery_preserves_same_name_replacement() {
    let fixture = prepared_fixture().await;
    let first = vector_creation_request(&fixture, "c6e6f29e-4729-4890-b8c4-b4cbef41c260", "dc8c4dcb-f8e7-4fe2-8531-e6d7c443c2fe");
    let (reservation, guard) = fixture
        .store
        .reserve_vector_index(&fixture.bucket, first.clone())
        .await
        .unwrap();
    let prepared = prepare_reserved_initial(&fixture, &first, &reservation).await;
    let entry = fixture
        .store
        .publish_vector_index_initial_head(&guard, &reservation, prepared)
        .await
        .unwrap();
    fixture
        .store
        .delete_vector_index_with_guard(
            &guard,
            &entry,
            CatalogCommitId::try_new("77e7dd7b-23ad-4d7d-a088-a7d52dcd1fc7".into()).unwrap(),
        )
        .await
        .unwrap();
    drop(guard);
    let second =
        vector_creation_request(&fixture, "8e35bbf3-29da-4332-a763-fd1a4e49012b", "10fd427c-15f2-4242-8529-16c3c95e7624");
    let (reservation, guard) = fixture
        .store
        .reserve_vector_index(&fixture.bucket, second.clone())
        .await
        .unwrap();
    let prepared = prepare_reserved_initial(&fixture, &second, &reservation).await;
    let replacement = fixture
        .store
        .publish_vector_index_initial_head(&guard, &reservation, prepared)
        .await
        .unwrap();
    let path = fixture
        .store
        .paths
        .table_entry_path(fixture.bucket.storage_bucket_id().as_str(), &second.namespace, &second.name);
    let before = fixture.backend.read_object(RUSTFS_META_BUCKET, &path).await.unwrap().unwrap();
    let old_object = fixture
        .backend
        .read_object(fixture.bucket.storage_bucket_id().as_str(), first.planned_manifest.object_key().as_str())
        .await
        .unwrap()
        .unwrap();
    let replacement_object = fixture
        .backend
        .read_object(fixture.bucket.storage_bucket_id().as_str(), second.planned_manifest.object_key().as_str())
        .await
        .unwrap()
        .unwrap();
    let restarted = ObjectTableCatalogStore::for_vector_scope(fixture.backend.clone(), fixture.bucket.clone())
        .await
        .unwrap();
    restarted
        .recover_vector_index_deletion(&fixture.bucket, first.resource_identity.table_id().as_str())
        .await
        .unwrap();
    let after = fixture.backend.read_object(RUSTFS_META_BUCKET, &path).await.unwrap().unwrap();
    assert_eq!(after.data, before.data);
    assert_eq!(after.etag, before.etag);
    assert_eq!(replacement.head().resource_identity(), &second.resource_identity);
    assert_eq!(
        fixture
            .backend
            .read_object(fixture.bucket.storage_bucket_id().as_str(), first.planned_manifest.object_key().as_str())
            .await
            .unwrap()
            .unwrap()
            .data,
        old_object.data
    );
    assert_eq!(
        fixture
            .backend
            .read_object(fixture.bucket.storage_bucket_id().as_str(), second.planned_manifest.object_key().as_str())
            .await
            .unwrap()
            .unwrap()
            .data,
        replacement_object.data
    );
}

#[tokio::test]
async fn catalog_vector_v3_prepared_delete_intent_does_not_prove_publication() {
    let fixture = prepared_v3_fixture().await;
    let (guard, entry) = fixture
        .store
        .begin_vector_operation(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .unwrap();
    let intent_path = fixture
        .store
        .paths
        .vector_delete_intent_path(fixture.bucket.storage_bucket_id().as_str(), &fixture.entry.table_id);
    fixture.backend.fail_after_next_put(RUSTFS_META_BUCKET, &intent_path).await;
    assert_matches!(
        fixture.store.delete_vector_index_with_guard(
            &guard,
            &entry,
            CatalogCommitId::try_new("5b753425-e78b-4818-8251-73ab46f94fb2".into()).unwrap()
        ).await,
        Err(TableCatalogStoreError::Internal(message)) if message.contains("injected post-commit put failure")
    );
    let intent_object = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &intent_path)
        .await
        .unwrap()
        .unwrap();
    let intent: VectorIndexDeleteIntentV1 = serde_json::from_slice(&intent_object.data).unwrap();
    assert_eq!(intent.phase, VectorIndexDeletePhase::Prepared);
    let changed = fixture
        .store
        .update_vector_tags_with_guard(
            &guard,
            &entry,
            VectorTagUpdate::Set(BTreeMap::from([("changed".into(), "true".into())])),
            CatalogCommitId::try_new("a35b6b74-f938-47af-adfe-4506ff71eaa4".into()).unwrap(),
        )
        .await
        .unwrap();
    assert_matches!(
        fixture
            .store
            .recover_vector_index_deletion(&fixture.bucket, &fixture.entry.table_id)
            .await,
        Err(TableCatalogStoreError::Conflict(_) | TableCatalogStoreError::Invalid(_))
    );
    let current = fixture.store.read_vector_entry_with_guard(&guard).await.unwrap();
    assert_eq!(current.head().expected_head(), changed.head().expected_head());
    assert_eq!(current.definition(), changed.definition());
}

#[tokio::test]
async fn catalog_vector_v3_cancelled_delete_keeps_gc_until_terminal_intent() {
    let fixture = prepared_v3_fixture().await;
    let (guard, entry) = fixture
        .store
        .begin_vector_operation(
            &fixture.bucket,
            &Namespace::parse("indexes").unwrap(),
            &CatalogName::parse("index.v1").unwrap(),
        )
        .await
        .unwrap();
    let table_path =
        fixture
            .store
            .paths
            .table_entry_path(fixture.bucket.storage_bucket_id().as_str(), guard.namespace(), guard.name());
    let pause = fixture.backend.pause_next_put(RUSTFS_META_BUCKET, &table_path).await;
    let store = fixture.store.clone();
    let operation_guard = guard.clone();
    let mut caller = tokio::spawn(async move {
        store
            .delete_vector_index_with_guard(
                &operation_guard,
                &entry,
                CatalogCommitId::try_new("567ec5d8-85b8-46c2-a88a-bca961414867".into()).unwrap(),
            )
            .await
    });
    tokio::time::timeout(StdDuration::from_secs(5), async {
        tokio::select! {
            _ = pause.wait_started() => {}
            result = &mut caller => panic!("publication ended before the selected head write: {result:?}"),
        }
    })
    .await
    .expect("publication must reach the selected head write");
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    drop(guard);
    let gc_path = fixture
        .store
        .paths
        .vector_gc_lock_path(fixture.bucket.storage_bucket_id().as_str(), &fixture.entry.table_id);
    let before = fixture
        .backend
        .write_lock_acquisition_count(RUSTFS_META_BUCKET, &gc_path)
        .await;
    let backend = fixture.backend.clone();
    let writer_path = gc_path.clone();
    let writer = tokio::spawn(async move { backend.acquire_write_lock(RUSTFS_META_BUCKET, &writer_path).await });
    tokio::time::timeout(StdDuration::from_secs(5), async {
        while fixture
            .backend
            .write_lock_acquisition_count(RUSTFS_META_BUCKET, &gc_path)
            .await
            == before
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!writer.is_finished());
    pause.release();
    let exclusive = tokio::time::timeout(StdDuration::from_secs(5), writer)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let intent_path = fixture
        .store
        .paths
        .vector_delete_intent_path(fixture.bucket.storage_bucket_id().as_str(), &fixture.entry.table_id);
    let intent = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &intent_path)
        .await
        .unwrap()
        .unwrap();
    let intent: VectorIndexDeleteIntentV1 = serde_json::from_slice(&intent.data).unwrap();
    assert_eq!(intent.phase, VectorIndexDeletePhase::LogicallyDeleted);
    let head = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &table_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(serde_json::from_slice::<TableEntry>(&head.data).unwrap(), intent.deleted_entry);
    drop(exclusive);
}

#[tokio::test]
async fn catalog_vector_v3_published_resume_does_not_require_retired_initial_object() {
    let fixture = prepared_fixture().await;
    let request =
        vector_creation_request(&fixture, "c477807f-eebc-429c-909a-abf8c32c0e70", "019685ec-590a-4805-aec8-c5f560016bbd");
    let (reservation, guard) = fixture
        .store
        .reserve_vector_index(&fixture.bucket, request.clone())
        .await
        .unwrap();
    let prepared = prepare_reserved_initial(&fixture, &request, &reservation).await;
    let initial = fixture
        .store
        .publish_vector_index_initial_head(&guard, &reservation, prepared)
        .await
        .unwrap();
    // This test exercises Catalog recovery's history proof independently of
    // physical retention eligibility, which belongs to the maintenance owner.
    fixture
        .backend
        .delete_object(
            fixture.bucket.storage_bucket_id().as_str(),
            request.planned_manifest.object_key().as_str(),
        )
        .await
        .unwrap();
    drop(guard);
    let restarted = ObjectTableCatalogStore::for_vector_scope(fixture.backend.clone(), fixture.bucket.clone())
        .await
        .unwrap();
    let (resumed, _) = restarted
        .resume_vector_index_creation(&fixture.bucket, &request.namespace, &request.name)
        .await
        .unwrap();
    assert_eq!(resumed.phase(), VectorIndexCreatePhase::Published);
    assert_eq!(resumed.published_entry().unwrap().head().expected_head(), initial.head().expected_head());
    assert_eq!(resumed.initialization_payload(), request.initialization_payload);
}

#[tokio::test]
async fn catalog_vector_v3_parent_registration_recovers_all_three_creation_boundaries() {
    for stop_after in ["bucket-metadata", "parent", "namespace"] {
        let backend = TestCatalogObjectBackend::default();
        let bucket = vector_bucket();
        backend
            .state
            .lock()
            .await
            .bucket_handles
            .insert(bucket.storage_bucket_id().as_str().into(), bucket.clone());
        let store = ObjectTableCatalogStore::for_vector_scope(backend.clone(), bucket.clone())
            .await
            .unwrap();
        let namespace = Namespace::parse("indexes").unwrap();
        let parent_path = store.paths.table_bucket_entry_path(bucket.storage_bucket_id().as_str());
        let namespace_path = store
            .paths
            .namespace_entry_path(bucket.storage_bucket_id().as_str(), &namespace);
        if stop_after != "bucket-metadata" {
            let fail_path = if stop_after == "parent" {
                &parent_path
            } else {
                &namespace_path
            };
            backend.fail_after_next_put(RUSTFS_META_BUCKET, fail_path).await;
            let lifecycle = backend.acquire_bucket_lifecycle_read(&bucket).await.unwrap();
            assert_matches!(
                store.ensure_vector_catalog_parent_guarded(&bucket, lifecycle).await,
                Err(TableCatalogStoreError::Internal(message)) if message.contains("injected post-commit put failure")
            );
            assert!(backend.read_object(RUSTFS_META_BUCKET, &parent_path).await.unwrap().is_some());
            assert_eq!(
                backend
                    .read_object(RUSTFS_META_BUCKET, &namespace_path)
                    .await
                    .unwrap()
                    .is_some(),
                stop_after == "namespace"
            );
        }
        let parent_before = backend.read_object(RUSTFS_META_BUCKET, &parent_path).await.unwrap();
        let namespace_before = backend.read_object(RUSTFS_META_BUCKET, &namespace_path).await.unwrap();
        let restarted = ObjectTableCatalogStore::for_vector_scope(backend.clone(), bucket.clone())
            .await
            .unwrap();
        let lifecycle = backend.acquire_bucket_lifecycle_read(&bucket).await.unwrap();
        restarted
            .ensure_vector_catalog_parent_guarded(&bucket, lifecycle)
            .await
            .unwrap();
        let parent = backend.read_object(RUSTFS_META_BUCKET, &parent_path).await.unwrap().unwrap();
        let indexes = backend
            .read_object(RUSTFS_META_BUCKET, &namespace_path)
            .await
            .unwrap()
            .unwrap();
        if let Some(before) = parent_before {
            assert_eq!(parent.data, before.data);
            assert_eq!(parent.etag, before.etag);
        }
        if let Some(before) = namespace_before {
            assert_eq!(indexes.data, before.data);
            assert_eq!(indexes.etag, before.etag);
        }
        let parent: TableBucketEntry = serde_json::from_slice(&parent.data).unwrap();
        let indexes: NamespaceEntry = serde_json::from_slice(&indexes.data).unwrap();
        assert_eq!(parent.vector_bucket_binding.unwrap().to_handle().unwrap(), bucket);
        assert_eq!(parent.state, TableCatalogEntryState::Active);
        assert_eq!(indexes.namespace, "indexes");
        assert_eq!(indexes.state, TableCatalogEntryState::Active);
        let lifecycle = backend.acquire_bucket_lifecycle_read(&bucket).await.unwrap();
        restarted
            .ensure_vector_catalog_parent_guarded(&bucket, lifecycle)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn catalog_vector_v3_parent_registration_rejects_retained_old_incarnation() {
    use crate::storage_api::table::managed::BucketIncarnation;
    let fixture = prepared_fixture().await;
    let parent_path = fixture
        .store
        .paths
        .table_bucket_entry_path(fixture.bucket.storage_bucket_id().as_str());
    let before = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &parent_path)
        .await
        .unwrap()
        .unwrap();
    let replacement = BucketHandle::try_from_parts(
        fixture.bucket.kind(),
        fixture.bucket.account_namespace_id().clone(),
        fixture.bucket.region().to_string(),
        fixture.bucket.public_name().clone(),
        fixture.bucket.storage_bucket_id().clone(),
        BucketIncarnation::try_new(Uuid::from_u128(0xe7ec11a771f64388915866e3e5be8d62)).unwrap(),
    )
    .unwrap();
    fixture
        .backend
        .state
        .lock()
        .await
        .bucket_handles
        .insert(replacement.storage_bucket_id().as_str().into(), replacement.clone());
    let store = ObjectTableCatalogStore::for_vector_scope(fixture.backend.clone(), replacement.clone())
        .await
        .unwrap();
    let lifecycle = fixture.backend.acquire_bucket_lifecycle_read(&replacement).await.unwrap();
    assert_matches!(
        store.ensure_vector_catalog_parent_guarded(&replacement, lifecycle).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
    let after = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &parent_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.data, before.data);
    assert_eq!(after.etag, before.etag);
}

type NativeVectorParentStorage = crate::storage_api::startup::services::ECStore;

struct NativeVectorParentFixture {
    bucket: BucketHandle,
    storage: Arc<NativeVectorParentStorage>,
    backend: EcStoreTableCatalogObjectBackend<NativeVectorParentStorage>,
    catalog: ObjectTableCatalogStore<EcStoreTableCatalogObjectBackend<NativeVectorParentStorage>>,
    guard: Arc<crate::storage_api::table::managed::BucketIncarnationFenceGuard>,
}

async fn native_vector_parent_fixture() -> (NativeVectorParentFixture, impl Send) {
    let (request, access, storage, reset) =
        crate::app::object::managed_request_fixture_with_encryption(crate::app::object::ManagedEncryption::Unencrypted).await;
    let app = request.server_context().unwrap().installed_app_context().unwrap();
    let bucket = access.bucket().clone();
    let guard = Arc::new(
        crate::app::bucket_usecase::DefaultBucketUsecase::with_context(Some(app))
            .acquire_bucket_handle_guard(&bucket)
            .await
            .expect("real Managed bucket lifecycle guard"),
    );
    let backend = EcStoreTableCatalogObjectBackend::new(storage.clone());
    let catalog = ObjectTableCatalogStore::for_vector_scope(backend.clone(), bucket.clone())
        .await
        .expect("real Vector Catalog scope");
    (
        NativeVectorParentFixture {
            bucket,
            storage,
            backend,
            catalog,
            guard,
        },
        reset,
    )
}

#[tokio::test]
#[serial_test::serial]
async fn catalog_vector_v3_native_parent_reuses_same_instance_guard() {
    use crate::storage_api::table::managed::ManagedBucketStorage as _;
    let (fixture, _reset) = native_vector_parent_fixture().await;
    let bucket = fixture.bucket.storage_bucket_id().as_str();
    fixture.storage.validate_managed_bucket_fence(&fixture.guard, bucket).unwrap();
    fixture
        .catalog
        .ensure_vector_catalog_parent(&fixture.bucket, fixture.guard.clone())
        .await
        .unwrap();
    let namespace = Namespace::parse("indexes").unwrap();
    let parent_path = fixture.catalog.paths.table_bucket_entry_path(bucket);
    let namespace_path = fixture.catalog.paths.namespace_entry_path(bucket, &namespace);
    let parent = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &parent_path)
        .await
        .unwrap()
        .unwrap();
    let indexes = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &namespace_path)
        .await
        .unwrap()
        .unwrap();
    let parent_entry: TableBucketEntry = serde_json::from_slice(&parent.data).unwrap();
    let namespace_entry: NamespaceEntry = serde_json::from_slice(&indexes.data).unwrap();
    assert_eq!(parent_entry.vector_bucket_binding.unwrap().to_handle().unwrap(), fixture.bucket);
    assert_eq!(parent_entry.state, TableCatalogEntryState::Active);
    assert_eq!(namespace_entry.table_bucket, bucket);
    assert_eq!(namespace_entry.namespace, "indexes");
    assert_eq!(namespace_entry.state, TableCatalogEntryState::Active);
    fixture
        .catalog
        .ensure_vector_catalog_parent(&fixture.bucket, fixture.guard.clone())
        .await
        .unwrap();
    let repeated_parent = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &parent_path)
        .await
        .unwrap()
        .unwrap();
    let repeated_indexes = fixture
        .backend
        .read_object(RUSTFS_META_BUCKET, &namespace_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repeated_parent.data, parent.data);
    assert_eq!(repeated_parent.etag, parent.etag);
    assert_eq!(repeated_parent.body_revision, parent.body_revision);
    assert_eq!(repeated_indexes.data, indexes.data);
    assert_eq!(repeated_indexes.etag, indexes.etag);
    assert_eq!(repeated_indexes.body_revision, indexes.body_revision);
    assert_eq!(
        Arc::strong_count(&fixture.guard),
        1,
        "completed registration releases its borrowed ownership"
    );
    fixture.storage.validate_managed_bucket_fence(&fixture.guard, bucket).unwrap();
}

#[tokio::test]
#[serial_test::serial]
async fn catalog_vector_v3_native_parent_rejects_another_bucket_guard() {
    use crate::storage_api::table::managed::ManagedBucketStorage as _;
    let (source, _source_reset) = native_vector_parent_fixture().await;
    let (target, _target_reset) = native_vector_parent_fixture().await;
    assert!(Arc::ptr_eq(&source.storage, &target.storage));
    assert_ne!(source.bucket.storage_bucket_id(), target.bucket.storage_bucket_id());
    target
        .storage
        .validate_managed_bucket_fence(&target.guard, target.bucket.storage_bucket_id().as_str())
        .unwrap();
    let parent_path = target
        .catalog
        .paths
        .table_bucket_entry_path(target.bucket.storage_bucket_id().as_str());
    let namespace_path = target
        .catalog
        .paths
        .namespace_entry_path(target.bucket.storage_bucket_id().as_str(), &Namespace::parse("indexes").unwrap());
    assert!(
        target
            .backend
            .read_object(RUSTFS_META_BUCKET, &parent_path)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        target
            .backend
            .read_object(RUSTFS_META_BUCKET, &namespace_path)
            .await
            .unwrap()
            .is_none()
    );
    assert_matches!(
        target.catalog.ensure_vector_catalog_parent(&target.bucket, source.guard.clone()).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("validate shared managed bucket lifecycle")
    );
    assert!(
        target
            .backend
            .read_object(RUSTFS_META_BUCKET, &parent_path)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        target
            .backend
            .read_object(RUSTFS_META_BUCKET, &namespace_path)
            .await
            .unwrap()
            .is_none()
    );
    source
        .storage
        .validate_managed_bucket_fence(&source.guard, source.bucket.storage_bucket_id().as_str())
        .unwrap();
}

#[tokio::test]
#[serial_test::serial]
async fn catalog_vector_v3_native_parent_rejects_another_instance_guard() {
    use crate::app::storage_api::managed::make_managed_bucket;
    use crate::app::storage_api::test::contract::bucket::MakeBucketOptions;
    use crate::storage_api::table::managed::ManagedBucketStorage as _;
    let (source, _reset) = native_vector_parent_fixture().await;
    let (_temp, _paths, isolated) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
    assert!(!Arc::ptr_eq(&source.storage, &isolated));
    make_managed_bucket(&isolated, &source.bucket, &[], &[], &MakeBucketOptions::default())
        .await
        .unwrap();
    let backend = EcStoreTableCatalogObjectBackend::new(isolated.clone());
    let catalog = ObjectTableCatalogStore::for_vector_scope(backend.clone(), source.bucket.clone())
        .await
        .unwrap();
    let parent_path = catalog
        .paths
        .table_bucket_entry_path(source.bucket.storage_bucket_id().as_str());
    let namespace_path = catalog
        .paths
        .namespace_entry_path(source.bucket.storage_bucket_id().as_str(), &Namespace::parse("indexes").unwrap());
    assert!(backend.read_object(RUSTFS_META_BUCKET, &parent_path).await.unwrap().is_none());
    assert!(
        backend
            .read_object(RUSTFS_META_BUCKET, &namespace_path)
            .await
            .unwrap()
            .is_none()
    );
    assert_matches!(
        isolated.validate_managed_bucket_fence(&source.guard, source.bucket.storage_bucket_id().as_str()),
        Err(crate::storage_api::table::StorageError::PreconditionFailed)
    );
    assert_matches!(
        catalog.ensure_vector_catalog_parent(&source.bucket, source.guard.clone()).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("validate shared managed bucket lifecycle")
    );
    assert!(backend.read_object(RUSTFS_META_BUCKET, &parent_path).await.unwrap().is_none());
    assert!(
        backend
            .read_object(RUSTFS_META_BUCKET, &namespace_path)
            .await
            .unwrap()
            .is_none()
    );
    source
        .storage
        .validate_managed_bucket_fence(&source.guard, source.bucket.storage_bucket_id().as_str())
        .unwrap();
}

#[tokio::test]
async fn catalog_vector_v3_creation_enumeration_preserves_pending_and_published_records() {
    let fixture = prepared_fixture().await;
    let mut pending =
        vector_creation_request(&fixture, "acb99a64-f604-461c-984a-bf38aa8c3991", "28f4b8a3-1728-4611-92f4-e461c1e32015");
    pending.name = CatalogName::parse("pending-index").unwrap();
    let (pending_reservation, pending_guard) = fixture
        .store
        .reserve_vector_index(&fixture.bucket, pending.clone())
        .await
        .unwrap();
    let mut published =
        vector_creation_request(&fixture, "e2ead908-a802-4a0a-9a14-d18ab29b5366", "9469611b-66bf-426d-98ef-3a1b5eb74d12");
    published.name = CatalogName::parse("published-index").unwrap();
    let (published_reservation, published_guard) = fixture
        .store
        .reserve_vector_index(&fixture.bucket, published.clone())
        .await
        .unwrap();
    let prepared = prepare_reserved_initial(&fixture, &published, &published_reservation).await;
    fixture
        .store
        .publish_vector_index_initial_head(&published_guard, &published_reservation, prepared)
        .await
        .unwrap();
    let before_objects = fixture.backend.state.lock().await.objects.clone();
    let before_puts = fixture.backend.state.lock().await.put_attempts.clone();
    let listed = fixture
        .store
        .list_vector_index_creation_reservations(&fixture.bucket)
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);
    for (request, phase) in [
        (&pending, VectorIndexCreatePhase::Reserved),
        (&published, VectorIndexCreatePhase::Published),
    ] {
        let found = listed
            .iter()
            .find(|item| item.name() == &request.name)
            .expect("both canonical reservations are enumerated");
        assert_eq!(found.phase(), phase);
        assert_eq!(found.resource_identity(), &request.resource_identity);
        assert_eq!(found.create_id(), &request.create_id);
        assert_eq!(found.namespace(), &request.namespace);
        assert_eq!(found.planned_manifest(), &request.planned_manifest);
        assert_eq!(found.definition(), &request.definition);
        assert_eq!(found.initialization_payload(), request.initialization_payload);
        assert!(found.published_entry().is_none(), "enumeration cannot replace guarded publication proof");
    }
    assert_eq!(pending_reservation.phase(), VectorIndexCreatePhase::Reserved);
    pending_guard.check_held().unwrap();
    published_guard.check_held().unwrap();
    assert_eq!(fixture.backend.state.lock().await.objects, before_objects);
    assert_eq!(fixture.backend.state.lock().await.put_attempts, before_puts);
}

#[tokio::test]
async fn catalog_vector_v3_creation_enumeration_rejects_malformed_or_changed_digest() {
    for corruption in ["json", "digest"] {
        let fixture = prepared_fixture().await;
        let request =
            vector_creation_request(&fixture, "bd057f04-e76a-40fd-bdef-715e285f8e47", "dc71e65a-df0e-4a72-8475-3855017b2618");
        let (_reservation, guard) = fixture
            .store
            .reserve_vector_index(&fixture.bucket, request.clone())
            .await
            .unwrap();
        let path = create_intent_path(&fixture, &request);
        let saved = fixture.backend.read_object(RUSTFS_META_BUCKET, &path).await.unwrap().unwrap();
        let corrupted = if corruption == "json" {
            b"{\"version\":".to_vec()
        } else {
            let mut intent: VectorIndexCreateIntentV1 = serde_json::from_slice(&saved.data).unwrap();
            intent
                .definition
                .tags
                .insert("changed-after-reservation".into(), "yes".into());
            serde_json::to_vec(&intent).unwrap()
        };
        fixture.backend.seed_object(RUSTFS_META_BUCKET, &path, corrupted).await;
        let before_objects = fixture.backend.state.lock().await.objects.clone();
        let before_puts = fixture.backend.state.lock().await.put_attempts.clone();
        let result = fixture.store.list_vector_index_creation_reservations(&fixture.bucket).await;
        assert_matches!(result, Err(TableCatalogStoreError::Invalid(_)));
        assert_eq!(fixture.backend.state.lock().await.objects, before_objects);
        assert_eq!(fixture.backend.state.lock().await.put_attempts, before_puts);
        guard.check_held().unwrap();
    }
}

#[tokio::test]
async fn catalog_vector_v3_creation_enumeration_rejects_disappearance_after_list() {
    let fixture = prepared_fixture().await;
    let request =
        vector_creation_request(&fixture, "44b49813-efce-4600-bd25-46df8837b026", "955bf96b-9fc4-440e-a3e0-f59c01feb5e9");
    let (_reservation, guard) = fixture
        .store
        .reserve_vector_index(&fixture.bucket, request.clone())
        .await
        .unwrap();
    let path = create_intent_path(&fixture, &request);
    let pause = fixture.backend.pause_before_next_read(RUSTFS_META_BUCKET, &path).await;
    let lists_before = fixture.backend.list_call_count().await;
    let before_puts = fixture.backend.state.lock().await.put_attempts.clone();
    let store = fixture.store.clone();
    let bucket = fixture.bucket.clone();
    let mut enumeration = tokio::spawn(async move { store.list_vector_index_creation_reservations(&bucket).await });
    tokio::select! {
        result = &mut enumeration => panic!("enumeration returned before its selected record read: {result:?}"),
        paused = tokio::time::timeout(std::time::Duration::from_secs(5), pause.wait_started()) => {
            paused.expect("enumeration reaches the selected read after LIST");
        },
    }
    assert_eq!(fixture.backend.list_call_count().await, lists_before + 1);
    assert!(
        fixture
            .backend
            .state
            .lock()
            .await
            .objects
            .contains_key(&(RUSTFS_META_BUCKET.into(), path.clone()))
    );
    // Inject backend disappearance after LIST and before the paused object lookup.
    fixture.backend.delete_object(RUSTFS_META_BUCKET, &path).await.unwrap();
    assert!(
        !fixture
            .backend
            .state
            .lock()
            .await
            .objects
            .contains_key(&(RUSTFS_META_BUCKET.into(), path))
    );
    pause.release();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), enumeration)
        .await
        .unwrap()
        .unwrap();
    assert_matches!(result, Err(TableCatalogStoreError::Conflict(message)) if message.contains("disappeared during enumeration"));
    assert_eq!(fixture.backend.state.lock().await.put_attempts, before_puts);
    guard.check_held().unwrap();
}

#[tokio::test]
async fn catalog_vector_v3_creation_enumeration_rejects_another_incarnation() {
    use crate::storage_api::table::managed::BucketIncarnation;
    let fixture = prepared_fixture().await;
    let request =
        vector_creation_request(&fixture, "3b964306-2723-478b-b766-aebf270df0b3", "03131648-24d0-4139-b5ec-c12c854e2501");
    let (_reservation, guard) = fixture
        .store
        .reserve_vector_index(&fixture.bucket, request.clone())
        .await
        .unwrap();
    let path = create_intent_path(&fixture, &request);
    let saved = fixture.backend.read_object(RUSTFS_META_BUCKET, &path).await.unwrap().unwrap();
    let other = BucketHandle::try_from_parts(
        fixture.bucket.kind(),
        fixture.bucket.account_namespace_id().clone(),
        fixture.bucket.region().into(),
        fixture.bucket.public_name().clone(),
        fixture.bucket.storage_bucket_id().clone(),
        BucketIncarnation::try_new(Uuid::new_v4()).unwrap(),
    )
    .unwrap();
    let mut intent: VectorIndexCreateIntentV1 = serde_json::from_slice(&saved.data).unwrap();
    intent.bucket = VectorBucketBindingV1::from_handle(&other).unwrap();
    intent.expected_parent.bucket = intent.bucket.clone();
    intent.request_sha256 = intent.canonical_request_digest().unwrap().to_hex();
    intent
        .validate()
        .expect("record is internally valid for the other incarnation");
    fixture
        .backend
        .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&intent).unwrap())
        .await;
    let before_objects = fixture.backend.state.lock().await.objects.clone();
    let before_puts = fixture.backend.state.lock().await.put_attempts.clone();
    assert_matches!(
        fixture.store.list_vector_index_creation_reservations(&fixture.bucket).await,
        Err(TableCatalogStoreError::Conflict(message)) if message.contains("another bucket incarnation")
    );
    assert_eq!(fixture.backend.state.lock().await.objects, before_objects);
    assert_eq!(fixture.backend.state.lock().await.put_attempts, before_puts);
    guard.check_held().unwrap();
}
