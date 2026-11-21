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

use super::*;
use crate::table_catalog::test_support::table_metadata_json;

fn observation(revision: Option<CatalogBodyRevision>, limit: Option<usize>) -> IcebergGraphObjectObservation {
    IcebergGraphObjectObservation::try_new(
        "analytics".into(),
        "tables/id/manifest.avro".into(),
        revision,
        4,
        IcebergGraphObjectIdentity::ContentSha256(CatalogContentSha256::digest(b"data")),
        limit,
    )
    .unwrap()
}

fn entry() -> TableEntry {
    TableEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: "analytics".into(),
        namespace: "default".into(),
        table: "events".into(),
        table_id: "id".into(),
        table_uuid: "948b1d91-fb9b-4cb3-ac19-500ba5d7a792".into(),
        format: "ICEBERG".into(),
        format_version: 2,
        warehouse_location: "s3://analytics/tables/id".into(),
        metadata_location: "tables/id/metadata/00001.metadata.json".into(),
        version_token: "v1".into(),
        generation: 1,
        state: TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
        vector_index_definition: None,
        vector_head_binding: None,
    }
}

fn proof(metadata: &serde_json::Value) -> ValidatedIcebergSnapshotGraph {
    ValidatedIcebergSnapshotGraph::from_validated_parts(
        &entry(),
        None,
        metadata,
        IcebergGraphReadSet::try_new(IcebergGraphObserverId::new(), vec![]).unwrap(),
    )
    .unwrap()
}

#[test]
fn observation_merge_preserves_revision_and_tightens_budget() {
    let revision = CatalogBodyRevision::try_new(Uuid::new_v4()).unwrap();
    let captured = observation(None, Some(16));
    let merged = captured.merge(&observation(Some(revision), Some(8))).unwrap();
    assert_eq!(merged.body_revision(), Some(revision));
    assert_eq!(merged.max_size(), Some(8));
    merged.validate_supports(&captured).unwrap();
    assert!(captured.validate_supports(&merged).is_err());
    assert!(
        merged
            .merge(&observation(Some(CatalogBodyRevision::try_new(Uuid::new_v4()).unwrap()), Some(8)))
            .is_err()
    );
    assert!(
        IcebergGraphObjectObservation::try_new(
            "analytics".into(),
            "key".into(),
            None,
            4,
            IcebergGraphObjectIdentity::ContentSha256(CatalogContentSha256::digest(b"data")),
            Some(3)
        )
        .is_err()
    );
}

#[test]
fn observation_rejects_replacement_and_duplicate_read_set() {
    let first = observation(None, None);
    let mut replaced = first.clone();
    replaced.identity = IcebergGraphObjectIdentity::ContentSha256(CatalogContentSha256::digest(b"edit"));
    assert!(first.merge(&replaced).is_err());
    assert!(first.validate_supports(&replaced).is_err());
    let first = Arc::new(first);
    assert!(IcebergGraphReadSet::try_new(IcebergGraphObserverId::new(), vec![first.clone(), first]).is_err());
    let observer = IcebergGraphObserverId::new();
    assert!(observer.same_observer(&observer.clone()));
    assert!(!observer.same_observer(&IcebergGraphObserverId::new()));
}

#[test]
fn matching_commit_rebind_accepts_only_reconstructed_timestamps() {
    let entry = entry();
    let original = table_metadata_json(&entry.table_uuid, &entry.warehouse_location);
    let mut retry = original.clone();
    retry["last-updated-ms"] = serde_json::json!(2);
    let rebound = proof(&original).rebind_after_matching_commit(&retry).unwrap();
    assert_eq!(rebound.candidate_metadata_sha256, metadata_sha256(&retry).unwrap());
    for field in ["location", "properties", "metadata-log"] {
        let mut changed = retry.clone();
        changed[field] = match field {
            "location" => serde_json::json!("s3://analytics/tables/other"),
            "properties" => serde_json::json!({"changed": "true"}),
            _ => serde_json::json!([{"timestamp-ms":1,"metadata-file":"s3://analytics/tables/id/metadata/other.json"}]),
        };
        assert!(proof(&original).rebind_after_matching_commit(&changed).is_err(), "{field}");
    }
}

#[test]
fn retry_digest_preserves_snapshot_definition_and_physical_inputs() {
    let original = serde_json::json!({"last-updated-ms":1,"snapshot-log":[{"timestamp-ms":1,"snapshot-id":7}],
        "snapshots":[{"snapshot-id":7,"timestamp-ms":1,"manifest-list":"a.avro"}],"statistics":[]});
    let mut retry = original.clone();
    retry["last-updated-ms"] = serde_json::json!(2);
    retry["snapshot-log"][0]["timestamp-ms"] = serde_json::json!(2);
    assert_eq!(
        retry_equivalent_metadata_sha256(&original).unwrap(),
        retry_equivalent_metadata_sha256(&retry).unwrap()
    );
    for field in ["timestamp-ms", "manifest-list"] {
        let mut changed = retry.clone();
        changed["snapshots"][0][field] = serde_json::json!("changed");
        assert_ne!(
            retry_equivalent_metadata_sha256(&original).unwrap(),
            retry_equivalent_metadata_sha256(&changed).unwrap()
        );
    }
    retry["statistics"] = serde_json::json!([{"statistics-path":"new.puffin"}]);
    assert_ne!(
        retry_equivalent_metadata_sha256(&original).unwrap(),
        retry_equivalent_metadata_sha256(&retry).unwrap()
    );
}

#[test]
fn canonical_proof_and_raw_reference_bind_different_properties() {
    let entry = entry();
    let metadata = table_metadata_json(&entry.table_uuid, &entry.warehouse_location);
    let compact = serde_json::to_vec(&metadata).unwrap();
    let pretty = serde_json::to_vec_pretty(&metadata).unwrap();
    assert_ne!(CatalogContentSha256::digest(&compact), CatalogContentSha256::digest(&pretty));
    let object = TableCatalogObject {
        data: compact.clone(),
        body_revision: Some(Uuid::new_v4()),
        etag: Some("etag".into()),
        mod_time: None,
    };
    let reference = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(entry.metadata_location).unwrap(), &object).unwrap();
    let changed = TableCatalogObject {
        data: pretty.clone(),
        ..object
    };
    assert!(reference.verify_read(&changed).is_err());
    assert_eq!(
        metadata_sha256(&serde_json::from_slice(&compact).unwrap()).unwrap(),
        metadata_sha256(&serde_json::from_slice(&pretty).unwrap()).unwrap()
    );
}

struct Publication {
    observer: IcebergGraphObserverId,
    prepared: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl TableCommitPublication for Publication {
    async fn begin_table_bucket(&self, _bucket: &str) -> TableCatalogStoreResult<()> {
        Ok(())
    }
    async fn prepare(&self, _bucket: &str, _namespace: &str, _table: &str) -> TableCatalogStoreResult<()> {
        self.prepared.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn holds_table_bucket(&self, _bucket: &str) -> bool {
        self.prepared.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn holds_table(&self, _bucket: &str, _namespace: &str, _table: &str) -> bool {
        self.holds_table_bucket("")
    }
    fn complete(&self) {
        self.prepared.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}
impl IcebergGraphPublication for Publication {
    fn capture_graph_read_set(&self) -> TableCatalogStoreResult<IcebergGraphReadSet> {
        IcebergGraphReadSet::try_new(self.observer.clone(), vec![])
    }
    fn validate_prepared_graph_read_set(&self, read_set: &IcebergGraphReadSet) -> TableCatalogStoreResult<()> {
        if !self.holds_table_bucket("") || !self.observer.same_observer(read_set.observer()) {
            return Err(TableCatalogStoreError::Conflict("publication evidence is unavailable".into()));
        }
        Ok(())
    }
}

#[test]
fn full_graph_accepts_actual_base_and_changes_requires_exact_base() {
    let entry = entry();
    let base = table_metadata_json(&entry.table_uuid, &entry.warehouse_location);
    let mut candidate = base.clone();
    candidate["last-updated-ms"] = serde_json::json!(2);
    let publication = Publication {
        observer: IcebergGraphObserverId::new(),
        prepared: true.into(),
    };
    let full = ValidatedIcebergSnapshotGraph::from_validated_parts(
        &entry,
        None,
        &candidate,
        publication.capture_graph_read_set().unwrap(),
    )
    .unwrap();
    full.validate_for_commit(&entry, Some(&base), &candidate, &publication)
        .unwrap();
    let changes = ValidatedIcebergSnapshotGraph::from_validated_parts(
        &entry,
        Some(&base),
        &candidate,
        publication.capture_graph_read_set().unwrap(),
    )
    .unwrap();
    changes
        .validate_for_commit(&entry, Some(&base), &candidate, &publication)
        .unwrap();
    assert!(changes.validate_for_commit(&entry, None, &candidate, &publication).is_err());
    assert!(
        changes
            .validate_for_commit(&entry, Some(&candidate), &candidate, &publication)
            .is_err()
    );
    assert!(full.validate_for_commit(&entry, Some(&base), &base, &publication).is_err());
    let mut other = entry.clone();
    other.table_id = "another-table".into();
    assert!(
        full.validate_for_commit(&other, Some(&base), &candidate, &publication)
            .is_err()
    );
    let other_publication = Publication {
        observer: IcebergGraphObserverId::new(),
        prepared: true.into(),
    };
    assert!(
        full.validate_for_commit(&entry, Some(&base), &candidate, &other_publication)
            .is_err()
    );
    publication.complete();
    assert!(
        full.validate_for_commit(&entry, Some(&base), &candidate, &publication)
            .is_err()
    );
}

#[test]
fn matching_commit_rebind_requires_final_full_digest() {
    let entry = entry();
    let original = table_metadata_json(&entry.table_uuid, &entry.warehouse_location);
    let mut final_metadata = original.clone();
    final_metadata["last-updated-ms"] = serde_json::json!(3);
    let publication = Publication {
        observer: IcebergGraphObserverId::new(),
        prepared: true.into(),
    };
    let proof = ValidatedIcebergSnapshotGraph::from_validated_parts(
        &entry,
        None,
        &original,
        publication.capture_graph_read_set().unwrap(),
    )
    .unwrap()
    .rebind_after_matching_commit(&final_metadata)
    .unwrap();
    proof
        .validate_for_commit(&entry, None, &final_metadata, &publication)
        .unwrap();
    assert!(proof.validate_for_commit(&entry, None, &original, &publication).is_err());
}
