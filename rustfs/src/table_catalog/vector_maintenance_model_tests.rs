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

const JOB: &str = r####"{
  "version": 1,
  "job_id": "c04706bf-885b-802e-99b7-7f1dd94a7311",
  "resource": {
    "bucket": {
      "version": 1,
      "storage_bucket_id": "vectors-storage",
      "bucket_incarnation": "11111111-1111-4111-8111-111111111111",
      "account_namespace_id": "",
      "region": "us-east-1",
      "public_name": "vectors-demo"
    },
    "namespace": "indexes",
    "name": "example-index",
    "table_id": "22222222-2222-4222-8222-222222222222"
  },
  "kind": "CREATE_RECOVERY",
  "origin": "RECOVERY",
  "request_id": "07fdec93-cf24-8e23-9fad-0fe72e987b7a",
  "input": {
    "base_head": null,
    "source": {
      "kind": "CREATE_INTENT",
      "reference": {
        "object_key": "s3vectors/catalog/table-buckets/85802340a2454c2db04e7ded648c30264e6cb28ec788fb9693b0783786b6c1ed/resources/b454f82c5857ebabf342b7258e5cf7def78b7cd975814119462973de9a38df10/maintenance/v1/sources/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json",
        "content_length": 128,
        "content_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "body_revision": "44444444-4444-4444-8444-444444444444",
        "etag": null
      }
    },
    "plan_ref": null
  },
  "execution_identity": {
    "account": "maintenance-example",
    "user_generation": "11111111111111111111111111111111"
  },
  "initiator_ref": null,
  "policy": {
    "lease_timeout_seconds": 900,
    "heartbeat_interval_seconds": 60,
    "max_attempts": 3,
    "retry_initial_backoff_seconds": 5,
    "retry_max_backoff_seconds": 300
  },
  "spec_sha256": "b8b2a3cb12a85dc7fff32c8ba25db5197f22681afe534d205636770026eb3907",
  "revision": "66666666-6666-4666-8666-666666666666",
  "last_update_id": "e030db8b-46e8-85ab-baa7-2bf5a6fc13e1",
  "last_update_sha256": "e030db8b46e8f5abfaa72bf5a6fc13e1d8e9a6d04f278556deed6cade4e3491a",
  "state": "QUEUED",
  "attempt": 0,
  "lease": null,
  "created_at": "2026-11-22T00:00:00.000000000Z",
  "updated_at": "2026-11-22T00:00:00.000000000Z",
  "next_retry_at": null,
  "finished_at": null,
  "failure": null,
  "result_ref": null
}"####;

#[test]
fn maintenance_record_requires_explicit_null_and_rejects_unknown_fields() {
    let original: serde_json::Value = serde_json::from_str(JOB).unwrap();
    for field in ["lease", "next_retry_at", "result_ref", "initiator_ref"] {
        let mut value = original.clone();
        value.as_object_mut().unwrap().remove(field);
        assert!(decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    let mut value = original;
    value["unknown"] = serde_json::json!(true);
    assert!(decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn maintenance_record_rejects_duplicate_nested_fields_and_arrays() {
    let duplicate = JOB.replace("\"version\": 1,", "\"version\": 1, \"version\": 1,");
    assert!(decode_vector_maintenance_record::<VectorMaintenanceJobV1>(duplicate.as_bytes()).is_err());
    assert!(decode_vector_maintenance_record::<VectorMaintenanceJobV1>(b"[]").is_err());
    let missing_etag = JOB
        .replace("        \"etag\": null\n", "")
        .replace("\"44444444-4444-4444-8444-444444444444\",", "\"44444444-4444-4444-8444-444444444444\"");
    assert!(decode_vector_maintenance_record::<VectorMaintenanceJobV1>(missing_etag.as_bytes()).is_err());
}

#[test]
fn maintenance_job_roundtrip_and_stable_ids_bind_identity_and_input() {
    let job = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(JOB.as_bytes()).unwrap();
    let encoded = encode_vector_maintenance_record(&job).unwrap();
    assert_eq!(decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&encoded).unwrap(), job);
    let spec = job.semantic_spec();
    assert_eq!(vector_maintenance_job_ids(&spec).unwrap(), (*job.job_id(), *job.request_id()));
    let mut changed = spec.clone();
    changed.execution_identity.user_generation[0] ^= 1;
    assert_ne!(vector_maintenance_job_ids(&changed).unwrap(), vector_maintenance_job_ids(&spec).unwrap());
    let reserve = vector_maintenance_stage_commit_id(*job.job_id(), 1, "reserve-fragments").unwrap();
    let rewrite = vector_maintenance_stage_commit_id(*job.job_id(), 1, "rewrite").unwrap();
    assert_ne!(reserve, rewrite);
    assert_ne!(
        reserve,
        vector_maintenance_stage_commit_id(*job.job_id(), 2, "reserve-fragments").unwrap()
    );
    assert!(vector_maintenance_stage_commit_id(*job.job_id(), 0, "rewrite").is_err());
    assert!(vector_maintenance_stage_commit_id(*job.job_id(), 1, "unknown").is_err());
}

#[test]
fn maintenance_job_rejects_state_and_policy_contradictions() {
    let original: serde_json::Value = serde_json::from_str(JOB).unwrap();
    for (field, value) in [
        ("attempt", serde_json::json!(1)),
        ("version", serde_json::json!(2)),
        ("state", serde_json::json!("RUNNING")),
    ] {
        let mut invalid = original.clone();
        invalid[field] = value;
        assert!(decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&serde_json::to_vec(&invalid).unwrap()).is_err());
    }
    let mut invalid = original;
    invalid["policy"]["heartbeat_interval_seconds"] = serde_json::json!(301);
    assert!(decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&serde_json::to_vec(&invalid).unwrap()).is_err());
}

#[test]
fn maintenance_record_checks_byte_depth_and_reference_limits() {
    let oversized = vec![b' '; VECTOR_MAINTENANCE_JOB_MAX_BYTES + 1];
    assert!(decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&oversized).is_err());
    let mut target: VectorRetirementTargetV1 = serde_json::from_str(r#"{"object_key":".rustfs-table/22222222-2222-4222-8222-222222222222/dataset/data/test.lance","body_revision":"77777777-7777-4777-8777-777777777777","mod_time":null,"content_length":1,"etag":null,"version_id":null,"delete_marker":false}"#).unwrap();
    assert!(target.validate().is_ok());
    target.delete_marker = true;
    assert!(target.validate().is_err());
    target.delete_marker = false;
    target.version_id = Some(Uuid::new_v4());
    assert!(target.validate().is_err());
}

#[test]
fn maintenance_descriptor_v2_preserves_opaque_payload_and_metadata_binding() {
    let metadata = CatalogMetadataRef::from_read(CatalogObjectKey::try_new(".rustfs-table/22222222-2222-4222-8222-222222222222/dataset/_versions/77777777-7777-4777-8777-777777777777/1.manifest".to_string()).unwrap(), &TableCatalogObject { data: vec![1], body_revision: Some(Uuid::new_v4()), etag: None, mod_time: None }).unwrap();
    let descriptor = CatalogFormatDescriptor::try_new(1, CatalogFormat::Lance, 2, metadata.clone(), vec![1, 2]).unwrap();
    assert_eq!(descriptor.metadata(), &metadata);
    assert_eq!(descriptor.payload(), &[1, 2]);
    assert!(CatalogFormatDescriptor::try_new(1, CatalogFormat::Lance, 3, metadata, vec![1]).is_err());
}

#[test]
fn maintenance_v2_mutation_requires_exact_upgrade_and_retirement_chain() {
    let definition = VectorIndexDefinitionV1 {
        version: 1,
        effective_encryption: VectorEffectiveEncryptionV1::Unencrypted,
        tags: BTreeMap::new(),
    };
    let mutation = VectorResourceMutationBindingV2 {
        version: 2,
        kind: VectorResourceMutationKind::Enroll,
        expected_definition: definition.clone(),
        next_definition: definition,
        expected_state: TableCatalogEntryState::Active,
        next_state: TableCatalogEntryState::Active,
        expected_maintenance: None,
        next_maintenance: VectorMaintenanceBindingV1::empty(),
    };
    assert!(mutation.validate().is_ok());
    let encoded = encode_vector_maintenance_record(&mutation).unwrap();
    assert_eq!(
        decode_vector_maintenance_record::<VectorResourceMutationBindingV2>(&encoded).unwrap(),
        mutation
    );
    let binding = VectorResourceMutationBinding::from(mutation.clone());
    assert_eq!(serde_json::to_vec(&binding).unwrap(), serde_json::to_vec(&mutation).unwrap());
    assert_eq!(maintenance_canonical_bytes(&binding).unwrap(), encoded);
    assert_eq!(serde_json::from_slice::<VectorResourceMutationBinding>(&encoded).unwrap(), binding);
    let mut invalid = mutation;
    invalid.kind = VectorResourceMutationKind::Retire;
    assert!(invalid.validate().is_err());
    let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    value.as_object_mut().unwrap().remove("expected_maintenance");
    assert!(decode_vector_maintenance_record::<VectorResourceMutationBindingV2>(&serde_json::to_vec(&value).unwrap()).is_err());
    assert!(serde_json::from_value::<VectorResourceMutationBinding>(value).is_err());
}

#[test]
fn maintenance_canonical_encoding_checks_depth_and_uuid_spelling() {
    let mut value = serde_json::json!({"leaf":true});
    for _ in 0..31 {
        value = serde_json::json!({"child":value});
    }
    assert!(maintenance_canonical_bytes(&value).is_ok());
    value = serde_json::json!({"child":value});
    assert!(maintenance_canonical_bytes(&value).is_err());
    for key in ["maintenance_revision", "last_maintenance_update_id", "job_id"] {
        let value = serde_json::json!({(key):"AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"});
        assert!(maintenance_canonical_bytes(&value).is_err());
    }
}

#[test]
fn maintenance_latest_update_digest_and_total_attempts_are_validated() {
    let valid = decode_vector_maintenance_record::<VectorMaintenanceJobV1>(JOB.as_bytes()).unwrap();
    let mut malformed = valid.clone();
    malformed.last_update_id = Uuid::new_v4();
    assert!(encode_vector_maintenance_record(&malformed).is_err());
    for attempts in [0, 11] {
        let mut malformed = valid.clone();
        malformed.policy.max_attempts = attempts;
        assert!(malformed.policy.validate().is_err());
    }
    let mut malformed = serde_json::to_value(valid).unwrap();
    malformed["execution_identity"]["user_generation"] = serde_json::json!(vec![1; 16]);
    assert!(decode_vector_maintenance_record::<VectorMaintenanceJobV1>(&serde_json::to_vec(&malformed).unwrap()).is_err());
}
