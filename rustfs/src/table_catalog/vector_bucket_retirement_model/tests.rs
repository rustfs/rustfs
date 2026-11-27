// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use serde_json::{Value, json};

fn golden() -> Value {
    serde_json::from_str(include_str!("encoding-golden.json")).unwrap()
}

fn plan() -> VectorBucketCatalogRetirementPlanPageV1 {
    let case = &golden()["cases"][0];
    VectorBucketCatalogRetirementPlanPageV1 {
        version: 1,
        bucket: serde_json::from_value(case["bucket"].clone()).unwrap(),
        retirement_id: Uuid::parse_str(case["retirement_id"].as_str().unwrap()).unwrap(),
        page_id: Uuid::from_u128(9),
        next_page: None,
        resources: Vec::new(),
        targets: serde_json::from_value(case["targets"].clone()).unwrap(),
    }
}

#[test]
fn catalog_retirement_rejects_unknown_version_fields_and_duplicate_fields() {
    let page = plan();
    let bytes = encode_vector_bucket_retirement_record(&page).unwrap();
    let decoded: VectorBucketCatalogRetirementPlanPageV1 = decode_vector_maintenance_record(&bytes).unwrap();
    assert_eq!(decoded, page);
    let mut wrong = serde_json::to_value(&page).unwrap();
    wrong["version"] = json!(2);
    assert!(
        decode_vector_maintenance_record::<VectorBucketCatalogRetirementPlanPageV1>(&serde_json::to_vec(&wrong).unwrap())
            .is_err()
    );
    wrong = serde_json::to_value(&page).unwrap();
    wrong["unknown"] = json!(true);
    assert!(
        decode_vector_maintenance_record::<VectorBucketCatalogRetirementPlanPageV1>(&serde_json::to_vec(&wrong).unwrap())
            .is_err()
    );
    let text = String::from_utf8(bytes).unwrap().replacen("{", "{\"version\":1,", 1);
    assert!(decode_vector_maintenance_record::<VectorBucketCatalogRetirementPlanPageV1>(text.as_bytes()).is_err());
}

#[test]
fn catalog_retirement_required_option_fields_are_not_defaults() {
    let mut value = serde_json::to_value(plan()).unwrap();
    value.as_object_mut().unwrap().remove("next_page");
    assert!(
        decode_vector_maintenance_record::<VectorBucketCatalogRetirementPlanPageV1>(&serde_json::to_vec(&value).unwrap())
            .is_err()
    );
    let mut value = serde_json::to_value(plan()).unwrap();
    value["targets"][0].as_object_mut().unwrap().remove("resource");
    assert!(
        decode_vector_maintenance_record::<VectorBucketCatalogRetirementPlanPageV1>(&serde_json::to_vec(&value).unwrap())
            .is_err()
    );
}

#[test]
fn catalog_retirement_rejects_foreign_resource_and_duplicate_target() {
    let mut page = plan();
    page.targets.push(page.targets[0].clone());
    assert!(page.validate().is_err());
    let case = &golden()["cases"][1];
    let mut target: VectorCatalogRetirementTargetV1 = serde_json::from_value(case["targets"][2].clone()).unwrap();
    target.resource.as_mut().unwrap().bucket.bucket_incarnation = Uuid::from_u128(8);
    let mut page = plan();
    page.targets.push(target);
    assert!(page.validate().is_err());
}

#[test]
fn catalog_retirement_rejects_noncanonical_or_nil_new_ids() {
    for value in [
        "00000000-0000-0000-0000-000000000000",
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        "9",
    ] {
        let mut page = serde_json::to_value(plan()).unwrap();
        page["page_id"] = json!(value);
        assert!(
            decode_vector_maintenance_record::<VectorBucketCatalogRetirementPlanPageV1>(&serde_json::to_vec(&page).unwrap())
                .is_err()
        );
    }
}

#[test]
fn catalog_retirement_digest_matches_independent_golden_bytes() {
    for case in golden()["cases"].as_array().unwrap() {
        let bucket: VectorBucketBindingV1 = serde_json::from_value(case["bucket"].clone()).unwrap();
        let retirement_id = Uuid::parse_str(case["retirement_id"].as_str().unwrap()).unwrap();
        let mut targets: Vec<VectorCatalogRetirementTargetV1> = serde_json::from_value(case["targets"].clone()).unwrap();
        targets.sort_by(|a, b| a.object_key.cmp(&b.object_key));
        let mut inventory = CatalogRetirementInventoryDigest::new(&bucket, retirement_id, targets.len() as u64).unwrap();
        for target in &targets {
            inventory.push(target).unwrap();
        }
        let inventory = inventory.finish().unwrap();
        assert_eq!(inventory, case["inventory_sha256"].as_str().unwrap());
        let groups = case["resources"].as_array().unwrap();
        let mut closure = CatalogRetirementClosureDigest::new(&bucket, retirement_id, &inventory, groups.len() as u64).unwrap();
        for group in groups {
            closure
                .push(
                    &serde_json::from_value(group["resource"].clone()).unwrap(),
                    Uuid::parse_str(group["proof_id"].as_str().unwrap()).unwrap(),
                    group["fact_count"].as_u64().unwrap(),
                    group["proof_sha256"].as_str().unwrap(),
                )
                .unwrap();
        }
        assert_eq!(closure.finish().unwrap(), case["closure_sha256"].as_str().unwrap());
    }
}

#[test]
fn catalog_retirement_digest_rejects_repeated_or_missing_targets() {
    let page = plan();
    let mut digest = CatalogRetirementInventoryDigest::new(&page.bucket, page.retirement_id, 2).unwrap();
    digest.push(&page.targets[0]).unwrap();
    assert!(digest.push(&page.targets[0]).is_err());
    let digest = CatalogRetirementInventoryDigest::new(&page.bucket, page.retirement_id, 1).unwrap();
    assert!(digest.finish().is_err());
}

#[test]
fn catalog_retirement_pending_and_unknown_cannot_claim_acknowledgment() {
    let pending = VectorCatalogRetirementTargetProgressV1 {
        ordinal: 0,
        state: VectorCatalogRetirementTargetState::Pending,
        dispatch_id: None,
        worker_instance_id: None,
        worker_boot_id: None,
        started_at: None,
        acknowledged_at: None,
        failure: None,
    };
    pending.validate().unwrap();
    let mut false_ack = pending.clone();
    false_ack.state = VectorCatalogRetirementTargetState::DeleteAcknowledged;
    assert!(false_ack.validate().is_err());
    let mut unknown = pending;
    unknown.state = VectorCatalogRetirementTargetState::Unknown;
    unknown.dispatch_id = Some(Uuid::from_u128(1));
    unknown.worker_instance_id = Some(Uuid::from_u128(2));
    unknown.worker_boot_id = Some(Uuid::from_u128(3));
    unknown.started_at = Some("2026-11-26T00:00:00.000000000Z".into());
    unknown.failure = Some(VectorMaintenanceFailureCode::UnknownOutcome);
    unknown.validate().unwrap();
    unknown.acknowledged_at = Some("2026-11-26T00:00:01.000000000Z".into());
    assert!(unknown.validate().is_err());
}

#[test]
fn catalog_retirement_target_decoder_preserves_reference_strictness() {
    let value = serde_json::to_value(&plan().targets[0]).unwrap();
    let bytes = serde_json::to_vec(&value).unwrap();
    decode_vector_maintenance_record::<VectorCatalogRetirementTargetV1>(&bytes).unwrap();
    let reference = json!({"kind": "COMMIT_LOG", "reference": value});
    assert!(decode_vector_maintenance_record::<VectorMaintenanceSourceV1>(&serde_json::to_vec(&reference).unwrap()).is_err());
    for kind in [json!("bucket_entry"), json!("UNKNOWN"), json!(1)] {
        let mut wrong = value.clone();
        wrong["kind"] = kind;
        assert!(
            decode_vector_maintenance_record::<VectorCatalogRetirementTargetV1>(&serde_json::to_vec(&wrong).unwrap()).is_err()
        );
    }
    for missing in [
        "kind",
        "resource",
        "object_key",
        "etag",
        "body_revision",
        "content_sha256",
        "content_length",
    ] {
        let mut wrong = value.clone();
        wrong.as_object_mut().unwrap().remove(missing);
        assert!(
            decode_vector_maintenance_record::<VectorCatalogRetirementTargetV1>(&serde_json::to_vec(&wrong).unwrap()).is_err()
        );
    }
    let mut extra = value;
    extra["extra"] = json!(true);
    assert!(decode_vector_maintenance_record::<VectorCatalogRetirementTargetV1>(&serde_json::to_vec(&extra).unwrap()).is_err());
    let duplicate = String::from_utf8(bytes)
        .unwrap()
        .replacen("{", "{\"kind\":\"BUCKET_ENTRY\",", 1);
    assert!(decode_vector_maintenance_record::<VectorCatalogRetirementTargetV1>(duplicate.as_bytes()).is_err());
}

fn proof_fixture() -> (VectorMaintenanceResourceV1, Uuid, Uuid, Vec<VectorCatalogRetirementProofFactV1>) {
    let owner: VectorMaintenanceResourceV1 =
        serde_json::from_value(golden()["cases"][1]["resources"][0]["resource"].clone()).unwrap();
    let identity = owner.identity().unwrap();
    let metadata = CatalogMetadataRef::from_read(
        CatalogObjectKey::try_new(format!("{}versions/1.manifest", identity.dataset_prefix())).unwrap(),
        &TableCatalogObject {
            data: vec![1],
            body_revision: Some(Uuid::from_u128(10)),
            etag: Some("manifest-etag".into()),
            mod_time: None,
        },
    )
    .unwrap();
    let descriptor = CatalogFormatDescriptor::try_new(1, CatalogFormat::Lance, 2, metadata.clone(), vec![1, 2]).unwrap();
    let head = VectorMaintenanceHeadV1 {
        entry_version: 4,
        state: TableCatalogEntryState::Deleted,
        version_token: "deleted-version".into(),
        generation: 1,
        binding: VectorHeadBindingV1::try_new(&identity, &metadata, &descriptor).unwrap(),
        maintenance: Some(VectorMaintenanceBindingV1::empty()),
    };
    let reference: CatalogMetadataRefRecordV1 = (&CatalogMetadataRef::from_read(
        CatalogObjectKey::try_new(format!(
            "s3vectors/catalog/table-buckets/{}/resources/{}/delete-intent.json",
            table_catalog_path_hash(&owner.bucket.storage_bucket_id),
            table_catalog_path_hash(&owner.table_id),
        ))
        .unwrap(),
        &TableCatalogObject {
            data: b"{}".to_vec(),
            body_revision: Some(Uuid::from_u128(11)),
            etag: Some("intent-etag".into()),
            mod_time: None,
        },
    )
    .unwrap())
        .into();
    let facts = vec![
        VectorCatalogRetirementProofFactV1::ResourceAnchor {
            delete_intent: reference.clone(),
            deleted_head: Box::new(head),
        },
        VectorCatalogRetirementProofFactV1::SourceRecord { reference },
    ];
    (owner, Uuid::from_u128(12), Uuid::from_u128(13), facts)
}

#[test]
fn catalog_retirement_proof_digest_requires_anchor_exact_count_and_cross_batch_order() {
    let (owner, retirement_id, proof_id, facts) = proof_fixture();
    let new = |count| CatalogRetirementProofDigest::new(&owner.bucket, &owner, retirement_id, proof_id, count).unwrap();
    let mut complete = new(2);
    complete.push(&facts[0]).unwrap();
    complete.push(&facts[1]).unwrap();
    assert_eq!(complete.finish().unwrap().len(), 64);
    let mut no_anchor = new(1);
    no_anchor.push(&facts[1]).unwrap();
    assert!(no_anchor.finish().is_err());
    let mut short = new(2);
    short.push(&facts[0]).unwrap();
    assert!(short.finish().is_err());
    let mut excess = new(1);
    excess.push(&facts[0]).unwrap();
    assert!(excess.push(&facts[1]).is_err());
    let mut repeated = new(3);
    for fact in &facts {
        repeated.push(fact).unwrap();
    }
    assert!(repeated.push(&facts[1]).is_err());
    let mut reversed = new(2);
    reversed.push(&facts[1]).unwrap();
    assert!(reversed.push(&facts[0]).is_err());
    let mut foreign = facts[0].clone();
    if let VectorCatalogRetirementProofFactV1::ResourceAnchor { deleted_head, .. } = &mut foreign {
        deleted_head.binding.bucket.bucket_incarnation = Uuid::from_u128(14);
    }
    assert!(new(1).push(&foreign).is_err());
}

#[test]
fn catalog_retirement_proof_page_checks_ranges_finality_order_and_identity() {
    let (owner, retirement_id, proof_id, facts) = proof_fixture();
    let mut digest = CatalogRetirementProofDigest::new(&owner.bucket, &owner, retirement_id, proof_id, 2).unwrap();
    for fact in &facts {
        digest.push(fact).unwrap();
    }
    let page = VectorCatalogRetiredResourceProofPageV1 {
        version: 1,
        bucket: owner.bucket.clone(),
        retirement_id,
        resource: owner,
        proof_id,
        page_id: Uuid::from_u128(15),
        page_ordinal: 0,
        page_count: 1,
        first_fact_ordinal: 0,
        total_fact_count: 2,
        proof_sha256: digest.finish().unwrap(),
        next_page: None,
        facts,
    };
    page.validate().unwrap();
    let encoded = encode_vector_bucket_retirement_record(&page).unwrap();
    assert_eq!(
        decode_vector_maintenance_record::<VectorCatalogRetiredResourceProofPageV1>(&encoded).unwrap(),
        page
    );
    for (field, value) in [
        ("page_count", json!(0)),
        ("page_ordinal", json!(1)),
        ("first_fact_ordinal", json!(1)),
        ("first_fact_ordinal", json!(u64::MAX)),
        ("total_fact_count", json!(1)),
        ("total_fact_count", json!(3)),
    ] {
        let mut wrong = serde_json::to_value(&page).unwrap();
        wrong[field] = value;
        assert!(
            decode_vector_maintenance_record::<VectorCatalogRetiredResourceProofPageV1>(&serde_json::to_vec(&wrong).unwrap())
                .is_err()
        );
    }
    let next = match &page.facts[1] {
        VectorCatalogRetirementProofFactV1::SourceRecord { reference } => reference.clone(),
        _ => unreachable!(),
    };
    let mut wrong = page.clone();
    wrong.next_page = Some(next.clone());
    assert!(wrong.validate().is_err());
    let mut first = page.clone();
    first.page_count = 2;
    first.total_fact_count = 3;
    first.next_page = Some(next);
    first.validate().unwrap();
    first.next_page = None;
    assert!(first.validate().is_err());
    let mut last = page.clone();
    last.page_count = 2;
    last.page_ordinal = 1;
    last.first_fact_ordinal = 2;
    last.total_fact_count = 4;
    last.validate().unwrap();
    last.first_fact_ordinal = 0;
    assert!(last.validate().is_err());
    for facts in [
        vec![page.facts[0].clone(), page.facts[0].clone()],
        vec![page.facts[1].clone(), page.facts[0].clone()],
    ] {
        let mut wrong = page.clone();
        wrong.facts = facts;
        assert!(wrong.validate().is_err());
    }
    let mut wrong = page;
    if let VectorCatalogRetirementProofFactV1::ResourceAnchor { deleted_head, .. } = &mut wrong.facts[0] {
        deleted_head.binding.bucket.bucket_incarnation = Uuid::from_u128(16);
    }
    assert!(wrong.validate().is_err());
}

#[test]
fn catalog_retirement_history_edges_preserve_original_optional_etags() {
    let (_, _, _, facts) = proof_fixture();
    let mut reference = match &facts[1] {
        VectorCatalogRetirementProofFactV1::SourceRecord { reference } => reference.clone(),
        _ => unreachable!(),
    };
    reference.etag = None;
    let history = VectorCatalogRetirementProofFactV1::CommitHistoryReference {
        commit_id: "committed-history".into(),
        ordinal: 0,
        reference: reference.clone(),
    };
    history.validate().unwrap();
    let source = VectorCatalogRetirementProofFactV1::JobSourceReference {
        job_id: Uuid::from_u128(17),
        revision: Uuid::from_u128(18),
        ordinal: 0,
        reference: reference.clone(),
    };
    source.validate().unwrap();
    assert!(
        VectorCatalogRetirementProofFactV1::SourceRecord { reference }
            .validate()
            .is_err()
    );
    assert_eq!(history.compare_order(&source), std::cmp::Ordering::Less);
    assert_eq!(history.compare_order(&history), std::cmp::Ordering::Equal);
}

#[test]
fn catalog_retirement_job_edges_preserve_maintenance_reference_validation() {
    let (_, _, _, facts) = proof_fixture();
    let reference = match &facts[1] {
        VectorCatalogRetirementProofFactV1::SourceRecord { reference } => reference.clone(),
        _ => unreachable!(),
    };
    let fact = VectorCatalogRetirementProofFactV1::JobSourceReference {
        job_id: Uuid::from_u128(17),
        revision: Uuid::from_u128(18),
        ordinal: 0,
        reference,
    };
    let mut valid = serde_json::to_value(&fact).unwrap();
    valid["reference"]["etag"] = serde_json::Value::Null;
    decode_vector_maintenance_record::<VectorCatalogRetirementProofFactV1>(&serde_json::to_vec(&valid).unwrap()).unwrap();
    for (field, value) in [
        ("content_length", serde_json::json!(0)),
        ("body_revision", serde_json::Value::Null),
        ("body_revision", serde_json::json!(Uuid::nil())),
        ("etag", serde_json::json!("")),
        ("etag", serde_json::json!("\n")),
        ("etag", serde_json::json!("a".repeat(4097))),
    ] {
        let mut invalid = valid.clone();
        invalid["reference"][field] = value;
        assert!(
            decode_vector_maintenance_record::<VectorCatalogRetirementProofFactV1>(&serde_json::to_vec(&invalid).unwrap())
                .is_err(),
            "invalid source field: {field}"
        );
    }
}
