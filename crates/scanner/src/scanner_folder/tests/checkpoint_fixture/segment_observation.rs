//! Fixture-only range diagnostics. No result is supplied to a scan selector.

use super::*;
use crate::segment_invalidation::{
    MAX_SEGMENT_INVALIDATION_BYTES, MAX_SEGMENT_INVALIDATION_ENTRIES, SegmentInvalidationDomain, SegmentInvalidationEnvelope,
    SegmentInvalidationError, SegmentInvalidationProducer, SegmentInvalidationProof, admit_segment_invalidation,
};
use std::collections::BTreeSet;

const MAX_WALK_SAMPLES: usize = 32;
const MAX_WALK_BYTES: usize = 1024;

fn segment_producers() -> BTreeSet<SegmentInvalidationProducer> {
    SegmentInvalidationProducer::REQUIRED.into_iter().collect()
}

fn segment_envelope() -> SegmentInvalidationEnvelope {
    SegmentInvalidationEnvelope {
        source: DataUsageCacheSource::new(2, 3),
        bucket_incarnation: uuid::Uuid::from_u128(0x12345678123456781234567812345678),
        key_format: crate::DATA_USAGE_CACHE_KEY_FORMAT,
        baseline_scan_plan_digest: DataUsageScanPlanDigest([9; 32]),
        process_epoch: "epoch-a".to_string(),
        generation_start: 11,
        generation_end: 13,
        restart_gap: false,
        overflow: false,
        producers: segment_producers(),
    }
}

fn segment_proof() -> SegmentInvalidationProof {
    let envelope = segment_envelope();
    SegmentInvalidationProof {
        source: envelope.source,
        bucket_incarnation: envelope.bucket_incarnation,
        key_format: envelope.key_format,
        baseline_scan_plan_digest: envelope.baseline_scan_plan_digest,
        process_epoch: envelope.process_epoch,
        generation_start: envelope.generation_start,
        generation_end: envelope.generation_end,
        durable_producer_identity: true,
        invalidation_domain: SegmentInvalidationDomain::LocalSingleSet,
        distributed_ec_invalidation: false,
        cold_zero_walk_oracle: true,
    }
}

#[test]
fn segment_observation_fixture_proposal_bounds() {
    let envelope = segment_envelope();
    let proof = segment_proof();
    assert_eq!(
        admit_segment_invalidation(&envelope, &proof, ["hot/one", "hot/two"]),
        Ok(BTreeSet::from(["hot".to_string()]))
    );
    assert_eq!(
        admit_segment_invalidation(&envelope, &proof, ["a", "b", "c", "d"])
            .expect("entry boundary")
            .len(),
        MAX_SEGMENT_INVALIDATION_ENTRIES
    );
    assert_eq!(
        admit_segment_invalidation(&envelope, &proof, ["a", "b", "c", "d", "e"]),
        Err(SegmentInvalidationError::EntryLimit)
    );
    let exact = "x".repeat(MAX_SEGMENT_INVALIDATION_BYTES);
    assert!(admit_segment_invalidation(&envelope, &proof, [&exact]).is_ok());
    assert_eq!(
        admit_segment_invalidation(&envelope, &proof, [&exact, "y"]),
        Err(SegmentInvalidationError::ByteLimit)
    );
    let oversized = "x".repeat(MAX_SEGMENT_INVALIDATION_BYTES + 1);
    assert_eq!(
        admit_segment_invalidation(&envelope, &proof, [&oversized]),
        Err(SegmentInvalidationError::ByteLimit)
    );
    for key in ["", "/hot", "hot/../cold", "hot//one", "hot\\one", "hot/\0"] {
        assert_eq!(
            admit_segment_invalidation(&envelope, &proof, [key]),
            Err(SegmentInvalidationError::InvalidKey)
        );
    }
}

#[test]
fn segment_observation_trusted_proposal_requires_identity_and_complete_producer_coverage() {
    let envelope = segment_envelope();
    let proof = segment_proof();

    assert_eq!(
        admit_segment_invalidation(&envelope, &proof, ["hot/one", "hot/two", "archive/delete-marker"]),
        Ok(BTreeSet::from(["archive".to_string(), "hot".to_string()]))
    );

    let mut wrong_source = envelope.clone();
    wrong_source.source = DataUsageCacheSource::new(2, 4);
    assert_eq!(
        admit_segment_invalidation(&wrong_source, &proof, ["hot/one"]),
        Err(SegmentInvalidationError::InvalidProof)
    );

    let mut missing_incarnation = envelope.clone();
    missing_incarnation.bucket_incarnation = uuid::Uuid::nil();
    assert_eq!(
        admit_segment_invalidation(&missing_incarnation, &proof, ["hot/one"]),
        Err(SegmentInvalidationError::InvalidProof)
    );

    let mut wrong_key_format = envelope.clone();
    wrong_key_format.key_format = crate::DATA_USAGE_CACHE_KEY_FORMAT.saturating_add(1);
    assert_eq!(
        admit_segment_invalidation(&wrong_key_format, &proof, ["hot/one"]),
        Err(SegmentInvalidationError::InvalidProof)
    );

    let mut wrong_baseline = envelope.clone();
    wrong_baseline.baseline_scan_plan_digest = DataUsageScanPlanDigest([8; 32]);
    assert_eq!(
        admit_segment_invalidation(&wrong_baseline, &proof, ["hot/one"]),
        Err(SegmentInvalidationError::InvalidProof)
    );

    let mut wrong_epoch = envelope.clone();
    wrong_epoch.process_epoch = "epoch-b".to_string();
    assert_eq!(
        admit_segment_invalidation(&wrong_epoch, &proof, ["hot/one"]),
        Err(SegmentInvalidationError::InvalidProof)
    );

    let mut wrong_generation_start = proof.clone();
    wrong_generation_start.generation_start = wrong_generation_start.generation_start.saturating_sub(1);
    assert_eq!(
        admit_segment_invalidation(&envelope, &wrong_generation_start, ["hot/one"]),
        Err(SegmentInvalidationError::InvalidProof)
    );

    let mut wrong_generation_end = proof.clone();
    wrong_generation_end.generation_end = wrong_generation_end.generation_end.saturating_add(1);
    assert_eq!(
        admit_segment_invalidation(&envelope, &wrong_generation_end, ["hot/one"]),
        Err(SegmentInvalidationError::InvalidProof)
    );

    let mut no_durable_identity = proof.clone();
    no_durable_identity.durable_producer_identity = false;
    assert_eq!(
        admit_segment_invalidation(&envelope, &no_durable_identity, ["hot/one"]),
        Err(SegmentInvalidationError::InvalidProof)
    );

    let mut restart_gap = envelope.clone();
    restart_gap.restart_gap = true;
    assert_eq!(
        admit_segment_invalidation(&restart_gap, &proof, ["hot/one"]),
        Err(SegmentInvalidationError::InvalidProof)
    );

    let mut overflow = envelope.clone();
    overflow.overflow = true;
    assert_eq!(
        admit_segment_invalidation(&overflow, &proof, ["hot/one"]),
        Err(SegmentInvalidationError::InvalidProof)
    );

    let mut generation_gap = envelope.clone();
    generation_gap.generation_end = generation_gap.generation_start - 1;
    assert_eq!(
        admit_segment_invalidation(&generation_gap, &proof, ["hot/one"]),
        Err(SegmentInvalidationError::InvalidProof)
    );

    let mut missing_producer = envelope.clone();
    missing_producer.producers.remove(&SegmentInvalidationProducer::Replication);
    assert_eq!(
        admit_segment_invalidation(&missing_producer, &proof, ["hot/one"]),
        Err(SegmentInvalidationError::InvalidProof)
    );

    let mut missing_zero_walk_oracle = proof.clone();
    missing_zero_walk_oracle.cold_zero_walk_oracle = false;
    assert_eq!(
        admit_segment_invalidation(&envelope, &missing_zero_walk_oracle, ["hot/one"]),
        Err(SegmentInvalidationError::InvalidProof)
    );

    let mut distributed_without_invalidation = proof;
    distributed_without_invalidation.invalidation_domain = SegmentInvalidationDomain::DistributedEc;
    assert_eq!(
        admit_segment_invalidation(&envelope, &distributed_without_invalidation, ["hot/one"]),
        Err(SegmentInvalidationError::InvalidProof)
    );

    let mut distributed_with_invalidation = distributed_without_invalidation;
    distributed_with_invalidation.distributed_ec_invalidation = true;
    assert_eq!(
        admit_segment_invalidation(&envelope, &distributed_with_invalidation, ["hot/one", "hot/two", "archive/delete-marker"]),
        Ok(BTreeSet::from(["archive".to_string(), "hot".to_string()]))
    );
}

fn cache_value(cache: &DataUsageCache) -> serde_json::Value {
    let mut value = serde_json::to_value(cache).expect("serialize the entire cache");
    // Children are a HashSet: canonicalize only that unordered field, without
    // discarding any cache fields or changing ordered histogram arrays.
    for (path, entry) in &cache.cache {
        value["cache"][path]["children"] =
            serde_json::to_value(entry.children.iter().collect::<BTreeSet<_>>()).expect("canonical child set");
    }
    value
}

async fn walk_and_save(observe: bool) -> (Vec<String>, serde_json::Value) {
    let (mut scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for prefix in ["hot", "cold", "other"] {
        for leaf in ["one", "two"] {
            let object = format!("{prefix}/{leaf}");
            let mut metadata = FileMeta::new();
            let mut info = FileInfo::new(&object, 4, 2);
            info.volume = "bucket".to_string();
            info.name = object.clone();
            info.size = 1;
            info.mod_time = Some(time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid fixture timestamp"));
            info.metadata.insert("etag".to_string(), "before".to_string());
            metadata.add_version(info).expect("construct segment fixture metadata");
            write_test_object_metadata_bytes(&root, "bucket", &object, &metadata.marshal_msg().expect("encode metadata")).await;
        }
    }
    let changed_key = "hot/one";
    let changed_path = root.join("bucket").join(changed_key).join("xl.meta");
    let before = tokio::fs::read(&changed_path).await.expect("read initial hot metadata");
    let mut metadata = FileMeta::new();
    let mut info = FileInfo::new(changed_key, 4, 2);
    info.volume = "bucket".to_string();
    info.name = changed_key.to_string();
    info.size = 1;
    info.mod_time = Some(time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid fixture timestamp"));
    info.metadata.insert("etag".to_string(), "after!".to_string());
    metadata.add_version(info).expect("construct same-size hot mutation");
    write_test_object_metadata_bytes(&root, "bucket", changed_key, &metadata.marshal_msg().expect("encode hot mutation")).await;
    let after = tokio::fs::read(&changed_path)
        .await
        .expect("read back committed fixture mutation");
    assert_eq!(before.len(), after.len(), "fixture rewrite must keep metadata byte length unchanged");
    assert_ne!(before, after, "a changed key requires an observable successful fixture write");
    scanner.old_cache.info.name = "bucket".to_string();
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.update_cache.info.name = "bucket".to_string();
    let paths = Arc::new(Mutex::new(Vec::<String>::new()));
    let proposed_walked = Arc::new(Mutex::new(BTreeSet::<String>::new()));
    scanner.update_current_path = Arc::new({
        let paths = paths.clone();
        let proposed_walked = proposed_walked.clone();
        move |path: &str| {
            let mut paths = paths.lock().expect("lock bounded actual-walk samples");
            assert!(paths.len() < MAX_WALK_SAMPLES, "fixture walk exceeded its entry budget");
            let bytes: usize = paths.iter().map(String::len).sum();
            assert!(path.len() <= MAX_WALK_BYTES - bytes, "fixture walk exceeded its byte budget");
            paths.push(path.to_string());
            if observe {
                let proposed = admit_segment_invalidation(&segment_envelope(), &segment_proof(), [changed_key])
                    .expect("bounded successful fixture mutation");
                if let Some(segment) = path.strip_prefix("bucket/").and_then(|path| path.split('/').next())
                    && proposed.contains(segment)
                {
                    proposed_walked
                        .lock()
                        .expect("lock bounded observed segments")
                        .insert(segment.to_string());
                }
            }
            Box::pin(async {})
        }
    });
    scanner
        .scan_folder(
            CancellationToken::new(),
            CachedFolder {
                name: "bucket".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut DataUsageEntry::default(),
        )
        .await
        .expect("actual folder walker must finish independently of diagnostics");
    let paths = paths.lock().expect("read walk samples").clone();
    assert!(!paths.is_empty());
    for prefix in ["hot", "cold", "other"] {
        assert!(
            paths.iter().any(|path| path == &format!("bucket/{prefix}")),
            "all fixture segments must actually be walked"
        );
    }
    let store = FixtureStore::new();
    let revisions = DataUsageCache::default()
        .load_with_revisions(store.clone(), CACHE_NAME)
        .await
        .expect("read empty fixture revisions");
    scanner
        .new_cache
        .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0)
        .await
        .expect("save actual walker output through the cache codec and revision gate");
    let loaded = store.strict_load().await;
    assert_eq!(loaded.checked_flatten("bucket").expect("complete fixture tree").objects, 6);
    assert_eq!(
        cache_value(&loaded),
        cache_value(&scanner.new_cache),
        "codec round-trip must retain the entire cache, not just aggregate size"
    );
    if observe {
        let proposed = proposed_walked.lock().expect("read callback observations").clone();
        assert_eq!(proposed, BTreeSet::from(["hot".to_string()]));
        let walked_segments: BTreeSet<_> = paths
            .iter()
            .filter_map(|path| path.strip_prefix("bucket/"))
            .filter_map(|path| path.split('/').next())
            .collect();
        assert_eq!(walked_segments, BTreeSet::from(["cold", "hot", "other"]));
        assert!(proposed.iter().all(|segment| walked_segments.contains(segment.as_str())));
        assert_eq!(
            walked_segments.len() - proposed.len(),
            2,
            "the two non-proposed segments must still be walked"
        );
    } else {
        assert!(proposed_walked.lock().expect("read disabled observations").is_empty());
    }
    // Compare semantic values because map encoding order is not content identity.
    (paths, cache_value(&loaded))
}

#[tokio::test]
#[serial]
async fn segment_observation_on_off_preserves_actual_walk_and_saved_cache() {
    let off = walk_and_save(false).await;
    let on = walk_and_save(true).await;
    assert_eq!(off.0, on.0, "diagnostics must not change actual traversal order or coverage");
    assert_eq!(off.1, on.1, "diagnostics must not change the saved cache result");
}
