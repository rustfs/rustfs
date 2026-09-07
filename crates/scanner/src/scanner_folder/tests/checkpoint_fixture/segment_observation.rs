//! Fixture-only range diagnostics. No result is supplied to a scan selector.

use super::*;
use crate::DATA_USAGE_CACHE_KEY_FORMAT;
use std::collections::BTreeSet;

const MAX_SEGMENTS: usize = 4;
const MAX_SEGMENT_BYTES: usize = 128;
const MAX_WALK_SAMPLES: usize = 32;
const MAX_WALK_BYTES: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProposalError {
    EntryLimit,
    ByteLimit,
    InvalidKey,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProducerKind {
    Put,
    Delete,
    DeleteMarker,
    Multipart,
    Replication,
    Tier,
    DirectoryObject,
}

impl ProducerKind {
    const REQUIRED: [Self; 7] = [
        Self::Put,
        Self::Delete,
        Self::DeleteMarker,
        Self::Multipart,
        Self::Replication,
        Self::Tier,
        Self::DirectoryObject,
    ];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SegmentInvalidationDomain {
    LocalSingleSet,
    DistributedEc,
}

#[derive(Clone, Debug)]
struct SegmentObservationEnvelope<'a> {
    source: DataUsageCacheSource,
    bucket_incarnation: uuid::Uuid,
    key_format: u16,
    baseline_scan_plan_digest: DataUsageScanPlanDigest,
    process_epoch: &'a str,
    generation_start: u64,
    generation_end: u64,
    restart_gap: bool,
    overflow: bool,
    producers: BTreeSet<&'a str>,
    keys: &'a [&'a str],
}

#[derive(Clone, Debug)]
struct SegmentObservationProof<'a> {
    source: DataUsageCacheSource,
    bucket_incarnation: uuid::Uuid,
    key_format: u16,
    baseline_scan_plan_digest: DataUsageScanPlanDigest,
    process_epoch: &'a str,
    durable_producer_identity: bool,
    invalidation_domain: SegmentInvalidationDomain,
    distributed_ec_invalidation: bool,
    cold_zero_walk_oracle: bool,
}

fn producer_name(kind: ProducerKind) -> &'static str {
    match kind {
        ProducerKind::Put => "put",
        ProducerKind::Delete => "delete",
        ProducerKind::DeleteMarker => "delete_marker",
        ProducerKind::Multipart => "multipart",
        ProducerKind::Replication => "replication",
        ProducerKind::Tier => "tier",
        ProducerKind::DirectoryObject => "directory_object",
    }
}

fn trusted_fixture_proposal(
    envelope: &SegmentObservationEnvelope<'_>,
    proof: &SegmentObservationProof<'_>,
) -> Result<BTreeSet<String>, ProposalError> {
    if envelope.source != proof.source
        || envelope.bucket_incarnation.is_nil()
        || envelope.bucket_incarnation != proof.bucket_incarnation
        || envelope.key_format != proof.key_format
        || envelope.baseline_scan_plan_digest != proof.baseline_scan_plan_digest
        || envelope.process_epoch != proof.process_epoch
        || !proof.durable_producer_identity
        || !proof.cold_zero_walk_oracle
        || (proof.invalidation_domain == SegmentInvalidationDomain::DistributedEc && !proof.distributed_ec_invalidation)
        || envelope.generation_start == 0
        || envelope.generation_end < envelope.generation_start
        || envelope.restart_gap
        || envelope.overflow
        || !ProducerKind::REQUIRED
            .iter()
            .all(|producer| envelope.producers.contains(producer_name(*producer)))
    {
        return Err(ProposalError::InvalidKey);
    }

    fixture_proposal(envelope.keys)
}

// Keys come from successful fixture writes, not a production mutation stream.
fn fixture_proposal(keys: &[&str]) -> Result<BTreeSet<String>, ProposalError> {
    let mut segments = BTreeSet::new();
    let mut bytes = 0;
    for key in keys {
        if key.is_empty() || key.contains(['\\', '\0']) || key.split('/').any(|part| matches!(part, "" | "." | "..")) {
            return Err(ProposalError::InvalidKey);
        }
        let segment = key.split('/').next().expect("validated nonempty key");
        if segments.contains(segment) {
            continue;
        }
        if segments.len() == MAX_SEGMENTS {
            return Err(ProposalError::EntryLimit);
        }
        if segment.len() > MAX_SEGMENT_BYTES - bytes {
            return Err(ProposalError::ByteLimit);
        }
        bytes += segment.len();
        segments.insert(segment.to_string());
    }
    Ok(segments)
}

#[test]
fn segment_observation_fixture_proposal_bounds() {
    assert_eq!(fixture_proposal(&["hot/one", "hot/two"]), Ok(BTreeSet::from(["hot".to_string()])));
    assert_eq!(fixture_proposal(&["a", "b", "c", "d"]).expect("entry boundary").len(), MAX_SEGMENTS);
    assert_eq!(fixture_proposal(&["a", "b", "c", "d", "e"]), Err(ProposalError::EntryLimit));
    let exact = "x".repeat(MAX_SEGMENT_BYTES);
    assert!(fixture_proposal(&[&exact]).is_ok());
    assert_eq!(fixture_proposal(&[&exact, "y"]), Err(ProposalError::ByteLimit));
    let oversized = "x".repeat(MAX_SEGMENT_BYTES + 1);
    assert_eq!(fixture_proposal(&[&oversized]), Err(ProposalError::ByteLimit));
    for key in ["", "/hot", "hot/../cold", "hot//one", "hot\\one", "hot/\0"] {
        assert_eq!(fixture_proposal(&[key]), Err(ProposalError::InvalidKey));
    }
}

#[test]
fn segment_observation_trusted_proposal_requires_identity_and_complete_producer_coverage() {
    let source = DataUsageCacheSource::new(2, 3);
    let incarnation = uuid::Uuid::from_u128(0x12345678123456781234567812345678);
    let baseline = DataUsageScanPlanDigest([9; 32]);
    let producers = ProducerKind::REQUIRED
        .iter()
        .map(|producer| producer_name(*producer))
        .collect::<BTreeSet<_>>();
    let envelope = SegmentObservationEnvelope {
        source,
        bucket_incarnation: incarnation,
        key_format: DATA_USAGE_CACHE_KEY_FORMAT,
        baseline_scan_plan_digest: baseline,
        process_epoch: "epoch-a",
        generation_start: 11,
        generation_end: 13,
        restart_gap: false,
        overflow: false,
        producers,
        keys: &["hot/one", "hot/two", "archive/delete-marker"],
    };
    let proof = SegmentObservationProof {
        source,
        bucket_incarnation: incarnation,
        key_format: DATA_USAGE_CACHE_KEY_FORMAT,
        baseline_scan_plan_digest: baseline,
        process_epoch: "epoch-a",
        durable_producer_identity: true,
        invalidation_domain: SegmentInvalidationDomain::LocalSingleSet,
        distributed_ec_invalidation: false,
        cold_zero_walk_oracle: true,
    };

    assert_eq!(
        trusted_fixture_proposal(&envelope, &proof),
        Ok(BTreeSet::from(["archive".to_string(), "hot".to_string()]))
    );

    let mut wrong_source = envelope.clone();
    wrong_source.source = DataUsageCacheSource::new(2, 4);
    assert_eq!(trusted_fixture_proposal(&wrong_source, &proof), Err(ProposalError::InvalidKey));

    let mut missing_incarnation = envelope.clone();
    missing_incarnation.bucket_incarnation = uuid::Uuid::nil();
    assert_eq!(trusted_fixture_proposal(&missing_incarnation, &proof), Err(ProposalError::InvalidKey));

    let mut wrong_key_format = envelope.clone();
    wrong_key_format.key_format = DATA_USAGE_CACHE_KEY_FORMAT.saturating_add(1);
    assert_eq!(trusted_fixture_proposal(&wrong_key_format, &proof), Err(ProposalError::InvalidKey));

    let mut wrong_baseline = envelope.clone();
    wrong_baseline.baseline_scan_plan_digest = DataUsageScanPlanDigest([8; 32]);
    assert_eq!(trusted_fixture_proposal(&wrong_baseline, &proof), Err(ProposalError::InvalidKey));

    let mut wrong_epoch = envelope.clone();
    wrong_epoch.process_epoch = "epoch-b";
    assert_eq!(trusted_fixture_proposal(&wrong_epoch, &proof), Err(ProposalError::InvalidKey));

    let mut no_durable_identity = proof.clone();
    no_durable_identity.durable_producer_identity = false;
    assert_eq!(trusted_fixture_proposal(&envelope, &no_durable_identity), Err(ProposalError::InvalidKey));

    let mut restart_gap = envelope.clone();
    restart_gap.restart_gap = true;
    assert_eq!(trusted_fixture_proposal(&restart_gap, &proof), Err(ProposalError::InvalidKey));

    let mut overflow = envelope.clone();
    overflow.overflow = true;
    assert_eq!(trusted_fixture_proposal(&overflow, &proof), Err(ProposalError::InvalidKey));

    let mut generation_gap = envelope.clone();
    generation_gap.generation_end = generation_gap.generation_start - 1;
    assert_eq!(trusted_fixture_proposal(&generation_gap, &proof), Err(ProposalError::InvalidKey));

    let mut missing_producer = envelope.clone();
    missing_producer.producers.remove(producer_name(ProducerKind::Replication));
    assert_eq!(trusted_fixture_proposal(&missing_producer, &proof), Err(ProposalError::InvalidKey));

    let mut missing_zero_walk_oracle = proof.clone();
    missing_zero_walk_oracle.cold_zero_walk_oracle = false;
    assert_eq!(
        trusted_fixture_proposal(&envelope, &missing_zero_walk_oracle),
        Err(ProposalError::InvalidKey)
    );

    let mut distributed_without_invalidation = proof.clone();
    distributed_without_invalidation.invalidation_domain = SegmentInvalidationDomain::DistributedEc;
    assert_eq!(
        trusted_fixture_proposal(&envelope, &distributed_without_invalidation),
        Err(ProposalError::InvalidKey)
    );

    let mut distributed_with_invalidation = distributed_without_invalidation;
    distributed_with_invalidation.distributed_ec_invalidation = true;
    assert_eq!(
        trusted_fixture_proposal(&envelope, &distributed_with_invalidation),
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
                let proposed = fixture_proposal(&[changed_key]).expect("bounded successful fixture mutation");
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
