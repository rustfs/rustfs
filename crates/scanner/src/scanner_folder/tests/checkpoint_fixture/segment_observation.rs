//! Fixture-only range diagnostics. No result is supplied to a scan selector.

use super::*;
use std::collections::BTreeSet;

const MAX_SEGMENTS: usize = 4;
const MAX_SEGMENT_BYTES: usize = 128;
const MAX_WALK_SAMPLES: usize = 32;
const MAX_WALK_BYTES: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Untrusted {
    MissingProducer,
    Restart,
    Gap,
    CompactedCoverage,
    EntryLimit,
    ByteLimit,
    InvalidKey,
}

// The keys and coverage reason are explicit test inputs, not an inferred
// production mutation stream. Equal usage totals never establish coverage.
fn fixture_proposal(keys: &[&str], coverage: Result<(), Untrusted>) -> Result<BTreeSet<String>, Untrusted> {
    coverage?;
    let mut segments = BTreeSet::new();
    let mut bytes = 0;
    for key in keys {
        if key.is_empty() || key.contains(['\\', '\0']) || key.split('/').any(|part| matches!(part, "" | "." | "..")) {
            return Err(Untrusted::InvalidKey);
        }
        let segment = key.split('/').next().expect("validated nonempty key");
        if segments.contains(segment) {
            continue;
        }
        if segments.len() == MAX_SEGMENTS {
            return Err(Untrusted::EntryLimit);
        }
        if segment.len() > MAX_SEGMENT_BYTES - bytes {
            return Err(Untrusted::ByteLimit);
        }
        bytes += segment.len();
        segments.insert(segment.to_string());
    }
    Ok(segments)
}

#[test]
fn segment_observation_fixture_coverage_and_bounds_are_explicit() {
    assert_eq!(fixture_proposal(&["hot/one", "hot/two"], Ok(())), Ok(BTreeSet::from(["hot".to_string()])));
    for reason in [
        Untrusted::MissingProducer,
        Untrusted::Restart,
        Untrusted::Gap,
        Untrusted::CompactedCoverage,
    ] {
        assert_eq!(fixture_proposal(&["hot/one"], Err(reason)), Err(reason));
    }
    assert_eq!(
        fixture_proposal(&["a", "b", "c", "d"], Ok(())).expect("entry boundary").len(),
        MAX_SEGMENTS
    );
    assert_eq!(fixture_proposal(&["a", "b", "c", "d", "e"], Ok(())), Err(Untrusted::EntryLimit));
    let exact = "x".repeat(MAX_SEGMENT_BYTES);
    assert!(fixture_proposal(&[&exact], Ok(())).is_ok());
    assert_eq!(fixture_proposal(&[&exact, "y"], Ok(())), Err(Untrusted::ByteLimit));
    let oversized = "x".repeat(MAX_SEGMENT_BYTES + 1);
    assert_eq!(fixture_proposal(&[&oversized], Ok(())), Err(Untrusted::ByteLimit));
    for key in ["", "/hot", "hot/../cold", "hot//one", "hot\\one", "hot/\0"] {
        assert_eq!(fixture_proposal(&[key], Ok(())), Err(Untrusted::InvalidKey));
    }
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
            info.size = 1;
            info.mod_time = Some(time::OffsetDateTime::UNIX_EPOCH);
            metadata.add_version(info).expect("construct segment fixture metadata");
            write_test_object_metadata_bytes(&root, "bucket", &object, metadata.marshal_msg().expect("encode metadata")).await;
        }
    }
    scanner.old_cache.info.name = "bucket".to_string();
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.update_cache.info.name = "bucket".to_string();
    let paths = Arc::new(Mutex::new(Vec::<String>::new()));
    scanner.update_current_path = Arc::new({
        let paths = paths.clone();
        move |path: &str| {
            let mut paths = paths.lock().expect("lock bounded actual-walk samples");
            assert!(paths.len() < MAX_WALK_SAMPLES, "fixture walk exceeded its entry budget");
            let bytes: usize = paths.iter().map(String::len).sum();
            assert!(path.len() <= MAX_WALK_BYTES - bytes, "fixture walk exceeded its byte budget");
            paths.push(path.to_string());
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
        let proposed = fixture_proposal(&["hot/one", "hot/two"], Ok(())).expect("explicit fixture coverage");
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
        eprintln!(
            "segment fixture: proposed={proposed:?}, actual_segments={walked_segments:?}, actual_walk_callbacks={}, production_producer_coverage=unverified",
            paths.len()
        );
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
