// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::hint::black_box;
use std::sync::atomic::AtomicU64;
use std::time::Instant as WallInstant;

const MAX_WIRE_BYTES: u64 = 32 * 1024 * 1024;
const CACHE_NAME: &str = "bucket/cache-cost.bin";

/// Two bounded memory slots model revision preconditions and count the bytes
/// consumed by the real save entry point, not disk writes or fsync latency.
#[derive(Debug, Default)]
struct CountingStore {
    slots: Mutex<[(u64, Vec<u8>); 2]>,
    puts: AtomicU64,
    bytes: AtomicU64,
    ingest_ns: AtomicU64,
}

impl CountingStore {
    fn slot(object: &str) -> usize {
        let main = path_join_buf(&[BUCKET_META_PREFIX, CACHE_NAME]);
        if object == main {
            0
        } else {
            assert_eq!(object, format!("{main}.bkp"), "only two fixture cache paths are permitted");
            1
        }
    }

    fn reset_counts(&self) {
        self.puts.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
        self.ingest_ns.store(0, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl ObjectIO for CountingStore {
    type Error = Error;
    type RangeSpec = HTTPRangeSpec;
    type HeaderMap = HeaderMap;
    type ObjectOptions = ObjectOptions;
    type ObjectInfo = ObjectInfo;
    type GetObjectReader = ScannerGetObjectReader;
    type PutObjectReader = ScannerPutObjReader;

    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        _range: Option<Self::RangeSpec>,
        _headers: Self::HeaderMap,
        _options: &Self::ObjectOptions,
    ) -> StorageResult<Self::GetObjectReader> {
        // The real loader may probe the legacy metadata bucket on a miss.
        if bucket != RUSTFS_META_BUCKET {
            return Err(Error::FileNotFound);
        }
        let slots = self.slots.lock().await;
        let (revision, bytes) = &slots[Self::slot(object)];
        if *revision == 0 {
            return Err(Error::FileNotFound);
        }
        Ok(CacheReadStore::reader(CacheReadBody::Bytes(bytes.clone()), &revision.to_string()))
    }

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: &mut Self::PutObjectReader,
        options: &Self::ObjectOptions,
    ) -> StorageResult<Self::ObjectInfo> {
        assert_eq!(bucket, RUSTFS_META_BUCKET);
        let started = WallInstant::now();
        let mut bytes = Vec::new();
        (&mut data.stream).take(MAX_WIRE_BYTES + 1).read_to_end(&mut bytes).await?;
        assert!(u64::try_from(bytes.len()).expect("wire length") <= MAX_WIRE_BYTES);
        let mut slots = self.slots.lock().await;
        let (revision, stored) = &mut slots[Self::slot(object)];
        let preconditions = options.http_preconditions.as_ref().expect("profile saves must use CAS");
        let expected = revision.to_string();
        if (*revision == 0 && preconditions.if_none_match_value() != Some("*"))
            || (*revision != 0 && preconditions.if_match_value() != Some(expected.as_str()))
        {
            return Err(Error::PreconditionFailed);
        }
        self.bytes
            .fetch_add(u64::try_from(bytes.len()).expect("save length"), Ordering::Relaxed);
        self.puts.fetch_add(1, Ordering::Relaxed);
        *stored = bytes;
        *revision += 1;
        self.ingest_ns.fetch_add(elapsed_ns(started), Ordering::Relaxed);
        Ok(ObjectInfo {
            etag: Some(revision.to_string()),
            ..Default::default()
        })
    }
}

#[async_trait::async_trait]
impl crate::ScannerConfigObjectDelete for CountingStore {
    async fn delete_config_object(
        &self,
        _bucket: &str,
        _object: &str,
        _options: crate::ScannerObjectOptions,
    ) -> crate::EcstoreResult<crate::ScannerObjectInfo> {
        Err(Error::NotImplemented)
    }

    async fn scanner_data_usage_publication_admission(&self) -> Option<crate::ScannerDataUsagePublicationAdmission> {
        Some(crate::ScannerDataUsagePublicationAdmission::unfenced())
    }
}

fn elapsed_ns(started: WallInstant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).expect("bounded profile duration")
}

fn fixture(objects: usize) -> DataUsageCache {
    assert!((1..=16384).contains(&objects));
    let mut cache = DataUsageCache::default();
    cache.info.name = "bucket".to_string();
    cache.info.snapshot_complete = true;
    cache.replace("bucket", "", DataUsageEntry::default());
    for index in 0..objects {
        cache.replace(
            &format!("bucket/object-{index:05}"),
            "bucket",
            DataUsageEntry {
                objects: 1,
                versions: 2,
                size: 4096,
                ..Default::default()
            },
        );
    }
    cache
}

fn canonical_cache_value(mut value: Value) -> Value {
    for entry in value["cache"].as_object_mut().expect("cache entry map").values_mut() {
        let children = entry["children"].as_array_mut().expect("entry children set");
        // Sort only the set representation. Do not deduplicate or reorder
        // histograms and other arrays whose element positions carry meaning.
        children.sort_unstable_by(|left, right| {
            left.as_str()
                .expect("child key string")
                .cmp(right.as_str().expect("child key string"))
        });
    }
    value
}

fn same_cache(actual: &DataUsageCache, expected: &DataUsageCache) {
    assert_eq!(
        canonical_cache_value(serde_json::to_value(actual).expect("actual cache structure")),
        canonical_cache_value(serde_json::to_value(expected).expect("expected cache structure")),
        "every cache field and map entry must be retained"
    );
}

#[test]
fn cache_cost_comparison_preserves_set_and_ordered_field_semantics() {
    let forward = fixture(2);
    let mut reverse = forward.clone();
    let children = &mut reverse.cache.get_mut(&hash_path("bucket").key()).expect("root").children;
    children.clear();
    for index in (0..2).rev() {
        children.insert(hash_path(&format!("bucket/object-{index:05}")).key());
    }
    same_cache(&forward, &reverse);

    let original = serde_json::json!({"cache": {"root": {"children": ["a", "b"], "size": 1, "histogram": [1, 2]}}});
    let mut reordered = original.clone();
    reordered["cache"]["root"]["children"] = serde_json::json!(["b", "a"]);
    assert_eq!(canonical_cache_value(original.clone()), canonical_cache_value(reordered));
    for children in [serde_json::json!(["a"]), serde_json::json!(["a", "b", "b"])] {
        let mut changed = original.clone();
        changed["cache"]["root"]["children"] = children;
        assert_ne!(canonical_cache_value(original.clone()), canonical_cache_value(changed));
    }
    for (field, replacement) in [("size", serde_json::json!(2)), ("histogram", serde_json::json!([2, 1]))] {
        let mut changed = original.clone();
        changed["cache"]["root"][field] = replacement;
        assert_ne!(canonical_cache_value(original.clone()), canonical_cache_value(changed));
    }
}

fn quantiles(mut samples: Vec<u64>) -> Value {
    assert!(!samples.is_empty() && samples.len() <= 5);
    samples.sort_unstable();
    serde_json::json!({"p50_ns": samples[samples.len() / 2], "max_ns": samples[samples.len() - 1]})
}

async fn profile_case(objects: usize, scenario: &str, samples: usize) {
    let baseline = fixture(objects);
    let mut cache = baseline.clone();
    let dirty = match scenario {
        "unchanged" => 0,
        "small_dirty" => (objects / 100).max(1),
        "all_dirty" => objects,
        _ => panic!("unknown fixed scenario"),
    };
    let mut changed_entry_wire_bytes = 0;
    for index in 0..dirty {
        let entry = cache
            .cache
            .get_mut(&hash_path(&format!("bucket/object-{index:05}")).key())
            .expect("dirty leaf");
        entry.size += 1;
        entry.versions += 1;
        changed_entry_wire_bytes += rmp_serde::to_vec(entry).expect("changed entry wire bytes").len();
    }
    if scenario == "small_dirty" {
        cache.info.snapshot_complete = false;
        cache.info.scan_resume_after = Some("bucket/object-00000".to_string());
    }
    let expected_wire = cache.marshal_msg().expect("fixture encoding");
    assert!(u64::try_from(expected_wire.len()).expect("fixture bytes") <= MAX_WIRE_BYTES);
    let store = Arc::new(CountingStore::default());
    let mut loaded = DataUsageCache::default();
    let initial = loaded
        .load_with_revisions(store.clone(), CACHE_NAME)
        .await
        .expect("initial revisions");
    baseline
        .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &initial, 0)
        .await
        .expect("baseline save");

    let mut clone_ns = Vec::new();
    let mut copy_ns = Vec::new();
    let mut flatten_ns = Vec::new();
    let mut encode_ns = Vec::new();
    let mut save_ns = Vec::new();
    let mut ingest_ns = Vec::new();
    for _ in 0..samples {
        let started = WallInstant::now();
        let cloned = black_box(cache.clone());
        clone_ns.push(elapsed_ns(started));
        same_cache(&cloned, &cache);
        drop(cloned);

        let mut copied = DataUsageCache {
            info: cache.info.clone(),
            ..Default::default()
        };
        let started = WallInstant::now();
        copied.copy_with_children(black_box(&cache), &hash_path("bucket"), &None);
        copy_ns.push(elapsed_ns(started));
        same_cache(&copied, &cache);
        drop(copied);

        let started = WallInstant::now();
        let aggregate = black_box(cache.checked_flatten("bucket").expect("valid fixture tree"));
        flatten_ns.push(elapsed_ns(started));
        assert_eq!(
            (aggregate.objects, aggregate.versions, aggregate.size),
            (objects, objects * 2 + dirty, objects * 4096 + dirty)
        );

        let started = WallInstant::now();
        let encoded = black_box(cache.marshal_msg().expect("measured encoding"));
        encode_ns.push(elapsed_ns(started));
        assert_eq!(encoded, expected_wire);
        same_cache(&DataUsageCache::unmarshal(&encoded).expect("measured wire reload"), &cache);

        let revisions = loaded
            .load_with_revisions(store.clone(), CACHE_NAME)
            .await
            .expect("current revisions");
        store.reset_counts();
        let started = WallInstant::now();
        cache
            .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0)
            .await
            .expect("measured save");
        save_ns.push(elapsed_ns(started));
        ingest_ns.push(store.ingest_ns.load(Ordering::Relaxed));
        assert_eq!(store.puts.load(Ordering::Relaxed), 2, "main and backup writes must both occur");
        assert_eq!(
            store.bytes.load(Ordering::Relaxed),
            u64::try_from(expected_wire.len() * 2).expect("two saved bodies")
        );
        loaded
            .load_with_revisions(store.clone(), CACHE_NAME)
            .await
            .expect("saved cache reload");
        same_cache(&loaded, &cache);
    }

    let before_rejected = store.slots.lock().await[0].1.clone();
    let mut conflicting = cache.clone();
    conflicting.info.next_cycle += 1;
    assert!(matches!(
        conflicting
            .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &initial, 0)
            .await,
        Err(Error::PreconditionFailed)
    ));
    assert_eq!(
        store.slots.lock().await[0].1,
        before_rejected,
        "stale CAS must not replace the retained checkpoint"
    );
    println!(
        "CACHE_COST {}",
        serde_json::json!({
            "schema": 1, "scenario": scenario, "objects": objects, "dirty_objects": dirty, "samples": samples,
            "build": {
                "debug_assertions": cfg!(debug_assertions),
                "test_opt_level_override": option_env!("CARGO_PROFILE_TEST_OPT_LEVEL"),
                "dev_opt_level_override": option_env!("CARGO_PROFILE_DEV_OPT_LEVEL"),
                "rustflags_visible_to_rustc": option_env!("RUSTFLAGS"),
                "encoded_rustflags_visible_to_rustc": option_env!("CARGO_ENCODED_RUSTFLAGS"),
                "source_revision": option_env!("RUSTFS_CACHE_COST_SOURCE"),
                "source_tree": option_env!("RUSTFS_CACHE_COST_TREE"),
            },
            "retained_cache_entries": cache.cache.len(), "cache_wire_bytes": expected_wire.len(),
            "changed_entry_wire_bytes": changed_entry_wire_bytes, "save_body_bytes_per_sample": expected_wire.len() * 2,
            "snapshot_complete": cache.info.snapshot_complete,
            "clone": quantiles(clone_ns), "copy_with_children": quantiles(copy_ns), "checked_flatten": quantiles(flatten_ns),
            "encode": quantiles(encode_ns), "save_inclusive": quantiles(save_ns), "memory_backend_ingest": quantiles(ingest_ns),
        })
    );
}

#[tokio::test]
async fn cache_cost_profile_preserves_checkpoint_and_counts() {
    let profile = match std::env::var("RUSTFS_CACHE_COST_PROFILE") {
        Err(std::env::VarError::NotPresent) => false,
        Ok(value) if value == "1" => true,
        _ => panic!("RUSTFS_CACHE_COST_PROFILE must be absent or 1"),
    };
    let (sizes, samples): (&[usize], usize) = if profile { (&[1024, 4096, 16384], 5) } else { (&[64], 1) };
    for &objects in sizes {
        for scenario in ["unchanged", "small_dirty", "all_dirty"] {
            profile_case(objects, scenario, samples).await;
        }
    }
}
