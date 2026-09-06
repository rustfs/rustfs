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
use crate::scanner_budget::ScannerCycleBudgetConfig;
use crate::scanner_io::{ScannerDiskScanOptions, ScannerDiskScanOutcome, ScannerIODisk};
use crate::storage_api::scanner_io::ObjectIO;
use crate::{DataUsageCacheSource, DataUsageScanPlanDigest};
use std::io::Cursor;
use tokio::io::AsyncReadExt;

mod deep_compacted;
mod segment_observation;

const CACHE_NAME: &str = "bucket/checkpoint-fixture.bin";
const STATIC_OBJECTS: u64 = 24;
const MAX_CACHE_BYTES: u64 = 1024 * 1024;
const SOURCE: DataUsageCacheSource = DataUsageCacheSource::new(0, 0);
const PLAN: DataUsageScanPlanDigest = DataUsageScanPlanDigest([17; 32]);

fn scan_options(scan_mode: HealScanMode) -> ScannerDiskScanOptions {
    ScannerDiskScanOptions {
        scan_mode,
        prefix_scan_scope: None,
    }
}

/// Real cache persistence codec and CAS calls, backed by two bounded local files.
#[derive(Debug)]
struct FixtureStore {
    root: tempfile::TempDir,
    reject_save: AtomicBool,
}

impl FixtureStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            root: tempfile::tempdir().expect("checkpoint fixture storage directory"),
            reject_save: AtomicBool::new(false),
        })
    }

    fn path(&self, object: &str) -> std::path::PathBuf {
        assert!(object.ends_with(CACHE_NAME) || object.ends_with(&format!("{CACHE_NAME}.bkp")));
        self.root
            .path()
            .join(if object.ends_with(".bkp") { "backup" } else { "main" })
    }

    async fn strict_load(&self) -> DataUsageCache {
        let bytes = tokio::fs::read(self.root.path().join("main"))
            .await
            .expect("saved checkpoint fixture must exist");
        decode_fixture(&bytes).expect("saved checkpoint fixture must contain a valid bucket root")
    }
}

#[async_trait::async_trait]
impl ObjectIO for FixtureStore {
    type Error = crate::EcstoreError;
    type RangeSpec = crate::storage_api::scanner_io::HTTPRangeSpec;
    type HeaderMap = http::HeaderMap;
    type ObjectOptions = crate::ScannerObjectOptions;
    type ObjectInfo = crate::ScannerObjectInfo;
    type GetObjectReader = crate::ScannerGetObjectReader;
    type PutObjectReader = crate::ScannerPutObjReader;

    async fn get_object_reader(
        &self,
        _bucket: &str,
        object: &str,
        _range: Option<Self::RangeSpec>,
        _headers: Self::HeaderMap,
        _options: &Self::ObjectOptions,
    ) -> crate::EcstoreResult<Self::GetObjectReader> {
        let bytes = tokio::fs::read(self.path(object)).await.map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                crate::EcstoreError::FileNotFound
            } else {
                crate::EcstoreError::from(error)
            }
        })?;
        assert!(u64::try_from(bytes.len()).expect("cache length") <= MAX_CACHE_BYTES);
        Ok(crate::ScannerGetObjectReader {
            stream: Box::new(Cursor::new(bytes)),
            object_info: crate::ScannerObjectInfo {
                etag: Some("fixture".into()),
                ..Default::default()
            },
            buffered_body: None,
            body_source: Default::default(),
        })
    }

    async fn put_object(
        &self,
        _bucket: &str,
        object: &str,
        data: &mut Self::PutObjectReader,
        options: &Self::ObjectOptions,
    ) -> crate::EcstoreResult<Self::ObjectInfo> {
        if self.reject_save.load(Ordering::SeqCst) {
            return Err(crate::EcstoreError::PreconditionFailed);
        }
        let path = self.path(object);
        let exists = tokio::fs::try_exists(&path).await?;
        let preconditions = options.http_preconditions.as_ref().expect("checkpoint writes must use CAS");
        if (exists && preconditions.if_none_match_value() == Some("*"))
            || (!exists && preconditions.if_match_value().is_some())
            || (exists && preconditions.if_match_value() != Some("fixture"))
        {
            return Err(crate::EcstoreError::PreconditionFailed);
        }
        let mut bytes = Vec::new();
        (&mut data.stream).take(MAX_CACHE_BYTES + 1).read_to_end(&mut bytes).await?;
        assert!(u64::try_from(bytes.len()).expect("cache length") <= MAX_CACHE_BYTES);
        tokio::fs::write(path, bytes).await?;
        Ok(crate::ScannerObjectInfo {
            etag: Some("fixture".into()),
            ..Default::default()
        })
    }
}

#[async_trait::async_trait]
impl crate::ScannerConfigObjectDelete for FixtureStore {
    async fn delete_config_object(
        &self,
        _bucket: &str,
        _object: &str,
        _options: crate::ScannerObjectOptions,
    ) -> crate::EcstoreResult<crate::ScannerObjectInfo> {
        Err(crate::EcstoreError::NotImplemented)
    }

    async fn scanner_data_usage_publication_admission(&self) -> Option<crate::ScannerDataUsagePublicationAdmission> {
        Some(crate::ScannerDataUsagePublicationAdmission::unfenced())
    }
}

fn decode_fixture(bytes: &[u8]) -> Result<DataUsageCache, &'static str> {
    if bytes.is_empty() || bytes.len() > usize::try_from(MAX_CACHE_BYTES).expect("fixture bound") {
        return Err("missing or oversized checkpoint fixture");
    }
    let cache = DataUsageCache::unmarshal(bytes).map_err(|_| "corrupt checkpoint fixture")?;
    if cache.info.name != "bucket" || cache.checked_flatten("bucket").is_none() {
        return Err("checkpoint fixture has no valid bucket root");
    }
    Ok(cache)
}

fn retained(cache: &DataUsageCache) -> u64 {
    assert!(
        !cache.root().is_some_and(|root| root.compacted),
        "a compacted bucket root cannot prove static-prefix coverage"
    );
    cache
        .checked_flatten("bucket/static")
        .map_or(0, |entry| u64::try_from(entry.objects).expect("fixture object count fits u64"))
}

#[derive(Debug, PartialEq, Eq)]
enum CoverageDiagnosis {
    Progress,
    NoNewWork,
    LostAtPrepare,
    LostAtReload,
    WalkWithoutRetention,
}

fn diagnose(previous: u64, prepared: u64, walked: u64, scanned: u64, reloaded: u64) -> CoverageDiagnosis {
    if reloaded < scanned {
        CoverageDiagnosis::LostAtReload
    } else if prepared < previous {
        CoverageDiagnosis::LostAtPrepare
    } else if walked > 0 && reloaded <= previous {
        CoverageDiagnosis::WalkWithoutRetention
    } else if reloaded > previous {
        CoverageDiagnosis::Progress
    } else {
        CoverageDiagnosis::NoNewWork
    }
}

#[test]
fn checkpoint_fixture_diagnosis_rejects_walk_without_retention() {
    assert_eq!(diagnose(4, 4, 9, 8, 8), CoverageDiagnosis::Progress);
    assert_eq!(diagnose(4, 4, 9, 4, 4), CoverageDiagnosis::WalkWithoutRetention);
    assert_eq!(diagnose(4, 0, 9, 4, 4), CoverageDiagnosis::LostAtPrepare);
    assert_eq!(diagnose(4, 4, 9, 8, 4), CoverageDiagnosis::LostAtReload);
    assert_eq!(diagnose(4, 4, 0, 4, 4), CoverageDiagnosis::NoNewWork);
}

#[test]
fn checkpoint_fixture_missing_and_corrupt_inputs_fail() {
    for bytes in [
        vec![],
        vec![0xc1],
        DataUsageCache::default().marshal_msg().expect("empty cache encoding"),
        vec![0; usize::try_from(MAX_CACHE_BYTES + 1).expect("oversized fixture")],
    ] {
        assert!(decode_fixture(&bytes).is_err(), "invalid fixture must not become an empty complete root");
    }
}

#[test]
fn checkpoint_fixture_compaction_preserves_aggregate_not_child_enumeration() {
    let mut cache = DataUsageCache::default();
    cache.info.name = "bucket".to_string();
    cache.replace("bucket", "", DataUsageEntry::default());
    cache.replace("bucket/static", "bucket", DataUsageEntry::default());
    for index in 0..4 {
        cache.replace(
            &format!("bucket/static/{index}"),
            "bucket/static",
            DataUsageEntry {
                objects: 1,
                ..Default::default()
            },
        );
    }
    cache.reduce_children_of(&hash_path("bucket/static"), 1, true);
    let decoded = decode_fixture(&cache.marshal_msg().expect("encode compacted cache")).expect("decode compacted fixture");
    let entry = decoded
        .find("bucket/static")
        .expect("compaction must retain the static subtree root");
    assert!(entry.compacted);
    assert!(entry.children.is_empty());
    assert_eq!(
        retained(&decoded),
        4,
        "compaction retains aggregate coverage even when leaf keys are absent"
    );
}

fn bound_checkpoint() -> (DataUsageCache, crate::DataUsageScanIdentity) {
    let identity = crate::DataUsageScanIdentity {
        version: 1,
        bucket_incarnation: Uuid::from_u128(1),
        set_layout: DataUsageScanPlanDigest([41; 32]),
        publication_epoch: 0,
        tier_registry_generation: 7,
        scan_mode: HealScanMode::Normal,
    };
    let mut cache = DataUsageCache::default();
    cache.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, identity);
    cache.replace("bucket", "", DataUsageEntry::default());
    cache.replace(
        "bucket/static",
        "bucket",
        DataUsageEntry {
            objects: 3,
            ..Default::default()
        },
    );
    cache.info.scan_resume_after = Some("bucket/static".into());
    cache.info.scan_checkpoint = Some(DataUsageScanCheckpoint::new(
        "bucket/static".into(),
        DataUsageScanCheckpointReason::Objects,
    ));
    cache
        .seal_scan_frontier(Some("bucket/static"))
        .expect("completed fixture prefix receipt");
    (cache, identity)
}

#[test]
fn checkpoint_fixture_roundtrip_retains_verified_scope_but_old_reader_rebuilds() {
    let (cache, identity) = bound_checkpoint();
    let mut cache = decode_fixture(&cache.marshal_msg().expect("encode bound progress")).expect("read bound progress");
    let next_plan = DataUsageScanPlanDigest([42; 32]);
    assert_eq!(
        cache.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, next_plan, identity),
        crate::DataUsageCachePrepareOutcome::Reused
    );
    assert_eq!(retained(&cache), 3);
    assert_eq!(cache.info.scan_identity, Some(identity));
    assert_eq!(
        cache.info.scan_progress,
        Some(crate::DataUsageScanProgress {
            started_plan: PLAN,
            requested_plan: next_plan
        })
    );
    assert!(cache.info.scan_plan_digest.is_none());
    let mut old_wire = serde_json::to_value(&cache).expect("map-encoded compatibility fixture");
    let old_info = old_wire["info"].as_object_mut().expect("cache info is a map");
    old_info.remove("scan_identity");
    old_info.remove("scan_progress");
    old_info.remove("scan_coverage_receipt");
    let mut old_view: DataUsageCache = serde_json::from_value(old_wire).expect("old writer drops unknown metadata");
    assert_eq!(
        old_view.prepare_for_scan("bucket", 11, 7, SOURCE, next_plan, true),
        crate::DataUsageCachePrepareOutcome::Reset
    );
    assert!(old_view.cache.is_empty());
    assert!(!old_view.info.snapshot_complete);
}

#[test]
fn checkpoint_fixture_optional_coverage_metadata_roundtrips_together() {
    let (mut cache, _) = bound_checkpoint();
    cache.info.scan_coverage_digest = Some(DataUsageScanPlanDigest([77; 32]));
    let encoded = cache.marshal_msg().expect("encode every optional coverage field");
    let decoded = DataUsageCache::unmarshal(&encoded).expect("decode combined W03/W04 metadata map");
    assert_eq!(decoded.info.scan_identity, cache.info.scan_identity);
    assert_eq!(decoded.info.scan_progress, cache.info.scan_progress);
    assert_eq!(decoded.info.scan_coverage_receipt, cache.info.scan_coverage_receipt);
    assert_eq!(decoded.info.scan_coverage_digest, cache.info.scan_coverage_digest);
    assert_eq!(decoded.validated_scan_frontier(), Some("bucket/static"));
    assert!(!decoded.info.snapshot_complete);
}

#[test]
fn checkpoint_fixture_unchanged_complete_plan_keeps_existing_rescan_policy() {
    let (mut cache, identity) = bound_checkpoint();
    cache.info.scan_progress = None;
    cache.info.scan_plan_digest = Some(PLAN);
    cache.info.scan_resume_after = None;
    cache.info.scan_checkpoint = None;
    cache.info.scan_coverage_receipt = None;
    cache.info.snapshot_complete = true;
    assert_eq!(
        cache.prepare_bucket_checkpoint("bucket", 12, 7, SOURCE, PLAN, identity),
        crate::DataUsageCachePrepareOutcome::Reused
    );
    assert!(
        cache.info.scan_progress.is_none(),
        "unchanged complete coverage needs no forced verification sweep"
    );
    assert_eq!(cache.info.scan_plan_digest, Some(PLAN));
    assert_eq!(retained(&cache), 3);
    let next = DataUsageScanPlanDigest([44; 32]);
    cache.prepare_bucket_checkpoint("bucket", 12, 7, SOURCE, next, identity);
    assert_eq!(cache.info.scan_progress.expect("changed plan must be verified").started_plan, next);
    assert!(cache.info.scan_plan_digest.is_none());
    assert!(!cache.info.snapshot_complete);
}

#[test]
fn checkpoint_fixture_identity_changes_and_future_state_fail_closed() {
    let (cache, identity) = bound_checkpoint();
    for next_identity in [
        crate::DataUsageScanIdentity {
            bucket_incarnation: Uuid::from_u128(2),
            ..identity
        },
        crate::DataUsageScanIdentity {
            set_layout: DataUsageScanPlanDigest([9; 32]),
            ..identity
        },
        crate::DataUsageScanIdentity {
            publication_epoch: 1,
            ..identity
        },
        crate::DataUsageScanIdentity {
            tier_registry_generation: 8,
            ..identity
        },
        crate::DataUsageScanIdentity {
            scan_mode: HealScanMode::Deep,
            ..identity
        },
    ] {
        let mut next = cache.clone();
        assert_eq!(
            next.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, next_identity),
            crate::DataUsageCachePrepareOutcome::Reset
        );
        assert!(next.cache.is_empty());
        assert!(next.info.scan_checkpoint.is_none());
        assert!(!next.info.snapshot_complete);
    }
    for (source, epoch) in [(crate::DataUsageCacheSource::new(1, 0), 7), (SOURCE, 8)] {
        let mut next = cache.clone();
        assert_eq!(
            next.prepare_bucket_checkpoint("bucket", 11, epoch, source, PLAN, identity),
            crate::DataUsageCachePrepareOutcome::Reset
        );
        assert!(next.cache.is_empty());
    }
    for (cycle, epoch, expected) in [
        (10, 7, crate::DataUsageCachePrepareOutcome::RejectedNewerCycle),
        (11, 6, crate::DataUsageCachePrepareOutcome::RejectedNewerLeader),
    ] {
        let mut next = cache.clone();
        assert_eq!(next.prepare_bucket_checkpoint("bucket", cycle, epoch, SOURCE, PLAN, identity), expected);
        assert_eq!(
            serde_json::to_value(&next).expect("current cache"),
            serde_json::to_value(&cache).expect("saved cache")
        );
    }
    for invalid in [
        crate::DataUsageScanIdentity { version: 2, ..identity },
        crate::DataUsageScanIdentity {
            bucket_incarnation: Uuid::nil(),
            ..identity
        },
    ] {
        let mut next = cache.clone();
        crate::scanner_io::current_cache_root_or_prepare_with_generation(
            &mut next,
            "bucket",
            SOURCE,
            11,
            7,
            PLAN,
            crate::scanner_io::DataUsageCacheReuseOptions {
                checkpoint_identity: Some(invalid),
                ..Default::default()
            },
        );
        assert!(next.cache.is_empty(), "unsupported identity must not retain coverage");
    }
}

#[test]
fn checkpoint_fixture_corrupt_cursor_restarts_validation_without_claiming_completion() {
    let (cache, identity) = bound_checkpoint();
    for resume in ["other/static", "bucket/missing"] {
        let mut next = cache.clone();
        next.info.scan_resume_after = Some(resume.into());
        next.info.scan_checkpoint = Some(DataUsageScanCheckpoint::new(resume.into(), DataUsageScanCheckpointReason::Objects));
        let plan = DataUsageScanPlanDigest([43; 32]);
        next.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, plan, identity);
        assert_eq!(retained(&next), 3, "observations may survive an invalid cursor");
        assert!(next.info.scan_resume_after.is_none());
        assert!(next.info.scan_checkpoint.is_none());
        assert_eq!(next.info.scan_progress.expect("new verification sweep").started_plan, plan);
        assert!(!next.info.snapshot_complete);
        assert!(next.info.scan_plan_digest.is_none());
    }
}

#[test]
fn checkpoint_fixture_receipt_binds_covered_prefix_not_unvisited_suffix() {
    let (mut cache, _) = bound_checkpoint();
    cache.replace(
        "bucket/z-unvisited",
        "bucket",
        DataUsageEntry {
            objects: 99,
            ..Default::default()
        },
    );
    assert_eq!(cache.validated_scan_frontier(), Some("bucket/static"));
    let saved = decode_fixture(&cache.marshal_msg().expect("persist receipt")).expect("load receipt");
    assert_eq!(saved.validated_scan_frontier(), Some("bucket/static"));
    cache.cache.get_mut("bucket/static").expect("covered prefix").objects = 100;
    assert!(
        cache.validated_scan_frontier().is_none(),
        "altered covered content must invalidate the receipt"
    );
}

#[tokio::test]
#[serial]
async fn checkpoint_fixture_existing_uncovered_cursor_cannot_skip_to_complete() {
    let (scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for (object, size) in [
        ("a-done/object", 1),
        ("b-pending/first", 7),
        ("b-pending/second", 1),
        ("z-stale/object", 2),
    ] {
        write_checkpoint_object(&root, object, &[(None, size)]).await;
    }
    let identity = crate::DataUsageScanIdentity {
        tier_registry_generation: crate::runtime_tier_registry_for_cycle(11, 7).await.generation,
        ..bound_checkpoint().1
    };
    for tamper_receipt_path in [false, true] {
        let store = FixtureStore::new();
        let mut cache = DataUsageCache::default();
        let revisions = cache
            .load_with_revisions(store.clone(), CACHE_NAME)
            .await
            .expect("empty fixture revisions");
        cache.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, identity);
        cache.replace("bucket", "", DataUsageEntry::default());
        for (prefix, objects) in [("a-done", 1), ("b-pending", 99), ("z-stale", 99)] {
            cache.replace(
                &format!("bucket/{prefix}"),
                "bucket",
                DataUsageEntry {
                    objects,
                    size: objects,
                    compacted: true,
                    ..Default::default()
                },
            );
        }
        cache
            .seal_scan_frontier(Some("bucket/a-done"))
            .expect("actual completed prefix receipt");
        cache.info.scan_resume_after = Some("bucket/z-stale".into());
        cache.info.scan_checkpoint = Some(DataUsageScanCheckpoint::new(
            "bucket/z-stale".into(),
            DataUsageScanCheckpointReason::Objects,
        ));
        if tamper_receipt_path {
            cache.info.scan_coverage_receipt.as_mut().expect("receipt").through = "bucket/z-stale".into();
        }
        cache
            .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0)
            .await
            .expect("persist corrupted existing cursor");
        let mut loaded = store.strict_load().await;
        let revisions = loaded
            .load_with_revisions(store.clone(), CACHE_NAME)
            .await
            .expect("corrupt cursor CAS revision");
        assert!(loaded.validated_scan_frontier().is_none());
        loaded.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, identity);
        assert!(loaded.info.scan_resume_after.is_none());
        loaded.info.skip_healing = true;
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new_with_progress_tracking(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(2),
                ..Default::default()
            },
        );
        let outcome = scanner
            .local_disk
            .clone()
            .nsscanner_disk(
                budget.token(),
                budget.clone(),
                vec![scanner.local_disk.clone()],
                loaded,
                None,
                scan_options(HealScanMode::Normal),
            )
            .await
            .expect("scan must revisit the prefix");
        let ScannerDiskScanOutcome::Partial(cache) = outcome else {
            panic!("uncovered suffix must not become complete")
        };
        assert_eq!(budget.progress().0, 2);
        assert_eq!(cache.checked_flatten("bucket/b-pending").expect("revisited prefix").size, 7);
        assert!(!cache.info.snapshot_complete);
        cache
            .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0)
            .await
            .expect("persist verified partial");
        assert!(!store.strict_load().await.info.snapshot_complete);
    }
}

#[tokio::test]
#[serial]
async fn checkpoint_fixture_failed_child_prevents_receipt_advancing_past_gap() {
    let (scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for object in ["a-good", "b-skipped", "c-later"] {
        write_checkpoint_object(&root, object, &[(None, 1)]).await;
    }
    let identity = crate::DataUsageScanIdentity {
        tier_registry_generation: crate::runtime_tier_registry_for_cycle(11, 7).await.generation,
        ..bound_checkpoint().1
    };
    let mut cache = DataUsageCache::default();
    cache.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, identity);
    cache.info.skip_healing = true;
    cache.info.failed_objects.insert(
        root.join("bucket/b-skipped/xl.meta").to_string_lossy().into_owned(),
        FolderScanner::now_secs(),
    );
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        ScannerCycleBudgetConfig {
            max_objects: Some(2),
            ..Default::default()
        },
    );
    let result = scanner
        .local_disk
        .clone()
        .nsscanner_disk(
            budget.token(),
            budget,
            vec![scanner.local_disk.clone()],
            cache,
            None,
            scan_options(HealScanMode::Normal),
        )
        .await
        .expect("scan with a known failed child");
    let ScannerDiskScanOutcome::Partial(cache) = result else {
        panic!("skipped failure is not complete")
    };
    assert_eq!(cache.validated_scan_frontier(), Some("bucket/a-good"));
    assert!(!cache.info.failed_objects.is_empty());
    assert!(!cache.info.snapshot_complete);
}

#[tokio::test]
#[serial]
async fn checkpoint_fixture_complete_sampling_partial_resumes_with_fixed_budget() {
    check_complete_sampling_resumption(HealScanMode::Normal).await;
}

#[tokio::test]
#[serial]
async fn checkpoint_fixture_normal_partial_reenters_prefix_for_deep_scan() {
    check_complete_sampling_resumption(HealScanMode::Deep).await;
}

async fn check_complete_sampling_resumption(resume_mode: HealScanMode) {
    let (scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for index in 0..9 {
        write_checkpoint_object(&root, &format!("prefix/{index:04}"), &[(None, 1)]).await;
    }
    let identity = crate::DataUsageScanIdentity {
        tier_registry_generation: crate::runtime_tier_registry_for_cycle(11, 7).await.generation,
        ..bound_checkpoint().1
    };
    let mut cache = DataUsageCache::default();
    cache.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, identity);
    cache.info.skip_healing = true;
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, Default::default());
    // Seed an existing complete baseline; every recovery attempt below is bounded.
    let baseline = scanner
        .local_disk
        .clone()
        .nsscanner_disk(
            budget.token(),
            budget,
            vec![scanner.local_disk.clone()],
            cache,
            None,
            scan_options(HealScanMode::Normal),
        )
        .await
        .expect("initial complete baseline");
    let ScannerDiskScanOutcome::Complete(mut cache) = baseline else { panic!("baseline is complete") };
    let mut deep_current = cache.clone();
    let deep_identity = crate::DataUsageScanIdentity {
        scan_mode: HealScanMode::Deep,
        ..identity
    };
    let state = crate::scanner_io::current_cache_root_or_prepare_with_generation(
        &mut deep_current,
        "bucket",
        SOURCE,
        11,
        7,
        PLAN,
        crate::scanner_io::DataUsageCacheReuseOptions {
            checkpoint_identity: Some(deep_identity),
            ..Default::default()
        },
    );
    assert!(
        matches!(state, crate::scanner_io::DataUsageCacheScanState::Prepared { .. }),
        "Normal complete is not Deep Current"
    );
    cache.prepare_bucket_checkpoint("bucket", 12, 7, SOURCE, PLAN, identity);
    assert!(cache.info.scan_progress.is_none(), "complete unchanged baseline uses existing sampling");
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        ScannerCycleBudgetConfig {
            max_objects: Some(3),
            ..Default::default()
        },
    );
    let outcome = scanner
        .local_disk
        .clone()
        .nsscanner_disk(
            budget.token(),
            budget,
            vec![scanner.local_disk.clone()],
            cache,
            None,
            scan_options(HealScanMode::Normal),
        )
        .await
        .expect("sampling interruption");
    let ScannerDiskScanOutcome::Partial(cache) = outcome else {
        panic!("sampling must exhaust the three-object budget")
    };
    assert!(cache.info.scan_progress.is_none());
    assert_eq!(cache.info.scan_plan_digest, Some(PLAN));
    assert!(cache.info.scan_checkpoint.is_some());
    let store = FixtureStore::new();
    let mut loaded = DataUsageCache::default();
    let revisions = loaded
        .load_with_revisions(store.clone(), CACHE_NAME)
        .await
        .expect("fixture revision");
    cache
        .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0)
        .await
        .expect("persist sampling partial");
    if resume_mode == HealScanMode::Deep {
        write_checkpoint_object(&root, "prefix/0000", &[(None, 7)]).await;
    }
    let resumed_identity = crate::DataUsageScanIdentity {
        scan_mode: resume_mode,
        ..identity
    };
    for round in 0..16 {
        let mut loaded = DataUsageCache::default();
        let revisions = loaded
            .load_with_revisions(store.clone(), CACHE_NAME)
            .await
            .expect("reload partial each recovery round");
        loaded.prepare_bucket_checkpoint("bucket", 12, 7, SOURCE, PLAN, resumed_identity);
        assert!(loaded.info.scan_progress.is_some(), "sampling partial must enter forward validation");
        loaded.info.skip_healing = true;
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new_with_progress_tracking(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(3),
                ..Default::default()
            },
        );
        let result = scanner
            .local_disk
            .clone()
            .nsscanner_disk(
                budget.token(),
                budget,
                vec![scanner.local_disk.clone()],
                loaded,
                None,
                scan_options(resume_mode),
            )
            .await
            .expect("bounded recovery scan");
        let (cache, complete) = match result {
            ScannerDiskScanOutcome::Partial(cache) => (cache, false),
            ScannerDiskScanOutcome::Complete(cache) => (cache, true),
            _ => panic!("fixture namespace remains present"),
        };
        if round == 0 && resume_mode == HealScanMode::Deep {
            assert_eq!(cache.find("bucket/prefix/0000").expect("Deep revisits earlier prefix").size, 7);
        }
        cache
            .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0)
            .await
            .expect("save bounded recovery");
        if complete {
            let root = store.strict_load().await.checked_flatten("bucket").expect("certified root");
            assert_eq!(root.objects, 9);
            assert_eq!(root.size, if resume_mode == HealScanMode::Deep { 15 } else { 9 });
            return;
        }
    }
    panic!("sampling interruption must recover with the unchanged three-object budget");
}

#[tokio::test]
#[serial]
async fn checkpoint_fixture_save_reload_resume() {
    run_checkpoint_fixture(false).await;
}

#[tokio::test]
#[serial]
async fn checkpoint_fixture_hot_digest_retains_partial_progress() {
    run_checkpoint_fixture(true).await;
}

async fn write_checkpoint_object(root: &std::path::Path, object: &str, versions: &[(Option<Uuid>, i64)]) {
    let mut metadata = FileMeta::new();
    for (index, (version_id, size)) in versions.iter().enumerate() {
        let mut info = FileInfo::new(object, 4, 2);
        info.volume = "bucket".into();
        info.version_id = *version_id;
        info.versioned = version_id.is_some();
        info.size = *size;
        info.mod_time = Some(
            OffsetDateTime::from_unix_timestamp(1_700_000_000 + i64::try_from(index).expect("fixture index"))
                .expect("non-sentinel fixture modification time"),
        );
        metadata.add_version(info).expect("fixture version");
    }
    write_test_object_metadata_bytes(root, "bucket", object, &metadata.marshal_msg().expect("fixture metadata")).await;
}

async fn run_checkpoint_fixture(change_digest: bool) {
    let (scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for index in 0..STATIC_OBJECTS {
        write_checkpoint_object(&root, &format!("static/{index:04}"), &[(None, 1)]).await;
    }
    let identity = crate::DataUsageScanIdentity {
        version: 1,
        bucket_incarnation: Uuid::from_u128(1),
        set_layout: DataUsageScanPlanDigest([41; 32]),
        publication_epoch: 0,
        tier_registry_generation: crate::runtime_tier_registry_for_cycle(11, 7).await.generation,
        scan_mode: HealScanMode::Normal,
    };
    let store = FixtureStore::new();
    let mut previous = 0;
    let mut visited = 0;
    for round in 0..3_u8 {
        write_checkpoint_object(&root, "hot/current", &[(None, 1)]).await;
        let mut cache = DataUsageCache::default();
        let revisions = cache
            .load_with_revisions(store.clone(), CACHE_NAME)
            .await
            .expect("load checkpoint revisions");
        if round > 0 {
            assert_eq!(retained(&store.strict_load().await), previous);
        }
        let plan = crate::scanner_io::checkpoint_fixture_bucket_digest(PLAN, change_digest.then_some(u64::from(round)));
        crate::scanner_io::current_cache_root_or_prepare_with_generation(
            &mut cache,
            "bucket",
            SOURCE,
            11,
            7,
            plan,
            crate::scanner_io::DataUsageCacheReuseOptions {
                require_source: true,
                tier_registry_generation: None,
                checkpoint_identity: Some(identity),
            },
        );
        let prepared = retained(&cache);
        cache.info.skip_healing = true;
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new_with_progress_tracking(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(4),
                ..Default::default()
            },
        );
        let outcome = scanner
            .local_disk
            .clone()
            .nsscanner_disk(
                budget.token(),
                budget.clone(),
                vec![scanner.local_disk.clone()],
                cache,
                None,
                ScannerDiskScanOptions {
                    scan_mode: HealScanMode::Normal,
                    prefix_scan_scope: None,
                },
            )
            .await
            .expect("budgeted local disk scan returns partial cache");
        let ScannerDiskScanOutcome::Partial(cache) = outcome else {
            panic!("budgeted fixture must remain partial")
        };
        assert!(!cache.info.snapshot_complete, "partial must never publish a complete root");
        assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Objects));
        let scanned = retained(&cache);
        cache
            .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0)
            .await
            .expect("persist partial checkpoint");
        let mut loaded = DataUsageCache::default();
        loaded
            .load(store.clone(), CACHE_NAME)
            .await
            .expect("reload persisted partial checkpoint");
        let reloaded = retained(&loaded);
        assert_eq!(reloaded, retained(&store.strict_load().await));
        assert_eq!(scanned, reloaded, "save/load must retain static subtree coverage");
        assert!(!loaded.info.snapshot_complete);
        visited += budget.entries_visited();
        let diagnosis = diagnose(previous, prepared, budget.entries_visited(), scanned, reloaded);
        eprintln!(
            "checkpoint_fixture round={round} hot_digest={change_digest} visited_total={visited} before={previous} prepared={prepared} scanned={scanned} reloaded={reloaded} diagnosis={diagnosis:?}"
        );
        assert_eq!(
            diagnosis,
            CoverageDiagnosis::Progress,
            "visited growth must produce durable static coverage"
        );
        assert!(loaded.info.scan_plan_digest.is_none(), "old readers must rebuild an uncertified sweep");
        crate::remote_scanner::checkpoint_fixture_partial_return(budget.progress(), budget.entries_visited()).await;
        previous = reloaded;
    }
    assert!(visited > 0, "fixture must exercise the directory walk");
    assert!(previous > 0, "fixture must retain and enumerate static subtree entries");

    let mut loaded = DataUsageCache::default();
    let revisions = loaded
        .load_with_revisions(store.clone(), CACHE_NAME)
        .await
        .expect("load final checkpoint");
    let before = tokio::fs::read(store.root.path().join("main"))
        .await
        .expect("read durable checkpoint bytes");
    let epoch_error = loaded
        .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 1)
        .await
        .expect_err("stale publication epoch must reject persistence");
    assert!(epoch_error.to_string().contains(crate::SCANNER_PUBLICATION_EPOCH_CHANGED));
    store.reject_save.store(true, Ordering::SeqCst);
    loaded.info.next_cycle += 1;
    loaded
        .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0)
        .await
        .expect_err("injected save failure must not report durable progress");
    assert_eq!(
        tokio::fs::read(store.root.path().join("main"))
            .await
            .expect("read unchanged checkpoint bytes"),
        before
    );

    let parent = CancellationToken::new();
    parent.cancel();
    let budget = ScannerCycleBudget::new(&parent, Default::default());
    let result = scanner
        .local_disk
        .clone()
        .nsscanner_disk(
            budget.token(),
            budget.clone(),
            vec![scanner.local_disk.clone()],
            loaded.clone(),
            None,
            ScannerDiskScanOptions {
                scan_mode: HealScanMode::Normal,
                prefix_scan_scope: None,
            },
        )
        .await;
    assert!(result.is_err(), "pre-scan cancellation must not produce a complete root");
    assert_eq!(budget.reason(), None, "parent cancellation is not object budget exhaustion");

    store.reject_save.store(false, Ordering::SeqCst);
    write_checkpoint_object(&root, "static/0000", &[(Some(Uuid::from_u128(2)), 7), (Some(Uuid::from_u128(3)), 3)]).await;
    tokio::fs::remove_dir_all(root.join("bucket/static/0001"))
        .await
        .expect("remove previously scanned fixture object");
    write_checkpoint_object(&root, "hot/later", &[(None, 1)]).await;
    let final_plan = crate::scanner_io::checkpoint_fixture_bucket_digest(PLAN, Some(3));
    let mut saw_mixed_sweep_end = false;
    for _ in 0..32 {
        let mut cache = DataUsageCache::default();
        let revisions = cache
            .load_with_revisions(store.clone(), CACHE_NAME)
            .await
            .expect("reload every bounded round");
        crate::scanner_io::current_cache_root_or_prepare_with_generation(
            &mut cache,
            "bucket",
            SOURCE,
            11,
            7,
            final_plan,
            crate::scanner_io::DataUsageCacheReuseOptions {
                require_source: true,
                tier_registry_generation: Some(identity.tier_registry_generation),
                checkpoint_identity: Some(identity),
            },
        );
        cache.info.skip_healing = true;
        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new_with_progress_tracking(
            &parent,
            ScannerCycleBudgetConfig {
                max_objects: Some(4),
                ..Default::default()
            },
        );
        let outcome = scanner
            .local_disk
            .clone()
            .nsscanner_disk(
                budget.token(),
                budget.clone(),
                vec![scanner.local_disk.clone()],
                cache,
                None,
                scan_options(HealScanMode::Normal),
            )
            .await
            .expect("bounded sweep outcome");
        let (cache, complete) = match outcome {
            ScannerDiskScanOutcome::Complete(cache) => (cache, true),
            ScannerDiskScanOutcome::Partial(cache) => (cache, false),
            ScannerDiskScanOutcome::NamespaceNotFound(_) => panic!("fixture namespace exists"),
        };
        cache
            .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0)
            .await
            .expect("save each bounded sweep");
        let saved = store.strict_load().await;
        if complete {
            assert!(
                saw_mixed_sweep_end,
                "a clean tail must first finish as partial before a new validation sweep"
            );
            assert!(saved.info.snapshot_complete);
            assert!(saved.info.scan_progress.is_none());
            assert!(saved.info.scan_checkpoint.is_none());
            assert_eq!(saved.info.scan_plan_digest, Some(final_plan));
            let total = saved.checked_flatten("bucket").expect("complete bucket root");
            assert_eq!((total.objects, total.versions, total.size), (25, 2, 34));
            assert_eq!(saved.checked_flatten("bucket/static").expect("static subtree").objects, 23);
            assert_eq!(saved.checked_flatten("bucket/hot").expect("hot subtree").objects, 2);
            return;
        }
        assert!(!saved.info.snapshot_complete);
        assert!(saved.info.scan_plan_digest.is_none());
        if !budget.budget_elapsed() {
            saw_mixed_sweep_end = true;
        }
    }
    panic!("finite stable fixture must converge using the same four-object budget without an unbounded final sweep");
}
