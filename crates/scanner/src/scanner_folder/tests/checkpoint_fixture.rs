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
use crate::scanner_io::{ScannerDiskScanOutcome, ScannerIODisk};
use crate::storage_api::scanner_io::ObjectIO;
use crate::{DataUsageCacheSource, DataUsageScanPlanDigest};
use std::io::Cursor;
use tokio::io::AsyncReadExt;

mod segment_observation;

const CACHE_NAME: &str = "bucket/checkpoint-fixture.bin";
const STATIC_OBJECTS: u64 = 24;
const MAX_CACHE_BYTES: u64 = 1024 * 1024;
const SOURCE: DataUsageCacheSource = DataUsageCacheSource::new(0, 0);
const PLAN: DataUsageScanPlanDigest = DataUsageScanPlanDigest([17; 32]);

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

#[tokio::test]
#[serial]
async fn checkpoint_fixture_save_reload_resume() {
    run_checkpoint_fixture(false).await;
}

#[tokio::test]
#[serial]
async fn checkpoint_fixture_hot_digest_diagnostic() {
    run_checkpoint_fixture(true).await;
}

async fn run_checkpoint_fixture(change_digest: bool) {
    let (scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for index in 0..STATIC_OBJECTS {
        write_test_object_metadata(&root, "bucket", &format!("static/{index:04}")).await;
    }
    let store = FixtureStore::new();
    let mut previous = 0;
    let mut visited = 0;
    for round in 0..3_u8 {
        write_test_object_metadata(&root, "bucket", "hot/current").await;
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
            },
        );
        let prepared = retained(&cache);
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
                HealScanMode::Normal,
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
        if !change_digest || std::env::var_os("RUSTFS_CHECKPOINT_REQUIRE_PROGRESS").is_some() {
            assert_eq!(
                diagnosis,
                CoverageDiagnosis::Progress,
                "visited growth must produce durable static coverage"
            );
        }
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
            HealScanMode::Normal,
        )
        .await;
    assert!(result.is_err(), "pre-scan cancellation must not produce a complete root");
    assert_eq!(budget.reason(), None, "parent cancellation is not object budget exhaustion");

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, Default::default());
    let result = scanner
        .local_disk
        .clone()
        .nsscanner_disk(
            budget.token(),
            budget,
            vec![scanner.local_disk.clone()],
            loaded,
            None,
            HealScanMode::Normal,
        )
        .await
        .expect("unbounded scan must complete after durable partial progress");
    let ScannerDiskScanOutcome::Complete(cache) = result else {
        panic!("unbounded fixture must produce a complete disk cache");
    };
    assert!(cache.info.snapshot_complete);
    assert!(cache.info.scan_checkpoint.is_none());
    assert_eq!(
        cache.checked_flatten("bucket").expect("complete bucket root").objects,
        usize::try_from(STATIC_OBJECTS + 1).expect("fixture object count fits usize")
    );
}
