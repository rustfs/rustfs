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
use crate::data_usage_define::{DATA_USAGE_OBJ_NAME_PATH, read_config_with_revision};
use crate::storage_api::owner::EcstoreDiskAPI;

type DriveIdentities = HashMap<String, (Uuid, DataUsageCacheSource)>;
type WalkCounts = HashMap<(String, String, String), u64>;

async fn drive_identities(store: &ECStore) -> DriveIdentities {
    let mut identities = HashMap::new();
    let mut ids = HashSet::new();
    for set in store.all_set_disks() {
        let source = DataUsageCacheSource::new(set.pool_index, set.set_index);
        for disk in scanner_set_disk_inventory(set.as_ref()).await {
            let id = EcstoreDiskAPI::get_disk_id(disk.as_ref())
                .await
                .expect("fixture disk identity should be readable")
                .expect("fixture disk must have a durable identity");
            assert!(!id.is_nil());
            assert!(ids.insert(id), "fixture disk identities must be unique");
            let path = crate::ScannerDiskExt::path(disk.as_ref()).to_string_lossy().into_owned();
            assert!(identities.insert(path, (id, source)).is_none());
        }
    }
    assert_eq!(identities.len(), 8);
    identities
}

fn walk_counts(drives: &DriveIdentities) -> WalkCounts {
    rustfs_scanner_metrics::metrics::global_metrics()
        .scanner_runtime_details_report()
        .bucket_drive_results
        .into_iter()
        .filter(|result| drives.contains_key(&result.drive))
        .map(|result| ((result.bucket, result.drive, result.result), result.count))
        .collect()
}

async fn put_and_settle(store: &ECStore, bucket: &str, object: &str) {
    let set = &store.pools[0].disk_set[0];
    let mut reader = ScannerPutObjReader::from_vec(b"object".to_vec());
    set.put_object(bucket, object, &mut reader, &ScannerObjectOptions::default())
        .await
        .expect("fixture object should persist");
    let lock = set.new_ns_lock(bucket, object).await.expect("fixture namespace lock");
    let _settled = lock
        .get_write_lock(Duration::from_secs(30))
        .await
        .expect("quorum-ACK rename tail must settle before taking the activity baseline");
}

async fn create_bucket(store: &ECStore, bucket: &str) {
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("fixture bucket should be created");
    put_and_settle(store, bucket, "initial").await;
}

async fn persist_baseline(store: &Arc<ECStore>, baseline: &DataUsageInfo) {
    let mut baseline = baseline.clone();
    baseline.usage_snapshot_converged = Some(true);
    crate::save_config(
        store.clone(),
        DATA_USAGE_OBJ_NAME_PATH.as_str(),
        serde_json::to_vec(&baseline).expect("baseline should encode"),
    )
    .await
    .expect("fixture baseline should persist");
}

// Every invocation uses the production default scope. The expected walker set
// comes from storage's per-source inventory, not the resolver's selected names.
async fn run_entry(store: &Arc<ECStore>, cycle: u64, selected: Option<&str>, expect_walks: bool) -> DataUsageInfo {
    let drives = drive_identities(store).await;
    let inventory = store
        .list_bucket_for_scanner(&BucketOptions::default())
        .await
        .expect("fixture inventory should be complete");
    assert!(inventory.topology_complete);
    let expected_walks = if expect_walks {
        inventory
            .set_buckets
            .into_iter()
            .flat_map(|set| {
                let source = DataUsageCacheSource::new(set.pool_index, set.set_index);
                set.buckets.into_iter().map(move |bucket| ((source, bucket.name), 1_u64))
            })
            .collect::<HashMap<_, _>>()
    } else {
        HashMap::new()
    };
    let root_before = read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("root baseline should be readable");
    let dirty_before = dirty_usage_buckets_for_tests();
    let generation_before = dirty_usage_generation();
    let before = walk_counts(&drives);
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    let (updates, mut receiver) = mpsc::channel(1);
    let (observer, observed) = tokio::sync::oneshot::channel();
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        nsscanner_with_storage_status_scoped(
            store.as_ref(),
            ScannerCycleRequest {
                ctx,
                budget,
                updates,
                want_cycle: cycle,
                leader_epoch: 11,
                scan_mode: HealScanMode::Normal,
                scan_scope: ScannerBucketScanScope::default(),
                persisted_usage_baseline: root_before.0.clone().map(Bytes::from),
                requires_full_scan: false,
                resolved_scope_observer: Some(observer),
            },
        ),
    )
    .await
    .expect("entry cycle should finish within the fixture deadline")
    .expect("entry cycle should succeed");
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    let scope = observed.await.expect("production resolver should report its decision");
    assert_eq!(
        scope.selected_buckets.as_deref(),
        selected.map(|name| HashSet::from([name.to_string()])).as_ref()
    );
    let usage = receiver.recv().await.expect("one candidate should be delivered");
    assert!(receiver.recv().await.is_none(), "there must be exactly one terminal candidate");
    assert!(usage.usage_snapshot_complete);
    assert!(!usage.usage_snapshot_partial);
    assert_eq!(usage.scanner_cycle, Some(cycle));
    assert_eq!(
        drive_identities(store).await,
        drives,
        "drive identities must not change during the oracle"
    );

    let after = walk_counts(&drives);
    let mut actual = HashMap::new();
    for key in before.keys() {
        assert!(after.contains_key(key), "metrics eviction would invalidate this exact-delta oracle");
    }
    for ((bucket, drive, outcome), count) in after {
        let previous = before
            .get(&(bucket.clone(), drive.clone(), outcome.clone()))
            .copied()
            .unwrap_or(0);
        let delta = count.checked_sub(previous).expect("fixture counters must not reset");
        if delta > 0 {
            assert_eq!(outcome, "success", "no error or partial walker is expected");
            *actual.entry((drives[&drive].1, bucket)).or_insert(0_u64) += delta;
        }
    }
    assert_eq!(
        actual, expected_walks,
        "each listed source/bucket must have exactly the expected real walks"
    );
    assert_eq!(
        read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("root after scan"),
        root_before,
        "producing a candidate must not replace the coordinator-owned root baseline"
    );
    assert_eq!(dirty_usage_generation(), generation_before);
    assert!(
        dirty_usage_buckets_for_tests() == dirty_before,
        "candidate delivery must not ACK pending dirty buckets"
    );
    usage
}

#[tokio::test]
#[serial]
async fn scoped_entry_fallback_distinguishes_planned_scope_from_real_cold_walks() {
    let (_dir, store) = setup_two_pool_scanner_store().await;
    clear_dirty_usage_buckets_for_tests();
    let hot = format!("hot-{}", Uuid::new_v4().simple());
    let cold = format!("cold-{}", Uuid::new_v4().simple());
    create_bucket(&store, &hot).await;
    create_bucket(&store, &cold).await;
    record_dirty_usage_bucket(&hot);
    let baseline = run_entry(&store, 1, None, true).await;
    persist_baseline(&store, &baseline).await;

    // A same-intent, same-cycle Current cache is a retry, not proof that a
    // later cycle may reuse unselected buckets without durable incarnation.
    run_entry(&store, 1, Some(&hot), false).await;
    let usage = run_entry(&store, 2, Some(&hot), true).await;
    assert_eq!(usage.buckets_usage[&hot].objects_count, 1);
    assert_eq!(usage.buckets_usage[&cold].objects_count, 1);
    assert_eq!(usage.objects_total_count, 2);
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn scoped_entry_fallback_rejects_invalid_persisted_baseline_at_the_walker() {
    let (_dir, store) = setup_two_pool_scanner_store().await;
    clear_dirty_usage_buckets_for_tests();
    let hot = format!("hot-{}", Uuid::new_v4().simple());
    let cold = format!("cold-{}", Uuid::new_v4().simple());
    create_bucket(&store, &hot).await;
    create_bucket(&store, &cold).await;
    record_dirty_usage_bucket(&hot);
    // The first real scan is also the missing persisted-baseline case.
    let baseline = run_entry(&store, 1, None, true).await;
    for (index, kind) in [
        "malformed",
        "unconverged",
        "missing-set",
        "wrong-source",
        "mixed-plan",
        "wrong-epoch",
    ]
    .into_iter()
    .enumerate()
    {
        let mut candidate = baseline.clone();
        candidate.usage_snapshot_converged = Some(true);
        match kind {
            "unconverged" => candidate.usage_snapshot_converged = Some(false),
            "missing-set" => {
                candidate.usage_snapshot_set_states.pop();
            }
            "wrong-source" => candidate.usage_snapshot_set_states[0].set_index = 99,
            "mixed-plan" => candidate.usage_snapshot_set_states[1].scan_plan_digest = Some([0xA5; 32]),
            "wrong-epoch" => candidate.usage_snapshot_set_states[0].scanner_epoch = Some(10),
            "malformed" => {}
            _ => unreachable!(),
        }
        let bytes = if kind == "malformed" {
            b"{broken".to_vec()
        } else {
            serde_json::to_vec(&candidate).expect("candidate JSON")
        };
        crate::save_config(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str(), bytes)
            .await
            .expect("negative baseline should persist");
        let usage = run_entry(&store, u64::try_from(index).expect("fixture cycle index should fit") + 2, None, true).await;
        assert_eq!(usage.objects_total_count, 2, "{kind}");
        assert_eq!(usage.buckets_usage[&cold].objects_count, 1, "{kind}");
    }
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn scoped_entry_fallback_covers_overflow_and_new_bucket_inventory() {
    let (_dir, store) = setup_two_pool_scanner_store().await;
    clear_dirty_usage_buckets_for_tests();
    let hot = format!("hot-{}", Uuid::new_v4().simple());
    create_bucket(&store, &hot).await;
    record_dirty_usage_bucket(&hot);
    let baseline = run_entry(&store, 1, None, true).await;
    persist_baseline(&store, &baseline).await;
    for index in 0..=crate::SCANNER_DIRTY_USAGE_SNAPSHOT_MAX_ENTRIES {
        record_dirty_usage_bucket(&format!("overflow-{index}"));
    }
    assert!(dirty_usage_buckets_for_tests().len() > crate::SCANNER_DIRTY_USAGE_SNAPSHOT_MAX_ENTRIES);
    let usage = run_entry(&store, 2, None, true).await;
    assert_eq!(usage.objects_total_count, 1);

    clear_dirty_usage_buckets_for_tests();
    record_dirty_usage_bucket(&hot);
    let new_bucket = format!("new-{}", Uuid::new_v4().simple());
    create_bucket(&store, &new_bucket).await;
    // Even a previously valid baseline cannot cover the changed inventory.
    let usage = run_entry(&store, 3, None, true).await;
    assert_eq!(usage.objects_total_count, 2);
    assert_eq!(usage.buckets_usage[&new_bucket].objects_count, 1);
    clear_dirty_usage_buckets_for_tests();
}
