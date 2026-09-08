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
use crate::storage_api::EcstoreHealResultItem as HealItem;
use crate::storage_api::scanner_io::BucketInfo;
use rustfs_common::mrf_channel::{
    MrfIngressResult, MrfKind, MrfScope, note_mrf_repaired, take_mrf_repaired_events_for, try_send_mrf_intent_typed,
};
use rustfs_heal::heal::{
    manager::{HealConfig, HealManager},
    mrf_queue::spawn_mrf_consumer,
    storage::{HealListItem, HealObjectInfo, HealStorageAPI},
};
use rustfs_heal_contracts::heal_channel::HealOpts;

#[tokio::test]
async fn mrf_ownership_admission_observation_does_not_postpone_retry() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(u64::MAX, usize::MAX, &mut scanner, temp_dir);
    scanner.new_cache.info.pending_heals.push(pending_heal(
        PendingScannerHealKind::Object,
        "bucket",
        Some("object"),
        None,
        100,
        2,
    ));
    for result in [
        HealAdmissionResult::Accepted,
        HealAdmissionResult::Merged,
        HealAdmissionResult::Full,
        HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull),
        HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped),
    ] {
        scanner.update_pending_scanner_heal_after_admission(
            PendingScannerHealKind::Object,
            "bucket",
            Some("object"),
            None,
            HealScanMode::Deep,
            result,
        );
        let entry = &scanner.new_cache.info.pending_heals[0];
        assert_eq!((entry.last_attempt, entry.attempts), (100, 2));
        assert!(pending_scanner_heal_retry_candidates_at(&scanner.new_cache.info.pending_heals, "bucket", 1899).is_empty());
        assert_eq!(
            pending_scanner_heal_retry_candidates_at(&scanner.new_cache.info.pending_heals, "bucket", 1900).len(),
            1
        );
    }
    scanner.update_pending_scanner_heal_after_admission(
        PendingScannerHealKind::Object,
        "bucket",
        Some("new-object"),
        None,
        HealScanMode::Deep,
        HealAdmissionResult::Accepted,
    );
    assert_eq!(
        scanner.new_cache.info.pending_heals.len(),
        1,
        "successful admission does not create a new ledger"
    );
}

#[test]
fn mrf_ownership_retry_due_boundaries_and_priority_are_bounded() {
    let mut entry = pending_heal(PendingScannerHealKind::Object, "bucket", Some("object"), None, 100, 1);
    entry.last_admission_result = "accepted".to_string();
    assert!(pending_scanner_heal_retry_candidates_at(std::slice::from_ref(&entry), "bucket", 999).is_empty());
    assert_eq!(
        pending_scanner_heal_retry_candidates_at(std::slice::from_ref(&entry), "bucket", 1000).len(),
        1
    );
    assert_eq!(
        build_pending_scanner_heal_request(&entry).expect("request").priority,
        HealChannelPriority::Low
    );
    record_pending_heal_attempt(&mut entry, 1000);
    observe_pending_heal_admission(&mut entry, HealAdmissionResult::Full);
    assert!(pending_scanner_heal_retry_candidates_at(std::slice::from_ref(&entry), "bucket", 2799).is_empty());
    assert_eq!(
        pending_scanner_heal_retry_candidates_at(std::slice::from_ref(&entry), "bucket", 2800).len(),
        1
    );
    assert_eq!(
        build_pending_scanner_heal_request(&entry).expect("request").priority,
        HealChannelPriority::High
    );
    entry.attempts = u32::MAX;
    assert!(pending_scanner_heal_retry_candidates_at(std::slice::from_ref(&entry), "bucket", 22599).is_empty());
    assert_eq!(
        pending_scanner_heal_retry_candidates_at(std::slice::from_ref(&entry), "bucket", 22600).len(),
        1
    );
    entry.last_attempt = u64::MAX;
    assert!(pending_scanner_heal_retry_candidates_at(&[entry], "bucket", 22600).is_empty());
}

#[tokio::test]
async fn mrf_ownership_full_hint_table_has_bounded_multicycle_work_and_sync() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(u64::MAX, usize::MAX, &mut scanner, temp_dir);
    let base = 1_700_000_000;
    scanner.new_cache.info.pending_heals = (0..MAX_PENDING_SCANNER_HEALS_PER_BUCKET)
        .map(|index| {
            let mut entry =
                pending_heal(PendingScannerHealKind::Object, "bucket", Some(&format!("object-{index}")), None, base, 1);
            entry.first_seen = base;
            entry.last_admission_result = "accepted".to_string();
            entry
        })
        .collect();
    let indices: HashMap<String, usize> = scanner
        .new_cache
        .info
        .pending_heals
        .iter()
        .enumerate()
        .map(|(index, entry)| (entry.object.clone().expect("object identity"), index))
        .collect();
    scanner.sync_pending_heals();
    let initial_syncs = scanner.pending_heal_sync_count;
    let mut requests = 0usize;
    let mut nonempty_batches = 0;
    for minute in 0..24 * 60 {
        let now = base + minute * 60;
        let candidates = pending_scanner_heal_retry_candidates_at(&scanner.new_cache.info.pending_heals, "bucket", now);
        assert!(candidates.len() <= MAX_PENDING_SCANNER_HEAL_RETRIES_PER_BUCKET);
        if candidates.is_empty() {
            continue;
        }
        nonempty_batches += 1;
        let before = scanner.pending_heal_sync_count;
        {
            let batch = PendingHealSyncBatch::new(&mut scanner);
            for candidate in candidates {
                assert_eq!(
                    build_pending_scanner_heal_request(&candidate)
                        .expect("retry request")
                        .priority,
                    HealChannelPriority::Low
                );
                let index = indices[candidate.object.as_ref().expect("object identity")];
                record_pending_heal_attempt(&mut batch.scanner.new_cache.info.pending_heals[index], now);
                observe_pending_heal_admission(
                    &mut batch.scanner.new_cache.info.pending_heals[index],
                    HealAdmissionResult::Accepted,
                );
                batch.scanner.sync_pending_heals();
                requests += 1;
            }
        }
        assert_eq!(scanner.pending_heal_sync_count, before + 1, "one table clone per changed retry batch");
        assert_eq!(scanner.new_cache.info.pending_heals.len(), MAX_PENDING_SCANNER_HEALS_PER_BUCKET);
    }
    assert!(requests >= MAX_PENDING_SCANNER_HEALS_PER_BUCKET, "every retained hint receives a retry");
    assert!(
        requests <= 7 * MAX_PENDING_SCANNER_HEALS_PER_BUCKET,
        "15min..6h backoff bounds repeated work within 24h"
    );
    assert!(scanner.new_cache.info.pending_heals.iter().all(|entry| entry.attempts >= 2));
    assert_eq!(scanner.pending_heal_sync_count - initial_syncs, nonempty_batches);
    assert_eq!(scanner.update_cache.info.pending_heals, scanner.new_cache.info.pending_heals);
}

#[tokio::test]
async fn mrf_ownership_cancelled_batch_restores_sync_without_per_item_clones() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(u64::MAX, usize::MAX, &mut scanner, temp_dir);
    scanner
        .new_cache
        .info
        .pending_heals
        .push(pending_heal(PendingScannerHealKind::Object, "bucket", Some("object"), None, 1, 1));
    let before = scanner.pending_heal_sync_count;
    let mut work = Box::pin(async {
        let batch = PendingHealSyncBatch::new(&mut scanner);
        record_pending_heal_attempt(&mut batch.scanner.new_cache.info.pending_heals[0], 100);
        batch.scanner.sync_pending_heals();
        batch.scanner.sync_pending_heals();
        std::future::pending::<()>().await;
    });
    assert!(futures::poll!(&mut work).is_pending());
    drop(work);
    assert!(!scanner.pending_heal_sync_deferred);
    assert!(!scanner.pending_heal_batch_dirty);
    assert_eq!(scanner.pending_heal_sync_count, before + 1);
    assert_eq!(scanner.new_cache.info.pending_heals[0].attempts, 2);
    assert!(pending_scanner_heal_retry_candidates_at(&scanner.new_cache.info.pending_heals, "bucket", 101).is_empty());
    assert_eq!(scanner.update_cache.info.pending_heals, scanner.new_cache.info.pending_heals);
    let result: std::result::Result<(), &'static str> = async {
        let batch = PendingHealSyncBatch::new(&mut scanner);
        record_pending_heal_attempt(&mut batch.scanner.new_cache.info.pending_heals[0], 200);
        observe_pending_heal_admission(&mut batch.scanner.new_cache.info.pending_heals[0], HealAdmissionResult::Merged);
        batch.scanner.sync_pending_heals();
        Err("injected retry batch failure")
    }
    .await;
    assert!(result.is_err());
    assert_eq!(scanner.pending_heal_sync_count, before + 2);
    assert!(!scanner.pending_heal_sync_deferred);
    assert_eq!(scanner.update_cache.info.pending_heals, scanner.new_cache.info.pending_heals);
}

#[derive(Default)]
struct NoticeStorage {
    calls: std::sync::Mutex<HashMap<String, u32>>,
    retry_started: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl HealStorageAPI for NoticeStorage {
    async fn get_object_meta(&self, _: &str, _: &str) -> rustfs_heal::Result<Option<HealObjectInfo>> {
        Ok(None)
    }
    async fn ec_decode_rebuild(&self, _: &str, _: &str) -> rustfs_heal::Result<Vec<u8>> {
        Err(rustfs_heal::Error::other("unused decode fixture"))
    }
    async fn get_bucket_info(&self, bucket: &str) -> rustfs_heal::Result<Option<BucketInfo>> {
        Ok(Some(BucketInfo {
            name: bucket.to_string(),
            ..Default::default()
        }))
    }
    async fn list_buckets(&self) -> rustfs_heal::Result<Vec<BucketInfo>> {
        Ok(Vec::new())
    }
    async fn object_exists(&self, _: &str, _: &str) -> rustfs_heal::Result<bool> {
        Ok(true)
    }
    async fn heal_object(
        &self,
        _: &str,
        object: &str,
        _: Option<&str>,
        _: &HealOpts,
    ) -> rustfs_heal::Result<(HealItem, Option<rustfs_heal::Error>)> {
        let retry = {
            let mut calls = self.calls.lock().expect("fixture calls");
            let count = calls.entry(object.to_string()).or_default();
            *count += 1;
            *count > 1
        };
        if retry {
            self.retry_started.notify_one();
            std::future::pending::<()>().await;
        }
        match object {
            "grace" => Ok((
                HealItem::default(),
                Some(rustfs_heal::Error::Disk(crate::DiskError::other(
                    "dangling object deletion deferred by heal grace window; retry_after_secs=3599; grace_secs=3600",
                ))),
            )),
            "failed" => Err(rustfs_heal::Error::other("permanent fixture failure")),
            "cancelled" => Err(rustfs_heal::Error::TaskCancelled),
            _ => Ok((HealItem::default(), None)),
        }
    }
    async fn heal_bucket(&self, _: &str, _: &HealOpts) -> rustfs_heal::Result<HealItem> {
        Ok(HealItem::default())
    }
    async fn heal_format(&self, _: bool) -> rustfs_heal::Result<(HealItem, Option<rustfs_heal::Error>)> {
        Ok((HealItem::default(), None))
    }
    async fn list_objects_for_heal_page(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: bool,
    ) -> rustfs_heal::Result<(Vec<HealListItem>, Option<String>, bool)> {
        Ok((Vec::new(), None, false))
    }
    async fn get_disk_for_resume(&self, _: &str) -> rustfs_heal::Result<crate::DiskStore> {
        Err(rustfs_heal::Error::other("unused resume fixture"))
    }
}

#[tokio::test]
#[serial]
async fn mrf_ownership_manager_completion_preserves_scanner_pending() {
    const CHILD: &str = "RUSTFS_MRF_OWNERSHIP_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "scanner_folder::tests::mrf_ownership::mrf_ownership_manager_completion_preserves_scanner_pending",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("RUSTFS_HEAL_MRF_ENABLE", "true")
            .output()
            .expect("isolated ingress test process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("1 passed;"),
            "{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    // The production ingress channel is a process singleton; isolation keeps
    // its receiver and lease generations independent from other scanner tests.
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(u64::MAX, usize::MAX, &mut scanner, temp_dir);
    let bucket = format!("mrf-ownership-{}", Uuid::new_v4());
    scanner.new_cache.info.name = bucket.clone();
    scanner.update_cache.info.name = bucket.clone();
    scanner.heal_object_select = 1;
    let storage = Arc::new(NoticeStorage::default());
    let manager = Arc::new(HealManager::new(
        storage.clone(),
        Some(HealConfig {
            enable_auto_heal: false,
            mainline_throttle_enable: false,
            heal_interval: Duration::from_millis(10),
            ..Default::default()
        }),
    ));
    manager.start().await.expect("production manager starts");
    spawn_mrf_consumer(manager.clone());
    for (index, object) in ["grace", "unknown", "failed", "cancelled"].iter().enumerate() {
        let version = Uuid::new_v4();
        scanner.new_cache.info.pending_heals.push(pending_heal(
            PendingScannerHealKind::Object,
            &bucket,
            Some(object),
            Some(&version.to_string()),
            1,
            1,
        ));
        let scope = Some(MrfScope {
            pool_index: 0,
            set_index: 0,
        });
        assert_eq!(
            try_send_mrf_intent_typed(MrfKind::PartialWrite, &bucket, object, Some(version), scope),
            MrfIngressResult::Enqueued
        );
        // Re-admission establishes that the first terminal callback released
        // its ingress lease. Statistics alone precede notice publication.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match try_send_mrf_intent_typed(MrfKind::PartialWrite, &bucket, object, Some(version), scope) {
                    MrfIngressResult::Enqueued => break,
                    MrfIngressResult::Coalesced => tokio::task::yield_now().await,
                    other => panic!("unexpected retry ingress result: {other:?}"),
                }
            }
        })
        .await
        .expect("production terminal releases its ingress lease");
        tokio::time::timeout(Duration::from_secs(5), storage.retry_started.notified())
            .await
            .expect("the real consumer starts the second generation");
        assert!(
            take_mrf_repaired_events_for(&bucket).is_empty(),
            "{object}: task completion must not emit an unproved repair"
        );
        if *object == "unknown" {
            assert_eq!(
                manager.get_statistics().await.total_objects_healed,
                1,
                "legacy healed count is not repair proof"
            );
        }
        assert_eq!(
            try_send_mrf_intent_typed(MrfKind::PartialWrite, &bucket, object, Some(version), scope),
            MrfIngressResult::Coalesced,
            "the in-flight retry retains its new ingress lease"
        );
        note_mrf_repaired(&bucket, object, Some(*version.as_bytes()));
        let syncs_before_retry = scanner.pending_heal_sync_count;
        scanner
            .retry_pending_scanner_heals()
            .await
            .expect("real scanner ledger retry");
        assert_eq!(scanner.pending_heal_sync_count, syncs_before_retry + 1, "the real retry batch syncs once");
        assert_eq!(
            scanner.new_cache.info.pending_heals.len(),
            index + 1,
            "{object}: pending responsibility survives"
        );
        let restored = DataUsageCache::unmarshal(&scanner.new_cache.marshal_msg().expect("serialize pending cache"))
            .expect("decode pending cache");
        assert_eq!(restored.info.pending_heals.len(), index + 1);
        assert_eq!(
            manager
                .cancel_tasks_for_path(&format!("{bucket}/{object}"))
                .await
                .expect("cancel blocked retry"),
            1
        );
    }
    manager.stop().await.expect("production manager stops");
}
