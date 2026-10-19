// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Admission-side behaviour of the per-object disposition cache (backlog#2900).

use super::*;
use crate::heal::manager::disposition::{HealDispositionCache, HealDispositionKey, HealDispositionKind, record_task_disposition};
use crate::heal::storage::HealListItem;
use rustfs_heal_contracts::heal_channel::{HealAdmissionDropReason, HealAdmissionResult};
use std::time::Duration;
use tokio::time::Instant;

const BUCKET: &str = "bucket-a";

fn object_request(object: &str, source: HealRequestSource, priority: HealPriority, version: Option<&str>) -> HealRequest {
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: BUCKET.to_string(),
            object: object.to_string(),
            version_id: version.map(str::to_string),
        },
        HealOptions::default(),
        priority,
    );
    request.source = source;
    request
}

fn key(object: &str, version: Option<&str>) -> HealDispositionKey {
    HealDispositionKey::new(BUCKET, object, version, None, None)
}

fn seed(manager: &HealManager, object: &str, kind: HealDispositionKind, task_id: &str) {
    manager
        .disposition_cache
        .lock()
        .expect("disposition cache")
        .record(key(object, None), kind, task_id, Instant::now());
}

fn cached_kind(manager: &HealManager, object: &str) -> Option<HealDispositionKind> {
    manager
        .disposition_cache
        .lock()
        .expect("disposition cache")
        .get(&key(object, None))
        .map(|entry| entry.kind)
}

#[tokio::test]
async fn disposition_absent_hit_drops_best_effort_object_requests_before_the_queue() {
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
    seed(&manager, "gone", HealDispositionKind::Absent, "absent-task");
    for source in [
        HealRequestSource::ReadRepair,
        HealRequestSource::Scanner,
        HealRequestSource::AutoHeal,
        HealRequestSource::Internal,
        HealRequestSource::Mrf,
    ] {
        for priority in [HealPriority::Low, HealPriority::Normal] {
            let receipt = manager
                .submit_heal_request_with_receipt(object_request("gone", source, priority, None))
                .await
                .expect("admission decides");
            assert_eq!(
                receipt.result,
                HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped),
                "{source:?} {priority:?}"
            );
            assert_eq!(receipt.task_id, "absent-task", "the receipt points at the task that observed absence");
        }
    }
    assert!(manager.heal_queue.lock().await.is_empty(), "a cached absence never reaches the queue");
    // A different version of the same key is a different memory.
    let receipt = manager
        .submit_heal_request_with_receipt(object_request("gone", HealRequestSource::ReadRepair, HealPriority::Low, Some("v1")))
        .await
        .expect("admission decides");
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
}

#[tokio::test]
async fn disposition_scoped_absence_does_not_block_other_scopes() {
    for (pool, set, expected) in [
        (Some(0), Some(0), HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped)),
        (Some(1), Some(0), HealAdmissionResult::Accepted),
        (Some(0), Some(1), HealAdmissionResult::Accepted),
        (Some(0), None, HealAdmissionResult::Accepted),
        (None, Some(0), HealAdmissionResult::Accepted),
        (None, None, HealAdmissionResult::Accepted),
    ] {
        let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
        let mut old_pool = object_request("moved", HealRequestSource::ReadRepair, HealPriority::Low, None);
        old_pool.options.pool_index = Some(0);
        old_pool.options.set_index = Some(0);
        let absent = task_for(old_pool);
        absent.mark_object_absent();
        record_task_disposition(&manager.disposition_cache, &absent, &Ok(()), false).await;

        let mut request = object_request("moved", HealRequestSource::ReadRepair, HealPriority::Low, None);
        request.options.pool_index = pool;
        request.options.set_index = set;
        let receipt = manager
            .submit_heal_request_with_receipt(request)
            .await
            .expect("scoped admission decides");
        assert_eq!(receipt.result, expected, "pool={pool:?}, set={set:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn disposition_unhealthy_admin_object_name_does_not_cache_absence() {
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
    let object = "reports/File not found.txt";
    let task = task_for(object_request(object, HealRequestSource::Admin, HealPriority::Normal, None));
    let failure = Error::TaskExecutionFailed {
        message: format!(
            "Heal left one or more drives unhealthy for {BUCKET}/{object}; retry after the missing drives are restored"
        ),
    };
    record_task_disposition(&manager.disposition_cache, &task, &Err(failure), false).await;
    assert_eq!(cached_kind(&manager, object), Some(HealDispositionKind::Permanent));

    tokio::time::advance(Duration::from_secs(61)).await;
    let receipt = manager
        .submit_heal_request_with_receipt(object_request(object, HealRequestSource::ReadRepair, HealPriority::Low, None))
        .await
        .expect("repair can retry after the failure backoff");
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
}

#[tokio::test]
async fn disposition_cache_is_bypassed_by_admin_forced_high_priority_dry_run_durable_mrf_and_other_kinds() {
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
    for object in ["admin", "forced", "high", "urgent", "dry", "durable", "metadata"] {
        seed(&manager, object, HealDispositionKind::Absent, "absent-task");
    }
    let admin = object_request("admin", HealRequestSource::Admin, HealPriority::Normal, None);
    let mut forced = object_request("forced", HealRequestSource::Scanner, HealPriority::Low, None);
    forced.force_start = true;
    let high = object_request("high", HealRequestSource::Scanner, HealPriority::High, None);
    let urgent = object_request("urgent", HealRequestSource::ReadRepair, HealPriority::Urgent, None);
    let mut dry = object_request("dry", HealRequestSource::Scanner, HealPriority::Low, None);
    dry.options.dry_run = true;
    let mut durable = object_request("durable", HealRequestSource::Mrf, HealPriority::Normal, None);
    durable.expected_mrf_bucket_incarnation_id = Some(Uuid::new_v4());
    let mut metadata = HealRequest::new(
        HealType::Metadata {
            bucket: BUCKET.to_string(),
            object: "metadata".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    metadata.source = HealRequestSource::Scanner;
    for (label, request) in [
        ("admin", admin),
        ("forced", forced),
        ("high", high),
        ("urgent", urgent),
        ("dry", dry),
        ("durable", durable),
        ("metadata", metadata),
    ] {
        let receipt = manager
            .submit_heal_request_with_receipt(request)
            .await
            .expect("admission decides");
        assert_eq!(receipt.result, HealAdmissionResult::Accepted, "{label}");
    }
    assert_eq!(manager.heal_queue.lock().await.len(), 7);
}

#[tokio::test(start_paused = true)]
async fn disposition_backed_off_failure_is_dropped_then_admitted_after_the_backoff() {
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
    seed(&manager, "flaky", HealDispositionKind::RetryExhausted, "failed-task");
    let dropped = manager
        .submit_heal_request_with_receipt(object_request("flaky", HealRequestSource::Scanner, HealPriority::Low, None))
        .await
        .expect("admission decides");
    assert_eq!(dropped.result, HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped));
    assert_eq!(dropped.task_id, "failed-task");
    tokio::time::advance(Duration::from_secs(61)).await;
    let admitted = manager
        .submit_heal_request_with_receipt(object_request("flaky", HealRequestSource::Scanner, HealPriority::Low, None))
        .await
        .expect("admission decides");
    assert_eq!(admitted.result, HealAdmissionResult::Accepted, "the backoff elapsed");
    assert_eq!(
        cached_kind(&manager, "flaky"),
        Some(HealDispositionKind::RetryExhausted),
        "admission does not forget the failure"
    );
}

fn task_for(request: HealRequest) -> HealTask {
    HealTask::from_request(request, Arc::new(MockStorage))
}

#[tokio::test]
async fn disposition_record_task_result_maps_terminal_outcomes() {
    let cache = StdMutex::new(HealDispositionCache::with_limits(16, Duration::from_secs(600)));
    let kind = |object: &str| cache.lock().expect("cache").get(&key(object, None)).map(|entry| entry.kind);

    let absent = task_for(object_request("absent", HealRequestSource::ReadRepair, HealPriority::Low, None));
    absent.mark_object_absent();
    record_task_disposition(&cache, &absent, &Ok(()), false).await;
    assert_eq!(kind("absent"), Some(HealDispositionKind::Absent));

    let retried = task_for(object_request("retried", HealRequestSource::Scanner, HealPriority::Low, None));
    let quorum = || Error::Storage(EcstoreError::InsufficientReadQuorum(BUCKET.to_string(), "retried".to_string()));
    record_task_disposition(&cache, &retried, &Err(quorum()), true).await;
    assert_eq!(kind("retried"), None, "a scheduled retry is not terminal");
    record_task_disposition(&cache, &retried, &Err(quorum()), false).await;
    assert_eq!(kind("retried"), Some(HealDispositionKind::RetryExhausted));

    let denied = task_for(object_request("denied", HealRequestSource::AutoHeal, HealPriority::Normal, None));
    record_task_disposition(&cache, &denied, &Err(Error::Disk(DiskError::FileAccessDenied)), false).await;
    assert_eq!(kind("denied"), Some(HealDispositionKind::Permanent));

    let cancelled = task_for(object_request("cancelled", HealRequestSource::Scanner, HealPriority::Low, None));
    record_task_disposition(&cache, &cancelled, &Err(Error::TaskCancelled), false).await;
    record_task_disposition(&cache, &cancelled, &Err(Error::TaskTimeout), false).await;
    assert_eq!(kind("cancelled"), None);

    // A positive completion forgets an earlier failure.
    record_task_disposition(&cache, &denied, &Ok(()), false).await;
    assert_eq!(kind("denied"), None);

    // Durable MRF, dry runs and other task kinds never write the memory.
    let mut durable_request = object_request("durable", HealRequestSource::Mrf, HealPriority::Normal, None);
    durable_request.expected_mrf_bucket_incarnation_id = Some(Uuid::new_v4());
    let durable = task_for(durable_request);
    durable.mark_object_absent();
    record_task_disposition(&cache, &durable, &Ok(()), false).await;
    assert_eq!(kind("durable"), None);
    let mut dry_request = object_request("dry", HealRequestSource::Scanner, HealPriority::Low, None);
    dry_request.options.dry_run = true;
    let dry = task_for(dry_request);
    dry.mark_object_absent();
    record_task_disposition(&cache, &dry, &Ok(()), false).await;
    assert_eq!(kind("dry"), None);
    let mut metadata = HealRequest::new(
        HealType::Metadata {
            bucket: BUCKET.to_string(),
            object: "metadata".to_string(),
        },
        HealOptions::default(),
        HealPriority::Low,
    );
    metadata.source = HealRequestSource::Scanner;
    let metadata = task_for(metadata);
    record_task_disposition(&cache, &metadata, &Err(Error::Disk(DiskError::FileAccessDenied)), false).await;
    assert_eq!(kind("metadata"), None);
}

struct AbsentStorage;

#[async_trait::async_trait]
impl HealStorageAPI for AbsentStorage {
    async fn admit_bucket_incarnation(&self, _: &str) -> Result<Uuid> {
        Ok(Uuid::from_u128(7))
    }
    async fn heal_bucket_at_incarnation(&self, bucket: &str, expected: Uuid, opts: &HealOpts) -> Result<HealResultItem> {
        self.validate_bucket_incarnation(bucket, Some(expected)).await?;
        self.heal_bucket(bucket, opts).await
    }
    async fn heal_object_at_incarnation(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
        expected: Uuid,
        opts: &HealOpts,
    ) -> Result<crate::heal::storage::HealStorageObjectResult> {
        self.validate_bucket_incarnation(bucket, Some(expected)).await?;
        self.heal_object_with_receipt(bucket, object, version_id, opts).await
    }
    async fn get_object_meta(&self, _: &str, _: &str) -> Result<Option<HealObjectInfo>> {
        Ok(None)
    }
    async fn ec_decode_rebuild(&self, _: &str, _: &str) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }
    async fn get_bucket_info(&self, _: &str) -> Result<Option<BucketInfo>> {
        Ok(None)
    }
    async fn list_buckets_for_admin_heal(
        &self,
        _selector: Option<crate::heal::storage_api::storage::DiskSetSelector>,
    ) -> Result<Vec<BucketInfo>> {
        Ok(Vec::new())
    }
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        Ok(Vec::new())
    }
    async fn object_exists(&self, _: &str, _: &str) -> Result<bool> {
        Ok(false)
    }
    async fn heal_bucket(&self, _: &str, _: &HealOpts) -> Result<HealResultItem> {
        Ok(HealResultItem::default())
    }
    async fn heal_format(&self, _: bool) -> Result<(HealResultItem, Option<Error>)> {
        Ok((HealResultItem::default(), None))
    }
    async fn get_disk_for_resume(&self, _: &str) -> Result<DiskStore> {
        Err(Error::other("no resume disk in this fixture"))
    }
    async fn list_objects_for_heal_page(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: bool,
    ) -> Result<(Vec<HealListItem>, Option<String>, bool)> {
        Ok((Vec::new(), None, false))
    }
    async fn heal_object(&self, _: &str, _: &str, _: Option<&str>, _: &HealOpts) -> Result<(HealResultItem, Option<Error>)> {
        // The locked quorum read found no replica on any disk.
        Ok((HealResultItem::default(), Some(Error::Storage(EcstoreError::FileNotFound))))
    }
}

async fn wait_until_idle(manager: &HealManager) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !manager.active_heals.lock().await.is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the absent object heal completes");
}

#[tokio::test]
async fn disposition_read_repair_absent_object_is_remembered_after_one_task() {
    let manager = HealManager::new_without_root_recovery_for_test(
        Arc::new(AbsentStorage),
        Some(HealConfig {
            max_concurrent_heals: 1,
            ..HealConfig::default()
        }),
    );
    let first = object_request("deleted", HealRequestSource::ReadRepair, HealPriority::Low, None);
    let first_id = first.id.clone();
    let receipt = manager
        .submit_heal_request_with_receipt(first)
        .await
        .expect("admission decides");
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    process_manager_queue_once(&manager).await;
    wait_until_idle(&manager).await;
    assert_eq!(
        manager
            .get_task_status(&first_id)
            .await
            .expect("completed status is retained"),
        HealTaskStatus::Completed,
        "an absent object is a completed heal, not a failed one"
    );
    assert_eq!(cached_kind(&manager, "deleted"), Some(HealDispositionKind::Absent));

    // The producer's reservation expired and it resubmits: nothing is queued.
    let second = manager
        .submit_heal_request_with_receipt(object_request("deleted", HealRequestSource::ReadRepair, HealPriority::Low, None))
        .await
        .expect("admission decides");
    assert_eq!(second.result, HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped));
    assert_eq!(second.task_id, first_id);
    assert!(manager.heal_queue.lock().await.is_empty());

    // A durable MRF responsibility for the same key still reaches the queue.
    let mut durable = object_request("deleted", HealRequestSource::Mrf, HealPriority::Normal, None);
    durable.expected_mrf_bucket_incarnation_id = Some(Uuid::from_u128(7));
    let durable = manager
        .submit_heal_request_with_receipt(durable)
        .await
        .expect("admission decides");
    assert_eq!(durable.result, HealAdmissionResult::Accepted);
}
