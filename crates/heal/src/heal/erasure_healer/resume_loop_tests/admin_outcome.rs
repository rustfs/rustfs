// Copyright 2024 RustFS Team
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
use crate::heal::outcome::{HealDeferredReason, HealObjectDisposition, HealObjectIdentity, HealObjectKind, HealTaskOutcome};
use crate::heal::task::{HealOptions, HealPriority, HealRequest, HealTask, HealType, MAX_BUCKET_OBJECT_HEAL_RETRIES};
use rustfs_heal_contracts::heal_channel::HealScanMode;

fn request() -> HealRequest {
    let mut request = HealRequest::new(
        HealType::ErasureSet {
            buckets: Vec::new(),
            set_disk_id: "pool_0_set_0".to_string(),
        },
        HealOptions {
            timeout: None,
            scan_mode: HealScanMode::Deep,
            pool_index: Some(0),
            set_index: Some(0),
            recursive: true,
            ..Default::default()
        },
        HealPriority::High,
    );
    request.source = HealRequestSource::Admin;
    request
}

async fn fixture() -> (TempDir, Arc<FakeStorage>) {
    let temp = TempDir::new().expect("administrator fixture");
    let storage = Arc::new(FakeStorage::default());
    *storage.admin_disk.lock().expect("admin disk") = Some(make_disk(&temp).await);
    (temp, storage)
}

fn objects(storage: &FakeStorage, bucket: &str, names: &[&str]) {
    storage.bucket_pages.lock().expect("bucket pages").insert(
        bucket.to_owned(),
        Page {
            items: names.iter().map(|name| item(name, Some("null"), false)).collect(),
            next: None,
            truncated: false,
        },
    );
}

fn assert_partition(outcome: &HealTaskOutcome) {
    let c = &outcome.counters;
    assert_eq!(c.processed, c.healed + c.unchanged + c.skipped + c.failed);
    assert!(c.unknown <= c.skipped);
}

fn admin_object_identity() -> HealObjectIdentity {
    HealObjectIdentity {
        kind: HealObjectKind::Object,
        bucket: "a".to_owned(),
        object: "object".to_owned(),
        version_id: Some("null".to_owned()),
        bucket_incarnation_id: Some(uuid::Uuid::from_u128(42)),
        pool_index: None,
        set_index: None,
    }
}

#[tokio::test(start_paused = true)]
async fn admin_object_rpc_cancelled_retries_require_verified_receipt() {
    for failures in [1, MAX_BUCKET_OBJECT_HEAL_RETRIES] {
        for verified in [true, false] {
            let storage = FakeStorage::default();
            storage.set_outcome("object", Some("null"), HealOutcome::RpcCancelled(failures));
            storage
                .receipts
                .lock()
                .expect("receipt fixture")
                .insert(compose_key("object", Some("null")), verified.then_some(HealObjectDisposition::Repaired));
            let options = HealOpts::default();
            let cancel = CancellationToken::new();
            let identity = admin_object_identity();
            let heal = super::super::admin::heal_object(&storage, &options, identity.clone(), "rpc-retry", &cancel);
            tokio::pin!(heal);
            for attempt in 1..=failures {
                assert!(matches!(futures::poll!(heal.as_mut()), std::task::Poll::Pending));
                assert_eq!(storage.calls().len(), usize::try_from(attempt).expect("attempt fits"));
                tokio::time::advance(HealTask::bucket_object_retry_delay("rpc-retry", attempt)).await;
            }
            let ((_, result), record) = heal.await;
            assert!(matches!(result, Ok(true)));
            assert_eq!(storage.calls().len(), usize::try_from(failures + 1).expect("attempts fit"));
            let (outcome, attempt_failures) = record.expect("completed attempt records its disposition");
            assert_eq!(outcome.identity, identity);
            assert_eq!(attempt_failures, failures);
            assert_eq!(
                outcome.disposition,
                if verified {
                    HealObjectDisposition::Repaired
                } else {
                    HealObjectDisposition::Unknown
                },
                "only a matching positive receipt proves repair after retries"
            );
            assert!(outcome.detail.is_none());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn admin_object_rpc_cancelled_exhausts_bounded_retries_as_deferred() {
    let storage = FakeStorage::default();
    storage.set_outcome("object", Some("null"), HealOutcome::RpcCancelled(MAX_BUCKET_OBJECT_HEAL_RETRIES + 1));
    let options = HealOpts::default();
    let cancel = CancellationToken::new();
    let identity = admin_object_identity();
    let heal = super::super::admin::heal_object(&storage, &options, identity.clone(), "rpc-exhausted", &cancel);
    tokio::pin!(heal);
    for attempt in 1..=MAX_BUCKET_OBJECT_HEAL_RETRIES {
        assert!(matches!(futures::poll!(heal.as_mut()), std::task::Poll::Pending));
        assert_eq!(storage.calls().len(), usize::try_from(attempt).expect("attempt fits"));
        tokio::time::advance(HealTask::bucket_object_retry_delay("rpc-exhausted", attempt)).await;
    }
    let ((_, result), record) = heal.await;
    assert!(matches!(result, Err(Error::TransientSkip { .. })));
    assert_eq!(
        storage.calls().len(),
        usize::try_from(MAX_BUCKET_OBJECT_HEAL_RETRIES + 1).expect("attempts fit")
    );
    let (outcome, attempt_failures) = record.expect("exhausted retry records a deferred outcome");
    assert_eq!(outcome.identity, identity);
    assert_eq!(attempt_failures, MAX_BUCKET_OBJECT_HEAL_RETRIES + 1);
    assert_eq!(
        outcome.disposition,
        HealObjectDisposition::Deferred {
            reason: HealDeferredReason::TransientExistenceCheck,
            retry_not_before: None,
        }
    );
    assert!(outcome.detail.is_some());
}

#[tokio::test(start_paused = true)]
async fn admin_object_rpc_cancelled_backoff_honors_task_cancellation() {
    let storage = FakeStorage::default();
    storage.set_outcome("object", Some("null"), HealOutcome::RpcCancelled(MAX_BUCKET_OBJECT_HEAL_RETRIES + 1));
    let options = HealOpts::default();
    let cancel = CancellationToken::new();
    let started = tokio::time::Instant::now();
    let heal = super::super::admin::heal_object(&storage, &options, admin_object_identity(), "rpc-cancel", &cancel);
    tokio::pin!(heal);
    assert!(matches!(futures::poll!(heal.as_mut()), std::task::Poll::Pending));
    assert_eq!(storage.calls().len(), 1, "the first peer cancellation has reached retry backoff");
    cancel.cancel();
    let ((_, result), record) = heal.await;
    assert!(matches!(result, Err(Error::TaskCancelled)));
    assert!(record.is_none(), "task cancellation must not publish an object disposition");
    assert_eq!(storage.calls().len(), 1, "cancellation must prevent another storage call");
    assert_eq!(tokio::time::Instant::now(), started, "cancellation must not wait for backoff");
}

#[tokio::test(start_paused = true)]
async fn admin_object_task_cancellation_and_timeout_remain_terminal() {
    for timeout in [false, true] {
        let storage = FakeStorage::default();
        storage.set_outcome(
            "object",
            Some("null"),
            if timeout {
                HealOutcome::Timeout
            } else {
                HealOutcome::Cancelled
            },
        );
        let started = tokio::time::Instant::now();
        let ((_, result), record) = super::super::admin::heal_object(
            &storage,
            &HealOpts::default(),
            admin_object_identity(),
            "task-terminal",
            &CancellationToken::new(),
        )
        .await;
        if timeout {
            assert!(matches!(result, Err(Error::TaskTimeout)));
        } else {
            assert!(matches!(result, Err(Error::TaskCancelled)));
        }
        assert!(record.is_none());
        assert_eq!(storage.calls().len(), 1, "terminal task errors must not retry");
        assert_eq!(tokio::time::Instant::now(), started);
    }
}

#[tokio::test]
async fn admin_erasure_two_buckets_have_five_exact_receipts() {
    let (_temp, storage) = fixture().await;
    objects(&storage, "a", &["one", "two"]);
    objects(&storage, "b", &["three", "four", "five"]);
    let task = HealTask::from_request(request(), storage);
    task.execute().await.expect("all-buckets erasure heal");
    let outcome = task.get_outcome().await;
    assert_eq!((outcome.counters.processed, outcome.counters.healed), (5, 5));
    assert_eq!(task.get_result_items().await.len(), 2, "retain legacy bucket prepass results");
    assert_eq!(
        (task.get_progress().await.objects_scanned, task.get_progress().await.objects_healed),
        (5, 5)
    );
    let identities = outcome
        .objects
        .iter()
        .map(|item| {
            assert_eq!(item.disposition, HealObjectDisposition::Repaired);
            assert_eq!(item.identity.version_id.as_deref(), Some("null"));
            assert_eq!(item.identity.bucket_incarnation_id, Some(uuid::Uuid::from_u128(42)));
            assert_eq!((item.identity.pool_index, item.identity.set_index), (Some(0), Some(0)));
            (item.identity.bucket.as_str(), item.identity.object.as_str())
        })
        .collect::<HashSet<_>>();
    assert_eq!(
        identities,
        HashSet::from([("a", "one"), ("a", "two"), ("b", "three"), ("b", "four"), ("b", "five")])
    );
    assert_partition(&outcome);
}

#[tokio::test]
async fn admin_erasure_dry_run_retains_observations_without_positive_receipts() {
    let (_temp, storage) = fixture().await;
    objects(&storage, "a", &["one", "two"]);
    let mut request = request();
    request.options.dry_run = true;
    let task = HealTask::from_request(request.clone(), storage.clone());
    task.execute().await.expect("dry-run observation");
    let outcome = task.get_outcome().await;
    assert_eq!((outcome.counters.processed, outcome.counters.healed, outcome.counters.skipped), (2, 0, 2));
    assert!(outcome.objects.iter().all(|item| {
        item.disposition == HealObjectDisposition::DryRunObserved && item.identity.bucket_incarnation_id.is_none()
    }));
    assert_eq!(task.get_result_items().await.len(), 1, "retain dry-run bucket prepass result");
    assert_partition(&outcome);
    let restarted = HealTask::from_request(request, storage);
    restarted.execute().await.expect("restore dry-run outcome");
    assert_eq!(restarted.get_outcome().await, outcome);
}

#[tokio::test]
async fn admin_erasure_restart_retains_receipts_scope_and_completed_proof() {
    let (_temp, storage) = fixture().await;
    objects(&storage, "a", &["one", "two", "three"]);
    storage.set_outcome("two", Some("null"), HealOutcome::Timeout);
    let request = request();
    let first = HealTask::from_request(request.clone(), storage.clone());
    assert!(matches!(first.execute().await, Err(Error::TaskTimeout)));
    assert_eq!(first.get_outcome().await.counters.healed, 1);
    drop(first);
    storage.set_outcome("two", Some("null"), HealOutcome::Ok);
    objects(&storage, "new-bucket", &["outside-original-scope"]);
    let resumed = HealTask::from_request(request.clone(), storage.clone());
    resumed.execute().await.expect("resume original token");
    let expected = resumed.get_outcome().await;
    assert_eq!((expected.counters.processed, expected.counters.healed), (3, 3));
    assert_eq!(storage.calls().iter().filter(|(name, _)| name == "one").count(), 1);
    assert!(storage.calls().iter().all(|(name, _)| name != "outside-original-scope"));
    let calls = storage.calls();
    drop(resumed);
    let completed = HealTask::from_request(request, storage.clone());
    completed
        .execute()
        .await
        .expect("completed checkpoint survives before terminal publication");
    assert_eq!(storage.calls(), calls);
    assert_eq!(completed.get_outcome().await, expected);
}

#[tokio::test]
async fn admin_erasure_outcome_window_is_bounded_but_counters_survive_restart() {
    let (_temp, storage) = fixture().await;
    let names = (0..257).map(|index| format!("object-{index:03}")).collect::<Vec<_>>();
    objects(&storage, "a", &names.iter().map(String::as_str).collect::<Vec<_>>());
    let request = request();
    let task = HealTask::from_request(request.clone(), storage.clone());
    task.execute().await.expect("large scope");
    let outcome = task.get_outcome().await;
    assert_eq!((outcome.counters.processed, outcome.counters.healed), (257, 257));
    assert_eq!(outcome.objects.len(), 128);
    assert!(outcome.objects_truncated);
    assert_partition(&outcome);
    let restarted = HealTask::from_request(request, storage);
    restarted.execute().await.expect("restore bounded diagnostic window");
    assert_eq!(restarted.get_outcome().await, outcome);
}

#[tokio::test]
async fn admin_erasure_recreated_bucket_cannot_rebind_checkpoint() {
    let (_temp, storage) = fixture().await;
    objects(&storage, "a", &["one", "two"]);
    storage.set_outcome("two", Some("null"), HealOutcome::Timeout);
    let request = request();
    let first = HealTask::from_request(request.clone(), storage.clone());
    assert!(matches!(first.execute().await, Err(Error::TaskTimeout)));
    let calls = storage.calls();
    storage
        .incarnations
        .lock()
        .expect("recreate")
        .insert("a".to_owned(), uuid::Uuid::from_u128(43));
    let restarted = HealTask::from_request(request, storage.clone());
    assert!(matches!(restarted.execute().await, Err(Error::StaleBucketIncarnation { .. })));
    assert_eq!(storage.calls(), calls);
    assert_eq!(restarted.get_outcome().await.counters.healed, 1);
}

#[tokio::test(start_paused = true)]
async fn admin_erasure_dispositions_do_not_invent_success_and_retries_count_once() {
    let (_temp, storage) = fixture().await;
    objects(&storage, "a", &["healthy", "absent", "unproven", "gone", "offline"]);
    storage.receipts.lock().expect("proofs").extend([
        (compose_key("healthy", Some("null")), Some(HealObjectDisposition::VerifiedHealthy)),
        (compose_key("absent", Some("null")), Some(HealObjectDisposition::AuthoritativelyAbsent)),
        (compose_key("unproven", Some("null")), None),
    ]);
    storage.set_outcome("gone", Some("null"), HealOutcome::VersionNotFound);
    storage.set_outcome("offline", Some("null"), HealOutcome::Transient);
    let task = HealTask::from_request(request(), storage.clone());
    task.execute()
        .await
        .expect_err("unhealed objects must retain the failed task status");
    let outcome = task.get_outcome().await;
    let c = &outcome.counters;
    assert_eq!((c.processed, c.healed, c.unchanged, c.failed, c.skipped, c.unknown), (5, 0, 2, 0, 3, 1));
    assert_eq!(c.attempt_failures, 8);
    assert_eq!(storage.calls().iter().filter(|(name, _)| name == "gone").count(), 4);
    assert_eq!(storage.calls().iter().filter(|(name, _)| name == "offline").count(), 4);
    let gone = outcome
        .objects
        .iter()
        .find(|item| item.identity.object == "gone")
        .expect("missing version outcome");
    assert_eq!(
        gone.disposition,
        HealObjectDisposition::Deferred {
            reason: HealDeferredReason::TransientExistenceCheck,
            retry_not_before: None,
        },
        "exhausting retries cannot certify the missing version as absent"
    );
    assert_partition(&outcome);
}

#[tokio::test]
async fn admin_erasure_rejects_every_receipt_identity_mismatch() {
    for field in ["bucket", "object", "version", "incarnation", "pool", "set"] {
        let (_temp, storage) = fixture().await;
        objects(&storage, "a", &["object"]);
        *storage.receipt_mismatch.lock().expect("mismatch") = Some(field);
        let task = HealTask::from_request(request(), storage);
        task.execute().await.expect("unverified scan");
        let outcome = task.get_outcome().await;
        assert_eq!(
            (outcome.counters.processed, outcome.counters.healed, outcome.counters.unknown),
            (1, 0, 1),
            "field={field}"
        );
    }
}

#[tokio::test]
async fn admin_erasure_prepass_failure_retains_acknowledged_outcome() {
    let (_temp, storage) = fixture().await;
    objects(&storage, "a", &["one", "two"]);
    storage.set_outcome("two", Some("null"), HealOutcome::Timeout);
    let request = request();
    let first = HealTask::from_request(request.clone(), storage.clone());
    assert!(matches!(first.execute().await, Err(Error::TaskTimeout)));
    storage.format_failure.store(true, Ordering::SeqCst);
    let restarted = HealTask::from_request(request, storage);
    assert!(restarted.execute().await.is_err());
    assert_eq!(restarted.get_outcome().await.counters.healed, 1);
}

#[tokio::test]
async fn admin_erasure_keeps_null_data_versions_and_delete_markers_distinct() {
    let (_temp, storage) = fixture().await;
    let versions = [
        uuid::Uuid::nil().to_string(),
        uuid::Uuid::from_u128(1).to_string(),
        uuid::Uuid::from_u128(2).to_string(),
    ];
    storage.bucket_pages.lock().expect("versioned page").insert(
        "a".to_owned(),
        Page {
            items: versions
                .iter()
                .enumerate()
                .map(|(index, version)| item("same", Some(version), index == 2))
                .collect(),
            next: None,
            truncated: false,
        },
    );
    storage.receipts.lock().expect("marker proof").insert(
        compose_key("same", Some(&versions[2])),
        Some(HealObjectDisposition::AuthoritativelyAbsent),
    );
    let task = HealTask::from_request(request(), storage);
    task.execute().await.expect("versioned traversal");
    let outcome = task.get_outcome().await;
    assert_eq!(
        (outcome.counters.processed, outcome.counters.healed, outcome.counters.unchanged),
        (3, 2, 1)
    );
    assert_eq!(
        outcome
            .objects
            .iter()
            .filter_map(|item| item.identity.version_id.clone())
            .collect::<HashSet<_>>(),
        HashSet::from(versions)
    );
}

#[tokio::test]
async fn admin_erasure_cutoff_and_lifecycle_skips_are_persisted_as_unresolved() {
    let (_temp, storage) = fixture().await;
    objects(&storage, "a", &["new", "expired"]);
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    storage.bucket_pages.lock().expect("page").get_mut("a").expect("bucket").items[0].mod_time_unix_nanos =
        Some(i128::try_from(future).expect("fixture timestamp") + 120_000_000_000);
    storage.set_lifecycle_expired("expired", Some("null"));
    let request = request();
    let first = HealTask::from_request(request.clone(), storage.clone());
    first.execute().await.expect("explicit skip traversal");
    let outcome = first.get_outcome().await;
    assert_eq!(
        (
            outcome.counters.processed,
            outcome.counters.healed,
            outcome.counters.skipped,
            outcome.counters.unknown
        ),
        (2, 0, 2, 2)
    );
    assert_eq!(
        (
            first.get_progress().await.skipped_new_versions,
            first.get_progress().await.skipped_ilm_expired
        ),
        (1, 1)
    );
    assert!(outcome.objects.iter().all(|item| item.detail.is_some()));
    let restarted = HealTask::from_request(request, storage.clone());
    restarted.execute().await.expect("retain skip responsibility");
    assert_eq!(restarted.get_outcome().await, outcome);
    assert!(storage.calls().is_empty());
}

#[tokio::test]
async fn admin_erasure_checkpoint_publication_cannot_undo_acknowledged_cancel() {
    use crate::heal::outcome::{HealAbortReason, HealExecutionOutcome, HealTraversalCoverage};
    let (_temp, storage) = fixture().await;
    let task = HealTask::from_request(request(), storage);
    let mut committed = HealTaskOutcome::default();
    committed.start();
    committed.counters.processed = 1;
    committed.counters.healed = 1;
    task.cancel()
        .await
        .expect("acknowledge cancellation before checkpoint publication returns");
    task.restore_outcome(committed).await;
    let outcome = task.get_outcome().await;
    assert_eq!(outcome.execution, HealExecutionOutcome::Aborted(HealAbortReason::Cancelled));
    assert_eq!(outcome.coverage, HealTraversalCoverage::Partial);
    assert_eq!((outcome.counters.processed, outcome.counters.healed), (1, 1));
}
