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

fn bucket_task(storage: Arc<MockStorage>) -> HealTask {
    HealTask::from_request(
        HealRequest::new(
            HealType::Bucket {
                bucket: "bucket-a".to_string(),
            },
            HealOptions {
                recursive: true,
                timeout: None,
                ..Default::default()
            },
            HealPriority::Normal,
        ),
        storage,
    )
}

fn pages_storage(pages: &[&[&str]]) -> MockStorage {
    MockStorage {
        retry_test_pages: Some(
            pages
                .iter()
                .map(|page| page.iter().map(|name| heal_item(name)).collect())
                .collect(),
        ),
        ..Default::default()
    }
}

fn fail_once(storage: &MockStorage, name: &str) {
    storage
        .heal_object_outcomes
        .lock()
        .expect("outcomes")
        .insert(name.to_string(), VecDeque::from([MockHealObjectOutcome::RetryableLock]));
}

#[tokio::test(start_paused = true)]
async fn slow_listing_does_not_starve_due_object_retry() {
    let storage = Arc::new(MockStorage {
        recoverable_second_page_failures: Mutex::new(Some(1)),
        retry_test_listing_delays: Mutex::new(VecDeque::from([Duration::ZERO, Duration::from_secs(31)])),
        ..Default::default()
    });
    fail_once(&storage, "object-a");
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(10), &mut execution).await.is_err());
    assert_eq!(
        storage.retry_test_events.lock().expect("events").as_slice(),
        ["list:first", "heal:object-a", "list:second", "heal:object-a"],
        "the object retry must run while the slow listing remains in flight"
    );
    assert_eq!(task.get_outcome().await.counters.failed, 0);
    execution.await.expect("the resumed listing and retried object complete");
    assert_eq!(
        storage.retry_test_events.lock().expect("events").as_slice(),
        [
            "list:first",
            "heal:object-a",
            "list:second",
            "heal:object-a",
            "list:second",
            "heal:object-b"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn slow_set_disk_walk_does_not_starve_due_object_retry() {
    let storage = Arc::new(MockStorage {
        erasure_set_scopes: Mutex::new(vec![(0, 0)]),
        retry_test_disk_walk_pages: true,
        retry_test_pages: Some(vec![vec![heal_item("a")], vec![heal_item("b")]]),
        retry_test_listing_delays: Mutex::new(VecDeque::from([Duration::ZERO, Duration::from_secs(31)])),
        ..Default::default()
    });
    fail_once(&storage, "a");
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(10), &mut execution).await.is_err());
    assert_eq!(
        storage.retry_test_events.lock().expect("events").as_slice(),
        ["list:first", "heal:a", "list:1", "heal:a"]
    );
    assert_eq!(storage.disk_walk_calls.lock().expect("walks").len(), 2);
    execution.await.expect("the same disk walk resumes and both objects heal");
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["a", "a", "b"]);
    assert_eq!(storage.disk_walk_calls.lock().expect("walks").len(), 2);
    assert_eq!(task.get_outcome().await.counters.failed, 0);
}

#[tokio::test(start_paused = true)]
async fn cancelling_an_inflight_listing_keeps_deferred_outcome_cancelled() {
    let incarnation = Uuid::new_v4();
    let storage = Arc::new(MockStorage {
        recoverable_second_page_failures: Mutex::new(Some(1)),
        retry_test_listing_delays: Mutex::new(VecDeque::from([Duration::ZERO, Duration::from_secs(31)])),
        bucket_incarnation_id: Mutex::new(Some(incarnation)),
        ..Default::default()
    });
    fail_once(&storage, "object-a");
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(1), &mut execution).await.is_err());
    assert_eq!(
        storage.retry_test_events.lock().expect("events").as_slice(),
        ["list:first", "heal:object-a", "list:second"]
    );
    task.cancel().await.expect("cancel");
    assert!(matches!(execution.await, Err(Error::TaskCancelled)));
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["object-a"]);
    let outcome = task.get_outcome().await;
    assert_eq!(outcome.objects.len(), 1);
    assert_eq!(outcome.objects[0].disposition, HealObjectDisposition::Cancelled);
    assert_eq!(outcome.objects[0].identity.bucket_incarnation_id, Some(incarnation));
}

#[tokio::test(start_paused = true)]
async fn slow_listing_services_multiple_retries_without_restarting_walk() {
    let storage = Arc::new(MockStorage {
        recoverable_second_page_failures: Mutex::new(Some(1)),
        retry_test_listing_delays: Mutex::new(VecDeque::from([Duration::ZERO, Duration::from_secs(29)])),
        ..Default::default()
    });
    storage.heal_object_outcomes.lock().expect("outcomes").insert(
        "object-a".to_string(),
        VecDeque::from([
            MockHealObjectOutcome::RetryableSlowDown,
            MockHealObjectOutcome::RetryableSlowDown,
        ]),
    );
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(10), &mut execution).await.is_err());
    assert_eq!(
        storage.retry_test_events.lock().expect("events").as_slice(),
        ["list:first", "heal:object-a", "list:second", "heal:object-a", "heal:object-a"]
    );
    let outcome = task.get_outcome().await;
    assert_eq!(
        (outcome.counters.processed, outcome.counters.failed, outcome.counters.attempt_failures),
        (1, 0, 2)
    );
    execution.await.expect("object and resumed listing complete");
    assert_eq!(
        storage.retry_test_events.lock().expect("events").as_slice(),
        [
            "list:first",
            "heal:object-a",
            "list:second",
            "heal:object-a",
            "heal:object-a",
            "list:second",
            "heal:object-b"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn slow_listing_preserves_retry_limit_and_failure() {
    let storage = Arc::new(MockStorage {
        recoverable_second_page_failures: Mutex::new(Some(1)),
        retry_test_listing_delays: Mutex::new(VecDeque::from([Duration::ZERO, Duration::from_secs(31)])),
        ..Default::default()
    });
    storage
        .heal_object_outcomes
        .lock()
        .expect("outcomes")
        .insert("object-a".to_string(), (0..5).map(|_| MockHealObjectOutcome::RetryableSlowDown).collect());
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(
        tokio::time::timeout(Duration::from_millis(31_500), &mut execution)
            .await
            .is_err()
    );
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["object-a"; 5]);
    let outcome = task.get_outcome().await;
    assert_eq!(
        (outcome.counters.processed, outcome.counters.failed, outcome.counters.attempt_failures),
        (1, 1, 6) // Five object failures and one listing failure.
    );
    assert_eq!(
        outcome.objects[0].disposition,
        HealObjectDisposition::Failed(HealFailureClass::RetryExhausted)
    );
    execution.await.expect_err("exhausted object must remain a batch failure");
    assert_eq!(
        storage.heal_object_calls.lock().expect("calls").last().map(String::as_str),
        Some("object-b")
    );
}

#[tokio::test(start_paused = true)]
async fn full_window_abort_accounts_inline_once_and_leaves_unstarted_tail_unprocessed() {
    for cancel in [true, false] {
        let names: Vec<String> = (0..258).map(|index| format!("blocked-{index}")).collect();
        let mut page: Vec<HealListItem> = names.iter().map(|name| heal_item(name)).collect();
        page[256].version_id = Some("inline-version".to_string());
        let storage = Arc::new(MockStorage {
            retry_test_pages: Some(vec![page, vec![heal_item("healthy")]]),
            ..Default::default()
        });
        for name in &names {
            fail_once(&storage, name);
        }
        let mut task = bucket_task(storage.clone());
        if !cancel {
            task.options.timeout = Some(Duration::from_secs(1));
        }
        let execution = task.execute();
        tokio::pin!(execution);
        assert!(
            tokio::time::timeout(Duration::from_millis(500), &mut execution)
                .await
                .is_err()
        );
        assert_eq!(storage.heal_object_calls.lock().expect("calls").len(), 257);
        assert_eq!(storage.listing_tokens.lock().expect("tokens").len(), 1);
        if cancel {
            task.cancel().await.expect("cancel");
        }
        let result = execution.await;
        assert!(matches!(
            (&result, cancel),
            (Err(Error::TaskCancelled), true) | (Err(Error::TaskTimeout), false)
        ));
        let outcome = task.get_outcome().await;
        assert_eq!(
            (
                outcome.counters.processed,
                outcome.counters.skipped,
                outcome.counters.failed,
                outcome.counters.healed
            ),
            (257, 257, 0, 0)
        );
        assert_eq!(outcome.coverage, crate::heal::outcome::HealTraversalCoverage::Partial);
        assert_eq!(
            outcome.execution,
            crate::heal::outcome::HealExecutionOutcome::Aborted(if cancel {
                HealAbortReason::Cancelled
            } else {
                HealAbortReason::Deadline
            })
        );
        let inline: Vec<_> = outcome
            .objects
            .iter()
            .filter(|item| item.identity.object == "blocked-256")
            .collect();
        assert_eq!(inline.len(), 1);
        assert_eq!(inline[0].identity.version_id.as_deref(), Some("inline-version"));
        assert_eq!(
            inline[0].disposition,
            if cancel {
                HealObjectDisposition::Cancelled
            } else {
                HealObjectDisposition::Deferred {
                    reason: HealDeferredReason::Deadline,
                    retry_not_before: None,
                }
            }
        );
        let progress = task.get_progress().await;
        assert_eq!(
            (
                progress.objects_scanned,
                progress.skipped_objects,
                progress.objects_failed,
                progress.objects_healed
            ),
            (257, 257, 0, 0)
        );
        assert!(
            !outcome
                .objects
                .iter()
                .any(|item| item.identity.object == "blocked-257" || item.identity.object == "healthy")
        );
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(storage.heal_object_calls.lock().expect("calls").len(), 257);
        assert_eq!(storage.listing_tokens.lock().expect("tokens").len(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn slowdown_can_recover_within_existing_deferred_age_budget() {
    let storage = Arc::new(pages_storage(&[&["a"]]));
    storage
        .heal_object_outcomes
        .lock()
        .expect("outcomes")
        .insert("a".to_string(), (0..4).map(|_| MockHealObjectOutcome::RetryableSlowDown).collect());
    let task = bucket_task(storage.clone());
    let started = tokio::time::Instant::now();
    task.execute()
        .await
        .expect("transient slowdown recovers within the deferred age budget");
    assert!(started.elapsed() < Duration::from_secs(30));
    assert_eq!(storage.heal_object_calls.lock().expect("calls").len(), 5);
    let outcome = task.get_outcome().await;
    assert_eq!(
        (outcome.counters.processed, outcome.counters.failed, outcome.counters.attempt_failures),
        (1, 0, 4)
    );
    assert_eq!(outcome.objects.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn repeated_slowdown_keeps_attempts_and_forward_pages_bounded() {
    let storage = Arc::new(pages_storage(&[&["a"], &["b"], &["c"], &["d"]]));
    for name in ["a", "b", "c", "d"] {
        storage
            .heal_object_outcomes
            .lock()
            .expect("outcomes")
            .insert(name.to_string(), (0..5).map(|_| MockHealObjectOutcome::RetryableSlowDown).collect());
    }
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(1), &mut execution).await.is_err());
    assert_eq!(storage.listing_tokens.lock().expect("tokens").len(), 3);
    execution.await.expect_err("all four objects exhaust retries");
    let outcome = task.get_outcome().await;
    assert_eq!(
        (outcome.counters.processed, outcome.counters.failed, outcome.counters.attempt_failures),
        (4, 4, 20)
    );
    assert_eq!(storage.heal_object_calls.lock().expect("calls").len(), 20);
    for name in ["a", "b", "c", "d"] {
        assert_eq!(outcome.objects.iter().filter(|item| item.identity.object == name).count(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn listing_retry_keeps_cursor_and_does_not_replay_successful_objects() {
    let storage = Arc::new(MockStorage {
        recoverable_second_page_failures: Mutex::new(Some(1)),
        ..Default::default()
    });
    fail_once(&storage, "object-a");
    let task = bucket_task(storage.clone());
    task.execute().await.expect("both retries complete");
    assert_eq!(
        storage.listing_tokens.lock().expect("tokens").as_slice(),
        [None, Some("second".to_string()), Some("second".to_string())]
    );
    assert_eq!(
        storage.heal_object_calls.lock().expect("calls").as_slice(),
        ["object-a", "object-a", "object-b"]
    );
    let outcome = task.get_outcome().await;
    assert_eq!((outcome.counters.processed, outcome.counters.attempt_failures), (2, 2));
}

#[tokio::test(start_paused = true)]
async fn typed_lock_contention_allows_only_two_forward_pages() {
    let storage = Arc::new(pages_storage(&[&["a"], &["b"], &["c"], &["d"]]));
    fail_once(&storage, "a");
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(1), &mut execution).await.is_err());
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["a", "b", "c"]);
    assert_eq!(storage.listing_tokens.lock().expect("tokens").len(), 3);
    execution.await.expect("all objects complete");
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["a", "b", "c", "a", "d"]);
    assert_eq!(task.get_outcome().await.counters.processed, 4);
}

#[tokio::test(start_paused = true)]
async fn due_retry_runs_before_next_object_in_a_slow_healthy_page() {
    let mut storage = pages_storage(&[&["a"], &["b", "c"]]);
    storage.retry_test_delays.insert("b".to_string(), Duration::from_secs(3));
    fail_once(&storage, "a");
    let storage = Arc::new(storage);
    bucket_task(storage.clone()).execute().await.expect("all objects complete");
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["a", "b", "a", "c"]);
}

#[tokio::test(start_paused = true)]
async fn slow_unrelated_object_does_not_starve_due_retry() {
    let mut storage = pages_storage(&[&["a", "b"]]);
    storage.retry_test_delays.insert("b".to_string(), Duration::from_secs(31));
    fail_once(&storage, "a");
    let storage = Arc::new(storage);
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(10), &mut execution).await.is_err());
    assert_eq!(
        storage.heal_object_calls.lock().expect("calls").as_slice(),
        ["a", "b", "a"],
        "a due retry must run while an unrelated object is still healing"
    );
    execution.await.expect("both objects complete");
    let outcome = task.get_outcome().await;
    assert_eq!((outcome.counters.processed, outcome.counters.failed), (2, 0));
}

#[tokio::test(start_paused = true)]
async fn cancellation_drains_slow_replacement_object_after_a_concurrent_retry() {
    let mut storage = pages_storage(&[&["a", "b"]]);
    storage.retry_test_delays.insert("b".to_string(), Duration::from_secs(31));
    storage
        .retry_test_attempt_delays
        .lock()
        .expect("attempt delays")
        .insert("a".to_string(), VecDeque::from([Duration::ZERO, Duration::from_secs(31)]));
    fail_once(&storage, "a");
    let storage = Arc::new(storage);
    let task = bucket_task(storage.clone());
    let owner = crate::heal::storage::ReplacementExecution::for_test(Vec::new(), Vec::new());
    *task.replacement_execution.write().await = Some(owner);
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(10), &mut execution).await.is_err());
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["a", "b", "a"]);
    assert_eq!(storage.heal_object_version_ids.lock().expect("completed calls").len(), 1);
    task.cancel_token.cancel();
    assert!(tokio::time::timeout(Duration::from_secs(1), &mut execution).await.is_err());
    assert!(matches!(execution.await, Err(Error::TaskCancelled)));
    assert!(task.replacement_execution.read().await.is_none());
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["a", "b", "a"]);
    assert_eq!(storage.heal_object_version_ids.lock().expect("completed calls").len(), 3);
}

#[tokio::test(start_paused = true)]
async fn stale_bucket_retry_drains_another_inflight_replacement() {
    let original = Uuid::new_v4();
    let mut storage = pages_storage(&[&["a", "b"]]);
    *storage.bucket_incarnation_id.lock().expect("incarnation") = Some(original);
    storage.retry_test_delays.insert("b".to_string(), Duration::from_secs(31));
    fail_once(&storage, "a");
    let storage = Arc::new(storage);
    let mut task = bucket_task(storage.clone());
    task.bucket_incarnation_id = Some(original);
    let owner = crate::heal::storage::ReplacementExecution::for_test(Vec::new(), Vec::new());
    *task.replacement_execution.write().await = Some(owner);
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(1), &mut execution).await.is_err());
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["a", "b"]);
    *storage.bucket_incarnation_id.lock().expect("incarnation") = Some(Uuid::new_v4());
    assert!(tokio::time::timeout(Duration::from_secs(10), &mut execution).await.is_err());
    let result = execution.await;
    assert!(
        matches!(result, Err(Error::StaleBucketIncarnation { .. })),
        "unexpected result: {result:?}"
    );
    assert_eq!(storage.heal_object_version_ids.lock().expect("completed calls").len(), 2);
    assert!(task.replacement_execution.read().await.is_none());
    let outcome = task.get_outcome().await;
    let stale = outcome
        .objects
        .iter()
        .find(|item| item.identity.object == "a")
        .expect("stale retry outcome");
    assert_eq!(stale.identity.bucket_incarnation_id, Some(original));
    assert_eq!(stale.disposition, HealObjectDisposition::Unknown);
    let other = outcome
        .objects
        .iter()
        .find(|item| item.identity.object == "b")
        .expect("b outcome");
    assert_eq!(other.identity.bucket_incarnation_id, Some(original));
    assert_eq!(other.disposition, HealObjectDisposition::Unknown);
}

#[tokio::test(start_paused = true)]
async fn stale_retry_preserves_verified_receipt_from_another_inflight_replacement() {
    let original = Uuid::new_v4();
    let mut storage = pages_storage(&[&["a", "b"]]);
    *storage.bucket_incarnation_id.lock().expect("incarnation") = Some(original);
    storage.retry_test_delays.insert("b".to_string(), Duration::from_secs(31));
    storage.heal_object_receipts.lock().expect("receipts").insert(
        "b".to_string(),
        VecDeque::from([object_receipt("b", None, HealObjectDisposition::Repaired, original)]),
    );
    fail_once(&storage, "a");
    let storage = Arc::new(storage);
    let mut task = bucket_task(storage.clone());
    task.bucket_incarnation_id = Some(original);
    *task.replacement_execution.write().await =
        Some(crate::heal::storage::ReplacementExecution::for_test(Vec::new(), Vec::new()));
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(1), &mut execution).await.is_err());
    *storage.bucket_incarnation_id.lock().expect("incarnation") = Some(Uuid::new_v4());
    assert!(matches!(execution.await, Err(Error::StaleBucketIncarnation { .. })));
    let outcome = task.get_outcome().await;
    let repaired = outcome
        .objects
        .iter()
        .find(|item| item.identity.object == "b")
        .expect("b outcome");
    assert_eq!(repaired.identity.bucket_incarnation_id, Some(original));
    assert_eq!(repaired.disposition, HealObjectDisposition::Repaired);
}

#[tokio::test(start_paused = true)]
async fn stale_retry_does_not_start_other_queued_retries_while_draining() {
    let original = Uuid::new_v4();
    let mut storage = pages_storage(&[&["a", "c", "b"]]);
    *storage.bucket_incarnation_id.lock().expect("incarnation") = Some(original);
    storage.retry_test_delays.insert("b".to_string(), Duration::from_secs(31));
    fail_once(&storage, "a");
    fail_once(&storage, "c");
    let storage = Arc::new(storage);
    let mut task = bucket_task(storage.clone());
    task.bucket_incarnation_id = Some(original);
    *task.replacement_execution.write().await =
        Some(crate::heal::storage::ReplacementExecution::for_test(Vec::new(), Vec::new()));
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(1), &mut execution).await.is_err());
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["a", "c", "b"]);
    let before = storage.bucket_incarnation_reads.load(Ordering::Relaxed);
    *storage.bucket_incarnation_id.lock().expect("incarnation") = Some(Uuid::new_v4());
    assert!(matches!(execution.await, Err(Error::StaleBucketIncarnation { .. })));
    assert_eq!(storage.bucket_incarnation_reads.load(Ordering::Relaxed), before + 1);
}

#[tokio::test(start_paused = true)]
async fn slow_retries_keep_inflight_work_bounded() {
    let names: Vec<String> = (0..10).map(|index| format!("retry-{index}")).collect();
    let mut page: Vec<_> = names.iter().map(|name| heal_item(name)).collect();
    page.push(heal_item("slow"));
    let mut storage = MockStorage {
        retry_test_pages: Some(vec![page]),
        ..Default::default()
    };
    storage.retry_test_delays.insert("slow".to_string(), Duration::from_secs(31));
    for name in &names {
        fail_once(&storage, name);
        storage
            .retry_test_attempt_delays
            .lock()
            .expect("attempt delays")
            .insert(name.clone(), VecDeque::from([Duration::ZERO, Duration::from_secs(31)]));
    }
    let storage = Arc::new(storage);
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(10), &mut execution).await.is_err());
    assert_eq!(storage.heal_object_calls.lock().expect("calls").len(), 18);
    assert_eq!(storage.heal_object_version_ids.lock().expect("completed calls").len(), 10);
    task.cancel_token.cancel();
    assert!(matches!(execution.await, Err(Error::TaskCancelled)));
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(storage.heal_object_calls.lock().expect("calls").len(), 18);
}

#[tokio::test(start_paused = true)]
async fn inline_retry_is_not_starved_by_slow_deferred_retries() {
    let names: Vec<_> = (0..256).map(|index| format!("slow-{index:03}")).collect();
    let mut page: Vec<_> = names.iter().map(|name| heal_item(name)).collect();
    page.push(heal_item("inline"));
    let storage = MockStorage {
        retry_test_pages: Some(vec![page]),
        ..Default::default()
    };
    for name in &names {
        fail_once(&storage, name);
        storage
            .retry_test_attempt_delays
            .lock()
            .expect("delays")
            .insert(name.clone(), VecDeque::from([Duration::ZERO, Duration::from_secs(31)]));
    }
    fail_once(&storage, "inline");
    let storage = Arc::new(storage);
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(10), &mut execution).await.is_err());
    assert_eq!(
        storage
            .heal_object_calls
            .lock()
            .expect("calls")
            .iter()
            .filter(|name| name.as_str() == "inline")
            .count(),
        2,
        "the overflow identity must be retried before slow work occupies every slot"
    );
    task.cancel_token.cancel();
    assert!(matches!(execution.await, Err(Error::TaskCancelled)));
}

#[tokio::test(start_paused = true)]
async fn concurrent_retry_overflow_preserves_every_attempted_identity() {
    assert_concurrent_retry_overflow_preserves_identities(true).await;
}

#[tokio::test(start_paused = true)]
async fn concurrent_retry_overflow_completes_every_attempted_identity() {
    assert_concurrent_retry_overflow_preserves_identities(false).await;
}

async fn assert_concurrent_retry_overflow_preserves_identities(cancel: bool) {
    let budget = super::super::heal_bucket::deferred_payload_budget_for_test();
    let mut lengths = vec![100; 7];
    let mut remaining = budget - 300 - 700;
    while remaining > 1_000 {
        lengths.push(1_000);
        remaining -= 1_000;
    }
    if remaining > 0 && remaining < 4 {
        *lengths.last_mut().expect("previous key") -= 4 - remaining;
        remaining = 4;
    }
    if remaining != 0 {
        lengths.push(remaining);
    }
    assert!(lengths.len() < 256);
    let names: Vec<_> = lengths
        .into_iter()
        .enumerate()
        .map(|(index, length)| format!("{index:04}{}", "x".repeat(length - 4)))
        .collect();
    let extra = format!("extra{}", "y".repeat(895));
    let mut page: Vec<_> = names.iter().map(|name| heal_item(name)).collect();
    page.push(heal_item(&extra));
    let storage = MockStorage {
        retry_test_pages: Some(vec![page]),
        ..Default::default()
    };
    for (index, name) in names.iter().enumerate() {
        storage.heal_object_outcomes.lock().expect("outcomes").insert(
            name.clone(),
            VecDeque::from([MockHealObjectOutcome::RetryableLock, MockHealObjectOutcome::RetryableLock]),
        );
        let delays = if index < 7 {
            vec![Duration::ZERO, Duration::from_millis(1200)]
        } else if index == 7 {
            vec![Duration::from_millis(1500)]
        } else {
            Vec::new()
        };
        storage
            .retry_test_attempt_delays
            .lock()
            .expect("delays")
            .insert(name.clone(), delays.into());
    }
    storage.heal_object_outcomes.lock().expect("outcomes").insert(
        extra.clone(),
        VecDeque::from([MockHealObjectOutcome::RetryableLock, MockHealObjectOutcome::RetryableLock]),
    );
    storage.retry_test_attempt_delays.lock().expect("delays").insert(
        extra,
        VecDeque::from([
            Duration::from_millis(1100),
            if cancel {
                Duration::from_secs(31)
            } else {
                Duration::from_secs(2)
            },
        ]),
    );
    let storage = Arc::new(storage);
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(5), &mut execution).await.is_err());
    if cancel {
        task.cancel_token.cancel();
        assert!(matches!(execution.await, Err(Error::TaskCancelled)));
    } else {
        execution.await.expect("all attempted identities finish");
    }
    assert_eq!(
        task.get_outcome().await.counters.processed,
        u64::try_from(names.len() + 1).expect("count")
    );
}

#[tokio::test(start_paused = true)]
async fn due_retry_does_not_overlap_another_version_of_same_object() {
    let mut first = heal_item("a");
    first.version_id = Some("first".to_string());
    let mut second = heal_item("a");
    second.version_id = Some("second".to_string());
    let mut storage = MockStorage {
        retry_test_pages: Some(vec![vec![first, second]]),
        ..Default::default()
    };
    storage.retry_test_delays.insert("a".to_string(), Duration::from_secs(31));
    fail_once(&storage, "a");
    let storage = Arc::new(storage);
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(45), &mut execution).await.is_err());
    assert_eq!(
        storage.heal_object_calls.lock().expect("calls").as_slice(),
        ["a", "a"],
        "a retry must not overlap an in-flight version of the same object"
    );
}

#[tokio::test(start_paused = true)]
async fn retry_uses_remaining_time_before_age_limit() {
    let mut storage = pages_storage(&[&["a", "b"]]);
    storage.retry_test_delays.insert("b".to_string(), Duration::from_secs(27));
    storage.heal_object_outcomes.lock().expect("outcomes").insert(
        "a".to_string(),
        VecDeque::from([MockHealObjectOutcome::RetryableLock, MockHealObjectOutcome::RetryableLock]),
    );
    let storage = Arc::new(storage);
    let task = bucket_task(storage.clone());
    task.execute().await.expect("the last retry can run before the age limit");
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["a", "b", "a", "a"]);
    assert_eq!(task.get_outcome().await.counters.failed, 0);
}

#[tokio::test(start_paused = true)]
async fn expired_retry_is_terminal_without_an_extra_storage_attempt() {
    let mut first = heal_item("a");
    first.version_id = Some("first".to_string());
    let mut second = heal_item("a");
    second.version_id = Some("second".to_string());
    let mut storage = MockStorage {
        retry_test_pages: Some(vec![vec![first, second]]),
        ..Default::default()
    };
    storage.retry_test_delays.insert("a".to_string(), Duration::from_secs(31));
    fail_once(&storage, "a");
    let storage = Arc::new(storage);
    let task = bucket_task(storage.clone());
    task.execute().await.expect_err("aged pending responsibility is not success");
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["a", "a"]);
    let outcome = task.get_outcome().await;
    assert_eq!(outcome.counters.processed, 2);
    assert_eq!(outcome.counters.attempt_failures, 1);
    assert_eq!(outcome.counters.failed, 1);
    assert_eq!(
        outcome
            .objects
            .iter()
            .find(|item| item.identity.version_id.as_deref() == Some("first"))
            .expect("a outcome")
            .disposition,
        HealObjectDisposition::Failed(HealFailureClass::RetryExhausted)
    );
}

#[tokio::test(start_paused = true)]
async fn cancellation_drains_owned_retries_once() {
    let storage = Arc::new(pages_storage(&[&["a"], &["b"]]));
    fail_once(&storage, "a");
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(1), &mut execution).await.is_err());
    task.cancel().await.expect("cancel");
    assert!(matches!(execution.await, Err(Error::TaskCancelled)));
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["a", "b"]);
    let outcome = task.get_outcome().await;
    assert_eq!(outcome.counters.processed, 2);
    assert_eq!(outcome.objects.iter().filter(|item| item.identity.object == "a").count(), 1);
    assert_eq!(
        outcome
            .objects
            .iter()
            .find(|item| item.identity.object == "a")
            .expect("a")
            .disposition,
        HealObjectDisposition::Cancelled
    );
}

#[tokio::test(start_paused = true)]
async fn deadline_drains_owned_retries_without_false_completion() {
    let incarnation = Uuid::new_v4();
    let storage = pages_storage(&[&["a"], &["b"]]);
    *storage.bucket_incarnation_id.lock().expect("incarnation") = Some(incarnation);
    let storage = Arc::new(storage);
    fail_once(&storage, "a");
    let mut task = bucket_task(storage.clone());
    task.options.timeout = Some(Duration::from_secs(1));
    assert!(matches!(task.execute().await, Err(Error::TaskTimeout)));
    let outcome = task.get_outcome().await;
    assert_eq!(outcome.counters.processed, 2);
    assert_eq!(
        outcome
            .objects
            .iter()
            .find(|item| item.identity.object == "a")
            .expect("a")
            .identity
            .bucket_incarnation_id,
        Some(incarnation)
    );
    assert_eq!(
        outcome
            .objects
            .iter()
            .find(|item| item.identity.object == "a")
            .expect("a")
            .disposition,
        HealObjectDisposition::Deferred {
            reason: HealDeferredReason::Deadline,
            retry_not_before: None
        }
    );
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["a", "b"]);
}

#[tokio::test(start_paused = true)]
async fn full_window_backpressures_without_losing_the_current_page_tail() {
    let names: Vec<String> = (0..258).map(|index| format!("blocked-{index}")).collect();
    let mut storage = MockStorage {
        retry_test_pages: Some(vec![names.iter().map(|name| heal_item(name)).collect(), vec![heal_item("healthy")]]),
        ..Default::default()
    };
    for name in &names {
        fail_once(&storage, name);
    }
    // The last item has a version, proving the current-page tail is not rebuilt
    // from names alone when the window fills.
    storage.retry_test_pages.as_mut().expect("pages")[0][257].version_id = Some("version-tail".to_string());
    let storage = Arc::new(storage);
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(1), &mut execution).await.is_err());
    assert_eq!(storage.heal_object_calls.lock().expect("calls").len(), 257);
    assert_eq!(storage.listing_tokens.lock().expect("tokens").len(), 1);
    execution.await.expect("every owned item eventually completes");
    assert_eq!(task.get_outcome().await.counters.processed, 259);
    let calls = storage.heal_object_calls.lock().expect("calls");
    for name in &names {
        assert_eq!(calls.iter().filter(|called| *called == name).count(), 2);
    }
    let versions = storage.heal_object_version_ids.lock().expect("versions");
    for (name, version) in calls.iter().zip(versions.iter()) {
        if name == "blocked-257" {
            assert_eq!(version.as_deref(), Some("version-tail"));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn oversized_identity_stays_inline_without_losing_version() {
    let name = "k".repeat(256 * 1024);
    let mut item = heal_item(&name);
    item.version_id = Some("v".repeat(1024));
    let storage = Arc::new(MockStorage {
        retry_test_pages: Some(vec![vec![item], vec![heal_item("healthy")]]),
        ..Default::default()
    });
    fail_once(&storage, &name);
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(tokio::time::timeout(Duration::from_secs(1), &mut execution).await.is_err());
    assert_eq!(storage.listing_tokens.lock().expect("tokens").len(), 1);
    execution.await.expect("oversized identity retries inline");
    assert_eq!(task.get_outcome().await.counters.processed, 2);
    let versions = storage.heal_object_version_ids.lock().expect("versions");
    assert_eq!(versions[0], versions[1]);
    assert_eq!(versions[0].as_ref().expect("version").len(), 1024);
}

#[tokio::test(start_paused = true)]
async fn terminal_listing_failure_keeps_deferred_identity_unknown() {
    let storage = Arc::new(MockStorage {
        fail_second_listing_page: true,
        ..Default::default()
    });
    fail_once(&storage, "object-a");
    let task = bucket_task(storage.clone());
    task.execute().await.expect_err("listing cannot continue");
    let outcome = task.get_outcome().await;
    assert_eq!(outcome.counters.processed, 1);
    assert_eq!(outcome.objects[0].disposition, HealObjectDisposition::Unknown);
    assert_eq!(outcome.coverage, crate::heal::outcome::HealTraversalCoverage::Partial);
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["object-a"]);
}

#[tokio::test(start_paused = true)]
async fn healthy_second_page_advances_before_first_retry_is_due() {
    let storage = Arc::new(MockStorage {
        recoverable_second_page_failures: Mutex::new(Some(0)),
        ..Default::default()
    });
    storage
        .heal_object_outcomes
        .lock()
        .expect("outcomes")
        .insert("object-a".to_string(), VecDeque::from([MockHealObjectOutcome::RetryableSlowDown]));
    let task = HealTask::from_request(
        HealRequest::new(
            HealType::Bucket {
                bucket: "bucket-a".to_string(),
            },
            HealOptions {
                recursive: true,
                timeout: None,
                ..Default::default()
            },
            HealPriority::Normal,
        ),
        storage.clone(),
    );
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut execution).await.is_err(),
        "the deferred first object must remain pending before its retry is due"
    );
    assert_eq!(
        storage.heal_object_calls.lock().expect("calls").as_slice(),
        ["object-a", "object-b"],
        "a retryable page head must not hold the healthy second page behind its backoff"
    );
    execution.await.expect("retry eventually succeeds");
    let outcome = task.get_outcome().await;
    assert_eq!(outcome.counters.processed, 2);
    assert_eq!(outcome.counters.attempt_failures, 1);
    assert_eq!(
        storage.heal_object_calls.lock().expect("calls").as_slice(),
        ["object-a", "object-b", "object-a"]
    );
}
