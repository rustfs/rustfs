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
async fn slow_listing_retry_services_due_object_then_age_before_next_listing() {
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
    assert!(
        tokio::time::timeout(Duration::from_millis(30_500), &mut execution)
            .await
            .is_err()
    );
    assert_eq!(
        storage.retry_test_events.lock().expect("events").as_slice(),
        ["list:first", "heal:object-a", "list:second", "heal:object-a"]
    );
    let outcome = task.get_outcome().await;
    assert_eq!(outcome.counters.processed, 1);
    assert_eq!(
        outcome.objects[0].disposition,
        HealObjectDisposition::Failed(HealFailureClass::RetryExhausted)
    );
    execution.await.expect_err("age exhausted object must remain a batch failure");
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
    let outcome = task.get_outcome().await;
    assert_eq!((outcome.counters.processed, outcome.counters.attempt_failures), (2, 3));
}

#[tokio::test(start_paused = true)]
async fn listing_return_after_age_expires_does_not_start_another_heal_attempt() {
    let storage = Arc::new(MockStorage {
        recoverable_second_page_failures: Mutex::new(Some(1)),
        retry_test_listing_delays: Mutex::new(VecDeque::from([Duration::ZERO, Duration::from_secs(31)])),
        ..Default::default()
    });
    fail_once(&storage, "object-a");
    let task = bucket_task(storage.clone());
    let execution = task.execute();
    tokio::pin!(execution);
    assert!(
        tokio::time::timeout(Duration::from_millis(31_500), &mut execution)
            .await
            .is_err()
    );
    assert_eq!(
        storage.retry_test_events.lock().expect("events").as_slice(),
        ["list:first", "heal:object-a", "list:second"]
    );
    assert_eq!(task.get_outcome().await.counters.failed, 1);
    execution.await.expect_err("age exhaustion remains a failure");
    assert_eq!(
        storage.retry_test_events.lock().expect("events").as_slice(),
        ["list:first", "heal:object-a", "list:second", "list:second", "heal:object-b"]
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
async fn repeated_slowdown_keeps_attempts_and_forward_pages_bounded() {
    let storage = Arc::new(pages_storage(&[&["a"], &["b"], &["c"], &["d"]]));
    for name in ["a", "b", "c", "d"] {
        storage
            .heal_object_outcomes
            .lock()
            .expect("outcomes")
            .insert(name.to_string(), (0..4).map(|_| MockHealObjectOutcome::RetryableSlowDown).collect());
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
        (4, 4, 16)
    );
    assert_eq!(storage.heal_object_calls.lock().expect("calls").len(), 16);
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
async fn expired_retry_is_terminal_without_an_extra_storage_attempt() {
    let mut storage = pages_storage(&[&["a"], &["b"]]);
    storage.retry_test_delays.insert("b".to_string(), Duration::from_secs(31));
    fail_once(&storage, "a");
    let storage = Arc::new(storage);
    let task = bucket_task(storage.clone());
    task.execute().await.expect_err("aged pending responsibility is not success");
    assert_eq!(storage.heal_object_calls.lock().expect("calls").as_slice(), ["a", "b"]);
    let outcome = task.get_outcome().await;
    assert_eq!(outcome.counters.processed, 2);
    assert_eq!(outcome.counters.attempt_failures, 1);
    assert_eq!(outcome.counters.failed, 1);
    assert_eq!(
        outcome
            .objects
            .iter()
            .find(|item| item.identity.object == "a")
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
    let storage = Arc::new(pages_storage(&[&["a"], &["b"]]));
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
