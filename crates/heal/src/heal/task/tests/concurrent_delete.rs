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

fn admin_traversal(heal_type: HealType, storage: Arc<MockStorage>, dry_run: bool) -> HealTask {
    let mut request = HealRequest::new(
        heal_type,
        HealOptions {
            recursive: true,
            recreate_missing: false,
            dry_run,
            timeout: None,
            ..Default::default()
        },
        HealPriority::Normal,
    );
    request.source = HealRequestSource::Admin;
    HealTask::from_request(request, storage)
}

#[tokio::test]
async fn admin_traversal_rejects_bucket_recreated_after_enumeration() {
    for heal_type in [
        HealType::Cluster,
        HealType::Prefix {
            bucket: "bucket-a".to_owned(),
            prefix: "logs/".to_owned(),
        },
    ] {
        let original = Uuid::new_v4();
        let storage = Arc::new(MockStorage {
            bucket_incarnation_id: Mutex::new(Some(original)),
            bucket_incarnation_after_listing: Mutex::new(Some(Uuid::new_v4())),
            ..Default::default()
        });
        let task = admin_traversal(heal_type, storage.clone(), false);

        let error = task.execute().await.expect_err("the enumerated bucket was replaced");
        assert!(error.to_string().contains("stale_bucket_incarnation"), "{error}");
        assert!(
            storage.heal_object_calls.lock().expect("object calls").is_empty(),
            "the old traversal must reject the successor before repairing any object"
        );
        assert!(matches!(task.get_status().await, HealTaskStatus::Failed { .. }));
    }
}

#[tokio::test]
async fn admin_traversal_keeps_bucket_identity_across_erasure_sets() {
    let storage = Arc::new(MockStorage {
        bucket_incarnation_id: Mutex::new(Some(Uuid::new_v4())),
        bucket_incarnation_after_object_heal: Mutex::new(Some(Uuid::new_v4())),
        erasure_set_scopes: Mutex::new(vec![(0, 0), (1, 0)]),
        ..Default::default()
    });
    let task = admin_traversal(HealType::Cluster, storage.clone(), false);

    let error = task
        .execute()
        .await
        .expect_err("the next pool must retain the original bucket identity");
    assert!(error.to_string().contains("stale_bucket_incarnation"), "{error}");
    assert_eq!(
        storage.disk_walk_calls.lock().expect("disk walk calls").as_slice(),
        ["pool_0_set_0"],
        "the successor must not be enumerated in the next pool"
    );
    assert_eq!(
        storage.heal_object_calls.lock().expect("object calls").as_slice(),
        ["pool_0_set_0-object"]
    );
}

#[tokio::test]
async fn admin_traversal_requires_identity_before_enumeration() {
    for unavailable in [false, true] {
        let storage = Arc::new(MockStorage {
            bucket_incarnation_unavailable: Mutex::new(unavailable),
            ..Default::default()
        });
        let task = admin_traversal(HealType::Cluster, storage.clone(), false);

        task.execute()
            .await
            .expect_err("an unowned traversal cannot classify deleted objects");
        assert!(storage.listing_tokens.lock().expect("listing tokens").is_empty());
        assert!(storage.heal_object_calls.lock().expect("object calls").is_empty());
        assert_eq!(task.get_progress().await.objects_scanned, 0);
    }
}

#[tokio::test]
async fn admin_dry_run_can_observe_without_bucket_identity() {
    let storage = Arc::new(MockStorage {
        bucket_incarnation_unavailable: Mutex::new(true),
        ..Default::default()
    });
    let task = admin_traversal(HealType::Cluster, storage, true);

    task.execute().await.expect("dry run retains its observation-only behavior");
    let outcome = task.get_outcome().await;
    assert_eq!(outcome.counters.processed, 2);
    assert!(
        outcome
            .objects
            .iter()
            .all(|item| item.disposition == HealObjectDisposition::DryRunObserved)
    );
}

#[tokio::test(start_paused = true)]
async fn admin_traversal_never_converts_unproven_errors_to_absence() {
    for (error, class) in [
        (MockHealObjectOutcome::MissingVersion, HealFailureClass::Permanent),
        (MockHealObjectOutcome::PermissionDenied, HealFailureClass::Permanent),
        (MockHealObjectOutcome::ErrOther("file not found"), HealFailureClass::Permanent),
        (MockHealObjectOutcome::RetryableReadQuorum, HealFailureClass::RetryExhausted),
        (MockHealObjectOutcome::RetryableLockTimeout, HealFailureClass::RetryExhausted),
    ] {
        let storage = Arc::new(MockStorage {
            bucket_incarnation_id: Mutex::new(Some(Uuid::new_v4())),
            retry_test_pages: Some(vec![vec![heal_item("object-a")]]),
            heal_object_outcomes: Mutex::new(HashMap::from([("object-a".to_owned(), VecDeque::from(vec![error; 4]))])),
            ..Default::default()
        });
        let task = admin_traversal(HealType::Cluster, storage, false);

        task.execute()
            .await
            .expect_err("a storage error without an absence proof cannot complete the root");
        let outcome = task.get_outcome().await;
        assert_eq!(outcome.counters.processed, 1);
        assert_eq!(outcome.counters.failed, 1);
        assert_eq!(outcome.counters.unchanged, 0);
        assert_eq!(outcome.objects[0].disposition, HealObjectDisposition::Failed(class));
    }
}
