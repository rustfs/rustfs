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

fn object_request(source: HealRequestSource, priority: HealPriority, force_start: bool) -> HealRequest {
    let mut request = HealRequest::new(
        HealType::Object {
            bucket: "canonical-owner".to_owned(),
            object: "object".to_owned(),
            version_id: None,
        },
        HealOptions::default(),
        priority,
    );
    request.source = source;
    request.force_start = force_start;
    request
}

fn scoped_object_request(
    source: HealRequestSource,
    version_id: &str,
    pool_index: Option<usize>,
    set_index: Option<usize>,
    force_start: bool,
) -> HealRequest {
    let mut request = object_request(source, HealPriority::Low, force_start);
    let HealType::Object { version_id: version, .. } = &mut request.heal_type else {
        unreachable!("object_request always constructs an object heal")
    };
    *version = Some(version_id.to_owned());
    request.options.pool_index = pool_index;
    request.options.set_index = set_index;
    request.options.recreate_missing = source != HealRequestSource::Scanner;
    request
}

#[tokio::test]
async fn scanner_receipt_tracks_scoped_read_repair_forced_holders() {
    let manager = HealManager::new_without_root_recovery_for_test(
        Arc::new(MockStorage),
        Some(HealConfig {
            queue_size: 16,
            low_priority_merge_enable: true,
            ..HealConfig::default()
        }),
    );
    let version = "00000000-0000-4000-8000-000000000001";
    let original = scoped_object_request(HealRequestSource::ReadRepair, version, Some(0), Some(1), false);
    let original_id = original.id.clone();
    let key = PriorityHealQueue::make_dedup_key(&original);
    let admitted = manager
        .submit_heal_request_with_receipt(original)
        .await
        .expect("admit read-repair owner");
    assert_eq!(admitted.result, HealAdmissionResult::Accepted);
    assert_eq!(admitted.task_id, original_id);

    let receipt = manager
        .submit_heal_request_with_receipt(scoped_object_request(HealRequestSource::Scanner, version, Some(0), Some(1), false))
        .await
        .expect("scanner merges into the scoped read-repair owner");
    assert_eq!(receipt.result, HealAdmissionResult::Merged);
    assert_eq!(receipt.task_id, original_id);

    let first_forced = scoped_object_request(HealRequestSource::Internal, version, Some(0), Some(1), true);
    let first_forced_id = first_forced.id.clone();
    let second_forced = scoped_object_request(HealRequestSource::Internal, version, Some(0), Some(1), true);
    let second_forced_id = second_forced.id.clone();
    {
        let mut queue = manager.heal_queue.lock().await;
        assert_eq!(queue.push(first_forced), QueuePushOutcome::Accepted);
        assert_eq!(queue.push(second_forced), QueuePushOutcome::Accepted);
        let (selected, skipped) = queue.pop_runnable_with_skips(|_| true, |_| None);
        assert!(skipped.is_empty());
        assert_eq!(selected.expect("read-repair owner should leave first").id, original_id);
        assert_eq!(queue.queued_request_id_for_dedup_key(&key), Some(original_id.as_str()));
    }

    let receipt = manager
        .submit_heal_request_with_receipt(scoped_object_request(HealRequestSource::Scanner, version, Some(0), Some(1), false))
        .await
        .expect("scanner merges into the remaining owner");
    assert_eq!(receipt.result, HealAdmissionResult::Merged);
    assert_eq!(receipt.task_id, first_forced_id);

    {
        let mut queue = manager.heal_queue.lock().await;
        let (selected, _) = queue.pop_runnable_with_skips(|request| request.id == first_forced_id, |_| None);
        assert_eq!(selected.expect("first forced holder should leave").id, first_forced_id);
        assert!(queue.contains_request_id(&second_forced_id));
    }
    let receipt = manager
        .submit_heal_request_with_receipt(scoped_object_request(HealRequestSource::Scanner, version, Some(0), Some(1), false))
        .await
        .expect("scanner merges into the last queued holder");
    assert_eq!(receipt.result, HealAdmissionResult::Merged);
    assert_eq!(receipt.task_id, second_forced_id);
}

#[tokio::test]
async fn scanner_receipt_does_not_merge_read_repair_with_a_different_scope_or_version() {
    let version = "00000000-0000-4000-8000-000000000001";
    let other_version = "00000000-0000-4000-8000-000000000002";
    for (candidate_version, pool, set) in [
        (version, None, None),
        (version, Some(1), Some(1)),
        (version, Some(0), Some(2)),
        (other_version, Some(0), Some(1)),
    ] {
        let manager = HealManager::new_without_root_recovery_for_test(
            Arc::new(MockStorage),
            Some(HealConfig {
                queue_size: 16,
                low_priority_merge_enable: true,
                ..HealConfig::default()
            }),
        );
        let original = scoped_object_request(HealRequestSource::ReadRepair, version, Some(0), Some(1), false);
        let original_id = original.id.clone();
        let initial = manager
            .submit_heal_request_with_receipt(original)
            .await
            .expect("admit read-repair owner");
        assert_eq!(initial.result, HealAdmissionResult::Accepted);
        assert_eq!(initial.task_id, original_id);

        let candidate = scoped_object_request(HealRequestSource::Scanner, candidate_version, pool, set, false);
        let candidate_id = candidate.id.clone();
        let receipt = manager
            .submit_heal_request_with_receipt(candidate)
            .await
            .expect("admit scanner with a distinct scope or version");
        assert_eq!(
            receipt.result,
            HealAdmissionResult::Accepted,
            "version={candidate_version}, pool={pool:?}, set={set:?}"
        );
        assert_eq!(receipt.task_id, candidate_id);
        let queue = manager.heal_queue.lock().await;
        assert!(queue.contains_request_id(&original_id));
        assert!(queue.contains_request_id(&candidate_id));
    }
}

async fn scanner_receipt_tracks_queued_owner(use_fairness: bool) {
    let manager = HealManager::new_without_root_recovery_for_test(
        Arc::new(MockStorage),
        Some(HealConfig {
            queue_size: 16,
            low_priority_merge_enable: true,
            overlap_policy: HealOverlapPolicy::Merge,
            ..HealConfig::default()
        }),
    );

    if use_fairness {
        let mut queue = manager.heal_queue.lock().await;
        for index in 0..4 {
            let request = bucket_request(&format!("internal-{index}"), HealPriority::Normal, HealRequestSource::Internal);
            assert_eq!(queue.push(request), QueuePushOutcome::Accepted);
        }
        for _ in 0..4 {
            let (request, _) = queue.pop_runnable_with_fairness(|_| true, |_| None);
            assert_eq!(request.expect("internal work should run").source, HealRequestSource::Internal);
        }
    }

    let first = object_request(HealRequestSource::Scanner, HealPriority::High, false);
    let first_id = first.id.clone();
    let key = PriorityHealQueue::make_dedup_key(&first);
    let second = object_request(HealRequestSource::Admin, HealPriority::High, true);
    let second_id = second.id.clone();
    for request in [first, second] {
        assert_eq!(
            manager
                .submit_heal_request_with_receipt(request)
                .await
                .expect("admit task")
                .result,
            HealAdmissionResult::Accepted
        );
    }

    let first = {
        let mut queue = manager.heal_queue.lock().await;
        let (selected, skipped) = if use_fairness {
            queue.pop_runnable_with_fairness(|_| true, |_| None)
        } else {
            queue.pop_runnable_with_skips(|_| true, |_| None)
        };
        assert!(skipped.is_empty());
        let selected = selected.expect("the first same-priority task should run");
        assert_eq!(selected.id, first_id);
        assert!(queue.contains_request_id(&second_id));
        assert_eq!(
            queue.queued_request_id_for_dedup_key(&key),
            Some(first_id.as_str()),
            "the existing representative for other sources must remain unchanged"
        );
        selected
    };
    let QueuedHeal { request, observation_id } = first;
    manager.active_heals.lock().await.insert(
        first_id.clone(),
        Arc::new(HealTask::from_request(request, Arc::new(MockStorage)).with_observation_id(observation_id)),
    );

    let active = manager
        .submit_heal_request_with_receipt(object_request(HealRequestSource::Scanner, HealPriority::Low, false))
        .await
        .expect("scanner admission while the first task is active");
    assert_eq!(active.result, HealAdmissionResult::Merged);
    assert_eq!(active.task_id, first_id, "active ownership keeps precedence");

    let completed_task = manager
        .active_heals
        .lock()
        .await
        .remove(&first_id)
        .expect("first task should leave active ownership");
    let mut retained = CompletedHealStatus::snapshot(&completed_task, HealTaskStatus::Completed).await;
    Arc::make_mut(retained.outcome.as_mut().expect("retained task outcome")).finish(None);
    manager
        .completed_heals
        .lock()
        .await
        .insert(first_id.clone(), Arc::new(retained));
    let queued = manager
        .submit_heal_request_with_receipt(object_request(HealRequestSource::Scanner, HealPriority::Low, false))
        .await
        .expect("scanner admission after the first task leaves active state");
    assert_eq!(queued.result, HealAdmissionResult::Merged);
    assert_eq!(queued.task_id, second_id, "the receipt must name the remaining queued task");
}

#[tokio::test]
async fn scanner_receipt_tracks_queued_owner_after_normal_dequeue() {
    scanner_receipt_tracks_queued_owner(false).await;
}

#[tokio::test]
async fn scanner_receipt_tracks_queued_owner_after_fair_dequeue() {
    scanner_receipt_tracks_queued_owner(true).await;
}

#[tokio::test]
async fn scanner_receipt_keeps_queued_owner_when_a_different_forced_task_runs() {
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
    let first = object_request(HealRequestSource::Scanner, HealPriority::Low, false);
    let first_id = first.id.clone();
    let second = object_request(HealRequestSource::Admin, HealPriority::High, true);
    let second_id = second.id.clone();
    {
        let mut queue = manager.heal_queue.lock().await;
        assert_eq!(queue.push(first), QueuePushOutcome::Accepted);
        assert_eq!(queue.push(second), QueuePushOutcome::Accepted);
        let (selected, _) = queue.pop_runnable_with_skips(|_| true, |_| None);
        assert_eq!(selected.expect("higher-priority forced task should run").id, second_id);
    }
    let receipt = manager
        .submit_heal_request_with_receipt(object_request(HealRequestSource::Scanner, HealPriority::High, false))
        .await
        .expect("scanner admission after a non-representative task leaves the queue");
    assert_eq!(receipt.result, HealAdmissionResult::Merged);
    assert_eq!(receipt.task_id, first_id);
}

#[tokio::test]
async fn scanner_receipt_reselects_after_the_queued_owner_is_cancelled() {
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
    let first = object_request(HealRequestSource::Scanner, HealPriority::High, false);
    let first_id = first.id.clone();
    let second = object_request(HealRequestSource::Admin, HealPriority::High, true);
    let second_id = second.id.clone();
    let third = object_request(HealRequestSource::Internal, HealPriority::High, true);
    let third_id = third.id.clone();
    {
        let mut queue = manager.heal_queue.lock().await;
        for request in [first, second, third] {
            assert_eq!(queue.push(request), QueuePushOutcome::Accepted);
        }
        let (selected, _) = queue.pop_runnable_with_skips(|_| true, |_| None);
        assert_eq!(selected.expect("first task should run").id, first_id);
    }
    let first_receipt = manager
        .submit_heal_request_with_receipt(object_request(HealRequestSource::Scanner, HealPriority::High, false))
        .await
        .expect("first scanner admission");
    assert_eq!(first_receipt.result, HealAdmissionResult::Merged);
    assert!(first_receipt.task_id == second_id || first_receipt.task_id == third_id);
    let remaining = if first_receipt.task_id == second_id {
        third_id
    } else {
        second_id
    };
    manager.heal_queue.lock().await.remove_request_id(&first_receipt.task_id);

    let next_receipt = manager
        .submit_heal_request_with_receipt(object_request(HealRequestSource::Scanner, HealPriority::High, false))
        .await
        .expect("scanner admission after the queued owner is removed");
    assert_eq!(next_receipt.result, HealAdmissionResult::Merged);
    assert_eq!(next_receipt.task_id, remaining);
}

#[tokio::test]
async fn scanner_receipt_reselects_after_multiple_forced_dequeues() {
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
    let first = object_request(HealRequestSource::Scanner, HealPriority::High, false);
    let first_id = first.id.clone();
    let second = object_request(HealRequestSource::Admin, HealPriority::High, true);
    let second_id = second.id.clone();
    let third = object_request(HealRequestSource::Internal, HealPriority::High, true);
    let third_id = third.id.clone();
    {
        let mut queue = manager.heal_queue.lock().await;
        for request in [first, second, third] {
            assert_eq!(queue.push(request), QueuePushOutcome::Accepted);
        }
        let (selected, _) = queue.pop_runnable_with_skips(|_| true, |_| None);
        assert_eq!(selected.expect("first task should run").id, first_id);
    }
    let first_receipt = manager
        .submit_heal_request_with_receipt(object_request(HealRequestSource::Scanner, HealPriority::High, false))
        .await
        .expect("first scanner admission");
    assert_eq!(first_receipt.result, HealAdmissionResult::Merged);
    assert!(first_receipt.task_id == second_id || first_receipt.task_id == third_id);
    let remaining = if first_receipt.task_id == second_id {
        third_id
    } else {
        second_id
    };
    {
        let mut queue = manager.heal_queue.lock().await;
        let (selected, _) = queue.pop_runnable_with_skips(|request| request.id == first_receipt.task_id, |_| None);
        assert_eq!(selected.expect("selected queued owner should run").id, first_receipt.task_id);
    }
    let next_receipt = manager
        .submit_heal_request_with_receipt(object_request(HealRequestSource::Scanner, HealPriority::High, false))
        .await
        .expect("scanner admission after another queued owner starts");
    assert_eq!(next_receipt.result, HealAdmissionResult::Merged);
    assert_eq!(next_receipt.task_id, remaining);
}

#[test]
fn scanner_owner_index_survives_a_single_remaining_holder_and_new_forced_work() {
    let mut queue = PriorityHealQueue::new();
    let first = object_request(HealRequestSource::Scanner, HealPriority::High, false);
    let first_id = first.id.clone();
    let key = PriorityHealQueue::make_dedup_key(&first);
    let second = object_request(HealRequestSource::Admin, HealPriority::High, true);
    let second_id = second.id.clone();
    let third = object_request(HealRequestSource::Internal, HealPriority::High, true);
    let third_id = third.id.clone();

    assert_eq!(queue.push(first), QueuePushOutcome::Accepted);
    assert!(matches!(&queue.dedup_keys[&key].scanner_queued_owners, ScannerQueuedOwners::Single(_)));
    assert_eq!(queue.push(second), QueuePushOutcome::Accepted);
    assert_eq!(queue.pop().expect("first queued task").id, first_id);
    assert_eq!(queue.queued_request_id_for_dedup_key(&key), Some(first_id.as_str()));
    assert_eq!(queue.scanner_queued_request_id_for_dedup_key(&key), Some(second_id.as_str()));

    assert_eq!(queue.push(third), QueuePushOutcome::Accepted);
    assert_eq!(queue.scanner_queued_request_id_for_dedup_key(&key), Some(second_id.as_str()));
    assert_eq!(queue.pop().expect("second queued task").id, second_id);
    assert_eq!(queue.scanner_queued_request_id_for_dedup_key(&key), Some(third_id.as_str()));
}

#[test]
fn scanner_owner_index_distinguishes_holders_with_the_same_request_id() {
    let mut queue = PriorityHealQueue::new();
    let first = object_request(HealRequestSource::Scanner, HealPriority::High, false);
    let key = PriorityHealQueue::make_dedup_key(&first);
    let shared_id = first.id.clone();
    let mut second = object_request(HealRequestSource::Admin, HealPriority::High, true);
    second.id.clone_from(&shared_id);
    let third = object_request(HealRequestSource::Internal, HealPriority::High, true);
    let third_id = third.id.clone();
    for request in [first, second, third] {
        assert_eq!(queue.push(request), QueuePushOutcome::Accepted);
    }

    assert_eq!(queue.pop().expect("first holder").id, shared_id);
    assert_eq!(queue.scanner_queued_request_id_for_dedup_key(&key), Some(shared_id.as_str()));
    assert_eq!(queue.remove_request_id(&shared_id).expect("second holder").id, shared_id);
    assert_eq!(queue.scanner_queued_request_id_for_dedup_key(&key), Some(third_id.as_str()));
    assert_eq!(queue.queued_request_id_for_dedup_key(&key), Some(third_id.as_str()));
}

#[test]
fn scanner_owner_index_tracks_bulk_removal_and_key_release() {
    let mut queue = PriorityHealQueue::new();
    let first = object_request(HealRequestSource::Scanner, HealPriority::High, false);
    let first_id = first.id.clone();
    let key = PriorityHealQueue::make_dedup_key(&first);
    let second = object_request(HealRequestSource::Admin, HealPriority::Normal, true);
    let second_id = second.id.clone();
    let third = object_request(HealRequestSource::Internal, HealPriority::Low, true);
    let third_id = third.id.clone();
    for request in [first, second, third] {
        assert_eq!(queue.push(request), QueuePushOutcome::Accepted);
    }

    let removed = queue.remove_matching(|request| request.id == first_id || request.id == second_id);
    assert_eq!(removed.len(), 2);
    assert_eq!(queue.scanner_queued_request_id_for_dedup_key(&key), Some(third_id.as_str()));
    assert_eq!(queue.queued_request_id_for_dedup_key(&key), Some(third_id.as_str()));
    assert_eq!(queue.remove_matching(|request| request.id == third_id).len(), 1);
    assert!(!queue.dedup_keys.contains_key(&key));

    let next = object_request(HealRequestSource::Scanner, HealPriority::High, false);
    let next_id = next.id.clone();
    assert_eq!(queue.push(next), QueuePushOutcome::Accepted);
    assert!(matches!(&queue.dedup_keys[&key].scanner_queued_owners, ScannerQueuedOwners::Single(_)));
    assert_eq!(queue.scanner_queued_request_id_for_dedup_key(&key), Some(next_id.as_str()));
}

#[test]
fn scanner_owner_index_tracks_displacement_without_changing_legacy_reselection() {
    let mut queue = PriorityHealQueue::new();
    let first = object_request(HealRequestSource::Scanner, HealPriority::Low, false);
    let first_id = first.id.clone();
    let key = PriorityHealQueue::make_dedup_key(&first);
    let second = object_request(HealRequestSource::Admin, HealPriority::Normal, true);
    let second_id = second.id.clone();
    assert_eq!(queue.push(first), QueuePushOutcome::Accepted);
    assert_eq!(queue.push(second), QueuePushOutcome::Accepted);

    let incoming = bucket_request("displacement", HealPriority::Urgent, HealRequestSource::Internal);
    let displaced = queue
        .push_displacing_lower_priority(incoming)
        .expect("lower priority task should be displaced");
    assert_eq!(displaced.id, first_id);
    assert_eq!(queue.scanner_queued_request_id_for_dedup_key(&key), Some(second_id.as_str()));
    assert_eq!(queue.queued_request_id_for_dedup_key(&key), Some(second_id.as_str()));
}

#[tokio::test]
async fn scanner_receipt_rejects_a_missing_or_empty_holder_index() {
    for missing in [true, false] {
        let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
        let first = object_request(HealRequestSource::Scanner, HealPriority::High, false);
        let key = PriorityHealQueue::make_dedup_key(&first);
        let second = object_request(HealRequestSource::Admin, HealPriority::High, true);
        {
            let mut queue = manager.heal_queue.lock().await;
            assert_eq!(queue.push(first), QueuePushOutcome::Accepted);
            assert_eq!(queue.push(second), QueuePushOutcome::Accepted);
            let entry = queue.dedup_keys.get_mut(&key).expect("dedup key should exist");
            entry.scanner_queued_owners = if missing {
                ScannerQueuedOwners::Single(0)
            } else {
                ScannerQueuedOwners::Indexed(std::collections::BTreeMap::new())
            };
        }

        for leave_one in [false, true] {
            if leave_one {
                manager.heal_queue.lock().await.pop().expect("one holder should leave");
            }
            let request = object_request(HealRequestSource::Scanner, HealPriority::High, false);
            let request_id = request.id.clone();
            let receipt = manager
                .submit_heal_request_with_receipt(request)
                .await
                .expect("admission should fail closed");
            assert_eq!(receipt.result, HealAdmissionResult::Dropped(HealAdmissionDropReason::AlreadyRunning));
            assert_eq!(receipt.task_id, request_id);
        }
    }
}

#[test]
fn scanner_and_legacy_owners_keep_their_distinct_reselection_orders() {
    let mut queue = PriorityHealQueue::new();
    let first = object_request(HealRequestSource::Scanner, HealPriority::High, false);
    let first_id = first.id.clone();
    let key = PriorityHealQueue::make_dedup_key(&first);
    let second = object_request(HealRequestSource::Admin, HealPriority::Low, true);
    let second_id = second.id.clone();
    let third = object_request(HealRequestSource::Internal, HealPriority::Normal, true);
    let third_id = third.id.clone();
    for request in [first, second, third] {
        assert_eq!(queue.push(request), QueuePushOutcome::Accepted);
    }

    assert_eq!(queue.remove_request_id(&first_id).expect("first holder should be removed").id, first_id);
    assert_eq!(queue.scanner_queued_request_id_for_dedup_key(&key), Some(second_id.as_str()));
    assert_eq!(queue.queued_request_id_for_dedup_key(&key), Some(third_id.as_str()));
}

#[test]
fn scanner_owner_index_tracks_a_series_of_forced_holders() {
    let mut queue = PriorityHealQueue::new();
    let first = object_request(HealRequestSource::Scanner, HealPriority::High, false);
    let key = PriorityHealQueue::make_dedup_key(&first);
    let mut ids = vec![first.id.clone()];
    assert_eq!(queue.push(first), QueuePushOutcome::Accepted);
    for _ in 0..128 {
        let request = object_request(HealRequestSource::Internal, HealPriority::High, true);
        ids.push(request.id.clone());
        assert_eq!(queue.push(request), QueuePushOutcome::Accepted);
    }
    for id in &ids {
        assert_eq!(queue.scanner_queued_request_id_for_dedup_key(&key), Some(id.as_str()));
        assert_eq!(&queue.pop().expect("indexed holder should remain queued").id, id);
    }
    assert!(!queue.dedup_keys.contains_key(&key));
}

#[tokio::test]
async fn observed_scanner_and_legacy_receipts_pair_their_own_reselected_holders() {
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
    let first = object_request(HealRequestSource::Scanner, HealPriority::High, false);
    let first_id = first.id.clone();
    let key = PriorityHealQueue::make_dedup_key(&first);
    let second = object_request(HealRequestSource::Internal, HealPriority::Low, true);
    let second_id = second.id.clone();
    let third = object_request(HealRequestSource::Internal, HealPriority::Normal, true);
    let third_id = third.id.clone();
    {
        let mut queue = manager.heal_queue.lock().await;
        for (request, observation_id) in [(first, 11), (second, 12), (third, 13)] {
            assert_eq!(
                queue.push_with_replacement_scope(request, false, Some(observation_id)),
                QueuePushOutcome::Accepted
            );
        }
        assert_eq!(queue.remove_request_id(&first_id).expect("remove first holder").id, first_id);
        assert_eq!(queue.queued_scanner_owner_for_dedup_key(&key), Some((second_id.as_str(), Some(12))));
        assert_eq!(queue.queued_legacy_owner_for_dedup_key(&key), Some((third_id.as_str(), Some(13))));
    }

    for (source, expected_id, expected_observation) in [
        (HealRequestSource::Scanner, &second_id, 12),
        (HealRequestSource::Internal, &third_id, 13),
    ] {
        let receipt = manager
            .submit_heal_request_with_observation_and_alias(object_request(source, HealPriority::High, false), false)
            .await
            .expect("merge into queued owner");
        assert_eq!(receipt.result, HealAdmissionResult::Merged);
        assert_eq!(&receipt.task_id, expected_id);
        assert_eq!(receipt.observation_id, Some(expected_observation));
    }
}

#[tokio::test]
async fn observed_same_id_holders_use_the_selected_sequence_not_the_id() {
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
    let first = object_request(HealRequestSource::Scanner, HealPriority::High, false);
    let id = first.id.clone();
    let key = PriorityHealQueue::make_dedup_key(&first);
    let mut second = object_request(HealRequestSource::Internal, HealPriority::High, true);
    second.id.clone_from(&id);
    {
        let mut queue = manager.heal_queue.lock().await;
        assert_eq!(queue.push_with_replacement_scope(first, false, Some(21)), QueuePushOutcome::Accepted);
        assert_eq!(queue.push_with_replacement_scope(second, false, Some(22)), QueuePushOutcome::Accepted);
        let (selected, _) = queue.pop_runnable_with_skips(|_| true, |_| None);
        let selected = selected.expect("first holder should run");
        assert_eq!(selected.id, id);
        assert_eq!(selected.observation_id, Some(21));
        assert_eq!(queue.queued_scanner_owner_for_dedup_key(&key), Some((id.as_str(), Some(22))));
        assert_eq!(queue.queued_legacy_owner_for_dedup_key(&key), Some((id.as_str(), None)));
    }

    for (source, expected_observation) in [(HealRequestSource::Scanner, Some(22)), (HealRequestSource::Internal, None)] {
        let receipt = manager
            .submit_heal_request_with_observation_and_alias(object_request(source, HealPriority::High, false), false)
            .await
            .expect("merge into the selected holder");
        assert_eq!(receipt.result, HealAdmissionResult::Merged);
        assert_eq!(receipt.task_id, id);
        assert_eq!(receipt.observation_id, expected_observation);
    }
}

#[test]
fn displaced_holder_reselects_the_remaining_id_and_observation_together() {
    let mut queue = PriorityHealQueue::new();
    let first = object_request(HealRequestSource::Scanner, HealPriority::Low, false);
    let first_id = first.id.clone();
    let key = PriorityHealQueue::make_dedup_key(&first);
    let second = object_request(HealRequestSource::Internal, HealPriority::Normal, true);
    let second_id = second.id.clone();
    assert_eq!(queue.push_with_replacement_scope(first, false, Some(31)), QueuePushOutcome::Accepted);
    assert_eq!(queue.push_with_replacement_scope(second, false, Some(32)), QueuePushOutcome::Accepted);
    let incoming = bucket_request("observation-displacement", HealPriority::Urgent, HealRequestSource::Internal);
    let displaced = queue
        .push_displacing_lower_priority_where(incoming, |_| true, Some(33))
        .expect("lower-priority holder should be displaced");
    assert_eq!(displaced.id, first_id);
    assert_eq!(queue.queued_scanner_owner_for_dedup_key(&key), Some((second_id.as_str(), Some(32))));
    assert_eq!(queue.queued_legacy_owner_for_dedup_key(&key), Some((second_id.as_str(), Some(32))));
}

#[tokio::test]
async fn missing_selected_token_or_inconsistent_index_stays_unknown() {
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
    let first = object_request(HealRequestSource::Scanner, HealPriority::High, false);
    let key = PriorityHealQueue::make_dedup_key(&first);
    let second = object_request(HealRequestSource::Internal, HealPriority::High, true);
    let second_id = second.id.clone();
    {
        let mut queue = manager.heal_queue.lock().await;
        assert_eq!(queue.push_with_replacement_scope(first, false, Some(41)), QueuePushOutcome::Accepted);
        assert_eq!(queue.push_with_replacement_scope(second, false, None), QueuePushOutcome::Accepted);
        queue.pop().expect("first holder should leave");
        assert_eq!(queue.queued_scanner_owner_for_dedup_key(&key), Some((second_id.as_str(), None)));
    }
    let receipt = manager
        .submit_heal_request_with_observation_and_alias(
            object_request(HealRequestSource::Scanner, HealPriority::High, false),
            false,
        )
        .await
        .expect("merge into owner without an observation token");
    assert_eq!(receipt.result, HealAdmissionResult::Merged);
    assert_eq!(receipt.task_id, second_id);
    assert_eq!(receipt.observation_id, None);

    let mut queue = PriorityHealQueue::new();
    let first = object_request(HealRequestSource::Scanner, HealPriority::High, false);
    let key = PriorityHealQueue::make_dedup_key(&first);
    let second = object_request(HealRequestSource::Internal, HealPriority::High, true);
    assert_eq!(queue.push_with_replacement_scope(first, false, Some(51)), QueuePushOutcome::Accepted);
    assert_eq!(queue.push_with_replacement_scope(second, false, Some(52)), QueuePushOutcome::Accepted);
    let ScannerQueuedOwners::Indexed(owners) = &mut queue.dedup_keys.get_mut(&key).expect("key").scanner_queued_owners else {
        panic!("forced holder should create an index");
    };
    let first_sequence = *owners.first_key_value().expect("first indexed holder").0;
    assert!(owners.remove(&first_sequence).is_some());
    assert_eq!(queue.queued_scanner_owner_for_dedup_key(&key), None);
    queue.pop().expect("first holder should leave");
    assert_eq!(queue.queued_scanner_owner_for_dedup_key(&key), None);
    assert_eq!(queue.queued_legacy_owner_for_dedup_key(&key).and_then(|(_, token)| token), None);
}
