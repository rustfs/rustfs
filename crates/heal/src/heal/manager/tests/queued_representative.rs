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
    manager
        .active_heals
        .lock()
        .await
        .insert(first_id.clone(), Arc::new(HealTask::from_request(first, Arc::new(MockStorage))));

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
