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
use rustfs_heal_contracts::heal_channel::HealScanMode;
use std::future::Future;
use std::task::Poll;

#[derive(Clone, Copy, Debug)]
enum OwnerState {
    Active,
    Queued,
    Retrying,
}

async fn install_owner(manager: &HealManager, request: HealRequest, state: OwnerState) -> String {
    let id = request.id.clone();
    match state {
        OwnerState::Active => {
            insert_active_task(manager, request).await;
        }
        OwnerState::Queued => {
            assert_eq!(manager.heal_queue.lock().await.push(request), QueuePushOutcome::Accepted);
        }
        OwnerState::Retrying => {
            manager.retrying_heals.lock().await.insert(
                id.clone(),
                RetryingHeal {
                    request,
                    error: "recoverable fixture error".to_string(),
                    cancel_token: CancellationToken::new(),
                },
            );
        }
    }
    id
}

fn admin_object_request(object: &str) -> HealRequest {
    let mut request = HealRequest::object("bucket".to_string(), object.to_string(), None);
    request.source = HealRequestSource::Admin;
    request
}

#[tokio::test]
async fn admin_overlap_default_rejects_parent_child_without_new_owner() {
    for (existing, incoming) in [("scope/", "scope/child/"), ("scope/child/", "scope/")] {
        let manager = manager_with_policy(HealOverlapPolicy::Merge);
        let owner = insert_active_task(&manager, admin_prefix_request("bucket", existing)).await;
        let receipt = manager
            .submit_heal_request_with_receipt(admin_prefix_request("bucket", incoming))
            .await
            .expect("overlapping admin admission should return a typed decision");
        assert_eq!(
            receipt.result,
            HealAdmissionResult::Dropped(HealAdmissionDropReason::OverlappingPaths),
            "existing={existing}, incoming={incoming}"
        );
        assert_eq!(receipt.task_id, owner, "a rejection must identify the existing owner");
        assert!(manager.heal_queue.lock().await.is_empty(), "rejected starts must not enter the queue");
        assert_eq!(manager.active_heals.lock().await.len(), 1);
    }
}

#[tokio::test]
async fn admin_overlap_policy_matrix_covers_every_live_owner_state() {
    for state in [OwnerState::Active, OwnerState::Queued, OwnerState::Retrying] {
        for policy in [HealOverlapPolicy::Merge, HealOverlapPolicy::MinioError] {
            for (existing, incoming, relation) in [
                ("scope/", "scope/", OverlapVerdict::SameTarget),
                ("scope/", "scope/child/", OverlapVerdict::Overlapping),
                ("scope/child/", "scope/", OverlapVerdict::Overlapping),
                ("scope/child/", "scope/other/", OverlapVerdict::Disjoint),
            ] {
                let manager = manager_with_policy(policy);
                let owner = install_owner(&manager, admin_prefix_request("bucket", existing), state).await;
                let request = admin_prefix_request("bucket", incoming);
                let request_id = request.id.clone();
                let receipt = manager
                    .submit_heal_request_with_receipt(request)
                    .await
                    .expect("typed admission");
                let expected = match (relation, policy) {
                    (OverlapVerdict::Disjoint, _) => HealAdmissionResult::Accepted,
                    (OverlapVerdict::SameTarget, HealOverlapPolicy::Merge) => HealAdmissionResult::Merged,
                    (OverlapVerdict::SameTarget, _) => HealAdmissionResult::Dropped(HealAdmissionDropReason::AlreadyRunning),
                    (OverlapVerdict::Overlapping, _) => HealAdmissionResult::Dropped(HealAdmissionDropReason::OverlappingPaths),
                };
                assert_eq!(receipt.result, expected, "{state:?} {policy:?} {existing} -> {incoming}");
                assert_eq!(
                    receipt.task_id,
                    if relation == OverlapVerdict::Disjoint {
                        request_id.clone()
                    } else {
                        owner
                    }
                );
                if matches!(expected, HealAdmissionResult::Dropped(_)) {
                    assert!(
                        !manager
                            .heal_queue
                            .lock()
                            .await
                            .requests()
                            .any(|queued| queued.id == request_id)
                    );
                    assert!(manager.task_aliases.lock().await.is_empty(), "rejection must not create a token alias");
                }
            }
        }
    }
}

#[tokio::test]
async fn admin_overlap_incompatible_options_conflict_without_replacing_settings() {
    for state in [OwnerState::Active, OwnerState::Queued, OwnerState::Retrying] {
        for option in ["dry_run", "scan_mode", "remove", "recreate", "parity", "recursive"] {
            let manager = manager_with_policy(HealOverlapPolicy::Merge);
            let original = admin_prefix_request("bucket", "scope/");
            let expected_options = original.options.clone();
            let owner = install_owner(&manager, original, state).await;
            let mut request = admin_prefix_request("bucket", "scope/");
            match option {
                "dry_run" => request.options.dry_run = true,
                "scan_mode" => request.options.scan_mode = HealScanMode::Deep,
                "remove" => request.options.remove_corrupted = true,
                "recreate" => request.options.recreate_missing = false,
                "parity" => request.options.update_parity = false,
                "recursive" => request.options.recursive = true,
                _ => unreachable!("fixture option"),
            }
            let receipt = manager
                .submit_heal_request_with_receipt(request)
                .await
                .expect("option conflict");
            assert_eq!(
                receipt.result,
                HealAdmissionResult::Dropped(HealAdmissionDropReason::AlreadyRunning),
                "{state:?} {option}"
            );
            assert_eq!(receipt.task_id, owner);
            assert_eq!(
                manager
                    .get_task_report(&owner)
                    .await
                    .expect("original settings remain queryable")
                    .options,
                Some(expected_options)
            );
        }
    }
}

#[tokio::test]
async fn admin_overlap_consumed_timeout_does_not_break_equivalent_token_reuse() {
    let manager = manager_with_policy(HealOverlapPolicy::Merge);
    let mut original = admin_prefix_request("bucket", "scope/");
    original.options.timeout = Some(Duration::from_secs(17));
    let owner = install_owner(&manager, original, OwnerState::Retrying).await;
    let receipt = manager
        .submit_heal_request_with_receipt(admin_prefix_request("bucket", "scope/"))
        .await
        .expect("retry owner retains its token");
    assert_eq!(receipt.result, HealAdmissionResult::Merged);
    assert_eq!(receipt.task_id, owner);
    assert_eq!(
        manager.retrying_heals.lock().await[&owner].request.options.timeout,
        Some(Duration::from_secs(17))
    );
}

#[tokio::test]
async fn admin_overlap_typed_s3_targets_do_not_confuse_objects_and_prefixes() {
    let object = |key: &str| admin_object_request(key).heal_type;
    let prefix = |key: &str| admin_prefix_request("bucket", key).heal_type;
    let erasure_set = HealType::ErasureSet {
        buckets: vec![],
        set_disk_id: "pool_0_set_1".to_string(),
    };
    for (left, right, expected) in [
        (object("foo"), object("foobar"), OverlapVerdict::Disjoint),
        (prefix("foo"), prefix("foobar"), OverlapVerdict::Overlapping),
        (prefix("foo/"), object("foobar"), OverlapVerdict::Disjoint),
        (object("foo"), prefix("foo/"), OverlapVerdict::Disjoint),
        (prefix("scope/"), object("scope/child"), OverlapVerdict::Overlapping),
        (object("/foo"), object("foo"), OverlapVerdict::Disjoint),
        (object("foo/"), object("foo"), OverlapVerdict::Disjoint),
        (prefix("scope%2F"), prefix("scope/"), OverlapVerdict::Disjoint),
        (prefix("中文/"), object("中文/文件"), OverlapVerdict::Overlapping),
        (HealType::Cluster, prefix("scope/"), OverlapVerdict::Overlapping),
        (
            HealType::Bucket {
                bucket: "bucket".to_string(),
            },
            prefix("scope/"),
            OverlapVerdict::Overlapping,
        ),
        (erasure_set.clone(), prefix("scope/"), OverlapVerdict::Overlapping),
        (
            HealType::ErasureSet {
                buckets: vec!["other".to_string()],
                set_disk_id: "pool_0_set_1".to_string(),
            },
            prefix("scope/"),
            OverlapVerdict::Disjoint,
        ),
        (
            HealType::Object {
                bucket: "bucket".to_string(),
                object: "foo".to_string(),
                version_id: Some("version-1".to_string()),
            },
            HealType::Object {
                bucket: "bucket".to_string(),
                object: "foo".to_string(),
                version_id: Some("version-2".to_string()),
            },
            OverlapVerdict::Overlapping,
        ),
    ] {
        for (existing, incoming) in [(&left, &right), (&right, &left)] {
            let manager = manager_with_policy(HealOverlapPolicy::Merge);
            let mut owner_request = HealRequest::new(existing.clone(), HealOptions::default(), HealPriority::Normal);
            owner_request.source = HealRequestSource::Admin;
            install_owner(&manager, owner_request, OwnerState::Active).await;
            let mut request = HealRequest::new(incoming.clone(), HealOptions::default(), HealPriority::Normal);
            request.source = HealRequestSource::Admin;
            let receipt = manager
                .submit_heal_request_with_receipt(request)
                .await
                .expect("typed target admission");
            assert_eq!(
                receipt.result,
                if expected == OverlapVerdict::Disjoint {
                    HealAdmissionResult::Accepted
                } else {
                    HealAdmissionResult::Dropped(HealAdmissionDropReason::OverlappingPaths)
                },
                "{existing:?} -> {incoming:?}"
            );
        }
    }
}

#[tokio::test]
async fn admin_overlap_pool_set_scope_is_consistent_across_owner_states() {
    for state in [OwnerState::Active, OwnerState::Queued, OwnerState::Retrying] {
        for object in [false, true] {
            for (incoming_pool, incoming_set, expected) in [
                (Some(0), Some(1), HealAdmissionResult::Merged),
                (Some(0), Some(2), HealAdmissionResult::Accepted),
                (Some(1), Some(1), HealAdmissionResult::Accepted),
                (Some(0), None, HealAdmissionResult::Dropped(HealAdmissionDropReason::OverlappingPaths)),
                (None, None, HealAdmissionResult::Dropped(HealAdmissionDropReason::OverlappingPaths)),
            ] {
                let make_request = || {
                    if object {
                        admin_object_request("scope/object")
                    } else {
                        admin_prefix_request("bucket", "scope/")
                    }
                };
                let manager = manager_with_policy(HealOverlapPolicy::Merge);
                let mut original = make_request();
                original.options.pool_index = Some(0);
                original.options.set_index = Some(1);
                let owner = install_owner(&manager, original, state).await;
                let mut incoming = make_request();
                incoming.options.pool_index = incoming_pool;
                incoming.options.set_index = incoming_set;
                let id = incoming.id.clone();
                let receipt = manager
                    .submit_heal_request_with_receipt(incoming)
                    .await
                    .expect("scoped admission");
                assert_eq!(
                    receipt.result, expected,
                    "{state:?} object={object}, pool={incoming_pool:?}, set={incoming_set:?}"
                );
                assert_eq!(
                    receipt.task_id,
                    if expected == HealAdmissionResult::Accepted {
                        id
                    } else {
                        owner
                    }
                );
            }
        }
    }
}

#[tokio::test]
async fn admin_overlap_background_admission_and_strict_policy_keep_their_boundaries() {
    for policy in [HealOverlapPolicy::Merge, HealOverlapPolicy::MinioError] {
        for background in [
            HealRequestSource::Scanner,
            HealRequestSource::AutoHeal,
            HealRequestSource::ReadRepair,
            HealRequestSource::Internal,
        ] {
            let manager = manager_with_policy(policy);
            install_owner(&manager, admin_prefix_request("bucket", "scope/"), OwnerState::Active).await;
            let mut request = admin_prefix_request("bucket", "scope/child/");
            request.source = background;
            assert_eq!(
                manager
                    .submit_heal_request(request)
                    .await
                    .expect("background source remains admitted"),
                HealAdmissionResult::Accepted
            );

            let manager = manager_with_policy(policy);
            let mut original = admin_prefix_request("bucket", "scope/");
            original.source = background;
            install_owner(&manager, original, OwnerState::Active).await;
            assert_eq!(
                manager
                    .submit_heal_request(admin_prefix_request("bucket", "scope/child/"))
                    .await
                    .expect("admin policy decision"),
                if policy == HealOverlapPolicy::Merge {
                    HealAdmissionResult::Accepted
                } else {
                    HealAdmissionResult::Dropped(HealAdmissionDropReason::OverlappingPaths)
                }
            );
        }
    }
}

#[tokio::test]
async fn admin_overlap_force_start_cancels_every_overlapping_state_and_keeps_disjoint_work() {
    for policy in [HealOverlapPolicy::Merge, HealOverlapPolicy::MinioError] {
        let manager = manager_with_policy(policy);
        let active = install_owner(&manager, admin_prefix_request("bucket", "scope/active/"), OwnerState::Active).await;
        let queued = install_owner(&manager, admin_prefix_request("bucket", "scope/queued/"), OwnerState::Queued).await;
        let retrying = install_owner(&manager, admin_prefix_request("bucket", "scope/retrying/"), OwnerState::Retrying).await;
        let retry_cancel = manager.retrying_heals.lock().await[&retrying].cancel_token.clone();
        let disjoint = install_owner(&manager, admin_prefix_request("bucket", "other/"), OwnerState::Queued).await;
        let mut replacement = admin_prefix_request("bucket", "scope/");
        replacement.force_start = true;
        let replacement_id = replacement.id.clone();
        let receipt = manager
            .submit_heal_request_with_receipt(replacement)
            .await
            .expect("replace overlapping owners");
        assert_eq!(receipt.result, HealAdmissionResult::Accepted);
        assert_eq!(receipt.task_id, replacement_id);
        assert_eq!(
            manager.get_task_status(&active).await.expect("active cancellation retained"),
            HealTaskStatus::Cancelled
        );
        assert!(retry_cancel.is_cancelled());
        assert!(!manager.retrying_heals.lock().await.contains_key(&retrying));
        let ids = manager
            .heal_queue
            .lock()
            .await
            .requests()
            .map(|request| request.id.clone())
            .collect::<HashSet<_>>();
        assert_eq!(ids, HashSet::from([disjoint, replacement_id]));
        assert!(!ids.contains(&queued));
    }
}

#[tokio::test]
async fn admin_overlap_concurrent_parent_child_starts_admit_exactly_one_owner() {
    for _ in 0..8 {
        let manager = manager_with_policy(HealOverlapPolicy::Merge);
        let (parent, child) = tokio::join!(
            manager.submit_heal_request_with_receipt(admin_prefix_request("bucket", "scope/")),
            manager.submit_heal_request_with_receipt(admin_prefix_request("bucket", "scope/child/"))
        );
        let receipts = [parent.expect("parent decision"), child.expect("child decision")];
        assert_eq!(
            receipts
                .iter()
                .filter(|receipt| receipt.result == HealAdmissionResult::Accepted)
                .count(),
            1
        );
        assert_eq!(
            receipts
                .iter()
                .filter(|receipt| receipt.result == HealAdmissionResult::Dropped(HealAdmissionDropReason::OverlappingPaths))
                .count(),
            1
        );
        assert_eq!(receipts[0].task_id, receipts[1].task_id);
        assert_eq!(manager.heal_queue.lock().await.len(), 1);
    }
}

#[tokio::test]
async fn admin_overlap_normal_start_cannot_enter_force_start_cancellation_window() {
    let manager = manager_with_policy(HealOverlapPolicy::Merge);
    install_owner(&manager, admin_prefix_request("bucket", "scope/child/"), OwnerState::Active).await;
    // Cancellation resolves aliases before acquiring runtime state. Holding
    // this lock leaves the registry available inside the replacement window.
    let aliases = manager.task_aliases.lock().await;
    let mut replacement = admin_prefix_request("bucket", "scope/");
    replacement.force_start = true;
    let mut forced = Box::pin(manager.submit_heal_request_with_receipt(replacement));
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            assert!(
                std::pin::pin!(tokio::task::unconstrained(forced.as_mut()))
                    .poll(cx)
                    .is_pending()
            );
            if manager.active_heals.try_lock().is_ok() {
                Poll::Ready(())
            } else {
                // Another test can briefly hold the shared admission probe lock.
                // Drive the start until cancellation is blocked on our alias gate.
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }),
    )
    .await
    .expect("forceStart reaches cancellation with registry locks released");
    let mut normal = Box::pin(manager.submit_heal_request_with_receipt(admin_prefix_request("bucket", "scope/other/")));
    assert!(
        futures::poll!(tokio::task::unconstrained(normal.as_mut())).is_pending(),
        "ordinary START must wait for the replacement gate"
    );
    drop(aliases);
    let (forced, normal) = tokio::join!(forced, normal);
    assert_eq!(forced.expect("forced decision").result, HealAdmissionResult::Accepted);
    assert_eq!(
        normal.expect("normal decision").result,
        HealAdmissionResult::Dropped(HealAdmissionDropReason::OverlappingPaths)
    );
    assert_eq!(manager.heal_queue.lock().await.len(), 1);
}

#[tokio::test]
async fn admin_overlap_cancel_cannot_miss_a_queued_to_active_transition() {
    let manager = manager_with_policy(HealOverlapPolicy::Merge);
    let owner = install_owner(&manager, admin_prefix_request("bucket", "scope/"), OwnerState::Queued).await;
    let retrying = manager.retrying_heals.lock().await;
    let mut cancelled = Box::pin(manager.cancel_task(&owner));
    assert!(futures::poll!(cancelled.as_mut()).is_pending());
    let mut scheduled = Box::pin(process_manager_queue_once(&manager));
    assert!(
        futures::poll!(scheduled.as_mut()).is_pending(),
        "scheduler cannot move the owner between cancellation lookups"
    );
    drop(retrying);
    let (cancelled, ()) = tokio::join!(cancelled, scheduled);
    cancelled.expect("queued owner is cancelled");
    assert!(manager.heal_queue.lock().await.is_empty());
    assert!(!manager.active_heals.lock().await.contains_key(&owner));
}

#[tokio::test]
async fn admin_overlap_forced_request_id_replay_has_no_cancellation_side_effects() {
    let manager = manager_with_policy(HealOverlapPolicy::Merge);
    let mut original = admin_prefix_request("bucket", "scope/");
    original.force_start = true;
    let original_id = install_owner(&manager, original.clone(), OwnerState::Queued).await;
    // Older releases can leave independently accepted, overlapping owners.
    let other_id = install_owner(&manager, admin_prefix_request("bucket", "scope/child/"), OwnerState::Active).await;
    let receipt = manager
        .submit_heal_request_with_receipt(original)
        .await
        .expect("replay original receipt");
    assert_eq!(receipt.result, HealAdmissionResult::Accepted);
    assert_eq!(receipt.task_id, original_id);
    assert!(
        manager.active_heals.lock().await.contains_key(&other_id),
        "receipt replay must not repeat cancellation"
    );
    assert_eq!(manager.heal_queue.lock().await.len(), 1);
}

struct RetryQueueHook {
    reached: Notify,
    release: Notify,
    resumed: Notify,
}

static RETRY_QUEUE_HOOKS: LazyLock<StdMutex<HashMap<String, Arc<RetryQueueHook>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

pub(in crate::heal::manager) async fn pause_before_retry_queue(task_id: &str) {
    let hook = RETRY_QUEUE_HOOKS.lock().expect("retry queue hooks").get(task_id).cloned();
    if let Some(hook) = hook {
        hook.reached.notify_one();
        hook.release.notified().await;
        hook.resumed.notify_one();
    }
}

#[tokio::test]
async fn admin_overlap_cancelled_retry_cannot_requeue_after_its_backoff_checks() {
    let manager = manager_with_policy(HealOverlapPolicy::Merge);
    let mut original = admin_object_request("object");
    original.heal_type = HealType::Object {
        bucket: "retry-transition".to_string(),
        object: "object".to_string(),
        version_id: None,
    };
    let owner = original.id.clone();
    let hook = Arc::new(RetryQueueHook {
        reached: Notify::new(),
        release: Notify::new(),
        resumed: Notify::new(),
    });
    RETRY_QUEUE_HOOKS
        .lock()
        .expect("install retry hook")
        .insert(owner.clone(), Arc::clone(&hook));
    manager
        .submit_heal_request(original)
        .await
        .expect("admit retryable object heal");
    process_manager_queue_once(&manager).await;
    tokio::time::timeout(Duration::from_secs(10), hook.reached.notified())
        .await
        .expect("real executor reaches retry queue acquisition");
    manager
        .cancel_task(&owner)
        .await
        .expect("cancel after the retry ownership prechecks");
    hook.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), hook.resumed.notified())
        .await
        .expect("retry worker resumes");
    // The test runs on a single-threaded runtime. Once this notification is
    // observed, the worker has executed the uncontended queue ownership check.
    assert!(manager.heal_queue.lock().await.is_empty(), "cancelled retry must not be republished");
    assert!(!manager.retrying_heals.lock().await.contains_key(&owner));
    RETRY_QUEUE_HOOKS.lock().expect("remove retry hook").remove(&owner);
}
