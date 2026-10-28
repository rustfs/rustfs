use super::*;
use crate::heal::outcome::{HealObjectIdentity, HealObjectKind, HealObjectOutcome};
use rustfs_heal_contracts::{
    completion::{subscribe_heal_completions, subscribe_observed_heal_completions},
    heal_channel::HealScanMode,
};

#[test]
fn observation_requires_the_current_task_or_retry_object() {
    let request = bucket_request("observation-object-owner", HealPriority::Normal, HealRequestSource::Internal);
    let id = request.id.clone();
    let stale = Arc::new(HealTask::from_request(request.clone(), Arc::new(MockStorage)).with_observation_id(Some(301)));
    let current = Arc::new(HealTask::from_request(request.clone(), Arc::new(MockStorage)).with_observation_id(Some(302)));
    let active = HashMap::from([(id.clone(), Arc::clone(&current))]);
    assert_eq!(current_task_observation_id(&active, &stale), None);
    assert_eq!(current_task_observation_id(&active, &current), Some(302));

    let current_token = CancellationToken::new();
    let stale_token = CancellationToken::new();
    let retrying = HashMap::from([(
        id.clone(),
        RetryingHeal {
            request,
            observation_id: Some(303),
            error: "retrying".into(),
            cancel_token: current_token.clone(),
        },
    )]);
    assert_eq!(current_retry_observation_id(&retrying, &id, &stale_token), None);
    assert_eq!(current_retry_observation_id(&retrying, &id, &current_token), Some(303));
}

#[tokio::test]
async fn queued_same_id_replay_is_unknown_but_canonical_merge_retains_owner_identity() {
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
    let request = bucket_request("queued-id-replay-observation", HealPriority::Normal, HealRequestSource::Internal);
    let first = manager
        .submit_heal_request_with_observation_and_alias(request.clone(), false)
        .await
        .expect("first owner");
    assert!(first.observation_id.is_some());
    let replay = manager
        .submit_heal_request_with_observation_and_alias(request, false)
        .await
        .expect("same ID replay");
    assert_eq!(replay.result, HealAdmissionResult::Accepted);
    assert_eq!(replay.task_id, first.task_id);
    assert_eq!(replay.observation_id, None);

    let duplicate = bucket_request("queued-id-replay-observation", HealPriority::Normal, HealRequestSource::Scanner);
    let merged = manager
        .submit_heal_request_with_observation_and_alias(duplicate, false)
        .await
        .expect("canonical merge");
    assert_eq!(merged.result, HealAdmissionResult::Merged);
    assert_eq!(merged.task_id, first.task_id);
    assert_eq!(merged.observation_id, first.observation_id);
}

#[tokio::test]
async fn merged_receipt_uses_live_active_or_retrying_owner_identity() {
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
    let request = bucket_request("owner-observation-states", HealPriority::Normal, HealRequestSource::Internal);
    let id = request.id.clone();
    manager.active_heals.lock().await.insert(
        id.clone(),
        Arc::new(HealTask::from_request(request.clone(), Arc::new(MockStorage)).with_observation_id(Some(201))),
    );
    let scanner = bucket_request("owner-observation-states", HealPriority::Normal, HealRequestSource::Scanner);
    let active = manager
        .submit_heal_request_with_observation_and_alias(scanner.clone(), false)
        .await
        .expect("active merge");
    assert_eq!(active.result, HealAdmissionResult::Merged);
    assert_eq!(active.task_id, id);
    assert_eq!(active.observation_id, Some(201));
    manager.active_heals.lock().await.remove(&id);
    manager.retrying_heals.lock().await.insert(
        id.clone(),
        RetryingHeal {
            request,
            observation_id: Some(202),
            error: "retrying".into(),
            cancel_token: CancellationToken::new(),
        },
    );
    let mut scanner_retry = scanner;
    scanner_retry.id = Uuid::new_v4().to_string();
    let retrying = manager
        .submit_heal_request_with_observation_and_alias(scanner_retry, false)
        .await
        .expect("retry merge");
    assert_eq!(retrying.result, HealAdmissionResult::Merged);
    assert_eq!(retrying.task_id, id);
    assert_eq!(retrying.observation_id, Some(202));
}

#[tokio::test]
async fn cancelled_or_displaced_queue_owner_does_not_reuse_its_observation_identity() {
    for displace in [false, true] {
        let manager = HealManager::new_without_root_recovery_for_test(
            Arc::new(MockStorage),
            Some(HealConfig {
                queue_size: 1,
                event_driven_scheduler_enable: false,
                ..Default::default()
            }),
        );
        let request = bucket_request("removed-observation-owner", HealPriority::Low, HealRequestSource::Scanner);
        let old = manager
            .submit_heal_request_with_observation_and_alias(request.clone(), false)
            .await
            .expect("first owner");
        assert_eq!(old.result, HealAdmissionResult::Accepted);
        assert!(old.observation_id.is_some());
        if displace {
            let urgent = bucket_request("displacing-owner", HealPriority::Urgent, HealRequestSource::Internal);
            let urgent_id = urgent.id.clone();
            let admitted = manager
                .submit_heal_request_with_observation_and_alias(urgent, false)
                .await
                .expect("displacing owner");
            assert_eq!(admitted.result, HealAdmissionResult::Accepted);
            assert!(
                manager
                    .heal_queue
                    .lock()
                    .await
                    .requests()
                    .all(|queued| queued.id != old.task_id)
            );
            manager.cancel_task(&urgent_id).await.expect("free queue after displacement");
        } else {
            manager.cancel_task(&old.task_id).await.expect("cancel queued owner");
        }
        let next = manager
            .submit_heal_request_with_observation_and_alias(request, false)
            .await
            .expect("readmit same id");
        assert_eq!(next.result, HealAdmissionResult::Accepted);
        assert_eq!(next.task_id, old.task_id);
        assert!(next.observation_id.is_some());
        assert_ne!(next.observation_id, old.observation_id);
    }
}

#[tokio::test]
async fn reused_id_has_distinct_owner_generations_in_receipts_and_terminals() {
    let manager = HealManager::new_without_root_recovery_for_test(Arc::new(MockStorage), None);
    let mut receiver = subscribe_observed_heal_completions();
    let request = bucket_request("observation-id-reuse", HealPriority::Normal, HealRequestSource::Internal);

    let first = manager
        .submit_heal_request_with_observation_and_alias(request.clone(), false)
        .await
        .expect("first owner");
    let mut scanner = bucket_request("observation-id-reuse", HealPriority::Normal, HealRequestSource::Scanner);
    let merged = manager
        .submit_heal_request_with_observation_and_alias(scanner.clone(), false)
        .await
        .expect("first merge");
    assert_eq!(merged.result, HealAdmissionResult::Merged);
    assert_eq!(merged.task_id, first.task_id);
    assert!(first.observation_id.is_some());
    assert_eq!(merged.observation_id, first.observation_id);
    process_manager_queue_once(&manager).await;
    let first_terminal = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let terminal = receiver.recv().await.expect("terminal channel");
            if terminal.task_id == first.task_id {
                break terminal;
            }
        }
    })
    .await
    .expect("first terminal");
    assert_eq!(first_terminal.observation_id, first.observation_id);

    let second = manager
        .submit_heal_request_with_observation_and_alias(request, false)
        .await
        .expect("reused id");
    assert_eq!(second.result, HealAdmissionResult::Accepted);
    assert_eq!(second.task_id, first.task_id);
    assert_ne!(second.observation_id, first.observation_id);
    scanner.id = Uuid::new_v4().to_string();
    let merged_again = manager
        .submit_heal_request_with_observation_and_alias(scanner, false)
        .await
        .expect("second merge");
    assert_eq!(merged_again.result, HealAdmissionResult::Merged);
    assert_eq!(merged_again.observation_id, second.observation_id);
    process_manager_queue_once(&manager).await;
    let second_terminal = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let terminal = receiver.recv().await.expect("terminal channel");
            if terminal.task_id == second.task_id && terminal.observation_id == second.observation_id {
                break terminal;
            }
        }
    })
    .await
    .expect("second terminal");
    assert_eq!(second_terminal.observation_id, second.observation_id);
}

#[tokio::test]
async fn canonical_completion_publishes_typed_objects_only_for_terminal_untruncated_results() {
    let mut receiver = subscribe_heal_completions();
    let completed_heals = Mutex::new(HashMap::new());
    let aliases = Mutex::new(HashMap::new());
    let id = Uuid::new_v4().to_string();
    let alias = Uuid::new_v4().to_string();
    aliases
        .lock()
        .await
        .insert(alias.clone(), HealTaskAlias { task_id: id.clone() });
    let mut completed = completed_retention_fixture(SystemTime::now());
    completed.heal_type = HealType::Object {
        bucket: "bucket".into(),
        object: "object".into(),
        version_id: None,
    };
    completed.options.scan_mode = HealScanMode::Normal;
    let mut outcome = HealTaskOutcome::default();
    outcome.record(HealObjectOutcome {
        identity: HealObjectIdentity {
            kind: HealObjectKind::Object,
            bucket: "bucket".into(),
            object: "object".into(),
            version_id: None,
            bucket_incarnation_id: Some(Uuid::from_u128(44)),
            pool_index: Some(0),
            set_index: Some(1),
        },
        disposition: HealObjectDisposition::Deferred {
            reason: HealDeferredReason::Deadline,
            retry_not_before: None,
        },
        detail: None,
    });
    outcome.finish(None);
    completed.outcome = Some(Arc::new(outcome.clone()));
    completed.status = HealTaskStatus::Retrying {
        error: "retry".into(),
        retry_attempt: 1,
    };
    publish_completed_heal(&completed_heals, &aliases, &id, completed.clone(), false, Some(1)).await;
    while let Ok(observed) = receiver.try_recv() {
        assert_ne!(observed.task_id, id, "a retry attempt must not publish a terminal");
    }
    completed.status = HealTaskStatus::Completed;
    publish_completed_heal(&completed_heals, &aliases, &id, completed.clone(), true, Some(1)).await;
    let observed = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let event = receiver.recv().await.expect("terminal broadcaster stays open");
            if event.task_id == id {
                break event;
            }
        }
    })
    .await
    .expect("canonical terminal must be published");
    assert_eq!(observed.scan_mode, HealScanMode::Normal);
    assert_eq!(observed.execution, Some(HealExecutionOutcome::Completed));
    assert!(matches!(
        observed.object.as_ref().expect("single typed outcome").disposition,
        HealObjectDisposition::Deferred {
            reason: HealDeferredReason::Deadline,
            ..
        }
    ));
    assert!(completed_heals.lock().await.contains_key(&alias), "existing alias retention is preserved");

    outcome.objects_truncated = true;
    completed.outcome = Some(Arc::new(outcome));
    let truncated_id = Uuid::new_v4().to_string();
    publish_completed_heal(&completed_heals, &aliases, &truncated_id, completed, true, Some(1)).await;
    let truncated = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let event = receiver.recv().await.expect("terminal broadcaster stays open");
            if event.task_id == truncated_id {
                break event;
            }
        }
    })
    .await
    .expect("truncated terminal must still be observable");
    assert!(truncated.object.is_none(), "a retained tail cannot certify a whole result");
}
