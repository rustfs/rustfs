use super::*;
use crate::heal::outcome::{HealAbortReason, HealObjectIdentity, HealObjectKind, HealObjectOutcome};
use rustfs_heal_contracts::{completion::subscribe_heal_completions, heal_channel::HealScanMode};

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
        deep_check_observed: false,
        quorum_selected_data_dir: None,
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
    publish_completed_heal(&completed_heals, &aliases, &id, completed.clone(), false).await;
    while let Ok(observed) = receiver.try_recv() {
        assert_ne!(observed.task_id, id, "a retry attempt must not publish a terminal");
    }
    completed.status = HealTaskStatus::Completed;
    publish_completed_heal(&completed_heals, &aliases, &id, completed.clone(), true).await;
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
    publish_completed_heal(&completed_heals, &aliases, &truncated_id, completed, true).await;
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

#[tokio::test]
async fn completion_carries_local_generation_but_wire_roundtrip_does_not() {
    let mut receiver = subscribe_heal_completions();
    let completed_heals = Mutex::new(HashMap::new());
    let aliases = Mutex::new(HashMap::new());
    let generation = Uuid::new_v4();
    let mut outcome = HealTaskOutcome::default();
    outcome.record(HealObjectOutcome {
        deep_check_observed: true,
        identity: HealObjectIdentity {
            kind: HealObjectKind::Object,
            bucket: "bucket".into(),
            object: "object".into(),
            version_id: None,
            bucket_incarnation_id: Some(Uuid::new_v4()),
            pool_index: Some(0),
            set_index: Some(0),
        },
        disposition: HealObjectDisposition::VerifiedHealthy,
        detail: None,
        quorum_selected_data_dir: Some(generation),
    });
    outcome.finish(None);
    let wire = serde_json::to_value(&outcome).expect("serialize outcome");
    assert!(wire["objects"][0].get("quorumSelectedDataDir").is_none());
    assert!(wire["objects"][0].get("deepCheckObserved").is_none());
    let restored: HealTaskOutcome = serde_json::from_value(wire).expect("deserialize outcome");

    for (observed_outcome, expected_generation, expected_proof) in [(outcome, Some(generation), true), (restored, None, false)] {
        let id = Uuid::new_v4().to_string();
        let mut completed = completed_retention_fixture(SystemTime::now());
        completed.heal_type = HealType::Object {
            bucket: "bucket".into(),
            object: "object".into(),
            version_id: None,
        };
        completed.status = HealTaskStatus::Completed;
        completed.outcome = Some(Arc::new(observed_outcome));
        publish_completed_heal(&completed_heals, &aliases, &id, completed, true).await;
        let observed = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let event = receiver.recv().await.expect("terminal broadcaster stays open");
                if event.task_id == id {
                    break event;
                }
            }
        })
        .await
        .expect("terminal should be published");
        let receipt = observed.object.as_ref().expect("single typed outcome");
        assert_eq!(receipt.quorum_selected_data_dir, expected_generation);
        assert_eq!(receipt.deep_check_observed, expected_proof);
    }
}

#[tokio::test]
async fn completion_requires_success_and_a_non_nil_selection_for_deep_observation() {
    let mut receiver = subscribe_heal_completions();
    let completed_heals = Mutex::new(HashMap::new());
    let aliases = Mutex::new(HashMap::new());
    let generation = Uuid::new_v4();
    for (status, execution, dry_run, observed_generation) in [
        (
            HealTaskStatus::Cancelled,
            HealExecutionOutcome::Aborted(HealAbortReason::Cancelled),
            false,
            Some(generation),
        ),
        (
            HealTaskStatus::Failed { error: "failure".into() },
            HealExecutionOutcome::Completed,
            false,
            Some(generation),
        ),
        (
            HealTaskStatus::Completed,
            HealExecutionOutcome::Aborted(HealAbortReason::Deadline),
            false,
            Some(generation),
        ),
        (HealTaskStatus::Completed, HealExecutionOutcome::Completed, true, Some(generation)),
        (HealTaskStatus::Completed, HealExecutionOutcome::Completed, false, None),
        (HealTaskStatus::Completed, HealExecutionOutcome::Completed, false, Some(Uuid::nil())),
    ] {
        let id = Uuid::new_v4().to_string();
        let mut outcome = HealTaskOutcome::default();
        outcome.record(HealObjectOutcome {
            identity: HealObjectIdentity {
                kind: HealObjectKind::Object,
                bucket: "bucket".into(),
                object: "object".into(),
                version_id: None,
                bucket_incarnation_id: Some(Uuid::new_v4()),
                pool_index: Some(0),
                set_index: Some(0),
            },
            disposition: HealObjectDisposition::VerifiedHealthy,
            detail: None,
            quorum_selected_data_dir: observed_generation,
            deep_check_observed: true,
        });
        outcome.execution = execution;
        let mut completed = completed_retention_fixture(SystemTime::now());
        completed.heal_type = HealType::Object {
            bucket: "bucket".into(),
            object: "object".into(),
            version_id: None,
        };
        completed.options.dry_run = dry_run;
        completed.status = status;
        completed.outcome = Some(Arc::new(outcome));
        publish_completed_heal(&completed_heals, &aliases, &id, completed, true).await;
        let observed = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let event = receiver.recv().await.expect("terminal broadcaster stays open");
                if event.task_id == id {
                    break event;
                }
            }
        })
        .await
        .expect("terminal should be published");
        assert!(!observed.object.as_ref().expect("single typed outcome").deep_check_observed);
    }
}
