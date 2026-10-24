use super::*;
use crate::heal::outcome::{HealObjectIdentity, HealObjectKind, HealObjectOutcome};
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
