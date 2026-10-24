use super::*;
use rustfs_heal_contracts::outcome::{HealAbortReason, HealDeferredReason, HealFailureClass, HealObjectReceipt};
use uuid::Uuid;

fn request() -> HealChannelRequest {
    HealChannelRequest {
        bucket: "bucket".into(),
        object_prefix: Some("object".into()),
        object_version_id: Some(Uuid::from_u128(11).to_string()),
        expected_bucket_incarnation_id: Some(Uuid::from_u128(22)),
        pool_index: Some(0),
        set_index: Some(1),
        scan_mode: Some(HealScanMode::Deep),
        ..HealChannelRequest::new()
    }
}

fn candidate() -> Candidate {
    Candidate::from_request(&request()).expect("bounded candidate")
}

fn completion(task: &str, disposition: HealObjectDisposition) -> Arc<HealCompletion> {
    Arc::new(HealCompletion {
        task_id: task.into(),
        scan_mode: HealScanMode::Deep,
        dry_run: false,
        execution: Some(HealExecutionOutcome::Completed),
        object: Some(HealObjectReceipt {
            identity: candidate().identity.expect("exact object identity"),
            disposition,
        }),
    })
}

#[test]
fn merged_candidates_close_only_on_the_canonical_task_once() {
    let mut observations = Observations::default();
    assert!(observations.admit("owner".into(), candidate()).is_none());
    assert!(observations.admit("owner".into(), candidate()).is_none());
    assert_eq!(observations.pending.len(), 1);
    assert!(
        observations
            .complete(completion("discarded-id", HealObjectDisposition::VerifiedHealthy), Instant::now())
            .is_none()
    );
    assert_eq!(observations.pending.len(), 1);
    let terminal = completion("owner", HealObjectDisposition::VerifiedHealthy);
    let settled = observations
        .complete(terminal.clone(), Instant::now())
        .expect("canonical completion settles both candidates");
    assert_eq!(settled.candidates, 2);
    assert!(settled.first_task_observation);
    assert_eq!(settled.scope, "content_and_leader_unknown");
    assert!(observations.pending.is_empty());
    assert!(
        observations.complete(terminal, Instant::now()).is_none(),
        "duplicate completion is ignored"
    );
    let merged_late = observations
        .admit("owner".into(), candidate())
        .expect("terminal before merged admission is retained");
    assert!(!merged_late.first_task_observation);
    assert_eq!(merged_late.candidates, 1);
}

#[test]
fn terminal_before_admission_is_observed_without_claiming_coverage() {
    let mut observations = Observations::default();
    assert!(
        observations
            .complete(completion("fast", HealObjectDisposition::Repaired), Instant::now())
            .is_none()
    );
    let settled = observations
        .admit("fast".into(), candidate())
        .expect("fast terminal retained");
    assert_eq!(settled.disposition, "repaired");
    assert_eq!(settled.scope, "content_and_leader_unknown");
    assert!(settled.first_task_observation);
}

#[test]
fn replacement_bucket_version_or_location_cannot_match_an_old_result() {
    for change in 0..4 {
        let mut observed = candidate();
        let identity = observed.identity.as_mut().expect("candidate identity");
        match change {
            0 => identity.bucket_incarnation_id = Some(Uuid::from_u128(23)),
            1 => identity.version_id = Some(Uuid::from_u128(12).to_string()),
            2 => identity.pool_index = Some(1),
            _ => identity.set_index = Some(2),
        }
        let mut recent = Recent {
            completion: completion("old", HealObjectDisposition::VerifiedHealthy),
            counted: false,
        };
        assert_eq!(Settlement::new(observed, &mut recent).scope, "identity_mismatch");
    }
}

#[test]
fn unversioned_overwrite_and_unknown_leader_never_certify_payload() {
    let mut observed = candidate();
    observed.identity.as_mut().expect("identity").version_id = None;
    let mut terminal = (*completion("same-name", HealObjectDisposition::VerifiedHealthy)).clone();
    terminal.object.as_mut().expect("receipt").identity.version_id = None;
    let mut recent = Recent {
        completion: Arc::new(terminal),
        counted: false,
    };
    assert_eq!(Settlement::new(observed, &mut recent).scope, "identity_unknown");
    let mut recent = Recent {
        completion: completion("old-leader", HealObjectDisposition::VerifiedHealthy),
        counted: false,
    };
    assert_eq!(Settlement::new(candidate(), &mut recent).scope, "content_and_leader_unknown");
}

#[test]
fn normal_metadata_and_unproven_repairs_do_not_satisfy_deep_candidates() {
    for disposition in [HealObjectDisposition::MetadataHealthy, HealObjectDisposition::Repaired] {
        let mut terminal = (*completion("normal", disposition)).clone();
        terminal.scan_mode = HealScanMode::Normal;
        let mut recent = Recent {
            completion: Arc::new(terminal),
            counted: false,
        };
        assert_eq!(Settlement::new(candidate(), &mut recent).scope, "insufficient_scan");
    }
}

#[test]
fn command_success_does_not_hide_deferred_failed_cancelled_or_dry_run_objects() {
    for (disposition, label) in [
        (
            HealObjectDisposition::Deferred {
                reason: HealDeferredReason::Deadline,
                retry_not_before: None,
            },
            "deferred",
        ),
        (HealObjectDisposition::Failed(HealFailureClass::Permanent), "failed"),
        (HealObjectDisposition::Cancelled, "cancelled"),
        (HealObjectDisposition::DryRunObserved, "dry_run"),
        (HealObjectDisposition::AuthoritativelyAbsent, "absent"),
    ] {
        let mut recent = Recent {
            completion: completion("completed-command", disposition),
            counted: false,
        };
        let settled = Settlement::new(candidate(), &mut recent);
        assert_eq!(settled.disposition, label);
        assert_eq!(settled.scope, "content_and_leader_unknown");
    }
    let mut terminal = (*completion("cancelled", HealObjectDisposition::VerifiedHealthy)).clone();
    terminal.execution = Some(HealExecutionOutcome::Aborted(HealAbortReason::Cancelled));
    let mut recent = Recent {
        completion: Arc::new(terminal),
        counted: false,
    };
    assert_eq!(Settlement::new(candidate(), &mut recent).scope, "execution_incomplete");
}

#[test]
fn missing_or_truncated_object_result_remains_unknown() {
    let mut terminal = (*completion("truncated", HealObjectDisposition::VerifiedHealthy)).clone();
    terminal.object = None;
    let mut recent = Recent {
        completion: Arc::new(terminal),
        counted: false,
    };
    assert_eq!(Settlement::new(candidate(), &mut recent).scope, "identity_unknown");
}

#[test]
fn overflow_loss_expiry_and_restart_are_bounded_and_do_not_restore_pending() {
    let mut observations = Observations::default();
    for index in 0..=MAX_PENDING {
        observations.admit(format!("pending-{index}"), candidate());
    }
    assert_eq!(observations.pending.len(), MAX_PENDING);
    assert!(!observations.pending.contains_key(&format!("pending-{MAX_PENDING}")));
    observations.lost();
    assert!(observations.pending.is_empty());
    assert!(
        observations
            .complete(completion("pending-0", HealObjectDisposition::VerifiedHealthy), Instant::now())
            .is_none()
    );
    observations.admit("expires".into(), candidate());
    observations.maintain(Instant::now() + PENDING_TTL);
    assert!(observations.pending.is_empty());
    assert!(observations.recent.is_empty());
    let mut restarted = Observations::default();
    assert!(
        restarted
            .complete(completion("expires", HealObjectDisposition::VerifiedHealthy), Instant::now())
            .is_none()
    );
}

#[test]
fn terminal_history_eviction_loses_evidence_without_inventing_a_completion() {
    let mut observations = Observations::default();
    for index in 0..=MAX_RECENT {
        observations.complete(
            completion(&format!("terminal-{index}"), HealObjectDisposition::VerifiedHealthy),
            Instant::now(),
        );
    }
    assert_eq!(observations.recent.len(), MAX_RECENT);
    assert_eq!(observations.recent_order.len(), MAX_RECENT);
    assert!(observations.admit("terminal-0".into(), candidate()).is_none());
    assert!(observations.pending.contains_key("terminal-0"));
}

#[test]
fn identity_size_is_bounded_before_cloning() {
    let mut large = request();
    large.object_prefix = Some("x".repeat(MAX_COMPLETION_IDENTITY_BYTES));
    assert!(Candidate::from_request(&large).is_none());
}

#[test]
fn observer_reinstalls_after_runtime_teardown() {
    let registry = Mutex::new(None);
    let first = {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("first runtime");
        runtime.block_on(async { observer_from(&registry) })
    };
    assert!(first.is_closed(), "runtime teardown closes the old observer");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("replacement runtime");
    runtime.block_on(async {
        let replacement = observer_from(&registry);
        assert!(!replacement.is_closed(), "a stale global sender must not suppress a new observer");
        replacement
            .try_send(("new-runtime".into(), candidate()))
            .expect("replacement observer admits work");
    });
}

#[derive(Default)]
struct CompletionRecorder(Mutex<HashMap<metrics::Key, Arc<std::sync::atomic::AtomicU64>>>);

impl metrics::Recorder for CompletionRecorder {
    fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::from_arc(
            self.0
                .lock()
                .expect("completion recorder")
                .entry(key.clone())
                .or_default()
                .clone(),
        )
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

impl CompletionRecorder {
    fn total(&self, metric: &str) -> u64 {
        self.0
            .lock()
            .expect("completion recorder")
            .iter()
            .filter(|(key, _)| key.name() == metric)
            .map(|(_, value)| value.load(std::sync::atomic::Ordering::Relaxed))
            .sum()
    }

    async fn wait_for_terminals(&self, expected: u64) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.total("rustfs_scanner_heal_candidate_terminal_total") < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the live observer must settle admitted candidates");
    }
}

// A local installation avoids sharing another test runtime's observer. The
// current-thread runtime keeps its spawned observer under this local recorder.
#[tokio::test]
async fn live_observer_associates_early_completion_with_canonical_merged_receipts() {
    use rustfs_heal_contracts::{completion::publish_heal_completion, heal_channel::HealAdmissionResult};

    let recorder = CompletionRecorder::default();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    let registry = Mutex::new(None);
    let sender = observer_from(&registry);
    let canonical_id = Uuid::new_v4().to_string();
    let sentinel_id = Uuid::new_v4().to_string();
    let source_request = request();
    assert_ne!(source_request.id, canonical_id, "merged requests retain their owner's ID");

    observe_on(
        &sender,
        Some(candidate()),
        Ok(&HealAdmissionReceipt {
            result: HealAdmissionResult::Accepted,
            task_id: sentinel_id.clone(),
        }),
    );
    publish_heal_completion((*completion(&canonical_id, HealObjectDisposition::VerifiedHealthy)).clone());
    // FIFO on the real broadcast channel makes this terminal an acknowledgement
    // that the earlier unmatched completion was consumed before its admission.
    publish_heal_completion((*completion(&sentinel_id, HealObjectDisposition::MetadataHealthy)).clone());
    recorder.wait_for_terminals(1).await;

    let merged = HealAdmissionReceipt {
        result: HealAdmissionResult::Merged,
        task_id: canonical_id,
    };
    observe_on(&sender, Candidate::from_request(&source_request), Ok(&merged));
    recorder.wait_for_terminals(2).await;
    observe_on(&sender, Candidate::from_request(&source_request), Ok(&merged));
    recorder.wait_for_terminals(3).await;

    assert_eq!(recorder.total("rustfs_scanner_heal_candidate_terminal_total"), 3);
    assert_eq!(
        recorder.total("rustfs_scanner_heal_task_terminal_observations_total"),
        2,
        "two merged candidates must add only one canonical task observation"
    );
    assert_eq!(
        recorder.total("rustfs_scanner_heal_candidate_unknown_total"),
        3,
        "real typed terminals still lack content and leader proof"
    );
    let metrics = recorder.0.lock().expect("completion recorder");
    let (_, verified_healthy) = metrics
        .iter()
        .find(|(key, _)| {
            key.name() == "rustfs_scanner_heal_candidate_terminal_total"
                && key
                    .labels()
                    .any(|label| label.key() == "disposition" && label.value() == "verified_healthy")
        })
        .expect("typed storage result must reach the real terminal metric");
    assert_eq!(verified_healthy.load(std::sync::atomic::Ordering::Relaxed), 2);
    for (key, _) in metrics
        .iter()
        .filter(|(key, _)| key.name() == "rustfs_scanner_heal_candidate_terminal_total")
    {
        assert!(
            key.labels()
                .any(|label| label.key() == "scope" && label.value() == "content_and_leader_unknown")
        );
    }
}
