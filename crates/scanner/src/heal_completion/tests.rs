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
fn missing_identity_fields_are_unknown_on_either_side() {
    type IdentityChange = fn(&mut HealObjectIdentity);
    let changes: [(&str, IdentityChange); 6] = [
        ("version", |identity| identity.version_id = None),
        ("empty version", |identity| identity.version_id = Some(String::new())),
        ("incarnation", |identity| identity.bucket_incarnation_id = None),
        ("nil incarnation", |identity| identity.bucket_incarnation_id = Some(Uuid::nil())),
        ("pool", |identity| identity.pool_index = None),
        ("set", |identity| identity.set_index = None),
    ];
    for (label, change) in changes {
        for candidate_missing in [true, false] {
            let mut observed = candidate();
            let mut terminal = (*completion("partial", HealObjectDisposition::VerifiedHealthy)).clone();
            if candidate_missing {
                change(observed.identity.as_mut().expect("candidate identity"));
            } else {
                change(&mut terminal.object.as_mut().expect("receipt").identity);
            }
            let mut recent = Recent {
                completion: Arc::new(terminal),
                counted: false,
            };
            assert_eq!(
                Settlement::new(observed, &mut recent).scope,
                "identity_unknown",
                "{label}, candidate missing: {candidate_missing}"
            );
        }
    }
}

#[test]
fn known_identity_conflicts_are_not_hidden_by_other_missing_fields() {
    for change in 0..3 {
        let mut observed = candidate();
        let mut terminal = (*completion("conflict", HealObjectDisposition::VerifiedHealthy)).clone();
        let expected = observed.identity.as_mut().expect("candidate identity");
        let actual = &mut terminal.object.as_mut().expect("receipt").identity;
        match change {
            0 => {
                expected.version_id = None;
                actual.bucket = "replacement".into();
            }
            1 => {
                expected.pool_index = None;
                actual.version_id = Some(Uuid::from_u128(12).to_string());
            }
            _ => {
                expected.bucket_incarnation_id = None;
                actual.set_index = Some(2);
            }
        }
        let mut recent = Recent {
            completion: Arc::new(terminal),
            counted: false,
        };
        assert_eq!(Settlement::new(observed, &mut recent).scope, "identity_mismatch", "case {change}");
    }
}

#[test]
fn scanner_candidate_without_location_records_identity_unknown() {
    let mut scanner_request = request();
    scanner_request.expected_bucket_incarnation_id = None;
    scanner_request.pool_index = None;
    scanner_request.set_index = None;
    let mut observations = Observations::default();
    assert!(
        observations
            .admit(
                "scanner".into(),
                Candidate::from_request(&scanner_request).expect("bounded scanner candidate"),
            )
            .is_none()
    );
    let settlement = observations
        .complete(completion("scanner", HealObjectDisposition::VerifiedHealthy), Instant::now())
        .expect("terminal candidate observed");
    assert_eq!(settlement.scope, "identity_unknown");
    assert_eq!(observations.unknown_counts.get("identity_unknown"), Some(&1));
    assert_eq!(observations.unknown_counts.get("identity_mismatch"), None);
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
    assert_eq!(Settlement::new(candidate(), &mut recent).execution, "aborted_cancelled");
    let mut errors = (*completion("errors", HealObjectDisposition::Repaired)).clone();
    errors.execution = Some(HealExecutionOutcome::CompletedWithErrors);
    let mut recent = Recent {
        completion: Arc::new(errors),
        counted: false,
    };
    let settled = Settlement::new(candidate(), &mut recent);
    assert_eq!(settled.execution, "completed_with_errors");
    assert_eq!(settled.scope, "execution_incomplete");
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
    assert!(first.sender.is_closed(), "runtime teardown closes the old observer");
    let stopped = observation_status_from(&registry);
    assert_eq!(stopped.state, "stopped");
    assert_eq!(stopped.retained_pending_tasks, None);
    assert_eq!(stopped.integrity_state, "unknown");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("replacement runtime");
    runtime.block_on(async {
        let replacement = observer_from(&registry);
        assert!(!replacement.sender.is_closed(), "a stale global sender must not suppress a new observer");
        assert!(replacement.generation > first.generation);
        first.record_unknown("completion_lost", 9);
        assert!(observation_status_from(&registry).unknown_candidates.is_empty());
        replacement
            .sender
            .try_send(("new-runtime".into(), candidate()))
            .expect("replacement observer admits work");
    });
}

#[test]
fn observer_status_read_does_not_install_or_invent_zero() {
    let registry = Mutex::new(None);
    let status = observation_status_from(&registry);
    assert_eq!(status.state, "not_started");
    assert_eq!(status.retained_pending_tasks, None);
    assert_eq!(status.terminal_task_observations_total, None);
    assert_eq!(status.integrity_state, "unknown");
    assert!(
        registry.lock().expect("registry").is_none(),
        "a status read must work without a Tokio runtime"
    );
    let json = serde_json::to_value(status).expect("serialize unstarted observer");
    assert!(json["retained_pending_tasks"].is_null());
}

#[test]
fn observer_status_preserves_loss_expiry_and_rejected_admissions() {
    use rustfs_heal_contracts::heal_channel::HealAdmissionResult;

    let (sender, receiver) = mpsc::channel(1);
    let observer = ObserverHandle {
        sender,
        state: Arc::new(Mutex::new(Observations::default())),
        generation: 1,
        started_at_unix_secs: Some(20),
    };
    let registry = Mutex::new(Some(observer.clone()));
    {
        let mut state = observer.state.lock().expect("observations");
        for index in 0..=MAX_PENDING {
            let _ = state.admit(format!("pending-{index}"), candidate());
        }
        state.lagged(3);
        let _ = state.admit("expired".into(), candidate());
        state.maintain(Instant::now() + PENDING_TTL);
    }
    let status = observation_status_from(&registry);
    assert_eq!(status.retained_pending_tasks, Some(0));
    assert_eq!(status.lost_completion_notifications, Some(3));
    assert_eq!(status.integrity_state, "unknown");
    let unknown = status
        .unknown_candidates
        .iter()
        .map(|row| (row.reason, row.count))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(unknown["pending_full"], 1);
    assert_eq!(unknown["completion_lost"], u64::try_from(MAX_PENDING).expect("bounded pending count"));
    assert_eq!(unknown["expired"], 1);

    let admitted = HealAdmissionReceipt {
        result: HealAdmissionResult::Accepted,
        task_id: "canonical".into(),
    };
    observe_on(&observer, None, Ok(&admitted));
    observe_on(
        &observer,
        Some(candidate()),
        Ok(&HealAdmissionReceipt {
            result: HealAdmissionResult::Accepted,
            task_id: String::new(),
        }),
    );
    observe_on(&observer, Some(candidate()), Err(()));
    observe_on(&observer, Some(candidate()), Ok(&admitted));
    assert_eq!(observation_status_from(&registry).queued_admissions, Some(1));
    observe_on(&observer, Some(candidate()), Ok(&admitted));
    drop(receiver);
    observe_on(&observer, Some(candidate()), Ok(&admitted));
    let status = observation_status_from(&registry);
    assert_eq!(status.state, "stopped");
    assert_eq!(status.retained_pending_tasks, None);
    let unknown = status
        .unknown_candidates
        .iter()
        .map(|row| (row.reason, row.count))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(unknown["identity_too_large"], 1);
    assert_eq!(unknown["missing_task_id"], 1);
    assert_eq!(unknown["not_admitted"], 1);
    assert_eq!(unknown["observer_full_or_closed"], 2);
}

#[tokio::test]
async fn observer_status_tracks_canonical_terminals_without_a_metrics_recorder() {
    use rustfs_heal_contracts::{completion::publish_heal_completion, heal_channel::HealAdmissionResult};

    let recorder = metrics::NoopRecorder;
    let _recorder = metrics::set_default_local_recorder(&recorder);
    let registry = Mutex::new(None);
    let observer = observer_from(&registry);
    let canonical = Uuid::new_v4().to_string();
    let deferred = Uuid::new_v4().to_string();
    for (task_id, result) in [
        (canonical.clone(), HealAdmissionResult::Accepted),
        (canonical.clone(), HealAdmissionResult::Merged),
        (deferred.clone(), HealAdmissionResult::Accepted),
    ] {
        observe_on(&observer, Some(candidate()), Ok(&HealAdmissionReceipt { result, task_id }));
    }
    publish_heal_completion((*completion(&canonical, HealObjectDisposition::VerifiedHealthy)).clone());
    publish_heal_completion(
        (*completion(
            &deferred,
            HealObjectDisposition::Deferred {
                reason: HealDeferredReason::Deadline,
                retry_not_before: None,
            },
        ))
        .clone(),
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let status = observation_status_from(&registry);
            if status.terminal_candidates.iter().map(|row| row.count).sum::<u64>() == 3 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real observer must update its own status without an exporter");

    let status = observation_status_from(&registry);
    assert_eq!(status.retained_pending_tasks, Some(0));
    assert_eq!(status.terminal_task_observations_total, Some(2));
    assert_eq!(status.unknown_candidates.iter().map(|row| row.count).sum::<u64>(), 3);
    assert_eq!(status.integrity_state, "unknown");
    assert!(
        status
            .terminal_candidates
            .iter()
            .all(|row| row.execution == "completed" && row.scope == "content_and_leader_unknown")
    );
    assert!(
        status
            .terminal_candidates
            .iter()
            .any(|row| row.disposition == "verified_healthy" && row.count == 2)
    );
    assert!(
        status
            .terminal_candidates
            .iter()
            .any(|row| row.disposition == "deferred" && row.count == 1)
    );
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
