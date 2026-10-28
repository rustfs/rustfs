use super::*;
use rustfs_heal_contracts::{
    completion::{HealCompletion, publish_observed_heal_completion},
    heal_channel::HealAdmissionReceipt,
    outcome::{HealAbortReason, HealDeferredReason, HealFailureClass, HealObjectReceipt},
};
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

fn key(task_id: &str) -> ObservationKey {
    ObservationKey {
        task_id: task_id.into(),
        observation_id: 1,
    }
}

fn completion(task: &str, disposition: HealObjectDisposition) -> Arc<ObservedHealCompletion> {
    Arc::new(ObservedHealCompletion {
        observation_id: Some(1),
        completion: Arc::new(HealCompletion {
            task_id: task.into(),
            scan_mode: HealScanMode::Deep,
            dry_run: false,
            execution: Some(HealExecutionOutcome::Completed),
            object: Some(HealObjectReceipt {
                identity: candidate().identity.expect("exact object identity"),
                disposition,
            }),
        }),
    })
}

fn publish_terminal(observed: Arc<ObservedHealCompletion>) {
    publish_observed_heal_completion((*observed.completion).clone(), observed.observation_id);
}

#[test]
fn mixed_candidate_semantics_settle_independently_in_either_order() {
    for terminal_first in [false, true] {
        for reverse in [false, true] {
            let mut observations = Observations::default();
            let owner = key("mixed-candidates");
            let mut terminal = (*completion("mixed-candidates", HealObjectDisposition::VerifiedHealthy)).clone();
            Arc::make_mut(&mut terminal.completion).scan_mode = HealScanMode::Normal;
            let terminal = Arc::new(terminal);
            if terminal_first {
                assert!(observations.complete(terminal.clone(), Instant::now()).is_none());
            }

            let mut normal = candidate();
            normal.mode = HealScanMode::Normal;
            let mut same_normal = candidate();
            same_normal.mode = HealScanMode::Normal;
            let deep = candidate();
            let mut different = candidate();
            different.mode = HealScanMode::Normal;
            different.identity.as_mut().expect("candidate identity").object = "different".into();
            let mut unknown = candidate();
            unknown.mode = HealScanMode::Normal;
            unknown.identity = None;
            let mut dry_run = candidate();
            dry_run.mode = HealScanMode::Normal;
            dry_run.dry_run = true;
            let mut candidates = vec![normal, same_normal, deep, different, unknown, dry_run];
            if reverse {
                candidates.reverse();
            }
            for candidate in candidates {
                assert_eq!(observations.admit(owner.clone(), candidate).is_some(), terminal_first);
            }
            if !terminal_first {
                assert!(observations.complete(terminal.clone(), Instant::now()).is_some());
            }
            assert!(observations.complete(terminal, Instant::now()).is_none());
            assert_eq!(observations.terminal_tasks, 1);
            assert_eq!(observations.terminal_counts.values().sum::<u64>(), 6);
            for (scope, count) in [
                ("content_and_leader_unknown", 2),
                ("insufficient_scan", 1),
                ("identity_mismatch", 1),
                ("identity_unknown", 1),
                ("dry_run", 1),
            ] {
                assert_eq!(observations.unknown_counts.get(scope), Some(&count), "{scope}");
            }
            assert_eq!(
                observations
                    .terminal_counts
                    .get(&("completed", "verified_healthy", "content_and_leader_unknown", "normal")),
                Some(&2)
            );
            assert_eq!(
                observations
                    .terminal_counts
                    .get(&("completed", "verified_healthy", "insufficient_scan", "deep")),
                Some(&1)
            );
        }
    }
}

#[test]
fn candidate_group_capacity_is_global_and_released_by_completion() {
    let mut observations = Observations::default();
    let split = MAX_PENDING / 2;
    for index in 0..MAX_PENDING {
        let mut observed = candidate();
        observed.identity.as_mut().expect("candidate identity").object = format!("object-{index}");
        let owner = if index < split { "owner-a" } else { "owner-b" };
        assert!(observations.admit(key(owner), observed).is_none());
    }
    assert_eq!(observations.pending.len(), 2);

    let mut duplicate = candidate();
    duplicate.identity.as_mut().expect("candidate identity").object = "object-0".into();
    assert!(observations.admit(key("owner-a"), duplicate).is_none());
    assert!(observations.admit(key("owner-a"), candidate()).is_none());
    assert!(observations.admit(key("owner-c"), candidate()).is_none());
    assert_eq!(observations.unknown_counts.get("pending_groups_full"), Some(&2));
    assert!(!observations.pending.contains_key(&key("owner-c")));

    assert!(
        observations
            .complete(completion("owner-a", HealObjectDisposition::VerifiedHealthy), Instant::now())
            .is_some()
    );
    assert_eq!(
        observations.terminal_counts.values().sum::<u64>(),
        u64::try_from(split + 1).expect("bounded count")
    );
    assert!(observations.admit(key("owner-c"), candidate()).is_none());
    assert!(observations.pending.contains_key(&key("owner-c")));
    assert_eq!(observations.unknown_counts.get("pending_groups_full"), Some(&2));
}

#[test]
fn candidate_group_expiry_and_notification_loss_are_independent() {
    let mut observations = Observations::default();
    let now = Instant::now();
    let mut old = candidate();
    old.first_seen = now;
    old.mode = HealScanMode::Normal;
    let mut old_duplicate = candidate();
    old_duplicate.first_seen = now;
    old_duplicate.mode = HealScanMode::Normal;
    let mut newer = candidate();
    newer.first_seen = now + Duration::from_secs(1);
    newer.dry_run = true;
    for candidate in [old, old_duplicate, newer] {
        assert!(observations.admit(key("expiring-owner"), candidate).is_none());
    }

    observations.maintain(now + PENDING_TTL);
    assert_eq!(observations.unknown_counts.get("expired"), Some(&2));
    assert!(observations.pending.contains_key(&key("expiring-owner")));
    assert_eq!(observations.terminal_tasks, 0);

    observations.lagged(3);
    assert_eq!(observations.lost_notifications, 3);
    assert_eq!(observations.unknown_counts.get("completion_lost"), Some(&1));
    assert!(observations.pending.is_empty());
    assert!(
        observations
            .complete(completion("expiring-owner", HealObjectDisposition::VerifiedHealthy), Instant::now())
            .is_none()
    );
    assert_eq!(observations.terminal_tasks, 0);
    assert!(observations.admit(key("after-loss"), candidate()).is_none());
    assert!(observations.pending.contains_key(&key("after-loss")));
}

#[test]
fn candidate_group_expiry_uses_earliest_discovery_not_admission_order() {
    let mut observations = Observations::default();
    let now = Instant::now();
    let mut later = candidate();
    later.first_seen = now + Duration::from_secs(1);
    let mut earlier = candidate();
    earlier.first_seen = now;
    assert!(observations.admit(key("reordered"), later).is_none());
    assert!(observations.admit(key("reordered"), earlier).is_none());
    observations.maintain(now + PENDING_TTL);
    assert!(!observations.pending.contains_key(&key("reordered")));
    assert_eq!(observations.unknown_counts.get("expired"), Some(&2));
    assert_eq!(observations.terminal_tasks, 0);
}

#[test]
fn later_candidate_group_also_expires_from_earliest_discovery() {
    let mut observations = Observations::default();
    let now = Instant::now();
    let mut first_group = candidate();
    first_group.first_seen = now + Duration::from_secs(2);
    let mut later = candidate();
    later.mode = HealScanMode::Normal;
    later.first_seen = now + Duration::from_secs(1);
    let mut earlier = candidate();
    earlier.mode = HealScanMode::Normal;
    earlier.first_seen = now;
    for candidate in [first_group, later, earlier] {
        assert!(observations.admit(key("reordered-other-group"), candidate).is_none());
    }

    observations.maintain(now + PENDING_TTL);
    assert!(observations.pending.contains_key(&key("reordered-other-group")));
    assert_eq!(observations.unknown_counts.get("expired"), Some(&2));
    assert_eq!(observations.terminal_tasks, 0);
}

#[test]
fn candidate_expiry_is_enforced_when_terminal_arrives_between_ticks() {
    let mut observations = Observations::default();
    let now = Instant::now();
    let mut old = candidate();
    old.mode = HealScanMode::Normal;
    old.first_seen = now;
    let mut fresh = candidate();
    fresh.first_seen = now + Duration::from_secs(1);
    assert!(observations.admit(key("terminal-expiry"), old).is_none());
    assert!(observations.admit(key("terminal-expiry"), fresh).is_none());

    assert!(
        observations
            .complete(completion("terminal-expiry", HealObjectDisposition::VerifiedHealthy), now + PENDING_TTL)
            .is_some()
    );
    assert_eq!(observations.unknown_counts.get("expired"), Some(&1));
    assert_eq!(observations.terminal_counts.values().sum::<u64>(), 1);
    assert_eq!(observations.terminal_tasks, 1);
    assert!(observations.pending.is_empty());
}

#[test]
fn expired_candidate_is_unknown_even_if_terminal_was_seen_first() {
    for terminal_first in [true, false] {
        let mut observations = Observations::default();
        let now = Instant::now();
        if terminal_first {
            assert!(
                observations
                    .complete(completion("late-admission", HealObjectDisposition::VerifiedHealthy), now)
                    .is_none()
            );
        }
        let mut old = candidate();
        old.first_seen = now
            .checked_sub(PENDING_TTL + Duration::from_secs(1))
            .expect("monotonic clock supports the candidate TTL");
        assert!(observations.admit(key("late-admission"), old).is_none());
        assert_eq!(observations.unknown_counts.get("expired"), Some(&1));
        assert!(!observations.pending.contains_key(&key("late-admission")));
        assert_eq!(observations.terminal_tasks, 0);
        assert!(observations.terminal_counts.is_empty());
    }
}

#[test]
fn grouped_pending_status_counts_candidates_after_partial_expiry() {
    let (sender, _receiver) = mpsc::channel(1);
    let observer = ObserverHandle {
        sender,
        state: Arc::new(Mutex::new(Observations::default())),
        generation: 1,
        started_at_unix_secs: None,
    };
    let registry = Mutex::new(Some(observer.clone()));
    let now = Instant::now();
    {
        let mut observations = observer.state.lock().expect("observations");
        let mut old = candidate();
        old.first_seen = now;
        let mut newer = candidate();
        newer.first_seen = now + Duration::from_secs(1);
        newer.mode = HealScanMode::Normal;
        let mut newer_duplicate = candidate();
        newer_duplicate.first_seen = now + Duration::from_secs(1);
        newer_duplicate.mode = HealScanMode::Normal;
        for candidate in [old, newer, newer_duplicate] {
            assert!(observations.admit(key("status-owner"), candidate).is_none());
        }
    }
    let status = observation_status_from(&registry);
    assert_eq!(status.retained_pending_tasks, Some(1));
    assert_eq!(status.retained_pending_candidates, Some(3));

    observer.state.lock().expect("observations").maintain(now + PENDING_TTL);
    let status = observation_status_from(&registry);
    assert_eq!(status.retained_pending_tasks, Some(1));
    assert_eq!(status.retained_pending_candidates, Some(2));
    assert_eq!(
        status
            .unknown_candidates
            .iter()
            .find(|row| row.reason == "expired")
            .map(|row| row.count),
        Some(1)
    );

    observer.state.lock().expect("observations").lagged(1);
    let status = observation_status_from(&registry);
    assert_eq!(status.retained_pending_tasks, Some(0));
    assert_eq!(status.retained_pending_candidates, Some(0));
    assert_eq!(
        status
            .unknown_candidates
            .iter()
            .find(|row| row.reason == "completion_lost")
            .map(|row| row.count),
        Some(2)
    );
}

#[test]
fn observation_id_keeps_grouped_owner_generations_separate() {
    let mut observations = Observations::default();
    let first = key("reused-owner");
    let second = ObservationKey {
        task_id: first.task_id.clone(),
        observation_id: 2,
    };
    let mut normal = candidate();
    normal.mode = HealScanMode::Normal;
    assert!(observations.admit(first.clone(), normal).is_none());
    assert!(observations.admit(first.clone(), candidate()).is_none());
    assert!(observations.admit(second.clone(), candidate()).is_none());

    let first_terminal = completion("reused-owner", HealObjectDisposition::VerifiedHealthy);
    assert!(observations.complete(first_terminal.clone(), Instant::now()).is_some());
    assert!(!observations.pending.contains_key(&first));
    assert!(observations.pending.contains_key(&second));
    assert_eq!(observations.terminal_tasks, 1);
    assert!(observations.complete(first_terminal.clone(), Instant::now()).is_none());
    assert_eq!(observations.terminal_tasks, 1);

    let mut second_terminal = (*first_terminal).clone();
    second_terminal.observation_id = Some(2);
    assert!(observations.complete(Arc::new(second_terminal), Instant::now()).is_some());
    assert!(observations.pending.is_empty());
    assert_eq!(observations.terminal_tasks, 2);
    assert_eq!(observations.terminal_counts.values().sum::<u64>(), 3);
}

#[test]
fn merged_candidates_close_only_on_the_canonical_task_once() {
    let mut observations = Observations::default();
    assert!(observations.admit(key("owner"), candidate()).is_none());
    assert!(observations.admit(key("owner"), candidate()).is_none());
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
        .admit(key("owner"), candidate())
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
    let settled = observations.admit(key("fast"), candidate()).expect("fast terminal retained");
    assert_eq!(settled.disposition, "repaired");
    assert_eq!(settled.scope, "content_and_leader_unknown");
    assert!(settled.first_task_observation);
}

#[test]
fn status_and_pending_metric_count_each_owner_generation() {
    #[derive(Default)]
    struct GaugeValue(Mutex<f64>);

    impl metrics::GaugeFn for GaugeValue {
        fn increment(&self, value: f64) {
            *self.0.lock().expect("pending gauge") += value;
        }
        fn decrement(&self, value: f64) {
            *self.0.lock().expect("pending gauge") -= value;
        }
        fn set(&self, value: f64) {
            *self.0.lock().expect("pending gauge") = value;
        }
    }

    #[derive(Default)]
    struct PendingRecorder(Arc<GaugeValue>);

    impl metrics::Recorder for PendingRecorder {
        fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
        fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
        fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
        fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
            metrics::Counter::noop()
        }
        fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            if key.name() == "rustfs_scanner_heal_pending_tasks" {
                metrics::Gauge::from_arc(Arc::clone(&self.0))
            } else {
                metrics::Gauge::noop()
            }
        }
        fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    let recorder = PendingRecorder::default();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    let (sender, _receiver) = mpsc::channel(1);
    let observer = ObserverHandle {
        sender,
        state: Arc::new(Mutex::new(Observations::default())),
        generation: 1,
        started_at_unix_secs: None,
    };
    let registry = Mutex::new(Some(observer.clone()));
    let first = key("same-task");
    let second = ObservationKey {
        task_id: "same-task".into(),
        observation_id: 2,
    };
    {
        let mut observations = observer.state.lock().expect("observations");
        assert!(observations.admit(first.clone(), candidate()).is_none());
        assert!(observations.admit(first, candidate()).is_none());
        assert!(observations.admit(second, candidate()).is_none());
        observations.maintain(Instant::now());
    }
    let status = observation_status_from(&registry);
    assert_eq!(status.retained_pending_tasks, Some(2));
    assert_eq!(status.retained_pending_candidates, Some(3));
    assert_eq!(*recorder.0.0.lock().expect("pending gauge"), 2.0);
    let json = serde_json::to_value(&status).expect("serialize observation status");
    assert_eq!(json["retained_pending_tasks"], 2);
    assert_eq!(json["retained_pending_candidates"], 3);

    {
        let mut observations = observer.state.lock().expect("observations");
        let settlement = observations
            .complete(completion("same-task", HealObjectDisposition::VerifiedHealthy), Instant::now())
            .expect("the first generation settles its two candidates");
        assert_eq!(settlement.candidates, 2);
        observations.maintain(Instant::now());
    }
    let status = observation_status_from(&registry);
    assert_eq!(status.retained_pending_tasks, Some(1));
    assert_eq!(status.retained_pending_candidates, Some(1));
    assert_eq!(*recorder.0.0.lock().expect("pending gauge"), 1.0);
}

#[test]
fn reused_task_id_keeps_terminal_observations_on_their_owner_generation() {
    let mut observations = Observations::default();
    let old_key = key("owner");
    assert!(observations.admit(old_key.clone(), candidate()).is_none());
    assert!(
        observations
            .complete(completion("owner", HealObjectDisposition::VerifiedHealthy), Instant::now())
            .is_some()
    );

    let new_key = ObservationKey {
        task_id: "owner".into(),
        observation_id: 2,
    };
    assert!(
        observations.admit(new_key.clone(), candidate()).is_none(),
        "the old terminal cannot settle a new owner"
    );
    let mut new_terminal = (*completion("owner", HealObjectDisposition::Repaired)).clone();
    new_terminal.observation_id = Some(2);
    let settled = observations
        .complete(Arc::new(new_terminal), Instant::now())
        .expect("the new owner settles itself");
    assert_eq!(settled.disposition, "repaired");
    assert!(observations.pending.is_empty());

    let fast_key = ObservationKey {
        task_id: "owner".into(),
        observation_id: 3,
    };
    let mut fast_terminal = (*completion("owner", HealObjectDisposition::MetadataHealthy)).clone();
    fast_terminal.observation_id = Some(3);
    assert!(observations.complete(Arc::new(fast_terminal), Instant::now()).is_none());
    assert!(
        observations.admit(fast_key, candidate()).is_some(),
        "a fast terminal stays available to its owner"
    );
}

#[test]
fn removed_owner_remains_pending_then_expires_unknown_after_id_reuse() {
    let mut observations = Observations::default();
    let old = key("removed-owner");
    assert!(observations.admit(old.clone(), candidate()).is_none());
    let mut new_terminal = (*completion("removed-owner", HealObjectDisposition::VerifiedHealthy)).clone();
    new_terminal.observation_id = Some(2);
    assert!(observations.complete(Arc::new(new_terminal), Instant::now()).is_none());
    assert!(observations.pending.contains_key(&old));
    observations.maintain(Instant::now() + PENDING_TTL);
    assert!(!observations.pending.contains_key(&old));
    assert_eq!(observations.unknown_counts.get("expired"), Some(&1));
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
    Arc::make_mut(&mut terminal.completion)
        .object
        .as_mut()
        .expect("receipt")
        .identity
        .version_id = None;
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
        Arc::make_mut(&mut terminal.completion).scan_mode = HealScanMode::Normal;
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
    Arc::make_mut(&mut terminal.completion).execution = Some(HealExecutionOutcome::Aborted(HealAbortReason::Cancelled));
    let mut recent = Recent {
        completion: Arc::new(terminal),
        counted: false,
    };
    assert_eq!(Settlement::new(candidate(), &mut recent).scope, "execution_incomplete");
    assert_eq!(Settlement::new(candidate(), &mut recent).execution, "aborted_cancelled");
    let mut errors = (*completion("errors", HealObjectDisposition::Repaired)).clone();
    Arc::make_mut(&mut errors.completion).execution = Some(HealExecutionOutcome::CompletedWithErrors);
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
    Arc::make_mut(&mut terminal.completion).object = None;
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
        observations.admit(key(&format!("pending-{index}")), candidate());
    }
    assert_eq!(observations.pending.len(), MAX_PENDING);
    assert!(!observations.pending.contains_key(&key(&format!("pending-{MAX_PENDING}"))));
    observations.lost();
    assert!(observations.pending.is_empty());
    assert!(
        observations
            .complete(completion("pending-0", HealObjectDisposition::VerifiedHealthy), Instant::now())
            .is_none()
    );
    observations.admit(key("expires"), candidate());
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
    assert!(observations.admit(key("terminal-0"), candidate()).is_none());
    assert!(observations.pending.contains_key(&key("terminal-0")));
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
            .try_send((key("new-runtime"), candidate()))
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
            let _ = state.admit(key(&format!("pending-{index}")), candidate());
        }
        state.lagged(3);
        let _ = state.admit(key("expired"), candidate());
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

    let admitted = ObservedHealAdmissionReceipt {
        receipt: HealAdmissionReceipt {
            result: HealAdmissionResult::Accepted,
            task_id: "canonical".into(),
        },
        observation_id: Some(1),
    };
    observe_on(
        &observer,
        Some(candidate()),
        Ok(&ObservedHealAdmissionReceipt {
            receipt: HealAdmissionReceipt {
                result: HealAdmissionResult::Merged,
                task_id: "unproven-owner".into(),
            },
            observation_id: None,
        }),
    );
    observe_on(&observer, None, Ok(&admitted));
    observe_on(
        &observer,
        Some(candidate()),
        Ok(&ObservedHealAdmissionReceipt {
            receipt: HealAdmissionReceipt {
                result: HealAdmissionResult::Accepted,
                task_id: String::new(),
            },
            observation_id: Some(1),
        }),
    );
    observe_on(&observer, Some(candidate()), Err(&HealObservationChannelError::NotInitialized));
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
    assert_eq!(unknown["owner_generation_unknown"], 1);
    assert_eq!(unknown["missing_task_id"], 1);
    assert_eq!(unknown["admission_unknown"], 1);
    assert_eq!(unknown["observer_full_or_closed"], 2);
}

#[test]
fn explicit_rejections_are_reported_not_admitted() {
    use rustfs_heal_contracts::heal_channel::{HealAdmissionDropReason, HealAdmissionResult};

    let recorder = CompletionRecorder::default();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    let (sender, _receiver) = mpsc::channel(1);
    let observer = ObserverHandle {
        sender,
        state: Arc::new(Mutex::new(Observations::default())),
        generation: 1,
        started_at_unix_secs: None,
    };
    let registry = Mutex::new(Some(observer.clone()));
    for result in [
        HealAdmissionResult::Full,
        HealAdmissionResult::Dropped(HealAdmissionDropReason::QueueFull),
    ] {
        let receipt = ObservedHealAdmissionReceipt {
            receipt: HealAdmissionReceipt {
                result,
                task_id: String::new(),
            },
            observation_id: None,
        };
        observe_on(&observer, Some(candidate()), Ok(&receipt));
    }
    let status = observation_status_from(&registry);
    assert_eq!(status.integrity_state, "unknown");
    assert_eq!(status.retained_pending_candidates, Some(0));
    assert_eq!(status.unknown_candidates.iter().map(|row| row.count).sum::<u64>(), 2);
    assert!(
        status
            .unknown_candidates
            .iter()
            .any(|row| row.reason == "not_admitted" && row.count == 2)
    );
    assert_eq!(recorder.reason_total("not_admitted"), 2);
    assert_eq!(recorder.reason_total("admission_unknown"), 0);
    assert_eq!(recorder.total("rustfs_scanner_heal_candidate_unknown_total"), 2);
}

#[tokio::test]
async fn admission_errors_without_decisions_are_reported_unknown() {
    let recorder = CompletionRecorder::default();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    let (sender, _receiver) = mpsc::channel(1);
    let observer = ObserverHandle {
        sender,
        state: Arc::new(Mutex::new(Observations::default())),
        generation: 1,
        started_at_unix_secs: None,
    };
    let registry = Mutex::new(Some(observer.clone()));
    let (sender, observed_reply) = tokio::sync::oneshot::channel::<()>();
    drop(sender);
    let (sender, legacy_reply) = tokio::sync::oneshot::channel::<()>();
    drop(sender);
    for error in [
        HealObservationChannelError::NotInitialized,
        HealObservationChannelError::LegacyCommandClosed,
        HealObservationChannelError::ObservedResponseClosed(observed_reply.await.expect_err("reply was dropped")),
        HealObservationChannelError::LegacyResponseClosed(legacy_reply.await.expect_err("reply was dropped")),
        HealObservationChannelError::AdmissionFailed("unconfirmed processor error".into()),
    ] {
        observe_on(&observer, Some(candidate()), Err(&error));
    }

    let status = observation_status_from(&registry);
    assert_eq!(status.integrity_state, "unknown");
    assert_eq!(status.retained_pending_candidates, Some(0));
    assert_eq!(status.unknown_candidates.iter().map(|row| row.count).sum::<u64>(), 5);
    assert!(
        status
            .unknown_candidates
            .iter()
            .any(|row| row.reason == "admission_unknown" && row.count == 5)
    );
    assert_eq!(recorder.reason_total("admission_unknown"), 5);
    assert_eq!(recorder.reason_total("not_admitted"), 0);
    assert_eq!(recorder.total("rustfs_scanner_heal_candidate_unknown_total"), 5);
}

#[tokio::test]
async fn observer_status_tracks_canonical_terminals_without_a_metrics_recorder() {
    use rustfs_heal_contracts::heal_channel::HealAdmissionResult;

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
        observe_on(
            &observer,
            Some(candidate()),
            Ok(&ObservedHealAdmissionReceipt {
                receipt: HealAdmissionReceipt { result, task_id },
                observation_id: Some(1),
            }),
        );
    }
    publish_terminal(completion(&canonical, HealObjectDisposition::VerifiedHealthy));
    publish_terminal(completion(
        &deferred,
        HealObjectDisposition::Deferred {
            reason: HealDeferredReason::Deadline,
            retry_not_before: None,
        },
    ));
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

    fn reason_total(&self, reason: &str) -> u64 {
        self.0
            .lock()
            .expect("completion recorder")
            .iter()
            .filter(|(key, _)| {
                key.name() == "rustfs_scanner_heal_candidate_unknown_total"
                    && key.labels().any(|label| label.key() == "reason" && label.value() == reason)
            })
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
    use rustfs_heal_contracts::heal_channel::HealAdmissionResult;

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
        Ok(&ObservedHealAdmissionReceipt {
            receipt: HealAdmissionReceipt {
                result: HealAdmissionResult::Accepted,
                task_id: sentinel_id.clone(),
            },
            observation_id: Some(1),
        }),
    );
    publish_terminal(completion(&canonical_id, HealObjectDisposition::VerifiedHealthy));
    // FIFO on the real broadcast channel makes this terminal an acknowledgement
    // that the earlier unmatched completion was consumed before its admission.
    publish_terminal(completion(&sentinel_id, HealObjectDisposition::MetadataHealthy));
    recorder.wait_for_terminals(1).await;

    let merged = ObservedHealAdmissionReceipt {
        receipt: HealAdmissionReceipt {
            result: HealAdmissionResult::Merged,
            task_id: canonical_id,
        },
        observation_id: Some(1),
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
