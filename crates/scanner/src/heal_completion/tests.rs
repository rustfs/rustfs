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
    Candidate::from_request(&request(), Some(Uuid::from_u128(33))).expect("bounded candidate")
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
            resolved_data_dir: Some(Uuid::from_u128(33)),
            payload_integrity_verified: false,
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
fn reused_task_id_does_not_attach_an_old_terminal_to_a_new_execution() {
    let recorder = CompletionRecorder::default();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    let mut observations = Observations::default();
    assert!(observations.admit("owner".into(), candidate()).is_none());
    let old_terminal = completion("owner", HealObjectDisposition::VerifiedHealthy);
    assert!(observations.complete(old_terminal, Instant::now()).is_some());

    let mut new_candidate = candidate();
    new_candidate.data_dir = Some(Uuid::from_u128(34));
    let old_terminal_was_reused = observations.admit("owner".into(), new_candidate).is_some();
    let mut new_terminal = (*completion("owner", HealObjectDisposition::VerifiedHealthy)).clone();
    new_terminal.object.as_mut().expect("object receipt").resolved_data_dir = Some(Uuid::from_u128(34));
    let _ = observations.complete(Arc::new(new_terminal), Instant::now());

    assert!(
        !old_terminal_was_reused,
        "HEAL_ID_REUSE_UNIT_STALE_TERMINAL: a later execution cannot settle from an earlier terminal"
    );
    assert_eq!(
        recorder.total("rustfs_scanner_heal_candidate_generation_mismatch_total"),
        0,
        "both executions verified the generation discovered for their own candidate"
    );
}

#[test]
fn reused_task_id_with_unchanged_data_dir_does_not_reuse_the_old_terminal() {
    let mut observations = Observations::default();
    assert!(observations.admit("owner".into(), candidate()).is_none());
    assert!(
        observations
            .complete(completion("owner", HealObjectDisposition::VerifiedHealthy), Instant::now())
            .is_some()
    );
    let old_terminal_was_reused = observations.admit("owner".into(), candidate()).is_some();
    let _ = observations.complete(completion("owner", HealObjectDisposition::VerifiedHealthy), Instant::now());
    assert!(
        !old_terminal_was_reused,
        "HEAL_ID_REUSE_UNIT_SAME_DATA_STALE_TERMINAL: equal data directories do not identify an execution"
    );
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
fn candidate_captures_only_non_nil_object_data_dir() {
    let generation = Uuid::from_u128(33);
    for (data_dir, expected) in [(Some(generation), Some(generation)), (Some(Uuid::nil()), None), (None, None)] {
        let object = crate::ScannerObjectInfo {
            data_dir,
            ..Default::default()
        };
        let observed = Candidate::from_request(&request(), object.data_dir).expect("bounded object candidate");
        assert_eq!(observed.data_dir, expected);
    }
}

#[test]
fn generation_scope_requires_known_matching_data_dirs() {
    let generation = Uuid::from_u128(33);
    let replacement = Uuid::from_u128(34);
    for (expected, resolved, scope) in [
        (Some(generation), Some(generation), "content_and_leader_unknown"),
        (Some(generation), Some(replacement), "identity_mismatch"),
        (None, Some(generation), "identity_unknown"),
        (Some(generation), None, "identity_unknown"),
        (None, None, "identity_unknown"),
        (Some(Uuid::nil()), Some(generation), "identity_unknown"),
        (Some(generation), Some(Uuid::nil()), "identity_unknown"),
    ] {
        let mut observed = candidate();
        observed.data_dir = expected;
        let mut terminal = (*completion("generation", HealObjectDisposition::VerifiedHealthy)).clone();
        terminal.object.as_mut().expect("object receipt").resolved_data_dir = resolved;
        let mut recent = Recent {
            completion: Arc::new(terminal),
            counted: false,
        };
        assert_eq!(Settlement::new(observed, &mut recent).scope, scope, "{expected:?}, {resolved:?}");
    }
}

#[test]
fn known_generation_conflict_is_not_hidden_by_missing_location() {
    let mut observed = candidate();
    observed.identity.as_mut().expect("candidate identity").pool_index = None;
    let mut terminal = (*completion("generation", HealObjectDisposition::VerifiedHealthy)).clone();
    let receipt = terminal.object.as_mut().expect("object receipt");
    receipt.identity.set_index = None;
    receipt.resolved_data_dir = Some(Uuid::from_u128(34));
    let mut recent = Recent {
        completion: Arc::new(terminal),
        counted: false,
    };
    assert_eq!(Settlement::new(observed, &mut recent).scope, "identity_mismatch");
}

#[test]
fn merged_generations_settle_independently_in_any_arrival_order() {
    for terminal_first in [false, true] {
        for reversed in [false, true] {
            let recorder = CompletionRecorder::default();
            let _recorder = metrics::set_default_local_recorder(&recorder);
            let mut observations = Observations::default();
            let terminal = completion("owner", HealObjectDisposition::VerifiedHealthy);
            if terminal_first {
                assert!(observations.complete(terminal.clone(), Instant::now()).is_none());
            }
            let mut generations = [Some(Uuid::from_u128(33)), Some(Uuid::from_u128(34)), None];
            if reversed {
                generations.reverse();
            }
            for generation in generations {
                let mut observed = candidate();
                observed.data_dir = generation;
                assert_eq!(observations.admit("owner".into(), observed).is_some(), terminal_first);
            }
            if !terminal_first {
                assert!(observations.complete(terminal.clone(), Instant::now()).is_some());
            }
            assert!(observations.complete(terminal, Instant::now()).is_none(), "duplicate terminal ignored");
            assert_eq!(observations.pending_groups, 0);
            assert_eq!(observations.terminal_tasks, 1);
            assert_eq!(observations.unknown_counts.get("content_and_leader_unknown"), Some(&1));
            assert_eq!(observations.unknown_counts.get("identity_mismatch"), Some(&1));
            assert_eq!(observations.unknown_counts.get("identity_unknown"), Some(&1));
            assert_eq!(observations.terminal_counts.values().sum::<u64>(), 3);
            assert_eq!(recorder.total("rustfs_scanner_heal_candidate_generation_mismatch_total"), 1);
        }
    }
}

#[test]
fn pending_group_limit_is_global_and_overflow_stays_unknown() {
    let recorder = CompletionRecorder::default();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    let mut observations = Observations::default();
    for index in 1..=MAX_PENDING_GROUPS {
        let mut observed = candidate();
        observed.data_dir = Some(Uuid::from_u128(u128::try_from(index).expect("group index")));
        assert!(observations.admit("owner".into(), observed).is_none());
    }
    assert_eq!(observations.pending_groups, MAX_PENDING_GROUPS);
    assert_eq!(observations.pending.len(), 1);
    let mut overflow = candidate();
    overflow.data_dir = Some(Uuid::from_u128(2000));
    assert!(observations.admit("owner".into(), overflow).is_none());
    assert!(observations.admit("another-owner".into(), candidate()).is_none());
    assert_eq!(observations.unknown_counts.get("pending_groups_full"), Some(&2));
    assert!(observations.admit("owner".into(), candidate()).is_none());
    assert_eq!(observations.pending_groups, MAX_PENDING_GROUPS);

    assert!(
        observations
            .complete(completion("owner", HealObjectDisposition::VerifiedHealthy), Instant::now())
            .is_some()
    );
    assert_eq!(observations.pending_groups, 0);
    assert_eq!(observations.terminal_tasks, 1);
    assert_eq!(observations.unknown_counts.get("content_and_leader_unknown"), Some(&2));
    assert_eq!(
        observations.unknown_counts.get("identity_mismatch"),
        Some(&(u64::try_from(MAX_PENDING_GROUPS).expect("bounded group count") - 1))
    );
    assert_eq!(
        recorder.total("rustfs_scanner_heal_candidate_generation_mismatch_total"),
        u64::try_from(MAX_PENDING_GROUPS).expect("bounded group count") - 1
    );
}

#[test]
fn insufficient_scan_and_generation_mismatch_are_both_counted() {
    let recorder = CompletionRecorder::default();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    let mut observations = Observations::default();
    observations.admit("owner".into(), candidate());
    observations.admit("owner".into(), candidate());
    let mut terminal = (*completion("owner", HealObjectDisposition::MetadataHealthy)).clone();
    terminal.scan_mode = HealScanMode::Normal;
    terminal.object.as_mut().expect("object receipt").resolved_data_dir = Some(Uuid::from_u128(34));
    let terminal = Arc::new(terminal);
    let settled = observations
        .complete(terminal.clone(), Instant::now())
        .expect("canonical terminal observed");
    assert_eq!(settled.scope, "insufficient_scan");
    assert!(settled.generation_mismatch);
    assert_eq!(observations.unknown_counts.get("insufficient_scan"), Some(&2));
    assert_eq!(recorder.total("rustfs_scanner_heal_candidate_generation_mismatch_total"), 2);
    assert!(observations.complete(terminal, Instant::now()).is_none());
    assert_eq!(recorder.total("rustfs_scanner_heal_candidate_generation_mismatch_total"), 2);

    let mut matching = candidate();
    matching.data_dir = Some(Uuid::from_u128(34));
    let settled = observations.admit("owner".into(), matching).expect("late merged admission");
    assert_eq!(settled.scope, "insufficient_scan");
    assert!(!settled.generation_mismatch);
    assert_eq!(recorder.total("rustfs_scanner_heal_candidate_generation_mismatch_total"), 2);
    assert_eq!(observations.terminal_tasks, 1);
}

#[test]
fn proved_payload_task_is_counted_once_for_either_terminal_order() {
    for terminal_first in [false, true] {
        for requested_mode in [HealScanMode::Normal, HealScanMode::Deep] {
            let recorder = CompletionRecorder::default();
            let _recorder = metrics::set_default_local_recorder(&recorder);
            let mut observations = Observations::default();
            let mut terminal = (*completion("owner", HealObjectDisposition::VerifiedHealthy)).clone();
            terminal.scan_mode = requested_mode;
            terminal.object.as_mut().expect("object receipt").payload_integrity_verified = true;
            let terminal = Arc::new(terminal);
            if terminal_first {
                assert!(observations.complete(terminal.clone(), Instant::now()).is_none());
            }
            observations.admit("owner".into(), candidate());
            observations.admit("owner".into(), candidate());
            if !terminal_first {
                assert!(observations.complete(terminal.clone(), Instant::now()).is_some());
            }
            assert!(observations.complete(terminal, Instant::now()).is_none(), "duplicate terminal ignored");
            assert_eq!(observations.terminal_tasks, 1);
            assert_eq!(observations.unknown_counts.get("content_and_leader_unknown"), Some(&2));
            assert_eq!(recorder.total("rustfs_scanner_heal_task_deep_payload_verified_observations_total"), 1);
        }
    }
}

#[test]
fn task_payload_counter_requires_completed_nondry_proved_receipt() {
    for change in 0..6 {
        let recorder = CompletionRecorder::default();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        let mut observations = Observations::default();
        observations.admit("owner".into(), candidate());
        let mut terminal = (*completion("owner", HealObjectDisposition::VerifiedHealthy)).clone();
        terminal.object.as_mut().expect("object receipt").payload_integrity_verified = true;
        match change {
            0 => terminal.object.as_mut().expect("object receipt").payload_integrity_verified = false,
            1 => terminal.object = None,
            2 => terminal.dry_run = true,
            3 => terminal.execution = None,
            4 => terminal.execution = Some(HealExecutionOutcome::CompletedWithErrors),
            _ => terminal.execution = Some(HealExecutionOutcome::Aborted(HealAbortReason::Cancelled)),
        }
        let settled = observations
            .complete(Arc::new(terminal), Instant::now())
            .expect("terminal observed");
        assert!(!settled.deep_payload_verified_task, "case {change}");
        assert_eq!(
            recorder.total("rustfs_scanner_heal_task_deep_payload_verified_observations_total"),
            0,
            "case {change}"
        );
    }
}

#[test]
fn normal_request_proof_only_satisfies_deep_mode_for_the_same_generation() {
    for change in 0..7 {
        let mut observed = candidate();
        let mut terminal = (*completion("owner", HealObjectDisposition::VerifiedHealthy)).clone();
        terminal.scan_mode = HealScanMode::Normal;
        let receipt = terminal.object.as_mut().expect("object receipt");
        receipt.payload_integrity_verified = true;
        match change {
            0 => {}
            1 => observed.identity.as_mut().expect("candidate identity").version_id = None,
            2 => observed.data_dir = None,
            3 => receipt.resolved_data_dir = None,
            4 => receipt.resolved_data_dir = Some(Uuid::from_u128(34)),
            5 => receipt.identity.object = "another-object".into(),
            _ => receipt.payload_integrity_verified = false,
        }
        let mut recent = Recent {
            completion: Arc::new(terminal),
            counted: false,
        };
        let settled = Settlement::new(observed, &mut recent);
        let expected = match change {
            0 => "content_and_leader_unknown",
            1 => "identity_unknown",
            _ => "insufficient_scan",
        };
        assert_eq!(settled.scope, expected, "case {change}");
        assert_eq!(settled.generation_mismatch, change == 4, "case {change}");
        assert_eq!(settled.deep_payload_verified_task, change != 6, "case {change}");
    }
}

#[test]
fn merged_candidates_keep_scan_mode_and_dry_run_scopes_separate() {
    let recorder = CompletionRecorder::default();
    let _recorder = metrics::set_default_local_recorder(&recorder);
    let mut observations = Observations::default();
    let mut normal = candidate();
    normal.mode = HealScanMode::Normal;
    let mut dry = candidate();
    dry.dry_run = true;
    observations.admit("owner".into(), candidate());
    observations.admit("owner".into(), normal);
    observations.admit("owner".into(), dry);
    assert_eq!(observations.pending_groups, 3);
    let mut terminal = (*completion("owner", HealObjectDisposition::MetadataHealthy)).clone();
    terminal.scan_mode = HealScanMode::Normal;
    terminal.object.as_mut().expect("object receipt").resolved_data_dir = Some(Uuid::from_u128(34));
    observations.complete(Arc::new(terminal), Instant::now());
    assert_eq!(observations.unknown_counts.get("insufficient_scan"), Some(&1));
    assert_eq!(observations.unknown_counts.get("identity_mismatch"), Some(&1));
    assert_eq!(observations.unknown_counts.get("dry_run"), Some(&1));
    assert_eq!(recorder.total("rustfs_scanner_heal_candidate_generation_mismatch_total"), 3);
    assert_eq!(observations.terminal_tasks, 1);
}

#[test]
fn generation_mismatch_counter_requires_matching_object_identity_and_known_data_dirs() {
    for change in 0..6 {
        let mut observed = candidate();
        let mut terminal = (*completion("unrelated", HealObjectDisposition::VerifiedHealthy)).clone();
        let receipt = terminal.object.as_mut().expect("object receipt");
        receipt.resolved_data_dir = Some(Uuid::from_u128(34));
        match change {
            0 => receipt.identity.bucket = "another-bucket".into(),
            1 => receipt.identity.object = "another-object".into(),
            2 => receipt.identity.kind = HealObjectKind::Metadata,
            3 => observed.data_dir = None,
            4 => observed.data_dir = Some(Uuid::nil()),
            _ => receipt.resolved_data_dir = Some(Uuid::nil()),
        }
        let mut recent = Recent {
            completion: Arc::new(terminal),
            counted: false,
        };
        assert!(!Settlement::new(observed, &mut recent).generation_mismatch, "case {change}");
    }
}

#[test]
fn group_expiry_and_lag_release_capacity_without_settling_candidates() {
    let mut observations = Observations::default();
    let now = Instant::now();
    let mut old = candidate();
    old.first_seen = now;
    let mut newer = candidate();
    newer.data_dir = Some(Uuid::from_u128(34));
    newer.first_seen = now + Duration::from_secs(1);
    observations.admit("owner".into(), old);
    observations.admit("owner".into(), newer);
    observations.maintain(now + PENDING_TTL);
    assert_eq!(observations.pending_groups, 1);
    assert_eq!(observations.pending["owner"].first.data_dir, Some(Uuid::from_u128(34)));
    assert_eq!(observations.unknown_counts.get("expired"), Some(&1));

    observations.lagged(1);
    assert_eq!(observations.pending_groups, 0);
    assert_eq!(observations.unknown_counts.get("completion_lost"), Some(&1));
    assert!(
        observations
            .complete(completion("owner", HealObjectDisposition::VerifiedHealthy), Instant::now())
            .is_none()
    );
    assert_eq!(observations.terminal_tasks, 0);
    assert!(observations.admit("new-owner".into(), candidate()).is_none());
    assert_eq!(observations.pending_groups, 1);
}

#[test]
fn matching_merged_generations_still_reject_a_replacement_terminal() {
    let mut observations = Observations::default();
    assert!(observations.admit("owner".into(), candidate()).is_none());
    assert!(observations.admit("owner".into(), candidate()).is_none());
    let mut terminal = (*completion("owner", HealObjectDisposition::VerifiedHealthy)).clone();
    terminal.object.as_mut().expect("object receipt").resolved_data_dir = Some(Uuid::from_u128(34));
    let settled = observations
        .complete(Arc::new(terminal), Instant::now())
        .expect("canonical terminal observed");
    assert_eq!(settled.candidates, 2);
    assert_eq!(settled.scope, "identity_mismatch");
    assert_eq!(observations.unknown_counts.get("identity_mismatch"), Some(&2));
}

#[test]
fn early_terminal_compares_each_late_admission_generation() {
    let mut observations = Observations::default();
    let terminal = completion("fast", HealObjectDisposition::VerifiedHealthy);
    assert!(observations.complete(terminal.clone(), Instant::now()).is_none());
    let mut replaced = candidate();
    replaced.data_dir = Some(Uuid::from_u128(34));
    let first = observations.admit("fast".into(), replaced).expect("early terminal retained");
    assert_eq!(first.scope, "identity_mismatch");
    assert!(first.first_task_observation);
    assert!(observations.complete(terminal, Instant::now()).is_none(), "duplicate terminal ignored");
    let second = observations
        .admit("fast".into(), candidate())
        .expect("merged admission observed");
    assert_eq!(second.scope, "content_and_leader_unknown");
    assert!(!second.first_task_observation);
    assert_eq!(observations.terminal_tasks, 1);
}

#[test]
fn missing_identity_fields_are_unknown_on_either_side() {
    let changes: [(&str, fn(&mut HealObjectIdentity)); 6] = [
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
                Candidate::from_request(&scanner_request, Some(Uuid::from_u128(33))).expect("bounded scanner candidate"),
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
    assert!(Candidate::from_request(&large, Some(Uuid::from_u128(33))).is_none());
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
async fn broadcast_terminal_rejects_overwrite_for_merged_candidates() {
    use rustfs_heal_contracts::{completion::publish_heal_completion, heal_channel::HealAdmissionResult};

    let registry = Mutex::new(None);
    let observer = observer_from(&registry);
    let canonical = Uuid::new_v4().to_string();
    let object = crate::ScannerObjectInfo {
        data_dir: Some(Uuid::from_u128(33)),
        ..Default::default()
    };
    for result in [HealAdmissionResult::Accepted, HealAdmissionResult::Merged] {
        observe_on(
            &observer,
            Candidate::from_request(&request(), object.data_dir),
            Ok(&HealAdmissionReceipt {
                result,
                task_id: canonical.clone(),
            }),
        );
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while observation_status_from(&registry).retained_pending_candidates != Some(2) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both admissions must reach the observer before the terminal");

    let mut terminal = (*completion(&canonical, HealObjectDisposition::VerifiedHealthy)).clone();
    terminal.object.as_mut().expect("object receipt").resolved_data_dir = Some(Uuid::from_u128(34));
    publish_heal_completion(terminal);
    tokio::time::timeout(Duration::from_secs(2), async {
        while observation_status_from(&registry).terminal_task_observations_total != Some(1) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("broadcast terminal must reach the observer");

    let status = observation_status_from(&registry);
    assert_eq!(status.retained_pending_candidates, Some(0));
    assert_eq!(status.integrity_state, "unknown");
    assert_eq!(status.terminal_candidates.len(), 1);
    assert_eq!(status.terminal_candidates[0].scope, "identity_mismatch");
    assert_eq!(status.terminal_candidates[0].count, 2);
    assert_eq!(status.unknown_candidates.len(), 1);
    assert_eq!(status.unknown_candidates[0].reason, "identity_mismatch");
    assert_eq!(status.unknown_candidates[0].count, 2);
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
    observe_on(&sender, Candidate::from_request(&source_request, Some(Uuid::from_u128(33))), Ok(&merged));
    recorder.wait_for_terminals(2).await;
    observe_on(&sender, Candidate::from_request(&source_request, Some(Uuid::from_u128(33))), Ok(&merged));
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
