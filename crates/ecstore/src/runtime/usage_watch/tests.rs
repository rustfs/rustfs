// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

fn request(bucket: &str) -> ScannerUsageWatchRequest {
    ScannerUsageWatchRequest {
        bucket: bucket.into(),
        bucket_incarnation: Uuid::from_u128(7),
        watch_id: None,
        release: false,
        session_id: None,
    }
}

fn query(registry: &UsageWatchRegistry, request: &ScannerUsageWatchRequest, at: Instant) -> ScannerUsageWatchKnown {
    registry
        .observe_at(Uuid::from_u128(1), request, at)
        .expect("known bucket proof")
}

#[test]
fn watch_installation_detects_an_already_active_mutation() {
    let registry = Arc::new(UsageWatchRegistry::default());
    let now = Instant::now();
    let mutation = registry.begin(Some("bucket"));
    let before = query(&registry, &request("bucket"), now);
    assert_eq!(before.inflight, 1);
    drop(mutation);
    let after = query(&registry, &request("bucket"), now);
    assert_eq!(after.inflight, 0);
    assert!(after.revision > before.revision);
    assert!(!after.unchanged_since(&before));
}

#[test]
fn every_begin_and_physical_tail_end_advances_only_the_target_bucket() {
    let registry = Arc::new(UsageWatchRegistry::default());
    let now = Instant::now();
    let before = query(&registry, &request("cold"), now);
    let hot = registry.begin(Some("hot"));
    assert!(query(&registry, &request("cold"), now).unchanged_since(&before));
    drop(hot);
    assert!(query(&registry, &request("cold"), now).unchanged_since(&before));
    let owner = Arc::new(registry.begin(Some("cold")));
    let physical_tail = Arc::clone(&owner);
    let during = query(&registry, &request("cold"), now);
    assert_eq!(during.revision, before.revision + 1);
    assert_eq!(during.inflight, 1);
    drop(owner);
    assert_eq!(query(&registry, &request("cold"), now).inflight, 1);
    drop(physical_tail);
    let after = query(&registry, &request("cold"), now);
    assert_eq!(after.revision, before.revision + 2);
    assert_eq!(after.inflight, 0);
}

#[test]
fn expiry_reincarnation_and_restart_do_not_resurrect_old_tokens() {
    let registry = UsageWatchRegistry::default();
    let now = Instant::now();
    let before = query(&registry, &request("bucket"), now);
    let mut refresh = request("bucket");
    refresh.watch_id = Some(before.watch_id);
    let expired = now + Duration::from_millis(SCANNER_USAGE_WATCH_LEASE_MS);
    assert_eq!(registry.observe_at(Uuid::from_u128(1), &refresh, expired), None);
    let after = query(&registry, &request("bucket"), expired);
    assert_ne!(after.watch_id, before.watch_id);
    assert!(!after.unchanged_since(&before));
    refresh.bucket_incarnation = Uuid::new_v4();
    assert_eq!(registry.observe_at(Uuid::from_u128(1), &refresh, expired), None);
    refresh.watch_id = None;
    let recreated = query(&registry, &refresh, expired);
    assert_ne!(recreated.watch_id, after.watch_id);
    let restarted = UsageWatchRegistry::default();
    let restart_proof = query(&restarted, &refresh, expired);
    assert!(!restart_proof.unchanged_since(&recreated));
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-12", "assertion_id": "USAGE-12-A05", "status": "PASS",
            "evidence": {"expired_token": before.watch_id, "replacement_token": after.watch_id, "lease_ms": SCANNER_USAGE_WATCH_LEASE_MS, "expired_proof_accepted": false}
        })
    );
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-12", "assertion_id": "USAGE-12-A06", "status": "PASS",
            "evidence": {"previous_session": recreated.session_id, "new_session": restart_proof.session_id, "unchanged": restart_proof.unchanged_since(&recreated)}
        })
    );
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-12", "status": "PASS", "scope": "process-local"})
    );
}

#[test]
fn watch_capacity_is_bounded_and_rejected_entries_are_unknown() {
    let registry = UsageWatchRegistry::default();
    let now = Instant::now();
    for index in 0..SCANNER_USAGE_WATCH_MAX_ENTRIES {
        query(&registry, &request(&format!("bucket-{index}")), now);
    }
    assert_eq!(registry.observe_at(Uuid::from_u128(1), &request("overflow"), now), None);
    let state = registry.state.lock().expect("registry state");
    assert_eq!(state.watches.len(), SCANNER_USAGE_WATCH_MAX_ENTRIES);
    assert!(state.watch_name_bytes <= SCANNER_USAGE_WATCH_MAX_NAME_BYTES);
}

#[test]
fn active_mutation_overflow_revokes_all_proofs_until_untracked_work_ends() {
    let registry = Arc::new(UsageWatchRegistry::default());
    let now = Instant::now();
    let old = query(&registry, &request("cold"), now);
    let mut active = Vec::new();
    for index in 0..SCANNER_USAGE_WATCH_MAX_ACTIVE_BUCKETS {
        active.push(registry.begin(Some(&format!("active-{index}"))));
    }
    let overflow = registry.begin(Some("not-in-active-map"));
    assert_eq!(registry.observe_at(Uuid::from_u128(1), &request("cold"), now), None);
    {
        let state = registry.state.lock().expect("registry state");
        assert_eq!(state.active.len(), SCANNER_USAGE_WATCH_MAX_ACTIVE_BUCKETS);
        assert!(state.active_name_bytes <= SCANNER_USAGE_WATCH_MAX_ACTIVE_NAME_BYTES);
    }
    drop(overflow);
    let new = query(&registry, &request("cold"), now);
    assert_ne!(new.watch_id, old.watch_id);
    assert!(new.coverage_epoch > old.coverage_epoch);
    drop(active);
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-12", "assertion_id": "USAGE-12-A04", "status": "PASS",
            "evidence": {"active_capacity": SCANNER_USAGE_WATCH_MAX_ACTIVE_BUCKETS, "previous_token": old.watch_id, "replacement_token": new.watch_id, "previous_coverage": old.coverage_epoch, "new_coverage": new.coverage_epoch, "active_after_cleanup": registry.state.lock().expect("drained registry").active.len()}
        })
    );
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-12", "status": "PASS", "scope": "process-local"})
    );
}

#[test]
fn unknown_scope_and_replay_invalidate_without_reusing_generation() {
    let registry = Arc::new(UsageWatchRegistry::default());
    let now = Instant::now();
    let old = query(&registry, &request("bucket"), now);
    let mutation = registry.begin(None);
    assert_eq!(registry.observe_at(Uuid::from_u128(1), &request("bucket"), now), None);
    drop(mutation);
    let new = query(&registry, &request("bucket"), now);
    assert_ne!(new.watch_id, old.watch_id);
    registry.invalidate();
    let replayed = query(&registry, &request("bucket"), now);
    assert_ne!(replayed.watch_id, new.watch_id);
    assert!(replayed.coverage_epoch > new.coverage_epoch);
}

#[test]
fn revision_exhaustion_invalidates_the_token_without_aba() {
    let registry = Arc::new(UsageWatchRegistry::default());
    let now = Instant::now();
    let old = query(&registry, &request("bucket"), now);
    registry
        .state
        .lock()
        .expect("registry state")
        .watches
        .get_mut("bucket")
        .expect("installed watch")
        .revision = u64::MAX - 1;
    let mutation = registry.begin(Some("bucket"));
    let refresh = ScannerUsageWatchRequest {
        watch_id: Some(old.watch_id),
        ..request("bucket")
    };
    assert_eq!(registry.observe_at(Uuid::from_u128(1), &refresh, now), None);
    let during = query(&registry, &request("bucket"), now);
    assert_ne!(during.watch_id, old.watch_id);
    assert_eq!(during.inflight, 1);
    drop(mutation);
    assert_eq!(query(&registry, &request("bucket"), now).revision, during.revision + 1);
}

#[test]
fn coverage_epoch_exhaustion_stays_unknown_without_wrapping() {
    let registry = UsageWatchRegistry::default();
    let now = Instant::now();
    query(&registry, &request("bucket"), now);
    registry.state.lock().expect("registry state").coverage_epoch = u64::MAX - 1;
    registry.invalidate();
    assert_eq!(registry.observe_at(Uuid::from_u128(1), &request("bucket"), now), None);
    registry.invalidate();
    assert_eq!(registry.observe_at(Uuid::from_u128(1), &request("bucket"), now), None);
    let state = registry.state.lock().expect("registry state");
    assert!(state.exhausted);
    assert!(state.watches.is_empty());
    assert_eq!(state.coverage_epoch, u64::MAX - 1);
}

#[test]
fn proof_comparison_rejects_every_changed_identity_and_busy_endpoint() {
    let registry = UsageWatchRegistry::default();
    let original = query(&registry, &request("bucket"), Instant::now());
    for field in 0..10 {
        let mut changed = original.clone();
        match field {
            0 => changed.owner_id = Uuid::new_v4(),
            1 => changed.session_id = Uuid::new_v4(),
            2 => changed.watch_id = Uuid::new_v4(),
            3 => changed.bucket_incarnation = Uuid::new_v4(),
            4 => changed.revision += 1,
            5 => changed.inflight = 1,
            6 => changed.coverage_version += 1,
            7 => changed.coverage_epoch += 1,
            8 => changed.lease_remaining_ms = 0,
            _ => {
                changed.revision = 0;
                changed.watch_id = Uuid::nil();
            }
        }
        assert!(!changed.unchanged_since(&original));
    }
}

#[test]
fn watch_contention_cost_preserves_all_mutation_boundaries() {
    let registry = Arc::new(UsageWatchRegistry::default());
    let before = query(&registry, &request("hot"), Instant::now());
    let threads = 8_u64;
    let writes_per_thread = 10_000_u64;
    let started = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..threads {
            let registry = Arc::clone(&registry);
            scope.spawn(move || {
                for _ in 0..writes_per_thread {
                    drop(registry.begin(Some("hot")));
                }
            });
        }
    });
    let elapsed = started.elapsed();
    let after = query(&registry, &request("hot"), Instant::now());
    let writes = threads * writes_per_thread;
    assert_eq!(after.revision, before.revision + 2 * writes);
    assert_eq!(after.inflight, 0);
    assert!(registry.state.lock().expect("drained registry").active.is_empty());
    eprintln!(
        "usage_watch_cost threads={threads} writes={writes} elapsed_us={} wall_ns_per_begin_end={}",
        elapsed.as_micros(),
        elapsed.as_nanos() / u128::from(writes)
    );
}

#[test]
fn exact_release_preserves_other_sessions_tokens_and_active_mutations() {
    let registry = Arc::new(UsageWatchRegistry::default());
    let owner = Uuid::from_u128(1);
    let now = Instant::now();
    let before = query(&registry, &request("bucket"), now);
    let release = ScannerUsageWatchRequest {
        watch_id: Some(before.watch_id),
        session_id: Some(before.session_id),
        release: true,
        ..request("bucket")
    };
    for case in 0..5 {
        let mut wrong = release.clone();
        let mut wrong_owner = owner;
        match case {
            0 => wrong.watch_id = Some(Uuid::new_v4()),
            1 => wrong.session_id = Some(Uuid::new_v4()),
            2 => wrong.bucket_incarnation = Uuid::new_v4(),
            3 => wrong.watch_id = None,
            _ => wrong_owner = Uuid::new_v4(),
        }
        assert_eq!(registry.observe_at(wrong_owner, &wrong, now), None);
        assert!(query(&registry, &request("bucket"), now).unchanged_since(&before));
    }
    let mutation = registry.begin(Some("bucket"));
    assert_eq!(registry.observe_at(owner, &release, now), None);
    assert!(registry.state.lock().expect("registry state").watches.is_empty());
    let replacement = query(&registry, &request("bucket"), now);
    assert_ne!(replacement.watch_id, before.watch_id);
    assert_eq!(replacement.inflight, 1, "release cannot discard physical mutation ownership");
    assert_eq!(registry.observe_at(owner, &release, now), None);
    assert_eq!(query(&registry, &request("bucket"), now).watch_id, replacement.watch_id);
    drop(mutation);
    let after = query(&registry, &request("bucket"), now);
    assert_eq!(after.inflight, 0);
    assert!(after.revision > replacement.revision);
}

#[test]
fn completed_watch_release_admits_later_buckets_before_lease_expiry() {
    let registry = UsageWatchRegistry::default();
    let now = Instant::now();
    let mut completed = Vec::new();
    for index in 0..SCANNER_USAGE_WATCH_MAX_ENTRIES {
        let req = request(&format!("bucket-{index:03}"));
        let known = query(&registry, &req, now);
        completed.push(ScannerUsageWatchRequest {
            watch_id: Some(known.watch_id),
            session_id: Some(known.session_id),
            release: true,
            ..req
        });
    }
    assert_eq!(registry.observe_at(Uuid::from_u128(1), &request("later-bucket"), now), None);
    for release in completed {
        assert_eq!(registry.observe_at(Uuid::from_u128(1), &release, now), None);
    }
    let state = registry.state.lock().expect("registry state");
    assert_eq!(state.watches.len(), 0);
    assert_eq!(state.watch_name_bytes, 0);
    drop(state);
    assert!(query(&registry, &request("later-bucket"), now).is_well_formed());
}
