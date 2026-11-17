// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! A real source collector must leave before its candidate can be sealed.

use super::*;
use sha2::Digest;

const SEAL_BUCKET: &str = "usage-native-candidate-seal";

#[derive(Default)]
struct CollectorProbe {
    entered: CancellationToken,
    release: CancellationToken,
    senders: StdMutex<Vec<mpsc::WeakSender<DataUsageCache>>>,
}

static PROBE: StdMutex<Option<Arc<CollectorProbe>>> = StdMutex::new(None);

struct ProbeGuard(Arc<CollectorProbe>);

impl ProbeGuard {
    fn install() -> Self {
        let probe = Arc::new(CollectorProbe::default());
        let mut slot = PROBE.lock().expect("candidate collector probe lock");
        assert!(slot.is_none(), "candidate collector fixtures must run serially");
        *slot = Some(probe.clone());
        Self(probe)
    }
}

impl Drop for ProbeGuard {
    fn drop(&mut self) {
        self.0.release.cancel();
        *PROBE.lock().expect("candidate collector probe cleanup") = None;
        clear_dirty_usage_buckets_for_tests();
    }
}

pub(crate) async fn observe_collector(cache: &DataUsageCache, sender: &mpsc::WeakSender<DataUsageCache>) {
    if cache.find(SEAL_BUCKET).is_none() {
        return;
    }
    let probe = PROBE.lock().expect("candidate collector probe lock").clone();
    let Some(probe) = probe else { return };
    probe
        .senders
        .lock()
        .expect("candidate collector sender list")
        .push(sender.clone());
    probe.entered.cancel();
    probe.release.cancelled().await;
}

#[tokio::test]
#[serial]
async fn usage_sealed_candidate_rejects_late_source_collector_updates() {
    let (directory, store) = setup_local_scanner_store(1, 4).await;
    clear_dirty_usage_buckets_for_tests();
    store
        .make_bucket(SEAL_BUCKET, &MakeBucketOptions::default())
        .await
        .expect("create candidate seal bucket");
    store
        .put_object(
            SEAL_BUCKET,
            "object",
            &mut ScannerPutObjReader::from_vec(b"sealed".to_vec()),
            &ScannerObjectOptions::default(),
        )
        .await
        .expect("persist candidate seal fixture");
    wait_for_namespace_commit_tails(store.as_ref()).await;
    record_dirty_usage_bucket(SEAL_BUCKET);
    let dirty_before = dirty_usage_buckets_for_tests();
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    let guard = ProbeGuard::install();
    let (updates, mut receiver) = mpsc::channel(1);
    let scan = tokio::time::timeout(Duration::from_secs(60), async {
        let scan = nsscanner_with_storage_status_scoped(
            store.as_ref(),
            ScannerCycleRequest {
                owner_ctx: ctx.clone(),
                ctx: budget.token(),
                budget: budget.clone(),
                updates,
                want_cycle: 71,
                leader_epoch: 11,
                scan_mode: HealScanMode::Normal,
                scan_scope: ScannerBucketScanScope::default(),
                persisted_usage_baseline: None,
                observed_usage_candidate: None,
                requires_full_scan: true,
                service_cohort: None,
                resolved_scope_observer: None,
                post_walk_hook: None,
                post_walk_gate: None,
                peer_observations: None,
            },
        );
        tokio::pin!(scan);
        tokio::select! {
            result = &mut scan => panic!("the real source collector must gate candidate creation: {result:?}"),
            _ = guard.0.entered.cancelled() => {}
        }
        assert!(
            futures::poll!(&mut scan).is_pending(),
            "the source collector must join before finalization"
        );
        assert!(
            matches!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "no candidate before its source joins"
        );
        guard.0.release.cancel();
        scan.await.expect("complete the real collector and prepare its candidate")
    })
    .await
    .expect("the gated collector must complete within its fixture deadline");
    assert_eq!(scan.status, ScannerCycleStatus::Complete);
    let candidate = receiver.recv().await.expect("sealed usage candidate");
    assert!(receiver.recv().await.is_none(), "one terminal candidate leaves no publisher behind");
    assert_eq!(candidate.buckets_usage[SEAL_BUCKET].objects_count, 1);
    assert_eq!(candidate.buckets_usage[SEAL_BUCKET].size, 6);
    let before = serde_json::to_vec(&candidate).expect("sealed candidate bytes");
    let digest: [u8; 32] = sha2::Sha256::digest(&before).into();
    let expectation = scan.publication_expectation().expect("real prepared expectation");
    assert!(expectation.matches_encoded_candidate(&digest));
    let senders = guard.0.senders.lock().expect("captured real collector endpoints").clone();
    assert!(!senders.is_empty(), "the test must observe a real source message");
    let rejected = senders.iter().filter(|sender| sender.upgrade().is_none()).count();
    assert_eq!(
        rejected,
        senders.len(),
        "all old source endpoints must be closed after the collector joins"
    );
    tokio::task::yield_now().await;
    let after = serde_json::to_vec(&candidate).expect("candidate after attempted late source reuse");
    assert_eq!(after, before, "values, observations, certificates and pending marks are sealed together");
    assert!(expectation.matches_encoded_candidate(&sha2::Sha256::digest(&after).into()));
    assert_eq!(
        dirty_usage_buckets_for_tests(),
        dirty_before,
        "sealing alone never acknowledges dirty work"
    );
    let evidence = serde_json::json!({
        "captured_source_messages": senders.len(), "late_source_endpoints_rejected": rejected,
        "sealed_candidate_sha256": hex_simd::encode_to_string(sha2::Sha256::digest(&before), hex_simd::AsciiCase::Lower),
        "retained_dirty_buckets": dirty_before.len(), "objects_count": candidate.buckets_usage[SEAL_BUCKET].objects_count,
    });
    drop(guard);
    drop(store);
    let fixture_path = directory.path().to_path_buf();
    directory.close().expect("remove candidate seal physical disk fixture");
    assert!(!fixture_path.exists());
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-13", "assertion_id": "USAGE-13-A07", "status": "PASS", "evidence": evidence,
        })
    );
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-13", "status": "PASS", "fixture_removed": !fixture_path.exists()})
    );
}
