// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Exercise remote completion through the real disk, decoder and collectors.

use super::*;
use crate::scanner_budget::ScannerCycleBudgetReason;
use crate::storage_api::remote_transport_fixture::{
    Error as TransportError, FileReader, FileWriter, InternodeDataTransport, InternodeDataTransportCapabilities,
    NS_SCANNER_PROTOCOL_VERSION, NsScannerCapabilityRequest, NsScannerCapabilityResponse, NsScannerStreamRequest,
    ReadStreamRequest, Result as TransportResult, WalkDirStreamRequest, WriteStreamRequest, remote_disk_with_transport,
};
use sha2::Digest;

const BUCKET: &str = "usage-native-remote-drain";
const SEED_CYCLE: u64 = 91;
const REMOTE_CYCLE: u64 = 92;
const EPOCH: u64 = 13;

struct WalkerProbe {
    set: Arc<SetDisks>,
    remote: crate::DiskStore,
}

static WALKER_PROBE: StdMutex<Option<WalkerProbe>> = StdMutex::new(None);

struct WalkerGuard;

impl Drop for WalkerGuard {
    fn drop(&mut self) {
        *WALKER_PROBE.lock().expect("remove controlled walker") = None;
        clear_dirty_usage_buckets_for_tests();
    }
}

pub(crate) fn walker_disks(set: &Arc<SetDisks>, buckets: &[BucketInfo], disks: Vec<crate::DiskStore>) -> Vec<crate::DiskStore> {
    let probe = WALKER_PROBE.lock().expect("controlled walker probe");
    match probe.as_ref() {
        Some(probe) if Arc::ptr_eq(&probe.set, set) && buckets.iter().any(|bucket| bucket.name == BUCKET) => {
            assert_eq!(buckets.len(), 1, "only the isolated fixture bucket uses this transport");
            vec![probe.remote.clone()]
        }
        _ => disks,
    }
}

struct DrainTransport {
    source_cache: DataUsageCache,
    source_store: Arc<SetDisks>,
    release_complete: CancellationToken,
    opened: AtomicUsize,
    writers: StdMutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl std::fmt::Debug for DrainTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("DrainTransport").finish_non_exhaustive()
    }
}

impl Drop for DrainTransport {
    fn drop(&mut self) {
        self.release_complete.cancel();
        for writer in self.writers.lock().expect("fixture writers during cleanup").drain(..) {
            writer.abort();
        }
    }
}

#[async_trait::async_trait]
impl InternodeDataTransport for DrainTransport {
    async fn open_read(&self, _request: ReadStreamRequest) -> TransportResult<FileReader> {
        Err(TransportError::MethodNotAllowed)
    }

    async fn open_write(&self, _request: WriteStreamRequest) -> TransportResult<FileWriter> {
        Err(TransportError::MethodNotAllowed)
    }

    async fn open_walk_dir(&self, _request: WalkDirStreamRequest) -> TransportResult<FileReader> {
        Err(TransportError::MethodNotAllowed)
    }

    async fn open_ns_scanner(&self, request: NsScannerStreamRequest) -> TransportResult<FileReader> {
        assert_eq!(request.next_cycle, REMOTE_CYCLE);
        assert_eq!(request.leader_epoch, EPOCH);
        self.opened.fetch_add(1, Ordering::AcqRel);
        let (reader, writer) = tokio::io::duplex(64 * 1024);
        let cache = self.source_cache.clone();
        let store = self.source_store.clone();
        let release = self.release_complete.clone();
        let writer = tokio::spawn(async move {
            crate::remote_scanner::write_budget_drain_fixture_frames(
                writer,
                &request.body,
                DataUsageCacheSource::new(0, 0),
                cache,
                store,
                release,
            )
            .await;
        });
        self.writers
            .lock()
            .expect("register controlled transport writer")
            .push(writer);
        Ok(Box::new(reader))
    }

    async fn probe_ns_scanner_capability(
        &self,
        request: NsScannerCapabilityRequest,
    ) -> TransportResult<NsScannerCapabilityResponse> {
        assert!(request.supports_checkpoint_coverage);
        assert!(request.supports_durable_progress);
        Ok(NsScannerCapabilityResponse {
            version: NS_SCANNER_PROTOCOL_VERSION,
            server_epoch: Uuid::from_u128(0x9292),
            proof: Vec::new(),
            supports_tier_registry_generation: Some(true),
            supports_checkpoint_coverage: Some(true),
            supports_durable_progress: Some(true),
            supports_complete_observation: Some(true),
            usage_watch_proof: None,
        })
    }

    fn name(&self) -> &'static str {
        "tcp-http"
    }

    fn capabilities(&self) -> InternodeDataTransportCapabilities {
        InternodeDataTransportCapabilities::tcp_http()
    }
}

fn request(
    owner: &CancellationToken,
    budget: &Arc<ScannerCycleBudget>,
    updates: mpsc::Sender<DataUsageInfo>,
    cycle: u64,
) -> ScannerCycleRequest {
    ScannerCycleRequest {
        owner_ctx: owner.clone(),
        ctx: budget.token(),
        budget: budget.clone(),
        updates,
        want_cycle: cycle,
        leader_epoch: EPOCH,
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
    }
}

#[tokio::test]
#[serial]
async fn usage_remote_complete_after_count_stop_reaches_real_candidate() {
    let _ = rustfs_credentials::set_global_rpc_secret("usage-native-controlled-transport-secret".to_string());
    let (directory, store) = setup_local_scanner_store(1, 4).await;
    clear_dirty_usage_buckets_for_tests();
    store
        .make_bucket(BUCKET, &MakeBucketOptions::default())
        .await
        .expect("create remote-drain physical bucket");
    store
        .put_object(
            BUCKET,
            "object",
            &mut ScannerPutObjReader::from_vec(b"remote-root".to_vec()),
            &ScannerObjectOptions::default(),
        )
        .await
        .expect("persist the source object");
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let owner = CancellationToken::new();
    let seed_budget = ScannerCycleBudget::new(&owner, Default::default());
    let (updates, mut receiver) = mpsc::channel(1);
    let seed = tokio::time::timeout(
        Duration::from_secs(60),
        nsscanner_with_storage_status_scoped(store.as_ref(), request(&owner, &seed_budget, updates, SEED_CYCLE)),
    )
    .await
    .expect("bounded physical seed scan")
    .expect("physical seed scan");
    assert_eq!(seed.status, ScannerCycleStatus::Complete);
    let seed_usage = receiver.recv().await.expect("physical source usage");
    assert_eq!(seed_usage.buckets_usage[BUCKET].objects_count, 1);
    assert_eq!(seed_usage.buckets_usage[BUCKET].size, 11);
    assert!(receiver.recv().await.is_none());
    let set = store.pools[0].disk_set[0].clone();
    let mut durable = DataUsageCache::default();
    durable
        .load(set.clone(), &path_join_buf(&[BUCKET, DATA_USAGE_CACHE_NAME]))
        .await
        .expect("read the actual durable bucket root");
    assert!(durable.info.snapshot_complete);
    let usage = cache_root_entry_info(&durable).expect("flatten the actual durable root");
    assert_eq!(usage.entry.objects, 1);
    assert_eq!(usage.entry.size, 11);
    let transport = Arc::new(DrainTransport {
        source_cache: durable,
        source_store: set.clone(),
        release_complete: CancellationToken::new(),
        opened: AtomicUsize::new(0),
        writers: StdMutex::new(Vec::new()),
    });
    let mut endpoint = Endpoint::try_from("http://192.0.2.1:9000/usage-native-remote").expect("controlled remote endpoint");
    endpoint.is_local = false;
    endpoint.set_pool_index(0);
    endpoint.set_set_index(0);
    endpoint.set_disk_index(0);
    let remote = remote_disk_with_transport(&endpoint, transport.clone())
        .await
        .expect("actual RemoteDisk with fixture transport");
    assert!(!remote.is_local());
    {
        let mut slot = WALKER_PROBE.lock().expect("install controlled walker");
        assert!(slot.is_none());
        *slot = Some(WalkerProbe {
            set: set.clone(),
            remote,
        });
    }
    let guard = WalkerGuard;
    record_dirty_usage_bucket(BUCKET);
    let dirty_before = dirty_usage_buckets_for_tests();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &owner,
        ScannerCycleBudgetConfig {
            max_objects: Some(1),
            ..Default::default()
        },
    );
    // This path includes physical bucket and source-cache commits. Use the
    // production drain window instead of the accelerated unit-test timer.
    budget.use_production_drain_grace_for_tests();
    let budget_stopped = budget.token();
    let (updates, mut receiver) = mpsc::channel(1);
    let (scan, first_deadline) = tokio::time::timeout(Duration::from_secs(60), async {
        let scan = nsscanner_with_storage_status_scoped(store.as_ref(), request(&owner, &budget, updates, REMOTE_CYCLE));
        tokio::pin!(scan);
        tokio::select! {
            result = &mut scan => panic!("remote Complete must remain gated before count stop: {result:?}"),
            _ = budget_stopped.cancelled() => {}
        }
        assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Objects));
        assert!(!owner.is_cancelled(), "a count stop preserves owner authority");
        assert_eq!(transport.opened.load(Ordering::Acquire), 1, "the real remote path must open exactly once");
        assert!(matches!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        let first_deadline = budget
            .drain_deadline(Duration::from_secs(30))
            .expect("first budget drain deadline");
        assert_eq!(budget.shutdown_deadline(), first_deadline);
        transport.release_complete.cancel();
        (scan.await.expect("accept the authenticated late Complete"), first_deadline)
    })
    .await
    .expect("the actual pipeline must finish within the fixture deadline");
    assert_eq!(scan.status, ScannerCycleStatus::Complete, "the actual remote drain result: {scan:#?}");
    assert!(!owner.is_cancelled());
    assert_eq!(budget.drain_deadline(Duration::from_secs(30)), Some(first_deadline));
    assert_eq!(budget.shutdown_deadline(), first_deadline);
    assert!(
        tokio::time::Instant::now() <= first_deadline,
        "the physical aggregate must finish within the original drain"
    );
    let candidate = receiver
        .recv()
        .await
        .expect("remote Complete must reach candidate preparation");
    assert!(receiver.recv().await.is_none());
    assert_eq!(candidate.scanner_cycle, Some(REMOTE_CYCLE));
    assert_eq!(candidate.buckets_usage[BUCKET].objects_count, 1);
    assert_eq!(candidate.buckets_usage[BUCKET].size, 11);
    assert!(
        candidate.usage_snapshot_pending_buckets.is_empty(),
        "the Complete must not be downgraded to carry"
    );
    assert_eq!(candidate.usage_snapshot_set_states.len(), 1);
    assert!(candidate.usage_snapshot_set_states[0].complete);
    assert_eq!(candidate.usage_snapshot_set_states[0].scanner_cycle, Some(REMOTE_CYCLE));
    let bytes = serde_json::to_vec(&candidate).expect("actual candidate bytes");
    assert!(
        scan.publication_expectation()
            .expect("real candidate publication decision")
            .matches_encoded_candidate(&sha2::Sha256::digest(&bytes).into())
    );
    let mut source = DataUsageCache::default();
    source
        .load(set.clone(), DATA_USAGE_CACHE_NAME)
        .await
        .expect("load collector's durable source aggregate");
    let origin = source
        .info
        .scan_bucket_value_origins
        .get(BUCKET)
        .expect("remote contribution provenance");
    assert_eq!(
        origin.cycle, REMOTE_CYCLE,
        "the aggregate must contain this remote Complete, not the seed"
    );
    assert!(!origin.carried);
    assert_eq!(dirty_usage_buckets_for_tests(), dirty_before, "candidate preparation alone does not ACK");
    let writers = std::mem::take(&mut *transport.writers.lock().expect("take fixture writer handles"));
    for writer in writers {
        writer.await.expect("authenticated writer completed and shut down");
    }
    let evidence = serde_json::json!({
        "transport_opens": transport.opened.load(Ordering::Acquire), "budget_reason": "objects",
        "owner_live_after_complete": !owner.is_cancelled(), "source_origin_cycle": origin.cycle,
        "physical_objects": 1, "physical_bytes": 11, "pending_buckets": candidate.usage_snapshot_pending_buckets.len(),
        "candidate_sha256": hex_simd::encode_to_string(sha2::Sha256::digest(&bytes), hex_simd::AsciiCase::Lower),
        "transport": "controlled signed frames through RemoteDisk", "source_root": "physical scanner fixture and current-request CAS read-back",
    });
    drop(guard);
    drop(transport);
    drop(set);
    drop(store);
    let fixture_path = directory.path().to_path_buf();
    directory.close().expect("remove remote-drain physical fixture");
    assert!(!fixture_path.exists());
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-13", "assertion_id": "USAGE-13-A03", "status": "PASS", "evidence": evidence,
        })
    );
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-13", "status": "PASS", "fixture_removed": !fixture_path.exists()})
    );
}
