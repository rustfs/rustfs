// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::super::*;
use super::*;
use std::io::Cursor;
use std::sync::RwLock;
use std::sync::atomic::AtomicUsize;

const TEST_SOURCE: DataUsageCacheSource = DataUsageCacheSource::new(1, 2);
const TEST_PLAN_DIGEST: DataUsageScanPlanDigest = DataUsageScanPlanDigest([5; 32]);

#[test]
fn semantic_deadline_is_capped_by_rpc_lifetime_without_overflow() {
    let now = Instant::now();
    let rpc_deadline = now + Duration::from_secs(10);

    assert_eq!(bounded_remote_scanner_deadline(now, Duration::MAX, rpc_deadline), rpc_deadline);
    assert_eq!(
        bounded_remote_scanner_deadline(now, Duration::from_secs(2), rpc_deadline),
        now + Duration::from_secs(2)
    );
}

#[derive(Debug)]
struct CycleStateStore {
    state: Option<Vec<u8>>,
}

#[derive(Debug)]
struct CountingCycleStateStore {
    state: Vec<u8>,
    reads: AtomicUsize,
}

#[derive(Debug)]
struct MutableCycleStateStore {
    state: RwLock<Vec<u8>>,
}

#[async_trait::async_trait]
impl crate::storage_api::owner::ObjectIO for CycleStateStore {
    type Error = EcstoreError;
    type RangeSpec = crate::storage_api::owner::HTTPRangeSpec;
    type HeaderMap = http::HeaderMap;
    type ObjectOptions = crate::ScannerObjectOptions;
    type ObjectInfo = crate::ScannerObjectInfo;
    type GetObjectReader = crate::ScannerGetObjectReader;
    type PutObjectReader = crate::ScannerPutObjReader;

    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        _range: Option<Self::RangeSpec>,
        _h: Self::HeaderMap,
        _opts: &Self::ObjectOptions,
    ) -> Result<Self::GetObjectReader, Self::Error> {
        if bucket != RUSTFS_META_BUCKET || object != DATA_USAGE_BLOOM_NAME_PATH.as_str() {
            return Err(EcstoreError::FileNotFound);
        }
        let state = self.state.clone().ok_or(EcstoreError::FileNotFound)?;
        Ok(crate::ScannerGetObjectReader {
            stream: Box::new(Cursor::new(state)),
            object_info: crate::ScannerObjectInfo {
                etag: Some("cycle-state".to_string()),
                ..Default::default()
            },
            buffered_body: None,
            body_source: Default::default(),
        })
    }

    async fn put_object(
        &self,
        _bucket: &str,
        _object: &str,
        _data: &mut Self::PutObjectReader,
        _opts: &Self::ObjectOptions,
    ) -> Result<Self::ObjectInfo, Self::Error> {
        Err(EcstoreError::other("cycle state test store is read-only"))
    }
}

#[async_trait::async_trait]
impl crate::storage_api::owner::ObjectIO for CountingCycleStateStore {
    type Error = EcstoreError;
    type RangeSpec = crate::storage_api::owner::HTTPRangeSpec;
    type HeaderMap = http::HeaderMap;
    type ObjectOptions = crate::ScannerObjectOptions;
    type ObjectInfo = crate::ScannerObjectInfo;
    type GetObjectReader = crate::ScannerGetObjectReader;
    type PutObjectReader = crate::ScannerPutObjReader;

    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        _range: Option<Self::RangeSpec>,
        _h: Self::HeaderMap,
        _opts: &Self::ObjectOptions,
    ) -> Result<Self::GetObjectReader, Self::Error> {
        if bucket != RUSTFS_META_BUCKET || object != DATA_USAGE_BLOOM_NAME_PATH.as_str() {
            return Err(EcstoreError::FileNotFound);
        }
        self.reads.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(25)).await;
        Ok(crate::ScannerGetObjectReader {
            stream: Box::new(Cursor::new(self.state.clone())),
            object_info: crate::ScannerObjectInfo {
                etag: Some("cycle-state".to_string()),
                ..Default::default()
            },
            buffered_body: None,
            body_source: Default::default(),
        })
    }

    async fn put_object(
        &self,
        _bucket: &str,
        _object: &str,
        _data: &mut Self::PutObjectReader,
        _opts: &Self::ObjectOptions,
    ) -> Result<Self::ObjectInfo, Self::Error> {
        Err(EcstoreError::other("cycle state test store is read-only"))
    }
}

#[async_trait::async_trait]
impl crate::storage_api::owner::ObjectIO for MutableCycleStateStore {
    type Error = EcstoreError;
    type RangeSpec = crate::storage_api::owner::HTTPRangeSpec;
    type HeaderMap = http::HeaderMap;
    type ObjectOptions = crate::ScannerObjectOptions;
    type ObjectInfo = crate::ScannerObjectInfo;
    type GetObjectReader = crate::ScannerGetObjectReader;
    type PutObjectReader = crate::ScannerPutObjReader;

    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        _range: Option<Self::RangeSpec>,
        _h: Self::HeaderMap,
        _opts: &Self::ObjectOptions,
    ) -> Result<Self::GetObjectReader, Self::Error> {
        if bucket != RUSTFS_META_BUCKET || object != DATA_USAGE_BLOOM_NAME_PATH.as_str() {
            return Err(EcstoreError::FileNotFound);
        }
        let state = self
            .state
            .read()
            .map_err(|_| EcstoreError::other("cycle state test lock is poisoned"))?
            .clone();
        Ok(crate::ScannerGetObjectReader {
            stream: Box::new(Cursor::new(state)),
            object_info: crate::ScannerObjectInfo {
                etag: Some("cycle-state".to_string()),
                ..Default::default()
            },
            buffered_body: None,
            body_source: Default::default(),
        })
    }

    async fn put_object(
        &self,
        _bucket: &str,
        _object: &str,
        _data: &mut Self::PutObjectReader,
        _opts: &Self::ObjectOptions,
    ) -> Result<Self::ObjectInfo, Self::Error> {
        Err(EcstoreError::other("cycle state test store is read-only"))
    }
}

fn test_usage(bucket: &str, objects: usize) -> DataUsageEntryInfo {
    let entry = crate::DataUsageEntry {
        objects,
        ..Default::default()
    };
    DataUsageEntryInfo {
        name: bucket.to_string(),
        parent: crate::DATA_USAGE_ROOT.to_string(),
        entry,
    }
}

fn test_request(request_id: Uuid) -> RemoteScannerRequestWire {
    RemoteScannerRequestWire {
        version: NS_SCANNER_PROTOCOL_VERSION,
        request_id,
        server_epoch: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
        session_sequence: 0,
        bucket: "bucket".to_string(),
        next_cycle: 7,
        leader_epoch: 9,
        scan_plan_digest: TEST_PLAN_DIGEST,
        skip_healing: true,
        scan_mode: HealScanMode::Deep,
        budget: RemoteScannerBudget {
            max_duration_ms: Some(1234),
            max_objects: Some(10),
            max_directories: Some(20),
        },
    }
}

#[test]
fn request_round_trip_preserves_scan_inputs_without_cache_data() {
    let request_id = Uuid::new_v4();
    let body = rmp_serde::to_vec_named(&test_request(request_id)).expect("request should encode");
    let decoded = decode_remote_scanner_request(&body).expect("request should decode");

    assert_eq!(decoded.0.request_id, request_id);
    assert!(!decoded.0.server_epoch.is_nil());
    assert!(!decoded.0.session_id.is_nil());
    assert_eq!(decoded.0.session_sequence, 0);
    assert_eq!(decoded.0.bucket, "bucket");
    assert_eq!(decoded.0.next_cycle, 7);
    assert_eq!(decoded.0.leader_epoch, 9);
    assert_eq!(decoded.0.scan_plan_digest, TEST_PLAN_DIGEST);
    assert!(decoded.0.skip_healing);
    assert_eq!(decoded.0.scan_mode, HealScanMode::Deep);
    assert_eq!(decoded.0.budget.max_objects, Some(10));
    assert_eq!(decoded.0.budget.max_directories, Some(20));
    assert!(body.len() < 512);
}

#[test]
fn request_envelope_must_match_every_pre_body_replay_field() {
    let request = RemoteScannerRequest(test_request(Uuid::new_v4()));
    assert!(remote_scanner_request_matches_envelope(
        &request,
        request.0.request_id,
        request.0.server_epoch,
        request.0.session_id,
        request.0.session_sequence,
        request.0.next_cycle,
        request.0.leader_epoch,
    ));
    assert!(!remote_scanner_request_matches_envelope(
        &request,
        request.0.request_id,
        Uuid::new_v4(),
        request.0.session_id,
        request.0.session_sequence,
        request.0.next_cycle,
        request.0.leader_epoch,
    ));
    assert!(!remote_scanner_request_matches_envelope(
        &request,
        request.0.request_id,
        request.0.server_epoch,
        request.0.session_id,
        request.0.session_sequence + 1,
        request.0.next_cycle,
        request.0.leader_epoch,
    ));
    assert!(!remote_scanner_request_matches_envelope(
        &request,
        request.0.request_id,
        request.0.server_epoch,
        request.0.session_id,
        request.0.session_sequence,
        request.0.next_cycle,
        request.0.leader_epoch + 1,
    ));
}

#[tokio::test]
async fn bounded_frame_write_releases_a_stalled_response() {
    let request_id = Uuid::new_v4();
    let authenticator = FrameAuthenticator::for_test(request_id);
    let (_reader, mut writer) = tokio::io::duplex(1);
    let disconnect = CancellationToken::new();
    let mut sequence = 0;

    let error = write_frame_bounded(
        &mut writer,
        &authenticator,
        &mut sequence,
        &RemoteScannerFrame::progress(RemoteScannerProgress::default()),
        &disconnect,
        Duration::from_millis(10),
    )
    .await
    .expect_err("a response with no reader must time out");

    assert!(error.to_string().contains("timed out"));
    assert_eq!(sequence, 0);
}

#[tokio::test]
async fn external_cancellation_interrupts_a_stalled_frame_read() {
    let request_id = Uuid::new_v4();
    let authenticator = FrameAuthenticator::for_test(request_id);
    let (_writer, reader) = tokio::io::duplex(1);
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let cancel = parent.clone();
    let task = tokio::spawn(async move {
        consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, authenticator).await
    });

    tokio::task::yield_now().await;
    cancel.cancel();
    let error = tokio::time::timeout(Duration::from_millis(100), task)
        .await
        .expect("cancelled frame read should finish promptly")
        .expect("cancelled frame read task should not panic")
        .expect_err("cancelled frame read must fail");

    assert!(error.to_string().contains("cancelled"));
}

#[test]
fn request_decode_rejects_unknown_fields_without_a_protocol_bump() {
    #[derive(Serialize)]
    struct AdditiveRequestWire {
        version: u16,
        request_id: Uuid,
        server_epoch: Uuid,
        session_id: Uuid,
        session_sequence: u64,
        bucket: String,
        next_cycle: u64,
        leader_epoch: u64,
        scan_plan_digest: DataUsageScanPlanDigest,
        skip_healing: bool,
        scan_mode: HealScanMode,
        budget: RemoteScannerBudget,
        future_optional_hint: bool,
    }

    let request_id = Uuid::new_v4();
    let body = rmp_serde::to_vec_named(&AdditiveRequestWire {
        version: NS_SCANNER_PROTOCOL_VERSION,
        request_id,
        server_epoch: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
        session_sequence: 0,
        bucket: "bucket".to_string(),
        next_cycle: 7,
        leader_epoch: 9,
        scan_plan_digest: TEST_PLAN_DIGEST,
        skip_healing: true,
        scan_mode: HealScanMode::Deep,
        budget: RemoteScannerBudget::default(),
        future_optional_hint: true,
    })
    .expect("additive request should encode");
    assert!(decode_remote_scanner_request(&body).is_err());
}

#[test]
fn request_rejects_invalid_bucket_and_nil_ids() {
    let mut invalid_bucket = test_request(Uuid::new_v4());
    invalid_bucket.bucket = "../bucket".to_string();
    let body = rmp_serde::to_vec_named(&invalid_bucket).expect("request should encode");
    assert!(decode_remote_scanner_request(&body).is_err());

    let body = rmp_serde::to_vec_named(&test_request(Uuid::nil())).expect("request should encode");
    assert!(decode_remote_scanner_request(&body).is_err());

    let mut nil_epoch = test_request(Uuid::new_v4());
    nil_epoch.server_epoch = Uuid::nil();
    let body = rmp_serde::to_vec_named(&nil_epoch).expect("request should encode");
    assert!(decode_remote_scanner_request(&body).is_err());

    let mut nil_session = test_request(Uuid::new_v4());
    nil_session.session_id = Uuid::nil();
    let body = rmp_serde::to_vec_named(&nil_session).expect("request should encode");
    assert!(decode_remote_scanner_request(&body).is_err());

    let mut zero_leader_epoch = test_request(Uuid::new_v4());
    zero_leader_epoch.leader_epoch = 0;
    let body = rmp_serde::to_vec_named(&zero_leader_epoch).expect("request should encode");
    assert!(decode_remote_scanner_request(&body).is_err());
}

#[test]
fn request_rejects_empty_truncated_oversized_and_wrong_version_payloads() {
    assert_eq!(NS_SCANNER_PROTOCOL_VERSION, 3);
    assert!(decode_remote_scanner_request(&[]).is_err());

    let mut body = rmp_serde::to_vec_named(&test_request(Uuid::new_v4())).expect("request should encode");
    body.truncate(body.len() / 2);
    assert!(decode_remote_scanner_request(&body).is_err());

    let oversized = vec![0_u8; NS_SCANNER_MAX_REQUEST_BODY_SIZE + 1];
    assert!(decode_remote_scanner_request(&oversized).is_err());

    for version in [2, 4] {
        let mut wrong_version = test_request(Uuid::new_v4());
        wrong_version.version = version;
        let body = rmp_serde::to_vec_named(&wrong_version).expect("request should encode");
        assert!(decode_remote_scanner_request(&body).is_err());
    }
}

#[test]
fn disk_scan_error_scope_distinguishes_bucket_failures_from_offline_workers() {
    let bucket_error = RemoteScannerServerError::disk_scan(ScannerError::Other("metadata corrupt".to_string()), true);
    let worker_error = RemoteScannerServerError::disk_scan(ScannerError::Other("disk offline".to_string()), false);

    assert_eq!(bucket_error.scope, RemoteScannerErrorScope::Bucket);
    assert_eq!(worker_error.scope, RemoteScannerErrorScope::Worker);
}

#[test]
fn persisted_cycle_decoder_requires_a_valid_leader_fence() {
    let extended = crate::scanner::encode_scanner_cycle_fence_for_test(42, 9);

    assert_eq!(
        crate::scanner::decode_persisted_scanner_cycle_fence(&extended).expect("fenced cycle"),
        (42, 9)
    );
    assert_eq!(
        crate::scanner::decode_persisted_scanner_cycle_fence(&42_u64.to_le_bytes()).expect("legacy cycle"),
        (42, 0)
    );
    assert!(crate::scanner::decode_persisted_scanner_cycle_fence(&[0_u8; 7]).is_err());
}

#[tokio::test]
async fn request_cycle_validation_reads_persisted_store_state() {
    let request = RemoteScannerRequest(test_request(Uuid::new_v4()));
    let store = Arc::new(CycleStateStore {
        state: Some(crate::scanner::encode_scanner_cycle_fence_for_test(
            request.0.next_cycle,
            request.0.leader_epoch,
        )),
    });
    assert_eq!(
        validate_remote_scanner_request_fence_with_store(request.0.next_cycle, request.0.leader_epoch, store)
            .await
            .expect("matching persisted fence should validate"),
        (7, 9)
    );

    let stale_store = Arc::new(CycleStateStore {
        state: Some(crate::scanner::encode_scanner_cycle_fence_for_test(8, request.0.leader_epoch)),
    });
    let err = validate_remote_scanner_request_fence_with_store(request.0.next_cycle, request.0.leader_epoch, stale_store)
        .await
        .expect_err("mismatched persisted cycle must be rejected");
    assert!(err.to_string().contains("requested_cycle=7"));

    let mut initial_request = test_request(Uuid::new_v4());
    initial_request.next_cycle = 0;
    initial_request.leader_epoch = 0;
    assert_eq!(
        validate_remote_scanner_request_fence_with_store(
            initial_request.next_cycle,
            initial_request.leader_epoch,
            Arc::new(CycleStateStore { state: None }),
        )
        .await
        .expect("missing state should validate only the initial fence"),
        (0, 0)
    );
}

#[tokio::test]
async fn fence_watcher_stops_work_after_persisted_leader_changes() {
    let store = Arc::new(MutableCycleStateStore {
        state: RwLock::new(crate::scanner::encode_scanner_cycle_fence_for_test(7, 9)),
    });
    let cache = Arc::new(Mutex::new(None));
    let refresh = Arc::new(AsyncMutex::new(()));
    let watcher = tokio::spawn({
        let cache = cache.clone();
        let refresh = refresh.clone();
        let store = store.clone();
        async move {
            watch_remote_scanner_request_fence_with_cache(
                7,
                9,
                store,
                Duration::from_millis(5),
                cache.as_ref(),
                refresh.as_ref(),
                Duration::from_millis(5),
            )
            .await
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    *store.state.write().expect("cycle state test lock should remain available") =
        crate::scanner::encode_scanner_cycle_fence_for_test(7, 10);

    let err = tokio::time::timeout(Duration::from_millis(100), watcher)
        .await
        .expect("fence watcher should observe the leader change")
        .expect("fence watcher task should not panic")
        .expect_err("a replacement leader must cancel old scanner work");
    assert!(err.to_string().contains("requested_epoch=9"));
}

#[tokio::test]
async fn concurrent_cycle_cache_misses_share_one_backend_read() {
    let request = RemoteScannerRequest(test_request(Uuid::new_v4()));
    let store = Arc::new(CountingCycleStateStore {
        state: crate::scanner::encode_scanner_cycle_fence_for_test(request.0.next_cycle, request.0.leader_epoch),
        reads: AtomicUsize::new(0),
    });
    let cache = Mutex::new(None);
    let refresh = AsyncMutex::new(());

    let results = futures::future::join_all((0..16).map(|_| {
        validate_remote_scanner_request_fence_cached(
            request.0.next_cycle,
            request.0.leader_epoch,
            store.clone(),
            &cache,
            &refresh,
            NS_SCANNER_VALIDATED_CYCLE_TTL,
        )
    }))
    .await;

    assert!(results.into_iter().all(|result| result.is_ok()));
    assert_eq!(store.reads.load(Ordering::Relaxed), 1);
}

#[test]
fn remote_scanner_admission_allows_one_request_per_disk() {
    let key = format!("test-disk-{}", Uuid::new_v4());
    let first = try_admit_remote_scanner_key(key.clone()).expect("first request should be admitted");
    assert!(try_admit_remote_scanner_key(key.clone()).is_err());
    drop(first);
    assert!(try_admit_remote_scanner_key(key).is_ok());
}

#[test]
fn admission_rejects_busy_disk_before_replay_state_changes() {
    let key = format!("test-disk-{}", Uuid::new_v4());
    let active = try_admit_remote_scanner_key(key.clone()).expect("first request should be admitted");

    assert!(matches!(try_admit_remote_scanner_key(key.clone()), Err(ScannerError::RemoteDiskBusy)));
    drop(active);

    assert!(try_admit_remote_scanner_key(key).is_ok());
}

#[test]
fn replay_cache_requires_contiguous_sequence_per_disk_session() {
    let session_id = Uuid::new_v4();
    let mut cache = RemoteScannerReplayCache::default();

    cache
        .claim("disk-a".to_string(), session_id, 7, 9, 0)
        .expect("first session request should be accepted");
    assert!(matches!(
        cache.claim("disk-a".to_string(), session_id, 7, 9, 0),
        Err(ScannerError::RemoteRequestReplay)
    ));
    assert!(matches!(
        cache.claim("disk-a".to_string(), session_id, 7, 9, 2),
        Err(ScannerError::RemoteRequestReplay)
    ));
    cache
        .claim("disk-a".to_string(), session_id, 7, 9, 1)
        .expect("next contiguous session request should be accepted");
}

#[test]
fn replay_preflight_rejects_a_claimed_request_without_mutating_state() {
    let session_id = Uuid::new_v4();
    let mut cache = RemoteScannerReplayCache::default();
    cache
        .claim("disk-a".to_string(), session_id, 7, 9, 0)
        .expect("first request should be accepted");
    let state_before = (cache.cycle, cache.leader_epoch, cache.sessions.clone());

    assert!(matches!(
        cache.preflight("disk-a", session_id, 7, 9, 0),
        Err(ScannerError::RemoteRequestReplay)
    ));
    assert_eq!((cache.cycle, cache.leader_epoch, cache.sessions), state_before);
}

#[test]
fn replay_cache_discards_sessions_from_prior_cycle() {
    let session_id = Uuid::new_v4();
    let mut cache = RemoteScannerReplayCache::default();

    cache
        .claim("disk-a".to_string(), session_id, 7, 9, 0)
        .expect("first cycle session should be accepted");
    cache
        .claim("disk-a".to_string(), session_id, 8, 9, 0)
        .expect("new cycle should start a fresh session sequence");
    assert_eq!(cache.sessions.len(), 1);
    assert!(matches!(
        cache.claim("disk-a".to_string(), Uuid::new_v4(), 7, 9, 0),
        Err(ScannerError::RemoteRequestReplay)
    ));
    assert_eq!(cache.cycle, Some(8));
    assert_eq!(cache.sessions.len(), 1);
}

#[test]
fn replay_cache_fences_sessions_by_leader_epoch() {
    let mut cache = RemoteScannerReplayCache::default();
    cache
        .claim("disk-a".to_string(), Uuid::new_v4(), 7, 9, 0)
        .expect("first leader session should be accepted");
    cache
        .claim("disk-a".to_string(), Uuid::new_v4(), 7, 10, 0)
        .expect("replacement leader should reset replay state");

    assert_eq!(cache.cycle, Some(7));
    assert_eq!(cache.leader_epoch, Some(10));
    assert_eq!(cache.sessions.len(), 1);
    assert!(matches!(
        cache.claim("disk-a".to_string(), Uuid::new_v4(), 8, 9, 0),
        Err(ScannerError::RemoteRequestReplay)
    ));
}

#[test]
fn replay_cache_scales_with_sessions_instead_of_bucket_requests() {
    let session_id = Uuid::new_v4();
    let mut cache = RemoteScannerReplayCache::default();
    let request_count = u64::try_from(NS_SCANNER_MAX_REPLAY_SESSIONS)
        .expect("session limit should fit in u64")
        .saturating_add(1024);

    for sequence in 0..request_count {
        cache
            .claim("disk-a".to_string(), session_id, 7, 9, sequence)
            .expect("contiguous requests in one worker session should not consume session capacity");
    }

    assert_eq!(cache.sessions.len(), 1);
    assert_eq!(cache.sessions.get(&("disk-a".to_string(), session_id)), Some(&(request_count - 1)));
}

#[test]
fn validated_cycle_cache_expires_and_rejects_other_cycles() {
    let now = Instant::now();
    let cached = RemoteScannerValidatedCycle {
        cycle: 7,
        leader_epoch: 9,
        valid_until: now + NS_SCANNER_VALIDATED_CYCLE_TTL,
    };

    assert!(cached.matches(7, 9, now));
    assert!(!cached.matches(8, 9, now));
    assert!(!cached.matches(7, 10, now));
    assert!(!cached.matches(7, 9, now + NS_SCANNER_VALIDATED_CYCLE_TTL));
}

#[test]
fn replay_cache_fails_closed_at_capacity() {
    let mut cache = RemoteScannerReplayCache::default();
    for _ in 0..NS_SCANNER_MAX_REPLAY_SESSIONS {
        cache
            .claim("disk-a".to_string(), Uuid::new_v4(), 7, 9, 0)
            .expect("session should fit in replay cache");
    }

    let error = cache
        .claim("disk-a".to_string(), Uuid::new_v4(), 7, 9, 0)
        .expect_err("session beyond replay cache capacity must fail");
    assert!(matches!(error, ScannerError::RemoteReplayCapacity));
}

#[tokio::test]
async fn complete_terminal_frame_reconciles_progress_and_usage() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::progress(RemoteScannerProgress {
                objects_scanned: 2,
                directories_started: 1,
                ..Default::default()
            }),
        )
        .await
        .expect("progress should write");
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress {
                    objects_scanned: 3,
                    directories_started: 2,
                    ..Default::default()
                },
                RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                    source: TEST_SOURCE,
                    scan_plan_digest: TEST_PLAN_DIGEST,
                    usage: test_usage("bucket", 3),
                    pending_maintenance_work: true,
                })),
            ),
        )
        .await
        .expect("terminal frame should write");
        writer.shutdown().await.expect("writer should shut down");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &parent,
        ScannerCycleBudgetConfig {
            max_objects: Some(10),
            max_directories: Some(10),
            ..Default::default()
        },
    );
    let outcome =
        consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect("stream should complete");

    let RemoteScannerOutcome::Complete {
        usage,
        pending_maintenance_work,
    } = outcome
    else {
        panic!("expected complete result");
    };
    assert_eq!(usage.entry.objects, 3);
    assert!(pending_maintenance_work);
    assert_eq!(budget.progress(), (3, 2));
}

#[tokio::test]
async fn complete_terminal_frame_after_budget_expiry_is_partial() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress {
                    objects_scanned: 3,
                    directories_started: 1,
                    ..Default::default()
                },
                RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                    source: TEST_SOURCE,
                    scan_plan_digest: TEST_PLAN_DIGEST,
                    usage: test_usage("bucket", 3),
                    pending_maintenance_work: false,
                })),
            ),
        )
        .await
        .expect("terminal frame should write");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &parent,
        ScannerCycleBudgetConfig {
            max_objects: Some(3),
            ..Default::default()
        },
    );
    let outcome =
        consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect("stream should finish as partial");

    assert!(matches!(outcome, RemoteScannerOutcome::Partial));
    assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Objects));
}

#[tokio::test]
async fn partial_terminal_frame_is_reported_without_usage() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress {
                    objects_scanned: 4,
                    directories_started: 2,
                    ..Default::default()
                },
                RemoteScannerFrameResult::Partial,
            ),
        )
        .await
        .expect("partial terminal frame should write");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, ScannerCycleBudgetConfig::default());
    let outcome =
        consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect("partial terminal frame should be accepted");

    assert!(matches!(outcome, RemoteScannerOutcome::Partial));
    assert_eq!(budget.progress(), (4, 2));
}

#[tokio::test]
async fn namespace_not_found_terminal_frame_is_reported_without_usage() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress {
                    objects_scanned: 4,
                    directories_started: 2,
                    ..Default::default()
                },
                RemoteScannerFrameResult::NamespaceNotFound,
            ),
        )
        .await
        .expect("namespace-not-found terminal frame should write");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, ScannerCycleBudgetConfig::default());
    let outcome =
        consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect("namespace-not-found terminal frame should be accepted");

    assert!(matches!(outcome, RemoteScannerOutcome::NamespaceNotFound));
    assert_eq!(budget.progress(), (4, 2));
}

#[tokio::test]
async fn error_terminal_frame_is_reported_as_failure() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress::default(),
                RemoteScannerFrameResult::Error(RemoteScannerErrorFrame {
                    scope: RemoteScannerErrorScope::Bucket,
                    message: "cache save failed".to_string(),
                }),
            ),
        )
        .await
        .expect("error terminal frame should write");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &parent,
        ScannerCycleBudgetConfig {
            max_objects: Some(10),
            ..Default::default()
        },
    );
    let stream_result =
        consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth).await;
    let error = finish_remote_scanner_stream(stream_result, budget.as_ref()).expect_err("error terminal frame must fail");

    assert!(error.to_string().contains("cache save failed"));
    assert!(!error.retire_worker());
    assert!(!budget.budget_elapsed());
}

#[tokio::test]
async fn retry_bucket_error_terminal_frame_requeues_bucket_work() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress::default(),
                RemoteScannerFrameResult::Error(RemoteScannerErrorFrame {
                    scope: RemoteScannerErrorScope::Bucket,
                    message: format!("{NS_SCANNER_RETRY_BUCKET_ERROR_PREFIX} cache lock contention"),
                }),
            ),
        )
        .await
        .expect("retry-bucket error terminal frame should write");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let stream_result =
        consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth).await;
    let error = finish_remote_scanner_stream(stream_result, budget.as_ref()).expect_err("retry-bucket frame must fail");

    assert!(error.retry_bucket_work());
    assert!(!error.retire_worker());
    assert!(error.to_string().contains("cache lock contention"));
}

#[tokio::test]
async fn worker_error_terminal_frame_retires_worker_without_cancelling_reported_budget() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress::default(),
                RemoteScannerFrameResult::Error(RemoteScannerErrorFrame {
                    scope: RemoteScannerErrorScope::Worker,
                    message: "object layer unavailable".to_string(),
                }),
            ),
        )
        .await
        .expect("error terminal frame should write");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &parent,
        ScannerCycleBudgetConfig {
            max_objects: Some(10),
            ..Default::default()
        },
    );
    let stream_result =
        consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth).await;
    let error = finish_remote_scanner_stream(stream_result, budget.as_ref()).expect_err("worker error must fail");

    assert!(error.to_string().contains("object layer unavailable"));
    assert!(error.retire_worker());
    assert!(!error.retry_bucket_work());
    assert!(!budget.budget_elapsed());
}

#[test]
fn uncertain_stream_failure_cancels_count_budget() {
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &parent,
        ScannerCycleBudgetConfig {
            max_objects: Some(10),
            ..Default::default()
        },
    );
    let result = Err(RemoteScannerStreamError::uncertain(StorageError::other("connection lost")));

    let error = finish_remote_scanner_stream(result, budget.as_ref()).expect_err("transport failure must fail");

    assert!(error.to_string().contains("connection lost"));
    assert!(error.retire_worker());
    assert_eq!(budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Objects));
}

#[test]
fn directory_entry_activity_counts_as_semantic_progress() {
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let mut last = RemoteScannerProgress::default();

    let advanced = apply_remote_progress(
        budget.as_ref(),
        &mut last,
        RemoteScannerProgress {
            entries_visited: 32,
            ..Default::default()
        },
    )
    .expect("entry activity should be valid progress");

    assert!(advanced);
    assert_eq!(last.entries_visited, 32);
    assert_eq!(budget.progress(), (0, 0));
}

#[tokio::test]
async fn disconnect_after_persisting_keeps_reported_count_budget() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::with_phase(
                RemoteScannerProgress {
                    objects_scanned: 3,
                    directories_started: 2,
                    ..Default::default()
                },
                RemoteScannerPhase::Persisting,
                RemoteScannerFrameResult::Progress,
            ),
        )
        .await
        .expect("persisting progress should write");
        writer.shutdown().await.expect("writer should shut down");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &parent,
        ScannerCycleBudgetConfig {
            max_objects: Some(10),
            max_directories: Some(10),
            ..Default::default()
        },
    );
    let stream_result =
        consume_remote_scanner_stream(reader, parent, budget.clone(), "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth).await;

    finish_remote_scanner_stream(stream_result, budget.as_ref())
        .expect_err("disconnect before terminal persistence result must fail");
    assert_eq!(budget.progress(), (3, 2));
    assert!(!budget.budget_elapsed());
}

#[tokio::test]
async fn scanner_phase_cannot_move_back_to_scanning() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        for phase in [RemoteScannerPhase::Persisting, RemoteScannerPhase::Scanning] {
            write_frame(
                &mut writer,
                &writer_auth,
                &mut sequence,
                &RemoteScannerFrame::with_phase(RemoteScannerProgress::default(), phase, RemoteScannerFrameResult::Progress),
            )
            .await
            .expect("phase frame should write");
        }
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
        .await
        .expect_err("backwards phase must fail");

    assert!(error.to_string().contains("phase moved backwards"));
}

#[tokio::test]
async fn terminal_usage_for_another_bucket_is_rejected() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress::default(),
                RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                    source: TEST_SOURCE,
                    scan_plan_digest: TEST_PLAN_DIGEST,
                    usage: test_usage("other-bucket", 1),
                    pending_maintenance_work: false,
                })),
            ),
        )
        .await
        .expect("terminal frame should write");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
        .await
        .expect_err("wrong bucket must fail");
    assert!(error.to_string().contains("wrong bucket"));
}

#[tokio::test]
async fn terminal_usage_for_another_source_is_rejected() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress::default(),
                RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                    source: DataUsageCacheSource::new(TEST_SOURCE.pool_index + 1, TEST_SOURCE.set_index),
                    scan_plan_digest: TEST_PLAN_DIGEST,
                    usage: test_usage("bucket", 1),
                    pending_maintenance_work: false,
                })),
            ),
        )
        .await
        .expect("terminal frame should write");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
        .await
        .expect_err("wrong source must fail");
    assert!(error.to_string().contains("wrong pool or set"));
}

#[tokio::test]
async fn terminal_usage_must_be_flattened() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut usage = test_usage("bucket", 1);
        usage.entry.children.insert("child-hash".to_string());
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress::default(),
                RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                    source: TEST_SOURCE,
                    scan_plan_digest: TEST_PLAN_DIGEST,
                    usage,
                    pending_maintenance_work: false,
                })),
            ),
        )
        .await
        .expect("terminal frame should write");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
        .await
        .expect_err("non-flattened usage must fail");
    assert!(error.to_string().contains("non-flattened"));
}

#[tokio::test]
async fn terminal_cycle_ahead_reports_required_cycle() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress::default(),
                RemoteScannerFrameResult::CycleAhead {
                    required_cycle: TEST_NEXT_CYCLE + 1,
                },
            ),
        )
        .await
        .expect("terminal frame should write");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let outcome = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
        .await
        .expect("newer remote cache cycle should be reported");
    assert!(matches!(
        outcome,
        RemoteScannerOutcome::CycleAhead(required_cycle) if required_cycle == TEST_NEXT_CYCLE + 1
    ));
}

#[tokio::test]
async fn terminal_cycle_ahead_rejects_nonincreasing_cycle() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress::default(),
                RemoteScannerFrameResult::CycleAhead {
                    required_cycle: TEST_NEXT_CYCLE,
                },
            ),
        )
        .await
        .expect("terminal frame should write");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
        .await
        .expect_err("nonincreasing required cycle must fail");
    assert!(error.to_string().contains("invalid required cycle"));
}

#[tokio::test]
async fn terminal_usage_accepts_large_replication_target_payload_within_frame_limit() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(NS_SCANNER_MAX_FRAME_SIZE);
    tokio::spawn(async move {
        let mut usage = test_usage("bucket", 1);
        let stats = usage.entry.replication_stats.get_or_insert_default();
        for index in 0..=1024 {
            stats.targets.insert(format!("target-{index}"), Default::default());
        }
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress::default(),
                RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                    source: TEST_SOURCE,
                    scan_plan_digest: TEST_PLAN_DIGEST,
                    usage,
                    pending_maintenance_work: false,
                })),
            ),
        )
        .await
        .expect("terminal frame should write");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let outcome = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
        .await
        .expect("large historical replication target payload should remain readable");
    assert!(matches!(outcome, RemoteScannerOutcome::Complete { .. }));
}

#[tokio::test]
async fn tampered_frame_authentication_is_rejected() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator {
        request_id,
        secret: "different-secret".to_string(),
    };
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(RemoteScannerProgress::default(), RemoteScannerFrameResult::Partial),
        )
        .await
        .expect("terminal frame should write");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
        .await
        .expect_err("tampered authentication must fail");
    assert!(error.to_string().contains("authentication failed"));
}

#[tokio::test]
async fn wrong_frame_version_and_sequence_are_rejected() {
    assert_eq!(NS_SCANNER_PROTOCOL_VERSION, 3);
    let request_id = Uuid::new_v4();
    let auth = FrameAuthenticator::for_test(request_id);
    let frame = RemoteScannerFrame::progress(RemoteScannerProgress::default());
    let payload = rmp_serde::to_vec_named(&frame).expect("frame should encode");

    for (version, sequence, expected_error) in [
        (2, 0, "unsupported remote namespace scanner frame version"),
        (4, 0, "unsupported remote namespace scanner frame version"),
        (NS_SCANNER_PROTOCOL_VERSION, 1, "frame sequence is invalid"),
    ] {
        let envelope = RemoteScannerFrameEnvelope {
            version,
            sequence,
            mac: auth.sign(sequence, &payload).expect("frame should sign"),
            payload: payload.clone(),
        };
        let encoded = rmp_serde::to_vec_named(&envelope).expect("envelope should encode");
        let mut input = std::io::Cursor::new(Vec::with_capacity(encoded.len() + 4));
        input.get_mut().extend_from_slice(
            &u32::try_from(encoded.len())
                .expect("encoded frame length should fit u32")
                .to_be_bytes(),
        );
        input.get_mut().extend_from_slice(&encoded);
        input.set_position(0);

        let mut expected_sequence = 0;
        let error = read_frame(&mut input, &auth, &mut expected_sequence)
            .await
            .expect_err("invalid frame must fail");
        assert!(error.to_string().contains(expected_error));
    }
}

#[tokio::test]
async fn backwards_progress_is_rejected() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::progress(RemoteScannerProgress {
                objects_scanned: 2,
                directories_started: 1,
                ..Default::default()
            }),
        )
        .await
        .expect("progress should write");
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress {
                    objects_scanned: 1,
                    directories_started: 1,
                    ..Default::default()
                },
                RemoteScannerFrameResult::Partial,
            ),
        )
        .await
        .expect("terminal frame should write");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
        .await
        .expect_err("backwards progress must fail");
    assert!(error.to_string().contains("moved backwards"));
}

#[tokio::test]
async fn eof_without_terminal_frame_is_rejected() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut sequence = 0;
        write_frame(
            &mut writer,
            &writer_auth,
            &mut sequence,
            &RemoteScannerFrame::progress(RemoteScannerProgress::default()),
        )
        .await
        .expect("progress should write");
        writer.shutdown().await.expect("writer should shut down");
    });

    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
        .await
        .expect_err("missing terminal frame must fail");
    assert!(error.to_string().contains("before a terminal frame"));
}

#[test]
fn oversized_frame_is_rejected_before_allocation() {
    let request_id = Uuid::new_v4();
    let auth = FrameAuthenticator::for_test(request_id);
    let mut input = std::io::Cursor::new(
        u32::try_from(NS_SCANNER_MAX_FRAME_SIZE + 1)
            .expect("test frame size should fit u32")
            .to_be_bytes()
            .to_vec(),
    );
    let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
    let mut sequence = 0;
    let error = runtime
        .block_on(read_frame(&mut input, &auth, &mut sequence))
        .expect_err("oversized frame must fail");
    assert_eq!(error.kind(), ErrorKind::InvalidData);
}
