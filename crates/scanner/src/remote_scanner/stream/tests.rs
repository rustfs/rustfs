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
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::task::Poll;
use tokio::io::ReadBuf;

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
        bucket_incarnation: Some(Uuid::from_u128(7)),
        tier_registry_generation: Some(0),
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
        checkpoint_plan_digest: None,
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
fn request_accepts_additive_checkpoint_coverage_identity() {
    #[derive(Serialize)]
    struct ExtendedRequest<'a, T> {
        #[serde(flatten)]
        legacy: &'a RemoteScannerRequestWire,
        checkpoint_plan_digest: T,
    }
    let legacy = test_request(Uuid::new_v4());
    let request = ExtendedRequest {
        legacy: &legacy,
        checkpoint_plan_digest: DataUsageScanPlanDigest([23; 32]),
    };
    let body = rmp_serde::to_vec_named(&request).expect("extended request body");
    let decoded =
        decode_remote_scanner_request(&body).expect("an authenticated extended request must preserve its coverage identity");
    assert_eq!(decoded.0.checkpoint_plan_digest, Some(DataUsageScanPlanDigest([23; 32])));
    let malformed = ExtendedRequest {
        legacy: &legacy,
        checkpoint_plan_digest: vec![23_u8],
    };
    let malformed = rmp_serde::to_vec_named(&malformed).expect("malformed coverage identity body");
    assert!(
        decode_remote_scanner_request(&malformed).is_err(),
        "invalid coverage identities must not default to legacy semantics"
    );
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
    assert_eq!(
        decoded.0.checkpoint_plan_digest, None,
        "old coordinators retain reuse-plan checkpoint semantics"
    );
    assert!(
        !serde_json::to_value(&decoded.0)
            .expect("legacy wire shape")
            .as_object()
            .expect("request map")
            .contains_key("checkpoint_plan_digest")
    );
    assert!(decoded.0.skip_healing);
    assert_eq!(decoded.0.scan_mode, HealScanMode::Deep);
    assert_eq!(decoded.0.budget.max_objects, Some(10));
    assert_eq!(decoded.0.budget.max_directories, Some(20));
    assert!(body.len() < 512);
}

#[tokio::test]
async fn remote_sender_preserves_independent_bucket_plan_identities_in_signed_body() {
    use crate::storage_api::owner::{EcstoreDiskOption, EcstoreEndpoint, ecstore_new_disk};
    use sha2::Digest as _;

    let _ = rustfs_credentials::set_global_rpc_secret("remote-scanner-sender-test-secret".to_string());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("remote worker listener");
    let address = listener.local_addr().expect("remote worker listener address");
    let worker = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("actual sender connection");
        let mut bytes = Vec::new();
        let header_end = loop {
            if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                break end + 4;
            }
            let mut chunk = [0; 1024];
            let read = socket.read(&mut chunk).await.expect("sender request headers");
            assert!(read > 0 && bytes.len() < 16 * 1024, "bounded complete request headers");
            bytes.extend_from_slice(&chunk[..read]);
        };
        let headers = std::str::from_utf8(&bytes[..header_end]).expect("HTTP request headers");
        let target = headers.split_whitespace().nth(1).expect("request target").to_string();
        let length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().expect("body length"))
            })
            .expect("sender must frame the request body");
        assert!(length <= NS_SCANNER_MAX_REQUEST_BODY_SIZE);
        while bytes.len() < header_end + length {
            let mut chunk = [0; 1024];
            let read = socket.read(&mut chunk).await.expect("sender request body");
            assert!(read > 0, "sender must deliver the complete body");
            bytes.extend_from_slice(&chunk[..read]);
        }
        let body = &bytes[header_end..header_end + length];
        let digest = hex_simd::encode_to_string(Sha256::digest(body), hex_simd::AsciiCase::Lower);
        assert!(
            target
                .split('&')
                .any(|part| part == format!("ns_scanner_body_sha256={digest}")),
            "the signed request URL must bind the actual body"
        );
        let request = decode_remote_scanner_request(body).expect("actual sender wire request").0;
        let auth = FrameAuthenticator::from_rpc_secret(request.request_id).expect("authenticated worker response");
        let mut response = Vec::new();
        write_frame(
            &mut response,
            &auth,
            &mut 0,
            &RemoteScannerFrame::terminal(
                RemoteScannerProgress::default(),
                RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
                    source: TEST_SOURCE,
                    scan_plan_digest: request.scan_plan_digest,
                    usage: test_usage("bucket", 3),
                    pending_maintenance_work: false,
                })),
            ),
        )
        .await
        .expect("authenticated terminal response");
        socket
            .write_all(format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", response.len()).as_bytes())
            .await
            .expect("worker response headers");
        socket.write_all(&response).await.expect("worker response body");
        request
    });
    let mut endpoint = EcstoreEndpoint::try_from(format!("http://{address}/data/disk1").as_str()).expect("remote endpoint");
    endpoint.is_local = false;
    endpoint.set_pool_index(TEST_SOURCE.pool_index);
    endpoint.set_set_index(TEST_SOURCE.set_index);
    let disk = ecstore_new_disk(
        &endpoint,
        &EcstoreDiskOption {
            cleanup: false,
            health_check: false,
        },
    )
    .await
    .expect("remote disk");
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(
        &parent,
        ScannerCycleBudgetConfig {
            max_objects: Some(7),
            ..Default::default()
        },
    );
    let reuse = DataUsageScanPlanDigest([11; 32]);
    let coverage = DataUsageScanPlanDigest([23; 32]);
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        scan_remote_bucket(
            &disk,
            parent,
            budget,
            RemoteScannerScanSpec {
                bucket: "bucket",
                next_cycle: 7,
                leader_epoch: 9,
                server_epoch: Uuid::new_v4(),
                session_id: Uuid::new_v4(),
                session_sequence: 0,
                plans: ScannerBucketPlanDigests::from_work_plans(reuse, coverage, None),
                tier_registry_generation: 0,
                skip_healing: true,
                scan_mode: HealScanMode::Deep,
            },
        ),
    )
    .await
    .expect("bounded actual sender")
    .expect("authenticated remote scan");
    assert!(matches!(outcome, RemoteScannerOutcome::Complete { .. }));
    let captured = worker.await.expect("capture actual sender request");
    assert_eq!(captured.scan_plan_digest, reuse);
    assert_eq!(captured.checkpoint_plan_digest, Some(coverage));
    assert_eq!(captured.scan_mode, HealScanMode::Deep);
    assert_eq!(captured.budget.max_objects, Some(7));
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
async fn background_frame_matches_foreground_wire_mac_and_sequence() {
    let authenticator = FrameAuthenticator::for_test(Uuid::from_u128(7));
    let frames = [
        RemoteScannerFrame::progress(RemoteScannerProgress::default()),
        RemoteScannerFrame::terminal(RemoteScannerProgress::default(), eof_test_terminal(0)),
        RemoteScannerFrame::terminal(
            RemoteScannerProgress::default(),
            RemoteScannerFrameResult::Error(RemoteScannerErrorFrame {
                scope: RemoteScannerErrorScope::Worker,
                message: "目录/🙂/\0".to_string(),
            }),
        ),
    ];

    for frame in frames {
        for initial_sequence in [0, 128] {
            let owner = EcstoreBackgroundMemoryBudget::new();
            owner.set_sampled_limit(64 * 1024, Duration::from_secs(30));
            let (mut foreground, mut default, mut background) = (Vec::new(), Vec::new(), Vec::new());
            let (mut foreground_sequence, mut default_sequence, mut background_sequence) =
                (initial_sequence, initial_sequence, initial_sequence);
            write_frame(&mut foreground, &authenticator, &mut foreground_sequence, &frame)
                .await
                .expect("foreground frame should encode");
            write_frame_bounded(
                &mut default,
                &authenticator,
                &mut default_sequence,
                &frame,
                &CancellationToken::new(),
                Duration::from_secs(1),
            )
            .await
            .expect("default writer should use the foreground encoding");
            write_background_frame(&mut background, &authenticator, &mut background_sequence, &frame, &owner)
                .await
                .expect("background frame should encode");
            assert_eq!(foreground, default);
            assert_eq!(foreground, background);
            assert_eq!(foreground_sequence, initial_sequence + 1);
            assert_eq!(default_sequence, foreground_sequence);
            assert_eq!(background_sequence, foreground_sequence);
            assert_eq!(owner.snapshot().used, 0);

            let mut expected_sequence = initial_sequence;
            let decoded = read_frame(&mut Cursor::new(background), &authenticator, &mut expected_sequence, None)
                .await
                .expect("the original decoder must verify the background MAC and sequence");
            assert_eq!(expected_sequence, foreground_sequence);
            assert_eq!(
                rmp_serde::to_vec_named(&decoded).expect("decoded payload"),
                rmp_serde::to_vec_named(&frame).expect("original payload")
            );
        }
    }
}

#[tokio::test]
async fn background_frame_payload_and_envelope_denial_preserve_io_cause_without_output() {
    let authenticator = FrameAuthenticator::for_test(Uuid::from_u128(7));
    let frame = RemoteScannerFrame::progress(RemoteScannerProgress::default());
    let probe_owner = EcstoreBackgroundMemoryBudget::new();
    probe_owner.set_sampled_limit(64 * 1024, Duration::from_secs(30));
    let payload = encode_background(&frame, &probe_owner).expect("measure retained payload capacity");
    let payload_capacity = probe_owner.snapshot().used;
    assert!(payload_capacity > 0);
    drop(payload);
    assert_eq!(probe_owner.snapshot().used, 0);

    for limit in [0, payload_capacity * 2] {
        let owner = EcstoreBackgroundMemoryBudget::new();
        owner.set_sampled_limit(limit, Duration::from_secs(30));
        if limit != 0 {
            let payload = encode_background(&frame, &owner).expect("this budget admits the entire inner encoding");
            assert_eq!(owner.snapshot().used, payload_capacity);
            drop(payload);
            assert_eq!(owner.snapshot().used, 0);
        }

        let mut output = Vec::new();
        let mut sequence = 17;
        let error = write_background_frame(&mut output, &authenticator, &mut sequence, &frame, &owner)
            .await
            .expect_err("deny the payload at zero or the envelope after an admitted payload");
        let ScannerError::Io(error) = error else {
            panic!("serializer memory denial must remain an I/O error");
        };
        assert_eq!(error.kind(), ErrorKind::WouldBlock);
        assert!(error.get_ref().is_some(), "the serializer must preserve the original memory-denial cause");
        assert!(output.is_empty());
        assert_eq!(sequence, 17);
        assert_eq!(owner.snapshot().used, 0);
    }
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

fn eof_test_terminal(case: usize) -> RemoteScannerFrameResult {
    match case {
        0 => RemoteScannerFrameResult::Complete(Box::new(RemoteScannerComplete {
            source: TEST_SOURCE,
            scan_plan_digest: TEST_PLAN_DIGEST,
            usage: test_usage("bucket", 1),
            pending_maintenance_work: false,
        })),
        1 => RemoteScannerFrameResult::Partial,
        2 => RemoteScannerFrameResult::NamespaceNotFound,
        3 => RemoteScannerFrameResult::CycleAhead {
            required_cycle: TEST_NEXT_CYCLE + 1,
        },
        4 => RemoteScannerFrameResult::Error(RemoteScannerErrorFrame {
            scope: RemoteScannerErrorScope::Bucket,
            message: "expected remote failure".to_string(),
        }),
        _ => panic!("unexpected terminal test case"),
    }
}

struct DropObservedReader<R> {
    inner: R,
    dropped: Arc<AtomicBool>,
}

impl<R: AsyncRead + Unpin> AsyncRead for DropObservedReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<R> Drop for DropObservedReader<R> {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

async fn encoded_eof_test_terminal(case: usize, authenticator: &FrameAuthenticator) -> Vec<u8> {
    let mut bytes = Vec::new();
    write_frame(
        &mut bytes,
        authenticator,
        &mut 0,
        &RemoteScannerFrame::terminal(RemoteScannerProgress::default(), eof_test_terminal(case)),
    )
    .await
    .expect("terminal test frame must encode");
    bytes
}

#[tokio::test]
async fn every_terminal_outcome_waits_for_clean_response_eof() {
    for case in 0..5 {
        let request_id = Uuid::new_v4();
        let writer_auth = FrameAuthenticator::for_test(request_id);
        let reader_auth = FrameAuthenticator::for_test(request_id);
        let (mut writer, reader) = tokio::io::duplex(4096);
        let dropped = Arc::new(AtomicBool::new(false));
        let reader = DropObservedReader {
            inner: reader,
            dropped: dropped.clone(),
        };
        writer
            .write_all(&encoded_eof_test_terminal(case, &writer_auth).await)
            .await
            .expect("terminal frame must reach response body");

        let parent = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
        let mut task = tokio::spawn(async move {
            consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth).await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut task).await.is_err(),
            "terminal case {case} returned before the response body finished"
        );
        assert!(
            !dropped.load(Ordering::SeqCst),
            "terminal case {case} dropped the response body before EOF"
        );
        writer.shutdown().await.expect("response body must finish cleanly");
        let result = task.await.expect("consumer must not panic");
        assert!(
            dropped.load(Ordering::SeqCst),
            "terminal case {case} retained the response body after EOF"
        );
        match (case, result) {
            (0, Ok(RemoteScannerOutcome::Complete { .. }))
            | (1, Ok(RemoteScannerOutcome::Partial))
            | (2, Ok(RemoteScannerOutcome::NamespaceNotFound))
            | (3, Ok(RemoteScannerOutcome::CycleAhead(_))) => {}
            (4, Err(error)) if error.to_string().contains("expected remote failure") => {}
            (_, result) => panic!("unexpected terminal case {case} result: {result:?}"),
        }
    }
}

#[tokio::test]
async fn trailing_bytes_after_terminal_frame_are_rejected() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let mut bytes = encoded_eof_test_terminal(1, &writer_auth).await;
    bytes.push(0);
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let error =
        consume_remote_scanner_stream(Cursor::new(bytes), parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
            .await
            .expect_err("trailing protocol byte must fail");
    assert!(error.to_string().contains("trailing bytes"));
}

#[tokio::test]
async fn terminal_frame_without_eof_still_obeys_rpc_deadline() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    writer
        .write_all(&encoded_eof_test_terminal(1, &writer_auth).await)
        .await
        .expect("terminal frame must reach response body");
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let error = consume_remote_scanner_stream_until(
        reader,
        parent,
        budget,
        RemoteScannerResponseExpectation {
            bucket: "bucket",
            source: TEST_SOURCE,
            next_cycle: TEST_NEXT_CYCLE,
            scan_plan_digest: TEST_PLAN_DIGEST,
            tier_registry_generation: 0,
        },
        reader_auth,
        Instant::now() + Duration::from_millis(20),
    )
    .await
    .expect_err("terminal frame without EOF must not succeed");
    assert!(error.to_string().contains("RPC lifetime exceeded"));
}

#[tokio::test(start_paused = true)]
async fn terminal_frame_without_eof_still_obeys_semantic_stall_timeout() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    writer
        .write_all(&encoded_eof_test_terminal(1, &writer_auth).await)
        .await
        .expect("terminal frame must reach response body");
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
        .await
        .expect_err("terminal frame without EOF must hit semantic stall limit");
    assert!(error.to_string().contains("no semantic progress"));
}

struct ResetAfterFrame {
    bytes: Vec<u8>,
    position: usize,
}

impl AsyncRead for ResetAfterFrame {
    fn poll_read(mut self: Pin<&mut Self>, _cx: &mut std::task::Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if self.position == self.bytes.len() {
            return Poll::Ready(Err(std::io::Error::new(ErrorKind::ConnectionReset, "injected stream reset")));
        }
        let end = (self.position + buf.remaining()).min(self.bytes.len());
        buf.put_slice(&self.bytes[self.position..end]);
        self.position = end;
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn disconnect_after_terminal_frame_is_not_success() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let reader = ResetAfterFrame {
        bytes: encoded_eof_test_terminal(0, &writer_auth).await,
        position: 0,
    };
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let error = consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth)
        .await
        .expect_err("reset after terminal frame must fail");
    assert!(error.to_string().contains("injected stream reset"));
}

#[tokio::test]
async fn cancellation_interrupts_terminal_eof_wait() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    writer
        .write_all(&encoded_eof_test_terminal(1, &writer_auth).await)
        .await
        .expect("terminal frame must reach response body");
    let parent = CancellationToken::new();
    let cancel = parent.clone();
    let budget = ScannerCycleBudget::new(&parent, ScannerCycleBudgetConfig::default());
    let task = tokio::spawn(async move {
        consume_remote_scanner_stream(reader, parent, budget, "bucket", TEST_SOURCE, TEST_PLAN_DIGEST, reader_auth).await
    });
    tokio::task::yield_now().await;
    cancel.cancel();
    let error = tokio::time::timeout(Duration::from_millis(100), task)
        .await
        .expect("cancelled EOF wait must finish promptly")
        .expect("consumer must not panic")
        .expect_err("cancelled EOF wait must fail");
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
async fn node_admission_wait_does_not_hide_replacement_leader() {
    use rustfs_concurrency::adaptive::{FeedbackController, FeedbackObservation, NodeAdmission, ResourcePressure};
    use std::num::NonZeroU16;

    let store = Arc::new(MutableCycleStateStore {
        state: RwLock::new(crate::scanner::encode_scanner_cycle_fence_for_test(7, 9)),
    });
    let cache = Mutex::new(None);
    let refresh = AsyncMutex::new(());
    let watcher = watch_remote_scanner_request_fence_with_cache(
        7,
        9,
        store.clone(),
        Duration::from_millis(5),
        &cache,
        &refresh,
        Duration::from_millis(5),
    );
    tokio::pin!(watcher);
    let gate = NodeAdmission::new(NonZeroU16::new(1).expect("one shared node unit"));
    let storage_polled = AtomicBool::new(false);
    {
        let resume = async {
            let _permit = gate.acquire().await;
            storage_polled.store(true, Ordering::Relaxed);
            Ok(())
        };
        let waiting = wait_for_remote_node_resumption(resume, watcher.as_mut());
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending(), "no resource sample grants a node unit");
        *store.state.write().expect("persist replacement leader") = crate::scanner::encode_scanner_cycle_fence_for_test(7, 10);
        let error = tokio::time::timeout(Duration::from_millis(100), waiting)
            .await
            .expect("resource pause must not hide a changed leader")
            .expect_err("old leader cannot resume storage work");
        assert!(error.to_string().contains("requested_epoch=9"));
    }
    assert!(!storage_polled.load(Ordering::Relaxed));
    assert_eq!(gate.snapshot().active, 0);
    gate.update(
        &mut FeedbackController::new(),
        Some(FeedbackObservation {
            generation: 1,
            max_concurrency: 1,
            pressure: ResourcePressure::Unknown,
            completed_units: None,
        }),
    );
    assert!(gate.try_acquire().is_some(), "failed fencing drops the old node queue ticket");
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
fn local_disk_admission_share_holds_one_permit_until_the_last_walker_leaves() {
    let key = format!("test-disk-{}", Uuid::new_v4());
    let share = LocalDiskAdmissionShare::new();
    let first = share
        .acquire_with(|| try_admit_remote_scanner_key(key.clone()))
        .expect("first walker acquires the disk permit");
    let second = share
        .acquire_with(|| try_admit_remote_scanner_key(key.clone()))
        .expect("second walker shares the held permit");
    assert!(
        matches!(try_admit_remote_scanner_key(key.clone()), Err(ScannerError::RemoteDiskBusy)),
        "a remote request sees one busy disk while any walker is active"
    );
    drop(first);
    assert!(
        matches!(try_admit_remote_scanner_key(key.clone()), Err(ScannerError::RemoteDiskBusy)),
        "the permit is released by the last holder, not the first"
    );
    drop(second);
    assert!(try_admit_remote_scanner_key(key).is_ok(), "no walker left: the disk is free again");
}

#[test]
fn local_disk_admission_share_retries_after_a_rejected_admission() {
    let key = format!("test-disk-{}", Uuid::new_v4());
    let share = LocalDiskAdmissionShare::new();
    let remote = try_admit_remote_scanner_key(key.clone()).expect("remote request occupies the disk");
    assert!(
        matches!(
            share.acquire_with(|| try_admit_remote_scanner_key(key.clone())),
            Err(ScannerError::RemoteDiskBusy)
        ),
        "walkers do not pre-empt an active remote session"
    );
    drop(remote);
    let hold = share
        .acquire_with(|| try_admit_remote_scanner_key(key.clone()))
        .expect("a rejected attempt leaves the share empty for a later retry");
    assert!(matches!(try_admit_remote_scanner_key(key.clone()), Err(ScannerError::RemoteDiskBusy)));
    drop(hold);
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
        for _ in 0..2 {
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
        }
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
    let metrics = Metrics::new();
    let start = metrics
        .start_scan_cycle_work_with_cycle_and_remote_progress(Default::default(), budget.remote_scan_progress())
        .await;
    let outcome = consume_remote_scanner_stream_until(
        reader,
        parent,
        budget.clone(),
        RemoteScannerResponseExpectation {
            bucket: "bucket",
            source: TEST_SOURCE,
            next_cycle: TEST_NEXT_CYCLE,
            scan_plan_digest: TEST_PLAN_DIGEST,
            tier_registry_generation: 0,
        },
        reader_auth,
        Instant::now() + NS_SCANNER_MAX_RPC_LIFETIME,
    )
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
    let active = metrics.report().await;
    assert_eq!(active.current_cycle_objects_scanned, 3);
    assert_eq!(active.current_cycle_directories_scanned, 2);
    assert_eq!(metrics.lifetime(Metric::ScanObject), 0);
    assert_eq!(metrics.lifetime(Metric::ScanFolder), 0);
    metrics.finish_scan_cycle_work(start);
    let finished = metrics.report().await;
    assert_eq!(finished.last_cycle_objects_scanned, 3);
    assert_eq!(finished.last_cycle_directories_scanned, 2);
}

#[tokio::test]
async fn terminal_usage_from_a_different_tier_generation_is_rejected() {
    let request_id = Uuid::new_v4();
    let writer_auth = FrameAuthenticator::for_test(request_id);
    let reader_auth = FrameAuthenticator::for_test(request_id);
    let (mut writer, reader) = tokio::io::duplex(4096);
    tokio::spawn(async move {
        let mut usage = test_usage("bucket", 1);
        usage.tier_registry_generation = Some(1);
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
        .expect_err("generation mismatch must fail closed");

    assert!(error.to_string().contains("tier registry generation"));
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

#[tokio::test]
async fn remote_progress_rejects_regression_and_counts_new_attempts_once() {
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, ScannerCycleBudgetConfig::default());
    let metrics = Metrics::new();
    let _start = metrics
        .start_scan_cycle_work_with_cycle_and_remote_progress(Default::default(), budget.remote_scan_progress())
        .await;
    let mut last = RemoteScannerProgress::default();
    let progress = RemoteScannerProgress {
        objects_scanned: 4,
        directories_started: 2,
        entries_visited: 6,
    };
    assert!(apply_remote_progress(&budget, &mut last, progress).expect("first progress must advance"));
    assert!(!apply_remote_progress(&budget, &mut last, progress).expect("duplicate progress must be accepted"));
    let regression = RemoteScannerProgress {
        objects_scanned: 3,
        ..progress
    };
    assert!(apply_remote_progress(&budget, &mut last, regression).is_err());
    assert_eq!(last, progress);
    assert_eq!(metrics.report().await.current_cycle_objects_scanned, 4);
    assert_eq!(metrics.report().await.current_cycle_directories_scanned, 2);

    let mut retry = RemoteScannerProgress::default();
    apply_remote_progress(&budget, &mut retry, progress).expect("new attempt counts repeated work");
    assert_eq!(budget.progress(), (8, 4));
    let report = metrics.report().await;
    assert_eq!(report.current_cycle_objects_scanned, 8);
    assert_eq!(report.current_cycle_directories_scanned, 4);
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
        let error = read_frame(&mut input, &auth, &mut expected_sequence, None)
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
        .block_on(read_frame(&mut input, &auth, &mut sequence, None))
        .expect_err("oversized frame must fail");
    assert_eq!(error.kind(), ErrorKind::InvalidData);
}

#[tokio::test]
async fn raw_frame_staging_denial_consumes_only_the_length_prefix() {
    let auth = FrameAuthenticator::for_test(Uuid::new_v4());
    let frame = RemoteScannerFrame::progress(RemoteScannerProgress::default());
    let mut wire = Vec::new();
    write_frame(&mut wire, &auth, &mut 0, &frame).await.expect("wire fixture");
    let owner = EcstoreBackgroundMemoryBudget::new();
    let mut input = Cursor::new(wire.clone());
    let mut sequence = 0;
    let error = read_frame(&mut input, &auth, &mut sequence, Some(&owner))
        .await
        .expect_err("closed staging budget");
    assert_eq!(error.kind(), ErrorKind::WouldBlock);
    assert_eq!(input.position(), 4, "do not read a frame body after staging denial");
    assert_eq!(sequence, 0);
    assert_eq!(owner.snapshot().used, 0);

    let length = wire.len() - 4;
    owner.set_sampled_limit(u64::try_from(length).expect("frame length"), Duration::from_secs(30));
    let decoded = read_frame(&mut input, &auth, &mut sequence, Some(&owner)).await;
    assert!(decoded.is_err(), "denial is terminal; do not resume from the consumed prefix");
    let mut input = Cursor::new(wire.clone());
    let error = read_frame(&mut input, &auth, &mut sequence, Some(&owner))
        .await
        .expect_err("raw-only capacity cannot fund an envelope-owned payload");
    assert_eq!(error.kind(), ErrorKind::WouldBlock);
    assert!(error.get_ref().is_some(), "preserve the original memory-denial cause");
    assert_eq!(sequence, 0);
    assert_eq!(owner.snapshot().used, 0);

    // Each allocation is bounded by length: raw, the retained other field,
    // and both old and new field growth allocations can coexist.
    owner.set_sampled_limit(u64::try_from(length * 4).expect("growth peak bound"), Duration::from_secs(30));
    let mut input = Cursor::new(wire);
    read_frame(&mut input, &auth, &mut sequence, Some(&owner))
        .await
        .expect("a fresh reader with all retained storage and growth funded decodes");
    assert_eq!(sequence, 1);
    assert_eq!(owner.snapshot().used, 0);
}

#[tokio::test]
async fn raw_frame_staging_failure_keeps_existing_validation_errors() {
    let auth = FrameAuthenticator::for_test(Uuid::new_v4());
    let mut wire = Vec::new();
    write_frame(&mut wire, &auth, &mut 0, &RemoteScannerFrame::progress(RemoteScannerProgress::default()))
        .await
        .expect("wire fixture");
    let owner = EcstoreBackgroundMemoryBudget::new();
    owner.set_sampled_limit(4 * 1024 * 1024, Duration::from_secs(30));
    let mut truncated = wire.clone();
    truncated.pop();
    for bytes in [
        vec![0, 0, 0],
        vec![0, 0, 0, 0],
        (2_097_153_u32).to_be_bytes().to_vec(),
        truncated,
    ] {
        let original = read_frame(&mut Cursor::new(bytes.clone()), &auth, &mut 0, None)
            .await
            .expect_err("invalid original frame");
        let mut sequence = 0;
        let error = read_frame(&mut Cursor::new(bytes), &auth, &mut sequence, Some(&owner))
            .await
            .expect_err("invalid admitted frame");
        assert_eq!(error.kind(), original.kind());
        assert_eq!(error.to_string(), original.to_string());
        assert_eq!(sequence, 0);
        assert_eq!(owner.snapshot().used, 0);
    }
}

struct ShrinkDecodeBudgetAfterFrame {
    input: Cursor<Vec<u8>>,
    first_frame_end: u64,
    memory: Arc<EcstoreBackgroundMemoryBudget>,
}

impl AsyncRead for ShrinkDecodeBudgetAfterFrame {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.input.position() >= self.first_frame_end {
            self.memory.set_sampled_limit(0, Duration::from_secs(30));
        }
        std::pin::Pin::new(&mut self.input).poll_read(cx, buf)
    }
}

#[tokio::test]
async fn namespace_decode_denial_keeps_previously_reported_progress() {
    const CHILD: &str = "RUSTFS_NAMESPACE_DECODE_PROGRESS_CHILD";
    const TEST: &str = "remote_scanner::stream::tests::namespace_decode_denial_keeps_previously_reported_progress";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
            .env(CHILD, "1")
            .output()
            .expect("isolated installed-policy consumer test");
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("NAMESPACE_DECODE_PROGRESS_OK"));
        return;
    }
    let owner = EcstoreBackgroundMemoryBudget::new();
    assert!(owner.install());
    owner.set_sampled_limit(4 * 1024 * 1024, Duration::from_secs(30));
    let auth = FrameAuthenticator::for_test(Uuid::new_v4());
    let mut wire = Vec::new();
    let mut sequence = 0;
    write_frame(
        &mut wire,
        &auth,
        &mut sequence,
        &RemoteScannerFrame::progress(RemoteScannerProgress {
            objects_scanned: 3,
            directories_started: 1,
            entries_visited: 3,
        }),
    )
    .await
    .expect("reported progress frame");
    let first_frame_end = u64::try_from(wire.len()).expect("frame position");
    write_frame(
        &mut wire,
        &auth,
        &mut sequence,
        &RemoteScannerFrame::terminal(
            RemoteScannerProgress {
                objects_scanned: 3,
                directories_started: 1,
                entries_visited: 3,
            },
            RemoteScannerFrameResult::NamespaceNotFound,
        ),
    )
    .await
    .expect("terminal frame that must not be accepted after denial");
    let mut input = ShrinkDecodeBudgetAfterFrame {
        input: Cursor::new(wire),
        first_frame_end,
        memory: owner.clone(),
    };
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &CancellationToken::new(),
        ScannerCycleBudgetConfig {
            max_objects: Some(100),
            ..ScannerCycleBudgetConfig::default()
        },
    );
    let result = consume_remote_scanner_stream_until(
        &mut input,
        budget.token(),
        budget.clone(),
        RemoteScannerResponseExpectation {
            bucket: "bucket",
            source: TEST_SOURCE,
            next_cycle: 7,
            scan_plan_digest: TEST_PLAN_DIGEST,
            tier_registry_generation: 0,
        },
        auth,
        Instant::now() + Duration::from_secs(30),
    )
    .await;
    let failure = finish_remote_scanner_stream(result, &budget).expect_err("staging denial must not become completion");
    assert!(failure.retire_worker());
    assert_eq!(budget.progress(), (3, 1));
    assert!(budget.token().is_cancelled());
    assert_eq!(input.input.position(), first_frame_end + 4);
    assert_eq!(owner.snapshot().used, 0);
    println!("NAMESPACE_DECODE_PROGRESS_OK");
}

#[test]
fn background_envelope_does_not_reserve_an_unread_sequence_hint() {
    let owner = EcstoreBackgroundMemoryBudget::new();
    // Compact envelope: [version, sequence, payload, mac]. Neither forged
    // byte-array hint contains a byte, so a closed budget must not be queried.
    for encoded in [
        &[0x94, 3, 0, 0xdd, 0xff, 0xff, 0xff, 0xff][..],
        &[0x94, 3, 0, 0x90, 0xdd, 0xff, 0xff, 0xff, 0xff][..],
    ] {
        let error = decode_background_envelope(encoded, &owner)
            .err()
            .expect("truncated byte array");
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(owner.snapshot().used, 0);
    }
    let error = decode_background_envelope(&[0x94, 3, 0, 0x91, 7, 0x90], &owner)
        .err()
        .expect("the first actual payload byte needs admission");
    assert_eq!(error.kind(), ErrorKind::WouldBlock);
    assert!(error.get_ref().is_some(), "preserve the original I/O cause");
    assert_eq!(owner.snapshot().used, 0);
}

#[tokio::test]
async fn background_envelope_mac_denial_follows_same_budget_payload_success() {
    let auth = FrameAuthenticator::for_test(Uuid::from_u128(7));
    let frame = RemoteScannerFrame::progress(RemoteScannerProgress::default());
    let payload = rmp_serde::to_vec(&frame).expect("compact frame");
    assert!(!payload.is_empty() && payload.len() <= 32, "single 32-byte payload allocation");
    let envelope = RemoteScannerFrameEnvelope {
        version: NS_SCANNER_PROTOCOL_VERSION,
        sequence: 0,
        mac: auth.sign(0, &payload).expect("MAC"),
        payload,
    };
    let encoded = rmp_serde::to_vec_named(&envelope).expect("envelope");
    let owner = EcstoreBackgroundMemoryBudget::new();
    let limit = u64::try_from(encoded.len() + 32).expect("raw plus payload capacity");
    owner.set_sampled_limit(limit, Duration::from_secs(30));
    let mut raw = EcstoreBackgroundEncodeBuffer::new(owner.clone(), encoded.len());
    raw.prepare_read(encoded.len())
        .expect("raw admission")
        .copy_from_slice(&encoded);
    let payload_wire = rmp_serde::to_vec(&envelope.payload).expect("payload byte-array wire");
    let mut failure = None;
    let payload_seed = BackgroundEnvelopeBytesSeed {
        buffer: EcstoreBackgroundEncodeBuffer::new(owner.clone(), encoded.len()),
        failure: &mut failure,
    };
    let payload =
        serde::de::DeserializeSeed::deserialize(payload_seed, &mut rmp_serde::Deserializer::from_read_ref(&payload_wire))
            .expect("the identical budget admits raw staging and the complete payload");
    assert_eq!(payload.as_ref(), envelope.payload.as_slice());
    assert_eq!(owner.snapshot().used, limit);
    drop(payload);
    drop(raw);
    assert_eq!(owner.snapshot().used, 0);

    let mut wire = u32::try_from(encoded.len()).expect("frame length").to_be_bytes().to_vec();
    wire.extend_from_slice(&encoded);
    let mut input = Cursor::new(wire);
    let mut sequence = 0;
    let error = read_frame(&mut input, &auth, &mut sequence, Some(&owner))
        .await
        .expect_err("MAC allocation must fail after the admitted payload");
    assert_eq!(error.kind(), ErrorKind::WouldBlock);
    assert!(error.get_ref().is_some(), "preserve the original I/O cause");
    assert_eq!(input.position(), u64::try_from(encoded.len() + 4).expect("consumed frame"));
    assert_eq!(sequence, 0);
    assert_eq!(owner.snapshot().used, 0);
}

#[tokio::test]
async fn background_envelope_accepts_the_full_wire_limit_and_matches_none() {
    let auth = FrameAuthenticator::for_test(Uuid::from_u128(7));
    let encode = |length| {
        let frame = RemoteScannerFrame {
            progress: RemoteScannerProgress {
                objects_scanned: 3,
                directories_started: 2,
                entries_visited: 4,
            },
            phase: RemoteScannerPhase::Persisting,
            result: RemoteScannerFrameResult::Error(RemoteScannerErrorFrame {
                scope: RemoteScannerErrorScope::Bucket,
                message: "x".repeat(length),
            }),
        };
        let payload = rmp_serde::to_vec_named(&frame).expect("frame");
        rmp_serde::to_vec_named(&RemoteScannerFrameEnvelope {
            version: NS_SCANNER_PROTOCOL_VERSION,
            sequence: 0,
            mac: auth.sign(0, &payload).expect("MAC"),
            payload,
        })
        .expect("envelope")
    };
    let (mut low, mut high) = (0, NS_SCANNER_MAX_FRAME_SIZE);
    while low < high {
        let length = low + (high - low).div_ceil(2);
        if encode(length).len() <= NS_SCANNER_MAX_FRAME_SIZE {
            low = length;
        } else {
            high = length - 1;
        }
    }
    // MAC integers have variable MessagePack widths; do not assume monotonicity
    // within this small neighborhood when finding the exact valid boundary.
    let encoded = std::iter::once(low)
        .chain(low.saturating_sub(64)..=low.saturating_add(64).min(NS_SCANNER_MAX_FRAME_SIZE))
        .map(encode)
        .find(|encoded| encoded.len() == NS_SCANNER_MAX_FRAME_SIZE)
        .expect("valid authenticated frame exactly at the existing wire limit");
    let mut wire = u32::try_from(encoded.len()).expect("wire length").to_be_bytes().to_vec();
    wire.extend_from_slice(&encoded);
    let owner = EcstoreBackgroundMemoryBudget::new();
    // Each individual allocation is bounded by encoded.len(): raw staging,
    // the retained other field, and both old and new field growth allocations.
    owner.set_sampled_limit(u64::try_from(encoded.len() * 4).expect("growth peak bound"), Duration::from_secs(30));
    let (mut foreground_sequence, mut background_sequence) = (0, 0);
    let foreground = read_frame(&mut Cursor::new(&wire), &auth, &mut foreground_sequence, None)
        .await
        .expect("legacy None decoding");
    let background = read_frame(&mut Cursor::new(&wire), &auth, &mut background_sequence, Some(&owner))
        .await
        .expect("budgeted full-limit decoding");
    assert_eq!(
        rmp_serde::to_vec_named(&background).expect("background frame"),
        rmp_serde::to_vec_named(&foreground).expect("foreground frame")
    );
    assert_eq!((foreground_sequence, background_sequence), (1, 1));
    assert_eq!(owner.snapshot().used, 0);
}
