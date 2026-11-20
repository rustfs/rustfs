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

use super::*;
use futures::stream;

fn ample_space(_file: &std::fs::File) -> io::Result<StagingSpace> {
    Ok(StagingSpace {
        available: 64 * 1024 * 1024 * 1024,
        total: 64 * 1024 * 1024 * 1024,
        block_size: 1,
    })
}

fn test_closer() -> SyncSender<StagingOwned> {
    static CLOSER: OnceLock<SyncSender<StagingOwned>> = OnceLock::new();
    CLOSER.get_or_init(|| staging_closer(128).expect("start test closer")).clone()
}

async fn wait_for_slots(slots: &Arc<Semaphore>, count: u32) {
    let permit = tokio::time::timeout(Duration::from_secs(5), Arc::clone(slots).acquire_many_owned(count))
        .await
        .expect("staged file must close")
        .expect("staging slots are open");
    drop(permit);
}

async fn wait_for_slot(slots: &Arc<Semaphore>) {
    wait_for_slots(slots, 1).await;
}

fn fixture_with_budget(file: std::fs::File, slots: &Arc<Semaphore>, budget: &Arc<StagingBudget>) -> Arc<StagedFile> {
    Arc::new(StagedFile::new(
        file,
        staging_cipher().expect("test key"),
        Arc::clone(budget),
        acquire_staging_slot(slots).expect("acquire test slot"),
        test_closer(),
    ))
}

fn fixture(file: std::fs::File) -> (Arc<Semaphore>, Arc<StagedFile>) {
    let slots = Arc::new(Semaphore::new(1));
    let budget = Arc::new(StagingBudget::new(POST_STAGING_BUDGET, ample_space));
    let staged = fixture_with_budget(file, &slots, &budget);
    (slots, staged)
}

#[cfg(unix)]
#[test]
fn post_staging_refuses_linked_temporary_files() {
    let named = tempfile::NamedTempFile::new().expect("named temporary file");
    assert!(ensure_unlinked(named.as_file()).is_err(), "linked files are not accepted");
    let anonymous = tempfile::tempfile().expect("anonymous temporary file");
    ensure_unlinked(&anonymous).expect("unlinked file is accepted");
}

async fn contents(file: &Arc<StagedFile>) -> Vec<u8> {
    file_io(Arc::clone(file), |file| {
        file.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    })
    .await
    .expect("read staged bytes")
}

async fn partial_plaintext(file: &Arc<StagedFile>) -> Zeroizing<Vec<u8>> {
    let staged = Arc::clone(file);
    file_io(Arc::clone(file), move |file| {
        file.seek(SeekFrom::Start(0))?;
        let plaintext = read_frame(file, &staged.cipher, 0)?;
        assert_eq!(read_chunk(file, &mut [0; 1])?, 0, "no later frame was written");
        Ok(plaintext)
    })
    .await
    .expect("authenticate staged frame")
}

async fn rewind(file: &Arc<StagedFile>) {
    file_io(Arc::clone(file), |file| {
        file.seek(SeekFrom::Start(0))?;
        Ok(())
    })
    .await
    .expect("rewind staged file");
}

#[tokio::test]
async fn post_staging_requires_eof_and_rejects_the_first_byte_over_its_limit() {
    let (_, file) = fixture(tempfile::tempfile().expect("temporary file"));
    let chunks = Box::new(stream::iter([Ok(Bytes::from_static(b"hello"))]));
    assert_eq!(copy_post_file(&file, chunks, 5).await.unwrap(), 5);
    rewind(&file).await;
    let (mut replay, _) = BodyChunks::with_trailers(replay_post_file(file, 5), None);
    assert_eq!(replay.next().await.unwrap().unwrap(), Bytes::from_static(b"hello"));
    assert!(replay.next().await.is_none());

    let (_, file) = fixture(tempfile::tempfile().expect("temporary file"));
    let chunks = Box::new(stream::iter([Ok(Bytes::from_static(b"hello")), Ok(Bytes::from_static(b"!"))]));
    let error = copy_post_file(&file, chunks, 5).await.unwrap_err();
    assert_eq!(error.code(), &S3ErrorCode::EntityTooLarge);
    assert!(contents(&file).await.is_empty(), "neither pending nor over-limit bytes were written");
    rewind(&file).await;
    let (mut replay, _) = BodyChunks::with_trailers(replay_post_file(file, 5), None);
    assert!(replay.next().await.expect("incomplete staging fails").is_err());

    let (_, file) = fixture(tempfile::tempfile().expect("temporary file"));
    let incomplete = rustfs_gateway_stream::StreamError::incomplete_body();
    let chunks: UploadChunks = Box::new(stream::iter([Ok(Bytes::from_static(b"hello")), Err(Box::new(incomplete) as _)]));
    let error = copy_post_file(&file, chunks, 5).await.unwrap_err();
    assert_eq!(error.code(), &S3ErrorCode::MalformedPOSTRequest);
    assert!(contents(&file).await.is_empty(), "pending bytes were not written after the failure");
    rewind(&file).await;
    let (mut replay, _) = BodyChunks::with_trailers(replay_post_file(file, 5), None);
    assert!(replay.next().await.expect("incomplete staging fails").is_err());
}

#[tokio::test]
async fn post_staging_checked_body_validates_checksums_before_replay() {
    use sha2::{Digest as _, Sha256};

    for size in [0, REPLAY_CHUNK_SIZE + 37] {
        let bytes = vec![42; size];
        let sha = base64_simd::STANDARD.encode_to_string(Sha256::digest(&bytes));
        let md5 = base64_simd::STANDARD.encode_to_string(md5::Md5::digest(&bytes));
        for (wrong_sha, wrong_md5) in [(false, false), (true, false), (false, true)] {
            let (_, file) = fixture(tempfile::tempfile().expect("temporary file"));
            let mut headers = HeaderMap::new();
            let sha = if wrong_sha {
                base64_simd::STANDARD.encode_to_string([0u8; 32])
            } else {
                sha.clone()
            };
            headers.insert("x-amz-checksum-sha256", sha.parse().expect("checksum header"));
            let md5 = if wrong_md5 {
                base64_simd::STANDARD.encode_to_string([0u8; 16])
            } else {
                md5.clone()
            };
            let chunks: UploadChunks = Box::new(stream::iter([Ok(Bytes::copy_from_slice(&bytes))]));
            let checked = checked_post_chunks(
                chunks,
                Some(PostBodyIntegrity {
                    checksum_headers: headers,
                    content_md5: Some(md5),
                    content_sha256: None,
                }),
            )
            .expect("valid checksum declarations");
            let result = copy_post_file(&file, checked, u64::try_from(size).expect("test size")).await;
            if wrong_sha || wrong_md5 {
                assert_eq!(result.expect_err("incorrect checksum must fail").code(), &S3ErrorCode::BadDigest);
                rewind(&file).await;
                let (mut replay, _) = BodyChunks::with_trailers(replay_post_file(file, size as u64), None);
                let mut failed = false;
                while let Some(chunk) = replay.next().await {
                    if chunk.is_err() {
                        failed = true;
                        break;
                    }
                }
                assert!(failed, "failed integrity verification must not produce a complete replay");
            } else {
                assert_eq!(result.expect("matching checksums"), i64::try_from(size).expect("test length"));
                rewind(&file).await;
                let (mut replay, _) = BodyChunks::with_trailers(replay_post_file(file, size as u64), None);
                let mut actual = Vec::new();
                while let Some(chunk) = replay.next().await {
                    actual.extend_from_slice(&chunk.expect("verified replay"));
                }
                assert_eq!(actual, bytes);
            }
        }
    }
}

#[tokio::test]
async fn post_staging_checked_body_refuses_malformed_md5_before_reading() {
    use std::sync::atomic::AtomicBool;

    for value in ["invalid", "AAAA"] {
        let polled = Arc::new(AtomicBool::new(false));
        let observer = Arc::clone(&polled);
        let chunks: UploadChunks = Box::new(stream::iter([Ok(Bytes::from_static(b"hello"))]).inspect(move |_| {
            observer.store(true, Ordering::Relaxed);
        }));
        let result = checked_post_chunks(
            chunks,
            Some(PostBodyIntegrity {
                checksum_headers: HeaderMap::new(),
                content_md5: Some(value.to_owned()),
                content_sha256: None,
            }),
        );
        let error = match result {
            Ok(_) => panic!("invalid MD5 must not produce a body"),
            Err(error) => error,
        };
        assert_eq!(error.code(), &S3ErrorCode::InvalidDigest);
        assert!(!polled.load(Ordering::Relaxed), "invalid MD5 must fail before body polling");
    }
}

#[tokio::test]
async fn post_staging_checked_body_rejects_md5_mismatch() {
    let (_, file) = fixture(tempfile::tempfile().expect("temporary file"));
    let chunks: UploadChunks = Box::new(stream::iter([Ok(Bytes::from_static(b"hello"))]));
    let checked = checked_post_chunks(
        chunks,
        Some(PostBodyIntegrity {
            checksum_headers: HeaderMap::new(),
            content_md5: Some("AAAAAAAAAAAAAAAAAAAAAA==".to_owned()),
            content_sha256: None,
        }),
    )
    .expect("valid MD5 declaration");
    let error = copy_post_file(&file, checked, 5)
        .await
        .expect_err("wrong MD5 must fail staging");
    assert_eq!(error.code(), &S3ErrorCode::BadDigest);
    rewind(&file).await;
    let (mut replay, _) = BodyChunks::with_trailers(replay_post_file(file, 5), None);
    assert!(replay.next().await.expect("missing end frame fails").is_err());
}

#[tokio::test]
async fn post_staging_integrity_precedes_replay_and_releases_budget_on_failure() {
    use sha2::{Digest as _, Sha256};

    let bytes = vec![42; REPLAY_CHUNK_SIZE + 37];
    let good = base64_simd::STANDARD.encode_to_string(Sha256::digest(&bytes));
    for wrong in [false, true] {
        let slots = Arc::new(Semaphore::new(1));
        let budget = Arc::new(StagingBudget::new(POST_STAGING_BUDGET, ample_space));
        let mut headers = HeaderMap::new();
        let expected = if wrong {
            base64_simd::STANDARD.encode_to_string([0u8; 32])
        } else {
            good.clone()
        };
        headers.insert("x-amz-checksum-sha256", expected.parse().expect("checksum header"));
        let body = from_io_stream(stream::iter([Ok(Bytes::copy_from_slice(&bytes))]));
        let envelope = staging_envelope();
        let result = stage_admitted_post_body(
            &envelope,
            "bucket",
            "key",
            (
                body,
                Some(PostBodyIntegrity {
                    checksum_headers: headers,
                    content_md5: None,
                    content_sha256: None,
                }),
            ),
            acquire_staging_slot(&slots).expect("admit staging"),
            Arc::clone(&budget),
            test_closer(),
        )
        .await;
        if wrong {
            let error = match result {
                Ok(_) => panic!("a mismatched archive checksum must not yield a replay"),
                Err(error) => error,
            };
            assert_eq!(error.code(), &S3ErrorCode::BadDigest);
        } else {
            let (body, length) = result.expect("valid checksum permits staging");
            assert_eq!(length, i64::try_from(bytes.len()).expect("test length"));
            let (mut replay, _) = BodyChunks::with_trailers(body, None);
            let mut actual = Vec::new();
            while let Some(chunk) = replay.next().await {
                actual.extend_from_slice(&chunk.expect("verified replay"));
            }
            assert_eq!(actual, bytes);
            drop(replay);
        }
        wait_for_slot(&slots).await;
        assert_eq!(budget.state.lock().reserved, 0, "staging budget is released after close");
    }
}

#[tokio::test]
async fn post_staging_checked_body_preserves_incomplete_transport_failure() {
    let (_, file) = fixture(tempfile::tempfile().expect("temporary file"));
    let incomplete = rustfs_gateway_stream::StreamError::incomplete_body();
    let chunks: UploadChunks = Box::new(stream::iter([Ok(Bytes::from_static(b"hello")), Err(Box::new(incomplete) as _)]));
    let checked = checked_post_chunks(
        chunks,
        Some(PostBodyIntegrity {
            checksum_headers: HeaderMap::new(),
            content_md5: None,
            content_sha256: None,
        }),
    )
    .expect("empty checksum declaration");
    let error = copy_post_file(&file, checked, 5)
        .await
        .expect_err("incomplete source must not stage successfully");
    assert_eq!(error.code(), &S3ErrorCode::MalformedPOSTRequest);
    assert!(
        contents(&file).await.is_empty(),
        "an incomplete source must not create a complete staged file"
    );
}

#[tokio::test]
async fn post_staging_source_and_limit_errors_are_not_masked_by_unwritable_staging() {
    for over_limit in [false, true] {
        let path = tempfile::NamedTempFile::new().expect("temporary file");
        let (_, file) = fixture(std::fs::File::open(path.path()).expect("read-only file"));
        let last = if over_limit {
            Ok(Bytes::from_static(b"!"))
        } else {
            Err(Box::new(rustfs_gateway_stream::StreamError::incomplete_body()) as Box<dyn std::error::Error + Send + Sync>)
        };
        let chunks: UploadChunks = Box::new(stream::iter([Ok(Bytes::from_static(b"hello")), last]));
        let error = copy_post_file(&file, chunks, 5).await.unwrap_err();
        let expected = if over_limit {
            S3ErrorCode::EntityTooLarge
        } else {
            S3ErrorCode::MalformedPOSTRequest
        };
        assert_eq!(error.code(), &expected);
        assert!(contents(&file).await.is_empty(), "pending data was not written");
    }
}

#[tokio::test(start_paused = true)]
async fn post_staging_native_progress_reports_request_timeout() {
    use futures::poll;

    let (_, file) = fixture(tempfile::tempfile().expect("temporary file"));
    let chunks: UploadChunks = Box::new(stream::pending());
    let (chunks, control) = guard_native_upload_progress(chunks, Duration::from_secs(1), "bucket", "key", "request", 0);
    let mut stage = Box::pin(copy_post_file(&file, demand_native_upload(chunks, control), 5));
    assert!(poll!(stage.as_mut()).is_pending(), "the file body is initially stalled");
    tokio::time::advance(Duration::from_secs(1)).await;
    let error = stage.await.expect_err("stalled staging must time out");
    assert_eq!(error.code(), &S3ErrorCode::RequestTimeout);
}

#[tokio::test]
async fn post_staging_failure_leaves_only_complete_frames_without_an_end_frame() {
    for over_limit in [false, true] {
        let (_, file) = fixture(tempfile::tempfile().expect("temporary file"));
        let first = Bytes::from(vec![17; REPLAY_CHUNK_SIZE + 3]);
        let last = if over_limit {
            Ok(Bytes::from_static(b"!"))
        } else {
            Err(Box::new(rustfs_gateway_stream::StreamError::incomplete_body()) as Box<dyn std::error::Error + Send + Sync>)
        };
        let chunks: UploadChunks = Box::new(stream::iter([Ok(first), last]));
        let error = copy_post_file(&file, chunks, (REPLAY_CHUNK_SIZE + 3) as u64)
            .await
            .unwrap_err();
        let expected = if over_limit {
            S3ErrorCode::EntityTooLarge
        } else {
            S3ErrorCode::MalformedPOSTRequest
        };
        assert_eq!(error.code(), &expected);
        let expected_frame = vec![17; REPLAY_CHUNK_SIZE];
        assert_eq!(partial_plaintext(&file).await.as_slice(), expected_frame.as_slice());
        rewind(&file).await;
        let (mut replay, _) = BodyChunks::with_trailers(replay_post_file(file, (REPLAY_CHUNK_SIZE + 3) as u64), None);
        assert_eq!(replay.next().await.unwrap().unwrap().as_ref(), expected_frame.as_slice());
        assert!(replay.next().await.expect("missing end frame fails").is_err());
    }
}

#[tokio::test]
async fn post_staging_refuses_a_failed_disk_write() {
    let path = tempfile::NamedTempFile::new().expect("temporary file");
    let (_, file) = fixture(std::fs::File::open(path.path()).expect("read-only file"));
    let chunks = Box::new(stream::iter([Ok(Bytes::from_static(b"hello"))]));
    let error = copy_post_file(&file, chunks, 5).await.unwrap_err();
    assert_eq!(error.code(), &S3ErrorCode::InternalError);
    assert_eq!(error.message(), Some("Unable to stage the POST file"));
}

#[tokio::test]
async fn post_staging_cancellation_holds_slot_until_blocking_io_finishes() {
    for read in [false, true] {
        let (slots, file) = fixture(tempfile::tempfile().expect("temporary file"));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = Arc::clone(&file);
        let request = tokio::spawn(async move {
            file_io(worker, move |file| {
                started_tx.send(()).map_err(|_| io::Error::other("test lost its observer"))?;
                release_rx.recv().map_err(|_| io::Error::other("test lost its release"))?;
                if read {
                    file.read(&mut [0; 1]).map(|_| ())
                } else {
                    file.write_all(b"data")
                }
            })
            .await
        });
        started_rx.await.expect("blocking I/O started");
        drop(file);
        request.abort();
        assert!(request.await.expect_err("request cancelled").is_cancelled());
        assert!(Arc::clone(&slots).try_acquire_owned().is_err(), "in-flight I/O owns the slot");
        release_tx.send(()).expect("release blocking I/O");
        let permit = tokio::time::timeout(std::time::Duration::from_secs(5), Arc::clone(&slots).acquire_owned())
            .await
            .expect("slot returned after I/O")
            .expect("slot is open");
        drop(permit);
    }
}

#[tokio::test]
async fn post_staging_cancelled_upload_releases_slot_without_eof() {
    let (slots, file) = fixture(tempfile::tempfile().expect("temporary file"));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let first = stream::iter([Ok(Bytes::from(vec![7; REPLAY_CHUNK_SIZE + 11]))]);
    let wait = stream::once(async move {
        let _ = started_tx.send(());
        futures::future::pending::<Result<Bytes, Box<dyn std::error::Error + Send + Sync>>>().await
    });
    let chunks: UploadChunks = Box::new(Box::pin(first.chain(wait)));
    let request = tokio::spawn(async move { copy_post_file(&file, chunks, (REPLAY_CHUNK_SIZE + 11) as u64).await });
    started_rx.await.expect("first encrypted frame was written");
    request.abort();
    assert!(request.await.expect_err("upload cancelled").is_cancelled());
    let permit = tokio::time::timeout(std::time::Duration::from_secs(5), Arc::clone(&slots).acquire_owned())
        .await
        .expect("slot returned after cancellation")
        .expect("slot is open");
    drop(permit);
}

#[tokio::test]
async fn post_staging_closes_off_executor_and_retains_resources_until_close() {
    let slots = Arc::new(Semaphore::new(2));
    let budget = Arc::new(StagingBudget::new(1024, ample_space));
    let closer = staging_closer(2).expect("start dedicated closer");
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    let mut first = StagedFile::new(
        tempfile::tempfile().expect("first staging file"),
        staging_cipher().expect("first key"),
        Arc::clone(&budget),
        acquire_staging_slot(&slots).expect("first slot"),
        closer.clone(),
    );
    let second = StagedFile::new(
        tempfile::tempfile().expect("second staging file"),
        staging_cipher().expect("second key"),
        Arc::clone(&budget),
        acquire_staging_slot(&slots).expect("second slot"),
        closer,
    );
    for staged in [&first, &second] {
        let file = staged.file.lock();
        drop(
            staged
                .allocation
                .reserve(&file, FRAME_OVERHEAD)
                .expect("reserve staging bytes"),
        );
    }
    first.owned.as_mut().expect("live staged resources").before_close = Some(Box::new(move || {
        let _ = started_tx.send(std::thread::current().id());
        let _ = release_rx.into_inner().expect("close test lock").recv();
    }));
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let dropper = std::thread::spawn(move || {
        let caller = std::thread::current().id();
        drop(first);
        let _ = dropped_tx.send(caller);
    });
    let closer_thread = tokio::time::timeout(Duration::from_secs(5), started_rx)
        .await
        .expect("closer starts")
        .expect("closer reports its thread");
    let caller_thread = tokio::time::timeout(Duration::from_secs(5), dropped_rx)
        .await
        .expect("dropping the file must not wait for close")
        .expect("dropper reports its thread");
    assert_ne!(closer_thread, caller_thread, "file closure runs outside the caller");
    drop(second);
    assert!(acquire_staging_slot(&slots).is_err(), "closing and queued files keep both slots");
    assert_eq!(budget.state.lock().reserved, 2 * FRAME_OVERHEAD, "both files remain charged");
    release_tx.send(()).expect("release closer");
    wait_for_slots(&slots, 2).await;
    dropper.join().expect("dropper completed");
    assert_eq!(budget.state.lock().reserved, 0, "budget returns after both files close");
}

#[test]
fn post_staging_failed_closer_still_releases_budget_and_slot() {
    let slots = Arc::new(Semaphore::new(1));
    let budget = Arc::new(StagingBudget::new(1024, ample_space));
    let (closer, receiver) = mpsc::sync_channel(1);
    drop(receiver);
    let staged = StagedFile::new(
        tempfile::tempfile().expect("staging file"),
        staging_cipher().expect("key"),
        Arc::clone(&budget),
        acquire_staging_slot(&slots).expect("slot"),
        closer,
    );
    let file = staged.file.lock();
    drop(
        staged
            .allocation
            .reserve(&file, FRAME_OVERHEAD)
            .expect("reserve staging bytes"),
    );
    drop(file);
    drop(staged);
    assert_eq!(budget.state.lock().reserved, 0);
    assert!(acquire_staging_slot(&slots).is_ok(), "a disconnected closer cannot strand a slot");
}

#[tokio::test]
async fn post_staging_cancelled_creation_retains_slot_until_file_closes() {
    let slots = Arc::new(Semaphore::new(1));
    let permit = acquire_staging_slot(&slots).expect("admit staging");
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let request = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            started_tx.send(()).expect("creation observer");
            release_rx.recv().expect("creation release");
            let file = tempfile::tempfile().expect("temporary file");
            StagedFile::new(
                file,
                staging_cipher().expect("test key"),
                Arc::new(StagingBudget::new(POST_STAGING_BUDGET, ample_space)),
                permit,
                test_closer(),
            )
        })
        .await
    });
    started_rx.await.expect("creation started");
    request.abort();
    let error = match request.await {
        Err(error) => error,
        Ok(_) => panic!("creation request was not cancelled"),
    };
    assert!(error.is_cancelled());
    assert!(Arc::clone(&slots).try_acquire_owned().is_err(), "creation owns the slot");
    release_tx.send(()).expect("release creation");
    let permit = tokio::time::timeout(std::time::Duration::from_secs(5), Arc::clone(&slots).acquire_owned())
        .await
        .expect("slot returned after creation")
        .expect("slot is open");
    drop(permit);
}

#[test]
fn post_staging_saturation_returns_slowdown_and_releases_slot() {
    let slots = Arc::new(Semaphore::new(1));
    let permit = acquire_staging_slot(&slots).expect("first upload is admitted");
    assert_eq!(
        acquire_staging_slot(&slots).expect_err("second upload is refused").code(),
        &S3ErrorCode::SlowDown
    );
    drop(permit);
    assert!(acquire_staging_slot(&slots).is_ok(), "the slot becomes available");
}

#[test]
fn post_staging_default_admits_two_requests_and_fails_fast_at_capacity() {
    let resources = StagingResources::new().expect("start staging resources");
    let first = acquire_staging_slot(&resources.slots).expect("first slot");
    let second = acquire_staging_slot(&resources.slots).expect("second slot");
    assert_eq!(
        acquire_staging_slot(&resources.slots)
            .expect_err("third request is refused")
            .code(),
        &S3ErrorCode::SlowDown
    );
    drop(first);
    assert!(acquire_staging_slot(&resources.slots).is_ok(), "a released slot is reusable");
    drop(second);
}

#[tokio::test]
async fn post_staging_budget_counts_all_frames_until_replay_is_dropped() {
    let slots = Arc::new(Semaphore::new(2));
    let budget = Arc::new(StagingBudget::new(45, ample_space));
    let first = fixture_with_budget(tempfile::tempfile().expect("first file"), &slots, &budget);
    let second = fixture_with_budget(tempfile::tempfile().expect("second file"), &slots, &budget);
    let chunks = Box::new(stream::iter([Ok(Bytes::from_static(b"hello"))]));
    assert_eq!(copy_post_file(&first, chunks, 5).await.expect("stage first upload"), 5);
    assert_eq!(contents(&first).await.len(), 45, "data header and tag plus the end frame are budgeted");
    assert_eq!(budget.state.lock().reserved, 45);

    let empty: UploadChunks = Box::new(stream::empty());
    let error = copy_post_file(&second, empty, 0).await.expect_err("shared budget is full");
    assert_eq!(error.code(), &S3ErrorCode::SlowDown);
    assert!(contents(&second).await.is_empty(), "a rejected end frame is not written");
    rewind(&first).await;
    let replay = replay_post_file(first, 5);
    assert_eq!(budget.state.lock().reserved, 45, "replay retains the allocation");
    drop(replay);
    wait_for_slot(&slots).await;
    assert_eq!(budget.state.lock().reserved, 0, "closing the staged file returns the budget");

    let empty: UploadChunks = Box::new(stream::empty());
    assert_eq!(copy_post_file(&second, empty, 0).await.expect("reuse the budget"), 0);
    assert_eq!(contents(&second).await.len(), 20, "the empty end frame is counted");
}

#[tokio::test]
async fn post_staging_budget_rejects_before_the_unreserved_frame() {
    let slots = Arc::new(Semaphore::new(1));
    let budget = Arc::new(StagingBudget::new(25, ample_space));
    let file = fixture_with_budget(tempfile::tempfile().expect("file"), &slots, &budget);
    let chunks = Box::new(stream::iter([Ok(Bytes::from_static(b"hello"))]));
    let error = copy_post_file(&file, chunks, 5).await.expect_err("end frame exceeds budget");
    assert_eq!(error.code(), &S3ErrorCode::SlowDown);
    assert_eq!(contents(&file).await.len(), 25, "only the fully reserved data frame was written");
    assert_eq!(budget.state.lock().reserved, 25);
    rewind(&file).await;
    let (mut replay, _) = BodyChunks::with_trailers(replay_post_file(file, 5), None);
    assert!(replay.next().await.expect("missing end frame fails").is_err());
    drop(replay);
    wait_for_slot(&slots).await;
    assert_eq!(budget.state.lock().reserved, 0);
}

fn space_at_end_frame_boundary(_file: &std::fs::File) -> io::Result<StagingSpace> {
    Ok(StagingSpace {
        available: POST_STAGING_MAX_FREE_RESERVE + FRAME_OVERHEAD,
        total: 10 * POST_STAGING_MAX_FREE_RESERVE,
        block_size: 1,
    })
}

fn space_below_end_frame_boundary(_file: &std::fs::File) -> io::Result<StagingSpace> {
    Ok(StagingSpace {
        available: POST_STAGING_MAX_FREE_RESERVE + FRAME_OVERHEAD - 1,
        total: 10 * POST_STAGING_MAX_FREE_RESERVE,
        block_size: 1,
    })
}

fn space_at_percentage_floor(_file: &std::fs::File) -> io::Result<StagingSpace> {
    Ok(StagingSpace {
        available: 2 * POST_STAGING_MAX_FREE_RESERVE / 10 + 4096,
        total: 2 * POST_STAGING_MAX_FREE_RESERVE,
        block_size: 4096,
    })
}

fn space_below_percentage_floor(_file: &std::fs::File) -> io::Result<StagingSpace> {
    Ok(StagingSpace {
        available: 2 * POST_STAGING_MAX_FREE_RESERVE / 10 + 4095,
        total: 2 * POST_STAGING_MAX_FREE_RESERVE,
        block_size: 4096,
    })
}

fn unavailable_space(_file: &std::fs::File) -> io::Result<StagingSpace> {
    Err(io::Error::other("injected space query failure"))
}

fn space_on_large_volume(_file: &std::fs::File) -> io::Result<StagingSpace> {
    Ok(StagingSpace {
        available: 20 * POST_STAGING_MAX_FREE_RESERVE,
        total: 926 * POST_STAGING_MAX_FREE_RESERVE,
        block_size: 4096,
    })
}

#[test]
fn post_staging_filesystem_statistics_reject_invalid_blocks_and_overflow() {
    let valid = staging_space_from_blocks(4096, 100, 60, 50).expect("valid filesystem statistics");
    assert_eq!(valid.total, 409_600);
    assert_eq!(valid.available, 204_800);
    assert_eq!(valid.block_size, 4096);
    for (block_size, blocks, free, available) in [
        (0, 100, 60, 50),
        (4096, 0, 0, 0),
        (4096, 100, 101, 50),
        (4096, 100, 60, 61),
        (4096, u64::MAX, 1, 1),
    ] {
        assert!(
            staging_space_from_blocks(block_size, blocks, free, available).is_err(),
            "invalid filesystem statistics must fail closed"
        );
    }
    let no_space = staging_space_from_blocks(4096, 100, 0, 0).expect("zero available blocks is valid");
    assert_eq!(no_space.available, 0);
}

#[test]
fn post_staging_space_requirement_rejects_invalid_totals_and_overflow() {
    let empty = StagingSpace {
        available: 0,
        total: 0,
        block_size: 1,
    };
    assert!(matches!(empty.required(0, 20), Err(StagingWriteError::Io)));
    let invalid = StagingSpace {
        available: 2,
        total: 1,
        block_size: 1,
    };
    assert!(matches!(invalid.required(0, 20), Err(StagingWriteError::Io)));
    let zero_block = StagingSpace {
        available: 1,
        total: 1,
        block_size: 0,
    };
    assert!(matches!(zero_block.required(0, 20), Err(StagingWriteError::Io)));
    let tiny = StagingSpace {
        available: 1,
        total: 1,
        block_size: 1,
    };
    assert_eq!(tiny.required(0, 0).expect("nonzero floor"), 1);
    assert!(matches!(tiny.required(u64::MAX, 1), Err(StagingWriteError::Capacity)));
}

#[tokio::test]
async fn post_staging_space_floor_and_query_failure_are_fail_closed() {
    for (probe, expected) in [
        (space_at_end_frame_boundary as fn(&std::fs::File) -> io::Result<StagingSpace>, None),
        (space_below_end_frame_boundary, Some(S3ErrorCode::SlowDown)),
        (space_at_percentage_floor, None),
        (space_below_percentage_floor, Some(S3ErrorCode::SlowDown)),
        (unavailable_space, Some(S3ErrorCode::InternalError)),
    ] {
        let slots = Arc::new(Semaphore::new(1));
        let budget = Arc::new(StagingBudget::new(1024, probe));
        let file = fixture_with_budget(tempfile::tempfile().expect("file"), &slots, &budget);
        let chunks: UploadChunks = Box::new(stream::empty());
        let result = copy_post_file(&file, chunks, 0).await;
        match expected {
            None => assert_eq!(result.expect("exact floor is allowed"), 0),
            Some(code) => {
                assert_eq!(result.expect_err("space check rejects").code(), &code);
                assert!(contents(&file).await.is_empty(), "rejection precedes writing");
                assert_eq!(budget.state.lock().reserved, 0);
            }
        }
    }
}

fn space_for_one_pending_frame(_file: &std::fs::File) -> io::Result<StagingSpace> {
    Ok(StagingSpace {
        available: POST_STAGING_MAX_FREE_RESERVE + 100,
        total: 10 * POST_STAGING_MAX_FREE_RESERVE,
        block_size: 1,
    })
}

fn oversized_block_space(_file: &std::fs::File) -> io::Result<StagingSpace> {
    Ok(StagingSpace {
        available: u64::MAX,
        total: u64::MAX,
        block_size: u64::MAX,
    })
}

#[tokio::test]
async fn post_staging_capacity_arithmetic_rejects_overflow() {
    let slots = Arc::new(Semaphore::new(2));
    let budget = Arc::new(StagingBudget::new(u64::MAX, ample_space));
    let first = fixture_with_budget(tempfile::tempfile().expect("first file"), &slots, &budget);
    {
        let first_file = first.file.lock();
        let pending = first.allocation.reserve(&first_file, 1).expect("small reservation");
        assert!(matches!(
            first.allocation.reserve(&first_file, u64::MAX),
            Err(StagingWriteError::Capacity)
        ));
        assert_eq!(budget.state.lock().reserved, 1);
        drop(pending);
    }
    drop(first);
    wait_for_slots(&slots, 2).await;
    assert_eq!(budget.state.lock().reserved, 0);

    let budget = Arc::new(StagingBudget::new(u64::MAX, oversized_block_space));
    let file = fixture_with_budget(tempfile::tempfile().expect("file"), &slots, &budget);
    let handle = file.file.lock();
    assert!(matches!(file.allocation.reserve(&handle, FRAME_OVERHEAD), Err(StagingWriteError::Io)));
    assert_eq!(budget.state.lock().reserved, 0);
}

#[test]
fn post_staging_space_floor_counts_concurrent_pending_writes() {
    let slots = Arc::new(Semaphore::new(2));
    let budget = Arc::new(StagingBudget::new(1024, space_for_one_pending_frame));
    let first = fixture_with_budget(tempfile::tempfile().expect("first file"), &slots, &budget);
    let second = fixture_with_budget(tempfile::tempfile().expect("second file"), &slots, &budget);
    let first_file = first.file.lock();
    let pending = first
        .allocation
        .reserve(&first_file, 80)
        .expect("reserve first pending write");
    let second_file = second.file.lock();
    assert!(
        matches!(second.allocation.reserve(&second_file, 21), Err(StagingWriteError::Capacity)),
        "outstanding writes count against the floor"
    );
    drop(pending);
    assert!(
        second.allocation.reserve(&second_file, 21).is_ok(),
        "completed write no longer counts as pending"
    );
}

#[test]
fn post_staging_space_retries_stale_snapshots_without_holding_the_budget_lock() {
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let slots = Arc::new(Semaphore::new(2));
        let budget = Arc::new(StagingBudget::new(1024, ample_space));
        let first = fixture_with_budget(tempfile::tempfile().expect("first file"), &slots, &budget);
        let second = fixture_with_budget(tempfile::tempfile().expect("second file"), &slots, &budget);
        let first_file = first.file.lock();
        let second_file = second.file.lock();
        let mut pending = Some(first.allocation.reserve(&first_file, 1).expect("first write"));
        let mut calls = 0;
        let second_pending = second
            .allocation
            .reserve_with(&second_file, 20, |file| {
                calls += 1;
                drop(pending.take());
                ample_space(file)
            })
            .expect("retry after the first snapshot became stale");
        assert_eq!(calls, 2);
        drop(second_pending);

        let mut calls = 0;
        let result = second.allocation.reserve_with(&second_file, 20, |file| {
            calls += 1;
            drop(first.allocation.reserve(&first_file, 1).expect("concurrent write"));
            ample_space(file)
        });
        assert!(matches!(result, Err(StagingWriteError::Capacity)));
        assert_eq!(calls, POST_STAGING_SPACE_ATTEMPTS, "retries are bounded");
        finished_tx.send(()).expect("report completion");
    });
    finished_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("space queries must not hold the budget lock");
    worker.join().expect("space query test completed");
}

#[cfg(unix)]
#[test]
fn post_staging_space_queries_the_open_file() {
    let file = tempfile::tempfile().expect("anonymous file");
    let actual = staging_space(&file).expect("file handle space query");
    let stat = rustix::fs::fstatvfs(&file).expect("filesystem statistics");
    let block_size = if stat.f_frsize == 0 { stat.f_bsize } else { stat.f_frsize };
    assert_eq!(actual.block_size, block_size);
    assert!(actual.available <= actual.total, "the sampled available space is valid");
    assert_eq!(actual.total, stat.f_blocks.checked_mul(block_size).expect("total bytes"));
}

#[cfg(not(unix))]
#[test]
fn post_staging_space_fails_closed_without_a_file_handle_query() {
    let file = tempfile::tempfile().expect("temporary file");
    assert!(staging_space(&file).is_err());
}

fn staging_envelope() -> RequestEnvelope {
    RequestEnvelope::from_parts(
        crate::app::RequestHead {
            method: http::Method::POST,
            uri: http::Uri::from_static("/bucket"),
            headers: http::HeaderMap::new(),
            credentials: None,
            region: None,
            service: None,
        },
        http::Extensions::new(),
    )
}

#[tokio::test]
async fn post_staging_checks_space_before_polling_the_body() {
    use std::sync::atomic::AtomicBool;

    let slots = Arc::new(Semaphore::new(1));
    let budget = Arc::new(StagingBudget::new(1024, unavailable_space));
    let permit = acquire_staging_slot(&slots).expect("admit request");
    let polled = Arc::new(AtomicBool::new(false));
    let observer = Arc::clone(&polled);
    let body = from_io_stream(stream::once(async move {
        observer.store(true, Ordering::Relaxed);
        Ok(Bytes::from_static(b"hello"))
    }));
    let envelope = staging_envelope();
    let result =
        stage_admitted_post_body(&envelope, "bucket", "key", (body, None), permit, Arc::clone(&budget), test_closer()).await;
    let error = match result {
        Ok(_) => panic!("an unavailable space query must fail"),
        Err(error) => error,
    };
    assert_eq!(error.code(), &S3ErrorCode::InternalError);
    assert!(!polled.load(Ordering::Relaxed), "the body must not be consumed");
    assert_eq!(budget.state.lock().reserved, 0);
    assert!(acquire_staging_slot(&slots).is_ok());
}

#[tokio::test]
async fn post_staging_low_watermark_is_checked_before_body_polling() {
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Context, Poll};

    struct CountedBody {
        polls: Arc<AtomicUsize>,
        chunk: Option<Bytes>,
    }

    impl futures::Stream for CountedBody {
        type Item = io::Result<Bytes>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(self.chunk.take().map(Ok))
        }
    }

    for (probe, allowed, has_data) in [
        (
            space_below_end_frame_boundary as fn(&std::fs::File) -> io::Result<StagingSpace>,
            false,
            true,
        ),
        (space_at_end_frame_boundary, true, false),
        (ample_space, true, true),
        (space_on_large_volume, true, true),
    ] {
        let slots = Arc::new(Semaphore::new(1));
        let budget = Arc::new(StagingBudget::new(1024, probe));
        let permit = acquire_staging_slot(&slots).expect("admit request");
        let polls = Arc::new(AtomicUsize::new(0));
        let body = from_io_stream(CountedBody {
            polls: Arc::clone(&polls),
            chunk: has_data.then(|| Bytes::from_static(b"hello")),
        });
        let envelope = staging_envelope();
        let result = stage_admitted_post_body(&envelope, "bucket", "key", (body, None), permit, budget, test_closer()).await;
        if allowed {
            let (replay, length) = result.expect("sufficient space admits the body");
            assert_eq!(length, if has_data { 5 } else { 0 });
            assert!(polls.load(Ordering::SeqCst) > 0, "the admitted body is polled");
            drop(replay);
        } else {
            let error = match result {
                Ok(_) => panic!("low space must reject the body"),
                Err(error) => error,
            };
            assert_eq!(error.code(), &S3ErrorCode::SlowDown);
            assert_eq!(polls.load(Ordering::SeqCst), 0, "low space rejects before the first body poll");
        }
    }
}

#[tokio::test]
async fn post_staging_admitted_body_replays_when_completed_before_deadline() {
    let slots = Arc::new(Semaphore::new(1));
    let budget = Arc::new(StagingBudget::new(1024, ample_space));
    let permit = acquire_staging_slot(&slots).expect("admit request");
    let body = from_io_stream(stream::iter([Ok(Bytes::from_static(b"hello"))]));
    let envelope = staging_envelope();
    let (replay, length) = with_staging_deadline(
        Duration::from_secs(10),
        stage_admitted_post_body(&envelope, "bucket", "key", (body, None), permit, Arc::clone(&budget), test_closer()),
    )
    .await
    .expect("stage before deadline");
    assert_eq!(length, 5);
    assert_eq!(budget.state.lock().reserved, 45);
    let (mut chunks, _) = BodyChunks::with_trailers(replay, None);
    assert_eq!(
        chunks.next().await.expect("replay chunk").expect("valid frame"),
        Bytes::from_static(b"hello")
    );
    drop(chunks);
    wait_for_slot(&slots).await;
    assert_eq!(budget.state.lock().reserved, 0);
    assert!(acquire_staging_slot(&slots).is_ok());
}

#[tokio::test]
async fn post_staging_absolute_deadline_cancels_only_the_staging_phase() {
    let slots = Arc::new(Semaphore::new(1));
    let budget = Arc::new(StagingBudget::new(POST_STAGING_BUDGET, ample_space));
    let permit = acquire_staging_slot(&slots).expect("admit request");
    let request_budget = Arc::clone(&budget);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let chunks = stream::iter([Ok(Bytes::from(vec![7; REPLAY_CHUNK_SIZE]))]).chain(stream::once(async move {
        let _ = started_tx.send(());
        futures::future::pending::<io::Result<Bytes>>().await
    }));
    let request = tokio::spawn(async move {
        let envelope = staging_envelope();
        let body = from_io_stream(chunks);
        with_staging_deadline(
            POST_STAGING_TIMEOUT,
            stage_admitted_post_body(&envelope, "bucket", "key", (body, None), permit, request_budget, test_closer()),
        )
        .await
        .map(|(_, length)| length)
    });
    tokio::time::timeout(Duration::from_secs(10), started_rx)
        .await
        .expect("the first frame was staged in time")
        .expect("a frame was staged before the stalled body");
    assert!(budget.state.lock().reserved > 0);
    tokio::time::pause();
    tokio::time::advance(POST_STAGING_TIMEOUT).await;
    let error = request.await.expect("deadline task").expect_err("staging deadline expires");
    assert_eq!(error.code(), &S3ErrorCode::RequestTimeout);
    tokio::time::resume();
    wait_for_slot(&slots).await;
    assert_eq!(budget.state.lock().reserved, 0);
    assert!(acquire_staging_slot(&slots).is_ok(), "the timed-out file has closed");

    let completed = with_staging_deadline(Duration::from_secs(10), async { Ok::<_, S3Error>(17) })
        .await
        .expect("a completed stage does not time out");
    assert_eq!(completed, 17);
}

#[tokio::test]
async fn post_staging_cancelled_blocking_write_retains_budget_until_file_closes() {
    use std::sync::atomic::AtomicUsize;

    let slots = Arc::new(Semaphore::new(1));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let started = Mutex::new(Some(started_tx));
    let release = Mutex::new(release_rx);
    let calls = AtomicUsize::new(0);
    let budget = Arc::new(StagingBudget::new(POST_STAGING_BUDGET, move |file| {
        if calls.fetch_add(1, Ordering::Relaxed) == 1 {
            started
                .lock()
                .take()
                .ok_or_else(|| io::Error::other("missing observer"))?
                .send(())
                .map_err(|_| io::Error::other("test lost its observer"))?;
            release.lock().recv().map_err(|_| io::Error::other("test lost its release"))?;
        }
        ample_space(file)
    }));
    let file = fixture_with_budget(tempfile::tempfile().expect("file"), &slots, &budget);
    let request = tokio::spawn(async move {
        let chunks = Box::new(stream::iter([Ok(Bytes::from(vec![7; 2 * REPLAY_CHUNK_SIZE]))]));
        copy_post_file(&file, chunks, (2 * REPLAY_CHUNK_SIZE) as u64).await
    });
    tokio::time::timeout(Duration::from_secs(10), started_rx)
        .await
        .expect("second frame was reached in time")
        .expect("second frame space query blocked");
    request.abort();
    assert!(request.await.expect_err("request cancelled").is_cancelled());
    let observer_budget = Arc::clone(&budget);
    let reserved = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || observer_budget.state.lock().reserved),
    )
    .await
    .expect("a blocked filesystem query must not hold the budget lock")
    .expect("observe reserved bytes");
    assert_eq!(reserved, REPLAY_CHUNK_SIZE as u64 + FRAME_OVERHEAD);
    assert!(acquire_staging_slot(&slots).is_err(), "the in-flight write owns the slot");
    release_tx.send(()).expect("release the blocking write");
    let permit = tokio::time::timeout(Duration::from_secs(5), Arc::clone(&slots).acquire_owned())
        .await
        .expect("slot returned")
        .expect("slot remains open");
    assert_eq!(budget.state.lock().reserved, 0, "budget returns after the file closes");
    drop(permit);
}

#[tokio::test]
async fn post_staging_replay_preserves_bytes_across_chunks_and_empty_files() {
    for bytes in [Vec::new(), vec![42; REPLAY_CHUNK_SIZE + 37]] {
        let (_, file) = fixture(tempfile::tempfile().expect("temporary file"));
        let expected = bytes.clone();
        let length = bytes.len() as u64;
        let source = bytes.clone();
        let input = Box::new(stream::iter(
            (0..source.len())
                .step_by(137)
                .map(move |start| Ok(Bytes::copy_from_slice(&source[start..source.len().min(start + 137)]))),
        ));
        assert_eq!(copy_post_file(&file, input, length).await.unwrap(), length as i64);
        let disk = contents(&file).await;
        if !bytes.is_empty() {
            assert!(
                !disk.windows(32).any(|window| window == &bytes[..32]),
                "plaintext must not appear on disk"
            );
        }
        rewind(&file).await;
        let (mut chunks, _) = BodyChunks::with_trailers(replay_post_file(file, length), None);
        let mut actual = Vec::new();
        while let Some(chunk) = chunks.next().await {
            actual.extend_from_slice(&chunk.expect("read replay chunk"));
        }
        assert_eq!(actual, expected);
    }
}

#[tokio::test]
async fn post_staging_replay_bytes_survive_stream_drop_through_their_owner() {
    let (_, file) = fixture(tempfile::tempfile().expect("temporary file"));
    let chunks = Box::new(stream::iter([Ok(Bytes::from_static(b"hello"))]));
    assert_eq!(copy_post_file(&file, chunks, 5).await.unwrap(), 5);
    rewind(&file).await;
    let (mut replay, _) = BodyChunks::with_trailers(replay_post_file(file, 5), None);
    let bytes = replay.next().await.unwrap().unwrap();
    let slice = bytes.slice(1..);
    drop(bytes);
    drop(replay);
    assert_eq!(slice.as_ref(), b"ello");
}

#[test]
fn post_staging_zeroizing_owner_is_released_after_last_bytes_reference() {
    use std::sync::atomic::AtomicBool;

    struct Owner {
        bytes: [u8; 5],
        zeroized: Arc<AtomicBool>,
    }

    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl zeroize::Zeroize for Owner {
        fn zeroize(&mut self) {
            self.bytes.zeroize();
            self.zeroized.store(self.bytes == [0; 5], Ordering::SeqCst);
        }
    }

    let zeroized = Arc::new(AtomicBool::new(false));
    let bytes = Bytes::from_owner(Zeroizing::new(Owner {
        bytes: *b"hello",
        zeroized: Arc::clone(&zeroized),
    }));
    let clone = bytes.clone();
    let slice = bytes.slice(1..);
    drop(bytes);
    drop(clone);
    assert!(!zeroized.load(Ordering::SeqCst), "a live slice retains the owner");
    assert_eq!(slice.as_ref(), b"ello");
    drop(slice);
    assert!(zeroized.load(Ordering::SeqCst), "the last reference zeroizes the owner");
}

#[tokio::test]
async fn post_staging_uses_distinct_frame_nonces_and_upload_keys() {
    let plaintext = vec![42; 2 * REPLAY_CHUNK_SIZE];
    let mut ciphertexts = Vec::new();
    for _ in 0..2 {
        let (_, file) = fixture(tempfile::tempfile().expect("temporary file"));
        let chunks = Box::new(stream::iter([Ok(Bytes::copy_from_slice(&plaintext))]));
        copy_post_file(&file, chunks, plaintext.len() as u64).await.unwrap();
        let disk = contents(&file).await;
        let frame_size = 4 + REPLAY_CHUNK_SIZE + TAG_SIZE;
        assert_ne!(&disk[4..4 + REPLAY_CHUNK_SIZE], &disk[frame_size + 4..frame_size + 4 + REPLAY_CHUNK_SIZE]);
        ciphertexts.push(disk[4..4 + REPLAY_CHUNK_SIZE].to_vec());
    }
    assert_ne!(ciphertexts[0], ciphertexts[1]);
}

#[tokio::test]
async fn post_staging_replay_propagates_read_failure() {
    let path = tempfile::NamedTempFile::new().expect("temporary file");
    let writer = std::fs::OpenOptions::new()
        .write(true)
        .open(path.path())
        .expect("write-only file");
    let (_, file) = fixture(writer);
    let (mut chunks, _) = BodyChunks::with_trailers(replay_post_file(file, 1), None);
    assert!(chunks.next().await.expect("a failed read produces a result").is_err());
}

#[tokio::test]
async fn post_staging_replay_rejects_corruption_truncation_reordering_and_trailing_bytes() {
    let plaintext = [vec![13; REPLAY_CHUNK_SIZE], vec![29; REPLAY_CHUNK_SIZE]].concat();
    let frame_size = 4 + REPLAY_CHUNK_SIZE + TAG_SIZE;
    for corruption in [
        "first_tag",
        "last_tag",
        "header",
        "truncated_end",
        "truncated_header",
        "trailing",
        "swap",
    ] {
        let (_, file) = fixture(tempfile::tempfile().expect("temporary file"));
        let input = Box::new(stream::iter([Ok(Bytes::copy_from_slice(&plaintext))]));
        assert_eq!(
            copy_post_file(&file, input, plaintext.len() as u64).await.unwrap(),
            plaintext.len() as i64
        );
        let mut disk = contents(&file).await;
        let expected_prefix = match corruption {
            "first_tag" => {
                disk[frame_size - 1] ^= 1;
                0
            }
            "last_tag" => {
                disk[2 * frame_size - 1] ^= 1;
                REPLAY_CHUNK_SIZE
            }
            "header" => {
                disk[0] ^= 0x80;
                0
            }
            "truncated_end" => {
                disk.pop();
                REPLAY_CHUNK_SIZE
            }
            "truncated_header" => {
                disk.truncate(2);
                0
            }
            "trailing" => {
                disk.push(0);
                REPLAY_CHUNK_SIZE
            }
            "swap" => {
                let (first, rest) = disk.split_at_mut(frame_size);
                first.swap_with_slice(&mut rest[..frame_size]);
                0
            }
            _ => unreachable!(),
        };
        file_io(Arc::clone(&file), move |file| {
            file.seek(SeekFrom::Start(0))?;
            file.set_len(0)?;
            file.write_all(&disk)?;
            file.seek(SeekFrom::Start(0))?;
            Ok(())
        })
        .await
        .expect("corrupt staged file");
        let (mut replay, _) = BodyChunks::with_trailers(replay_post_file(file, plaintext.len() as u64), None);
        let mut actual = Vec::new();
        let mut failed = false;
        while let Some(chunk) = replay.next().await {
            match chunk {
                Ok(chunk) => actual.extend_from_slice(&chunk),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        assert!(failed, "{corruption} must fail");
        assert_eq!(
            actual,
            plaintext[..expected_prefix],
            "{corruption} must not release an unauthenticated frame"
        );
    }
}

#[test]
#[serial_test::serial]
fn post_staging_corruption_does_not_publish_or_replace_an_object() {
    use crate::app::RequestHead;
    use crate::app::object_usecase::{DefaultObjectUsecase, PutObjectWriteInput};
    use crate::app::storage_api::context::PostObjectRequestMarker;
    use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
    use crate::app::storage_api::test::contract::object::ObjectIO as _;
    use crate::app::storage_api::test::{StorageObjectOptions, StoragePutObjReader};
    use crate::runtime_sources::AppContext;
    use futures::StreamExt as _;
    use http::{Extensions, HeaderMap, Method, Uri};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::AsyncReadExt as _;

    const CHILD: &str = "RUSTFS_POST_STAGING_CORRUPTION_TEST_CHILD";
    const TEST: &str = "app::object::post_body::tests::post_staging_corruption_does_not_publish_or_replace_an_object";
    if std::env::var(CHILD).as_deref() != Ok("1") {
        // The eager PUT limits are cached process-wide, so isolate their initialization.
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", TEST, "--test-threads=1", "--color=never"])
            .env(CHILD, "1")
            .env("RUSTFS_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES", "0")
            .env("RUSTFS_SMALL_EAGER_PUT_MAX_SIZE_BYTES", "0")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost")
            .output()
            .expect("run isolated POST staging corruption test");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let summaries: Vec<_> = stdout.lines().filter(|line| line.starts_with("test result: ")).collect();
        assert!(
            output.status.success()
                && summaries.len() == 1
                && summaries[0].starts_with("test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured;"),
            "isolated POST staging corruption test failed:\n{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    temp_env::with_vars(
        [
            ("RUSTFS_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES", Some("0")),
            ("RUSTFS_SMALL_EAGER_PUT_MAX_SIZE_BYTES", Some("0")),
        ],
        || {
            crate::app::gating_test_env::run_large_stack_test("post-staging-corruption", || async {
                let store = crate::app::gating_test_env::shared_gating_ecstore().await;
                let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
                let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
                let usecase = DefaultObjectUsecase::with_context(Some(context));
                let bucket = format!("post-staging-corrupt-{}", uuid::Uuid::new_v4().simple());
                let options = StorageObjectOptions::default();
                store
                    .make_bucket(&bucket, &MakeBucketOptions::default())
                    .await
                    .expect("create bucket");
                store
                    .put_object(&bucket, "existing", &mut StoragePutObjReader::from_vec(b"original".to_vec()), &options)
                    .await
                    .expect("create existing object");

                for (key, corrupt) in [("existing", true), ("new", true), ("control", false)] {
                    let (_, file) = fixture(tempfile::tempfile().expect("temporary file"));
                    let bytes = vec![42; 17 * 1024 * 1024 + 37];
                    let length = i64::try_from(bytes.len()).expect("test length");
                    let size = u64::try_from(length).expect("nonnegative test length");
                    let chunks = Box::new(stream::iter([Ok(Bytes::from(bytes))]));
                    assert_eq!(copy_post_file(&file, chunks, size).await.expect("stage upload"), length);
                    if corrupt {
                        let mut disk = contents(&file).await;
                        *disk.last_mut().expect("authenticated end frame") ^= 1;
                        file_io(Arc::clone(&file), move |file| {
                            file.seek(SeekFrom::Start(0))?;
                            file.set_len(0)?;
                            file.write_all(&disk)?;
                            file.seek(SeekFrom::Start(0))?;
                            Ok(())
                        })
                        .await
                        .expect("corrupt end frame");
                    } else {
                        rewind(&file).await;
                    }
                    let mut extensions = Extensions::new();
                    extensions.insert(PostObjectRequestMarker);
                    let envelope = RequestEnvelope::from_parts(
                        RequestHead {
                            method: Method::POST,
                            uri: Uri::from_static("/bucket"),
                            headers: HeaderMap::new(),
                            credentials: None,
                            region: None,
                            service: None,
                        },
                        extensions,
                    );
                    let observed = Arc::new(AtomicBool::new(false));
                    let observed_failure = Arc::clone(&observed);
                    let (chunks, _) = BodyChunks::with_trailers(replay_post_file(file, size), None);
                    let chunks = chunks.map(move |result| {
                        result.map_err(|error| {
                            observed_failure.store(true, Ordering::SeqCst);
                            s3s_body_error_to_io(error)
                        })
                    });
                    let input = PutObjectWriteInput {
                        bucket: bucket.clone(),
                        key: key.to_owned(),
                        body: Some(from_io_stream(chunks)),
                        content_length: Some(length),
                        ..Default::default()
                    };
                    let result = Box::pin(usecase.execute_put_object_owned(envelope, input)).await;
                    if corrupt {
                        let Err(error) = result else { panic!("corrupted replay succeeded") };
                        assert_eq!(error.code(), &S3ErrorCode::InternalError);
                        assert!(observed.load(Ordering::SeqCst), "the storage path must observe the replay failure");
                    } else {
                        assert!(result.is_ok(), "an intact replay must succeed");
                        assert!(!observed.load(Ordering::SeqCst));
                    }
                }
                let mut reader = store
                    .get_object_reader(&bucket, "existing", None, HeaderMap::new(), &options)
                    .await
                    .expect("existing object remains readable");
                let mut actual = Vec::new();
                reader.stream.read_to_end(&mut actual).await.expect("read existing object");
                assert_eq!(actual, b"original");
                let error = match store
                    .get_object_reader(&bucket, "new", None, HeaderMap::new(), &options)
                    .await
                {
                    Err(error) => error,
                    Ok(_) => panic!("a corrupted new object was published"),
                };
                assert!(
                    crate::app::storage_api::object_usecase::error::is_err_object_not_found(&error),
                    "{error:?}"
                );
                let mut reader = store
                    .get_object_reader(&bucket, "control", None, HeaderMap::new(), &options)
                    .await
                    .expect("intact object is readable");
                let mut actual = Vec::new();
                reader.stream.read_to_end(&mut actual).await.expect("read intact object");
                assert_eq!(actual, vec![42; 17 * 1024 * 1024 + 37]);
            })
        },
    );
}

#[tokio::test]
async fn post_staging_empty_replay_requires_authenticated_end_and_eof() {
    let (_, valid) = fixture(tempfile::tempfile().expect("temporary file"));
    let chunks: UploadChunks = Box::new(stream::empty());
    assert_eq!(copy_post_file(&valid, chunks, 0).await.unwrap(), 0);
    rewind(&valid).await;
    verify_empty_staging(&valid).await.expect("valid empty file");
    let (mut replay, _) = BodyChunks::with_trailers(replay_post_file(valid, 0), None);
    assert!(replay.next().await.is_none());

    for corruption in ["tag", "truncate", "trailing"] {
        let (_, file) = fixture(tempfile::tempfile().expect("temporary file"));
        let chunks: UploadChunks = Box::new(stream::empty());
        assert_eq!(copy_post_file(&file, chunks, 0).await.unwrap(), 0);
        let mut disk = contents(&file).await;
        match corruption {
            "tag" => disk[4] ^= 1,
            "truncate" => {
                disk.pop();
            }
            "trailing" => disk.push(0),
            _ => unreachable!(),
        }
        file_io(Arc::clone(&file), move |file| {
            file.seek(SeekFrom::Start(0))?;
            file.set_len(0)?;
            file.write_all(&disk)?;
            file.seek(SeekFrom::Start(0))?;
            Ok(())
        })
        .await
        .expect("corrupt empty staged file");
        assert!(verify_empty_staging(&file).await.is_err(), "the no-poll gate must reject {corruption}");
        rewind(&file).await;
        let (mut replay, _) = BodyChunks::with_trailers(replay_post_file(file, 0), None);
        assert!(replay.next().await.expect("invalid empty replay fails").is_err());
    }
}

#[test]
fn post_staging_replay_retries_interrupted_reads() {
    struct InterruptedOnce(bool);
    impl Read for InterruptedOnce {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if !self.0 {
                self.0 = true;
                Err(io::Error::from(io::ErrorKind::Interrupted))
            } else {
                buffer[0] = b'x';
                Ok(1)
            }
        }
    }
    let mut reader = InterruptedOnce(false);
    let mut buffer = [0; 8];
    assert_eq!(read_chunk(&mut reader, &mut buffer).expect("retry interrupted read"), 1);
    assert_eq!(buffer[0], b'x');
}
