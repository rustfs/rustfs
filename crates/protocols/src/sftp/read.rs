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

//! Read-side operation handlers: open_read and the body of the read()
//! Handler trait method.

use super::attrs::{apply_user_metadata_to_sftp_attrs, s3_attrs_to_sftp, timestamp_to_mtime};
use super::constants::limits::{MAX_READ_LEN, READ_CACHE_DISABLED};
use super::driver::SftpDriver;
use super::errors::{SftpError, s3_error_to_sftp};
use super::paths::parse_s3_path;
use super::state::HandleState;
use crate::common::client::s3::StorageBackend;
use crate::common::gateway::S3Action;
use futures_util::StreamExt;
use russh_sftp::protocol::{Data, Handle, StatusCode};

impl<S: StorageBackend + Send + Sync + 'static> SftpDriver<S> {
    /// Read-side OPEN: authorise GetObject, HEAD the object to capture
    /// size and mtime, allocate a File handle. Errors are mapped through
    /// s3_error_to_sftp so a missing object returns NoSuchFile and a
    /// permission failure as PermissionDenied.
    pub(super) async fn open_read(&mut self, id: u32, filename: &str) -> Result<Handle, SftpError> {
        let (bucket, key) = parse_s3_path(filename)?;
        let Some(object_key) = key else {
            return Err(SftpError::code(StatusCode::NoSuchFile));
        };
        if bucket.is_empty() {
            return Err(SftpError::code(StatusCode::NoSuchFile));
        }

        self.authorize(&S3Action::GetObject, &bucket, Some(&object_key)).await?;

        // Fetch object metadata (size, last-modified) without downloading
        // the body. These are cached on the handle so READ can detect EOF
        // and FSTAT can answer without another backend call.
        let head = self
            .run_backend(
                "head_object",
                self.storage
                    .head_object(&bucket, &object_key, self.access_key(), self.secret_key()),
            )
            .await?;
        let size = head.content_length.unwrap_or(0).max(0) as u64;
        let mtime = timestamp_to_mtime(head.last_modified);
        let mut attrs = s3_attrs_to_sftp(size, mtime, false);
        if let Some(metadata) = head.metadata {
            apply_user_metadata_to_sftp_attrs(&mut attrs, &metadata);
        }

        let read_cache = self.new_read_cache();
        let handle = self.allocate_handle(HandleState::File {
            bucket,
            key: object_key,
            size,
            attrs,
            read_cache,
        })?;
        Ok(Handle { id, handle })
    }

    /// Body of the SSH_FXP_READ handler. Returns up to len bytes starting
    /// at offset, capped at MAX_READ_LEN and the cached object size.
    /// Zero-length requests are rejected with BadMessage at the boundary.
    /// Offsets at or past end-of-file return Eof without a network call.
    ///
    /// Cache-aware. When the requested bytes are already in the
    /// per-handle cached chunk, they are returned without a backend
    /// round trip. Otherwise a window-sized range is fetched from the
    /// backend, the cache is populated when the new chunk would not
    /// push the process-wide memory total past the configured
    /// ceiling, and the requested bytes are returned from the fetched
    /// data. When the populate call is skipped due to the memory
    /// ceiling, the read still completes from the fetched bytes.
    /// Only the caching step is dropped, at the cost of one backend
    /// call per FXP_READ.
    ///
    /// When read_cache_window is set to READ_CACHE_DISABLED the cache
    /// is bypassed entirely. The cache-hit probe always misses
    /// because the buffer is never populated, the fetch length equals
    /// the requested length, and try_populate_read_cache returns
    /// early without touching the process-wide accumulator.
    pub(super) async fn read_inner(&mut self, id: u32, handle: String, offset: u64, len: u32) -> Result<Data, SftpError> {
        if len == 0 {
            // Reject zero-length reads at the boundary. The S3 range header
            // would otherwise underflow when calculating the inclusive end
            // offset.
            return Err(SftpError::code(StatusCode::BadMessage));
        }
        // Cap the client-requested length to MAX_READ_LEN (256 KiB) to
        // bound the per-request memory allocation.
        let capped_len = len.min(MAX_READ_LEN);

        let (bucket, key, size) = self.with_handle_ref(&handle, |state| match state {
            HandleState::File { bucket, key, size, .. } => Ok((bucket.clone(), key.clone(), *size)),
            HandleState::Dir(_) | HandleState::Write { .. } => Err(SftpError::code(StatusCode::Failure)),
        })?;

        // Reading at or past EOF returns Eof without a backend call.
        // Clamp the read length to the remaining bytes.
        if offset >= size {
            return Err(SftpError::code(StatusCode::Eof));
        }
        let remaining = size - offset;
        let actual_len = (capped_len as u64).min(remaining);

        // Cache-hit fast path. Probe the cache while only borrowing
        // the handle table. No backend call, no auth call, no await,
        // so cancellation cannot fire between the probe and the
        // return.
        let cached = self.with_handle_ref(&handle, |state| match state {
            HandleState::File { read_cache, .. } => Ok(read_cache.get(offset, actual_len).map(|s| s.to_vec())),
            _ => Err(SftpError::code(StatusCode::Failure)),
        })?;
        if let Some(data) = cached {
            return Ok(Data { id, data });
        }

        // Cache miss. Authorise and fetch a window-sized range. The
        // fetch length is normally read_cache_window. Near EOF it
        // shrinks to the remaining bytes so a tail read does not
        // over-fetch past the object. The fetch length is also held
        // at or above actual_len so that when read_cache_window is
        // smaller than actual_len, or when read_cache_window is the
        // READ_CACHE_DISABLED sentinel (0), the backend call still
        // returns the bytes the client requested.
        self.authorize(&S3Action::GetObject, &bucket, Some(&key)).await?;
        let fetch_len = self.read_cache_window.max(actual_len).min(remaining);

        let window_bytes = self.fetch_object_range(&bucket, &key, offset, fetch_len).await?;

        if window_bytes.is_empty() {
            return Err(SftpError::code(StatusCode::Eof));
        }

        // Slice the response from the front of the fetched bytes.
        // The remainder is offered to the cache below for reuse on
        // subsequent reads inside the same chunk.
        let response_len = actual_len.min(window_bytes.len() as u64) as usize;
        let data = window_bytes[..response_len].to_vec();

        self.try_populate_read_cache(&handle, offset, window_bytes);

        Ok(Data { id, data })
    }

    /// Issue one get_object_range backend call and drain the response
    /// body into a contiguous buffer. Each per-chunk await is wrapped
    /// in the same per-call deadline that bounds the outer
    /// get_object_range. A backend that returns a body and then stalls
    /// mid-stream returns Failure here rather than pinning the session
    /// task on body.next().
    async fn fetch_object_range(&self, bucket: &str, key: &str, offset: u64, fetch_len: u64) -> Result<Vec<u8>, SftpError> {
        let out = self
            .run_backend(
                "get_object_range",
                self.storage
                    .get_object_range(bucket, key, self.access_key(), self.secret_key(), offset, fetch_len),
            )
            .await?;

        let Some(mut body) = out.body else {
            return Err(SftpError::code(StatusCode::Failure));
        };

        let mut buf = Vec::with_capacity(usize::try_from(fetch_len).unwrap_or(0));
        loop {
            let chunk_timeout = std::time::Duration::from_secs(self.backend_op_timeout_secs);
            let next = match tokio::time::timeout(chunk_timeout, body.next()).await {
                Ok(next) => next,
                Err(_elapsed) => {
                    return Err(s3_error_to_sftp(
                        "get_object_stream",
                        format!("stream chunk timed out after {} seconds", self.backend_op_timeout_secs),
                    ));
                }
            };
            let Some(chunk) = next else { break };
            let bytes = chunk.map_err(|e| s3_error_to_sftp("get_object_stream", e))?;
            buf.extend_from_slice(&bytes);
        }
        Ok(buf)
    }

    /// Populate the per-handle read cache when the projected total
    /// memory across all live caches would stay at or below the
    /// configured ceiling. The check is a best-effort peek-then-add.
    /// Under concurrent populate calls from many sessions the
    /// projected total can briefly drift above the limit by at most
    /// (concurrent_populates * window_bytes). The limit is a soft
    /// cap. When the projected total exceeds the limit, the bytes
    /// are dropped without storing them, and a subsequent FXP_READ
    /// inside the same chunk-aligned range issues a fresh backend
    /// call instead of being served from cache.
    ///
    /// The accumulator load and the populate call run with no
    /// intervening await, so the snapshot is still valid when the
    /// populate call executes.
    fn try_populate_read_cache(&mut self, handle: &str, offset: u64, window_bytes: Vec<u8>) {
        if self.read_cache_window == READ_CACHE_DISABLED {
            return;
        }
        let cap_now = self.read_cache_in_use.load(std::sync::atomic::Ordering::Relaxed);
        let cache_state = match self.handles.get(handle) {
            Some(HandleState::File { read_cache, .. }) => read_cache.capacity() as u64,
            _ => return,
        };
        let new_cap = window_bytes.capacity() as u64;
        let projected = cap_now.saturating_sub(cache_state).saturating_add(new_cap);
        if projected > self.read_cache_total_mem_limit {
            return;
        }
        if let Some(state) = self.handles.get_mut(handle)
            && let HandleState::File { read_cache, .. } = state
        {
            read_cache.populate(offset, window_bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::constants::limits::READ_CACHE_DISABLED;
    use super::super::state::HandleState;
    use super::super::test_support::{
        TEST_PART_SIZE, build_driver, build_driver_with_read_cache, build_driver_with_timeout, capture_tracing_at, file_handle,
    };
    use crate::common::dummy_storage::{DummyBackend, DummyError};
    use crate::common::gateway::with_test_auth_override;
    use russh_sftp::protocol::{FileAttributes, StatusCode};
    use russh_sftp::server::Handler;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tracing::Level;

    #[tokio::test]
    async fn read_with_len_zero_returns_bad_message_before_backend_call() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend, TEST_PART_SIZE);
        let handle_id = driver
            .allocate_handle(file_handle("b", "k", 100, FileAttributes::default()))
            .expect("allocate");
        let err = driver
            .read(1, handle_id, 0, 0)
            .await
            .expect_err("len=0 must return BadMessage");
        assert!(matches!(StatusCode::from(err), StatusCode::BadMessage));
    }

    #[tokio::test]
    async fn read_at_offset_past_size_returns_eof_before_backend_call() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend.clone(), TEST_PART_SIZE);
        let handle_id = driver
            .allocate_handle(file_handle("b", "k", 10, FileAttributes::default()))
            .expect("allocate");
        let err = driver
            .read(2, handle_id, 10, 4)
            .await
            .expect_err("offset==size must return Eof");
        assert!(matches!(StatusCode::from(err), StatusCode::Eof));
    }

    #[tokio::test]
    async fn read_normal_path_returns_bytes_from_backend() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_get_object_range_bytes(b"hello".to_vec());
        let mut driver = build_driver(backend, TEST_PART_SIZE);
        let handle_id = driver
            .allocate_handle(file_handle("b", "k", 5, FileAttributes::default()))
            .expect("allocate");

        let data = with_test_auth_override(|_, _, _| true, driver.read(3, handle_id, 0, 1024))
            .await
            .expect("read must succeed");
        assert_eq!(data.data, b"hello".to_vec());
    }

    /// Read past end-of-file is the spec-mandated SFTP termination
    /// signal. The handler must return Eof on the wire and stay silent
    /// in the log so a normal download burst does not generate one
    /// error-level event per file.
    #[tokio::test]
    async fn read_past_eof_emits_no_error_level_event() {
        let backend = Arc::new(DummyBackend::new());
        let mut driver = build_driver(backend, TEST_PART_SIZE);
        let handle_id = driver
            .allocate_handle(file_handle("b", "k", 10, FileAttributes::default()))
            .expect("allocate");

        let (result, captured) = capture_tracing_at(Level::ERROR, async { driver.read(11, handle_id, 10, 4).await }).await;
        let err = result.expect_err("offset==size must return Eof");
        assert!(matches!(StatusCode::from(err), StatusCode::Eof));
        assert!(
            !captured.contains("ERROR"),
            "Eof return must not produce an error-level event, captured: {captured}"
        );
        assert!(
            !captured.contains("SFTP READ failed"),
            "Eof return must not log SFTP READ failed, captured: {captured}"
        );
    }

    /// A non-Eof failure on the read path is operator-visible. The
    /// assertion below confirms a backend error produces an
    /// error-level event.
    #[tokio::test]
    async fn read_backend_failure_emits_error_level_event() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_get_object_range_err(DummyError::Injected("backend exploded".into()));
        let mut driver = build_driver(Arc::clone(&backend), TEST_PART_SIZE);
        let handle_id = driver
            .allocate_handle(file_handle("b", "k", 1024, FileAttributes::default()))
            .expect("allocate");

        let (result, captured) =
            capture_tracing_at(Level::ERROR, with_test_auth_override(|_, _, _| true, driver.read(12, handle_id, 0, 256))).await;
        let err = result.expect_err("backend error must propagate as Err");
        assert!(!matches!(StatusCode::from(err), StatusCode::Eof), "backend error must not be Eof");
        assert!(
            captured.contains("ERROR"),
            "non-Eof backend failure must produce an error-level event, captured: {captured}"
        );
        assert!(
            captured.contains("SFTP READ failed"),
            "error-level event must carry the SFTP READ failed message, captured: {captured}"
        );
    }

    /// run_backend wraps the outer get_object_range call in the per-call
    /// deadline, but the body iteration inside read_inner is a separate
    /// stream of awaits. A backend that returns the body and then stalls
    /// mid-stream pins the session task on body.next() until something
    /// else closes the connection. The per-chunk timeout closes that gap.
    /// This test queues a body that emits one chunk and stalls forever
    /// on the next .next() poll, runs read with a 1 s backend deadline,
    /// and asserts that the call returns Failure within the deadline plus
    /// a generous buffer rather than waiting on the outer 10 s guard.
    #[tokio::test(flavor = "current_thread")]
    async fn read_chunk_stall_returns_failure_within_deadline() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_get_object_range_stalling_after_chunk(b"prefix".to_vec(), 4096);

        let timeout_secs: u64 = 1;
        let mut driver = build_driver_with_timeout(Arc::clone(&backend), TEST_PART_SIZE, timeout_secs);
        let handle_id = driver
            .allocate_handle(file_handle("b", "k", 4096, FileAttributes::default()))
            .expect("allocate");

        let start = Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            with_test_auth_override(|_, _, _| true, driver.read(14, handle_id, 0, 4096)),
        )
        .await;
        let elapsed = start.elapsed();

        let inner = outcome.expect("per-chunk deadline must fire before the outer 10 s guard");
        let err = inner.expect_err("stalled body must surface as Err");
        assert!(
            !matches!(StatusCode::from(err), StatusCode::Eof),
            "stalled body must not be reported as Eof"
        );
        assert!(
            elapsed < Duration::from_secs(timeout_secs + 4),
            "stalled body must time out within {} s, elapsed: {:?}",
            timeout_secs + 4,
            elapsed,
        );
    }

    /// Sequential reads on the same handle are served from the cache
    /// after the first miss. The DummyBackend queues exactly one
    /// get_object_range response sized to the configured window. With
    /// the cache wired the driver consumes that one response on the
    /// first read. Subsequent reads inside the cached chunk are
    /// returned from the buffer without a second backend call. The
    /// queue is empty after the first response, so any second backend
    /// call would return NoSuchKey and fail the test.
    #[tokio::test]
    async fn sequential_reads_cache_hit_after_first_miss() {
        let window: u64 = 64 * 1024;
        let object_size: u64 = window;
        let payload: Vec<u8> = (0..object_size as usize).map(|i| i as u8).collect();

        let backend = Arc::new(DummyBackend::new());
        backend.queue_get_object_range_bytes(payload.clone());

        let mut driver = build_driver_with_read_cache(Arc::clone(&backend), TEST_PART_SIZE, window, 1024 * 1024 * 1024);
        let handle_id = driver
            .allocate_handle(file_handle("b", "k", object_size, FileAttributes::default()))
            .expect("allocate");

        let chunk: u32 = 8 * 1024;
        let mut offset: u64 = 0;
        let mut assembled: Vec<u8> = Vec::with_capacity(object_size as usize);
        let mut reads: u32 = 0;
        while offset < object_size {
            let data = with_test_auth_override(|_, _, _| true, driver.read(20 + reads, handle_id.clone(), offset, chunk))
                .await
                .expect("read inside the cached window must succeed without a second backend call");
            assert!(!data.data.is_empty(), "non-empty hit");
            assembled.extend_from_slice(&data.data);
            offset += data.data.len() as u64;
            reads += 1;
            assert!(reads < 100, "loop guard: reads must terminate inside the window");
        }
        assert_eq!(assembled, payload, "assembled bytes must match seed");
        assert!(reads > 1, "test must drive more than one FXP_READ to exercise the cache");
    }

    /// A read sequence that crosses two windows triggers exactly two
    /// backend calls. Two responses sized to the window are queued.
    /// Reads within window 1 are served from the buffer after the
    /// miss that fetched it. The boundary read at offset == window
    /// falls outside the cached chunk and triggers a second backend
    /// call to fetch window 2.
    #[tokio::test]
    async fn read_crossing_two_windows_triggers_two_backend_calls() {
        let window: u64 = 64 * 1024;
        let object_size: u64 = window * 2;
        let first_window: Vec<u8> = vec![0xAA_u8; window as usize];
        let second_window: Vec<u8> = vec![0xBB_u8; window as usize];

        let backend = Arc::new(DummyBackend::new());
        backend.queue_get_object_range_bytes(first_window.clone());
        backend.queue_get_object_range_bytes(second_window.clone());

        let mut driver = build_driver_with_read_cache(Arc::clone(&backend), TEST_PART_SIZE, window, 1024 * 1024 * 1024);
        let handle_id = driver
            .allocate_handle(file_handle("b", "k", object_size, FileAttributes::default()))
            .expect("allocate");

        // First read fetches from the backend and populates window 1.
        let r1 = with_test_auth_override(|_, _, _| true, driver.read(30, handle_id.clone(), 0, 1024))
            .await
            .expect("first read must succeed");
        assert!(r1.data.iter().all(|b| *b == 0xAA), "first read must come from window 1");

        // Second read inside the cached chunk is served from the
        // buffer. No second backend call yet.
        let r2 = with_test_auth_override(|_, _, _| true, driver.read(31, handle_id.clone(), 1024, 1024))
            .await
            .expect("mid-window read must succeed from cache");
        assert!(r2.data.iter().all(|b| *b == 0xAA), "mid-window read still in window 1");

        // Reading at offset == window falls outside the cached chunk
        // and triggers the second backend call.
        let r3 = with_test_auth_override(|_, _, _| true, driver.read(32, handle_id.clone(), window, 1024))
            .await
            .expect("read at offset=window must succeed via second backend call");
        assert!(r3.data.iter().all(|b| *b == 0xBB), "read at window boundary must come from window 2");

        // A read inside the second cached chunk is served from the
        // buffer. The queue is empty by now, so any third backend
        // call would fail.
        let r4 = with_test_auth_override(|_, _, _| true, driver.read(33, handle_id, window + 1024, 1024))
            .await
            .expect("mid-window-2 read must succeed from cache");
        assert!(r4.data.iter().all(|b| *b == 0xBB), "mid-window-2 read still in window 2");
    }

    /// A partial-hit FXP_READ at the window edge returns only the
    /// portion of the requested range that sits inside the cached
    /// chunk. The driver must not issue a backend call to make up
    /// the rest of the requested length on the same FXP_READ. The
    /// next FXP_READ from the client triggers the refresh.
    #[tokio::test]
    async fn partial_window_edge_hit_returns_short_read() {
        let window: u64 = 1024;
        let object_size: u64 = window * 2;
        let first_window: Vec<u8> = vec![0xCC_u8; window as usize];
        let second_window: Vec<u8> = vec![0xDD_u8; window as usize];

        let backend = Arc::new(DummyBackend::new());
        backend.queue_get_object_range_bytes(first_window);
        backend.queue_get_object_range_bytes(second_window);

        let mut driver = build_driver_with_read_cache(Arc::clone(&backend), TEST_PART_SIZE, window, 1024 * 1024 * 1024);
        let handle_id = driver
            .allocate_handle(file_handle("b", "k", object_size, FileAttributes::default()))
            .expect("allocate");

        // Populate window 1 with a full read.
        let _ = with_test_auth_override(|_, _, _| true, driver.read(40, handle_id.clone(), 0, window as u32))
            .await
            .expect("populate window 1");

        // Ask for 256 bytes starting 64 bytes before window end. Only
        // 64 bytes are in the window. The driver must return 64.
        let edge = with_test_auth_override(|_, _, _| true, driver.read(41, handle_id, window - 64, 256))
            .await
            .expect("partial-hit read must succeed");
        assert_eq!(edge.data.len(), 64, "partial hit must return only the in-window portion");
        assert!(edge.data.iter().all(|b| *b == 0xCC), "partial hit bytes must come from window 1");
    }

    /// With READ_CACHE_DISABLED set as the window value the cache is
    /// bypassed entirely. Each FXP_READ must hit the backend, and no
    /// buffer is retained between reads. Verified by queueing one
    /// backend response per expected FXP_READ; if any read short-
    /// circuited via the cache the queue would still hold a response
    /// at the end, and a subsequent read would return an extra
    /// backend payload. A separate assertion confirms the per-handle
    /// ReadCache buf stays at zero capacity across the read sequence.
    #[tokio::test]
    async fn read_cache_disabled_hits_backend_on_every_read() {
        let chunk_size: usize = 4 * 1024;
        let read_count: u32 = 5;
        let object_size: u64 = (chunk_size as u64) * (read_count as u64);

        let backend = Arc::new(DummyBackend::new());
        for i in 0..read_count {
            let payload = vec![(i + 1) as u8; chunk_size];
            backend.queue_get_object_range_bytes(payload);
        }

        let mut driver =
            build_driver_with_read_cache(Arc::clone(&backend), TEST_PART_SIZE, READ_CACHE_DISABLED, 1024 * 1024 * 1024);
        let handle_id = driver
            .allocate_handle(file_handle("b", "k", object_size, FileAttributes::default()))
            .expect("allocate");

        for i in 0..read_count {
            let offset = (chunk_size as u64) * (i as u64);
            let data = with_test_auth_override(|_, _, _| true, driver.read(50 + i, handle_id.clone(), offset, chunk_size as u32))
                .await
                .expect("each read must succeed via the backend");
            assert_eq!(data.data.len(), chunk_size, "read must return full requested length");
            let expected_byte = (i + 1) as u8;
            assert!(
                data.data.iter().all(|b| *b == expected_byte),
                "read {i} payload must come from the i-th queued backend response"
            );
            let cap = driver.with_handle_ref(&handle_id, |state| match state {
                HandleState::File { read_cache, .. } => Ok(read_cache.capacity()),
                _ => Ok(usize::MAX),
            });
            assert_eq!(cap.expect("handle present"), 0, "ReadCache buf must stay empty when disabled");
        }
    }
}
