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

//! Streaming `AsyncRead` over ranged S3 GETs with bounded memory.

use crate::common::client::s3::StorageBackend;
use futures_lite::{AsyncRead, io::Result as AsyncIoResult};
use futures_util::StreamExt;
use rustfs_credentials::Credentials;
use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::timeout;
use tracing::warn;

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_TFTP_READER: &str = "tftp_reader";
const EVENT_TFTP_READ_STATE: &str = "tftp_read_state";

type FetchFuture = Pin<Box<dyn std::future::Future<Output = std::io::Result<Vec<u8>>> + Send>>;

/// Bounded-memory reader for TFTP RRQ transfers.
pub struct ObjectReader<S: StorageBackend + Send + Sync + 'static> {
    storage: Arc<S>,
    bucket: String,
    key: String,
    credentials: Credentials,
    object_size: u64,
    offset: u64,
    fetch_bytes: u64,
    backend_timeout: Duration,
    buffer: Vec<u8>,
    buffer_pos: usize,
    fetch_future: Option<FetchFuture>,
    _permit: OwnedSemaphorePermit,
}

impl<S: StorageBackend + Send + Sync + 'static> ObjectReader<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: Arc<S>,
        bucket: String,
        key: String,
        credentials: Credentials,
        object_size: u64,
        fetch_bytes: u64,
        backend_timeout: Duration,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            storage,
            bucket,
            key,
            credentials,
            object_size,
            offset: 0,
            fetch_bytes,
            backend_timeout,
            buffer: Vec::new(),
            buffer_pos: 0,
            fetch_future: None,
            _permit: permit,
        }
    }

    fn start_fetch(&mut self) {
        if self.offset >= self.object_size {
            return;
        }
        let remaining = self.object_size - self.offset;
        let fetch_len = remaining.min(self.fetch_bytes);
        let storage = Arc::clone(&self.storage);
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let credentials = self.credentials.clone();
        let offset = self.offset;
        let backend_timeout = self.backend_timeout;

        self.fetch_future = Some(Box::pin(async move {
            let fut = storage.get_object_range(&bucket, &key, &credentials, offset, fetch_len);
            let out = timeout(backend_timeout, fut)
                .await
                .map_err(|_| Error::new(ErrorKind::TimedOut, "get_object_range timed out"))?
                .map_err(Error::other)?;

            let Some(mut body) = out.body else {
                return Ok(Vec::new());
            };

            let mut buf = Vec::with_capacity(usize::try_from(fetch_len).unwrap_or(0));
            loop {
                let chunk = timeout(backend_timeout, body.next())
                    .await
                    .map_err(|_| Error::new(ErrorKind::TimedOut, "get_object stream timed out"))?
                    .transpose()
                    .map_err(Error::other)?;
                let Some(chunk) = chunk else { break };
                buf.extend_from_slice(&chunk);
            }
            Ok(buf)
        }));
    }
}

impl<S: StorageBackend + Send + Sync + 'static> AsyncRead for ObjectReader<S> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<AsyncIoResult<usize>> {
        if self.offset >= self.object_size {
            return Poll::Ready(Ok(0));
        }

        if self.buffer_pos >= self.buffer.len() {
            if self.fetch_future.is_none() {
                self.start_fetch();
            }

            if let Some(mut fut) = self.fetch_future.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(data)) => {
                        self.buffer = data;
                        self.buffer_pos = 0;
                        if self.buffer.is_empty() && self.offset < self.object_size {
                            warn!(
                                event = EVENT_TFTP_READ_STATE,
                                component = LOG_COMPONENT_PROTOCOLS,
                                subsystem = LOG_SUBSYSTEM_TFTP_READER,
                                bucket = %self.bucket,
                                key = %self.key,
                                offset = self.offset,
                                result = "unexpected_eof",
                                "tftp read state changed"
                            );
                            return Poll::Ready(Err(ErrorKind::UnexpectedEof.into()));
                        }
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => {
                        self.fetch_future = Some(fut);
                        return Poll::Pending;
                    }
                }
            }
        }

        let available = self.buffer.len().saturating_sub(self.buffer_pos);
        if available == 0 {
            return Poll::Ready(Ok(0));
        }

        let to_copy = available.min(buf.len());
        buf[..to_copy].copy_from_slice(&self.buffer[self.buffer_pos..self.buffer_pos + to_copy]);
        self.buffer_pos += to_copy;
        self.offset += to_copy as u64;
        Poll::Ready(Ok(to_copy))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{build_reader, capture_tracing_at, read_chunk, read_to_end, test_permit};
    use crate::common::dummy_storage::{DummyBackend, DummyError};
    use futures_lite::AsyncReadExt;
    use std::io::ErrorKind;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tracing::Level;

    #[tokio::test]
    async fn read_returns_full_object() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_get_object_range_bytes(b"hello".to_vec());
        let mut reader = build_reader(Arc::clone(&backend), 5, 1024, 60, test_permit());
        let data = read_to_end(&mut reader).await.expect("read must succeed");
        assert_eq!(data, b"hello");
    }

    #[tokio::test]
    async fn read_at_eof_returns_zero_without_extra_backend_call() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_get_object_range_bytes(b"hello".to_vec());
        let mut reader = build_reader(Arc::clone(&backend), 5, 1024, 60, test_permit());
        let data = read_to_end(&mut reader).await.expect("first read must succeed");
        assert_eq!(data, b"hello");

        let mut tail = vec![0_u8; 8];
        let n = reader.read(&mut tail).await.expect("eof read must succeed");
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn read_crosses_fetch_window_triggers_second_backend_call() {
        let fetch_bytes: u64 = 64;
        let object_size: u64 = fetch_bytes * 2;
        let first_window = vec![0xAA_u8; fetch_bytes as usize];
        let second_window = vec![0xBB_u8; fetch_bytes as usize];

        let backend = Arc::new(DummyBackend::new());
        backend.queue_get_object_range_bytes(first_window);
        backend.queue_get_object_range_bytes(second_window.clone());

        let mut reader = build_reader(Arc::clone(&backend), object_size, fetch_bytes, 60, test_permit());
        let data = read_to_end(&mut reader).await.expect("read across windows must succeed");
        assert_eq!(data.len(), object_size as usize);
        assert!(data[..fetch_bytes as usize].iter().all(|b| *b == 0xAA));
        assert!(data[fetch_bytes as usize..].iter().all(|b| *b == 0xBB));
    }

    #[tokio::test]
    async fn unexpected_empty_range_returns_unexpected_eof() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_get_object_range_bytes(Vec::new());

        let mut reader = build_reader(backend, 16, 1024, 60, test_permit());
        let (result, captured) = capture_tracing_at(Level::WARN, read_to_end(&mut reader)).await;
        let err = result.expect_err("empty range before eof must fail");
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
        assert!(
            captured.contains("unexpected_eof"),
            "empty range must emit unexpected_eof warn, captured: {captured}"
        );
    }

    #[tokio::test]
    async fn get_object_range_backend_error_propagates() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_get_object_range_err(DummyError::Injected("backend exploded".into()));

        let mut reader = build_reader(backend, 16, 1024, 60, test_permit());
        let err = read_to_end(&mut reader).await.expect_err("backend error must propagate");
        assert_ne!(err.kind(), ErrorKind::UnexpectedEof);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn chunk_stall_times_out_within_deadline() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_get_object_range_stalling_after_chunk(b"prefix".to_vec(), 4096);

        let timeout_secs: u64 = 1;
        let mut reader = build_reader(Arc::clone(&backend), 4096, 4096, timeout_secs, test_permit());

        let start = Instant::now();
        let outcome = tokio::time::timeout(Duration::from_secs(10), read_to_end(&mut reader)).await;
        let elapsed = start.elapsed();

        let inner = outcome.expect("per-chunk deadline must fire before the outer guard");
        let err = inner.expect_err("stalled body must surface as Err");
        assert_eq!(err.kind(), ErrorKind::TimedOut);
        assert!(
            elapsed < Duration::from_secs(timeout_secs + 4),
            "stalled body must time out within {} s, elapsed: {:?}",
            timeout_secs + 4,
            elapsed,
        );
    }

    #[tokio::test]
    async fn sequential_small_poll_reads_reuse_one_fetch() {
        let fetch_bytes: u64 = 1024;
        let payload: Vec<u8> = (0..fetch_bytes).map(|i| i as u8).collect();

        let backend = Arc::new(DummyBackend::new());
        backend.queue_get_object_range_bytes(payload.clone());

        let mut reader = build_reader(Arc::clone(&backend), fetch_bytes, fetch_bytes, 60, test_permit());
        let mut assembled = Vec::with_capacity(fetch_bytes as usize);
        for _ in 0..4 {
            let chunk = read_chunk(&mut reader, 256).await.expect("chunk read must succeed");
            assert!(!chunk.is_empty(), "chunk must carry bytes");
            assembled.extend_from_slice(&chunk);
        }
        assert_eq!(assembled, payload);
    }

    #[tokio::test]
    async fn fetch_len_clamped_to_remaining_bytes() {
        let object_size: u64 = 100;
        let payload = vec![0x42_u8; object_size as usize];

        let backend = Arc::new(DummyBackend::new());
        backend.queue_get_object_range_bytes(payload.clone());

        let mut reader = build_reader(Arc::clone(&backend), object_size, 4 * 1024 * 1024, 60, test_permit());
        let data = read_to_end(&mut reader).await.expect("single fetch must satisfy the object");
        assert_eq!(data, payload);
    }
}
