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

use crate::common::{
    client::s3::StorageBackend,
    gateway::{S3Action, authorize_operation},
    session::SessionContext,
};
use bytes::Bytes;
use s3s::dto::*;
use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::Semaphore,
};
use unftp_core::storage::{Error, ErrorKind, Result};

// One in-flight part per upload; never buffer the next part while the backend consumes this one.
const PART_SIZE: usize = 16 * 1024 * 1024;
const MAX_PARTS: i32 = 10_000;
const ABORT_TIMEOUT: Duration = Duration::from_secs(30);
static ABORT_PERMITS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(32)));
const EVENT_FTPS_UPLOAD_CLEANUP: &str = "ftps_upload_cleanup";
const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_FTPS_UPLOAD: &str = "ftps_upload";

fn upload_error(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::TransientFileNotAvailable, error.to_string())
}

async fn authorize(session: &SessionContext, action: S3Action, bucket: &str, key: &str) -> Result<()> {
    authorize_operation(session, &action, bucket, Some(key))
        .await
        .map_err(|_| Error::new(ErrorKind::PermanentFileNotAvailable, "Access denied"))
}

fn next_part_number(completed: usize) -> Result<i32> {
    let completed = i32::try_from(completed).map_err(upload_error)?;
    if completed >= MAX_PARTS {
        return Err(Error::new(
            ErrorKind::ExceededStorageAllocationError,
            "FTPS multipart upload exceeds 10000 parts",
        ));
    }
    Ok(completed + 1)
}

async fn read_part(reader: &mut (impl AsyncRead + Unpin)) -> Result<Vec<u8>> {
    let mut part = Vec::with_capacity(PART_SIZE);
    reader
        .take(u64::try_from(PART_SIZE).map_err(upload_error)?)
        .read_to_end(&mut part)
        .await
        .map_err(upload_error)?;
    Ok(part)
}

fn body(part: Vec<u8>) -> StreamingBlob {
    StreamingBlob::from_bytes(Bytes::from(part))
}

// Keep the upload ID alive across all cancellable awaits after CreateMultipartUpload.
// A process/runtime crash still requires an AbortIncompleteMultipartUpload lifecycle rule.
struct PendingUpload<S: StorageBackend + 'static> {
    storage: Arc<S>,
    session: SessionContext,
    input: Option<AbortMultipartUploadInput>,
}

impl<S: StorageBackend + 'static> PendingUpload<S> {
    async fn abort(&mut self) {
        if let Some(input) = self.input.as_ref() {
            abort_upload(&*self.storage, &self.session, input.clone()).await;
            self.input = None;
        }
    }
}

async fn abort_upload<S: StorageBackend>(storage: &S, session: &SessionContext, input: AbortMultipartUploadInput) {
    let cleanup = async {
        authorize(session, S3Action::AbortMultipartUpload, &input.bucket, &input.key).await?;
        storage
            .abort_multipart_upload(input, session.credentials())
            .await
            .map_err(upload_error)?;
        Ok::<_, Error>(())
    };
    if !matches!(tokio::time::timeout(ABORT_TIMEOUT, cleanup).await, Ok(Ok(()))) {
        tracing::warn!(
            event = EVENT_FTPS_UPLOAD_CLEANUP,
            component = LOG_COMPONENT_PROTOCOLS,
            subsystem = LOG_SUBSYSTEM_FTPS_UPLOAD,
            result = "abort_failed",
            "FTPS incomplete upload requires lifecycle cleanup"
        );
    }
}

impl<S: StorageBackend + 'static> Drop for PendingUpload<S> {
    fn drop(&mut self) {
        let Some(input) = self.input.take() else {
            return;
        };
        let (Ok(runtime), Ok(permit)) = (tokio::runtime::Handle::try_current(), Arc::clone(&ABORT_PERMITS).try_acquire_owned())
        else {
            tracing::warn!(
                event = EVENT_FTPS_UPLOAD_CLEANUP,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_FTPS_UPLOAD,
                result = "abort_unavailable",
                "FTPS incomplete upload requires lifecycle cleanup"
            );
            return;
        };
        let storage = Arc::clone(&self.storage);
        let session = self.session.clone();
        runtime.spawn(async move {
            let _permit = permit;
            abort_upload(&*storage, &session, input).await;
        });
    }
}

pub(super) async fn upload<S: StorageBackend + 'static>(
    storage: Arc<S>,
    session: &SessionContext,
    mut reader: impl AsyncRead + Unpin,
    bucket: &str,
    key: &str,
) -> Result<u64> {
    let mut part = read_part(&mut reader).await?;
    if part.len() < PART_SIZE {
        let size = u64::try_from(part.len()).map_err(upload_error)?;
        authorize(session, S3Action::PutObject, bucket, key).await?;
        let input = PutObjectInput::builder()
            .bucket(bucket.to_owned())
            .key(key.to_owned())
            .content_length(Some(i64::try_from(size).map_err(upload_error)?))
            .body(Some(body(part)))
            .build()
            .map_err(upload_error)?;
        storage.put_object(input, session.credentials()).await.map_err(upload_error)?;
        return Ok(size);
    }

    authorize(session, S3Action::CreateMultipartUpload, bucket, key).await?;
    let input = CreateMultipartUploadInput::builder()
        .bucket(bucket.to_owned())
        .key(key.to_owned())
        .build()
        .map_err(upload_error)?;
    let output = storage
        .create_multipart_upload(input, session.credentials())
        .await
        .map_err(upload_error)?;
    let upload_id = output
        .upload_id
        .filter(|id| !id.is_empty())
        .ok_or_else(|| upload_error("CreateMultipartUpload returned no upload ID"))?;
    let abort = AbortMultipartUploadInput::builder()
        .bucket(bucket.to_owned())
        .key(key.to_owned())
        .upload_id(upload_id.clone())
        .build()
        .map_err(upload_error)?;
    let mut pending = PendingUpload {
        storage: Arc::clone(&storage),
        session: session.clone(),
        input: Some(abort),
    };

    let result = async {
        let mut parts = Vec::new();
        let mut total = 0u64;
        while !part.is_empty() {
            let number = next_part_number(parts.len())?;
            let length = i64::try_from(part.len()).map_err(upload_error)?;
            total = total
                .checked_add(u64::try_from(part.len()).map_err(upload_error)?)
                .ok_or_else(|| upload_error("FTPS upload size overflow"))?;
            authorize(session, S3Action::UploadPart, bucket, key).await?;
            let input = UploadPartInput::builder()
                .bucket(bucket.to_owned())
                .key(key.to_owned())
                .upload_id(upload_id.clone())
                .part_number(number)
                .content_length(Some(length))
                .body(Some(body(part)))
                .build()
                .map_err(upload_error)?;
            let output = storage
                .upload_part(input, session.credentials())
                .await
                .map_err(upload_error)?;
            let etag = output.e_tag.ok_or_else(|| upload_error("UploadPart returned no ETag"))?;
            parts.push(CompletedPart {
                e_tag: Some(etag),
                part_number: Some(number),
                ..Default::default()
            });
            part = read_part(&mut reader).await?;
        }
        authorize(session, S3Action::CompleteMultipartUpload, bucket, key).await?;
        let input = CompleteMultipartUploadInput::builder()
            .bucket(bucket.to_owned())
            .key(key.to_owned())
            .upload_id(upload_id)
            .multipart_upload(Some(CompletedMultipartUpload { parts: Some(parts) }))
            .build()
            .map_err(upload_error)?;
        storage
            .complete_multipart_upload(input, session.credentials())
            .await
            .map_err(upload_error)?;
        // No await between a successful commit and disarming cancellation cleanup.
        pending.input = None;
        Ok(total)
    }
    .await;
    if result.is_err() {
        pending.abort().await;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        common::{
            dummy_storage::{DummyBackend, DummyError},
            gateway::{is_operation_supported, with_test_auth_override},
            session::{Protocol, test_session},
        },
        ftps::{driver::FtpsDriver, server::FtpsUser},
    };
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::{io::ReadBuf, sync::Notify};
    use unftp_core::storage::StorageBackend as _;

    fn user() -> FtpsUser {
        FtpsUser {
            username: "upload-test".into(),
            name: None,
            session_context: test_session(Protocol::Ftps),
        }
    }

    fn multipart_backend() -> DummyBackend {
        let backend = DummyBackend::new();
        backend.queue_create_multipart_upload_ok("upload-1");
        backend.queue_complete_multipart_upload_ok();
        backend
    }

    // Refuse to produce part N+1 before part N has reached the backend. The old
    // read-to-EOF implementation fails this without allocating a huge fixture.
    struct PacedReader {
        backend: DummyBackend,
        position: usize,
        length: usize,
        fail_at_end: bool,
    }
    impl AsyncRead for PacedReader {
        fn poll_read(mut self: Pin<&mut Self>, _: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if self.position / PART_SIZE > self.backend.upload_part_calls().len() {
                return Poll::Ready(Err(std::io::Error::other("read ahead of uploaded part")));
            }
            if self.position == self.length && self.fail_at_end {
                return Poll::Ready(Err(std::io::Error::other("interrupted source")));
            }
            let count = buf.remaining().min(self.length - self.position).min(8191);
            let value = u8::try_from(self.position / PART_SIZE).expect("small test part number");
            // Do not cross a part boundary in a single read, so each part has a distinct byte.
            let count = count.min(PART_SIZE - self.position % PART_SIZE);
            buf.put_slice(&vec![value; count]);
            self.position += count;
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn ftps_part_numbers_stop_at_s3_limit() {
        assert_eq!(next_part_number(0).expect("first part"), 1);
        assert_eq!(next_part_number(9_999).expect("last part"), 10_000);
        assert!(next_part_number(10_000).is_err());
        assert!(next_part_number(usize::MAX).is_err());
    }

    #[tokio::test]
    async fn ftps_part_buffer_does_not_grow_to_detect_eof() {
        let mut reader = tokio::io::repeat(0).take(u64::try_from(PART_SIZE).expect("size"));
        let part = read_part(&mut reader).await.expect("read full part");
        assert_eq!(part.len(), PART_SIZE);
        assert_eq!(part.capacity(), PART_SIZE);
        assert!(read_part(&mut reader).await.expect("EOF").is_empty());
    }

    #[tokio::test]
    async fn ftps_small_and_empty_uploads_preserve_bytes() {
        for payload in [Vec::new(), b"small upload\x00\xff".to_vec()] {
            let backend = DummyBackend::new();
            backend.capture_upload_bodies();
            let driver = FtpsDriver::new(backend.clone());
            let result = with_test_auth_override(
                |_, _, _| true,
                driver.put(&user(), std::io::Cursor::new(payload.clone()), "/bucket/key", 0),
            )
            .await
            .expect("STOR");
            assert_eq!(result, u64::try_from(payload.len()).expect("size"));
            assert_eq!(backend.upload_bodies(), vec![payload]);
            assert!(backend.create_multipart_calls().is_empty());
        }
    }

    #[tokio::test]
    async fn ftps_upload_streams_before_eof_and_preserves_part_bytes() {
        for length in [PART_SIZE - 1, PART_SIZE, PART_SIZE + 1, 2 * PART_SIZE, 2 * PART_SIZE + 7] {
            let backend = multipart_backend();
            backend.capture_upload_bodies();
            for n in 1..=3 {
                backend.queue_upload_part_ok(format!("etag-{n}"));
            }
            let driver = FtpsDriver::new(backend.clone());
            let reader = PacedReader {
                backend: backend.clone(),
                position: 0,
                length,
                fail_at_end: false,
            };
            let size = with_test_auth_override(|_, _, _| true, driver.put(&user(), reader, "/bucket/key", 0))
                .await
                .expect("bounded STOR");
            assert_eq!(size, u64::try_from(length).expect("size"));
            let bodies = backend.upload_bodies();
            assert_eq!(bodies.iter().map(Vec::len).sum::<usize>(), length);
            for (index, part) in bodies.iter().enumerate() {
                assert!(part.len() <= PART_SIZE);
                assert!(part.iter().all(|b| *b == u8::try_from(index).expect("index")));
            }
            if length >= PART_SIZE {
                assert_eq!(backend.complete_multipart_calls()[0].part_count, length.div_ceil(PART_SIZE));
                for (index, call) in backend.upload_part_calls().iter().enumerate() {
                    assert_eq!(call.part_number, i32::try_from(index + 1).expect("part"));
                    assert_eq!(call.content_length, Some(i64::try_from(bodies[index].len()).expect("length")));
                }
            }
            assert!(backend.abort_multipart_calls().is_empty());
        }
    }

    #[tokio::test]
    async fn ftps_upload_aborts_on_source_part_or_complete_failure() {
        for failure in ["source", "part", "etag", "complete"] {
            let backend = DummyBackend::new();
            backend.queue_create_multipart_upload_ok("upload-1");
            match failure {
                "part" => backend.queue_upload_part_err(DummyError::Injected("part failed".into())),
                "etag" => backend.queue_upload_part_ok_without_etag(),
                _ => backend.queue_upload_part_ok("etag-1"),
            }
            backend.queue_complete_multipart_upload_err(DummyError::Injected("complete failed".into()));
            let driver = FtpsDriver::new(backend.clone());
            let reader = PacedReader {
                backend: backend.clone(),
                position: 0,
                length: PART_SIZE,
                fail_at_end: failure == "source",
            };
            let result = with_test_auth_override(|_, _, _| true, driver.put(&user(), reader, "/bucket/key", 0)).await;
            assert!(result.is_err(), "{failure} must not report a committed object");
            assert_eq!(backend.abort_multipart_calls().len(), 1, "{failure}");
            assert_eq!(backend.abort_multipart_calls()[0].upload_id, "upload-1");
            assert_eq!(backend.complete_multipart_calls().len(), usize::from(failure == "complete"));
        }
    }

    #[tokio::test]
    async fn ftps_upload_enforces_authorization_at_each_mutation() {
        for denied in [
            S3Action::PutObject,
            S3Action::CreateMultipartUpload,
            S3Action::UploadPart,
            S3Action::CompleteMultipartUpload,
        ] {
            assert!(is_operation_supported(Protocol::Ftps, &denied));
            let backend = multipart_backend();
            backend.queue_upload_part_ok("etag-1");
            let driver = FtpsDriver::new(backend.clone());
            let reader = tokio::io::repeat(0).take(u64::try_from(PART_SIZE).expect("size"));
            let denied_action = denied.clone();
            let result = with_test_auth_override(
                move |action, _, _| action != &denied_action,
                driver.put(&user(), reader, "/bucket/key", 0),
            )
            .await;
            assert!(result.is_err());
            match denied {
                S3Action::PutObject | S3Action::CreateMultipartUpload => assert!(backend.create_multipart_calls().is_empty()),
                S3Action::UploadPart => assert!(backend.upload_part_calls().is_empty()),
                _ => assert!(backend.complete_multipart_calls().is_empty()),
            }
        }
    }

    #[tokio::test]
    async fn ftps_upload_cleanup_does_not_bypass_abort_permission() {
        let backend = multipart_backend();
        backend.queue_upload_part_err(DummyError::Injected("part failed".into()));
        let driver = FtpsDriver::new(backend.clone());
        let reader = tokio::io::repeat(0).take(u64::try_from(PART_SIZE).expect("size"));
        let result = with_test_auth_override(
            |action, _, _| action != &S3Action::AbortMultipartUpload,
            driver.put(&user(), reader, "/bucket/key", 0),
        )
        .await;
        assert!(result.is_err());
        assert!(backend.abort_multipart_calls().is_empty());
    }

    #[tokio::test]
    async fn ftps_cancelled_upload_aborts_pending_parts() {
        let backend = multipart_backend();
        let entered = Arc::new(Notify::new());
        backend.stall_upload_part(Arc::clone(&entered));
        let driver = FtpsDriver::new(backend.clone());
        let user = user();
        with_test_auth_override(|_, _, _| true, async {
            let reader = tokio::io::repeat(0).take(u64::try_from(PART_SIZE * 2).expect("size"));
            let mut put = Box::pin(driver.put(&user, reader, "/bucket/key", 0));
            tokio::select! {
                _ = entered.notified() => {},
                result = &mut put => panic!("upload should stall: {result:?}"),
            }
            drop(put);
            tokio::time::timeout(Duration::from_secs(5), async {
                while backend.abort_multipart_calls().is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cancellation cleanup");
        })
        .await;
        assert_eq!(backend.abort_multipart_calls().len(), 1);
        assert!(backend.complete_multipart_calls().is_empty());
    }
    // Yield on each read so the memory probe keeps all upload buffers live at once.
    struct YieldingReader {
        remaining: u64,
        yielded: bool,
    }

    impl AsyncRead for YieldingReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if self.remaining == 0 {
                return Poll::Ready(Ok(()));
            }
            if !self.yielded {
                self.yielded = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            self.yielded = false;
            let count = buf
                .remaining()
                .min(1024 * 1024)
                .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
            buf.initialize_unfilled_to(count).fill(0x5a);
            buf.advance(count);
            self.remaining -= u64::try_from(count).expect("read size");
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    #[ignore = "manual synthetic concurrent-upload memory measurement"]
    async fn ftps_large_upload_memory_probe() {
        let bytes: u64 = std::env::var("RUSTFS_FTPS_TEST_BYTES")
            .unwrap_or_else(|_| "67108864".into())
            .parse()
            .expect("byte count");
        let concurrency: usize = std::env::var("RUSTFS_FTPS_TEST_CONCURRENCY")
            .unwrap_or_else(|_| "10".into())
            .parse()
            .expect("concurrency");
        assert!(bytes > 0 && concurrency > 0);
        let operations = (0..concurrency).map(|_| async {
            let backend = multipart_backend();
            for n in 0..bytes.div_ceil(u64::try_from(PART_SIZE).expect("part size")) {
                backend.queue_upload_part_ok(format!("part-{n}"));
            }
            let driver = FtpsDriver::new(backend);
            let reader = YieldingReader {
                remaining: bytes,
                yielded: false,
            };
            driver.put(&user(), reader, "/bucket/key", 0).await
        });
        let results = with_test_auth_override(|_, _, _| true, futures_util::future::join_all(operations)).await;
        for result in results {
            assert_eq!(result.expect("upload"), bytes);
        }
        println!("synthetic_upload_bytes={bytes} concurrent_uploads={concurrency}");
    }
}
