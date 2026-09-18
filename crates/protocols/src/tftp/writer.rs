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

//! Streaming `AsyncWrite` with commit-on-close and abort-on-drop semantics.

use super::constants::{S3_MAX_MULTIPART_PARTS, S3_MIN_PART_SIZE};
use super::errors::is_no_such_upload_backend_error;
use crate::common::client::s3::StorageBackend;
use bytes::Bytes;
use futures_lite::{AsyncWrite, io::Result as AsyncIoResult};
use futures_util::stream;
use rustfs_credentials::Credentials;
use s3s::dto::{
    AbortMultipartUploadInput, CompleteMultipartUploadInput, CompletedMultipartUpload, CompletedPart as S3CompletedPart,
    CreateMultipartUploadInput, ETag, PutObjectInput, StreamingBlob, UploadPartInput,
};

fn etag_to_string(etag: &ETag) -> String {
    match etag {
        ETag::Strong(s) | ETag::Weak(s) => s.clone(),
    }
}
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::timeout;
use tracing::{debug, warn};

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_TFTP_WRITER: &str = "tftp_writer";
const EVENT_TFTP_WRITE_STATE: &str = "tftp_write_state";
const EVENT_TFTP_ABORT_STATE: &str = "tftp_abort_state";

struct UploadedPart {
    part_number: i32,
    e_tag: String,
}

struct MultipartState {
    upload_id: String,
    next_part_number: i32,
    uploaded_parts: Vec<UploadedPart>,
}

enum WriterStep {
    /// CreateMultipartUpload finished; `multipart` is restored before UploadPart runs.
    UploadCreated {
        state: MultipartState,
    },
    PartFlushed {
        state: MultipartState,
        bytes: u64,
    },
    Committed,
}

/// Bounded-memory writer for TFTP WRQ transfers.
///
/// `async-tftp` calls `close()` only after a successful WRQ. Failed transfers
/// drop the writer without closing, so `Drop` aborts in-progress multipart
/// uploads and never commits partial data.
pub struct ObjectWriter<S: StorageBackend + Send + Sync + 'static> {
    storage: Arc<S>,
    bucket: String,
    key: String,
    credentials: Credentials,
    part_size: u64,
    max_transfer_bytes: u64,
    flushed_bytes: u64,
    buffer: Vec<u8>,
    multipart: Option<MultipartState>,
    /// Copy of the live S3 upload_id kept across `multipart.take()` for in-flight
    /// UploadPart / Complete calls so Drop can still abort.
    live_upload_id: Option<String>,
    completed: bool,
    backend_timeout: Duration,
    op_future: Option<Pin<Box<dyn std::future::Future<Output = std::io::Result<WriterStep>> + Send>>>,
    _permit: OwnedSemaphorePermit,
}

impl<S: StorageBackend + Send + Sync + 'static> ObjectWriter<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: Arc<S>,
        bucket: String,
        key: String,
        credentials: Credentials,
        part_size: u64,
        max_transfer_bytes: u64,
        backend_timeout: Duration,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            storage,
            bucket,
            key,
            credentials,
            part_size: part_size.max(S3_MIN_PART_SIZE),
            max_transfer_bytes,
            flushed_bytes: 0,
            buffer: Vec::new(),
            multipart: None,
            live_upload_id: None,
            completed: false,
            backend_timeout,
            op_future: None,
            _permit: permit,
        }
    }

    fn remember_live_upload_id(&mut self, upload_id: &str) {
        if let Some(id) = non_empty_upload_id(upload_id) {
            self.live_upload_id = Some(id);
        }
    }

    fn preserve_live_upload_id(&mut self) {
        if let Some(id) = self
            .multipart
            .as_ref()
            .and_then(|state| non_empty_upload_id(&state.upload_id))
        {
            self.live_upload_id = Some(id);
        }
    }

    fn clear_live_upload_id(&mut self) {
        self.live_upload_id = None;
    }

    fn upload_id_for_abort(&mut self) -> Option<String> {
        self.live_upload_id
            .take()
            .or_else(|| self.multipart.take().and_then(|state| non_empty_upload_id(&state.upload_id)))
    }

    fn total_bytes(&self) -> u64 {
        self.flushed_bytes.saturating_add(self.buffer.len() as u64)
    }

    fn check_transfer_limit(&self, additional: usize) -> Result<()> {
        if self.total_bytes().saturating_add(additional as u64) > self.max_transfer_bytes {
            return Err(Error::new(ErrorKind::WriteZero, "transfer size limit exceeded"));
        }
        Ok(())
    }

    fn should_use_multipart(&self) -> bool {
        self.multipart.is_some() || self.total_bytes() > self.part_size
    }

    fn needs_multipart_create(&self) -> bool {
        self.multipart
            .as_ref()
            .map(|state| state.upload_id.is_empty())
            .unwrap_or(true)
    }

    fn poll_op(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let Some(mut fut) = self.op_future.take() else {
            return Poll::Ready(Ok(()));
        };
        match fut.as_mut().poll(cx) {
            Poll::Ready(Ok(step)) => {
                match step {
                    WriterStep::UploadCreated { state } => {
                        self.remember_live_upload_id(&state.upload_id);
                        self.multipart = Some(state);
                    }
                    WriterStep::PartFlushed { state, bytes } => {
                        self.remember_live_upload_id(&state.upload_id);
                        self.multipart = Some(state);
                        self.flushed_bytes = self.flushed_bytes.saturating_add(bytes);
                    }
                    WriterStep::Committed => {
                        self.completed = true;
                        self.clear_live_upload_id();
                    }
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                self.op_future = Some(fut);
                Poll::Pending
            }
        }
    }

    fn drive_multipart_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        while self.should_use_multipart() && self.buffer.len() >= self.part_size as usize {
            if self.op_future.is_some() {
                return match self.poll_op(cx) {
                    Poll::Ready(Ok(())) => continue,
                    other => other,
                };
            }

            if self.needs_multipart_create() {
                self.spawn_create_multipart();
            } else {
                self.spawn_flush_part();
            }

            match self.poll_op(cx) {
                Poll::Ready(Ok(())) => continue,
                other => return other,
            }
        }
        Poll::Ready(Ok(()))
    }

    fn spawn_create_multipart(&mut self) {
        let storage = Arc::clone(&self.storage);
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let credentials = self.credentials.clone();
        let backend_timeout = self.backend_timeout;
        let state = self.multipart.take().unwrap_or(MultipartState {
            upload_id: String::new(),
            next_part_number: 1,
            uploaded_parts: Vec::new(),
        });

        self.op_future = Some(Box::pin(async move {
            let input = CreateMultipartUploadInput::builder()
                .bucket(bucket.clone())
                .key(key.clone())
                .build()
                .map_err(Error::other)?;
            let out = timeout(backend_timeout, storage.create_multipart_upload(input, &credentials))
                .await
                .map_err(|_| Error::new(ErrorKind::TimedOut, "create_multipart_upload timed out"))?
                .map_err(Error::other)?;
            let upload_id = out.upload_id.ok_or_else(|| Error::other("missing upload_id"))?;
            Ok(WriterStep::UploadCreated {
                state: MultipartState {
                    upload_id,
                    next_part_number: state.next_part_number,
                    uploaded_parts: state.uploaded_parts,
                },
            })
        }));
    }

    fn spawn_flush_part(&mut self) {
        if self.buffer.len() < self.part_size as usize {
            return;
        }

        self.preserve_live_upload_id();

        let part_bytes: Vec<u8> = self.buffer.drain(..self.part_size as usize).collect();
        let part_len = part_bytes.len() as u64;
        let storage = Arc::clone(&self.storage);
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let credentials = self.credentials.clone();
        let backend_timeout = self.backend_timeout;
        let mut state = self.multipart.take().expect("flush requires multipart state");

        self.op_future = Some(Box::pin(async move {
            if state.upload_id.is_empty() {
                return Err(Error::other("upload_part requires upload_id"));
            }

            if state.next_part_number > S3_MAX_MULTIPART_PARTS {
                abort_upload(storage.as_ref(), &bucket, &key, &state.upload_id, &credentials, backend_timeout).await;
                return Err(Error::new(ErrorKind::WriteZero, "multipart part limit exceeded"));
            }

            let part_number = state.next_part_number;
            let body_stream = stream::once(async move { Ok::<Bytes, std::io::Error>(Bytes::from(part_bytes)) });
            let streaming = StreamingBlob::wrap(body_stream);
            let input = UploadPartInput::builder()
                .bucket(bucket.clone())
                .key(key.clone())
                .upload_id(state.upload_id.clone())
                .part_number(part_number)
                .content_length(Some(part_len as i64))
                .body(Some(streaming))
                .build()
                .map_err(Error::other)?;

            let out = match timeout(backend_timeout, storage.upload_part(input, &credentials)).await {
                Ok(Ok(out)) => out,
                Ok(Err(e)) => {
                    abort_upload(storage.as_ref(), &bucket, &key, &state.upload_id, &credentials, backend_timeout).await;
                    return Err(Error::other(e));
                }
                Err(_) => {
                    abort_upload(storage.as_ref(), &bucket, &key, &state.upload_id, &credentials, backend_timeout).await;
                    return Err(Error::new(ErrorKind::TimedOut, "upload_part timed out"));
                }
            };

            let e_tag = out
                .e_tag
                .as_ref()
                .map(etag_to_string)
                .ok_or_else(|| Error::other("missing etag"))?;
            state.uploaded_parts.push(UploadedPart { part_number, e_tag });
            state.next_part_number += 1;
            Ok(WriterStep::PartFlushed { state, bytes: part_len })
        }));
    }

    fn spawn_commit(&mut self) {
        self.preserve_live_upload_id();

        let storage = Arc::clone(&self.storage);
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let credentials = self.credentials.clone();
        let backend_timeout = self.backend_timeout;
        let remaining = std::mem::take(&mut self.buffer);
        let multipart = self.multipart.take();

        self.op_future = Some(Box::pin(async move {
            if let Some(mut state) = multipart {
                if !remaining.is_empty() {
                    if state.next_part_number > S3_MAX_MULTIPART_PARTS {
                        abort_upload(storage.as_ref(), &bucket, &key, &state.upload_id, &credentials, backend_timeout).await;
                        return Err(Error::new(ErrorKind::WriteZero, "multipart part limit exceeded"));
                    }
                    let part_number = state.next_part_number;
                    let part_len = remaining.len();
                    let body_stream = stream::once(async move { Ok::<Bytes, std::io::Error>(Bytes::from(remaining)) });
                    let streaming = StreamingBlob::wrap(body_stream);
                    let input = UploadPartInput::builder()
                        .bucket(bucket.clone())
                        .key(key.clone())
                        .upload_id(state.upload_id.clone())
                        .part_number(part_number)
                        .content_length(Some(part_len as i64))
                        .body(Some(streaming))
                        .build()
                        .map_err(Error::other)?;
                    let out = match timeout(backend_timeout, storage.upload_part(input, &credentials)).await {
                        Ok(Ok(out)) => out,
                        Ok(Err(e)) => {
                            abort_upload(storage.as_ref(), &bucket, &key, &state.upload_id, &credentials, backend_timeout).await;
                            return Err(Error::other(e));
                        }
                        Err(_) => {
                            abort_upload(storage.as_ref(), &bucket, &key, &state.upload_id, &credentials, backend_timeout).await;
                            return Err(Error::new(ErrorKind::TimedOut, "upload_part timed out"));
                        }
                    };
                    let e_tag = out
                        .e_tag
                        .as_ref()
                        .map(etag_to_string)
                        .ok_or_else(|| Error::other("missing etag"))?;
                    state.uploaded_parts.push(UploadedPart { part_number, e_tag });
                }

                let parts: Vec<S3CompletedPart> = state
                    .uploaded_parts
                    .into_iter()
                    .map(|p| S3CompletedPart {
                        part_number: Some(p.part_number),
                        e_tag: Some(ETag::Strong(p.e_tag)),
                        ..Default::default()
                    })
                    .collect();
                let upload_id = state.upload_id.clone();
                let input = CompleteMultipartUploadInput::builder()
                    .bucket(bucket.clone())
                    .key(key.clone())
                    .upload_id(upload_id.clone())
                    .multipart_upload(Some(CompletedMultipartUpload { parts: Some(parts) }))
                    .build()
                    .map_err(Error::other)?;
                match timeout(backend_timeout, storage.complete_multipart_upload(input, &credentials)).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        abort_upload(storage.as_ref(), &bucket, &key, &upload_id, &credentials, backend_timeout).await;
                        return Err(Error::other(e));
                    }
                    Err(_) => {
                        abort_upload(storage.as_ref(), &bucket, &key, &upload_id, &credentials, backend_timeout).await;
                        return Err(Error::new(ErrorKind::TimedOut, "complete_multipart_upload timed out"));
                    }
                }
            } else {
                let size = remaining.len() as i64;
                let body_stream = stream::once(async move { Ok::<Bytes, std::io::Error>(Bytes::from(remaining)) });
                let streaming = StreamingBlob::wrap(body_stream);
                let input = PutObjectInput::builder()
                    .bucket(bucket)
                    .key(key)
                    .content_length(Some(size))
                    .body(Some(streaming))
                    .build()
                    .map_err(Error::other)?;
                timeout(backend_timeout, storage.put_object(input, &credentials))
                    .await
                    .map_err(|_| Error::new(ErrorKind::TimedOut, "put_object timed out"))?
                    .map_err(Error::other)?;
            }
            Ok(WriterStep::Committed)
        }));
    }
}

async fn abort_upload<S: StorageBackend + Send + Sync>(
    storage: &S,
    bucket: &str,
    key: &str,
    upload_id: &str,
    credentials: &Credentials,
    backend_timeout: Duration,
) {
    let input = match AbortMultipartUploadInput::builder()
        .bucket(bucket.to_string())
        .key(key.to_string())
        .upload_id(upload_id.to_string())
        .build()
    {
        Ok(input) => input,
        Err(e) => {
            warn!(
                event = EVENT_TFTP_ABORT_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_WRITER,
                bucket = %bucket,
                key = %key,
                upload_id = %upload_id,
                result = "build_failed",
                error = %e,
                "tftp abort state changed"
            );
            return;
        }
    };
    match timeout(backend_timeout, storage.abort_multipart_upload(input, credentials)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) if is_no_such_upload_backend_error(&e) => {
            debug!(
                event = EVENT_TFTP_ABORT_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_WRITER,
                bucket = %bucket,
                key = %key,
                upload_id = %upload_id,
                result = "already_aborted",
                "tftp abort state changed"
            );
        }
        Ok(Err(e)) => {
            warn!(
                event = EVENT_TFTP_ABORT_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_WRITER,
                bucket = %bucket,
                key = %key,
                upload_id = %upload_id,
                result = "failed",
                error = %e,
                "tftp abort state changed"
            );
        }
        Err(_) => {
            warn!(
                event = EVENT_TFTP_ABORT_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_WRITER,
                bucket = %bucket,
                key = %key,
                upload_id = %upload_id,
                result = "timed_out",
                "tftp abort state changed"
            );
        }
    }
}

fn non_empty_upload_id(upload_id: &str) -> Option<String> {
    if upload_id.is_empty() {
        None
    } else {
        Some(upload_id.to_owned())
    }
}

impl<S: StorageBackend + Send + Sync + 'static> AsyncWrite for ObjectWriter<S> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<AsyncIoResult<usize>> {
        if self.completed {
            return Poll::Ready(Err(ErrorKind::NotConnected.into()));
        }
        if self.op_future.is_some() {
            match self.poll_op(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if let Err(e) = self.check_transfer_limit(buf.len()) {
            return Poll::Ready(Err(e));
        }

        self.buffer.extend_from_slice(buf);
        match self.drive_multipart_flush(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(buf.len())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<AsyncIoResult<()>> {
        if self.op_future.is_some() {
            return match self.poll_op(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            };
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<AsyncIoResult<()>> {
        if self.completed {
            return Poll::Ready(Ok(()));
        }
        if self.op_future.is_some() {
            match self.poll_op(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        match self.drive_multipart_flush(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }

        if self.op_future.is_none() {
            self.spawn_commit();
        }
        match self.poll_op(cx) {
            Poll::Ready(Ok(())) => {
                debug!(
                    event = EVENT_TFTP_WRITE_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_TFTP_WRITER,
                    bucket = %self.bucket,
                    key = %self.key,
                    bytes = self.total_bytes(),
                    result = "committed",
                    "tftp write state changed"
                );
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: StorageBackend + Send + Sync + 'static> Drop for ObjectWriter<S> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let Some(upload_id) = self.upload_id_for_abort() else {
            return;
        };
        let storage = Arc::clone(&self.storage);
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let credentials = self.credentials.clone();
        let backend_timeout = self.backend_timeout;

        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                abort_upload(storage.as_ref(), &bucket, &key, &upload_id, &credentials, backend_timeout).await;
            });
        } else {
            warn!(
                event = EVENT_TFTP_ABORT_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_TFTP_WRITER,
                bucket = %bucket,
                key = %key,
                upload_id = %upload_id,
                result = "no_runtime",
                "tftp abort state changed"
            );
        }
    }
}

#[cfg(test)]
impl<S: StorageBackend + Send + Sync + 'static> ObjectWriter<S> {
    fn seed_multipart_for_test(&mut self, upload_id: &str, next_part_number: i32) {
        self.remember_live_upload_id(upload_id);
        self.multipart = Some(MultipartState {
            upload_id: upload_id.to_string(),
            next_part_number,
            uploaded_parts: Vec::new(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::dummy_storage::{AbortCall, DummyBackend, DummyError};
    use crate::tftp::constants::{S3_MAX_MULTIPART_PARTS, S3_MIN_PART_SIZE};
    use futures_lite::AsyncWriteExt;
    use rustfs_credentials::Credentials;
    use std::io::ErrorKind;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{OwnedSemaphorePermit, Semaphore};

    const TEST_PART_SIZE: u64 = S3_MIN_PART_SIZE;

    async fn test_permit() -> OwnedSemaphorePermit {
        Arc::new(Semaphore::new(1)).acquire_owned().await.expect("semaphore permit")
    }

    async fn build_writer(backend: Arc<DummyBackend>, max_transfer_bytes: u64) -> ObjectWriter<DummyBackend> {
        ObjectWriter::new(
            backend,
            "b".to_string(),
            "k".to_string(),
            Credentials::default(),
            TEST_PART_SIZE,
            max_transfer_bytes,
            Duration::from_secs(30),
            test_permit().await,
        )
    }

    async fn drain_drop_abort_tasks() {
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
    }

    fn multipart_payload(extra: usize) -> Vec<u8> {
        vec![0u8; TEST_PART_SIZE as usize + extra]
    }

    #[tokio::test]
    async fn close_small_object_uses_put_object_without_abort() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_put_object_ok();
        let mut writer = build_writer(backend.clone(), TEST_PART_SIZE * 2).await;

        writer.write_all(b"small-payload").await.expect("write");
        writer.close().await.expect("close");

        assert_eq!(backend.put_object_calls().len(), 1);
        assert!(backend.create_multipart_calls().is_empty());
        assert!(backend.abort_multipart_calls().is_empty());
    }

    #[tokio::test]
    async fn close_multipart_object_completes_without_abort() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_create_multipart_upload_ok("UP-OK");
        backend.queue_upload_part_ok("etag-full");
        backend.queue_upload_part_ok("etag-tail");
        backend.queue_complete_multipart_upload_ok();
        let mut writer = build_writer(backend.clone(), TEST_PART_SIZE * 4).await;

        writer
            .write_all(&multipart_payload(512))
            .await
            .expect("write multipart payload");
        writer.close().await.expect("close");

        assert_eq!(backend.create_multipart_calls().len(), 1);
        assert_eq!(backend.upload_part_calls().len(), 2);
        assert_eq!(backend.complete_multipart_calls().len(), 1);
        assert!(backend.put_object_calls().is_empty());
        assert!(backend.abort_multipart_calls().is_empty());
    }

    #[tokio::test]
    async fn close_complete_multipart_err_aborts_once() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_create_multipart_upload_ok("UP-COMPLETE-ERR");
        backend.queue_upload_part_ok("etag-full");
        backend.queue_upload_part_ok("etag-tail");
        backend.queue_complete_multipart_upload_err(DummyError::Injected("complete failed".into()));
        let mut writer = build_writer(backend.clone(), TEST_PART_SIZE * 4).await;

        writer.write_all(&multipart_payload(1)).await.expect("write");
        let err = writer.close().await.expect_err("complete failure must propagate");
        assert_eq!(err.kind(), ErrorKind::Other);

        let aborts = backend.abort_multipart_calls();
        assert_eq!(aborts.len(), 1);
        assert_eq!(aborts[0].upload_id, "UP-COMPLETE-ERR");
    }

    #[tokio::test]
    async fn flush_upload_part_err_aborts_without_complete() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_create_multipart_upload_ok("UP-FLUSH-ERR");
        backend.queue_upload_part_err(DummyError::Injected("flush upload failed".into()));
        let mut writer = build_writer(backend.clone(), TEST_PART_SIZE * 4).await;

        let err = writer
            .write_all(&multipart_payload(1))
            .await
            .expect_err("flush UploadPart failure must propagate");
        assert_eq!(err.kind(), ErrorKind::Other);

        let aborts = backend.abort_multipart_calls();
        assert_eq!(aborts.len(), 1);
        assert_eq!(aborts[0].upload_id, "UP-FLUSH-ERR");
        assert!(backend.complete_multipart_calls().is_empty());
    }

    #[tokio::test]
    async fn trailing_upload_part_err_aborts_without_complete() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_create_multipart_upload_ok("UP-TAIL-ERR");
        backend.queue_upload_part_ok("etag-full");
        backend.queue_upload_part_err(DummyError::Injected("trailing upload failed".into()));
        let mut writer = build_writer(backend.clone(), TEST_PART_SIZE * 4).await;

        writer.write_all(&multipart_payload(128)).await.expect("write");
        let err = writer.close().await.expect_err("trailing UploadPart failure must propagate");
        assert_eq!(err.kind(), ErrorKind::Other);

        let aborts = backend.abort_multipart_calls();
        assert_eq!(aborts.len(), 1);
        assert_eq!(aborts[0].upload_id, "UP-TAIL-ERR");
        assert!(backend.complete_multipart_calls().is_empty());
    }

    #[tokio::test]
    async fn flush_part_limit_exceeded_aborts_with_write_zero() {
        let backend = Arc::new(DummyBackend::new());
        let mut writer = build_writer(backend.clone(), TEST_PART_SIZE * 4).await;
        writer.seed_multipart_for_test("UP-PART-LIMIT", S3_MAX_MULTIPART_PARTS + 1);

        let err = writer
            .write_all(&vec![0u8; TEST_PART_SIZE as usize])
            .await
            .expect_err("part limit must fail");
        assert_eq!(err.kind(), ErrorKind::WriteZero);

        let aborts = backend.abort_multipart_calls();
        assert_eq!(aborts.len(), 1);
        assert_eq!(aborts[0].upload_id, "UP-PART-LIMIT");
    }

    #[tokio::test]
    async fn trailing_part_limit_exceeded_aborts_with_write_zero() {
        let backend = Arc::new(DummyBackend::new());
        let mut writer = build_writer(backend.clone(), TEST_PART_SIZE * 4).await;
        writer.seed_multipart_for_test("UP-TAIL-LIMIT", S3_MAX_MULTIPART_PARTS + 1);

        writer.write_all(b"tail").await.expect("buffer trailing bytes");
        let err = writer.close().await.expect_err("trailing part limit must fail");
        assert_eq!(err.kind(), ErrorKind::WriteZero);

        let aborts = backend.abort_multipart_calls();
        assert_eq!(aborts.len(), 1);
        assert_eq!(aborts[0].upload_id, "UP-TAIL-LIMIT");
    }

    #[tokio::test]
    async fn write_exceeding_max_transfer_bytes_returns_write_zero() {
        let backend = Arc::new(DummyBackend::new());
        let mut writer = build_writer(backend.clone(), 128).await;

        let err = writer.write_all(&vec![0u8; 129]).await.expect_err("transfer limit must fail");
        assert_eq!(err.kind(), ErrorKind::WriteZero);
        assert!(backend.put_object_calls().is_empty());
        assert!(backend.abort_multipart_calls().is_empty());
    }

    #[tokio::test]
    async fn drop_incomplete_multipart_aborts_once() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_create_multipart_upload_ok("UP-DROP");
        backend.queue_upload_part_ok("etag-full");
        let writer = build_writer(backend.clone(), TEST_PART_SIZE * 4).await;
        let mut writer = writer;
        writer.write_all(&multipart_payload(1)).await.expect("write one flushed part");

        drop(writer);
        drain_drop_abort_tasks().await;

        let aborts = backend.abort_multipart_calls();
        assert_eq!(aborts.len(), 1);
        let AbortCall { bucket, key, upload_id } = &aborts[0];
        assert_eq!(bucket, "b");
        assert_eq!(key, "k");
        assert_eq!(upload_id, "UP-DROP");
    }

    #[tokio::test]
    async fn drop_put_object_path_does_not_abort() {
        let backend = Arc::new(DummyBackend::new());
        let mut writer = build_writer(backend.clone(), TEST_PART_SIZE * 2).await;
        writer.write_all(b"uncommitted-small").await.expect("write");

        drop(writer);
        drain_drop_abort_tasks().await;

        assert!(backend.abort_multipart_calls().is_empty());
    }

    #[tokio::test]
    async fn drop_after_successful_close_does_not_abort() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_put_object_ok();
        let mut writer = build_writer(backend.clone(), TEST_PART_SIZE * 2).await;
        writer.write_all(b"committed-small").await.expect("write");
        writer.close().await.expect("close");

        drop(writer);
        drain_drop_abort_tasks().await;

        assert!(backend.abort_multipart_calls().is_empty());
    }

    #[tokio::test]
    async fn drop_during_in_flight_upload_part_aborts_via_live_upload_id() {
        let backend = Arc::new(DummyBackend::new());
        backend.queue_create_multipart_upload_ok("UP-LIVE");
        let entered = Arc::new(tokio::sync::Notify::new());
        backend.stall_upload_part(entered.clone());

        let mut writer = build_writer(backend.clone(), TEST_PART_SIZE * 4).await;
        let payload = multipart_payload(1);
        let write_fut = writer.write_all(&payload);

        tokio::select! {
            biased;
            _ = entered.notified() => {
                drop(writer);
            }
            result = write_fut => {
                panic!("write must stall inside upload_part, got: {result:?}");
            }
        }

        drain_drop_abort_tasks().await;

        let aborts = backend.abort_multipart_calls();
        assert_eq!(aborts.len(), 1, "Drop during in-flight UploadPart must abort once");
        assert_eq!(aborts[0].upload_id, "UP-LIVE");
    }
}
