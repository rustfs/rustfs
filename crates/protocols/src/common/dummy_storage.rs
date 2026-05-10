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

#![cfg(test)]

//! Storage-backend double for protocol driver unit tests.
//!
//! DummyBackend is a queue-driven StorageBackend implementation with
//! per-method response queues and per-call observation logs. Each
//! async method pops the next response from its queue; an empty queue
//! returns a default not-found or not-implemented error so a test
//! that forgets to configure a branch errors at the call site rather
//! than passing silently.
//!
//! Send + Sync behind a single Mutex. Tests share state between the
//! driver-held Arc and a cloned Arc kept for observation after the
//! driver is dropped. SessionContext fixtures live next to the
//! SessionContext type in common::session.

use crate::common::client::s3::StorageBackend;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, CompleteMultipartUploadInput, CompleteMultipartUploadOutput,
    CopyObjectInput, CopyObjectOutput, CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput,
    DeleteBucketOutput, DeleteObjectOutput, ETag, GetObjectOutput, HeadBucketOutput, HeadObjectOutput, ListBucketsOutput,
    ListObjectsV2Input, ListObjectsV2Output, PutObjectInput, PutObjectOutput, StreamingBlob, Timestamp, UploadPartCopyInput,
    UploadPartCopyOutput, UploadPartInput, UploadPartOutput,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::Notify;

/// Error type returned by DummyBackend. Display strings include substrings
/// the driver's error-mapping helpers match against, so a queued NoSuchKey
/// error is reported as a not-found status at the protocol layer and an
/// AccessDenied error is reported as a permission-denied status.
#[derive(Debug, Error)]
pub enum DummyError {
    /// Display includes the NoSuchKey substring. S3-style error mappers
    /// map this to not-found.
    #[error("NoSuchKey: {0}")]
    NoSuchKey(String),
    /// Display includes the NoSuchBucket substring. S3-style error mappers
    /// map this to not-found.
    #[error("NoSuchBucket: {0}")]
    NoSuchBucket(String),
    /// Free-form error string pre-seeded by a test. Must contain one of the
    /// S3 error-code substrings if the test wants a specific status code
    /// from the driver's error-mapping helper.
    #[error("{0}")]
    Injected(String),
    /// Default response when the per-method queue is empty and the method
    /// has no NotFound default. Any test reaching this path has forgotten
    /// to configure the branch.
    #[error("DummyBackend method not configured: {0}")]
    Unconfigured(&'static str),
}

/// Recorded invocation of abort_multipart_upload. Tests assert on these to
/// observe tombstone-driven abort-on-drop behaviour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortCall {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
}

/// Recorded invocation of upload_part. Tests assert on these to observe
/// the sequence of parts a write path issues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadPartCall {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub part_number: i32,
    pub content_length: Option<i64>,
}

/// Recorded invocation of complete_multipart_upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteCall {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub part_count: usize,
}

/// Recorded invocation of head_object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadObjectCall {
    pub bucket: String,
    pub key: String,
}

struct Inner {
    // Response queues. Each method pops from its own queue. Empty queue
    // plus no default means a configured-miss error.
    get_object: VecDeque<Result<GetObjectOutput, DummyError>>,
    get_object_range: VecDeque<Result<GetObjectOutput, DummyError>>,
    put_object: VecDeque<Result<PutObjectOutput, DummyError>>,
    delete_object: VecDeque<Result<DeleteObjectOutput, DummyError>>,
    head_object: VecDeque<Result<HeadObjectOutput, DummyError>>,
    head_bucket: VecDeque<Result<HeadBucketOutput, DummyError>>,
    list_objects_v2: VecDeque<Result<ListObjectsV2Output, DummyError>>,
    list_buckets: VecDeque<Result<ListBucketsOutput, DummyError>>,
    create_bucket: VecDeque<Result<CreateBucketOutput, DummyError>>,
    delete_bucket: VecDeque<Result<DeleteBucketOutput, DummyError>>,
    copy_object: VecDeque<Result<CopyObjectOutput, DummyError>>,
    create_multipart_upload: VecDeque<Result<CreateMultipartUploadOutput, DummyError>>,
    upload_part: VecDeque<Result<UploadPartOutput, DummyError>>,
    complete_multipart_upload: VecDeque<Result<CompleteMultipartUploadOutput, DummyError>>,
    abort_multipart_upload: VecDeque<Result<AbortMultipartUploadOutput, DummyError>>,
    upload_part_copy: VecDeque<Result<UploadPartCopyOutput, DummyError>>,

    // Observation logs.
    abort_multipart_calls: Vec<AbortCall>,
    upload_part_calls: Vec<UploadPartCall>,
    complete_multipart_calls: Vec<CompleteCall>,
    head_object_calls: Vec<HeadObjectCall>,

    // Cancellation-test support. When stall_upload_part is true every
    // upload_part invocation signals upload_part_entered and then awaits
    // std::future::pending. The pending future is cancellable: the caller's
    // select or Drop cancels it without blocking the runtime.
    stall_upload_part: bool,
    upload_part_entered: Option<Arc<Notify>>,

    // When stall_put_object is true every put_object invocation signals
    // put_object_entered and then awaits std::future::pending. Used by
    // the run_backend timeout integration tests where the driver must
    // observe an Elapsed deadline rather than a backend Err.
    stall_put_object: bool,
    put_object_entered: Option<Arc<Notify>>,

    // When stall_list_objects_v2 is true every list_objects_v2
    // invocation signals list_objects_v2_entered and then awaits
    // std::future::pending. Used by the cursor-corruption regression
    // test that pins the un-advanced cursor after a cancelled READDIR
    // mid-await.
    stall_list_objects_v2: bool,
    list_objects_v2_entered: Option<Arc<Notify>>,
}

impl Inner {
    fn new() -> Self {
        Self {
            get_object: VecDeque::new(),
            get_object_range: VecDeque::new(),
            put_object: VecDeque::new(),
            delete_object: VecDeque::new(),
            head_object: VecDeque::new(),
            head_bucket: VecDeque::new(),
            list_objects_v2: VecDeque::new(),
            list_buckets: VecDeque::new(),
            create_bucket: VecDeque::new(),
            delete_bucket: VecDeque::new(),
            copy_object: VecDeque::new(),
            create_multipart_upload: VecDeque::new(),
            upload_part: VecDeque::new(),
            complete_multipart_upload: VecDeque::new(),
            abort_multipart_upload: VecDeque::new(),
            upload_part_copy: VecDeque::new(),
            abort_multipart_calls: Vec::new(),
            upload_part_calls: Vec::new(),
            complete_multipart_calls: Vec::new(),
            head_object_calls: Vec::new(),
            stall_upload_part: false,
            upload_part_entered: None,
            stall_put_object: false,
            put_object_entered: None,
            stall_list_objects_v2: false,
            list_objects_v2_entered: None,
        }
    }
}

/// Queue-driven StorageBackend test double. Holds internal state behind a
/// single Mutex. Tests configure response queues via queue_* methods,
/// wrap the backend in Arc, hand one clone to the protocol driver being
/// tested, and keep another clone for observation. Method calls are
/// fire-and-forget from the driver's perspective and synchronous on the
/// test side.
pub struct DummyBackend {
    inner: Mutex<Inner>,
}

impl Default for DummyBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl DummyBackend {
    /// Build an empty backend. Every method returns a default not-found or
    /// configured-miss error until a queue is populated.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::new()),
        }
    }

    // Queue-configuration helpers. Each test stages the responses it
    // expects in order. The method pops in FIFO order.

    /// Queue a head_object Ok response with the given size and mtime.
    pub fn queue_head_object_ok(&self, size: u64, mtime: Option<Timestamp>) {
        let out = HeadObjectOutput {
            content_length: Some(size as i64),
            last_modified: mtime,
            ..Default::default()
        };
        self.inner.lock().expect("lock").head_object.push_back(Ok(out));
    }

    /// Queue a head_object NoSuchKey response for the next call.
    pub fn queue_head_object_not_found(&self) {
        self.inner
            .lock()
            .expect("lock")
            .head_object
            .push_back(Err(DummyError::NoSuchKey(String::from("head_object"))));
    }

    /// Queue a put_object Ok response (default PutObjectOutput).
    pub fn queue_put_object_ok(&self) {
        self.inner
            .lock()
            .expect("lock")
            .put_object
            .push_back(Ok(PutObjectOutput::default()));
    }

    /// Queue a put_object error. Used by the commit_write retry tests
    /// to script SlowDown / AccessDenied sequences against the
    /// rustfs_utils::retry::is_s3code_in_message_retryable predicate.
    pub fn queue_put_object_err(&self, err: DummyError) {
        self.inner.lock().expect("lock").put_object.push_back(Err(err));
    }

    /// Number of unconsumed put_object responses left in the queue.
    /// Used to assert that a non-retryable error did not consume more
    /// than one queued response.
    pub fn put_object_queue_len(&self) -> usize {
        self.inner.lock().expect("lock").put_object.len()
    }

    /// Queue an arbitrary head_object error for the next call. Used by
    /// the run_backend_with_err pass-through test that verifies the
    /// backend Err reaches the caller unchanged when no timeout fires.
    pub fn queue_head_object_err(&self, err: DummyError) {
        self.inner.lock().expect("lock").head_object.push_back(Err(err));
    }

    /// Queue a create_multipart_upload Ok carrying the given upload_id.
    pub fn queue_create_multipart_upload_ok(&self, upload_id: impl Into<String>) {
        let out = CreateMultipartUploadOutput {
            upload_id: Some(upload_id.into()),
            ..Default::default()
        };
        self.inner.lock().expect("lock").create_multipart_upload.push_back(Ok(out));
    }

    /// Queue an upload_part Ok response carrying the given ETag. The
    /// string is wrapped in ETag::Strong. Callers that need ETag::Weak
    /// can queue a custom UploadPartOutput instead of using this helper.
    pub fn queue_upload_part_ok(&self, e_tag: impl Into<String>) {
        let out = UploadPartOutput {
            e_tag: Some(ETag::Strong(e_tag.into())),
            ..Default::default()
        };
        self.inner.lock().expect("lock").upload_part.push_back(Ok(out));
    }

    /// Queue an upload_part Ok response with no ETag. Exercises the
    /// missing-ETag branch a driver may guard against.
    pub fn queue_upload_part_ok_without_etag(&self) {
        let out = UploadPartOutput {
            e_tag: None,
            ..Default::default()
        };
        self.inner.lock().expect("lock").upload_part.push_back(Ok(out));
    }

    /// Queue an upload_part error. The error string flows through the
    /// driver's error-mapping helper, so Injected("AccessDenied") produces
    /// a permission-denied status at the driver boundary.
    pub fn queue_upload_part_err(&self, err: DummyError) {
        self.inner.lock().expect("lock").upload_part.push_back(Err(err));
    }

    /// Queue a complete_multipart_upload Ok response.
    pub fn queue_complete_multipart_upload_ok(&self) {
        self.inner
            .lock()
            .expect("lock")
            .complete_multipart_upload
            .push_back(Ok(CompleteMultipartUploadOutput::default()));
    }

    /// Queue a complete_multipart_upload error.
    pub fn queue_complete_multipart_upload_err(&self, err: DummyError) {
        self.inner.lock().expect("lock").complete_multipart_upload.push_back(Err(err));
    }

    /// Queue a list_objects_v2 Ok response with no contents and no
    /// common prefixes. The directory-empty validate path treats this
    /// as "directory is empty".
    pub fn queue_list_objects_v2_ok_empty(&self) {
        self.inner
            .lock()
            .expect("lock")
            .list_objects_v2
            .push_back(Ok(ListObjectsV2Output::default()));
    }

    /// Queue a list_objects_v2 error. Used to verify that callers do
    /// not fall through to a destructive operation when the empty-check
    /// itself fails.
    pub fn queue_list_objects_v2_err(&self, err: DummyError) {
        self.inner.lock().expect("lock").list_objects_v2.push_back(Err(err));
    }

    /// Queue a get_object_range error. Used to verify that the SFTP read
    /// handler surfaces a non-Eof backend failure as an error-level log
    /// event after the wire response has been mapped through
    /// s3_error_to_sftp.
    pub fn queue_get_object_range_err(&self, err: DummyError) {
        self.inner.lock().expect("lock").get_object_range.push_back(Err(err));
    }

    /// Queue a get_object_range Ok response carrying the given bytes as
    /// the streaming body. content_length is set to bytes.len().
    pub fn queue_get_object_range_bytes(&self, payload: Vec<u8>) {
        let size = payload.len() as i64;
        let body = Bytes::from(payload);
        let blob = StreamingBlob::wrap(stream::once(async move { Ok::<Bytes, std::io::Error>(body) }));
        let out = GetObjectOutput {
            body: Some(blob),
            content_length: Some(size),
            ..Default::default()
        };
        self.inner.lock().expect("lock").get_object_range.push_back(Ok(out));
    }

    /// Queue a get_object_range Ok response whose body emits one
    /// initial chunk and then stalls forever on the next .next() poll.
    /// Used by the chunk-deadline regression test to verify that a
    /// stalled mid-stream backend is reaped by the per-chunk timeout
    /// rather than pinning the SFTP session task indefinitely.
    /// reported_content_length sets the GetObjectOutput.content_length
    /// field so the read handler is happy to keep iterating past the
    /// initial chunk.
    pub fn queue_get_object_range_stalling_after_chunk(&self, initial_chunk: Vec<u8>, reported_content_length: i64) {
        let head = Bytes::from(initial_chunk);
        let body_stream = stream::once(async move { Ok::<Bytes, std::io::Error>(head) })
            .chain(stream::pending::<Result<Bytes, std::io::Error>>());
        let blob = StreamingBlob::wrap(body_stream);
        let out = GetObjectOutput {
            body: Some(blob),
            content_length: Some(reported_content_length),
            ..Default::default()
        };
        self.inner.lock().expect("lock").get_object_range.push_back(Ok(out));
    }

    /// Configure upload_part to stall indefinitely. Each call notifies the
    /// supplied Notify once, then awaits std::future::pending, which the
    /// caller cancels by dropping the future.
    pub fn stall_upload_part(&self, entered: Arc<Notify>) {
        let mut inner = self.inner.lock().expect("lock");
        inner.stall_upload_part = true;
        inner.upload_part_entered = Some(entered);
    }

    /// Configure put_object to stall indefinitely. Each call notifies
    /// the supplied Notify once, then awaits std::future::pending. The
    /// run_backend timeout integration test uses this to confirm the
    /// driver's deadline fires when the backend never returns.
    pub fn stall_put_object(&self, entered: Arc<Notify>) {
        let mut inner = self.inner.lock().expect("lock");
        inner.stall_put_object = true;
        inner.put_object_entered = Some(entered);
    }

    /// Configure list_objects_v2 to stall indefinitely. Each call
    /// notifies the supplied Notify once, then awaits
    /// std::future::pending. The cursor-corruption regression test
    /// uses this to cancel a READDIR mid-await and assert the
    /// un-advanced cursor reissues the same first page.
    pub fn stall_list_objects_v2(&self, entered: Arc<Notify>) {
        let mut inner = self.inner.lock().expect("lock");
        inner.stall_list_objects_v2 = true;
        inner.list_objects_v2_entered = Some(entered);
    }

    /// Turn the list_objects_v2 stall back off so subsequent calls
    /// pop from the queue normally. Used by the cursor-corruption
    /// regression test after the first READDIR has been cancelled
    /// mid-await, so the re-issued READDIR can complete against a
    /// queued Ok response.
    pub fn clear_stall_list_objects_v2(&self) {
        let mut inner = self.inner.lock().expect("lock");
        inner.stall_list_objects_v2 = false;
        inner.list_objects_v2_entered = None;
    }

    // Observers. Tests call these after the driver has run to verify the
    // backend received the expected calls.

    /// Snapshot the abort_multipart_upload call log.
    pub fn abort_multipart_calls(&self) -> Vec<AbortCall> {
        self.inner.lock().expect("lock").abort_multipart_calls.clone()
    }

    /// Snapshot the upload_part call log.
    pub fn upload_part_calls(&self) -> Vec<UploadPartCall> {
        self.inner.lock().expect("lock").upload_part_calls.clone()
    }

    /// Snapshot the complete_multipart_upload call log.
    pub fn complete_multipart_calls(&self) -> Vec<CompleteCall> {
        self.inner.lock().expect("lock").complete_multipart_calls.clone()
    }

    /// Snapshot the head_object call log.
    pub fn head_object_calls(&self) -> Vec<HeadObjectCall> {
        self.inner.lock().expect("lock").head_object_calls.clone()
    }
}

#[async_trait]
impl StorageBackend for DummyBackend {
    type Error = DummyError;

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        _ak: &str,
        _sk: &str,
        _start_pos: Option<u64>,
    ) -> Result<GetObjectOutput, Self::Error> {
        match self.inner.lock().expect("lock").get_object.pop_front() {
            Some(r) => r,
            None => Err(DummyError::NoSuchKey(format!("{bucket}/{key}"))),
        }
    }

    async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        _ak: &str,
        _sk: &str,
        _start_pos: u64,
        _length: u64,
    ) -> Result<GetObjectOutput, Self::Error> {
        match self.inner.lock().expect("lock").get_object_range.pop_front() {
            Some(r) => r,
            None => Err(DummyError::NoSuchKey(format!("{bucket}/{key}"))),
        }
    }

    async fn put_object(&self, _input: PutObjectInput, _ak: &str, _sk: &str) -> Result<PutObjectOutput, Self::Error> {
        // Decide control flow while holding the lock. Release before
        // awaiting so the stall path does not hold the Mutex across
        // an await point.
        let (stall, entered, popped) = {
            let mut inner = self.inner.lock().expect("lock");
            let stall = inner.stall_put_object;
            let entered = inner.put_object_entered.clone();
            let popped = if stall { None } else { inner.put_object.pop_front() };
            (stall, entered, popped)
        };
        if stall {
            if let Some(n) = entered {
                n.notify_one();
            }
            std::future::pending::<Result<PutObjectOutput, Self::Error>>().await
        } else {
            match popped {
                Some(r) => r,
                None => Ok(PutObjectOutput::default()),
            }
        }
    }

    async fn delete_object(&self, bucket: &str, key: &str, _ak: &str, _sk: &str) -> Result<DeleteObjectOutput, Self::Error> {
        match self.inner.lock().expect("lock").delete_object.pop_front() {
            Some(r) => r,
            None => Err(DummyError::NoSuchKey(format!("{bucket}/{key}"))),
        }
    }

    async fn head_object(&self, bucket: &str, key: &str, _ak: &str, _sk: &str) -> Result<HeadObjectOutput, Self::Error> {
        {
            let mut inner = self.inner.lock().expect("lock");
            inner.head_object_calls.push(HeadObjectCall {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }
        match self.inner.lock().expect("lock").head_object.pop_front() {
            Some(r) => r,
            None => Err(DummyError::NoSuchKey(format!("{bucket}/{key}"))),
        }
    }

    async fn head_bucket(&self, bucket: &str, _ak: &str, _sk: &str) -> Result<HeadBucketOutput, Self::Error> {
        match self.inner.lock().expect("lock").head_bucket.pop_front() {
            Some(r) => r,
            None => Err(DummyError::NoSuchBucket(bucket.to_string())),
        }
    }

    async fn list_objects_v2(
        &self,
        _input: ListObjectsV2Input,
        _ak: &str,
        _sk: &str,
    ) -> Result<ListObjectsV2Output, Self::Error> {
        // Decide control flow while holding the lock. Release before
        // awaiting so the stall path does not hold the Mutex across
        // an await point.
        let (stall, entered, popped) = {
            let mut inner = self.inner.lock().expect("lock");
            let stall = inner.stall_list_objects_v2;
            let entered = inner.list_objects_v2_entered.clone();
            let popped = if stall { None } else { inner.list_objects_v2.pop_front() };
            (stall, entered, popped)
        };
        if stall {
            if let Some(n) = entered {
                n.notify_one();
            }
            std::future::pending::<Result<ListObjectsV2Output, Self::Error>>().await
        } else {
            match popped {
                Some(r) => r,
                None => Ok(ListObjectsV2Output::default()),
            }
        }
    }

    async fn list_buckets(&self, _ak: &str, _sk: &str) -> Result<ListBucketsOutput, Self::Error> {
        match self.inner.lock().expect("lock").list_buckets.pop_front() {
            Some(r) => r,
            None => Ok(ListBucketsOutput::default()),
        }
    }

    async fn create_bucket(&self, _bucket: &str, _ak: &str, _sk: &str) -> Result<CreateBucketOutput, Self::Error> {
        match self.inner.lock().expect("lock").create_bucket.pop_front() {
            Some(r) => r,
            None => Err(DummyError::Unconfigured("create_bucket")),
        }
    }

    async fn delete_bucket(&self, bucket: &str, _ak: &str, _sk: &str) -> Result<DeleteBucketOutput, Self::Error> {
        match self.inner.lock().expect("lock").delete_bucket.pop_front() {
            Some(r) => r,
            None => Err(DummyError::NoSuchBucket(bucket.to_string())),
        }
    }

    async fn copy_object(&self, _input: CopyObjectInput, _ak: &str, _sk: &str) -> Result<CopyObjectOutput, Self::Error> {
        match self.inner.lock().expect("lock").copy_object.pop_front() {
            Some(r) => r,
            None => Err(DummyError::Unconfigured("copy_object")),
        }
    }

    async fn create_multipart_upload(
        &self,
        _input: CreateMultipartUploadInput,
        _ak: &str,
        _sk: &str,
    ) -> Result<CreateMultipartUploadOutput, Self::Error> {
        match self.inner.lock().expect("lock").create_multipart_upload.pop_front() {
            Some(r) => r,
            None => Err(DummyError::Unconfigured("create_multipart_upload")),
        }
    }

    async fn upload_part(&self, input: UploadPartInput, _ak: &str, _sk: &str) -> Result<UploadPartOutput, Self::Error> {
        // Record the call and decide the control flow while holding the
        // lock. Release the lock before awaiting so the stall path does
        // not hold the Mutex across an await point.
        let (stall, entered, popped) = {
            let mut inner = self.inner.lock().expect("lock");
            inner.upload_part_calls.push(UploadPartCall {
                bucket: input.bucket.to_string(),
                key: input.key.to_string(),
                upload_id: input.upload_id.to_string(),
                part_number: input.part_number,
                content_length: input.content_length,
            });
            let stall = inner.stall_upload_part;
            let entered = inner.upload_part_entered.clone();
            let popped = if stall { None } else { inner.upload_part.pop_front() };
            (stall, entered, popped)
        };
        if stall {
            if let Some(n) = entered {
                n.notify_one();
            }
            std::future::pending::<Result<UploadPartOutput, Self::Error>>().await
        } else {
            match popped {
                Some(r) => r,
                None => Err(DummyError::Unconfigured("upload_part")),
            }
        }
    }

    async fn complete_multipart_upload(
        &self,
        input: CompleteMultipartUploadInput,
        _ak: &str,
        _sk: &str,
    ) -> Result<CompleteMultipartUploadOutput, Self::Error> {
        let part_count = input
            .multipart_upload
            .as_ref()
            .and_then(|mpu| mpu.parts.as_ref().map(|p| p.len()))
            .unwrap_or(0);
        {
            let mut inner = self.inner.lock().expect("lock");
            inner.complete_multipart_calls.push(CompleteCall {
                bucket: input.bucket.to_string(),
                key: input.key.to_string(),
                upload_id: input.upload_id.to_string(),
                part_count,
            });
        }
        match self.inner.lock().expect("lock").complete_multipart_upload.pop_front() {
            Some(r) => r,
            None => Err(DummyError::Unconfigured("complete_multipart_upload")),
        }
    }

    async fn abort_multipart_upload(
        &self,
        input: AbortMultipartUploadInput,
        _ak: &str,
        _sk: &str,
    ) -> Result<AbortMultipartUploadOutput, Self::Error> {
        {
            let mut inner = self.inner.lock().expect("lock");
            inner.abort_multipart_calls.push(AbortCall {
                bucket: input.bucket.to_string(),
                key: input.key.to_string(),
                upload_id: input.upload_id.to_string(),
            });
        }
        match self.inner.lock().expect("lock").abort_multipart_upload.pop_front() {
            Some(r) => r,
            None => Ok(AbortMultipartUploadOutput::default()),
        }
    }

    async fn upload_part_copy(
        &self,
        _input: UploadPartCopyInput,
        _ak: &str,
        _sk: &str,
    ) -> Result<UploadPartCopyOutput, Self::Error> {
        match self.inner.lock().expect("lock").upload_part_copy.pop_front() {
            Some(r) => r,
            None => Err(DummyError::Unconfigured("upload_part_copy")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dummy_backend_reports_not_found_by_default() {
        let backend = DummyBackend::new();
        let result = backend.head_object("b", "k", "ak", "sk").await;
        let Err(err) = result else {
            panic!("default head_object must return an error");
        };
        assert!(
            err.to_string().contains("NoSuchKey"),
            "default error must carry the NoSuchKey substring so drivers map it to not-found; got: {err}",
        );
    }

    #[tokio::test]
    async fn dummy_backend_returns_queued_head_object_response() {
        let backend = DummyBackend::new();
        backend.queue_head_object_ok(42, None);
        let out = backend.head_object("b", "k", "ak", "sk").await.expect("queued Ok");
        assert_eq!(out.content_length, Some(42));
    }

    #[tokio::test]
    async fn dummy_backend_logs_abort_multipart_calls() {
        let backend = Arc::new(DummyBackend::new());
        let input = AbortMultipartUploadInput::builder()
            .bucket("b".to_string())
            .key("k".to_string())
            .upload_id("UP-1".to_string())
            .build()
            .expect("build");
        backend.abort_multipart_upload(input, "ak", "sk").await.expect("Ok");
        let calls = backend.abort_multipart_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].upload_id, "UP-1");
    }

    #[tokio::test]
    async fn dummy_backend_unconfigured_errors_loudly() {
        let backend = DummyBackend::new();
        let err = backend
            .create_multipart_upload(
                CreateMultipartUploadInput::builder()
                    .bucket("b".to_string())
                    .key("k".to_string())
                    .build()
                    .expect("build"),
                "ak",
                "sk",
            )
            .await
            .expect_err("default create_multipart_upload must error");
        assert!(err.to_string().contains("not configured"));
    }
}
