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

//! Write-back pipeline of on-demand migration (rustfs/backlog#2153).
//!
//! Two paths store a source object locally:
//!
//! - **inline**: the GET handler tees the source body to the client and to
//!   [`commit_inline`], which writes the copy through the injected
//!   [`OdmWriteBack`] in one pass;
//! - **background**: [`BucketOdmState::enqueue_pull`] puts a key on the
//!   bucket's bounded [`PullQueue`]; a dispatcher takes one job at a time,
//!   waits for a pull slot (singleflight plus `max_concurrent_pulls`, shared
//!   with the inline path) and runs the pull in its own task: local
//!   existence check, HEAD, GET, single-part or multipart write-back.
//!
//! The local write must look like an ordinary client PUT (bucket default
//! SSE, quota, versioning, Object Lock, replication scheduling, events).
//! Those policies live in the `rustfs` app layer, which this crate cannot
//! call, so the write is delegated to an [`OdmWriteBack`] trait object that
//! the binary injects at startup through
//! [`OnDemandMigrationSys::set_write_back`](super::sys::OnDemandMigrationSys::set_write_back).
//! Source reads go through [`PullSource`], implemented by [`SourceClient`],
//! so the pipeline is testable without HTTP.
//!
//! The source body never reaches the write-back directly: a pump task
//! copies it into a bounded channel while enforcing the idle timeout, the
//! cancellation token and the advertised content length. A truncated or
//! oversized body therefore fails the local write before it can commit,
//! independently of the digest check the write-back performs.

use super::backfill::PullPriority;
use super::source_client::{SourceClient, SourceError, SourceHead};
use super::stats::{PullFailureReason, PullPath};
use super::sys::{BucketOdmState, OnDemandMigrationSys, PullError, PullOutcome, PullSlot};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt, future::Shared};
use parking_lot::Mutex;
use rand::RngExt;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

const EVENT_ODM_PULL_STORED: &str = "odm_pull_stored";
const EVENT_ODM_PULL_SKIPPED: &str = "odm_pull_skipped";
const EVENT_ODM_PULL_FAILED: &str = "odm_pull_failed";
const EVENT_ODM_PULL_QUEUE_STOPPED: &str = "odm_pull_queue_stopped";
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_ON_DEMAND_MIGRATION: &str = "on_demand_migration";

/// Retries after the first attempt for a retryable source failure.
pub const PULL_MAX_RETRIES: usize = 3;
/// Base delay before retry `n` (a random jitter of up to 25% is added).
pub const PULL_RETRY_BASE_DELAYS: [Duration; PULL_MAX_RETRIES] =
    [Duration::from_secs(1), Duration::from_secs(4), Duration::from_secs(16)];
/// S3 multipart upload limit; larger objects need a larger part size.
pub const MAX_MULTIPART_PARTS: u64 = 10_000;
/// Chunk size the source body is read with.
const SOURCE_READ_CHUNK_BYTES: usize = 256 * 1024;
/// Chunks the pump may run ahead of the write-back.
const PUMP_CHANNEL_CHUNKS: usize = 8;

/// Why a background pull was requested; selects the `pulled_objects_total`
/// label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PullReason {
    /// A Range GET was served from the source; fetch the whole object.
    RangeGet,
    /// The object exceeded `inline_max_bytes`.
    LargeObject,
    /// The backfill job listed the key on the source.
    Backfill,
}

impl PullReason {
    pub fn as_str(self) -> &'static str {
        match self {
            PullReason::RangeGet => "range_get",
            PullReason::LargeObject => "large_object",
            PullReason::Backfill => "backfill",
        }
    }

    pub fn path(self) -> PullPath {
        match self {
            PullReason::RangeGet | PullReason::LargeObject => PullPath::Background,
            PullReason::Backfill => PullPath::Backfill,
        }
    }

    /// Backfill pulls yield pull permits to online requests.
    pub fn priority(self) -> PullPriority {
        match self {
            PullReason::RangeGet | PullReason::LargeObject => PullPriority::Online,
            PullReason::Backfill => PullPriority::Backfill,
        }
    }
}

/// How one queued pull ended, delivered to the requester that asked for a
/// report (the backfill job counts these).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueuedPullOutcome {
    /// A new local object of `size` bytes was written.
    Stored {
        size: u64,
    },
    /// A current local version already existed; nothing was pulled.
    AlreadyPresent,
    Failed(PullError),
}

pub type QueuedPullReport = Shared<oneshot::Receiver<QueuedPullOutcome>>;

/// Result of [`PullQueue::enqueue`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EnqueueOutcome {
    /// A new job was queued.
    Enqueued,
    /// The key is already queued or being pulled; nothing was added.
    Coalesced,
    /// `pull_queue_capacity` jobs are waiting; the caller only counts this.
    QueueFull,
    /// The bucket has no usable source client or write-back, or its state
    /// was torn down.
    Unavailable,
}

/// Body a source read produces; consumed inside the pump task only.
pub type SourceBody = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + 'static>>;

/// Says whether an [`idle_guarded_body`] stream ended because the source
/// stalled. The tee flattens a stalled source into an ordinary body read
/// error, which the write-back would otherwise report as a local write
/// failure; the inline path consults this to count the pull under
/// [`PullFailureReason::SourceTimeout`] instead.
#[derive(Clone, Debug, Default)]
pub struct SourceIdleGuard {
    timed_out: Arc<AtomicBool>,
}

impl SourceIdleGuard {
    /// True once the guarded stream ended on the idle budget.
    pub fn timed_out(&self) -> bool {
        self.timed_out.load(Ordering::Acquire)
    }
}

/// Applies `source_timeout.idle_ms` to a source body chunk by chunk: a chunk
/// that does not arrive within `idle_timeout` ends the stream with a timeout
/// error. Both pull paths share this one implementation — the background pump
/// wraps the body it copies into the write-back channel, and the app layer
/// wraps the body it tees between the client and [`commit_inline`].
///
/// The budget measures the source, not the consumer: the timeout is armed
/// inside the stream's own poll, so a consumer that stops asking for bytes (a
/// slow client, or a tee whose queue is full) leaves no timer running and is
/// never mistaken for an idle source.
pub fn idle_guarded_body(body: SourceBody, idle_timeout: Duration) -> (SourceBody, SourceIdleGuard) {
    let guard = SourceIdleGuard::default();
    let timed_out = Arc::clone(&guard.timed_out);
    let stream = futures::stream::unfold(Some(body), move |state| {
        let timed_out = Arc::clone(&timed_out);
        async move {
            let mut body = state?;
            match tokio::time::timeout(idle_timeout, body.next()).await {
                Err(_elapsed) => {
                    timed_out.store(true, Ordering::Release);
                    Some((
                        Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!("source body stalled for more than {}ms", idle_timeout.as_millis()),
                        )),
                        None,
                    ))
                }
                Ok(None) => None,
                Ok(Some(Err(err))) => Some((Err(err), None)),
                Ok(Some(Ok(chunk))) => Some((Ok(chunk), Some(body))),
            }
        }
    });
    (Box::pin(stream), guard)
}

/// Body handed to the write-back; `Sync` because the app-layer put path
/// wraps it into an S3 streaming blob.
pub type WriteBackBody = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync + 'static>>;

/// Read-only view of the source a pull needs.
#[async_trait]
pub trait PullSource: Send + Sync {
    async fn head_object(&self, key: &str) -> Result<SourceHead, SourceError>;

    /// Unranged GET: the returned head describes the whole object.
    async fn get_object(&self, key: &str) -> Result<(SourceHead, SourceBody), SourceError>;

    async fn get_object_tagging(&self, key: &str) -> Result<HashMap<String, String>, SourceError>;
}

#[async_trait]
impl PullSource for SourceClient {
    async fn head_object(&self, key: &str) -> Result<SourceHead, SourceError> {
        SourceClient::head_object(self, key).await
    }

    async fn get_object(&self, key: &str) -> Result<(SourceHead, SourceBody), SourceError> {
        let get = SourceClient::get_object(self, key, None).await?;
        let body = tokio_util::io::ReaderStream::with_capacity(get.body.into_async_read(), SOURCE_READ_CHUNK_BYTES);
        Ok((get.head, Box::pin(body)))
    }

    async fn get_object_tagging(&self, key: &str) -> Result<HashMap<String, String>, SourceError> {
        SourceClient::get_object_tagging(self, key).await
    }
}

/// What the write-back must store. Everything the app layer needs to build
/// the provenance keys, the metadata allowlist and the ETag policy.
#[derive(Clone, Debug)]
pub struct WriteBackRequest {
    pub bucket: String,
    /// Identity captured with the source configuration, retained through cleanup.
    pub bucket_incarnation_id: uuid::Uuid,
    pub key: String,
    /// Source HEAD/GET of the whole object.
    pub head: SourceHead,
    /// `<provider>:<source bucket>`, stored under `odm-source`.
    pub source_label: String,
    pub pulled_at: OffsetDateTime,
    /// `policy.preserve_etag`.
    pub preserve_etag: bool,
    /// `policy.emit_events`.
    pub emit_events: bool,
    pub respect_delete_marker: bool,
    /// Source tags to copy (`policy.copy_tags`), `None` to skip.
    pub tags: Option<HashMap<String, String>>,
}

impl WriteBackRequest {
    pub fn new(state: &BucketOdmState, key: &str, head: SourceHead, tags: Option<HashMap<String, String>>) -> Self {
        let config = state.config();
        Self {
            bucket: state.bucket().to_string(),
            bucket_incarnation_id: state.incarnation_id(),
            key: key.to_string(),
            head,
            source_label: format!("{}:{}", config.source.provider.as_str(), config.source.bucket),
            pulled_at: OffsetDateTime::now_utc(),
            preserve_etag: config.policy.preserve_etag,
            emit_events: config.policy.emit_events,
            respect_delete_marker: config.policy.respect_local_delete_marker,
            tags,
        }
    }
}

/// The committed local object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteBackOutcome {
    pub etag: Option<String>,
    pub size: u64,
    pub version_id: Option<String>,
}

/// One staged part of an internal multipart upload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteBackPart {
    pub part_number: usize,
    pub etag: String,
}

/// The current local version of a key, when one exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalObject {
    pub etag: Option<String>,
    pub size: u64,
    pub delete_marker: bool,
}

/// Why the local write did not commit. `reason()` maps it onto the
/// `pull_failures_total` label set fixed by ODM-05.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WriteBackError {
    /// The body did not hash to the ETag the source advertised.
    #[error("source bytes did not match the source ETag")]
    Integrity,
    /// The bucket quota rejected the write.
    #[error("bucket quota exceeded: {0}")]
    Quota(String),
    /// The object cannot be represented locally (for example too many parts).
    #[error("unsupported source object: {0}")]
    Unsupported(String),
    /// Any other local failure.
    #[error("local write failed: {0}")]
    Local(String),
}

impl WriteBackError {
    pub fn reason(&self) -> PullFailureReason {
        match self {
            WriteBackError::Integrity => PullFailureReason::EtagMismatch,
            WriteBackError::Unsupported(_) => PullFailureReason::SourceUnsupported,
            WriteBackError::Quota(_) => PullFailureReason::Quota,
            WriteBackError::Local(_) => PullFailureReason::LocalWrite,
        }
    }
}

/// Local store facet of the pull pipeline: an ordinary app-layer write plus
/// the existence lookup that guards it. Implemented in the `rustfs` binary
/// on top of its internal put entry points and injected at startup.
#[async_trait]
pub trait OdmWriteBack: Send + Sync {
    /// Current version of `key`, `None` when the key has no readable
    /// current version (absent or delete marker latest).
    async fn local_object(&self, bucket: &str, key: &str) -> Result<Option<LocalObject>, WriteBackError>;

    /// Single-object write of exactly `request.head.size` bytes.
    async fn put_object(&self, request: &WriteBackRequest, body: WriteBackBody) -> Result<WriteBackOutcome, WriteBackError>;

    async fn create_multipart_upload(&self, request: &WriteBackRequest) -> Result<String, WriteBackError>;

    async fn upload_part(
        &self,
        request: &WriteBackRequest,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: WriteBackBody,
    ) -> Result<WriteBackPart, WriteBackError>;

    async fn complete_multipart_upload(
        &self,
        request: &WriteBackRequest,
        upload_id: &str,
        parts: Vec<WriteBackPart>,
    ) -> Result<WriteBackOutcome, WriteBackError>;

    async fn abort_multipart_upload(&self, request: &WriteBackRequest, upload_id: &str) -> Result<(), WriteBackError>;
}

/// Why the pump stopped feeding the write-back before EOF.
#[derive(Debug)]
enum PumpFailure {
    Source(SourceError),
    Canceled,
}

#[derive(Debug, Default)]
struct PumpState {
    failure: Mutex<Option<PumpFailure>>,
}

impl PumpState {
    fn fail(&self, failure: PumpFailure) -> io::Error {
        let message = match &failure {
            PumpFailure::Source(err) => err.to_string(),
            PumpFailure::Canceled => "pull canceled".to_string(),
        };
        let kind = match &failure {
            PumpFailure::Source(SourceError::Timeout) => io::ErrorKind::TimedOut,
            PumpFailure::Source(SourceError::Connect(_)) => io::ErrorKind::UnexpectedEof,
            PumpFailure::Source(_) => io::ErrorKind::Other,
            PumpFailure::Canceled => io::ErrorKind::Interrupted,
        };
        let mut slot = self.failure.lock();
        if slot.is_none() {
            *slot = Some(failure);
        }
        io::Error::new(kind, message)
    }

    fn take(&self) -> Option<PumpFailure> {
        self.failure.lock().take()
    }
}

/// Copies the source body into a bounded channel, enforcing `idle_timeout`
/// per chunk (through [`idle_guarded_body`]), `cancel`, and the advertised
/// `expected_size`.
fn spawn_pump(
    body: SourceBody,
    expected_size: u64,
    idle_timeout: Duration,
    cancel: CancellationToken,
) -> (mpsc::Receiver<io::Result<Bytes>>, Arc<PumpState>) {
    let (mut body, _idle) = idle_guarded_body(body, idle_timeout);
    let (tx, rx) = mpsc::channel(PUMP_CHANNEL_CHUNKS);
    let state = Arc::new(PumpState::default());
    let pump_state = Arc::clone(&state);
    tokio::spawn(async move {
        let mut delivered: u64 = 0;
        loop {
            let next = tokio::select! {
                _ = cancel.cancelled() => Err(pump_state.fail(PumpFailure::Canceled)),
                next = body.next() => match next {
                    None if delivered < expected_size => Err(pump_state.fail(PumpFailure::Source(SourceError::Connect(
                        format!("source body ended after {delivered} of {expected_size} bytes"),
                    )))),
                    None => return,
                    Some(Err(err)) => {
                        let failure = if err.kind() == io::ErrorKind::TimedOut {
                            SourceError::Timeout
                        } else {
                            SourceError::Connect(format!("source body read failed: {err}"))
                        };
                        Err(pump_state.fail(PumpFailure::Source(failure)))
                    }
                    Some(Ok(chunk)) => {
                        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
                        delivered = delivered.saturating_add(len);
                        if delivered > expected_size {
                            Err(pump_state.fail(PumpFailure::Source(SourceError::Other(format!(
                                "source body exceeded the advertised {expected_size} bytes"
                            )))))
                        } else {
                            Ok(chunk)
                        }
                    }
                },
            };
            let stop = next.is_err();
            if tx.send(next).await.is_err() || stop {
                return;
            }
        }
    });
    (rx, state)
}

/// The pumped body plus the tail of the last chunk that crossed a part
/// boundary. Parts are written sequentially, so the mutex is uncontended;
/// it is only ever held inside a poll.
struct SharedSource {
    rx: mpsc::Receiver<io::Result<Bytes>>,
    leftover: Option<Bytes>,
}

type SharedSourceHandle = Arc<Mutex<SharedSource>>;

/// Exactly `remaining` bytes of the shared source, then EOF. A source EOF
/// before that is an `UnexpectedEof` error, never a short part.
struct PartBody {
    source: SharedSourceHandle,
    remaining: u64,
}

impl PartBody {
    fn boxed(source: &SharedSourceHandle, len: u64) -> WriteBackBody {
        Box::pin(Self {
            source: Arc::clone(source),
            remaining: len,
        })
    }
}

impl Stream for PartBody {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }
        let mut chunk = {
            let mut source = self.source.lock();
            match source.leftover.take() {
                Some(leftover) => leftover,
                None => match source.rx.poll_recv(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => {
                        return Poll::Ready(Some(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!("source body ended with {} bytes outstanding", self.remaining),
                        ))));
                    }
                    Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                    Poll::Ready(Some(Ok(chunk))) => chunk,
                },
            }
        };
        let take = usize::try_from(self.remaining).map_or(chunk.len(), |remaining| remaining.min(chunk.len()));
        if take < chunk.len() {
            self.source.lock().leftover = Some(chunk.split_off(take));
        }
        self.remaining = self.remaining.saturating_sub(u64::try_from(take).unwrap_or(u64::MAX));
        Poll::Ready(Some(Ok(chunk)))
    }
}

/// How one pull ended without error.
#[derive(Debug)]
pub enum PullCompletion {
    /// The write-back committed a new local object.
    Stored(WriteBackOutcome),
    /// A current local version already existed; nothing was pulled.
    AlreadyPresent(LocalObject),
}

impl PullCompletion {
    fn outcome(&self) -> PullOutcome {
        match self {
            PullCompletion::Stored(outcome) => PullOutcome {
                etag: outcome.etag.clone(),
                size: outcome.size,
            },
            PullCompletion::AlreadyPresent(local) => PullOutcome {
                etag: local.etag.clone(),
                size: local.size,
            },
        }
    }
}

struct AttemptError {
    error: PullError,
    retryable: bool,
}

impl AttemptError {
    fn source(err: &SourceError) -> Self {
        Self {
            error: PullError::from(err),
            retryable: err.is_retryable(),
        }
    }

    fn write_back(err: &WriteBackError) -> Self {
        Self {
            error: PullError::new(err.reason(), err.to_string()),
            retryable: false,
        }
    }

    fn canceled() -> Self {
        Self {
            error: PullError::canceled("bucket on-demand migration state was removed"),
            retryable: false,
        }
    }
}

struct PullContext<'a> {
    state: &'a Arc<BucketOdmState>,
    source: &'a Arc<dyn PullSource>,
    write_back: &'a Arc<dyn OdmWriteBack>,
    key: &'a str,
    cancel: &'a CancellationToken,
}

impl PullContext<'_> {
    async fn observe<T>(&self, call: impl Future<Output = Result<T, SourceError>>) -> Result<T, AttemptError> {
        let started = Instant::now();
        let result = call.await;
        self.state.observe_source(started.elapsed(), self.key, result.as_ref().err());
        result.map_err(|err| AttemptError::source(&err))
    }

    async fn local_object(&self) -> Result<Option<LocalObject>, AttemptError> {
        self.write_back
            .local_object(self.state.bucket(), self.key)
            .await
            .map_err(|err| AttemptError::write_back(&err))
    }
}

fn retry_delay(retry: usize) -> Duration {
    let base = PULL_RETRY_BASE_DELAYS[retry.min(PULL_RETRY_BASE_DELAYS.len() - 1)];
    let jitter_cap = base.as_millis() / 4;
    let jitter = rand::rng().random_range(0..=jitter_cap);
    base + Duration::from_millis(u64::try_from(jitter).unwrap_or(0))
}

/// One attempt: local check, HEAD, optional tags, GET, second local check,
/// then the write-back. Errors carry whether a retry may help.
async fn pull_once(ctx: &PullContext<'_>) -> Result<PullCompletion, AttemptError> {
    if ctx.cancel.is_cancelled() {
        return Err(AttemptError::canceled());
    }
    if let Some(local) = ctx.local_object().await?
        && !local.delete_marker
    {
        return Ok(PullCompletion::AlreadyPresent(local));
    }

    let policy = &ctx.state.config().policy;
    ctx.observe(ctx.source.head_object(ctx.key)).await?;
    let tags = if policy.copy_tags {
        Some(ctx.observe(ctx.source.get_object_tagging(ctx.key)).await?)
    } else {
        None
    };
    let (head, body) = ctx.observe(ctx.source.get_object(ctx.key)).await?;

    // Re-checked after the source round trips: a client PUT that landed in
    // between must not be overwritten by the older source copy.
    if let Some(local) = ctx.local_object().await?
        && !local.delete_marker
    {
        return Ok(PullCompletion::AlreadyPresent(local));
    }

    let idle_timeout = Duration::from_millis(policy.source_timeout.idle_ms);
    let (rx, pump) = spawn_pump(body, head.size, idle_timeout, ctx.cancel.clone());
    let shared: SharedSourceHandle = Arc::new(Mutex::new(SharedSource { rx, leftover: None }));
    let request = WriteBackRequest::new(ctx.state, ctx.key, head, tags);
    let part_size = policy.multipart_part_size_bytes.max(1);
    let written = if request.head.size > part_size {
        write_multipart(ctx.write_back, &request, &shared, part_size).await
    } else {
        ctx.write_back
            .put_object(&request, PartBody::boxed(&shared, request.head.size))
            .await
    };
    match written {
        Ok(outcome) => Ok(PullCompletion::Stored(outcome)),
        Err(err) => Err(match pump.take() {
            Some(PumpFailure::Canceled) => AttemptError::canceled(),
            Some(PumpFailure::Source(source_err)) => AttemptError::source(&source_err),
            None => AttemptError::write_back(&err),
        }),
    }
}

async fn write_multipart(
    write_back: &Arc<dyn OdmWriteBack>,
    request: &WriteBackRequest,
    shared: &SharedSourceHandle,
    part_size: u64,
) -> Result<WriteBackOutcome, WriteBackError> {
    let size = request.head.size;
    let part_count = size.div_ceil(part_size);
    if part_count > MAX_MULTIPART_PARTS {
        return Err(WriteBackError::Unsupported(format!(
            "object of {size} bytes needs {part_count} parts of {part_size} bytes; the limit is {MAX_MULTIPART_PARTS}"
        )));
    }
    let upload_id = write_back.create_multipart_upload(request).await?;
    let staged = stage_parts(write_back, request, &upload_id, shared, part_size, part_count).await;
    let completed = match staged {
        Ok(parts) => write_back.complete_multipart_upload(request, &upload_id, parts).await,
        Err(err) => Err(err),
    };
    if completed.is_err()
        && let Err(abort_err) = write_back.abort_multipart_upload(request, &upload_id).await
    {
        debug!(
            event = EVENT_ODM_PULL_FAILED,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
            bucket = %request.bucket,
            key = %request.key,
            error = %abort_err,
            "On-demand migration multipart abort failed after a write-back error"
        );
    }
    completed
}

async fn stage_parts(
    write_back: &Arc<dyn OdmWriteBack>,
    request: &WriteBackRequest,
    upload_id: &str,
    shared: &SharedSourceHandle,
    part_size: u64,
    part_count: u64,
) -> Result<Vec<WriteBackPart>, WriteBackError> {
    let size = request.head.size;
    let mut parts = Vec::with_capacity(usize::try_from(part_count).unwrap_or(0));
    let mut offset = 0;
    for part_number in 1..=part_count {
        let len = part_size.min(size - offset);
        let part_number = usize::try_from(part_number)
            .map_err(|_| WriteBackError::Unsupported(format!("part number {part_number} does not fit this platform")))?;
        let part = write_back
            .upload_part(request, upload_id, part_number, len, PartBody::boxed(shared, len))
            .await?;
        parts.push(part);
        offset += len;
    }
    Ok(parts)
}

/// Full pull with the retry policy: transient source failures are retried
/// up to [`PULL_MAX_RETRIES`] times with [`PULL_RETRY_BASE_DELAYS`] plus
/// jitter; everything else fails immediately.
async fn pull_object(ctx: &PullContext<'_>) -> Result<PullCompletion, PullError> {
    let mut retries = 0;
    loop {
        match pull_once(ctx).await {
            Ok(completion) => return Ok(completion),
            Err(AttemptError { error, retryable }) => {
                if !retryable || retries >= PULL_MAX_RETRIES {
                    return Err(error);
                }
                let delay = retry_delay(retries);
                retries += 1;
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = ctx.cancel.cancelled() => return Err(AttemptError::canceled().error),
                }
            }
        }
    }
}

/// Counts and logs the end of one pull.
fn record_completion(state: &BucketOdmState, key: &str, path: PullPath, result: &Result<PullCompletion, PullError>) {
    let stats = state.stats();
    match result {
        Ok(PullCompletion::Stored(outcome)) => {
            stats.record_pulled_object(path);
            stats.record_pulled_bytes(outcome.size);
            trace!(
                event = EVENT_ODM_PULL_STORED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                path = path.as_str(),
                bucket = %state.bucket(),
                key = %key,
                size = outcome.size,
                "On-demand migration stored a source object locally"
            );
        }
        Ok(PullCompletion::AlreadyPresent(_)) => {
            debug!(
                event = EVENT_ODM_PULL_SKIPPED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                path = path.as_str(),
                bucket = %state.bucket(),
                key = %key,
                "On-demand migration pull skipped; a local object already exists"
            );
        }
        Err(err) => {
            stats.record_pull_failure(err.reason);
            debug!(
                event = EVENT_ODM_PULL_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                path = path.as_str(),
                reason = err.reason.as_str(),
                bucket = %state.bucket(),
                key = %key,
                "On-demand migration pull failed"
            );
        }
    }
}

/// Inline write-back of a body the GET handler is already streaming (the
/// tee secondary). No retry: the bytes cannot be re-read. The caller owns
/// the singleflight slot; this only writes and accounts.
///
/// `idle` is the guard of the [`idle_guarded_body`] the caller teed, so a
/// write-back that failed because the source stalled is counted as the source
/// timeout it is rather than as a local write failure. Callers that do not
/// wrap the source body pass a default guard.
pub async fn commit_inline(
    state: &Arc<BucketOdmState>,
    key: &str,
    head: SourceHead,
    tags: Option<HashMap<String, String>>,
    body: WriteBackBody,
    idle: &SourceIdleGuard,
) -> Result<WriteBackOutcome, PullError> {
    let Some(write_back) = state.write_back() else {
        let error = PullError::new(PullFailureReason::LocalWrite, "on-demand migration write-back is not installed");
        record_completion(state, key, PullPath::Inline, &Err(error.clone()));
        return Err(error);
    };
    commit_inline_with(state, write_back, key, head, tags, body, idle).await
}

/// [`commit_inline`] with an explicit write-back (tests and embedders).
pub async fn commit_inline_with(
    state: &Arc<BucketOdmState>,
    write_back: &Arc<dyn OdmWriteBack>,
    key: &str,
    head: SourceHead,
    tags: Option<HashMap<String, String>>,
    body: WriteBackBody,
    idle: &SourceIdleGuard,
) -> Result<WriteBackOutcome, PullError> {
    let request = WriteBackRequest::new(state, key, head, tags);
    let result = write_back.put_object(&request, body).await.map_err(|err| {
        if idle.timed_out() {
            PullError::new(PullFailureReason::SourceTimeout, SourceError::Timeout.to_string())
        } else {
            PullError::new(err.reason(), err.to_string())
        }
    });
    let completion = result
        .as_ref()
        .map(|outcome| PullCompletion::Stored(outcome.clone()))
        .map_err(Clone::clone);
    record_completion(state, key, PullPath::Inline, &completion);
    result
}

struct PullJob {
    key: String,
    reason: PullReason,
    /// Dropped without a send when the job is cancelled before it runs.
    report: Option<oneshot::Sender<QueuedPullOutcome>>,
}

/// Bounded per-bucket queue of background pulls. Keys are unique while
/// queued or in flight; capacity is `pull_queue_capacity`.
pub struct PullQueue {
    bucket: String,
    tx: mpsc::Sender<PullJob>,
    /// Keys queued or running; the job removes its key when it ends.
    pending: Mutex<HashMap<String, QueuedPullReport>>,
    capacity: usize,
    cancel: CancellationToken,
    stats: Arc<super::stats::OdmStats>,
    stopped: watch::Receiver<bool>,
}

impl fmt::Debug for PullQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PullQueue")
            .field("bucket", &self.bucket)
            .field("capacity", &self.capacity)
            .field("pending", &self.pending.lock().len())
            .field("stopped", &self.is_stopped())
            .finish()
    }
}

/// Removes the job's key from the pending set however the job ends.
struct PendingKeyGuard {
    queue: Arc<PullQueue>,
    key: String,
}

impl Drop for PendingKeyGuard {
    fn drop(&mut self) {
        self.queue.pending.lock().remove(&self.key);
    }
}

impl PullQueue {
    /// Starts the dispatcher for `state`. Requires a Tokio runtime.
    pub fn start(state: Arc<BucketOdmState>, source: Arc<dyn PullSource>, write_back: Arc<dyn OdmWriteBack>) -> Arc<Self> {
        let capacity = usize::try_from(state.config().policy.pull_queue_capacity.max(1)).unwrap_or(usize::MAX);
        let (tx, rx) = mpsc::channel(capacity);
        let (stopped_tx, stopped_rx) = watch::channel(false);
        let queue = Arc::new(Self {
            bucket: state.bucket().to_string(),
            tx,
            pending: Mutex::new(HashMap::new()),
            capacity,
            cancel: state.cancel_token(),
            stats: Arc::clone(state.stats()),
            stopped: stopped_rx,
        });
        tokio::spawn(dispatch(Arc::clone(&queue), state, source, write_back, rx, stopped_tx));
        queue
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Keys queued or in flight.
    pub fn pending_keys(&self) -> usize {
        self.pending.lock().len()
    }

    pub fn is_stopped(&self) -> bool {
        *self.stopped.borrow()
    }

    /// Resolves once the dispatcher and every in-flight job have exited.
    pub async fn wait_until_stopped(&self) {
        let mut stopped = self.stopped.clone();
        // A closed channel means the dispatcher is gone as well.
        let _ = stopped.wait_for(|stopped| *stopped).await;
    }

    pub fn enqueue(&self, key: &str, reason: PullReason) -> EnqueueOutcome {
        self.enqueue_with_report(key, reason).0
    }

    /// [`Self::enqueue`] with a shared report, including for coalesced pulls.
    pub fn enqueue_with_report(&self, key: &str, reason: PullReason) -> (EnqueueOutcome, Option<QueuedPullReport>) {
        if self.cancel.is_cancelled() {
            return (EnqueueOutcome::Unavailable, None);
        }
        let mut pending = self.pending.lock();
        if let Some(report) = pending.get(key) {
            return (EnqueueOutcome::Coalesced, Some(report.clone()));
        }
        let (report_tx, report_rx) = oneshot::channel();
        let report_rx = report_rx.shared();
        match self.tx.try_send(PullJob {
            key: key.to_string(),
            reason,
            report: Some(report_tx),
        }) {
            Ok(()) => {
                pending.insert(key.to_string(), report_rx.clone());
                (EnqueueOutcome::Enqueued, Some(report_rx))
            }
            Err(TrySendError::Full(_)) => {
                self.stats.record_pull_failure(PullFailureReason::QueueFull);
                (EnqueueOutcome::QueueFull, None)
            }
            Err(TrySendError::Closed(_)) => (EnqueueOutcome::Unavailable, None),
        }
    }
}

/// Takes jobs in order, waits for a pull slot for each (this is what bounds
/// concurrency to `max_concurrent_pulls`) and runs the pull in its own task.
/// On cancellation it stops taking jobs, fails the queued ones as
/// `canceled`, and waits for in-flight tasks before reporting stopped.
async fn dispatch(
    queue: Arc<PullQueue>,
    state: Arc<BucketOdmState>,
    source: Arc<dyn PullSource>,
    write_back: Arc<dyn OdmWriteBack>,
    mut rx: mpsc::Receiver<PullJob>,
    stopped: watch::Sender<bool>,
) {
    let cancel = state.cancel_token();
    let mut tasks = JoinSet::new();
    loop {
        while tasks.try_join_next().is_some() {}
        let job = tokio::select! {
            _ = cancel.cancelled() => break,
            job = rx.recv() => match job {
                Some(job) => job,
                None => break,
            },
        };
        let pending = PendingKeyGuard {
            queue: Arc::clone(&queue),
            key: job.key.clone(),
        };
        let slot = match state.acquire_pull_slot_with_priority(&job.key, job.reason.priority()).await {
            Ok(slot) => slot,
            Err(err) => {
                let result: Result<PullCompletion, PullError> = Err(err);
                record_completion(&state, &job.key, job.reason.path(), &result);
                drop(pending);
                break;
            }
        };
        tasks.spawn(run_job(
            Arc::clone(&state),
            Arc::clone(&source),
            Arc::clone(&write_back),
            slot,
            job,
            pending,
        ));
    }

    rx.close();
    while let Ok(job) = rx.try_recv() {
        queue.pending.lock().remove(&job.key);
        state.stats().record_pull_failure(PullFailureReason::Canceled);
    }
    while tasks.join_next().await.is_some() {}
    debug!(
        event = EVENT_ODM_PULL_QUEUE_STOPPED,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
        bucket = %state.bucket(),
        "On-demand migration pull queue stopped"
    );
    stopped.send_replace(true);
}

async fn run_job(
    state: Arc<BucketOdmState>,
    source: Arc<dyn PullSource>,
    write_back: Arc<dyn OdmWriteBack>,
    slot: PullSlot,
    job: PullJob,
    pending: PendingKeyGuard,
) {
    let _pending = pending;
    let cancel = state.cancel_token();
    let report = match slot {
        PullSlot::Follower(follower) => {
            // Someone else (inline GET or an earlier job) is pulling the key;
            // its result makes this job redundant.
            match follower.wait().await {
                Ok(outcome) => QueuedPullOutcome::Stored { size: outcome.size },
                Err(err) => QueuedPullOutcome::Failed(err),
            }
        }
        PullSlot::Leader(leader) => {
            let ctx = PullContext {
                state: &state,
                source: &source,
                write_back: &write_back,
                key: &job.key,
                cancel: &cancel,
            };
            let result = pull_object(&ctx).await;
            record_completion(&state, &job.key, job.reason.path(), &result);
            let report = match &result {
                Ok(PullCompletion::Stored(outcome)) => QueuedPullOutcome::Stored { size: outcome.size },
                Ok(PullCompletion::AlreadyPresent(_)) => QueuedPullOutcome::AlreadyPresent,
                Err(err) => QueuedPullOutcome::Failed(err.clone()),
            };
            leader.complete(result.map(|completion| completion.outcome()));
            report
        }
    };
    if let Some(tx) = job.report {
        let _ = tx.send(report);
    }
}

impl BucketOdmState {
    /// The bucket's queue, started on first use. `None` when the state is
    /// torn down, has no usable client, or no write-back was injected.
    pub fn pull_queue(self: &Arc<Self>) -> Option<Arc<PullQueue>> {
        if self.is_cancelled() {
            return None;
        }
        if let Some(queue) = self.pull_queue.get() {
            return Some(Arc::clone(queue));
        }
        let client: Arc<SourceClient> = Arc::clone(self.client().ok()?);
        let source: Arc<dyn PullSource> = client;
        let write_back = Arc::clone(self.write_back()?);
        tokio::runtime::Handle::try_current().ok()?;
        Some(Arc::clone(
            self.pull_queue
                .get_or_init(|| PullQueue::start(Arc::clone(self), source, write_back)),
        ))
    }

    /// Queues a background pull of `key`; see [`EnqueueOutcome`].
    pub fn enqueue_pull(self: &Arc<Self>, key: &str, reason: PullReason) -> EnqueueOutcome {
        self.enqueue_pull_with_report(key, reason).0
    }

    /// [`Self::enqueue_pull`] with the job's report channel.
    pub fn enqueue_pull_with_report(
        self: &Arc<Self>,
        key: &str,
        reason: PullReason,
    ) -> (EnqueueOutcome, Option<QueuedPullReport>) {
        match self.pull_queue() {
            Some(queue) => queue.enqueue_with_report(key, reason),
            None => (EnqueueOutcome::Unavailable, None),
        }
    }
}

impl OnDemandMigrationSys {
    /// [`BucketOdmState::enqueue_pull`] by bucket name; `Unavailable` when
    /// the bucket has no state.
    pub fn enqueue_pull(&self, bucket: &str, key: &str, reason: PullReason) -> EnqueueOutcome {
        match self.state(bucket) {
            Some(state) => state.enqueue_pull(key, reason),
            None => EnqueueOutcome::Unavailable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::on_demand_migration::config::{
        FilterConfig, OnDemandMigrationConfig, PathStyle as ConfigPathStyle, PolicyConfig, Provider, SourceConfig,
        SourceCredentials, TlsConfig,
    };
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const BUCKET: &str = "odm-pull-bucket";

    fn config() -> OnDemandMigrationConfig {
        OnDemandMigrationConfig {
            version: 1,
            enabled: true,
            source: SourceConfig {
                provider: Provider::Minio,
                endpoint: Some("https://source.example.com:9000".to_string()),
                region: "auto".to_string(),
                bucket: "legacy".to_string(),
                path_style: ConfigPathStyle::Auto,
                credentials: Some(SourceCredentials {
                    access_key: "AK".to_string(),
                    secret_key: "SK".to_string(),
                    session_token: None,
                }),
                tls: TlsConfig::default(),
                azure: None,
                gcs: None,
            },
            filter: FilterConfig::default(),
            policy: PolicyConfig::default(),
        }
    }

    async fn enabled_state(sys: &OnDemandMigrationSys, cfg: &OnDemandMigrationConfig) -> Arc<BucketOdmState> {
        sys.set_module_enabled(true);
        sys.apply(BUCKET, Some(cfg)).await;
        sys.state(BUCKET).expect("state must be installed")
    }

    fn head(size: u64) -> SourceHead {
        SourceHead {
            etag: Some("0123456789abcdef0123456789abcdef".to_string()),
            size,
            content_type: Some("text/plain".to_string()),
            ..Default::default()
        }
    }

    fn body_bytes(len: usize) -> Vec<u8> {
        (0..len).map(|i| u8::try_from(i % 251).expect("fits")).collect()
    }

    #[derive(Clone)]
    enum BodyKind {
        Bytes(Vec<u8>),
        Hang,
    }

    #[derive(Clone)]
    struct MockObject {
        head: SourceHead,
        body: BodyKind,
    }

    #[derive(Default)]
    struct MockSource {
        objects: Mutex<HashMap<String, MockObject>>,
        head_failures: Mutex<VecDeque<SourceError>>,
        get_failures: Mutex<VecDeque<SourceError>>,
        tags: HashMap<String, String>,
        head_calls: AtomicUsize,
        get_calls: AtomicUsize,
        tag_calls: AtomicUsize,
        /// Simulates a concurrent client PUT landing while the source GET
        /// is in flight: inserts the key into the write-back's local map.
        land_local_on_get: Mutex<Option<(Arc<MockWriteBack>, String)>>,
    }

    impl MockSource {
        fn with_object(key: &str, size: u64, body: BodyKind) -> Arc<Self> {
            let source = Self::default();
            source
                .objects
                .lock()
                .insert(key.to_string(), MockObject { head: head(size), body });
            Arc::new(source)
        }

        fn object(&self, key: &str) -> Result<MockObject, SourceError> {
            self.objects.lock().get(key).cloned().ok_or(SourceError::NotFound)
        }
    }

    #[async_trait]
    impl PullSource for MockSource {
        async fn head_object(&self, key: &str) -> Result<SourceHead, SourceError> {
            self.head_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(err) = self.head_failures.lock().pop_front() {
                return Err(err);
            }
            Ok(self.object(key)?.head)
        }

        async fn get_object(&self, key: &str) -> Result<(SourceHead, SourceBody), SourceError> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(err) = self.get_failures.lock().pop_front() {
                return Err(err);
            }
            let object = self.object(key)?;
            if let Some((write_back, local_key)) = self.land_local_on_get.lock().take() {
                write_back.local.lock().insert(
                    local_key,
                    LocalObject {
                        etag: Some("client-put".to_string()),
                        size: 1,
                        delete_marker: false,
                    },
                );
            }
            let body: SourceBody = match object.body {
                BodyKind::Bytes(bytes) => {
                    let chunks: Vec<io::Result<Bytes>> =
                        bytes.chunks(700).map(|chunk| Ok(Bytes::copy_from_slice(chunk))).collect();
                    Box::pin(futures::stream::iter(chunks))
                }
                BodyKind::Hang => Box::pin(futures::stream::pending()),
            };
            Ok((object.head, body))
        }

        async fn get_object_tagging(&self, _key: &str) -> Result<HashMap<String, String>, SourceError> {
            self.tag_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.tags.clone())
        }
    }

    /// `(upload id, part number, size, bytes)` as staged by the mock.
    type StagedPart = (String, usize, u64, Vec<u8>);

    #[derive(Default)]
    struct MockWriteBack {
        local: Mutex<HashMap<String, LocalObject>>,
        puts: Mutex<Vec<(WriteBackRequest, Vec<u8>)>>,
        failed_puts: AtomicUsize,
        uploads: Mutex<Vec<String>>,
        parts: Mutex<Vec<StagedPart>>,
        completed: Mutex<Vec<(String, Vec<WriteBackPart>)>>,
        aborted: Mutex<Vec<String>>,
        fail_part: Option<usize>,
        forced_put_error: Mutex<Option<WriteBackError>>,
        upload_seq: AtomicUsize,
    }

    async fn drain(mut body: WriteBackBody, expected: u64) -> Result<Vec<u8>, WriteBackError> {
        let mut buf = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|err| WriteBackError::Local(format!("body read failed: {err}")))?;
            buf.extend_from_slice(&chunk);
        }
        if u64::try_from(buf.len()).expect("fits") != expected {
            return Err(WriteBackError::Local(format!("expected {expected} bytes, got {}", buf.len())));
        }
        Ok(buf)
    }

    #[async_trait]
    impl OdmWriteBack for MockWriteBack {
        async fn local_object(&self, _bucket: &str, key: &str) -> Result<Option<LocalObject>, WriteBackError> {
            Ok(self.local.lock().get(key).cloned())
        }

        async fn put_object(&self, request: &WriteBackRequest, body: WriteBackBody) -> Result<WriteBackOutcome, WriteBackError> {
            let drained = drain(body, request.head.size).await;
            let bytes = match drained {
                Ok(bytes) => bytes,
                Err(err) => {
                    self.failed_puts.fetch_add(1, Ordering::SeqCst);
                    return Err(err);
                }
            };
            if let Some(err) = self.forced_put_error.lock().take() {
                self.failed_puts.fetch_add(1, Ordering::SeqCst);
                return Err(err);
            }
            self.puts.lock().push((request.clone(), bytes));
            self.local.lock().insert(
                request.key.clone(),
                LocalObject {
                    etag: request.head.etag.clone(),
                    size: request.head.size,
                    delete_marker: false,
                },
            );
            Ok(WriteBackOutcome {
                etag: request.head.etag.clone(),
                size: request.head.size,
                version_id: None,
            })
        }

        async fn create_multipart_upload(&self, _request: &WriteBackRequest) -> Result<String, WriteBackError> {
            let id = format!("upload-{}", self.upload_seq.fetch_add(1, Ordering::SeqCst));
            self.uploads.lock().push(id.clone());
            Ok(id)
        }

        async fn upload_part(
            &self,
            _request: &WriteBackRequest,
            upload_id: &str,
            part_number: usize,
            size: u64,
            body: WriteBackBody,
        ) -> Result<WriteBackPart, WriteBackError> {
            let bytes = drain(body, size).await?;
            if self.fail_part == Some(part_number) {
                return Err(WriteBackError::Local("injected part failure".to_string()));
            }
            self.parts.lock().push((upload_id.to_string(), part_number, size, bytes));
            Ok(WriteBackPart {
                part_number,
                etag: format!("part-{part_number}"),
            })
        }

        async fn complete_multipart_upload(
            &self,
            request: &WriteBackRequest,
            upload_id: &str,
            parts: Vec<WriteBackPart>,
        ) -> Result<WriteBackOutcome, WriteBackError> {
            self.completed.lock().push((upload_id.to_string(), parts));
            self.local.lock().insert(
                request.key.clone(),
                LocalObject {
                    etag: request.head.etag.clone(),
                    size: request.head.size,
                    delete_marker: false,
                },
            );
            Ok(WriteBackOutcome {
                etag: request.head.etag.clone(),
                size: request.head.size,
                version_id: None,
            })
        }

        async fn abort_multipart_upload(&self, _request: &WriteBackRequest, upload_id: &str) -> Result<(), WriteBackError> {
            self.aborted.lock().push(upload_id.to_string());
            Ok(())
        }
    }

    async fn wait_until(what: &str, condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }

    fn failures(state: &BucketOdmState) -> std::collections::BTreeMap<String, u64> {
        state
            .snapshot()
            .stats
            .pull_failures_total
            .into_iter()
            .filter(|(_, count)| *count > 0)
            .collect()
    }

    fn pulled(state: &BucketOdmState, path: PullPath) -> u64 {
        state.snapshot().stats.pulled_objects_total[path.as_str()]
    }

    async fn pull(
        state: &Arc<BucketOdmState>,
        source: &Arc<dyn PullSource>,
        write_back: &Arc<dyn OdmWriteBack>,
        key: &str,
    ) -> Result<PullCompletion, PullError> {
        let cancel = state.cancel_token();
        let ctx = PullContext {
            state,
            source,
            write_back,
            key,
            cancel: &cancel,
        };
        let result = pull_object(&ctx).await;
        record_completion(state, key, PullPath::Background, &result);
        result
    }

    #[tokio::test]
    async fn coalesced_enqueues_produce_one_source_get() {
        let sys = OnDemandMigrationSys::new();
        let state = enabled_state(&sys, &config()).await;
        let body = body_bytes(1000);
        let source = MockSource::with_object("a", 1000, BodyKind::Bytes(body.clone()));
        let write_back = Arc::new(MockWriteBack::default());
        let queue = PullQueue::start(Arc::clone(&state), source.clone(), write_back.clone());
        assert_eq!(queue.capacity(), 1024);

        let mut outcomes = HashMap::new();
        let mut shared_report = None;
        for _ in 0..100 {
            let (outcome, report) = queue.enqueue_with_report("a", PullReason::RangeGet);
            *outcomes.entry(outcome).or_insert(0) += 1;
            shared_report = report;
        }
        assert_eq!(outcomes.get(&EnqueueOutcome::Enqueued), Some(&1));
        assert_eq!(outcomes.get(&EnqueueOutcome::Coalesced), Some(&99));
        assert_eq!(queue.pending_keys(), 1);

        assert_eq!(
            shared_report.expect("coalesced report").await,
            Ok(QueuedPullOutcome::Stored { size: 1000 })
        );

        wait_until("first pull to finish", || queue.pending_keys() == 0).await;
        assert_eq!(source.head_calls.load(Ordering::SeqCst), 1);
        assert_eq!(source.get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(source.tag_calls.load(Ordering::SeqCst), 0, "copy_tags is off by default");
        {
            let puts = write_back.puts.lock();
            assert_eq!(puts.len(), 1);
            let (request, stored) = &puts[0];
            assert_eq!(stored, &body);
            assert_eq!(request.bucket, BUCKET);
            assert_eq!(request.key, "a");
            assert_eq!(request.source_label, "minio:legacy");
            assert!(request.preserve_etag && request.emit_events, "policy defaults flow into the request");
            assert!(request.tags.is_none());
        }
        assert_eq!(pulled(&state, PullPath::Background), 1);
        assert_eq!(state.snapshot().stats.pulled_bytes_total, 1000);
        assert!(failures(&state).is_empty(), "{:?}", failures(&state));

        // A second round finds the local copy and does not touch the source.
        assert_eq!(queue.enqueue("a", PullReason::Backfill), EnqueueOutcome::Enqueued);
        wait_until("second job to finish", || queue.pending_keys() == 0).await;
        assert_eq!(source.get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(pulled(&state, PullPath::Background), 1);
        assert_eq!(pulled(&state, PullPath::Backfill), 0);
        assert_eq!(state.inflight_keys(), 0);

        sys.remove(BUCKET);
        queue.wait_until_stopped().await;
        assert_eq!(queue.enqueue("a", PullReason::RangeGet), EnqueueOutcome::Unavailable);
    }

    #[tokio::test]
    async fn coalesced_enqueues_share_failure_reports() {
        let sys = OnDemandMigrationSys::new();
        let state = enabled_state(&sys, &config()).await;
        let source = MockSource::with_object("missing", 1000, BodyKind::Bytes(body_bytes(1000)));
        let queue = PullQueue::start(Arc::clone(&state), source, Arc::new(MockWriteBack::default()));
        let (first, first_report) = queue.enqueue_with_report("absent", PullReason::RangeGet);
        let (second, second_report) = queue.enqueue_with_report("absent", PullReason::Backfill);
        assert_eq!(first, EnqueueOutcome::Enqueued);
        assert_eq!(second, EnqueueOutcome::Coalesced);
        let (first, second) = tokio::join!(first_report.expect("leader report"), second_report.expect("coalesced report"));
        assert_eq!(first, second);
        assert!(matches!(first, Ok(QueuedPullOutcome::Failed(_))));
        sys.remove(BUCKET);
        queue.wait_until_stopped().await;
    }

    #[tokio::test]
    async fn queue_full_is_reported_and_cancel_drains_without_leaking_tasks() {
        let sys = OnDemandMigrationSys::new();
        let mut cfg = config();
        cfg.policy.pull_queue_capacity = 1;
        cfg.policy.max_concurrent_pulls = 1;
        let state = enabled_state(&sys, &cfg).await;
        let source = MockSource::with_object("hang", 10, BodyKind::Hang);
        for key in ["b", "c", "d"] {
            source.objects.lock().insert(
                key.to_string(),
                MockObject {
                    head: head(3),
                    body: BodyKind::Bytes(vec![1, 2, 3]),
                },
            );
        }
        let write_back = Arc::new(MockWriteBack::default());
        let queue = PullQueue::start(Arc::clone(&state), source.clone(), write_back.clone());

        assert_eq!(queue.enqueue("hang", PullReason::LargeObject), EnqueueOutcome::Enqueued);
        wait_until("hanging pull to reach the body", || source.get_calls.load(Ordering::SeqCst) == 1).await;
        assert_eq!(state.stats().inflight_pulls(), 1);
        // The dispatcher takes "b" and blocks on the pull slot; "c" fills the
        // single channel slot; "d" has nowhere to go.
        assert_eq!(queue.enqueue("b", PullReason::LargeObject), EnqueueOutcome::Enqueued);
        wait_until("dispatcher to wait for a slot", || state.stats().queue_depth() == 1).await;
        assert_eq!(queue.enqueue("c", PullReason::LargeObject), EnqueueOutcome::Enqueued);
        assert_eq!(queue.enqueue("d", PullReason::LargeObject), EnqueueOutcome::QueueFull);
        let (coalesced, canceled_report) = queue.enqueue_with_report("c", PullReason::LargeObject);
        assert_eq!(coalesced, EnqueueOutcome::Coalesced);
        assert_eq!(queue.pending_keys(), 3);
        assert_eq!(failures(&state).get("queue_full"), Some(&1));
        assert!(!queue.is_stopped());

        assert_eq!(sys.remove(BUCKET), crate::on_demand_migration::ApplyOutcome::Removed);
        tokio::time::timeout(Duration::from_secs(5), queue.wait_until_stopped())
            .await
            .expect("dispatcher and in-flight job must exit after cancel");
        assert!(queue.is_stopped());
        assert!(
            tokio::time::timeout(Duration::from_secs(5), canceled_report.expect("coalesced cancellation report"))
                .await
                .expect("cancellation closes the report")
                .is_err()
        );
        assert_eq!(queue.pending_keys(), 0);
        assert_eq!(state.inflight_keys(), 0);
        assert_eq!(state.stats().inflight_pulls(), 0);
        assert_eq!(state.stats().queue_depth(), 0);
        let failures = failures(&state);
        assert_eq!(failures.get("canceled"), Some(&3), "{failures:?}");
        assert!(write_back.puts.lock().is_empty());
        assert_eq!(queue.enqueue("b", PullReason::LargeObject), EnqueueOutcome::Unavailable);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_policy_retries_transient_source_errors_only() {
        let sys = OnDemandMigrationSys::new();
        let state = enabled_state(&sys, &config()).await;
        let source = MockSource::with_object("a", 5, BodyKind::Bytes(vec![7; 5]));
        source
            .head_failures
            .lock()
            .extend([SourceError::ServerError(500), SourceError::ServerError(503)]);
        let write_back = Arc::new(MockWriteBack::default());
        let source_dyn: Arc<dyn PullSource> = source.clone();
        let write_back_dyn: Arc<dyn OdmWriteBack> = write_back.clone();

        let started = tokio::time::Instant::now();
        let completion = pull(&state, &source_dyn, &write_back_dyn, "a")
            .await
            .expect("two server errors are within the retry budget");
        assert!(matches!(completion, PullCompletion::Stored(_)));
        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_secs(5), "1s + 4s base backoff, got {elapsed:?}");
        assert!(elapsed <= Duration::from_millis(6_300), "jitter is at most 25%, got {elapsed:?}");
        assert_eq!(source.head_calls.load(Ordering::SeqCst), 3);
        assert_eq!(source.get_calls.load(Ordering::SeqCst), 1);
        assert!(failures(&state).is_empty(), "{:?}", failures(&state));
        assert_eq!(pulled(&state, PullPath::Background), 1);

        write_back.local.lock().clear();
        source.head_failures.lock().push_back(SourceError::AccessDenied);
        let err = pull(&state, &source_dyn, &write_back_dyn, "a")
            .await
            .expect_err("access denied is not retried");
        assert_eq!(err.reason, PullFailureReason::SourceAccessDenied);
        assert_eq!(source.head_calls.load(Ordering::SeqCst), 4);
        assert_eq!(source.get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failures(&state).get("source_access_denied"), Some(&1));

        // Exhausting the budget: four attempts, then the last error wins.
        source.get_failures.lock().extend((0..4).map(|_| SourceError::Timeout));
        let err = pull(&state, &source_dyn, &write_back_dyn, "a")
            .await
            .expect_err("four timeouts exceed three retries");
        assert_eq!(err.reason, PullFailureReason::SourceTimeout);
        assert_eq!(source.get_calls.load(Ordering::SeqCst), 5);
        assert_eq!(failures(&state).get("source_timeout"), Some(&1));
    }

    #[tokio::test(start_paused = true)]
    async fn truncated_source_body_never_commits_and_is_retried() {
        let sys = OnDemandMigrationSys::new();
        let state = enabled_state(&sys, &config()).await;
        // Advertises 1000 bytes, delivers 600.
        let source = MockSource::with_object("short", 1000, BodyKind::Bytes(body_bytes(600)));
        let write_back = Arc::new(MockWriteBack::default());
        let source_dyn: Arc<dyn PullSource> = source.clone();
        let write_back_dyn: Arc<dyn OdmWriteBack> = write_back.clone();

        let err = pull(&state, &source_dyn, &write_back_dyn, "short")
            .await
            .expect_err("a truncated body must not commit");
        assert_eq!(err.reason, PullFailureReason::SourceConnect, "{err}");
        assert_eq!(source.get_calls.load(Ordering::SeqCst), 1 + PULL_MAX_RETRIES);
        assert_eq!(write_back.failed_puts.load(Ordering::SeqCst), 1 + PULL_MAX_RETRIES);
        assert!(write_back.puts.lock().is_empty());
        assert!(write_back.local.lock().is_empty());
        assert_eq!(failures(&state).get("source_connect"), Some(&1));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_guarded_body_ends_a_stalled_stream_and_passes_chunks_through() {
        let (tx, rx) = mpsc::channel::<io::Result<Bytes>>(4);
        let body: SourceBody = Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
        let (mut guarded, idle) = idle_guarded_body(body, Duration::from_secs(2));

        tx.send(Ok(Bytes::from_static(b"chunk"))).await.expect("send a chunk");
        assert_eq!(guarded.next().await.expect("a chunk").expect("chunk is ok"), Bytes::from_static(b"chunk"));
        assert!(!idle.timed_out(), "a delivered chunk is not a stall");

        // The producer never sends again: the guard ends the stream itself.
        let started = tokio::time::Instant::now();
        let err = guarded
            .next()
            .await
            .expect("the guard yields the timeout")
            .expect_err("a stalled body must time out");
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() >= Duration::from_secs(2));
        assert!(idle.timed_out(), "the guard reports why the stream ended");
        assert!(guarded.next().await.is_none(), "the stream ends after the timeout");
        drop(tx);
    }

    /// A source that keeps producing, only slowly, must survive: the budget
    /// is per chunk, not for the whole body.
    #[tokio::test(start_paused = true)]
    async fn idle_guarded_body_accepts_a_slow_but_advancing_source() {
        let (tx, rx) = mpsc::channel::<io::Result<Bytes>>(1);
        let body: SourceBody = Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
        let (mut guarded, idle) = idle_guarded_body(body, Duration::from_secs(2));

        let producer = tokio::spawn(async move {
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(1_500)).await;
                if tx.send(Ok(Bytes::from_static(b"chunk"))).await.is_err() {
                    return;
                }
            }
        });

        let mut chunks = 0;
        while let Some(chunk) = guarded.next().await {
            chunk.expect("a source that keeps advancing must not time out");
            chunks += 1;
        }
        producer.await.expect("producer task");
        assert_eq!(chunks, 5);
        assert!(!idle.timed_out());
    }

    /// The budget measures the source, not the consumer: a reader that stops
    /// asking for bytes for far longer than the budget still gets the rest of
    /// the body once it resumes.
    #[tokio::test(start_paused = true)]
    async fn idle_guarded_body_does_not_time_out_a_slow_consumer() {
        let (tx, rx) = mpsc::channel::<io::Result<Bytes>>(4);
        let body: SourceBody = Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
        let (mut guarded, idle) = idle_guarded_body(body, Duration::from_secs(2));

        for _ in 0..3 {
            tx.send(Ok(Bytes::from_static(b"chunk"))).await.expect("send a chunk");
        }
        drop(tx);

        let mut chunks = 0;
        while let Some(chunk) = guarded.next().await {
            chunk.expect("a stalled consumer must not fail the source");
            chunks += 1;
            // Ten times the budget passes between reads.
            tokio::time::sleep(Duration::from_secs(20)).await;
        }
        assert_eq!(chunks, 3);
        assert!(!idle.timed_out(), "the consumer's own pace is not source idleness");
    }

    /// The inline path has no pump: the guard is what turns a stalled source
    /// into a `source_timeout` failure instead of a local write failure.
    #[tokio::test(start_paused = true)]
    async fn commit_inline_reports_a_stalled_source_as_a_source_timeout() {
        let sys = OnDemandMigrationSys::new();
        let mock = Arc::new(MockWriteBack::default());
        let mock_dyn: Arc<dyn OdmWriteBack> = mock.clone();
        sys.set_write_back(mock_dyn);
        let state = enabled_state(&sys, &config()).await;

        let (tx, rx) = mpsc::channel::<io::Result<Bytes>>(4);
        let body: SourceBody = Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
        let (mut guarded, idle) = idle_guarded_body(body, Duration::from_secs(1));
        tx.send(Ok(Bytes::from(body_bytes(100)))).await.expect("send the first chunk");
        // The source announced 300 bytes and then stops sending; holding the
        // sender keeps the stream open the way a stalled connection would.
        let keep_open = tx;

        // Stands in for the tee, which forwards the guarded source to the
        // write-back and is the reason the write-back only ever sees a plain
        // body read error.
        let (relay_tx, relay_rx) = mpsc::channel::<io::Result<Bytes>>(4);
        tokio::spawn(async move {
            while let Some(chunk) = guarded.next().await {
                let failed = chunk.is_err();
                if relay_tx.send(chunk).await.is_err() || failed {
                    return;
                }
            }
        });
        let write_body: WriteBackBody = Box::pin(tokio_stream::wrappers::ReceiverStream::new(relay_rx));

        // The inline path holds the singleflight slot across the commit, the
        // way `odm_get_inline` does.
        let PullSlot::Leader(leader) = state.acquire_pull_slot("stalled").await.expect("the first caller leads") else {
            panic!("the first caller must be the leader");
        };
        assert_eq!(state.stats().inflight_pulls(), 1);

        let err = commit_inline(&state, "stalled", head(300), None, write_body, &idle)
            .await
            .expect_err("a stalled source must not commit");
        assert_eq!(err.reason, PullFailureReason::SourceTimeout, "{err}");
        assert!(idle.timed_out());
        assert_eq!(failures(&state).get("source_timeout"), Some(&1));
        assert_eq!(failures(&state).get("local_write"), None, "a stalled source is not a local write failure");
        assert!(mock.local.lock().get("stalled").is_none(), "nothing is stored");

        leader.complete(Err(err));
        assert_eq!(state.stats().inflight_pulls(), 0, "the aborted pull releases its slot");
        assert_eq!(state.inflight_keys(), 0);
        drop(keep_open);
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_source_body_hits_the_idle_timeout() {
        let sys = OnDemandMigrationSys::new();
        let mut cfg = config();
        cfg.policy.source_timeout.idle_ms = 1_000;
        let state = enabled_state(&sys, &cfg).await;
        let source = MockSource::with_object("stall", 10, BodyKind::Hang);
        let write_back = Arc::new(MockWriteBack::default());
        let source_dyn: Arc<dyn PullSource> = source.clone();
        let write_back_dyn: Arc<dyn OdmWriteBack> = write_back.clone();

        let started = tokio::time::Instant::now();
        let err = pull(&state, &source_dyn, &write_back_dyn, "stall")
            .await
            .expect_err("a stalled body must time out");
        assert_eq!(err.reason, PullFailureReason::SourceTimeout, "{err}");
        assert!(started.elapsed() >= Duration::from_secs(4 + 21), "4 idle timeouts + 3 backoffs");
        assert!(write_back.local.lock().is_empty());
        assert_eq!(failures(&state).get("source_timeout"), Some(&1));
    }

    #[tokio::test]
    async fn multipart_path_splits_parts_and_aborts_on_failure() {
        let sys = OnDemandMigrationSys::new();
        let mut cfg = config();
        cfg.policy.multipart_part_size_bytes = 1024;
        let state = enabled_state(&sys, &cfg).await;
        let body = body_bytes(2500);
        let source = MockSource::with_object("big", 2500, BodyKind::Bytes(body.clone()));
        let write_back = Arc::new(MockWriteBack::default());
        let source_dyn: Arc<dyn PullSource> = source.clone();
        let write_back_dyn: Arc<dyn OdmWriteBack> = write_back.clone();

        let completion = pull(&state, &source_dyn, &write_back_dyn, "big")
            .await
            .expect("multipart write-back must succeed");
        let PullCompletion::Stored(outcome) = completion else {
            panic!("expected a stored object");
        };
        assert_eq!(outcome.size, 2500);
        let parts = write_back.parts.lock().clone();
        assert_eq!(parts.iter().map(|(_, n, _, _)| *n).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(parts.iter().map(|(_, _, size, _)| *size).collect::<Vec<_>>(), vec![1024, 1024, 452]);
        let joined: Vec<u8> = parts.iter().flat_map(|(_, _, _, bytes)| bytes.clone()).collect();
        assert_eq!(joined, body);
        {
            let completed = write_back.completed.lock();
            assert_eq!(completed.len(), 1);
            assert_eq!(completed[0].1.len(), 3);
        }
        assert!(write_back.aborted.lock().is_empty());
        assert!(write_back.puts.lock().is_empty(), "large objects never use the single-part path");
        assert_eq!(state.snapshot().stats.pulled_bytes_total, 2500);

        // A failing part aborts the upload and reports a local write failure.
        let failing = Arc::new(MockWriteBack {
            fail_part: Some(2),
            ..Default::default()
        });
        let failing_dyn: Arc<dyn OdmWriteBack> = failing.clone();
        let err = pull(&state, &source_dyn, &failing_dyn, "big")
            .await
            .expect_err("part failure must fail the pull");
        assert_eq!(err.reason, PullFailureReason::LocalWrite);
        assert_eq!(failing.aborted.lock().as_slice(), ["upload-0"]);
        assert!(failing.completed.lock().is_empty());
        assert!(failing.local.lock().is_empty());
        assert_eq!(failures(&state).get("local_write"), Some(&1));

        // Too many parts is rejected before any upload is created.
        let mut tiny = config();
        tiny.policy.multipart_part_size_bytes = 1;
        let sys2 = OnDemandMigrationSys::new();
        let state2 = enabled_state(&sys2, &tiny).await;
        let big = Arc::new(MockWriteBack::default());
        let big_dyn: Arc<dyn OdmWriteBack> = big.clone();
        let source2 = MockSource::with_object("huge", 20_000, BodyKind::Bytes(body_bytes(20_000)));
        let source2_dyn: Arc<dyn PullSource> = source2;
        let err = pull(&state2, &source2_dyn, &big_dyn, "huge")
            .await
            .expect_err("more than 10000 parts is unsupported");
        assert_eq!(err.reason, PullFailureReason::SourceUnsupported);
        assert!(big.uploads.lock().is_empty());
    }

    #[tokio::test]
    async fn local_object_checks_skip_the_pull_before_and_after_the_source_get() {
        let sys = OnDemandMigrationSys::new();
        let state = enabled_state(&sys, &config()).await;
        let source = MockSource::with_object("present", 4, BodyKind::Bytes(vec![1; 4]));
        let write_back = Arc::new(MockWriteBack::default());
        write_back.local.lock().insert(
            "present".to_string(),
            LocalObject {
                etag: Some("local".to_string()),
                size: 4,
                delete_marker: false,
            },
        );
        let source_dyn: Arc<dyn PullSource> = source.clone();
        let write_back_dyn: Arc<dyn OdmWriteBack> = write_back.clone();
        let completion = pull(&state, &source_dyn, &write_back_dyn, "present").await.expect("skip");
        assert!(matches!(completion, PullCompletion::AlreadyPresent(ref local) if local.etag.as_deref() == Some("local")));
        assert_eq!(source.head_calls.load(Ordering::SeqCst), 0);
        assert_eq!(source.get_calls.load(Ordering::SeqCst), 0);
        assert_eq!(pulled(&state, PullPath::Background), 0);

        // A client PUT that lands during the source GET wins over the copy.
        source.objects.lock().insert(
            "racy".to_string(),
            MockObject {
                head: head(4),
                body: BodyKind::Bytes(vec![2; 4]),
            },
        );
        *source.land_local_on_get.lock() = Some((write_back.clone(), "racy".to_string()));
        let completion = pull(&state, &source_dyn, &write_back_dyn, "racy").await.expect("skip");
        assert!(matches!(completion, PullCompletion::AlreadyPresent(ref local) if local.etag.as_deref() == Some("client-put")));
        assert_eq!(source.get_calls.load(Ordering::SeqCst), 1);
        assert!(write_back.puts.lock().is_empty(), "the source copy must not overwrite the client write");
        assert!(failures(&state).is_empty());
    }

    #[tokio::test]
    async fn copy_tags_policy_fetches_source_tags() {
        let sys = OnDemandMigrationSys::new();
        let mut cfg = config();
        cfg.policy.copy_tags = true;
        let state = enabled_state(&sys, &cfg).await;
        let source = MockSource {
            tags: HashMap::from([("env".to_string(), "prod".to_string())]),
            ..Default::default()
        };
        source.objects.lock().insert(
            "tagged".to_string(),
            MockObject {
                head: head(2),
                body: BodyKind::Bytes(vec![9, 9]),
            },
        );
        let source = Arc::new(source);
        let write_back = Arc::new(MockWriteBack::default());
        let source_dyn: Arc<dyn PullSource> = source.clone();
        let write_back_dyn: Arc<dyn OdmWriteBack> = write_back.clone();
        pull(&state, &source_dyn, &write_back_dyn, "tagged").await.expect("stored");
        assert_eq!(source.tag_calls.load(Ordering::SeqCst), 1);
        let puts = write_back.puts.lock();
        assert_eq!(puts[0].0.tags.as_ref().and_then(|tags| tags.get("env")).map(String::as_str), Some("prod"));
    }

    #[tokio::test]
    async fn commit_inline_accounts_the_inline_path_and_maps_write_errors() {
        let sys = OnDemandMigrationSys::new();
        let mock = Arc::new(MockWriteBack::default());
        let mock_dyn: Arc<dyn OdmWriteBack> = mock.clone();
        sys.set_write_back(mock_dyn.clone());
        let state = enabled_state(&sys, &config()).await;
        assert!(state.write_back().is_some(), "states capture the injected write-back");

        let body = body_bytes(300);
        let stream: WriteBackBody = Box::pin(futures::stream::iter(vec![Ok(Bytes::from(body.clone()))]));
        let outcome = commit_inline(&state, "inline", head(300), None, stream, &SourceIdleGuard::default())
            .await
            .expect("inline commit succeeds");
        assert_eq!(outcome.size, 300);
        assert_eq!(mock.puts.lock()[0].1, body);
        assert_eq!(pulled(&state, PullPath::Inline), 1);
        assert_eq!(pulled(&state, PullPath::Background), 0);
        assert_eq!(state.snapshot().stats.pulled_bytes_total, 300);

        *mock.forced_put_error.lock() = Some(WriteBackError::Integrity);
        let stream: WriteBackBody = Box::pin(futures::stream::iter(vec![Ok(Bytes::from(body.clone()))]));
        let err = commit_inline_with(&state, &mock_dyn, "inline", head(300), None, stream, &SourceIdleGuard::default())
            .await
            .expect_err("integrity failure surfaces");
        assert_eq!(err.reason, PullFailureReason::EtagMismatch);
        assert_eq!(failures(&state).get("etag_mismatch"), Some(&1));

        // A tee secondary that ends early (primary dropped) never commits.
        let stream: WriteBackBody = Box::pin(futures::stream::iter(vec![
            Ok(Bytes::from(body_bytes(100))),
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "tee primary dropped")),
        ]));
        let err = commit_inline(&state, "torn", head(300), None, stream, &SourceIdleGuard::default())
            .await
            .expect_err("a broken secondary must fail");
        assert_eq!(err.reason, PullFailureReason::LocalWrite);
        assert!(mock.local.lock().get("torn").is_none());

        // Without an injected write-back the inline path fails closed.
        let bare = OnDemandMigrationSys::new();
        let bare_state = enabled_state(&bare, &config()).await;
        assert!(bare_state.write_back().is_none());
        let stream: WriteBackBody = Box::pin(futures::stream::empty());
        let err = commit_inline(&bare_state, "x", head(0), None, stream, &SourceIdleGuard::default())
            .await
            .expect_err("no write-back");
        assert_eq!(err.reason, PullFailureReason::LocalWrite);
        assert_eq!(failures(&bare_state).get("local_write"), Some(&1));
    }

    #[tokio::test]
    async fn state_and_sys_enqueue_entry_points() {
        let sys = OnDemandMigrationSys::new();
        assert_eq!(sys.enqueue_pull("missing", "k", PullReason::Backfill), EnqueueOutcome::Unavailable);
        let state = enabled_state(&sys, &config()).await;
        // No write-back injected: the lazily started queue is unavailable.
        assert!(state.pull_queue().is_none());
        assert_eq!(state.enqueue_pull("k", PullReason::Backfill), EnqueueOutcome::Unavailable);

        let source = MockSource::with_object("k", 3, BodyKind::Bytes(vec![1, 2, 3]));
        let write_back = Arc::new(MockWriteBack::default());
        let queue = PullQueue::start(Arc::clone(&state), source.clone(), write_back.clone());
        assert!(state.pull_queue.set(Arc::clone(&queue)).is_ok());
        assert!(Arc::ptr_eq(&state.pull_queue().expect("seeded"), &queue));
        assert_eq!(sys.enqueue_pull(BUCKET, "k", PullReason::Backfill), EnqueueOutcome::Enqueued);
        assert_eq!(state.enqueue_pull("k", PullReason::Backfill), EnqueueOutcome::Coalesced);
        wait_until("backfill job", || queue.pending_keys() == 0).await;
        assert_eq!(pulled(&state, PullPath::Backfill), 1);
        assert_eq!(
            format!("{queue:?}"),
            format!("PullQueue {{ bucket: {BUCKET:?}, capacity: 1024, pending: 0, stopped: false }}")
        );
        sys.remove(BUCKET);
        queue.wait_until_stopped().await;
    }

    #[tokio::test]
    async fn part_body_slices_the_shared_source_at_part_boundaries() {
        let (tx, rx) = mpsc::channel(4);
        tx.send(Ok(Bytes::from(body_bytes(300)))).await.expect("send");
        tx.send(Ok(Bytes::from(body_bytes(500)))).await.expect("send");
        drop(tx);
        let shared: SharedSourceHandle = Arc::new(Mutex::new(SharedSource { rx, leftover: None }));
        let mut expected = body_bytes(300);
        expected.extend(body_bytes(500));

        let first = drain(PartBody::boxed(&shared, 600), 600).await.expect("first part");
        assert_eq!(first, expected[..600]);
        let second = drain(PartBody::boxed(&shared, 200), 200).await.expect("second part");
        assert_eq!(second, expected[600..]);
        let err = drain(PartBody::boxed(&shared, 1), 1).await.expect_err("source exhausted");
        assert!(
            err.to_string().contains("UnexpectedEof") || err.to_string().contains("outstanding"),
            "{err}"
        );
        let empty = drain(PartBody::boxed(&shared, 0), 0).await.expect("zero-length part");
        assert!(empty.is_empty());
    }

    #[test]
    fn reasons_paths_and_error_classes_are_fixed() {
        assert_eq!(PullReason::RangeGet.path(), PullPath::Background);
        assert_eq!(PullReason::LargeObject.path(), PullPath::Background);
        assert_eq!(PullReason::Backfill.path(), PullPath::Backfill);
        assert_eq!(PullReason::RangeGet.as_str(), "range_get");
        assert_eq!(WriteBackError::Integrity.reason(), PullFailureReason::EtagMismatch);
        assert_eq!(WriteBackError::Quota("full".into()).reason(), PullFailureReason::Quota);
        assert_eq!(WriteBackError::Local("x".into()).reason(), PullFailureReason::LocalWrite);
        assert_eq!(WriteBackError::Unsupported("x".into()).reason(), PullFailureReason::SourceUnsupported);
        for (retry, base) in PULL_RETRY_BASE_DELAYS.iter().enumerate() {
            let delay = retry_delay(retry);
            assert!(delay >= *base && delay <= *base + *base / 4, "{delay:?}");
        }
    }
}
