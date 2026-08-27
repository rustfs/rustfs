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

//! GetObject / GetObjectAttributes read path: cold fill, resume, stream tuning.

use super::*;

struct ColdFillDiskPermitMetric {
    owner: ColdFillDiskPermitOwner,
    metric_recorded: bool,
}

#[cfg(test)]
static COLD_FILL_FOLLOWER_DISK_PERMITS_FOR_TEST: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
struct ColdFillPublicationBarrier {
    reached: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

#[cfg(test)]
type ColdFillPublicationBarrierState = Option<(rustfs_object_data_cache::ObjectDataCacheKey, Arc<ColdFillPublicationBarrier>)>;

#[cfg(test)]
static COLD_FILL_PUBLICATION_BARRIER: OnceLock<Mutex<ColdFillPublicationBarrierState>> = OnceLock::new();

#[cfg(test)]
type ColdFillReaderOpenProbeState = Option<(rustfs_object_data_cache::ObjectDataCacheKey, Arc<AtomicU64>)>;

#[cfg(test)]
static COLD_FILL_READER_OPEN_PROBE: OnceLock<Mutex<ColdFillReaderOpenProbeState>> = OnceLock::new();

fn adjust_cold_fill_disk_permit_metric(owner: ColdFillDiskPermitOwner, acquired: bool) {
    macro_rules! adjust_gauge {
        ($name:literal) => {{
            #[cfg(not(test))]
            let gauge = {
                static HANDLE: std::sync::LazyLock<metrics::Gauge> = std::sync::LazyLock::new(|| metrics::gauge!($name));
                &*HANDLE
            };
            #[cfg(test)]
            let gauge = metrics::gauge!($name);
            if acquired {
                gauge.increment(1.0);
            } else {
                gauge.decrement(1.0);
            }
        }};
    }

    match owner {
        ColdFillDiskPermitOwner::Producer => {
            adjust_gauge!("rustfs_object_data_cache_cold_fill_producer_disk_permits");
        }
        ColdFillDiskPermitOwner::Follower => {
            adjust_gauge!("rustfs_object_data_cache_cold_fill_follower_disk_permits");
        }
    }
}

#[cfg(test)]
async fn wait_cold_fill_publication_barrier(plan: &rustfs_object_data_cache::ObjectDataCacheGetPlan) {
    let Some(key) = plan.key() else {
        return;
    };
    let barrier = COLD_FILL_PUBLICATION_BARRIER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .filter(|(barrier_key, _)| barrier_key == key)
        .map(|(_, barrier)| Arc::clone(barrier));
    if let Some(barrier) = barrier {
        barrier.reached.add_permits(1);
        if let Ok(permit) = barrier.release.acquire().await {
            permit.forget();
        }
    }
}

#[cfg(test)]
fn record_cold_fill_reader_open_for_test(plan: &rustfs_object_data_cache::ObjectDataCacheGetPlan) {
    let Some(key) = plan.key() else {
        return;
    };
    let probe = COLD_FILL_READER_OPEN_PROBE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .filter(|(probe_key, _)| probe_key == key)
        .map(|(_, count)| Arc::clone(count));
    if let Some(count) = probe {
        count.fetch_add(1, Ordering::Relaxed);
    }
}

impl ColdFillDiskPermitMetric {
    fn new(owner: ColdFillDiskPermitOwner) -> Self {
        let metric_recorded = rustfs_io_metrics::metrics_enabled();
        if metric_recorded {
            adjust_cold_fill_disk_permit_metric(owner, true);
        }
        #[cfg(test)]
        if matches!(owner, ColdFillDiskPermitOwner::Follower) {
            COLD_FILL_FOLLOWER_DISK_PERMITS_FOR_TEST.fetch_add(1, Ordering::Relaxed);
        }
        Self { owner, metric_recorded }
    }
}

impl Drop for ColdFillDiskPermitMetric {
    fn drop(&mut self) {
        if self.metric_recorded {
            adjust_cold_fill_disk_permit_metric(self.owner, false);
        }
        #[cfg(test)]
        if matches!(self.owner, ColdFillDiskPermitOwner::Follower) {
            COLD_FILL_FOLLOWER_DISK_PERMITS_FOR_TEST.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

struct GetObjectDiskPermit {
    permit: Option<OwnedSemaphorePermit>,
    metric: Option<ColdFillDiskPermitMetric>,
}

impl GetObjectDiskPermit {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        Self {
            permit: Some(permit),
            metric: current_cold_fill_disk_permit_owner().map(ColdFillDiskPermitMetric::new),
        }
    }

    fn release(&mut self) {
        self.permit.take();
        self.metric.take();
    }
}

impl From<OwnedSemaphorePermit> for GetObjectDiskPermit {
    fn from(permit: OwnedSemaphorePermit) -> Self {
        Self::new(permit)
    }
}

impl Drop for GetObjectDiskPermit {
    fn drop(&mut self) {
        self.release();
    }
}

const COLD_FILL_HARD_MAX_DURATION: Duration = Duration::from_secs(10 * 60);

pub(crate) const MAX_GET_OBJECT_MEMORY_BUFFER_BYTES: i64 = 64 * 1024 * 1024;

const MEDIUM_CONCURRENCY_GET_OBJECT_MEMORY_BUFFER_BYTES: i64 = 8 * 1024 * 1024;

const HIGH_CONCURRENCY_GET_OBJECT_MEMORY_BUFFER_BYTES: i64 = 4 * 1024 * 1024;

const VERY_HIGH_CONCURRENCY_GET_OBJECT_MEMORY_BUFFER_BYTES: i64 = 1024 * 1024;

const EVENT_GET_OBJECT_STREAM_BODY: &str = "get_object_stream_body";

const GET_OBJECT_STAGE_PATH_S3_HANDLER: &str = "s3_handler";

const GET_OBJECT_STAGE_REQUEST_INGRESS_TO_CONTEXT: &str = "request_ingress_to_context";

const GET_OBJECT_STAGE_OUTPUT_STRATEGY: &str = "output_strategy";

const GET_OBJECT_STAGE_BODY_BUILD: &str = "body_build";

const GET_OBJECT_STAGE_BODY_ENCRYPTED_BUFFER_READ: &str = "body_encrypted_buffer_read";

const GET_OBJECT_STAGE_BODY_MEMORY_BLOB: &str = "body_memory_blob";

const GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ: &str = "body_seek_buffer_read";

const GET_OBJECT_STAGE_BODY_STREAM_STRATEGY: &str = "body_stream_strategy";

const GET_OBJECT_STAGE_BODY_STREAMING_BLOB: &str = "body_streaming_blob";

const GET_OBJECT_STAGE_CHECKSUM_HEADERS: &str = "checksum_headers";

const GET_OBJECT_STAGE_LIFECYCLE_EXPIRATION: &str = "lifecycle_expiration";

const GET_OBJECT_STAGE_METADATA_FILTER: &str = "metadata_filter";

const GET_OBJECT_STREAM_WARN_THRESHOLD: Duration = Duration::from_secs(5);

static GET_OBJECT_BUFFER_THRESHOLD_WARNED: AtomicBool = AtomicBool::new(false);

fn record_get_object_s3_handler_stage_duration(stage: &'static str, start: Option<std::time::Instant>) {
    if let Some(start) = start {
        rustfs_io_metrics::record_get_object_stage_duration(
            GET_OBJECT_STAGE_PATH_S3_HANDLER,
            stage,
            start.elapsed().as_secs_f64(),
        );
    }
}

struct GetObjectBootstrap {
    timeout_config: GetObjectTimeoutPolicy,
    wrapper: RequestTimeoutWrapper,
    request_start: std::time::Instant,
    request_guard: GetObjectGuard,
    _deadlock_request_guard: Option<DeadlockRequestGuard>,
    concurrent_requests: usize,
}

struct GetObjectIoPlanning {
    /// `None` when inline fast path skips disk I/O semaphore.
    disk_permit: Option<GetObjectDiskPermit>,
    permit_wait_duration: Duration,
    queue_status: concurrency::IoQueueStatus,
    queue_utilization: f64,
}

#[derive(Clone, Copy)]
struct GetObjectRequestTimeout<'a> {
    wrapper: &'a RequestTimeoutWrapper,
    policy: &'a GetObjectTimeoutPolicy,
}

struct GetObjectRequestContext {
    bucket: String,
    key: String,
    version_id_for_event: String,
    part_number: Option<usize>,
    rs: Option<HTTPRangeSpec>,
    opts: ObjectOptions,
}

/// Request fields that passed the cheap GET validations, ready for the
/// bucket-metadata work in [`DefaultObjectUsecase::prepare_get_object_request_context`].
struct GetObjectValidatedRequest {
    bucket: String,
    key: String,
    version_id: Option<String>,
    part_number: Option<usize>,
    rs: Option<HTTPRangeSpec>,
}

struct GetObjectReadSetup {
    info: ObjectInfo,
    final_stream: DynReader,
    buffered_body: Option<Bytes>,
    /// ODC-16: `buffered_body` is the body the ecstore cache hook served, so the
    /// app layer serves it as the object-data-cache source without a re-lookup.
    cache_hook_served: bool,
    /// ODC-16: the cache hook probed this read (served or missed), so the app
    /// layer must skip its own lookup.
    cache_hook_probed: bool,
    cache_fill_allowed: bool,
    rs: Option<HTTPRangeSpec>,
    content_type: Option<ContentType>,
    last_modified: Option<Timestamp>,
    response_content_length: i64,
    content_range: Option<String>,
    server_side_encryption: Option<ServerSideEncryption>,
    sse_customer_algorithm: Option<SSECustomerAlgorithm>,
    sse_customer_key_md5: Option<SSECustomerKeyMD5>,
    ssekms_key_id: Option<SSEKMSKeyId>,
    encryption_applied: bool,
    /// Resolved plaintext start offset of the committed response body
    /// (`get_offset_length` output; 0 for a full-object read). Feeds the
    /// mid-stream resume offset.
    resume_range_start: i64,
    /// Resolved inclusive plaintext end offset of the committed response body;
    /// -1 when the committed body runs to the end of the object.
    resume_range_end: i64,
}

struct GetObjectPreparedRead {
    io_planning: GetObjectIoPlanning,
    read_setup: GetObjectReadSetup,
}

struct GetObjectStrategyContext {
    #[allow(dead_code, reason = "written but never read back (backlog#1823)")]
    io_strategy: concurrency::IoStrategy,
    optimal_buffer_size: usize,
    enable_readahead: bool,
}

struct GetObjectOutputContext {
    output: GetObjectOutput,
    event_info: Option<ObjectInfo>,
    response_content_length: i64,
    optimal_buffer_size: usize,
    extra_checksum_headers: Vec<(&'static str, String)>,
}

enum GetObjectTimeoutStage {
    BeforeProcessing,
    DiskPermitWait { permit_wait_duration: Duration },
    BeforeRead,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GetObjectStreamStrategy {
    Standard,
    LargeSequentialReadahead,
}

impl GetObjectStreamStrategy {
    fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::LargeSequentialReadahead => "large_sequential_readahead",
        }
    }
}

const LARGE_SEQUENTIAL_GET_THRESHOLD_BYTES: i64 = 1024 * 1024 * 1024;

const LARGE_SEQUENTIAL_GET_STREAM_BUFFER_CAP_BYTES: usize = 4 * MI_B;

const LARGE_SEQUENTIAL_GET_READAHEAD_MULTIPLIER: usize = 2;

const LARGE_BODY_READER_STREAM_BUFFER_FLOOR_BYTES: usize = MI_B;

const LARGE_BODY_READER_STREAM_BUFFER_THRESHOLD_BYTES: i64 = 4 * MI_B as i64;

const MID_BODY_READER_STREAM_BUFFER_FLOOR_BYTES: usize = 512 * 1024;

const MID_BODY_READER_STREAM_BUFFER_THRESHOLD_BYTES: i64 = MI_B as i64;

const ENV_RUSTFS_GET_SEEK_BUFFER_ENABLE: &str = "RUSTFS_GET_SEEK_BUFFER_ENABLE";

const ENV_RUSTFS_GET_READER_STREAM_BUFFER_SIZE: &str = "RUSTFS_GET_READER_STREAM_BUFFER_SIZE";

const ENV_RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE: &str = "RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE";

const ENV_RUSTFS_GET_SMALL_BODY_ONCE_ENABLE: &str = "RUSTFS_GET_SMALL_BODY_ONCE_ENABLE";

const GET_READER_STREAM_BUFFER_SOURCE_SELECTED: &str = "selected";

const GET_READER_STREAM_BUFFER_SOURCE_ENV_OVERRIDE: &str = "env_override";

const GET_READER_STREAM_POLL_PENDING: &str = "pending";

const GET_READER_STREAM_POLL_READY_DATA: &str = "ready_data";

const GET_READER_STREAM_POLL_READY_EMPTY: &str = "ready_empty";

const GET_READER_STREAM_POLL_READY_ERROR: &str = "ready_error";

const GET_STREAMING_BODY_FAILURE_STAGE_READER_STREAM: &str = "reader_stream";

const GET_STREAMING_BODY_FAILURE_REASON_READER_ERROR: &str = "reader_error";

const GET_STREAMING_BODY_FAILURE_REASON_SHORT_EOF: &str = "short_eof";

const GET_MEMORY_BODY_SOURCE_BUFFERED_BODY: &str = "buffered_body";

const GET_MEMORY_BODY_SOURCE_OBJECT_DATA_CACHE: &str = "object_data_cache";

const GET_MEMORY_BODY_SOURCE_OBJECT_DATA_CACHE_MATERIALIZED: &str = "object_data_cache_materialized";

const GET_MEMORY_BODY_SOURCE_SEEK_BUFFER: &str = "seek_buffer";

const GET_MEMORY_BODY_SOURCE_ENCRYPTED_BUFFER: &str = "encrypted_buffer";

const GET_OBJECT_STAGE_BODY_CACHE_MATERIALIZE_READ: &str = "body_cache_materialize_read";

fn get_reader_stream_buffer_size_override() -> Option<usize> {
    static GET_READER_STREAM_BUFFER_SIZE_OVERRIDE: OnceLock<Option<usize>> = OnceLock::new();
    *GET_READER_STREAM_BUFFER_SIZE_OVERRIDE.get_or_init(|| {
        std::env::var(ENV_RUSTFS_GET_READER_STREAM_BUFFER_SIZE)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
    })
}

fn is_get_output_handoff_attribution_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| rustfs_utils::get_env_bool(ENV_RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE, false))
}

fn is_get_small_body_once_enabled() -> bool {
    #[cfg(test)]
    {
        rustfs_utils::get_env_bool(ENV_RUSTFS_GET_SMALL_BODY_ONCE_ENABLE, false)
    }
    #[cfg(not(test))]
    {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| rustfs_utils::get_env_bool(ENV_RUSTFS_GET_SMALL_BODY_ONCE_ENABLE, false))
    }
}

fn is_get_seek_buffer_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| rustfs_utils::get_env_bool(ENV_RUSTFS_GET_SEEK_BUFFER_ENABLE, false))
}

fn resolve_reader_stream_buffer_size(selected_size: usize, override_size: Option<usize>) -> (usize, &'static str) {
    if let Some(override_size) = override_size.filter(|value| *value > 0) {
        return (override_size, GET_READER_STREAM_BUFFER_SOURCE_ENV_OVERRIDE);
    }

    (selected_size.max(1), GET_READER_STREAM_BUFFER_SOURCE_SELECTED)
}

fn tune_reader_stream_buffer_size(
    selected_size: usize,
    response_content_length: i64,
    stream_strategy: GetObjectStreamStrategy,
) -> usize {
    if stream_strategy == GetObjectStreamStrategy::Standard
        && response_content_length >= LARGE_BODY_READER_STREAM_BUFFER_THRESHOLD_BYTES
    {
        return selected_size.max(LARGE_BODY_READER_STREAM_BUFFER_FLOOR_BYTES);
    }

    if stream_strategy == GetObjectStreamStrategy::Standard
        && response_content_length >= MID_BODY_READER_STREAM_BUFFER_THRESHOLD_BYTES
    {
        return selected_size.max(MID_BODY_READER_STREAM_BUFFER_FLOOR_BYTES);
    }

    selected_size
}

fn get_object_stream_size_bucket(expected: usize) -> &'static str {
    rustfs_io_metrics::get_object_size_bucket(i64::try_from(expected).unwrap_or(i64::MAX))
}

fn classify_get_object_stream_read_error(err: &std::io::Error) -> &'static str {
    if let Some(inner) = err.get_ref() {
        if inner.is::<rustfs_rio::IncompleteBody>() {
            return "short_eof";
        }

        if inner.is::<rustfs_rio::ChecksumMismatch>() {
            return "bitrot";
        }

        let error_msg = inner.to_string().to_lowercase();
        if error_msg.contains("bitrot") {
            return "bitrot";
        }
        if error_msg.contains("read quorum") || error_msg.contains("insufficient read quorum") || error_msg.contains("erasure") {
            return "read_quorum";
        }
    }

    match err.kind() {
        std::io::ErrorKind::UnexpectedEof => "short_eof",
        std::io::ErrorKind::TimedOut => "timeout",
        std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData => "range_or_length_invalid",
        _ => "io",
    }
}

fn get_object_stream_failure_reason(error_class: &'static str) -> &'static str {
    if error_class == "short_eof" {
        GET_STREAMING_BODY_FAILURE_REASON_SHORT_EOF
    } else {
        GET_STREAMING_BODY_FAILURE_REASON_READER_ERROR
    }
}

fn record_get_object_reader_stream_failure(
    reason: &'static str,
    error_class: &'static str,
    strategy: &'static str,
    buffer_source: &'static str,
    expected: usize,
    emitted: usize,
    remaining: usize,
) {
    rustfs_io_metrics::record_get_object_streaming_body_failure(rustfs_io_metrics::GetObjectStreamingBodyFailure {
        stage: GET_STREAMING_BODY_FAILURE_STAGE_READER_STREAM,
        reason,
        error_class,
        strategy,
        buffer_source,
        size_bucket: get_object_stream_size_bucket(expected),
        emitted_bytes: emitted,
        remaining_bytes: remaining,
    });
}

struct MemoryTrackedBytesStream {
    bytes: Option<Bytes>,
    emitted: bool,
    completed: bool,
    expected: usize,
    /// Set when the materialized buffer length disagrees with the declared
    /// content length. Such a body would be truncated (short) or over-long
    /// relative to the already-committed `Content-Length`, so the stream must
    /// surface an error instead of a clean short/over-long body. See #1324.
    length_mismatch: bool,
    started: std::time::Instant,
    source: &'static str,
    _guard: Option<rustfs_io_metrics::MemoryGaugeGuard>,
    lifecycle: GetObjectBodyLifecycle,
}

struct MemoryOnceBodyOwner {
    bytes: Bytes,
    _guard: Option<rustfs_io_metrics::MemoryGaugeGuard>,
    // Body::Once has no poll hook, so this opt-in path only holds the request
    // guard until the bytes are dropped; the result status remains unknown.
    _lifecycle: GetObjectBodyLifecycle,
}

impl MemoryOnceBodyOwner {
    fn new(bytes: Bytes, guard: Option<rustfs_io_metrics::MemoryGaugeGuard>, lifecycle: GetObjectBodyLifecycle) -> Self {
        Self {
            bytes,
            _guard: guard,
            _lifecycle: lifecycle,
        }
    }
}

impl AsRef<[u8]> for MemoryOnceBodyOwner {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

#[derive(Default)]
struct GetObjectBodyLifecycle {
    request_guard: Option<GetObjectGuard>,
}

impl GetObjectBodyLifecycle {
    fn tracked(request_guard: GetObjectGuard) -> Self {
        Self {
            request_guard: Some(request_guard),
        }
    }

    #[cfg(test)]
    fn disabled() -> Self {
        Self { request_guard: None }
    }

    fn is_finished(&self) -> bool {
        self.request_guard.is_none()
    }

    fn finish_ok(&mut self) {
        if let Some(mut request_guard) = self.request_guard.take() {
            request_guard.finish_ok();
        }
    }

    fn finish_err(&mut self) {
        if let Some(mut request_guard) = self.request_guard.take() {
            request_guard.finish_err();
        }
    }
}

pin_project! {
    // Keep the disk-read admission permit tied to the response body. This is
    // intentionally conservative backpressure: a streaming GET should occupy a
    // read slot until the client drains or drops the body.
    struct DiskReadPermitReader<R> {
        #[pin]
        inner: R,
        disk_permit: Option<GetObjectDiskPermit>,
    }
}

impl<R> DiskReadPermitReader<R> {
    fn new(inner: R, disk_permit: GetObjectDiskPermit) -> Self {
        Self {
            inner,
            disk_permit: Some(disk_permit),
        }
    }
}

impl<R> AsyncRead for DiskReadPermitReader<R>
where
    R: AsyncRead,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();
        let had_capacity = buf.remaining() > 0;
        let filled_before = buf.filled().len();
        let poll = this.inner.poll_read(cx, buf);
        // EOF: no more disk reads can happen through this stream, so release
        // the permit instead of holding it until the client drops the body.
        if had_capacity
            && matches!(poll, Poll::Ready(Ok(())))
            && buf.filled().len() == filled_before
            && let Some(mut disk_permit) = this.disk_permit.take()
        {
            disk_permit.release();
        }
        poll
    }
}

pin_project! {
    struct GetObjectReaderStream<R> {
        #[pin]
        reader: Option<R>,
        capacity: usize,
        strategy: &'static str,
        buffer_source: &'static str,
        remaining: usize,
        emitted: usize,
        expected: usize,
        // Diagnostic-only identity for the body this stream is serving. Unset in
        // unit tests that drive the stream over a bare reader; every production
        // body carries it via `with_diagnostics`.
        diagnostics: GetObjectReaderStreamDiagnostics,
    }
}

/// Object identity carried alongside a streaming GET body purely so a
/// mid-stream failure names the object it happened on.
#[derive(Clone, Default)]
struct GetObjectReaderStreamDiagnostics {
    bucket: String,
    object: String,
    request_id: String,
}

impl MemoryTrackedBytesStream {
    fn new(
        bytes: Bytes,
        expected: usize,
        source: &'static str,
        guard: Option<rustfs_io_metrics::MemoryGaugeGuard>,
        lifecycle: GetObjectBodyLifecycle,
    ) -> Self {
        let length_mismatch = bytes.len() != expected;
        Self {
            bytes: Some(bytes),
            emitted: false,
            completed: !length_mismatch && expected == 0,
            expected,
            length_mismatch,
            started: std::time::Instant::now(),
            source,
            _guard: guard,
            lifecycle,
        }
    }

    fn finish_ok(&mut self) {
        self.completed = true;
        self.lifecycle.finish_ok();
    }

    fn finish_err(&mut self) {
        self.lifecycle.finish_err();
    }
}

impl<R> GetObjectReaderStream<R>
where
    R: AsyncRead,
{
    fn new(reader: R, capacity: usize, remaining: usize, strategy: &'static str, buffer_source: &'static str) -> Self {
        if is_get_output_handoff_attribution_enabled() {
            rustfs_io_metrics::record_get_object_reader_stream_buffer_size(strategy, buffer_source, capacity);
        }
        Self {
            reader: Some(reader),
            capacity,
            strategy,
            buffer_source,
            remaining,
            emitted: 0,
            expected: remaining,
            diagnostics: GetObjectReaderStreamDiagnostics::default(),
        }
    }

    /// Attach the object identity a failed body should be reported against.
    fn with_diagnostics(mut self, bucket: &str, object: &str, request_id: &str) -> Self {
        self.diagnostics = GetObjectReaderStreamDiagnostics {
            bucket: bucket.to_string(),
            object: object.to_string(),
            request_id: request_id.to_string(),
        };
        self
    }
}

impl futures::Stream for MemoryTrackedBytesStream {
    type Item = Result<Bytes, S3StdError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let poll_start = is_get_output_handoff_attribution_enabled().then(std::time::Instant::now);
        if this.emitted {
            if let Some(poll_start) = poll_start {
                rustfs_io_metrics::record_get_object_memory_body_stream_poll(
                    this.source,
                    GET_READER_STREAM_POLL_READY_EMPTY,
                    0,
                    poll_start.elapsed().as_secs_f64(),
                );
            }
            return Poll::Ready(None);
        }

        // Strict materialization guard (#1324): a body whose length disagrees
        // with the declared content length must fail the transfer rather than be
        // delivered as a clean short body (truncation) or an over-long body
        // (protocol violation). The HTTP layer has already committed to
        // `Content-Length == expected`, so there is no safe way to serve a
        // differently sized body. This is a defense-in-depth backstop; the
        // buffered/cache callers reject the mismatch before headers are sent.
        if this.length_mismatch {
            let actual = this.bytes.as_ref().map_or(0, Bytes::len);
            this.emitted = true;
            this.finish_err();
            return Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("materialized GET body length mismatch: expected {}, got {}", this.expected, actual),
            )
            .into())));
        }

        let Some(bytes) = this.bytes.take() else {
            return Poll::Ready(None);
        };
        let bytes_len = bytes.len();
        let first_byte_elapsed = (!bytes.is_empty()).then(|| this.started.elapsed());
        this.emitted = true;
        if let Some(elapsed) = first_byte_elapsed {
            rustfs_io_metrics::record_get_object_first_byte_latency(GET_OBJECT_STAGE_PATH_S3_HANDLER, elapsed.as_secs_f64());
        }
        if bytes_len >= this.expected {
            this.finish_ok();
        }
        if let Some(poll_start) = poll_start {
            rustfs_io_metrics::record_get_object_memory_body_stream_poll(
                this.source,
                GET_READER_STREAM_POLL_READY_DATA,
                bytes_len,
                poll_start.elapsed().as_secs_f64(),
            );
        }
        Poll::Ready(Some(Ok(bytes)))
    }
}

impl ByteStream for MemoryTrackedBytesStream {
    fn remaining_length(&self) -> RemainingLength {
        if self.emitted || self.bytes.is_none() {
            RemainingLength::new_exact(0)
        } else {
            RemainingLength::new_exact(self.expected)
        }
    }
}

impl Drop for MemoryTrackedBytesStream {
    fn drop(&mut self) {
        if self.lifecycle.is_finished() {
            return;
        }

        if self.completed {
            self.finish_ok();
        } else {
            self.finish_err();
        }
    }
}

/// Failure modes of strictly materializing an object body into memory (#1324).
#[derive(Debug)]
enum StrictMaterializeError {
    /// The reader produced a different number of bytes than the declared content
    /// length (short or over-long). The response has already committed to
    /// `Content-Length == expected`, so any other length is an unrecoverable,
    /// broken HTTP response and must fail before headers are sent.
    LengthMismatch { expected: usize, actual: usize },
    /// A read error occurred after `consumed` bytes were already drained from the
    /// reader. The caller MUST NOT fall back to streaming the same reader: the
    /// drained prefix is gone, so streaming would ship a body missing its prefix
    /// (the seek-buffer prefix-misalignment bug this issue closes).
    Read { consumed: usize, source: std::io::Error },
}

impl std::fmt::Display for StrictMaterializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LengthMismatch { expected, actual, .. } => {
                write!(f, "materialized length mismatch: expected {expected}, got {actual}")
            }
            Self::Read { consumed, source } => {
                write!(f, "read failed after {consumed} bytes: {source}")
            }
        }
    }
}

impl StrictMaterializeError {
    fn into_storage_error(self) -> StorageError {
        match self {
            Self::LengthMismatch { expected, actual, .. } if actual < expected => StorageError::LessData,
            Self::LengthMismatch { .. } => StorageError::MoreData,
            Self::Read { source, .. } if source.kind() == std::io::ErrorKind::TimedOut => StorageError::Timeout,
            Self::Read { source, .. } => StorageError::Io(std::io::Error::new(source.kind(), "object body read failed")),
        }
    }

    fn into_s3_error(self, _response_content_length: i64) -> S3Error {
        ApiError::from(self.into_storage_error()).into()
    }
}

/// Strictly materialize an object body into memory, enforcing an exact-length
/// contract (#1324).
///
/// Reads at most `expected + 1` bytes so an over-long stream is detected without
/// buffering it unbounded, then requires `bytes_read == expected`. A short read
/// (clean EOF before `expected`), an over-long read, or a mid-stream read error
/// all return an error; only an exact-length read yields the buffer. Because the
/// HTTP response commits to `Content-Length == expected` before the body is
/// produced, this mirrors the streaming path (which already fails a short read
/// with `UnexpectedEof`) and the ODC materialize-fill path, closing the
/// warn-and-serve holes in the encrypted, seek, and cache memory branches.
///
/// On error the reader has already been (partially) consumed, so callers must
/// propagate the error rather than fall back to streaming the same reader.
async fn strict_materialize_object_body<R>(
    reader: R,
    expected: usize,
    stage: &'static str,
) -> Result<Vec<u8>, StrictMaterializeError>
where
    R: AsyncRead + Unpin,
{
    // Stop filling before the Vec reaches capacity. Calling `read_to_end` on a
    // bounded reader can still reserve beyond `expected` before observing EOF.
    // The over-long probe below stays outside this Vec so the admitted body
    // allocation remains exactly `expected` bytes.
    let mut buf = Vec::with_capacity(expected);
    let mut reader = reader;
    let read_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
    let read_result = loop {
        if buf.len() == expected {
            break Ok(());
        }
        match tokio::io::AsyncReadExt::read_buf(&mut reader, &mut buf).await {
            Ok(0) => break Ok(()),
            Ok(_) => {}
            Err(source) => break Err(source),
        }
    };
    let actual = buf.len();
    let probe_result = if read_result.is_ok() && actual == expected {
        let mut probe = [0_u8; 1];
        tokio::io::AsyncReadExt::read(&mut reader, &mut probe).await
    } else {
        Ok(0)
    };
    record_get_object_s3_handler_stage_duration(stage, read_start);
    match (read_result, probe_result) {
        (Ok(_), Ok(extra)) => {
            let actual = actual.saturating_add(extra);
            if actual == expected {
                Ok(buf)
            } else {
                Err(StrictMaterializeError::LengthMismatch { expected, actual })
            }
        }
        (Err(source), _) | (_, Err(source)) => Err(StrictMaterializeError::Read {
            consumed: actual,
            source,
        }),
    }
}

struct ColdFillProducerExecution {
    expected: usize,
    deadline: Option<tokio::time::Instant>,
    adapter: Arc<ObjectDataCacheAdapter>,
    engine_plan: rustfs_object_data_cache::ObjectDataCacheGetPlan,
}

enum ColdFillStartupWaitError {
    Cancelled,
    DeadlineExceeded,
}

async fn await_cold_fill_startup<F>(
    future: F,
    cancellation: &tokio_util::sync::CancellationToken,
    deadline: Option<tokio::time::Instant>,
) -> Result<F::Output, ColdFillStartupWaitError>
where
    F: Future,
{
    tokio::pin!(future);
    match deadline {
        Some(deadline) => {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err(ColdFillStartupWaitError::Cancelled),
                result = tokio::time::timeout_at(deadline, &mut future) => {
                    result.map_err(|_| ColdFillStartupWaitError::DeadlineExceeded)
                }
            }
        }
        None => {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err(ColdFillStartupWaitError::Cancelled),
                result = &mut future => Ok(result),
            }
        }
    }
}

async fn start_cold_fill_producer<AcquireIo, AcquireIoFuture, OpenReader, OpenReaderFuture>(
    producer: ColdFillProducer,
    reservation: Option<rustfs_object_data_cache::ObjectDataCacheBodyReservation>,
    acquire_io: AcquireIo,
    open_reader: OpenReader,
    execution: ColdFillProducerExecution,
) where
    AcquireIo: FnOnce() -> AcquireIoFuture,
    AcquireIoFuture: Future<Output = Result<GetObjectIoPlanning, ColdFillError>>,
    OpenReader: FnOnce() -> OpenReaderFuture,
    OpenReaderFuture: Future<Output = Result<GetObjectReader, StorageError>>,
{
    let ColdFillProducerExecution {
        expected,
        deadline,
        adapter,
        engine_plan,
    } = execution;
    let hard_deadline = tokio::time::Instant::now() + COLD_FILL_HARD_MAX_DURATION;
    let deadline = deadline.map_or(hard_deadline, |request_deadline| request_deadline.min(hard_deadline));
    let cancellation = producer.cancellation_token();
    let Some(reservation) = reservation else {
        producer.bypass();
        return;
    };
    let acquire = acquire_io();
    tokio::pin!(acquire);
    let producer_io = tokio::select! {
        _ = cancellation.cancelled() => {
            producer.finish(Err(StorageError::OperationCanceled));
            return;
        }
        result = tokio::time::timeout_at(deadline, &mut acquire) => match result {
            Ok(result) => result,
            Err(_) => {
                producer.relinquish_or_finish(ColdFillError::Storage(StorageError::Timeout));
                return;
            }
        }
    };
    let producer_io = match producer_io {
        Ok(io) => io,
        Err(err) => {
            producer.relinquish_or_finish(err);
            return;
        }
    };

    let open = open_reader();
    tokio::pin!(open);
    let reader = match tokio::select! {
        _ = cancellation.cancelled() => Err(StorageError::OperationCanceled),
        result = tokio::time::timeout_at(deadline, &mut open) => {
            result.unwrap_or(Err(StorageError::Timeout))
        }
    } {
        Ok(reader) => reader,
        Err(err) => {
            producer.relinquish_or_finish(ColdFillError::Storage(err));
            return;
        }
    };
    producer.mark_reader_started();
    let materialize = async move {
        let GetObjectReader {
            stream, buffered_body, ..
        } = reader;
        let body = if let Some(body) = buffered_body {
            if body.len() == expected {
                body
            } else {
                return Err(StorageError::other(format!(
                    "cold-fill buffered body length mismatch: expected {expected}, got {}",
                    body.len()
                )));
            }
        } else {
            let stream = if let Some(permit) = producer_io.disk_permit {
                wrap_reader(DiskReadPermitReader::new(stream, permit))
            } else {
                stream
            };
            Bytes::from(
                strict_materialize_object_body(stream, expected, GET_OBJECT_STAGE_BODY_CACHE_MATERIALIZE_READ)
                    .await
                    .map_err(StrictMaterializeError::into_storage_error)?,
            )
        };
        Ok::<_, StorageError>((body, reservation))
    };
    let materialized = tokio::select! {
            _ = cancellation.cancelled() => Err(StorageError::OperationCanceled),
            result = tokio::time::timeout_at(deadline, materialize) => {
                result.unwrap_or(Err(StorageError::Timeout))
            }
    };
    let result = match materialized {
        Ok((body, reservation)) => {
            if cancellation.is_cancelled() {
                producer.finish(Err(StorageError::OperationCanceled));
                return;
            }
            if deadline <= tokio::time::Instant::now() {
                producer.finish(Err(StorageError::Timeout));
                return;
            }
            let reserved = reservation.wrap_bytes(body);
            let shared = reserved.bytes();
            let publish = async {
                #[cfg(test)]
                wait_cold_fill_publication_barrier(&engine_plan).await;
                adapter.fill_reserved_body(&engine_plan, reserved).await
            };
            tokio::pin!(publish);
            tokio::select! {
                _ = cancellation.cancelled() => Err(StorageError::OperationCanceled),
                _ = tokio::time::sleep_until(deadline) => {
                    Err(StorageError::Timeout)
                }
                _ = &mut publish => Ok(shared),
            }
        }
        Err(err) => Err(err),
    };
    producer.finish(result);
}

fn cold_fill_deadline(
    wrapper: &RequestTimeoutWrapper,
    timeout_config: &GetObjectTimeoutPolicy,
    response_size: u64,
) -> Option<tokio::time::Instant> {
    if !timeout_config.is_timeout_enabled() {
        return None;
    }
    Some(tokio::time::Instant::now() + wrapper.remaining_time_for_size(Some(response_size)).unwrap_or(Duration::ZERO))
}

fn cold_fill_producer_deadline(timeout_config: &GetObjectTimeoutPolicy, response_size: u64) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    let hard_deadline = now + COLD_FILL_HARD_MAX_DURATION;
    if timeout_config.is_timeout_enabled() {
        (now + timeout_config.calculate_timeout_for_size(response_size)).min(hard_deadline)
    } else {
        hard_deadline
    }
}

async fn lookup_cold_fill_second_chance(
    adapter: &ObjectDataCacheAdapter,
    plan: &rustfs_object_data_cache::ObjectDataCacheGetPlan,
) -> Option<Bytes> {
    match adapter.peek_body_untracked(plan).await {
        rustfs_object_data_cache::ObjectDataCacheLookup::Hit(body) => Some(body),
        _ => None,
    }
}

fn retain_cold_fill_producer_for_matching_plan(
    producer: ColdFillProducer,
    current: &GetObjectBodyCachePlan,
    expected: &rustfs_object_data_cache::ObjectDataCacheGetPlan,
) -> Option<ColdFillProducer> {
    if current == &GetObjectBodyCachePlan::Cacheable(expected.clone()) {
        Some(producer)
    } else {
        producer.bypass();
        None
    }
}

impl<R> futures::Stream for GetObjectReaderStream<R>
where
    R: AsyncRead,
{
    type Item = Result<Bytes, S3StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.remaining == 0 {
            return Poll::Ready(None);
        }

        let remaining_before = *this.remaining;
        let attribution_enabled = is_get_output_handoff_attribution_enabled();
        let poll_start = attribution_enabled.then(std::time::Instant::now);
        let reader = match this.reader.as_mut().as_pin_mut() {
            Some(reader) => reader,
            None => return Poll::Ready(None),
        };
        let read_capacity = (*this.capacity).min(*this.remaining);
        let mut buf = BytesMut::with_capacity(read_capacity);
        let poll_read = poll_read_buf(reader, cx, &mut buf);

        let result: Poll<Option<Self::Item>> = match poll_read {
            Poll::Ready(Ok(bytes_read)) if bytes_read > 0 => {
                let bytes = buf.freeze();
                *this.remaining -= bytes.len();
                *this.emitted += bytes.len();
                #[cfg(feature = "tracing-chunk-debug")]
                {
                    tracing::debug!(
                        emitted = *this.emitted,
                        expected = *this.expected,
                        chunk_len = bytes.len(),
                        "GetObject ReaderStream emitted bytes"
                    );
                }
                if bytes.is_empty() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Ok(bytes)))
                }
            }
            Poll::Ready(Ok(_)) => {
                this.reader.set(None);
                let remaining = i64::try_from(*this.remaining).unwrap_or(i64::MAX);
                let err = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, rustfs_rio::IncompleteBody { remaining });
                record_get_object_reader_stream_failure(
                    GET_STREAMING_BODY_FAILURE_REASON_SHORT_EOF,
                    "short_eof",
                    this.strategy,
                    this.buffer_source,
                    *this.expected,
                    *this.emitted,
                    *this.remaining,
                );
                // The inner GetObjectStreamingReader is what normally reports a
                // short body, so reaching this arm means the reader signalled a
                // clean EOF while this layer still owed bytes against an
                // already-committed Content-Length. That disagreement is a data
                // plane fault, not chunk noise: log it unconditionally so the
                // truncated object is named in the operator's log rather than
                // only in a metric counter (issue #4784).
                error!(
                    event = EVENT_GET_OBJECT_STREAM_BODY,
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    bucket = %this.diagnostics.bucket,
                    object = %this.diagnostics.object,
                    request_id = %this.diagnostics.request_id,
                    size_bucket = get_object_stream_size_bucket(*this.expected),
                    expected = *this.expected,
                    emitted = *this.emitted,
                    remaining = *this.remaining,
                    strategy = this.strategy,
                    buffer_source = this.buffer_source,
                    state = "reader_stream_short_eof",
                    error = %err,
                    "GetObject reader stream ended before the committed content length"
                );
                Poll::Ready(Some(Err(Box::new(err) as S3StdError)))
            }
            Poll::Ready(Err(err)) => {
                this.reader.set(None);
                let error_class = classify_get_object_stream_read_error(&err);
                record_get_object_reader_stream_failure(
                    get_object_stream_failure_reason(error_class),
                    error_class,
                    this.strategy,
                    this.buffer_source,
                    *this.expected,
                    *this.emitted,
                    *this.remaining,
                );
                // Deliberately not logged at warn here: every production body
                // wraps a GetObjectStreamingReader, and that layer already
                // reports this same error once with `state = "read_failed"` and
                // the object identity. A second unconditional line per failed
                // GET would read as two distinct faults. The chunk-debug build
                // still gets this layer's view of the same error.
                #[cfg(feature = "tracing-chunk-debug")]
                tracing::error!(
                    emitted = *this.emitted,
                    expected = *this.expected,
                    error_class = error_class,
                    error = %err,
                    "GetObject ReaderStream returned error"
                );
                Poll::Ready(Some(Err(Box::new(err) as S3StdError)))
            }
            Poll::Pending => Poll::Pending,
        };

        let emitted_bytes = match &result {
            Poll::Ready(Some(Ok(bytes))) => bytes.len(),
            _ => 0,
        };
        let outcome = match &result {
            Poll::Ready(Some(Ok(bytes))) if !bytes.is_empty() => GET_READER_STREAM_POLL_READY_DATA,
            Poll::Ready(Some(Ok(_))) | Poll::Ready(None) => GET_READER_STREAM_POLL_READY_EMPTY,
            Poll::Ready(Some(Err(_))) => GET_READER_STREAM_POLL_READY_ERROR,
            Poll::Pending => GET_READER_STREAM_POLL_PENDING,
        };
        if attribution_enabled {
            rustfs_io_metrics::record_get_object_reader_stream_poll(
                this.strategy,
                this.buffer_source,
                outcome,
                remaining_before,
                emitted_bytes,
                poll_start.map_or(0.0, |start| start.elapsed().as_secs_f64()),
            );
        }

        result
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.remaining == 0 || self.reader.is_none() {
            (0, Some(0))
        } else {
            (1, None)
        }
    }
}

impl<R> ByteStream for GetObjectReaderStream<R>
where
    R: AsyncRead,
{
    fn remaining_length(&self) -> RemainingLength {
        RemainingLength::new_exact(self.remaining)
    }
}

struct GetObjectStreamingReader<R> {
    inner: Option<R>,
    // bucket/object + request_id + optional content_range are only used for diagnostic
    // correlation and failure bucketing; they do not alter stream behavior. The object
    // identity is what turns a mid-stream failure into an actionable report: a request_id
    // alone cannot tell an operator which object reads short (issue #4784).
    bucket: String,
    object: String,
    request_id: String,
    content_range: Option<String>,
    expected: usize,
    emitted: usize,
    timeout: Duration,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    started: std::time::Instant,
    first_byte_reported: bool,
    completed: bool,
    lifecycle: GetObjectBodyLifecycle,
    resume: Option<GetObjectResumeControl<R>>,
    _foreground_read_guard: rustfs_scanner::ForegroundReadGuard,
}

impl<R> GetObjectStreamingReader<R> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        inner: R,
        bucket: &str,
        key: &str,
        request_id: &str,
        content_range: Option<String>,
        expected: usize,
        timeout: Duration,
        lifecycle: GetObjectBodyLifecycle,
        resume: Option<GetObjectResumeControl<R>>,
    ) -> Self {
        Self {
            inner: Some(inner),
            bucket: bucket.to_string(),
            object: key.to_string(),
            request_id: request_id.to_string(),
            content_range,
            expected,
            emitted: 0,
            timeout,
            timer: None,
            started: std::time::Instant::now(),
            first_byte_reported: false,
            completed: expected == 0,
            lifecycle,
            resume,
            _foreground_read_guard: rustfs_scanner::ForegroundReadGuard::new(),
        }
    }

    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    // Classify transport/read failures before logging so operators can quickly
    // distinguish truncated upstream bodies, corruption, quorum issues, and
    // genuine downstream-close disconnects.
    fn classify_read_error(err: &std::io::Error) -> &'static str {
        classify_get_object_stream_read_error(err)
    }

    fn finish_ok(&mut self) {
        self.completed = true;
        self.lifecycle.finish_ok();
    }

    fn finish_err(&mut self) {
        self.lifecycle.finish_err();
    }

    fn resume_in_flight(&self) -> bool {
        matches!(
            self.resume.as_ref().map(|resume| &resume.stage),
            Some(GetObjectResumeStage::Backoff | GetObjectResumeStage::Reopening(_))
        )
    }

    fn begin_resume(&mut self, error: std::io::Error) {
        let Some(resume) = self.resume.as_mut() else {
            return;
        };
        self.inner.take();
        resume.begin(error);
    }

    // Drive the armed resume flow: backoff ticks gate each reopen attempt, and
    // a successful reopen swaps the failed stream out for the replacement.
    fn poll_resume(&mut self, cx: &mut Context<'_>) -> GetObjectResumePoll {
        let Some(mut resume) = self.resume.take() else {
            // resume_in_flight guards every call site.
            unreachable!("poll_resume requires an armed resume control");
        };
        let outcome = loop {
            let stage = std::mem::replace(&mut resume.stage, GetObjectResumeStage::Idle);
            match stage {
                GetObjectResumeStage::Idle => unreachable!("resume control is only polled while armed"),
                GetObjectResumeStage::Backoff => match Pin::new(&mut resume.timer).poll_next(cx) {
                    Poll::Ready(Some(())) => {
                        resume.attempts += 1;
                        resume.stage = GetObjectResumeStage::Reopening(Mutex::new((resume.reopen)(self.emitted)));
                    }
                    Poll::Ready(None) => {
                        let error = resume.take_trigger_error();
                        break GetObjectResumePoll::Failed {
                            error,
                            attempts: resume.attempts,
                        };
                    }
                    Poll::Pending => {
                        resume.stage = GetObjectResumeStage::Backoff;
                        break GetObjectResumePoll::Pending;
                    }
                },
                GetObjectResumeStage::Reopening(reopening) => {
                    let poll = match reopening.try_lock() {
                        Ok(mut reopening) => reopening.as_mut().poll(cx),
                        // Only reachable when a poll of the reopen future
                        // panicked and poisoned the mutex: fail closed with the
                        // original trigger error instead of polling it again.
                        Err(_) => {
                            let error = resume.take_trigger_error();
                            break GetObjectResumePoll::Failed {
                                error,
                                attempts: resume.attempts,
                            };
                        }
                    };
                    match poll {
                        Poll::Ready(Ok(reader)) => {
                            self.inner = Some(reader);
                            break GetObjectResumePoll::Resumed {
                                attempts: resume.attempts,
                            };
                        }
                        Poll::Ready(Err(GetObjectResumeFailure::Retryable)) => {
                            resume.stage = GetObjectResumeStage::Backoff;
                        }
                        Poll::Ready(Err(GetObjectResumeFailure::Fatal)) => {
                            let error = resume.take_trigger_error();
                            break GetObjectResumePoll::Failed {
                                error,
                                attempts: resume.attempts,
                            };
                        }
                        Poll::Pending => {
                            resume.stage = GetObjectResumeStage::Reopening(reopening);
                            break GetObjectResumePoll::Pending;
                        }
                    }
                }
            }
        };
        if matches!(outcome, GetObjectResumePoll::Resumed { .. } | GetObjectResumePoll::Pending) {
            self.resume = Some(resume);
        }
        outcome
    }

    fn poll_stall_timeout(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.timeout.is_zero() {
            return Poll::Pending;
        }

        if self.timer.is_none() {
            self.timer = Some(Box::pin(tokio::time::sleep(self.timeout)));
        }

        if let Some(timer) = self.timer.as_mut()
            && std::future::Future::poll(timer.as_mut(), cx).is_ready()
        {
            self.timer = None;
            warn!(
                event = EVENT_GET_OBJECT_STREAM_BODY,
                component = LOG_COMPONENT_APP,
                subsystem = LOG_SUBSYSTEM_OBJECT,
                bucket = %self.bucket,
                object = %self.object,
                request_id = %self.request_id,
                range = %self.content_range.as_deref().unwrap_or("full"),
                size_bucket = get_object_stream_size_bucket(self.expected),
                expected = self.expected,
                emitted = self.emitted,
                elapsed_ms = self.elapsed().as_millis(),
                timeout_ms = self.timeout.as_millis(),
                state = "stall_timeout",
                "GetObject streaming body stalled"
            );
            self.finish_err();
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "get object streaming body stall timeout",
            )));
        }

        Poll::Pending
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for GetObjectStreamingReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let filled_before = buf.filled().len();

        loop {
            // An armed resume owns the reader until it swaps in a reopened
            // stream or exhausts its budget; the failed inner stream is never
            // polled again.
            if self.resume_in_flight() {
                match self.poll_resume(cx) {
                    GetObjectResumePoll::Resumed { attempts } => {
                        debug!(
                            event = EVENT_GET_OBJECT_STREAM_BODY,
                            component = LOG_COMPONENT_APP,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            bucket = %self.bucket,
                            object = %self.object,
                            request_id = %self.request_id,
                            range = %self.content_range.as_deref().unwrap_or("full"),
                            size_bucket = get_object_stream_size_bucket(self.expected),
                            expected = self.expected,
                            emitted = self.emitted,
                            resume_attempts = attempts,
                            state = "resumed",
                            "GetObject streaming body resumed from a reopened object read"
                        );
                        // The replacement stream starts a fresh stall window.
                        self.timer = None;
                        continue;
                    }
                    GetObjectResumePoll::Pending => return self.poll_stall_timeout(cx),
                    GetObjectResumePoll::Failed { error, attempts } => {
                        self.timer = None;
                        let failure_reason = Self::classify_read_error(&error);
                        self.finish_err();
                        error!(
                            event = EVENT_GET_OBJECT_STREAM_BODY,
                            component = LOG_COMPONENT_APP,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            bucket = %self.bucket,
                            object = %self.object,
                            request_id = %self.request_id,
                            range = %self.content_range.as_deref().unwrap_or("full"),
                            size_bucket = get_object_stream_size_bucket(self.expected),
                            expected = self.expected,
                            emitted = self.emitted,
                            elapsed_ms = self.elapsed().as_millis(),
                            state = "read_failed",
                            failure_reason = failure_reason,
                            resume_attempts = attempts,
                            error = %error,
                            "GetObject streaming body read failed; mid-stream resume did not recover"
                        );
                        return Poll::Ready(Err(error));
                    }
                }
            }

            let Some(inner) = self.inner.as_mut() else {
                self.finish_err();
                return Poll::Ready(Err(std::io::Error::other(
                    "get object streaming reader lost its active read outside resume",
                )));
            };
            match Pin::new(inner).poll_read(cx, buf) {
                Poll::Ready(Ok(())) => {
                    self.timer = None;
                    let produced = buf.filled().len().saturating_sub(filled_before);
                    if produced > 0 {
                        self.emitted = self.emitted.saturating_add(produced);
                        if !self.first_byte_reported {
                            self.first_byte_reported = true;
                            let elapsed = self.elapsed();
                            rustfs_io_metrics::record_get_object_first_byte_latency(
                                GET_OBJECT_STAGE_PATH_S3_HANDLER,
                                elapsed.as_secs_f64(),
                            );
                            if elapsed >= GET_OBJECT_STREAM_WARN_THRESHOLD {
                                warn!(
                                        event = EVENT_GET_OBJECT_STREAM_BODY,
                                        component = LOG_COMPONENT_APP,
                                        subsystem = LOG_SUBSYSTEM_OBJECT,
                                        bucket = %self.bucket,
                                        object = %self.object,
                                        request_id = %self.request_id,
                                        range = %self.content_range.as_deref().unwrap_or("full"),
                                        size_bucket = get_object_stream_size_bucket(self.expected),
                                        expected = self.expected,
                                        emitted = self.emitted,
                                        elapsed_ms = elapsed.as_millis(),
                                        state = "first_byte_slow",
                                        "GetObject streaming body first byte was slow"
                                );
                            }
                        }
                        if self.emitted >= self.expected {
                            self.completed = true;
                            self.finish_ok();
                        }
                        return Poll::Ready(Ok(()));
                    }

                    if self.emitted < self.expected {
                        // The inner reader signalled a clean EOF before delivering the full
                        // Content-Length. Returning Ok here would hand the client a truncated body
                        // under a full Content-Length: the peer treats the short body as complete
                        // (e.g. `mc mirror` writes a short file and considers it done — the
                        // "incomplete data mirroring" in issue #2955). Surface an error instead so
                        // the transfer fails loudly and the client retries rather than persisting
                        // truncated data.
                        let error = std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            rustfs_rio::IncompleteBody {
                                remaining: self.expected.saturating_sub(self.emitted) as i64,
                            },
                        );
                        // A premature EOF is also how the legacy duplex read path
                        // surfaces the object data vanishing mid-stream (typed
                        // errors do not survive that pump), so arm the resume
                        // flow before failing loudly when one is attached.
                        if self.resume.is_some() {
                            self.begin_resume(error);
                            continue;
                        }
                        error!(
                            event = EVENT_GET_OBJECT_STREAM_BODY,
                            component = LOG_COMPONENT_APP,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            bucket = %self.bucket,
                            object = %self.object,
                            request_id = %self.request_id,
                            range = %self.content_range.as_deref().unwrap_or("full"),
                            size_bucket = get_object_stream_size_bucket(self.expected),
                            expected = self.expected,
                            emitted = self.emitted,
                            elapsed_ms = self.elapsed().as_millis(),
                            state = "short_eof",
                            "GetObject streaming body ended before expected length"
                        );
                        self.finish_err();
                        return Poll::Ready(Err(error));
                    }

                    self.completed = true;
                    self.finish_ok();
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Err(err)) => {
                    // Typed relocation errors (the codec read path delivers them
                    // in-band) mean rebalance/decommission removed the pinned
                    // object data mid-stream: reopen and continue instead of
                    // failing the download. The error is only intercepted before
                    // the committed body length has been fully delivered.
                    if self.emitted < self.expected && is_object_relocation_error(&err) && self.resume.is_some() {
                        self.begin_resume(err);
                        continue;
                    }
                    let failure_reason = Self::classify_read_error(&err);
                    self.timer = None;
                    self.finish_err();
                    error!(
                        event = EVENT_GET_OBJECT_STREAM_BODY,
                        component = LOG_COMPONENT_APP,
                        subsystem = LOG_SUBSYSTEM_OBJECT,
                        bucket = %self.bucket,
                        object = %self.object,
                        request_id = %self.request_id,
                        range = %self.content_range.as_deref().unwrap_or("full"),
                        size_bucket = get_object_stream_size_bucket(self.expected),
                        expected = self.expected,
                        emitted = self.emitted,
                        elapsed_ms = self.elapsed().as_millis(),
                        state = "read_failed",
                        failure_reason = failure_reason,
                        error = %err,
                        "GetObject streaming body read failed"
                    );
                    return Poll::Ready(Err(err));
                }
                Poll::Pending => return self.poll_stall_timeout(cx),
            }
        }
    }
}

impl<R> Drop for GetObjectStreamingReader<R> {
    fn drop(&mut self) {
        if self.lifecycle.is_finished() {
            return;
        }

        if self.expected == 0 || self.completed || self.emitted >= self.expected {
            self.finish_ok();
            return;
        }

        self.finish_err();
        warn!(
            event = EVENT_GET_OBJECT_STREAM_BODY,
            component = LOG_COMPONENT_APP,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            bucket = %self.bucket,
            object = %self.object,
            request_id = %self.request_id,
            range = %self.content_range.as_deref().unwrap_or("full"),
            size_bucket = get_object_stream_size_bucket(self.expected),
            expected = self.expected,
            emitted = self.emitted,
            elapsed_ms = self.elapsed().as_millis(),
            state = "dropped_incomplete",
            "GetObject streaming body dropped before expected length"
        );
    }
}

/// Reopen budget for a single GetObject body. Three attempts against the
/// jittered 200ms/400ms RetryTimer schedule (~600ms worst case) bound the
/// metadata fan-out a storm of relocated downloads can multiply.
const GET_OBJECT_RESUME_MAX_ATTEMPTS: i64 = 3;

type GetObjectResumeFuture<R> = Pin<Box<dyn std::future::Future<Output = Result<R, GetObjectResumeFailure>> + Send>>;

type GetObjectReopen<R> = Box<dyn FnMut(usize) -> GetObjectResumeFuture<R> + Send + Sync>;

enum GetObjectResumePoll {
    Resumed { attempts: usize },
    Pending,
    Failed { error: std::io::Error, attempts: usize },
}

/// Why a single resume attempt did not produce a replacement stream.
#[derive(Debug)]
enum GetObjectResumeFailure {
    /// Reopen/admission failure that may clear on the next attempt.
    Retryable,
    /// The reopened object is not the version this response committed to (or
    /// admission is permanently unavailable): continuing would splice two
    /// versions into one 200 response, so fail with the original error.
    Fatal,
}

enum GetObjectResumeStage<R> {
    Idle,
    Backoff,
    // The store's boxed read futures are Send but not Sync, while the
    // streaming body requires the reader to be Sync, so the in-flight reopen
    // future is stored behind a mutex. It is only ever locked under `&mut
    // self` in `poll_resume`, so the lock never contends.
    Reopening(Mutex<GetObjectResumeFuture<R>>),
}

/// Mid-stream resume machinery for [`GetObjectStreamingReader`]: when the
/// pinned object data vanishes mid-body (rebalance/decommission copies the
/// version elsewhere, then deletes the source), reopen the object at the
/// emitted offset and continue instead of failing the download.
struct GetObjectResumeControl<R> {
    reopen: GetObjectReopen<R>,
    timer: RetryTimer,
    stage: GetObjectResumeStage<R>,
    original_error: Option<std::io::Error>,
    attempts: usize,
}

impl<R> GetObjectResumeControl<R> {
    fn new(reopen: GetObjectReopen<R>, timer: RetryTimer) -> Self {
        Self {
            reopen,
            timer,
            stage: GetObjectResumeStage::Idle,
            original_error: None,
            attempts: 0,
        }
    }

    fn begin(&mut self, error: std::io::Error) {
        self.original_error = Some(error);
        self.stage = GetObjectResumeStage::Backoff;
    }

    // The trigger error is always recorded by `begin`; the fallback is a
    // fail-closed internal error, never a fabricated success.
    fn take_trigger_error(&mut self) -> std::io::Error {
        self.original_error
            .take()
            .unwrap_or_else(|| std::io::Error::other("get object resume lost its trigger error"))
    }
}

/// Object-version identity captured when the response committed to a body. A
/// resumed read must serve exactly this version; `data_dir` is deliberately
/// excluded because rebalance regenerates it for the same version.
struct GetObjectResumeIdentity {
    version_id: Option<Uuid>,
    mod_time: Option<OffsetDateTime>,
    size: i64,
    etag: Option<String>,
    // The store rewrites a read's `object_info.size` to the per-read delivered
    // length for encrypted and compressed objects (readers.rs Encrypted /
    // Compressed transforms), so a reopened subrange reports `size - emitted`
    // while a plain read reports the range-invariant `oi.size`. The flag only
    // chooses the comparison arithmetic; a transform change that no longer
    // matches it fails the identity check, which is the closed direction.
    range_dependent_size: bool,
}

impl GetObjectResumeIdentity {
    fn matches(&self, info: &ObjectInfo, emitted: usize) -> bool {
        let expected_size = if self.range_dependent_size {
            self.size - emitted as i64
        } else {
            self.size
        };
        self.version_id == info.version_id
            && self.mod_time == info.mod_time
            && expected_size == info.size
            && self.etag == info.etag
    }
}

/// Reopen parameters for a mid-stream resume. Only the SSE-C headers the store
/// read path consumes are retained: the store-level `get_object_reader` spans
/// record their header argument at debug level, so retaining the full request
/// headers would re-log credentials on every attempt.
struct GetObjectResumeContext {
    store: Arc<ECStore>,
    bucket: String,
    key: String,
    opts: ObjectOptions,
    ssec_headers: HeaderMap,
    // Resolved plaintext offsets of the committed response body, captured
    // after `HTTPRangeSpec::get_offset_length`: suffix ranges and partNumber
    // GETs are already resolved to absolute offsets at that point, so the
    // resume offset is `range_start + emitted` regardless of request shape.
    range_start: i64,
    range_end: i64,
    identity: GetObjectResumeIdentity,
}

impl GetObjectResumeContext {
    #[allow(clippy::too_many_arguments)]
    fn new(
        store: Arc<ECStore>,
        bucket: &str,
        key: &str,
        mut opts: ObjectOptions,
        request_headers: &HeaderMap,
        info: &ObjectInfo,
        range_start: i64,
        range_end: i64,
    ) -> Self {
        if opts.version_id.is_none()
            && let Some(version_id) = info.version_id
        {
            opts.version_id = Some(version_id.to_string());
        }
        // Store spans record their header argument at debug level. Retain only
        // the SSE-C inputs needed to reopen the reader and keep them redacted.
        let ssec_headers = project_ssec_transport_headers(request_headers);
        Self {
            store,
            bucket: bucket.to_string(),
            key: key.to_string(),
            opts,
            ssec_headers,
            range_start,
            range_end,
            identity: GetObjectResumeIdentity {
                version_id: info.version_id,
                mod_time: info.mod_time,
                size: info.size,
                etag: info.etag.clone(),
                range_dependent_size: info.is_encrypted() || info.is_compressed(),
            },
        }
    }

    fn resume_range(range_start: i64, range_end: i64, emitted: usize) -> Option<HTTPRangeSpec> {
        let start = range_start + emitted as i64;
        if start == 0 && range_end < 0 {
            // Nothing was emitted from a full-object read: reopen without a
            // range so the replacement stream keeps the codec fast path
            // instead of the duplex fallback a synthesized range forces.
            return None;
        }
        Some(HTTPRangeSpec {
            is_suffix_length: false,
            start,
            end: range_end,
        })
    }

    async fn reopen(&self, emitted: usize) -> Result<DynReader, GetObjectResumeFailure> {
        #[cfg(test)]
        GET_OBJECT_RESUME_ATTEMPTS_FOR_TEST.fetch_add(1, Ordering::Relaxed);

        // A resumed read must hold disk-read admission just like the initial
        // read; otherwise recovery reads bypass the concurrency caps exactly
        // while rebalance is stressing the pool.
        let disk_permit = DefaultObjectUsecase::admit_get_object_disk_read(get_concurrency_manager(), &self.bucket, &self.key)
            .await
            .map_err(|err| {
                if err.code() == &S3ErrorCode::SlowDown {
                    GetObjectResumeFailure::Retryable
                } else {
                    GetObjectResumeFailure::Fatal
                }
            })?;
        let range = Self::resume_range(self.range_start, self.range_end, emitted);
        let reader = self
            .store
            .get_object_reader(&self.bucket, &self.key, range, self.ssec_headers.clone(), &self.opts)
            .await
            .map_err(|err| {
                debug!(
                    bucket = %self.bucket,
                    object = %self.key,
                    error = %err,
                    "GetObject mid-stream resume reopen failed"
                );
                GetObjectResumeFailure::Retryable
            })?;
        if !self.identity.matches(&reader.object_info, emitted) {
            warn!(
                bucket = %self.bucket,
                object = %self.key,
                "GetObject mid-stream resume resolved a different object version; refusing to splice content"
            );
            return Err(GetObjectResumeFailure::Fatal);
        }
        let stream = wrap_reader(reader.stream);
        Ok(match disk_permit {
            Some(disk_permit) => wrap_reader(DiskReadPermitReader::new(stream, disk_permit)),
            None => stream,
        })
    }
}

#[cfg(test)]
static GET_OBJECT_RESUME_ATTEMPTS_FOR_TEST: AtomicUsize = AtomicUsize::new(0);

fn get_object_resume_control(ctx: GetObjectResumeContext) -> GetObjectResumeControl<DynReader> {
    use rand::RngExt as _;
    let ctx = Arc::new(ctx);
    let reopen: GetObjectReopen<DynReader> = Box::new(move |emitted| {
        let ctx = Arc::clone(&ctx);
        Box::pin(async move { ctx.reopen(emitted).await })
    });
    GetObjectResumeControl::new(
        reopen,
        RetryTimer::new(
            GET_OBJECT_RESUME_MAX_ATTEMPTS,
            DEFAULT_RETRY_UNIT,
            DEFAULT_RETRY_CAP,
            MAX_JITTER,
            rand::rng().random_range(10..=50),
        ),
    )
}

/// Mid-stream errors that mean the pinned object data is gone (rebalance or
/// decommission removed it after copying the version elsewhere). Only typed
/// `StorageError`s qualify; generic I/O errors and string-matched "not enough
/// disks" failures keep the existing fail-loud behavior.
fn is_object_relocation_error(err: &std::io::Error) -> bool {
    let Some(inner) = err.get_ref() else { return false };
    match inner.downcast_ref::<StorageError>() {
        Some(StorageError::FileNotFound | StorageError::ObjectNotFound(..) | StorageError::InsufficientReadQuorum(..)) => true,
        Some(StorageError::Io(source)) => source.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}

pub(crate) fn object_seek_support_threshold() -> usize {
    static OBJECT_SEEK_SUPPORT_THRESHOLD: OnceLock<usize> = OnceLock::new();
    *OBJECT_SEEK_SUPPORT_THRESHOLD.get_or_init(|| {
        rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_SEEK_SUPPORT_THRESHOLD,
            rustfs_config::DEFAULT_OBJECT_SEEK_SUPPORT_THRESHOLD,
        )
    })
}

fn object_seek_support_concurrency_thresholds() -> (usize, usize) {
    static OBJECT_SEEK_SUPPORT_CONCURRENCY_THRESHOLDS: OnceLock<(usize, usize)> = OnceLock::new();
    *OBJECT_SEEK_SUPPORT_CONCURRENCY_THRESHOLDS.get_or_init(|| {
        let medium = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
            rustfs_config::DEFAULT_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
        )
        .max(1);
        let high = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
            rustfs_config::DEFAULT_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
        )
        .max(medium + 1);
        (medium, high)
    })
}

fn concurrency_aware_seek_support_threshold(configured_threshold: i64, concurrent_requests: usize) -> i64 {
    let (medium_threshold, high_threshold) = object_seek_support_concurrency_thresholds();
    let effective_threshold = configured_threshold.min(MAX_GET_OBJECT_MEMORY_BUFFER_BYTES);

    if concurrent_requests >= high_threshold.saturating_mul(2) {
        return effective_threshold.min(VERY_HIGH_CONCURRENCY_GET_OBJECT_MEMORY_BUFFER_BYTES);
    }
    if concurrent_requests >= high_threshold {
        return effective_threshold.min(HIGH_CONCURRENCY_GET_OBJECT_MEMORY_BUFFER_BYTES);
    }
    if concurrent_requests >= medium_threshold {
        return effective_threshold.min(MEDIUM_CONCURRENCY_GET_OBJECT_MEMORY_BUFFER_BYTES);
    }

    effective_threshold
}

fn should_buffer_get_object_in_memory(
    info: &ObjectInfo,
    response_content_length: i64,
    part_number: Option<usize>,
    has_range: bool,
    concurrent_requests: usize,
) -> bool {
    let configured_threshold = object_seek_support_threshold() as i64;
    should_buffer_get_object_in_memory_with_threshold(
        info,
        response_content_length,
        part_number,
        has_range,
        configured_threshold,
        concurrent_requests,
        is_get_seek_buffer_enabled(),
    )
}

fn should_materialize_get_object_body_for_cache(
    info: &ObjectInfo,
    response_content_length: i64,
    part_number: Option<usize>,
    has_range: bool,
    concurrent_requests: usize,
) -> bool {
    let configured_threshold = object_seek_support_threshold() as i64;
    should_buffer_get_object_in_memory_with_threshold(
        info,
        response_content_length,
        part_number,
        has_range,
        configured_threshold,
        concurrent_requests,
        true,
    )
}

fn should_buffer_get_object_in_memory_with_threshold(
    _info: &ObjectInfo,
    response_content_length: i64,
    part_number: Option<usize>,
    has_range: bool,
    configured_threshold: i64,
    concurrent_requests: usize,
    seek_buffer_enabled: bool,
) -> bool {
    if !seek_buffer_enabled || part_number.is_some() || has_range || response_content_length <= 0 || configured_threshold <= 0 {
        return false;
    }
    if usize::try_from(response_content_length).is_err() {
        return false;
    }

    let effective_threshold = concurrency_aware_seek_support_threshold(configured_threshold, concurrent_requests);
    if configured_threshold > MAX_GET_OBJECT_MEMORY_BUFFER_BYTES
        && GET_OBJECT_BUFFER_THRESHOLD_WARNED
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        warn!(
            configured_threshold_bytes = configured_threshold,
            hard_limit_bytes = MAX_GET_OBJECT_MEMORY_BUFFER_BYTES,
            "RUSTFS_OBJECT_SEEK_SUPPORT_THRESHOLD exceeds safety cap; using capped in-memory buffer threshold"
        );
    }

    if response_content_length > effective_threshold {
        return false;
    }

    true
}

impl DefaultObjectUsecase {
    fn build_memory_bytes_blob(
        bytes: Bytes,
        response_content_length: i64,
        source: &'static str,
        lifecycle: GetObjectBodyLifecycle,
    ) -> StreamingBlob {
        let get_stage_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
        let memory_blob_start = get_stage_metrics_enabled.then(std::time::Instant::now);
        let handoff_start = get_stage_metrics_enabled.then(std::time::Instant::now);
        let bytes_len = bytes.len();
        let guard = rustfs_io_metrics::track_get_object_buffered_bytes(bytes_len);
        let remaining = usize::try_from(response_content_length.max(0)).unwrap_or(usize::MAX);
        let blob = if is_get_small_body_once_enabled() && bytes_len == remaining {
            let owner = MemoryOnceBodyOwner::new(bytes, guard, lifecycle);
            StreamingBlob::from_bytes(Bytes::from_owner(owner))
        } else {
            StreamingBlob::new(MemoryTrackedBytesStream::new(bytes, remaining, source, guard, lifecycle))
        };
        if let Some(handoff_start) = handoff_start {
            rustfs_io_metrics::record_get_object_response_handoff(
                "single_chunk",
                source,
                bytes_len,
                response_content_length,
                handoff_start.elapsed().as_secs_f64(),
            );
        }
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_BODY_MEMORY_BLOB, memory_blob_start);
        blob
    }

    fn build_memory_blob(
        buf: Vec<u8>,
        response_content_length: i64,
        source: &'static str,
        lifecycle: GetObjectBodyLifecycle,
    ) -> StreamingBlob {
        Self::build_memory_bytes_blob(Bytes::from(buf), response_content_length, source, lifecycle)
    }

    fn select_stream_buffer_strategy(
        response_content_length: i64,
        optimal_buffer_size: usize,
        enable_readahead: bool,
        has_range: bool,
    ) -> (usize, GetObjectStreamStrategy) {
        if enable_readahead && !has_range && response_content_length >= LARGE_SEQUENTIAL_GET_THRESHOLD_BYTES {
            let expanded_buffer_size = optimal_buffer_size
                .saturating_mul(LARGE_SEQUENTIAL_GET_READAHEAD_MULTIPLIER)
                .min(LARGE_SEQUENTIAL_GET_STREAM_BUFFER_CAP_BYTES)
                .max(optimal_buffer_size);
            return (expanded_buffer_size, GetObjectStreamStrategy::LargeSequentialReadahead);
        }

        (optimal_buffer_size, GetObjectStreamStrategy::Standard)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_reader_blob<R>(
        reader: R,
        response_content_length: i64,
        request_id: &str,
        content_range: Option<&str>,
        stream_buffer_size: usize,
        stream_strategy: GetObjectStreamStrategy,
        bucket: &str,
        key: &str,
        lifecycle: GetObjectBodyLifecycle,
        resume: Option<GetObjectResumeControl<R>>,
    ) -> StreamingBlob
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        let streaming_blob_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let expected = usize::try_from(response_content_length.max(0)).unwrap_or(usize::MAX);
        let tuned_stream_buffer_size =
            tune_reader_stream_buffer_size(stream_buffer_size, response_content_length, stream_strategy);
        let (stream_buffer_size, buffer_source) =
            resolve_reader_stream_buffer_size(tuned_stream_buffer_size, get_reader_stream_buffer_size_override());
        let get_stage_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
        if get_stage_metrics_enabled {
            rustfs_io_metrics::record_get_object_stream_strategy(
                stream_strategy.as_str(),
                stream_buffer_size,
                response_content_length,
            );
        }
        let handoff_start = get_stage_metrics_enabled.then(std::time::Instant::now);
        let reader = GetObjectStreamingReader::new(
            reader,
            bucket,
            key,
            request_id,
            content_range.map(|content_range| content_range.to_string()),
            expected,
            get_object_disk_read_timeout(),
            lifecycle,
            resume,
        );
        let stream = GetObjectReaderStream::new(reader, stream_buffer_size, expected, stream_strategy.as_str(), buffer_source)
            .with_diagnostics(bucket, key, request_id);
        let blob = StreamingBlob::new(stream);
        if let Some(handoff_start) = handoff_start {
            rustfs_io_metrics::record_get_object_response_handoff(
                stream_strategy.as_str(),
                buffer_source,
                stream_buffer_size,
                response_content_length,
                handoff_start.elapsed().as_secs_f64(),
            );
        }
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_BODY_STREAMING_BLOB, streaming_blob_start);
        blob
    }

    fn init_get_object_bootstrap(&self, bucket: &str, key: &str, request_id: &str) -> S3Result<GetObjectBootstrap> {
        #[cfg(test)]
        let timeout_config = self
            .get_object_timeout_policy
            .clone()
            .unwrap_or_else(GetObjectTimeoutPolicy::cached_from_env);
        #[cfg(not(test))]
        let timeout_config = GetObjectTimeoutPolicy::cached_from_env();
        let wrapper = RequestTimeoutWrapper::with_request_id(timeout_config.clone(), request_id.to_string());
        let request_start = std::time::Instant::now();
        let request_guard = ConcurrencyManager::track_request();
        let concurrent_requests = GetObjectGuard::concurrent_requests();

        let deadlock_detector = deadlock_detector::get_deadlock_detector();
        let deadlock_request_guard = DeadlockRequestGuard::register_if_enabled(deadlock_detector, wrapper.request_id(), || {
            format!("GetObject {bucket}/{key}")
        });

        Self::ensure_get_object_not_timed_out(&wrapper, &timeout_config, bucket, key, GetObjectTimeoutStage::BeforeProcessing)?;

        debug!(
            "GetObject request started with {} concurrent requests, timeout={:?}",
            concurrent_requests, timeout_config.get_object_timeout
        );

        Ok(GetObjectBootstrap {
            timeout_config,
            wrapper,
            request_start,
            request_guard,
            _deadlock_request_guard: deadlock_request_guard,
            concurrent_requests,
        })
    }

    fn validate_get_object_part_number(part_number: Option<usize>, info: &ObjectInfo) -> S3Result<()> {
        if let Some(part_number) = part_number
            && part_number > 1
            && !info.parts.iter().any(|part| part.number == part_number)
        {
            return Err(s3_error!(InvalidPart));
        }
        Ok(())
    }

    fn validate_get_object_before_cold_fill(headers: &HeaderMap, part_number: Option<usize>, info: &ObjectInfo) -> S3Result<()> {
        check_preconditions(headers, info)?;
        Self::validate_get_object_part_number(part_number, info)
    }

    /// How long a GET waits for a disk read permit before degrading to a
    /// permit-less read. Cached: consulted per GET. Zero disables the bound.
    fn disk_permit_wait_timeout() -> Duration {
        static CACHED: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
        *CACHED.get_or_init(|| {
            Duration::from_secs(rustfs_utils::get_env_u64(
                rustfs_config::ENV_OBJECT_DISK_PERMIT_WAIT_TIMEOUT,
                rustfs_config::DEFAULT_OBJECT_DISK_PERMIT_WAIT_TIMEOUT,
            ))
        })
    }

    async fn acquire_get_object_io_planning(
        manager: &ConcurrencyManager,
        request_timeout: Option<GetObjectRequestTimeout<'_>>,
        bucket: &str,
        key: &str,
    ) -> S3Result<GetObjectIoPlanning> {
        let permit_wait_start = std::time::Instant::now();
        let disk_permit = Self::admit_get_object_disk_read(manager, bucket, key).await?;
        let permit_wait_duration = permit_wait_start.elapsed();

        if let Some(timeout) = request_timeout {
            Self::ensure_get_object_not_timed_out(
                timeout.wrapper,
                timeout.policy,
                bucket,
                key,
                GetObjectTimeoutStage::DiskPermitWait { permit_wait_duration },
            )?;
        }

        let queue_status = manager.io_queue_status();
        let queue_snapshot = GetObjectQueueSnapshot::from_available_permits(
            queue_status.total_permits,
            queue_status.total_permits.saturating_sub(queue_status.permits_in_use),
        );
        let queue_utilization = queue_snapshot.utilization_percent();

        if queue_snapshot.is_congested(80.0) {
            // Metrics count every congested request; only the WARN is rate
            // limited, because under saturation every GET crosses the
            // threshold and per-request WARNs flood the log.
            rustfs_io_metrics::record_io_queue_congestion();

            if let Some(suppressed_warns) = IO_QUEUE_CONGESTION_WARN_THROTTLE.claim(IoQueueCongestionWarnThrottle::now_ms()) {
                warn!(
                    bucket = %bucket,
                    key = %key,
                    queue_utilization = format!("{:.1}%", queue_utilization),
                    permits_in_use = queue_status.permits_in_use,
                    total_permits = queue_status.total_permits,
                    suppressed_warns,
                    "I/O queue congestion detected"
                );
            }
        }

        if let Some(timeout) = request_timeout {
            Self::ensure_get_object_not_timed_out(
                timeout.wrapper,
                timeout.policy,
                bucket,
                key,
                GetObjectTimeoutStage::BeforeRead,
            )?;
        }

        Ok(GetObjectIoPlanning {
            disk_permit,
            permit_wait_duration,
            queue_status,
            queue_utilization,
        })
    }

    // Shared by the initial read path and the mid-stream resume reopen, which
    // must hold the same admission token before touching disks. The permit
    // wait inside is bounded by the primary-pool timeout.
    async fn admit_get_object_disk_read(
        manager: &ConcurrencyManager,
        bucket: &str,
        key: &str,
    ) -> S3Result<Option<GetObjectDiskPermit>> {
        let permit_wait_start = std::time::Instant::now();
        let permit_wait_timeout = Self::disk_permit_wait_timeout();
        // Permits are held for the whole body transfer, so slow clients can pin
        // all of them while disks are idle. Bound the wait on the primary pool
        // and, on timeout, admit from a bounded degraded overflow lane. Total
        // concurrent disk-active GETs are hard-capped at
        // `primary_cap + degraded_cap`; once that cap is reached we reject with
        // `SlowDown` instead of reading without any admission token. Never
        // proceed permit-less.
        let disk_permit = match manager
            .admit_disk_read(permit_wait_timeout)
            .await
            .map_err(|_| s3_error!(InternalError, "disk read semaphore closed"))?
        {
            DiskReadAdmission::Primary(permit) => Some(permit),
            // Throttling disabled by config (primary cap 0): proceed without an
            // admission token. Not a saturation bypass.
            DiskReadAdmission::Unbounded => None,
            DiskReadAdmission::Degraded(permit) => {
                metrics::counter!("rustfs.get_object.disk_permit.degraded.total").increment(1);
                warn!(
                    bucket = %bucket,
                    key = %key,
                    wait_ms = permit_wait_start.elapsed().as_millis() as u64,
                    "GetObject admitted into bounded degraded disk-read lane after primary pool saturation"
                );
                Some(permit)
            }
            DiskReadAdmission::Rejected => {
                metrics::counter!("rustfs.get_object.disk_permit.hard_reject.total").increment(1);
                warn!(
                    bucket = %bucket,
                    key = %key,
                    wait_ms = permit_wait_start.elapsed().as_millis() as u64,
                    "GetObject rejected: disk-read hard concurrency cap reached"
                );
                return Err(s3_error!(
                    SlowDown,
                    "disk read concurrency limit reached, please reduce your request rate"
                ));
            }
        };
        Ok(disk_permit.map(GetObjectDiskPermit::new))
    }

    async fn acquire_cold_fill_io_planning(
        manager: &'static ConcurrencyManager,
        bucket: &str,
        key: &str,
    ) -> Result<GetObjectIoPlanning, ColdFillError> {
        match Self::acquire_get_object_io_planning(manager, None, bucket, key).await {
            Ok(io) => Ok(io),
            Err(err) if err.code() == &S3ErrorCode::SlowDown => Err(ColdFillError::Storage(StorageError::SlowDown)),
            Err(_) => Err(ColdFillError::DiskAdmissionClosed),
        }
    }

    fn get_object_io_planning_without_disk(manager: &ConcurrencyManager) -> GetObjectIoPlanning {
        let queue_status = manager.io_queue_status();
        let queue_snapshot = GetObjectQueueSnapshot::from_available_permits(
            queue_status.total_permits,
            queue_status.total_permits.saturating_sub(queue_status.permits_in_use),
        );
        GetObjectIoPlanning {
            disk_permit: None,
            permit_wait_duration: Duration::ZERO,
            queue_utilization: queue_snapshot.utilization_percent(),
            queue_status,
        }
    }

    /// Cheap request-shape validations, run before the bucket-existence store
    /// lookup so invalid requests keep their InvalidArgument precedence.
    fn validate_get_object_request(req: &S3Request<GetObjectInput>) -> S3Result<GetObjectValidatedRequest> {
        // Clone only the fields this path needs instead of the whole input.
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let version_id = req.input.version_id.clone();
        let part_number = req.input.part_number;
        let range = req.input.range;

        validate_object_key(&key, "GET")?;

        let part_number = parse_part_number_i32_to_usize(part_number, "GET")?;

        let rs = range.map(range_to_http_range_spec).transpose()?;

        if rs.is_some() && part_number.is_some() {
            return Err(s3_error!(InvalidArgument, "range and part_number invalid"));
        }

        Ok(GetObjectValidatedRequest {
            bucket,
            key,
            version_id,
            part_number,
            rs,
        })
    }

    async fn prepare_get_object_request_context(
        validated: GetObjectValidatedRequest,
        headers: &HeaderMap,
    ) -> S3Result<GetObjectRequestContext> {
        let GetObjectValidatedRequest {
            bucket,
            key,
            version_id,
            part_number,
            rs,
        } = validated;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), part_number, headers)
            .await
            .map_err(ApiError::from)?;

        Ok(GetObjectRequestContext {
            version_id_for_event: version_id.unwrap_or_default(),
            bucket,
            key,
            part_number,
            rs,
            opts,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_get_object_read_execution(
        &self,
        req: &S3Request<GetObjectInput>,
        manager: &'static ConcurrencyManager,
        store: Arc<ECStore>,
        wrapper: &RequestTimeoutWrapper,
        timeout_config: &GetObjectTimeoutPolicy,
        bucket: &str,
        key: &str,
        rs: Option<HTTPRangeSpec>,
        opts: &ObjectOptions,
        part_number: Option<usize>,
        object_traffic_health: Option<Arc<ObjectTrafficHealth>>,
    ) -> S3Result<GetObjectPreparedRead> {
        let read_start = std::time::Instant::now();
        let read_stage_start = rustfs_io_metrics::get_stage_metrics_enabled().then_some(read_start);
        let store_headers = project_ssec_transport_headers(&req.headers);
        let cache_adapter = self.object_data_cache();
        if cache_adapter.is_disabled() || !cache_adapter.materialize_fill_enabled() {
            let io_planning = Self::acquire_get_object_io_planning(
                manager,
                Some(GetObjectRequestTimeout {
                    wrapper,
                    policy: timeout_config,
                }),
                bucket,
                key,
            )
            .await?;
            let reader = track_object_read_setup(
                object_traffic_health.as_deref(),
                store.get_object_reader(bucket, key, rs.clone(), store_headers, opts),
            )
            .await
            .map_err(map_get_object_reader_error)?;
            let read_setup =
                Self::finish_get_object_read(req, manager, bucket, key, rs, part_number, read_start, reader, true).await?;
            return Ok(GetObjectPreparedRead { io_planning, read_setup });
        }

        // Preserve the legacy metadata-fanout bound without making followers
        // hold a body-transfer permit while they wait on the cold-fill session.
        let mut metadata_admission = Some(
            Self::acquire_get_object_io_planning(
                manager,
                Some(GetObjectRequestTimeout {
                    wrapper,
                    policy: timeout_config,
                }),
                bucket,
                key,
            )
            .await?,
        );
        let mut prepared = Some(
            track_object_read_setup(
                object_traffic_health.as_deref(),
                store.prepare_get_object_reader(bucket, key, rs.clone(), HeaderMap::new(), opts),
            )
            .await
            .map_err(map_get_object_reader_error)?,
        );
        let mut cache_fill_allowed = true;
        let mut legacy_hook_missed = false;
        'snapshot: {
            let info = prepared
                .as_ref()
                .ok_or_else(|| s3_error!(InternalError, "prepared metadata snapshot is unavailable"))?
                .object_info();
            // Preconditions, cache planning, and the authoritative hook lookup all
            // run against one namespace-locked metadata snapshot. Cacheable misses
            // release both the lock and short admission before joining cold fill.
            let Some(response_content_length) = get_object_body_cache_plaintext_len(&rs, opts, info) else {
                break 'snapshot;
            };
            let cache_plan = build_get_object_body_cache_plan(
                &cache_adapter,
                GetObjectBodyCacheRequest {
                    bucket,
                    key,
                    info,
                    response_content_length,
                    has_range: rs.is_some(),
                    part_number,
                    encryption_applied: info.is_encrypted(),
                },
            );

            // The legacy hook is evaluated once, before cold-fill coordination.
            // In-session producer retries never re-enter this snapshot block.
            let legacy_probe = lookup_preplanned_get_object_body_cache_hook(
                Arc::clone(&cache_adapter),
                cache_plan.clone(),
                bucket,
                key,
                &rs,
                opts,
                info,
            )
            .await;
            if matches!(legacy_probe, GetObjectBodyCacheHookLookup::Ineligible) {
                break 'snapshot;
            }
            Self::validate_get_object_before_cold_fill(&req.headers, part_number, info)?;
            if let GetObjectBodyCacheHookLookup::Hit(body) = legacy_probe {
                drop(metadata_admission.take());
                let info = prepared
                    .take()
                    .ok_or_else(|| s3_error!(InternalError, "prepared cache-hit reader is unavailable"))?
                    .into_object_info();
                let reader = GetObjectReader::from_cache_body(info, body).map_err(ApiError::from)?;
                let read_setup =
                    Self::finish_get_object_read(req, manager, bucket, key, rs, part_number, read_start, reader, true).await?;
                return Ok(GetObjectPreparedRead {
                    io_planning: Self::get_object_io_planning_without_disk(manager),
                    read_setup,
                });
            }
            if matches!(legacy_probe, GetObjectBodyCacheHookLookup::Miss) {
                legacy_hook_missed = true;
            }
            if !legacy_hook_missed
                && let GetObjectBodyCacheLookup::Hit(body) = lookup_get_object_body_cache_hit(&cache_adapter, &cache_plan).await
            {
                drop(metadata_admission.take());
                let info = prepared
                    .take()
                    .ok_or_else(|| s3_error!(InternalError, "prepared cache-hit reader is unavailable"))?
                    .into_object_info();
                let reader = GetObjectReader::from_cache_body(info, body).map_err(ApiError::from)?;
                let read_setup =
                    Self::finish_get_object_read(req, manager, bucket, key, rs, part_number, read_start, reader, true).await?;
                return Ok(GetObjectPreparedRead {
                    io_planning: Self::get_object_io_planning_without_disk(manager),
                    read_setup,
                });
            }

            let GetObjectBodyCachePlan::Cacheable(engine_plan) = &cache_plan else {
                break 'snapshot;
            };
            let Some(cache_key) = cache_plan.key().cloned() else {
                break 'snapshot;
            };
            let expected = usize::try_from(response_content_length)
                .map_err(|_| s3_error!(InternalError, "cold-fill body length is not representable"))?;
            let response_size = u64::try_from(response_content_length)
                .map_err(|_| s3_error!(InternalError, "cold-fill body length is negative"))?;
            let waiter_deadline = cold_fill_deadline(wrapper, timeout_config, response_size);
            let proposed_producer_deadline = cold_fill_producer_deadline(timeout_config, response_size);
            let coordinator = cache_adapter.cold_fill_coordinator();
            let info = prepared
                .take()
                .ok_or_else(|| s3_error!(InternalError, "prepared cold-fill reader is unavailable"))?
                .into_object_info();
            drop(metadata_admission.take());
            let outcome = coordinate_cold_fill(&coordinator, cache_key, waiter_deadline, Some(proposed_producer_deadline), {
                let adapter = &cache_adapter;
                let headers = &store_headers;
                let store = &store;
                let range = &rs;
                let object_traffic_health = &object_traffic_health;
                move |producer| {
                    let adapter = Arc::clone(adapter);
                    let engine_plan = engine_plan.clone();
                    let h = headers.clone();
                    let store = Arc::clone(store);
                    let range = range.clone();
                    let bucket = bucket.to_owned();
                    let key = key.to_owned();
                    let opts = opts.clone();
                    let object_traffic_health = object_traffic_health.as_ref().map(Arc::clone);
                    async move {
                        let producer_deadline = producer.deadline();
                        let cancellation = producer.cancellation_token();
                        let second_chance = match await_cold_fill_startup(
                            lookup_cold_fill_second_chance(&adapter, &engine_plan),
                            &cancellation,
                            producer_deadline,
                        )
                        .await
                        {
                            Ok(body) => body,
                            Err(ColdFillStartupWaitError::Cancelled) => {
                                producer.finish(Err(StorageError::OperationCanceled));
                                return;
                            }
                            Err(ColdFillStartupWaitError::DeadlineExceeded) => {
                                producer.relinquish_or_finish(ColdFillError::Storage(StorageError::Timeout));
                                return;
                            }
                        };
                        if let Some(body) = second_chance {
                            producer.finish_shared(Ok(body));
                            return;
                        }

                        let acquire = Self::acquire_cold_fill_io_planning(manager, &bucket, &key);
                        let producer_io = match await_cold_fill_startup(acquire, &cancellation, producer_deadline).await {
                            Ok(result) => result,
                            Err(ColdFillStartupWaitError::Cancelled) => {
                                producer.finish(Err(StorageError::OperationCanceled));
                                return;
                            }
                            Err(ColdFillStartupWaitError::DeadlineExceeded) => {
                                producer.relinquish_or_finish(ColdFillError::Storage(StorageError::Timeout));
                                return;
                            }
                        };
                        let producer_io = match producer_io {
                            Ok(io) => io,
                            Err(err) => {
                                producer.finish_shared(Err(err));
                                return;
                            }
                        };

                        let prepare = track_object_read_setup(
                            object_traffic_health.as_deref(),
                            store.prepare_get_object_reader(&bucket, &key, range.clone(), HeaderMap::new(), &opts),
                        );
                        let prepared = match match await_cold_fill_startup(prepare, &cancellation, producer_deadline).await {
                            Ok(result) => result,
                            Err(ColdFillStartupWaitError::Cancelled) => {
                                producer.finish(Err(StorageError::OperationCanceled));
                                return;
                            }
                            Err(ColdFillStartupWaitError::DeadlineExceeded) => {
                                producer.relinquish_or_finish(ColdFillError::Storage(StorageError::Timeout));
                                return;
                            }
                        } {
                            Ok(prepared) => prepared,
                            Err(err) => {
                                producer.relinquish_or_finish(ColdFillError::Storage(err));
                                return;
                            }
                        };
                        let current_info = prepared.object_info();
                        let current_length = match current_info.get_actual_size() {
                            Ok(length) => length,
                            Err(err) => {
                                let _ = err;
                                producer.finish_shared(Err(ColdFillError::Storage(StorageError::FileCorrupt)));
                                return;
                            }
                        };
                        let current_plan = build_get_object_body_cache_plan_for_revalidation(
                            &adapter,
                            GetObjectBodyCacheRequest {
                                bucket: &bucket,
                                key: &key,
                                info: current_info,
                                response_content_length: current_length,
                                has_range: range.is_some(),
                                part_number,
                                encryption_applied: current_info.is_encrypted(),
                            },
                        );
                        let Some(producer) = retain_cold_fill_producer_for_matching_plan(producer, &current_plan, &engine_plan)
                        else {
                            return;
                        };

                        let reservation = adapter.reserve_body(&engine_plan);
                        #[cfg(test)]
                        let reader_open_plan = engine_plan.clone();
                        start_cold_fill_producer(
                            producer,
                            reservation,
                            || async move { Ok(producer_io) },
                            || {
                                #[cfg(test)]
                                record_cold_fill_reader_open_for_test(&reader_open_plan);
                                let open_reader = prepared.with_headers(h).into_reader();
                                async move { track_object_read_setup(object_traffic_health.as_deref(), open_reader).await }
                            },
                            ColdFillProducerExecution {
                                expected,
                                deadline: producer_deadline,
                                adapter,
                                engine_plan,
                            },
                        )
                        .await;
                    }
                }
            })
            .await;

            match outcome {
                ColdFillCoordinateOutcome::Ready(result) => {
                    let body = match result {
                        Ok(body) => body,
                        Err(ColdFillError::Storage(err)) => return Err(map_get_object_reader_error(err).into()),
                        Err(ColdFillError::DiskAdmissionClosed) => {
                            return Err(s3_error!(InternalError, "disk read semaphore closed"));
                        }
                    };
                    let reader = GetObjectReader::from_cache_body(info, body).map_err(ApiError::from)?;
                    let read_setup =
                        Self::finish_get_object_read(req, manager, bucket, key, rs, part_number, read_start, reader, true)
                            .await?;
                    return Ok(GetObjectPreparedRead {
                        io_planning: Self::get_object_io_planning_without_disk(manager),
                        read_setup,
                    });
                }
                ColdFillCoordinateOutcome::Bypass => {
                    cache_fill_allowed = false;
                    break 'snapshot;
                }
                ColdFillCoordinateOutcome::Rejected => return Err(ApiError::from(StorageError::SlowDown).into()),
            }
        }

        let (io_planning, reader) = if let Some(prepared) = prepared.take() {
            let io_planning = metadata_admission
                .take()
                .ok_or_else(|| s3_error!(InternalError, "prepared metadata admission is unavailable"))?;
            let reader =
                track_object_read_setup(object_traffic_health.as_deref(), prepared.with_headers(store_headers).into_reader())
                    .await
                    .map_err(map_get_object_reader_error)?;
            (io_planning, reader)
        } else {
            let io_planning = Self::acquire_get_object_io_planning(
                manager,
                Some(GetObjectRequestTimeout {
                    wrapper,
                    policy: timeout_config,
                }),
                bucket,
                key,
            )
            .await?;
            let reader = if legacy_hook_missed {
                let prepared = track_object_read_setup(
                    object_traffic_health.as_deref(),
                    store.prepare_get_object_reader(bucket, key, rs.clone(), HeaderMap::new(), opts),
                )
                .await
                .map_err(map_get_object_reader_error)?;
                track_object_read_setup(object_traffic_health.as_deref(), prepared.with_headers(store_headers).into_reader())
                    .await
                    .map_err(map_get_object_reader_error)?
            } else {
                track_object_read_setup(
                    object_traffic_health.as_deref(),
                    store.get_object_reader(bucket, key, rs.clone(), store_headers, opts),
                )
                .await
                .map_err(map_get_object_reader_error)?
            };
            (io_planning, reader)
        };
        let read_setup =
            Self::finish_get_object_read(req, manager, bucket, key, rs, part_number, read_start, reader, cache_fill_allowed)
                .await?;
        if let Some(read_stage_start) = read_stage_start {
            rustfs_io_metrics::record_get_object_stage_duration(
                "s3_handler",
                "store_reader_setup",
                read_stage_start.elapsed().as_secs_f64(),
            );
        }
        Ok(GetObjectPreparedRead { io_planning, read_setup })
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_get_object_read(
        req: &S3Request<GetObjectInput>,
        manager: &ConcurrencyManager,
        bucket: &str,
        key: &str,
        mut rs: Option<HTTPRangeSpec>,
        part_number: Option<usize>,
        read_start: std::time::Instant,
        reader: GetObjectReader,
        cache_fill_allowed: bool,
    ) -> S3Result<GetObjectReadSetup> {
        // ODC-16: capture whether the ecstore cache hook already probed this
        // read, so the app layer does not repeat the lookup it ran after fresh
        // metadata resolution.
        let cache_hook_served = reader.is_cache_hook_served();
        let cache_hook_probed = reader.cache_hook_probed();
        let info = reader.object_info;
        let stream = reader.stream;
        let buffered_body = reader.buffered_body;

        let read_duration = read_start.elapsed();

        // Conditional metrics recording to reduce overhead
        if rustfs_io_metrics::get_stage_metrics_enabled() {
            use rustfs_io_metrics::record_zero_copy_read;
            record_zero_copy_read(info.size as usize, read_duration.as_secs_f64() * 1000.0);
            manager.record_disk_operation(info.size as u64, read_duration, true).await;
        }

        check_preconditions(&req.headers, &info)?;
        Self::validate_get_object_part_number(part_number, &info)?;

        debug!(object_size = info.size, part_count = info.parts.len(), "GET object metadata snapshot");
        for part in info.parts.iter() {
            debug!(
                part_number = part.number,
                part_size = part.size,
                part_actual_size = part.actual_size,
                "GET object part details"
            );
        }

        let content_type = if let Some(content_type) = &info.content_type {
            match ContentType::from_str(content_type) {
                Ok(res) => Some(res),
                Err(err) => {
                    error!(content_type, error = ?err, "GET object content-type parse failed");
                    None
                }
            }
        } else {
            None
        };
        let last_modified = info.mod_time.map(Timestamp::from);

        if let Some(part_number) = part_number
            && rs.is_none()
        {
            rs = HTTPRangeSpec::from_part_sizes(
                info.size,
                part_number,
                info.parts.iter().map(|part| {
                    if part.actual_size > 0 {
                        part.actual_size
                    } else {
                        i64::try_from(part.size).unwrap_or(i64::MAX)
                    }
                }),
            );
        }

        validate_sse_headers_for_read(&info.user_defined, &req.headers)?;

        let mut content_length = info.get_actual_size().map_err(ApiError::from)?;
        let (resume_range_start, resume_range_end, content_range) = if let Some(rs) = &rs {
            let total_size = content_length;
            let (start, length) = rs.get_offset_length(total_size).map_err(ApiError::from)?;
            content_length = length;
            let start = start as i64;
            // Inclusive end of the committed body; may precede `start` when a
            // zero-length range was requested, in which case the body completes
            // immediately and the resume range is never consulted.
            (
                start,
                start + length - 1,
                Some(format!("bytes {}-{}/{}", start, start + length - 1, total_size)),
            )
        } else {
            (0, -1, None)
        };

        debug!(
            "GET object metadata check: parts={}, provided_sse_key={:?}",
            info.parts.len(),
            req.input.sse_customer_key.is_some()
        );

        let read_principal = SseKmsPrincipal::from_request(req);
        let decryption_request = DecryptionRequest {
            bucket,
            key,
            metadata: &info.user_defined,
            sse_customer_key: req.input.sse_customer_key.as_ref(),
            sse_customer_key_md5: req.input.sse_customer_key_md5.as_ref(),
            principal: read_principal.as_ref(),
        };

        let response_content_length = content_length;

        let (
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id,
            encryption_applied,
            final_stream,
            buffered_body,
        ) = match classify_sse_read_response(decryption_request).await? {
            // The stream is already decrypted by the object layer's encryption
            // resolver; only the response headers, authorization and audit
            // summary are derived here, without a second KMS unwrap.
            Some(headers) => (
                Some(headers.server_side_encryption),
                headers.sse_customer_algorithm,
                headers.sse_customer_key_md5,
                headers.ssekms_key_id,
                true,
                wrap_reader(stream),
                None,
            ),
            None => (None, None, None, None, false, wrap_reader(stream), buffered_body),
        };

        Ok(GetObjectReadSetup {
            info,
            final_stream,
            buffered_body,
            cache_hook_served,
            cache_hook_probed,
            cache_fill_allowed,
            rs,
            content_type,
            last_modified,
            response_content_length,
            content_range,
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id,
            encryption_applied,
            resume_range_start,
            resume_range_end,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_get_object_strategy(
        &self,
        manager: &ConcurrencyManager,
        bucket: &str,
        key: &str,
        info: &ObjectInfo,
        rs: Option<&HTTPRangeSpec>,
        response_content_length: i64,
        permit_wait_duration: Duration,
        queue_utilization: f64,
        queue_status: &concurrency::IoQueueStatus,
        concurrent_requests: usize,
    ) -> GetObjectStrategyContext {
        let base_buffer_size = if response_content_length > 0 {
            get_buffer_size_opt_in(response_content_length)
        } else {
            self.base_buffer_size()
        };

        let is_sequential_hint = if rs.is_none() {
            true
        } else if let Some(range_spec) = rs {
            range_spec.start == 0 && !range_spec.is_suffix_length
        } else {
            false
        };

        // Conditional metrics recording to reduce overhead
        if rustfs_io_metrics::get_stage_metrics_enabled() {
            if let Some(range_spec) = rs
                && range_spec.start >= 0
            {
                manager.record_access(range_spec.start as u64, response_content_length as u64);
            }

            if response_content_length > 0 {
                manager.record_transfer(response_content_length as u64, permit_wait_duration);
            }
        }

        let io_strategy =
            manager.calculate_io_strategy_with_context(info.size, base_buffer_size, permit_wait_duration, is_sequential_hint);

        debug!(
            wait_ms = permit_wait_duration.as_millis() as u64,
            load_level = ?io_strategy.load_level,
            buffer_size = io_strategy.buffer_size,
            buffer_multiplier = io_strategy.buffer_multiplier,
            readahead = io_strategy.enable_readahead,
            storage_media = ?io_strategy.storage_media,
            access_pattern = ?io_strategy.access_pattern,
            bandwidth_tier = ?io_strategy.bandwidth_tier,
            concurrent_requests = io_strategy.concurrent_requests,
            file_size = info.size,
            is_sequential = is_sequential_hint,
            "Enhanced multi-factor I/O strategy calculated"
        );

        let io_priority = manager.get_io_priority(response_content_length);

        if manager.is_priority_scheduling_enabled() {
            debug!(
                bucket = %bucket,
                key = %key,
                priority = %io_priority,
                request_size = response_content_length,
                "I/O priority assigned (based on actual request size)"
            );
        }

        rustfs_io_metrics::record_get_object_io_state(
            permit_wait_duration.as_secs_f64(),
            queue_utilization,
            queue_status.permits_in_use,
            queue_status.total_permits.saturating_sub(queue_status.permits_in_use),
            io_strategy.load_level.as_str(),
            io_strategy.buffer_multiplier,
        );
        rustfs_io_metrics::record_io_priority_assignment(io_priority.as_str());

        debug!(
            actual_request_size = response_content_length,
            priority = %io_priority.as_str(),
            "I/O priority finalized with actual request size"
        );

        let optimal_buffer_size = if io_strategy.buffer_size > 0 {
            io_strategy.buffer_size
        } else {
            get_concurrency_aware_buffer_size(response_content_length, base_buffer_size)
        };

        debug!(
            "GetObject buffer sizing: file_size={}, base={}, optimal={}, concurrent_requests={}, io_strategy={:?}",
            response_content_length, base_buffer_size, optimal_buffer_size, concurrent_requests, io_strategy.load_level
        );
        let enable_readahead = io_strategy.enable_readahead;

        GetObjectStrategyContext {
            io_strategy,
            optimal_buffer_size,
            enable_readahead,
        }
    }

    fn build_get_object_checksums(
        info: &ObjectInfo,
        headers: &HeaderMap,
        part_number: Option<usize>,
        rs: Option<&HTTPRangeSpec>,
    ) -> S3Result<ResponseChecksums> {
        if let Some(checksum_mode) = headers.get(AMZ_CHECKSUM_MODE)
            && checksum_mode.to_str().unwrap_or_default() == "ENABLED"
            && rs.is_none()
        {
            let (decrypted_checksums, is_multipart) = info.decrypt_checksums(part_number.unwrap_or(0), headers).map_err(|e| {
                error!(error = %e, "GetObject checksum decryption failed");
                ApiError::from(e)
            })?;

            return Ok(classify_response_checksums(decrypted_checksums, is_multipart));
        }

        Ok(ResponseChecksums::default())
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_get_object_body<R, F>(
        final_stream: R,
        info: &ObjectInfo,
        response_content_length: i64,
        request_id: &str,
        content_range: Option<&str>,
        optimal_buffer_size: usize,
        enable_readahead: bool,
        concurrent_requests: usize,
        part_number: Option<usize>,
        has_range: bool,
        encryption_applied: bool,
        buffered_body: Option<Bytes>,
        bucket: &str,
        key: &str,
        mut lifecycle: GetObjectBodyLifecycle,
        resume: F,
    ) -> S3Result<StreamingBlob>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
        F: FnOnce(&ObjectInfo) -> Option<GetObjectResumeControl<R>>,
    {
        if encryption_applied {
            let should_buffer_encrypted_object =
                should_buffer_get_object_in_memory(info, response_content_length, part_number, has_range, concurrent_requests);

            if should_buffer_encrypted_object {
                // Strict materialization (#1324): a decrypted body that is shorter
                // or longer than the declared content length must hard-fail before
                // headers, not warn-and-serve a truncated/over-long body.
                let expected = usize::try_from(response_content_length.max(0)).unwrap_or(usize::MAX);
                match strict_materialize_object_body(final_stream, expected, GET_OBJECT_STAGE_BODY_ENCRYPTED_BUFFER_READ).await {
                    Ok(buf) => {
                        return Ok(Self::build_memory_blob(
                            buf,
                            response_content_length,
                            GET_MEMORY_BODY_SOURCE_ENCRYPTED_BUFFER,
                            lifecycle,
                        ));
                    }
                    Err(e) => {
                        lifecycle.finish_err();
                        error!(error = %e, "GetObject decrypted object strict materialization failed");
                        return Err(e.into_s3_error(response_content_length));
                    }
                }
            }

            debug!(buffer_size = optimal_buffer_size, "Encrypted object uses streaming decrypt path");
            let stream_strategy_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
            let (stream_buffer_size, stream_strategy) =
                Self::select_stream_buffer_strategy(response_content_length, optimal_buffer_size, enable_readahead, has_range);
            record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_BODY_STREAM_STRATEGY, stream_strategy_start);
            return Ok(Self::build_reader_blob(
                final_stream,
                response_content_length,
                request_id,
                content_range,
                stream_buffer_size,
                stream_strategy,
                bucket,
                key,
                lifecycle,
                resume(info),
            ));
        }

        if let Some(buffered_body) = buffered_body {
            // Strict materialization (#1324): the buffered body is the exact
            // response payload; a length disagreement means an upstream/cache bug
            // and must hard-fail before headers rather than serve a body that does
            // not match its committed Content-Length.
            let expected = usize::try_from(response_content_length.max(0)).unwrap_or(usize::MAX);
            if buffered_body.len() != expected {
                lifecycle.finish_err();
                error!(
                    expected = response_content_length,
                    actual = buffered_body.len(),
                    "Buffered GetObject body length mismatch"
                );
                return Err(ApiError::from(StorageError::other(format!(
                    "Buffered GetObject body length mismatch: expected {response_content_length}, got {}",
                    buffered_body.len()
                )))
                .into());
            }

            return Ok(Self::build_memory_bytes_blob(
                buffered_body,
                response_content_length,
                GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
                lifecycle,
            ));
        }

        let should_provide_seek_support =
            should_buffer_get_object_in_memory(info, response_content_length, part_number, has_range, concurrent_requests);

        if should_provide_seek_support {
            // Strict materialization (#1324): the previous implementation only
            // logged a warning on a length mismatch, and — most dangerously — on a read
            // error it fell through to streaming the *same* reader after
            // `read_to_end` had already drained K bytes, shipping a body missing
            // its prefix (prefix-misaligned data). Both are now hard errors: an
            // exact-length read is required, and any read error returns without
            // reusing the partially consumed reader.
            let expected = usize::try_from(response_content_length.max(0)).unwrap_or(usize::MAX);
            match strict_materialize_object_body(final_stream, expected, GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ).await {
                Ok(buf) => {
                    return Ok(Self::build_memory_blob(
                        buf,
                        response_content_length,
                        GET_MEMORY_BODY_SOURCE_SEEK_BUFFER,
                        lifecycle,
                    ));
                }
                Err(e) => {
                    lifecycle.finish_err();
                    error!(
                        error = %e,
                        "GetObject seek-support strict materialization failed; refusing to reuse the partially consumed reader"
                    );
                    return Err(e.into_s3_error(response_content_length));
                }
            }
        }

        let stream_strategy_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let (stream_buffer_size, stream_strategy) =
            Self::select_stream_buffer_strategy(response_content_length, optimal_buffer_size, enable_readahead, has_range);
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_BODY_STREAM_STRATEGY, stream_strategy_start);
        Ok(Self::build_reader_blob(
            final_stream,
            response_content_length,
            request_id,
            content_range,
            stream_buffer_size,
            stream_strategy,
            bucket,
            key,
            lifecycle,
            resume(info),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_get_object_body_with_cache<R, F>(
        cache_adapter: &ObjectDataCacheAdapter,
        final_stream: R,
        info: &ObjectInfo,
        response_content_length: i64,
        request_id: &str,
        content_range: Option<&str>,
        optimal_buffer_size: usize,
        enable_readahead: bool,
        concurrent_requests: usize,
        part_number: Option<usize>,
        has_range: bool,
        encryption_applied: bool,
        mut buffered_body: Option<Bytes>,
        cache_hook_served: bool,
        cache_hook_probed: bool,
        cache_fill_allowed: bool,
        bucket: &str,
        key: &str,
        mut lifecycle: GetObjectBodyLifecycle,
        resume: F,
    ) -> S3Result<StreamingBlob>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
        F: FnOnce(&ObjectInfo) -> Option<GetObjectResumeControl<R>>,
    {
        // ODC-16 (backlog#1121): when the ecstore hook or shared cold fill
        // already supplied this body, the request-level plan was built before
        // the authoritative lookup. Serve it without planning a second time.
        if cache_hook_served && let Some(bytes) = buffered_body.take() {
            return Ok(Self::build_memory_bytes_blob(
                bytes,
                response_content_length,
                GET_MEMORY_BODY_SOURCE_OBJECT_DATA_CACHE,
                lifecycle,
            ));
        }

        if !cache_fill_allowed {
            return Self::build_get_object_body(
                final_stream,
                info,
                response_content_length,
                request_id,
                content_range,
                optimal_buffer_size,
                enable_readahead,
                concurrent_requests,
                part_number,
                has_range,
                encryption_applied,
                buffered_body,
                bucket,
                key,
                lifecycle,
                resume,
            )
            .await;
        }

        let cache_request = GetObjectBodyCacheRequest {
            bucket,
            key,
            info,
            response_content_length,
            has_range,
            part_number,
            encryption_applied,
        };
        let cache_plan = build_get_object_body_cache_plan(cache_adapter, cache_request);

        // ODC-16: only look up when the hook did not probe this read. When it did
        // probe (a served body handled above, or a miss), its result is
        // authoritative because it ran after fresh metadata resolution, so the
        // app layer skips its own lookup and only uses the plan to fill.
        if !cache_hook_probed {
            match lookup_get_object_body_cache_hit(cache_adapter, &cache_plan).await {
                GetObjectBodyCacheLookup::Hit(bytes) => {
                    return Ok(Self::build_memory_bytes_blob(
                        bytes,
                        response_content_length,
                        GET_MEMORY_BODY_SOURCE_OBJECT_DATA_CACHE,
                        lifecycle,
                    ));
                }
                GetObjectBodyCacheLookup::Disabled | GetObjectBodyCacheLookup::Skip | GetObjectBodyCacheLookup::Miss => {}
            }
        }

        if let Some(buffered_body) = buffered_body {
            // ODC-15: the body is already fully in hand, so keep the fill off the
            // response's critical path. For a cacheable plan, run the fill in a
            // detached task (Bytes is a cheap clone) and return immediately. For
            // a non-cacheable plan the fill is a pure metric-only skip with no
            // I/O, so record it inline to preserve observability.
            if cache_fill_allowed && matches!(cache_plan, GetObjectBodyCachePlan::Cacheable(_)) {
                let cache_adapter = cache_adapter.clone();
                let cache_plan = cache_plan.clone();
                let fill_bytes = buffered_body.clone();
                tokio::spawn(async move {
                    let _ = fill_get_object_body_cache_from_buffered_body(&cache_adapter, &cache_plan, &fill_bytes).await;
                });
            } else if cache_fill_allowed {
                let _ = fill_get_object_body_cache_from_buffered_body(cache_adapter, &cache_plan, &buffered_body).await;
            }

            return Ok(Self::build_memory_bytes_blob(
                buffered_body,
                response_content_length,
                GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
                lifecycle,
            ));
        }

        let should_materialize_for_cache = cache_adapter.materialize_fill_enabled()
            && cache_fill_allowed
            && matches!(cache_plan, GetObjectBodyCachePlan::Cacheable(_))
            && should_materialize_get_object_body_for_cache(
                info,
                response_content_length,
                part_number,
                has_range,
                concurrent_requests,
            );

        if should_materialize_for_cache {
            let Ok(materialized_capacity) = usize::try_from(response_content_length) else {
                warn!(
                    expected = response_content_length,
                    "GetObject materialize-fill skipped because content length is not representable"
                );
                return Self::build_get_object_body(
                    final_stream,
                    info,
                    response_content_length,
                    request_id,
                    content_range,
                    optimal_buffer_size,
                    enable_readahead,
                    concurrent_requests,
                    part_number,
                    has_range,
                    encryption_applied,
                    None,
                    bucket,
                    key,
                    lifecycle,
                    resume,
                )
                .await;
            };
            // ODC-07 / #1324: share the strict exact-length materialization gate
            // with the encrypted and seek memory branches. The helper bounds the
            // read to `capacity + 1` (so an over-long stream is detected without
            // buffering it unbounded), rejects short and over-long reads, and on a
            // partial-read error refuses to reuse the consumed reader.
            match strict_materialize_object_body(
                final_stream,
                materialized_capacity,
                GET_OBJECT_STAGE_BODY_CACHE_MATERIALIZE_READ,
            )
            .await
            {
                Ok(buf) => {
                    let bytes = Bytes::from(buf);
                    // ODC-15: fill off the response's critical path (see the
                    // buffered-body branch above).
                    let cache_adapter = cache_adapter.clone();
                    let cache_plan = cache_plan.clone();
                    let fill_bytes = bytes.clone();
                    tokio::spawn(async move {
                        let _ = fill_get_object_body_cache_from_materialized_body(&cache_adapter, &cache_plan, &fill_bytes).await;
                    });

                    return Ok(Self::build_memory_bytes_blob(
                        bytes,
                        response_content_length,
                        GET_MEMORY_BODY_SOURCE_OBJECT_DATA_CACHE_MATERIALIZED,
                        lifecycle,
                    ));
                }
                Err(e) => {
                    lifecycle.finish_err();
                    error!(error = %e, "GetObject materialize-fill strict materialization failed");
                    // A short/over-long body would ship a truncated or over-long
                    // response; a partial-read error leaves the stream consumed so
                    // falling back to streaming would send a prefix-misaligned
                    // body. Both fail the request.
                    return Err(e.into_s3_error(response_content_length));
                }
            }
        }

        Self::build_get_object_body(
            final_stream,
            info,
            response_content_length,
            request_id,
            content_range,
            optimal_buffer_size,
            enable_readahead,
            concurrent_requests,
            part_number,
            has_range,
            encryption_applied,
            None,
            bucket,
            key,
            lifecycle,
            resume,
        )
        .await
    }

    fn finalize_get_object_completion(
        wrapper: &RequestTimeoutWrapper,
        timeout_config: &GetObjectTimeoutPolicy,
        total_duration: Duration,
        response_content_length: i64,
        optimal_buffer_size: usize,
    ) {
        rustfs_io_metrics::record_get_object_completion(
            total_duration.as_secs_f64(),
            response_content_length,
            optimal_buffer_size,
        );

        rustfs_io_metrics::record_get_object(total_duration.as_millis() as f64, response_content_length);

        if wrapper.is_timeout() {
            warn!(
                "GetObject request exceeded timeout: duration={:?} timeout={:?}",
                wrapper.elapsed(),
                timeout_config.get_object_timeout
            );
            rustfs_io_metrics::record_get_object_timeout(None, Some(wrapper.elapsed().as_secs_f64()));
        }

        debug!(
            "GetObject completed: size={} duration={:?} buffer={}",
            response_content_length, total_duration, optimal_buffer_size
        );
    }

    fn ensure_get_object_not_timed_out(
        wrapper: &RequestTimeoutWrapper,
        timeout_config: &GetObjectTimeoutPolicy,
        bucket: &str,
        key: &str,
        stage: GetObjectTimeoutStage,
    ) -> S3Result<()> {
        if !wrapper.is_timeout() {
            return Ok(());
        }

        let timeout_secs = timeout_config.get_object_timeout.as_secs();
        let elapsed_ms = wrapper.elapsed().as_millis();

        match stage {
            GetObjectTimeoutStage::BeforeProcessing => {
                warn!(
                    bucket = %bucket,
                    key = %key,
                    timeout_secs,
                    elapsed_ms,
                    "GetObject request timed out before processing"
                );
                Err(s3_error!(InternalError, "Request timeout before processing"))
            }
            GetObjectTimeoutStage::DiskPermitWait { permit_wait_duration } => {
                warn!(
                    bucket = %bucket,
                    key = %key,
                    wait_ms = permit_wait_duration.as_millis(),
                    timeout_secs,
                    elapsed_ms,
                    "GetObject request timed out while waiting for disk permit"
                );
                rustfs_io_metrics::record_get_object_timeout(Some("disk_permit"), Some(wrapper.elapsed().as_secs_f64()));
                Err(s3_error!(InternalError, "Request timeout while waiting for disk permit"))
            }
            GetObjectTimeoutStage::BeforeRead => {
                warn!(
                    bucket = %bucket,
                    key = %key,
                    timeout_secs,
                    elapsed_ms,
                    "GetObject request timed out before reading object"
                );
                rustfs_io_metrics::record_get_object_timeout(Some("before_read"), Some(wrapper.elapsed().as_secs_f64()));
                Err(s3_error!(InternalError, "Request timeout before reading object"))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn finalize_get_object_response(
        helper: OperationHelper,
        bucket: &str,
        method: &hyper::Method,
        headers: &HeaderMap,
        event_info: Option<ObjectInfo>,
        version_id_for_event: String,
        output: GetObjectOutput,
        extra_checksum_headers: Vec<(&'static str, String)>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let helper = match event_info {
            Some(event_info) => helper.object(event_info),
            None => helper,
        };
        let helper = helper.version_id(version_id_for_event);
        let mut response = wrap_response_with_cors(bucket, method, headers, output).await;
        inject_accept_ranges_header(&mut response.headers);
        // Emit XXHash3/64/128 and SHA-512 checksums that s3s GetObjectOutput cannot
        // carry (#1257). This is the download-side integrity path AWS SDKs verify.
        inject_additional_checksum_headers(&mut response.headers, &extra_checksum_headers);
        let result = Ok(response);
        let _ = helper.complete(&result);
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_get_object_output_context<F>(
        &self,
        req: &S3Request<GetObjectInput>,
        manager: &ConcurrencyManager,
        bucket: &str,
        key: &str,
        info: ObjectInfo,
        event_info: Option<ObjectInfo>,
        final_stream: DynReader,
        buffered_body: Option<Bytes>,
        cache_hook_served: bool,
        cache_hook_probed: bool,
        cache_fill_allowed: bool,
        rs: Option<HTTPRangeSpec>,
        content_type: Option<ContentType>,
        last_modified: Option<Timestamp>,
        response_content_length: i64,
        content_range: Option<String>,
        request_id: &str,
        server_side_encryption: Option<ServerSideEncryption>,
        sse_customer_algorithm: Option<SSECustomerAlgorithm>,
        sse_customer_key_md5: Option<SSECustomerKeyMD5>,
        ssekms_key_id: Option<SSEKMSKeyId>,
        encryption_applied: bool,
        permit_wait_duration: Duration,
        queue_utilization: f64,
        queue_status: &concurrency::IoQueueStatus,
        concurrent_requests: usize,
        part_number: Option<usize>,
        versioned: bool,
        lifecycle: GetObjectBodyLifecycle,
        resume: F,
    ) -> S3Result<GetObjectOutputContext>
    where
        F: FnOnce(&ObjectInfo) -> Option<GetObjectResumeControl<DynReader>>,
    {
        let strategy_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let strategy = self.finalize_get_object_strategy(
            manager,
            bucket,
            key,
            &info,
            rs.as_ref(),
            response_content_length,
            permit_wait_duration,
            queue_utilization,
            queue_status,
            concurrent_requests,
        );
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_OUTPUT_STRATEGY, strategy_start);
        let GetObjectStrategyContext {
            io_strategy: _,
            optimal_buffer_size,
            enable_readahead,
        } = strategy;
        let cache_adapter = self.object_data_cache();

        let body_build_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let body = Self::build_get_object_body_with_cache(
            &cache_adapter,
            final_stream,
            &info,
            response_content_length,
            request_id,
            content_range.as_deref(),
            optimal_buffer_size,
            enable_readahead,
            concurrent_requests,
            part_number,
            rs.is_some(),
            encryption_applied,
            buffered_body,
            cache_hook_served,
            cache_hook_probed,
            cache_fill_allowed,
            bucket,
            key,
            lifecycle,
            resume,
        )
        .await?;
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_BODY_BUILD, body_build_start);

        let checksum_headers_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let checksums = Self::build_get_object_checksums(&info, &req.headers, part_number, rs.as_ref())?;
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_CHECKSUM_HEADERS, checksum_headers_start);

        let output_version_id = if versioned {
            info.version_id.map(|vid| {
                if vid == Uuid::nil() {
                    "null".to_string()
                } else {
                    vid.to_string()
                }
            })
        } else {
            None
        };

        // x-amz-restore: extract from object metadata
        let restore = info.user_defined.get(X_AMZ_RESTORE.as_str()).and_then(|v| {
            let rs = parse_restore_obj_status(v).ok()?;
            Some(rs.to_string2())
        });

        // x-amz-expiration: predict from lifecycle configuration
        let lifecycle_expiration_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let expiration = resolve_put_object_expiration(bucket, &info).await;
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_LIFECYCLE_EXPIRATION, lifecycle_expiration_start);
        let storage_class = response_storage_class(&info, &info.user_defined);
        let cache_control = info.user_defined.get("cache-control").cloned();
        let content_disposition = info.user_defined.get("content-disposition").cloned();

        let metadata_filter_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let metadata = filter_object_metadata(&info.user_defined);
        record_get_object_s3_handler_stage_duration(GET_OBJECT_STAGE_METADATA_FILTER, metadata_filter_start);

        let output = GetObjectOutput {
            body: Some(body),
            content_length: Some(response_content_length),
            last_modified,
            content_type,
            content_encoding: info.content_encoding.clone(),
            cache_control,
            content_disposition,
            content_range,
            e_tag: info.etag.map(|etag| to_s3s_etag(&etag)),
            metadata,
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id,
            checksum_crc32: checksums.crc32,
            checksum_crc32c: checksums.crc32c,
            checksum_sha1: checksums.sha1,
            checksum_sha256: checksums.sha256,
            checksum_crc64nvme: checksums.crc64nvme,
            checksum_type: checksums.checksum_type,
            version_id: output_version_id,
            restore,
            expiration,
            storage_class,
            ..Default::default()
        };

        Ok(GetObjectOutputContext {
            output,
            event_info,
            response_content_length,
            optimal_buffer_size,
            extra_checksum_headers: checksums.extra,
        })
    }

    /// Serve a GET whose local read failed with not-found by proxying to the
    /// bucket's replication targets (MinIO `proxyGetToReplicationTarget`,
    /// backlog#1675 P1-5). Returns None when no target can serve the object;
    /// the caller then returns the original local error.
    async fn proxy_get_object_to_replication_targets(
        req: &S3Request<GetObjectInput>,
        bucket: &str,
        key: &str,
        opts: &ObjectOptions,
    ) -> Option<GetObjectOutput> {
        let targets = get_read_proxy_targets(bucket, key, opts).await;
        if targets.is_empty() {
            return None;
        }
        let extra_headers = Self::proxy_read_passthrough_headers(&req.headers);
        let range = req
            .headers
            .get(http::header::RANGE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let part_number = req.input.part_number;

        for target in targets {
            match target
                .get_object(
                    &target.bucket,
                    key,
                    opts.version_id.clone(),
                    range.clone(),
                    part_number,
                    extra_headers.clone(),
                )
                .await
            {
                Ok(remote) => {
                    // MinIO-aligned accounting: one total per proxy attempt
                    // (targets were available), one failed when no target
                    // served it — never per target.
                    record_replication_proxy(bucket, "GetObject", false).await;
                    return Some(Self::proxy_sdk_get_output_to_s3s(remote));
                }
                Err(err) if Self::proxy_sdk_error_is_not_found(&err) => {
                    debug!(bucket, key, arn = %target.arn, "read proxy: target does not have the object");
                }
                Err(err) => {
                    warn!(bucket, key, arn = %target.arn, error = %err, "read proxy: GET against replication target failed");
                }
            }
        }
        record_replication_proxy(bucket, "GetObject", true).await;
        None
    }

    /// Translate a proxied SDK GET response into the s3s output, forwarding
    /// the body as a stream (no buffering, no local persistence).
    fn proxy_sdk_get_output_to_s3s(remote: aws_sdk_s3::operation::get_object::GetObjectOutput) -> GetObjectOutput {
        let body = remote.body;
        let body_stream = tokio_util::io::ReaderStream::with_capacity(body.into_async_read(), 64 * 1024);
        GetObjectOutput {
            body: Some(StreamingBlob::wrap(body_stream)),
            content_length: remote.content_length,
            content_range: remote.content_range,
            content_type: remote.content_type.as_deref().and_then(|v| ContentType::from_str(v).ok()),
            content_encoding: remote.content_encoding,
            content_disposition: remote.content_disposition,
            content_language: remote.content_language,
            cache_control: remote.cache_control,
            e_tag: remote.e_tag.as_deref().and_then(|v| ETag::from_str(v).ok()),
            last_modified: remote
                .last_modified
                .and_then(|dt| OffsetDateTime::from_unix_timestamp_nanos(dt.as_nanos()).ok())
                .map(Timestamp::from),
            metadata: remote.metadata,
            version_id: remote.version_id,
            server_side_encryption: remote
                .server_side_encryption
                .map(|sse| ServerSideEncryption::from(sse.as_str().to_string())),
            sse_customer_algorithm: remote.sse_customer_algorithm,
            sse_customer_key_md5: remote.sse_customer_key_md5,
            ssekms_key_id: remote.ssekms_key_id,
            parts_count: remote.parts_count,
            tag_count: remote.tag_count,
            storage_class: remote.storage_class.map(|sc| StorageClass::from(sc.as_str().to_string())),
            expiration: remote.expiration,
            restore: remote.restore,
            checksum_crc32: remote.checksum_crc32,
            checksum_crc32c: remote.checksum_crc32_c,
            checksum_crc64nvme: remote.checksum_crc64_nvme,
            checksum_sha1: remote.checksum_sha1,
            checksum_sha256: remote.checksum_sha256,
            checksum_type: remote.checksum_type.map(|ct| ChecksumType::from(ct.as_str().to_string())),
            ..Default::default()
        }
    }

    #[instrument(name = "execute_get_object", level = "trace", skip(self, req))]
    pub async fn execute_get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        self.execute_get_object_boxed(req).await
    }

    fn execute_get_object_boxed(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> impl std::future::Future<Output = S3Result<S3Response<GetObjectOutput>>> + Send + '_ {
        Box::pin(self.execute_get_object_inner(req))
    }

    async fn execute_get_object_inner(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let inbound_request_context = req.extensions.get::<request_context::RequestContext>();
        let request_id = inbound_request_context
            .map(|ctx| ctx.request_id.clone())
            .unwrap_or_else(|| request_context::RequestContext::fallback().request_id);
        if rustfs_io_metrics::get_stage_metrics_enabled()
            && let Some(context) = inbound_request_context
        {
            rustfs_io_metrics::record_get_object_stage_duration(
                GET_OBJECT_STAGE_PATH_S3_HANDLER,
                GET_OBJECT_STAGE_REQUEST_INGRESS_TO_CONTEXT,
                context.start_time.elapsed().as_secs_f64(),
            );
        }
        let bootstrap = self.init_get_object_bootstrap(&req.input.bucket, &req.input.key, &request_id)?;
        let timeout_config = bootstrap.timeout_config;
        let wrapper = bootstrap.wrapper;
        let request_start = bootstrap.request_start;
        let concurrent_requests = bootstrap.concurrent_requests;
        let mut lifecycle = GetObjectBodyLifecycle::tracked(bootstrap.request_guard);

        let helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject).suppress_event();
        // mc get 3

        // Cheap request-shape validations run first so invalid requests keep
        // their InvalidArgument precedence over bucket existence.
        let validated = match Self::validate_get_object_request(&req) {
            Ok(validated) => validated,
            Err(err) => {
                lifecycle.finish_err();
                return Err(err);
            }
        };

        // SF05: Store lookup next (5s-TTL bucket-validation cache). Bucket
        // existence is established before any bucket-metadata work, so requests
        // naming nonexistent buckets fail before the versioning lookup in
        // get_opts. The store comes from the request-bound server context
        // (backlog#1052 S6), not the process-global handle.
        let object_traffic_health = self.object_traffic_health();
        let object_metadata_progress = object_traffic_health
            .as_deref()
            .and_then(ObjectTrafficHealth::track_read_metadata);
        let store_lookup_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let Some(store) = self.object_store() else {
            lifecycle.finish_err();
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        if let Err(err) = validate_bucket_exists(&store, &req.input.bucket).await {
            lifecycle.finish_err();
            return Err(err);
        }
        if let Some(store_lookup_start) = store_lookup_start {
            rustfs_io_metrics::record_get_object_stage_duration(
                "s3_handler",
                "store_lookup",
                store_lookup_start.elapsed().as_secs_f64(),
            );
        }

        let request_context_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let request_context = match Self::prepare_get_object_request_context(validated, &req.headers).await {
            Ok(request_context) => request_context,
            Err(err) => {
                lifecycle.finish_err();
                return Err(err);
            }
        };
        if let Some(request_context_start) = request_context_start {
            rustfs_io_metrics::record_get_object_stage_duration(
                "s3_handler",
                "request_context",
                request_context_start.elapsed().as_secs_f64(),
            );
        }
        let GetObjectRequestContext {
            bucket,
            key,
            version_id_for_event,
            part_number,
            rs,
            opts,
        } = request_context;
        drop(object_metadata_progress);

        let manager = get_concurrency_manager();

        let prepared_read = match self
            .prepare_get_object_read_execution(
                &req,
                manager,
                store.clone(),
                &wrapper,
                &timeout_config,
                &bucket,
                &key,
                rs,
                &opts,
                part_number,
                object_traffic_health,
            )
            .await
        {
            Ok(prepared_read) => prepared_read,
            Err(err) => {
                // Active-active replication lag window: an object missing
                // locally (and only missing — other errors keep their
                // semantics) may still be served by proxying the GET to a
                // replication target (backlog#1675 P1-5).
                if matches!(*err.code(), S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion)
                    && let Some(output) = Self::proxy_get_object_to_replication_targets(&req, &bucket, &key, &opts).await
                {
                    lifecycle.finish_ok();
                    let mut response = wrap_response_with_cors(&bucket, &req.method, &req.headers, output).await;
                    inject_accept_ranges_header(&mut response.headers);
                    let result = Ok(response);
                    let _ = helper.version_id(version_id_for_event).complete(&result);
                    return result;
                }
                lifecycle.finish_err();
                return Err(err);
            }
        };
        let GetObjectPreparedRead { io_planning, read_setup } = prepared_read;
        let GetObjectIoPlanning {
            disk_permit,
            permit_wait_duration,
            queue_status,
            queue_utilization,
        } = io_planning;

        let GetObjectReadSetup {
            info,
            final_stream,
            buffered_body,
            cache_hook_served,
            cache_hook_probed,
            cache_fill_allowed,
            rs,
            content_type,
            last_modified,
            response_content_length,
            content_range,
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id,
            encryption_applied,
            resume_range_start,
            resume_range_end,
        } = read_setup;
        let final_stream = if let Some(disk_permit) = disk_permit {
            wrap_reader(DiskReadPermitReader::new(final_stream, disk_permit))
        } else {
            final_stream
        };

        // Clone ObjectInfo for event notification only when an event will
        // actually be built — the clone is expensive for multipart objects.
        let event_info = helper.wants_object_info().then(|| info.clone());

        let output_build_start = rustfs_io_metrics::get_stage_metrics_enabled().then(std::time::Instant::now);
        let output_context = self
            .build_get_object_output_context(
                &req,
                manager,
                &bucket,
                &key,
                info,
                event_info,
                final_stream,
                buffered_body,
                cache_hook_served,
                cache_hook_probed,
                cache_fill_allowed,
                rs,
                content_type,
                last_modified,
                response_content_length,
                content_range,
                &request_id,
                server_side_encryption,
                sse_customer_algorithm,
                sse_customer_key_md5,
                ssekms_key_id,
                encryption_applied,
                permit_wait_duration,
                queue_utilization,
                &queue_status,
                concurrent_requests,
                part_number,
                opts.versioned,
                lifecycle,
                |info| {
                    Some(get_object_resume_control(GetObjectResumeContext::new(
                        store,
                        &bucket,
                        &key,
                        opts,
                        &req.headers,
                        info,
                        resume_range_start,
                        resume_range_end,
                    )))
                },
            )
            .await;
        let output_context = match output_context {
            Ok(output_context) => output_context,
            Err(err) => return Err(err),
        };
        if let Some(output_build_start) = output_build_start {
            rustfs_io_metrics::record_get_object_stage_duration(
                "s3_handler",
                "output_build",
                output_build_start.elapsed().as_secs_f64(),
            );
        }
        let GetObjectOutputContext {
            output,
            event_info,
            response_content_length,
            optimal_buffer_size,
            extra_checksum_headers,
        } = output_context;

        let total_duration = request_start.elapsed();
        Self::finalize_get_object_completion(
            &wrapper,
            &timeout_config,
            total_duration,
            response_content_length,
            optimal_buffer_size,
        );

        Self::finalize_get_object_response(
            helper,
            &bucket,
            &req.method,
            &req.headers,
            event_info,
            version_id_for_event,
            output,
            extra_checksum_headers,
        )
        .await
    }

    pub async fn execute_get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper =
            OperationHelper::new(&req, EventName::ObjectAccessedAttributes, S3Operation::GetObjectAttributes).suppress_event();
        let GetObjectAttributesInput {
            bucket,
            key,
            max_parts,
            object_attributes,
            part_number_marker,
            version_id,
            sse_customer_key,
            sse_customer_key_md5,
            ..
        } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let mut opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        opts.include_part_checksums = object_attributes_requested(&object_attributes, ObjectAttributes::OBJECT_PARTS);

        let info = match store.get_object_info(&bucket, &key, &opts).await {
            Ok(info) => info,
            Err(err) => {
                if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                    if is_dir_object(&key) {
                        let has_children = match probe_prefix_has_children(store, &bucket, &key, false).await {
                            Ok(has_children) => has_children,
                            Err(e) => {
                                error!(
                                    "Failed to probe children for object attributes (bucket: {}, key: {}): {}",
                                    bucket, key, e
                                );
                                false
                            }
                        };
                        let msg = head_prefix_not_found_message(&bucket, &key, has_children);
                        return Err(S3Error::with_message(S3ErrorCode::NoSuchKey, msg));
                    }
                    return Err(S3Error::new(S3ErrorCode::NoSuchKey));
                }
                return Err(ApiError::from(err).into());
            }
        };

        if info.delete_marker {
            if opts.version_id.is_none() {
                return Err(S3Error::new(S3ErrorCode::NoSuchKey));
            }
            return Err(S3Error::new(S3ErrorCode::MethodNotAllowed));
        }

        validate_ssec_for_read(&info.user_defined, sse_customer_key.as_ref(), sse_customer_key_md5.as_ref())?;

        let metadata_map = info.user_defined.clone();
        debug!(
            "GetObjectAttributes raw object_attributes={:?}",
            object_attributes.iter().map(|value| value.as_str()).collect::<Vec<_>>()
        );

        let requested = |name: &'static str| -> bool { object_attributes_requested(&object_attributes, name) };
        let storage_class =
            response_storage_class_for_object_attributes(&info, &metadata_map, requested(ObjectAttributes::STORAGE_CLASS));

        let e_tag = if requested(ObjectAttributes::ETAG) {
            info.etag.as_ref().map(|etag| to_s3s_etag(etag))
        } else {
            None
        };

        let object_size = if requested(ObjectAttributes::OBJECT_SIZE) {
            Some(info.get_actual_size().map_err(ApiError::from)?)
        } else {
            None
        };

        let checksum = if requested(ObjectAttributes::CHECKSUM) {
            let (checksums, is_multipart) = info.decrypt_checksums(0, &req.headers).map_err(ApiError::from)?;
            // GetObjectAttributes returns checksums in the XML body, and s3s's Checksum
            // type has no field for the additional algorithms, so `extra` cannot be
            // surfaced here (unlike the header-based GET/HEAD paths) — an s3s limitation
            // tracked for when it gains typed fields.
            let ResponseChecksums {
                crc32: checksum_crc32,
                crc32c: checksum_crc32c,
                sha1: checksum_sha1,
                sha256: checksum_sha256,
                crc64nvme: checksum_crc64nvme,
                checksum_type,
                ..
            } = classify_response_checksums(checksums, is_multipart);

            Some(Checksum {
                checksum_crc32,
                checksum_crc32c,
                checksum_sha1,
                checksum_sha256,
                checksum_crc64nvme,
                checksum_type,
                ..Default::default()
            })
        } else {
            None
        };
        let object_parts = if requested(ObjectAttributes::OBJECT_PARTS) && info.is_multipart() {
            let params = parse_list_parts_params(part_number_marker, max_parts)?;
            let mut parts = Vec::new();
            let mut marker = params.part_number_marker;
            let max_parts = params.max_parts;
            let mut start_at = 0usize;

            if let Some(marker_value) = marker {
                if let Some(index) = info.parts.iter().position(|part| part.number == marker_value) {
                    start_at = index + 1;
                } else {
                    marker = None;
                }
            }

            let max_parts: i32 = max_parts.try_into().map_err(|_| {
                S3Error::with_message(S3ErrorCode::InvalidArgument, "max-parts value is out of range".to_string())
            })?;
            let end = (start_at + params.max_parts).min(info.parts.len());
            let is_truncated = end < info.parts.len();

            for part in &info.parts[start_at..end] {
                let (checksums, is_multipart) = info.decrypt_checksums(part.number, &req.headers).map_err(ApiError::from)?;
                // Additional algorithms cannot be surfaced in the ObjectPart XML body
                // (s3s has no field); same limitation as the object-level attributes above.
                let ResponseChecksums {
                    crc32: checksum_crc32,
                    crc32c: checksum_crc32c,
                    sha1: checksum_sha1,
                    sha256: checksum_sha256,
                    crc64nvme: checksum_crc64nvme,
                    ..
                } = classify_response_checksums(checksums, is_multipart);

                let part_size = if part.actual_size > 0 {
                    part.actual_size
                } else {
                    part.size.try_into().map_err(|_| {
                        S3Error::with_message(S3ErrorCode::InvalidArgument, "Part size value is out of range".to_string())
                    })?
                };

                parts.push(ObjectPart {
                    checksum_crc32,
                    checksum_crc32c,
                    checksum_sha1,
                    checksum_sha256,
                    checksum_crc64nvme,
                    part_number: i32::try_from(part.number).ok(),
                    size: Some(part_size),
                    ..Default::default()
                });
            }

            let part_number_marker = marker.and_then(|v| i32::try_from(v).ok());
            let next_part_number_marker = parts.last().and_then(|part| part.part_number);

            Some(GetObjectAttributesParts {
                is_truncated: Some(is_truncated),
                max_parts: Some(max_parts),
                next_part_number_marker,
                part_number_marker,
                parts: Some(parts),
                total_parts_count: Some(i32::try_from(info.parts.len()).map_err(|_| {
                    S3Error::with_message(S3ErrorCode::InvalidArgument, "Part count is out of range".to_string())
                })?),
            })
        } else {
            None
        };

        let version_id = if BucketVersioningSys::prefix_enabled(&bucket, &key).await {
            info.version_id.map(|vid| {
                if vid == Uuid::nil() {
                    "null".to_string()
                } else {
                    vid.to_string()
                }
            })
        } else {
            None
        };

        let output = GetObjectAttributesOutput {
            checksum,
            delete_marker: if info.delete_marker { Some(true) } else { None },
            e_tag,
            last_modified: info.mod_time.map(Timestamp::from),
            object_parts,
            object_size,
            storage_class,
            version_id: version_id.clone(),
            ..Default::default()
        };

        helper = helper.object(info).version_id(version_id.unwrap_or_default());

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }
}

fn object_attributes_requested(object_attributes: &[ObjectAttributes], name: &'static str) -> bool {
    object_attributes.iter().any(|value| {
        value.as_str().split(',').any(|part| {
            part.trim_matches(|c: char| c.is_whitespace() || c == '"' || c == '\'')
                .eq_ignore_ascii_case(name)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue, Method};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, ReadBuf};

    #[tokio::test(start_paused = true)]
    async fn cold_fill_disk_admission_preserves_slow_down() {
        let manager = Box::leak(Box::new(ConcurrencyManager::with_disk_read_caps_for_test(1, 1)));
        let primary = match manager.admit_disk_read(Duration::from_millis(1)).await.unwrap() {
            DiskReadAdmission::Primary(permit) => permit,
            other => panic!("expected primary admission, got {other:?}"),
        };
        let degraded = match manager.admit_disk_read(Duration::from_millis(1)).await.unwrap() {
            DiskReadAdmission::Degraded(permit) => permit,
            other => panic!("expected degraded admission, got {other:?}"),
        };

        let result = DefaultObjectUsecase::acquire_cold_fill_io_planning(manager, "bucket", "object").await;
        assert!(matches!(result, Err(ColdFillError::Storage(StorageError::SlowDown))));

        drop(degraded);
        drop(primary);
    }

    #[tokio::test]
    async fn cold_fill_closed_disk_admission_is_not_slow_down() {
        let manager = Box::leak(Box::new(ConcurrencyManager::with_disk_read_caps_for_test(1, 1)));
        manager.close_disk_read_admission_for_test();

        let result = DefaultObjectUsecase::acquire_cold_fill_io_planning(manager, "bucket", "object").await;
        assert!(matches!(result, Err(ColdFillError::DiskAdmissionClosed)));
    }

    #[tokio::test]
    async fn finalize_get_object_response_injects_accept_ranges_header() {
        let req = build_request(GetObjectInput::default(), Method::GET);
        let helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, S3Operation::GetObject).suppress_event();
        let response = DefaultObjectUsecase::finalize_get_object_response(
            helper,
            "bucket",
            &req.method,
            &req.headers,
            None,
            String::new(),
            GetObjectOutput::default(),
            Vec::new(),
        )
        .await
        .expect("finalize response");

        assert_eq!(response.headers.get(http::header::ACCEPT_RANGES).unwrap(), ACCEPT_RANGES_BYTES);
    }

    #[test]
    fn should_buffer_get_object_in_memory_respects_hard_safety_cap() {
        let info = ObjectInfo::default();
        let configured_threshold = 20_i64 * 1024 * 1024 * 1024;
        let response_len = 80_i64 * 1024 * 1024;
        let should_buffer =
            should_buffer_get_object_in_memory_with_threshold(&info, response_len, None, false, configured_threshold, 1, true);

        assert!(
            !should_buffer,
            "64MiB hard cap must force streaming when response exceeds cap even if configured threshold is much higher"
        );
    }

    #[test]
    fn should_buffer_get_object_in_memory_allows_small_non_range_requests() {
        let info = ObjectInfo::default();
        let configured_threshold = 10_i64 * 1024 * 1024;

        assert!(should_buffer_get_object_in_memory_with_threshold(
            &info,
            1024 * 1024,
            None,
            false,
            configured_threshold,
            1,
            true
        ));
        assert!(!should_buffer_get_object_in_memory_with_threshold(
            &info,
            1024 * 1024,
            Some(1),
            false,
            configured_threshold,
            1,
            true
        ));
        assert!(!should_buffer_get_object_in_memory_with_threshold(
            &info,
            1024 * 1024,
            None,
            true,
            configured_threshold,
            1,
            true
        ));
    }

    #[test]
    fn should_buffer_get_object_in_memory_requires_seek_buffer_opt_in() {
        let info = ObjectInfo::default();
        let configured_threshold = 10_i64 * 1024 * 1024;

        assert!(!should_buffer_get_object_in_memory_with_threshold(
            &info,
            1024,
            None,
            false,
            configured_threshold,
            1,
            false
        ));
    }

    #[test]
    fn should_buffer_get_object_in_memory_respects_configured_threshold_below_cap() {
        let info = ObjectInfo::default();
        let configured_threshold = 10_i64 * 1024 * 1024;

        assert!(should_buffer_get_object_in_memory_with_threshold(
            &info,
            configured_threshold,
            None,
            false,
            configured_threshold,
            1,
            true
        ));
        assert!(!should_buffer_get_object_in_memory_with_threshold(
            &info,
            configured_threshold + 1,
            None,
            false,
            configured_threshold,
            1,
            true
        ));
    }

    #[test]
    fn should_buffer_get_object_in_memory_rejects_unknown_lengths_and_disabled_thresholds() {
        let info = ObjectInfo::default();
        let configured_threshold = 10_i64 * 1024 * 1024;

        assert!(!should_buffer_get_object_in_memory_with_threshold(
            &info,
            0,
            None,
            false,
            configured_threshold,
            1,
            true
        ));
        assert!(!should_buffer_get_object_in_memory_with_threshold(
            &info,
            -1,
            None,
            false,
            configured_threshold,
            1,
            true
        ));
        assert!(!should_buffer_get_object_in_memory_with_threshold(&info, 1024, None, false, 0, 1, true));
    }

    #[test]
    fn should_buffer_get_object_in_memory_reduces_threshold_under_concurrency() {
        let info = ObjectInfo::default();
        let configured_threshold = 10_i64 * 1024 * 1024;

        assert!(should_buffer_get_object_in_memory_with_threshold(
            &info,
            configured_threshold,
            None,
            false,
            configured_threshold,
            1,
            true
        ));
        assert!(!should_buffer_get_object_in_memory_with_threshold(
            &info,
            configured_threshold,
            None,
            false,
            configured_threshold,
            32,
            true
        ));
        assert!(should_buffer_get_object_in_memory_with_threshold(
            &info,
            4_i64 * 1024 * 1024,
            None,
            false,
            configured_threshold,
            rustfs_config::DEFAULT_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
            true
        ));
    }

    /// Polls the cache until the detached fill (ODC-15) populates the entry, so
    /// a follow-up GET is a deterministic hit rather than racing the fill task.
    async fn wait_for_cache_hit(
        adapter: &crate::app::object_data_cache::ObjectDataCacheAdapter,
        bucket: &str,
        object: &str,
        etag: &str,
        size: u64,
    ) {
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket,
            object,
            version_id: None,
            etag,
            size,
            data_dir_u128: None,
            mod_time_unix_nanos: 0,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        for _ in 0..400 {
            if matches!(adapter.lookup_body(&plan).await, rustfs_object_data_cache::ObjectDataCacheLookup::Hit(_)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("detached fill did not populate the cache within the timeout");
    }

    struct ReadProbeReader {
        reads: Arc<AtomicUsize>,
    }

    impl AsyncRead for ReadProbeReader {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            self.reads.fetch_add(1, AtomicOrdering::Relaxed);
            Poll::Ready(Ok(()))
        }
    }

    struct DataProbeReader {
        reads: Arc<AtomicUsize>,
        data: std::io::Cursor<Vec<u8>>,
    }

    struct ColdFillMatrixReader {
        inner: tokio::io::DuplexStream,
        first_poll_recorded: bool,
        completion_recorded: bool,
        first_polls: Arc<AtomicUsize>,
        completed: Arc<AtomicUsize>,
        bytes_read: Arc<AtomicUsize>,
    }

    impl AsyncRead for ColdFillMatrixReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if !self.first_poll_recorded {
                self.first_poll_recorded = true;
                self.first_polls.fetch_add(1, AtomicOrdering::Relaxed);
            }
            let before = buf.filled().len();
            match Pin::new(&mut self.inner).poll_read(cx, buf) {
                Poll::Ready(Ok(())) => {
                    let read = buf.filled().len().saturating_sub(before);
                    self.bytes_read.fetch_add(read, AtomicOrdering::Relaxed);
                    if read == 0 && !self.completion_recorded {
                        self.completion_recorded = true;
                        self.completed.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    Poll::Ready(Ok(()))
                }
                other => other,
            }
        }
    }

    impl AsyncRead for DataProbeReader {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            self.reads.fetch_add(1, AtomicOrdering::Relaxed);

            let remaining = buf.remaining();
            if remaining == 0 {
                return Poll::Ready(Ok(()));
            }

            let position = usize::try_from(self.data.position()).unwrap_or(usize::MAX);
            let source = self.data.get_ref();
            if position >= source.len() {
                return Poll::Ready(Ok(()));
            }

            let end = position.saturating_add(remaining).min(source.len());
            buf.put_slice(&source[position..end]);
            self.data.set_position(u64::try_from(end).unwrap_or(u64::MAX));
            Poll::Ready(Ok(()))
        }
    }

    struct PendingReader;

    impl AsyncRead for PendingReader {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    // Emits `fail_after` bytes from `data`, then returns a hard read error. Used
    // to inject the "read K bytes then Err" partial-read case (#1324).
    struct ErrAfterReader {
        data: std::io::Cursor<Vec<u8>>,
        fail_after: usize,
        emitted: usize,
    }

    impl AsyncRead for ErrAfterReader {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if self.emitted >= self.fail_after {
                return Poll::Ready(Err(std::io::Error::other("injected mid-stream read error")));
            }
            let remaining = buf.remaining();
            if remaining == 0 {
                return Poll::Ready(Ok(()));
            }
            let want = (self.fail_after - self.emitted).min(remaining);
            let position = usize::try_from(self.data.position()).unwrap_or(usize::MAX);
            let source = self.data.get_ref();
            let end = position.saturating_add(want).min(source.len());
            if end <= position {
                return Poll::Ready(Err(std::io::Error::other("injected mid-stream read error")));
            }
            let chunk_len = end - position;
            buf.put_slice(&source[position..end]);
            self.data.set_position(u64::try_from(end).unwrap_or(u64::MAX));
            self.emitted += chunk_len;
            Poll::Ready(Ok(()))
        }
    }

    fn cursor_reader(bytes: &[u8]) -> std::io::Cursor<Vec<u8>> {
        std::io::Cursor::new(bytes.to_vec())
    }

    // #1324: the strict materialization helper is the shared exact-length gate
    // for the encrypted, seek, and cache memory branches. For a declared length N
    // only an exact N-byte read succeeds; a short read (N-1), an over-long read
    // (N+1), and a mid-stream read error all hard-fail. This is the reversal
    // guard for every one of those sources at once: restoring WARN-and-serve or a
    // partial fallback would flip the short/over-long/error assertions to Ok.
    #[tokio::test]
    async fn strict_materialize_object_body_requires_exact_length() {
        // Exact length: the only accepted outcome.
        let buf = strict_materialize_object_body(cursor_reader(b"hello"), 5, GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ)
            .await
            .expect("exact-length read must materialize");
        assert_eq!(buf, b"hello");
        assert_eq!(buf.capacity(), 5, "exact materialization must allocate only the declared body length");

        let exact_large = vec![7_u8; 64 * 1024];
        let buf = strict_materialize_object_body(
            std::io::Cursor::new(exact_large.clone()),
            exact_large.len(),
            GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ,
        )
        .await
        .expect("64 KiB exact-length read must materialize");
        assert_eq!(buf.capacity(), exact_large.len());

        let mut overlong_large = exact_large;
        overlong_large.push(9);
        let overlong = strict_materialize_object_body(
            std::io::Cursor::new(overlong_large),
            64 * 1024,
            GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ,
        )
        .await;
        assert!(matches!(
            overlong,
            Err(StrictMaterializeError::LengthMismatch {
                expected: 65_536,
                actual: 65_537
            })
        ));

        // Short read (actual = expected - 1): a clean EOF before the declared
        // length must be a hard error, never a truncated served body.
        let short = strict_materialize_object_body(cursor_reader(b"hell"), 5, GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ).await;
        assert!(
            matches!(
                short,
                Err(StrictMaterializeError::LengthMismatch {
                    expected: 5,
                    actual: 4,
                    ..
                })
            ),
            "short read must fail with a length mismatch, got {short:?}",
            short = short.as_ref().map(|b| b.len())
        );

        // Over-long read (actual = expected + 1): must fail rather than silently
        // truncate to the committed Content-Length.
        let long = strict_materialize_object_body(cursor_reader(b"hello!"), 5, GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ).await;
        assert!(
            matches!(long, Err(StrictMaterializeError::LengthMismatch { expected: 5, actual: 6 })),
            "over-long read must fail with a length mismatch, got {long:?}",
            long = long.as_ref().map(|b| b.len())
        );

        // Read K bytes then Err: must surface the read error and never return the
        // partially consumed buffer (which the caller could otherwise re-stream).
        let reader = ErrAfterReader {
            data: cursor_reader(b"hello"),
            fail_after: 3,
            emitted: 0,
        };
        let errored = strict_materialize_object_body(reader, 5, GET_OBJECT_STAGE_BODY_SEEK_BUFFER_READ).await;
        assert!(
            matches!(errored, Err(StrictMaterializeError::Read { consumed: 3, .. })),
            "a mid-stream read error must be reported as a read failure"
        );
    }

    #[test]
    fn cold_fill_zero_timeout_policy_disables_deadline() {
        let policy = GetObjectTimeoutPolicy {
            get_object_timeout: Duration::ZERO,
            ..GetObjectTimeoutPolicy::default()
        };
        let wrapper = RequestTimeoutWrapper::with_request_id(policy.clone(), "cold-fill-zero-timeout");
        assert!(cold_fill_deadline(&wrapper, &policy, 1).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn cold_fill_producer_deadline_is_capped_at_ten_minutes() {
        let disabled = GetObjectTimeoutPolicy {
            get_object_timeout: Duration::ZERO,
            ..GetObjectTimeoutPolicy::default()
        };
        let now = tokio::time::Instant::now();
        assert_eq!(cold_fill_producer_deadline(&disabled, 1) - now, Duration::from_secs(600));

        let long = GetObjectTimeoutPolicy {
            get_object_timeout: Duration::from_secs(3600),
            enable_dynamic_timeout: false,
            ..GetObjectTimeoutPolicy::default()
        };
        let now = tokio::time::Instant::now();
        assert_eq!(cold_fill_producer_deadline(&long, 1) - now, Duration::from_secs(600));
    }

    #[tokio::test]
    async fn cold_fill_startup_wait_stops_when_last_consumer_cancels() {
        let cancellation = tokio_util::sync::CancellationToken::new();
        let waiting = tokio::spawn({
            let cancellation = cancellation.clone();
            async move { await_cold_fill_startup(std::future::pending::<()>(), &cancellation, None).await }
        });
        tokio::task::yield_now().await;

        cancellation.cancel();

        let result = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("startup wait must observe cancellation")
            .expect("startup wait task must not panic");
        assert!(matches!(result, Err(ColdFillStartupWaitError::Cancelled)));
    }

    #[tokio::test(start_paused = true)]
    async fn cold_fill_startup_wait_with_deadline_still_observes_cancellation() {
        let cancellation = tokio_util::sync::CancellationToken::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let waiting = tokio::spawn({
            let cancellation = cancellation.clone();
            async move { await_cold_fill_startup(std::future::pending::<()>(), &cancellation, Some(deadline)).await }
        });
        tokio::task::yield_now().await;

        cancellation.cancel();

        let result = waiting.await.expect("startup wait task must not panic");
        assert!(matches!(result, Err(ColdFillStartupWaitError::Cancelled)));
    }

    #[tokio::test(start_paused = true)]
    async fn cold_fill_startup_wait_reports_deadline_exceeded() {
        let cancellation = tokio_util::sync::CancellationToken::new();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(1);

        let result = await_cold_fill_startup(std::future::pending::<()>(), &cancellation, Some(deadline)).await;

        assert!(matches!(result, Err(ColdFillStartupWaitError::DeadlineExceeded)));
    }

    #[tokio::test]
    async fn cold_fill_late_miss_second_chance_hits_without_reader() {
        let adapter = ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
            mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
            max_bytes: 1024 * 1024,
            max_memory_percent: 0,
            max_entry_bytes: 1024,
            min_free_memory_percent: 0,
            fill_concurrency_max: 1,
            ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
        })
        .expect("second-chance cache config must be valid");
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "late-bucket",
            object: "late-object",
            version_id: None,
            etag: "late-etag",
            size: 4,
            data_dir_u128: Some(1),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        assert!(matches!(
            adapter.lookup_body(&plan).await,
            rustfs_object_data_cache::ObjectDataCacheLookup::Miss
        ));
        let request_lookups = adapter.cache().stats().lookups;
        assert_eq!(request_lookups, 1, "the authoritative request lookup must be counted once");

        let reservation = adapter.reserve_body(&plan).expect("late producer must reserve");
        let reserved = reservation.wrap_bytes(Bytes::from_static(b"body"));
        let _ = adapter.fill_reserved_body(&plan, reserved).await;
        let coordinator = adapter.cold_fill_coordinator();
        let cache_key = plan.key().cloned().expect("late plan must be cacheable");
        let adapter = Arc::new(adapter);
        let readers = Arc::new(AtomicUsize::new(0));
        let outcome = coordinate_cold_fill(&coordinator, cache_key, None, None, {
            let adapter = Arc::clone(&adapter);
            let readers = Arc::clone(&readers);
            move |producer| {
                let adapter = Arc::clone(&adapter);
                let plan = plan.clone();
                let readers = Arc::clone(&readers);
                async move {
                    if let Some(body) = lookup_cold_fill_second_chance(&adapter, &plan).await {
                        producer.finish_shared(Ok(body));
                        return;
                    }
                    readers.fetch_add(1, AtomicOrdering::Relaxed);
                    producer.bypass();
                }
            }
        })
        .await;
        let ColdFillCoordinateOutcome::Ready(Ok(body)) = outcome else {
            panic!("late request must observe the completed fill, got {outcome:?}");
        };
        assert_eq!(body, Bytes::from_static(b"body"));
        assert_eq!(
            adapter.cache().stats().lookups,
            request_lookups,
            "the producer second chance must not count another request lookup"
        );
        assert_eq!(readers.load(AtomicOrdering::Relaxed), 0);
    }

    #[tokio::test]
    async fn cold_fill_timeout_is_shared_and_releases_resources() {
        let adapter = Arc::new(
            ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 1024,
                min_free_memory_percent: 0,
                fill_concurrency_max: 1,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("timeout cache config must be valid"),
        );
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "timeout-bucket",
            object: "timeout-object",
            version_id: None,
            etag: "timeout-etag",
            size: 1,
            data_dir_u128: Some(1),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let key = plan.key().cloned().expect("timeout body must be cacheable");
        let coordinator = adapter.cold_fill_coordinator();
        let ColdFillRole::Produce(mut producer) = coordinator.join(key.clone()) else {
            panic!("first timeout request must produce");
        };
        let leader = producer.waiter();
        let reservation = adapter.reserve_body(&plan);
        let disk_permits = Arc::new(tokio::sync::Semaphore::new(1));
        let disk_gate = Arc::clone(&disk_permits);
        let readers = Arc::new(AtomicUsize::new(0));
        let reader_count = Arc::clone(&readers);
        let producer_task = tokio::spawn(start_cold_fill_producer(
            producer,
            reservation,
            move || async move {
                let permit = disk_gate
                    .acquire_owned()
                    .await
                    .map_err(|_| ColdFillError::DiskAdmissionClosed)?;
                let mut io = DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager());
                io.disk_permit = Some(permit.into());
                Ok(io)
            },
            move || async move {
                reader_count.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(GetObjectReader {
                    stream: Box::new(PendingReader),
                    object_info: ObjectInfo {
                        size: 1,
                        actual_size: 1,
                        ..Default::default()
                    },
                    buffered_body: None,
                    body_source: GetObjectBodySource::HookMissed,
                })
            },
            ColdFillProducerExecution {
                expected: 1,
                deadline: Some(tokio::time::Instant::now() + Duration::from_millis(20)),
                adapter: Arc::clone(&adapter),
                engine_plan: plan.clone(),
            },
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while readers.load(AtomicOrdering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("producer reader must open");
        let ColdFillRole::Wait(follower) = coordinator.join(key.clone()) else {
            panic!("second timeout request must follow");
        };

        let (leader_result, follower_result) =
            tokio::time::timeout(Duration::from_secs(2), async { tokio::join!(leader.wait(), follower.wait()) })
                .await
                .expect("typed timeout must wake all waiters");
        assert!(matches!(
            leader_result,
            ColdFillWaitOutcome::Ready(Err(ColdFillError::Storage(StorageError::Timeout)))
        ));
        assert!(matches!(
            follower_result,
            ColdFillWaitOutcome::Ready(Err(ColdFillError::Storage(StorageError::Timeout)))
        ));
        assert_eq!(readers.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(disk_permits.available_permits(), 1);
        assert_eq!(coordinator.global_waiter_count_for_test(), 0);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
        assert!(matches!(
            adapter.lookup_body(&plan).await,
            rustfs_object_data_cache::ObjectDataCacheLookup::Miss
        ));
        producer_task.await.expect("producer task must join");
        assert!(adapter.reserve_body(&plan).is_some(), "timeout must release the body reservation");
        let ColdFillRole::Produce(successor) = coordinator.join(key) else {
            panic!("timeout must release the session for a successor");
        };
        drop(successor);
    }

    #[tokio::test]
    async fn cold_fill_survives_leader_request_cancellation_without_second_producer() {
        let adapter = Arc::new(
            ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 1024,
                min_free_memory_percent: 0,
                fill_concurrency_max: 1,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("cancellation cache config must be valid"),
        );
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "cancel-bucket",
            object: "cancel-object",
            version_id: None,
            etag: "cancel-etag",
            size: 4,
            data_dir_u128: Some(1),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let key = plan.key().cloned().expect("cancellation body must be cacheable");
        let coordinator = adapter.cold_fill_coordinator();
        let ColdFillRole::Produce(mut producer) = coordinator.join(key.clone()) else {
            panic!("first cancellation request must produce");
        };
        let leader = producer.waiter();
        let reservation = adapter.reserve_body(&plan);
        let readers = Arc::new(AtomicUsize::new(0));
        let reader_count = Arc::clone(&readers);
        let writer_slot = Arc::new(Mutex::new(None));
        let writer_output = Arc::clone(&writer_slot);
        let producer_task = tokio::spawn(start_cold_fill_producer(
            producer,
            reservation,
            || async { Ok(DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager())) },
            move || async move {
                reader_count.fetch_add(1, AtomicOrdering::Relaxed);
                let (writer, reader) = tokio::io::duplex(16);
                *writer_output.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(writer);
                Ok(GetObjectReader {
                    stream: Box::new(reader),
                    object_info: ObjectInfo {
                        size: 4,
                        actual_size: 4,
                        ..Default::default()
                    },
                    buffered_body: None,
                    body_source: GetObjectBodySource::HookMissed,
                })
            },
            ColdFillProducerExecution {
                expected: 4,
                deadline: None,
                adapter: Arc::clone(&adapter),
                engine_plan: plan.clone(),
            },
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while readers.load(AtomicOrdering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancellation producer reader must open");
        let ColdFillRole::Wait(follower) = coordinator.join(key.clone()) else {
            panic!("second cancellation request must follow");
        };
        drop(leader);
        assert_eq!(readers.load(AtomicOrdering::Relaxed), 1);
        let ColdFillRole::Wait(late) = coordinator.join(key) else {
            panic!("leader cancellation must not open a successor session");
        };
        drop(late);

        let mut writer = writer_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .expect("reader factory must publish writer");
        tokio::io::AsyncWriteExt::write_all(&mut writer, b"body")
            .await
            .expect("body write must succeed");
        tokio::io::AsyncWriteExt::shutdown(&mut writer)
            .await
            .expect("body writer must close");
        let ColdFillWaitOutcome::Ready(result) = follower.wait().await else {
            panic!("follower must receive producer result");
        };
        assert_eq!(result.expect("surviving producer must succeed"), Bytes::from_static(b"body"));
        producer_task.await.expect("producer task must join");
        assert_eq!(readers.load(AtomicOrdering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cold_fill_reservation_rejection_streams_without_materializing() {
        let coordinator = Arc::new(crate::app::object_data_cache::ColdFillCoordinator::default());
        let plan = rustfs_object_data_cache::ObjectDataCacheGetPlan::Disabled;
        let ColdFillRole::Produce(mut producer) = coordinator.join(rustfs_object_data_cache::ObjectDataCacheKey::new(
            "bucket",
            "object",
            None,
            "etag",
            4,
            rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        )) else {
            panic!("first rejected reservation request must produce");
        };
        let leader = producer.waiter();
        let permits = Arc::new(AtomicUsize::new(0));
        let readers = Arc::new(AtomicUsize::new(0));
        let permit_count = Arc::clone(&permits);
        let reader_count = Arc::clone(&readers);
        start_cold_fill_producer(
            producer,
            None,
            move || async move {
                permit_count.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager()))
            },
            move || async move {
                reader_count.fetch_add(1, AtomicOrdering::Relaxed);
                Err(StorageError::other("reader must not open"))
            },
            ColdFillProducerExecution {
                expected: 4,
                deadline: None,
                adapter: Arc::new(ObjectDataCacheAdapter::disabled()),
                engine_plan: plan,
            },
        )
        .await;
        assert!(matches!(leader.wait().await, ColdFillWaitOutcome::Bypass));
        assert_eq!(permits.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(readers.load(AtomicOrdering::Relaxed), 0);

        let fallback_reads = Arc::new(AtomicUsize::new(0));
        let fallback_reader = DataProbeReader {
            reads: Arc::clone(&fallback_reads),
            data: std::io::Cursor::new(b"body".to_vec()),
        };
        let info = ObjectInfo {
            size: 4,
            actual_size: 4,
            ..Default::default()
        };
        let mut fallback_body = DefaultObjectUsecase::build_get_object_body(
            fallback_reader,
            &info,
            4,
            "req-cold-fill",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            "bucket",
            "object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await
        .expect("reservation bypass must construct the normal streaming fallback");
        let chunk = fallback_body
            .next()
            .await
            .expect("fallback stream must yield a body chunk")
            .expect("fallback stream must not fail");
        assert_eq!(chunk, Bytes::from_static(b"body"));
        assert!(fallback_reads.load(AtomicOrdering::Relaxed) > 0);
        assert_eq!(readers.load(AtomicOrdering::Relaxed), 0, "cold-fill materialization must remain unopened");
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn cold_fill_internal_movement_and_restore_reads_never_join_sessions() {
        let coordinator = Arc::new(crate::app::object_data_cache::ColdFillCoordinator::default());
        let info = ObjectInfo {
            size: 4,
            actual_size: 4,
            ..Default::default()
        };
        let mut restore = ObjectOptions::default();
        restore.transition.restore_request.days = Some(1);
        let cases = [
            ObjectOptions {
                raw_data_movement_read: true,
                ..Default::default()
            },
            ObjectOptions {
                data_movement: true,
                ..Default::default()
            },
            restore,
        ];

        for opts in &cases {
            assert!(matches!(
                lookup_get_object_body_cache_hook("bucket", "object", &None, opts, &info).await,
                GetObjectBodyCacheHookLookup::Ineligible
            ));
            assert_eq!(coordinator.active_session_count_for_test(), 0);
        }

        let delete_marker = ObjectInfo {
            delete_marker: true,
            etag: Some("delete-marker-etag".to_string()),
            ..Default::default()
        };
        let delete_marker_part = ObjectOptions {
            part_number: Some(2),
            ..Default::default()
        };
        assert!(matches!(
            lookup_get_object_body_cache_hook("bucket", "object", &None, &delete_marker_part, &delete_marker).await,
            GetObjectBodyCacheHookLookup::Ineligible
        ));
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn cold_fill_generation_change_bypasses_before_opening_body() {
        let adapter = Arc::new(
            ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 1024,
                min_free_memory_percent: 0,
                fill_concurrency_max: 1,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("generation retry cache config must be valid"),
        );
        let request = |data_dir_u128| rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "generation-bucket",
            object: "generation-object",
            version_id: None,
            etag: "generation-etag",
            size: 4,
            data_dir_u128: Some(data_dir_u128),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        };
        let initial_plan = adapter.plan_get(request(1));
        let changed_plan = GetObjectBodyCachePlan::Cacheable(adapter.plan_get(request(2)));
        let cache_key = initial_plan.key().cloned().expect("initial generation must be cacheable");
        let coordinator = adapter.cold_fill_coordinator();
        let body_opens = Arc::new(AtomicUsize::new(0));
        let producer_attempts = Arc::new(AtomicUsize::new(0));

        let outcome = coordinate_cold_fill(&coordinator, cache_key, None, None, {
            let body_opens = Arc::clone(&body_opens);
            let producer_attempts = Arc::clone(&producer_attempts);
            move |producer| {
                let body_opens = Arc::clone(&body_opens);
                let producer_attempts = Arc::clone(&producer_attempts);
                let changed_plan = changed_plan.clone();
                let initial_plan = initial_plan.clone();
                async move {
                    producer_attempts.fetch_add(1, AtomicOrdering::Relaxed);
                    let Some(producer) = retain_cold_fill_producer_for_matching_plan(producer, &changed_plan, &initial_plan)
                    else {
                        return;
                    };
                    body_opens.fetch_add(1, AtomicOrdering::Relaxed);
                    producer.bypass();
                }
            }
        })
        .await;

        assert!(matches!(outcome, ColdFillCoordinateOutcome::Bypass));
        assert_eq!(producer_attempts.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(body_opens.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn execute_get_object_rejects_conditions_before_joining_cold_fill() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let (store, context) = real_cold_fill_test_context().await;
        let bucket = format!("cold-condition-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("real cold-fill condition bucket must be created");
        let body = vec![b'a'; 1_300_000];
        let info = put_real_cold_fill_object(&store, &bucket, object, &body).await;
        let adapter = context.object_data_cache();
        let plan = real_cold_fill_plan(&adapter, &bucket, object, &info);
        let coordinator = adapter.cold_fill_coordinator();
        let ColdFillRole::Produce(producer) =
            coordinator.join(plan.key().cloned().expect("real cold-fill plan must expose its key"))
        else {
            panic!("test must reserve the initial cold-fill producer");
        };

        let input = GetObjectInput::builder()
            .bucket(bucket)
            .key(object.to_string())
            .build()
            .expect("real cold-fill GET input must build");
        let mut req = build_request(input, Method::GET);
        let etag = info.etag.expect("real cold-fill test object must have an ETag");
        req.headers.insert(
            http::header::IF_NONE_MATCH,
            HeaderValue::from_str(&format!("\"{etag}\"")).expect("ETag header must be valid"),
        );
        let usecase = DefaultObjectUsecase::with_context(Some(context));
        let result = tokio::time::timeout(Duration::from_secs(2), usecase.execute_get_object(req))
            .await
            .expect("conditional GET must not wait for the reserved cold-fill session")
            .expect_err("matching If-None-Match must reject the GET");

        assert_eq!(result.code(), &S3ErrorCode::NotModified);
        assert_eq!(coordinator.global_waiter_count_for_test(), 0);
        drop(producer);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn execute_get_object_maps_cold_fill_session_rejection_to_slow_down_without_opening_reader() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let (store, context) = real_cold_fill_test_context().await;
        let bucket = format!("cold-rejected-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("real cold-fill rejection bucket must be created");
        let body = vec![b'a'; 1_300_000];
        let info = put_real_cold_fill_object(&store, &bucket, object, &body).await;
        let adapter = context.object_data_cache();
        let plan = real_cold_fill_plan(&adapter, &bucket, object, &info);
        let cache_key = plan.key().cloned().expect("real cold-fill plan must expose its key");
        let coordinator = adapter.cold_fill_coordinator();
        let mut held_producers = Vec::new();
        for index in 0..2048 {
            let saturation_key = rustfs_object_data_cache::ObjectDataCacheKey::new(
                "cold-fill-saturation",
                format!("object-{index}"),
                None,
                "etag",
                4,
                rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
            );
            match coordinator.join(saturation_key) {
                ColdFillRole::Produce(producer) => held_producers.push(producer),
                ColdFillRole::Rejected => break,
                ColdFillRole::Wait(_) | ColdFillRole::Bypass => panic!("unique saturation keys must produce or reject"),
            }
        }
        assert_eq!(coordinator.active_session_count_for_test(), held_producers.len());
        assert!(!held_producers.is_empty(), "saturation must reserve cold-fill sessions");

        let reader_opens = Arc::new(AtomicU64::new(0));
        *COLD_FILL_READER_OPEN_PROBE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((cache_key, Arc::clone(&reader_opens)));
        let input = GetObjectInput::builder()
            .bucket(bucket)
            .key(object.to_string())
            .build()
            .expect("real cold-fill rejection GET input must build");
        let usecase = DefaultObjectUsecase::with_context(Some(context));
        let result = tokio::time::timeout(Duration::from_secs(2), usecase.execute_get_object(build_request(input, Method::GET)))
            .await
            .expect("rejected real GET must not wait for a cold-fill session")
            .expect_err("rejected real GET must return an S3 error");
        *COLD_FILL_READER_OPEN_PROBE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;

        assert_eq!(result.code(), &S3ErrorCode::SlowDown);
        assert_eq!(reader_opens.load(Ordering::Relaxed), 0, "rejected GET must not open its body reader");
        assert_eq!(coordinator.active_session_count_for_test(), held_producers.len());
        drop(held_producers);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn execute_get_object_generation_change_bypasses_old_cold_fill_plan() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let (store, context) = real_cold_fill_test_context().await;
        let bucket = format!("cold-generation-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("real cold-fill generation bucket must be created");
        let initial_body = vec![b'a'; 1_300_000];
        let changed_body = vec![b'b'; initial_body.len()];
        let initial_info = put_real_cold_fill_object(&store, &bucket, object, &initial_body).await;
        let adapter = context.object_data_cache();
        let initial_plan = real_cold_fill_plan(&adapter, &bucket, object, &initial_info);
        let coordinator = adapter.cold_fill_coordinator();
        let ColdFillRole::Produce(producer) =
            coordinator.join(initial_plan.key().cloned().expect("real cold-fill plan must expose its key"))
        else {
            panic!("test must reserve the initial cold-fill producer");
        };

        let input = GetObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .expect("real cold-fill GET input must build");
        // The request is intentionally held behind the first producer while a
        // 1.3 MiB replacement write changes its generation. Disable dynamic
        // sizing for this test so runner I/O load cannot consume the five-second
        // production minimum before the behavior under test is released.
        let usecase = DefaultObjectUsecase::with_context_and_get_object_timeout_policy(
            Some(context),
            GetObjectTimeoutPolicy {
                enable_dynamic_timeout: false,
                ..GetObjectTimeoutPolicy::default()
            },
        );
        let request = tokio::spawn(async move { usecase.execute_get_object(build_request(input, Method::GET)).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while coordinator.global_waiter_count_for_test() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real GET must join the reserved cold-fill session");

        let changed_info = put_real_cold_fill_object(&store, &bucket, object, &changed_body).await;
        assert_ne!(initial_info.etag, changed_info.etag);
        producer.relinquish_or_finish(ColdFillError::Storage(StorageError::Timeout));

        let mut response = tokio::time::timeout(Duration::from_secs(10), request)
            .await
            .expect("generation-changing GET must complete")
            .expect("generation-changing GET task must join")
            .expect("generation-changing GET must fall back successfully");
        let mut response_body = response.output.body.take().expect("GET response must include a body");
        let mut actual = Vec::with_capacity(changed_body.len());
        while let Some(chunk) = response_body.next().await {
            actual.extend_from_slice(&chunk.expect("fallback body chunk must be readable"));
        }

        assert_eq!(actual, changed_body);
        assert!(matches!(
            adapter.lookup_body(&initial_plan).await,
            rustfs_object_data_cache::ObjectDataCacheLookup::Miss
        ));
        assert_eq!(coordinator.global_waiter_count_for_test(), 0);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn cold_fill_open_error_retries_once_then_single_successor_succeeds() {
        let adapter = Arc::new(
            ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 1024,
                min_free_memory_percent: 0,
                fill_concurrency_max: 1,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("open retry cache config must be valid"),
        );
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "open-retry-bucket",
            object: "open-retry-object",
            version_id: None,
            etag: "open-retry-etag",
            size: 4,
            data_dir_u128: Some(1),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let cache_key = plan.key().cloned().expect("open retry plan must be cacheable");
        let coordinator = adapter.cold_fill_coordinator();
        let open_attempts = Arc::new(AtomicUsize::new(0));
        let open_attempts_for_start = Arc::clone(&open_attempts);

        let outcome = coordinate_cold_fill(&coordinator, cache_key, None, None, move |producer| {
            let reservation = adapter.reserve_body(&plan);
            let adapter = Arc::clone(&adapter);
            let plan = plan.clone();
            let open_attempts = Arc::clone(&open_attempts_for_start);
            async move {
                start_cold_fill_producer(
                    producer,
                    reservation,
                    || async { Ok(DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager())) },
                    move || async move {
                        let attempt = open_attempts.fetch_add(1, AtomicOrdering::Relaxed);
                        if attempt == 0 {
                            return Err(StorageError::other("first open fails"));
                        }
                        Ok(GetObjectReader {
                            stream: Box::new(std::io::Cursor::new(Vec::<u8>::new())),
                            object_info: ObjectInfo {
                                size: 4,
                                actual_size: 4,
                                ..Default::default()
                            },
                            buffered_body: Some(Bytes::from_static(b"body")),
                            body_source: GetObjectBodySource::HookMissed,
                        })
                    },
                    ColdFillProducerExecution {
                        expected: 4,
                        deadline: None,
                        adapter,
                        engine_plan: plan,
                    },
                )
                .await
            }
        })
        .await;

        let ColdFillCoordinateOutcome::Ready(Ok(body)) = outcome else {
            panic!("the unique successor must publish the body");
        };
        assert_eq!(body, Bytes::from_static(b"body"));
        assert_eq!(open_attempts.load(AtomicOrdering::Relaxed), 2);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn cold_fill_open_timeout_retries_once_then_is_terminal() {
        tokio::time::pause();
        let adapter = Arc::new(
            ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 1024,
                min_free_memory_percent: 0,
                fill_concurrency_max: 1,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("open timeout cache config must be valid"),
        );
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "open-timeout-bucket",
            object: "open-timeout-object",
            version_id: None,
            etag: "open-timeout-etag",
            size: 4,
            data_dir_u128: Some(1),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let cache_key = plan.key().cloned().expect("open timeout plan must be cacheable");
        let coordinator = adapter.cold_fill_coordinator();
        let open_attempts = Arc::new(AtomicUsize::new(0));

        let deadline = tokio::time::Instant::now() + Duration::from_millis(10);
        let task = tokio::spawn({
            let adapter = Arc::clone(&adapter);
            let coordinator = Arc::clone(&coordinator);
            let plan = plan.clone();
            let open_attempts = Arc::clone(&open_attempts);
            async move {
                coordinate_cold_fill(&coordinator, cache_key, None, Some(deadline), move |producer| {
                    let adapter = Arc::clone(&adapter);
                    let plan = plan.clone();
                    let open_attempts = Arc::clone(&open_attempts);
                    let reservation = adapter.reserve_body(&plan);
                    let producer_deadline = producer.deadline();
                    async move {
                        start_cold_fill_producer(
                            producer,
                            reservation,
                            || async { Ok(DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager())) },
                            move || async move {
                                open_attempts.fetch_add(1, AtomicOrdering::Relaxed);
                                std::future::pending::<Result<GetObjectReader, StorageError>>().await
                            },
                            ColdFillProducerExecution {
                                expected: 4,
                                deadline: producer_deadline,
                                adapter,
                                engine_plan: plan,
                            },
                        )
                        .await
                    }
                })
                .await
            }
        });
        while open_attempts.load(AtomicOrdering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(11)).await;
        let outcome = task.await.expect("open timeout task must join");
        assert!(matches!(
            outcome,
            ColdFillCoordinateOutcome::Ready(Err(ColdFillError::Storage(StorageError::Timeout)))
        ));

        assert_eq!(open_attempts.load(AtomicOrdering::Relaxed), 2);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn cold_fill_pre_reader_failure_promotes_one_of_two_thousand_waiters() {
        const REQUESTS: usize = 2000;
        let adapter = Arc::new(
            ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 1024,
                min_free_memory_percent: 0,
                fill_concurrency_max: 1,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("successor cache config must be valid"),
        );
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "successor-bucket",
            object: "successor-object",
            version_id: None,
            etag: "successor-etag",
            size: 4,
            data_dir_u128: Some(1),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let cache_key = plan.key().cloned().expect("successor plan must be cacheable");
        let coordinator = adapter.cold_fill_coordinator();
        let admission_attempts = Arc::new(AtomicUsize::new(0));
        let open_attempts = Arc::new(AtomicUsize::new(0));
        let first_open_release = Arc::new(tokio::sync::Semaphore::new(0));
        let mut tasks = tokio::task::JoinSet::new();

        for _ in 0..REQUESTS {
            let adapter = Arc::clone(&adapter);
            let coordinator = Arc::clone(&coordinator);
            let cache_key = cache_key.clone();
            let plan = plan.clone();
            let admission_attempts = Arc::clone(&admission_attempts);
            let open_attempts = Arc::clone(&open_attempts);
            let first_open_release = Arc::clone(&first_open_release);
            tasks.spawn(async move {
                coordinate_cold_fill(&coordinator, cache_key, None, None, move |producer| {
                    let reservation = adapter.reserve_body(&plan);
                    let adapter = Arc::clone(&adapter);
                    let plan = plan.clone();
                    let admission_attempts = Arc::clone(&admission_attempts);
                    let open_attempts = Arc::clone(&open_attempts);
                    let first_open_release = Arc::clone(&first_open_release);
                    async move {
                        start_cold_fill_producer(
                            producer,
                            reservation,
                            move || async move {
                                admission_attempts.fetch_add(1, AtomicOrdering::Relaxed);
                                Ok(DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager()))
                            },
                            move || async move {
                                if open_attempts.fetch_add(1, AtomicOrdering::Relaxed) == 0 {
                                    first_open_release
                                        .acquire()
                                        .await
                                        .expect("first open release gate must remain open")
                                        .forget();
                                    return Err(StorageError::other("first open fails"));
                                }
                                Ok(GetObjectReader {
                                    stream: Box::new(std::io::Cursor::new(Vec::<u8>::new())),
                                    object_info: ObjectInfo {
                                        size: 4,
                                        actual_size: 4,
                                        ..Default::default()
                                    },
                                    buffered_body: Some(Bytes::from_static(b"body")),
                                    body_source: GetObjectBodySource::HookMissed,
                                })
                            },
                            ColdFillProducerExecution {
                                expected: 4,
                                deadline: None,
                                adapter,
                                engine_plan: plan,
                            },
                        )
                        .await
                    }
                })
                .await
            });
        }

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if coordinator.global_waiter_count_for_test() == REQUESTS - 1 && open_attempts.load(AtomicOrdering::Relaxed) == 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all followers must join before the first open fails");
        first_open_release.add_permits(1);

        while let Some(result) = tasks.join_next().await {
            let ColdFillCoordinateOutcome::Ready(Ok(body)) = result.expect("successor request task must join") else {
                panic!("all followers must receive the successor body");
            };
            assert_eq!(body, Bytes::from_static(b"body"));
        }
        assert_eq!(admission_attempts.load(AtomicOrdering::Relaxed), 2);
        assert_eq!(open_attempts.load(AtomicOrdering::Relaxed), 2);
        assert_eq!(coordinator.global_waiter_count_for_test(), 0);
        assert_eq!(coordinator.active_session_count_for_test(), 0);
    }

    fn install_cold_fill_publication_barrier(
        plan: &rustfs_object_data_cache::ObjectDataCacheGetPlan,
    ) -> Arc<ColdFillPublicationBarrier> {
        let barrier = Arc::new(ColdFillPublicationBarrier {
            reached: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        let key = plan.key().cloned().expect("publication barrier plan must be cacheable");
        *COLD_FILL_PUBLICATION_BARRIER
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((key, Arc::clone(&barrier)));
        barrier
    }

    fn clear_cold_fill_publication_barrier() {
        *COLD_FILL_PUBLICATION_BARRIER
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    fn publication_test_adapter() -> Arc<ObjectDataCacheAdapter> {
        Arc::new(
            ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 1024 * 1024,
                max_memory_percent: 0,
                max_entry_bytes: 1024,
                min_free_memory_percent: 0,
                fill_concurrency_max: 1,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("publication cache config must be valid"),
        )
    }

    fn publication_test_plan(adapter: &ObjectDataCacheAdapter, object: &str) -> rustfs_object_data_cache::ObjectDataCacheGetPlan {
        adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "publication-bucket",
            object,
            version_id: None,
            etag: "publication-etag",
            size: 4,
            data_dir_u128: Some(1),
            mod_time_unix_nanos: 1,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        })
    }

    #[tokio::test]
    #[serial_test::serial(cold_fill_publication_barrier)]
    async fn cold_fill_last_consumer_cancel_releases_session_before_publication_barrier() {
        let adapter = publication_test_adapter();
        let plan = publication_test_plan(&adapter, "cancel");
        let barrier = install_cold_fill_publication_barrier(&plan);
        let coordinator = adapter.cold_fill_coordinator();
        let key = plan.key().cloned().expect("publication plan must be cacheable");
        let ColdFillRole::Produce(mut producer) = coordinator.join(key) else {
            panic!("publication request must produce");
        };
        let leader = producer.waiter();
        let reservation = adapter.reserve_body(&plan);
        let disk_permits = Arc::new(tokio::sync::Semaphore::new(1));
        let disk_gate = Arc::clone(&disk_permits);
        let producer_task = tokio::spawn(scope_cold_fill_disk_permit_owner_for_test(
            ColdFillDiskPermitOwner::Producer,
            start_cold_fill_producer(
                producer,
                reservation,
                move || async move {
                    let permit = disk_gate
                        .acquire_owned()
                        .await
                        .map_err(|_| ColdFillError::DiskAdmissionClosed)?;
                    let mut io = DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager());
                    io.disk_permit = Some(permit.into());
                    Ok(io)
                },
                || async {
                    Ok(GetObjectReader {
                        stream: Box::new(std::io::Cursor::new(b"body".to_vec())),
                        object_info: ObjectInfo {
                            size: 4,
                            actual_size: 4,
                            ..Default::default()
                        },
                        buffered_body: None,
                        body_source: GetObjectBodySource::HookMissed,
                    })
                },
                ColdFillProducerExecution {
                    expected: 4,
                    deadline: None,
                    adapter: Arc::clone(&adapter),
                    engine_plan: plan.clone(),
                },
            ),
        ));

        let reached = barrier.reached.acquire().await.expect("publication barrier must remain open");
        reached.forget();
        assert_eq!(
            disk_permits.available_permits(),
            1,
            "the producer disk permit and its gauge guard must end before publication"
        );
        let clear_adapter = Arc::clone(&adapter);
        let clear = tokio::spawn(async move {
            clear_adapter
                .clear(rustfs_object_data_cache::ObjectDataCacheInvalidationReason::Manual)
                .await
        });
        tokio::task::yield_now().await;
        assert!(!clear.is_finished(), "clear must wait while publication owns its reservation");
        drop(leader);
        tokio::time::timeout(Duration::from_secs(1), async {
            while coordinator.active_session_count_for_test() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("last-consumer cancellation must release the session immediately");
        tokio::time::timeout(Duration::from_secs(1), clear)
            .await
            .expect("clear must finish after publication cancellation")
            .expect("clear task must join");
        producer_task.await.expect("producer task must join");

        barrier.release.add_permits(1);
        clear_cold_fill_publication_barrier();
        drop(adapter.reserve_body(&plan).expect("publication reservation must be released"));
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial(cold_fill_publication_barrier)]
    async fn cold_fill_hard_deadline_releases_session_at_publication_barrier() {
        let adapter = publication_test_adapter();
        let plan = publication_test_plan(&adapter, "deadline");
        let barrier = install_cold_fill_publication_barrier(&plan);
        let coordinator = adapter.cold_fill_coordinator();
        let key = plan.key().cloned().expect("publication plan must be cacheable");
        let ColdFillRole::Produce(mut producer) = coordinator.join(key) else {
            panic!("publication request must produce");
        };
        let leader = producer.waiter();
        let reservation = adapter.reserve_body(&plan);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
        let producer_task = tokio::spawn(start_cold_fill_producer(
            producer,
            reservation,
            || async { Ok(DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager())) },
            || async {
                Ok(GetObjectReader {
                    stream: Box::new(std::io::Cursor::new(Vec::<u8>::new())),
                    object_info: ObjectInfo {
                        size: 4,
                        actual_size: 4,
                        ..Default::default()
                    },
                    buffered_body: Some(Bytes::from_static(b"body")),
                    body_source: GetObjectBodySource::HookMissed,
                })
            },
            ColdFillProducerExecution {
                expected: 4,
                deadline: Some(deadline),
                adapter: Arc::clone(&adapter),
                engine_plan: plan.clone(),
            },
        ));

        let reached = barrier.reached.acquire().await.expect("publication barrier must remain open");
        reached.forget();
        tokio::time::advance(Duration::from_millis(20)).await;
        assert!(matches!(
            leader.wait().await,
            ColdFillWaitOutcome::Ready(Err(ColdFillError::Storage(StorageError::Timeout)))
        ));
        assert_eq!(coordinator.active_session_count_for_test(), 0);
        producer_task.await.expect("producer task must join");

        barrier.release.add_permits(1);
        clear_cold_fill_publication_barrier();
        drop(
            adapter
                .reserve_body(&plan)
                .expect("deadline must release the publication reservation"),
        );
        tokio::time::timeout(
            Duration::from_secs(1),
            adapter.clear(rustfs_object_data_cache::ObjectDataCacheInvalidationReason::Manual),
        )
        .await
        .expect("clear must complete after publication deadline");
    }

    #[tokio::test(start_paused = true)]
    async fn cold_fill_without_request_timeout_stops_at_ten_minute_hard_cap() {
        let adapter = publication_test_adapter();
        let plan = publication_test_plan(&adapter, "hard-cap");
        let coordinator = adapter.cold_fill_coordinator();
        let key = plan.key().cloned().expect("hard-cap plan must be cacheable");
        let ColdFillRole::Produce(mut producer) = coordinator.join(key) else {
            panic!("hard-cap request must produce");
        };
        let leader = producer.waiter();
        let reservation = adapter.reserve_body(&plan);
        let producer_task = tokio::spawn(start_cold_fill_producer(
            producer,
            reservation,
            || async { Ok(DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager())) },
            || async {
                Ok(GetObjectReader {
                    stream: Box::new(PendingReader),
                    object_info: ObjectInfo {
                        size: 4,
                        actual_size: 4,
                        ..Default::default()
                    },
                    buffered_body: None,
                    body_source: GetObjectBodySource::HookMissed,
                })
            },
            ColdFillProducerExecution {
                expected: 4,
                deadline: None,
                adapter: Arc::clone(&adapter),
                engine_plan: plan.clone(),
            },
        ));
        let wait = tokio::spawn(async move { leader.wait().await });

        tokio::time::advance(Duration::from_secs(599)).await;
        tokio::task::yield_now().await;
        assert!(!wait.is_finished(), "hard cap must not fire before 600 seconds");
        assert!(adapter.reserve_body(&plan).is_none(), "reservation must remain owned before the hard cap");

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(matches!(
            wait.await.expect("hard-cap waiter must join"),
            ColdFillWaitOutcome::Ready(Err(ColdFillError::Storage(StorageError::Timeout)))
        ));
        producer_task.await.expect("producer task must join");
        assert_eq!(coordinator.active_session_count_for_test(), 0);
        drop(
            adapter
                .reserve_body(&plan)
                .expect("hard cap must release the body reservation"),
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_same_key_cold_fill_consumes_one_reader() {
        const REQUESTS: usize = 2000;
        const BODY_BYTES: usize = 64 * 1024;
        const BODY_BYTES_U64: u64 = 64 * 1024;
        const BODY_BYTES_I64: i64 = 64 * 1024;

        for key_count in [1_usize, 4, 32] {
            let adapter = Arc::new(
                ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                    mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                    max_bytes: 128 * 1024 * 1024,
                    max_memory_percent: 0,
                    max_entry_bytes: 1024 * 1024,
                    min_free_memory_percent: 0,
                    fill_concurrency_per_cpu: 64,
                    fill_concurrency_max: 64,
                    ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
                })
                .expect("matrix cache config must be valid"),
            );
            let coordinator = adapter.cold_fill_coordinator();
            let disk_permits = Arc::new(tokio::sync::Semaphore::new(key_count));
            let writers = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(key_count)));
            let permit_acquires = Arc::new(AtomicUsize::new(0));
            let reader_factories = Arc::new(AtomicUsize::new(0));
            let first_polls = Arc::new(AtomicUsize::new(0));
            let completed = Arc::new(AtomicUsize::new(0));
            let bytes_read = Arc::new(AtomicUsize::new(0));
            let mut tasks = tokio::task::JoinSet::new();

            for request in 0..REQUESTS {
                let key_index = request % key_count;
                let object = format!("matrix-object-{key_index}");
                let engine_plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
                    bucket: "matrix-bucket",
                    object: &object,
                    version_id: None,
                    etag: "matrix-etag",
                    size: BODY_BYTES_U64,
                    data_dir_u128: Some(u128::try_from(key_index).unwrap_or(u128::MAX) + 1),
                    mod_time_unix_nanos: 1,
                    body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
                });
                let cache_key = engine_plan.key().cloned().expect("matrix body must be cacheable");
                let adapter = Arc::clone(&adapter);
                let coordinator = Arc::clone(&coordinator);
                let disk_permits = Arc::clone(&disk_permits);
                let writers = Arc::clone(&writers);
                let permit_acquires = Arc::clone(&permit_acquires);
                let reader_factories = Arc::clone(&reader_factories);
                let first_polls = Arc::clone(&first_polls);
                let completed = Arc::clone(&completed);
                let bytes_read = Arc::clone(&bytes_read);
                tasks.spawn(async move {
                    let outcome = coordinate_cold_fill(&coordinator, cache_key, None, None, move |producer| {
                        let reservation = adapter.reserve_body(&engine_plan);
                        let adapter = Arc::clone(&adapter);
                        let disk_permits = Arc::clone(&disk_permits);
                        let writers = Arc::clone(&writers);
                        let permit_acquires = Arc::clone(&permit_acquires);
                        let reader_factories = Arc::clone(&reader_factories);
                        let first_polls = Arc::clone(&first_polls);
                        let completed = Arc::clone(&completed);
                        let bytes_read = Arc::clone(&bytes_read);
                        let fill_plan = engine_plan.clone();
                        async move {
                            start_cold_fill_producer(
                                producer,
                                reservation,
                                || async move {
                                    permit_acquires.fetch_add(1, AtomicOrdering::Relaxed);
                                    let permit = disk_permits
                                        .acquire_owned()
                                        .await
                                        .map_err(|_| ColdFillError::DiskAdmissionClosed)?;
                                    let mut io =
                                        DefaultObjectUsecase::get_object_io_planning_without_disk(get_concurrency_manager());
                                    io.disk_permit = Some(permit.into());
                                    Ok(io)
                                },
                                || async move {
                                    reader_factories.fetch_add(1, AtomicOrdering::Relaxed);
                                    let (writer, reader) = tokio::io::duplex(BODY_BYTES * 2);
                                    writers.lock().await.push(writer);
                                    Ok(GetObjectReader {
                                        stream: Box::new(ColdFillMatrixReader {
                                            inner: reader,
                                            first_poll_recorded: false,
                                            completion_recorded: false,
                                            first_polls,
                                            completed,
                                            bytes_read,
                                        }),
                                        object_info: ObjectInfo {
                                            size: BODY_BYTES_I64,
                                            actual_size: BODY_BYTES_I64,
                                            ..Default::default()
                                        },
                                        buffered_body: None,
                                        body_source: GetObjectBodySource::HookMissed,
                                    })
                                },
                                ColdFillProducerExecution {
                                    expected: BODY_BYTES,
                                    deadline: None,
                                    adapter,
                                    engine_plan: fill_plan,
                                },
                            )
                            .await
                        }
                    })
                    .await;
                    let ColdFillCoordinateOutcome::Ready(Ok(body)) = outcome else {
                        panic!("matrix request must receive the shared body, got {outcome:?}");
                    };
                    assert_eq!(body.len(), BODY_BYTES);
                    assert!(body.iter().all(|byte| *byte == 7));
                    (key_index, body.as_ptr() as usize)
                });
            }

            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    if writers.lock().await.len() == key_count
                        && coordinator.global_waiter_count_for_test() == REQUESTS - key_count
                        && first_polls.load(AtomicOrdering::Relaxed) == key_count
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("all matrix followers must join before releasing bodies");

            let mut body_writers = std::mem::take(&mut *writers.lock().await);
            let body = vec![7_u8; BODY_BYTES];
            for writer in &mut body_writers {
                tokio::io::AsyncWriteExt::write_all(writer, &body)
                    .await
                    .expect("matrix body write must succeed");
                tokio::io::AsyncWriteExt::shutdown(writer)
                    .await
                    .expect("matrix body writer must close");
            }
            let mut backing_pointers = std::collections::HashMap::<usize, std::collections::HashSet<usize>>::new();
            tokio::time::timeout(Duration::from_secs(30), async {
                while let Some(result) = tasks.join_next().await {
                    let (key_index, body_pointer) = result.expect("matrix GET task must complete");
                    backing_pointers.entry(key_index).or_default().insert(body_pointer);
                }
            })
            .await
            .expect("matrix GET tasks must complete before the watchdog");

            assert_eq!(permit_acquires.load(AtomicOrdering::Relaxed), key_count);
            assert_eq!(reader_factories.load(AtomicOrdering::Relaxed), key_count);
            assert_eq!(first_polls.load(AtomicOrdering::Relaxed), key_count);
            assert_eq!(completed.load(AtomicOrdering::Relaxed), key_count);
            assert_eq!(bytes_read.load(AtomicOrdering::Relaxed), key_count * BODY_BYTES);
            assert_eq!(backing_pointers.len(), key_count);
            assert!(
                backing_pointers.values().all(|pointers| pointers.len() == 1),
                "all followers of one key must share one backing allocation"
            );
            assert_eq!(
                backing_pointers
                    .values()
                    .flatten()
                    .copied()
                    .collect::<std::collections::HashSet<_>>()
                    .len(),
                key_count
            );
            assert_eq!(coordinator.global_waiter_count_for_test(), 0);
            assert_eq!(coordinator.active_session_count_for_test(), 0);
            assert_eq!(disk_permits.available_permits(), key_count);

            for key_index in 0..key_count {
                let object = format!("matrix-object-{key_index}");
                let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
                    bucket: "matrix-bucket",
                    object: &object,
                    version_id: None,
                    etag: "matrix-etag",
                    size: BODY_BYTES_U64,
                    data_dir_u128: Some(u128::try_from(key_index).unwrap_or(u128::MAX) + 1),
                    mod_time_unix_nanos: 1,
                    body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
                });
                assert!(matches!(
                    adapter.lookup_body(&plan).await,
                    rustfs_object_data_cache::ObjectDataCacheLookup::Hit(_)
                ));
            }
        }
    }

    // #1324: the in-memory (buffered/cache) source is guarded by
    // MemoryTrackedBytesStream. A buffer whose length disagrees with the declared
    // content length must yield a stream error on first poll instead of a clean
    // short body or an over-long body. Reverting to the old warn-and-serve
    // behavior would make these assertions observe Ok chunks.
    #[tokio::test]
    #[serial_test::serial]
    async fn memory_tracked_bytes_stream_fails_short_body() {
        let mut stream = MemoryTrackedBytesStream::new(
            Bytes::from_static(b"test"),
            5,
            GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
            None,
            GetObjectBodyLifecycle::disabled(),
        );
        let err = stream
            .next()
            .await
            .expect("mismatched memory body must yield an item")
            .expect_err("a short memory body must fail the stream instead of serving a truncated body");
        assert_eq!(
            err.downcast_ref::<std::io::Error>().map(std::io::Error::kind),
            Some(std::io::ErrorKind::InvalidData)
        );
        assert!(stream.next().await.is_none(), "stream must terminate after the error");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn memory_tracked_bytes_stream_fails_over_long_body() {
        let mut stream = MemoryTrackedBytesStream::new(
            Bytes::from_static(b"hello!"),
            5,
            GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
            None,
            GetObjectBodyLifecycle::disabled(),
        );
        let err = stream
            .next()
            .await
            .expect("mismatched memory body must yield an item")
            .expect_err("an over-long memory body must fail the stream instead of serving mismatched bytes");
        assert_eq!(
            err.downcast_ref::<std::io::Error>().map(std::io::Error::kind),
            Some(std::io::ErrorKind::InvalidData)
        );
    }

    #[test]
    fn memory_blob_preserves_exact_remaining_length() {
        let blob = DefaultObjectUsecase::build_memory_bytes_blob(
            Bytes::from_static(b"hello"),
            5,
            GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
            GetObjectBodyLifecycle::disabled(),
        );

        assert_eq!(blob.remaining_length().exact(), Some(5));
    }

    #[test]
    #[serial_test::serial]
    fn memory_blob_once_fast_path_holds_guard_until_bytes_drop() {
        temp_env::with_var(ENV_RUSTFS_GET_SMALL_BODY_ONCE_ENABLE, Some("true"), || {
            let initial = GetObjectGuard::concurrent_count();
            let guard = GetObjectGuard::new();
            assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);

            let blob = DefaultObjectUsecase::build_memory_bytes_blob(
                Bytes::from_static(b"hello"),
                5,
                GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
                GetObjectBodyLifecycle::tracked(guard),
            );
            let mut body = s3s::Body::from(blob);
            let bytes = body.take_bytes().expect("opt-in exact memory body should stay on Body::Once");

            assert_eq!(bytes, Bytes::from_static(b"hello"));
            assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);
            drop(bytes);
            assert_eq!(GetObjectGuard::concurrent_count(), initial);
        });
    }

    #[test]
    #[serial_test::serial]
    fn memory_blob_once_fast_path_rejects_length_mismatch() {
        temp_env::with_var(ENV_RUSTFS_GET_SMALL_BODY_ONCE_ENABLE, Some("true"), || {
            let blob = DefaultObjectUsecase::build_memory_bytes_blob(
                Bytes::from_static(b"test"),
                5,
                GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
                GetObjectBodyLifecycle::disabled(),
            );
            let mut body = s3s::Body::from(blob);

            assert!(body.take_bytes().is_none(), "mismatched memory body must keep the guarded stream path");
        });
    }

    #[tokio::test]
    async fn get_object_streaming_reader_times_out_when_body_stalls() {
        let reader = GetObjectStreamingReader::new(
            PendingReader,
            "test-bucket",
            "stalled-object",
            "req-stalled-stream",
            None,
            1,
            Duration::from_millis(1),
            GetObjectBodyLifecycle::disabled(),
            None,
        );
        let mut stream = ReaderStream::with_capacity(reader, 1024);

        let err = stream
            .next()
            .await
            .expect("reader stream should yield timeout")
            .expect_err("stalled reader should return an error");

        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn get_object_streaming_reader_fails_closed_without_active_reader() {
        use tokio::io::AsyncReadExt;

        let mut reader = GetObjectStreamingReader::new(
            cursor_reader(b"x"),
            "test-bucket",
            "missing-reader-object",
            "req-missing-reader",
            None,
            1,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            None,
        );
        reader.inner.take();

        let err = reader
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("an impossible missing active reader must fail closed");

        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert_eq!(err.to_string(), "get object streaming reader lost its active read outside resume");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_streaming_reader_holds_request_guard_until_eof() {
        use tokio::io::AsyncReadExt;

        let initial = GetObjectGuard::concurrent_count();
        let guard = GetObjectGuard::new();
        assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);

        let mut reader = GetObjectStreamingReader::new(
            std::io::Cursor::new(b"hello".to_vec()),
            "test-bucket",
            "complete-object",
            "req-complete-stream",
            None,
            5,
            Duration::ZERO,
            GetObjectBodyLifecycle::tracked(guard),
            None,
        );
        let mut out = Vec::new();

        reader
            .read_to_end(&mut out)
            .await
            .expect("complete streaming body should read successfully");

        assert_eq!(out, b"hello");
        assert_eq!(GetObjectGuard::concurrent_count(), initial);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_streaming_reader_errors_on_short_eof() {
        use tokio::io::AsyncReadExt;

        // The inner reader delivers 5 bytes then a clean EOF, but the advertised
        // Content-Length is 10. The reader must surface an error rather than a clean EOF, so
        // the client sees a failed transfer instead of silently persisting a truncated body
        // (the "incomplete data mirroring" of #2955).
        let initial = GetObjectGuard::concurrent_count();
        let guard = GetObjectGuard::new();
        assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);

        let mut reader = GetObjectStreamingReader::new(
            std::io::Cursor::new(b"short".to_vec()),
            "test-bucket",
            "truncated-object",
            "req-short-eof",
            None,
            10,
            Duration::ZERO,
            GetObjectBodyLifecycle::tracked(guard),
            None,
        );
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("short body under a larger Content-Length must fail the stream");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        let incomplete_body = err
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<rustfs_rio::IncompleteBody>())
            .expect("short eof should include remaining bytes as IncompleteBody");
        assert_eq!(incomplete_body.remaining, 5);
        assert_eq!(out, b"short", "bytes read before the short EOF are still delivered");

        drop(reader);
        assert_eq!(GetObjectGuard::concurrent_count(), initial);
    }

    #[test]
    #[serial_test::serial]
    fn get_object_streaming_reader_releases_request_guard_when_dropped_incomplete() {
        let initial = GetObjectGuard::concurrent_count();
        let guard = GetObjectGuard::new();
        assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);

        let reader = GetObjectStreamingReader::new(
            std::io::Cursor::new(b"short".to_vec()),
            "test-bucket",
            "dropped-object",
            "req-dropped-stream",
            None,
            10,
            Duration::ZERO,
            GetObjectBodyLifecycle::tracked(guard),
            None,
        );
        drop(reader);

        assert_eq!(GetObjectGuard::concurrent_count(), initial);
    }

    // Emits all of `data`, then either the injected error or a clean EOF. Drives
    // the mid-stream resume state machine through its typed-error and
    // premature-EOF triggers without a store.
    struct FailAtEndReader {
        data: std::io::Cursor<Vec<u8>>,
        error: Option<std::io::Error>,
    }

    impl FailAtEndReader {
        fn new(data: &[u8], error: Option<std::io::Error>) -> Self {
            Self {
                data: std::io::Cursor::new(data.to_vec()),
                error,
            }
        }
    }

    impl AsyncRead for FailAtEndReader {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            let position = usize::try_from(self.data.position()).unwrap_or(usize::MAX);
            let source_len = self.data.get_ref().len();
            if position >= source_len {
                return match self.error.take() {
                    Some(error) => Poll::Ready(Err(error)),
                    None => Poll::Ready(Ok(())),
                };
            }
            let want = buf.remaining().min(source_len - position);
            if want == 0 {
                return Poll::Ready(Ok(()));
            }
            buf.put_slice(&self.data.get_ref()[position..position + want]);
            self.data.set_position(u64::try_from(position + want).unwrap_or(u64::MAX));
            Poll::Ready(Ok(()))
        }
    }

    fn relocation_read_error() -> std::io::Error {
        std::io::Error::other(StorageError::FileNotFound)
    }

    fn counting_resume_control(
        reopen_count: Arc<AtomicUsize>,
        mut reopen: impl FnMut(usize) -> Result<FailAtEndReader, GetObjectResumeFailure> + Send + Sync + 'static,
    ) -> GetObjectResumeControl<FailAtEndReader> {
        let reopen: GetObjectReopen<FailAtEndReader> = Box::new(move |emitted| {
            reopen_count.fetch_add(1, Ordering::Relaxed);
            let outcome = reopen(emitted);
            Box::pin(async move { outcome })
        });
        GetObjectResumeControl::new(
            reopen,
            RetryTimer::new(
                GET_OBJECT_RESUME_MAX_ATTEMPTS,
                Duration::from_millis(1),
                Duration::from_millis(2),
                rustfs_utils::retry::NO_JITTER,
                0,
            ),
        )
    }

    #[tokio::test]
    async fn get_object_streaming_reader_resumes_after_relocation_error() {
        use tokio::io::AsyncReadExt;

        // Every typed relocation variant the codec read path can surface
        // mid-body must arm the resume flow.
        for variant in [
            StorageError::FileNotFound,
            StorageError::ObjectNotFound("test-bucket".to_string(), "relocated-object".to_string()),
            StorageError::InsufficientReadQuorum("test-bucket".to_string(), "relocated-object".to_string()),
            StorageError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "relocated shard disappeared")),
        ] {
            let reopen_count = Arc::new(AtomicUsize::new(0));
            let control = counting_resume_control(Arc::clone(&reopen_count), |emitted| {
                assert_eq!(emitted, 6, "resume must reopen at the emitted offset");
                Ok(FailAtEndReader::new(b"world", None))
            });
            let mut reader = GetObjectStreamingReader::new(
                FailAtEndReader::new(b"hello ", Some(std::io::Error::other(variant))),
                "test-bucket",
                "relocated-object",
                "req-resume-typed-error",
                None,
                11,
                Duration::ZERO,
                GetObjectBodyLifecycle::disabled(),
                Some(control),
            );
            let mut out = Vec::new();
            reader
                .read_to_end(&mut out)
                .await
                .expect("a resumed body must deliver the full committed content");

            assert_eq!(out, b"hello world");
            assert_eq!(reopen_count.load(Ordering::Relaxed), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn get_object_streaming_reader_releases_failed_disk_permit_before_reopen() {
        use tokio::io::AsyncReadExt;

        let manager = Arc::new(ConcurrencyManager::with_disk_read_caps_for_test(1, 0));
        let initial_permit = match manager
            .admit_disk_read(Duration::ZERO)
            .await
            .expect("test disk admission must remain open")
        {
            DiskReadAdmission::Primary(permit) => permit,
            other => panic!("initial read must hold the only primary permit, got {other:?}"),
        };
        let reopen_count = Arc::new(AtomicUsize::new(0));
        let reopen: GetObjectReopen<DiskReadPermitReader<FailAtEndReader>> = Box::new({
            let manager = Arc::clone(&manager);
            let reopen_count = Arc::clone(&reopen_count);
            move |emitted| {
                assert_eq!(emitted, 6, "resume must reopen at the emitted offset");
                reopen_count.fetch_add(1, Ordering::Relaxed);
                let manager = Arc::clone(&manager);
                Box::pin(async move {
                    match manager
                        .admit_disk_read(Duration::from_millis(1))
                        .await
                        .map_err(|_| GetObjectResumeFailure::Fatal)?
                    {
                        DiskReadAdmission::Primary(permit) => {
                            Ok(DiskReadPermitReader::new(FailAtEndReader::new(b"world", None), permit.into()))
                        }
                        _ => Err(GetObjectResumeFailure::Retryable),
                    }
                })
            }
        });
        let control = GetObjectResumeControl::new(
            reopen,
            RetryTimer::new(
                GET_OBJECT_RESUME_MAX_ATTEMPTS,
                Duration::from_millis(1),
                Duration::from_millis(2),
                rustfs_utils::retry::NO_JITTER,
                0,
            ),
        );
        let initial_reader =
            DiskReadPermitReader::new(FailAtEndReader::new(b"hello ", Some(relocation_read_error())), initial_permit.into());
        let mut reader = GetObjectStreamingReader::new(
            initial_reader,
            "test-bucket",
            "relocated-object",
            "req-resume-single-permit",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();

        reader
            .read_to_end(&mut out)
            .await
            .expect("resume must not wait on the failed reader's permit");

        assert_eq!(out, b"hello world");
        assert_eq!(reopen_count.load(Ordering::Relaxed), 1);
        assert_eq!(
            manager.io_queue_status().permits_in_use,
            0,
            "the replacement reader must release its permit at EOF"
        );
    }

    #[tokio::test]
    async fn get_object_streaming_reader_resumes_after_premature_eof() {
        use tokio::io::AsyncReadExt;

        // The legacy duplex read path surfaces vanished object data as a clean
        // EOF before the committed length; the resume flow must treat it like
        // the typed relocation error.
        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |emitted| {
            assert_eq!(emitted, 6, "resume must reopen at the emitted offset");
            Ok(FailAtEndReader::new(b"world", None))
        });
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello ", None),
            "test-bucket",
            "truncated-object",
            "req-resume-short-eof",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        reader
            .read_to_end(&mut out)
            .await
            .expect("a resumed body must deliver the full committed content");

        assert_eq!(out, b"hello world");
        assert_eq!(reopen_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn get_object_streaming_reader_clean_eof_does_not_resume() {
        use tokio::io::AsyncReadExt;

        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |_| {
            panic!("a cleanly completed body must never reopen");
        });
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello world", None),
            "test-bucket",
            "complete-object",
            "req-resume-clean-eof",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.expect("complete body must read");

        assert_eq!(out, b"hello world");
        assert_eq!(reopen_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn get_object_streaming_reader_fatal_resume_failure_returns_original_error() {
        use tokio::io::AsyncReadExt;

        // A fatal reopen failure (the reopened object is a different version)
        // must surface the original trigger error after exactly one attempt,
        // with only the originally emitted prefix delivered.
        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |_| Err(GetObjectResumeFailure::Fatal));
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello ", Some(relocation_read_error())),
            "test-bucket",
            "replaced-object",
            "req-resume-fatal",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("a fatal resume failure must fail the body with the original error");

        assert!(
            err.get_ref().is_some_and(|inner| inner.is::<StorageError>()),
            "the surfaced error must be the original typed trigger, got: {err}"
        );
        assert_eq!(out, b"hello ");
        assert_eq!(
            reopen_count.load(Ordering::Relaxed),
            1,
            "a fatal failure must short-circuit the retry budget"
        );
    }

    #[tokio::test]
    async fn get_object_streaming_reader_exhausts_resume_budget() {
        use tokio::io::AsyncReadExt;

        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |_| Err(GetObjectResumeFailure::Retryable));
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello ", Some(relocation_read_error())),
            "test-bucket",
            "vanished-object",
            "req-resume-budget",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("an exhausted resume budget must fail the body with the original error");

        assert!(
            err.get_ref().is_some_and(|inner| inner.is::<StorageError>()),
            "the surfaced error must be the original typed trigger, got: {err}"
        );
        assert_eq!(out, b"hello ");
        assert_eq!(
            reopen_count.load(Ordering::Relaxed),
            usize::try_from(GET_OBJECT_RESUME_MAX_ATTEMPTS).expect("resume budget fits usize"),
            "resume must stop after its reopen budget"
        );
    }

    #[tokio::test]
    async fn get_object_streaming_reader_rearms_resume_after_a_successful_resume() {
        use tokio::io::AsyncReadExt;

        // A successful resume restores the armed state: a second mid-stream
        // relocation error on the replacement stream must resume again.
        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |emitted| match emitted {
            6 => Ok(FailAtEndReader::new(b"wo", Some(relocation_read_error()))),
            8 => Ok(FailAtEndReader::new(b"rld", None)),
            other => panic!("unexpected reopen offset {other}"),
        });
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello ", Some(relocation_read_error())),
            "test-bucket",
            "twice-relocated-object",
            "req-resume-rearm",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        reader
            .read_to_end(&mut out)
            .await
            .expect("a re-armed resume must deliver the full committed content");

        assert_eq!(out, b"hello world");
        assert_eq!(reopen_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn get_object_streaming_reader_resume_budget_is_per_body() {
        use tokio::io::AsyncReadExt;

        // The retry budget is consumed across the whole body, not reset per
        // error: one successful resume plus two failed reopens exhausts it.
        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |emitted| match emitted {
            6 => Ok(FailAtEndReader::new(b"wo", Some(relocation_read_error()))),
            _ => Err(GetObjectResumeFailure::Retryable),
        });
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello ", Some(relocation_read_error())),
            "test-bucket",
            "budget-shared-object",
            "req-resume-budget-per-body",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("the shared budget must exhaust and surface the latest trigger error");

        assert!(
            err.get_ref().is_some_and(|inner| inner.is::<StorageError>()),
            "the surfaced error must be the typed trigger, got: {err}"
        );
        assert_eq!(out, b"hello wo");
        assert_eq!(
            reopen_count.load(Ordering::Relaxed),
            usize::try_from(GET_OBJECT_RESUME_MAX_ATTEMPTS).expect("resume budget fits usize"),
            "the budget spans every resume of the same body"
        );
    }

    #[tokio::test]
    async fn get_object_streaming_reader_non_relocation_error_passes_through() {
        use tokio::io::AsyncReadExt;

        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |_| {
            panic!("a non-relocation read error must not reopen");
        });
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello ", Some(std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt"))),
            "test-bucket",
            "corrupt-object",
            "req-resume-passthrough",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("a non-relocation error must fail the body unchanged");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(out, b"hello ");
        assert_eq!(reopen_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn get_object_streaming_reader_error_after_full_delivery_does_not_resume() {
        use tokio::io::AsyncReadExt;

        // The committed length is already delivered when the inner stream
        // errors, so the error must keep the existing fail-loud behavior
        // instead of arming a resume.
        let reopen_count = Arc::new(AtomicUsize::new(0));
        let control = counting_resume_control(Arc::clone(&reopen_count), |_| {
            panic!("an error after full delivery must not reopen");
        });
        let mut reader = GetObjectStreamingReader::new(
            FailAtEndReader::new(b"hello world", Some(relocation_read_error())),
            "test-bucket",
            "fully-delivered-object",
            "req-resume-after-full",
            None,
            11,
            Duration::ZERO,
            GetObjectBodyLifecycle::disabled(),
            Some(control),
        );
        let mut out = Vec::new();
        let err = reader
            .read_to_end(&mut out)
            .await
            .expect_err("a post-completion inner error still surfaces instead of being swallowed");

        assert!(
            err.get_ref().is_some_and(|inner| inner.is::<StorageError>()),
            "the surfaced error must be the inner typed error, got: {err}"
        );
        assert_eq!(out, b"hello world");
        assert_eq!(reopen_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn get_object_resume_identity_requires_same_version() {
        let version_id = Uuid::from_u128(0x1234);
        let mod_time = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
        let later_mod_time = OffsetDateTime::from_unix_timestamp(1_700_000_100).expect("valid timestamp");
        let identity = GetObjectResumeIdentity {
            version_id: Some(version_id),
            mod_time: Some(mod_time),
            size: 11,
            etag: Some("etag-a".to_string()),
            range_dependent_size: false,
        };
        let info = ObjectInfo {
            version_id: Some(version_id),
            mod_time: Some(mod_time),
            size: 11,
            etag: Some("etag-a".to_string()),
            ..Default::default()
        };
        assert!(identity.matches(&info, 0));
        assert!(identity.matches(&info, 6), "a plain read reports the range-invariant oi.size");
        // Rebalance regenerates data_dir for the same version: identity must
        // still match so a relocated read can resume.
        assert!(identity.matches(
            &ObjectInfo {
                data_dir: Some(Uuid::from_u128(0xbeef)),
                ..info.clone()
            },
            0
        ));
        assert!(!identity.matches(
            &ObjectInfo {
                version_id: Some(Uuid::from_u128(0x5678)),
                ..info.clone()
            },
            0
        ));
        assert!(!identity.matches(
            &ObjectInfo {
                version_id: None,
                ..info.clone()
            },
            0
        ));
        assert!(!identity.matches(
            &ObjectInfo {
                mod_time: Some(later_mod_time),
                ..info.clone()
            },
            0
        ));
        assert!(!identity.matches(
            &ObjectInfo {
                size: 12,
                ..info.clone()
            },
            0
        ));
        assert!(!identity.matches(
            &ObjectInfo {
                etag: Some("etag-b".to_string()),
                ..info.clone()
            },
            0
        ));
        assert!(!identity.matches(&ObjectInfo { etag: None, ..info }, 0));
    }

    #[test]
    fn get_object_resume_identity_normalizes_range_dependent_size() {
        // Encrypted and compressed reads report the per-read delivered length
        // as object_info.size, so the reopened subrange reports size - emitted
        // for the same version.
        let version_id = Uuid::from_u128(0x1234);
        let mod_time = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
        let identity = GetObjectResumeIdentity {
            version_id: Some(version_id),
            mod_time: Some(mod_time),
            size: 11,
            etag: Some("etag-a".to_string()),
            range_dependent_size: true,
        };
        let reopened = ObjectInfo {
            version_id: Some(version_id),
            mod_time: Some(mod_time),
            size: 5,
            etag: Some("etag-a".to_string()),
            ..Default::default()
        };
        assert!(identity.matches(&reopened, 6), "the reopened subrange reports size - emitted");
        assert!(identity.matches(
            &ObjectInfo {
                size: 11,
                ..reopened.clone()
            },
            0
        ));
        assert!(
            !identity.matches(
                &ObjectInfo {
                    size: 11,
                    ..reopened.clone()
                },
                6
            ),
            "an unshrunk range-dependent size after emitted bytes is a different object"
        );
        assert!(!identity.matches(&ObjectInfo { size: 4, ..reopened }, 6));
    }

    #[test]
    fn get_object_resume_range_offsets() {
        // A full-object read that emitted nothing reopens range-free so the
        // replacement stream keeps the codec fast path.
        assert!(GetObjectResumeContext::resume_range(0, -1, 0).is_none());

        // Mid-stream full-object resume: open-ended from the emitted offset.
        let range = GetObjectResumeContext::resume_range(0, -1, 6).expect("a mid-stream resume must carry a range");
        assert!(!range.is_suffix_length);
        assert_eq!((range.start, range.end), (6, -1));

        // Ranged reads resume at absolute offsets with the committed end
        // preserved (suffix ranges and partNumber GETs are resolved to absolute
        // offsets before these values are captured).
        let range = GetObjectResumeContext::resume_range(10, 19, 0).expect("a ranged resume must carry a range");
        assert!(!range.is_suffix_length);
        assert_eq!((range.start, range.end), (10, 19));
        let range = GetObjectResumeContext::resume_range(10, 19, 5).expect("a ranged resume must carry a range");
        assert_eq!((range.start, range.end), (15, 19));
    }

    async fn real_get_resume_test_context() -> (Vec<std::path::PathBuf>, Arc<ECStore>, Arc<AppContext>) {
        let (disk_paths, store) = crate::app::gating_test_env::shared_gating_ecstore_and_disk_paths().await;
        if current_app_context().is_none() {
            crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let ambient = current_app_context().expect("resume wiring tests require an ambient AppContext");
        let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        (disk_paths, store, context)
    }

    // Uploads a real multipart object through the store and returns the
    // concatenated body, so resume wiring tests can verify byte-exact delivery
    // against on-disk part files.
    async fn put_real_multipart_object(
        store: &Arc<ECStore>,
        bucket: &str,
        object: &str,
        part_size: usize,
        part_count: usize,
        fill: u8,
    ) -> Vec<u8> {
        use crate::app::storage_api::multipart_usecase::contract::multipart::{CompletePart, MultipartOperations as _};

        let upload = store
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("create multipart upload");
        let mut parts = Vec::new();
        let mut body = Vec::with_capacity(part_size * part_count);
        for part_id in 1..=part_count {
            let part_fill = fill.wrapping_add(u8::try_from(part_id - 1).expect("test part index must fit u8"));
            let part_body = vec![part_fill; part_size];
            body.extend_from_slice(&part_body);
            let mut reader = PutObjReader::from_vec(part_body);
            let part = store
                .put_object_part(bucket, object, &upload.upload_id, part_id, &mut reader, &ObjectOptions::default())
                .await
                .expect("upload multipart part");
            parts.push(CompletePart {
                part_num: part_id,
                etag: part.etag,
                ..Default::default()
            });
        }
        store
            .clone()
            .complete_multipart_upload(bucket, object, &upload.upload_id, parts, &ObjectOptions::default())
            .await
            .expect("complete multipart upload");
        body
    }

    // An erasure-coded write returns once write-quorum disks commit, so a
    // lagging disk can legally still be missing its xl.meta when the write
    // call returns. Fixtures that iterate every disk of the owning pool must
    // wait for full materialization first, or they race the trailing disk
    // writes under CI load (issue #6703). Bounded so a genuinely failed disk
    // write still surfaces as a test failure instead of a hang.
    async fn wait_for_object_on_every_disk(disk_paths: &[std::path::PathBuf], bucket: &str, object: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if disk_paths
                .iter()
                .all(|path| path.join(bucket).join(object).join("xl.meta").is_file())
            {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "object {bucket}/{object} must materialize xl.meta on every pool disk within the readiness window"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    // Deletes the given part files from every version data dir present on the
    // disks, simulating rebalance removing the object data while xl.meta stays
    // readable. Returns the number of version dirs visited and files removed.
    fn delete_object_part_shards(
        disk_paths: &[std::path::PathBuf],
        bucket: &str,
        object: &str,
        part_numbers: &[usize],
    ) -> (usize, usize) {
        let mut version_dirs = 0;
        let mut deleted = 0;
        for disk_path in disk_paths {
            let object_dir = disk_path.join(bucket).join(object);
            let entries = match std::fs::read_dir(&object_dir) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => panic!("object directory must be readable: {error}"),
            };
            for entry in entries {
                let entry = entry.expect("object directory entry must read");
                if !entry.file_type().expect("entry file type must read").is_dir() {
                    continue;
                }
                version_dirs += 1;
                for part_number in part_numbers {
                    let part_file = entry.path().join(format!("part.{part_number}"));
                    if part_file.exists() {
                        std::fs::remove_file(&part_file).expect("part shard must be removable");
                        deleted += 1;
                    }
                }
            }
        }
        (version_dirs, deleted)
    }

    // The surfaced mid-stream failure must be the original trigger: a typed
    // relocation StorageError from the codec read path, or an IncompleteBody
    // (UnexpectedEof) from the duplex path. The resume flow must never
    // fabricate a different error.
    fn assert_original_trigger_error(error: &(dyn std::error::Error + Send + Sync + 'static)) {
        let Some(io_error) = error.downcast_ref::<std::io::Error>() else {
            panic!("body error must be an io::Error, got: {error}");
        };
        let is_trigger = io_error.kind() == std::io::ErrorKind::UnexpectedEof || is_object_relocation_error(io_error);
        assert!(is_trigger, "body error must be the original relocation trigger, got: {error}");
    }

    #[tokio::test]
    #[serial_test::serial]
    // SAFETY: the test mutates one process env var before any use; nextest runs
    // each test in its own process, so the mutation cannot race another test.
    #[allow(unsafe_code)]
    async fn execute_get_object_resume_exhausts_budget_when_object_data_vanishes() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        // The resume phase runs inside the body stall budget (default 10s),
        // and three real reopen attempts against missing shards approach it on
        // loaded CI disks; widen the budget so this test asserts the resume
        // outcome instead of racing the stall timer.
        unsafe { std::env::set_var(rustfs_config::ENV_OBJECT_DISK_READ_TIMEOUT, "120") };

        let (disk_paths, store, context) = real_get_resume_test_context().await;
        let bucket = format!("resume-vanish-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create resume failure-path bucket");
        let part_size = 6 * 1024 * 1024;
        let body = put_real_multipart_object(&store, &bucket, object, part_size, 3, 0xAA).await;

        // Remove the part.2/part.3 shards on every disk before the GET starts,
        // so no file descriptor for them can exist: the stream must fail at the
        // part-2 boundary, and every reopen resolves intact metadata whose data
        // is gone, so the whole resume budget burns down.
        let (version_dirs, deleted) = delete_object_part_shards(&disk_paths, &bucket, object, &[2, 3]);
        assert!(version_dirs > 0, "the multipart object must have at least one version data directory");
        assert_eq!(deleted, version_dirs * 2);

        let input = GetObjectInput::builder()
            .bucket(bucket)
            .key(object.to_string())
            .build()
            .expect("resume failure-path GET input must build");
        let usecase = DefaultObjectUsecase::with_context(Some(context));
        let attempts_before = GET_OBJECT_RESUME_ATTEMPTS_FOR_TEST.load(Ordering::Relaxed);
        let mut response = usecase
            .execute_get_object(build_request(input, Method::GET))
            .await
            .expect("the GET commits a response; the body fails mid-stream");
        let mut response_body = response.output.body.take().expect("GET response must include a body");
        let mut collected = Vec::new();
        let mut stream_error = None;
        while let Some(chunk) = response_body.next().await {
            match chunk {
                Ok(bytes) => collected.extend_from_slice(&bytes),
                Err(error) => {
                    stream_error = Some(error);
                    break;
                }
            }
        }

        assert_eq!(
            collected,
            &body[..part_size],
            "only the first part can be delivered before the object data vanishes"
        );
        assert_original_trigger_error(
            stream_error
                .as_deref()
                .expect("the body stream must fail at the missing part"),
        );
        let attempts = GET_OBJECT_RESUME_ATTEMPTS_FOR_TEST.load(Ordering::Relaxed) - attempts_before;
        assert_eq!(
            attempts,
            usize::try_from(GET_OBJECT_RESUME_MAX_ATTEMPTS).expect("resume budget fits usize"),
            "resume must exhaust its reopen budget before failing"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_get_object_resumes_from_relocated_pool_without_splicing_body() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let (temp_dir, pool_disk_paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        if current_app_context().is_none() {
            crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let ambient = current_app_context().expect("multi-pool resume test requires an ambient AppContext");
        let context = Arc::new(AppContext::new(Arc::clone(&store), ambient.iam(), ambient.kms()));
        let bucket = format!("resume-relocate-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create multi-pool resume bucket");
        let part_size = 24 * 1024 * 1024;
        let body = put_real_multipart_object(&store, &bucket, object, part_size, 3, 0xA5).await;
        let upload_pool = pool_disk_paths
            .iter()
            .position(|paths| {
                paths
                    .iter()
                    .any(|path| path.join(&bucket).join(object).join("xl.meta").is_file())
            })
            .expect("multipart object must be placed in one source pool");
        wait_for_object_on_every_disk(&pool_disk_paths[upload_pool], &bucket, object).await;
        if upload_pool != 0 {
            let mut normalized_disks = 0;
            for (source_disk, target_disk) in pool_disk_paths[upload_pool].iter().zip(&pool_disk_paths[0]) {
                let source_object = source_disk.join(&bucket).join(object);
                // The multipart commit succeeds on write quorum, so under suite
                // IO load a lagging disk of the erasure set can legitimately
                // hold no object directory (#6701). Normalize the disks that
                // do hold it; the reader tolerates the same minority gap.
                if !source_object.exists() {
                    continue;
                }
                let target_bucket = target_disk.join(&bucket);
                std::fs::create_dir_all(&target_bucket).expect("create normalized target bucket directory");
                std::fs::rename(source_object, target_bucket.join(object)).expect("normalize the test object into the old pool");
                normalized_disks += 1;
            }
            assert!(
                normalized_disks > pool_disk_paths[upload_pool].len() / 2,
                "a write-quorum majority of the upload pool's disks must hold the multipart object to normalize"
            );
        }
        let source_pool = 0;
        let target_pool = 1;

        let input = GetObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .expect("multi-pool resume GET input must build");
        let usecase = DefaultObjectUsecase::with_context(Some(context));
        let mut response = usecase
            .execute_get_object(build_request(input, Method::GET))
            .await
            .expect("multi-pool GET must commit its response");
        let mut response_body = response
            .output
            .body
            .take()
            .expect("multi-pool GET response must include a body");

        // Open the source reader before publishing the relocated object. Build
        // each replica outside the bucket and rename it into place atomically so
        // background maintenance never observes a metadata-less target object.
        let mut staged_targets = Vec::with_capacity(pool_disk_paths[target_pool].len());
        for (source_disk, target_disk) in pool_disk_paths[source_pool].iter().zip(&pool_disk_paths[target_pool]) {
            let source_dir = source_disk.join(&bucket).join(object);
            // The same write-quorum minority gap tolerated above (#6701) can
            // leave a lagging source-pool disk without the object; skip it and
            // stage the replicas that exist — the reader tolerates the gap.
            if !source_dir.join("xl.meta").is_file() {
                continue;
            }
            let target_dir = target_disk.join(&bucket).join(object);
            let staging_dir = temp_dir.path().join(format!("resume-relocate-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&staging_dir).expect("create relocated target staging directory");
            for entry in std::fs::read_dir(&source_dir).expect("read source object directory") {
                let entry = entry.expect("read source object entry");
                if !entry.file_type().expect("read source object entry type").is_dir() {
                    continue;
                }
                let target_entry = staging_dir.join(entry.file_name());
                std::fs::create_dir_all(&target_entry).expect("create relocated target data directory");
                for child in std::fs::read_dir(entry.path()).expect("read source object data directory") {
                    let child = child.expect("read source object data entry");
                    std::fs::copy(child.path(), target_entry.join(child.file_name())).expect("copy relocated object data entry");
                }
            }
            std::fs::copy(source_dir.join("xl.meta"), staging_dir.join("xl.meta")).expect("stage relocated object metadata");
            staged_targets.push((staging_dir, target_dir, source_dir.join("xl.meta")));
        }
        assert!(
            staged_targets.len() > pool_disk_paths[source_pool].len() / 2,
            "a write-quorum majority of the source pool's disks must hold the object to stage the relocation"
        );
        let (version_dirs, deleted) = delete_object_part_shards(&pool_disk_paths[source_pool], &bucket, object, &[2, 3]);
        assert!(version_dirs > 0, "the source pool must have at least one version data directory");
        assert_eq!(deleted, version_dirs * 2);

        for (staging_dir, target_dir, source_meta) in staged_targets {
            std::fs::rename(staging_dir, target_dir).expect("publish relocated target object");
            std::fs::remove_file(source_meta).expect("remove relocated source object metadata");
        }
        store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("the relocated object must resolve from the target pool");

        let attempts_before = GET_OBJECT_RESUME_ATTEMPTS_FOR_TEST.load(Ordering::Relaxed);
        let mut collected = Vec::new();
        while let Some(chunk) = response_body.next().await {
            match chunk {
                Ok(chunk) => collected.extend_from_slice(&chunk),
                Err(err) => panic!(
                    "relocated GET from pool {source_pool} must resume from pool {target_pool} after {} attempts: {err:?}",
                    GET_OBJECT_RESUME_ATTEMPTS_FOR_TEST.load(Ordering::Relaxed) - attempts_before
                ),
            }
        }

        assert_eq!(collected, body, "resumed production GET must preserve the complete body byte-for-byte");
        assert_eq!(
            GET_OBJECT_RESUME_ATTEMPTS_FOR_TEST.load(Ordering::Relaxed) - attempts_before,
            1,
            "the relocated body must reopen exactly once"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_resume_reopen_rejects_a_replaced_object_version() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
        use tokio::io::AsyncReadExt as _;

        let (_disk_paths, store, _context) = real_get_resume_test_context().await;
        let bucket = format!("resume-identity-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create resume identity bucket");
        let body = vec![0xAA; 1024 * 1024];
        put_real_cold_fill_object(&store, &bucket, object, &body).await;
        let info = store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("read the committed object metadata");

        let ctx = GetObjectResumeContext::new(
            Arc::clone(&store),
            &bucket,
            object,
            ObjectOptions::default(),
            &HeaderMap::new(),
            &info,
            0,
            -1,
        );

        // Positive control: the same version reopens and streams the body.
        let manager = get_concurrency_manager();
        let permits_before = manager.io_queue_status().permits_in_use;
        let mut reader = ctx.reopen(0).await.expect("reopening the same version must succeed");
        assert_eq!(
            manager.io_queue_status().permits_in_use,
            permits_before + 1,
            "the resumed stream must hold disk-read admission like the initial read"
        );
        let mut reopened_body = Vec::new();
        reader
            .read_to_end(&mut reopened_body)
            .await
            .expect("the reopened reader must stream the body");
        assert_eq!(reopened_body, body);
        // The reopened reader holds the object read lock; drop it before the
        // delete below requests the write lock.
        drop(reader);
        assert_eq!(
            manager.io_queue_status().permits_in_use,
            permits_before,
            "dropping the resumed stream must release its disk-read admission"
        );

        // A nonzero-offset reopen must splice the remaining bytes exactly.
        let mut reader = ctx.reopen(1024).await.expect("reopening at a nonzero offset must succeed");
        let mut tail = Vec::new();
        reader
            .read_to_end(&mut tail)
            .await
            .expect("the offset reader must stream the remaining body");
        assert_eq!(tail, body[1024..], "the resumed stream must continue from the emitted offset exactly");
        drop(reader);

        // Delete and re-PUT the key, then the stale context must refuse to
        // splice the replacement version into the committed response.
        store
            .delete_object(&bucket, object, ObjectOptions::default())
            .await
            .expect("delete the original object");
        let replacement_body = vec![0xBB; 2 * 1024 * 1024];
        put_real_cold_fill_object(&store, &bucket, object, &replacement_body).await;
        let result = ctx.reopen(0).await;
        assert!(
            matches!(result, Err(GetObjectResumeFailure::Fatal)),
            "reopening a replaced version must fail closed"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_resume_context_pins_latest_read_to_resolved_version() {
        let (_disk_paths, store, _context) = real_get_resume_test_context().await;
        let resolved_version = Uuid::new_v4();
        let info = ObjectInfo {
            version_id: Some(resolved_version),
            ..Default::default()
        };

        let ctx = GetObjectResumeContext::new(
            Arc::clone(&store),
            "bucket",
            "object.bin",
            ObjectOptions::default(),
            &HeaderMap::new(),
            &info,
            0,
            -1,
        );
        assert_eq!(
            ctx.opts.version_id,
            Some(resolved_version.to_string()),
            "latest GET resume must reopen the initially resolved version, not the moving latest"
        );

        let explicit_version = Uuid::new_v4().to_string();
        let explicit_opts = ObjectOptions {
            version_id: Some(explicit_version.clone()),
            ..Default::default()
        };
        let ctx = GetObjectResumeContext::new(
            Arc::clone(&store),
            "bucket",
            "object.bin",
            explicit_opts,
            &HeaderMap::new(),
            &info,
            0,
            -1,
        );
        assert_eq!(
            ctx.opts.version_id.as_deref(),
            Some(explicit_version.as_str()),
            "an explicit request version must stay authoritative"
        );

        let unversioned_info = ObjectInfo::default();
        let ctx = GetObjectResumeContext::new(
            Arc::clone(&store),
            "bucket",
            "object.bin",
            ObjectOptions::default(),
            &HeaderMap::new(),
            &unversioned_info,
            0,
            -1,
        );
        assert_eq!(ctx.opts.version_id, None, "unversioned reads have no version to pin");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_resume_context_redacts_ssec_headers_and_flags_range_dependent_size() {
        let (_disk_paths, store, _context) = real_get_resume_test_context().await;

        let mut request_headers = HeaderMap::new();
        request_headers.insert(SSEC_ALGORITHM_HEADER, HeaderValue::from_static("AES256"));
        request_headers.insert(SSEC_KEY_HEADER, HeaderValue::from_static("dGVzdC1rZXk="));
        request_headers.insert(SSEC_KEY_MD5_HEADER, HeaderValue::from_static("bWQ1"));
        request_headers.insert(http::header::AUTHORIZATION, HeaderValue::from_static("AWS4-HMAC-SHA256 Credential=test"));
        request_headers.insert("x-amz-security-token", HeaderValue::from_static("session-token"));
        let store_headers = project_ssec_transport_headers(&request_headers);
        assert_eq!(store_headers.len(), 3, "only store-consumed SSE-C headers are forwarded");
        assert!(store_headers.values().all(HeaderValue::is_sensitive));
        assert!(store_headers.get(http::header::AUTHORIZATION).is_none());
        assert!(store_headers.get("x-amz-security-token").is_none());
        assert!(!format!("{store_headers:?}").contains("dGVzdC1rZXk="));
        let plain_info = ObjectInfo {
            size: 11,
            ..Default::default()
        };
        let ctx = GetObjectResumeContext::new(
            Arc::clone(&store),
            "bucket",
            "object.bin",
            ObjectOptions::default(),
            &request_headers,
            &plain_info,
            0,
            -1,
        );
        for name in [SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER] {
            let value = ctx.ssec_headers.get(name).expect("the SSE-C trio is retained");
            assert!(value.is_sensitive(), "store spans record headers at debug; {name} must be redacted there");
        }
        assert_eq!(
            ctx.ssec_headers.len(),
            3,
            "only the SSE-C trio may be retained; credential headers must never be replayed into store spans"
        );
        assert!(!ctx.identity.range_dependent_size, "plain reads report the range-invariant oi.size");

        let encrypted_info = ObjectInfo {
            size: 11,
            user_defined: Arc::new(
                [("x-amz-server-side-encryption".to_string(), "aws:kms".to_string())]
                    .into_iter()
                    .collect(),
            ),
            ..Default::default()
        };
        let ctx = GetObjectResumeContext::new(
            Arc::clone(&store),
            "bucket",
            "object.bin",
            ObjectOptions::default(),
            &HeaderMap::new(),
            &encrypted_info,
            0,
            -1,
        );
        assert!(ctx.identity.range_dependent_size, "encrypted reads report the per-read delivered length");

        let compressed_info = ObjectInfo {
            size: 11,
            user_defined: Arc::new(
                [("x-rustfs-internal-compression".to_string(), "snappy".to_string())]
                    .into_iter()
                    .collect(),
            ),
            ..Default::default()
        };
        let ctx = GetObjectResumeContext::new(
            Arc::clone(&store),
            "bucket",
            "object.bin",
            ObjectOptions::default(),
            &HeaderMap::new(),
            &compressed_info,
            0,
            -1,
        );
        assert!(ctx.identity.range_dependent_size, "compressed reads report the per-read delivered length");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn memory_tracked_bytes_stream_releases_request_guard_after_emit() {
        let initial = GetObjectGuard::concurrent_count();
        let guard = GetObjectGuard::new();
        assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);

        let mut stream = MemoryTrackedBytesStream::new(
            Bytes::from_static(b"hello"),
            5,
            GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
            None,
            GetObjectBodyLifecycle::tracked(guard),
        );
        let chunk = stream
            .next()
            .await
            .expect("memory body should emit one chunk")
            .expect("memory body chunk should be readable");

        assert_eq!(chunk.as_ref(), b"hello");
        assert_eq!(GetObjectGuard::concurrent_count(), initial);
    }

    #[test]
    #[serial_test::serial]
    fn memory_tracked_bytes_stream_releases_request_guard_for_zero_length_without_poll() {
        let initial = GetObjectGuard::concurrent_count();
        let guard = GetObjectGuard::new();
        assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);

        let stream = MemoryTrackedBytesStream::new(
            Bytes::new(),
            0,
            GET_MEMORY_BODY_SOURCE_BUFFERED_BODY,
            None,
            GetObjectBodyLifecycle::tracked(guard),
        );
        drop(stream);

        assert_eq!(GetObjectGuard::concurrent_count(), initial);
    }

    #[tokio::test]
    async fn disk_read_permit_reader_holds_permit_until_reader_is_dropped() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("test semaphore should grant owned permit");

        let reader = DiskReadPermitReader::new(std::io::Cursor::new(Vec::<u8>::new()), permit.into());
        assert_eq!(semaphore.available_permits(), 0);

        drop(reader);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test]
    #[serial_test::serial(cold_fill_metrics_gate)]
    async fn cold_fill_follower_disk_permit_metric_tracks_actual_permit_lifetime() {
        COLD_FILL_FOLLOWER_DISK_PERMITS_FOR_TEST.store(0, Ordering::Relaxed);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        scope_cold_fill_disk_permit_owner_for_test(ColdFillDiskPermitOwner::Follower, async {
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("follower test semaphore must grant an owned permit");
            let tracked = GetObjectDiskPermit::new(permit);
            assert_eq!(semaphore.available_permits(), 0);
            assert_eq!(COLD_FILL_FOLLOWER_DISK_PERMITS_FOR_TEST.load(Ordering::Relaxed), 1);

            drop(tracked);
            assert_eq!(semaphore.available_permits(), 1);
            assert_eq!(COLD_FILL_FOLLOWER_DISK_PERMITS_FOR_TEST.load(Ordering::Relaxed), 0);
        })
        .await;
    }

    #[test]
    #[serial_test::serial(cold_fill_metrics_gate)]
    fn cold_fill_disk_permit_metrics_obey_gate_and_return_to_zero() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let metrics_was_enabled = rustfs_io_metrics::metrics_enabled();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("metric test runtime must build");
        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                rustfs_io_metrics::set_metrics_enabled(false);
                let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
                scope_cold_fill_disk_permit_owner_for_test(ColdFillDiskPermitOwner::Follower, async {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("metric test permit must be available");
                    let tracked = GetObjectDiskPermit::new(permit);
                    rustfs_io_metrics::set_metrics_enabled(true);
                    drop(tracked);
                })
                .await;
                assert!(
                    snapshotter.snapshot().into_vec().into_iter().all(|(composite, _, _, _)| {
                        !composite.key().name().starts_with("rustfs_object_data_cache_cold_fill_")
                    }),
                    "a permit acquired while metrics were disabled must not record an unmatched decrement"
                );

                scope_cold_fill_disk_permit_owner_for_test(ColdFillDiskPermitOwner::Producer, async {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("metric test permit must be available");
                    let tracked = GetObjectDiskPermit::new(permit);
                    rustfs_io_metrics::set_metrics_enabled(false);
                    drop(tracked);
                })
                .await;
                rustfs_io_metrics::set_metrics_enabled(true);
                scope_cold_fill_disk_permit_owner_for_test(ColdFillDiskPermitOwner::Follower, async {
                    let permit = semaphore.acquire_owned().await.expect("metric test permit must be available");
                    let tracked = GetObjectDiskPermit::new(permit);
                    let _replacement = crate::app::object_data_cache::ColdFillCoordinator::default();
                    drop(tracked);
                })
                .await;
            });
        });

        let values = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(composite, _unit, _description, value)| {
                composite
                    .key()
                    .name()
                    .starts_with("rustfs_object_data_cache_cold_fill_")
                    .then_some((composite.key().name().to_string(), value))
            })
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(values.len(), 2);
        for name in [
            "rustfs_object_data_cache_cold_fill_producer_disk_permits",
            "rustfs_object_data_cache_cold_fill_follower_disk_permits",
        ] {
            let DebugValue::Gauge(value) = values.get(name).unwrap_or_else(|| panic!("missing {name} gauge")) else {
                panic!("{name} must be a gauge");
            };
            assert_eq!(value.into_inner(), 0.0, "{name} must return to zero after permit drop");
        }
        rustfs_io_metrics::set_metrics_enabled(metrics_was_enabled);
    }

    #[tokio::test]
    async fn build_get_object_body_keeps_large_objects_on_streaming_path_without_preread() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 18_i64 * 1024 * 1024 * 1024,
            ..Default::default()
        };

        let _body = DefaultObjectUsecase::build_get_object_body(
            reader,
            &info,
            18_i64 * 1024 * 1024 * 1024,
            "req-large-object",
            None,
            128 * 1024,
            true,
            1,
            None,
            false,
            false,
            None,
            "test-bucket",
            "large-object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await
        .expect("build_get_object_body should succeed for streaming path");

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "large-object response construction should not pre-read object data"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_keeps_large_encrypted_objects_on_streaming_path_without_preread() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 18_i64 * 1024 * 1024 * 1024,
            ..Default::default()
        };

        let _body = DefaultObjectUsecase::build_get_object_body(
            reader,
            &info,
            18_i64 * 1024 * 1024 * 1024,
            "req-large-encrypted-object",
            None,
            128 * 1024,
            true,
            1,
            None,
            false,
            true,
            None,
            "test-bucket",
            "large-encrypted-object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await
        .expect("build_get_object_body should succeed for encrypted streaming path");

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "large encrypted object response construction should not pre-read object data"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_uses_buffered_body_without_reader_preread() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 4,
            ..Default::default()
        };

        let _body = DefaultObjectUsecase::build_get_object_body(
            reader,
            &info,
            4,
            "req-direct-memory-object",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            Some(Bytes::from_static(b"test")),
            "test-bucket",
            "direct-memory-object",
            GetObjectBodyLifecycle::disabled(),
            |_| panic!("a buffered body must not initialize streaming resume state"),
        )
        .await
        .expect("build_get_object_body should consume buffered body");

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "buffered GetObject body must not be read from the fallback reader"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_uses_cached_body_without_reader_preread() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillBufferedOnly,
                max_bytes: 8_388_608,
                // Fill must not depend on the live memory reading (host vs container).
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("fill-enabled cache adapter should initialize");
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "test-bucket",
            object: "cached-object",
            version_id: None,
            etag: "etag",
            size: 5,
            data_dir_u128: None,
            mod_time_unix_nanos: 0,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let fill = adapter.cache().fill_body(&plan, Bytes::from_static(b"hello")).await;

        assert_eq!(fill, rustfs_object_data_cache::ObjectDataCacheFillResult::Inserted);

        let _body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            "req-cached-object",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "cached-object",
            GetObjectBodyLifecycle::disabled(),
            |_| panic!("a cache hit must not initialize streaming resume state"),
        )
        .await
        .expect("cache hit body handoff should succeed");

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "cache hit body handoff must not read from the fallback reader"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_rejects_size_mismatch_fill() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillBufferedOnly,
                max_bytes: 8_388_608,
                // Fill must not depend on the live memory reading (host vs container).
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("fill-enabled cache adapter should initialize");
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "test-bucket",
            object: "cached-object",
            version_id: None,
            etag: "etag",
            size: 5,
            data_dir_u128: None,
            mod_time_unix_nanos: 0,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let fill = adapter.cache().fill_body(&plan, Bytes::from_static(b"oops")).await;

        let _body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            "req-rejects-size-mismatch-fill",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "cached-object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await
        .expect("size-mismatched direct fill should not create a cache hit");
        let lookup_after_mismatch = adapter.lookup_body(&plan).await;

        assert_eq!(fill, rustfs_object_data_cache::ObjectDataCacheFillResult::SkippedSizeMismatch);
        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "size-mismatched rejected fill should construct the fallback stream without pre-reading"
        );
        assert!(
            matches!(lookup_after_mismatch, rustfs_object_data_cache::ObjectDataCacheLookup::Miss),
            "size-mismatched fill must not leave a reusable cache entry"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_fills_from_buffered_body_without_reader_preread() {
        let first_reads = Arc::new(AtomicUsize::new(0));
        let first_reader = ReadProbeReader {
            reads: Arc::clone(&first_reads),
        };
        let second_reads = Arc::new(AtomicUsize::new(0));
        let second_reader = ReadProbeReader {
            reads: Arc::clone(&second_reads),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillBufferedOnly,
                max_bytes: 8_388_608,
                // Fill must not depend on the live memory reading (host vs container).
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("fill-enabled cache adapter should initialize");

        let _first_body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            first_reader,
            &info,
            5,
            "req-cache-fill-first",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            Some(Bytes::from_static(b"hello")),
            false,
            false,
            true,
            "test-bucket",
            "cached-object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await
        .expect("buffered-body handoff should succeed");

        // ODC-15: the fill is detached from the response path, so wait for it to
        // populate the cache before the follow-up GET to keep the hit deterministic.
        wait_for_cache_hit(&adapter, "test-bucket", "cached-object", "etag", 5).await;

        let _second_body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            second_reader,
            &info,
            5,
            "req-cache-fill-second",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "cached-object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await
        .expect("follow-up cache hit should succeed");

        assert_eq!(
            first_reads.load(AtomicOrdering::Relaxed),
            0,
            "buffered-body fill path must not read from the fallback reader"
        );
        assert_eq!(
            second_reads.load(AtomicOrdering::Relaxed),
            0,
            "cache hit after buffered-body fill must not read from the fallback reader"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_skips_buffered_fill_on_size_mismatch() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillBufferedOnly,
                max_bytes: 8_388_608,
                // Fill must not depend on the live memory reading (host vs container).
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("fill-enabled cache adapter should initialize");
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "test-bucket",
            object: "cached-object",
            version_id: None,
            etag: "etag",
            size: 5,
            data_dir_u128: None,
            mod_time_unix_nanos: 0,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });

        let _body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            "req-rejects-buffered-size-mismatch",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            Some(Bytes::from_static(b"oops")),
            false,
            false,
            true,
            "test-bucket",
            "cached-object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await
        .expect("size-mismatched buffered-body handoff should still return a response body");
        let lookup = adapter.lookup_body(&plan).await;

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "buffered-body handoff must not read from the fallback reader"
        );
        assert!(
            matches!(lookup, rustfs_object_data_cache::ObjectDataCacheLookup::Miss),
            "size-mismatched buffered body must not be filled into cache"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_hook_served_records_no_second_lookup() {
        // ODC-16 (backlog#1121): a hook-served GET must record exactly one
        // lookup — the ecstore hook's. The app layer, handed the cache body as
        // buffered_body with cache_hook_served=true, must serve it directly
        // without a second lookup (which would double the hits and hit_bytes).
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillBufferedOnly,
                max_bytes: 8_388_608,
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("fill-enabled cache adapter should initialize");
        let plan = adapter.plan_get(rustfs_object_data_cache::ObjectDataCacheGetRequest {
            bucket: "test-bucket",
            object: "hook-served",
            version_id: None,
            etag: "etag",
            size: 5,
            data_dir_u128: None,
            mod_time_unix_nanos: 0,
            body_variant: rustfs_object_data_cache::ObjectDataCacheBodyVariant::FullObjectPlainV1,
        });
        let hit_body = Bytes::from_static(b"hello");
        assert_eq!(
            adapter.cache().fill_body(&plan, hit_body.clone()).await,
            rustfs_object_data_cache::ObjectDataCacheFillResult::Inserted
        );

        // Simulate the ecstore hook: it performs exactly one lookup after fresh
        // metadata resolution, hits, and hands the body forward as buffered_body.
        assert!(matches!(
            adapter.lookup_body(&plan).await,
            rustfs_object_data_cache::ObjectDataCacheLookup::Hit(_)
        ));
        let lookups_after_hook = adapter.cache().stats().lookups;
        assert_eq!(lookups_after_hook, 1, "the hook performs exactly one lookup");

        let _body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            "req-hook-served",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            Some(hit_body),
            /* cache_hook_served */ true,
            /* cache_hook_probed */ true,
            /* cache_fill_allowed */ true,
            "test-bucket",
            "hook-served",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await
        .expect("hook-served body handoff should succeed");

        assert_eq!(
            adapter.cache().stats().lookups,
            lookups_after_hook,
            "a hook-served GET must not record a second lookup in the app layer"
        );
        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "hook-served body handoff must not read from the fallback reader"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_hook_miss_skips_app_lookup() {
        // ODC-16: when the hook probed and missed, its miss is authoritative
        // (it ran after fresh metadata resolution), so the app layer must not
        // run a second lookup — it only fills from the buffered body.
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillBufferedOnly,
                max_bytes: 8_388_608,
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("fill-enabled cache adapter should initialize");

        let lookups_before = adapter.cache().stats().lookups;
        let _body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            "req-hook-missed",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            Some(Bytes::from_static(b"hello")),
            /* cache_hook_served */ false,
            /* cache_hook_probed */ true,
            /* cache_fill_allowed */ true,
            "test-bucket",
            "hook-missed",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await
        .expect("hook-miss buffered-body handoff should succeed");

        assert_eq!(
            adapter.cache().stats().lookups,
            lookups_before,
            "a hook-probed miss must not trigger an app-layer lookup"
        );
        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "buffered-body handoff must not read from the fallback reader"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_materializes_once_and_hits_later() {
        let first_reads = Arc::new(AtomicUsize::new(0));
        let first_reader = DataProbeReader {
            reads: Arc::clone(&first_reads),
            data: std::io::Cursor::new(b"hello".to_vec()),
        };
        let second_reads = Arc::new(AtomicUsize::new(0));
        let second_reader = ReadProbeReader {
            reads: Arc::clone(&second_reads),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 8_388_608,
                // Fill must not depend on the live memory reading (host vs container).
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("materialize-fill cache adapter should initialize");

        let _first_body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            first_reader,
            &info,
            5,
            "req-materialize-first",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "materialized-object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await
        .expect("materialize-fill handoff should succeed");

        // ODC-15: the fill is detached from the response path, so wait for it to
        // populate the cache before the follow-up GET to keep the hit deterministic.
        wait_for_cache_hit(&adapter, "test-bucket", "materialized-object", "etag", 5).await;

        let _second_body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            second_reader,
            &info,
            5,
            "req-materialize-second",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "materialized-object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await
        .expect("follow-up cache hit should succeed");

        assert_eq!(
            first_reads.load(AtomicOrdering::Relaxed),
            2,
            "materialize-fill path should read the source stream once to data and once for EOF"
        );
        assert_eq!(
            second_reads.load(AtomicOrdering::Relaxed),
            0,
            "cache hit after materialize-fill must not read from the fallback reader"
        );
    }

    // ODC-07: a materialize read that yields more than the declared content
    // length must be a hard error, not a warn-and-serve, matching the
    // direct-memory GET path. The bounded `take` reads one byte past capacity so
    // the over-long stream is detected without buffering it unbounded.
    #[tokio::test]
    async fn build_get_object_body_with_cache_materialize_rejects_length_mismatch() {
        let reads = Arc::new(AtomicUsize::new(0));
        // Declared content length is 5, but the stream yields 6 bytes.
        let reader = DataProbeReader {
            reads: Arc::clone(&reads),
            data: std::io::Cursor::new(b"hello!".to_vec()),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 8_388_608,
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("materialize-fill cache adapter should initialize");

        let result = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            "req-materialize-mismatch",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "mismatch-object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await;

        assert!(
            result.is_err(),
            "an over-long materialize read must be a hard error, not a truncated served body"
        );
    }

    // #1324: a materialize-fill read that ends short of the declared content
    // length (clean EOF at N-1 for a declared N) must hard-fail, matching the
    // over-long case above. Reverting to warn-and-serve would return Ok with a
    // truncated body.
    #[tokio::test]
    async fn build_get_object_body_with_cache_materialize_rejects_short_read() {
        let reads = Arc::new(AtomicUsize::new(0));
        // Declared content length is 5, but the stream only yields 4 bytes.
        let reader = DataProbeReader {
            reads: Arc::clone(&reads),
            data: std::io::Cursor::new(b"hell".to_vec()),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 8_388_608,
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("materialize-fill cache adapter should initialize");

        let result = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            "req-materialize-short",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "short-object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await;

        assert!(
            result.is_err(),
            "a short materialize read must be a hard error, not a truncated served body"
        );
    }

    // #1324: a materialize-fill read that fails after draining K bytes must
    // propagate the read error and must NOT fall back to streaming the same
    // (partially consumed) reader, which would ship a prefix-misaligned body.
    #[tokio::test]
    async fn build_get_object_body_with_cache_materialize_rejects_partial_read_error() {
        let reader = ErrAfterReader {
            data: std::io::Cursor::new(b"hello".to_vec()),
            fail_after: 3,
            emitted: 0,
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 8_388_608,
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("materialize-fill cache adapter should initialize");

        let result = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            "req-materialize-partial",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "partial-read-object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await;

        assert!(
            result.is_err(),
            "a partial-read error during materialization must fail the request, not stream a prefix-misaligned body"
        );
    }

    // #1324: the buffered-body (direct-memory / cache-served) source must also
    // enforce the exact-length contract. A buffered body shorter than the
    // declared content length is a hard error before headers.
    #[tokio::test]
    async fn build_get_object_body_rejects_short_buffered_body() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 5,
            ..Default::default()
        };

        let result = DefaultObjectUsecase::build_get_object_body(
            reader,
            &info,
            5,
            "req-short-buffered-object",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            // Declared length 5 but only 4 buffered bytes.
            Some(Bytes::from_static(b"hell")),
            "test-bucket",
            "short-buffered-object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await;

        assert!(result.is_err(), "a buffered body shorter than the declared content length must hard-fail");
        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "the mismatch must be caught without touching the fallback reader"
        );
    }

    // #1324 compatibility boundary: a legacy/backfilled object whose decoded
    // bytes exactly equal its declared content length must still serve cleanly.
    // The strict contract keys off actual-vs-declared equality only, so it never
    // flips a legitimate exact-length object into a hard failure — it only
    // rejects genuine short/over-long/errored reads.
    #[tokio::test]
    async fn build_get_object_body_serves_exact_length_buffered_body() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 5,
            ..Default::default()
        };

        let _body = DefaultObjectUsecase::build_get_object_body(
            reader,
            &info,
            5,
            "req-exact-buffered-object",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            Some(Bytes::from_static(b"hello")),
            "test-bucket",
            "exact-buffered-object",
            GetObjectBodyLifecycle::disabled(),
            |_| panic!("an exact-length buffered body must not initialize streaming resume state"),
        )
        .await
        .expect("an exact-length buffered body must serve without error");

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "an exact-length buffered body must not read from the fallback reader"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_with_cache_skips_materialize_when_too_large_for_cache() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = DataProbeReader {
            reads: Arc::clone(&reads),
            data: std::io::Cursor::new(b"hello".to_vec()),
        };
        let info = ObjectInfo {
            size: 5,
            etag: Some("etag".to_string()),
            ..Default::default()
        };
        let adapter =
            crate::app::object_data_cache::ObjectDataCacheAdapter::new(rustfs_object_data_cache::ObjectDataCacheConfig {
                mode: rustfs_object_data_cache::ObjectDataCacheMode::FillMaterializeEnabled,
                max_bytes: 8_388_608,
                max_entry_bytes: 4,
                // Fill must not depend on the live memory reading (host vs container).
                min_free_memory_percent: 0,
                ..rustfs_object_data_cache::ObjectDataCacheConfig::default()
            })
            .expect("materialize-fill cache adapter should initialize");

        let _body = DefaultObjectUsecase::build_get_object_body_with_cache(
            &adapter,
            reader,
            &info,
            5,
            "req-materialize-too-large",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            false,
            false,
            true,
            "test-bucket",
            "too-large-object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await
        .expect("too-large cache candidate should use streaming fallback");

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "too-large materialize-fill candidate must not pre-read the fallback reader"
        );
    }

    #[tokio::test]
    async fn build_get_object_body_keeps_small_plain_objects_on_streaming_path_by_default() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = ReadProbeReader {
            reads: Arc::clone(&reads),
        };
        let info = ObjectInfo {
            size: 4,
            ..Default::default()
        };

        let _body = DefaultObjectUsecase::build_get_object_body(
            reader,
            &info,
            4,
            "req-small-plain-object",
            None,
            128 * 1024,
            false,
            1,
            None,
            false,
            false,
            None,
            "test-bucket",
            "small-plain-object",
            GetObjectBodyLifecycle::disabled(),
            |_| None,
        )
        .await
        .expect("build_get_object_body should keep small plain object on streaming path");

        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            0,
            "default GetObject response construction should not pre-read small plain object data"
        );
    }

    #[test]
    fn select_stream_buffer_strategy_expands_large_sequential_gets() {
        let (buffer_size, strategy) =
            DefaultObjectUsecase::select_stream_buffer_strategy(2_i64 * 1024 * 1024 * 1024, 2 * MI_B, true, false);

        assert_eq!(strategy, GetObjectStreamStrategy::LargeSequentialReadahead);
        assert_eq!(buffer_size, 4 * MI_B);
    }

    #[test]
    fn select_stream_buffer_strategy_keeps_ranges_and_small_gets_standard() {
        let (range_buffer_size, range_strategy) =
            DefaultObjectUsecase::select_stream_buffer_strategy(2_i64 * 1024 * 1024 * 1024, 2 * MI_B, true, true);
        assert_eq!(range_strategy, GetObjectStreamStrategy::Standard);
        assert_eq!(range_buffer_size, 2 * MI_B);

        let (small_buffer_size, small_strategy) =
            DefaultObjectUsecase::select_stream_buffer_strategy(64 * 1024 * 1024, 512 * 1024, true, false);
        assert_eq!(small_strategy, GetObjectStreamStrategy::Standard);
        assert_eq!(small_buffer_size, 512 * 1024);
    }

    #[test]
    fn tune_reader_stream_buffer_size_raises_large_standard_streams_only() {
        assert_eq!(
            tune_reader_stream_buffer_size(128 * 1024, 10 * MI_B as i64, GetObjectStreamStrategy::Standard),
            LARGE_BODY_READER_STREAM_BUFFER_FLOOR_BYTES
        );
        assert_eq!(
            tune_reader_stream_buffer_size(512 * 1024, 10 * MI_B as i64, GetObjectStreamStrategy::Standard),
            LARGE_BODY_READER_STREAM_BUFFER_FLOOR_BYTES
        );
        assert_eq!(
            tune_reader_stream_buffer_size(2 * MI_B, 10 * MI_B as i64, GetObjectStreamStrategy::Standard),
            2 * MI_B
        );
        assert_eq!(
            tune_reader_stream_buffer_size(128 * 1024, MI_B as i64, GetObjectStreamStrategy::Standard),
            MID_BODY_READER_STREAM_BUFFER_FLOOR_BYTES
        );
        assert_eq!(
            tune_reader_stream_buffer_size(256 * 1024, 2 * MI_B as i64, GetObjectStreamStrategy::Standard),
            MID_BODY_READER_STREAM_BUFFER_FLOOR_BYTES
        );
        assert_eq!(
            tune_reader_stream_buffer_size(128 * 1024, 10 * MI_B as i64, GetObjectStreamStrategy::LargeSequentialReadahead),
            128 * 1024
        );
    }

    #[test]
    fn resolve_reader_stream_buffer_size_keeps_selected_default() {
        let (buffer_size, source) = resolve_reader_stream_buffer_size(128 * 1024, None);

        assert_eq!(buffer_size, 128 * 1024);
        assert_eq!(source, GET_READER_STREAM_BUFFER_SOURCE_SELECTED);
    }

    #[test]
    fn resolve_reader_stream_buffer_size_applies_positive_override() {
        let (buffer_size, source) = resolve_reader_stream_buffer_size(128 * 1024, Some(MI_B));

        assert_eq!(buffer_size, MI_B);
        assert_eq!(source, GET_READER_STREAM_BUFFER_SOURCE_ENV_OVERRIDE);
    }

    #[test]
    fn resolve_reader_stream_buffer_size_ignores_zero_override() {
        let (buffer_size, source) = resolve_reader_stream_buffer_size(128 * 1024, Some(0));

        assert_eq!(buffer_size, 128 * 1024);
        assert_eq!(source, GET_READER_STREAM_BUFFER_SOURCE_SELECTED);
    }

    #[tokio::test]
    async fn get_object_reader_stream_tracks_remaining_length() {
        let mut stream = GetObjectReaderStream::new(
            std::io::Cursor::new(b"hello".to_vec()),
            2,
            5,
            GetObjectStreamStrategy::Standard.as_str(),
            GET_READER_STREAM_BUFFER_SOURCE_SELECTED,
        );

        assert_eq!(stream.remaining_length().exact(), Some(5));

        let first = stream
            .next()
            .await
            .expect("reader stream should emit first chunk")
            .expect("first chunk should read");

        assert_eq!(first.as_ref(), b"he");
        assert_eq!(stream.remaining_length().exact(), Some(3));
    }

    #[tokio::test]
    async fn get_object_reader_stream_truncates_to_expected_length() {
        let stream = GetObjectReaderStream::new(
            std::io::Cursor::new(b"hello!".to_vec()),
            64,
            5,
            GetObjectStreamStrategy::Standard.as_str(),
            GET_READER_STREAM_BUFFER_SOURCE_SELECTED,
        );

        let chunks = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("reader stream should read");
        let body = chunks.into_iter().fold(Vec::new(), |mut acc, chunk| {
            acc.extend_from_slice(&chunk);
            acc
        });

        assert_eq!(body, b"hello");
    }

    #[tokio::test]
    async fn get_object_reader_stream_bounds_read_buffer_to_remaining() {
        struct RecordingReader {
            data: &'static [u8],
            pos: usize,
            observed_remaining: Arc<Mutex<Vec<usize>>>,
        }

        impl AsyncRead for RecordingReader {
            fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
                let requested = buf.remaining();
                self.observed_remaining
                    .lock()
                    .expect("observed buffer sizes should not poison")
                    .push(requested);
                let available = self.data.len().saturating_sub(self.pos);
                let to_copy = requested.min(available);
                if to_copy > 0 {
                    let end = self.pos + to_copy;
                    buf.put_slice(&self.data[self.pos..end]);
                    self.pos = end;
                }
                Poll::Ready(Ok(()))
            }
        }

        let observed_remaining = Arc::new(Mutex::new(Vec::new()));
        let stream = GetObjectReaderStream::new(
            RecordingReader {
                data: b"hello",
                pos: 0,
                observed_remaining: Arc::clone(&observed_remaining),
            },
            64,
            5,
            GetObjectStreamStrategy::Standard.as_str(),
            GET_READER_STREAM_BUFFER_SOURCE_SELECTED,
        );

        let chunks = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("reader stream should read exact payload");
        assert_eq!(chunks, vec![Bytes::from_static(b"hello")]);
        assert_eq!(
            *observed_remaining.lock().expect("observed buffer sizes should not poison"),
            vec![5],
            "stream should not ask the reader for more bytes than the response has left"
        );
    }

    #[tokio::test]
    async fn get_object_reader_stream_bounds_multi_chunk_final_read() {
        let stream = GetObjectReaderStream::new(
            std::io::Cursor::new(vec![b'a'; 66]),
            64,
            65,
            GetObjectStreamStrategy::Standard.as_str(),
            GET_READER_STREAM_BUFFER_SOURCE_SELECTED,
        );

        let chunks = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("reader stream should ignore bytes past declared length");
        let chunk_lengths = chunks.iter().map(Bytes::len).collect::<Vec<_>>();
        let body = chunks.into_iter().fold(Vec::new(), |mut acc, chunk| {
            acc.extend_from_slice(&chunk);
            acc
        });

        assert_eq!(chunk_lengths, vec![64, 1]);
        assert_eq!(body, vec![b'a'; 65]);
    }

    // Serial with the capture test below: both drive the same short-EOF log
    // callsite, and `tracing` caches callsite interest process-wide. Running
    // this one concurrently on a thread with no subscriber re-caches that
    // callsite as "never interested" and blinds the capture.
    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_reader_stream_errors_on_short_eof() {
        let stream = GetObjectReaderStream::new(
            std::io::Cursor::new(b"he".to_vec()),
            64,
            5,
            GetObjectStreamStrategy::Standard.as_str(),
            GET_READER_STREAM_BUFFER_SOURCE_SELECTED,
        );

        let err = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect_err("short reader should fail the streaming body");

        assert_eq!(
            err.downcast_ref::<std::io::Error>().map(std::io::Error::kind),
            Some(std::io::ErrorKind::UnexpectedEof)
        );
    }

    /// Collects the structured fields of every event emitted while installed,
    /// so a test can assert what an operator would actually read in the log
    /// rather than only that an error value was returned.
    type CapturedFieldMap = std::collections::HashMap<String, String>;

    type CapturedEventLog = Arc<Mutex<Vec<CapturedFieldMap>>>;

    struct CapturedEvents(CapturedEventLog);

    struct CapturedFields(CapturedFieldMap);

    impl tracing::field::Visit for CapturedFields {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().to_string(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapturedEvents {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
            let mut fields = CapturedFields(CapturedFieldMap::new());
            event.record(&mut fields);
            self.0.lock().expect("captured events should not poison").push(fields.0);
        }
    }

    fn capture_events() -> (CapturedEventLog, tracing::subscriber::DefaultGuard) {
        use tracing_subscriber::{Registry, prelude::*};

        let captured = Arc::new(Mutex::new(Vec::new()));
        let subscriber = Registry::default().with(CapturedEvents(Arc::clone(&captured)));
        let guard = tracing::subscriber::set_default(subscriber);
        // `tracing` caches per-callsite interest process-wide, so a subscriber
        // installed by a test running in parallel can leave the log sites below
        // cached as "never interested" and this capture would silently see
        // nothing. Force the callsites to re-ask the subscriber we just
        // installed.
        tracing::callsite::rebuild_interest_cache();
        (captured, guard)
    }

    fn find_stream_body_event(captured: &CapturedEventLog, state: &str) -> CapturedFieldMap {
        let events = captured.lock().expect("captured events should not poison");
        events
            .iter()
            .find(|fields| fields.get("state").is_some_and(|value| value == state))
            .unwrap_or_else(|| {
                panic!(
                    "a `{state}` streaming body failure must be logged, not only counted in a metric. \
                     Captured {} event(s): {:?}",
                    events.len(),
                    events
                )
            })
            .clone()
    }

    /// rustfs#4784: a GET body that ends short of its committed Content-Length
    /// is the fault that breaks every downstream copier (replication, site
    /// replication, `rclone sync`), yet this layer only fed a metric counter —
    /// its log line was compiled out unless the `tracing-chunk-debug` feature
    /// was on, so operators saw nothing on the source side.
    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_reader_stream_short_eof_names_the_object() {
        let (captured, _guard) = capture_events();

        let stream = GetObjectReaderStream::new(
            std::io::Cursor::new(b"he".to_vec()),
            64,
            5,
            GetObjectStreamStrategy::Standard.as_str(),
            GET_READER_STREAM_BUFFER_SOURCE_SELECTED,
        )
        .with_diagnostics("restic-paperless", "index/41b5a4c2344edb90", "req-reader-stream-short-eof");

        stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect_err("short reader should fail the streaming body");

        let event = find_stream_body_event(&captured, "reader_stream_short_eof");
        assert_eq!(event.get("bucket").map(String::as_str), Some("restic-paperless"));
        assert_eq!(event.get("object").map(String::as_str), Some("index/41b5a4c2344edb90"));
        assert_eq!(event.get("request_id").map(String::as_str), Some("req-reader-stream-short-eof"));
        assert_eq!(event.get("expected").map(String::as_str), Some("5"));
        assert_eq!(event.get("emitted").map(String::as_str), Some("2"));
        assert_eq!(event.get("remaining").map(String::as_str), Some("3"));
    }

    /// The inner reader already logged mid-stream failures, but only under a
    /// request_id — which cannot be resolved back to an object once the request
    /// is gone. Without the identity the report in #4784 was unactionable.
    #[tokio::test]
    #[serial_test::serial]
    async fn get_object_streaming_reader_short_eof_names_the_object() {
        use tokio::io::AsyncReadExt;

        let (captured, _guard) = capture_events();

        let mut reader = GetObjectStreamingReader::new(
            std::io::Cursor::new(b"short".to_vec()),
            "restic-paperless",
            "index/41b5a4c2344edb90",
            "req-streaming-short-eof",
            None,
            10,
            Duration::ZERO,
            GetObjectBodyLifecycle::tracked(GetObjectGuard::new()),
            None,
        );

        let mut out = Vec::new();
        reader
            .read_to_end(&mut out)
            .await
            .expect_err("short body under a larger Content-Length must fail the stream");

        let event = find_stream_body_event(&captured, "short_eof");
        assert_eq!(event.get("bucket").map(String::as_str), Some("restic-paperless"));
        assert_eq!(event.get("object").map(String::as_str), Some("index/41b5a4c2344edb90"));
        assert_eq!(event.get("request_id").map(String::as_str), Some("req-streaming-short-eof"));
    }

    #[test]
    fn get_object_stream_failure_labels_are_low_cardinality() {
        assert_eq!(get_object_stream_failure_reason("short_eof"), GET_STREAMING_BODY_FAILURE_REASON_SHORT_EOF);
        assert_eq!(
            get_object_stream_failure_reason("timeout"),
            GET_STREAMING_BODY_FAILURE_REASON_READER_ERROR
        );
        assert_eq!(
            get_object_stream_size_bucket(4 * 1024 * 1024),
            rustfs_io_metrics::GET_OBJECT_SIZE_BUCKET_GT_1_MIB
        );
    }

    #[tokio::test]
    async fn disk_read_permit_reader_releases_permit_at_eof() {
        use tokio::io::AsyncReadExt;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await.expect("acquire permit");
        assert_eq!(semaphore.available_permits(), 0);

        let mut reader = DiskReadPermitReader::new(std::io::Cursor::new(b"hello".to_vec()), permit.into());
        let mut body = Vec::new();
        reader.read_to_end(&mut body).await.expect("read body");
        assert_eq!(body, b"hello");

        // The reader is still alive (client hasn't dropped the body), but EOF
        // was observed, so the permit must already be back in the semaphore.
        assert_eq!(semaphore.available_permits(), 1);
        drop(reader);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn build_get_object_output_context_returns_standard_headers() {
        let mut metadata = HashMap::new();
        metadata.insert("cache-control".to_string(), "public, max-age=259200".to_string());
        metadata.insert("content-disposition".to_string(), "attachment; filename=\"demo.png\"".to_string());

        let info = ObjectInfo {
            bucket: "test-bucket".to_string(),
            name: "path/raw".to_string(),
            user_defined: Arc::new(metadata),
            ..Default::default()
        };

        let input = GetObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("path/raw".to_string())
            .build()
            .unwrap();
        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();
        let queue_status = concurrency::IoQueueStatus::default();

        let context = usecase
            .build_get_object_output_context(
                &req,
                get_concurrency_manager(),
                "test-bucket",
                "path/raw",
                info.clone(),
                Some(info),
                wrap_reader(tokio::io::empty()),
                Some(Bytes::new()),
                false,
                false,
                true,
                None,
                None,
                None,
                0,
                None,
                "req-output-content-disposition",
                None,
                None,
                None,
                None,
                false,
                Duration::ZERO,
                0.0,
                &queue_status,
                1,
                None,
                false,
                GetObjectBodyLifecycle::disabled(),
                |_| panic!("a buffered output must not initialize streaming resume state"),
            )
            .await
            .expect("get object output context");

        assert_eq!(context.output.cache_control.as_deref(), Some("public, max-age=259200"));
        assert_eq!(context.output.content_disposition.as_deref(), Some("attachment; filename=\"demo.png\""));
        assert!(
            !context
                .output
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata.contains_key("cache-control"))
        );
        assert!(
            !context
                .output
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata.contains_key("content-disposition"))
        );
    }

    #[tokio::test]
    async fn execute_get_object_rejects_zero_part_number() {
        let input = GetObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .part_number(Some(0))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_get_object(req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn parse_get_object_part_number_rejects_above_s3_max() {
        let err = parse_part_number_i32_to_usize(Some(10001), "GET").expect_err("partNumber above S3 max must fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("GET: partNumber must be between 1 and 10000"));
    }

    #[test]
    fn validate_get_object_part_number_rejects_missing_part() {
        let info = ObjectInfo {
            parts: Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
                number: 1,
                ..Default::default()
            }]),
            ..Default::default()
        };

        let err =
            DefaultObjectUsecase::validate_get_object_part_number(Some(2), &info).expect_err("missing requested part must fail");

        assert_eq!(err.code(), &S3ErrorCode::InvalidPart);
        assert!(DefaultObjectUsecase::validate_get_object_part_number(Some(1), &info).is_ok());
    }

    #[test]
    fn cold_fill_conditions_fail_before_phase_probe_advances() {
        fn run_phase_probe(headers: &HeaderMap, info: &ObjectInfo) -> (S3Result<()>, [usize; 3]) {
            let coordination = AtomicUsize::new(0);
            let permit = AtomicUsize::new(0);
            let reader = AtomicUsize::new(0);
            let result = DefaultObjectUsecase::validate_get_object_before_cold_fill(headers, None, info);
            if result.is_ok() {
                coordination.fetch_add(1, AtomicOrdering::Relaxed);
                permit.fetch_add(1, AtomicOrdering::Relaxed);
                reader.fetch_add(1, AtomicOrdering::Relaxed);
            }
            (
                result,
                [
                    coordination.load(AtomicOrdering::Relaxed),
                    permit.load(AtomicOrdering::Relaxed),
                    reader.load(AtomicOrdering::Relaxed),
                ],
            )
        }

        let info = ObjectInfo {
            etag: Some("phase-etag".to_string()),
            parts: Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
                number: 1,
                ..Default::default()
            }]),
            ..Default::default()
        };

        let mut not_modified = HeaderMap::new();
        not_modified.insert(http::header::IF_NONE_MATCH, HeaderValue::from_static("\"phase-etag\""));
        let (result, phases) = run_phase_probe(&not_modified, &info);
        assert_eq!(result.expect_err("matching If-None-Match must reject").code(), &S3ErrorCode::NotModified);
        assert_eq!(phases, [0, 0, 0]);

        let mut precondition_failed = HeaderMap::new();
        precondition_failed.insert(http::header::IF_MATCH, HeaderValue::from_static("\"other-etag\""));
        let (result, phases) = run_phase_probe(&precondition_failed, &info);
        assert_eq!(
            result.expect_err("mismatched If-Match must reject").code(),
            &S3ErrorCode::PreconditionFailed
        );
        assert_eq!(phases, [0, 0, 0]);
    }

    #[tokio::test]
    async fn execute_get_object_rejects_range_with_part_number() {
        let input = GetObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .part_number(Some(1))
            .range(Some(Range::Int { first: 0, last: Some(1) }))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = Box::pin(usecase.execute_get_object(req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_get_object_attributes_returns_internal_error_when_store_uninitialized() {
        let input = GetObjectAttributesInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_get_object_attributes(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn object_attributes_requested_with_single_value() {
        let object_attributes = vec![ObjectAttributes::from_static(ObjectAttributes::ETAG)];

        assert!(object_attributes_requested(&object_attributes, ObjectAttributes::ETAG));
        assert!(!object_attributes_requested(&object_attributes, ObjectAttributes::OBJECT_SIZE));
    }

    #[test]
    fn object_attributes_requested_with_comma_separated_values() {
        let object_attributes = vec![
            ObjectAttributes::from_static("ObjectParts,etag"),
            ObjectAttributes::from_static("StorageClass"),
        ];

        assert!(object_attributes_requested(&object_attributes, ObjectAttributes::OBJECT_PARTS));
        assert!(object_attributes_requested(&object_attributes, ObjectAttributes::ETAG));
        assert!(!object_attributes_requested(&object_attributes, ObjectAttributes::OBJECT_SIZE));
    }

    #[test]
    fn object_attributes_requested_with_quotes_and_spaces() {
        let object_attributes = vec![ObjectAttributes::from_static("'ObjectSize', \"Checksum\" , \"Etag\"")];

        assert!(object_attributes_requested(&object_attributes, ObjectAttributes::OBJECT_SIZE));
        assert!(object_attributes_requested(&object_attributes, ObjectAttributes::CHECKSUM));
        assert!(object_attributes_requested(&object_attributes, ObjectAttributes::ETAG));
    }

    #[test]
    fn object_attributes_requested_returns_false_for_missing_name() {
        let object_attributes = vec![ObjectAttributes::from_static("Checksum")];

        assert!(!object_attributes_requested(&object_attributes, ObjectAttributes::OBJECT_SIZE));
    }
}
