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

//! PutObject write path: body admission, eager commit, zero-copy tuning.

use super::*;

use crate::auth::{RUSTFS_MAX_CONTENT_LENGTH_QUERY, VerifiedPresignedRequest, parse_presigned_put_max_content_length};
use crate::error::UploadLimitExceeded;

const DEFAULT_PUT_LARGE_CONCURRENCY_TUNING_MIN_SIZE_BYTES: i64 = 32 * 1024 * 1024;

const ENV_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES: &str = "RUSTFS_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES";

/// Maximum body size materialized by the ordinary eager PUT path.
///
/// Bodies above this boundary stay streaming so a 1 MiB request does not
/// reserve a full request-sized buffer while the EC writer is consuming it.
/// The environment override keeps the boundary reversible for workload A/B
/// tests and for deployments whose measured workload favors eager ingestion.
const ENV_SMALL_EAGER_PUT_MAX_SIZE_BYTES: &str = "RUSTFS_SMALL_EAGER_PUT_MAX_SIZE_BYTES";

const DEFAULT_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES: usize = 16 * 1024 * 1024;

const DEFAULT_SMALL_EAGER_PUT_MAX_SIZE_BYTES: usize = 512 * 1024;

/// Keep the eager buffer bounded as concurrent PUTs rise. The thresholds are
/// deliberately conservative: tiny objects remain eager, while bursty
/// traffic sheds larger per-request allocations before memory pressure builds.
const SMALL_EAGER_CONCURRENCY_SOFT_LIMIT: usize = 64;
const SMALL_EAGER_CONCURRENCY_HARD_LIMIT: usize = 128;
const MIN_DYNAMIC_SMALL_EAGER_PUT_MAX_SIZE_BYTES: i64 = 128 * 1024;

// Keep bounded conditional writes eager through the historical 1 MiB boundary
// so the old object remains readable until the replacement body is complete.
const CONDITIONAL_SMALL_EAGER_PUT_MAX_SIZE_BYTES: i64 = 1024 * 1024;

const PUT_EAGER_STATUS_ELIGIBLE: &str = "eligible";

const PUT_EAGER_STATUS_EXTRACT: &str = "extract";

const PUT_EAGER_STATUS_COMPRESSED: &str = "compressed";

const PUT_EAGER_STATUS_ENCRYPTED: &str = "encrypted";

const PUT_EAGER_STATUS_INVALID_SIZE: &str = "invalid_size";

const PUT_EAGER_STATUS_ABOVE_EAGER_MAX: &str = "above_eager_max";

const PUT_EAGER_STATUS_ZERO_COPY_INELIGIBLE: &str = "zero_copy_ineligible";

const PUT_EAGER_STATUS_AWS_CHUNKED_MISSING_DECODED_LENGTH: &str = "aws_chunked_missing_decoded_length";

static CACHED_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

static CACHED_SMALL_EAGER_PUT_MAX_SIZE_BYTES: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

const EVENT_PUT_OBJECT_STORE_INFLIGHT_SLOW: &str = "put_object_store_inflight_slow";

const EVENT_PUT_OBJECT_STORE_RETURNED: &str = "put_object_store_returned";

const EVENT_PUT_OBJECT_COMMIT_OWNER_DEADLINE: &str = "put_object_commit_owner_deadline";

const EVENT_PUT_OBJECT_BODY_READ_STALLED: &str = "put_object_body_read_stalled";

const PUT_OBJECT_STORE_WARN_THRESHOLD: Duration = Duration::from_secs(5);

// Eager PUT bodies are fully materialized before the storage owner starts. On
// request cancellation, keep the commit/publication tail alive briefly, then
// request pre-commit rollback and await cleanup so its write-health guard is
// reaped without abandoning staged shards.
const EAGER_PUT_COMMIT_CANCELLATION_GRACE: Duration =
    Duration::from_secs(rustfs_config::DEFAULT_DRIVE_MAX_TIMEOUT_DURATION_SECS * 4);

/// Resolve the authoritative object length that bucket-quota admission (and downstream sizing) must use.
///
/// `Content-Encoding: aws-chunked` alone only *declares* the encoding; whether the body actually arrived chunk-framed is signalled by a `STREAMING-*` `x-amz-content-sha256`, and the S3 auth layer both requires `x-amz-decoded-content-length` for those requests and hands the body down already de-framed. So when a decoded length is present it is authoritative (the wire `Content-Length` counts chunk framing and would overcount); a framed body without a decoded length is rejected rather than falling back to the framed wire length. A declared-only aws-chunked request (issue #1857 clients) carries an unframed body, so its wire `Content-Length` is the authoritative size, exactly as for a plain PUT. A negative or otherwise unknown length is rejected so it can never be reinterpreted as an enormous unsigned size downstream.
fn resolve_put_object_authoritative_size(headers: &HeaderMap, content_length: Option<i64>) -> S3Result<i64> {
    let decoded_content_length = decoded_content_length_from_headers(headers)?;
    let aws_chunked = request_uses_aws_chunked(headers) || request_body_is_aws_chunked_framed(headers);
    let size = match (aws_chunked, decoded_content_length, content_length) {
        (true, Some(decoded), _) => decoded,
        // Declared aws-chunked without a streaming payload: the body is not framed (the auth
        // layer only de-frames STREAMING-* payloads, which always carry a decoded length), so
        // the wire Content-Length is the real object size.
        (true, None, Some(raw)) if !request_body_is_aws_chunked_framed(headers) => raw,
        (true, None, _) => return Err(s3_error!(UnexpectedContent)),
        (false, _, Some(raw)) => raw,
        (false, Some(decoded), None) => decoded,
        (false, None, None) => return Err(s3_error!(UnexpectedContent)),
    };

    if size < 0 {
        return Err(s3_error!(UnexpectedContent));
    }

    Ok(size)
}

/// Resolve the S3 request-body inter-chunk read timeout from the environment.
///
/// Returns `Duration::ZERO` when disabled (`RUSTFS_HTTP_REQUEST_BODY_READ_TIMEOUT=0`),
/// in which case [`guard_put_object_body_read_timeout`] passes the body through
/// untouched.
pub(crate) fn put_object_body_read_timeout() -> Duration {
    Duration::from_secs(rustfs_utils::get_env_u64(
        rustfs_config::ENV_HTTP_REQUEST_BODY_READ_TIMEOUT,
        rustfs_config::DEFAULT_HTTP_REQUEST_BODY_READ_TIMEOUT,
    ))
}

/// A [`ByteStream`] decorator that aborts a request body whose peer stops
/// sending bytes without closing the connection.
///
/// A well-behaved short body ends with EOF and is rejected promptly by the
/// eager/streaming readers. The failure this guards against is different: a
/// reverse proxy or CDN forwards a *partial* body and then goes silent while
/// holding the connection open, so the inner stream neither yields more bytes
/// nor reports EOF. Without a bound, RustFS would wait forever for bytes that
/// never arrive and the client eventually sees a hang/abort with no server-side
/// explanation (issue #3076).
///
/// The timer resets on every chunk, so slow-but-progressing uploads are not
/// penalized; it only fires after `timeout` of complete silence. On timeout the
/// stall is logged with the received/expected byte counts and the read fails
/// with an `ErrorKind::TimedOut` error instead of hanging.
///
/// `remaining_length` and `size_hint` are forwarded from the inner stream so
/// wrapping is transparent to length/content handling downstream.
struct RequestBodyReadTimeout {
    inner: DynByteStream,
    timeout: Duration,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    received: u64,
    expected: Option<u64>,
    bucket: String,
    key: String,
    request_id: String,
    timed_out: bool,
}

/// Enforces a maximum size on the decoded request entity while preserving the
/// streaming behavior of the underlying S3 body.
struct MaxContentLengthStream {
    inner: StreamingBlob,
    limit: u64,
    received: u64,
    exceeded: bool,
}

impl Stream for MaxContentLengthStream {
    type Item = Result<Bytes, StdError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        if this.exceeded {
            return Poll::Ready(None);
        }

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                let chunk_len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
                let exceeds = this.received > this.limit || chunk_len > this.limit.saturating_sub(this.received);
                if exceeds {
                    this.exceeded = true;
                    return Poll::Ready(Some(Err(Box::new(UploadLimitExceeded { limit: this.limit }))));
                }

                this.received = this.received.saturating_add(chunk_len);
                Poll::Ready(Some(Ok(chunk)))
            }
            other => other,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = usize::try_from(self.limit.saturating_sub(self.received)).unwrap_or(usize::MAX);
        let (lower, upper) = self.inner.size_hint();
        (lower.min(remaining), upper.map(|upper| upper.min(remaining)))
    }
}

impl ByteStream for MaxContentLengthStream {
    fn remaining_length(&self) -> RemainingLength {
        let remaining = usize::try_from(self.limit.saturating_sub(self.received)).unwrap_or(usize::MAX);
        let inner = self.inner.remaining_length();
        inner
            .exact()
            .map(|exact| RemainingLength::new_exact(exact.min(remaining)))
            .unwrap_or_else(RemainingLength::unknown)
    }
}

impl Stream for RequestBodyReadTimeout {
    type Item = Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Once we have surfaced a stall error, treat the stream as terminated so
        // we never poll the abandoned inner stream again.
        if this.timed_out {
            return Poll::Ready(None);
        }

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.timer = None;
                this.received = this.received.saturating_add(chunk.len() as u64);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(other) => {
                this.timer = None;
                Poll::Ready(other)
            }
            Poll::Pending => {
                if this.timeout.is_zero() {
                    return Poll::Pending;
                }

                if this.timer.is_none() {
                    this.timer = Some(Box::pin(tokio::time::sleep(this.timeout)));
                }

                if let Some(timer) = this.timer.as_mut()
                    && std::future::Future::poll(timer.as_mut(), cx).is_ready()
                {
                    this.timer = None;
                    this.timed_out = true;
                    let expected_display = this.expected.map(|v| v.to_string()).unwrap_or_else(|| "unknown".to_string());
                    warn!(
                        target: "rustfs::app::object_usecase",
                        event = EVENT_PUT_OBJECT_BODY_READ_STALLED,
                        component = LOG_COMPONENT_APP,
                        subsystem = LOG_SUBSYSTEM_OBJECT,
                        request_id = %this.request_id,
                        bucket = %this.bucket,
                        key = %this.key,
                        received_bytes = this.received,
                        expected_bytes = %expected_display,
                        timeout_secs = this.timeout.as_secs(),
                        state = "stall_timeout",
                        "PutObject request body read stalled; aborting. A proxy/CDN likely forwarded a partial body without closing the connection."
                    );
                    return Poll::Ready(Some(Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!(
                            "request body read stalled: received {} of {} bytes, no data for {}s",
                            this.received,
                            expected_display,
                            this.timeout.as_secs()
                        ),
                    )) as StdError)));
                }

                Poll::Pending
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ByteStream for RequestBodyReadTimeout {
    fn remaining_length(&self) -> RemainingLength {
        self.inner.remaining_length()
    }
}

/// Wrap an incoming request body with [`RequestBodyReadTimeout`] unless the
/// feature is disabled (`timeout == 0`), in which case the body is returned
/// untouched. `remaining_length` is preserved via [`StreamingBlob::new`].
pub(crate) fn guard_put_object_body_read_timeout(
    body: StreamingBlob,
    bucket: &str,
    key: &str,
    request_id: &str,
    expected: Option<i64>,
    timeout: Duration,
) -> StreamingBlob {
    if timeout.is_zero() {
        return body;
    }

    StreamingBlob::new(RequestBodyReadTimeout {
        inner: body.into(),
        timeout,
        timer: None,
        received: 0,
        expected: expected.and_then(|v| u64::try_from(v).ok()),
        bucket: bucket.to_string(),
        key: key.to_string(),
        request_id: request_id.to_string(),
        timed_out: false,
    })
}

pub(super) struct PooledBufferReader {
    buffer: PooledBuffer,
    len: usize,
    pos: usize,
}

impl PooledBufferReader {
    pub(super) fn new(buffer: PooledBuffer, len: usize) -> Self {
        Self { buffer, len, pos: 0 }
    }
}

impl AsyncRead for PooledBufferReader {
    fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if self.pos >= self.len {
            return Poll::Ready(Ok(()));
        }

        let remaining = self.len - self.pos;
        let to_read = remaining.min(buf.remaining());
        buf.put_slice(&self.buffer[self.pos..self.pos + to_read]);
        self.pos += to_read;

        Poll::Ready(Ok(()))
    }
}

struct ChunkedBytesReader {
    chunks: Vec<Bytes>,
    chunk_index: usize,
    chunk_offset: usize,
}

impl ChunkedBytesReader {
    fn new(chunks: Vec<Bytes>) -> Self {
        Self {
            chunks,
            chunk_index: 0,
            chunk_offset: 0,
        }
    }
}

impl AsyncRead for ChunkedBytesReader {
    fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        while self.chunk_index < self.chunks.len() {
            let chunk = &self.chunks[self.chunk_index];
            if self.chunk_offset >= chunk.len() {
                self.chunk_index += 1;
                self.chunk_offset = 0;
                continue;
            }

            let remaining = &chunk[self.chunk_offset..];
            let to_read = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..to_read]);
            self.chunk_offset += to_read;
            return Poll::Ready(Ok(()));
        }

        Poll::Ready(Ok(()))
    }
}

/// Determine if zero-copy write should be used for this PutObject operation.
///
/// Zero-copy is beneficial for large objects without encryption or compression.
///
/// # Arguments
///
/// * `size` - Object size in bytes
/// * `headers` - HTTP headers (to check for encryption/compression)
///
/// # Returns
///
/// `true` if zero-copy should be used, `false` otherwise
fn should_use_zero_copy(size: i64, headers: &HeaderMap) -> bool {
    // Only use zero-copy for objects larger than 1MB
    const ZERO_COPY_MIN_SIZE: i64 = 1024 * 1024;

    if size <= ZERO_COPY_MIN_SIZE {
        return false;
    }

    // Don't use zero-copy if encryption is requested
    if headers.get(AMZ_SERVER_SIDE_ENCRYPTION).is_some()
        || headers.get(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM).is_some()
        || headers.get(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID).is_some()
    {
        return false;
    }

    // Don't use zero-copy if compression is likely (compressible content types)
    // The compression check happens later in the flow
    if let Some(content_type) = headers.get(CONTENT_TYPE)
        && let Ok(ct) = content_type.to_str()
    {
        // Skip zero-copy for easily compressible content types
        // since compression will be applied
        let compressible_types = [
            "text/plain",
            "text/html",
            "text/css",
            "text/javascript",
            "application/javascript",
            "application/json",
            "application/xml",
            "text/xml",
        ];
        for ct_type in compressible_types {
            if ct.contains(ct_type) {
                return false;
            }
        }
    }

    true
}

#[cfg(test)]
fn should_use_zero_copy_eager_put_path(
    size: i64,
    headers: &HeaderMap,
    server_side_encryption_requested: bool,
    should_compress: bool,
    is_extract: bool,
) -> bool {
    zero_copy_eager_put_path_status(size, headers, server_side_encryption_requested, should_compress, is_extract)
        == PUT_EAGER_STATUS_ELIGIBLE
}

fn zero_copy_eager_put_path_status(
    size: i64,
    headers: &HeaderMap,
    server_side_encryption_requested: bool,
    should_compress: bool,
    is_extract: bool,
) -> &'static str {
    zero_copy_eager_put_path_status_with_max_size(
        size,
        headers,
        server_side_encryption_requested,
        should_compress,
        is_extract,
        zero_copy_eager_put_max_size_bytes(),
    )
}

fn zero_copy_eager_put_path_status_with_max_size(
    size: i64,
    headers: &HeaderMap,
    server_side_encryption_requested: bool,
    should_compress: bool,
    is_extract: bool,
    max_size: i64,
) -> &'static str {
    if is_extract {
        return PUT_EAGER_STATUS_EXTRACT;
    }
    if should_compress {
        return PUT_EAGER_STATUS_COMPRESSED;
    }
    if server_side_encryption_requested {
        return PUT_EAGER_STATUS_ENCRYPTED;
    }

    if size <= 0 {
        return PUT_EAGER_STATUS_INVALID_SIZE;
    }
    if size > max_size {
        return PUT_EAGER_STATUS_ABOVE_EAGER_MAX;
    }

    if !should_use_zero_copy(size, headers) {
        return PUT_EAGER_STATUS_ZERO_COPY_INELIGIBLE;
    }

    if request_uses_aws_chunked(headers) && decoded_content_length_from_headers(headers).ok().flatten().is_none() {
        return PUT_EAGER_STATUS_AWS_CHUNKED_MISSING_DECODED_LENGTH;
    }

    PUT_EAGER_STATUS_ELIGIBLE
}

fn zero_copy_eager_put_max_size_bytes() -> i64 {
    let configured = *CACHED_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES.get_or_init(|| {
        rustfs_utils::get_env_usize(ENV_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES, DEFAULT_ZERO_COPY_EAGER_PUT_MAX_SIZE_BYTES)
    });
    i64::try_from(configured).unwrap_or(i64::MAX)
}

#[cfg(test)]
fn should_use_small_eager_put_path(
    size: i64,
    headers: &HeaderMap,
    server_side_encryption_requested: bool,
    should_compress: bool,
    is_extract: bool,
) -> bool {
    should_use_small_eager_put_path_with_max_size(
        size,
        headers,
        server_side_encryption_requested,
        should_compress,
        is_extract,
        small_eager_put_max_size_bytes(),
    )
}

fn should_use_small_eager_put_path_with_max_size(
    size: i64,
    headers: &HeaderMap,
    server_side_encryption_requested: bool,
    should_compress: bool,
    is_extract: bool,
    max_size: i64,
) -> bool {
    if is_extract || should_compress || server_side_encryption_requested {
        return false;
    }

    let has_conditional_write = [http::header::IF_MATCH, http::header::IF_NONE_MATCH]
        .into_iter()
        .filter_map(|name| headers.get(name))
        .filter_map(|value| value.to_str().ok())
        .any(|value| !value.trim().is_empty());
    let max_size = if has_conditional_write {
        max_size.max(CONDITIONAL_SMALL_EAGER_PUT_MAX_SIZE_BYTES)
    } else {
        max_size
    };

    if size <= 0 || size > max_size {
        return false;
    }

    if has_put_sse_request_headers(headers) {
        return false;
    }

    if request_uses_aws_chunked(headers) && decoded_content_length_from_headers(headers).ok().flatten().is_none() {
        return false;
    }

    true
}

fn small_eager_put_max_size_bytes() -> i64 {
    let configured = *CACHED_SMALL_EAGER_PUT_MAX_SIZE_BYTES
        .get_or_init(|| rustfs_utils::get_env_usize(ENV_SMALL_EAGER_PUT_MAX_SIZE_BYTES, DEFAULT_SMALL_EAGER_PUT_MAX_SIZE_BYTES));
    i64::try_from(configured).unwrap_or(i64::MAX)
}

fn dynamic_small_eager_put_max_size_bytes(concurrent_put_requests: usize) -> i64 {
    let configured = small_eager_put_max_size_bytes();
    if configured <= MIN_DYNAMIC_SMALL_EAGER_PUT_MAX_SIZE_BYTES {
        return configured;
    }
    let adjusted = if concurrent_put_requests > SMALL_EAGER_CONCURRENCY_HARD_LIMIT {
        configured / 4
    } else if concurrent_put_requests > SMALL_EAGER_CONCURRENCY_SOFT_LIMIT {
        configured / 2
    } else {
        configured
    };

    adjusted.max(MIN_DYNAMIC_SMALL_EAGER_PUT_MAX_SIZE_BYTES)
}

#[cfg(test)]
fn select_put_path(
    size: i64,
    headers: &HeaderMap,
    server_side_encryption_requested: bool,
    should_compress: bool,
    is_extract: bool,
) -> (&'static str, &'static str, bool, bool) {
    select_put_path_with_concurrency(size, headers, server_side_encryption_requested, should_compress, is_extract, 0)
}

fn select_put_path_with_concurrency(
    size: i64,
    headers: &HeaderMap,
    server_side_encryption_requested: bool,
    should_compress: bool,
    is_extract: bool,
    concurrent_put_requests: usize,
) -> (&'static str, &'static str, bool, bool) {
    let use_empty_or_small_eager_put_path = size == 0
        || should_use_small_eager_put_path_with_max_size(
            size,
            headers,
            server_side_encryption_requested,
            should_compress,
            is_extract,
            dynamic_small_eager_put_max_size_bytes(concurrent_put_requests),
        );
    let zero_copy_eager_put_path_status =
        zero_copy_eager_put_path_status(size, headers, server_side_encryption_requested, should_compress, is_extract);
    let use_zero_copy_eager_put_path = zero_copy_eager_put_path_status == PUT_EAGER_STATUS_ELIGIBLE;
    let put_path = if should_compress {
        "stream_compressed"
    } else if use_zero_copy_eager_put_path {
        "zero_copy_eager"
    } else if use_empty_or_small_eager_put_path {
        "small_eager"
    } else {
        "streaming"
    };

    (
        put_path,
        zero_copy_eager_put_path_status,
        use_zero_copy_eager_put_path,
        use_empty_or_small_eager_put_path,
    )
}

/// Objects at or below this size bypass BytesPool and use direct allocation.
/// This avoids Small-tier Mutex contention under high concurrency for tiny objects
/// where the allocation cost is negligible (≤4KiB memcpy).
const POOL_BYPASS_MAX_SIZE: usize = 4 * 1024;

pub(super) async fn read_small_put_body_into<R, B>(body: &mut R, buf: &mut B, size: usize) -> S3Result<()>
where
    R: AsyncRead + Unpin,
    B: bytes::BufMut,
{
    let mut filled = 0;

    while filled < size {
        let mut remaining = (&mut *buf).limit(size - filled);
        let read = tokio::io::AsyncReadExt::read_buf(&mut *body, &mut remaining)
            .await
            .map_err(ApiError::from)?;
        if read == 0 {
            return Err(s3_error!(IncompleteBody));
        }
        filled += read;
    }

    let mut extra = [0u8; 1];
    let extra_read = tokio::io::AsyncReadExt::read(&mut *body, &mut extra)
        .await
        .map_err(ApiError::from)?;
    if extra_read != 0 {
        return Err(s3_error!(UnexpectedContent));
    }

    Ok(())
}

async fn read_small_put_body_exact_pooled<R>(mut body: R, size: usize, pool: &BytesPool) -> S3Result<PooledBuffer>
where
    R: AsyncRead + Unpin,
{
    let mut buf = pool.acquire_buffer(size).await;
    read_small_put_body_into(&mut body, &mut *buf, size).await?;
    Ok(buf)
}

/// Read small PUT body into a directly-allocated buffer, bypassing BytesPool.
/// Used for objects ≤4KiB where pool contention under high concurrency
/// outweighs the allocation cost.
async fn read_small_put_body_exact_direct<R>(mut body: R, size: usize) -> S3Result<std::io::Cursor<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut buf = Vec::with_capacity(size);
    read_small_put_body_into(&mut body, &mut buf, size).await?;
    Ok(std::io::Cursor::new(buf))
}

async fn read_zero_copy_put_body_exact<S, E>(mut body: S, size: usize) -> S3Result<ChunkedBytesReader>
where
    S: futures::Stream<Item = std::result::Result<Bytes, E>> + Unpin,
    E: Into<StdError>,
{
    let mut chunks = Vec::new();
    let mut filled = 0usize;

    while filled < size {
        let Some(chunk) = body.next().await else {
            return Err(s3_error!(IncompleteBody));
        };
        let chunk = chunk.map_err(|err| ApiError::from(s3s_body_error_to_io(err.into())))?;
        if chunk.is_empty() {
            continue;
        }
        if filled.saturating_add(chunk.len()) > size {
            return Err(s3_error!(UnexpectedContent));
        }

        rustfs_io_metrics::record_zero_copy_buffer_operation("put_chunk", chunk.len());
        filled += chunk.len();
        chunks.push(chunk);
    }

    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|err| ApiError::from(s3s_body_error_to_io(err.into())))?;
        if !chunk.is_empty() {
            return Err(s3_error!(UnexpectedContent));
        }
    }

    Ok(ChunkedBytesReader::new(chunks))
}

#[derive(Default)]
pub(super) struct PutObjectChecksums {
    pub(super) crc32: Option<String>,
    pub(super) crc32c: Option<String>,
    pub(super) sha1: Option<String>,
    pub(super) sha256: Option<String>,
    pub(super) crc64nvme: Option<String>,
}

struct PutObjectCommitResult {
    obj_info: ObjectInfo,
    put_versioned: bool,
}

struct EagerPutCommitOwner<T: Send + 'static> {
    task: Option<tokio::task::JoinHandle<T>>,
    cancellation: tokio_util::sync::CancellationToken,
    cancellation_grace: Duration,
}

impl<T: Send + 'static> EagerPutCommitOwner<T> {
    fn new(
        task: tokio::task::JoinHandle<T>,
        cancellation: tokio_util::sync::CancellationToken,
        cancellation_grace: Duration,
    ) -> Self {
        Self {
            task: Some(task),
            cancellation,
            cancellation_grace,
        }
    }

    async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        let result = self.task.as_mut().expect("eager PUT commit owner task must be present").await;
        self.task = None;
        result
    }
}

impl<T: Send + 'static> Drop for EagerPutCommitOwner<T> {
    fn drop(&mut self) {
        let Some(mut task) = self.task.take() else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_err() {
            task.abort();
            return;
        }
        let cancellation = self.cancellation.clone();
        let cancellation_grace = self.cancellation_grace;
        spawn_traced(async move {
            if tokio::time::timeout(cancellation_grace, &mut task).await.is_err() {
                cancellation.cancel();
                metrics::counter!("rustfs_put_commit_owner_deadline_total", "put_path" => "eager").increment(1);
                warn!(
                    target: "rustfs::app::object_usecase",
                    event = EVENT_PUT_OBJECT_COMMIT_OWNER_DEADLINE,
                    component = LOG_COMPONENT_APP,
                    subsystem = LOG_SUBSYSTEM_OBJECT,
                    state = "cancellation_requested",
                    cancellation_grace_ms = cancellation_grace.as_millis() as u64,
                    "cancelled eager PutObject commit owner exceeded its grace period and requested storage cleanup"
                );
                let _ = task.await;
            }
        });
    }
}

#[cfg(test)]
type PutPostStoreTestHook = (String, Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>);

#[cfg(test)]
static PUT_POST_STORE_TEST_HOOK: OnceLock<Mutex<Option<PutPostStoreTestHook>>> = OnceLock::new();

#[cfg(test)]
fn install_put_post_store_test_hook(bucket: String, entered: Arc<tokio::sync::Barrier>, resume: Arc<tokio::sync::Barrier>) {
    *PUT_POST_STORE_TEST_HOOK
        .get_or_init(|| Mutex::new(None))
        .lock()
        .expect("PUT post-store test hook lock should not be poisoned") = Some((bucket, entered, resume));
}

#[cfg(test)]
async fn wait_for_put_post_store_test_hook(bucket: &str) {
    let hook = {
        let mut slot = PUT_POST_STORE_TEST_HOOK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .expect("PUT post-store test hook lock should not be poisoned");
        if slot.as_ref().is_some_and(|(expected_bucket, _, _)| expected_bucket == bucket) {
            slot.take()
        } else {
            None
        }
    };
    if let Some((_bucket, entered, resume)) = hook {
        entered.wait().await;
        resume.wait().await;
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_put_request_metadata(
    metadata: &mut HashMap<String, String>,
    headers: &HeaderMap,
    object_name: &str,
    cache_control: Option<CacheControl>,
    content_disposition: Option<ContentDisposition>,
    content_encoding: Option<ContentEncoding>,
    content_language: Option<ContentLanguage>,
    content_type: Option<ContentType>,
    expires: Option<String>,
    website_redirect_location: Option<WebsiteRedirectLocation>,
    tagging: Option<TaggingHeader>,
    storage_class: Option<StorageClass>,
) -> S3Result<()> {
    namespace_reserved_user_metadata(metadata);
    let expires = parse_expires_header(expires.as_deref())?;
    apply_standard_object_metadata(
        metadata,
        cache_control.as_deref(),
        content_disposition.as_deref(),
        content_encoding.as_deref(),
        content_language.as_deref(),
        content_type.as_deref(),
        expires.as_ref(),
        website_redirect_location.as_deref(),
    )?;
    if let Some(tags) = tagging {
        metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags);
    }
    if let Some(storage_class) = storage_class {
        metadata.insert(AMZ_STORAGE_CLASS.to_string(), storage_class.as_str().to_string());
    }

    extract_metadata_from_mime_with_object_name(headers, metadata, true, Some(object_name));
    Ok(())
}

pub(super) fn apply_put_request_object_lock_opts(
    bucket: &str,
    object_lock_config_state: &metadata_sys::ObjectLockConfigState,
    object_lock_legal_hold_status: Option<ObjectLockLegalHoldStatus>,
    object_lock_mode: Option<ObjectLockMode>,
    object_lock_retain_until_date: Option<Timestamp>,
    opts: &mut ObjectOptions,
) -> S3Result<()> {
    if let Some(eval_metadata) = build_put_like_object_lock_metadata(
        bucket,
        object_lock_config_state,
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_date,
    )? {
        opts.eval_metadata = Some(eval_metadata);
    }

    Ok(())
}

pub(super) fn is_sse_kms_requested(input: &PutObjectInput, headers: &HeaderMap) -> bool {
    input
        .server_side_encryption
        .as_ref()
        .is_some_and(|sse| sse.as_str().eq_ignore_ascii_case(ServerSideEncryption::AWS_KMS))
        || input.ssekms_key_id.is_some()
        || headers
            .get(AMZ_SERVER_SIDE_ENCRYPTION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case(ServerSideEncryption::AWS_KMS))
        || headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID)
}

fn is_post_object_sse_kms_requested(input: &PutObjectInput, headers: &HeaderMap) -> bool {
    is_sse_kms_requested(input, headers)
}

/// Standard content headers and the tagging / storage-class values of a PUT
/// that become object metadata through [`apply_put_request_metadata`].
pub(super) struct PutObjectContentInput {
    pub(super) cache_control: Option<CacheControl>,
    pub(super) content_disposition: Option<ContentDisposition>,
    pub(super) content_encoding: Option<ContentEncoding>,
    pub(super) content_language: Option<ContentLanguage>,
    pub(super) content_type: Option<ContentType>,
    pub(super) expires: Option<String>,
    pub(super) website_redirect_location: Option<WebsiteRedirectLocation>,
    pub(super) tagging: Option<TaggingHeader>,
    pub(super) storage_class: Option<StorageClass>,
}

/// Encryption inputs of a PUT after the S3 input/header merge.
pub(super) struct PutObjectSseInput {
    pub(super) server_side_encryption: Option<ServerSideEncryption>,
    pub(super) ssekms_key_id: Option<SSEKMSKeyId>,
    pub(super) sse_customer_algorithm: Option<SSECustomerAlgorithm>,
    pub(super) sse_customer_key: Option<s3s::dto::SSECustomerKey>,
    pub(super) sse_customer_key_md5: Option<SSECustomerKeyMD5>,
}

/// Explicit Object Lock values of a PUT; all `None` means the bucket default
/// retention decides.
pub(super) struct PutObjectLockInput {
    pub(super) legal_hold_status: Option<ObjectLockLegalHoldStatus>,
    pub(super) mode: Option<ObjectLockMode>,
    pub(super) retain_until_date: Option<Timestamp>,
}

/// Expected body MD5 as the caller carries it. It is decoded at the same
/// point of the write path for every origin so an invalid digest keeps its
/// precedence relative to quota, admission and Object Lock errors.
pub(super) enum PutObjectContentMd5 {
    /// `Content-MD5` request header value.
    Base64(String),
    /// Lowercase hex digest, as an internal caller already holds it.
    Hex(String),
}

/// Where a single-object write originates.
pub(super) enum PutObjectOrigin<'a> {
    /// The S3 PutObject/PostObject handler: request-bound identity, the
    /// audit/notification chain and the bucket-generation guard installed by
    /// the access layer all come from the request.
    S3 {
        req: &'a S3Request<PutObjectInput>,
        event_name: EventName,
    },
    /// A trusted in-process caller writing on the server's behalf. There is no
    /// request and no credential: managed-SSE authorization treats the write
    /// as internal, and the creation event, when requested, names
    /// `principal_id` instead of an access key.
    Internal {
        principal_id: &'static str,
        emit_events: bool,
        preserve_delete_marker: bool,
        expected_bucket_incarnation_id: Option<Uuid>,
    },
}

impl PutObjectOrigin<'_> {
    fn replication_request_authorized(&self) -> bool {
        match self {
            Self::S3 { req, .. } => replication_request_authorized(req),
            Self::Internal { .. } => false,
        }
    }

    fn apply_bucket_generation_guard(&self, bucket: &str, opts: &mut ObjectOptions) -> S3Result<()> {
        match self {
            Self::S3 { req, .. } => apply_bucket_generation_guard(req, bucket, opts),
            Self::Internal {
                expected_bucket_incarnation_id,
                ..
            } => {
                opts.expected_bucket_incarnation_id = *expected_bucket_incarnation_id;
                Ok(())
            }
        }
    }

    fn sse_principal(&self) -> Option<SseKmsPrincipal> {
        match self {
            Self::S3 { req, .. } => SseKmsPrincipal::from_request(req),
            Self::Internal { .. } => None,
        }
    }
}

/// Every input the shared single-object write path needs, independent of
/// whether an S3 request or an internal caller produced it.
pub(super) struct PutObjectWriteRequest<'a> {
    pub(super) bucket: String,
    pub(super) key: String,
    /// Authoritative plaintext length; never negative.
    pub(super) size: i64,
    pub(super) quota_operation: QuotaOperation,
    /// Authorized SSE-C replication body that is already ciphertext.
    pub(super) ciphertext_passthrough: bool,
    pub(super) inbound_replication_put: bool,
    /// Request headers, or the object's content headers for an internal write.
    pub(super) headers: &'a HeaderMap,
    pub(super) query: Option<&'a str>,
    pub(super) trailing_headers: Option<s3s::TrailingHeaders>,
    pub(super) version_id: Option<String>,
    pub(super) sse: PutObjectSseInput,
    /// User metadata keyed the way s3s delivers it (`x-amz-meta-` stripped).
    pub(super) user_metadata: HashMap<String, String>,
    /// Internal `x-rustfs-internal-*` / `x-minio-internal-*` keys written
    /// verbatim onto the object; empty for S3 requests.
    pub(super) internal_metadata: HashMap<String, String>,
    pub(super) content: PutObjectContentInput,
    pub(super) object_lock: PutObjectLockInput,
    pub(super) content_md5: Option<PutObjectContentMd5>,
    /// ETag to store instead of the computed one; `None` keeps the computed
    /// (or replication-header-derived) value.
    pub(super) preserve_etag: Option<String>,
    pub(super) origin: PutObjectOrigin<'a>,
}

/// Audit/notification completion of a write, per origin.
pub(super) enum PutObjectCompletion {
    S3(OperationHelper),
    Internal(Option<Box<InternalPutObjectEvent>>),
}

impl PutObjectCompletion {
    fn object(self, obj_info: ObjectInfo) -> Self {
        match self {
            Self::S3(helper) => Self::S3(helper.object(obj_info)),
            Self::Internal(event) => Self::Internal(event.map(|event| Box::new(event.object(obj_info)))),
        }
    }

    fn version_id(self, version_id: String) -> Self {
        match self {
            Self::S3(helper) => Self::S3(helper.version_id(version_id)),
            Self::Internal(event) => Self::Internal(event.map(|event| Box::new(event.version_id(version_id)))),
        }
    }

    fn complete<T>(self, result: &S3Result<S3Response<T>>) -> Self {
        match self {
            Self::S3(helper) => Self::S3(helper.complete(result)),
            Self::Internal(event) => {
                if let Some(event) = event {
                    event.complete(result);
                }
                Self::Internal(None)
            }
        }
    }
}

/// Record the failed completion and hand the error back to the caller.
fn fail_put_object(completion: PutObjectCompletion, err: S3Error) -> S3Error {
    let result: S3Result<S3Response<()>> = Err(err);
    let _ = completion.complete(&result);
    match result {
        Err(err) => err,
        Ok(_) => unreachable!("failed PutObject completion carries an error"),
    }
}

/// A committed single-object write awaiting its response-side completion.
pub(super) struct PutObjectCommitted {
    pub(super) obj_info: ObjectInfo,
    pub(super) put_versioned: bool,
    pub(super) effective_sse: Option<ServerSideEncryption>,
    pub(super) effective_kms_key_id: Option<SSEKMSKeyId>,
    pub(super) sse_customer_algorithm: Option<SSECustomerAlgorithm>,
    pub(super) sse_customer_key_md5: Option<SSECustomerKeyMD5>,
    pub(super) put_extra_checksum_headers: Vec<(&'static str, String)>,
    completion: PutObjectCompletion,
    put_request_guard: PutObjectGuard,
    bucket: String,
    key: String,
    start_time: Instant,
    size: i64,
    use_zero_copy_eager_put_path: bool,
    concurrent_put_requests: usize,
    buffer_size: usize,
}

impl PutObjectCommitted {
    /// Publish the audit entry / creation event for `result`, record the
    /// request-level PutObject metrics and release the request guard.
    pub(super) fn finish<T>(self, result: &S3Result<S3Response<T>>) {
        let Self {
            completion,
            mut put_request_guard,
            bucket,
            key,
            start_time,
            size,
            use_zero_copy_eager_put_path,
            concurrent_put_requests,
            buffer_size,
            ..
        } = self;
        let _ = completion.complete(result);

        // Record PutObject metrics via zero-copy-metrics
        {
            let duration_ms = start_time.elapsed().as_millis() as f64;
            rustfs_io_metrics::record_put_object(
                duration_ms,
                size,
                use_zero_copy_eager_put_path, // Track if zero-copy was enabled
            );
        }

        debug!(
            target: "rustfs::app::object_usecase",
            component = "app",
            subsystem = "object",
            bucket = %bucket,
            key = %key,
            concurrent_put_requests,
            buffer_size,
            "PutObject request completed"
        );

        put_request_guard.finish_ok();
    }
}

impl DefaultObjectUsecase {
    fn should_use_large_put_concurrency_tuning(size: i64) -> bool {
        size >= DEFAULT_PUT_LARGE_CONCURRENCY_TUNING_MIN_SIZE_BYTES
    }

    fn put_object_execution_context(req: &S3Request<PutObjectInput>) -> (EventName, QuotaOperation, &'static str) {
        if req.extensions.get::<PostObjectRequestMarker>().is_some() {
            (put_event_name_for_post_object(true), QuotaOperation::PostObject, "POST")
        } else {
            (put_event_name_for_post_object(false), QuotaOperation::PutObject, "PUT")
        }
    }

    #[instrument(name = "execute_put_object", level = "info", skip(self, _fs, req))]
    pub async fn execute_put_object(&self, _fs: &FS, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        self.execute_put_object_boxed(_fs, req).await
    }

    fn execute_put_object_boxed<'a>(
        &'a self,
        _fs: &'a FS,
        req: S3Request<PutObjectInput>,
    ) -> impl std::future::Future<Output = S3Result<S3Response<PutObjectOutput>>> + Send + 'a {
        Box::pin(self.execute_put_object_inner(_fs, req))
    }

    async fn execute_put_object_inner(&self, _fs: &FS, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let start_time = std::time::Instant::now();
        let put_stage_metrics_enabled = rustfs_io_metrics::put_stage_metrics_enabled();
        let mut req = req;

        let request_shape_stage_start = put_stage_metrics_enabled.then(Instant::now);

        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        // Authentication and header parsing happen in the S3 middleware before
        // this use case runs. Attribute that already-paid request prefix from
        // the request context without adding per-request work when stage
        // metrics are disabled.
        if put_stage_metrics_enabled && let Some(context) = req.extensions.get::<request_context::RequestContext>() {
            rustfs_io_metrics::record_put_object_stage_duration(
                "request_ingress_to_context",
                context.start_time.elapsed().as_secs_f64() * 1000.0,
            );
        }

        let (event_name, quota_operation, request_method_name) = Self::put_object_execution_context(&req);
        let max_content_length = parse_presigned_put_max_content_length(
            &req.headers,
            req.uri.query(),
            req.extensions.get::<VerifiedPresignedRequest>().is_some(),
        )?;
        if req.extensions.get::<PostObjectRequestMarker>().is_some() && is_post_object_sse_kms_requested(&req.input, &req.headers)
        {
            return Err(s3_error!(NotImplemented, "SSE-KMS is not supported for POST object uploads"));
        }
        if let Some(ref storage_class) = req.input.storage_class
            && !is_valid_storage_class(storage_class.as_str())
        {
            return Err(s3_error!(InvalidStorageClass));
        }
        // An authorized inbound replication PUT must store the replica verbatim.
        // Legacy snowball-extracted members may still carry the auto-extract
        // metadata, which replication replays as a header. Do not interpret that
        // historical user metadata as a request to untar the member again.
        let inbound_replication_put = replication_request_authorized(&req)
            && get_header(&req.headers, SUFFIX_SOURCE_REPLICATION_REQUEST).as_deref() == Some("true");
        if max_content_length.is_some() && is_put_object_extract_requested(&req.headers) {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                format!("{RUSTFS_MAX_CONTENT_LENGTH_QUERY} is not supported for archive extraction"),
            ));
        }
        if is_put_object_extract_requested(&req.headers) && !inbound_replication_put {
            return Box::pin(self.execute_put_object_extract(req)).await;
        }
        // SSE-C ciphertext passthrough (authorized replication only): the body
        // is already ciphertext and must be stored verbatim — no compression,
        // no bucket-default encryption.
        let ciphertext_passthrough =
            inbound_replication_put && rustfs_utils::http::ssec_transport_to_stored_metadata(&req.headers).is_some();

        let input = std::mem::take(&mut req.input);

        let PutObjectInput {
            body,
            bucket,
            cache_control,
            key,
            content_length,
            content_disposition,
            content_encoding,
            content_language,
            content_type,
            expires,
            tagging,
            metadata,
            version_id,
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ssekms_key_id,
            content_md5,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            storage_class,
            website_redirect_location,
            ..
        } = input;

        // Merge SSE-C params from headers (fallback when S3 layer does not populate input)
        let (h_algo, h_key, h_md5) = extract_ssec_params_from_headers(&req.headers)?;
        let sse_customer_algorithm = sse_customer_algorithm.or(h_algo);
        let sse_customer_key = sse_customer_key.or(h_key);
        let sse_customer_key_md5 = sse_customer_key_md5.or(h_md5);

        // Merge server_side_encryption from headers (fallback when S3 layer does not populate input)
        let server_side_encryption = server_side_encryption.or(extract_server_side_encryption_from_headers(&req.headers)?);

        // Validate object key
        validate_object_key(&key, request_method_name)?;
        validate_table_catalog_object_mutation(&bucket, &key).await?;

        // Validate archive content encoding (reject when strict mode is enabled)
        validate_archive_content_encoding(
            &key,
            req.headers.get("content-type").and_then(|value| value.to_str().ok()),
            req.headers.get("content-encoding").and_then(|value| value.to_str().ok()),
        )?;
        rustfs_io_metrics::record_put_object_stage_duration_from("app_request_shape", request_shape_stage_start);

        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        // Guard against a proxy/CDN that forwards a partial body then goes silent
        // without closing the connection: bound the inter-chunk wait so the read
        // fails (with a diagnostic log) instead of hanging forever (issue #3076).
        let body = {
            let request_id = req
                .extensions
                .get::<request_context::RequestContext>()
                .map(|ctx| ctx.request_id.clone())
                .unwrap_or_default();
            guard_put_object_body_read_timeout(body, &bucket, &key, &request_id, content_length, put_object_body_read_timeout())
        };

        let body = match max_content_length {
            Some(limit) => StreamingBlob::new(MaxContentLengthStream {
                inner: body,
                limit,
                received: 0,
                exceeded: false,
            }),
            None => body,
        };

        // Resolve the authoritative decoded/plain object length (rejecting negative/unknown) before anything else consumes it.
        let size = resolve_put_object_authoritative_size(&req.headers, content_length)?;

        if let Some(limit) = max_content_length
            && u64::try_from(size).is_ok_and(|size| size > limit)
        {
            return Err(S3Error::new(S3ErrorCode::EntityTooLarge));
        }

        let write = PutObjectWriteRequest {
            bucket: bucket.clone(),
            key,
            size,
            quota_operation,
            ciphertext_passthrough,
            inbound_replication_put,
            headers: &req.headers,
            query: req.uri.query(),
            trailing_headers: req.trailing_headers.clone(),
            version_id,
            sse: PutObjectSseInput {
                server_side_encryption,
                ssekms_key_id,
                sse_customer_algorithm,
                sse_customer_key,
                sse_customer_key_md5,
            },
            user_metadata: metadata.unwrap_or_default(),
            internal_metadata: HashMap::new(),
            content: PutObjectContentInput {
                cache_control,
                content_disposition,
                content_encoding,
                content_language,
                content_type,
                expires,
                website_redirect_location,
                tagging,
                storage_class,
            },
            object_lock: PutObjectLockInput {
                legal_hold_status: object_lock_legal_hold_status,
                mode: object_lock_mode,
                retain_until_date: object_lock_retain_until_date,
            },
            content_md5: content_md5.map(PutObjectContentMd5::Base64),
            preserve_etag: None,
            origin: PutObjectOrigin::S3 { req: &req, event_name },
        };
        let committed = self.put_object_core(write, body, start_time).await?;

        let raw_version = committed.obj_info.version_id.map(|v| v.to_string());
        let put_version = if committed.put_versioned { raw_version } else { None };

        let e_tag = committed.obj_info.etag.clone().map(|etag| to_s3s_etag(&etag));

        let expiration = resolve_put_object_expiration(&bucket, &committed.obj_info).await;

        let mut checksums = PutObjectChecksums {
            crc32: input.checksum_crc32,
            crc32c: input.checksum_crc32c,
            sha1: input.checksum_sha1,
            sha256: input.checksum_sha256,
            crc64nvme: input.checksum_crc64nvme,
        };
        apply_trailing_checksums(
            input.checksum_algorithm.as_ref().map(|a| a.as_str()),
            &req.trailing_headers,
            &mut checksums,
        );

        let output = PutObjectOutput {
            e_tag,
            server_side_encryption: committed.effective_sse.clone(),
            sse_customer_algorithm: committed.sse_customer_algorithm.clone(),
            sse_customer_key_md5: committed.sse_customer_key_md5.clone(),
            ssekms_key_id: committed.effective_kms_key_id.clone(),
            expiration,
            checksum_crc32: checksums.crc32,
            checksum_crc32c: checksums.crc32c,
            checksum_sha1: checksums.sha1,
            checksum_sha256: checksums.sha256,
            checksum_crc64nvme: checksums.crc64nvme,
            version_id: put_version,
            ..Default::default()
        };

        // For browser-based POST uploads (multipart/form-data), response status/body handling
        // is decided by s3s PostObject serializer (success_action_status / redirect semantics).

        let response_build_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let mut response = S3Response::new(output);
        // Echo XXHash3/64/128 / SHA-512 checksums that s3s PutObjectOutput has no typed
        // field for (#1256).
        inject_additional_checksum_headers(&mut response.headers, &committed.put_extra_checksum_headers);
        rustfs_io_metrics::record_put_object_stage_duration_from("app_response_build", response_build_stage_start);
        let result = Ok(response);
        committed.finish(&result);

        result
    }

    /// The single-object write path shared by the S3 handler and internal
    /// callers: quota admission, foreground write admission, bucket default
    /// SSE, Object Lock defaults, put options, the hashing/compressing/
    /// encrypting reader, the owned store commit, usage accounting,
    /// replication scheduling and the creation-event setup. The caller shapes
    /// the request before and builds its response after.
    pub(super) async fn put_object_core(
        &self,
        write: PutObjectWriteRequest<'_>,
        body: StreamingBlob,
        start_time: Instant,
    ) -> S3Result<PutObjectCommitted> {
        let put_stage_metrics_enabled = rustfs_io_metrics::put_stage_metrics_enabled();
        let PutObjectWriteRequest {
            bucket,
            key,
            mut size,
            quota_operation,
            ciphertext_passthrough,
            inbound_replication_put,
            headers,
            query,
            trailing_headers,
            version_id,
            sse,
            user_metadata,
            internal_metadata,
            content,
            object_lock,
            content_md5,
            preserve_etag,
            origin,
        } = write;
        let PutObjectSseInput {
            server_side_encryption,
            ssekms_key_id,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
        } = sse;

        // The app check preserves the existing S3 error contract; the storage
        // commit path reserves the exact net logical growth under its locks.
        let quota_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let quota_check = self
            .check_bucket_quota(
                &bucket,
                quota_operation,
                u64::try_from(size).map_err(|_| S3Error::new(S3ErrorCode::UnexpectedContent))?,
            )
            .await?;
        rustfs_io_metrics::record_put_object_stage_duration_from("app_quota_check", quota_stage_start);
        let quota_enabled = quota_check.as_ref().is_some_and(|result| result.quota_limit.is_some());
        if quota_enabled && ciphertext_passthrough {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "SSE-C ciphertext replication is unavailable for quota-enabled buckets".to_string(),
            ));
        }

        let ingress_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let should_compress =
            is_disk_compressible(headers, &key) && size > MIN_DISK_COMPRESSIBLE_SIZE as i64 && !ciphertext_passthrough;

        // Resolve the store through the request-bound server context
        // (backlog#1052 S6), not the process-global handle, so an embedded
        // second server never writes into the first server's store.
        let store_lookup_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        let bucket_validate_stage_start = put_stage_metrics_enabled.then(Instant::now);
        validate_bucket_exists(&store, &bucket).await?;
        rustfs_io_metrics::record_put_object_stage_duration_from("app_store_lookup", store_lookup_stage_start);
        rustfs_io_metrics::record_put_object_stage_duration_from("app_bucket_validate", bucket_validate_stage_start);

        let put_admission = match get_concurrency_manager()
            .admit_put_object(size)
            .await
            .map_err(|_| s3_error!(InternalError, "foreground write admission closed"))?
        {
            ForegroundWriteAdmission::Disabled => None,
            ForegroundWriteAdmission::Admitted(permit) => {
                counter!("rustfs.put_object.foreground_admission.total", "result" => "admitted").increment(1);
                Some(permit)
            }
            ForegroundWriteAdmission::Rejected => {
                counter!("rustfs.put_object.foreground_admission.total", "result" => "rejected").increment(1);
                return Err(s3_error!(
                    SlowDown,
                    "foreground write concurrency limit reached, please reduce your request rate"
                ));
            }
        };

        let mut put_request_guard = PutObjectGuard::new();
        let concurrent_put_requests = PutObjectGuard::concurrent_requests();

        // Apply adaptive buffer sizing based on file size for optimal streaming performance.
        // Uses workload profile configuration (enabled by default) to select appropriate buffer size.
        // Buffer sizes range from 32KB to 4MB depending on file size and configured workload profile.
        // Concurrency-aware adjustment reduces buffer size under high PUT concurrency to lower memory pressure.
        let base_buffer_size = get_buffer_size_opt_in(size);
        let use_large_put_concurrency_tuning = Self::should_use_large_put_concurrency_tuning(size);
        let buffer_size = if use_large_put_concurrency_tuning {
            get_put_concurrency_aware_buffer_size(size, base_buffer_size)
        } else {
            base_buffer_size
        };

        let sse_config_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let bucket_sse_config = load_bucket_default_sse_config(&bucket).await;
        rustfs_io_metrics::record_put_object_stage_duration_from("app_sse_config_lookup", sse_config_stage_start);
        let bucket_sse_config = bucket_sse_config?;
        debug!(
            target: "rustfs::app::object_usecase",
            component = "app",
            subsystem = "object",
            event = "bucket_sse_config_lookup",
            bucket = %bucket,
            found = bucket_sse_config.is_some(),
            "Bucket SSE configuration lookup completed"
        );

        let original_sse = server_side_encryption.clone();
        let (mut effective_sse, mut effective_kms_key_id) = resolve_bucket_default_sse(
            bucket_sse_config.as_ref().map(|(config, _timestamp)| config),
            server_side_encryption,
            ssekms_key_id,
            false,
        );
        debug!(
            target: "rustfs::app::object_usecase",
            component = "app",
            subsystem = "object",
            event = "effective_sse_resolved",
            bucket = %bucket,
            requested = ?original_sse,
            effective = ?effective_sse,
            "Resolved effective SSE configuration"
        );

        if ciphertext_passthrough {
            // The replica keeps the source's SSE-C metadata; the bucket
            // default must not claim managed encryption on it.
            effective_sse = None;
            effective_kms_key_id = None;
        }

        let server_side_encryption_requested =
            effective_sse.is_some() || sse_customer_algorithm.is_some() || effective_kms_key_id.is_some();
        let (put_path, zero_copy_eager_put_path_status, use_zero_copy_eager_put_path, use_empty_or_small_eager_put_path) =
            select_put_path_with_concurrency(
                size,
                headers,
                server_side_encryption_requested,
                should_compress,
                false,
                concurrent_put_requests,
            );
        if use_zero_copy_eager_put_path {
            counter!("rustfs_zero_copy_write_attempts_total").increment(1);
            histogram!("rustfs_zero_copy_write_size_bytes").record(size as f64);
            debug!("Zero-copy write enabled for {} byte object (bucket={}, key={})", size, bucket, key);
            counter!(buffered_write::ATTEMPTS_TOTAL).increment(1);
            histogram!(buffered_write::ATTEMPT_SIZE_BYTES).record(size as f64);
        }
        rustfs_io_metrics::record_put_object_diagnostics(
            put_path,
            zero_copy_eager_put_path_status,
            size,
            buffer_size,
            use_large_put_concurrency_tuning,
        );

        // Validate SSE-C headers early: reject partial/invalid combinations per S3 spec
        validate_sse_headers_for_write(
            effective_sse.as_ref(),
            effective_kms_key_id.as_ref(),
            extract_ssekms_context_from_headers(headers)?.as_ref(),
            sse_customer_algorithm.as_ref(),
            sse_customer_key.as_ref(),
            sse_customer_key_md5.as_ref(),
            true, // PutObject requires all three: algorithm, key, key_md5
        )?;

        let mut metadata = user_metadata;
        let has_explicit_object_lock_retention = object_lock.mode.is_some()
            || object_lock.retain_until_date.is_some()
            || has_replication_retention_update(headers, inbound_replication_put);
        let object_lock_config_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let object_lock_config_state = load_bucket_object_lock_config_state(&bucket).await?;
        rustfs_io_metrics::record_put_object_stage_duration_from("app_object_lock_config_lookup", object_lock_config_stage_start);
        apply_put_request_metadata(
            &mut metadata,
            headers,
            &key,
            content.cache_control,
            content.content_disposition,
            content.content_encoding,
            content.content_language,
            content.content_type,
            content.expires,
            content.website_redirect_location,
            content.tagging,
            content.storage_class,
        )?;
        apply_bucket_default_lock_retention(
            &bucket,
            &object_lock_config_state,
            &mut metadata,
            has_explicit_object_lock_retention,
        )?;
        metadata.extend(internal_metadata);

        let put_opts_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let mut opts: ObjectOptions = put_opts_with_replication_authorization(
            &bucket,
            &key,
            version_id.clone(),
            headers,
            metadata.clone(),
            origin.replication_request_authorized(),
        )
        .await
        .map_err(ApiError::from)?;
        if let Some(etag) = preserve_etag {
            opts.preserve_etag = Some(etag);
        }
        if let PutObjectOrigin::Internal {
            preserve_delete_marker, ..
        } = &origin
        {
            opts.preserve_delete_marker = *preserve_delete_marker;
        }
        if let Some(quota_check) = quota_check.as_ref() {
            apply_quota_admission(&mut opts, quota_check)?;
        }
        rustfs_io_metrics::record_put_object_stage_duration_from("app_put_opts_build", put_opts_stage_start);
        origin.apply_bucket_generation_guard(&bucket, &mut opts)?;
        apply_put_request_object_lock_opts(
            &bucket,
            &object_lock_config_state,
            object_lock.legal_hold_status,
            object_lock.mode,
            object_lock.retain_until_date,
            &mut opts,
        )?;
        let eager_put_commit_cancellation =
            (use_zero_copy_eager_put_path || use_empty_or_small_eager_put_path).then(tokio_util::sync::CancellationToken::new);
        opts.put_object_cancellation = eager_put_commit_cancellation.clone();

        // rustfs/backlog#1009: the pre-PUT lookup has exactly two consumers —
        // the existing-object WORM validation and usage accounting's
        // previous_current_size. When the bucket has no object locking (WORM is
        // a provable no-op; the gate fails closed on metadata errors) and the
        // PUT targets the latest version (no explicit version_id from internal
        // replication), the lookup is skipped and accounting is backfilled from
        // the dst xl.meta that rename_data already reads, saving a full-disk
        // metadata fanout per PUT.
        let prelookup_required = version_id.is_some() || object_lock_checks_required_for_state(&object_lock_config_state);
        // Outer None = prelookup skipped (accounting comes from the commit
        // backfill); Some(inner) = the previous current size as observed by the
        // lookup, with the pre-#1009 semantics kept bit-for-bit.
        let prelookup_stage_start = (prelookup_required && put_stage_metrics_enabled).then(Instant::now);
        let prelookup_previous_current_size: Option<Option<u64>> = if prelookup_required {
            let current_opts: ObjectOptions = internal_object_info_lookup_opts(
                get_opts(&bucket, &key, version_id.clone(), None, headers)
                    .await
                    .map_err(ApiError::from)?,
            );
            let previous_current_info = {
                crate::hp_guard!("S3::put_object_prelookup");
                store.get_object_info(&bucket, &key, &current_opts).await
            };
            Some(match previous_current_info {
                Ok(existing_obj_info) => {
                    validate_existing_object_lock_for_write(&object_lock_config_state, &existing_obj_info, &opts)?;
                    Some(if quota_enabled {
                        quota_object_size(&existing_obj_info).map_err(ApiError::from)?
                    } else {
                        existing_obj_info.size.max(0) as u64
                    })
                }
                Err(err) => {
                    if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                        return Err(ApiError::from(err).into());
                    }
                    None
                }
            })
        } else {
            None
        };
        rustfs_io_metrics::record_put_object_stage_duration_from("app_prelookup", prelookup_stage_start);

        let actual_size = size;
        if !ciphertext_passthrough && let Some(quota_check) = quota_check.as_ref() {
            ensure_object_size_within_quota(
                quota_check,
                u64::try_from(actual_size).map_err(|_| S3Error::new(S3ErrorCode::UnexpectedContent))?,
            )?;
        }

        let mut md5hex = match content_md5 {
            Some(PutObjectContentMd5::Base64(base64_md5)) => {
                let md5 = base64_simd::STANDARD
                    .decode_to_vec(base64_md5.as_bytes())
                    .map_err(|e| ApiError::from(StorageError::other(format!("Invalid content MD5: {e}"))))?;
                Some(hex_simd::encode_to_string(&md5, hex_simd::AsciiCase::Lower))
            }
            Some(PutObjectContentMd5::Hex(md5hex)) => Some(md5hex),
            None => None,
        };

        let mut sha256hex = get_content_sha256_with_query(headers, query);

        let mut write_plan = WritePlan::new();
        // Additional-checksum (XXHash3/64/128, SHA-512) values to echo on the PutObject
        // response (#1256); captured at want_checksum set points before opts is moved.
        let mut put_extra_checksum_headers: Vec<(&'static str, String)> = Vec::new();
        let mut reader = if should_compress {
            let body = tokio::io::BufReader::with_capacity(
                buffer_size,
                StreamReader::new(body.map(|f| f.map_err(s3s_body_error_to_io))),
            );
            let algorithm = CompressionAlgorithm::default();
            insert_str(&mut metadata, SUFFIX_COMPRESSION, compression_metadata_value(algorithm));
            insert_str(&mut metadata, SUFFIX_ACTUAL_SIZE, size.to_string());

            let mut hrd =
                HashReader::from_stream(body, size, size, md5hex.take(), sha256hex.take(), false).map_err(ApiError::from)?;

            if let Err(err) = hrd.add_checksum_from_s3s(headers, trailing_headers.clone(), false) {
                return Err(ApiError::from(err).into());
            }

            opts.want_checksum = hrd.checksum();
            put_extra_checksum_headers = additional_checksum_echo_pairs(&opts.want_checksum);
            insert_str(&mut opts.user_defined, SUFFIX_COMPRESSION, compression_metadata_value(algorithm));
            insert_str(&mut opts.user_defined, SUFFIX_ACTUAL_SIZE, size.to_string());

            size = HashReader::SIZE_PRESERVE_LAYER;
            write_plan = write_plan.with_compression(algorithm);
            hrd
        } else {
            if use_zero_copy_eager_put_path {
                let zero_copy_start = std::time::Instant::now();
                let eager_body = read_zero_copy_put_body_exact(body, actual_size as usize).await?;
                rustfs_io_metrics::record_zero_copy_write(actual_size as usize, zero_copy_start.elapsed().as_secs_f64() * 1000.0);
                HashReader::from_stream(eager_body, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?
            } else if use_empty_or_small_eager_put_path {
                if (actual_size as usize) <= POOL_BYPASS_MAX_SIZE {
                    // Bypass BytesPool for very small objects to avoid Small-tier
                    // Mutex contention under high concurrency. Direct allocation
                    // for ≤4KiB is negligible cost.
                    let eager_body = read_small_put_body_exact_direct(
                        StreamReader::new(body.map(|f| f.map_err(s3s_body_error_to_io))),
                        actual_size as usize,
                    )
                    .await?;
                    HashReader::from_stream(eager_body, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?
                } else {
                    let pool = get_concurrency_manager().bytes_pool();
                    let eager_body = read_small_put_body_exact_pooled(
                        StreamReader::new(body.map(|f| f.map_err(s3s_body_error_to_io))),
                        actual_size as usize,
                        pool.as_ref(),
                    )
                    .await?;
                    let eager_reader = PooledBufferReader::new(eager_body, actual_size as usize);
                    HashReader::from_stream(eager_reader, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?
                }
            } else {
                let body = tokio::io::BufReader::with_capacity(
                    buffer_size,
                    StreamReader::new(body.map(|f| f.map_err(s3s_body_error_to_io))),
                );
                HashReader::from_stream(body, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?
            }
        };

        if size >= 0 {
            if let Err(err) = reader.add_checksum_from_s3s(headers, trailing_headers.clone(), false) {
                return Err(ApiError::from(err).into());
            }

            opts.want_checksum = reader.checksum();
            put_extra_checksum_headers = additional_checksum_echo_pairs(&opts.want_checksum);
        }
        rustfs_io_metrics::record_put_object_path(put_path);
        rustfs_io_metrics::record_put_object_stage_duration_from("ingress_prepare", ingress_stage_start);

        let (mut completion, request_context) = match &origin {
            PutObjectOrigin::S3 { req, event_name } => (
                PutObjectCompletion::S3(OperationHelper::new(req, *event_name, S3Operation::PutObject)),
                req.extensions.get::<request_context::RequestContext>().cloned(),
            ),
            PutObjectOrigin::Internal {
                principal_id,
                emit_events,
                ..
            } => {
                let principal_id = *principal_id;
                let request_context = request_context::RequestContext::fallback();
                let event = emit_events.then(|| {
                    InternalPutObjectEvent::new(
                        current_notify_interface_for_context(self.context.as_deref()),
                        request_context.clone(),
                        EventName::ObjectCreatedPut,
                        &bucket,
                        &key,
                        principal_id,
                    )
                });
                (PutObjectCompletion::Internal(event.flatten().map(Box::new)), Some(request_context))
            }
        };
        let ssekms_context = extract_ssekms_context_from_headers(headers)?;

        // Apply encryption using unified SSE API.
        let encryption_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let write_principal = origin.sse_principal();
        let encryption_request = EncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption: effective_sse.clone(),
            ssekms_key_id: effective_kms_key_id.clone(),
            ssekms_context,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            content_size: actual_size,
            principal: write_principal.as_ref(),
        };

        // SSE-C ciphertext passthrough must skip sse_encryption entirely: an
        // explicit guard is required because prepare_sse_configuration inside
        // it falls back to the bucket default encryption config and would
        // double-encrypt the already-encrypted body.
        let encryption_material = if opts.preserve_ciphertext {
            None
        } else {
            match sse_encryption(encryption_request).await {
                Ok(material) => material,
                Err(err) => return Err(fail_put_object(completion, err.into())),
            }
        };

        if let Some(material) = encryption_material {
            effective_sse = Some(material.server_side_encryption.clone());
            effective_kms_key_id = material.kms_key_id.clone();

            write_plan = write_plan.with_encryption(material.write_encryption(None));

            let encryption_metadata = encryption_material_to_metadata(&material)?;
            metadata.extend(encryption_metadata.clone());
            opts.user_defined.extend(encryption_metadata);
            if opts.want_checksum.is_some() {
                insert_str(&mut metadata, SUFFIX_PLAINTEXT_CHECKSUM, "true".to_string());
                insert_str(&mut opts.user_defined, SUFFIX_PLAINTEXT_CHECKSUM, "true".to_string());
            }
        }

        reader = write_plan.apply(reader, actual_size).map_err(ApiError::from)?;
        rustfs_io_metrics::record_put_object_stage_duration_from("app_encryption_prepare", encryption_stage_start);

        let reader = PutObjReader::new(reader);

        let mt2 = metadata.clone();
        opts.user_defined.extend(metadata);
        let request_id = request_context
            .as_ref()
            .map(|ctx| ctx.request_id.clone())
            .unwrap_or_else(|| request_context::RequestContext::fallback().request_id);

        // Compute the replication decision exactly once per PUT. The same
        // immutable `dsc` drives both the pending metadata written below and the
        // post-commit schedule (see the reuse site further down), so a
        // replication-config hot update can no longer split the two phases
        // (https://github.com/rustfs/backlog/issues/1320).
        let replication_decision_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let dsc =
            must_replicate_object(&bucket, &key, &mt2, "".to_string(), opts.delete_marker_replication_status(), opts.clone())
                .await;
        rustfs_io_metrics::record_put_object_stage_duration_from("app_replication_decision", replication_decision_stage_start);

        if dsc.replicate_any() {
            insert_str(&mut opts.user_defined, SUFFIX_REPLICATION_GENERATION, Uuid::new_v4().to_string());
            insert_str(&mut opts.user_defined, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(
                &mut opts.user_defined,
                SUFFIX_REPLICATION_STATUS,
                dsc.pending_status().unwrap_or_default(),
            );
        }

        let cache_adapter = self.object_data_cache();
        let cache_invalidate_before_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &key).await;
        rustfs_io_metrics::record_put_object_stage_duration_from(
            "app_cache_invalidate_before",
            cache_invalidate_before_stage_start,
        );

        let store_put_watchdog = tokio_util::sync::CancellationToken::new();
        spawn_traced({
            let store_put_watchdog = store_put_watchdog.clone();
            let request_id = request_id.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            let put_path = put_path.to_string();
            async move {
                tokio::select! {
                    _ = store_put_watchdog.cancelled() => {}
                    _ = tokio::time::sleep(PUT_OBJECT_STORE_WARN_THRESHOLD) => {
                        warn!(
                            target: "rustfs::app::object_usecase",
                            event = EVENT_PUT_OBJECT_STORE_INFLIGHT_SLOW,
                            component = LOG_COMPONENT_APP,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            request_id = %request_id,
                            bucket = %bucket,
                            key = %key,
                            put_path = %put_path,
                            object_size = actual_size,
                            threshold_ms = PUT_OBJECT_STORE_WARN_THRESHOLD.as_millis() as u64,
                            state = "store_put_pending",
                            "PutObject store write remains in flight"
                        );
                    }
                }
            }
        });

        let object_traffic_health = if use_zero_copy_eager_put_path || use_empty_or_small_eager_put_path {
            self.object_traffic_health()
        } else {
            None
        };
        let put_commit = spawn_traced_join({
            let store = Arc::clone(&store);
            let bucket = bucket.clone();
            let key = key.clone();
            let opts = opts.clone();
            let cache_adapter = cache_adapter.clone();
            let request_id = request_id.clone();
            let put_path = put_path.to_string();
            let put_admission = put_admission;
            async move {
                let _put_admission = put_admission;
                let object_traffic_progress = object_traffic_health
                    .as_deref()
                    .and_then(ObjectTrafficHealth::track_write_storage);
                let mut reader = reader;
                let store_put_stage_start = put_stage_metrics_enabled.then(Instant::now);
                let (obj_info, backfilled_old_current_size) = match store
                    .put_object_with_old_current_size(&bucket, &key, &mut reader, &opts)
                    .await
                    .map_err(ApiError::from)
                {
                    Ok(obj_info) => {
                        store_put_watchdog.cancel();
                        debug!(
                            target: "rustfs::app::object_usecase",
                            event = EVENT_PUT_OBJECT_STORE_RETURNED,
                            component = LOG_COMPONENT_APP,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            request_id = %request_id,
                            bucket = %bucket,
                            key = %key,
                            put_path = %put_path,
                            object_size = actual_size,
                            duration_ms = start_time.elapsed().as_millis() as u64,
                            result = "success",
                            "PutObject store write returned"
                        );
                        obj_info
                    }
                    Err(err) => {
                        store_put_watchdog.cancel();
                        rustfs_io_metrics::record_put_object_stage_duration_from("app_store_put", store_put_stage_start);
                        warn!(
                            target: "rustfs::app::object_usecase",
                            event = EVENT_PUT_OBJECT_STORE_RETURNED,
                            component = LOG_COMPONENT_APP,
                            subsystem = LOG_SUBSYSTEM_OBJECT,
                            request_id = %request_id,
                            bucket = %bucket,
                            key = %key,
                            put_path = %put_path,
                            object_size = actual_size,
                            duration_ms = start_time.elapsed().as_millis() as u64,
                            result = "error",
                            error = %err,
                            "PutObject store write returned"
                        );
                        return Err(err.into());
                    }
                };
                rustfs_io_metrics::record_put_object_stage_duration_from("app_store_put", store_put_stage_start);
                drop(_put_admission);
                drop(object_traffic_progress);
                #[cfg(test)]
                wait_for_put_post_store_test_hook(&bucket).await;

                let post_store_stage_start = put_stage_metrics_enabled.then(Instant::now);
                maybe_enqueue_transition_immediate(&obj_info, LcEventSrc::S3PutObject).await;
                let _ = invalidate_object_data_cache_after_put_success(&cache_adapter, &bucket, &key).await;

                let put_versioned = BucketVersioningSys::prefix_enabled(&bucket, &key).await;
                // Fast in-memory update for immediate quota and admin usage consistency.
                // The previous current size comes from the prelookup when it ran,
                // otherwise from the rename_data backfill (rustfs/backlog#1009); the
                // backfill reproduces the lookup's observation bit for bit (latest
                // version's ObjectInfo.size — 0 for a delete-marker latest — or
                // not-found → None).
                let committed_size = quota_accounting_object_size(&obj_info, quota_enabled)?;
                match prelookup_previous_current_size.or_else(|| previous_current_size_from_backfill(backfilled_old_current_size))
                {
                    Some(previous_current_size) => {
                        if put_versioned {
                            record_bucket_object_version_write_memory(&bucket, previous_current_size, committed_size).await;
                        } else {
                            record_bucket_object_write_memory(&bucket, previous_current_size, committed_size).await;
                        }
                    }
                    None => {
                        // Neither source could determine the previous state (peers
                        // predating the backfill field during a rolling upgrade, or
                        // sub-quorum metadata divergence). Record the components that
                        // are correct regardless; the next authoritative scanner
                        // refresh replaces the in-memory numbers.
                        debug!(
                            target: "rustfs::app::object_usecase",
                            bucket = %bucket,
                            key = %key,
                            put_versioned,
                            "put_object old-size backfill unknown; recording degraded usage delta"
                        );
                        record_bucket_object_write_unknown_previous_memory(&bucket, committed_size, put_versioned).await;
                    }
                }

                if dsc.replicate_any() {
                    schedule_object_replication(obj_info.clone(), store, dsc).await;
                }

                rustfs_scanner::record_dirty_usage_object(&bucket, &key);
                rustfs_io_metrics::record_put_object_stage_duration_from("app_post_store_bookkeeping", post_store_stage_start);

                let capacity_update_stage_start = put_stage_metrics_enabled.then(Instant::now);
                let manager = get_capacity_manager();
                manager.record_write_operation().await;
                rustfs_io_metrics::record_put_object_stage_duration_from("app_capacity_update", capacity_update_stage_start);

                Ok::<_, S3Error>(PutObjectCommitResult { obj_info, put_versioned })
            }
        });
        let commit_stage_start = put_stage_metrics_enabled.then(Instant::now);
        let put_commit_result = if let Some(cancellation) = eager_put_commit_cancellation {
            EagerPutCommitOwner::new(put_commit, cancellation, EAGER_PUT_COMMIT_CANCELLATION_GRACE)
                .join()
                .await
        } else {
            put_commit.await
        };
        rustfs_io_metrics::record_put_object_stage_duration_from("store_commit", commit_stage_start);
        let PutObjectCommitResult { obj_info, put_versioned } = match put_commit_result {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => {
                put_request_guard.finish_err();
                return Err(fail_put_object(completion, err));
            }
            Err(err) => {
                put_request_guard.finish_err();
                return Err(fail_put_object(
                    completion,
                    S3Error::with_message(S3ErrorCode::InternalError, format!("put object commit owner task failed: {err}")),
                ));
            }
        };

        completion = completion.object(obj_info.clone());
        if let Some(version_id) = obj_info.version_id {
            completion = completion.version_id(version_id.to_string());
        }

        Ok(PutObjectCommitted {
            obj_info,
            put_versioned,
            effective_sse,
            effective_kms_key_id,
            sse_customer_algorithm,
            sse_customer_key_md5,
            put_extra_checksum_headers,
            completion,
            put_request_guard,
            bucket,
            key,
            start_time,
            size,
            use_zero_copy_eager_put_path,
            concurrent_put_requests,
            buffer_size,
        })
    }
}

/// rustfs/backlog#1009: map the rename_data old-size backfill onto the
/// `previous_current_size` value the usage-accounting helpers expect. Outer
/// `None` = unknown (no quorum agreement, or a peer predates the field) — the
/// caller must fall back to the degraded accounting path.
pub(super) fn previous_current_size_from_backfill(backfill: Option<OldCurrentSize>) -> Option<Option<u64>> {
    backfill.map(|observation| match observation {
        OldCurrentSize::Present(size) => Some(size.max(0) as u64),
        OldCurrentSize::Absent => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use http::{HeaderMap, HeaderName, HeaderValue, Method};
    use s3s::dto::{
        DefaultRetention, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockRule, ServerSideEncryptionByDefault,
        ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
    };
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, ReadBuf};

    #[tokio::test]
    async fn cancelled_eager_put_commit_owner_reaps_stalled_storage_task() {
        let health = Arc::new(ObjectTrafficHealth::enabled_for_test(Duration::ZERO));
        let task_health = Arc::clone(&health);
        let cancellation = tokio_util::sync::CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = spawn_traced_join(async move {
            let _progress = task_health.track_write_storage().expect("write tracking must be enabled");
            task_cancellation.cancelled().await;
        });
        let owner = EagerPutCommitOwner::new(task, cancellation, Duration::from_millis(10));
        let request = spawn_traced_join(owner.join());

        tokio::time::timeout(Duration::from_secs(2), async {
            while !health.snapshot().write_stalled {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stalled owner must publish write-storage progress");

        request.abort();
        let _ = request.await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while health.snapshot().write_stalled {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled owner must abort and reap the stalled storage task");
    }

    #[tokio::test]
    async fn max_content_length_stream_rejects_the_first_chunk_over_limit() {
        let inner = StreamingBlob::wrap(futures::stream::iter([
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"1234")),
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"56")),
        ]));
        let mut limited = MaxContentLengthStream {
            inner,
            limit: 5,
            received: 0,
            exceeded: false,
        };

        assert_eq!(limited.next().await.unwrap().unwrap(), Bytes::from_static(b"1234"));
        let error = limited.next().await.unwrap().unwrap_err();
        assert!(error.downcast_ref::<UploadLimitExceeded>().is_some());
        assert!(limited.next().await.is_none());
    }

    #[tokio::test]
    async fn max_content_length_stream_allows_exact_limit() {
        let inner = StreamingBlob::from_bytes(Bytes::from_static(b"12345"));
        let mut limited = MaxContentLengthStream {
            inner,
            limit: 5,
            received: 0,
            exceeded: false,
        };

        assert_eq!(limited.next().await.unwrap().unwrap(), Bytes::from_static(b"12345"));
        assert!(limited.next().await.is_none());
    }

    #[test]
    fn put_request_user_metadata_cannot_suppress_bucket_default_retention() {
        let mut metadata =
            HashMap::from([(AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), ObjectLockRetentionMode::GOVERNANCE.to_string())]);
        apply_put_request_metadata(
            &mut metadata,
            &HeaderMap::new(),
            "object",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let state = metadata_sys::ObjectLockConfigState::Configured {
            config: ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: Some(ObjectLockRule {
                    default_retention: Some(DefaultRetention {
                        mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::COMPLIANCE)),
                        days: Some(1),
                        years: None,
                    }),
                }),
            },
            updated_at: OffsetDateTime::now_utc(),
        };
        apply_bucket_default_lock_retention("bucket", &state, &mut metadata, false).unwrap();

        assert_eq!(metadata.get(AMZ_OBJECT_LOCK_MODE_LOWER).map(String::as_str), Some("COMPLIANCE"));
        assert!(metadata.contains_key(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER));
        assert_eq!(metadata.get("x-amz-meta-x-amz-object-lock-mode").map(String::as_str), Some("GOVERNANCE"));

        let mut replication_headers = HeaderMap::new();
        insert_header(&mut replication_headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        insert_header(
            &mut replication_headers,
            rustfs_utils::http::SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP,
            "2026-01-01T00:00:00Z",
        );
        let mut replica_metadata = HashMap::new();
        let explicit_clear = has_replication_retention_update(&replication_headers, true);
        apply_bucket_default_lock_retention("bucket", &state, &mut replica_metadata, explicit_clear).unwrap();
        assert!(!replica_metadata.contains_key(AMZ_OBJECT_LOCK_MODE_LOWER));
        assert!(!replica_metadata.contains_key(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER));
    }

    /// rustfs/backlog#1009: the backfill→accounting mapping must mirror the
    /// prelookup exactly — a live latest version maps to `Some(size)` (clamped
    /// at 0 like the prelookup's `.max(0)`), absent/delete-marker maps to
    /// `None`, and an unknown backfill maps to outer `None` so the caller
    /// takes the degraded path instead of fabricating "new object".
    #[test]
    fn previous_current_size_from_backfill_mirrors_prelookup_semantics() {
        assert_eq!(previous_current_size_from_backfill(Some(OldCurrentSize::Present(42))), Some(Some(42)));
        assert_eq!(previous_current_size_from_backfill(Some(OldCurrentSize::Present(-7))), Some(Some(0)));
        assert_eq!(previous_current_size_from_backfill(Some(OldCurrentSize::Absent)), Some(None));
        assert_eq!(previous_current_size_from_backfill(None), None);
    }

    #[test]
    fn should_use_zero_copy_rejects_boundary_at_1mb() {
        let headers = HeaderMap::new();

        assert!(!should_use_zero_copy(1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_small_objects() {
        let headers = HeaderMap::new();

        assert!(!should_use_zero_copy(1024 * 1024 - 1, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_one_megabyte() {
        let headers = HeaderMap::new();

        assert!(!should_use_zero_copy(1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_encrypted_requests() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION, HeaderValue::from_static("AES256"));

        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn object_progress_tracks_real_get_and_small_put_lock_waits() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let object_traffic_health = Arc::new(ObjectTrafficHealth::enabled_for_test(Duration::ZERO));
        let context = temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_DATA_CACHE_ENABLE, Some("false"))], async {
            crate::app::gating_test_env::app_context_with_object_traffic_health(Arc::clone(&object_traffic_health)).await
        })
        .await;
        let store = context.object_store();
        let bucket = format!("object-progress-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("object progress bucket must be created");
        put_real_cold_fill_object(&store, &bucket, object, b"initial").await;

        let metadata_entered = Arc::new(tokio::sync::Barrier::new(2));
        let metadata_resume = Arc::new(tokio::sync::Barrier::new(2));
        crate::storage::options::install_versioning_config_test_hook(
            bucket.clone(),
            Arc::clone(&metadata_entered),
            Arc::clone(&metadata_resume),
        );
        let metadata_input = GetObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .expect("metadata GET input must build");
        let metadata_usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));
        let metadata_get = tokio::spawn(async move {
            metadata_usecase
                .execute_get_object(build_request(metadata_input, Method::GET))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), metadata_entered.wait())
            .await
            .expect("GET must enter the bucket metadata stage");
        assert!(object_traffic_health.snapshot().read_stalled);
        assert!(!metadata_get.is_finished(), "GET must still be waiting in bucket metadata");
        metadata_resume.wait().await;
        let metadata_response = tokio::time::timeout(Duration::from_secs(10), metadata_get)
            .await
            .expect("metadata GET must finish after release")
            .expect("metadata GET task must join")
            .expect("metadata GET must succeed after release");
        assert!(!object_traffic_health.snapshot().read_stalled);
        drop(metadata_response);

        let read_lock = store
            .new_ns_lock(&bucket, object)
            .await
            .expect("read test namespace lock must be created")
            .get_write_lock(Duration::from_secs(5))
            .await
            .expect("read test namespace lock must be held");
        let get_input = GetObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .expect("GET input must build");
        let get_usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));
        let get = tokio::spawn(async move { get_usecase.execute_get_object(build_request(get_input, Method::GET)).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !object_traffic_health.read_storage_stalled_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocked GET must publish a storage stall");
        assert!(!get.is_finished(), "GET must still be waiting for the held namespace lock");
        drop(read_lock);
        let get_response = tokio::time::timeout(Duration::from_secs(10), get)
            .await
            .expect("GET must finish after releasing the lock")
            .expect("GET task must join")
            .expect("GET must succeed after releasing the lock");
        assert!(!object_traffic_health.snapshot().read_stalled);
        drop(get_response);

        let write_lock = store
            .new_ns_lock(&bucket, object)
            .await
            .expect("write test namespace lock must be created")
            .get_write_lock(Duration::from_secs(5))
            .await
            .expect("write test namespace lock must be held");
        let post_store_entered = Arc::new(tokio::sync::Barrier::new(2));
        let post_store_resume = Arc::new(tokio::sync::Barrier::new(2));
        install_put_post_store_test_hook(bucket.clone(), Arc::clone(&post_store_entered), Arc::clone(&post_store_resume));
        let payload = Bytes::from_static(b"replacement");
        let put_input = PutObjectInput::builder()
            .bucket(bucket)
            .key(object.to_string())
            .body(Some(StreamingBlob::from(s3s::Body::from(payload.clone()))))
            .content_length(Some(i64::try_from(payload.len()).expect("test payload length must fit i64")))
            .build()
            .expect("PUT input must build");
        let put_usecase = DefaultObjectUsecase::with_context(Some(context));
        let put = tokio::spawn(async move {
            put_usecase
                .execute_put_object(&FS::new(), build_request(put_input, Method::PUT))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !object_traffic_health.snapshot().write_stalled {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocked small PUT must publish a storage stall");
        assert!(!put.is_finished(), "PUT must still be waiting for the held namespace lock");
        drop(write_lock);
        tokio::time::timeout(Duration::from_secs(10), post_store_entered.wait())
            .await
            .expect("PUT must reach the first post-store hook");
        assert!(!object_traffic_health.snapshot().write_stalled);
        assert!(!put.is_finished(), "PUT must remain blocked after the store guard has ended");
        post_store_resume.wait().await;
        tokio::time::timeout(Duration::from_secs(10), put)
            .await
            .expect("PUT must finish after releasing the lock")
            .expect("PUT task must join")
            .expect("PUT must succeed after releasing the lock");
        let recovered = object_traffic_health.snapshot();
        assert!(!recovered.read_stalled);
        assert!(!recovered.write_stalled);
    }

    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn cancelled_put_request_completes_post_commit_publication() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let (store, context) = real_cold_fill_test_context().await;
        let bucket = format!("put-owner-tail-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("PUT owner-tail bucket must be created");

        let old_body = Bytes::from_static(b"old body that must be invalidated");
        let old_info = put_real_cold_fill_object(&store, &bucket, object, &old_body).await;
        let adapter = context.object_data_cache();
        let old_plan = real_cold_fill_plan(&adapter, &bucket, object, &old_info);

        let post_store_entered = Arc::new(tokio::sync::Barrier::new(2));
        let post_store_resume = Arc::new(tokio::sync::Barrier::new(2));
        install_put_post_store_test_hook(bucket.clone(), Arc::clone(&post_store_entered), Arc::clone(&post_store_resume));

        let payload = Bytes::from_static(b"published despite caller cancellation");
        let put_input = PutObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .body(Some(StreamingBlob::from(s3s::Body::from(payload.clone()))))
            .content_length(Some(i64::try_from(payload.len()).expect("test payload length must fit i64")))
            .build()
            .expect("PUT input must build");
        let put_usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));
        let put = tokio::spawn(async move {
            put_usecase
                .execute_put_object(&FS::new(), build_request(put_input, Method::PUT))
                .await
        });

        tokio::time::timeout(Duration::from_secs(10), post_store_entered.wait())
            .await
            .expect("PUT must reach the post-store owner-tail hook");
        assert_eq!(
            adapter.fill_body(&old_plan, old_body.clone()).await,
            rustfs_object_data_cache::ObjectDataCacheFillResult::Inserted,
            "test must republish the old body while the owner tail is paused"
        );
        put.abort();
        post_store_resume.wait().await;
        let _ = put.await.expect_err("outer request task must be cancelled");

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if matches!(
                    adapter.lookup_body(&old_plan).await,
                    rustfs_object_data_cache::ObjectDataCacheLookup::Miss
                ) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("post-commit owner tail must invalidate stale body cache after caller cancellation");

        let recovered = store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("cancelled request's owned commit must still publish the object");
        assert_eq!(recovered.size, i64::try_from(payload.len()).expect("test payload length must fit i64"));
    }

    #[tokio::test]
    async fn object_progress_tracks_zero_byte_and_zero_copy_put_lock_waits() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let object_traffic_health = Arc::new(ObjectTrafficHealth::enabled_for_test(Duration::ZERO));
        let context =
            crate::app::gating_test_env::app_context_with_object_traffic_health(Arc::clone(&object_traffic_health)).await;
        let store = context.object_store();
        let bucket = format!("progress-buffered-{}", Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("buffered PUT progress bucket must be created");

        let extra_body_object = "zero-byte-extra.bin";
        let extra_body_input = PutObjectInput::builder()
            .bucket(bucket.clone())
            .key(extra_body_object.to_string())
            .body(Some(StreamingBlob::from(s3s::Body::from(Bytes::from_static(b"x")))))
            .content_length(Some(88))
            .build()
            .expect("zero-byte extra-body PUT input must build");
        let extra_body_usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));
        let mut extra_body_request = build_request(extra_body_input, Method::PUT);
        extra_body_request.headers = streaming_headers(Some("0"));
        let extra_body_err = extra_body_usecase
            .execute_put_object(&FS::new(), extra_body_request)
            .await
            .expect_err("decoded zero-byte PUT with body data must fail");
        assert_eq!(extra_body_err.code(), &S3ErrorCode::UnexpectedContent);
        assert!(!object_traffic_health.snapshot().write_stalled);
        let lookup_err = store
            .get_object_info(&bucket, extra_body_object, &ObjectOptions::default())
            .await
            .expect_err("rejected zero-byte PUT must not create an object");
        assert!(is_err_object_not_found(&lookup_err));

        let zero_object = "zero-byte.bin";
        let zero_write_lock = store
            .new_ns_lock(&bucket, zero_object)
            .await
            .expect("zero-byte PUT namespace lock must be created")
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("zero-byte PUT namespace lock must be held");
        let (body_polled_tx, body_polled_rx) = tokio::sync::oneshot::channel();
        let (body_release_tx, body_release_rx) = tokio::sync::oneshot::channel();
        let pending_zero_body = StreamingBlob::wrap(futures::stream::once(async move {
            body_polled_tx.send(()).expect("zero-byte body poll signal must be received");
            body_release_rx.await.expect("zero-byte body EOF must be released");
            Ok::<Bytes, std::io::Error>(Bytes::new())
        }));
        let zero_input = PutObjectInput::builder()
            .bucket(bucket.clone())
            .key(zero_object.to_string())
            .body(Some(pending_zero_body))
            .content_length(Some(87))
            .build()
            .expect("zero-byte PUT input must build");
        let zero_usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));
        let mut zero_request = build_request(zero_input, Method::PUT);
        zero_request.headers = streaming_headers(Some("0"));
        let zero_put = tokio::spawn(async move { zero_usecase.execute_put_object(&FS::new(), zero_request).await });

        tokio::time::timeout(Duration::from_secs(30), body_polled_rx)
            .await
            .expect("zero-byte PUT body must be polled for EOF")
            .expect("zero-byte PUT body poll signal must be sent");
        assert!(!object_traffic_health.snapshot().write_stalled);
        assert!(!zero_put.is_finished(), "zero-byte PUT must still be waiting for request EOF");

        body_release_tx.send(()).expect("zero-byte PUT body EOF must be released");
        tokio::time::timeout(Duration::from_secs(30), async {
            while !object_traffic_health.snapshot().write_stalled {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fully received zero-byte PUT must publish a storage stall");
        assert!(!zero_put.is_finished(), "zero-byte PUT must still be waiting for the held namespace lock");

        drop(zero_write_lock);
        tokio::time::timeout(Duration::from_secs(30), zero_put)
            .await
            .expect("zero-byte PUT must finish after releasing the lock")
            .expect("zero-byte PUT task must join")
            .expect("zero-byte PUT must succeed after releasing the lock");
        assert!(!object_traffic_health.snapshot().write_stalled);

        let zero_copy_object = "zero-copy-eager.jpg";
        let zero_copy_payload = Bytes::from(vec![b'z'; 1024 * 1024 + 1]);
        let zero_copy_size = i64::try_from(zero_copy_payload.len()).expect("zero-copy payload length must fit i64");
        let zero_copy_headers = HeaderMap::new();
        assert!(!is_disk_compressible(&zero_copy_headers, zero_copy_object));
        assert_eq!(
            zero_copy_eager_put_path_status(zero_copy_size, &zero_copy_headers, false, false, false),
            PUT_EAGER_STATUS_ELIGIBLE,
            "test payload must exercise the production zero-copy eager path",
        );
        let zero_copy_write_lock = store
            .new_ns_lock(&bucket, zero_copy_object)
            .await
            .expect("zero-copy PUT namespace lock must be created")
            .get_write_lock(Duration::from_secs(30))
            .await
            .expect("zero-copy PUT namespace lock must be held");
        let zero_copy_input = PutObjectInput::builder()
            .bucket(bucket)
            .key(zero_copy_object.to_string())
            .body(Some(StreamingBlob::from(s3s::Body::from(zero_copy_payload))))
            .content_length(Some(zero_copy_size))
            .build()
            .expect("zero-copy PUT input must build");
        let zero_copy_usecase = DefaultObjectUsecase::with_context(Some(context));
        let zero_copy_put = tokio::spawn(async move {
            zero_copy_usecase
                .execute_put_object(&FS::new(), build_request(zero_copy_input, Method::PUT))
                .await
        });

        tokio::time::timeout(Duration::from_secs(30), async {
            while !object_traffic_health.snapshot().write_stalled {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocked zero-copy eager PUT must publish a storage stall");
        assert!(
            !zero_copy_put.is_finished(),
            "zero-copy PUT must still be waiting for the held namespace lock"
        );

        drop(zero_copy_write_lock);
        tokio::time::timeout(Duration::from_secs(30), zero_copy_put)
            .await
            .expect("zero-copy PUT must finish after releasing the lock")
            .expect("zero-copy PUT task must join")
            .expect("zero-copy PUT must succeed after releasing the lock");
        assert!(!object_traffic_health.snapshot().write_stalled);
    }

    #[tokio::test]
    async fn put_object_body_read_timeout_guard_aborts_on_stall() {
        // Inner stream never yields and never reports EOF (a proxy that forwarded
        // a partial body then went silent while holding the connection open).
        let inner = StreamingBlob::wrap(futures::stream::pending::<Result<Bytes, std::io::Error>>());
        let mut guarded = guard_put_object_body_read_timeout(
            inner,
            "test-bucket",
            "stalled-object",
            "req-1",
            Some(1024),
            Duration::from_millis(1),
        );

        let err = guarded
            .next()
            .await
            .expect("guard should yield a stall error")
            .expect_err("stalled body should return an error");
        let io_err = err
            .downcast_ref::<std::io::Error>()
            .expect("stall error should wrap an io::Error");
        assert_eq!(io_err.kind(), std::io::ErrorKind::TimedOut);

        // After a stall the guard terminates the stream instead of re-polling the
        // abandoned inner stream.
        assert!(guarded.next().await.is_none());
    }

    #[tokio::test]
    async fn put_object_body_read_timeout_guard_preserves_length_and_passes_through() {
        let body = StreamingBlob::from(s3s::Body::from(Bytes::from_static(b"hello world")));
        assert_eq!(body.remaining_length().exact(), Some(11));

        let mut guarded =
            guard_put_object_body_read_timeout(body, "test-bucket", "ok-object", "req-2", Some(11), Duration::from_secs(60));
        // remaining_length must be forwarded, not reset to unknown.
        assert_eq!(guarded.remaining_length().exact(), Some(11));

        let mut collected = Vec::new();
        while let Some(chunk) = guarded.next().await {
            collected.extend_from_slice(&chunk.expect("chunk should read"));
        }
        assert_eq!(collected, b"hello world");
    }

    #[tokio::test]
    async fn put_object_body_read_timeout_guard_disabled_passthrough() {
        let body = StreamingBlob::from(s3s::Body::from(Bytes::from_static(b"data")));
        let mut guarded = guard_put_object_body_read_timeout(body, "test-bucket", "ok-object", "req-3", Some(4), Duration::ZERO);

        let mut collected = Vec::new();
        while let Some(chunk) = guarded.next().await {
            collected.extend_from_slice(&chunk.expect("chunk should read"));
        }
        assert_eq!(collected, b"data");
    }

    #[test]
    fn should_use_zero_copy_rejects_encrypted_requests_with_sse_customer_algorithm() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, HeaderValue::from_static("AES256"));

        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_encrypted_requests_with_kms_key_id() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, HeaderValue::from_static("test-kms-key-id"));

        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_zero_copy_rejects_compressible_content_types() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json; charset=utf-8"));

        assert!(!should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[test]
    fn should_use_small_eager_put_path_keeps_small_objects_eager() {
        let headers = HeaderMap::new();

        assert!(should_use_small_eager_put_path(1024, &headers, false, false, false));
        assert!(should_use_small_eager_put_path(128 * 1024, &headers, false, false, false));
        assert!(should_use_small_eager_put_path(512 * 1024, &headers, false, false, false));
        assert!(!should_use_small_eager_put_path(512 * 1024 + 1, &headers, false, false, false));
        assert!(!should_use_small_eager_put_path(1024 * 1024, &headers, false, false, false));
    }

    #[test]
    fn select_put_path_switches_at_small_eager_boundary() {
        let headers = HeaderMap::new();

        let (small_path, _, use_zero_copy, use_small_eager) = select_put_path(512 * 1024, &headers, false, false, false);
        assert_eq!(small_path, "small_eager");
        assert!(!use_zero_copy);
        assert!(use_small_eager);

        let (streaming_path, _, use_zero_copy, use_small_eager) = select_put_path(512 * 1024 + 1, &headers, false, false, false);
        assert_eq!(streaming_path, "streaming");
        assert!(!use_zero_copy);
        assert!(!use_small_eager);
    }

    #[test]
    fn select_put_path_treats_bucket_default_sse_as_encrypted() {
        for (algorithm, kms_key_id) in [
            (ServerSideEncryption::AES256, None),
            (ServerSideEncryption::AWS_KMS, Some("bucket-key")),
        ] {
            let config = ServerSideEncryptionConfiguration {
                rules: vec![ServerSideEncryptionRule {
                    apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                        sse_algorithm: ServerSideEncryption::from_static(algorithm),
                        kms_master_key_id: kms_key_id.map(|id| SSEKMSKeyId::from(id.to_string())),
                    }),
                    blocked_encryption_types: None,
                    bucket_key_enabled: None,
                }],
            };
            let (effective_sse, effective_kms_key_id) = resolve_bucket_default_sse(Some(&config), None, None, false);
            let encryption_requested = effective_sse.is_some() || effective_kms_key_id.is_some();

            for size in [512 * 1024, 2 * 1024 * 1024] {
                let (path, status, use_zero_copy, use_small_eager) =
                    select_put_path_with_concurrency(size, &HeaderMap::new(), encryption_requested, false, false, 256);

                assert_eq!(path, "streaming");
                assert_eq!(status, PUT_EAGER_STATUS_ENCRYPTED);
                assert!(!use_zero_copy);
                assert!(!use_small_eager);
            }
        }
    }

    #[test]
    fn should_use_small_eager_put_path_allows_a_b_override_at_1mb() {
        let headers = HeaderMap::new();

        assert!(should_use_small_eager_put_path_with_max_size(
            1024 * 1024,
            &headers,
            false,
            false,
            false,
            1024 * 1024,
        ));
        assert!(!should_use_small_eager_put_path_with_max_size(
            1024 * 1024 + 1,
            &headers,
            false,
            false,
            false,
            1024 * 1024,
        ));
    }

    #[test]
    fn should_use_small_eager_put_path_keeps_conditional_1mb_writes_atomic() {
        for header in [http::header::IF_MATCH, http::header::IF_NONE_MATCH] {
            let mut headers = HeaderMap::new();
            headers.insert(header, HeaderValue::from_static("*"));

            assert!(should_use_small_eager_put_path_with_max_size(
                1024 * 1024,
                &headers,
                false,
                false,
                false,
                512 * 1024,
            ));
            assert!(!should_use_small_eager_put_path_with_max_size(
                1024 * 1024 + 1,
                &headers,
                false,
                false,
                false,
                512 * 1024,
            ));
        }
    }

    #[test]
    fn should_use_small_eager_put_path_rejects_sse_requests() {
        let headers = HeaderMap::new();

        assert!(!should_use_small_eager_put_path(1024, &headers, true, false, false));
    }

    #[test]
    fn should_use_small_eager_put_path_rejects_compressible_objects() {
        let headers = HeaderMap::new();

        assert!(!should_use_small_eager_put_path(1024, &headers, false, true, false));
    }

    #[test]
    fn should_use_small_eager_put_path_rejects_extract_requests() {
        let headers = HeaderMap::new();

        assert!(!should_use_small_eager_put_path(1024, &headers, false, false, true));
    }

    #[test]
    fn should_use_small_eager_put_path_rejects_large_or_empty_objects() {
        let headers = HeaderMap::new();

        assert!(!should_use_small_eager_put_path(0, &headers, false, false, false));
        assert!(!should_use_small_eager_put_path(1024 * 1024 + 1, &headers, false, false, false));
    }

    #[test]
    fn dynamic_small_eager_threshold_sheds_memory_only_above_concurrency_limits() {
        assert_eq!(dynamic_small_eager_put_max_size_bytes(0), 512 * 1024);
        assert_eq!(dynamic_small_eager_put_max_size_bytes(SMALL_EAGER_CONCURRENCY_SOFT_LIMIT), 512 * 1024);
        assert_eq!(dynamic_small_eager_put_max_size_bytes(SMALL_EAGER_CONCURRENCY_SOFT_LIMIT + 1), 256 * 1024);
        assert_eq!(dynamic_small_eager_put_max_size_bytes(SMALL_EAGER_CONCURRENCY_HARD_LIMIT + 1), 128 * 1024);
    }

    #[test]
    fn dynamic_small_eager_threshold_preserves_tiny_objects_under_pressure() {
        let headers = HeaderMap::new();

        let (path, _, _, small_eager) = select_put_path_with_concurrency(128 * 1024, &headers, false, false, false, 256);
        assert_eq!(path, "small_eager");
        assert!(small_eager);

        let (path, _, _, small_eager) = select_put_path_with_concurrency(256 * 1024, &headers, false, false, false, 256);
        assert_eq!(path, "streaming");
        assert!(!small_eager);
    }

    #[test]
    fn should_use_zero_copy_eager_put_path_allows_large_plain_objects_within_cap() {
        let headers = HeaderMap::new();

        assert!(should_use_zero_copy_eager_put_path(2 * 1024 * 1024, &headers, false, false, false));
        assert!(should_use_zero_copy_eager_put_path(16 * 1024 * 1024, &headers, false, false, false));
        assert!(!should_use_zero_copy_eager_put_path(16 * 1024 * 1024 + 1, &headers, false, false, false));
        assert_eq!(
            zero_copy_eager_put_path_status(16 * 1024 * 1024, &headers, false, false, false),
            PUT_EAGER_STATUS_ELIGIBLE
        );
        assert_eq!(
            zero_copy_eager_put_path_status(16 * 1024 * 1024 + 1, &headers, false, false, false),
            PUT_EAGER_STATUS_ABOVE_EAGER_MAX
        );
    }

    #[test]
    fn zero_copy_eager_put_path_status_honors_configured_cap() {
        let headers = HeaderMap::new();
        let max_size = 64 * 1024 * 1024;

        assert_eq!(
            zero_copy_eager_put_path_status_with_max_size(33 * 1024 * 1024, &headers, false, false, false, max_size),
            PUT_EAGER_STATUS_ELIGIBLE
        );
        assert_eq!(
            zero_copy_eager_put_path_status_with_max_size(65 * 1024 * 1024, &headers, false, false, false, max_size),
            PUT_EAGER_STATUS_ABOVE_EAGER_MAX
        );
    }

    #[test]
    fn should_use_zero_copy_eager_put_path_rejects_compression_sse_and_extract() {
        let headers = HeaderMap::new();

        assert!(!should_use_zero_copy_eager_put_path(2 * 1024 * 1024, &headers, true, false, false));
        assert!(!should_use_zero_copy_eager_put_path(2 * 1024 * 1024, &headers, false, true, false));
        assert!(!should_use_zero_copy_eager_put_path(2 * 1024 * 1024, &headers, false, false, true));
        assert_eq!(
            zero_copy_eager_put_path_status(2 * 1024 * 1024, &headers, true, false, false),
            PUT_EAGER_STATUS_ENCRYPTED
        );
        assert_eq!(
            zero_copy_eager_put_path_status(2 * 1024 * 1024, &headers, false, true, false),
            PUT_EAGER_STATUS_COMPRESSED
        );
        assert_eq!(
            zero_copy_eager_put_path_status(2 * 1024 * 1024, &headers, false, false, true),
            PUT_EAGER_STATUS_EXTRACT
        );
    }

    #[tokio::test]
    async fn read_small_put_body_maps_upload_stream_sha256_mismatch_to_bad_digest() {
        let body = StreamReader::new(futures::stream::iter(vec![Err::<Bytes, std::io::Error>(s3s_body_error_to_io(Box::new(
            MockUploadStreamSha256Mismatch,
        )))]));

        let error = read_small_put_body_exact_direct(body, 1)
            .await
            .expect_err("SHA256 mismatch should reject the small PUT body");

        assert_eq!(error.code(), &S3ErrorCode::BadDigest);
    }

    #[tokio::test]
    async fn read_zero_copy_put_body_maps_upload_stream_sha256_mismatch_to_bad_digest() {
        let body = futures::stream::iter(vec![Err::<Bytes, MockUploadStreamSha256Mismatch>(MockUploadStreamSha256Mismatch)]);

        let error = match read_zero_copy_put_body_exact(body, 1).await {
            Ok(_) => panic!("SHA256 mismatch should reject the zero-copy PUT body"),
            Err(error) => error,
        };

        assert_eq!(error.code(), &S3ErrorCode::BadDigest);
    }

    struct FragmentedBody {
        data: std::io::Cursor<Vec<u8>>,
    }

    impl AsyncRead for FragmentedBody {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            let position = usize::try_from(self.data.position()).expect("test cursor position should fit usize");
            let remaining = &self.data.get_ref()[position..];
            let copied = remaining.len().min(buf.remaining()).min(2);
            buf.put_slice(&remaining[..copied]);
            self.data
                .set_position(u64::try_from(position + copied).expect("test cursor position should fit u64"));
            Poll::Ready(Ok(()))
        }
    }

    struct InitializedLengthProbe {
        data: std::io::Cursor<Vec<u8>>,
        initialized_lengths: Arc<Mutex<Vec<usize>>>,
    }

    impl AsyncRead for InitializedLengthProbe {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            self.initialized_lengths
                .lock()
                .expect("initialized-length probe lock should not poison")
                .push(buf.initialized().len());
            let position = usize::try_from(self.data.position()).expect("test cursor position should fit usize");
            let remaining = &self.data.get_ref()[position..];
            let copied = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..copied]);
            self.data
                .set_position(u64::try_from(position + copied).expect("test cursor position should fit u64"));
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn read_small_put_body_exact_pooled_reads_exact_bytes_without_prefill() {
        let pool = get_concurrency_manager().bytes_pool();
        let initialized_lengths = Arc::new(Mutex::new(Vec::new()));
        let body = InitializedLengthProbe {
            data: std::io::Cursor::new(b"hello".to_vec()),
            initialized_lengths: Arc::clone(&initialized_lengths),
        };

        let buffer = read_small_put_body_exact_pooled(body, 5, pool.as_ref())
            .await
            .expect("pooled exact read should succeed");

        assert_eq!(&buffer[..5], b"hello");
        assert_eq!(buffer.len(), 5);
        assert_eq!(
            initialized_lengths
                .lock()
                .expect("initialized-length probe lock should not poison")[0],
            0,
            "the first pooled body read must use uninitialized spare capacity rather than a zero-filled slice"
        );
    }

    #[tokio::test]
    async fn read_small_put_body_exact_pooled_rejects_short_body() {
        let pool = get_concurrency_manager().bytes_pool();
        let body = std::io::Cursor::new(b"hell".to_vec());

        let err = match read_small_put_body_exact_pooled(body, 5, pool.as_ref()).await {
            Ok(_) => panic!("short pooled body should fail"),
            Err(err) => err,
        };

        assert_eq!(err.code(), &S3ErrorCode::IncompleteBody);
    }

    #[tokio::test]
    async fn read_small_put_body_exact_pooled_rejects_extra_body() {
        let pool = get_concurrency_manager().bytes_pool();
        let body = std::io::Cursor::new(b"hello!".to_vec());

        let err = match read_small_put_body_exact_pooled(body, 5, pool.as_ref()).await {
            Ok(_) => panic!("extra pooled body should fail"),
            Err(err) => err,
        };

        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[tokio::test]
    async fn read_small_put_body_exact_direct_reads_exact_bytes_without_prefill() {
        let body = std::io::Cursor::new(b"hello".to_vec());
        let reader = read_small_put_body_exact_direct(body, 5)
            .await
            .expect("direct exact read should succeed");

        assert_eq!(reader.get_ref().as_slice(), b"hello");
        assert_eq!(reader.get_ref().len(), 5);
    }

    #[tokio::test]
    async fn read_small_put_body_exact_direct_rejects_short_and_extra_bodies() {
        let short = read_small_put_body_exact_direct(std::io::Cursor::new(b"hell".to_vec()), 5)
            .await
            .expect_err("short direct body should fail");
        assert_eq!(short.code(), &S3ErrorCode::IncompleteBody);

        let extra = read_small_put_body_exact_direct(std::io::Cursor::new(b"hello!".to_vec()), 5)
            .await
            .expect_err("extra direct body should fail");
        assert_eq!(extra.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[tokio::test]
    async fn streaming_put_hash_reader_rejects_extra_byte_at_eager_boundary() {
        use tokio::io::AsyncReadExt;

        let declared_size = 512 * 1024;
        let declared_size_i64 = i64::try_from(declared_size).expect("test size should fit i64");
        let mut reader = HashReader::from_stream(
            std::io::Cursor::new(vec![0x5a; declared_size + 1]),
            declared_size_i64,
            declared_size_i64,
            None,
            None,
            false,
        )
        .expect("streaming PUT hash reader should be constructed");
        let mut body = Vec::new();

        let err = reader
            .read_to_end(&mut body)
            .await
            .expect_err("streaming PUT must reject a body larger than Content-Length");

        assert!(
            err.to_string().contains("more bytes than specified"),
            "unexpected extra-body error: {err}"
        );
        assert_eq!(body.len(), declared_size);
    }

    #[tokio::test]
    async fn read_small_put_body_exact_direct_handles_empty_body_boundary() {
        let empty = read_small_put_body_exact_direct(std::io::Cursor::new(Vec::<u8>::new()), 0)
            .await
            .expect("empty direct body should succeed");
        assert!(empty.get_ref().is_empty());

        let extra = read_small_put_body_exact_direct(std::io::Cursor::new(vec![1u8]), 0)
            .await
            .expect_err("non-empty body declared as empty should fail");
        assert_eq!(extra.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[tokio::test]
    async fn read_small_put_body_exact_direct_rejects_error_after_partial_read() {
        struct PartialThenError {
            delivered_prefix: bool,
        }

        impl AsyncRead for PartialThenError {
            fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
                if self.delivered_prefix {
                    return Poll::Ready(Err(std::io::Error::other("body read failed")));
                }

                self.delivered_prefix = true;
                buf.put_slice(b"he");
                Poll::Ready(Ok(()))
            }
        }

        let err = read_small_put_body_exact_direct(PartialThenError { delivered_prefix: false }, 5)
            .await
            .expect_err("a partial body followed by an I/O error must fail");

        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn read_small_put_body_exact_direct_accepts_fragmented_body() {
        let reader = read_small_put_body_exact_direct(
            FragmentedBody {
                data: std::io::Cursor::new(b"hello".to_vec()),
            },
            5,
        )
        .await
        .expect("a fragmented exact-length body should succeed");

        assert_eq!(reader.get_ref().as_slice(), b"hello");
    }

    #[tokio::test]
    async fn read_small_put_body_exact_direct_rejects_fragmented_extra_body() {
        let err = read_small_put_body_exact_direct(
            FragmentedBody {
                data: std::io::Cursor::new(b"hello!".to_vec()),
            },
            5,
        )
        .await
        .expect_err("a fragmented body longer than declared must fail");

        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[tokio::test]
    async fn read_small_put_body_exact_direct_reads_into_uninitialized_spare_capacity() {
        let initialized_lengths = Arc::new(Mutex::new(Vec::new()));
        let body = InitializedLengthProbe {
            data: std::io::Cursor::new(b"hello".to_vec()),
            initialized_lengths: Arc::clone(&initialized_lengths),
        };

        let reader = read_small_put_body_exact_direct(body, 5)
            .await
            .expect("direct exact read should succeed");

        assert_eq!(reader.get_ref().as_slice(), b"hello");
        assert_eq!(
            initialized_lengths
                .lock()
                .expect("initialized-length probe lock should not poison")[0],
            0,
            "the first body read must use uninitialized spare capacity rather than a zero-filled slice"
        );
    }

    #[tokio::test]
    async fn read_zero_copy_put_body_exact_reads_chunked_body() {
        use tokio::io::AsyncReadExt;

        let body = futures::stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"hello ")),
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"world")),
        ]);

        let mut reader = read_zero_copy_put_body_exact(body, 11)
            .await
            .expect("zero-copy eager body read should succeed");
        let mut out = Vec::new();
        reader
            .read_to_end(&mut out)
            .await
            .expect("chunked bytes reader should be readable");

        assert_eq!(out, b"hello world");
    }

    #[tokio::test]
    async fn read_zero_copy_put_body_exact_rejects_extra_bytes() {
        let body = futures::stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"hello")),
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"!")),
        ]);

        let err = match read_zero_copy_put_body_exact(body, 5).await {
            Ok(_) => panic!("extra bytes should fail"),
            Err(err) => err,
        };

        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[tokio::test]
    async fn pooled_buffer_reader_keeps_buffer_alive_until_consumed() {
        use tokio::io::AsyncReadExt;

        let pool = get_concurrency_manager().bytes_pool();
        let body = std::io::Cursor::new(b"hello".to_vec());
        let buffer = read_small_put_body_exact_pooled(body, 5, pool.as_ref())
            .await
            .expect("pooled exact read should succeed");
        let mut reader = PooledBufferReader::new(buffer, 5);
        let mut out = Vec::new();

        reader.read_to_end(&mut out).await.expect("pooled reader should be readable");

        assert_eq!(out, b"hello");
    }

    #[test]
    fn should_use_zero_copy_allows_large_unencrypted_binary_objects() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));

        assert!(should_use_zero_copy(2 * 1024 * 1024, &headers));
    }

    #[tokio::test]
    async fn execute_put_object_rejects_post_object_sse_kms_from_input() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .server_side_encryption(Some(ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS)))
            .build()
            .unwrap();

        let mut req = build_request(input, Method::POST);
        req.extensions.insert(PostObjectRequestMarker);

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = Box::pin(usecase.execute_put_object(&fs, req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn execute_put_object_rejects_extract_sse_kms() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("archive.tar".to_string())
            .server_side_encryption(Some(ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS)))
            .build()
            .unwrap();

        let mut req = build_request(input, Method::PUT);
        req.headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("true"));

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = Box::pin(usecase.execute_put_object(&fs, req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn execute_put_object_extract_rejects_invalid_storage_class() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("archive.tar".to_string())
            .storage_class(Some(StorageClass::from_static("INVALID")))
            .build()
            .unwrap();

        let mut req = build_request(input, Method::PUT);
        req.headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("true"));

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = Box::pin(usecase.execute_put_object(&fs, req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidStorageClass);
    }

    #[tokio::test]
    async fn execute_put_object_rejects_post_object_sse_kms_from_headers() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let mut req = build_request(input, Method::POST);
        req.extensions.insert(PostObjectRequestMarker);
        req.headers
            .insert(AMZ_SERVER_SIDE_ENCRYPTION, HeaderValue::from_static("aws:kms"));

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = Box::pin(usecase.execute_put_object(&fs, req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn execute_put_object_rejects_post_object_sse_kms_key_id_header() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let mut req = build_request(input, Method::POST);
        req.extensions.insert(PostObjectRequestMarker);
        req.headers
            .insert(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, HeaderValue::from_static("test-kms-key-id"));

        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = Box::pin(usecase.execute_put_object(&fs, req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
    }

    #[tokio::test]
    async fn execute_put_object_rejects_invalid_storage_class() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .storage_class(Some(StorageClass::from_static("INVALID-STORAGE-CLASS")))
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = Box::pin(usecase.execute_put_object(&fs, req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidStorageClass);
    }

    // https://github.com/rustfs/backlog/issues/1311 — bucket-quota admission must run against the authoritative
    // decoded/plain object length, never the aws-chunked wire Content-Length, and must reject negative/unknown lengths.
    // https://github.com/rustfs/backlog/issues/1336 — but Content-Encoding: aws-chunked alone is only a declared
    // encoding: without a STREAMING-* payload the body is unframed and the wire Content-Length is authoritative.
    fn aws_chunked_headers(decoded_len: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONTENT_ENCODING, HeaderValue::from_static("aws-chunked"));
        if let Some(decoded) = decoded_len {
            headers.insert(
                HeaderName::from_bytes(AMZ_DECODED_CONTENT_LENGTH.as_bytes()).unwrap(),
                HeaderValue::from_str(decoded).unwrap(),
            );
        }
        headers
    }

    fn streaming_headers(decoded_len: Option<&str>) -> HeaderMap {
        let mut headers = aws_chunked_headers(decoded_len);
        headers.insert(
            HeaderName::from_bytes(AMZ_CONTENT_SHA256.as_bytes()).unwrap(),
            HeaderValue::from_static("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
        );
        headers
    }

    #[test]
    fn authoritative_size_prefers_aws_chunked_decoded_over_wire_content_length() {
        // Wire Content-Length (chunk framing) differs from the decoded object length; the decoded length wins.
        let headers = streaming_headers(Some("1000"));
        let size = resolve_put_object_authoritative_size(&headers, Some(1088)).expect("decoded length is authoritative");
        assert_eq!(
            size, 1000,
            "aws-chunked admission must use the decoded object length, not the framed wire length"
        );

        // A declared-only aws-chunked request that still carries a decoded length behaves the same.
        let headers = aws_chunked_headers(Some("1000"));
        let size = resolve_put_object_authoritative_size(&headers, Some(1088)).expect("decoded length is authoritative");
        assert_eq!(size, 1000);
    }

    #[test]
    fn authoritative_size_streaming_without_content_encoding_uses_decoded_length() {
        // A streaming payload signals framing via x-amz-content-sha256 alone; Content-Encoding is optional.
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_bytes(AMZ_CONTENT_SHA256.as_bytes()).unwrap(),
            HeaderValue::from_static("STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
        );
        headers.insert(
            HeaderName::from_bytes(AMZ_DECODED_CONTENT_LENGTH.as_bytes()).unwrap(),
            HeaderValue::from_static("1000"),
        );
        let size = resolve_put_object_authoritative_size(&headers, Some(1088)).expect("decoded length is authoritative");
        assert_eq!(
            size, 1000,
            "a streaming payload without Content-Encoding must still use the decoded length"
        );
    }

    #[test]
    fn authoritative_size_rejects_framed_body_without_decoded_length() {
        // A genuinely framed upload without x-amz-decoded-content-length has no authoritative size;
        // the framed wire length must NOT be a fallback.
        let headers = streaming_headers(None);
        let err = resolve_put_object_authoritative_size(&headers, Some(1088))
            .expect_err("framed upload without decoded length must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);

        // ... even when the wire Content-Length is also absent.
        let err =
            resolve_put_object_authoritative_size(&headers, None).expect_err("framed upload without any length must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[test]
    fn authoritative_size_declared_aws_chunked_without_streaming_uses_wire_content_length() {
        // backlog#1336: an SDK PUT that merely declares Content-Encoding: aws-chunked (issue #1857
        // clients) has an unframed body and no decoded length; the wire Content-Length is the real
        // object size and the request must be admitted, not rejected with UnexpectedContent.
        let headers = aws_chunked_headers(None);
        let size = resolve_put_object_authoritative_size(&headers, Some(1088))
            .expect("declared-only aws-chunked must fall back to the wire Content-Length");
        assert_eq!(size, 1088);

        // Same for a combined declared encoding (aws-chunked,gzip).
        let mut headers = HeaderMap::new();
        headers.insert(http::header::CONTENT_ENCODING, HeaderValue::from_static("aws-chunked,gzip"));
        let size = resolve_put_object_authoritative_size(&headers, Some(2048))
            .expect("declared-only aws-chunked,gzip must fall back to the wire Content-Length");
        assert_eq!(size, 2048);

        // Without any length information it is still rejected.
        let headers = aws_chunked_headers(None);
        let err = resolve_put_object_authoritative_size(&headers, None)
            .expect_err("declared-only aws-chunked with no length at all must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[test]
    fn authoritative_size_plain_put_uses_content_length() {
        let headers = HeaderMap::new();
        let size = resolve_put_object_authoritative_size(&headers, Some(4096)).expect("plain PUT uses Content-Length");
        assert_eq!(size, 4096);
    }

    #[test]
    fn authoritative_size_plain_put_falls_back_to_decoded_length() {
        // Non-chunked request that only surfaced an explicit decoded length.
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_bytes(AMZ_DECODED_CONTENT_LENGTH.as_bytes()).unwrap(),
            HeaderValue::from_static("2048"),
        );
        let size = resolve_put_object_authoritative_size(&headers, None).expect("decoded length is the fallback");
        assert_eq!(size, 2048);
    }

    #[test]
    fn authoritative_size_rejects_unknown_length() {
        let headers = HeaderMap::new();
        let err = resolve_put_object_authoritative_size(&headers, None).expect_err("no length information must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[test]
    fn authoritative_size_rejects_negative_length() {
        // A negative decoded length would wrap to an enormous unsigned size for quota/buffer sizing; reject it.
        let headers = aws_chunked_headers(Some("-1"));
        let err =
            resolve_put_object_authoritative_size(&headers, Some(64)).expect_err("negative decoded length must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);

        let plain = HeaderMap::new();
        let err =
            resolve_put_object_authoritative_size(&plain, Some(-100)).expect_err("negative Content-Length must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::UnexpectedContent);
    }

    #[test]
    fn authoritative_size_accepts_exact_and_rejects_negative_boundary() {
        // Exact zero-length object is admissible (the over-by-1/exact-limit boundary is enforced by the quota checker on this value).
        let headers = aws_chunked_headers(Some("0"));
        assert_eq!(
            resolve_put_object_authoritative_size(&headers, Some(87)).expect("zero-length decoded is valid"),
            0
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn quota_rejects_ciphertext_replication_before_polling_the_body() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let (_store, bucket) =
            crate::app::gating_test_env::durable_quota_test_bucket("ciphertext-replication-early-reject", 4096).await;
        let body_polled = Arc::new(AtomicBool::new(false));
        let body_polled_in_stream = Arc::clone(&body_polled);
        let body = StreamingBlob::wrap(futures::stream::once(async move {
            body_polled_in_stream.store(true, Ordering::Release);
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"ciphertext"))
        }));
        let input = PutObjectInput::builder()
            .bucket(bucket)
            .key("object".to_string())
            .body(Some(body))
            .content_length(Some(10))
            .build()
            .expect("ciphertext replication PUT input should build");
        let mut request = build_request(input, Method::PUT);
        insert_header(&mut request.headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        request
            .headers
            .insert(rustfs_utils::http::REPLICATION_SSEC_ALGORITHM_HEADER, HeaderValue::from_static("AES256"));
        request.extensions.insert(crate::storage::access::ReqInfo {
            replication_request_authorized: true,
            ..Default::default()
        });

        let err = DefaultObjectUsecase::from_global()
            .execute_put_object(&FS::new(), request)
            .await
            .expect_err("quota-enabled ciphertext replication should fail at ingress");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(!body_polled.load(Ordering::Acquire), "rejected ciphertext body must not be consumed");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_quota_rejects_full_put_before_polling_the_body() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
        use std::sync::atomic::{AtomicBool, Ordering};

        const GI_B: u64 = 1024 * 1024 * 1024;
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        let bucket = format!("legacy-quota-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create legacy quota test bucket");
        crate::app::storage_api::test::data_usage::seed_bucket_usage_memory_for_test(&bucket, 4 * GI_B).await;
        let metadata_sys = DefaultObjectUsecase::from_global()
            .bucket_metadata_sys()
            .expect("test app context should expose bucket metadata");
        QuotaChecker::new(metadata_sys)
            .set_quota_config(
                &bucket,
                BucketQuota {
                    quota: Some(5 * GI_B),
                    ..Default::default()
                },
            )
            .await
            .expect("configure legacy quota");

        let body_polled = Arc::new(AtomicBool::new(false));
        let body_polled_in_stream = Arc::clone(&body_polled);
        let body = StreamingBlob::wrap(futures::stream::once(async move {
            body_polled_in_stream.store(true, Ordering::Release);
            Ok::<Bytes, std::io::Error>(Bytes::new())
        }));
        let input = PutObjectInput::builder()
            .bucket(bucket)
            .key("object".to_string())
            .body(Some(body))
            .content_length(Some(i64::try_from(2 * GI_B).expect("test size should fit i64")))
            .build()
            .expect("legacy quota PUT input should build");

        let err = DefaultObjectUsecase::from_global()
            .execute_put_object(&FS::new(), build_request(input, Method::PUT))
            .await
            .expect_err("4 GiB used plus a 2 GiB PUT must exceed a 5 GiB legacy quota");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(!body_polled.load(Ordering::Acquire), "legacy quota rejection must not consume the body");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn concurrent_puts_share_durable_bucket_quota_reservations() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("concurrent-put-quota", 6000).await;

        let first_opts = ObjectOptions::default();
        let second_opts = ObjectOptions::default();
        let first_store = Arc::clone(&store);
        let first_bucket = bucket.clone();
        let first = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x73; 4096]);
            first_store.put_object(&first_bucket, "first", &mut reader, &first_opts).await
        });
        let second = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x74; 4096]);
            store.put_object(&bucket, "second", &mut reader, &second_opts).await
        });
        let (first, second) = tokio::join!(first, second);
        let first = first.expect("first PUT task should not panic");
        let second = second.expect("second PUT task should not panic");

        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        let denied = first.err().or_else(|| second.err()).expect("one PUT must be denied");
        assert!(matches!(
            denied,
            StorageError::QuotaExceeded {
                current: 4096,
                limit: 6000
            }
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn concurrent_within_limit_puts_keep_independent_mutation_fences() {
        use crate::app::storage_api::test::set_disk::{PutObjectCommitBarrier, PutObjectCommitPause};

        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("concurrent-fence-quota", 8192).await;
        let first_barrier = PutObjectCommitBarrier::install(&bucket, "first", PutObjectCommitPause::BeforeQuotaRename);
        let second_barrier = PutObjectCommitBarrier::install(&bucket, "second", PutObjectCommitPause::BeforeQuotaRename);

        let first_store = Arc::clone(&store);
        let first_bucket = bucket.clone();
        let first = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x75; 4096]);
            first_store
                .put_object(&first_bucket, "first", &mut reader, &ObjectOptions::default())
                .await
        });
        let second_store = Arc::clone(&store);
        let second_bucket = bucket.clone();
        let second = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x76; 4096]);
            second_store
                .put_object(&second_bucket, "second", &mut reader, &ObjectOptions::default())
                .await
        });

        first_barrier.wait_until_paused().await;
        second_barrier.wait_until_paused().await;
        first_barrier.release();
        second_barrier.release();

        first
            .await
            .expect("first PUT task should not panic")
            .expect("first within-limit PUT should commit");
        second
            .await
            .expect("second PUT task should not panic")
            .expect("second within-limit PUT should commit");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn put_rejects_rotated_quota_capability_before_rename() {
        use crate::app::storage_api::test::set_disk::{PutObjectCommitBarrier, PutObjectCommitPause};

        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("rotated-proof-put-quota", 4096).await;
        let barrier = PutObjectCommitBarrier::install(&bucket, "object", PutObjectCommitPause::BeforeQuotaRename);
        let put_store = Arc::clone(&store);
        let put_bucket = bucket.clone();
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x77; 4096]);
            put_store
                .put_object(&put_bucket, "object", &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;
        assert!(
            crate::storage::storage_api::ecstore_notification::rotate_cross_pool_fence_fleet_proof_for_test(),
            "the gating environment must have a current fleet proof"
        );
        barrier.release();

        let err = put
            .await
            .expect("PUT task should not panic")
            .expect_err("a replaced fleet proof must fence the authoritative rename");
        assert!(matches!(
            err,
            StorageError::NamespaceLockQuorumUnavailable {
                mode: "quota_reservation",
                ..
            }
        ));
        store
            .get_object_info(&bucket, "object", &ObjectOptions::default())
            .await
            .expect_err("proof rotation before rename must leave no committed object");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_put_has_zero_quota_growth() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("data-movement-put-quota", 0).await;
        let mut reader = PutObjReader::from_vec(vec![0x79; 4096]);
        let stored = store
            .put_object(
                &bucket,
                "object",
                &mut reader,
                &ObjectOptions {
                    data_movement: true,
                    ..Default::default()
                },
            )
            .await
            .expect("moving an already-accounted object between pools must have zero quota growth");
        assert_eq!(stored.size, 4096);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn cancelled_put_releases_durable_quota_reservation() {
        use crate::app::storage_api::test::set_disk::{PutObjectCommitBarrier, PutObjectCommitPause};

        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("cancelled-put-quota", 4096).await;

        let barrier = PutObjectCommitBarrier::install(&bucket, "cancelled", PutObjectCommitPause::AfterQuotaReservation);
        let cancelled_store = Arc::clone(&store);
        let cancelled_bucket = bucket.clone();
        let cancelled = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x51; 4096]);
            cancelled_store
                .put_object(&cancelled_bucket, "cancelled", &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;
        cancelled.abort();
        let cancelled_result = cancelled.await;
        assert!(cancelled_result.is_err(), "the paused request must be cancelled");
        drop(barrier);

        let mut replacement = PutObjReader::from_vec(vec![0x52; 4096]);
        store
            .put_object(&bucket, "replacement", &mut replacement, &ObjectOptions::default())
            .await
            .expect("cancelling before commit must release the complete reservation");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn cancelled_put_after_commit_marker_is_reconciled() {
        use crate::app::storage_api::test::set_disk::{PutObjectCommitBarrier, PutObjectCommitPause};

        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("cancelled-spawned-put-quota", 4096).await;
        let commit_barrier = PutObjectCommitBarrier::install(&bucket, "object", PutObjectCommitPause::BeforeQuotaRename);
        let first_store = Arc::clone(&store);
        let first_bucket = bucket.clone();
        let first = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x53; 4096]);
            first_store
                .put_object(&first_bucket, "object", &mut reader, &ObjectOptions::default())
                .await
        });
        commit_barrier.wait_until_paused().await;
        first.abort();
        assert!(first.await.is_err(), "the outer request task must be cancelled");
        drop(commit_barrier);

        store
            .get_object_info(&bucket, "object", &ObjectOptions::default())
            .await
            .expect_err("cancelling before rename must not commit the object");
        let mut replacement = PutObjReader::from_vec(vec![0x54; 4096]);
        store
            .put_object(&bucket, "replacement", &mut replacement, &ObjectOptions::default())
            .await
            .expect("the next admission must reap the abandoned commit marker");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn committed_put_survives_quota_ledger_settlement_failure() {
        use crate::app::storage_api::test::set_disk::{
            PutObjectCommitBarrier, PutObjectCommitPause, fail_next_quota_ledger_save_for_test,
        };

        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("settlement-failure-quota", 4096).await;
        let barrier = PutObjectCommitBarrier::install(&bucket, "object", PutObjectCommitPause::BeforeQuotaRename);
        let put_store = Arc::clone(&store);
        let put_bucket = bucket.clone();
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x59; 4096]);
            put_store
                .put_object(&put_bucket, "object", &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;
        fail_next_quota_ledger_save_for_test();
        barrier.release();
        put.await
            .expect("PUT task should not panic")
            .expect("a post-commit ledger failure must not change the successful write result");
        let stored = store
            .get_object_info(&bucket, "object", &ObjectOptions::default())
            .await
            .expect("the committed object must remain visible");
        assert_eq!(stored.size, 4096);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn suspended_null_version_overwrite_uses_exact_quota_delta() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("suspended-version-quota", 6200).await;
        let mut versioned_reader = PutObjReader::from_vec(vec![0x61; 4096]);
        store
            .put_object(
                &bucket,
                "object",
                &mut versioned_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("write UUID version");

        for (size, byte) in [(1024, 0x62), (2048, 0x63)] {
            let mut reader = PutObjReader::from_vec(vec![byte; size]);
            store
                .put_object(
                    &bucket,
                    "object",
                    &mut reader,
                    &ObjectOptions {
                        version_suspended: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("suspended write should replace only the exact null version");
        }

        let mut excess = PutObjReader::from_vec(vec![0x64; 57]);
        let err = store
            .put_object(&bucket, "excess", &mut excess, &ObjectOptions::default())
            .await
            .expect_err("UUID plus replacement null version must consume 6144 bytes");
        assert!(matches!(
            err,
            StorageError::QuotaExceeded {
                current: 6144,
                limit: 6200
            }
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn durable_quota_reservation_observes_lowered_config_revision() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("lowered-quota-revision", 8192).await;
        let mut initial = PutObjReader::from_vec(vec![0x71; 4096]);
        store
            .put_object(&bucket, "initial", &mut initial, &ObjectOptions::default())
            .await
            .expect("write under original quota");

        let metadata_sys = DefaultObjectUsecase::from_global()
            .bucket_metadata_sys()
            .expect("test app context should expose bucket metadata");
        QuotaChecker::new(metadata_sys)
            .set_quota_config(&bucket, BucketQuota::new(Some(4096)))
            .await
            .expect("lower bucket quota");
        let mut excess = PutObjReader::from_vec(vec![0x72]);
        let err = store
            .put_object(&bucket, "excess", &mut excess, &ObjectOptions::default())
            .await
            .expect_err("reservation must not use the stale larger quota revision");
        assert!(matches!(
            err,
            StorageError::QuotaExceeded {
                current: 4096,
                limit: 4096
            }
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn quota_enable_waits_for_unlimited_commit() {
        use crate::app::storage_api::test::metadata_sys::ConfigWriteLockProbe;
        use crate::app::storage_api::test::set_disk::{PutObjectCommitBarrier, PutObjectCommitPause};

        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("quota-config-fence", 8192).await;
        let metadata_sys = DefaultObjectUsecase::from_global()
            .bucket_metadata_sys()
            .expect("test app context should expose bucket metadata");
        QuotaChecker::new(Arc::clone(&metadata_sys))
            .set_quota_config(&bucket, BucketQuota::new(None))
            .await
            .expect("clear quota before the fenced write");
        let barrier = PutObjectCommitBarrier::install(&bucket, "object", PutObjectCommitPause::AfterQuotaReservation);
        let put_store = Arc::clone(&store);
        let put_bucket = bucket.clone();
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(vec![0x73; 4096]);
            put_store
                .put_object(&put_bucket, "object", &mut reader, &ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;

        let update_probe = ConfigWriteLockProbe::install(&bucket);
        let update_bucket = bucket.clone();
        let update = tokio::spawn(async move {
            QuotaChecker::new(metadata_sys)
                .set_quota_config(&update_bucket, BucketQuota::new(Some(0)))
                .await
        });
        update_probe.wait_until_attempted().await;
        assert!(
            !update.is_finished(),
            "quota mutation must wait for the reservation's metadata transaction guard"
        );

        barrier.release();
        put.await
            .expect("PUT task should not panic")
            .expect("the write linearized before the quota update must commit");
        update
            .await
            .expect("quota update task should not panic")
            .expect("quota update should proceed after commit");

        let mut excess = PutObjReader::from_vec(vec![0x74]);
        let err = store
            .put_object(&bucket, "excess", &mut excess, &ObjectOptions::default())
            .await
            .expect_err("writes after the zero-byte quota update must be denied");
        assert!(matches!(err, StorageError::QuotaExceeded { current: 4096, limit: 0 }));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_put_object_refuses_a_bucket_whose_encryption_config_is_unreadable() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let (store, context) = real_store_test_context().await;
        let bucket = format!("put-sse-unreadable-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("unreadable-encryption PUT bucket must be created");
        install_unreadable_bucket_sse_config(&bucket).await;

        let payload = Bytes::from_static(b"an operator mandated encryption for this bucket");
        let input = PutObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .body(Some(StreamingBlob::from(s3s::Body::from(payload.clone()))))
            .content_length(Some(i64::try_from(payload.len()).expect("test payload length must fit i64")))
            .build()
            .expect("PUT input must build");
        let usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));

        let err = Box::pin(usecase.execute_put_object(&FS::new(), build_request(input, Method::PUT)))
            .await
            .expect_err("an unreadable bucket encryption configuration must refuse the write");

        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        let lookup_err = store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect_err("a refused PUT must not leave an object behind");
        assert!(is_err_object_not_found(&lookup_err), "{lookup_err}");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_put_object_still_writes_plaintext_without_bucket_encryption() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let (store, context) = real_store_test_context().await;
        let bucket = format!("put-sse-absent-{}", Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("plaintext PUT bucket must be created");

        let payload = Bytes::from_static(b"no default encryption is configured for this bucket");
        let input = PutObjectInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .body(Some(StreamingBlob::from(s3s::Body::from(payload.clone()))))
            .content_length(Some(i64::try_from(payload.len()).expect("test payload length must fit i64")))
            .build()
            .expect("PUT input must build");
        let usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));

        Box::pin(usecase.execute_put_object(&FS::new(), build_request(input, Method::PUT)))
            .await
            .expect("a bucket without default encryption must still accept a plaintext write");

        let stored = store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("the plaintext object must be readable");
        assert_eq!(stored.size, i64::try_from(payload.len()).expect("test payload length must fit i64"));
        assert!(
            !stored
                .user_defined
                .keys()
                .any(|key| key.eq_ignore_ascii_case(AMZ_SERVER_SIDE_ENCRYPTION)
                    || key.starts_with("x-rustfs-encryption-")
                    || key.starts_with("x-minio-encryption-")),
            "the object must carry no encryption metadata: {:?}",
            stored.user_defined
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn execute_put_object_extract_refuses_a_bucket_whose_encryption_config_is_unreadable() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let (store, context) = real_store_test_context().await;
        let bucket = format!("extract-sse-unreadable-{}", Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("unreadable-encryption extract bucket must be created");
        install_unreadable_bucket_sse_config(&bucket).await;

        let payload = Bytes::from_static(b"archive bytes that must never be unpacked in plaintext");
        let input = PutObjectInput::builder()
            .bucket(bucket.clone())
            .key("archive.tar".to_string())
            .body(Some(StreamingBlob::from(s3s::Body::from(payload.clone()))))
            .content_length(Some(i64::try_from(payload.len()).expect("test payload length must fit i64")))
            .build()
            .expect("extract PUT input must build");
        let mut req = build_request(input, Method::PUT);
        req.headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("true"));
        let usecase = DefaultObjectUsecase::with_context(Some(Arc::clone(&context)));

        let err = Box::pin(usecase.execute_put_object(&FS::new(), req))
            .await
            .expect_err("an unreadable bucket encryption configuration must refuse the extract upload");

        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        let lookup_err = store
            .get_object_info(&bucket, "archive.tar", &ObjectOptions::default())
            .await
            .expect_err("a refused extract upload must not leave an object behind");
        assert!(is_err_object_not_found(&lookup_err), "{lookup_err}");
    }
}
