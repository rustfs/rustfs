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

//! Snowball auto-extract (PutObject x-amz-meta-snowball-auto-extract) path.

use super::*;

// One logical member can be preceded by local PAX, GNU long-name, and GNU
// long-link records. Count all four physical headers without rejecting that
// compatible extension combination.
const EXTRACT_ARCHIVE_PHYSICAL_ENTRY_MULTIPLIER: u64 = 4;
// Sparse maps are metadata, so bound them independently of object byte quotas.
const EXTRACT_ARCHIVE_MAX_SPARSE_ENTRIES: u64 = 4_096;
const EXTRACT_ARCHIVE_MAX_SPARSE_CONTINUATION_BLOCKS: u64 = 256;

fn ensure_legacy_archive_size_within_quota(result: &QuotaCheckResult, total_unpacked_size: u64) -> S3Result<()> {
    if result.uses_durable_reservations {
        return Ok(());
    }
    let (Some(current_usage), Some(quota_limit)) = (result.current_usage, result.quota_limit) else {
        return Ok(());
    };
    let expected_usage = current_usage
        .checked_add(total_unpacked_size)
        .ok_or_else(|| s3_error!(InvalidArgument, "Archive total size overflowed quota accounting"))?;
    if expected_usage > quota_limit {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("Bucket quota exceeded. Current usage: {current_usage} bytes, limit: {quota_limit} bytes"),
        ));
    }
    Ok(())
}

pin_project! {
    struct ExtractArchiveEtagReader<R> {
        #[pin]
        inner: R,
        md5: Md5,
        expected_length: u64,
        bytes_read: u64,
        pending_final_byte: Option<u8>,
        validating_eof: bool,
        finished: bool,
        state: Arc<Mutex<ExtractArchiveUploadState>>,
    }
}

#[derive(Debug, Default)]
struct ExtractArchiveUploadState {
    etag: Option<String>,
    body_complete: bool,
}

fn resolve_extract_archive_format(key: &str, detected: CompressionFormat) -> CompressionFormat {
    // Zlib has no unambiguous magic. Preserve the legacy zlib/zz suffix
    // contract without letting other misleading suffixes override content
    // detection.
    if detected == CompressionFormat::Tar
        && Path::new(key)
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| CompressionFormat::from_extension(extension) == CompressionFormat::Zlib)
    {
        CompressionFormat::Zlib
    } else {
        detected
    }
}

impl<R> ExtractArchiveEtagReader<R> {
    fn new(inner: R, expected_length: u64, state: Arc<Mutex<ExtractArchiveUploadState>>) -> Self {
        Self {
            inner,
            md5: Md5::new(),
            expected_length,
            bytes_read: 0,
            pending_final_byte: None,
            validating_eof: expected_length == 0,
            finished: false,
            state,
        }
    }
}

fn extract_archive_incomplete_body(remaining: u64) -> std::io::Error {
    let Ok(remaining) = i64::try_from(remaining) else {
        return std::io::Error::new(std::io::ErrorKind::InvalidData, "archive remaining body length exceeds i64");
    };
    std::io::Error::new(std::io::ErrorKind::UnexpectedEof, rustfs_rio::IncompleteBody { remaining })
}

impl<R: AsyncRead> AsyncRead for ExtractArchiveEtagReader<R> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();
        if buf.remaining() == 0 || *this.finished {
            return Poll::Ready(Ok(()));
        }

        loop {
            if *this.validating_eof {
                let mut probe = [0u8; 1];
                let mut probe_buf = ReadBuf::new(&mut probe);
                match this.inner.as_mut().poll_read(cx, &mut probe_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Ready(Ok(())) if !probe_buf.filled().is_empty() => {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "archive body exceeded expected Content-Length",
                        )));
                    }
                    Poll::Ready(Ok(())) => {
                        if let Ok(mut state) = this.state.lock()
                            && !state.body_complete
                        {
                            state.etag =
                                Some(hex_simd::encode_to_string(this.md5.clone().finalize(), hex_simd::AsciiCase::Lower));
                            state.body_complete = true;
                        }
                        *this.validating_eof = false;
                        *this.finished = true;
                        if let Some(final_byte) = this.pending_final_byte.take() {
                            buf.put_slice(&[final_byte]);
                        }
                        return Poll::Ready(Ok(()));
                    }
                }
            }

            let remaining = *this.expected_length - *this.bytes_read;
            if remaining == 1 {
                let mut final_byte = [0u8; 1];
                let mut final_buf = ReadBuf::new(&mut final_byte);
                match this.inner.as_mut().poll_read(cx, &mut final_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Ready(Ok(())) if final_buf.filled().is_empty() => {
                        return Poll::Ready(Err(extract_archive_incomplete_body(*this.expected_length - *this.bytes_read)));
                    }
                    Poll::Ready(Ok(())) => {
                        this.md5.update(final_buf.filled());
                        *this.bytes_read = match this.bytes_read.checked_add(1) {
                            Some(bytes_read) => bytes_read,
                            None => return Poll::Ready(Err(std::io::Error::other("archive read length overflow"))),
                        };
                        *this.pending_final_byte = Some(final_buf.filled()[0]);
                        *this.validating_eof = true;
                        continue;
                    }
                }
            }

            let max_read = usize::try_from(remaining - 1).unwrap_or(usize::MAX).min(buf.remaining());
            let read_len = {
                let target = buf.initialize_unfilled_to(max_read);
                let mut limited_buf = ReadBuf::new(target);
                match this.inner.as_mut().poll_read(cx, &mut limited_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Ready(Ok(())) if limited_buf.filled().is_empty() => {
                        return Poll::Ready(Err(extract_archive_incomplete_body(*this.expected_length - *this.bytes_read)));
                    }
                    Poll::Ready(Ok(())) => {
                        this.md5.update(limited_buf.filled());
                        limited_buf.filled().len()
                    }
                }
            };
            let read = match u64::try_from(read_len) {
                Ok(read) => read,
                Err(_) => return Poll::Ready(Err(std::io::Error::other("archive read length exceeds u64"))),
            };
            *this.bytes_read = match this.bytes_read.checked_add(read) {
                Some(bytes_read) => bytes_read,
                None => return Poll::Ready(Err(std::io::Error::other("archive read length overflow"))),
            };
            buf.advance(read_len);
            return Poll::Ready(Ok(()));
        }
    }
}

pin_project! {
    struct ExtractMemberReadTracker {
        #[pin]
        inner: HashReader,
        failed: Arc<AtomicBool>,
    }
}

impl AsyncRead for ExtractMemberReadTracker {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let this = self.project();
        match this.inner.poll_read(cx, buf) {
            Poll::Ready(Err(err)) => {
                this.failed.store(true, Ordering::Release);
                Poll::Ready(Err(err))
            }
            other => other,
        }
    }
}

impl rustfs_rio::EtagResolvable for ExtractMemberReadTracker {
    fn try_resolve_etag(&mut self) -> Option<String> {
        rustfs_rio::EtagResolvable::try_resolve_etag(&mut self.inner)
    }
}

impl rustfs_rio::HashReaderDetector for ExtractMemberReadTracker {}

impl rustfs_rio::TryGetIndex for ExtractMemberReadTracker {
    fn try_get_index(&self) -> Option<&rustfs_rio::Index> {
        rustfs_rio::TryGetIndex::try_get_index(&self.inner)
    }
}

fn track_extract_member_read_errors(reader: HashReader) -> std::io::Result<(HashReader, Arc<AtomicBool>)> {
    let size = reader.size();
    let actual_size = reader.actual_size();
    let failed = Arc::new(AtomicBool::new(false));
    let tracker = ExtractMemberReadTracker {
        inner: reader,
        failed: failed.clone(),
    };
    let mut tracked = HashReader::from_reader(tracker, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)?;
    tracked.update_params(size, actual_size, None);
    Ok((tracked, failed))
}

fn should_ignore_extract_member_write_error(ignore_errors: bool, member_read_failed: &AtomicBool) -> bool {
    ignore_errors && !member_read_failed.load(Ordering::Acquire)
}

pin_project! {
    struct ExtractDecodedLimitReader<R> {
        #[pin]
        inner: R,
        remaining: u64,
    }
}

impl<R> ExtractDecodedLimitReader<R> {
    fn new(inner: R, limit: u64) -> Self {
        Self { inner, remaining: limit }
    }
}

impl<R: AsyncRead> AsyncRead for ExtractDecodedLimitReader<R> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let mut this = self.project();
        let allowed = this.remaining.saturating_add(1);
        let max_read = usize::try_from(allowed).unwrap_or(usize::MAX).min(buf.remaining());
        let read_len = {
            let unfilled = buf.initialize_unfilled_to(max_read);
            let mut limited = ReadBuf::new(unfilled);
            match this.inner.as_mut().poll_read(cx, &mut limited) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => limited.filled().len(),
            }
        };
        let read = u64::try_from(read_len).unwrap_or(u64::MAX);
        if read > *this.remaining {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "archive decoded size exceeds limit",
            )));
        }

        *this.remaining -= read;
        buf.advance(read_len);
        Poll::Ready(Ok(()))
    }
}

const AMZ_SNOWBALL_EXTRACT_COMPAT: &str = "X-Amz-Snowball-Auto-Extract";

#[cfg(test)]
const AMZ_SNOWBALL_PREFIX_INTERNAL: &str = "X-Amz-Meta-Rustfs-Snowball-Prefix";

#[cfg(test)]
const AMZ_SNOWBALL_IGNORE_DIRS_INTERNAL: &str = "X-Amz-Meta-Rustfs-Snowball-Ignore-Dirs";

#[cfg(test)]
const AMZ_SNOWBALL_IGNORE_ERRORS_INTERNAL: &str = "X-Amz-Meta-Rustfs-Snowball-Ignore-Errors";

const AMZ_META_PREFIX_LOWER: &str = "x-amz-meta-";

const SNOWBALL_PREFIX_SUFFIX_LOWER: &str = "snowball-prefix";

const SNOWBALL_IGNORE_DIRS_SUFFIX_LOWER: &str = "snowball-ignore-dirs";

const SNOWBALL_IGNORE_ERRORS_SUFFIX_LOWER: &str = "snowball-ignore-errors";

const SNOWBALL_PREFIX_HEADER_KEYS: &[&str] = &[AMZ_MINIO_SNOWBALL_PREFIX, AMZ_SNOWBALL_PREFIX, AMZ_RUSTFS_SNOWBALL_PREFIX];

const SNOWBALL_IGNORE_DIRS_HEADER_KEYS: &[&str] = &[
    AMZ_MINIO_SNOWBALL_IGNORE_DIRS,
    AMZ_SNOWBALL_IGNORE_DIRS,
    AMZ_RUSTFS_SNOWBALL_IGNORE_DIRS,
];

const SNOWBALL_IGNORE_ERRORS_HEADER_KEYS: &[&str] = &[
    AMZ_MINIO_SNOWBALL_IGNORE_ERRORS,
    AMZ_SNOWBALL_IGNORE_ERRORS,
    AMZ_RUSTFS_SNOWBALL_IGNORE_ERRORS,
];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PutObjectExtractOptions {
    prefix: Option<String>,
    ignore_dirs: bool,
    ignore_errors: bool,
}

fn header_value_is_true(headers: &HeaderMap, key: &str) -> bool {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"))
}

pub(super) fn is_put_object_extract_requested(headers: &HeaderMap) -> bool {
    header_value_is_true(headers, AMZ_SNOWBALL_EXTRACT) || header_value_is_true(headers, AMZ_SNOWBALL_EXTRACT_COMPAT)
}

fn trimmed_header_value(headers: &HeaderMap, key: &str) -> Option<String> {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
}

fn is_exact_snowball_meta_key(key: &str, exact_keys: &[&str]) -> bool {
    exact_keys.iter().any(|exact_key| key.eq_ignore_ascii_case(exact_key))
}

fn snowball_meta_value_by_suffix(headers: &HeaderMap, suffix_lower: &str, exact_keys: &[&str]) -> Option<String> {
    for (name, value) in headers {
        let key = name.as_str();
        if key.starts_with(AMZ_META_PREFIX_LOWER)
            && key.ends_with(suffix_lower)
            && !is_exact_snowball_meta_key(key, exact_keys)
            && let Ok(parsed) = value.to_str()
        {
            return Some(parsed.trim().to_string());
        }
    }

    None
}

fn snowball_meta_value(headers: &HeaderMap, exact_keys: &[&str], suffix_lower: &str) -> Option<String> {
    for key in exact_keys {
        if let Some(value) = trimmed_header_value(headers, key) {
            return Some(value);
        }
    }

    snowball_meta_value_by_suffix(headers, suffix_lower, exact_keys)
}

fn snowball_meta_flag(headers: &HeaderMap, exact_keys: &[&str], suffix_lower: &str) -> bool {
    snowball_meta_value(headers, exact_keys, suffix_lower).is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

/// Validates that an archive entry path does not escape the target bucket.
///
/// Delegates to [`rustfs_utils::path::validate_extract_relative_path`] and wraps
/// the result as an S3 error on failure.
pub fn validate_extract_relative_path(path: &str) -> S3Result<()> {
    rustfs_utils::path::validate_extract_relative_path(path).map_err(|msg| s3_error!(InvalidArgument, "{msg}"))
}

fn normalize_snowball_prefix(prefix: &str) -> S3Result<Option<String>> {
    let normalized = prefix.trim().trim_matches('/');
    if normalized.is_empty() {
        return Ok(None);
    }

    validate_extract_relative_path(normalized)?;

    Ok(Some(normalized.to_string()))
}

/// Normalizes an archive entry key by applying a prefix, trimming slashes,
/// and ensuring directory entries end with `/`.
///
/// Delegates to [`rustfs_utils::path::normalize_extract_entry_key`] and wraps
/// the result as an S3 error on failure.
pub fn normalize_extract_entry_key(path: &str, prefix: Option<&str>, is_dir: bool) -> S3Result<String> {
    rustfs_utils::path::normalize_extract_entry_key(path, prefix, is_dir).map_err(|msg| s3_error!(InvalidArgument, "{msg}"))
}

fn map_extract_archive_error(err: std::io::Error) -> S3Error {
    let message = err.to_string();
    let api_error = ApiError::from(err);
    if matches!(api_error.code, S3ErrorCode::BadDigest | S3ErrorCode::IncompleteBody) {
        return api_error.into();
    }

    let mut archive_error = s3_error!(InvalidArgument, "Failed to process archive entry: {}", message);
    archive_error.set_source(Box::new(api_error));
    archive_error
}

#[derive(Debug)]
enum ExtractEntryError {
    Fatal(S3Error),
    Recoverable(S3Error),
}

impl ExtractEntryError {
    fn into_s3_error(self) -> S3Error {
        match self {
            Self::Fatal(err) | Self::Recoverable(err) => err,
        }
    }

    fn ignore_or_return(self, ignore_errors: bool) -> S3Result<()> {
        match self {
            Self::Recoverable(_) if ignore_errors => Ok(()),
            Self::Fatal(err) | Self::Recoverable(err) => Err(err),
        }
    }

    #[cfg(test)]
    fn is_recoverable(&self) -> bool {
        matches!(self, Self::Recoverable(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExtractEntryDisposition {
    File,
    Directory,
    FormatSkip,
}

fn classify_extract_entry_type(entry_type: tokio_tar::EntryType) -> ExtractEntryDisposition {
    use tokio_tar::EntryType;

    match entry_type {
        EntryType::Regular | EntryType::Char | EntryType::Block | EntryType::Fifo | EntryType::GNUSparse => {
            ExtractEntryDisposition::File
        }
        EntryType::Directory => ExtractEntryDisposition::Directory,
        EntryType::Link
        | EntryType::Symlink
        | EntryType::GNULongName
        | EntryType::GNULongLink
        | EntryType::Continuous
        | EntryType::XGlobalHeader
        | EntryType::XHeader
        | EntryType::SolarisXHeader
        | EntryType::Other(_) => ExtractEntryDisposition::FormatSkip,
        _ => ExtractEntryDisposition::FormatSkip,
    }
}

fn extract_entry_quota_growth(disposition: ExtractEntryDisposition, entry_size: u64) -> u64 {
    match disposition {
        ExtractEntryDisposition::File => entry_size,
        ExtractEntryDisposition::Directory | ExtractEntryDisposition::FormatSkip => 0,
    }
}

fn extract_archive_entry_mod_time(header: &tokio_tar::Header) -> S3Result<Option<OffsetDateTime>> {
    let modified_at_secs = header.mtime().map_err(map_extract_archive_error)?;
    // GNU base-256 represents negative values with an all-sign-extended first
    // byte. MinIO treats non-positive mtimes as unset, while the tar parser's
    // unsigned API exposes `-1` as `u64::MAX`.
    if modified_at_secs == 0 || header.as_old().mtime[0] == 0xff {
        return Ok(None);
    }

    let modified_at_secs = i64::try_from(modified_at_secs)
        .map_err(|_| object_s3_error(S3ErrorCode::InvalidArgument, "Archive entry modification time is out of range"))?;
    OffsetDateTime::from_unix_timestamp(modified_at_secs)
        .map(Some)
        .map_err(|_| object_s3_error(S3ErrorCode::InvalidArgument, "Archive entry modification time is out of range"))
}

fn strict_extract_entry_path(path: &[u8]) -> Result<&str, ExtractEntryError> {
    std::str::from_utf8(path)
        .map_err(|_| ExtractEntryError::Recoverable(s3_error!(InvalidArgument, "Archive entry path must be valid UTF-8")))
}

fn is_empty_extract_entry_path(path: &str) -> bool {
    path.is_empty() || path == "." || path == "./"
}

fn validate_extract_member_key(path: &str, limits: ArchiveLimits) -> Result<(), ExtractEntryError> {
    validate_put_object_extract_entry_path(path, limits).map_err(ExtractEntryError::Recoverable)?;
    validate_object_key(path, "PUT").map_err(ExtractEntryError::Recoverable)
}

fn record_extract_pax_metadata_bytes(
    entry_size: &mut u64,
    total_size: &mut u64,
    key_size: usize,
    value_size: usize,
    limits: ArchiveLimits,
) -> Result<(), ExtractEntryError> {
    let record_size = key_size
        .checked_add(value_size)
        .and_then(|size| u64::try_from(size).ok())
        .ok_or_else(|| {
            ExtractEntryError::Fatal(object_s3_error(
                S3ErrorCode::InvalidArgument,
                "Archive PAX metadata size overflowed while processing entry",
            ))
        })?;
    *entry_size = (*entry_size).checked_add(record_size).ok_or_else(|| {
        ExtractEntryError::Fatal(object_s3_error(
            S3ErrorCode::InvalidArgument,
            "Archive PAX metadata size overflowed while processing entry",
        ))
    })?;
    *total_size = (*total_size).checked_add(record_size).ok_or_else(|| {
        ExtractEntryError::Fatal(object_s3_error(
            S3ErrorCode::InvalidArgument,
            "Archive total PAX metadata size overflowed",
        ))
    })?;

    if *entry_size > limits.max_pax_metadata_size {
        return Err(ExtractEntryError::Fatal(object_s3_error(
            S3ErrorCode::InvalidArgument,
            "Archive PAX metadata exceeds per-entry limit",
        )));
    }
    if *total_size > limits.max_total_pax_metadata_size {
        return Err(ExtractEntryError::Fatal(object_s3_error(
            S3ErrorCode::InvalidArgument,
            "Archive total PAX metadata exceeds limit",
        )));
    }

    Ok(())
}

fn record_extract_pax_metadata_record(
    entry_records: &mut usize,
    total_records: &mut usize,
    limits: ArchiveLimits,
) -> Result<(), ExtractEntryError> {
    *entry_records = entry_records.checked_add(1).ok_or_else(|| {
        ExtractEntryError::Fatal(object_s3_error(
            S3ErrorCode::InvalidArgument,
            "Archive PAX metadata record count overflowed",
        ))
    })?;
    *total_records = total_records.checked_add(1).ok_or_else(|| {
        ExtractEntryError::Fatal(object_s3_error(
            S3ErrorCode::InvalidArgument,
            "Archive total PAX metadata record count overflowed",
        ))
    })?;

    if *entry_records > limits.max_pax_metadata_records {
        return Err(ExtractEntryError::Fatal(object_s3_error(
            S3ErrorCode::InvalidArgument,
            "Archive PAX metadata record count exceeds per-entry limit",
        )));
    }
    if *total_records > limits.max_total_pax_metadata_records {
        return Err(ExtractEntryError::Fatal(object_s3_error(
            S3ErrorCode::InvalidArgument,
            "Archive total PAX metadata record count exceeds limit",
        )));
    }

    Ok(())
}

#[derive(Debug, Default)]
struct ExtractEntryPaxAuthorization {
    headers: HeaderMap,
    object_lock_legal_hold_status: Option<ObjectLockLegalHoldStatus>,
    object_lock_mode: Option<ObjectLockMode>,
    object_lock_retain_until_date: Option<Timestamp>,
}

async fn count_extract_entry_pax_metadata<R>(
    entry: &mut tokio_tar::Entry<Archive<R>>,
    total_pax_metadata_size: &mut u64,
    total_pax_metadata_records: &mut usize,
    limits: ArchiveLimits,
) -> Result<(), ExtractEntryError>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let Some(extensions) = entry
        .pax_extensions()
        .await
        .map_err(|err| ExtractEntryError::Fatal(map_extract_archive_error(err)))?
    else {
        return Ok(());
    };

    let mut entry_pax_metadata_size = 0u64;
    let mut entry_pax_metadata_records = 0usize;
    for ext in extensions {
        let ext = ext.map_err(|err| ExtractEntryError::Fatal(map_extract_archive_error(err)))?;
        record_extract_pax_metadata_record(&mut entry_pax_metadata_records, total_pax_metadata_records, limits)?;
        record_extract_pax_metadata_bytes(
            &mut entry_pax_metadata_size,
            total_pax_metadata_size,
            ext.key_bytes().len(),
            ext.value_bytes().len(),
            limits,
        )?;
    }
    Ok(())
}

async fn apply_extract_entry_pax_extensions<R>(
    entry: &mut tokio_tar::Entry<Archive<R>>,
    bucket: &str,
    object_name: &str,
    object_lock_config_state: &metadata_sys::ObjectLockConfigState,
    metadata: &mut HashMap<String, String>,
    opts: &mut ObjectOptions,
) -> Result<ExtractEntryPaxAuthorization, ExtractEntryError>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let Some(extensions) = entry
        .pax_extensions()
        .await
        .map_err(|err| ExtractEntryError::Fatal(map_extract_archive_error(err)))?
    else {
        return Ok(ExtractEntryPaxAuthorization::default());
    };

    let mut pax_headers = HeaderMap::new();
    let mut pax_version_id = None;
    for ext in extensions {
        let ext = ext.map_err(|err| ExtractEntryError::Fatal(map_extract_archive_error(err)))?;
        let key = ext.key().map_err(|err| {
            ExtractEntryError::Fatal(object_s3_error(
                S3ErrorCode::InvalidArgument,
                format!("Failed to process archive PAX key: {}", err),
            ))
        })?;
        let value = ext.value().map_err(|err| {
            ExtractEntryError::Fatal(object_s3_error(
                S3ErrorCode::InvalidArgument,
                format!("Failed to process archive PAX value: {}", err),
            ))
        })?;

        if let Some(meta_key) = key.strip_prefix("minio.metadata.") {
            if !meta_key.is_empty() {
                let name = http::HeaderName::from_bytes(meta_key.as_bytes()).map_err(|_| {
                    ExtractEntryError::Recoverable(s3_error!(InvalidArgument, "Invalid Snowball PAX metadata header"))
                })?;
                let header_value = HeaderValue::from_str(value).map_err(|_| {
                    ExtractEntryError::Recoverable(s3_error!(InvalidArgument, "Invalid Snowball PAX metadata value"))
                })?;
                preserve_unclassified_user_metadata(metadata, name.as_str(), value);
                pax_headers.insert(name, header_value);
            }
            continue;
        }

        if key == "minio.versionId" && !value.is_empty() {
            if Uuid::parse_str(value).is_err() {
                return Err(ExtractEntryError::Recoverable(s3_error!(
                    InvalidArgument,
                    "Invalid Snowball PAX version ID"
                )));
            }
            pax_version_id = Some(value.to_string());
        }
    }

    let has_replica_status = pax_headers.contains_key(AMZ_BUCKET_REPLICATION_STATUS);
    if let Some(value) = pax_headers.get(AMZ_BUCKET_REPLICATION_STATUS) {
        let status = value
            .to_str()
            .map_err(|_| ExtractEntryError::Recoverable(s3_error!(InvalidArgument, "Invalid Snowball replication status")))?;
        if !status.eq_ignore_ascii_case(ReplicationStatusType::Replica.as_str()) {
            return Err(ExtractEntryError::Recoverable(s3_error!(
                InvalidArgument,
                "Invalid Snowball replication status"
            )));
        }
        pax_headers.insert(AMZ_BUCKET_REPLICATION_STATUS, HeaderValue::from_static("REPLICA"));
    }

    let authorization_headers = pax_headers.clone();

    if let Some(value) = pax_headers.remove("x-amz-tagging") {
        let value = value
            .to_str()
            .map_err(|_| ExtractEntryError::Recoverable(s3_error!(InvalidArgument, "Invalid Snowball object tagging value")))?;
        metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), value.to_owned());
    }

    let object_lock_mode = pax_headers
        .remove(AMZ_OBJECT_LOCK_MODE_LOWER)
        .map(|value| {
            value
                .to_str()
                .map(|value| ObjectLockMode::from(value.to_string()))
                .map_err(|_| ExtractEntryError::Recoverable(s3_error!(InvalidArgument, "Invalid Snowball Object Lock mode")))
        })
        .transpose()?;
    let object_lock_retain_until_date = pax_headers
        .remove(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER)
        .map(|value| {
            let value = value.to_str().map_err(|_| {
                ExtractEntryError::Recoverable(s3_error!(InvalidArgument, "Invalid Snowball Object Lock retain-until date"))
            })?;
            OffsetDateTime::parse(value, &Rfc3339).map(Timestamp::from).map_err(|_| {
                ExtractEntryError::Recoverable(s3_error!(InvalidArgument, "Invalid Snowball Object Lock retain-until date"))
            })
        })
        .transpose()?;
    let object_lock_legal_hold_status = pax_headers
        .remove(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER)
        .map(|value| {
            value
                .to_str()
                .map(|value| ObjectLockLegalHoldStatus::from(value.to_string()))
                .map_err(|_| {
                    ExtractEntryError::Recoverable(s3_error!(InvalidArgument, "Invalid Snowball Object Lock legal-hold status"))
                })
        })
        .transpose()?;
    opts.version_id = pax_version_id;

    extract_metadata_from_mime_with_object_name(&pax_headers, metadata, false, Some(object_name));
    if has_replica_status {
        metadata.retain(|key, _| !key.eq_ignore_ascii_case(AMZ_BUCKET_REPLICATION_STATUS));
        metadata.insert(
            AMZ_BUCKET_REPLICATION_STATUS.to_string(),
            ReplicationStatusType::Replica.as_str().to_string(),
        );
    }
    if let Some(object_lock_metadata) = build_put_like_object_lock_metadata(
        bucket,
        object_lock_config_state,
        object_lock_legal_hold_status.clone(),
        object_lock_mode.clone(),
        object_lock_retain_until_date.clone(),
    )
    .map_err(ExtractEntryError::Recoverable)?
    {
        metadata.extend(object_lock_metadata);
    }

    Ok(ExtractEntryPaxAuthorization {
        headers: authorization_headers,
        object_lock_legal_hold_status,
        object_lock_mode,
        object_lock_retain_until_date,
    })
}

#[cfg(test)]
async fn apply_extract_entry_pax_extensions_for_test<R>(
    entry: &mut tokio_tar::Entry<Archive<R>>,
    bucket: &str,
    object_name: &str,
    object_lock_config_state: &metadata_sys::ObjectLockConfigState,
    metadata: &mut HashMap<String, String>,
    opts: &mut ObjectOptions,
) -> Result<ExtractEntryPaxAuthorization, ExtractEntryError>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    let mut total_pax_metadata_size = 0;
    let mut total_pax_metadata_records = 0;
    count_extract_entry_pax_metadata(
        entry,
        &mut total_pax_metadata_size,
        &mut total_pax_metadata_records,
        ArchiveLimits::default(),
    )
    .await?;
    apply_extract_entry_pax_extensions(entry, bucket, object_name, object_lock_config_state, metadata, opts).await
}

fn resolve_put_object_extract_options(headers: &HeaderMap) -> S3Result<PutObjectExtractOptions> {
    let prefix = snowball_meta_value(headers, SNOWBALL_PREFIX_HEADER_KEYS, SNOWBALL_PREFIX_SUFFIX_LOWER)
        .map(|value| normalize_snowball_prefix(&value))
        .transpose()?
        .flatten();
    let ignore_dirs = snowball_meta_flag(headers, SNOWBALL_IGNORE_DIRS_HEADER_KEYS, SNOWBALL_IGNORE_DIRS_SUFFIX_LOWER);
    let ignore_errors = snowball_meta_flag(headers, SNOWBALL_IGNORE_ERRORS_HEADER_KEYS, SNOWBALL_IGNORE_ERRORS_SUFFIX_LOWER);

    Ok(PutObjectExtractOptions {
        prefix,
        ignore_dirs,
        ignore_errors,
    })
}

fn put_object_extract_limits() -> ArchiveLimits {
    ArchiveLimits::default()
}

fn build_put_object_extract_archive<R>(decoder: R, limits: ArchiveLimits) -> Archive<R>
where
    R: AsyncRead + Unpin,
{
    let max_physical_entries = u64::try_from(limits.max_entries)
        .unwrap_or(u64::MAX)
        .saturating_mul(EXTRACT_ARCHIVE_PHYSICAL_ENTRY_MULTIPLIER);

    tokio_tar::ArchiveBuilder::new(decoder)
        .set_max_extension_entry_size(limits.max_pax_metadata_size)
        .set_max_total_extension_size(limits.max_total_pax_metadata_size)
        .set_max_physical_entries(max_physical_entries)
        .set_max_sparse_entries(EXTRACT_ARCHIVE_MAX_SPARSE_ENTRIES)
        .set_max_sparse_continuation_blocks(EXTRACT_ARCHIVE_MAX_SPARSE_CONTINUATION_BLOCKS)
        .build()
}

fn validate_put_object_extract_entry_count(count: usize, limits: ArchiveLimits) -> S3Result<()> {
    if count > limits.max_entries {
        return Err(s3_error!(
            InvalidArgument,
            "Archive entry count exceeds limit: count={}, limit={}",
            count,
            limits.max_entries
        ));
    }
    Ok(())
}

fn validate_put_object_extract_entry_size(path: &str, size: u64, limits: ArchiveLimits) -> S3Result<()> {
    if size > limits.max_entry_size {
        return Err(s3_error!(
            InvalidArgument,
            "Archive entry size exceeds limit for {}: size={}, limit={}",
            path,
            size,
            limits.max_entry_size
        ));
    }
    Ok(())
}

fn validate_put_object_extract_total_size(total_size: u64, limits: ArchiveLimits) -> S3Result<()> {
    if total_size > limits.max_total_unpacked_size {
        return Err(s3_error!(
            InvalidArgument,
            "Archive total unpacked size exceeds limit: size={}, limit={}",
            total_size,
            limits.max_total_unpacked_size
        ));
    }
    Ok(())
}

fn validate_put_object_extract_entry_path(path: &str, limits: ArchiveLimits) -> S3Result<()> {
    if path.len() > limits.max_path_length {
        return Err(s3_error!(
            InvalidArgument,
            "Archive entry path exceeds limit for {}: length={}, limit={}",
            path,
            path.len(),
            limits.max_path_length
        ));
    }
    Ok(())
}

impl DefaultObjectUsecase {
    #[instrument(level = "debug", skip(self, req))]
    #[hotpath::measure(impl_type = "DefaultObjectUsecase")]
    pub async fn execute_put_object_extract(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        self.execute_put_object_extract_boxed(req).await
    }

    fn execute_put_object_extract_boxed(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> impl std::future::Future<Output = S3Result<S3Response<PutObjectOutput>>> + Send + '_ {
        Box::pin(self.execute_put_object_extract_inner(req))
    }

    async fn execute_put_object_extract_inner(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let helper = OperationHelper::new(&req, EventName::ObjectCreatedPut, S3Operation::PutObject).suppress_event();
        let request_context = helper.request_context_or_from_request(&req);
        let auth_method = req.method.clone();
        let auth_uri = req.uri.clone();
        let auth_headers = req.headers.clone();
        let auth_extensions = req.extensions.clone();
        let auth_credentials = req.credentials.clone();
        let auth_region = req.region.clone();
        let auth_service = req.service.clone();
        let auth_trailing_headers = req.trailing_headers.clone();
        // Extract uploads reject SSE-KMS before reaching the SSE layer, so the principal is
        // only carried for the day that restriction lifts; the NotImplemented answer below
        // deliberately stays ahead of any key authorization.
        let extract_principal = SseKmsPrincipal::from_request(&req);
        if is_sse_kms_requested(&req.input, &req.headers) {
            return Err(s3_error!(NotImplemented, "SSE-KMS is not supported for extract uploads"));
        }
        let replication_authorized = replication_request_authorized(&req);
        let mut bucket_generation_opts = ObjectOptions::default();
        apply_bucket_generation_guard(&req, &req.input.bucket, &mut bucket_generation_opts)?;
        let expected_bucket_incarnation_id = bucket_generation_opts.expected_bucket_incarnation_id;
        let input = req.input;

        let PutObjectInput {
            body,
            bucket,
            key,
            version_id,
            cache_control,
            content_disposition,
            content_encoding,
            content_length,
            content_language,
            content_type,
            content_md5,
            expires,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ssekms_key_id,
            storage_class,
            tagging,
            website_redirect_location,
            ..
        } = input;

        let event_version_id = version_id;
        let (h_algo, h_key, h_md5) = extract_ssec_params_from_headers(&req.headers)?;
        let sse_customer_algorithm = sse_customer_algorithm.or(h_algo);
        let sse_customer_key = sse_customer_key.or(h_key);
        let sse_customer_key_md5 = sse_customer_key_md5.or(h_md5);

        let original_sse = server_side_encryption.or(extract_server_side_encryption_from_headers(&req.headers)?);
        let bucket_sse_config = metadata_sys::get_sse_config(&bucket).await.ok();
        let (mut effective_sse, mut effective_kms_key_id) = resolve_bucket_default_sse(
            bucket_sse_config.as_ref().map(|(config, _timestamp)| config),
            original_sse,
            ssekms_key_id,
            false,
        );
        if effective_sse
            .as_ref()
            .is_some_and(|sse| sse.as_str().eq_ignore_ascii_case(ServerSideEncryption::AWS_KMS))
        {
            return Err(s3_error!(NotImplemented, "SSE-KMS is not supported for extract uploads"));
        }
        validate_sse_headers_for_write(
            effective_sse.as_ref(),
            effective_kms_key_id.as_ref(),
            extract_ssekms_context_from_headers(&req.headers)?.as_ref(),
            sse_customer_algorithm.as_ref(),
            sse_customer_key.as_ref(),
            sse_customer_key_md5.as_ref(),
            true,
        )?;
        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };
        let body = guard_put_object_body_read_timeout(
            body,
            &bucket,
            &key,
            &request_context.request_id,
            content_length,
            put_object_body_read_timeout(),
        );

        let size = match content_length {
            Some(c) => c,
            None => {
                if let Some(val) = req.headers.get(AMZ_DECODED_CONTENT_LENGTH) {
                    match atoi::atoi::<i64>(val.as_bytes()) {
                        Some(x) => x,
                        None => return Err(s3_error!(UnexpectedContent)),
                    }
                } else {
                    return Err(s3_error!(UnexpectedContent));
                }
            }
        };
        if size < 0 {
            return Err(s3_error!(UnexpectedContent));
        }
        validate_object_key(&key, "PUT")?;
        validate_table_catalog_object_mutation(&bucket, &key).await?;
        let _ = self
            .check_bucket_quota(
                &bucket,
                QuotaOperation::PutObject,
                u64::try_from(size).map_err(|_| S3Error::new(S3ErrorCode::UnexpectedContent))?,
            )
            .await?;

        // Apply adaptive buffer sizing based on file size for optimal streaming performance.
        // Uses workload profile configuration (enabled by default) to select appropriate buffer size.
        // Buffer sizes range from 32KB to 4MB depending on file size and configured workload profile.
        let buffer_size = get_buffer_size_opt_in(size);
        let body =
            tokio::io::BufReader::with_capacity(buffer_size, StreamReader::new(body.map(|f| f.map_err(s3s_body_error_to_io))));

        let md5hex = if let Some(base64_md5) = content_md5 {
            let md5 = base64_simd::STANDARD
                .decode_to_vec(base64_md5.as_bytes())
                .map_err(|e| ApiError::from(StorageError::other(format!("Invalid content MD5: {e}"))))?;
            Some(hex_simd::encode_to_string(&md5, hex_simd::AsciiCase::Lower))
        } else {
            None
        };

        let sha256hex = get_content_sha256_with_query(&req.headers, req.uri.query());
        let actual_size = size;

        let mut archive_reader =
            HashReader::from_stream(body, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?;

        if let Err(err) = archive_reader.add_checksum_from_s3s(&req.headers, req.trailing_headers.clone(), false) {
            return Err(ApiError::from(err).into());
        }

        let expected_archive_length = u64::try_from(size).map_err(|_| S3Error::new(S3ErrorCode::UnexpectedContent))?;
        let archive_upload_state = Arc::new(Mutex::new(ExtractArchiveUploadState::default()));
        let extract_limits = put_object_extract_limits();
        let tracked_archive =
            ExtractArchiveEtagReader::new(archive_reader, expected_archive_length, archive_upload_state.clone());
        let (detected_archive_format, sniffed_archive) =
            CompressionFormat::sniff(tracked_archive).await.map_err(|err| match err {
                ZipError::InspectStream(source) => map_extract_archive_error(source),
                _ => s3_error!(InvalidArgument, "Failed to detect archive compression"),
            })?;
        let archive_format = resolve_extract_archive_format(&key, detected_archive_format);
        let decoder = archive_format.get_decoder(sniffed_archive).map_err(|e| {
            error!(error = ?e, "Archive decoder creation failed");
            s3_error!(InvalidArgument, "get_decoder err")
        })?;
        let decoder = ExtractDecodedLimitReader::new(decoder, extract_limits.max_decoded_size);

        let mut ar = build_put_object_extract_archive(decoder, extract_limits);
        let mut entries = ar.entries().map_err(|e| {
            error!(error = ?e, "Archive entry listing failed");
            s3_error!(InvalidArgument, "get entries err")
        })?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let extract_options = resolve_put_object_extract_options(&req.headers)?;
        let extract_quota_check = if let Some(metadata_sys) = self.bucket_metadata_sys() {
            let quota_checker = QuotaChecker::new(metadata_sys);
            let check_result =
                map_quota_check_outcome(&bucket, quota_checker.check_quota(&bucket, QuotaOperation::PutObject, 0).await)?;
            Some(check_result)
        } else {
            None
        };
        let extract_quota_enabled = extract_quota_check
            .as_ref()
            .is_some_and(|result| result.quota_limit.is_some());
        let version_id = match event_version_id {
            Some(v) => v.to_string(),
            None => String::new(),
        };

        let notify = current_notify_interface_for_context(self.context.as_deref());
        let req_params = rustfs_targets::extract_params_header(&req.headers);
        let host = get_request_host(&req.headers);
        let port = get_request_port(&req.headers);
        let user_agent = get_request_user_agent(&req.headers);
        let mut wrote_any_entry = false;
        let mut extracted_entry_count = 0usize;
        let mut resource_total_size = 0u64;
        let mut legacy_quota_growth = 0u64;
        let mut total_pax_metadata_size = 0u64;
        let mut total_pax_metadata_records = 0usize;
        let object_lock_config_snapshot = store.object_lock_config_snapshot(&bucket).await.map_err(ApiError::from)?;
        let object_lock_config_state = object_lock_config_snapshot.state();

        while let Some(entry) = entries.next().await {
            let mut f = match entry {
                Ok(f) => f,
                Err(e) => {
                    error!(error = %e, "Archive entry read failed");
                    return Err(s3_error!(InvalidArgument, "Failed to read archive entry: {:?}", e));
                }
            };
            extracted_entry_count = extracted_entry_count.saturating_add(1);
            validate_put_object_extract_entry_count(extracted_entry_count, extract_limits)?;
            let entry_size = f.effective_size();
            validate_put_object_extract_entry_size("archive member", entry_size, extract_limits)?;
            resource_total_size = resource_total_size
                .checked_add(entry_size)
                .ok_or_else(|| s3_error!(InvalidArgument, "Archive total unpacked size overflowed while processing entries"))?;
            validate_put_object_extract_total_size(resource_total_size, extract_limits)?;
            count_extract_entry_pax_metadata(
                &mut f,
                &mut total_pax_metadata_size,
                &mut total_pax_metadata_records,
                extract_limits,
            )
            .await
            .map_err(ExtractEntryError::into_s3_error)?;

            let entry_type = classify_extract_entry_type(f.header().entry_type());
            if entry_type == ExtractEntryDisposition::FormatSkip {
                continue;
            }
            let is_dir = entry_type == ExtractEntryDisposition::Directory;
            if is_dir && extract_options.ignore_dirs {
                continue;
            }
            let fpath = {
                let path_bytes = f.path_bytes().map_err(map_extract_archive_error)?;
                let path = match strict_extract_entry_path(path_bytes.as_ref()) {
                    Ok(path) => path,
                    Err(err) => {
                        err.ignore_or_return(extract_options.ignore_errors)?;
                        continue;
                    }
                };
                if is_empty_extract_entry_path(path) {
                    continue;
                }
                normalize_extract_entry_key(path, extract_options.prefix.as_deref(), is_dir)?
            };

            if let Err(err) = validate_extract_member_key(&fpath, extract_limits) {
                err.ignore_or_return(extract_options.ignore_errors)?;
                continue;
            }
            validate_table_catalog_object_mutation(&bucket, &fpath).await?;

            let mut auth_req = S3Request {
                input: PutObjectInput::default(),
                method: auth_method.clone(),
                uri: auth_uri.clone(),
                headers: auth_headers.clone(),
                extensions: auth_extensions.clone(),
                credentials: auth_credentials.clone(),
                region: auth_region.clone(),
                service: auth_service.clone(),
                trailing_headers: auth_trailing_headers.clone(),
            };
            {
                let req_info = req_info_mut(&mut auth_req)?;
                req_info.bucket = Some(bucket.clone());
                req_info.object = Some(fpath.clone());
                req_info.version_id = None;
            }
            let mut size =
                i64::try_from(entry_size).map_err(|_| s3_error!(InvalidArgument, "Archive entry size does not fit into i64"))?;
            // mtime 0 or a negative GNU base-256 value means "unset". xl.meta
            // also cannot represent the Unix epoch as an object mod_time, so
            // those cases fall back to the upload time (rustfs#4842).
            let archive_entry_mod_time = extract_archive_entry_mod_time(f.header())?;
            let mut metadata = HashMap::new();
            let has_explicit_object_lock_retention = object_lock_mode.is_some() || object_lock_retain_until_date.is_some();
            apply_put_request_metadata(
                &mut metadata,
                &req.headers,
                &fpath,
                cache_control.clone(),
                content_disposition.clone(),
                content_encoding.clone(),
                content_language.clone(),
                content_type.clone(),
                expires.clone(),
                website_redirect_location.clone(),
                tagging.clone(),
                storage_class.clone(),
            )?;
            apply_bucket_default_lock_retention(
                &bucket,
                object_lock_config_state,
                &mut metadata,
                has_explicit_object_lock_retention,
            )?;
            let mut opts = put_opts_with_replication_authorization(
                &bucket,
                &fpath,
                None,
                &req.headers,
                metadata.clone(),
                replication_authorized,
            )
            .await
            .map_err(ApiError::from)?;
            if let Some(quota_check) = extract_quota_check.as_ref() {
                apply_quota_admission(&mut opts, quota_check)?;
            }
            opts.expected_bucket_incarnation_id = expected_bucket_incarnation_id;
            opts.object_lock_config_snapshot = Some(Arc::clone(&object_lock_config_snapshot));
            let pax_authorization = match apply_extract_entry_pax_extensions(
                &mut f,
                &bucket,
                &fpath,
                object_lock_config_state,
                &mut metadata,
                &mut opts,
            )
            .await
            {
                Ok(authorization) => authorization,
                Err(err) => {
                    err.ignore_or_return(extract_options.ignore_errors)?;
                    continue;
                }
            };
            if let Some(quota_check) = extract_quota_check.as_ref() {
                let next_legacy_quota_growth = legacy_quota_growth
                    .checked_add(extract_entry_quota_growth(entry_type, entry_size))
                    .ok_or_else(|| {
                        object_s3_error(S3ErrorCode::InvalidArgument, "Archive quota growth overflowed while processing entries")
                    })?;
                ensure_legacy_archive_size_within_quota(quota_check, next_legacy_quota_growth)?;
                legacy_quota_growth = next_legacy_quota_growth;
            }
            for (name, value) in &pax_authorization.headers {
                auth_req.headers.insert(name.clone(), value.clone());
            }
            if let Some(version_id) = opts.version_id.as_ref() {
                req_info_mut(&mut auth_req)?.version_id = Some(version_id.clone());
            }
            authorize_request(&mut auth_req, Action::S3Action(S3Action::PutObjectAction)).await?;
            if pax_authorization.object_lock_mode.is_some() || pax_authorization.object_lock_retain_until_date.is_some() {
                authorize_request(&mut auth_req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;
            }
            if pax_authorization.object_lock_legal_hold_status.is_some() {
                authorize_request(&mut auth_req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
            }
            if opts.version_id.is_some() || pax_authorization.headers.contains_key(AMZ_BUCKET_REPLICATION_STATUS) {
                authorize_request(&mut auth_req, Action::S3Action(S3Action::ReplicateObjectAction)).await?;
            }
            let effective_object_lock_legal_hold_status = pax_authorization
                .object_lock_legal_hold_status
                .clone()
                .or_else(|| object_lock_legal_hold_status.clone());
            let (effective_object_lock_mode, effective_object_lock_retain_until_date) =
                if pax_authorization.object_lock_mode.is_some() || pax_authorization.object_lock_retain_until_date.is_some() {
                    (
                        pax_authorization.object_lock_mode.clone(),
                        pax_authorization.object_lock_retain_until_date.clone(),
                    )
                } else {
                    (object_lock_mode.clone(), object_lock_retain_until_date.clone())
                };
            if archive_entry_mod_time.is_some() {
                opts.mod_time = archive_entry_mod_time;
            }

            debug!("Extracting file: {}, size: {} bytes", fpath, size);

            if is_dir {
                size = 0;
            }

            let actual_size = size;

            let should_compress =
                !is_dir && is_disk_compressible(&HeaderMap::new(), &fpath) && size > MIN_DISK_COMPRESSIBLE_SIZE as i64;

            let mut write_plan = WritePlan::new();
            let mut hrd = if is_dir {
                HashReader::from_stream(std::io::Cursor::new(Vec::new()), size, actual_size, None, None, false)
                    .map_err(ApiError::from)?
            } else if should_compress {
                let algorithm = CompressionAlgorithm::default();
                insert_str(&mut metadata, SUFFIX_COMPRESSION, compression_metadata_value(algorithm));
                insert_str(&mut metadata, SUFFIX_ACTUAL_SIZE, size.to_string());

                let hrd = HashReader::from_stream(f, size, actual_size, None, None, false).map_err(ApiError::from)?;
                write_plan = write_plan.with_compression(algorithm);
                hrd
            } else {
                HashReader::from_stream(f, size, actual_size, None, None, false).map_err(ApiError::from)?
            };
            apply_put_request_object_lock_opts(
                &bucket,
                object_lock_config_state,
                effective_object_lock_legal_hold_status,
                effective_object_lock_mode,
                effective_object_lock_retain_until_date,
                &mut opts,
            )?;
            if let Some(material) = sse_encryption(EncryptionRequest {
                bucket: &bucket,
                key: &fpath,
                server_side_encryption: effective_sse.clone(),
                ssekms_key_id: effective_kms_key_id.clone(),
                ssekms_context: extract_ssekms_context_from_headers(&req.headers)?,
                sse_customer_algorithm: sse_customer_algorithm.clone(),
                sse_customer_key: sse_customer_key.clone(),
                sse_customer_key_md5: sse_customer_key_md5.clone(),
                content_size: actual_size,
                principal: extract_principal.as_ref(),
            })
            .await?
            {
                effective_sse = Some(material.server_side_encryption.clone());
                effective_kms_key_id = material.kms_key_id.clone();

                write_plan = write_plan.with_encryption(material.write_encryption(None));

                let encryption_metadata = encryption_material_to_metadata(&material)?;
                metadata.extend(encryption_metadata.clone());
                opts.user_defined.extend(encryption_metadata);
            }
            hrd = write_plan.apply(hrd, actual_size).map_err(ApiError::from)?;
            let (hrd, member_read_failed) = track_extract_member_read_errors(hrd).map_err(ApiError::from)?;
            opts.user_defined.extend(metadata);

            // Each extracted member is an independent user write and joins
            // bucket replication like a regular PUT (MinIO PutObjectExtract
            // parity). One immutable decision drives both the pending metadata
            // and the post-commit schedule below, same contract as the PUT path
            // (https://github.com/rustfs/backlog/issues/1320); inbound replica
            // writes are declined inside `must_replicate_object`.
            let dsc = must_replicate_object(
                &bucket,
                &fpath,
                &opts.user_defined,
                "".to_string(),
                opts.delete_marker_replication_status(),
                opts.clone(),
            )
            .await;
            if dsc.replicate_any() {
                insert_str(&mut opts.user_defined, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
                insert_str(
                    &mut opts.user_defined,
                    SUFFIX_REPLICATION_STATUS,
                    dsc.pending_status().unwrap_or_default(),
                );
            }

            let mut reader = PutObjReader::new(hrd);
            let cache_adapter = self.object_data_cache();
            let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &fpath).await;

            let (obj_info, backfilled_old_current_size) = match store
                .put_object_with_old_current_size(&bucket, &fpath, &mut reader, &opts)
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    if should_ignore_extract_member_write_error(extract_options.ignore_errors, &member_read_failed) {
                        warn!(error = %e, "Archive object write skipped due to ignore-errors");
                        continue;
                    }
                    return Err(ApiError::from(e).into());
                }
            };
            let extract_versioned = BucketVersioningSys::prefix_enabled(&bucket, &fpath).await;
            let post_commit_error = match quota_accounting_object_size(&obj_info, extract_quota_enabled) {
                Ok(committed_size) => {
                    match previous_current_size_from_backfill(backfilled_old_current_size) {
                        Some(previous_current_size) => {
                            if extract_versioned {
                                record_bucket_object_version_write_memory(&bucket, previous_current_size, committed_size).await;
                            } else {
                                record_bucket_object_write_memory(&bucket, previous_current_size, committed_size).await;
                            }
                        }
                        None => {
                            record_bucket_object_write_unknown_previous_memory(&bucket, committed_size, extract_versioned).await;
                        }
                    }
                    None
                }
                Err(err) => Some(err),
            };
            let _ = invalidate_object_data_cache_after_put_success(&cache_adapter, &bucket, &fpath).await;

            // Reuse the per-entry pre-commit decision (see `dsc` above) so the
            // persisted pending marker and the schedule always agree.
            if dsc.replicate_any() {
                schedule_object_replication(obj_info.clone(), store.clone(), dsc).await;
            }

            if !wrote_any_entry {
                rustfs_scanner::record_dirty_usage_bucket(&bucket);
                wrote_any_entry = true;
            }

            let _manager = get_concurrency_manager();
            let _fpath_clone = fpath.clone();
            let _bucket_clone = bucket.clone();
            let e_tag = obj_info.etag.clone().map(|etag| to_s3s_etag(&etag));

            let output = PutObjectOutput {
                e_tag,
                ..Default::default()
            };

            let event_args = rustfs_notify::EventArgs {
                event_name: put_event_name_for_post_object(false),
                bucket_name: bucket.clone(),
                object: convert_ecstore_object_info(obj_info.clone()),
                req_params: req_params.clone(),
                resp_elements: build_event_resp_elements(&S3Response::new(output.clone()), &request_context.request_id),
                version_id: version_id.clone(),
                host: host.clone(),
                port,
                user_agent: user_agent.clone(),
            };

            let notify = notify.clone();
            spawn_background_with_context(Some(request_context.clone()), async move {
                notify.notify(event_args).await;
            });

            if let Some(err) = post_commit_error {
                return Err(err);
            }
        }

        let mut checksums = PutObjectChecksums {
            crc32: input.checksum_crc32,
            crc32c: input.checksum_crc32c,
            sha1: input.checksum_sha1,
            sha256: input.checksum_sha256,
            crc64nvme: input.checksum_crc64nvme,
        };
        warn!(
            "put object extract checksum_crc32={:?}, checksum_crc32c={:?}, checksum_sha1={:?}, checksum_sha256={:?}, checksum_crc64nvme={:?}",
            checksums.crc32, checksums.crc32c, checksums.sha1, checksums.sha256, checksums.crc64nvme,
        );

        drop(entries);
        let mut decoder = match ar.into_inner() {
            Ok(decoder) => decoder,
            Err(_) => return Err(s3_error!(InvalidArgument, "Failed to finalize archive reader")),
        };
        tokio::io::copy(&mut decoder, &mut tokio::io::sink())
            .await
            .map_err(map_extract_archive_error)?;
        let archive_etag = {
            let state = archive_upload_state
                .lock()
                .map_err(|_| object_s3_error(S3ErrorCode::InternalError, "Archive upload state lock was poisoned"))?;
            if !state.body_complete {
                return Err(object_s3_error(
                    S3ErrorCode::UnexpectedContent,
                    "Archive decoder did not consume the complete request body",
                ));
            }
            state.etag.as_ref().map(|etag| to_s3s_etag(etag))
        };
        apply_trailing_checksums(
            input.checksum_algorithm.as_ref().map(|a| a.as_str()),
            &req.trailing_headers,
            &mut checksums,
        );

        let output = PutObjectOutput {
            e_tag: archive_etag,
            checksum_crc32: checksums.crc32,
            checksum_crc32c: checksums.crc32c,
            checksum_sha1: checksums.sha1,
            checksum_sha256: checksums.sha256,
            checksum_crc64nvme: checksums.crc64nvme,
            ..Default::default()
        };
        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderName, HeaderValue};
    use s3s::dto::{ObjectLockConfiguration, ObjectLockEnabled};
    use tokio::io::AsyncReadExt;
    use tokio_tar::{Builder, EntryType, Header};

    #[test]
    fn archive_format_uses_only_the_ambiguous_zlib_extension_as_a_fallback() {
        assert_eq!(
            resolve_extract_archive_format("archive.zlib", CompressionFormat::Tar),
            CompressionFormat::Zlib
        );
        assert_eq!(
            resolve_extract_archive_format("archive.zz", CompressionFormat::Tar),
            CompressionFormat::Zlib
        );
        assert_eq!(
            resolve_extract_archive_format("raw-but-named.tar.gz", CompressionFormat::Tar),
            CompressionFormat::Tar
        );
        assert_eq!(
            resolve_extract_archive_format("gzip-but-named.zlib", CompressionFormat::Gzip),
            CompressionFormat::Gzip
        );
    }

    #[tokio::test]
    async fn raw_tar_member_names_starting_with_codec_magic_are_not_misdetected() {
        let cases = [
            ("PK\u{3}\u{4}-member.txt", b"PK\x03\x04".as_slice()),
            ("BZh9-report.txt", b"BZh9".as_slice()),
            ("\u{4}\"M\u{18}-report.txt", b"\x04\x22\x4d\x18".as_slice()),
        ];

        for (path, expected_prefix) in cases {
            let mut builder = Builder::new(Vec::new());
            let mut header = Header::new_gnu();
            header.set_size(0);
            header.set_cksum();
            builder
                .append_data(&mut header, path, &b""[..])
                .await
                .expect("raw TAR fixture should accept the codec-like member name");
            let bytes = builder.into_inner().await.expect("raw TAR fixture should finalize");
            assert_eq!(&bytes[..expected_prefix.len()], expected_prefix);

            let (format, sniffed) = CompressionFormat::sniff(std::io::Cursor::new(bytes))
                .await
                .expect("raw TAR prefix should be inspected");
            assert_eq!(format, CompressionFormat::Tar, "member path={path:?}");
            let decoder = format.get_decoder(sniffed).expect("raw TAR decoder should be created");
            let mut archive = Archive::new(decoder);
            let mut entries = archive.entries().expect("raw TAR entry stream should be created");
            let entry = entries
                .next()
                .await
                .expect("raw TAR should contain its first member")
                .expect("raw TAR member should parse");

            assert_eq!(entry.path_bytes().expect("raw TAR member path should parse").as_ref(), path.as_bytes());
        }
    }

    #[tokio::test]
    async fn archive_etag_reader_validates_sha256_before_completion() {
        let payload = b"archive-with-wrong-sha256".to_vec();
        let expected_length = i64::try_from(payload.len()).expect("fixture length must fit i64");
        let hash_reader = HashReader::from_stream(
            std::io::Cursor::new(payload),
            expected_length,
            expected_length,
            None,
            Some("00".repeat(32)),
            false,
        )
        .expect("hash reader should be created");
        let state = Arc::new(Mutex::new(ExtractArchiveUploadState::default()));
        let mut reader = ExtractArchiveEtagReader::new(
            hash_reader,
            u64::try_from(expected_length).expect("fixture length must fit u64"),
            state.clone(),
        );
        let mut output = Vec::new();
        let err = reader
            .read_to_end(&mut output)
            .await
            .expect_err("SHA-256 must be checked before upload completion");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(!state.lock().expect("archive state lock must remain healthy").body_complete);
    }

    fn pax_record(key: &str, value: &[u8]) -> Vec<u8> {
        let body_len = 1 + key.len() + 1 + value.len() + 1;
        let mut len = body_len + 1;
        loop {
            let actual_len = len.to_string().len() + body_len;
            if actual_len == len {
                break;
            }
            len = actual_len;
        }

        let mut record = format!("{len} {key}=").into_bytes();
        record.extend_from_slice(value);
        record.push(b'\n');
        assert_eq!(record.len(), len);
        record
    }

    async fn entry_with_local_pax(record: &[u8], entry_type: EntryType) -> tokio_tar::Entry<Archive<std::io::Cursor<Vec<u8>>>> {
        let mut builder = Builder::new(std::io::Cursor::new(Vec::new()));
        let mut extension = Header::new_ustar();
        extension.set_size(record.len() as u64);
        extension.set_entry_type(EntryType::XHeader);
        builder
            .append_data(&mut extension, "pax", record)
            .await
            .expect("local PAX fixture should be appended");

        let mut member = Header::new_ustar();
        member.set_size(0);
        member.set_entry_type(entry_type);
        if entry_type == EntryType::Symlink {
            member.set_link_name("target").expect("symlink fixture should have a target");
        }
        builder
            .append_data(&mut member, "member", std::io::Cursor::new(Vec::new()))
            .await
            .expect("member fixture should be appended");

        let bytes = builder
            .into_inner()
            .await
            .expect("fixture builder should finish")
            .into_inner();
        let mut archive = Archive::new(std::io::Cursor::new(bytes));
        let mut entries = archive.entries().expect("fixture archive should be iterable");
        entries
            .next()
            .await
            .expect("fixture should contain a logical member")
            .expect("fixture member should parse")
    }

    #[tokio::test]
    async fn archive_etag_reader_validates_raw_eof_before_returning_final_byte() {
        let payload = b"decoder-consumed-exact-body".to_vec();
        let state = Arc::new(Mutex::new(ExtractArchiveUploadState::default()));
        let mut reader = ExtractArchiveEtagReader::new(
            std::io::Cursor::new(payload.clone()),
            u64::try_from(payload.len()).expect("fixture length must fit u64"),
            state.clone(),
        );
        let mut output = Vec::new();

        reader
            .read_to_end(&mut output)
            .await
            .expect("exact body should validate through raw EOF");

        assert_eq!(output, payload);
        let expected_etag = hex_simd::encode_to_string(Md5::digest(&payload), hex_simd::AsciiCase::Lower);
        let state = state.lock().expect("archive state lock must remain healthy");
        assert!(state.body_complete);
        assert_eq!(state.etag.as_deref(), Some(expected_etag.as_str()));
    }

    #[tokio::test]
    async fn archive_etag_reader_rejects_short_and_overlong_bodies() {
        let payload = b"body-length-fixture".to_vec();

        let short_state = Arc::new(Mutex::new(ExtractArchiveUploadState::default()));
        let mut short = ExtractArchiveEtagReader::new(
            std::io::Cursor::new(payload.clone()),
            u64::try_from(payload.len() + 1).expect("fixture length must fit u64"),
            short_state.clone(),
        );
        let short_err = short
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("short body must be rejected");
        assert_eq!(short_err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(
            !short_state
                .lock()
                .expect("archive state lock must remain healthy")
                .body_complete
        );

        let overlong_state = Arc::new(Mutex::new(ExtractArchiveUploadState::default()));
        let mut overlong = ExtractArchiveEtagReader::new(
            std::io::Cursor::new(payload.clone()),
            u64::try_from(payload.len() - 1).expect("fixture length must fit u64"),
            overlong_state.clone(),
        );
        let overlong_err = overlong
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("overlong body must be rejected");
        assert_eq!(overlong_err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            !overlong_state
                .lock()
                .expect("archive state lock must remain healthy")
                .body_complete
        );
    }

    #[tokio::test]
    async fn archive_etag_reader_validates_content_md5_before_completion() {
        let payload = b"archive-with-wrong-content-md5".to_vec();
        let expected_length = i64::try_from(payload.len()).expect("fixture length must fit i64");
        let hash_reader = HashReader::from_stream(
            std::io::Cursor::new(payload),
            expected_length,
            expected_length,
            Some("00000000000000000000000000000000".to_string()),
            None,
            false,
        )
        .expect("hash reader should be created");
        let state = Arc::new(Mutex::new(ExtractArchiveUploadState::default()));
        let mut reader = ExtractArchiveEtagReader::new(
            hash_reader,
            u64::try_from(expected_length).expect("fixture length must fit u64"),
            state.clone(),
        );

        let err = reader
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("Content-MD5 must be checked before upload completion");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(!state.lock().expect("archive state lock must remain healthy").body_complete);
    }

    #[tokio::test]
    async fn snowball_pax_rejects_unpaired_object_lock_retention() {
        let record = pax_record("minio.metadata.x-amz-object-lock-mode", b"GOVERNANCE");
        let mut builder = Builder::new(Vec::new());
        let mut extension = Header::new_ustar();
        extension.set_size(record.len() as u64);
        extension.set_entry_type(EntryType::XHeader);
        builder.append_data(&mut extension, "pax", &record[..]).await.unwrap();

        let mut file = Header::new_ustar();
        file.set_size(0);
        builder.append_data(&mut file, "object", &b""[..]).await.unwrap();
        let mut archive = Archive::new(std::io::Cursor::new(builder.into_inner().await.unwrap()));
        let mut entries = archive.entries().unwrap();
        let mut entry = entries.next().await.unwrap().unwrap();
        let mut metadata = HashMap::from([
            (AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), ObjectLockRetentionMode::COMPLIANCE.to_string()),
            (AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_string(), "2030-01-01T00:00:00Z".to_string()),
        ]);
        let mut opts = ObjectOptions::default();
        let state = metadata_sys::ObjectLockConfigState::Configured {
            config: ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: None,
            },
            updated_at: OffsetDateTime::now_utc(),
        };

        let err = apply_extract_entry_pax_extensions_for_test(&mut entry, "bucket", "object", &state, &mut metadata, &mut opts)
            .await
            .unwrap_err()
            .into_s3_error();

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(metadata.get(AMZ_OBJECT_LOCK_MODE_LOWER).map(String::as_str), Some("COMPLIANCE"));
    }

    #[tokio::test]
    async fn snowball_pax_privileged_fields_require_independent_authorization() {
        let mut retention = pax_record("minio.metadata.X-Amz-Object-Lock-Mode", b"GOVERNANCE");
        retention.extend(pax_record("minio.metadata.X-Amz-Object-Lock-Retain-Until-Date", b"2099-01-01T00:00:00Z"));
        let cases = [
            ("retention", retention, (true, false, false)),
            (
                "legal-hold",
                pax_record("minio.metadata.X-Amz-Object-Lock-Legal-Hold", b"ON"),
                (false, true, false),
            ),
            (
                "version-id",
                pax_record("minio.versionId", Uuid::nil().to_string().as_bytes()),
                (false, false, true),
            ),
            (
                "replication-status",
                pax_record("minio.metadata.x-amz-replication-status", b"REPLICA"),
                (false, false, true),
            ),
        ];
        let state = metadata_sys::ObjectLockConfigState::Configured {
            config: ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: None,
            },
            updated_at: OffsetDateTime::now_utc(),
        };

        for (case, record, expected) in cases {
            let mut builder = Builder::new(Vec::new());
            let mut extension = Header::new_ustar();
            extension.set_size(record.len() as u64);
            extension.set_entry_type(EntryType::XHeader);
            builder.append_data(&mut extension, "pax", &record[..]).await.unwrap();
            let mut file = Header::new_ustar();
            file.set_size(0);
            builder.append_data(&mut file, "object", &b""[..]).await.unwrap();
            let mut archive = Archive::new(std::io::Cursor::new(builder.into_inner().await.unwrap()));
            let mut entries = archive.entries().unwrap();
            let mut entry = entries.next().await.unwrap().unwrap();

            let mut opts = ObjectOptions::default();
            let authorization = apply_extract_entry_pax_extensions_for_test(
                &mut entry,
                "bucket",
                "object",
                &state,
                &mut HashMap::new(),
                &mut opts,
            )
            .await
            .unwrap();

            assert_eq!(
                (
                    authorization.object_lock_mode.is_some() || authorization.object_lock_retain_until_date.is_some(),
                    authorization.object_lock_legal_hold_status.is_some(),
                    opts.version_id.is_some() || authorization.headers.contains_key(AMZ_BUCKET_REPLICATION_STATUS),
                ),
                expected,
                "{case} must request only its own additional authorization"
            );
            match case {
                "retention" => {
                    assert!(authorization.headers.contains_key(AMZ_OBJECT_LOCK_MODE_LOWER));
                    assert!(authorization.headers.contains_key(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER));
                }
                "legal-hold" => assert!(authorization.headers.contains_key(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER)),
                "version-id" => assert_eq!(opts.version_id.as_deref(), Some(Uuid::nil().to_string().as_str())),
                "replication-status" => assert_eq!(
                    authorization
                        .headers
                        .get(AMZ_BUCKET_REPLICATION_STATUS)
                        .and_then(|value| value.to_str().ok()),
                    Some("REPLICA")
                ),
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn snowball_pax_rejects_invalid_retention_and_replication_values() {
        let mut invalid_mode = pax_record("minio.metadata.X-Amz-Object-Lock-Mode", b"INVALID");
        invalid_mode.extend(pax_record("minio.metadata.X-Amz-Object-Lock-Retain-Until-Date", b"2099-01-01T00:00:00Z"));
        let mut invalid_date = pax_record("minio.metadata.X-Amz-Object-Lock-Mode", b"COMPLIANCE");
        invalid_date.extend(pax_record("minio.metadata.X-Amz-Object-Lock-Retain-Until-Date", b"not-a-date"));
        let cases = [
            ("invalid-mode", invalid_mode),
            ("invalid-date", invalid_date),
            (
                "invalid-replication-status",
                pax_record("minio.metadata.x-amz-replication-status", b"INVALID"),
            ),
            ("invalid-version-id", pax_record("minio.versionId", b"not-a-uuid")),
        ];
        let state = metadata_sys::ObjectLockConfigState::Configured {
            config: ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: None,
            },
            updated_at: OffsetDateTime::now_utc(),
        };

        for (case, record) in cases {
            let mut builder = Builder::new(Vec::new());
            let mut extension = Header::new_ustar();
            extension.set_size(record.len() as u64);
            extension.set_entry_type(EntryType::XHeader);
            builder.append_data(&mut extension, "pax", &record[..]).await.unwrap();
            let mut file = Header::new_ustar();
            file.set_size(0);
            builder.append_data(&mut file, "object", &b""[..]).await.unwrap();
            let mut archive = Archive::new(std::io::Cursor::new(builder.into_inner().await.unwrap()));
            let mut entries = archive.entries().unwrap();
            let mut entry = entries.next().await.unwrap().unwrap();
            let err = apply_extract_entry_pax_extensions_for_test(
                &mut entry,
                "bucket",
                "object",
                &state,
                &mut HashMap::new(),
                &mut ObjectOptions::default(),
            )
            .await
            .unwrap_err()
            .into_s3_error();

            assert!(
                err.code() == &S3ErrorCode::InvalidArgument || err.code() == &S3ErrorCode::MalformedXML,
                "{case}"
            );
        }
    }

    #[tokio::test]
    async fn snowball_pax_preserves_canonical_minio_metadata_and_valid_retention() {
        let mut record = pax_record("minio.metadata.Content-Type", b"text/plain");
        record.extend(pax_record("minio.metadata.X-Amz-Meta-Owner", b"alice"));
        record.extend(pax_record("minio.metadata.project", b"alpha-demo"));
        record.extend(pax_record("minio.metadata.x-amz-tagging", b"classification=public"));
        record.extend(pax_record("minio.versionId", Uuid::nil().to_string().as_bytes()));
        record.extend(pax_record("minio.metadata.x-amz-replication-status", b"REPLICA"));
        record.extend(pax_record("minio.metadata.X-Amz-Object-Lock-Mode", b"GOVERNANCE"));
        record.extend(pax_record("minio.metadata.X-Amz-Object-Lock-Retain-Until-Date", b"2099-01-01T00:00:00Z"));
        record.extend(pax_record("minio.metadata.X-Amz-Object-Lock-Legal-Hold", b"ON"));
        let mut builder = Builder::new(Vec::new());
        let mut extension = Header::new_ustar();
        extension.set_size(record.len() as u64);
        extension.set_entry_type(EntryType::XHeader);
        builder.append_data(&mut extension, "pax", &record[..]).await.unwrap();

        let mut file = Header::new_ustar();
        file.set_size(0);
        builder.append_data(&mut file, "object.txt", &b""[..]).await.unwrap();
        let mut archive = Archive::new(std::io::Cursor::new(builder.into_inner().await.unwrap()));
        let mut entries = archive.entries().unwrap();
        let mut entry = entries.next().await.unwrap().unwrap();
        let mut metadata = HashMap::from([
            (AMZ_OBJECT_LOCK_MODE_LOWER.to_string(), ObjectLockRetentionMode::COMPLIANCE.to_string()),
            (AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.to_string(), "2030-01-01T00:00:00Z".to_string()),
        ]);
        let mut opts = ObjectOptions::default();
        let state = metadata_sys::ObjectLockConfigState::Configured {
            config: ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: None,
            },
            updated_at: OffsetDateTime::now_utc(),
        };

        let authorization =
            apply_extract_entry_pax_extensions_for_test(&mut entry, "bucket", "object.txt", &state, &mut metadata, &mut opts)
                .await
                .unwrap();

        assert_eq!(metadata.get("content-type").map(String::as_str), Some("text/plain"));
        assert_eq!(metadata.get("owner").map(String::as_str), Some("alice"));
        assert_eq!(metadata.get("project").map(String::as_str), Some("alpha-demo"));
        assert_eq!(metadata.get(AMZ_OBJECT_TAGGING).map(String::as_str), Some("classification=public"));
        assert!(!metadata.contains_key("x-amz-tagging"));
        assert_eq!(metadata.get(AMZ_OBJECT_LOCK_MODE_LOWER).map(String::as_str), Some("GOVERNANCE"));
        assert_eq!(
            metadata.get(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER).map(String::as_str),
            Some("2099-01-01T00:00:00Z")
        );
        assert_eq!(metadata.get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER).map(String::as_str), Some("ON"));
        assert!(metadata.contains_key("x-rustfs-internal-objectlock-legalhold-timestamp"));
        assert!(metadata.contains_key("x-minio-internal-objectlock-legalhold-timestamp"));
        assert_eq!(metadata.get(AMZ_BUCKET_REPLICATION_STATUS).map(String::as_str), Some("REPLICA"));
        assert_eq!(opts.version_id.as_deref(), Some("00000000-0000-0000-0000-000000000000"));
        assert!(authorization.object_lock_mode.is_some());
        assert!(authorization.object_lock_retain_until_date.is_some());
        assert!(authorization.object_lock_legal_hold_status.is_some());
        assert!(opts.version_id.is_some());
        assert!(authorization.headers.contains_key(AMZ_BUCKET_REPLICATION_STATUS));
    }

    #[tokio::test]
    async fn snowball_pax_rejects_legal_hold_without_bucket_object_lock() {
        let record = pax_record("minio.metadata.X-Amz-Object-Lock-Legal-Hold", b"ON");
        let mut builder = Builder::new(Vec::new());
        let mut extension = Header::new_ustar();
        extension.set_size(record.len() as u64);
        extension.set_entry_type(EntryType::XHeader);
        builder.append_data(&mut extension, "pax", &record[..]).await.unwrap();

        let mut file = Header::new_ustar();
        file.set_size(0);
        builder.append_data(&mut file, "object", &b""[..]).await.unwrap();
        let mut archive = Archive::new(std::io::Cursor::new(builder.into_inner().await.unwrap()));
        let mut entries = archive.entries().unwrap();
        let mut entry = entries.next().await.unwrap().unwrap();
        let mut metadata = HashMap::new();
        let err = apply_extract_entry_pax_extensions_for_test(
            &mut entry,
            "bucket",
            "object",
            &metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
            &mut metadata,
            &mut ObjectOptions::default(),
        )
        .await
        .unwrap_err()
        .into_s3_error();

        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(!metadata.contains_key(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER));
    }

    #[tokio::test]
    async fn snowball_pax_rejects_invalid_legal_hold_status() {
        let record = pax_record("minio.metadata.X-Amz-Object-Lock-Legal-Hold", b"INVALID");
        let mut builder = Builder::new(Vec::new());
        let mut extension = Header::new_ustar();
        extension.set_size(record.len() as u64);
        extension.set_entry_type(EntryType::XHeader);
        builder.append_data(&mut extension, "pax", &record[..]).await.unwrap();

        let mut file = Header::new_ustar();
        file.set_size(0);
        builder.append_data(&mut file, "object", &b""[..]).await.unwrap();
        let mut archive = Archive::new(std::io::Cursor::new(builder.into_inner().await.unwrap()));
        let mut entries = archive.entries().unwrap();
        let mut entry = entries.next().await.unwrap().unwrap();
        let mut metadata = HashMap::new();
        let state = metadata_sys::ObjectLockConfigState::Configured {
            config: ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: None,
            },
            updated_at: OffsetDateTime::now_utc(),
        };
        let err = apply_extract_entry_pax_extensions_for_test(
            &mut entry,
            "bucket",
            "object",
            &state,
            &mut metadata,
            &mut ObjectOptions::default(),
        )
        .await
        .unwrap_err()
        .into_s3_error();

        assert_eq!(err.code(), &S3ErrorCode::MalformedXML);
        assert!(!metadata.contains_key(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER));
    }

    #[test]
    fn is_put_object_extract_requested_accepts_meta_header() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("true"));

        assert!(is_put_object_extract_requested(&headers));
    }

    #[test]
    fn is_put_object_extract_requested_accepts_compat_header_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_EXTRACT_COMPAT, HeaderValue::from_static(" TRUE "));

        assert!(is_put_object_extract_requested(&headers));
    }

    #[test]
    fn is_put_object_extract_requested_rejects_missing_or_false_value() {
        let mut headers = HeaderMap::new();
        assert!(!is_put_object_extract_requested(&headers));

        headers.insert(AMZ_SNOWBALL_EXTRACT, HeaderValue::from_static("false"));
        assert!(!is_put_object_extract_requested(&headers));
    }

    #[test]
    fn normalize_snowball_prefix_trims_slashes_and_whitespace() {
        assert_eq!(
            normalize_snowball_prefix(" /batch/incoming/ ").unwrap(),
            Some("batch/incoming".to_string())
        );
        assert_eq!(normalize_snowball_prefix("///").unwrap(), None);
    }

    #[test]
    fn normalize_snowball_prefix_rejects_parent_dir_components() {
        assert!(normalize_snowball_prefix("../victim-bucket").is_err());
        assert!(normalize_snowball_prefix("safe/../../victim-bucket").is_err());
        assert!(normalize_snowball_prefix("safe\\..\\victim-bucket").is_err());
    }

    #[test]
    fn normalize_extract_entry_key_applies_prefix_and_directory_suffix() {
        assert_eq!(
            normalize_extract_entry_key("nested/path.txt", Some("imports"), false).unwrap(),
            "imports/nested/path.txt"
        );
        assert_eq!(
            normalize_extract_entry_key("nested/dir/", Some("imports"), true).unwrap(),
            "imports/nested/dir/"
        );
        assert_eq!(normalize_extract_entry_key("top-level", None, false).unwrap(), "top-level");
    }

    #[test]
    fn normalize_extract_entry_key_rejects_bucket_escape_paths() {
        assert!(normalize_extract_entry_key("../victim-bucket/evil.txt", None, false).is_err());
        assert!(normalize_extract_entry_key("safe/../../victim-bucket/evil.txt", None, false).is_err());
        assert!(normalize_extract_entry_key("safe\\..\\victim-bucket\\evil.txt", None, false).is_err());
        assert!(normalize_extract_entry_key("evil.txt", Some("../victim-bucket"), false).is_err());
    }

    #[test]
    fn resolve_put_object_extract_options_defaults_when_headers_missing() {
        let headers = HeaderMap::new();
        let options = resolve_put_object_extract_options(&headers).unwrap();
        assert_eq!(
            options,
            PutObjectExtractOptions {
                prefix: None,
                ignore_dirs: false,
                ignore_errors: false
            }
        );
    }

    #[test]
    fn resolve_put_object_extract_options_accepts_internal_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_PREFIX_INTERNAL, HeaderValue::from_static("/internal/prefix/"));
        headers.insert(AMZ_SNOWBALL_IGNORE_DIRS_INTERNAL, HeaderValue::from_static("true"));
        headers.insert(AMZ_SNOWBALL_IGNORE_ERRORS_INTERNAL, HeaderValue::from_static("TRUE"));

        let options = resolve_put_object_extract_options(&headers).unwrap();
        assert_eq!(options.prefix.as_deref(), Some("internal/prefix"));
        assert!(options.ignore_dirs);
        assert!(options.ignore_errors);
    }

    #[test]
    fn resolve_put_object_extract_options_accepts_standard_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_PREFIX, HeaderValue::from_static(" /standard/prefix/ "));
        headers.insert(AMZ_SNOWBALL_IGNORE_DIRS, HeaderValue::from_static(" true "));
        headers.insert(AMZ_SNOWBALL_IGNORE_ERRORS, HeaderValue::from_static("TRUE"));

        let options = resolve_put_object_extract_options(&headers).unwrap();
        assert_eq!(options.prefix.as_deref(), Some("standard/prefix"));
        assert!(options.ignore_dirs);
        assert!(options.ignore_errors);
    }

    #[test]
    fn resolve_put_object_extract_options_accepts_suffix_compatible_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-amz-meta-acme-snowball-prefix"),
            HeaderValue::from_static(" /partner/import "),
        );
        headers.insert(
            HeaderName::from_static("x-amz-meta-acme-snowball-ignore-dirs"),
            HeaderValue::from_static(" true "),
        );
        headers.insert(
            HeaderName::from_static("x-amz-meta-acme-snowball-ignore-errors"),
            HeaderValue::from_static("TRUE"),
        );

        let options = resolve_put_object_extract_options(&headers).unwrap();
        assert_eq!(options.prefix.as_deref(), Some("partner/import"));
        assert!(options.ignore_dirs);
        assert!(options.ignore_errors);
    }

    #[test]
    fn resolve_put_object_extract_options_prefers_exact_headers_over_suffix_fallback() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-meta-acme-snowball-prefix", HeaderValue::from_static("/fallback/prefix/"));
        headers.insert(AMZ_RUSTFS_SNOWBALL_PREFIX, HeaderValue::from_static("/internal/prefix/"));
        headers.insert(AMZ_SNOWBALL_PREFIX, HeaderValue::from_static("/standard/prefix/"));
        headers.insert(AMZ_MINIO_SNOWBALL_PREFIX, HeaderValue::from_static("/minio/prefix/"));

        let options = resolve_put_object_extract_options(&headers).unwrap();
        assert_eq!(options.prefix.as_deref(), Some("minio/prefix"));
    }

    #[test]
    fn resolve_put_object_extract_options_exact_flags_override_suffix_fallback() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_IGNORE_DIRS, HeaderValue::from_static("false"));
        headers.insert("x-amz-meta-acme-snowball-ignore-dirs", HeaderValue::from_static("true"));
        headers.insert(AMZ_RUSTFS_SNOWBALL_IGNORE_ERRORS, HeaderValue::from_static("false"));
        headers.insert("x-amz-meta-acme-snowball-ignore-errors", HeaderValue::from_static("true"));

        let options = resolve_put_object_extract_options(&headers).unwrap();
        assert!(!options.ignore_dirs);
        assert!(!options.ignore_errors);
    }

    #[test]
    fn resolve_put_object_extract_options_rejects_unsafe_prefix_header() {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_SNOWBALL_PREFIX, HeaderValue::from_static("../victim-bucket"));

        assert!(resolve_put_object_extract_options(&headers).is_err());
    }

    #[test]
    fn validate_put_object_extract_entry_count_rejects_limit_overflow() {
        let limits = ArchiveLimits {
            max_entries: 1,
            ..ArchiveLimits::default()
        };

        let err = validate_put_object_extract_entry_count(2, limits).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_put_object_extract_entry_size_rejects_oversized_entry() {
        let limits = ArchiveLimits {
            max_entry_size: 8,
            ..ArchiveLimits::default()
        };

        let err = validate_put_object_extract_entry_size("payload.bin", 9, limits).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_put_object_extract_total_size_rejects_cumulative_overflow() {
        let limits = ArchiveLimits {
            max_total_unpacked_size: 16,
            ..ArchiveLimits::default()
        };

        let err = validate_put_object_extract_total_size(17, limits).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_put_object_extract_entry_path_rejects_overlong_path() {
        let limits = ArchiveLimits {
            max_path_length: 8,
            ..ArchiveLimits::default()
        };

        let err = validate_put_object_extract_entry_path("toolong-path", limits).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn extract_entry_error_boundary_only_ignores_recoverable_members() {
        let recoverable = ExtractEntryError::Recoverable(object_s3_error(S3ErrorCode::InvalidArgument, "invalid member"));
        recoverable
            .ignore_or_return(true)
            .expect("recoverable member should be skipped");

        let recoverable = ExtractEntryError::Recoverable(object_s3_error(S3ErrorCode::InvalidArgument, "invalid member"));
        assert_eq!(
            recoverable
                .ignore_or_return(false)
                .expect_err("recoverable member should fail without ignore-errors")
                .code(),
            &S3ErrorCode::InvalidArgument
        );

        let fatal = ExtractEntryError::Fatal(object_s3_error(S3ErrorCode::InvalidArgument, "invalid archive"));
        assert_eq!(
            fatal
                .ignore_or_return(true)
                .expect_err("fatal archive errors must ignore ignore-errors")
                .code(),
            &S3ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn strict_extract_entry_path_rejects_non_utf8_without_lossy_replacement() {
        let invalid = strict_extract_entry_path(b"same-\xff-key").expect_err("non-UTF-8 member path must be rejected");
        assert!(invalid.is_recoverable());
        invalid
            .ignore_or_return(true)
            .expect("ignore-errors should skip an invalid member key");
    }

    #[test]
    fn classify_extract_entry_type_skips_links_extensions_and_continuous_entries() {
        assert_eq!(classify_extract_entry_type(EntryType::Regular), ExtractEntryDisposition::File);
        assert_eq!(classify_extract_entry_type(EntryType::Directory), ExtractEntryDisposition::Directory);
        for entry_type in [
            EntryType::Link,
            EntryType::Symlink,
            EntryType::Continuous,
            EntryType::XGlobalHeader,
            EntryType::Other(b'V'),
        ] {
            assert_eq!(
                classify_extract_entry_type(entry_type),
                ExtractEntryDisposition::FormatSkip,
                "{entry_type:?} must not be materialized as an object"
            );
        }
    }

    #[test]
    fn extract_entry_quota_growth_counts_only_materialized_files() {
        assert_eq!(extract_entry_quota_growth(ExtractEntryDisposition::File, 9), 9);
        assert_eq!(extract_entry_quota_growth(ExtractEntryDisposition::Directory, 9), 0);
        assert_eq!(extract_entry_quota_growth(ExtractEntryDisposition::FormatSkip, 9), 0);
    }

    #[test]
    fn archive_mod_time_treats_negative_gnu_base256_as_unset() {
        let mut header = Header::new_gnu();
        header.as_old_mut().mtime.fill(0xff);

        assert_eq!(
            extract_archive_entry_mod_time(&header).expect("negative GNU mtime should be accepted"),
            None
        );
    }

    #[test]
    fn archive_mod_time_keeps_malformed_octal_fatal() {
        let mut header = Header::new_ustar();
        header.as_old_mut().mtime.fill(b'9');

        let err = extract_archive_entry_mod_time(&header).expect_err("malformed octal mtime must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn pax_metadata_budget_is_fatal_even_with_ignore_errors() {
        let limits = ArchiveLimits {
            max_pax_metadata_size: 3,
            ..ArchiveLimits::default()
        };
        let mut entry_size = 0;
        let mut total_size = 0;
        let err = record_extract_pax_metadata_bytes(&mut entry_size, &mut total_size, 2, 2, limits)
            .expect_err("PAX metadata over the resource budget must fail");
        assert!(!err.is_recoverable());
        assert!(err.ignore_or_return(true).is_err(), "ignore-errors must not bypass resource limits");
    }

    #[test]
    fn pax_metadata_record_budget_is_fatal_even_with_ignore_errors() {
        let limits = ArchiveLimits {
            max_pax_metadata_records: 1,
            ..ArchiveLimits::default()
        };
        let mut entry_records = 0;
        let mut total_records = 0;

        record_extract_pax_metadata_record(&mut entry_records, &mut total_records, limits)
            .expect("first PAX record should fit the budget");
        let err = record_extract_pax_metadata_record(&mut entry_records, &mut total_records, limits)
            .expect_err("second PAX record must exceed the per-entry budget");

        assert!(!err.is_recoverable());
        assert!(err.ignore_or_return(true).is_err(), "ignore-errors must not bypass record limits");

        let limits = ArchiveLimits {
            max_pax_metadata_records: 2,
            max_total_pax_metadata_records: 1,
            ..ArchiveLimits::default()
        };
        let mut first_entry_records = 0;
        let mut second_entry_records = 0;
        let mut total_records = 0;
        record_extract_pax_metadata_record(&mut first_entry_records, &mut total_records, limits)
            .expect("first archive PAX record should fit the total budget");
        let err = record_extract_pax_metadata_record(&mut second_entry_records, &mut total_records, limits)
            .expect_err("second archive PAX record must exceed the total budget");
        assert!(!err.is_recoverable());
    }

    #[tokio::test]
    async fn extract_archive_builder_rejects_oversized_extension_payload() {
        let record = pax_record("comment", b"value");
        let mut builder = Builder::new(Vec::new());
        let mut extension = Header::new_ustar();
        extension.set_entry_type(EntryType::XHeader);
        extension.set_size(u64::try_from(record.len()).expect("fixture size must fit u64"));
        extension.set_cksum();
        builder
            .append_data(&mut extension, "pax", record.as_slice())
            .await
            .expect("PAX extension fixture should be appended");
        let bytes = builder.into_inner().await.expect("PAX extension fixture should finalize");
        let limits = ArchiveLimits {
            max_pax_metadata_size: u64::try_from(record.len() - 1).expect("fixture size must fit u64"),
            ..ArchiveLimits::default()
        };

        let mut archive = build_put_object_extract_archive(std::io::Cursor::new(bytes), limits);
        let err = archive
            .entries()
            .expect("archive entry stream should be created")
            .next()
            .await
            .expect("extension header should produce a result")
            .expect_err("dependency must reject the extension before buffering its payload");

        assert_eq!(err.to_string(), "archive extension entry size limit exceeded");
    }

    #[tokio::test]
    async fn pax_metadata_budget_counts_format_skipped_members() {
        let record = pax_record("comment", b"oversized-symlink-metadata");
        let mut entry = entry_with_local_pax(&record, EntryType::Symlink).await;
        let limits = ArchiveLimits {
            max_pax_metadata_size: 8,
            ..ArchiveLimits::default()
        };
        let mut total_size = 0;
        let mut total_records = 0;

        let err = count_extract_entry_pax_metadata(&mut entry, &mut total_size, &mut total_records, limits)
            .await
            .expect_err("format-skipped members must still consume the PAX budget");

        assert!(!err.is_recoverable());
        assert!(err.ignore_or_return(true).is_err());
    }

    #[tokio::test]
    async fn pax_metadata_budget_precedes_recoverable_semantic_errors() {
        let invalid_key = "minio.metadata.x-amz-meta-owner";
        let mut record = pax_record(invalid_key, b"\0");
        record.extend(pax_record("comment", b"oversized-tail"));
        let mut entry = entry_with_local_pax(&record, EntryType::Regular).await;

        let semantic_err = apply_extract_entry_pax_extensions(
            &mut entry,
            "bucket",
            "member",
            &metadata_sys::ObjectLockConfigState::ConfirmedAbsent,
            &mut HashMap::new(),
            &mut ObjectOptions::default(),
        )
        .await
        .expect_err("NUL metadata value should be a recoverable member error");
        assert!(semantic_err.is_recoverable());

        let limits = ArchiveLimits {
            max_pax_metadata_size: u64::try_from(invalid_key.len() + 1).expect("fixture size must fit u64"),
            ..ArchiveLimits::default()
        };
        let mut total_size = 0;
        let mut total_records = 0;
        let budget_err = count_extract_entry_pax_metadata(&mut entry, &mut total_size, &mut total_records, limits)
            .await
            .expect_err("the oversized tail must be counted before ignore-errors can skip the member");

        assert!(!budget_err.is_recoverable());
        assert!(budget_err.ignore_or_return(true).is_err());
    }

    #[tokio::test]
    async fn extract_decoded_reader_enforces_exact_byte_limit() {
        let mut exact = ExtractDecodedLimitReader::new(std::io::Cursor::new(b"1234"), 4);
        let mut exact_bytes = Vec::new();
        exact
            .read_to_end(&mut exact_bytes)
            .await
            .expect("decoded stream at the limit should succeed");
        assert_eq!(exact_bytes, b"1234");

        let mut oversized = ExtractDecodedLimitReader::new(std::io::Cursor::new(b"12345"), 4);
        let mut oversized_bytes = Vec::new();
        let err = oversized
            .read_to_end(&mut oversized_bytes)
            .await
            .expect_err("decoded stream over the limit must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn extract_member_read_tracker_keeps_integrity_errors_fatal_under_ignore_errors() {
        struct FailingReader;

        impl AsyncRead for FailingReader {
            fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
                Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "member decoder failed")))
            }
        }

        let reader = HashReader::from_stream(FailingReader, 1, 1, None, None, false).unwrap();
        let (mut tracked, failed) = track_extract_member_read_errors(reader).unwrap();
        let mut output = Vec::new();
        let err = tracked.read_to_end(&mut output).await.unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(failed.load(Ordering::Acquire));
        assert!(!should_ignore_extract_member_write_error(true, &failed));

        let storage_only_failure = AtomicBool::new(false);
        assert!(should_ignore_extract_member_write_error(true, &storage_only_failure));
        assert!(!should_ignore_extract_member_write_error(false, &storage_only_failure));
    }

    #[tokio::test]
    async fn snowball_extract_body_guard_aborts_stalled_upload() {
        let body = StreamingBlob::wrap(futures::stream::pending::<Result<Bytes, std::io::Error>>());
        let mut guarded = guard_put_object_body_read_timeout(
            body,
            "test-bucket",
            "archive.tar",
            "snowball-timeout",
            Some(512),
            Duration::from_millis(1),
        );

        let err = guarded
            .next()
            .await
            .expect("stalled Snowball body should yield an error")
            .expect_err("stalled Snowball body must not hang");
        let io_err = err
            .downcast_ref::<std::io::Error>()
            .expect("stall error should retain its I/O kind");
        assert_eq!(io_err.kind(), std::io::ErrorKind::TimedOut);
    }

    #[test]
    fn legacy_archive_quota_rejects_cumulative_size_and_overflow() {
        let legacy = QuotaCheckResult {
            allowed: true,
            current_usage: Some(4),
            quota_limit: Some(5),
            operation_size: 0,
            remaining: Some(1),
            uses_durable_reservations: false,
        };
        assert!(ensure_legacy_archive_size_within_quota(&legacy, 2).is_err());
        assert!(ensure_legacy_archive_size_within_quota(&legacy, 1).is_ok());

        let maxed = QuotaCheckResult {
            current_usage: Some(u64::MAX),
            quota_limit: Some(u64::MAX),
            ..legacy
        };
        assert!(ensure_legacy_archive_size_within_quota(&maxed, 1).is_err());
    }
}
