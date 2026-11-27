// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bulk Operations for Swift API
//!
//! This module implements bulk operations that allow batch processing of
//! multiple objects in a single request, improving efficiency for operations
//! that affect many files.
//!
//! # Operations
//!
//! ## Bulk Delete
//!
//! Delete multiple objects in a single request.
//!
//! **Endpoint**: `DELETE /?bulk-delete`
//!
//! **Request Body**: Newline-separated list of object paths
//! ```text
//! /container1/object1.txt
//! /container2/folder/object2.txt
//! /container1/object3.txt
//! ```
//!
//! **Response**: JSON with results for each object
//! ```json
//! {
//!   "Number Deleted": 2,
//!   "Number Not Found": 1,
//!   "Errors": [],
//!   "Response Status": "200 OK",
//!   "Response Body": ""
//! }
//! ```
//!
//! ## Bulk Extract
//!
//! Extract files from an uploaded archive into a container.
//!
//! **Endpoint**: `PUT /{container}?extract-archive=tar` (or tar.gz, tar.bz2)
//!
//! **Request Body**: Archive file contents
//!
//! **Response**: JSON with results for each extracted file
//! ```json
//! {
//!   "Number Files Created": 10,
//!   "Errors": [],
//!   "Response Status": "201 Created",
//!   "Response Body": ""
//! }
//! ```
//!
//! # Examples
//!
//! ```bash
//! # Bulk delete
//! echo -e "/container/file1.txt\n/container/file2.txt" | \
//!   swift delete --bulk
//!
//! # Bulk extract
//! tar czf archive.tar.gz files/
//! swift upload container --extract-archive archive.tar.gz
//! ```

use super::{SwiftError, SwiftResult, account, container, object};
use axum::http::{HeaderMap, HeaderValue, Response, StatusCode};
use futures::StreamExt;
use rustfs_credentials::Credentials;
use rustfs_s3_types::Body;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tracing::{debug, error};

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_SWIFT_BULK: &str = "swift_bulk";
const EVENT_SWIFT_BULK_DELETE_STATE: &str = "swift_bulk_delete_state";
const EVENT_SWIFT_BULK_EXTRACT_STATE: &str = "swift_bulk_extract_state";

/// Maximum request body size accepted by Swift bulk delete (8 MiB).
pub const MAX_BULK_DELETE_BODY_SIZE: usize = 8 * 1024 * 1024;
/// Maximum number of object paths accepted per Swift bulk delete request.
pub const MAX_BULK_DELETE_PATHS: usize = 10_000;
/// Maximum number of error entries retained in a bulk delete response.
pub const MAX_BULK_DELETE_ERRORS: usize = 1_000;

/// Maximum compressed request body size accepted by Swift bulk extract (64 MiB).
pub const MAX_BULK_EXTRACT_ARCHIVE_BYTES: usize = 64 * 1024 * 1024;
/// Maximum total decompressed bytes read from a bulk extract archive (256 MiB).
pub const MAX_BULK_EXTRACT_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
/// Maximum single extracted file size in a bulk extract archive (64 MiB).
pub const MAX_BULK_EXTRACT_ENTRY_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum number of regular files extracted per bulk extract archive.
pub const MAX_BULK_EXTRACT_FILES: usize = 1_000;
/// Maximum total tar entries (including directories) processed per bulk extract archive.
pub const MAX_BULK_EXTRACT_ENTRIES: usize = 10_000;
/// Maximum number of error entries retained in a bulk extract response.
pub const MAX_BULK_EXTRACT_ERRORS: usize = 1_000;

/// Limits enforced on bulk delete requests.
#[derive(Debug, Clone, Copy)]
pub struct BulkDeleteLimits {
    pub max_body_bytes: usize,
    pub max_paths: usize,
    pub max_errors: usize,
}

impl Default for BulkDeleteLimits {
    fn default() -> Self {
        Self {
            max_body_bytes: MAX_BULK_DELETE_BODY_SIZE,
            max_paths: MAX_BULK_DELETE_PATHS,
            max_errors: MAX_BULK_DELETE_ERRORS,
        }
    }
}

/// Limits enforced on bulk extract archives and their decompressed expansion.
#[derive(Debug, Clone, Copy)]
pub struct BulkExtractLimits {
    pub max_archive_bytes: usize,
    pub max_total_decompressed_bytes: u64,
    pub max_entry_bytes: u64,
    pub max_files: usize,
    pub max_total_entries: usize,
    pub max_errors: usize,
}

impl Default for BulkExtractLimits {
    fn default() -> Self {
        Self {
            max_archive_bytes: MAX_BULK_EXTRACT_ARCHIVE_BYTES,
            max_total_decompressed_bytes: MAX_BULK_EXTRACT_TOTAL_BYTES,
            max_entry_bytes: MAX_BULK_EXTRACT_ENTRY_BYTES,
            max_files: MAX_BULK_EXTRACT_FILES,
            max_total_entries: MAX_BULK_EXTRACT_ENTRIES,
            max_errors: MAX_BULK_EXTRACT_ERRORS,
        }
    }
}

/// Wrapper around an archive decompressor that aborts as soon as total
/// decompressed output exceeds `max_bytes`.
struct LimitedDecompressReader<R> {
    inner: R,
    max_bytes: u64,
    bytes_read: u64,
    exceeded: Arc<AtomicBool>,
}

impl<R> LimitedDecompressReader<R> {
    fn new(inner: R, max_bytes: u64, exceeded: Arc<AtomicBool>) -> Self {
        Self {
            inner,
            max_bytes,
            bytes_read: 0,
            exceeded,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for LimitedDecompressReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if self.exceeded.load(Ordering::Acquire) {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("decompressed archive exceeds maximum of {} bytes", self.max_bytes),
            )));
        }

        let remaining = self.max_bytes.saturating_sub(self.bytes_read);
        if remaining == 0 {
            // Probe at most 1 byte into a local buffer so the caller's ReadBuf is
            // never modified when returning Poll::Ready(Err(_)).
            let mut probe = [0u8; 1];
            let mut probe_buf = ReadBuf::new(&mut probe);
            return match Pin::new(&mut self.inner).poll_read(cx, &mut probe_buf) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => {
                    if probe_buf.filled().is_empty() {
                        Poll::Ready(Ok(()))
                    } else {
                        self.exceeded.store(true, Ordering::Release);
                        Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("decompressed archive exceeds maximum of {} bytes", self.max_bytes),
                        )))
                    }
                }
            };
        }

        let cap = remaining.min(buf.remaining() as u64) as usize;
        if cap == 0 {
            return Poll::Ready(Ok(()));
        }

        let slice = buf.initialize_unfilled_to(cap);
        let mut sub = ReadBuf::new(slice);
        match Pin::new(&mut self.inner).poll_read(cx, &mut sub) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Ready(Ok(())) => {
                let read = sub.filled().len();
                buf.advance(read);
                self.bytes_read = self.bytes_read.saturating_add(read as u64);
                Poll::Ready(Ok(()))
            }
        }
    }
}

/// Result of a single delete operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResult {
    /// Object path that was deleted
    pub path: String,

    /// HTTP status code for this operation
    pub status: u16,

    /// Error message if deletion failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Bulk delete response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkDeleteResponse {
    /// Number of objects successfully deleted
    #[serde(rename = "Number Deleted")]
    pub number_deleted: usize,

    /// Number of objects not found
    #[serde(rename = "Number Not Found")]
    pub number_not_found: usize,

    /// List of errors encountered
    #[serde(rename = "Errors")]
    pub errors: Vec<Vec<String>>,

    /// Overall response status
    #[serde(rename = "Response Status")]
    pub response_status: String,

    /// Response body (usually empty)
    #[serde(rename = "Response Body")]
    pub response_body: String,
}

impl Default for BulkDeleteResponse {
    fn default() -> Self {
        Self {
            number_deleted: 0,
            number_not_found: 0,
            errors: Vec::new(),
            response_status: "200 OK".to_string(),
            response_body: String::new(),
        }
    }
}

/// Result of a single file extraction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractResult {
    /// File path that was extracted
    pub path: String,

    /// HTTP status code for this operation
    pub status: u16,

    /// Error message if extraction failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Bulk extract response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkExtractResponse {
    /// Number of files successfully created
    #[serde(rename = "Number Files Created")]
    pub number_files_created: usize,

    /// List of errors encountered
    #[serde(rename = "Errors")]
    pub errors: Vec<Vec<String>>,

    /// Overall response status
    #[serde(rename = "Response Status")]
    pub response_status: String,

    /// Response body (usually empty)
    #[serde(rename = "Response Body")]
    pub response_body: String,
}

impl Default for BulkExtractResponse {
    fn default() -> Self {
        Self {
            number_files_created: 0,
            errors: Vec::new(),
            response_status: "201 Created".to_string(),
            response_body: String::new(),
        }
    }
}

/// Parse object path from bulk delete request
///
/// Paths should be in format: /container/object
fn parse_object_path(path: &str) -> SwiftResult<(String, String)> {
    let path = path.trim();

    if path.is_empty() {
        return Err(SwiftError::BadRequest("Empty path in bulk delete".to_string()));
    }

    // Remove leading slash
    let path = path.trim_start_matches('/');

    // Split into container and object
    let parts: Vec<&str> = path.splitn(2, '/').collect();

    if parts.len() != 2 {
        return Err(SwiftError::BadRequest(format!(
            "Invalid path format: {}. Expected /container/object",
            path
        )));
    }

    if parts[0].is_empty() || parts[1].is_empty() {
        return Err(SwiftError::BadRequest(format!("Empty container or object name in path: {}", path)));
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Handle bulk delete request
///
/// Deletes multiple objects specified in the request body
pub async fn handle_bulk_delete(account: &str, body: String, credentials: &Credentials) -> SwiftResult<Response<Body>> {
    handle_bulk_delete_with_limits(account, body, credentials, BulkDeleteLimits::default()).await
}

/// Handle bulk delete request with explicit resource limits.
pub async fn handle_bulk_delete_with_limits(
    account: &str,
    body: String,
    credentials: &Credentials,
    limits: BulkDeleteLimits,
) -> SwiftResult<Response<Body>> {
    debug!(
        event = EVENT_SWIFT_BULK_DELETE_STATE,
        component = LOG_COMPONENT_PROTOCOLS,
        subsystem = LOG_SUBSYSTEM_SWIFT_BULK,
        state = "started",
        account = %account,
        "swift bulk delete state changed"
    );

    if body.len() > limits.max_body_bytes {
        return Err(SwiftError::RequestEntityTooLarge(format!(
            "Bulk delete request body exceeds maximum size of {} bytes",
            limits.max_body_bytes
        )));
    }

    account::validate_account_access(account, credentials)?;

    // Count non-empty paths lazily without materializing an unbounded Vec.
    let mut path_count = 0usize;
    for _ in body.lines().map(str::trim).filter(|line| !line.is_empty()) {
        path_count += 1;
        if path_count > limits.max_paths {
            return Err(SwiftError::RequestEntityTooLarge(format!(
                "Bulk delete request exceeds maximum of {} paths",
                limits.max_paths
            )));
        }
    }

    if path_count == 0 {
        return Err(SwiftError::BadRequest("No paths provided for bulk delete".to_string()));
    }

    debug!(
        event = EVENT_SWIFT_BULK_DELETE_STATE,
        component = LOG_COMPONENT_PROTOCOLS,
        subsystem = LOG_SUBSYSTEM_SWIFT_BULK,
        state = "processing",
        account = %account,
        path_count,
        "swift bulk delete state changed"
    );

    let mut response = BulkDeleteResponse::default();

    // Process each path lazily while capping retained error entries.
    for path in body.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if response.errors.len() >= limits.max_errors {
            response.response_body = format!("Maximum number of errors ({}) exceeded", limits.max_errors);
            break;
        }

        match parse_object_path(path) {
            Ok((container, object_key)) => match object::delete_object(account, &container, &object_key, credentials).await {
                Ok(_) => {
                    response.number_deleted += 1;
                }
                Err(SwiftError::NotFound(_)) => {
                    response.number_not_found += 1;
                }
                Err(e) => {
                    error!(
                        event = EVENT_SWIFT_BULK_DELETE_STATE,
                        component = LOG_COMPONENT_PROTOCOLS,
                        subsystem = LOG_SUBSYSTEM_SWIFT_BULK,
                        result = "delete_failed",
                        account = %account,
                        path = %path,
                        error = %e,
                        "swift bulk delete state changed"
                    );
                    response.errors.push(vec![path.to_string(), e.to_string()]);
                }
            },
            Err(e) => {
                error!(
                    event = EVENT_SWIFT_BULK_DELETE_STATE,
                    component = LOG_COMPONENT_PROTOCOLS,
                    subsystem = LOG_SUBSYSTEM_SWIFT_BULK,
                    result = "invalid_path",
                    account = %account,
                    path = %path,
                    error = %e,
                    "swift bulk delete state changed"
                );
                response.errors.push(vec![path.to_string(), e.to_string()]);
            }
        }
    }

    // Determine overall status
    if !response.errors.is_empty() {
        response.response_status = "400 Bad Request".to_string();
    }

    // Serialize response
    let json = serde_json::to_string(&response)
        .map_err(|e| SwiftError::InternalServerError(format!("JSON serialization failed: {}", e)))?;

    let trans_id = super::handler::generate_trans_id();
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json; charset=utf-8")
        .header("content-length", json.len().to_string())
        .header("x-trans-id", trans_id.clone())
        .header("x-openstack-request-id", trans_id)
        .body(Body::from(json))
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
}

/// Archive format supported for bulk extract
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveFormat {
    /// Uncompressed tar
    Tar,
    /// Gzip-compressed tar
    TarGz,
    /// Bzip2-compressed tar
    TarBz2,
}

impl ArchiveFormat {
    /// Parse archive format from query parameter
    pub fn from_query(query: &str) -> SwiftResult<Self> {
        match query {
            "tar" => Ok(ArchiveFormat::Tar),
            "tar.gz" | "tgz" => Ok(ArchiveFormat::TarGz),
            "tar.bz2" | "tbz2" | "tbz" => Ok(ArchiveFormat::TarBz2),
            _ => Err(SwiftError::BadRequest(format!(
                "Unsupported archive format: {}. Supported: tar, tar.gz, tar.bz2",
                query
            ))),
        }
    }
}

/// Handle bulk extract request
///
/// Extracts files from an uploaded archive into the specified container, streaming
/// one entry at a time under strict compressed, decompressed, entry-count, and
/// container-quota limits.
pub async fn handle_bulk_extract(
    account: &str,
    container: &str,
    format: ArchiveFormat,
    body: Vec<u8>,
    credentials: &Credentials,
) -> SwiftResult<Response<Body>> {
    handle_bulk_extract_with_limits(account, container, format, body, credentials, BulkExtractLimits::default()).await
}

/// Handle bulk extract request with explicit archive expansion limits.
pub async fn handle_bulk_extract_with_limits(
    account: &str,
    container: &str,
    format: ArchiveFormat,
    body: Vec<u8>,
    credentials: &Credentials,
    limits: BulkExtractLimits,
) -> SwiftResult<Response<Body>> {
    debug!(
        event = EVENT_SWIFT_BULK_EXTRACT_STATE,
        component = LOG_COMPONENT_PROTOCOLS,
        subsystem = LOG_SUBSYSTEM_SWIFT_BULK,
        state = "started",
        account = %account,
        container = %container,
        format = ?format,
        "swift bulk extract state changed"
    );

    if body.len() > limits.max_archive_bytes {
        return Err(SwiftError::RequestEntityTooLarge(format!(
            "Bulk extract archive exceeds maximum size of {} bytes",
            limits.max_archive_bytes
        )));
    }

    // Verify account access and container existence without requiring a usage snapshot
    // (quotas are enforced per extracted object inside `object::put_object`).
    container::get_container_custom_metadata(account, container, credentials).await?;

    let mut response = BulkExtractResponse::default();
    let mut consumer = TarEntryConsumer::Upload {
        account,
        container,
        credentials,
        max_errors: limits.max_errors,
        response: &mut response,
    };
    stream_tar_entries(format, body, limits, &mut consumer).await?;

    // Determine overall status
    if response.number_files_created == 0 {
        response.response_status = "400 Bad Request".to_string();
    } else if !response.errors.is_empty() {
        response.response_status = "201 Created".to_string();
    }

    // Serialize response
    let json = serde_json::to_string(&response)
        .map_err(|e| SwiftError::InternalServerError(format!("JSON serialization failed: {}", e)))?;

    let trans_id = super::handler::generate_trans_id();
    let status = if response.number_files_created > 0 {
        StatusCode::CREATED
    } else {
        StatusCode::BAD_REQUEST
    };

    Response::builder()
        .status(status)
        .header("content-type", "application/json; charset=utf-8")
        .header("content-length", json.len().to_string())
        .header("x-trans-id", trans_id.clone())
        .header("x-openstack-request-id", trans_id)
        .body(Body::from(json))
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
}

fn build_bounded_archive_reader(
    format: ArchiveFormat,
    body: Vec<u8>,
    max_total_decompressed_bytes: u64,
    decompressed_exceeded: Arc<AtomicBool>,
) -> Box<dyn AsyncRead + Unpin + Send> {
    let raw_reader: Box<dyn AsyncRead + Unpin + Send> = match format {
        ArchiveFormat::Tar => Box::new(std::io::Cursor::new(body)),
        ArchiveFormat::TarGz => {
            let cursor = std::io::Cursor::new(body);
            Box::new(async_compression::tokio::bufread::GzipDecoder::new(tokio::io::BufReader::new(cursor)))
        }
        ArchiveFormat::TarBz2 => {
            let cursor = std::io::Cursor::new(body);
            Box::new(async_compression::tokio::bufread::BzDecoder::new(tokio::io::BufReader::new(cursor)))
        }
    };
    Box::new(LimitedDecompressReader::new(
        raw_reader,
        max_total_decompressed_bytes,
        decompressed_exceeded,
    ))
}

enum TarEntryConsumer<'a> {
    Upload {
        account: &'a str,
        container: &'a str,
        credentials: &'a Credentials,
        max_errors: usize,
        response: &'a mut BulkExtractResponse,
    },
    #[cfg(test)]
    Collect(&'a mut Vec<(String, Vec<u8>)>),
}

impl TarEntryConsumer<'_> {
    async fn handle_file(&mut self, path_str: String, contents: Vec<u8>) -> SwiftResult<()> {
        match self {
            Self::Upload {
                account,
                container,
                credentials,
                max_errors,
                response,
            } => {
                let mut upload_headers = HeaderMap::new();
                if let Ok(len_header) = HeaderValue::from_str(&contents.len().to_string()) {
                    upload_headers.insert("content-length", len_header);
                }

                match Box::pin(object::put_object(
                    account,
                    container,
                    &path_str,
                    credentials,
                    std::io::Cursor::new(contents),
                    &upload_headers,
                ))
                .await
                {
                    Ok(_) => {
                        response.number_files_created += 1;
                        debug!(
                            event = EVENT_SWIFT_BULK_EXTRACT_STATE,
                            component = LOG_COMPONENT_PROTOCOLS,
                            subsystem = LOG_SUBSYSTEM_SWIFT_BULK,
                            state = "file_created",
                            container = %container,
                            object = %path_str,
                            "swift bulk extract state changed"
                        );
                    }
                    Err(e) => {
                        error!(
                            event = EVENT_SWIFT_BULK_EXTRACT_STATE,
                            component = LOG_COMPONENT_PROTOCOLS,
                            subsystem = LOG_SUBSYSTEM_SWIFT_BULK,
                            result = "upload_failed",
                            container = %container,
                            object = %path_str,
                            error = %e,
                            "swift bulk extract state changed"
                        );
                        if response.errors.len() < *max_errors {
                            response.errors.push(vec![path_str, e.to_string()]);
                        } else if response.response_body.is_empty() {
                            response.response_body = format!("Maximum number of errors ({}) exceeded", *max_errors);
                        }
                    }
                }
                Ok(())
            }
            #[cfg(test)]
            Self::Collect(entries) => {
                entries.push((path_str, contents));
                Ok(())
            }
        }
    }
}

/// Stream tar entries one at a time under strict compressed, decompressed, per-entry,
/// and entry-count limits so neither a compression bomb nor a high-cardinality archive
/// can exhaust server memory.
async fn stream_tar_entries(
    format: ArchiveFormat,
    body: Vec<u8>,
    limits: BulkExtractLimits,
    consumer: &mut TarEntryConsumer<'_>,
) -> SwiftResult<()> {
    if body.len() > limits.max_archive_bytes {
        return Err(SwiftError::RequestEntityTooLarge(format!(
            "Bulk extract archive exceeds maximum size of {} bytes",
            limits.max_archive_bytes
        )));
    }

    let decompressed_exceeded = Arc::new(AtomicBool::new(false));
    let reader =
        build_bounded_archive_reader(format, body, limits.max_total_decompressed_bytes, Arc::clone(&decompressed_exceeded));

    let mut archive = tokio_tar::Archive::new(reader);
    let mut entries_iter = archive
        .entries()
        .map_err(|e| SwiftError::BadRequest(format!("Failed to read tar archive: {}", e)))?;

    let mut total_entries = 0usize;
    let mut regular_files = 0usize;
    let mut total_payload_bytes = 0u64;

    while let Some(entry_result) = entries_iter.next().await {
        let mut entry = match entry_result {
            Ok(entry) => entry,
            Err(e) => {
                if decompressed_exceeded.load(Ordering::Acquire) {
                    return Err(SwiftError::RequestEntityTooLarge(format!(
                        "Decompressed archive exceeds maximum size of {} bytes",
                        limits.max_total_decompressed_bytes
                    )));
                }
                return Err(SwiftError::BadRequest(format!("Failed to read tar entry: {}", e)));
            }
        };

        total_entries += 1;
        if total_entries > limits.max_total_entries {
            return Err(SwiftError::RequestEntityTooLarge(format!(
                "Archive exceeds maximum of {} entries",
                limits.max_total_entries
            )));
        }

        let path = entry
            .path()
            .map_err(|e| SwiftError::BadRequest(format!("Invalid path in tar entry: {}", e)))?;
        let path_str = path.to_string_lossy().to_string();

        let entry_type = entry.header().entry_type();
        if entry_type.is_dir() {
            debug!(
                event = EVENT_SWIFT_BULK_EXTRACT_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_SWIFT_BULK,
                state = "skipped_directory",
                path = %path_str,
                "swift bulk extract state changed"
            );
            continue;
        }

        if !entry_type.is_file() {
            continue;
        }

        regular_files += 1;
        if regular_files > limits.max_files {
            return Err(SwiftError::RequestEntityTooLarge(format!(
                "Archive exceeds maximum of {} extracted files",
                limits.max_files
            )));
        }

        let remaining_budget = limits.max_total_decompressed_bytes.saturating_sub(total_payload_bytes);
        let entry_cap = limits.max_entry_bytes.min(remaining_budget);

        if let Ok(declared_size) = entry.header().raw_file_size()
            && declared_size > entry_cap
        {
            return Err(SwiftError::RequestEntityTooLarge(format!(
                "Archive entry '{}' size {} exceeds allowed limit of {} bytes",
                path_str, declared_size, entry_cap
            )));
        }

        let mut contents = Vec::new();
        if let Err(e) = (&mut entry)
            .take(entry_cap.saturating_add(1))
            .read_to_end(&mut contents)
            .await
        {
            if decompressed_exceeded.load(Ordering::Acquire) {
                return Err(SwiftError::RequestEntityTooLarge(format!(
                    "Decompressed archive exceeds maximum size of {} bytes",
                    limits.max_total_decompressed_bytes
                )));
            }
            return Err(SwiftError::BadRequest(format!("Failed to read tar entry '{}': {}", path_str, e)));
        }

        if decompressed_exceeded.load(Ordering::Acquire) || (contents.len() as u64) > entry_cap {
            return Err(SwiftError::RequestEntityTooLarge(format!(
                "Archive entry '{}' exceeds allowed size limit of {} bytes",
                path_str, entry_cap
            )));
        }

        total_payload_bytes = total_payload_bytes.saturating_add(contents.len() as u64);
        consumer.handle_file(path_str, contents).await?;
    }

    if decompressed_exceeded.load(Ordering::Acquire) {
        return Err(SwiftError::RequestEntityTooLarge(format!(
            "Decompressed archive exceeds maximum size of {} bytes",
            limits.max_total_decompressed_bytes
        )));
    }

    Ok(())
}

/// Extract tar entries under explicit limits (used by tests to inspect extracted entries).
#[cfg(test)]
async fn extract_tar_entries(format: ArchiveFormat, body: Vec<u8>) -> SwiftResult<Vec<(String, Vec<u8>)>> {
    extract_tar_entries_with_limits(format, body, BulkExtractLimits::default()).await
}

#[cfg(test)]
async fn extract_tar_entries_with_limits(
    format: ArchiveFormat,
    body: Vec<u8>,
    limits: BulkExtractLimits,
) -> SwiftResult<Vec<(String, Vec<u8>)>> {
    let mut entries = Vec::new();
    let mut consumer = TarEntryConsumer::Collect(&mut entries);
    stream_tar_entries(format, body, limits, &mut consumer).await?;
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_object_path() {
        // Valid paths
        assert_eq!(
            parse_object_path("/container/object.txt").unwrap(),
            ("container".to_string(), "object.txt".to_string())
        );

        assert_eq!(
            parse_object_path("container/folder/object.txt").unwrap(),
            ("container".to_string(), "folder/object.txt".to_string())
        );

        assert_eq!(
            parse_object_path("/my-container/path/to/file.txt").unwrap(),
            ("my-container".to_string(), "path/to/file.txt".to_string())
        );

        // With whitespace
        assert_eq!(
            parse_object_path("  /container/object.txt  ").unwrap(),
            ("container".to_string(), "object.txt".to_string())
        );
    }

    #[test]
    fn test_parse_object_path_invalid() {
        // Empty path
        assert!(parse_object_path("").is_err());
        assert!(parse_object_path("   ").is_err());

        // Missing object
        assert!(parse_object_path("/container").is_err());
        assert!(parse_object_path("/container/").is_err());

        // Missing container
        assert!(parse_object_path("/").is_err());
        assert!(parse_object_path("//object").is_err());
    }

    #[test]
    fn test_archive_format_from_query() {
        assert_eq!(ArchiveFormat::from_query("tar").unwrap(), ArchiveFormat::Tar);
        assert_eq!(ArchiveFormat::from_query("tar.gz").unwrap(), ArchiveFormat::TarGz);
        assert_eq!(ArchiveFormat::from_query("tgz").unwrap(), ArchiveFormat::TarGz);
        assert_eq!(ArchiveFormat::from_query("tar.bz2").unwrap(), ArchiveFormat::TarBz2);
        assert_eq!(ArchiveFormat::from_query("tbz2").unwrap(), ArchiveFormat::TarBz2);
        assert_eq!(ArchiveFormat::from_query("tbz").unwrap(), ArchiveFormat::TarBz2);

        // Invalid formats
        assert!(ArchiveFormat::from_query("zip").is_err());
        assert!(ArchiveFormat::from_query("rar").is_err());
        assert!(ArchiveFormat::from_query("").is_err());
    }

    #[test]
    fn test_bulk_delete_response_default() {
        let response = BulkDeleteResponse::default();
        assert_eq!(response.number_deleted, 0);
        assert_eq!(response.number_not_found, 0);
        assert!(response.errors.is_empty());
        assert_eq!(response.response_status, "200 OK");
        assert!(response.response_body.is_empty());
    }

    #[test]
    fn test_bulk_extract_response_default() {
        let response = BulkExtractResponse::default();
        assert_eq!(response.number_files_created, 0);
        assert!(response.errors.is_empty());
        assert_eq!(response.response_status, "201 Created");
        assert!(response.response_body.is_empty());
    }

    #[test]
    fn test_parse_multiple_paths() {
        let body = "/container1/file1.txt\n/container2/file2.txt\n/container1/folder/file3.txt";
        let paths: Vec<&str> = body.lines().collect();

        assert_eq!(paths.len(), 3);

        let (c1, o1) = parse_object_path(paths[0]).unwrap();
        assert_eq!(c1, "container1");
        assert_eq!(o1, "file1.txt");

        let (c2, o2) = parse_object_path(paths[1]).unwrap();
        assert_eq!(c2, "container2");
        assert_eq!(o2, "file2.txt");

        let (c3, o3) = parse_object_path(paths[2]).unwrap();
        assert_eq!(c3, "container1");
        assert_eq!(o3, "folder/file3.txt");
    }

    #[test]
    fn test_parse_paths_with_empty_lines() {
        let body = "/container1/file1.txt\n\n/container2/file2.txt\n   \n/container1/file3.txt";
        assert_eq!(body.lines().filter(|line| !line.trim().is_empty()).count(), 3);
    }

    /// Tests for the `extract_tar_entries` async function.
    ///
    /// Conditionally compiled with the `swift` feature, which gates the
    /// `tokio_tar` (rustfs-tokio-tar) and `async_compression` dependencies.
    #[cfg(feature = "swift")]
    mod tar_extraction {
        use super::*;
        use tokio::io::AsyncWriteExt;

        /// Builds an uncompressed tar archive in memory.
        ///
        /// * `files` – `(path, content)` pairs added as regular files.
        /// * `dirs`  – paths added as directory entries.
        async fn make_tar(files: &[(&str, &[u8])], dirs: &[&str]) -> Vec<u8> {
            let buf = std::io::Cursor::new(Vec::new());
            let mut builder = tokio_tar::Builder::new(buf);

            for &dir in dirs {
                let mut header = tokio_tar::Header::new_gnu();
                header.set_entry_type(tokio_tar::EntryType::Directory);
                header.set_size(0);
                header.set_mode(0o755);
                header.set_cksum();
                builder
                    .append_data(&mut header, dir, std::io::Cursor::new(&[] as &[u8]))
                    .await
                    .unwrap();
            }

            for &(name, data) in files {
                let mut header = tokio_tar::Header::new_gnu();
                header.set_size(data.len() as u64);
                header.set_mode(0o644);
                header.set_cksum();
                builder
                    .append_data(&mut header, name, std::io::Cursor::new(data))
                    .await
                    .unwrap();
            }

            builder.into_inner().await.unwrap().into_inner()
        }

        #[tokio::test]
        async fn test_extract_plain_tar_paths_and_contents() {
            let tar_bytes = make_tar(&[("file1.txt", b"hello"), ("dir/file2.txt", b"world")], &[]).await;
            let entries = extract_tar_entries(ArchiveFormat::Tar, tar_bytes).await.unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].0, "file1.txt");
            assert_eq!(entries[0].1, b"hello");
            assert_eq!(entries[1].0, "dir/file2.txt");
            assert_eq!(entries[1].1, b"world");
        }

        #[tokio::test]
        async fn test_extract_tar_skips_directories() {
            let tar_bytes = make_tar(&[("file.txt", b"content")], &["subdir/", "another/"]).await;
            let entries = extract_tar_entries(ArchiveFormat::Tar, tar_bytes).await.unwrap();
            // Directory entries must be filtered out; only the regular file is returned.
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, "file.txt");
        }

        #[tokio::test]
        async fn test_extract_tar_gz() {
            let tar_bytes = make_tar(&[("file.txt", b"compressed content")], &[]).await;

            // Compress with gzip.
            let cursor = std::io::Cursor::new(Vec::new());
            let mut encoder = async_compression::tokio::write::GzipEncoder::new(cursor);
            encoder.write_all(&tar_bytes).await.unwrap();
            encoder.shutdown().await.unwrap();
            let gz_bytes = encoder.into_inner().into_inner();

            let entries = extract_tar_entries(ArchiveFormat::TarGz, gz_bytes).await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, "file.txt");
            assert_eq!(entries[0].1, b"compressed content");
        }

        #[tokio::test]
        async fn test_extract_tar_bz2() {
            let tar_bytes = make_tar(&[("file.txt", b"bzip2 content")], &[]).await;

            // Compress with bzip2.
            let cursor = std::io::Cursor::new(Vec::new());
            let mut encoder = async_compression::tokio::write::BzEncoder::new(cursor);
            encoder.write_all(&tar_bytes).await.unwrap();
            encoder.shutdown().await.unwrap();
            let bz2_bytes = encoder.into_inner().into_inner();

            let entries = extract_tar_entries(ArchiveFormat::TarBz2, bz2_bytes).await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, "file.txt");
            assert_eq!(entries[0].1, b"bzip2 content");
        }

        #[tokio::test]
        async fn test_extract_invalid_tar_returns_error() {
            // Fewer than 512 bytes → parser cannot read a complete tar header block.
            let bad_bytes = b"this is not a valid tar archive".to_vec();
            let result = extract_tar_entries(ArchiveFormat::Tar, bad_bytes).await;
            assert!(result.is_err(), "expected error for invalid tar data");
        }

        #[tokio::test]
        async fn test_extract_empty_tar() {
            let tar_bytes = make_tar(&[], &[]).await;
            let entries = extract_tar_entries(ArchiveFormat::Tar, tar_bytes).await.unwrap();
            assert!(entries.is_empty());
        }

        #[tokio::test]
        async fn test_extract_tar_gz_compression_bomb_rejected_at_limit() {
            // 2 MiB of zeroes inside a tar compresses to a few KiB with gzip (>500:1 expansion).
            let payload = vec![0u8; 2 * 1024 * 1024];
            let tar_bytes = make_tar(&[("bomb.bin", &payload)], &[]).await;

            let cursor = std::io::Cursor::new(Vec::new());
            let mut encoder = async_compression::tokio::write::GzipEncoder::new(cursor);
            encoder.write_all(&tar_bytes).await.unwrap();
            encoder.shutdown().await.unwrap();
            let gz_bytes = encoder.into_inner().into_inner();
            assert!(gz_bytes.len() < 16 * 1024, "fixture should be tiny compressed");

            let limits = BulkExtractLimits {
                max_archive_bytes: 1024 * 1024,
                max_total_decompressed_bytes: 64 * 1024,
                max_entry_bytes: 64 * 1024,
                max_files: 10,
                max_total_entries: 20,
                max_errors: 10,
            };
            let err = extract_tar_entries_with_limits(ArchiveFormat::TarGz, gz_bytes, limits)
                .await
                .expect_err("gzip bomb must be rejected");
            assert!(
                matches!(err, SwiftError::RequestEntityTooLarge(_)),
                "expected RequestEntityTooLarge, got {err:?}"
            );
        }

        #[tokio::test]
        async fn test_extract_tar_bz2_compression_bomb_rejected_at_limit() {
            let payload = vec![0u8; 2 * 1024 * 1024];
            let tar_bytes = make_tar(&[("bomb.bin", &payload)], &[]).await;

            let cursor = std::io::Cursor::new(Vec::new());
            let mut encoder = async_compression::tokio::write::BzEncoder::new(cursor);
            encoder.write_all(&tar_bytes).await.unwrap();
            encoder.shutdown().await.unwrap();
            let bz2_bytes = encoder.into_inner().into_inner();
            assert!(bz2_bytes.len() < 16 * 1024, "fixture should be tiny compressed");

            let limits = BulkExtractLimits {
                max_archive_bytes: 1024 * 1024,
                max_total_decompressed_bytes: 64 * 1024,
                max_entry_bytes: 64 * 1024,
                max_files: 10,
                max_total_entries: 20,
                max_errors: 10,
            };
            let err = extract_tar_entries_with_limits(ArchiveFormat::TarBz2, bz2_bytes, limits)
                .await
                .expect_err("bzip2 bomb must be rejected");
            assert!(
                matches!(err, SwiftError::RequestEntityTooLarge(_)),
                "expected RequestEntityTooLarge, got {err:?}"
            );
        }

        #[tokio::test]
        async fn test_extract_tar_enforces_archive_entry_file_and_dir_limits() {
            let tar_bytes = make_tar(&[("a.bin", &[b'a'; 2048])], &[]).await;

            // Compressed/raw archive size cap.
            let err = extract_tar_entries_with_limits(
                ArchiveFormat::Tar,
                tar_bytes.clone(),
                BulkExtractLimits {
                    max_archive_bytes: 512,
                    ..BulkExtractLimits::default()
                },
            )
            .await
            .expect_err("oversized raw archive must be rejected");
            assert!(matches!(err, SwiftError::RequestEntityTooLarge(_)));

            // Single entry cap.
            let err = extract_tar_entries_with_limits(
                ArchiveFormat::Tar,
                tar_bytes,
                BulkExtractLimits {
                    max_entry_bytes: 1024,
                    max_total_decompressed_bytes: 64 * 1024,
                    ..BulkExtractLimits::default()
                },
            )
            .await
            .expect_err("oversized entry must be rejected");
            assert!(matches!(err, SwiftError::RequestEntityTooLarge(_)));

            // Regular file count cap.
            let many_files = make_tar(&[("f1", b"1"), ("f2", b"2"), ("f3", b"3"), ("f4", b"4")], &[]).await;
            let err = extract_tar_entries_with_limits(
                ArchiveFormat::Tar,
                many_files,
                BulkExtractLimits {
                    max_files: 3,
                    ..BulkExtractLimits::default()
                },
            )
            .await
            .expect_err("too many regular files must be rejected");
            assert!(matches!(err, SwiftError::RequestEntityTooLarge(_)));

            // Directory entry flood cap (non-file entries still count toward max_total_entries).
            let dir_flood = make_tar(&[], &["d1/", "d2/", "d3/", "d4/", "d5/", "d6/"]).await;
            let err = extract_tar_entries_with_limits(
                ArchiveFormat::Tar,
                dir_flood,
                BulkExtractLimits {
                    max_files: 100,
                    max_total_entries: 5,
                    ..BulkExtractLimits::default()
                },
            )
            .await
            .expect_err("directory flood must be rejected");
            assert!(matches!(err, SwiftError::RequestEntityTooLarge(_)));
        }

        #[tokio::test]
        async fn test_bulk_delete_enforces_body_path_and_error_limits() {
            use http_body_util::BodyExt;

            let mut claims = std::collections::HashMap::new();
            claims.insert("keystone_project_id".to_string(), serde_json::json!("proj1"));
            let creds = Credentials {
                access_key: "keystone:test".to_string(),
                claims: Some(claims),
                ..Default::default()
            };

            // Oversized body fails closed with 413.
            let err = handle_bulk_delete_with_limits(
                "AUTH_proj1",
                "/c/a\n/c/b\n/c/c\n".to_string(),
                &creds,
                BulkDeleteLimits {
                    max_body_bytes: 8,
                    max_paths: 10,
                    max_errors: 10,
                },
            )
            .await
            .expect_err("oversized bulk-delete body must fail closed");
            assert!(matches!(err, SwiftError::RequestEntityTooLarge(_)));

            // Too many paths fails closed with 413.
            let err = handle_bulk_delete_with_limits(
                "AUTH_proj1",
                "/c/1\n/c/2\n/c/3\n/c/4\n".to_string(),
                &creds,
                BulkDeleteLimits {
                    max_body_bytes: 1024,
                    max_paths: 3,
                    max_errors: 10,
                },
            )
            .await
            .expect_err("too many bulk-delete paths must fail closed");
            assert!(matches!(err, SwiftError::RequestEntityTooLarge(_)));

            // Error flood caps retained error entries at max_errors.
            let invalid_paths = "bad1\nbad2\nbad3\nbad4\nbad5\nbad6\n".to_string();
            let resp = handle_bulk_delete_with_limits(
                "AUTH_proj1",
                invalid_paths,
                &creds,
                BulkDeleteLimits {
                    max_body_bytes: 1024,
                    max_paths: 100,
                    max_errors: 3,
                },
            )
            .await
            .expect("capped bulk-delete error response should succeed");
            let body_bytes = resp.into_body().collect().await.expect("response body").to_bytes();
            let parsed: BulkDeleteResponse = serde_json::from_slice(&body_bytes).expect("valid JSON");
            assert_eq!(parsed.errors.len(), 3, "errors must be capped at max_errors");
            assert_eq!(parsed.response_status, "400 Bad Request");
            assert!(parsed.response_body.contains("Maximum number of errors"));
        }
    }
}
