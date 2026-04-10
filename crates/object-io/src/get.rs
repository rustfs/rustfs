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

use bytes::Bytes;
use futures_util::StreamExt;
use http::HeaderMap;
use http::header::{CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_LANGUAGE};
use rustfs_ecstore::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE;
use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::store_api::{GetObjectChunkCopyMode, GetObjectChunkPath, GetObjectChunkResult, HTTPRangeSpec, ObjectInfo};
use rustfs_io_core::BoxChunkStream;
use rustfs_rio::Reader;
use rustfs_s3select_api::object_store::bytes_stream;
use rustfs_utils::http::{AMZ_CHECKSUM_MODE, AMZ_CHECKSUM_TYPE};
use s3s::dto::{
    ChecksumType, ContentType, GetObjectOutput, SSECustomerAlgorithm, SSECustomerKeyMD5, SSEKMSKeyId, ServerSideEncryption,
    StreamingBlob, Timestamp,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
#[cfg(test)]
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use tokio_util::io::ReaderStream;

pub struct InMemoryAsyncReader {
    cursor: std::io::Cursor<Bytes>,
}

impl InMemoryAsyncReader {
    pub fn new(data: Bytes) -> Self {
        Self {
            cursor: std::io::Cursor::new(data),
        }
    }
}

impl AsyncRead for InMemoryAsyncReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let unfilled = buf.initialize_unfilled();
        let bytes_read = std::io::Read::read(&mut self.cursor, unfilled)?;
        buf.advance(bytes_read);
        std::task::Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for InMemoryAsyncReader {
    fn start_seek(mut self: std::pin::Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        std::io::Seek::seek(&mut self.cursor, position)?;
        Ok(())
    }

    fn poll_complete(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<u64>> {
        std::task::Poll::Ready(Ok(self.cursor.position()))
    }
}

pub fn build_memory_blob(buf: Bytes, response_content_length: i64, optimal_buffer_size: usize) -> Option<StreamingBlob> {
    let mem_reader = InMemoryAsyncReader::new(buf);
    Some(StreamingBlob::wrap(bytes_stream(
        ReaderStream::with_capacity(Box::new(mem_reader), optimal_buffer_size),
        response_content_length as usize,
    )))
}

#[derive(Clone)]
pub struct FrozenGetObjectBody {
    body: Arc<Bytes>,
}

impl FrozenGetObjectBody {
    pub fn new(body: Bytes) -> Self {
        Self { body: Arc::new(body) }
    }

    pub fn shared_body(&self) -> &Arc<Bytes> {
        &self.body
    }

    pub fn into_shared_body(self) -> Arc<Bytes> {
        self.body
    }

    pub fn build_blob(&self, response_content_length: i64, optimal_buffer_size: usize) -> Option<StreamingBlob> {
        build_memory_blob((*self.body).clone(), response_content_length, optimal_buffer_size)
    }
}

pub fn build_reader_blob<R>(reader: R, response_content_length: i64, optimal_buffer_size: usize) -> Option<StreamingBlob>
where
    R: AsyncRead + Send + Sync + 'static,
{
    Some(StreamingBlob::wrap(bytes_stream(
        ReaderStream::with_capacity(reader, optimal_buffer_size),
        response_content_length as usize,
    )))
}

pub fn build_chunk_blob(chunk_stream: BoxChunkStream) -> Option<StreamingBlob> {
    Some(StreamingBlob::wrap(chunk_stream.map(|result| result.map(|chunk| chunk.as_bytes()))))
}

/// ADR-facing alias for the chunk-stream to HTTP body bridge.
pub fn build_chunk_http_body(chunk_stream: BoxChunkStream) -> Option<StreamingBlob> {
    build_chunk_blob(chunk_stream)
}

pub fn map_chunk_copy_mode(copy_mode: GetObjectChunkCopyMode) -> rustfs_io_metrics::CopyMode {
    match copy_mode {
        GetObjectChunkCopyMode::TrueZeroCopy => rustfs_io_metrics::CopyMode::TrueZeroCopy,
        GetObjectChunkCopyMode::SharedBytes => rustfs_io_metrics::CopyMode::SharedBytes,
        GetObjectChunkCopyMode::SingleCopy => rustfs_io_metrics::CopyMode::SingleCopy,
        GetObjectChunkCopyMode::Reconstructed => rustfs_io_metrics::CopyMode::Reconstructed,
    }
}

pub fn chunk_body_data_plane_labels(
    path: GetObjectChunkPath,
    copy_mode: rustfs_io_metrics::CopyMode,
) -> (rustfs_io_metrics::IoPath, rustfs_io_metrics::CopyMode) {
    (
        match path {
            GetObjectChunkPath::Direct | GetObjectChunkPath::Bridge => rustfs_io_metrics::IoPath::Fast,
        },
        copy_mode,
    )
}

#[must_use]
pub const fn get_object_chunk_path_label(path: GetObjectChunkPath) -> &'static str {
    match path {
        GetObjectChunkPath::Direct => "direct",
        GetObjectChunkPath::Bridge => "bridge",
    }
}

pub fn get_object_chunk_fast_path_guard(
    has_sse_customer_key: bool,
    has_sse_customer_key_md5: bool,
) -> Result<(), ChunkReadFallback> {
    if has_sse_customer_key || has_sse_customer_key_md5 {
        return Err(ChunkReadFallback::read_setup(rustfs_io_metrics::FallbackReason::EncryptionEnabled));
    }

    Ok(())
}

pub fn get_object_sequential_hint(rs: Option<&HTTPRangeSpec>) -> bool {
    if rs.is_none() {
        true
    } else if let Some(range_spec) = rs {
        range_spec.start == 0 && !range_spec.is_suffix_length
    } else {
        false
    }
}

pub struct GetObjectOutputContext {
    pub output: GetObjectOutput,
    pub event_info: ObjectInfo,
    pub response_content_length: i64,
    pub optimal_buffer_size: usize,
    pub copy_mode_override: Option<rustfs_io_metrics::CopyMode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetObjectStrategyLayout {
    pub is_sequential_hint: bool,
    pub optimal_buffer_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetObjectBodyPlanningInputs {
    pub is_part_request: bool,
    pub is_range_request: bool,
    pub encryption_applied: bool,
    pub response_size: i64,
}

pub enum GetObjectBodySource {
    Reader(Box<dyn Reader>),
    Chunk {
        stream: BoxChunkStream,
        path: GetObjectChunkPath,
        copy_mode: rustfs_io_metrics::CopyMode,
    },
}

pub struct GetObjectReadSetup {
    pub info: ObjectInfo,
    pub event_info: ObjectInfo,
    pub body_source: GetObjectBodySource,
    pub rs: Option<HTTPRangeSpec>,
    pub content_type: Option<ContentType>,
    pub last_modified: Option<Timestamp>,
    pub response_content_length: i64,
    pub content_range: Option<String>,
    pub server_side_encryption: Option<ServerSideEncryption>,
    pub sse_customer_algorithm: Option<SSECustomerAlgorithm>,
    pub sse_customer_key_md5: Option<SSECustomerKeyMD5>,
    pub ssekms_key_id: Option<SSEKMSKeyId>,
    pub encryption_applied: bool,
}

pub struct LegacyReadPlan {
    pub rs: Option<HTTPRangeSpec>,
    pub content_type: Option<ContentType>,
    pub last_modified: Option<Timestamp>,
    pub response_content_length: i64,
    pub content_range: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetObjectBodyPlan {
    BufferEncrypted,
    BufferSeekable,
    Stream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetObjectDataPlaneMetricContract {
    pub io_path: rustfs_io_metrics::IoPath,
    pub copy_mode: rustfs_io_metrics::CopyMode,
}

impl GetObjectDataPlaneMetricContract {
    pub fn disk(io_path: rustfs_io_metrics::IoPath, copy_mode: rustfs_io_metrics::CopyMode) -> Self {
        Self { io_path, copy_mode }
    }
}

pub struct GetObjectBodyMaterialization {
    pub body: Option<StreamingBlob>,
    pub plan: GetObjectBodyPlan,
}

#[derive(Default)]
pub struct GetObjectEncryptionState {
    pub server_side_encryption: Option<ServerSideEncryption>,
    pub sse_customer_algorithm: Option<SSECustomerAlgorithm>,
    pub sse_customer_key_md5: Option<SSECustomerKeyMD5>,
    pub ssekms_key_id: Option<SSEKMSKeyId>,
    pub encryption_applied: bool,
    pub response_content_length_override: Option<i64>,
}

pub struct ChunkReadSetupResult {
    pub read_setup: GetObjectReadSetup,
    pub io_path: rustfs_io_metrics::IoPath,
}

#[derive(Debug, thiserror::Error)]
pub enum MaterializeGetObjectBodyError {
    #[error("failed to read decrypted object: {0}")]
    EncryptedRead(std::io::Error),
}

pub struct GetObjectFlowResult {
    pub output: GetObjectOutput,
    pub event_info: ObjectInfo,
    pub version_id_for_event: String,
}

pub fn build_get_object_flow_result(
    output: GetObjectOutput,
    event_info: ObjectInfo,
    version_id_for_event: String,
) -> GetObjectFlowResult {
    GetObjectFlowResult {
        output,
        event_info,
        version_id_for_event,
    }
}

pub fn build_cors_wrapped_get_object_flow_result(
    output_context: GetObjectOutputContext,
    version_id_for_event: String,
) -> GetObjectFlowResult {
    let GetObjectOutputContext {
        output,
        event_info,
        response_content_length: _,
        optimal_buffer_size: _,
        copy_mode_override: _,
    } = output_context;
    build_get_object_flow_result(output, event_info, version_id_for_event)
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GetObjectChecksums {
    pub crc32: Option<String>,
    pub crc32c: Option<String>,
    pub sha1: Option<String>,
    pub sha256: Option<String>,
    pub crc64nvme: Option<String>,
    pub checksum_type: Option<ChecksumType>,
}

fn decode_get_object_checksums(decrypted_checksums: HashMap<String, String>) -> GetObjectChecksums {
    let mut checksums = GetObjectChecksums::default();

    for (key, checksum) in decrypted_checksums {
        if key == AMZ_CHECKSUM_TYPE {
            checksums.checksum_type = Some(ChecksumType::from(checksum));
            continue;
        }

        match rustfs_rio::ChecksumType::from_string(key.as_str()) {
            rustfs_rio::ChecksumType::CRC32 => checksums.crc32 = Some(checksum),
            rustfs_rio::ChecksumType::CRC32C => checksums.crc32c = Some(checksum),
            rustfs_rio::ChecksumType::SHA1 => checksums.sha1 = Some(checksum),
            rustfs_rio::ChecksumType::SHA256 => checksums.sha256 = Some(checksum),
            rustfs_rio::ChecksumType::CRC64_NVME => checksums.crc64nvme = Some(checksum),
            _ => (),
        }
    }

    checksums
}

fn read_object_checksums(
    info: &ObjectInfo,
    headers: &HeaderMap,
    part_number: Option<usize>,
) -> std::io::Result<GetObjectChecksums> {
    let (decrypted_checksums, _is_multipart) = info
        .decrypt_checksums(part_number.unwrap_or(0), headers)
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    Ok(decode_get_object_checksums(decrypted_checksums))
}

pub fn build_get_object_checksums(
    info: &ObjectInfo,
    headers: &HeaderMap,
    part_number: Option<usize>,
    rs: Option<&HTTPRangeSpec>,
) -> std::io::Result<GetObjectChecksums> {
    if let Some(checksum_mode) = headers.get(AMZ_CHECKSUM_MODE)
        && checksum_mode.to_str().unwrap_or_default() == "ENABLED"
        && rs.is_none()
    {
        return read_object_checksums(info, headers, part_number);
    }

    Ok(GetObjectChecksums::default())
}

pub fn build_output_version_id(versioned: bool, version_id: Option<&uuid::Uuid>) -> Option<String> {
    if !versioned {
        return None;
    }

    version_id.map(|vid| {
        if *vid == uuid::Uuid::nil() {
            "null".to_string()
        } else {
            vid.to_string()
        }
    })
}

pub fn plan_get_object_strategy_layout(
    rs: Option<&HTTPRangeSpec>,
    response_content_length: i64,
    suggested_buffer_size: usize,
    fallback_buffer_size: usize,
) -> GetObjectStrategyLayout {
    let is_sequential_hint = get_object_sequential_hint(rs);
    let optimal_buffer_size = if suggested_buffer_size > 0 {
        suggested_buffer_size.min(fallback_buffer_size)
    } else {
        rustfs_io_core::get_concurrency_aware_buffer_size(response_content_length, fallback_buffer_size)
    };

    GetObjectStrategyLayout {
        is_sequential_hint,
        optimal_buffer_size,
    }
}

pub fn plan_get_object_body(
    planning_inputs: GetObjectBodyPlanningInputs,
    seekable_object_size_threshold: usize,
) -> GetObjectBodyPlan {
    let should_buffer_for_seek = planning_inputs.response_size > 0
        && planning_inputs.response_size <= seekable_object_size_threshold as i64
        && !planning_inputs.is_part_request
        && !planning_inputs.is_range_request;

    if planning_inputs.encryption_applied && should_buffer_for_seek {
        GetObjectBodyPlan::BufferEncrypted
    } else if should_buffer_for_seek {
        GetObjectBodyPlan::BufferSeekable
    } else {
        GetObjectBodyPlan::Stream
    }
}

pub async fn materialize_get_object_body<R>(
    mut final_stream: R,
    plan: GetObjectBodyPlan,
    response_content_length: i64,
    optimal_buffer_size: usize,
) -> Result<GetObjectBodyMaterialization, MaterializeGetObjectBodyError>
where
    R: AsyncRead + Send + Sync + Unpin + 'static,
{
    match plan {
        GetObjectBodyPlan::BufferEncrypted => {
            let mut buf = Vec::with_capacity(response_content_length as usize);
            tokio::io::AsyncReadExt::read_to_end(&mut final_stream, &mut buf)
                .await
                .map_err(MaterializeGetObjectBodyError::EncryptedRead)?;
            let body = FrozenGetObjectBody::new(Bytes::from(buf));

            Ok(GetObjectBodyMaterialization {
                body: body.build_blob(response_content_length, optimal_buffer_size),
                plan,
            })
        }
        GetObjectBodyPlan::BufferSeekable => {
            let mut buf = Vec::with_capacity(response_content_length as usize);
            let body = match tokio::io::AsyncReadExt::read_to_end(&mut final_stream, &mut buf).await {
                Ok(_) => FrozenGetObjectBody::new(Bytes::from(buf)).build_blob(response_content_length, optimal_buffer_size),
                Err(_) => build_reader_blob(final_stream, response_content_length, optimal_buffer_size),
            };

            Ok(GetObjectBodyMaterialization { body, plan })
        }
        GetObjectBodyPlan::Stream => Ok(GetObjectBodyMaterialization {
            body: build_reader_blob(final_stream, response_content_length, optimal_buffer_size),
            plan,
        }),
    }
}

fn resolve_requested_range(
    info: &ObjectInfo,
    mut rs: Option<HTTPRangeSpec>,
    part_number: Option<usize>,
) -> Option<HTTPRangeSpec> {
    if let Some(part_number) = part_number
        && rs.is_none()
    {
        rs = HTTPRangeSpec::from_object_info(info, part_number);
    }

    rs
}

fn resolve_response_range(
    total_size: i64,
    rs: Option<HTTPRangeSpec>,
) -> std::io::Result<(Option<HTTPRangeSpec>, i64, Option<String>)> {
    let Some(range_spec) = rs else {
        return Ok((None, total_size, None));
    };

    let (start, length) = range_spec.get_offset_length(total_size)?;
    let content_range = Some(format!("bytes {}-{}/{}", start, start as i64 + length - 1, total_size));

    Ok((Some(range_spec), length, content_range))
}

pub fn plan_legacy_read(
    info: &ObjectInfo,
    rs: Option<HTTPRangeSpec>,
    part_number: Option<usize>,
) -> std::io::Result<LegacyReadPlan> {
    let content_type = info
        .content_type
        .as_ref()
        .and_then(|content_type| ContentType::from_str(content_type).ok());
    let last_modified = info.mod_time.map(Timestamp::from);
    let rs = resolve_requested_range(info, rs, part_number);
    let total_size = info.get_actual_size()?;
    let (rs, response_content_length, content_range) = resolve_response_range(total_size, rs)?;

    Ok(LegacyReadPlan {
        rs,
        content_type,
        last_modified,
        response_content_length,
        content_range,
    })
}

pub fn build_reader_read_setup(
    info: ObjectInfo,
    event_info: ObjectInfo,
    final_stream: Box<dyn Reader>,
    plan: LegacyReadPlan,
    encryption_state: GetObjectEncryptionState,
) -> GetObjectReadSetup {
    let LegacyReadPlan {
        rs,
        content_type,
        last_modified,
        response_content_length,
        content_range,
    } = plan;

    let GetObjectEncryptionState {
        server_side_encryption,
        sse_customer_algorithm,
        sse_customer_key_md5,
        ssekms_key_id,
        encryption_applied,
        response_content_length_override,
    } = encryption_state;

    GetObjectReadSetup {
        info,
        event_info,
        body_source: GetObjectBodySource::Reader(final_stream),
        rs,
        content_type,
        last_modified,
        response_content_length: response_content_length_override.unwrap_or(response_content_length),
        content_range,
        server_side_encryption,
        sse_customer_algorithm,
        sse_customer_key_md5,
        ssekms_key_id,
        encryption_applied,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_get_object_output_context(
    body: Option<StreamingBlob>,
    info: ObjectInfo,
    event_info: ObjectInfo,
    content_type: Option<ContentType>,
    last_modified: Option<Timestamp>,
    response_content_length: i64,
    content_range: Option<String>,
    server_side_encryption: Option<ServerSideEncryption>,
    sse_customer_algorithm: Option<SSECustomerAlgorithm>,
    sse_customer_key_md5: Option<SSECustomerKeyMD5>,
    ssekms_key_id: Option<SSEKMSKeyId>,
    checksums: &GetObjectChecksums,
    filtered_metadata: Option<HashMap<String, String>>,
    versioned: bool,
    optimal_buffer_size: usize,
    copy_mode_override: Option<rustfs_io_metrics::CopyMode>,
) -> GetObjectOutputContext {
    let output_version_id = build_output_version_id(versioned, info.version_id.as_ref());
    let output = build_get_object_output(
        body,
        &info,
        content_type,
        last_modified,
        response_content_length,
        content_range,
        server_side_encryption,
        sse_customer_algorithm,
        sse_customer_key_md5,
        ssekms_key_id,
        checksums,
        output_version_id,
        filtered_metadata,
    );

    GetObjectOutputContext {
        output,
        event_info,
        response_content_length,
        optimal_buffer_size,
        copy_mode_override,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_chunk_read_setup(
    info: ObjectInfo,
    event_info: ObjectInfo,
    path: GetObjectChunkPath,
    copy_mode: rustfs_io_metrics::CopyMode,
    stream: BoxChunkStream,
    plan: ChunkReadPlan,
) -> GetObjectReadSetup {
    let ChunkReadPlan {
        rs,
        content_type,
        last_modified,
        response_content_length,
        content_range,
    } = plan;

    GetObjectReadSetup {
        info,
        event_info,
        body_source: GetObjectBodySource::Chunk { stream, path, copy_mode },
        rs,
        content_type,
        last_modified,
        response_content_length,
        content_range,
        server_side_encryption: None,
        sse_customer_algorithm: None,
        sse_customer_key_md5: None,
        ssekms_key_id: None,
        encryption_applied: false,
    }
}

pub fn finalize_chunk_read_setup(
    info: ObjectInfo,
    event_info: ObjectInfo,
    chunk_result: GetObjectChunkResult,
    plan: ChunkReadPlan,
) -> ChunkReadSetupResult {
    let copy_mode = map_chunk_copy_mode(chunk_result.copy_mode);
    let (io_path, _) = chunk_body_data_plane_labels(chunk_result.path, copy_mode);

    ChunkReadSetupResult {
        io_path,
        read_setup: build_chunk_read_setup(info, event_info, chunk_result.path, copy_mode, chunk_result.stream, plan),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_get_object_output(
    body: Option<StreamingBlob>,
    info: &ObjectInfo,
    content_type: Option<ContentType>,
    last_modified: Option<Timestamp>,
    response_content_length: i64,
    content_range: Option<String>,
    server_side_encryption: Option<ServerSideEncryption>,
    sse_customer_algorithm: Option<SSECustomerAlgorithm>,
    sse_customer_key_md5: Option<SSECustomerKeyMD5>,
    ssekms_key_id: Option<SSEKMSKeyId>,
    checksums: &GetObjectChecksums,
    output_version_id: Option<String>,
    filtered_metadata: Option<HashMap<String, String>>,
) -> GetObjectOutput {
    GetObjectOutput {
        body,
        content_length: Some(response_content_length),
        last_modified,
        expires: info.expires.map(Timestamp::from),
        content_type,
        cache_control: info.user_defined.get(CACHE_CONTROL.as_str()).cloned(),
        content_disposition: info.user_defined.get(CONTENT_DISPOSITION.as_str()).cloned(),
        content_encoding: info.content_encoding.clone(),
        content_language: info.user_defined.get(CONTENT_LANGUAGE.as_str()).cloned(),
        accept_ranges: Some("bytes".to_string()),
        content_range,
        e_tag: info.etag.as_ref().map(|etag| to_s3s_etag(etag)),
        metadata: filtered_metadata,
        server_side_encryption,
        sse_customer_algorithm,
        sse_customer_key_md5,
        ssekms_key_id,
        checksum_crc32: checksums.crc32.clone(),
        checksum_crc32c: checksums.crc32c.clone(),
        checksum_sha1: checksums.sha1.clone(),
        checksum_sha256: checksums.sha256.clone(),
        checksum_crc64nvme: checksums.crc64nvme.clone(),
        checksum_type: checksums.checksum_type.clone(),
        version_id: output_version_id,
        ..Default::default()
    }
}

#[derive(Debug)]
pub struct ChunkReadPlan {
    pub rs: Option<HTTPRangeSpec>,
    pub content_type: Option<ContentType>,
    pub last_modified: Option<Timestamp>,
    pub response_content_length: i64,
    pub content_range: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkReadFallback {
    pub stage: rustfs_io_metrics::IoStage,
    pub reason: rustfs_io_metrics::FallbackReason,
}

impl ChunkReadFallback {
    pub const fn new(stage: rustfs_io_metrics::IoStage, reason: rustfs_io_metrics::FallbackReason) -> Self {
        Self { stage, reason }
    }

    pub const fn read_setup(reason: rustfs_io_metrics::FallbackReason) -> Self {
        Self::new(rustfs_io_metrics::IoStage::ReadSetup, reason)
    }

    pub const fn range_guard(reason: rustfs_io_metrics::FallbackReason) -> Self {
        Self::new(rustfs_io_metrics::IoStage::RangeGuard, reason)
    }
}

#[derive(Debug)]
pub enum ChunkReadDecision {
    Eligible(ChunkReadPlan),
    Fallback(ChunkReadFallback),
}

#[derive(Debug)]
pub enum ChunkReadPlanError {
    NoSuchKey,
    MethodNotAllowed,
    Io(std::io::Error),
}

impl From<std::io::Error> for ChunkReadPlanError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<StorageError> for ChunkReadPlanError {
    fn from(value: StorageError) -> Self {
        Self::Io(std::io::Error::other(value.to_string()))
    }
}

pub fn get_object_chunk_range_guard(rs: Option<&HTTPRangeSpec>) -> Result<(), ChunkReadFallback> {
    let Some(range_spec) = rs else {
        return Ok(());
    };

    let unsupported = if range_spec.is_suffix_length {
        range_spec.end != -1
    } else {
        range_spec.start < 0 || range_spec.end < -1 || (range_spec.end != -1 && range_spec.end < range_spec.start)
    };

    if unsupported {
        return Err(ChunkReadFallback::range_guard(rustfs_io_metrics::FallbackReason::RangeNotSupported));
    }

    Ok(())
}

pub fn plan_chunk_read(
    info: &ObjectInfo,
    version_id_missing: bool,
    rs: Option<HTTPRangeSpec>,
    part_number: Option<usize>,
) -> Result<ChunkReadDecision, ChunkReadPlanError> {
    if info.delete_marker {
        if version_id_missing {
            return Err(ChunkReadPlanError::NoSuchKey);
        }
        return Err(ChunkReadPlanError::MethodNotAllowed);
    }

    let (_, is_compressed) = info.is_compressed_ok()?;
    if is_compressed {
        return Ok(ChunkReadDecision::Fallback(ChunkReadFallback::read_setup(
            rustfs_io_metrics::FallbackReason::CompressionEnabled,
        )));
    }

    if info.transitioned_object.status == TRANSITION_COMPLETE {
        return Ok(ChunkReadDecision::Fallback(ChunkReadFallback::read_setup(
            rustfs_io_metrics::FallbackReason::NonLocalBackend,
        )));
    }

    let has_encryption_metadata = info.user_defined.contains_key("x-rustfs-encryption-key")
        || info.user_defined.contains_key("x-amz-server-side-encryption")
        || info
            .user_defined
            .contains_key("x-amz-server-side-encryption-customer-algorithm");
    if has_encryption_metadata {
        return Ok(ChunkReadDecision::Fallback(ChunkReadFallback::read_setup(
            rustfs_io_metrics::FallbackReason::EncryptionEnabled,
        )));
    }

    let rs = resolve_requested_range(info, rs, part_number);
    if let Err(fallback) = get_object_chunk_range_guard(rs.as_ref()) {
        return Ok(ChunkReadDecision::Fallback(fallback));
    }

    let content_type = info
        .content_type
        .as_ref()
        .and_then(|content_type| ContentType::from_str(content_type).ok());
    let last_modified = info.mod_time.map(Timestamp::from);
    let total_size = info.get_actual_size()?;
    let (rs, response_content_length, content_range) = resolve_response_range(total_size, rs)?;

    Ok(ChunkReadDecision::Eligible(ChunkReadPlan {
        rs,
        content_type,
        last_modified,
        response_content_length,
        content_range,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_chunk_copy_mode_uses_expected_metric_modes() {
        assert_eq!(
            map_chunk_copy_mode(GetObjectChunkCopyMode::TrueZeroCopy),
            rustfs_io_metrics::CopyMode::TrueZeroCopy
        );
        assert_eq!(
            map_chunk_copy_mode(GetObjectChunkCopyMode::SharedBytes),
            rustfs_io_metrics::CopyMode::SharedBytes
        );
        assert_eq!(
            map_chunk_copy_mode(GetObjectChunkCopyMode::SingleCopy),
            rustfs_io_metrics::CopyMode::SingleCopy
        );
        assert_eq!(
            map_chunk_copy_mode(GetObjectChunkCopyMode::Reconstructed),
            rustfs_io_metrics::CopyMode::Reconstructed
        );
    }

    #[test]
    fn chunk_body_labels_keep_fast_path_and_copy_mode() {
        let (path, copy_mode) = chunk_body_data_plane_labels(GetObjectChunkPath::Bridge, rustfs_io_metrics::CopyMode::SingleCopy);
        assert_eq!(path, rustfs_io_metrics::IoPath::Fast);
        assert_eq!(copy_mode, rustfs_io_metrics::CopyMode::SingleCopy);
    }

    #[test]
    fn get_object_chunk_fast_path_guard_rejects_ssec_requests() {
        let err = get_object_chunk_fast_path_guard(true, false).unwrap_err();
        assert_eq!(
            err,
            ChunkReadFallback {
                stage: rustfs_io_metrics::IoStage::ReadSetup,
                reason: rustfs_io_metrics::FallbackReason::EncryptionEnabled,
            }
        );
    }

    #[test]
    fn get_object_chunk_fast_path_guard_allows_plain_request() {
        assert!(get_object_chunk_fast_path_guard(false, false).is_ok());
    }

    #[test]
    fn get_object_chunk_range_guard_rejects_invalid_suffix_range() {
        let err = get_object_chunk_range_guard(Some(&HTTPRangeSpec {
            is_suffix_length: true,
            start: 4,
            end: 0,
        }))
        .unwrap_err();

        assert_eq!(
            err,
            ChunkReadFallback {
                stage: rustfs_io_metrics::IoStage::RangeGuard,
                reason: rustfs_io_metrics::FallbackReason::RangeNotSupported,
            }
        );
    }

    #[test]
    fn build_get_object_checksums_returns_default_when_mode_absent() {
        let checksums = build_get_object_checksums(&ObjectInfo::default(), &HeaderMap::new(), None, None).unwrap();
        assert_eq!(checksums, GetObjectChecksums::default());
    }

    #[test]
    fn plan_chunk_read_returns_fallback_for_compressed_object() {
        let mut info = ObjectInfo::default();
        rustfs_utils::http::insert_str(
            &mut info.user_defined,
            rustfs_utils::http::SUFFIX_COMPRESSION,
            rustfs_utils::CompressionAlgorithm::Zstd.to_string(),
        );

        let decision = plan_chunk_read(&info, true, None, None).unwrap();
        assert!(
            matches!(
                decision,
                ChunkReadDecision::Fallback(ChunkReadFallback {
                    stage: rustfs_io_metrics::IoStage::ReadSetup,
                    reason: rustfs_io_metrics::FallbackReason::CompressionEnabled,
                })
            ),
            "unexpected decision: {:?}",
            decision
        );
    }

    #[test]
    fn plan_chunk_read_returns_fallback_for_transitioned_object() {
        let mut info = ObjectInfo::default();
        info.transitioned_object.status = TRANSITION_COMPLETE.to_string();

        let decision = plan_chunk_read(&info, true, None, None).unwrap();
        assert!(matches!(
            decision,
            ChunkReadDecision::Fallback(ChunkReadFallback {
                stage: rustfs_io_metrics::IoStage::ReadSetup,
                reason: rustfs_io_metrics::FallbackReason::NonLocalBackend,
            })
        ));
    }

    #[test]
    fn plan_chunk_read_returns_range_guard_fallback_for_invalid_range() {
        let info = ObjectInfo {
            size: 16,
            actual_size: 16,
            ..Default::default()
        };

        let decision = plan_chunk_read(
            &info,
            true,
            Some(HTTPRangeSpec {
                is_suffix_length: false,
                start: -1,
                end: 4,
            }),
            None,
        )
        .unwrap();

        assert!(matches!(
            decision,
            ChunkReadDecision::Fallback(ChunkReadFallback {
                stage: rustfs_io_metrics::IoStage::RangeGuard,
                reason: rustfs_io_metrics::FallbackReason::RangeNotSupported,
            })
        ));
    }

    #[test]
    fn plan_chunk_read_allows_suffix_range() {
        let info = ObjectInfo {
            size: 16,
            actual_size: 16,
            ..Default::default()
        };

        let decision = plan_chunk_read(
            &info,
            true,
            Some(HTTPRangeSpec {
                is_suffix_length: true,
                start: 4,
                end: -1,
            }),
            None,
        )
        .unwrap();

        let ChunkReadDecision::Eligible(plan) = decision else {
            panic!("expected eligible plan");
        };
        let rs = plan.rs.expect("suffix range should be preserved");
        assert!(rs.is_suffix_length);
        assert_eq!(rs.start, 4);
        assert_eq!(plan.response_content_length, 4);
        assert_eq!(plan.content_range.as_deref(), Some("bytes 12-15/16"));
    }

    #[test]
    fn plan_chunk_read_uses_part_number_range_when_available() {
        let mut info = ObjectInfo {
            size: 12,
            actual_size: 12,
            content_type: Some("application/octet-stream".to_string()),
            ..Default::default()
        };
        info.parts = vec![Default::default(), Default::default()];
        info.parts[0].number = 1;
        info.parts[0].size = 5;
        info.parts[0].actual_size = 5;
        info.parts[1].number = 2;
        info.parts[1].size = 7;
        info.parts[1].actual_size = 7;

        let decision = plan_chunk_read(&info, true, None, Some(2)).unwrap();
        let ChunkReadDecision::Eligible(plan) = decision else {
            panic!("expected eligible plan");
        };
        let rs = plan.rs.expect("range from part number");
        assert_eq!(rs.start, 5);
        assert_eq!(rs.end, 11);
        assert_eq!(plan.response_content_length, 7);
        assert_eq!(plan.content_range.as_deref(), Some("bytes 5-11/12"));
        assert!(plan.content_type.is_some());
    }

    #[test]
    fn plan_chunk_read_returns_delete_marker_errors() {
        let info = ObjectInfo {
            delete_marker: true,
            ..Default::default()
        };

        let err = plan_chunk_read(&info, true, None, None).unwrap_err();
        assert!(matches!(err, ChunkReadPlanError::NoSuchKey));

        let err = plan_chunk_read(&info, false, None, None).unwrap_err();
        assert!(matches!(err, ChunkReadPlanError::MethodNotAllowed));
    }

    #[test]
    fn plan_legacy_read_uses_part_number_range_when_available() {
        let mut info = ObjectInfo {
            size: 12,
            actual_size: 12,
            content_type: Some("application/octet-stream".to_string()),
            ..Default::default()
        };
        info.parts = vec![Default::default(), Default::default()];
        info.parts[0].number = 1;
        info.parts[0].size = 5;
        info.parts[0].actual_size = 5;
        info.parts[1].number = 2;
        info.parts[1].size = 7;
        info.parts[1].actual_size = 7;

        let plan = plan_legacy_read(&info, None, Some(2)).unwrap();

        let rs = plan.rs.expect("range from part number");
        assert_eq!(rs.start, 5);
        assert_eq!(rs.end, 11);
        assert_eq!(plan.response_content_length, 7);
        assert_eq!(plan.content_range.as_deref(), Some("bytes 5-11/12"));
        assert!(plan.content_type.is_some());
    }

    #[test]
    fn build_reader_read_setup_uses_encryption_length_override() {
        let plan = LegacyReadPlan {
            rs: None,
            content_type: None,
            last_modified: None,
            response_content_length: 16,
            content_range: None,
        };
        let encryption_state = GetObjectEncryptionState {
            encryption_applied: true,
            response_content_length_override: Some(12),
            ..Default::default()
        };
        let reader = Box::new(rustfs_rio::WarpReader::new(tokio::io::empty())) as Box<dyn Reader>;

        let setup = build_reader_read_setup(ObjectInfo::default(), ObjectInfo::default(), reader, plan, encryption_state);

        assert!(setup.encryption_applied);
        assert_eq!(setup.response_content_length, 12);
        match setup.body_source {
            GetObjectBodySource::Reader(_) => {}
            GetObjectBodySource::Chunk { .. } => panic!("expected reader body source"),
        }
    }

    #[test]
    fn plan_get_object_body_buffers_seekable_small_plain_request() {
        let plan = plan_get_object_body(
            GetObjectBodyPlanningInputs {
                is_part_request: false,
                is_range_request: false,
                encryption_applied: false,
                response_size: 1024,
            },
            4096,
        );

        assert_eq!(plan, GetObjectBodyPlan::BufferSeekable);
    }

    #[test]
    fn disk_metric_contract_preserves_io_labels() {
        let contract =
            GetObjectDataPlaneMetricContract::disk(rustfs_io_metrics::IoPath::Legacy, rustfs_io_metrics::CopyMode::SingleCopy);

        assert_eq!(contract.io_path, rustfs_io_metrics::IoPath::Legacy);
        assert_eq!(contract.copy_mode, rustfs_io_metrics::CopyMode::SingleCopy);
    }

    #[test]
    fn plan_get_object_body_uses_encrypted_buffer_for_small_plain_request() {
        let plan = plan_get_object_body(
            GetObjectBodyPlanningInputs {
                is_part_request: false,
                is_range_request: false,
                encryption_applied: true,
                response_size: 1024,
            },
            4096,
        );

        assert_eq!(plan, GetObjectBodyPlan::BufferEncrypted);
    }

    #[test]
    fn get_object_sequential_hint_distinguishes_prefix_and_suffix_ranges() {
        assert!(get_object_sequential_hint(None));
        assert!(get_object_sequential_hint(Some(&HTTPRangeSpec {
            is_suffix_length: false,
            start: 0,
            end: -1,
        })));
        assert!(!get_object_sequential_hint(Some(&HTTPRangeSpec {
            is_suffix_length: false,
            start: 4,
            end: 8,
        })));
        assert!(!get_object_sequential_hint(Some(&HTTPRangeSpec {
            is_suffix_length: true,
            start: 4,
            end: -1,
        })));
    }

    #[test]
    fn plan_get_object_strategy_layout_caps_buffer_to_fallback() {
        let layout = plan_get_object_strategy_layout(None, 1024, 8192, 4096);

        assert!(layout.is_sequential_hint);
        assert_eq!(layout.optimal_buffer_size, 4096);
    }

    #[test]
    fn frozen_get_object_body_reuses_same_shared_bytes_for_memory_blob() {
        let frozen = FrozenGetObjectBody::new(Bytes::from_static(b"abc"));
        let shared = Arc::clone(frozen.shared_body());
        assert_eq!(*shared, Bytes::from_static(b"abc"));
        assert!(Arc::ptr_eq(&shared, frozen.shared_body()));
    }

    #[test]
    fn build_output_version_id_maps_nil_uuid_to_null() {
        let nil = uuid::Uuid::nil();
        let version_id = build_output_version_id(true, Some(&nil));

        assert_eq!(version_id.as_deref(), Some("null"));
    }

    #[test]
    fn build_get_object_output_context_preserves_copy_mode_override() {
        let info = ObjectInfo {
            version_id: Some(uuid::Uuid::nil()),
            ..Default::default()
        };
        let output_context = build_get_object_output_context(
            None,
            info,
            ObjectInfo::default(),
            None,
            None,
            8,
            None,
            None,
            None,
            None,
            None,
            &GetObjectChecksums::default(),
            None,
            true,
            4096,
            Some(rustfs_io_metrics::CopyMode::Reconstructed),
        );

        assert_eq!(output_context.output.version_id.as_deref(), Some("null"));
        assert_eq!(output_context.copy_mode_override, Some(rustfs_io_metrics::CopyMode::Reconstructed));
        assert_eq!(output_context.optimal_buffer_size, 4096);
    }

    #[test]
    fn build_get_object_output_preserves_http_metadata_like_cached_path() {
        let mut info = ObjectInfo {
            content_type: Some("application/octet-stream".to_string()),
            content_encoding: Some("zstd".to_string()),
            etag: Some("etag".to_string()),
            expires: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };
        info.user_defined
            .insert("cache-control".to_string(), "max-age=3600".to_string());
        info.user_defined
            .insert("content-disposition".to_string(), "attachment; filename=\"bundle.zip\"".to_string());
        info.user_defined.insert("content-language".to_string(), "en-US".to_string());

        let output = build_get_object_output(
            None,
            &info,
            info.content_type
                .as_ref()
                .and_then(|content_type| ContentType::from_str(content_type).ok()),
            info.mod_time.map(Timestamp::from),
            8,
            None,
            None,
            None,
            None,
            None,
            &GetObjectChecksums::default(),
            None,
            None,
        );

        assert_eq!(output.cache_control.as_deref(), Some("max-age=3600"));
        assert_eq!(output.content_disposition.as_deref(), Some("attachment; filename=\"bundle.zip\""));
        assert_eq!(output.content_language.as_deref(), Some("en-US"));
        assert_eq!(output.content_encoding.as_deref(), Some("zstd"));
        assert_eq!(output.expires, Some(Timestamp::from(OffsetDateTime::UNIX_EPOCH)));
    }

    #[test]
    fn build_cors_wrapped_get_object_flow_result_uses_wrapped_mode() {
        let result = build_cors_wrapped_get_object_flow_result(
            GetObjectOutputContext {
                output: GetObjectOutput::default(),
                event_info: ObjectInfo::default(),
                response_content_length: 1,
                optimal_buffer_size: 1024,
                copy_mode_override: None,
            },
            "vid".to_string(),
        );

        assert_eq!(result.version_id_for_event, "vid");
    }

    #[test]
    fn finalize_chunk_read_setup_preserves_body_source_and_io_path() {
        let chunk_result = GetObjectChunkResult {
            stream: Box::pin(futures_util::stream::empty::<std::io::Result<rustfs_io_core::IoChunk>>()),
            path: GetObjectChunkPath::Direct,
            copy_mode: GetObjectChunkCopyMode::Reconstructed,
        };
        let plan = ChunkReadPlan {
            rs: Some(HTTPRangeSpec {
                is_suffix_length: false,
                start: 0,
                end: 7,
            }),
            content_type: None,
            last_modified: None,
            response_content_length: 8,
            content_range: Some("bytes 0-7/8".to_string()),
        };

        let result = finalize_chunk_read_setup(ObjectInfo::default(), ObjectInfo::default(), chunk_result, plan);

        assert_eq!(result.io_path, rustfs_io_metrics::IoPath::Fast);
        match result.read_setup.body_source {
            GetObjectBodySource::Chunk { path, copy_mode, .. } => {
                assert_eq!(path, GetObjectChunkPath::Direct);
                assert_eq!(copy_mode, rustfs_io_metrics::CopyMode::Reconstructed);
            }
            GetObjectBodySource::Reader(_) => panic!("expected chunk body source"),
        }
        assert_eq!(result.read_setup.response_content_length, 8);
        assert_eq!(result.read_setup.content_range.as_deref(), Some("bytes 0-7/8"));
    }
}
