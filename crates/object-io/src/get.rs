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
use rustfs_concurrency::GetObjectCacheEligibility;
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
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
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

pub trait CachedGetObjectSource {
    fn body(&self) -> &Arc<Bytes>;
    fn content_length(&self) -> i64;
    fn content_type(&self) -> Option<&str>;
    fn e_tag(&self) -> Option<&str>;
    fn last_modified(&self) -> Option<&str>;
    fn cache_control(&self) -> Option<&str>;
    fn content_disposition(&self) -> Option<&str>;
    fn content_encoding(&self) -> Option<&str>;
    fn content_language(&self) -> Option<&str>;
    fn storage_class(&self) -> Option<&str>;
    fn version_id(&self) -> Option<&str>;
    fn delete_marker(&self) -> bool;
    fn tag_count(&self) -> Option<i32>;
    fn user_metadata(&self) -> &HashMap<String, String>;
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
    CacheWriteback,
    BufferEncrypted,
    BufferSeekable,
    Stream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetObjectDataPlaneRequestSource {
    CacheServed,
    Disk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetObjectDataPlaneMetricContract {
    pub request_source: GetObjectDataPlaneRequestSource,
    pub io_path: rustfs_io_metrics::IoPath,
    pub copy_mode: rustfs_io_metrics::CopyMode,
    pub record_cache_served_metric: bool,
    pub record_cache_writeback_metric: bool,
}

impl GetObjectDataPlaneMetricContract {
    pub fn cache_served() -> Self {
        Self {
            request_source: GetObjectDataPlaneRequestSource::CacheServed,
            io_path: rustfs_io_metrics::IoPath::Fast,
            copy_mode: rustfs_io_metrics::CopyMode::SharedBytes,
            record_cache_served_metric: true,
            record_cache_writeback_metric: false,
        }
    }

    pub fn disk(
        io_path: rustfs_io_metrics::IoPath,
        copy_mode: rustfs_io_metrics::CopyMode,
        body_plan: GetObjectBodyPlan,
    ) -> Self {
        Self {
            request_source: GetObjectDataPlaneRequestSource::Disk,
            io_path,
            copy_mode,
            record_cache_served_metric: false,
            record_cache_writeback_metric: matches!(body_plan, GetObjectBodyPlan::CacheWriteback),
        }
    }
}

pub struct GetObjectCacheWriteback {
    pub body: Arc<Bytes>,
    pub content_length: i64,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_language: Option<String>,
    pub expires: Option<String>,
    pub storage_class: Option<String>,
    pub version_id: Option<String>,
    pub delete_marker: bool,
    pub user_metadata: HashMap<String, String>,
    pub e_tag: Option<String>,
    pub last_modified: Option<String>,
}

pub struct GetObjectBodyMaterialization {
    pub body: Option<StreamingBlob>,
    pub cache_writeback: Option<GetObjectCacheWriteback>,
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
    #[error("failed to read object for caching: {0}")]
    CacheRead(std::io::Error),
    #[error("failed to read decrypted object: {0}")]
    EncryptedRead(std::io::Error),
}

pub enum GetObjectResponseMode {
    Plain,
    CorsWrapped,
}

pub struct GetObjectFlowResult {
    pub output: GetObjectOutput,
    pub event_info: ObjectInfo,
    pub version_id_for_event: String,
    pub response_mode: GetObjectResponseMode,
}

pub fn build_get_object_flow_result(
    output: GetObjectOutput,
    event_info: ObjectInfo,
    version_id_for_event: String,
    response_mode: GetObjectResponseMode,
) -> GetObjectFlowResult {
    GetObjectFlowResult {
        output,
        event_info,
        version_id_for_event,
        response_mode,
    }
}

pub fn build_cached_get_object_flow_result_from_source<T>(
    bucket: &str,
    key: &str,
    cached: &T,
    version_id_for_event: String,
) -> GetObjectFlowResult
where
    T: CachedGetObjectSource,
{
    build_get_object_flow_result(
        build_cached_get_object_output_from_source(cached),
        build_cached_get_object_event_info_from_source(bucket, key, cached),
        version_id_for_event,
        GetObjectResponseMode::Plain,
    )
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
    build_get_object_flow_result(output, event_info, version_id_for_event, GetObjectResponseMode::CorsWrapped)
}

pub fn build_cached_get_object_output_from_source<T>(cached: &T) -> GetObjectOutput
where
    T: CachedGetObjectSource,
{
    let body_data = Arc::clone(cached.body());
    let body = Some(StreamingBlob::wrap::<_, std::convert::Infallible>(futures_util::stream::once(
        async move { Ok((*body_data).clone()) },
    )));

    let last_modified = cached
        .last_modified()
        .and_then(|s| OffsetDateTime::parse(s, &Rfc3339).ok())
        .map(Timestamp::from);

    let content_type = cached.content_type().and_then(|ct| ContentType::from_str(ct).ok());

    let metadata = (!cached.user_metadata().is_empty()).then(|| cached.user_metadata().clone());

    GetObjectOutput {
        body,
        content_length: Some(cached.content_length()),
        accept_ranges: Some("bytes".to_string()),
        e_tag: cached.e_tag().map(to_s3s_etag),
        last_modified,
        content_type,
        cache_control: cached.cache_control().map(str::to_string),
        content_disposition: cached.content_disposition().map(str::to_string),
        content_encoding: cached.content_encoding().map(str::to_string),
        content_language: cached.content_language().map(str::to_string),
        version_id: cached.version_id().map(str::to_string),
        delete_marker: Some(cached.delete_marker()),
        tag_count: cached.tag_count(),
        metadata,
        ..Default::default()
    }
}
pub fn build_cached_get_object_event_info_from_source<T>(bucket: &str, key: &str, cached: &T) -> ObjectInfo
where
    T: CachedGetObjectSource,
{
    let last_modified = cached.last_modified().and_then(|s| OffsetDateTime::parse(s, &Rfc3339).ok());
    let version_id = cached.version_id().and_then(|v| uuid::Uuid::parse_str(v).ok());

    ObjectInfo {
        bucket: bucket.to_string(),
        name: key.to_string(),
        storage_class: cached.storage_class().map(str::to_string),
        mod_time: last_modified,
        size: cached.content_length(),
        actual_size: cached.content_length(),
        is_dir: false,
        user_defined: cached.user_metadata().clone(),
        version_id,
        delete_marker: cached.delete_marker(),
        content_type: cached.content_type().map(str::to_string),
        content_encoding: cached.content_encoding().map(str::to_string),
        etag: cached.e_tag().map(str::to_string),
        ..Default::default()
    }
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

pub fn build_get_object_checksums(
    info: &ObjectInfo,
    headers: &HeaderMap,
    part_number: Option<usize>,
    rs: Option<&HTTPRangeSpec>,
) -> std::io::Result<GetObjectChecksums> {
    let mut checksums = GetObjectChecksums::default();

    if let Some(checksum_mode) = headers.get(AMZ_CHECKSUM_MODE)
        && checksum_mode.to_str().unwrap_or_default() == "ENABLED"
        && rs.is_none()
    {
        let (decrypted_checksums, _is_multipart) = info
            .decrypt_checksums(part_number.unwrap_or(0), headers)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

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
    }

    Ok(checksums)
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
    cache_eligibility: GetObjectCacheEligibility,
    seekable_object_size_threshold: usize,
) -> GetObjectBodyPlan {
    if cache_eligibility.should_cache() {
        return GetObjectBodyPlan::CacheWriteback;
    }

    let should_buffer_for_seek = cache_eligibility.response_size > 0
        && cache_eligibility.response_size <= seekable_object_size_threshold as i64
        && !cache_eligibility.is_part_request
        && !cache_eligibility.is_range_request;

    if cache_eligibility.encryption_applied && should_buffer_for_seek {
        GetObjectBodyPlan::BufferEncrypted
    } else if should_buffer_for_seek {
        GetObjectBodyPlan::BufferSeekable
    } else {
        GetObjectBodyPlan::Stream
    }
}

pub fn build_get_object_cache_writeback(info: &ObjectInfo, body: Bytes, content_length: i64) -> GetObjectCacheWriteback {
    let body = FrozenGetObjectBody::new(body);
    GetObjectCacheWriteback {
        body: body.into_shared_body(),
        content_length,
        content_type: info.content_type.clone(),
        content_encoding: info.content_encoding.clone(),
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        storage_class: info.storage_class.clone(),
        version_id: info.version_id.map(|vid| {
            if vid == uuid::Uuid::nil() {
                "null".to_string()
            } else {
                vid.to_string()
            }
        }),
        delete_marker: info.delete_marker,
        user_metadata: HashMap::new(),
        e_tag: info.etag.clone(),
        last_modified: info.mod_time.and_then(|t| t.format(&Rfc3339).ok()),
    }
}

pub fn finalize_get_object_cache_writeback(
    info: &ObjectInfo,
    writeback: GetObjectCacheWriteback,
    user_metadata: HashMap<String, String>,
) -> GetObjectCacheWriteback {
    GetObjectCacheWriteback {
        cache_control: info.user_defined.get(CACHE_CONTROL.as_str()).cloned(),
        content_disposition: info.user_defined.get(CONTENT_DISPOSITION.as_str()).cloned(),
        content_language: info.user_defined.get(CONTENT_LANGUAGE.as_str()).cloned(),
        expires: info.expires.and_then(|t| t.format(&Rfc3339).ok()),
        user_metadata,
        ..writeback
    }
}

pub async fn materialize_get_object_body<R>(
    mut final_stream: R,
    info: &ObjectInfo,
    plan: GetObjectBodyPlan,
    response_content_length: i64,
    optimal_buffer_size: usize,
) -> Result<GetObjectBodyMaterialization, MaterializeGetObjectBodyError>
where
    R: AsyncRead + Send + Sync + Unpin + 'static,
{
    match plan {
        GetObjectBodyPlan::CacheWriteback => {
            let mut buf = Vec::with_capacity(response_content_length as usize);
            tokio::io::AsyncReadExt::read_to_end(&mut final_stream, &mut buf)
                .await
                .map_err(MaterializeGetObjectBodyError::CacheRead)?;
            let body = FrozenGetObjectBody::new(Bytes::from(buf));

            Ok(GetObjectBodyMaterialization {
                body: body.build_blob(response_content_length, optimal_buffer_size),
                cache_writeback: Some(build_get_object_cache_writeback(
                    info,
                    body.shared_body().as_ref().clone(),
                    response_content_length,
                )),
                plan,
            })
        }
        GetObjectBodyPlan::BufferEncrypted => {
            let mut buf = Vec::with_capacity(response_content_length as usize);
            tokio::io::AsyncReadExt::read_to_end(&mut final_stream, &mut buf)
                .await
                .map_err(MaterializeGetObjectBodyError::EncryptedRead)?;
            let body = FrozenGetObjectBody::new(Bytes::from(buf));

            Ok(GetObjectBodyMaterialization {
                body: body.build_blob(response_content_length, optimal_buffer_size),
                cache_writeback: None,
                plan,
            })
        }
        GetObjectBodyPlan::BufferSeekable => {
            let mut buf = Vec::with_capacity(response_content_length as usize);
            let body = match tokio::io::AsyncReadExt::read_to_end(&mut final_stream, &mut buf).await {
                Ok(_) => FrozenGetObjectBody::new(Bytes::from(buf)).build_blob(response_content_length, optimal_buffer_size),
                Err(_) => build_reader_blob(final_stream, response_content_length, optimal_buffer_size),
            };

            Ok(GetObjectBodyMaterialization {
                body,
                cache_writeback: None,
                plan,
            })
        }
        GetObjectBodyPlan::Stream => Ok(GetObjectBodyMaterialization {
            body: build_reader_blob(final_stream, response_content_length, optimal_buffer_size),
            cache_writeback: None,
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
        content_type,
        content_encoding: info.content_encoding.clone(),
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

    struct MockCachedSource {
        body: Arc<Bytes>,
        content_length: i64,
        content_type: Option<String>,
        e_tag: Option<String>,
        last_modified: Option<String>,
        cache_control: Option<String>,
        content_disposition: Option<String>,
        content_encoding: Option<String>,
        content_language: Option<String>,
        storage_class: Option<String>,
        version_id: Option<String>,
        delete_marker: bool,
        tag_count: Option<i32>,
        user_metadata: HashMap<String, String>,
    }

    impl CachedGetObjectSource for MockCachedSource {
        fn body(&self) -> &Arc<Bytes> {
            &self.body
        }

        fn content_length(&self) -> i64 {
            self.content_length
        }

        fn content_type(&self) -> Option<&str> {
            self.content_type.as_deref()
        }

        fn e_tag(&self) -> Option<&str> {
            self.e_tag.as_deref()
        }

        fn last_modified(&self) -> Option<&str> {
            self.last_modified.as_deref()
        }

        fn cache_control(&self) -> Option<&str> {
            self.cache_control.as_deref()
        }

        fn content_disposition(&self) -> Option<&str> {
            self.content_disposition.as_deref()
        }

        fn content_encoding(&self) -> Option<&str> {
            self.content_encoding.as_deref()
        }

        fn content_language(&self) -> Option<&str> {
            self.content_language.as_deref()
        }

        fn storage_class(&self) -> Option<&str> {
            self.storage_class.as_deref()
        }

        fn version_id(&self) -> Option<&str> {
            self.version_id.as_deref()
        }

        fn delete_marker(&self) -> bool {
            self.delete_marker
        }

        fn tag_count(&self) -> Option<i32> {
            self.tag_count
        }

        fn user_metadata(&self) -> &HashMap<String, String> {
            &self.user_metadata
        }
    }

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
    fn plan_get_object_body_prefers_cache_writeback_when_cacheable() {
        let plan = plan_get_object_body(
            GetObjectCacheEligibility {
                cache_enabled: true,
                cache_writeback_enabled: true,
                is_part_request: false,
                is_range_request: false,
                encryption_applied: false,
                response_size: 1024,
                max_cacheable_size: 2048,
            },
            4096,
        );

        assert_eq!(plan, GetObjectBodyPlan::CacheWriteback);
    }

    #[test]
    fn cache_served_metric_contract_is_mutually_exclusive_with_cache_writeback() {
        let contract = GetObjectDataPlaneMetricContract::cache_served();

        assert_eq!(contract.request_source, GetObjectDataPlaneRequestSource::CacheServed);
        assert_eq!(contract.io_path, rustfs_io_metrics::IoPath::Fast);
        assert_eq!(contract.copy_mode, rustfs_io_metrics::CopyMode::SharedBytes);
        assert!(contract.record_cache_served_metric);
        assert!(!contract.record_cache_writeback_metric);
    }

    #[test]
    fn disk_metric_contract_can_mark_cache_writeback_without_reclassifying_request_source() {
        let contract = GetObjectDataPlaneMetricContract::disk(
            rustfs_io_metrics::IoPath::Legacy,
            rustfs_io_metrics::CopyMode::SingleCopy,
            GetObjectBodyPlan::CacheWriteback,
        );

        assert_eq!(contract.request_source, GetObjectDataPlaneRequestSource::Disk);
        assert_eq!(contract.io_path, rustfs_io_metrics::IoPath::Legacy);
        assert_eq!(contract.copy_mode, rustfs_io_metrics::CopyMode::SingleCopy);
        assert!(!contract.record_cache_served_metric);
        assert!(contract.record_cache_writeback_metric);
    }

    #[test]
    fn plan_get_object_body_uses_encrypted_buffer_for_small_plain_request() {
        let plan = plan_get_object_body(
            GetObjectCacheEligibility {
                cache_enabled: false,
                cache_writeback_enabled: false,
                is_part_request: false,
                is_range_request: false,
                encryption_applied: true,
                response_size: 1024,
                max_cacheable_size: 0,
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
    fn build_get_object_cache_writeback_formats_metadata() {
        let info = ObjectInfo {
            content_type: Some("application/octet-stream".to_string()),
            etag: Some("abc123".to_string()),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };

        let writeback = build_get_object_cache_writeback(&info, Bytes::from_static(b"abc"), 3);

        assert_eq!(*writeback.body, Bytes::from_static(b"abc"));
        assert_eq!(writeback.content_length, 3);
        assert_eq!(writeback.content_type.as_deref(), Some("application/octet-stream"));
        assert_eq!(writeback.e_tag.as_deref(), Some("abc123"));
        assert_eq!(writeback.last_modified.as_deref(), Some("1970-01-01T00:00:00Z"));
    }

    #[test]
    fn finalize_get_object_cache_writeback_applies_http_metadata_and_user_metadata() {
        let mut info = ObjectInfo {
            expires: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };
        info.user_defined
            .insert("cache-control".to_string(), "max-age=3600".to_string());
        info.user_defined
            .insert("content-disposition".to_string(), "attachment".to_string());
        info.user_defined.insert("content-language".to_string(), "en-US".to_string());

        let writeback = finalize_get_object_cache_writeback(
            &info,
            GetObjectCacheWriteback {
                body: Arc::new(Bytes::from_static(b"abc")),
                content_length: 3,
                content_type: None,
                content_encoding: None,
                cache_control: None,
                content_disposition: None,
                content_language: None,
                expires: None,
                storage_class: None,
                version_id: None,
                delete_marker: false,
                user_metadata: HashMap::new(),
                e_tag: None,
                last_modified: None,
            },
            HashMap::from([(String::from("custom"), String::from("value"))]),
        );

        assert_eq!(writeback.cache_control.as_deref(), Some("max-age=3600"));
        assert_eq!(writeback.content_disposition.as_deref(), Some("attachment"));
        assert_eq!(writeback.content_language.as_deref(), Some("en-US"));
        assert_eq!(writeback.expires.as_deref(), Some("1970-01-01T00:00:00Z"));
        assert_eq!(writeback.user_metadata.get("custom").map(String::as_str), Some("value"));
    }

    #[test]
    fn frozen_get_object_body_reuses_same_shared_bytes_for_cache_writeback() {
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
    fn build_cached_get_object_flow_result_from_source_builds_plain_mode() {
        let result = build_cached_get_object_flow_result_from_source(
            "bucket",
            "key",
            &MockCachedSource {
                body: Arc::new(Bytes::from_static(b"abc")),
                content_length: 3,
                content_type: None,
                e_tag: None,
                last_modified: None,
                cache_control: None,
                content_disposition: None,
                content_encoding: None,
                content_language: None,
                storage_class: None,
                version_id: None,
                delete_marker: false,
                tag_count: None,
                user_metadata: HashMap::new(),
            },
            "vid".to_string(),
        );

        assert!(matches!(result.response_mode, GetObjectResponseMode::Plain));
        assert_eq!(result.version_id_for_event, "vid");
        assert_eq!(result.event_info.bucket, "bucket");
        assert_eq!(result.event_info.name, "key");
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

        assert!(matches!(result.response_mode, GetObjectResponseMode::CorsWrapped));
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
