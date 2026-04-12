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

use super::DeadlockRequestGuard;
use super::GetObjectRequestContext;
use super::get_object_zero_copy::{
    GetObjectIoPlanning, GetObjectPreparedRead, prepare_get_object_read, prepare_get_object_read_execution,
};
use crate::error::ApiError;
use crate::storage::concurrency::{ConcurrencyManager, GetObjectGuard, get_buffer_size_opt_in};
use crate::storage::get_validated_store;
use crate::storage::options::filter_object_metadata;
use crate::storage::timeout_wrapper::{RequestTimeoutWrapper, TimeoutConfig};
use bytes::Bytes;
use futures_util::StreamExt;
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::store_api::{HTTPRangeSpec, ObjectInfo};
use rustfs_io_core::BoxChunkStream;
use rustfs_object_io::get::{
    GetObjectBodyPlan as ObjectIoGetObjectBodyPlan, GetObjectBodyPlanningInputs as ObjectIoGetObjectBodyPlanningInputs,
    GetObjectBodySource, GetObjectDataPlaneMetricContract as ObjectIoGetObjectDataPlaneMetricContract, GetObjectFlowResult,
    GetObjectOutputContext, GetObjectReadSetup, MaterializeGetObjectBodyError as ObjectIoMaterializeGetObjectBodyError,
    build_cors_wrapped_get_object_flow_result as object_io_build_cors_wrapped_get_object_flow_result,
    build_get_object_checksums as object_io_build_get_object_checksums,
    build_get_object_output_context as object_io_build_get_object_output_context,
    build_memory_blob as object_io_build_memory_blob, chunk_body_data_plane_labels as object_io_chunk_body_data_plane_labels,
    get_object_chunk_path_label as object_io_get_object_chunk_path_label,
    materialize_get_object_body as object_io_materialize_get_object_body, plan_get_object_body as object_io_plan_get_object_body,
    plan_get_object_strategy_layout as object_io_plan_get_object_strategy_layout,
};
use s3s::S3Result;
use s3s::dto::StreamingBlob;
use std::time::Duration;
use tokio::io::AsyncRead;
use tracing::{debug, error, info, warn};

pub(super) struct GetObjectBootstrap {
    pub(super) timeout_config: TimeoutConfig,
    pub(super) wrapper: RequestTimeoutWrapper,
    pub(super) request_start: std::time::Instant,
    pub(super) request_guard: GetObjectGuard,
    pub(super) _deadlock_request_guard: DeadlockRequestGuard,
}

#[derive(Debug)]
struct ChunkCommitMaterializationError {
    source: std::io::Error,
    streamed_bytes: usize,
}

fn build_chunk_materialization_length_error(actual: usize, expected: usize) -> std::io::Error {
    let error_kind = if actual > expected {
        std::io::ErrorKind::InvalidData
    } else {
        std::io::ErrorKind::UnexpectedEof
    };

    std::io::Error::new(
        error_kind,
        format!("chunk fast path produced {actual} bytes before response commit, expected {expected}"),
    )
}

async fn materialize_chunk_stream_before_commit_with_threshold(
    mut chunk_stream: BoxChunkStream,
    response_content_length: i64,
    optimal_buffer_size: usize,
    in_memory_threshold_bytes: usize,
) -> Result<Option<StreamingBlob>, ChunkCommitMaterializationError> {
    let expected_bytes = usize::try_from(response_content_length).map_err(|_| ChunkCommitMaterializationError {
        source: std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("negative response content length {response_content_length} for chunk fast path"),
        ),
        streamed_bytes: 0,
    })?;

    // Objects larger than the in-memory threshold fall back to the legacy reader path
    // rather than spooling to disk, to avoid exhausting local disk under concurrent large downloads.
    if expected_bytes > in_memory_threshold_bytes {
        return Err(ChunkCommitMaterializationError {
            source: std::io::Error::other(format!(
                "chunk fast path object size {expected_bytes} exceeds in-memory threshold \
                 {in_memory_threshold_bytes}; falling back to legacy reader"
            )),
            streamed_bytes: 0,
        });
    }

    let mut buf = Vec::with_capacity(expected_bytes);
    let mut streamed_bytes = 0usize;

    while let Some(result) = chunk_stream.next().await {
        let chunk = result.map_err(|source| ChunkCommitMaterializationError { source, streamed_bytes })?;
        let bytes = chunk.as_bytes();
        streamed_bytes = streamed_bytes.saturating_add(bytes.len());
        if streamed_bytes > expected_bytes {
            return Err(ChunkCommitMaterializationError {
                source: build_chunk_materialization_length_error(streamed_bytes, expected_bytes),
                streamed_bytes,
            });
        }
        buf.extend_from_slice(bytes.as_ref());
    }

    if streamed_bytes != expected_bytes {
        return Err(ChunkCommitMaterializationError {
            source: build_chunk_materialization_length_error(streamed_bytes, expected_bytes),
            streamed_bytes,
        });
    }

    Ok(object_io_build_memory_blob(
        Bytes::from(buf),
        response_content_length,
        optimal_buffer_size,
    ))
}

async fn materialize_chunk_stream_before_commit(
    chunk_stream: BoxChunkStream,
    response_content_length: i64,
    optimal_buffer_size: usize,
) -> Result<Option<StreamingBlob>, ChunkCommitMaterializationError> {
    materialize_chunk_stream_before_commit_with_threshold(
        chunk_stream,
        response_content_length,
        optimal_buffer_size,
        rustfs_config::DEFAULT_OBJECT_SEEK_SUPPORT_THRESHOLD,
    )
    .await
}

async fn build_get_object_body_adapter<R>(
    final_stream: R,
    bucket: &str,
    key: &str,
    response_content_length: i64,
    optimal_buffer_size: usize,
    planning_inputs: ObjectIoGetObjectBodyPlanningInputs,
) -> S3Result<Option<StreamingBlob>>
where
    R: AsyncRead + Send + Sync + Unpin + 'static,
{
    let body_plan = object_io_plan_get_object_body(planning_inputs, rustfs_config::DEFAULT_OBJECT_SEEK_SUPPORT_THRESHOLD);

    match body_plan {
        ObjectIoGetObjectBodyPlan::BufferSeekable => {
            debug!(
                bucket = %bucket,
                key = %key,
                size = response_content_length,
                "reading object into memory for seek support"
            );
        }
        ObjectIoGetObjectBodyPlan::Stream if planning_inputs.encryption_applied => {
            info!(
                "Encrypted object: Using unlimited stream for decryption with buffer size {}",
                optimal_buffer_size
            );
        }
        _ => {}
    }

    let materialized =
        object_io_materialize_get_object_body(final_stream, body_plan, response_content_length, optimal_buffer_size)
            .await
            .map_err(|err| match err {
                ObjectIoMaterializeGetObjectBodyError::EncryptedRead(err) => {
                    error!("Failed to read decrypted object into memory: {}", err);
                    ApiError::from(StorageError::other(format!("Failed to read decrypted object: {err}")))
                }
            })?;

    Ok(materialized.body)
}

fn finalize_get_object_completion(
    request_context: &GetObjectRequestContext,
    wrapper: &RequestTimeoutWrapper,
    timeout_config: &TimeoutConfig,
    total_duration: Duration,
    response_content_length: i64,
    optimal_buffer_size: usize,
    metric_contract: ObjectIoGetObjectDataPlaneMetricContract,
) {
    rustfs_io_metrics::record_get_object_completion(total_duration.as_secs_f64(), response_content_length, optimal_buffer_size);

    rustfs_io_metrics::record_get_object(total_duration.as_millis() as f64, response_content_length);
    rustfs_io_metrics::record_io_copy_mode("get", metric_contract.copy_mode, response_content_length.max(0) as usize);

    if wrapper.is_timeout() {
        warn!(
            bucket = %request_context.bucket,
            key = %request_context.key,
            elapsed = ?wrapper.elapsed(),
            timeout = ?timeout_config.get_object_timeout,
            "GetObject request exceeded timeout"
        );
        rustfs_io_metrics::record_get_object_timeout(None, Some(wrapper.elapsed().as_secs_f64()));
    }

    debug!(
        bucket = %request_context.bucket,
        key = %request_context.key,
        size = response_content_length,
        duration = ?total_duration,
        buffer = optimal_buffer_size,
        "GetObject completed"
    );
}

fn get_object_strategy_range<'a>(
    request_context: &'a GetObjectRequestContext,
    resolved_range: Option<&'a HTTPRangeSpec>,
) -> Option<&'a HTTPRangeSpec> {
    resolved_range.or(request_context.rs.as_ref())
}

fn finalize_get_object_strategy_runtime(
    request_context: &GetObjectRequestContext,
    resolved_range: Option<&HTTPRangeSpec>,
    manager: &ConcurrencyManager,
    base_buffer_size: usize,
    info: &ObjectInfo,
    response_content_length: i64,
    io_planning: &GetObjectIoPlanning<'_>,
) -> usize {
    let strategy_range = get_object_strategy_range(request_context, resolved_range);
    let strategy_layout = object_io_plan_get_object_strategy_layout(
        strategy_range,
        response_content_length,
        0,
        get_buffer_size_opt_in(response_content_length),
    );

    if let Some(range_spec) = strategy_range
        && range_spec.start >= 0
    {
        manager.record_access(range_spec.start as u64, response_content_length as u64);
    }

    if response_content_length > 0 {
        manager.record_transfer(response_content_length as u64, io_planning.permit_wait_duration);
    }

    let io_strategy = manager.calculate_io_strategy_with_context(
        info.size,
        base_buffer_size,
        io_planning.permit_wait_duration,
        strategy_layout.is_sequential_hint,
    );

    debug!(
        wait_ms = io_planning.permit_wait_duration.as_millis() as u64,
        load_level = ?io_strategy.load_level,
        buffer_size = io_strategy.buffer_size,
        buffer_multiplier = io_strategy.buffer_multiplier,
        readahead = io_strategy.enable_readahead,
        storage_media = ?io_strategy.storage_media,
        access_pattern = ?io_strategy.access_pattern,
        bandwidth_tier = ?io_strategy.bandwidth_tier,
        concurrent_requests = io_strategy.concurrent_requests,
        file_size = info.size,
        is_sequential = strategy_layout.is_sequential_hint,
        "Enhanced multi-factor I/O strategy calculated"
    );

    let io_priority = manager.get_io_priority(response_content_length);

    if manager.is_priority_scheduling_enabled() {
        debug!(
            bucket = %request_context.bucket,
            key = %request_context.key,
            priority = %io_priority,
            request_size = response_content_length,
            "I/O priority assigned (based on actual request size)"
        );

        rustfs_io_metrics::record_io_priority_assignment(io_priority.as_str());
    }

    rustfs_io_metrics::record_get_object_io_state(
        io_planning.permit_wait_duration.as_secs_f64(),
        io_planning.queue_utilization,
        io_planning.queue_status.permits_in_use,
        io_planning
            .queue_status
            .total_permits
            .saturating_sub(io_planning.queue_status.permits_in_use),
        io_strategy.load_level.as_str(),
        io_strategy.buffer_multiplier,
    );

    let strategy_layout = object_io_plan_get_object_strategy_layout(
        strategy_range,
        response_content_length,
        io_strategy.buffer_size,
        get_buffer_size_opt_in(response_content_length),
    );

    debug!(
        actual_request_size = response_content_length,
        priority = %io_priority.as_str(),
        "I/O priority finalized with actual request size"
    );

    debug!(
        "GetObject buffer sizing: file_size={}, base={}, optimal={}, concurrent_requests={}, io_strategy={:?}",
        response_content_length,
        get_buffer_size_opt_in(response_content_length),
        strategy_layout.optimal_buffer_size,
        io_strategy.concurrent_requests,
        io_strategy.load_level
    );

    strategy_layout.optimal_buffer_size
}

pub(super) async fn build_get_object_output_context(
    request_context: &GetObjectRequestContext,
    manager: &ConcurrencyManager,
    read_setup: GetObjectReadSetup,
    io_planning: &GetObjectIoPlanning<'_>,
    base_buffer_size: usize,
    versioned: bool,
) -> S3Result<(GetObjectOutputContext, ObjectIoGetObjectDataPlaneMetricContract)> {
    let bucket = &request_context.bucket;
    let key = &request_context.key;
    let part_number = request_context.part_number;
    let mut active_read_setup = read_setup;

    loop {
        let GetObjectReadSetup {
            info,
            event_info,
            body_source,
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
        } = active_read_setup;

        let optimal_buffer_size = finalize_get_object_strategy_runtime(
            request_context,
            rs.as_ref(),
            manager,
            base_buffer_size,
            &info,
            response_content_length,
            io_planning,
        );

        let (body, metric_contract) = match body_source {
            GetObjectBodySource::Reader(final_stream) => {
                let body = build_get_object_body_adapter(
                    final_stream,
                    bucket,
                    key,
                    response_content_length,
                    optimal_buffer_size,
                    ObjectIoGetObjectBodyPlanningInputs {
                        is_part_request: part_number.is_some(),
                        is_range_request: rs.is_some(),
                        encryption_applied,
                        response_size: response_content_length,
                    },
                )
                .await?;
                let metric_contract = ObjectIoGetObjectDataPlaneMetricContract::disk(
                    rustfs_io_metrics::IoPath::Legacy,
                    rustfs_io_metrics::CopyMode::SingleCopy,
                );

                (body, metric_contract)
            }
            GetObjectBodySource::Chunk {
                stream: chunk_stream,
                path,
                copy_mode,
            } => {
                let (io_path, copy_mode) = object_io_chunk_body_data_plane_labels(path, copy_mode);
                match materialize_chunk_stream_before_commit(chunk_stream, response_content_length, optimal_buffer_size).await {
                    Ok(body) => (body, ObjectIoGetObjectDataPlaneMetricContract::disk(io_path, copy_mode)),
                    Err(err) => {
                        let path_label = object_io_get_object_chunk_path_label(path);
                        rustfs_io_metrics::record_io_fallback(
                            rustfs_io_metrics::IoStage::ReadSetup,
                            rustfs_io_metrics::FallbackReason::ProbeFailed,
                        );
                        rustfs_io_metrics::record_get_object_fast_path_probe_failed(
                            path_label,
                            copy_mode,
                            response_content_length,
                        );
                        warn!(
                            bucket = %request_context.bucket,
                            key = %request_context.key,
                            version_id = ?request_context.opts.version_id,
                            path = path_label,
                            copy_mode = copy_mode.as_str(),
                            promised_bytes = response_content_length,
                            materialized_bytes = err.streamed_bytes,
                            error = %err.source,
                            "GetObject chunk fast path full-body materialization failed before response commit"
                        );

                        let store = get_validated_store(&request_context.bucket).await?;
                        active_read_setup =
                            prepare_get_object_read(request_context, &store, manager, std::time::Instant::now()).await?;
                        continue;
                    }
                }
            }
        };

        let checksums = object_io_build_get_object_checksums(&info, &request_context.headers, part_number, rs.as_ref())
            .map_err(ApiError::from)?;
        let filtered_metadata = filter_object_metadata(&info.user_defined);

        return Ok((
            object_io_build_get_object_output_context(
                body,
                info,
                event_info,
                content_type,
                last_modified,
                response_content_length,
                content_range,
                server_side_encryption,
                sse_customer_algorithm,
                sse_customer_key_md5,
                ssekms_key_id,
                &checksums,
                filtered_metadata,
                versioned,
                optimal_buffer_size,
                Some(metric_contract.copy_mode),
            ),
            metric_contract,
        ));
    }
}

pub(super) async fn run_get_object_flow(
    request_context: GetObjectRequestContext,
    version_id_for_event: String,
    manager: &ConcurrencyManager,
    bootstrap: &GetObjectBootstrap,
    base_buffer_size: usize,
) -> S3Result<GetObjectFlowResult> {
    let timeout_config = &bootstrap.timeout_config;
    let wrapper = &bootstrap.wrapper;
    let request_start = bootstrap.request_start;

    let prepared_read = prepare_get_object_read_execution(&request_context, manager, wrapper, timeout_config).await?;
    let GetObjectPreparedRead { io_planning, read_setup } = prepared_read;

    let versioned = BucketVersioningSys::prefix_enabled(&request_context.bucket, &request_context.key).await;
    let (output_context, metric_contract) =
        build_get_object_output_context(&request_context, manager, read_setup, &io_planning, base_buffer_size, versioned).await?;
    let response_content_length = output_context.response_content_length;
    let optimal_buffer_size = output_context.optimal_buffer_size;

    let total_duration = request_start.elapsed();
    finalize_get_object_completion(
        &request_context,
        wrapper,
        timeout_config,
        total_duration,
        response_content_length,
        optimal_buffer_size,
        metric_contract,
    );

    Ok(object_io_build_cors_wrapped_get_object_flow_result(output_context, version_id_for_event))
}

#[cfg(test)]
mod tests {
    use super::get_object_strategy_range;
    use super::*;
    use futures_util::StreamExt;
    use http::HeaderMap;
    use rustfs_ecstore::store_api::ObjectOptions;
    use rustfs_io_core::IoChunk;

    fn sample_range(start: i64, end: i64) -> HTTPRangeSpec {
        HTTPRangeSpec {
            is_suffix_length: false,
            start,
            end,
        }
    }

    fn sample_request_context() -> GetObjectRequestContext {
        GetObjectRequestContext {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            part_number: None,
            rs: None,
            opts: ObjectOptions::default(),
            headers: HeaderMap::new(),
            sse_customer_key: None,
            sse_customer_key_md5: None,
        }
    }

    #[test]
    fn strategy_range_prefers_resolved_range_for_part_reads() {
        let request_context = sample_request_context();
        let resolved_range = sample_range(1024, 2047);

        let strategy_range = get_object_strategy_range(&request_context, Some(&resolved_range)).unwrap();

        assert_eq!(strategy_range.start, 1024);
        assert_eq!(strategy_range.end, 2047);
    }

    #[test]
    fn strategy_range_falls_back_to_raw_request_range() {
        let mut request_context = sample_request_context();
        request_context.rs = Some(sample_range(0, 511));

        let strategy_range = get_object_strategy_range(&request_context, None).unwrap();

        assert_eq!(strategy_range.start, 0);
        assert_eq!(strategy_range.end, 511);
    }

    #[tokio::test]
    async fn materialize_chunk_stream_before_commit_buffers_small_payload_in_memory() {
        let chunk_stream: BoxChunkStream = Box::pin(futures_util::stream::iter(vec![
            Ok(IoChunk::Shared(bytes::Bytes::from_static(b"hello"))),
            Ok(IoChunk::Shared(bytes::Bytes::from_static(b" world"))),
        ]));

        let mut body = materialize_chunk_stream_before_commit_with_threshold(chunk_stream, 11, 8 * 1024, 1024)
            .await
            .unwrap()
            .unwrap();

        let mut collected = Vec::new();
        while let Some(chunk) = body.next().await {
            collected.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(collected, b"hello world");
    }

    #[tokio::test]
    async fn materialize_chunk_stream_before_commit_falls_back_for_large_payload() {
        let chunk_stream: BoxChunkStream = Box::pin(futures_util::stream::iter(vec![
            Ok(IoChunk::Shared(bytes::Bytes::from_static(b"hello"))),
            Ok(IoChunk::Shared(bytes::Bytes::from_static(b" world"))),
        ]));

        // When payload exceeds the in-memory threshold an error is returned so the
        // caller can fall back to the legacy reader path rather than spooling to disk.
        let err = materialize_chunk_stream_before_commit_with_threshold(chunk_stream, 11, 8 * 1024, 4)
            .await
            .unwrap_err();

        assert_eq!(err.streamed_bytes, 0);
        assert_eq!(err.source.kind(), std::io::ErrorKind::Other);
    }

    #[tokio::test]
    async fn materialize_chunk_stream_before_commit_rejects_short_body() {
        let chunk_stream: BoxChunkStream =
            Box::pin(futures_util::stream::iter(vec![Ok(IoChunk::Shared(bytes::Bytes::from_static(b"hello")))]));

        let err = materialize_chunk_stream_before_commit_with_threshold(chunk_stream, 11, 8 * 1024, 1024)
            .await
            .unwrap_err();

        assert_eq!(err.streamed_bytes, 5);
        assert_eq!(err.source.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn materialize_chunk_stream_before_commit_rejects_long_body() {
        let chunk_stream: BoxChunkStream = Box::pin(futures_util::stream::iter(vec![
            Ok(IoChunk::Shared(bytes::Bytes::from_static(b"hello "))),
            Ok(IoChunk::Shared(bytes::Bytes::from_static(b"world!"))),
        ]));

        let err = materialize_chunk_stream_before_commit_with_threshold(chunk_stream, 11, 8 * 1024, 1024)
            .await
            .unwrap_err();

        assert_eq!(err.streamed_bytes, 12);
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn materialize_chunk_stream_before_commit_preserves_midstream_io_errors() {
        let chunk_stream: BoxChunkStream = Box::pin(futures_util::stream::iter(vec![
            Ok(IoChunk::Shared(bytes::Bytes::from_static(b"hello"))),
            Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer closed")),
        ]));

        let err = materialize_chunk_stream_before_commit_with_threshold(chunk_stream, 11, 8 * 1024, 1024)
            .await
            .unwrap_err();

        assert_eq!(err.streamed_bytes, 5);
        assert_eq!(err.source.kind(), std::io::ErrorKind::BrokenPipe);
    }
}
