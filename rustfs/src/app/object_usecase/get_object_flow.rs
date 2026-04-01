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
use super::app_adapters::{
    bucket_prefix_versioning_enabled, build_get_object_body_adapter, finalize_get_object_completion,
    finalize_get_object_strategy_runtime, maybe_get_cached_get_object_flow_result,
};
use super::get_object_zero_copy::{GetObjectPreparedRead, prepare_get_object_read_execution};
use super::types::GetObjectRequestContext;
use crate::error::ApiError;
use crate::storage::concurrency::{self, ConcurrencyManager, GetObjectGuard};
use crate::storage::options::filter_object_metadata;
use crate::storage::timeout_wrapper::{RequestTimeoutWrapper, TimeoutConfig};
use rustfs_ecstore::store_api::{HTTPRangeSpec, ObjectInfo};
use rustfs_object_io::get::{
    GetObjectBodySource, GetObjectFlowResult, GetObjectOutputContext, GetObjectReadSetup,
    build_chunk_blob as object_io_build_chunk_blob,
    build_cors_wrapped_get_object_flow_result as object_io_build_cors_wrapped_get_object_flow_result,
    build_get_object_checksums as object_io_build_get_object_checksums,
    build_get_object_output_context as object_io_build_get_object_output_context,
    chunk_body_data_plane_labels as object_io_chunk_body_data_plane_labels,
};
use s3s::S3Result;
use s3s::dto::{ContentType, SSECustomerAlgorithm, SSECustomerKeyMD5, SSEKMSKeyId, ServerSideEncryption, Timestamp};
use std::time::Duration;

pub(super) struct GetObjectBootstrap {
    pub(super) timeout_config: TimeoutConfig,
    pub(super) wrapper: RequestTimeoutWrapper,
    pub(super) request_start: std::time::Instant,
    pub(super) request_guard: GetObjectGuard,
    pub(super) _deadlock_request_guard: DeadlockRequestGuard,
    pub(super) concurrent_requests: usize,
}

#[derive(Clone, Copy)]
pub(super) struct GetObjectFlowRuntime<'a> {
    pub(super) manager: &'a ConcurrencyManager,
    pub(super) bootstrap: &'a GetObjectBootstrap,
    pub(super) base_buffer_size: usize,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn build_get_object_output_context(
    request_context: &GetObjectRequestContext,
    cache_key: &str,
    manager: &ConcurrencyManager,
    bucket: &str,
    key: &str,
    info: ObjectInfo,
    event_info: ObjectInfo,
    body_source: GetObjectBodySource,
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
    permit_wait_duration: Duration,
    queue_utilization: f64,
    queue_status: &concurrency::IoQueueStatus,
    concurrent_requests: usize,
    base_buffer_size: usize,
    part_number: Option<usize>,
    versioned: bool,
) -> S3Result<GetObjectOutputContext> {
    let (io_strategy, optimal_buffer_size) = finalize_get_object_strategy_runtime(
        base_buffer_size,
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

    let (body, copy_mode_override) = match body_source {
        GetObjectBodySource::Reader(final_stream) => {
            let cache_eligibility = manager.get_object_cache_eligibility(
                io_strategy.cache_writeback_enabled,
                part_number.is_some(),
                rs.is_some(),
                encryption_applied,
                response_content_length,
            );
            (
                build_get_object_body_adapter(
                    final_stream,
                    &info,
                    cache_key,
                    response_content_length,
                    optimal_buffer_size,
                    cache_eligibility,
                )
                .await?,
                None,
            )
        }
        GetObjectBodySource::Chunk {
            stream: chunk_stream,
            path,
            copy_mode,
        } => (
            object_io_build_chunk_blob(chunk_stream),
            Some(object_io_chunk_body_data_plane_labels(path, copy_mode).1),
        ),
    };

    let checksums = object_io_build_get_object_checksums(&info, &request_context.headers, part_number, rs.as_ref())
        .map_err(ApiError::from)?;
    let filtered_metadata = filter_object_metadata(&info.user_defined);

    Ok(object_io_build_get_object_output_context(
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
        copy_mode_override,
    ))
}

pub(super) async fn run_get_object_flow(
    request_context: GetObjectRequestContext,
    runtime: GetObjectFlowRuntime<'_>,
) -> S3Result<GetObjectFlowResult> {
    let GetObjectFlowRuntime {
        manager,
        bootstrap,
        base_buffer_size,
    } = runtime;
    let timeout_config = &bootstrap.timeout_config;
    let wrapper = &bootstrap.wrapper;
    let request_start = bootstrap.request_start;
    let concurrent_requests = bootstrap.concurrent_requests;
    let bucket = request_context.bucket.clone();
    let key = request_context.key.clone();
    let cache_key = request_context.cache_key.clone();
    let version_id_for_event = request_context.version_id_for_event.clone();
    let part_number = request_context.part_number;
    let rs = request_context.rs.clone();
    let opts = request_context.opts.clone();

    if let Some(cached_result) = maybe_get_cached_get_object_flow_result(
        manager,
        &bucket,
        &key,
        &cache_key,
        version_id_for_event.clone(),
        part_number,
        rs.as_ref(),
        request_start,
    )
    .await
    {
        return Ok(cached_result);
    }

    let prepared_read = prepare_get_object_read_execution(
        &request_context,
        manager,
        wrapper,
        timeout_config,
        &bucket,
        &key,
        rs,
        &opts,
        part_number,
    )
    .await?;
    let GetObjectPreparedRead { io_planning, read_setup } = prepared_read;
    let permit_wait_duration = io_planning.permit_wait_duration;
    let queue_status = io_planning.queue_status;
    let queue_utilization = io_planning.queue_utilization;

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
    } = read_setup;

    let versioned = bucket_prefix_versioning_enabled(&bucket, &key).await;
    let output_context = build_get_object_output_context(
        &request_context,
        &cache_key,
        manager,
        &bucket,
        &key,
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
        permit_wait_duration,
        queue_utilization,
        &queue_status,
        concurrent_requests,
        base_buffer_size,
        part_number,
        versioned,
    )
    .await?;
    let response_content_length = output_context.response_content_length;
    let optimal_buffer_size = output_context.optimal_buffer_size;
    let copy_mode_override = output_context.copy_mode_override;

    let total_duration = request_start.elapsed();
    finalize_get_object_completion(
        &cache_key,
        wrapper,
        timeout_config,
        total_duration,
        response_content_length,
        optimal_buffer_size,
        copy_mode_override,
    );

    Ok(object_io_build_cors_wrapped_get_object_flow_result(output_context, version_id_for_event))
}
