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

use super::GetObjectRequestContext;
use crate::error::ApiError;
use crate::storage::concurrency::{self, ConcurrencyManager};
use crate::storage::timeout_wrapper::{RequestTimeoutWrapper, TimeoutConfig};
use crate::storage::{
    DecryptionRequest, check_preconditions, get_validated_store, sse_decryption, validate_sse_headers_for_read,
    validate_ssec_for_read,
};
use futures_util::{StreamExt, stream};
use http::HeaderMap;
use rustfs_concurrency::GetObjectQueueSnapshot;
use rustfs_ecstore::store_api::{ObjectIO, ObjectOperations};
use rustfs_io_core::{BoxChunkStream, IoChunk};
use rustfs_object_io::get::{
    ChunkReadDecision, ChunkReadPlanError, GetObjectEncryptionState as ObjectIoGetObjectEncryptionState, GetObjectReadSetup,
    build_reader_read_setup as object_io_build_reader_read_setup,
    finalize_chunk_read_setup as object_io_finalize_chunk_read_setup,
    get_object_chunk_fast_path_guard as object_io_get_object_chunk_fast_path_guard,
    get_object_chunk_path_label as object_io_get_object_chunk_path_label, map_chunk_copy_mode as object_io_map_chunk_copy_mode,
    plan_chunk_read as object_io_plan_chunk_read, plan_legacy_read as object_io_plan_legacy_read,
};
use rustfs_rio::{Reader, WarpReader};
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};
use std::time::Duration;
use tracing::{debug, warn};

fn get_object_chunk_fast_path_enabled() -> bool {
    rustfs_utils::get_env_bool(
        rustfs_config::ENV_OBJECT_GET_CHUNK_FAST_PATH_ENABLE,
        rustfs_config::DEFAULT_OBJECT_GET_CHUNK_FAST_PATH_ENABLE,
    )
}

async fn probe_chunk_stream_before_commit(
    mut chunk_stream: BoxChunkStream,
    response_content_length: i64,
) -> Result<BoxChunkStream, rustfs_io_metrics::FallbackReason> {
    if response_content_length <= 0 {
        return Ok(chunk_stream);
    }

    let mut prefetched = Vec::new();

    loop {
        match chunk_stream.next().await {
            Some(Ok(chunk)) => {
                let chunk_len = chunk.len();
                prefetched.push(chunk);
                if chunk_len > 0 {
                    let prefix = stream::iter(prefetched.into_iter().map(Ok::<IoChunk, std::io::Error>));
                    return Ok(Box::pin(prefix.chain(chunk_stream)));
                }
            }
            Some(Err(_)) | None => return Err(rustfs_io_metrics::FallbackReason::ProbeFailed),
        }
    }
}

pub(super) struct GetObjectIoPlanning<'a> {
    pub(super) _disk_permit: tokio::sync::SemaphorePermit<'a>,
    pub(super) permit_wait_duration: Duration,
    pub(super) queue_status: concurrency::IoQueueStatus,
    pub(super) queue_utilization: f64,
}

pub(super) struct GetObjectPreparedRead<'a> {
    pub(super) io_planning: GetObjectIoPlanning<'a>,
    pub(super) read_setup: GetObjectReadSetup,
}

pub(super) async fn acquire_get_object_io_planning<'a>(
    manager: &'a ConcurrencyManager,
    wrapper: &RequestTimeoutWrapper,
    timeout_config: &TimeoutConfig,
    bucket: &str,
    key: &str,
) -> S3Result<GetObjectIoPlanning<'a>> {
    let permit_wait_start = std::time::Instant::now();
    let disk_permit = manager
        .acquire_disk_read_permit()
        .await
        .map_err(|_| s3_error!(InternalError, "disk read semaphore closed"))?;
    let permit_wait_duration = permit_wait_start.elapsed();

    if wrapper.is_timeout() {
        warn!(
            bucket = %bucket,
            key = %key,
            wait_ms = permit_wait_duration.as_millis(),
            timeout_secs = timeout_config.get_object_timeout.as_secs(),
            elapsed_ms = wrapper.elapsed().as_millis(),
            "GetObject request timed out while waiting for disk permit"
        );

        rustfs_io_metrics::record_get_object_timeout(Some("disk_permit"), Some(wrapper.elapsed().as_secs_f64()));
        return Err(s3_error!(InternalError, "Request timeout while waiting for disk permit"));
    }

    let queue_status = manager.io_queue_status();
    let queue_snapshot = GetObjectQueueSnapshot::from_available_permits(
        queue_status.total_permits,
        queue_status.total_permits.saturating_sub(queue_status.permits_in_use),
    );
    let queue_utilization = queue_snapshot.utilization_percent();

    if queue_snapshot.is_congested(80.0) {
        warn!(
            bucket = %bucket,
            key = %key,
            queue_utilization = format!("{:.1}%", queue_utilization),
            permits_in_use = queue_status.permits_in_use,
            total_permits = queue_status.total_permits,
            "I/O queue congestion detected"
        );

        rustfs_io_metrics::record_io_queue_congestion();
    }

    if wrapper.is_timeout() {
        warn!(
            bucket = %bucket,
            key = %key,
            timeout_secs = timeout_config.get_object_timeout.as_secs(),
            elapsed_ms = wrapper.elapsed().as_millis(),
            "GetObject request timed out before reading object"
        );
        rustfs_io_metrics::record_get_object_timeout(Some("before_read"), Some(wrapper.elapsed().as_secs_f64()));
        return Err(s3_error!(InternalError, "Request timeout before reading object"));
    }

    Ok(GetObjectIoPlanning {
        _disk_permit: disk_permit,
        permit_wait_duration,
        queue_status,
        queue_utilization,
    })
}

pub(super) async fn prepare_get_object_read(
    request_context: &GetObjectRequestContext,
    store: &rustfs_ecstore::store::ECStore,
    manager: &ConcurrencyManager,
    read_start: std::time::Instant,
) -> S3Result<GetObjectReadSetup> {
    let reader = store
        .get_object_reader(
            &request_context.bucket,
            &request_context.key,
            request_context.rs.clone(),
            HeaderMap::new(),
            &request_context.opts,
        )
        .await
        .map_err(ApiError::from)?;

    let info = reader.object_info;

    let read_duration = read_start.elapsed();
    rustfs_io_metrics::record_io_path_selected("get", rustfs_io_metrics::IoPath::Legacy);

    manager.record_disk_operation(info.size as u64, read_duration, true).await;

    check_preconditions(&request_context.headers, &info)?;

    debug!(object_size = info.size, part_count = info.parts.len(), "GET object metadata snapshot");
    for part in &info.parts {
        debug!(
            part_number = part.number,
            part_size = part.size,
            part_actual_size = part.actual_size,
            "GET object part details"
        );
    }

    let event_info = info.clone();
    validate_sse_headers_for_read(&info.user_defined, &request_context.headers)?;
    validate_ssec_for_read(
        &info.user_defined,
        request_context.sse_customer_key.as_ref(),
        request_context.sse_customer_key_md5.as_ref(),
    )?;
    let read_plan =
        object_io_plan_legacy_read(&info, request_context.rs.clone(), request_context.part_number).map_err(ApiError::from)?;

    debug!(
        "GET object metadata check: parts={}, provided_sse_key={:?}",
        info.parts.len(),
        request_context.sse_customer_key.is_some()
    );

    let decryption_request = DecryptionRequest {
        bucket: &request_context.bucket,
        key: &request_context.key,
        metadata: &info.user_defined,
        sse_customer_key: request_context.sse_customer_key.as_ref(),
        sse_customer_key_md5: request_context.sse_customer_key_md5.as_ref(),
        part_number: None,
        parts: &info.parts,
        etag: info.etag.as_deref(),
    };

    let encrypted_stream = reader.stream;

    let (encryption_state, final_stream) = match sse_decryption(decryption_request).await? {
        Some(material) => {
            let server_side_encryption = Some(material.server_side_encryption.clone());
            let sse_customer_algorithm = Some(material.algorithm.clone());
            let sse_customer_key_md5 = material.customer_key_md5.clone();
            let ssekms_key_id = material.kms_key_id.clone();
            let (decrypted_stream, plaintext_size) = material
                .wrap_reader(encrypted_stream, read_plan.response_content_length)
                .await
                .map_err(ApiError::from)?;

            (
                ObjectIoGetObjectEncryptionState {
                    server_side_encryption,
                    sse_customer_algorithm,
                    sse_customer_key_md5,
                    ssekms_key_id,
                    encryption_applied: true,
                    response_content_length_override: Some(plaintext_size),
                },
                decrypted_stream,
            )
        }
        None => (
            ObjectIoGetObjectEncryptionState::default(),
            Box::new(WarpReader::new(encrypted_stream)) as Box<dyn Reader>,
        ),
    };

    Ok(object_io_build_reader_read_setup(
        info,
        event_info,
        final_stream,
        read_plan,
        encryption_state,
    ))
}

pub(super) async fn prepare_get_object_read_execution<'a>(
    request_context: &GetObjectRequestContext,
    manager: &'a ConcurrencyManager,
    wrapper: &RequestTimeoutWrapper,
    timeout_config: &TimeoutConfig,
) -> S3Result<GetObjectPreparedRead<'a>> {
    let io_planning =
        acquire_get_object_io_planning(manager, wrapper, timeout_config, &request_context.bucket, &request_context.key).await?;
    let store = get_validated_store(&request_context.bucket).await?;

    let read_start = std::time::Instant::now();
    let read_setup = if !get_object_chunk_fast_path_enabled() {
        rustfs_io_metrics::record_io_fallback(
            rustfs_io_metrics::IoStage::ReadSetup,
            rustfs_io_metrics::FallbackReason::FeatureDisabled,
        );
        prepare_get_object_read(request_context, &store, manager, read_start).await?
    } else {
        match object_io_get_object_chunk_fast_path_guard(
            request_context.sse_customer_key.is_some(),
            request_context.sse_customer_key_md5.is_some(),
        ) {
            Ok(()) => match prepare_get_object_chunk_read(request_context, &store, manager, read_start).await? {
                Some(read_setup) => read_setup,
                None => prepare_get_object_read(request_context, &store, manager, read_start).await?,
            },
            Err(fallback) => {
                rustfs_io_metrics::record_io_fallback(fallback.stage, fallback.reason);
                prepare_get_object_read(request_context, &store, manager, read_start).await?
            }
        }
    };

    Ok(GetObjectPreparedRead { io_planning, read_setup })
}

pub(super) async fn prepare_get_object_chunk_read(
    request_context: &GetObjectRequestContext,
    store: &rustfs_ecstore::store::ECStore,
    manager: &ConcurrencyManager,
    read_start: std::time::Instant,
) -> S3Result<Option<GetObjectReadSetup>> {
    let info = store
        .get_object_info(&request_context.bucket, &request_context.key, &request_context.opts)
        .await
        .map_err(ApiError::from)?;

    validate_sse_headers_for_read(&info.user_defined, &request_context.headers)?;
    validate_ssec_for_read(
        &info.user_defined,
        request_context.sse_customer_key.as_ref(),
        request_context.sse_customer_key_md5.as_ref(),
    )?;
    check_preconditions(&request_context.headers, &info)?;

    let encrypted_object = info.user_defined.contains_key("x-rustfs-encryption-key")
        || info
            .user_defined
            .contains_key("x-amz-server-side-encryption-customer-algorithm");
    if encrypted_object {
        rustfs_io_metrics::record_io_fallback(
            rustfs_io_metrics::IoStage::ReadSetup,
            rustfs_io_metrics::FallbackReason::EncryptionEnabled,
        );
        return Ok(None);
    }

    let plan = match object_io_plan_chunk_read(
        &info,
        request_context.opts.version_id.is_none(),
        request_context.rs.clone(),
        request_context.part_number,
    ) {
        Ok(ChunkReadDecision::Eligible(plan)) => plan,
        Ok(ChunkReadDecision::Fallback(fallback)) => {
            rustfs_io_metrics::record_io_fallback(fallback.stage, fallback.reason);
            return Ok(None);
        }
        Err(ChunkReadPlanError::NoSuchKey) => return Err(S3Error::new(S3ErrorCode::NoSuchKey)),
        Err(ChunkReadPlanError::MethodNotAllowed) => return Err(S3Error::new(S3ErrorCode::MethodNotAllowed)),
        Err(ChunkReadPlanError::Io(err)) => return Err(ApiError::from(err).into()),
    };
    let rs = plan.rs.clone();
    let response_content_length = plan.response_content_length;

    let read_duration = read_start.elapsed();
    manager.record_disk_operation(info.size as u64, read_duration, true).await;
    let event_info = info.clone();

    let chunk_result = match store
        .get_object_chunks(
            &request_context.bucket,
            &request_context.key,
            rs.clone(),
            HeaderMap::new(),
            &request_context.opts,
        )
        .await
        .map_err(ApiError::from)
    {
        Ok(result) => result,
        Err(_err) => {
            rustfs_io_metrics::record_io_fallback(
                rustfs_io_metrics::IoStage::HttpBridge,
                rustfs_io_metrics::FallbackReason::ChunkBridgeUnavailable,
            );
            return Ok(None);
        }
    };
    let path_label = object_io_get_object_chunk_path_label(chunk_result.path);
    let copy_mode = object_io_map_chunk_copy_mode(chunk_result.copy_mode);
    let chunk_result = match probe_chunk_stream_before_commit(chunk_result.stream, response_content_length).await {
        Ok(stream) => rustfs_ecstore::store_api::GetObjectChunkResult {
            stream,
            path: chunk_result.path,
            copy_mode: chunk_result.copy_mode,
        },
        Err(reason) => {
            rustfs_io_metrics::record_io_fallback(rustfs_io_metrics::IoStage::ReadSetup, reason);
            rustfs_io_metrics::record_get_object_fast_path_probe_failed(path_label, copy_mode, response_content_length);
            warn!(
                bucket = %request_context.bucket,
                key = %request_context.key,
                version_id = ?request_context.opts.version_id,
                path = path_label,
                copy_mode = copy_mode.as_str(),
                promised_bytes = response_content_length,
                fallback_reason = reason.as_str(),
                "GetObject chunk fast path probe failed before response commit"
            );
            return Ok(None);
        }
    };

    let setup_result = object_io_finalize_chunk_read_setup(info, event_info, chunk_result, plan);
    rustfs_io_metrics::record_get_object_fast_path_selected(path_label, copy_mode, response_content_length);
    rustfs_io_metrics::record_io_path_selected("get", setup_result.io_path);

    Ok(Some(setup_result.read_setup))
}

#[cfg(test)]
mod tests {
    use super::{get_object_chunk_fast_path_enabled, probe_chunk_stream_before_commit};
    use bytes::Bytes;
    use futures_util::{StreamExt, stream};
    use rustfs_io_core::{BoxChunkStream, IoChunk};

    #[test]
    fn get_object_chunk_fast_path_defaults_to_disabled() {
        temp_env::with_var_unset(rustfs_config::ENV_OBJECT_GET_CHUNK_FAST_PATH_ENABLE, || {
            assert!(!get_object_chunk_fast_path_enabled());
        });
    }

    #[test]
    fn get_object_chunk_fast_path_can_be_explicitly_enabled() {
        temp_env::with_var(rustfs_config::ENV_OBJECT_GET_CHUNK_FAST_PATH_ENABLE, Some("true"), || {
            assert!(get_object_chunk_fast_path_enabled());
        });
    }

    #[tokio::test]
    async fn probe_chunk_stream_before_commit_preserves_prefetched_payload() {
        let stream: BoxChunkStream = Box::pin(stream::iter(vec![
            Ok(IoChunk::Shared(Bytes::from_static(b"hello "))),
            Ok(IoChunk::Shared(Bytes::from_static(b"world"))),
        ]));

        let mut probed = probe_chunk_stream_before_commit(stream, 11).await.unwrap();
        let mut collected = Vec::new();
        while let Some(chunk) = probed.next().await {
            collected.extend_from_slice(chunk.unwrap().as_bytes().as_ref());
        }

        assert_eq!(collected, b"hello world");
    }

    #[tokio::test]
    async fn probe_chunk_stream_before_commit_rejects_midstream_failure_before_first_chunk() {
        let stream: BoxChunkStream = Box::pin(stream::iter(vec![Err(std::io::Error::other("probe failed"))]));

        let err = match probe_chunk_stream_before_commit(stream, 1).await {
            Ok(_) => panic!("expected probe failure"),
            Err(err) => err,
        };
        assert_eq!(err, rustfs_io_metrics::FallbackReason::ProbeFailed);
    }

    #[tokio::test]
    async fn probe_chunk_stream_before_commit_rejects_unexpected_empty_stream() {
        let stream: BoxChunkStream = Box::pin(stream::empty());

        let err = match probe_chunk_stream_before_commit(stream, 1).await {
            Ok(_) => panic!("expected probe failure"),
            Err(err) => err,
        };
        assert_eq!(err, rustfs_io_metrics::FallbackReason::ProbeFailed);
    }
}
