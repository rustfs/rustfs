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

use super::types::GetObjectRequestContext;
use crate::error::ApiError;
use crate::storage::concurrency::{self, ConcurrencyManager};
use crate::storage::timeout_wrapper::{RequestTimeoutWrapper, TimeoutConfig};
use crate::storage::{
    DecryptionRequest, check_preconditions, get_validated_store, sse_decryption, validate_sse_headers_for_read,
    validate_ssec_for_read,
};
use http::HeaderMap;
use rustfs_concurrency::GetObjectQueueSnapshot;
use rustfs_ecstore::store_api::{ObjectIO, ObjectOperations};
use rustfs_object_io::get::{
    ChunkReadDecision, ChunkReadPlanError, GetObjectEncryptionState as ObjectIoGetObjectEncryptionState, GetObjectReadSetup,
    build_reader_read_setup as object_io_build_reader_read_setup,
    finalize_chunk_read_setup as object_io_finalize_chunk_read_setup,
    get_object_chunk_fast_path_guard as object_io_get_object_chunk_fast_path_guard, plan_chunk_read as object_io_plan_chunk_read,
    plan_legacy_read as object_io_plan_legacy_read,
};
use rustfs_rio::{Reader, WarpReader};
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};
use std::time::Duration;
use tracing::{debug, warn};

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
    let read_setup = match object_io_get_object_chunk_fast_path_guard(
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
    let setup_result = object_io_finalize_chunk_read_setup(info, event_info, chunk_result, plan);
    rustfs_io_metrics::record_io_path_selected("get", setup_result.io_path);

    Ok(Some(setup_result.read_setup))
}
