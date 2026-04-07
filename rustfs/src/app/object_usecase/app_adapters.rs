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

use super::get_object_flow::GetObjectBootstrap;
use super::*;
use crate::app::context::NotifyInterface;
use crate::storage::concurrency::{self, ConcurrencyManager, get_buffer_size_opt_in};
use hashbrown::HashMap;
use rustfs_object_io::get::{
    GetObjectBodyPlan as ObjectIoGetObjectBodyPlan, GetObjectBodyPlanningInputs as ObjectIoGetObjectBodyPlanningInputs,
    GetObjectDataPlaneMetricContract as ObjectIoGetObjectDataPlaneMetricContract, GetObjectFlowResult,
    MaterializeGetObjectBodyError as ObjectIoMaterializeGetObjectBodyError,
    materialize_get_object_body as object_io_materialize_get_object_body, plan_get_object_body as object_io_plan_get_object_body,
    plan_get_object_strategy_layout as object_io_plan_get_object_strategy_layout,
};

pub(super) async fn prepare_get_object_request_context(req: &S3Request<GetObjectInput>) -> S3Result<GetObjectRequestContext> {
    let GetObjectInput {
        bucket,
        key,
        version_id,
        part_number,
        range,
        ..
    } = req.input.clone();

    validate_object_key(&key, "GET")?;

    let part_number = part_number.map(|v| v as usize);

    if let Some(part_num) = part_number
        && part_num == 0
    {
        return Err(s3_error!(InvalidArgument, "Invalid part number: part number must be greater than 0"));
    }

    let rs = range.map(|v| match v {
        Range::Int { first, last } => HTTPRangeSpec {
            is_suffix_length: false,
            start: first as i64,
            end: if let Some(last) = last { last as i64 } else { -1 },
        },
        Range::Suffix { length } => HTTPRangeSpec {
            is_suffix_length: true,
            start: length as i64,
            end: -1,
        },
    });

    if rs.is_some() && part_number.is_some() {
        return Err(s3_error!(InvalidArgument, "range and part_number invalid"));
    }

    let opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), part_number, &req.headers)
        .await
        .map_err(ApiError::from)?;

    Ok(GetObjectRequestContext {
        version_id_for_event: version_id.unwrap_or_default(),
        bucket,
        key,
        part_number,
        rs,
        opts,
        headers: req.headers.clone(),
        method: req.method.clone(),
        sse_customer_key: req.input.sse_customer_key.clone(),
        sse_customer_key_md5: req.input.sse_customer_key_md5.clone(),
    })
}

pub(super) fn init_get_object_bootstrap(bucket: &str, key: &str, request_id: &str) -> S3Result<GetObjectBootstrap> {
    let timeout_config = TimeoutConfig::from_env();
    let wrapper = RequestTimeoutWrapper::with_request_id(timeout_config.clone(), request_id.to_string());
    let request_start = std::time::Instant::now();
    let request_guard = ConcurrencyManager::track_request();
    let concurrent_requests = GetObjectGuard::concurrent_requests();

    let deadlock_detector = deadlock_detector::get_deadlock_detector();
    deadlock_detector.register_request(request_id, format!("GetObject {bucket}/{key}"));
    let deadlock_request_guard = DeadlockRequestGuard::new(deadlock_detector, request_id.to_string());

    if wrapper.is_timeout() {
        warn!(
            bucket = %bucket,
            key = %key,
            timeout_secs = timeout_config.get_object_timeout.as_secs(),
            elapsed_ms = wrapper.elapsed().as_millis(),
            "GetObject request timed out before processing"
        );
        return Err(s3_error!(InternalError, "Request timeout before processing"));
    }

    rustfs_io_metrics::record_get_object_request_start(concurrent_requests);

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
    })
}

pub(super) async fn build_get_object_body_adapter<R>(
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

pub(super) struct GetObjectCompletionInputs<'a> {
    pub(super) bucket: &'a str,
    pub(super) key: &'a str,
    pub(super) wrapper: &'a RequestTimeoutWrapper,
    pub(super) timeout_config: &'a TimeoutConfig,
    pub(super) total_duration: Duration,
    pub(super) response_content_length: i64,
    pub(super) optimal_buffer_size: usize,
    pub(super) metric_contract: ObjectIoGetObjectDataPlaneMetricContract,
}

pub(super) struct GetObjectStrategyRuntimeInputs<'a> {
    pub(super) base_buffer_size: usize,
    pub(super) manager: &'a ConcurrencyManager,
    pub(super) bucket: &'a str,
    pub(super) key: &'a str,
    pub(super) info: &'a ObjectInfo,
    pub(super) rs: Option<&'a HTTPRangeSpec>,
    pub(super) response_content_length: i64,
    pub(super) permit_wait_duration: Duration,
    pub(super) queue_utilization: f64,
    pub(super) queue_status: &'a concurrency::IoQueueStatus,
}

pub(super) fn finalize_get_object_completion(inputs: GetObjectCompletionInputs<'_>) {
    let GetObjectCompletionInputs {
        bucket,
        key,
        wrapper,
        timeout_config,
        total_duration,
        response_content_length,
        optimal_buffer_size,
        metric_contract,
    } = inputs;

    rustfs_io_metrics::record_get_object_completion(total_duration.as_secs_f64(), response_content_length, optimal_buffer_size);

    rustfs_io_metrics::record_get_object(total_duration.as_millis() as f64, response_content_length);
    rustfs_io_metrics::record_io_copy_mode("get", metric_contract.copy_mode, response_content_length.max(0) as usize);

    if wrapper.is_timeout() {
        warn!(
            bucket = %bucket,
            key = %key,
            elapsed = ?wrapper.elapsed(),
            timeout = ?timeout_config.get_object_timeout,
            "GetObject request exceeded timeout"
        );
        rustfs_io_metrics::record_get_object_timeout(None, Some(wrapper.elapsed().as_secs_f64()));
    }

    debug!(
        bucket = %bucket,
        key = %key,
        size = response_content_length,
        duration = ?total_duration,
        buffer = optimal_buffer_size,
        "GetObject completed"
    );
}

pub(super) fn finalize_get_object_strategy_runtime(inputs: GetObjectStrategyRuntimeInputs<'_>) -> usize {
    let GetObjectStrategyRuntimeInputs {
        base_buffer_size,
        manager,
        bucket,
        key,
        info,
        rs,
        response_content_length,
        permit_wait_duration,
        queue_utilization,
        queue_status,
    } = inputs;

    let strategy_layout = object_io_plan_get_object_strategy_layout(
        rs,
        response_content_length,
        0,
        get_buffer_size_opt_in(response_content_length),
    );

    if let Some(range_spec) = rs
        && range_spec.start >= 0
    {
        manager.record_access(range_spec.start as u64, response_content_length as u64);
    }

    if response_content_length > 0 {
        manager.record_transfer(response_content_length as u64, permit_wait_duration);
    }

    let io_strategy = manager.calculate_io_strategy_with_context(
        info.size,
        base_buffer_size,
        permit_wait_duration,
        strategy_layout.is_sequential_hint,
    );

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
        is_sequential = strategy_layout.is_sequential_hint,
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

        rustfs_io_metrics::record_io_priority_assignment(io_priority.as_str());
    }

    rustfs_io_metrics::record_get_object_io_state(
        permit_wait_duration.as_secs_f64(),
        queue_utilization,
        queue_status.permits_in_use,
        queue_status.total_permits.saturating_sub(queue_status.permits_in_use),
        io_strategy.load_level.as_str(),
        io_strategy.buffer_multiplier,
    );

    let strategy_layout = object_io_plan_get_object_strategy_layout(
        rs,
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

pub(super) fn prepare_put_object_request_context(req: &S3Request<PutObjectInput>) -> PutObjectRequestContext {
    PutObjectRequestContext {
        headers: req.headers.clone(),
        trailing_headers: req.trailing_headers.clone(),
        uri_query: req.uri.query().map(str::to_string),
        is_post_object: req.extensions.get::<PostObjectRequestMarker>().is_some(),
        method: req.method.clone(),
        uri: req.uri.clone(),
        extensions: req.extensions.clone(),
        credentials: req.credentials.clone(),
        region: req.region.clone(),
        service: req.service.clone(),
    }
}

pub(super) fn put_object_execution_context(req: &S3Request<PutObjectInput>) -> (EventName, QuotaOperation, &'static str) {
    if req.extensions.get::<PostObjectRequestMarker>().is_some() {
        (EventName::ObjectCreatedPost, QuotaOperation::PostObject, "POST")
    } else {
        (EventName::ObjectCreatedPut, QuotaOperation::PutObject, "PUT")
    }
}

pub(super) fn new_operation_helper<T: Send + Sync>(
    req: &S3Request<T>,
    event_name: EventName,
    operation: S3Operation,
    suppress_event: bool,
) -> OperationHelper {
    let helper = OperationHelper::new(req, event_name, operation);
    if suppress_event { helper.suppress_event() } else { helper }
}

pub(super) fn bind_helper_object(
    helper: OperationHelper,
    object_info: ObjectInfo,
    version_id: Option<String>,
) -> OperationHelper {
    let helper = helper.object(object_info);
    if let Some(version_id) = version_id {
        helper.version_id(version_id)
    } else {
        helper
    }
}

pub(super) async fn complete_get_flow_result(
    helper: OperationHelper,
    request_context: &GetObjectRequestContext,
    flow_result: GetObjectFlowResult,
) -> S3Result<S3Response<GetObjectOutput>> {
    let helper = helper
        .object(flow_result.event_info)
        .version_id(flow_result.version_id_for_event);
    let response = wrap_response_with_cors(
        &request_context.bucket,
        &request_context.method,
        &request_context.headers,
        flow_result.output,
    )
    .await;
    let result = Ok(response);
    let _ = helper.complete(&result);
    result
}

pub(super) fn complete_put_response(helper: OperationHelper, output: PutObjectOutput) -> S3Result<S3Response<PutObjectOutput>> {
    let result = Ok(S3Response::new(output));
    let _ = helper.complete(&result);
    result
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_put_extract_notification(
    notify: Arc<dyn NotifyInterface>,
    request_context: Option<crate::storage::request_context::RequestContext>,
    bucket: String,
    req_params: HashMap<String, String>,
    version_id: String,
    host: String,
    port: u16,
    user_agent: String,
    obj_info: ObjectInfo,
    output: PutObjectOutput,
) {
    let event_args = rustfs_notify::EventArgs {
        event_name: EventName::ObjectCreatedPut,
        bucket_name: bucket,
        object: obj_info,
        req_params,
        resp_elements: extract_resp_elements(&S3Response::new(output)),
        version_id,
        host,
        port,
        user_agent,
    };

    crate::storage::helper::spawn_background_with_context(request_context, async move {
        notify.notify(event_args).await;
    });
}

pub(super) async fn get_validated_store_adapter(bucket: &str) -> S3Result<Arc<rustfs_ecstore::store::ECStore>> {
    get_validated_store(bucket).await
}

pub(super) async fn bucket_prefix_versioning_enabled(bucket: &str, key: &str) -> bool {
    BucketVersioningSys::prefix_enabled(bucket, key).await
}

pub(super) async fn authorize_extract_put_target(
    request_context: &PutObjectRequestContext,
    bucket: &str,
    object: &str,
) -> S3Result<()> {
    let mut auth_req = S3Request {
        input: PutObjectInput::default(),
        method: request_context.method.clone(),
        uri: request_context.uri.clone(),
        headers: request_context.headers.clone(),
        extensions: request_context.extensions.clone(),
        credentials: request_context.credentials.clone(),
        region: request_context.region.clone(),
        service: request_context.service.clone(),
        trailing_headers: request_context.trailing_headers.clone(),
    };
    {
        let req_info = req_info_mut(&mut auth_req)?;
        req_info.bucket = Some(bucket.to_string());
        req_info.object = Some(object.to_string());
        req_info.version_id = None;
    }
    authorize_request(&mut auth_req, Action::S3Action(S3Action::PutObjectAction)).await
}
