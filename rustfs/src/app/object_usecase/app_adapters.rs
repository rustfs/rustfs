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
use crate::storage::concurrency::{self, get_buffer_size_opt_in};
use hashbrown::HashMap;
use rustfs_object_io::get::{
    CachedGetObjectSource as ObjectIoCachedGetObjectSource, GetObjectBodyPlan as ObjectIoGetObjectBodyPlan,
    GetObjectCacheWriteback, GetObjectDataPlaneMetricContract as ObjectIoGetObjectDataPlaneMetricContract, GetObjectFlowResult,
    GetObjectResponseMode, MaterializeGetObjectBodyError as ObjectIoMaterializeGetObjectBodyError,
    build_cached_get_object_flow_result_from_source as object_io_build_cached_get_object_flow_result_from_source,
    finalize_get_object_cache_writeback as object_io_finalize_get_object_cache_writeback,
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
        cache_key: ConcurrencyManager::make_cache_key(&bucket, &key, version_id.as_deref()),
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

impl ObjectIoCachedGetObjectSource for CachedGetObject {
    fn body(&self) -> &std::sync::Arc<bytes::Bytes> {
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

    fn user_metadata(&self) -> &std::collections::HashMap<String, String> {
        &self.user_metadata
    }

    fn checksum_crc32(&self) -> Option<&str> {
        self.checksum_crc32.as_deref()
    }

    fn checksum_crc32c(&self) -> Option<&str> {
        self.checksum_crc32c.as_deref()
    }

    fn checksum_sha1(&self) -> Option<&str> {
        self.checksum_sha1.as_deref()
    }

    fn checksum_sha256(&self) -> Option<&str> {
        self.checksum_sha256.as_deref()
    }

    fn checksum_crc64nvme(&self) -> Option<&str> {
        self.checksum_crc64nvme.as_deref()
    }

    fn checksum_type(&self) -> Option<&ChecksumType> {
        self.checksum_type.as_ref()
    }
}

pub(super) fn init_get_object_bootstrap(bucket: &str, key: &str) -> S3Result<GetObjectBootstrap> {
    let timeout_config = TimeoutConfig::from_env();
    let wrapper = RequestTimeoutWrapper::with_request_id(timeout_config.clone(), format!("get-{bucket}-{key}"));
    let request_start = std::time::Instant::now();
    let request_guard = ConcurrencyManager::track_request();
    let concurrent_requests = GetObjectGuard::concurrent_requests();

    let deadlock_detector = deadlock_detector::get_deadlock_detector();
    let request_id = wrapper.request_id().to_string();
    deadlock_detector.register_request(&request_id, format!("GetObject {bucket}/{key}"));
    let deadlock_request_guard = DeadlockRequestGuard::new(deadlock_detector, request_id);

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
        concurrent_requests,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn maybe_get_cached_get_object_flow_result(
    manager: &ConcurrencyManager,
    bucket: &str,
    key: &str,
    cache_key: &str,
    version_id_for_event: String,
    part_number: Option<usize>,
    rs: Option<&HTTPRangeSpec>,
    request_start: std::time::Instant,
) -> Option<GetObjectFlowResult> {
    if !manager.is_cache_enabled() || part_number.is_some() || rs.is_some() {
        return None;
    }

    let cached = manager.get_cached_object(cache_key).await?;
    let cache_serve_duration = request_start.elapsed();
    let metric_contract = ObjectIoGetObjectDataPlaneMetricContract::cache_served();

    debug!("Serving object from response cache: {} (latency: {:?})", cache_key, cache_serve_duration);

    if metric_contract.record_cache_served_metric {
        rustfs_io_metrics::record_get_object_cache_served(cache_serve_duration.as_secs_f64(), cached.body.len());
    }
    rustfs_io_metrics::record_io_path_selected("get", metric_contract.io_path);
    rustfs_io_metrics::record_io_copy_mode("get", metric_contract.copy_mode, cached.body.len());

    manager.record_transfer(cached.content_length as u64, Duration::from_micros(1));

    rustfs_io_metrics::record_get_object(request_start.elapsed().as_millis() as f64, cached.content_length, true);

    Some(object_io_build_cached_get_object_flow_result_from_source(
        bucket,
        key,
        cached.as_ref(),
        version_id_for_event,
    ))
}

pub(super) struct GetObjectBodyAdapterOutput {
    pub(super) body: Option<StreamingBlob>,
    pub(super) body_plan: ObjectIoGetObjectBodyPlan,
    pub(super) cache_writeback: Option<GetObjectCacheWriteback>,
}

pub(super) fn spawn_get_object_cache_writeback(
    cache_key: &str,
    writeback: GetObjectCacheWriteback,
    metric_contract: ObjectIoGetObjectDataPlaneMetricContract,
) {
    debug_assert_eq!(
        metric_contract.request_source,
        rustfs_object_io::get::GetObjectDataPlaneRequestSource::Disk
    );
    debug_assert!(!metric_contract.record_cache_served_metric);
    debug_assert!(metric_contract.record_cache_writeback_metric);

    let cached_response = CachedGetObject::from_get_object_cache_writeback(writeback);

    let cache_key_clone = cache_key.to_string();
    tokio::spawn(async move {
        let manager = get_concurrency_manager();
        manager.put_cached_object(cache_key_clone.clone(), cached_response).await;
        debug!("Object cached successfully with metadata: {}", cache_key_clone);
    });

    if metric_contract.record_cache_writeback_metric {
        rustfs_io_metrics::record_object_cache_writeback();
    }
}

pub(super) async fn build_get_object_body_adapter<R>(
    final_stream: R,
    info: &ObjectInfo,
    cache_key: &str,
    response_content_length: i64,
    optimal_buffer_size: usize,
    cache_eligibility: rustfs_concurrency::GetObjectCacheEligibility,
) -> S3Result<GetObjectBodyAdapterOutput>
where
    R: AsyncRead + Send + Sync + Unpin + 'static,
{
    let body_plan = object_io_plan_get_object_body(cache_eligibility, rustfs_config::DEFAULT_OBJECT_SEEK_SUPPORT_THRESHOLD);

    match body_plan {
        ObjectIoGetObjectBodyPlan::CacheWriteback => {
            debug!(
                "Reading object into memory for caching: key={} size={}",
                cache_key, response_content_length
            );
        }
        ObjectIoGetObjectBodyPlan::BufferSeekable => {
            debug!(
                "Reading small object into memory for seek support: key={} size={}",
                cache_key, response_content_length
            );
        }
        ObjectIoGetObjectBodyPlan::Stream if cache_eligibility.encryption_applied => {
            info!(
                "Encrypted object: Using unlimited stream for decryption with buffer size {}",
                optimal_buffer_size
            );
        }
        _ => {}
    }

    let materialized =
        object_io_materialize_get_object_body(final_stream, info, body_plan, response_content_length, optimal_buffer_size)
            .await
            .map_err(|err| match err {
                ObjectIoMaterializeGetObjectBodyError::CacheRead(err) => {
                    error!("Failed to read object into memory for caching: {}", err);
                    ApiError::from(StorageError::other(format!("Failed to read object for caching: {err}")))
                }
                ObjectIoMaterializeGetObjectBodyError::EncryptedRead(err) => {
                    error!("Failed to read decrypted object into memory: {}", err);
                    ApiError::from(StorageError::other(format!("Failed to read decrypted object: {err}")))
                }
            })?;

    Ok(GetObjectBodyAdapterOutput {
        body: materialized.body,
        body_plan: materialized.plan,
        cache_writeback: materialized.cache_writeback.map(|writeback| {
            object_io_finalize_get_object_cache_writeback(
                info,
                writeback,
                filter_object_metadata(&info.user_defined).unwrap_or_default(),
            )
        }),
    })
}

pub(super) fn finalize_get_object_completion(
    cache_key: &str,
    wrapper: &RequestTimeoutWrapper,
    timeout_config: &TimeoutConfig,
    total_duration: Duration,
    response_content_length: i64,
    optimal_buffer_size: usize,
    metric_contract: ObjectIoGetObjectDataPlaneMetricContract,
) {
    rustfs_io_metrics::record_get_object_completion(total_duration.as_secs_f64(), response_content_length, optimal_buffer_size);

    rustfs_io_metrics::record_get_object(total_duration.as_millis() as f64, response_content_length, false);
    rustfs_io_metrics::record_io_copy_mode("get", metric_contract.copy_mode, response_content_length.max(0) as usize);

    if wrapper.is_timeout() {
        warn!(
            "GetObject request exceeded timeout: key={} duration={:?} timeout={:?}",
            cache_key,
            wrapper.elapsed(),
            timeout_config.get_object_timeout
        );
        rustfs_io_metrics::record_get_object_timeout(None, Some(wrapper.elapsed().as_secs_f64()));
    }

    debug!(
        "GetObject completed: key={} size={} duration={:?} buffer={}",
        cache_key, response_content_length, total_duration, optimal_buffer_size
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn finalize_get_object_strategy_runtime(
    base_buffer_size: usize,
    manager: &ConcurrencyManager,
    bucket: &str,
    key: &str,
    info: &ObjectInfo,
    rs: Option<&HTTPRangeSpec>,
    response_content_length: i64,
    permit_wait_duration: Duration,
    queue_utilization: f64,
    queue_status: &concurrency::IoQueueStatus,
    concurrent_requests: usize,
) -> (concurrency::IoStrategy, usize) {
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
        cache_wb = io_strategy.cache_writeback_enabled,
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
    rustfs_io_metrics::record_io_priority_assignment(io_priority.as_str());

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
        concurrent_requests,
        io_strategy.load_level
    );

    (io_strategy, strategy_layout.optimal_buffer_size)
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
    match flow_result.response_mode {
        GetObjectResponseMode::Plain => {
            let helper = bind_helper_object(helper, flow_result.event_info, Some(flow_result.version_id_for_event));
            let result = Ok(S3Response::new(flow_result.output));
            let _ = helper.complete(&result);
            result
        }
        GetObjectResponseMode::CorsWrapped => {
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
    }
}

pub(super) fn complete_put_response(helper: OperationHelper, output: PutObjectOutput) -> S3Result<S3Response<PutObjectOutput>> {
    let result = Ok(S3Response::new(output));
    let _ = helper.complete(&result);
    result
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_put_extract_notification(
    notify: Arc<dyn NotifyInterface>,
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

    tokio::spawn(async move {
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
