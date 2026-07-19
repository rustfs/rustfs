//! Stable, low-cardinality observability vocabulary for RustFS request paths.
//!
//! This module intentionally accepts no bucket, object, request, URL, or
//! credential values. Those values are either unbounded or sensitive and must
//! not become telemetry dimensions.

use metrics::{Gauge, counter, gauge, histogram};
use opentelemetry::trace::Status;
use rustfs_utils::trace_attributes;
use std::future::Future;
use std::time::Instant;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const METRIC_REQUESTS_TOTAL: &str = "rustfs_s3_requests_total";
const METRIC_REQUEST_FAILURES_TOTAL: &str = "rustfs_s3_request_failures_total";
const METRIC_REQUEST_DURATION_SECONDS: &str = "rustfs_s3_request_duration_seconds";
const METRIC_REQUESTS_IN_FLIGHT: &str = "rustfs_s3_requests_in_flight";

struct InFlightRequest {
    gauge: Gauge,
}

impl Drop for InFlightRequest {
    fn drop(&mut self) {
        self.gauge.decrement(1.0);
    }
}

/// A stable S3 operation name suitable for telemetry dimensions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    CreateBucket,
    DeleteBucket,
    HeadBucket,
    ListBuckets,
    ListObjects,
    ListObjectsV2,
    PutObject,
    GetObject,
    HeadObject,
    DeleteObject,
    CreateMultipartUpload,
    UploadPart,
    UploadPartCopy,
    CompleteMultipartUpload,
    AbortMultipartUpload,
}

impl Operation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CreateBucket => "create_bucket",
            Self::DeleteBucket => "delete_bucket",
            Self::HeadBucket => "head_bucket",
            Self::ListBuckets => "list_buckets",
            Self::ListObjects => "list_objects",
            Self::ListObjectsV2 => "list_objects_v2",
            Self::PutObject => "put_object",
            Self::GetObject => "get_object",
            Self::HeadObject => "head_object",
            Self::DeleteObject => "delete_object",
            Self::CreateMultipartUpload => "create_multipart_upload",
            Self::UploadPart => "upload_part",
            Self::UploadPartCopy => "upload_part_copy",
            Self::CompleteMultipartUpload => "complete_multipart_upload",
            Self::AbortMultipartUpload => "abort_multipart_upload",
        }
    }
}

/// A bounded internal operation stage. Do not create spans per shard or block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Stage {
    Authorize,
    Metadata,
    EcRead,
    EcWrite,
    Quorum,
    Response,
    ReplicationDispatch,
    ReplicationRemote,
}

/// Bounded direction for a stream traversing the object pipeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamDirection {
    Ingress,
    Copy,
}

impl StreamDirection {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ingress => trace_attributes::stream_direction::INGRESS,
            Self::Copy => trace_attributes::stream_direction::COPY,
        }
    }
}

impl Stage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Authorize => "authorize",
            Self::Metadata => "metadata",
            Self::EcRead => "ec_read",
            Self::EcWrite => "ec_write",
            Self::Quorum => "quorum",
            Self::Response => "response",
            Self::ReplicationDispatch => "replication_dispatch",
            Self::ReplicationRemote => "replication_remote",
        }
    }
}

/// Bounded result classification shared by metrics and spans.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultClass {
    Success,
    Error,
}

impl ResultClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
        }
    }
}

/// Stable error categories for future fine-grained operation instrumentation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorClass {
    Validation,
    NotFound,
    Conflict,
    Timeout,
    Canceled,
    ResourceExhausted,
    Io,
    Network,
    Quorum,
    Consistency,
    Internal,
}

impl ErrorClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Validation => "validation_error",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::Timeout => "timeout",
            Self::Canceled => "canceled",
            Self::ResourceExhausted => "resource_exhausted",
            Self::Io => "io_error",
            Self::Network => "network_error",
            Self::Quorum => "quorum_failed",
            Self::Consistency => "consistency_violation",
            Self::Internal => "internal_error",
        }
    }
}

/// Create a child span for a bounded internal stage.
pub fn stage_span(stage: Stage) -> tracing::Span {
    tracing::info_span!(
        "rustfs.stage",
        event = "rustfs_stage",
        component = "storage",
        stage = stage.as_str(),
        "rustfs.stage" = stage.as_str()
    )
}

/// Create a child span for a request stream without exposing payload contents.
pub fn stream_span(direction: StreamDirection, expected_bytes: i64, buffer_bytes: usize) -> tracing::Span {
    tracing::info_span!(
        "rustfs.stream",
        event = "rustfs_stream",
        component = "storage",
        "rustfs.stream.direction" = direction.as_str(),
        "rustfs.stream.expected_bytes" = expected_bytes,
        "rustfs.stream.buffer_bytes" = buffer_bytes
    )
}

/// Observe a complete S3 operation without exposing request data.
pub async fn observe_operation<T, E, F>(operation: Operation, future: F) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let operation_name = operation.as_str();
    let span = tracing::info_span!(
        "rustfs.s3.operation",
        event = "s3_operation",
        component = "storage",
        operation = operation_name,
        "rustfs.operation" = operation_name
    );
    let in_flight = gauge!(METRIC_REQUESTS_IN_FLIGHT, "operation" => operation_name);
    in_flight.increment(1.0);
    let _in_flight = InFlightRequest { gauge: in_flight };
    let started_at = Instant::now();
    let result = future.instrument(span.clone()).await;

    let result_class = if result.is_ok() {
        ResultClass::Success
    } else {
        ResultClass::Error
    };
    span.set_status(match result_class {
        ResultClass::Success => Status::Ok,
        ResultClass::Error => Status::error(ErrorClass::Internal.as_str()),
    });
    counter!(METRIC_REQUESTS_TOTAL, "operation" => operation_name, "result" => result_class.as_str()).increment(1);
    histogram!(METRIC_REQUEST_DURATION_SECONDS, "operation" => operation_name, "result" => result_class.as_str())
        .record(started_at.elapsed().as_secs_f64());
    if result_class == ResultClass::Error {
        counter!(METRIC_REQUEST_FAILURES_TOTAL, "operation" => operation_name, "error.type" => ErrorClass::Internal.as_str())
            .increment(1);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::collections::HashSet;
    use std::task::Poll;

    fn capture_operation_metrics(result: Result<(), ()>) -> Vec<(String, HashSet<String>)> {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Runtime::new().expect("create test runtime");
        with_local_recorder(&recorder, || {
            let observed = runtime.block_on(observe_operation(Operation::PutObject, async { result }));
            assert_eq!(observed, result);
        });
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(composite, _, _, _)| {
                (
                    composite.key().name().to_string(),
                    composite.key().labels().map(|label| label.key().to_string()).collect(),
                )
            })
            .collect()
    }

    #[test]
    fn operation_names_are_stable_and_bounded() {
        assert_eq!(Operation::PutObject.as_str(), "put_object");
        assert_eq!(Operation::UploadPartCopy.as_str(), "upload_part_copy");
        assert_eq!(Operation::CompleteMultipartUpload.as_str(), "complete_multipart_upload");
        assert_eq!(Stage::ReplicationRemote.as_str(), "replication_remote");
        assert_eq!(StreamDirection::Ingress.as_str(), trace_attributes::stream_direction::INGRESS);
        assert_eq!(ErrorClass::Quorum.as_str(), "quorum_failed");
    }

    #[test]
    fn operation_metrics_use_only_bounded_safe_labels() {
        let metrics = capture_operation_metrics(Err(()));
        let labels_for = |name| {
            metrics
                .iter()
                .filter(|(metric_name, _)| metric_name == name)
                .map(|(_, labels)| labels)
                .collect::<Vec<_>>()
        };

        let operation_result = HashSet::from(["operation".to_string(), "result".to_string()]);
        let operation_error = HashSet::from(["operation".to_string(), "error.type".to_string()]);
        let operation_only = HashSet::from(["operation".to_string()]);
        assert_eq!(labels_for(METRIC_REQUESTS_TOTAL), vec![&operation_result]);
        assert_eq!(labels_for(METRIC_REQUEST_FAILURES_TOTAL), vec![&operation_error]);
        assert_eq!(labels_for(METRIC_REQUEST_DURATION_SECONDS), vec![&operation_result]);
        assert_eq!(labels_for(METRIC_REQUESTS_IN_FLIGHT), vec![&operation_only]);
    }

    #[test]
    fn canceled_operation_releases_in_flight_gauge() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Runtime::new().expect("create test runtime");
        with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let operation = std::pin::pin!(observe_operation(Operation::GetObject, std::future::pending::<Result<(), ()>>()));
                assert!(matches!(futures_util::poll!(operation), Poll::Pending));
            });
        });

        let gauge = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find_map(|(composite, _, _, value)| (composite.key().name() == METRIC_REQUESTS_IN_FLIGHT).then_some(value));
        assert_eq!(gauge, Some(DebugValue::Gauge(0.0.into())));
    }
}
