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

//! Four-node EC regression gate for inline storage and the inline GET reader.
//!
//! The storage decision is based on shard bytes (256 KiB / 32 KiB objects for
//! the default EC 2+2 geometry), while the GET fast path has its own object-size
//! limits (128 KiB / 16 KiB). A local OTLP/HTTP collector observes the existing
//! reader-path counter without adding a scrape endpoint or production logging.
//! One S3 GET can select readers on multiple EC nodes, so the counter tracks
//! distributed reader selection rather than HTTP request count.

use crate::common::{RustFSTestClusterEnvironment, RustFSTestEnvironment, init_logging, local_http_client};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, ExpirationStatus,
    LifecycleRule, LifecycleRuleFilter, ServerSideEncryption, Transition, TransitionStorageClass, VersioningConfiguration,
};
use base64::Engine;
use bytes::Bytes;
use flate2::read::GzDecoder;
use http::header::{CONTENT_ENCODING, HOST};
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{KeyValue, any_value::Value as AnyValue};
use opentelemetry_proto::tonic::metrics::v1::{Metric, metric, number_data_point};
use prost::Message;
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use serial_test::serial;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::error::Error;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use uuid::Uuid;
use walkdir::WalkDir;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
type MetricPointVersions = BTreeMap<u64, (u64, u64)>;
type MetricValues = Arc<Mutex<BTreeMap<String, MetricPointVersions>>>;

const KIB: usize = 1024;
const READER_PATH_COUNTER: &str = "rustfs_io_get_object_reader_path_by_size_total";
const MSGPACK_JSON_FALLBACK_COUNTER: &str = "rustfs_system_network_internode_msgpack_json_fallback_total";
const DIRECTION_LABEL: &str = "direction";
const MESSAGE_LABEL: &str = "message";
const INLINE_DIRECT: &str = "inline_direct";
const LEGACY_DUPLEX: &str = "legacy_duplex";
const EMPTY: &str = "empty";
const REMOTE_TRANSITION: &str = "remote_transition";
const PLAIN_SINGLE_PART: &str = "plain_single_part";
const MULTIPART: &str = "multipart";
const ENCRYPTED: &str = "encrypted";
const COMPRESSED: &str = "compressed";
const RANGE: &str = "range";
const REMOTE: &str = "remote";
const FALLBACK_REQUEST_DIRECTION: &str = "request";
const FALLBACK_RESPONSE_DIRECTION: &str = "response";
const MPU_PART_1_SIZE: usize = 5 * 1024 * 1024;
const MPU_PART_2_SIZE: usize = 16 * KIB;
const TIER_BUCKET: &str = "inline-fallback-cold-tier";
const TIER_PREFIX: &str = "tiered";
const MSGPACK_FALLBACK_CONTROL_SERIES: [(&str, &str); 4] = [
    (FALLBACK_REQUEST_DIRECTION, "ReadMultipleReq"),
    (FALLBACK_RESPONSE_DIRECTION, "ReadMultipleResp"),
    (FALLBACK_REQUEST_DIRECTION, "BatchReadVersionReq"),
    (FALLBACK_RESPONSE_DIRECTION, "BatchReadVersionResp"),
];
const READER_SIZE_BUCKETS: [&str; 9] = [
    "le_4kib",
    "le_16kib",
    "le_64kib",
    "le_128kib",
    "le_192kib",
    "le_256kib",
    "le_512kib",
    "le_1mib",
    "gt_1mib",
];

struct BoundaryCase {
    label: String,
    size: usize,
    stored_inline: bool,
    expected_reader_path: &'static str,
}

#[derive(Clone, Copy)]
enum VersionState {
    Unversioned,
    Enabled,
    Suspended,
}

impl VersionState {
    fn label(self) -> &'static str {
        match self {
            Self::Unversioned => "unversioned",
            Self::Enabled => "versioned",
            Self::Suspended => "suspended-null",
        }
    }

    fn expects_version_id(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

/// Minimal in-process OTLP/HTTP sink. It accepts only the test's loopback
/// metric exports and retains decoded protobuf requests for counter snapshots.
struct OtlpMetricCollector {
    endpoint: String,
    values: MetricValues,
    fallback_values: MetricValues,
    task: JoinHandle<()>,
}

impl OtlpMetricCollector {
    async fn start() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/v1/metrics", listener.local_addr()?);
        let values = Arc::new(Mutex::new(BTreeMap::new()));
        let fallback_values = Arc::new(Mutex::new(BTreeMap::new()));
        let task_values = values.clone();
        let task_fallback_values = fallback_values.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let values = task_values.clone();
                let fallback_values = task_fallback_values.clone();
                tokio::spawn(async move {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(
                            TokioIo::new(stream),
                            service_fn(move |request| handle_metric_export(request, values.clone(), fallback_values.clone())),
                        )
                        .await;
                });
            }
        });
        Ok(Self {
            endpoint,
            values,
            fallback_values,
            task,
        })
    }

    async fn reader_path_total(&self, path: &str, object_class: &str, size_bucket: &str) -> u64 {
        self.reader_path_values(path, object_class, size_bucket).await.values().sum()
    }

    async fn reader_path_values(&self, path: &str, object_class: &str, size_bucket: &str) -> BTreeMap<u64, u64> {
        self.values
            .lock()
            .await
            .get(&reader_path_metric_key(path, object_class, size_bucket))
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|(start_time, (_, value))| (start_time, value))
            .collect()
    }

    async fn reader_path_totals(&self) -> BTreeMap<String, u64> {
        self.values
            .lock()
            .await
            .iter()
            .map(|(key, points)| (key.clone(), points.values().map(|(_, value)| value).sum()))
            .collect()
    }

    async fn msgpack_json_fallback_totals(&self) -> BTreeMap<String, u64> {
        self.fallback_values
            .lock()
            .await
            .iter()
            .map(|(key, points)| (key.clone(), points.values().map(|(_, value)| value).sum()))
            .collect()
    }

    async fn reader_path_totals_for(&self, object_class: &str, size_bucket: &str) -> BTreeMap<String, u64> {
        let values = self.values.lock().await;
        [INLINE_DIRECT, LEGACY_DUPLEX, EMPTY, REMOTE_TRANSITION]
            .into_iter()
            .map(|path| {
                let total = values
                    .get(&reader_path_metric_key(path, object_class, size_bucket))
                    .map(|points| points.values().map(|(_, value)| value).sum())
                    .unwrap_or_default();
                (path.to_string(), total)
            })
            .collect()
    }

    async fn wait_for_reader_paths_to_settle(&self, object_class: &str, size_bucket: &str) -> TestResult {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut last = self.reader_path_totals_for(object_class, size_bucket).await;
        let mut unchanged_since = Instant::now();
        loop {
            if unchanged_since.elapsed() >= Duration::from_millis(1_500) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!("timed out waiting for reader-path metrics to settle: {last:?}").into());
            }
            sleep(Duration::from_millis(100)).await;
            let current = self.reader_path_totals_for(object_class, size_bucket).await;
            if current != last {
                last = current;
                unchanged_since = Instant::now();
            }
        }
    }

    async fn wait_for_reader_path_total(
        &self,
        path: &str,
        object_class: &str,
        size_bucket: &str,
        expected: u64,
    ) -> TestResult<u64> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let total = self.reader_path_total(path, object_class, size_bucket).await;
            if total >= expected {
                return Ok(total);
            }
            if Instant::now() >= deadline {
                let observed_totals = self.reader_path_totals().await;
                return Err(format!(
                    "timed out waiting for {READER_PATH_COUNTER}{{path={path}, object_class={object_class}, size_bucket={size_bucket}}} >= {expected}; observed {total}; totals={observed_totals:?}"
                )
                .into());
            }
            sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for OtlpMetricCollector {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle_metric_export(
    request: Request<Incoming>,
    values: MetricValues,
    fallback_values: MetricValues,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if request.uri().path() != "/v1/metrics" {
        return Ok(response(StatusCode::NOT_FOUND));
    }

    let gzip = request
        .headers()
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("gzip"));
    let Ok(collected) = request.into_body().collect().await else {
        return Ok(response(StatusCode::BAD_REQUEST));
    };
    let body = collected.to_bytes();
    if body.len() > 4 * 1024 * 1024 {
        return Ok(response(StatusCode::PAYLOAD_TOO_LARGE));
    }
    let payload = if gzip {
        let mut decoder = GzDecoder::new(body.as_ref());
        let mut decoded = Vec::new();
        if decoder.by_ref().take(4 * 1024 * 1024 + 1).read_to_end(&mut decoded).is_err() || decoded.len() > 4 * 1024 * 1024 {
            return Ok(response(StatusCode::BAD_REQUEST));
        }
        decoded
    } else {
        body.to_vec()
    };
    match ExportMetricsServiceRequest::decode(payload.as_slice()) {
        Ok(export) => {
            let mut values = values.lock().await;
            let mut fallback_values = fallback_values.lock().await;
            record_reader_path_metrics(&export, &mut values);
            record_msgpack_fallback_metrics(&export, &mut fallback_values);
            Ok(response(StatusCode::OK))
        }
        Err(_) => Ok(response(StatusCode::BAD_REQUEST)),
    }
}

fn response(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .expect("static HTTP response is valid")
}

fn reader_path_metric_key(path: &str, object_class: &str, size_bucket: &str) -> String {
    format!("{path}\u{1f}{object_class}\u{1f}{size_bucket}")
}

fn record_reader_path_metrics(export: &ExportMetricsServiceRequest, values: &mut BTreeMap<String, MetricPointVersions>) {
    for resource_metrics in &export.resource_metrics {
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                record_reader_path_metric(metric, values);
            }
        }
    }
}

fn record_reader_path_metric(metric: &Metric, values: &mut BTreeMap<String, MetricPointVersions>) {
    if metric.name != READER_PATH_COUNTER {
        return;
    }
    let Some(metric::Data::Sum(sum)) = &metric.data else {
        return;
    };
    for point in &sum.data_points {
        let Some(path) = attribute_string(&point.attributes, "path") else {
            continue;
        };
        if ![INLINE_DIRECT, LEGACY_DUPLEX, EMPTY, REMOTE_TRANSITION].contains(&path) {
            continue;
        }
        let Some(object_class) = attribute_string(&point.attributes, "object_class") else {
            continue;
        };
        let Some(size_bucket) = attribute_string(&point.attributes, "size_bucket") else {
            continue;
        };
        let Some(number_data_point::Value::AsInt(value)) = point.value.as_ref() else {
            continue;
        };
        let value = u64::try_from(*value).unwrap_or_default();
        values
            .entry(reader_path_metric_key(path, object_class, size_bucket))
            .or_default()
            .entry(point.start_time_unix_nano)
            .and_modify(|current| {
                if point.time_unix_nano >= current.0 {
                    *current = (point.time_unix_nano, value);
                }
            })
            .or_insert((point.time_unix_nano, value));
    }
}

fn record_msgpack_fallback_metrics(export: &ExportMetricsServiceRequest, values: &mut BTreeMap<String, MetricPointVersions>) {
    for resource_metrics in &export.resource_metrics {
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                if metric.name != MSGPACK_JSON_FALLBACK_COUNTER {
                    continue;
                }
                let Some(metric::Data::Sum(sum)) = &metric.data else {
                    continue;
                };
                for point in &sum.data_points {
                    let Some(direction) = attribute_string(&point.attributes, DIRECTION_LABEL) else {
                        continue;
                    };
                    let Some(message) = attribute_string(&point.attributes, MESSAGE_LABEL) else {
                        continue;
                    };
                    let Some(number_data_point::Value::AsInt(value)) = point.value.as_ref() else {
                        continue;
                    };
                    let value = u64::try_from(*value).unwrap_or_default();
                    values
                        .entry(msgpack_fallback_metric_key(direction, message))
                        .or_default()
                        .entry(point.start_time_unix_nano)
                        .and_modify(|current| {
                            if point.time_unix_nano >= current.0 {
                                *current = (point.time_unix_nano, value);
                            }
                        })
                        .or_insert((point.time_unix_nano, value));
                }
            }
        }
    }
}

fn msgpack_fallback_metric_key(direction: &str, message: &str) -> String {
    format!("{direction}\u{1f}{message}")
}

async fn assert_msgpack_fallback_unchanged(
    collector: &OtlpMetricCollector,
    before: &BTreeMap<String, u64>,
    series: &[(&str, &str)],
) -> TestResult {
    let after = collector.msgpack_json_fallback_totals().await;
    for &(direction, message) in series {
        let key = msgpack_fallback_metric_key(direction, message);
        assert_eq!(
            before.get(&key).copied().unwrap_or_default(),
            after.get(&key).copied().unwrap_or_default(),
            "fallback counter changed for {direction}/{message}: before={:?}, after={:?}",
            before.get(&key),
            after.get(&key),
        );
    }
    Ok(())
}

fn attribute_string<'a>(attributes: &'a [KeyValue], wanted_key: &str) -> Option<&'a str> {
    attributes.iter().find_map(|attribute| {
        if attribute.key != wanted_key {
            return None;
        }
        match attribute.value.as_ref()?.value.as_ref()? {
            AnyValue::StringValue(value) => Some(value.as_str()),
            _ => None,
        }
    })
}

fn boundary_cases(state: VersionState) -> Vec<BoundaryCase> {
    let (fast_limit, storage_limit) = match state {
        VersionState::Enabled => (16 * KIB, 32 * KIB),
        VersionState::Unversioned => (128 * KIB, 256 * KIB),
        // A suspended bucket stores its null version using the unversioned
        // shard threshold, while ObjectInfo keeps version-aware GET semantics.
        VersionState::Suspended => (16 * KIB, 256 * KIB),
    };
    let mut sizes = vec![0, 16 * KIB - 1, 16 * KIB, 16 * KIB + 1, 32 * KIB - 1, 32 * KIB, 32 * KIB + 1];
    if !matches!(state, VersionState::Enabled) {
        sizes.extend([
            128 * KIB - 1,
            128 * KIB,
            128 * KIB + 1,
            256 * KIB - 1,
            256 * KIB,
            256 * KIB + 1,
        ]);
    }
    sizes
        .into_iter()
        .map(|size| BoundaryCase {
            label: format!("{size}-bytes"),
            size,
            stored_inline: size <= storage_limit,
            expected_reader_path: if size == 0 {
                EMPTY
            } else if size <= fast_limit {
                INLINE_DIRECT
            } else {
                LEGACY_DUPLEX
            },
        })
        .collect()
}

fn payload(size: usize, seed: u8) -> Vec<u8> {
    (0..size)
        .map(|index| (index as u64).wrapping_mul(2_654_435_761).wrapping_add(seed as u64) as u8)
        .collect()
}

fn compressible_payload(size: usize) -> Vec<u8> {
    let pattern = b"RustFS inline compressed fallback control. ";
    let mut body = Vec::with_capacity(size);
    while body.len() < size {
        body.extend_from_slice(pattern);
    }
    body.truncate(size);
    body
}

fn size_bucket(size: usize) -> &'static str {
    if size <= 4 * KIB {
        "le_4kib"
    } else if size <= 16 * KIB {
        "le_16kib"
    } else if size <= 64 * KIB {
        "le_64kib"
    } else if size <= 128 * KIB {
        "le_128kib"
    } else if size <= 192 * KIB {
        "le_192kib"
    } else if size <= 256 * KIB {
        "le_256kib"
    } else if size <= 512 * KIB {
        "le_512kib"
    } else if size <= 1024 * KIB {
        "le_1mib"
    } else {
        "gt_1mib"
    }
}

async fn configure_versioning(client: &Client, bucket: &str, state: VersionState) -> TestResult {
    let status = match state {
        VersionState::Unversioned => return Ok(()),
        VersionState::Enabled => BucketVersioningStatus::Enabled,
        VersionState::Suspended => BucketVersioningStatus::Suspended,
    };
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(VersioningConfiguration::builder().status(status).build())
        .send()
        .await?;
    Ok(())
}

fn configure_reader_metric_cluster(cluster: &mut RustFSTestClusterEnvironment, collector: &OtlpMetricCollector) {
    cluster.set_env("RUSTFS_OBS_ENDPOINT", collector.endpoint.trim_end_matches("/v1/metrics"));
    cluster.set_env("RUSTFS_OBS_METRIC_ENDPOINT", &collector.endpoint);
    cluster.set_env("RUSTFS_OBS_METRICS_EXPORT_ENABLED", "true");
    cluster.set_env("RUSTFS_OBS_TRACES_EXPORT_ENABLED", "false");
    cluster.set_env("RUSTFS_OBS_LOGS_EXPORT_ENABLED", "false");
    cluster.set_env("RUSTFS_OBS_METER_INTERVAL", "1");
    cluster.set_env("RUSTFS_OBS_USE_STDOUT", "false");
    cluster.set_env("RUSTFS_GET_CODEC_STREAMING_ENABLE", "false");
    cluster.set_env("RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY", "false");
}

fn configure_mixed_msgpack_cluster(cluster: &mut RustFSTestClusterEnvironment, collector: &OtlpMetricCollector) -> TestResult {
    configure_reader_metric_cluster(cluster, collector);
    cluster.set_node_env(0, "RUSTFS_INTERNODE_RPC_MSGPACK_ONLY", "true")?;
    cluster.set_node_env(0, "RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED", "true")?;
    cluster.set_node_env(1, "RUSTFS_INTERNODE_RPC_MSGPACK_ONLY", "true")?;
    cluster.set_node_env(1, "RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED", "true")?;
    cluster.set_node_env(2, "RUSTFS_INTERNODE_RPC_MSGPACK_ONLY", "false")?;
    cluster.set_node_env(3, "RUSTFS_INTERNODE_RPC_MSGPACK_ONLY", "false")?;
    Ok(())
}

async fn assert_case(
    cluster: &RustFSTestClusterEnvironment,
    client: &Client,
    bucket: &str,
    state: VersionState,
    case: &BoundaryCase,
    seed: u8,
) -> TestResult<(String, Vec<u8>, Option<String>, Option<String>)> {
    let key = format!("{}/{}/{}/{}.bin", state.label(), case.expected_reader_path, case.size, case.label);
    let body = payload(case.size, seed);
    let put = client
        .put_object()
        .bucket(bucket)
        .key(&key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    let version_id = put.version_id().map(str::to_owned);
    assert_eq!(
        version_id.is_some(),
        state.expects_version_id(),
        "{} {} PUT version-id compatibility changed: {version_id:?}",
        state.label(),
        case.label
    );
    assert_storage_layout(cluster, bucket, &key, version_id.as_deref(), case.stored_inline)?;
    Ok((key, body, put.e_tag().map(str::to_owned), version_id))
}

async fn get_and_assert(
    client: &Client,
    bucket: &str,
    key: &str,
    expected_body: &[u8],
    expected_etag: Option<&str>,
    expected_version_id: Option<&str>,
) -> TestResult {
    let response = client.get_object().bucket(bucket).key(key).send().await?;
    assert_eq!(
        response.content_length(),
        Some(expected_body.len() as i64),
        "GET content-length changed for {key}"
    );
    assert_eq!(response.e_tag(), expected_etag, "GET ETag changed for {key}");
    assert_eq!(response.version_id(), expected_version_id, "GET version-id changed for {key}");
    let body = response.body.collect().await?.into_bytes();
    assert_eq!(body.as_ref(), expected_body, "GET body changed for {key}");
    Ok(())
}

struct ReaderObject<'a> {
    bucket: &'a str,
    key: &'a str,
    body: &'a [u8],
    etag: Option<&'a str>,
    version_id: Option<&'a str>,
}

impl<'a> ReaderObject<'a> {
    fn new(bucket: &'a str, key: &'a str, body: &'a [u8], etag: Option<&'a str>, version_id: Option<&'a str>) -> Self {
        Self {
            bucket,
            key,
            body,
            etag,
            version_id,
        }
    }
}

struct ReaderPathExpectation<'a> {
    object: ReaderObject<'a>,
    expected_path: &'a str,
    object_class: &'a str,
    expected_size_bucket: Option<&'a str>,
}

struct PartNumberReaderPathExpectation<'a> {
    bucket: &'a str,
    key: &'a str,
    expected_part: &'a [u8],
    full_object_size: usize,
    object_class: &'a str,
    expected_path: &'a str,
}

impl<'a> PartNumberReaderPathExpectation<'a> {
    fn new(
        bucket: &'a str,
        key: &'a str,
        expected_part: &'a [u8],
        full_object_size: usize,
        object_class: &'a str,
        expected_path: &'a str,
    ) -> Self {
        Self {
            bucket,
            key,
            expected_part,
            full_object_size,
            object_class,
            expected_path,
        }
    }
}

impl<'a> ReaderPathExpectation<'a> {
    fn plain(object: ReaderObject<'a>, expected_path: &'a str) -> Self {
        Self::for_class(object, expected_path, PLAIN_SINGLE_PART)
    }

    fn for_class(object: ReaderObject<'a>, expected_path: &'a str, object_class: &'a str) -> Self {
        let expected_size_bucket = size_bucket(object.body.len());
        Self {
            object,
            expected_path,
            object_class,
            expected_size_bucket: Some(expected_size_bucket),
        }
    }

    fn with_size_bucket(
        object: ReaderObject<'a>,
        expected_path: &'a str,
        object_class: &'a str,
        expected_size_bucket: &'a str,
    ) -> Self {
        Self {
            object,
            expected_path,
            object_class,
            expected_size_bucket: Some(expected_size_bucket),
        }
    }

    fn with_any_size_bucket(object: ReaderObject<'a>, expected_path: &'a str, object_class: &'a str) -> Self {
        Self {
            object,
            expected_path,
            object_class,
            expected_size_bucket: None,
        }
    }
}

async fn assert_reader_path(
    collector: &OtlpMetricCollector,
    client: &Client,
    expectation: ReaderPathExpectation<'_>,
) -> TestResult {
    let ReaderPathExpectation {
        object,
        expected_path,
        object_class,
        expected_size_bucket,
    } = expectation;
    let paths = [INLINE_DIRECT, LEGACY_DUPLEX, EMPTY, REMOTE_TRANSITION];
    let size_buckets: Vec<&str> = match expected_size_bucket {
        Some(expected_size_bucket) => vec![expected_size_bucket],
        None => READER_SIZE_BUCKETS.to_vec(),
    };
    let size_buckets = size_buckets.as_slice();

    let mut before = BTreeMap::<&str, BTreeMap<&str, u64>>::new();
    for &size_bucket in size_buckets {
        let mut bucket_before = BTreeMap::new();
        for path in paths {
            bucket_before.insert(path, collector.reader_path_total(path, object_class, size_bucket).await);
        }
        before.insert(size_bucket, bucket_before);
    }

    get_and_assert(client, object.bucket, object.key, object.body, object.etag, object.version_id).await?;

    let selected_size_bucket = match expected_size_bucket {
        Some(expected_size_bucket) => {
            let path_before = before
                .get(expected_size_bucket)
                .and_then(|values| values.get(expected_path))
                .copied()
                .ok_or_else(|| format!("reader-path baseline missing for size bucket {expected_size_bucket}"))?;
            let expected_after = collector
                .wait_for_reader_path_total(expected_path, object_class, expected_size_bucket, path_before + 1)
                .await?;
            assert!(
                expected_after > path_before,
                "{READER_PATH_COUNTER}{{path={expected_path}}} must advance for {}",
                object.key
            );
            expected_size_bucket
        }
        None => {
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let mut matched_size_bucket = None;
                for &size_bucket in size_buckets {
                    let path_before = before
                        .get(size_bucket)
                        .and_then(|values| values.get(expected_path))
                        .copied()
                        .ok_or_else(|| format!("reader-path baseline missing for size bucket {size_bucket}"))?;
                    let path_after = collector.reader_path_total(expected_path, object_class, size_bucket).await;
                    if path_after < path_before + 1 {
                        continue;
                    }
                    let mut conflicting = false;
                    for path in paths {
                        if path == expected_path || expected_path == EMPTY {
                            continue;
                        }
                        let path_before = before
                            .get(size_bucket)
                            .and_then(|values| values.get(path))
                            .copied()
                            .ok_or_else(|| format!("reader-path baseline missing for size bucket {size_bucket}"))?;
                        let path_after = collector.reader_path_total(path, object_class, size_bucket).await;
                        if path_after != path_before {
                            conflicting = true;
                            break;
                        }
                    }
                    if !conflicting {
                        matched_size_bucket = Some(size_bucket);
                        break;
                    }
                }

                if let Some(size_bucket) = matched_size_bucket {
                    break size_bucket;
                }
                if Instant::now() >= deadline {
                    let mut observed = BTreeMap::<&str, BTreeMap<&str, u64>>::new();
                    for &size_bucket in size_buckets {
                        let mut bucket_after = BTreeMap::new();
                        for path in paths {
                            bucket_after.insert(path, collector.reader_path_total(path, object_class, size_bucket).await);
                        }
                        observed.insert(size_bucket, bucket_after);
                    }
                    return Err(format!(
                        "timed out waiting for {READER_PATH_COUNTER}{{object_class={object_class}, path={expected_path}}} to advance in any candidate size bucket; before={before:?}; after={observed:?}"
                    )
                    .into());
                }
                sleep(Duration::from_millis(100)).await;
            }
        }
    };

    collector
        .wait_for_reader_paths_to_settle(object_class, selected_size_bucket)
        .await?;

    for path in paths {
        if path == expected_path {
            continue;
        }
        if expected_path == EMPTY {
            continue;
        }
        assert_eq!(
            collector.reader_path_total(path, object_class, selected_size_bucket).await,
            before
                .get(selected_size_bucket)
                .and_then(|values| values.get(path))
                .copied()
                .ok_or_else(|| format!("reader-path baseline missing for size bucket {selected_size_bucket}"))?,
            "{READER_PATH_COUNTER}{{path={path}, object_class={object_class}, size_bucket={selected_size_bucket}}} must not advance for {}; expected {expected_path} only",
            object.key
        );
    }
    Ok(())
}

async fn assert_part_number_reader_path(
    collector: &OtlpMetricCollector,
    client: &Client,
    expectation: PartNumberReaderPathExpectation<'_>,
) -> TestResult {
    let PartNumberReaderPathExpectation {
        bucket,
        key,
        expected_part,
        full_object_size,
        object_class,
        expected_path,
    } = expectation;
    let paths = [INLINE_DIRECT, LEGACY_DUPLEX, EMPTY, REMOTE_TRANSITION];
    let size_bucket = size_bucket(full_object_size);
    let mut before = BTreeMap::<&str, u64>::new();
    for path in paths {
        before.insert(path, collector.reader_path_total(path, object_class, size_bucket).await);
    }
    let response = client.get_object().bucket(bucket).key(key).part_number(2).send().await?;
    assert_eq!(
        response.content_length(),
        Some(expected_part.len() as i64),
        "partNumber GET content-length changed for {key}"
    );
    let body = response.body.collect().await?.into_bytes();
    assert_eq!(body.as_ref(), expected_part, "partNumber GET body changed for {key}");
    collector
        .wait_for_reader_path_total(expected_path, object_class, size_bucket, before[expected_path] + 1)
        .await?;
    collector.wait_for_reader_paths_to_settle(object_class, size_bucket).await?;
    for path in paths {
        if path == expected_path {
            continue;
        }
        assert_eq!(
            collector.reader_path_total(path, object_class, size_bucket).await,
            before[path],
            "{READER_PATH_COUNTER}{{path={path}, object_class={object_class}, size_bucket={size_bucket}}} must not advance for partNumber GET {key}; expected {expected_path} only"
        );
    }
    Ok(())
}

async fn put_two_part_multipart(client: &Client, bucket: &str, key: &str) -> TestResult<(Vec<u8>, Vec<u8>, Option<String>)> {
    let part1 = payload(MPU_PART_1_SIZE, 0xA5);
    let part2 = payload(MPU_PART_2_SIZE, 0x5A);
    let create = client.create_multipart_upload().bucket(bucket).key(key).send().await?;
    let upload_id = create
        .upload_id()
        .ok_or("CreateMultipartUpload returned no upload id")?
        .to_string();
    let uploaded_part1 = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(part1.clone()))
        .send()
        .await?;
    let uploaded_part2 = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(2)
        .body(ByteStream::from(part2.clone()))
        .send()
        .await?;
    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(uploaded_part1.e_tag().unwrap_or_default())
                .build(),
        )
        .parts(
            CompletedPart::builder()
                .part_number(2)
                .e_tag(uploaded_part2.e_tag().unwrap_or_default())
                .build(),
        )
        .build();
    let complete = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(completed)
        .send()
        .await?;
    let mut body = part1;
    body.extend_from_slice(&part2);
    Ok((body, part2, complete.e_tag().map(str::to_owned)))
}

async fn signed_admin_request(
    base_url: &str,
    method: Method,
    path: &str,
    body: Option<&str>,
    access_key: &str,
    secret_key: &str,
) -> TestResult<(reqwest::StatusCode, String)> {
    let url = format!("{base_url}{path}");
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
    let body_bytes = body.map(|value| value.as_bytes().to_vec()).unwrap_or_default();

    let request = http::Request::builder()
        .method(method.clone())
        .uri(uri)
        .header(HOST, authority)
        .header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
    let signed = sign_v4(request.body(Body::empty())?, 0, access_key, secret_key, "", "us-east-1");

    let client = local_http_client();
    let mut request_builder = client.request(method, url.as_str());
    for (name, value) in signed.headers() {
        request_builder = request_builder.header(name, value);
    }
    if !body_bytes.is_empty() {
        request_builder = request_builder.body(body_bytes);
    }
    let response = request_builder.send().await?;
    let status = response.status();
    let text = response.text().await?;
    Ok((status, text))
}

fn unique_tier_name() -> String {
    format!("COLDTIER{}", Uuid::new_v4().simple()).to_ascii_uppercase()
}

async fn add_rustfs_tier(hot: &RustFSTestClusterEnvironment, cold: &RustFSTestEnvironment, tier_name: &str) -> TestResult {
    let body = serde_json::json!({
        "type": "rustfs",
        "rustfs": {
            "name": tier_name,
            "endpoint": cold.url.as_str(),
            "accessKey": cold.access_key.as_str(),
            "secretKey": cold.secret_key.as_str(),
            "bucket": TIER_BUCKET,
            "prefix": TIER_PREFIX,
            "region": "us-east-1",
            "storageClass": ""
        }
    })
    .to_string();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut attempts = Vec::new();
    let final_error = loop {
        let (status, response) = signed_admin_request(
            &hot.nodes[0].url,
            Method::PUT,
            "/rustfs/admin/v3/tier",
            Some(&body),
            &hot.access_key,
            &hot.secret_key,
        )
        .await?;
        let attempt = format!("status={status}, body={}", compact_body(&response));
        if status.is_success() {
            wait_for_tier_verifiable(hot, tier_name, &format!("status={status}, body={response}")).await?;
            return Ok(());
        }
        attempts.push(attempt);
        if Instant::now() >= deadline {
            break attempts.join("; ");
        }
        if !is_retryable_add_tier_error(&response) {
            break attempts.join("; ");
        }
        sleep(Duration::from_millis(500)).await;
    };
    Err(format!("AddTier(RustFS) failed after readiness polling: {final_error}").into())
}

async fn wait_for_tier_verifiable(hot: &RustFSTestClusterEnvironment, tier_name: &str, add_tier_response: &str) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(60);
    let final_error = loop {
        let snapshot = tier_readiness_snapshot(hot, tier_name).await?;
        if snapshot.iter().any(|node| node.verify_status.is_success()) {
            return Ok(());
        }
        if snapshot.iter().any(|node| !is_retryable_tier_error(&node.verify_body)) {
            break format!("non-retryable tier verification failure: {}", format_tier_readiness_snapshot(&snapshot));
        }
        if Instant::now() >= deadline {
            break format_tier_readiness_snapshot(&snapshot);
        }
        sleep(Duration::from_millis(500)).await;
    };
    Err(format!(
        "tier {tier_name} was not verifiable on any hot node within 60s after AddTier({add_tier_response}): {final_error}"
    )
    .into())
}

async fn wait_for_tier_converged(hot: &RustFSTestClusterEnvironment, add_tier_responses: &str) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(60);
    let final_error = loop {
        let snapshot = tier_readiness_snapshot(hot).await?;
        if snapshot
            .iter()
            .all(|node| node.list_status.is_success() && node.list_has_tier && node.verify_status.is_success())
        {
            return Ok(());
        }
        if snapshot.iter().any(|node| {
            !node.list_status.is_success() || (!node.verify_status.is_success() && !is_retryable_tier_error(&node.verify_body))
        }) {
            break format_tier_readiness_snapshot(&snapshot);
        }
        if Instant::now() >= deadline {
            break format_tier_readiness_snapshot(&snapshot);
        }
        sleep(Duration::from_millis(500)).await;
    };
    Err(format!(
        "tier {TIER_NAME} did not converge on every hot node within 60s after AddTier({add_tier_responses}): {final_error}"
    )
    .into())
}

struct TierNodeReadiness {
    node_index: usize,
    node_url: String,
    list_status: StatusCode,
    list_has_tier: bool,
    list_body: String,
    verify_status: StatusCode,
    verify_body: String,
}

async fn tier_readiness_snapshot(hot: &RustFSTestClusterEnvironment, tier_name: &str) -> TestResult<Vec<TierNodeReadiness>> {
    let mut snapshot = Vec::with_capacity(hot.nodes.len());
    for (node_index, node) in hot.nodes.iter().enumerate() {
        let (list_status, list_body) =
            signed_admin_request(&node.url, Method::GET, "/rustfs/admin/v3/tier", None, &hot.access_key, &hot.secret_key).await?;
        let (verify_status, verify_body) = signed_admin_request(
            &node.url,
            Method::GET,
            &format!("/rustfs/admin/v3/tier/{tier_name}"),
            None,
            &hot.access_key,
            &hot.secret_key,
        )
        .await?;
        snapshot.push(TierNodeReadiness {
            node_index,
            node_url: node.url.clone(),
            list_status,
            list_has_tier: tier_list_contains(&list_body, tier_name),
            list_body,
            verify_status,
            verify_body,
        });
    }
    Ok(snapshot)
}

fn tier_list_contains(response: &str, tier_name: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(response)
        .ok()
        .and_then(|value| value.as_array().cloned())
        .is_some_and(|tiers| {
            tiers.iter().any(|tier| {
                tier.get("name").and_then(serde_json::Value::as_str) == Some(tier_name)
                    || tier
                        .get("rustfs")
                        .and_then(|rustfs| rustfs.get("name"))
                        .and_then(serde_json::Value::as_str)
                        == Some(tier_name)
            })
        })
}

fn format_tier_readiness_snapshot(snapshot: &[TierNodeReadiness]) -> String {
    snapshot
        .iter()
        .map(|node| {
            format!(
                "node {} {} list_status={} list_has_tier={} list_body={} verify_status={} verify_body={}",
                node.node_index,
                node.node_url,
                node.list_status,
                node.list_has_tier,
                compact_body(&node.list_body),
                node.verify_status,
                compact_body(&node.verify_body)
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn compact_body(body: &str) -> String {
    const MAX_BODY_CHARS: usize = 1024;
    let mut value = body.split_whitespace().collect::<Vec<_>>().join(" ");
    if value.chars().count() > MAX_BODY_CHARS {
        value = value.chars().take(MAX_BODY_CHARS).collect::<String>();
        value.push_str("...");
    }
    value
}

fn is_retryable_tier_error(response: &str) -> bool {
    response.contains("<Code>TierNotFound</Code>")
        || response.contains("<Code>NoSuchTier</Code>")
        || (response.contains("<Code>TierVerificationFailed</Code>")
            && response.contains("Remote tier configuration is being replaced"))
        || response.contains("TierNotFound")
        || response.contains("NoSuchTier")
}

fn is_retryable_add_tier_error(response: &str) -> bool {
    response.contains("Remote tier configuration is already being replaced")
}

fn transition_rule(tier_name: &str) -> TestResult<LifecycleRule> {
    Ok(LifecycleRule::builder()
        .id("inline-fallback-transition")
        .filter(LifecycleRuleFilter::builder().prefix("transition/").build())
        .transitions(
            Transition::builder()
                .days(0)
                .storage_class(TransitionStorageClass::from(tier_name))
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()?)
}

async fn wait_for_transition(client: &Client, bucket: &str, key: &str, tier_name: &str) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let head = client.head_object().bucket(bucket).key(key).send().await?;
        if head.storage_class().map(|storage_class| storage_class.as_str()) == Some(tier_name) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "object {bucket}/{key} was not transitioned to {tier_name} within 90s (storage_class={:?})",
                head.storage_class()
            )
            .into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn cold_tier_object_count(cold_client: &Client) -> TestResult<usize> {
    Ok(cold_client
        .list_objects_v2()
        .bucket(TIER_BUCKET)
        .send()
        .await?
        .contents()
        .len())
}

async fn put_lifecycle_with_transition_retry(client: &Client, bucket: &str, tier_name: &str) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let lifecycle = BucketLifecycleConfiguration::builder()
            .rules(transition_rule(tier_name)?)
            .build()?;
        match client
            .put_bucket_lifecycle_configuration()
            .bucket(bucket)
            .lifecycle_configuration(lifecycle)
            .send()
            .await
        {
            Ok(_) => return Ok(()),
            Err(err) => {
                let message = format!("{err:?}");
                if !message.contains("invalid tier") || Instant::now() >= deadline {
                    return Err(err.into());
                }
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
}

fn assert_storage_layout(
    cluster: &RustFSTestClusterEnvironment,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    expected_inline: bool,
) -> TestResult {
    for (node_index, node) in cluster.nodes.iter().enumerate() {
        let object_dir = Path::new(&node.data_dir).join(bucket).join(key);
        let meta_path = object_dir.join("xl.meta");
        assert!(meta_path.is_file(), "node {node_index} is missing xl.meta for {key}");
        let metadata = rustfs_filemeta::FileMeta::load(&std::fs::read(&meta_path)?)?;
        let file_info = metadata.into_fileinfo(bucket, key, version_id.unwrap_or_default(), true, false, true)?;
        assert_eq!(
            file_info.inline_data(),
            expected_inline,
            "node {node_index} inline metadata differs for {key} (version {version_id:?})"
        );
        let has_part_file = WalkDir::new(&object_dir)
            .into_iter()
            .filter_map(Result::ok)
            .any(|entry| entry.file_type().is_file() && entry.file_name().to_string_lossy().starts_with("part."));
        assert_eq!(
            has_part_file, !expected_inline,
            "node {node_index} physical shard layout differs for {key} (version {version_id:?})"
        );
    }
    Ok(())
}

#[tokio::test]
#[serial]
async fn four_node_inline_storage_and_get_boundaries() -> TestResult {
    init_logging();

    let collector = OtlpMetricCollector::start().await?;
    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    configure_reader_metric_cluster(&mut cluster, &collector);
    cluster.start().await?;

    for (state_index, state) in [VersionState::Unversioned, VersionState::Enabled, VersionState::Suspended]
        .into_iter()
        .enumerate()
    {
        let bucket = format!("inline-boundary-{}", state.label());
        cluster.create_test_bucket(&bucket).await?;
        let client = cluster.create_s3_client(state_index % cluster.nodes.len())?;
        configure_versioning(&client, &bucket, state).await?;

        let mut objects = Vec::new();
        for (case_index, case) in boundary_cases(state).into_iter().enumerate() {
            let (key, body, etag, version_id) =
                assert_case(&cluster, &client, &bucket, state, &case, (state_index * 31 + case_index) as u8).await?;
            objects.push((key, body, etag, version_id, case.expected_reader_path));
        }
        for (key, body, etag, version_id, expected_path) in &objects {
            assert_reader_path(
                &collector,
                &client,
                ReaderPathExpectation::plain(
                    ReaderObject::new(&bucket, key, body, etag.as_deref(), version_id.as_deref()),
                    expected_path,
                ),
            )
            .await?;
        }
    }

    // Range is an explicit fallback control even for an otherwise eligible
    // unversioned object and must select a fallback reader.
    let client = cluster.create_s3_client(0)?;
    let bucket = "inline-boundary-unversioned";
    let key = "unversioned/inline_direct/131072/131072-bytes.bin";
    let range_size_bucket = size_bucket(128 * KIB);
    let mut before = BTreeMap::new();
    for path in [INLINE_DIRECT, LEGACY_DUPLEX, EMPTY, REMOTE_TRANSITION] {
        before.insert(path, collector.reader_path_total(path, RANGE, range_size_bucket).await);
    }
    let ranged = client.get_object().bucket(bucket).key(key).range("bytes=0-31").send().await?;
    assert_eq!(ranged.content_length(), Some(32), "range GET must retain S3 response semantics");
    assert_eq!(
        ranged.body.collect().await?.into_bytes().len(),
        32,
        "range GET must retain requested body length"
    );
    collector
        .wait_for_reader_path_total(LEGACY_DUPLEX, "range", range_size_bucket, before[LEGACY_DUPLEX] + 1)
        .await?;
    collector.wait_for_reader_paths_to_settle(RANGE, range_size_bucket).await?;
    assert_eq!(
        collector.reader_path_total(INLINE_DIRECT, "range", range_size_bucket).await,
        before[INLINE_DIRECT],
        "range GET must not select inline_direct"
    );
    assert_eq!(
        collector.reader_path_total(EMPTY, "range", range_size_bucket).await,
        before[EMPTY],
        "range GET must not select empty"
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn four_node_inline_fallback_controls() -> TestResult {
    init_logging();

    let collector = OtlpMetricCollector::start().await?;
    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    configure_reader_metric_cluster(&mut cluster, &collector);
    let sse_master_key = base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]);
    cluster.set_env("RUSTFS_SSE_S3_MASTER_KEY", &sse_master_key);
    cluster.start().await?;

    let bucket = "inline-fallback-controls";
    cluster.create_test_bucket(bucket).await?;
    let client = cluster.create_s3_client(0)?;

    let multipart_key = "multipart/two-part.bin";
    let (multipart_body, second_part, multipart_etag) = put_two_part_multipart(&client, bucket, multipart_key).await?;
    assert_reader_path(
        &collector,
        &client,
        ReaderPathExpectation::for_class(
            ReaderObject::new(bucket, multipart_key, &multipart_body, multipart_etag.as_deref(), None),
            LEGACY_DUPLEX,
            MULTIPART,
        ),
    )
    .await?;
    assert_part_number_reader_path(
        &collector,
        &client,
        PartNumberReaderPathExpectation::new(bucket, multipart_key, &second_part, multipart_body.len(), MULTIPART, LEGACY_DUPLEX),
    )
    .await?;

    let encrypted_key = "encrypted/sse-s3.bin";
    let encrypted_body = payload(16 * KIB, 0xE3);
    let encrypted_put = client
        .put_object()
        .bucket(bucket)
        .key(encrypted_key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from(encrypted_body.clone()))
        .send()
        .await?;
    let encrypted_head = client.head_object().bucket(bucket).key(encrypted_key).send().await?;
    assert_eq!(
        encrypted_head.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "HEAD must preserve SSE-S3 metadata for {encrypted_key}"
    );
    assert_reader_path(
        &collector,
        &client,
        ReaderPathExpectation::with_any_size_bucket(
            ReaderObject::new(bucket, encrypted_key, &encrypted_body, encrypted_put.e_tag(), None),
            LEGACY_DUPLEX,
            ENCRYPTED,
        ),
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn four_node_compressed_inline_fallback() -> TestResult {
    init_logging();

    let collector = OtlpMetricCollector::start().await?;
    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    configure_reader_metric_cluster(&mut cluster, &collector);
    cluster.set_env("RUSTFS_COMPRESSION_ENABLED", "true");
    cluster.start().await?;

    let bucket = "inline-compressed-fallback";
    cluster.create_test_bucket(bucket).await?;
    let client = cluster.create_s3_client(0)?;
    let key = "compressed/repeated.txt";
    let body = compressible_payload(64 * KIB);
    let put = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await?;
    assert_reader_path(
        &collector,
        &client,
        ReaderPathExpectation::with_size_bucket(
            ReaderObject::new(bucket, key, &body, put.e_tag(), None),
            LEGACY_DUPLEX,
            COMPRESSED,
            size_bucket(4 * KIB),
        ),
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn four_node_multipart_ignores_disk_compression_fallback() -> TestResult {
    init_logging();

    let collector = OtlpMetricCollector::start().await?;
    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    configure_reader_metric_cluster(&mut cluster, &collector);
    cluster.set_env("RUSTFS_COMPRESSION_ENABLED", "true");
    cluster.start().await?;

    let bucket = "inline-multipart-compression-fallback";
    cluster.create_test_bucket(bucket).await?;
    let client = cluster.create_s3_client(0)?;
    let key = "multipart/compression-disabled.txt";
    let (body, second_part, etag) = put_two_part_multipart(&client, bucket, key).await?;

    assert_reader_path(
        &collector,
        &client,
        ReaderPathExpectation::for_class(ReaderObject::new(bucket, key, &body, etag.as_deref(), None), LEGACY_DUPLEX, MULTIPART),
    )
    .await?;
    assert_part_number_reader_path(
        &collector,
        &client,
        PartNumberReaderPathExpectation::new(bucket, key, &second_part, body.len(), MULTIPART, LEGACY_DUPLEX),
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn four_node_mixed_msgpack_compat_mode_preserves_fallback_controls() -> TestResult {
    init_logging();

    let collector = OtlpMetricCollector::start().await?;
    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    let sse_master_key = base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]);
    cluster.set_env("RUSTFS_SSE_S3_MASTER_KEY", sse_master_key);
    cluster.set_env("RUSTFS_COMPRESSION_ENABLED", "true");
    configure_mixed_msgpack_cluster(&mut cluster, &collector)?;
    cluster.start().await?;

    let fallback_before = collector.msgpack_json_fallback_totals().await;

    let bucket = "inline-mixed-msgpack-controls";
    cluster.create_test_bucket(bucket).await?;
    let client = cluster.create_s3_client(0)?;

    let multipart_key = "mixed/multipart.bin";
    let (multipart_body, second_part, multipart_etag) = put_two_part_multipart(&client, bucket, multipart_key).await?;
    assert_reader_path(
        &collector,
        &client,
        ReaderPathExpectation::for_class(
            ReaderObject::new(bucket, multipart_key, &multipart_body, multipart_etag.as_deref(), None),
            LEGACY_DUPLEX,
            MULTIPART,
        ),
    )
    .await?;
    assert_part_number_reader_path(
        &collector,
        &client,
        PartNumberReaderPathExpectation::new(bucket, multipart_key, &second_part, multipart_body.len(), MULTIPART, LEGACY_DUPLEX),
    )
    .await?;
    assert_msgpack_fallback_unchanged(&collector, &fallback_before, &MSGPACK_FALLBACK_CONTROL_SERIES).await?;

    let encrypted_key = "encrypted/sse-s3.bin";
    let encrypted_body = payload(16 * KIB, 0xE3);
    let encrypted_put = client
        .put_object()
        .bucket(bucket)
        .key(encrypted_key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from(encrypted_body.clone()))
        .send()
        .await?;
    let encrypted_head = client.head_object().bucket(bucket).key(encrypted_key).send().await?;
    assert_eq!(
        encrypted_head.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "HEAD must preserve SSE-S3 metadata for {encrypted_key}"
    );
    assert_reader_path(
        &collector,
        &client,
        ReaderPathExpectation::with_any_size_bucket(
            ReaderObject::new(bucket, encrypted_key, &encrypted_body, encrypted_put.e_tag(), None),
            LEGACY_DUPLEX,
            ENCRYPTED,
        ),
    )
    .await?;

    let compressed_key = "compressed/repeated.txt";
    let compressed_body = compressible_payload(64 * KIB);
    let compressed_put = client
        .put_object()
        .bucket(bucket)
        .key(compressed_key)
        .body(ByteStream::from(compressed_body.clone()))
        .send()
        .await?;
    assert_reader_path(
        &collector,
        &client,
        ReaderPathExpectation::with_any_size_bucket(
            ReaderObject::new(bucket, compressed_key, &compressed_body, compressed_put.e_tag(), None),
            LEGACY_DUPLEX,
            COMPRESSED,
        ),
    )
    .await?;

    assert_msgpack_fallback_unchanged(&collector, &fallback_before, &MSGPACK_FALLBACK_CONTROL_SERIES).await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn four_node_add_tier_committed_replay_converges() -> TestResult {
    init_logging();

    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "inlineconcurrentcoldadmin".to_string();
    cold.secret_key = "inlineconcurrentcoldsecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    cold.create_s3_client().create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestClusterEnvironment::new(4).await?;
    hot.start().await?;

    add_rustfs_tier(&hot, &cold).await?;
    wait_for_tier_converged(&hot, "committed AddTier replay").await
}

#[tokio::test]
#[serial]
async fn four_node_mixed_msgpack_compat_mode_preserves_fallback_controls_during_transition() -> TestResult {
    init_logging();

    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "inlinecoldadmin".to_string();
    cold.secret_key = "inlinecoldsecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let collector = OtlpMetricCollector::start().await?;
    let mut hot = RustFSTestClusterEnvironment::new(4).await?;
    configure_mixed_msgpack_cluster(&mut hot, &collector)?;
    hot.set_env("RUSTFS_SCANNER_CYCLE", "1");
    hot.set_env("RUSTFS_ILM_PROCESS_TIME", "1");

    let sse_master_key = base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]);
    hot.set_env("RUSTFS_SSE_S3_MASTER_KEY", sse_master_key);
    hot.set_env("RUSTFS_COMPRESSION_ENABLED", "true");
    hot.start().await?;
    let hot_client = hot.create_s3_client(0)?;

    let fallback_before = collector.msgpack_json_fallback_totals().await;

    let tier_name = unique_tier_name();
    add_rustfs_tier(&hot, &cold, &tier_name).await?;
    let bucket = "inline-transitioned-mixed-msgpack-controls";
    hot_client.create_bucket().bucket(bucket).send().await?;
    put_lifecycle_with_transition_retry(&hot_client, bucket, &tier_name).await?;

    let key = "transition/mixed-multipart.bin";
    let (body, second_part, etag) = put_two_part_multipart(&hot_client, bucket, key).await?;
    wait_for_transition(&hot_client, bucket, key, &tier_name).await?;
    assert!(
        cold_tier_object_count(&cold_client).await? >= 1,
        "cold-tier bucket must hold transitioned objects"
    );
    assert_reader_path(
        &collector,
        &hot_client,
        ReaderPathExpectation::for_class(ReaderObject::new(bucket, key, &body, etag.as_deref(), None), REMOTE_TRANSITION, REMOTE),
    )
    .await?;
    assert_part_number_reader_path(
        &collector,
        &hot_client,
        PartNumberReaderPathExpectation::new(bucket, key, &second_part, body.len(), REMOTE, REMOTE_TRANSITION),
    )
    .await?;

    let encrypted_key = "transition/encrypted-sse.bin";
    let encrypted_body = payload(16 * KIB, 0xAB);
    let encrypted_put = hot_client
        .put_object()
        .bucket(bucket)
        .key(encrypted_key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from(encrypted_body.clone()))
        .send()
        .await?;
    let encrypted_head = hot_client.head_object().bucket(bucket).key(encrypted_key).send().await?;
    assert_eq!(
        encrypted_head.server_side_encryption(),
        Some(&ServerSideEncryption::Aes256),
        "HEAD must preserve SSE-S3 metadata for {encrypted_key}"
    );
    assert_reader_path(
        &collector,
        &hot_client,
        ReaderPathExpectation::with_any_size_bucket(
            ReaderObject::new(bucket, encrypted_key, &encrypted_body, encrypted_put.e_tag(), None),
            LEGACY_DUPLEX,
            ENCRYPTED,
        ),
    )
    .await?;

    let compressed_key = "transition/compressed-repeated.txt";
    let compressed_body = compressible_payload(64 * KIB);
    let compressed_put = hot_client
        .put_object()
        .bucket(bucket)
        .key(compressed_key)
        .body(ByteStream::from(compressed_body.clone()))
        .send()
        .await?;
    assert_reader_path(
        &collector,
        &hot_client,
        ReaderPathExpectation::with_any_size_bucket(
            ReaderObject::new(bucket, compressed_key, &compressed_body, compressed_put.e_tag(), None),
            LEGACY_DUPLEX,
            COMPRESSED,
        ),
    )
    .await?;

    assert_msgpack_fallback_unchanged(&collector, &fallback_before, &MSGPACK_FALLBACK_CONTROL_SERIES).await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn four_node_transitioned_inline_fallback() -> TestResult {
    init_logging();

    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "inlinecoldadmin".to_string();
    cold.secret_key = "inlinecoldsecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let collector = OtlpMetricCollector::start().await?;
    let mut hot = RustFSTestClusterEnvironment::new(4).await?;
    configure_reader_metric_cluster(&mut hot, &collector);
    hot.set_env("RUSTFS_SCANNER_CYCLE", "1");
    hot.set_env("RUSTFS_ILM_PROCESS_TIME", "1");
    hot.start().await?;
    let hot_client = hot.create_s3_client(0)?;

    let tier_name = unique_tier_name();
    add_rustfs_tier(&hot, &cold, &tier_name).await?;
    let bucket = "inline-transitioned-fallback";
    hot_client.create_bucket().bucket(bucket).send().await?;
    put_lifecycle_with_transition_retry(&hot_client, bucket, &tier_name).await?;

    let key = "transition/two-part.bin";
    let (body, _, etag) = put_two_part_multipart(&hot_client, bucket, key).await?;
    wait_for_transition(&hot_client, bucket, key, &tier_name).await?;
    assert!(
        cold_tier_object_count(&cold_client).await? >= 1,
        "cold-tier bucket must hold the transitioned object"
    );
    assert_reader_path(
        &collector,
        &hot_client,
        ReaderPathExpectation::for_class(ReaderObject::new(bucket, key, &body, etag.as_deref(), None), REMOTE_TRANSITION, REMOTE),
    )
    .await?;

    Ok(())
}
