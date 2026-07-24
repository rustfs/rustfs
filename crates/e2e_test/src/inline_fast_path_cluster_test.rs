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
use walkdir::WalkDir;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
type MetricPointVersions = BTreeMap<u64, (u64, u64)>;
type MetricValues = Arc<Mutex<BTreeMap<String, MetricPointVersions>>>;

const KIB: usize = 1024;
const READER_PATH_COUNTER: &str = "rustfs_io_get_object_reader_path_by_size_total";
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
const MPU_PART_1_SIZE: usize = 5 * 1024 * 1024;
const MPU_PART_2_SIZE: usize = 16 * KIB;
const TIER_NAME: &str = "COLDTIER";
const TIER_BUCKET: &str = "inline-fallback-cold-tier";
const TIER_PREFIX: &str = "tiered";

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
    task: JoinHandle<()>,
}

impl OtlpMetricCollector {
    async fn start() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/v1/metrics", listener.local_addr()?);
        let values = Arc::new(Mutex::new(BTreeMap::new()));
        let task_values = values.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let values = task_values.clone();
                tokio::spawn(async move {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(
                            TokioIo::new(stream),
                            service_fn(move |request| handle_metric_export(request, values.clone())),
                        )
                        .await;
                });
            }
        });
        Ok(Self { endpoint, values, task })
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

async fn handle_metric_export(request: Request<Incoming>, values: MetricValues) -> Result<Response<Full<Bytes>>, Infallible> {
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
            record_reader_path_metrics(&export, &mut values);
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
    expected_size_bucket: &'a str,
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
            expected_size_bucket,
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
            expected_size_bucket,
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
    let mut before = BTreeMap::<&str, u64>::new();
    for path in paths {
        before.insert(path, collector.reader_path_total(path, object_class, expected_size_bucket).await);
    }
    get_and_assert(client, object.bucket, object.key, object.body, object.etag, object.version_id).await?;
    let expected_after = collector
        .wait_for_reader_path_total(expected_path, object_class, expected_size_bucket, before[expected_path] + 1)
        .await?;
    assert!(
        expected_after > before[expected_path],
        "{READER_PATH_COUNTER}{{path={expected_path}}} must advance for {}",
        object.key
    );
    collector
        .wait_for_reader_paths_to_settle(object_class, expected_size_bucket)
        .await?;
    for path in paths {
        if path == expected_path {
            continue;
        }
        if expected_path == EMPTY {
            continue;
        }
        assert_eq!(
            collector.reader_path_total(path, object_class, expected_size_bucket).await,
            before[path],
            "{READER_PATH_COUNTER}{{path={path}, object_class={object_class}, size_bucket={expected_size_bucket}}} must not advance for {}; expected {expected_path} only",
            object.key
        );
    }
    Ok(())
}

async fn assert_part_number_reader_path(
    collector: &OtlpMetricCollector,
    client: &Client,
    bucket: &str,
    key: &str,
    expected_part: &[u8],
    full_object_size: usize,
    expected_path: &str,
) -> TestResult {
    let paths = [INLINE_DIRECT, LEGACY_DUPLEX, EMPTY, REMOTE_TRANSITION];
    let size_bucket = size_bucket(full_object_size);
    let mut before = BTreeMap::<&str, u64>::new();
    for path in paths {
        before.insert(path, collector.reader_path_total(path, MULTIPART, size_bucket).await);
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
        .wait_for_reader_path_total(expected_path, MULTIPART, size_bucket, before[expected_path] + 1)
        .await?;
    collector.wait_for_reader_paths_to_settle(MULTIPART, size_bucket).await?;
    for path in paths {
        if path == expected_path {
            continue;
        }
        assert_eq!(
            collector.reader_path_total(path, MULTIPART, size_bucket).await,
            before[path],
            "{READER_PATH_COUNTER}{{path={path}, object_class={MULTIPART}, size_bucket={size_bucket}}} must not advance for partNumber GET {key}; expected {expected_path} only"
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

async fn add_rustfs_tier(hot: &RustFSTestClusterEnvironment, cold: &RustFSTestEnvironment) -> TestResult {
    let body = serde_json::json!({
        "type": "rustfs",
        "rustfs": {
            "name": TIER_NAME,
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
        if status.is_success() || response.contains("<Code>TierNameAlreadyExist</Code>") {
            wait_for_tier_verifiable(hot).await?;
            return Ok(());
        }
        if Instant::now() >= deadline {
            break format!("status={status}, body={response}");
        }
        sleep(Duration::from_millis(500)).await;
    };
    Err(format!("AddTier(RustFS) failed after readiness polling: {final_error}").into())
}

async fn wait_for_tier_verifiable(hot: &RustFSTestClusterEnvironment) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(30);
    let node = &hot.nodes[0];
    let final_error = loop {
        let (status, response) = signed_admin_request(
            &node.url,
            Method::GET,
            &format!("/rustfs/admin/v3/tier/{TIER_NAME}"),
            None,
            &hot.access_key,
            &hot.secret_key,
        )
        .await?;
        if status.is_success() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            break format!("node {} verify tier failed: status={status}, body={response}", node.url);
        }
        sleep(Duration::from_millis(500)).await;
    };
    Err(format!("tier {TIER_NAME} was not verifiable on the primary hot node within 30s: {final_error}").into())
}

fn transition_rule() -> TestResult<LifecycleRule> {
    Ok(LifecycleRule::builder()
        .id("inline-fallback-transition")
        .filter(LifecycleRuleFilter::builder().prefix("transition/").build())
        .transitions(
            Transition::builder()
                .days(0)
                .storage_class(TransitionStorageClass::from(TIER_NAME))
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()?)
}

async fn wait_for_transition(client: &Client, bucket: &str, key: &str) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let head = client.head_object().bucket(bucket).key(key).send().await?;
        if head.storage_class().map(|storage_class| storage_class.as_str()) == Some(TIER_NAME) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "object {bucket}/{key} was not transitioned to {TIER_NAME} within 90s (storage_class={:?})",
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

async fn put_lifecycle_with_transition_retry(client: &Client, bucket: &str) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let lifecycle = BucketLifecycleConfiguration::builder().rules(transition_rule()?).build()?;
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
        bucket,
        multipart_key,
        &second_part,
        multipart_body.len(),
        LEGACY_DUPLEX,
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
        ReaderPathExpectation::with_size_bucket(
            ReaderObject::new(bucket, encrypted_key, &encrypted_body, encrypted_put.e_tag(), None),
            LEGACY_DUPLEX,
            ENCRYPTED,
            size_bucket(64 * KIB),
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
    assert_part_number_reader_path(&collector, &client, bucket, key, &second_part, body.len(), LEGACY_DUPLEX).await?;

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

    add_rustfs_tier(&hot, &cold).await?;
    let bucket = "inline-transitioned-fallback";
    hot_client.create_bucket().bucket(bucket).send().await?;
    put_lifecycle_with_transition_retry(&hot_client, bucket).await?;

    let key = "transition/two-part.bin";
    let (body, _, etag) = put_two_part_multipart(&hot_client, bucket, key).await?;
    wait_for_transition(&hot_client, bucket, key).await?;
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
