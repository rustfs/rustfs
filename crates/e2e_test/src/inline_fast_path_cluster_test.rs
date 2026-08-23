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
use opentelemetry_proto::tonic::metrics::v1::{
    Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric, number_data_point,
};
use prost::Message;
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
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
/// Physical bytes the erasure layer pulled from disk, emitted per shard read by
/// `crates/ecstore/src/erasure/coding/decode.rs`.
const SHARD_READ_BYTES_COUNTER: &str = "rustfs_io_get_object_shard_read_observed_bytes_total";
const MSGPACK_JSON_DECODE_COUNTER: &str = "rustfs_system_network_internode_msgpack_json_decode_total";
const MSGPACK_JSON_FALLBACK_COUNTER: &str = "rustfs_system_network_internode_msgpack_json_fallback_total";
const MSGPACK_JSON_DECODE_ERROR_COUNTER: &str = "rustfs_system_network_internode_msgpack_json_decode_error_total";
const DIRECTION_LABEL: &str = "direction";
const MESSAGE_LABEL: &str = "message";
const CODEC_LABEL: &str = "codec";
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
const MSGPACK_CODEC_MSGPACK: &str = "msgpack";
const MSGPACK_CODEC_JSON: &str = "json";
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
const MSGPACK_DECODE_ERROR_CODECS: [&str; 2] = [MSGPACK_CODEC_MSGPACK, MSGPACK_CODEC_JSON];
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
    decode_values: MetricValues,
    fallback_values: MetricValues,
    decode_error_values: MetricValues,
    shard_read_values: MetricValues,
    task: JoinHandle<()>,
}

impl OtlpMetricCollector {
    async fn start() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/v1/metrics", listener.local_addr()?);
        let values = Arc::new(Mutex::new(BTreeMap::new()));
        let decode_values = Arc::new(Mutex::new(BTreeMap::new()));
        let fallback_values = Arc::new(Mutex::new(BTreeMap::new()));
        let decode_error_values = Arc::new(Mutex::new(BTreeMap::new()));
        let shard_read_values = Arc::new(Mutex::new(BTreeMap::new()));
        let task_values = values.clone();
        let task_decode_values = decode_values.clone();
        let task_fallback_values = fallback_values.clone();
        let task_decode_error_values = decode_error_values.clone();
        let task_shard_read_values = shard_read_values.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let values = task_values.clone();
                let decode_values = task_decode_values.clone();
                let fallback_values = task_fallback_values.clone();
                let decode_error_values = task_decode_error_values.clone();
                let shard_read_values = task_shard_read_values.clone();
                tokio::spawn(async move {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(
                            TokioIo::new(stream),
                            service_fn(move |request| {
                                handle_metric_export(
                                    request,
                                    values.clone(),
                                    decode_values.clone(),
                                    fallback_values.clone(),
                                    decode_error_values.clone(),
                                    shard_read_values.clone(),
                                )
                            }),
                        )
                        .await;
                });
            }
        });
        Ok(Self {
            endpoint,
            values,
            decode_values,
            fallback_values,
            decode_error_values,
            shard_read_values,
            task,
        })
    }

    /// Total physical bytes read from disk across every shard-read label set.
    async fn shard_read_bytes_total(&self) -> u64 {
        self.shard_read_values
            .lock()
            .await
            .values()
            .map(|versions| versions.values().map(|(_, value)| *value).sum::<u64>())
            .sum()
    }

    /// Waits until the shard-read counter stops advancing so a measurement window
    /// is not polluted by exports still in flight.
    ///
    /// Requires several consecutive equal samples spanning more than one export
    /// interval (`RUSTFS_OBS_METER_INTERVAL=1`): a single unchanged sample only
    /// proves the latest export has not landed yet, which silently reads as "no
    /// disk reads happened" and makes any upper-bound assertion vacuous.
    async fn wait_for_shard_read_bytes_to_settle(&self) -> TestResult<u64> {
        const REQUIRED_STABLE_SAMPLES: usize = 5;
        let mut last = self.shard_read_bytes_total().await;
        let mut stable = 0;
        for _ in 0..60 {
            sleep(Duration::from_millis(500)).await;
            let current = self.shard_read_bytes_total().await;
            if current == last {
                stable += 1;
                if stable >= REQUIRED_STABLE_SAMPLES {
                    return Ok(current);
                }
            } else {
                stable = 0;
                last = current;
            }
        }
        Err("timed out waiting for shard-read byte counter to settle".into())
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

    async fn msgpack_json_decode_totals(&self) -> BTreeMap<String, u64> {
        self.decode_values
            .lock()
            .await
            .iter()
            .map(|(key, points)| (key.clone(), points.values().map(|(_, value)| value).sum()))
            .collect()
    }

    async fn msgpack_json_decode_error_totals(&self) -> BTreeMap<String, u64> {
        self.decode_error_values
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
    decode_values: MetricValues,
    fallback_values: MetricValues,
    decode_error_values: MetricValues,
    shard_read_values: MetricValues,
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
            let mut decode_values = decode_values.lock().await;
            let mut fallback_values = fallback_values.lock().await;
            let mut decode_error_values = decode_error_values.lock().await;
            let mut shard_read_values = shard_read_values.lock().await;
            record_reader_path_metrics(&export, &mut values);
            record_shard_read_bytes_metrics(&export, &mut shard_read_values);
            record_msgpack_decode_metrics(&export, &mut decode_values);
            record_msgpack_fallback_metrics(&export, &mut fallback_values);
            record_msgpack_decode_error_metrics(&export, &mut decode_error_values);
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

/// Accumulates `SHARD_READ_BYTES_COUNTER` across all label sets. Only the total
/// matters: it is the number of physical bytes the erasure layer actually pulled
/// from disk, which is what separates a bounded per-part read from a decode of
/// the whole object.
fn record_shard_read_bytes_metrics(export: &ExportMetricsServiceRequest, values: &mut BTreeMap<String, MetricPointVersions>) {
    for resource_metrics in &export.resource_metrics {
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                if metric.name != SHARD_READ_BYTES_COUNTER {
                    continue;
                }
                let Some(metric::Data::Sum(sum)) = &metric.data else {
                    continue;
                };
                for point in &sum.data_points {
                    let Some(number_data_point::Value::AsInt(value)) = point.value.as_ref() else {
                        continue;
                    };
                    let value = u64::try_from(*value).unwrap_or_default();
                    // Keyed by labels, not by position: point order within an export
                    // is not guaranteed stable, so an index key would alias distinct
                    // series across batches.
                    let key = format!(
                        "{}\u{1f}{}\u{1f}{}",
                        attribute_string(&point.attributes, "path").unwrap_or_default(),
                        attribute_string(&point.attributes, "role").unwrap_or_default(),
                        attribute_string(&point.attributes, "outcome").unwrap_or_default(),
                    );
                    values
                        .entry(key)
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
    record_msgpack_counter_metrics(export, MSGPACK_JSON_FALLBACK_COUNTER, values, |attributes| {
        let direction = attribute_string(attributes, DIRECTION_LABEL)?;
        let message = attribute_string(attributes, MESSAGE_LABEL)?;
        Some(msgpack_fallback_metric_key(direction, message))
    });
}

fn msgpack_fallback_metric_key(direction: &str, message: &str) -> String {
    format!("{direction}\u{1f}{message}")
}

fn record_msgpack_decode_metrics(export: &ExportMetricsServiceRequest, values: &mut BTreeMap<String, MetricPointVersions>) {
    record_msgpack_counter_metrics(export, MSGPACK_JSON_DECODE_COUNTER, values, |attributes| {
        let direction = attribute_string(attributes, DIRECTION_LABEL)?;
        let message = attribute_string(attributes, MESSAGE_LABEL)?;
        let codec = attribute_string(attributes, CODEC_LABEL)?;
        Some(msgpack_decode_metric_key(direction, message, codec))
    });
}

fn msgpack_decode_metric_key(direction: &str, message: &str, codec: &str) -> String {
    format!("{direction}\u{1f}{message}\u{1f}{codec}")
}

fn record_msgpack_decode_error_metrics(export: &ExportMetricsServiceRequest, values: &mut BTreeMap<String, MetricPointVersions>) {
    record_msgpack_counter_metrics(export, MSGPACK_JSON_DECODE_ERROR_COUNTER, values, |attributes| {
        let direction = attribute_string(attributes, DIRECTION_LABEL)?;
        let message = attribute_string(attributes, MESSAGE_LABEL)?;
        let codec = attribute_string(attributes, CODEC_LABEL)?;
        Some(msgpack_decode_error_metric_key(direction, message, codec))
    });
}

fn record_msgpack_counter_metrics<F>(
    export: &ExportMetricsServiceRequest,
    counter_name: &str,
    values: &mut BTreeMap<String, MetricPointVersions>,
    metric_key: F,
) where
    F: Fn(&[KeyValue]) -> Option<String>,
{
    for resource_metrics in &export.resource_metrics {
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                if metric.name != counter_name {
                    continue;
                }
                let Some(metric::Data::Sum(sum)) = &metric.data else {
                    continue;
                };
                for point in &sum.data_points {
                    let Some(key) = metric_key(&point.attributes) else {
                        continue;
                    };
                    let Some(number_data_point::Value::AsInt(value)) = point.value.as_ref() else {
                        continue;
                    };
                    let value = u64::try_from(*value).unwrap_or_default();
                    values
                        .entry(key)
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

fn msgpack_decode_error_metric_key(direction: &str, message: &str, codec: &str) -> String {
    format!("{direction}\u{1f}{message}\u{1f}{codec}")
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

async fn assert_msgpack_decode_observed(collector: &OtlpMetricCollector, before: &BTreeMap<String, u64>) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let after = collector.msgpack_json_decode_totals().await;
        let missing = [FALLBACK_REQUEST_DIRECTION, FALLBACK_RESPONSE_DIRECTION]
            .iter()
            .filter_map(|direction| {
                let prefix = format!("{direction}\u{1f}");
                let suffix = format!("\u{1f}{MSGPACK_CODEC_MSGPACK}");
                let before_total = before
                    .iter()
                    .filter(|(key, _)| key.starts_with(&prefix) && key.ends_with(&suffix))
                    .map(|(_, value)| *value)
                    .sum::<u64>();
                let after_total = after
                    .iter()
                    .filter(|(key, _)| key.starts_with(&prefix) && key.ends_with(&suffix))
                    .map(|(_, value)| *value)
                    .sum::<u64>();
                (after_total <= before_total).then_some(format!("{direction}/{}", MSGPACK_CODEC_MSGPACK))
            })
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for msgpack decode traffic for {missing:?}; totals={after:?}").into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn assert_msgpack_decode_errors_unchanged(
    collector: &OtlpMetricCollector,
    before: &BTreeMap<String, u64>,
    series: &[(&str, &str)],
) -> TestResult {
    let after = collector.msgpack_json_decode_error_totals().await;
    for &(direction, message) in series {
        for &codec in &MSGPACK_DECODE_ERROR_CODECS {
            let key = msgpack_decode_error_metric_key(direction, message, codec);
            assert_eq!(
                before.get(&key).copied().unwrap_or_default(),
                after.get(&key).copied().unwrap_or_default(),
                "decode-error counter changed for {direction}/{message}/{codec}: before={:?}, after={:?}",
                before.get(&key),
                after.get(&key),
            );
        }
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

#[test]
fn records_msgpack_decode_metric_with_codec_label() {
    let export = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: MSGPACK_JSON_DECODE_COUNTER.to_string(),
                    data: Some(metric::Data::Sum(Sum {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![
                                metric_attribute(DIRECTION_LABEL, FALLBACK_RESPONSE_DIRECTION),
                                metric_attribute(MESSAGE_LABEL, "ReadMultipleResp"),
                                metric_attribute(CODEC_LABEL, MSGPACK_CODEC_MSGPACK),
                            ],
                            start_time_unix_nano: 7,
                            time_unix_nano: 11,
                            value: Some(number_data_point::Value::AsInt(5)),
                            ..Default::default()
                        }],
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut values = BTreeMap::new();

    record_msgpack_decode_metrics(&export, &mut values);

    let key = msgpack_decode_metric_key(FALLBACK_RESPONSE_DIRECTION, "ReadMultipleResp", MSGPACK_CODEC_MSGPACK);
    assert_eq!(values.get(&key).and_then(|points| points.get(&7)).copied(), Some((11, 5)));
}

#[test]
fn records_msgpack_fallback_metric_with_direction_and_message_labels() {
    let export = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: MSGPACK_JSON_FALLBACK_COUNTER.to_string(),
                    data: Some(metric::Data::Sum(Sum {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![
                                metric_attribute(DIRECTION_LABEL, FALLBACK_RESPONSE_DIRECTION),
                                metric_attribute(MESSAGE_LABEL, "RenameDataResp"),
                            ],
                            start_time_unix_nano: 7,
                            time_unix_nano: 11,
                            value: Some(number_data_point::Value::AsInt(14)),
                            ..Default::default()
                        }],
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut values = BTreeMap::new();

    record_msgpack_fallback_metrics(&export, &mut values);

    let key = msgpack_fallback_metric_key(FALLBACK_RESPONSE_DIRECTION, "RenameDataResp");
    assert_eq!(values.get(&key).and_then(|points| points.get(&7)).copied(), Some((11, 14)));
}

#[test]
fn records_msgpack_decode_error_metric_with_codec_label() {
    let export = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: MSGPACK_JSON_DECODE_ERROR_COUNTER.to_string(),
                    data: Some(metric::Data::Sum(Sum {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![
                                metric_attribute(DIRECTION_LABEL, FALLBACK_REQUEST_DIRECTION),
                                metric_attribute(MESSAGE_LABEL, "ReadMultipleReq"),
                                metric_attribute(CODEC_LABEL, MSGPACK_CODEC_JSON),
                            ],
                            start_time_unix_nano: 7,
                            time_unix_nano: 11,
                            value: Some(number_data_point::Value::AsInt(3)),
                            ..Default::default()
                        }],
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut values = BTreeMap::new();

    record_msgpack_decode_error_metrics(&export, &mut values);

    let key = msgpack_decode_error_metric_key(FALLBACK_REQUEST_DIRECTION, "ReadMultipleReq", MSGPACK_CODEC_JSON);
    assert_eq!(values.get(&key).and_then(|points| points.get(&7)).copied(), Some((11, 3)));
}

fn metric_attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(AnyValue::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
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
    let response = submit_rustfs_tier(hot, cold, tier_name).await?;
    wait_for_tier_verifiable(hot, tier_name, &response).await
}

async fn submit_rustfs_tier(
    hot: &RustFSTestClusterEnvironment,
    cold: &RustFSTestEnvironment,
    tier_name: &str,
) -> TestResult<String> {
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
            return Ok(format!("status={status}, body={response}"));
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

async fn wait_for_tier_converged(hot: &RustFSTestClusterEnvironment, tier_name: &str, add_tier_responses: &str) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(60);
    let final_error = loop {
        let snapshot = tier_readiness_snapshot(hot, tier_name).await?;
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
        "tier {tier_name} did not converge on every hot node within 60s after AddTier({add_tier_responses}): {final_error}"
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

async fn start_manual_transition_job(hot: &RustFSTestClusterEnvironment, bucket: &str) -> TestResult<String> {
    let path = format!("/rustfs/admin/v3/ilm/transition/run?bucket={bucket}&async=true&dryRun=true&maxObjects=1");
    let (status, response) =
        signed_admin_request(&hot.nodes[0].url, Method::POST, &path, None, &hot.access_key, &hot.secret_key).await?;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "async manual transition run must be accepted: {}",
        compact_body(&response)
    );
    let value: serde_json::Value = serde_json::from_str(&response)?;
    assert_eq!(
        value["state"].as_str(),
        Some("accepted"),
        "manual transition run state changed: {response}"
    );
    assert_eq!(
        value["mode"].as_str(),
        Some("durable_job"),
        "manual transition run mode changed: {response}"
    );
    value["job_id"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("manual transition run response omitted job_id: {response}").into())
}

async fn start_manual_transition_job_on_node(
    hot: &RustFSTestClusterEnvironment,
    node_index: usize,
    bucket: &str,
    prefix: &str,
    tier_name: &str,
    dry_run: bool,
    max_objects: u64,
) -> TestResult<(StatusCode, String)> {
    let bucket = urlencoding::encode(bucket);
    let prefix = urlencoding::encode(prefix);
    let tier = urlencoding::encode(tier_name);
    let path = format!(
        "/rustfs/admin/v3/ilm/transition/run?bucket={bucket}&prefix={prefix}&tier={tier}&dryRun={dry_run}&maxObjects={max_objects}&mode=async"
    );
    signed_admin_request(&hot.nodes[node_index].url, Method::POST, &path, None, &hot.access_key, &hot.secret_key).await
}

async fn read_manual_transition_job_status_endpoint(
    hot: &RustFSTestClusterEnvironment,
    node_index: usize,
    status_endpoint: &str,
) -> TestResult<serde_json::Value> {
    let (status, response) = signed_admin_request(
        &hot.nodes[node_index].url,
        Method::GET,
        status_endpoint,
        None,
        &hot.access_key,
        &hot.secret_key,
    )
    .await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "manual transition job status endpoint was not readable on node {node_index}: {}",
        compact_body(&response)
    );
    Ok(serde_json::from_str(&response)?)
}

async fn wait_for_manual_transition_job_terminal(
    hot: &RustFSTestClusterEnvironment,
    node_index: usize,
    job_id: &str,
    retry_missing: bool,
) -> TestResult<serde_json::Value> {
    let path = format!("/rustfs/admin/v3/ilm/transition/jobs/{job_id}");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (status, response) =
            signed_admin_request(&hot.nodes[node_index].url, Method::GET, &path, None, &hot.access_key, &hot.secret_key).await?;
        if retry_missing && status == StatusCode::NOT_FOUND && Instant::now() < deadline {
            sleep(Duration::from_millis(200)).await;
            continue;
        }
        assert_eq!(
            status,
            StatusCode::OK,
            "manual transition job status read failed: {}",
            compact_body(&response)
        );
        let value: serde_json::Value = serde_json::from_str(&response)?;
        match value["status"].as_str() {
            Some("running") if Instant::now() < deadline => sleep(Duration::from_millis(200)).await,
            Some("running") => {
                return Err(format!("manual transition job {job_id} stayed running for 30s: {response}").into());
            }
            Some(_) => return Ok(value),
            None => return Err(format!("manual transition job status response omitted status: {response}").into()),
        }
    }
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
async fn four_node_empty_legacy_volumes_start_as_fresh() -> TestResult {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    for data_dir in cluster.nodes.iter().flat_map(|node| &node.data_dirs) {
        tokio::fs::create_dir_all(Path::new(data_dir).join(".minio.sys")).await?;
    }

    cluster.start().await?;

    // Starting is not the assertion. The regression is that an empty legacy
    // `.minio.sys` must be classified as a *fresh* volume, not as an existing
    // MinIO deployment to adopt or migrate. Pin what that classification leaves
    // on disk and in the namespace.
    let buckets = cluster.create_s3_client(0)?.list_buckets().send().await?;
    assert!(
        buckets.buckets().is_empty(),
        "a fresh classification must not adopt buckets from the pre-existing directories, got {:?}",
        buckets.buckets().iter().filter_map(|b| b.name()).collect::<Vec<_>>()
    );

    for data_dir in cluster.nodes.iter().flat_map(|node| &node.data_dirs) {
        assert!(
            Path::new(data_dir).join(".rustfs.sys").join("format.json").is_file(),
            "each drive must be formatted as fresh: {data_dir} has no .rustfs.sys/format.json"
        );
        let mut legacy = tokio::fs::read_dir(Path::new(data_dir).join(".minio.sys")).await?;
        assert!(
            legacy.next_entry().await?.is_none(),
            "the empty legacy directory must be left untouched, not migrated into: {data_dir}"
        );
    }

    Ok(())
}

#[tokio::test]
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
        ReaderPathExpectation::for_class(ReaderObject::new(bucket, key, &body, put.e_tag(), None), LEGACY_DUPLEX, COMPRESSED),
    )
    .await?;

    Ok(())
}

/// Multipart disk compression is live again, so a compression-enabled cluster classifies multipart objects as compressed and the roundtrip (full GET plus partNumber GET) must still return the original bytes.
/// Reverting the multipart compression fix must fail this test.
#[tokio::test]
async fn four_node_multipart_disk_compression_roundtrip() -> TestResult {
    init_logging();

    let collector = OtlpMetricCollector::start().await?;
    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    configure_reader_metric_cluster(&mut cluster, &collector);
    cluster.set_env("RUSTFS_COMPRESSION_ENABLED", "true");
    cluster.set_env("RUSTFS_COMPRESSION_MULTIPART_ENABLED", "true");
    cluster.start().await?;

    let bucket = "inline-multipart-compression-roundtrip";
    cluster.create_test_bucket(bucket).await?;
    let client = cluster.create_s3_client(0)?;
    let key = "multipart/compressed.txt";
    let (body, second_part, etag) = put_two_part_multipart(&client, bucket, key).await?;

    assert_reader_path(
        &collector,
        &client,
        ReaderPathExpectation::for_class(ReaderObject::new(bucket, key, &body, etag.as_deref(), None), LEGACY_DUPLEX, COMPRESSED),
    )
    .await?;
    assert_part_number_reader_path(
        &collector,
        &client,
        PartNumberReaderPathExpectation::new(bucket, key, &second_part, body.len(), COMPRESSED, LEGACY_DUPLEX),
    )
    .await?;

    Ok(())
}

/// A tail range over a compressed multipart object must read only the physical
/// data it needs, not decode the object from byte zero.
///
/// The byte-exactness tests around this one stay green even if the seek path
/// regresses into decoding from the start of the object: the bytes returned are
/// still correct, only the read amplification explodes. This asserts the cost
/// side, using `SHARD_READ_BYTES_COUNTER` — already emitted per shard read by the
/// erasure layer, so no production code is instrumented for the test.
///
/// `get_compressed_offsets` skips whole preceding parts by their stored size and
/// then seeks inside the covering part via its compression index, so a bounded
/// read costs on the order of the covering part's block size against a ~5 MiB
/// object.
#[tokio::test]
async fn four_node_compressed_multipart_tail_range_reads_are_bounded() -> TestResult {
    init_logging();

    let collector = OtlpMetricCollector::start().await?;
    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    configure_reader_metric_cluster(&mut cluster, &collector);
    cluster.set_env("RUSTFS_COMPRESSION_ENABLED", "true");
    cluster.set_env("RUSTFS_COMPRESSION_MULTIPART_ENABLED", "true");
    cluster.start().await?;

    let bucket = "inline-multipart-compression-tail-range";
    cluster.create_test_bucket(bucket).await?;
    let client = cluster.create_s3_client(0)?;
    let key = "multipart/tail-range.txt";
    let (body, _second_part, etag) = put_two_part_multipart(&client, bucket, key).await?;

    // Establish that the object really took the compressed read path; otherwise a
    // small delta below would only prove compression never happened.
    assert_reader_path(
        &collector,
        &client,
        ReaderPathExpectation::for_class(ReaderObject::new(bucket, key, &body, etag.as_deref(), None), LEGACY_DUPLEX, COMPRESSED),
    )
    .await?;

    let baseline = collector.wait_for_shard_read_bytes_to_settle().await?;

    let tail_len = 4 * KIB;
    let start = body.len() - tail_len;
    let end = body.len() - 1;
    let range = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(format!("bytes={start}-{end}"))
        .send()
        .await?;
    let tail = range.body.collect().await?.into_bytes();
    assert_eq!(tail.as_ref(), &body[start..], "tail range returned wrong bytes");

    let after = collector.wait_for_shard_read_bytes_to_settle().await?;
    let read_bytes = after.saturating_sub(baseline);

    // A zero delta means the window caught nothing — an unexported counter, or a
    // read served without touching the erasure layer — which would make the upper
    // bound vacuously true. Fail instead of passing blind.
    assert!(
        read_bytes > 0,
        "no shard reads observed for the tail range; the budget assertion below would be vacuous"
    );

    // Part 1 alone is MPU_PART_1_SIZE, so a whole-object decode cannot come in
    // under it. Half the logical size leaves generous headroom for erasure padding
    // and unrelated background reads while still failing loudly on a full decode.
    let budget = (body.len() / 2) as u64;
    assert!(
        read_bytes < budget,
        "tail range read {read_bytes} physical bytes for a {tail_len}-byte range (budget {budget}, object {} bytes): \
         the read is not bounded to the covering part",
        body.len()
    );

    Ok(())
}

#[tokio::test]
async fn four_node_mixed_msgpack_compat_mode_preserves_fallback_controls() -> TestResult {
    init_logging();

    let collector = OtlpMetricCollector::start().await?;
    let mut cluster = RustFSTestClusterEnvironment::new(4).await?;
    let sse_master_key = base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]);
    cluster.set_env("RUSTFS_SSE_S3_MASTER_KEY", sse_master_key);
    cluster.set_env("RUSTFS_COMPRESSION_ENABLED", "true");
    cluster.set_env("RUSTFS_COMPRESSION_MULTIPART_ENABLED", "true");
    configure_mixed_msgpack_cluster(&mut cluster, &collector)?;
    cluster.start().await?;

    let decode_before = collector.msgpack_json_decode_totals().await;
    let fallback_before = collector.msgpack_json_fallback_totals().await;
    let decode_errors_before = collector.msgpack_json_decode_error_totals().await;

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
            COMPRESSED,
        ),
    )
    .await?;
    assert_part_number_reader_path(
        &collector,
        &client,
        PartNumberReaderPathExpectation::new(
            bucket,
            multipart_key,
            &second_part,
            multipart_body.len(),
            COMPRESSED,
            LEGACY_DUPLEX,
        ),
    )
    .await?;
    assert_msgpack_decode_observed(&collector, &decode_before).await?;
    assert_msgpack_fallback_unchanged(&collector, &fallback_before, &MSGPACK_FALLBACK_CONTROL_SERIES).await?;
    assert_msgpack_decode_errors_unchanged(&collector, &decode_errors_before, &MSGPACK_FALLBACK_CONTROL_SERIES).await?;

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
    assert_msgpack_decode_errors_unchanged(&collector, &decode_errors_before, &MSGPACK_FALLBACK_CONTROL_SERIES).await?;

    Ok(())
}

#[tokio::test]
async fn four_node_add_tier_converges() -> TestResult {
    init_logging();

    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "inlineconcurrentcoldadmin".to_string();
    cold.secret_key = "inlineconcurrentcoldsecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    cold.create_s3_client().create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestClusterEnvironment::new(4).await?;
    hot.start().await?;

    let tier_name = unique_tier_name();
    add_rustfs_tier(&hot, &cold, &tier_name).await?;
    wait_for_tier_converged(&hot, &tier_name, "AddTier convergence").await
}

#[tokio::test]
async fn four_node_add_tier_converges_after_offline_node_restart_without_second_mutation() -> TestResult {
    init_logging();

    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "inlineofflinecoldadmin".to_string();
    cold.secret_key = "inlineofflinecoldsecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    cold.create_s3_client().create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestClusterEnvironment::new(4).await?;
    hot.start().await?;

    let tier_name = unique_tier_name();
    let add_tier_response = submit_rustfs_tier(&hot, &cold, &tier_name).await?;
    hot.stop_node(3)?;
    hot.start_node(3).await?;

    wait_for_tier_converged(&hot, &tier_name, &add_tier_response).await
}

#[tokio::test]
async fn four_node_manual_transition_job_status_survives_node_restart() -> TestResult {
    init_logging();

    let mut hot = RustFSTestClusterEnvironment::new(4).await?;
    hot.start().await?;

    let hot_client = hot.create_s3_client(0)?;
    let bucket = format!("manual-transition-job-{}", Uuid::new_v4().simple());
    hot_client.create_bucket().bucket(&bucket).send().await?;

    let job_id = start_manual_transition_job(&hot, &bucket).await?;
    let terminal = wait_for_manual_transition_job_terminal(&hot, 0, &job_id, false).await?;
    assert_eq!(
        terminal["status"].as_str(),
        Some("completed"),
        "dry-run manual transition job should complete: {terminal}"
    );
    assert_eq!(terminal["job_id"].as_str(), Some(job_id.as_str()));
    assert_eq!(terminal["bucket"].as_str(), Some(bucket.as_str()));
    assert_eq!(terminal["dry_run"].as_bool(), Some(true));

    hot.stop_node(3)?;
    hot.start_node(3).await?;
    let after_restart = wait_for_manual_transition_job_terminal(&hot, 3, &job_id, true).await?;
    assert_eq!(after_restart["status"], terminal["status"], "terminal job status changed after restart");
    assert_eq!(after_restart["job_id"].as_str(), Some(job_id.as_str()));
    assert_eq!(after_restart["bucket"].as_str(), Some(bucket.as_str()));
    assert_eq!(after_restart["dry_run"].as_bool(), Some(true));

    let job_endpoint = format!("/rustfs/admin/v3/ilm/transition/jobs/{job_id}");
    let (cancel_status, cancel_body) =
        signed_admin_request(&hot.nodes[3].url, Method::DELETE, &job_endpoint, None, &hot.access_key, &hot.secret_key).await?;
    assert_eq!(
        cancel_status,
        StatusCode::OK,
        "terminal manual transition job cancel after restart failed: {}",
        compact_body(&cancel_body)
    );
    let cancel_value: serde_json::Value = serde_json::from_str(&cancel_body)?;
    assert_eq!(
        cancel_value["status"], after_restart["status"],
        "terminal status changed after restart cancel"
    );
    assert_eq!(cancel_value["job_id"].as_str(), Some(job_id.as_str()));
    assert_eq!(cancel_value["bucket"].as_str(), Some(bucket.as_str()));
    assert_eq!(cancel_value["dry_run"].as_bool(), Some(true));
    assert_eq!(cancel_value["cancel_requested"].as_bool(), Some(false));

    let missing_job_id = Uuid::new_v4();
    let (missing_status, missing_body) = signed_admin_request(
        &hot.nodes[3].url,
        Method::GET,
        &format!("/rustfs/admin/v3/ilm/transition/jobs/{missing_job_id}"),
        None,
        &hot.access_key,
        &hot.secret_key,
    )
    .await?;
    assert_eq!(
        missing_status,
        StatusCode::NOT_FOUND,
        "unknown manual transition job must remain a 404 after restart: {}",
        compact_body(&missing_body)
    );
    assert!(
        missing_body.contains("<Code>NoSuchKey</Code>") || missing_body.contains("NoSuchKey"),
        "unknown manual transition job should return NoSuchKey, body: {}",
        compact_body(&missing_body)
    );

    Ok(())
}

#[tokio::test]
async fn four_node_manual_transition_distributed_admission_conflict_reports_status_and_backpressure() -> TestResult {
    init_logging();

    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "inlinedistadmissioncoldadmin".to_string();
    cold.secret_key = "inlinedistadmissioncoldsecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestClusterEnvironment::new(4).await?;
    hot.set_env("RUSTFS_SCANNER_ENABLED", "false");
    hot.set_env("RUSTFS_SCANNER_CYCLE", "3600");
    hot.set_env("RUSTFS_MAX_TRANSITION_WORKERS", "1");
    hot.set_env("RUSTFS_TRANSITION_QUEUE_CAPACITY", "1");
    hot.set_env("RUSTFS_TRANSITION_QUEUE_SEND_TIMEOUT_MS", "1");
    hot.start().await?;

    let hot_client = hot.create_s3_client(0)?;
    let tier_name = unique_tier_name();
    add_rustfs_tier(&hot, &cold, &tier_name).await?;

    let bucket = format!("distributed-admission-{}", Uuid::new_v4().simple());
    let prefix = "transition/distributed-admission/";
    hot_client.create_bucket().bucket(&bucket).send().await?;
    put_lifecycle_with_transition_retry(&hot_client, &bucket, &tier_name).await?;
    for index in 0u8..64 {
        let key = format!("{prefix}object-{index:02}.bin");
        hot_client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(payload(1024 * KIB, index)))
            .send()
            .await?;
    }

    let (node0, node1) = tokio::join!(
        start_manual_transition_job_on_node(&hot, 0, &bucket, prefix, &tier_name, false, 64),
        start_manual_transition_job_on_node(&hot, 1, &bucket, prefix, &tier_name, false, 64)
    );
    let responses = [(0usize, node0?), (1usize, node1?)];
    let accepted = responses
        .iter()
        .find(|(_, (status, _))| *status == StatusCode::ACCEPTED)
        .ok_or_else(|| format!("one distributed async run must be accepted: {responses:#?}"))?;
    let conflict = responses
        .iter()
        .find(|(_, (status, _))| *status == StatusCode::CONFLICT)
        .ok_or_else(|| format!("one distributed async run must report conflict: {responses:#?}"))?;
    assert_eq!(
        responses
            .iter()
            .filter(|(_, (status, _))| *status == StatusCode::ACCEPTED)
            .count(),
        1,
        "distributed admission must allow exactly one winner: {responses:#?}"
    );
    assert_eq!(
        responses
            .iter()
            .filter(|(_, (status, _))| *status == StatusCode::CONFLICT)
            .count(),
        1,
        "distributed admission must reject exactly one overlapping contender: {responses:#?}"
    );

    let accepted_body: serde_json::Value = serde_json::from_str(&accepted.1.1)?;
    let conflict_body: serde_json::Value = serde_json::from_str(&conflict.1.1)?;
    let job_id = accepted_body["job_id"]
        .as_str()
        .ok_or_else(|| format!("accepted response omitted job_id: {}", accepted.1.1))?;
    let status_endpoint = accepted_body["status_endpoint"]
        .as_str()
        .ok_or_else(|| format!("accepted response omitted status_endpoint: {}", accepted.1.1))?;
    assert_eq!(accepted_body["state"].as_str(), Some("accepted"));
    assert_eq!(accepted_body["mode"].as_str(), Some("durable_job"));
    assert_eq!(accepted_body["cancel_endpoint"].as_str(), Some(status_endpoint));
    assert_eq!(conflict_body["state"].as_str(), Some("conflict"));
    assert_eq!(conflict_body["mode"].as_str(), Some("durable_job"));
    assert_eq!(conflict_body["active_job_id"].as_str(), Some(job_id));
    assert_eq!(conflict_body["status_endpoint"].as_str(), Some(status_endpoint));
    assert_eq!(conflict_body["cancel_endpoint"].as_str(), Some(status_endpoint));
    assert!(
        conflict_body["scope_key"]
            .as_str()
            .is_some_and(|scope_key| !scope_key.is_empty()),
        "conflict response must expose a readable active scope key: {}",
        conflict.1.1
    );

    let status = read_manual_transition_job_status_endpoint(&hot, conflict.0, status_endpoint).await?;
    assert_eq!(status["job_id"].as_str(), Some(job_id));
    assert_eq!(status["status_endpoint"].as_str(), Some(status_endpoint));

    let terminal = wait_for_manual_transition_job_terminal(&hot, conflict.0, job_id, false).await?;
    assert_eq!(terminal["job_id"].as_str(), Some(job_id));
    assert_eq!(terminal["bucket"].as_str(), Some(bucket.as_str()));
    assert_eq!(terminal["prefix"].as_str(), Some(prefix));
    assert_eq!(terminal["dry_run"].as_bool(), Some(false));
    let terminal_status = terminal["status"].as_str();
    assert!(
        matches!(terminal_status, Some("partial" | "unknown")),
        "small transition queue should surface terminal backpressure: {terminal}"
    );
    if terminal_status == Some("unknown") {
        let failure_reason = terminal["failure_reason"]
            .as_str()
            .ok_or_else(|| format!("unknown terminal status omitted failure_reason: {terminal}"))?;
        assert!(
            failure_reason.contains("worker result was not persisted before the transition queue drained"),
            "unknown terminal status should identify lost worker-result persistence: {terminal}"
        );
    }
    let skipped_queue_full = terminal["report"]["skipped_queue_full"]
        .as_u64()
        .ok_or_else(|| format!("terminal status omitted report.skipped_queue_full: {terminal}"))?;
    assert!(
        skipped_queue_full > 0,
        "terminal status should include queue-full backpressure counters: {terminal}"
    );
    let queue_snapshot = terminal["queue_snapshot"]
        .as_object()
        .ok_or_else(|| format!("terminal status omitted queue_snapshot: {terminal}"))?;
    for field in [
        "queue_capacity",
        "queued",
        "active",
        "workers",
        "queue_full",
        "queue_send_timeout",
    ] {
        assert!(
            queue_snapshot.get(field).and_then(serde_json::Value::as_u64).is_some(),
            "queue_snapshot.{field} must be readable in terminal status: {terminal}"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "manual #1508 evidence harness: starts a 4-node cluster, a remote tier, and an in-flight transition job"]
async fn four_node_manual_transition_rollout_non_empty_restart_readback() -> TestResult {
    init_logging();

    let mut cold = RustFSTestEnvironment::new().await?;
    cold.access_key = "inline1508coldadmin".to_string();
    cold.secret_key = "inline1508coldsecret".to_string();
    cold.start_rustfs_server_without_cleanup(vec![]).await?;
    let cold_client = cold.create_s3_client();
    cold_client.create_bucket().bucket(TIER_BUCKET).send().await?;

    let mut hot = RustFSTestClusterEnvironment::new(4).await?;
    hot.set_env("RUSTFS_SCANNER_ENABLED", "false");
    hot.set_env("RUSTFS_SCANNER_CYCLE", "3600");
    hot.set_env("RUSTFS_MAX_TRANSITION_WORKERS", "2");
    hot.set_env("RUSTFS_TRANSITION_QUEUE_CAPACITY", "64");
    hot.start().await?;
    let hot_client = hot.create_s3_client(0)?;

    let tier_name = unique_tier_name();
    add_rustfs_tier(&hot, &cold, &tier_name).await?;
    let bucket = format!("manual-transition-1508-{}", Uuid::new_v4().simple());
    let prefix = "transition/manual-rollout/";
    hot_client.create_bucket().bucket(&bucket).send().await?;
    put_lifecycle_with_transition_retry(&hot_client, &bucket, &tier_name).await?;
    for index in 0u8..24 {
        let key = format!("{prefix}object-{index:02}.bin");
        hot_client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from(payload(64 * KIB, index)))
            .send()
            .await?;
    }

    let (accepted_status, accepted_body) =
        start_manual_transition_job_on_node(&hot, 1, &bucket, prefix, &tier_name, false, 24).await?;
    assert_eq!(
        accepted_status,
        StatusCode::ACCEPTED,
        "non-empty #1508 transition job should be accepted by a rollout node: {}",
        compact_body(&accepted_body)
    );
    let accepted: serde_json::Value = serde_json::from_str(&accepted_body)?;
    let job_id = accepted["job_id"]
        .as_str()
        .ok_or_else(|| format!("accepted #1508 response omitted job_id: {accepted_body}"))?;
    let status_endpoint = accepted["status_endpoint"]
        .as_str()
        .ok_or_else(|| format!("accepted #1508 response omitted status_endpoint: {accepted_body}"))?;

    let terminal = wait_for_manual_transition_job_terminal(&hot, 0, job_id, false).await?;
    assert_eq!(terminal["job_id"].as_str(), Some(job_id));
    assert_eq!(terminal["bucket"].as_str(), Some(bucket.as_str()));
    assert_eq!(terminal["prefix"].as_str(), Some(prefix));
    assert_eq!(terminal["dry_run"].as_bool(), Some(false));
    assert_eq!(
        terminal["status"].as_str(),
        Some("completed"),
        "non-empty #1508 rollout transition should complete before restart readback: {terminal}"
    );
    let transition_failed: u64 = terminal["report"]
        .get("transition_failed")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    assert_eq!(
        transition_failed, 0,
        "terminal #1508 report should not hide transition failures: {terminal}"
    );
    assert_eq!(
        terminal["report"]["tier_failure"].as_u64(),
        Some(0),
        "terminal #1508 report should not hide tier failures: {terminal}"
    );
    let transition_completed = terminal["report"]["transition_completed"]
        .as_u64()
        .ok_or_else(|| format!("terminal #1508 report omitted transition_completed: {terminal}"))?;
    assert!(
        transition_completed > 0,
        "terminal #1508 report should include real completed transitions: {terminal}"
    );
    assert!(
        cold_tier_object_count(&cold_client).await? > 0,
        "cold tier should contain transitioned #1508 objects"
    );

    hot.stop_node(2)?;
    hot.start_node(2).await?;
    let after_restart = read_manual_transition_job_status_endpoint(&hot, 2, status_endpoint).await?;
    assert_eq!(after_restart["job_id"].as_str(), Some(job_id));
    assert_eq!(
        after_restart["status"], terminal["status"],
        "terminal #1508 status changed after rollout node restart"
    );
    assert_eq!(
        after_restart["report"]["transition_completed"], terminal["report"]["transition_completed"],
        "terminal #1508 transition count changed after rollout node restart"
    );

    Ok(())
}

#[tokio::test]
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

    let decode_before = collector.msgpack_json_decode_totals().await;
    let fallback_before = collector.msgpack_json_fallback_totals().await;
    let decode_errors_before = collector.msgpack_json_decode_error_totals().await;

    let tier_name = unique_tier_name();
    add_rustfs_tier(&hot, &cold, &tier_name).await?;
    let bucket = "inline-transitioned-mixed-msgpack-controls";
    hot_client.create_bucket().bucket(bucket).send().await?;
    put_lifecycle_with_transition_retry(&hot_client, bucket, &tier_name).await?;

    // `.zip` sits on the disk-compression exclusion list: this test pins
    // msgpack compat controls across ILM transition, and a compressed object
    // would classify as `compressed` instead of `remote` (and the warm-tier
    // read path does not decode compression — tracked separately).
    let key = "transition/mixed-multipart.zip";
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
    assert_msgpack_decode_observed(&collector, &decode_before).await?;

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
    assert_msgpack_decode_errors_unchanged(&collector, &decode_errors_before, &MSGPACK_FALLBACK_CONTROL_SERIES).await?;

    Ok(())
}

#[tokio::test]
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
