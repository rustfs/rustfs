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

//! SDK responses, audit receipts and HTTP counters for isolated successful PUTs and a missing-object GET.
use super::reference::{SideA, binary_version};
use super::{BoxError, RustFSTestEnvironment, rustfs_binary_path};
use crate::common::admin_request;
use aws_sdk_s3::{operation::RequestId, primitives::ByteStream};
use bytes::Bytes;
use flate2::read::GzDecoder;
use http_body_util::{BodyExt, Empty, Limited};
use hyper::{Method, Request, Response, StatusCode, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use md5::{Digest, Md5};
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{KeyValue, any_value},
    metrics::v1::{metric::Data, number_data_point},
};
use prost::Message;
use rustfs_madmin::metrics::RealtimeMetrics;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

const BODY: &[u8] = b"observability contract";
const HTTP_COUNTER: &str = "rustfs_s3_http_requests_total";
const PUT: &str = "s3:PutObject";
const MAX_EXPORT: usize = 8 * 1024 * 1024;
const CONTRACT: &str = include_str!("../../../gateway-adapter/tests/fixtures/observability-contract.json");

#[derive(Deserialize)]
struct Contract {
    reference_version: String,
    sdk_puts: u64,
    s3_metrics: BTreeMap<String, BTreeSet<String>>,
    audit_fields: Vec<String>,
}

#[derive(Clone, Copy, Default)]
struct HttpCounts {
    put_success: u64,
    get_error: u64,
}

impl HttpCounts {
    fn series(&mut self, method: &str, op: &str, outcome: &str) -> Option<&mut u64> {
        match (method, op, outcome) {
            ("PUT", PUT, "2xx") => Some(&mut self.put_success),
            ("GET", "s3:GetObject", "4xx") => Some(&mut self.get_error),
            _ => None,
        }
    }
}

#[derive(Default)]
struct Observations {
    labels: BTreeMap<String, BTreeSet<String>>,
    exports: usize,
    http: HttpCounts,
    audits: BTreeMap<String, String>,
    error: Option<&'static str>,
}

impl Observations {
    fn audit(&mut self, event: &Value, bucket: &str, contract: &Contract) -> Result<(), &'static str> {
        if event.pointer("/api/name").and_then(Value::as_str) != Some(PUT)
            || event.pointer("/api/bucket").and_then(Value::as_str) != Some(bucket)
        {
            return Ok(());
        }
        if !contract.audit_fields.iter().all(|field| event.pointer(field).is_some()) {
            return Err("successful PUT audit omitted a required field");
        }
        if event.pointer("/api/status_code").and_then(Value::as_u64) != Some(200) {
            return Err("unexpected audited PUT status");
        }
        let id = event["requestID"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or("audit omitted request ID")?;
        let object = event["api"]["object"].as_str().ok_or("audit omitted object")?;
        if self.audits.insert(id.to_owned(), object.to_owned()).is_some() {
            return Err("duplicate PUT audit delivery");
        }
        Ok(())
    }

    fn metrics(&mut self, export: ExportMetricsServiceRequest, contract: &Contract) -> Result<(), &'static str> {
        for metric in export
            .resource_metrics
            .into_iter()
            .flat_map(|r| r.scope_metrics)
            .flat_map(|s| s.metrics)
        {
            if !contract.s3_metrics.contains_key(&metric.name) {
                continue;
            }
            let mut record = |attrs: &[KeyValue]| {
                self.labels
                    .entry(metric.name.clone())
                    .or_default()
                    .extend(attrs.iter().map(|kv| kv.key.clone()));
            };
            match metric.data {
                Some(Data::Gauge(gauge)) => gauge.data_points.iter().for_each(|p| record(&p.attributes)),
                Some(Data::Histogram(hist)) => hist.data_points.iter().for_each(|p| record(&p.attributes)),
                Some(Data::Sum(sum)) => {
                    for point in sum.data_points {
                        record(&point.attributes);
                        if metric.name != HTTP_COUNTER {
                            continue;
                        }
                        let attr = |key: &str| {
                            point
                                .attributes
                                .iter()
                                .find(|a| a.key == key)
                                .and_then(|a| match a.value.as_ref()?.value.as_ref()? {
                                    any_value::Value::StringValue(value) => Some(value.as_str()),
                                    _ => None,
                                })
                                .unwrap_or_default()
                        };
                        if let Some(count) = self.http.series(attr("method"), attr("op"), attr("outcome")) {
                            *count = match point.value {
                                Some(number_data_point::Value::AsInt(n)) => {
                                    u64::try_from(n).map_err(|_| "negative HTTP counter")?
                                }
                                Some(number_data_point::Value::AsDouble(n)) if n.is_finite() && n >= 0.0 && n.fract() == 0.0 => {
                                    n as u64
                                }
                                _ => return Err("invalid HTTP counter"),
                            };
                        }
                    }
                }
                _ => return Err("unexpected S3 metric aggregation"),
            }
        }
        self.exports += 1;
        Ok(())
    }
}

async fn collect(
    req: Request<Incoming>,
    state: &Mutex<Observations>,
    bucket: &str,
    contract: &Contract,
) -> Result<(), &'static str> {
    let (parts, body) = req.into_parts();
    // WebhookTarget checks the origin before delivering events to /audit.
    if parts.method == Method::HEAD && parts.uri == "/" {
        return Ok(());
    }
    if parts.method != Method::POST {
        return Err("unexpected collector method");
    }
    let bytes = Limited::new(body, MAX_EXPORT)
        .collect()
        .await
        .map_err(|_| "invalid export size")?
        .to_bytes();
    let mut decoded = Vec::new();
    let payload = if parts.headers.get("content-encoding").is_some_and(|v| v == "gzip") {
        GzDecoder::new(bytes.as_ref())
            .take(MAX_EXPORT as u64 + 1)
            .read_to_end(&mut decoded)
            .map_err(|_| "invalid gzip export")?;
        if decoded.len() > MAX_EXPORT {
            return Err("decoded export exceeds limit");
        }
        &decoded[..]
    } else {
        &bytes[..]
    };
    match parts.uri.path() {
        "/audit" => {
            let document: Value = serde_json::from_slice(payload).map_err(|_| "invalid audit JSON")?;
            let events = document
                .get("Records")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or(std::slice::from_ref(&document));
            let mut snapshot = state.lock().expect("collector mutex");
            for event in events {
                snapshot.audit(event, bucket, contract)?;
            }
            Ok(())
        }
        "/v1/metrics" => {
            let export = ExportMetricsServiceRequest::decode(payload).map_err(|_| "invalid OTLP export")?;
            state.lock().expect("collector mutex").metrics(export, contract)
        }
        _ => Err("unexpected collector path"),
    }
}

async fn listen(listener: TcpListener, state: Arc<Mutex<Observations>>, bucket: String, contract: Arc<Contract>) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            Ok((socket, _)) = listener.accept() => {
                let state = Arc::clone(&state);
                let contract = Arc::clone(&contract);
                let bucket = bucket.clone();
                connections.spawn(async move {
                    let svc = service_fn(|req| async {
                        let result = collect(req, &state, &bucket, &contract).await;
                        let mut response = Response::new(Empty::<Bytes>::new());
                        if let Err(error) = result {
                            state.lock().expect("collector mutex").error = Some(error);
                            *response.status_mut() = StatusCode::BAD_REQUEST;
                        }
                        Ok::<_, Infallible>(response)
                    });
                    let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(socket), svc).await;
                });
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}

async fn admin_http_counts(env: &RustFSTestEnvironment) -> Result<HttpCounts, BoxError> {
    let (code, text) = admin_request(
        &env.url,
        Method::GET,
        "/rustfs/admin/v3/realtime?types=512&by-host=true&n=1",
        None,
        &env.access_key,
        &env.secret_key,
    )
    .await?;
    assert_eq!(code, StatusCode::OK, "read realtime HTTP counters");
    let report: RealtimeMetrics = serde_json::from_str(text.lines().next().ok_or("empty admin metrics")?)?;
    assert!(report.errors.is_empty(), "admin reported metric collection errors");
    let http = report.aggregated.http.ok_or("missing admin HTTP metrics")?;
    let mut counts = HttpCounts::default();
    for row in http.requests {
        if let Some(total) = counts.series(&row.method, &row.operation, &row.outcome) {
            *total += row.total;
        }
    }
    Ok(counts)
}

async fn wait_for(state: &Mutex<Observations>, ready: impl Fn(&Observations) -> bool) -> Result<(), BoxError> {
    timeout(Duration::from_secs(45), async {
        loop {
            {
                let s = state.lock().expect("collector mutex");
                if let Some(e) = s.error {
                    return Err::<(), BoxError>(e.into());
                }
                if ready(&s) {
                    return Ok(());
                }
            }
            sleep(Duration::from_millis(150)).await;
        }
    })
    .await
    .map_err(|_| {
        let snapshot = state.lock().expect("collector mutex");
        format!(
            "timed out after {} OTLP exports, {} successful PUTs, {} failed GETs, {} audit receipts and {} S3 metrics",
            snapshot.exports,
            snapshot.http.put_success,
            snapshot.http.get_error,
            snapshot.audits.len(),
            snapshot.labels.len()
        )
    })?
}

async fn check(side: SideA) -> Result<(), BoxError> {
    let contract: Arc<Contract> = Arc::new(serde_json::from_str(CONTRACT)?);
    assert_eq!(contract.sdk_puts, 20);
    let (label, binary) = match &side {
        SideA::Reference(path) => ("reference", path.clone()),
        SideA::SameBinary => ("under-test", rustfs_binary_path()),
    };
    eprintln!(
        "observability {label}: {}; fixture {}",
        binary_version(&binary).await?,
        contract.reference_version
    );
    let bucket = format!("observability-{}", uuid::Uuid::new_v4().simple());
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let audit = format!("{origin}/audit");
    let metrics = format!("{origin}/v1/metrics");
    let state = Arc::new(Mutex::new(Observations::default()));
    let mut collector = JoinSet::new();
    collector.spawn(listen(listener, Arc::clone(&state), bucket.clone(), Arc::clone(&contract)));
    let mut env = RustFSTestEnvironment::new().await?;
    let queue = format!("{}/queue", env.temp_dir);
    let vars = [
        ("RUSTFS_OUTBOUND_ALLOW_ORIGINS", origin.as_str()),
        ("RUSTFS_CHECK_UPDATE", "false"),
        ("RUSTFS_AUDIT_ENABLE", "true"),
        ("RUSTFS_AUDIT_WEBHOOK_ENABLE_CONTRACT", "on"),
        ("RUSTFS_AUDIT_WEBHOOK_ENDPOINT_CONTRACT", audit.as_str()),
        ("RUSTFS_AUDIT_WEBHOOK_QUEUE_DIR_CONTRACT", queue.as_str()),
        ("RUSTFS_OBS_METRIC_ENDPOINT", metrics.as_str()),
        ("RUSTFS_OBS_METER_INTERVAL", "1"),
        ("RUSTFS_OBS_METRICS_EXPORT_ENABLED", "true"),
        ("RUSTFS_OBS_LOGS_EXPORT_ENABLED", "false"),
        ("RUSTFS_OBS_TRACES_EXPORT_ENABLED", "false"),
        ("NO_PROXY", "127.0.0.1,localhost"),
        ("no_proxy", "127.0.0.1,localhost"),
    ];
    match side {
        SideA::SameBinary => env.start_rustfs_server_without_cleanup_with_env(&vars).await?,
        SideA::Reference(path) => {
            env.start_rustfs_server_from_binary_without_cleanup(&path, vec![], &vars)
                .await?
        }
    };
    let client = env.create_s3_client();
    client.create_bucket().bucket(&bucket).send().await?;
    wait_for(&state, |s| s.exports > 0 && s.labels.contains_key(HTTP_COUNTER)).await?;
    let start = state.lock().expect("collector mutex").http;
    let before = admin_http_counts(&env).await?;
    let mut expected = BTreeMap::new();
    let etag = format!("\"{}\"", hex_simd::encode_to_string(Md5::digest(BODY), hex_simd::AsciiCase::Lower));
    for i in 0..contract.sdk_puts {
        let key = format!("contract-{i}");
        let output = client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(ByteStream::from_static(BODY))
            .send()
            .await?;
        assert_eq!(output.e_tag(), Some(etag.as_str()), "stored PUT payload ETag");
        let id = output
            .request_id()
            .filter(|s| !s.is_empty())
            .ok_or("PUT omitted its request ID")?;
        assert!(expected.insert(id.to_owned(), key).is_none(), "duplicate SDK request ID");
    }
    let missing = client
        .get_object()
        .bucket(&bucket)
        .key("never-written")
        .send()
        .await
        .expect_err("GET of a missing object must fail");
    assert_eq!(missing.raw_response().map(|r| r.status().as_u16()), Some(404));
    assert!(
        missing.as_service_error().is_some_and(|err| err.is_no_such_key()),
        "missing-object GET must return NoSuchKey"
    );
    let after = admin_http_counts(&env).await?;
    assert_eq!(after.put_success.checked_sub(before.put_success), Some(contract.sdk_puts));
    assert_eq!(after.get_error.checked_sub(before.get_error), Some(1), "admin GetObject 4xx delta");
    client.list_objects_v2().bucket(&bucket).send().await?;
    wait_for(&state, |s| {
        s.http.put_success >= start.put_success + contract.sdk_puts
            && s.http.get_error > start.get_error
            && s.audits.len() >= expected.len()
            && s.labels.len() == contract.s3_metrics.len()
    })
    .await?;
    // Keep collecting for another export after all PUTs are present to catch late duplicates.
    let complete_export = state.lock().expect("collector mutex").exports;
    wait_for(&state, |s| s.exports > complete_export).await?;
    let observed = state.lock().expect("collector mutex");
    if let Some(error) = observed.error {
        return Err(error.into());
    }
    assert_eq!(
        observed.http.put_success.checked_sub(start.put_success),
        Some(contract.sdk_puts),
        "OTLP PUT delta"
    );
    assert_eq!(observed.http.get_error.checked_sub(start.get_error), Some(1), "OTLP GetObject 4xx delta");
    assert!(
        observed.audits == expected,
        "audit IDs and objects must match each successful SDK PUT once"
    );
    assert_eq!(observed.labels, contract.s3_metrics, "stable S3 metric names and label keys");
    Ok(())
}

#[tokio::test]
async fn reference_observability_contract() -> Result<(), BoxError> {
    check(SideA::from_env()?).await
}

#[tokio::test]
async fn current_observability_contract() -> Result<(), BoxError> {
    check(SideA::SameBinary).await
}
