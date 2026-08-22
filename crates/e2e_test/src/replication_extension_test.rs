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

use crate::common::{
    RustFSTestEnvironment, admin_create_user, awscurl_available, awscurl_post_sts_form_urlencoded, init_logging,
    local_http_client, replication_fast_env, rustfs_binary_path, signed_request, signed_request_with_client,
    signed_request_with_session_token,
};
use crate::fake_s3_target::{
    FAKE_ACCESS_KEY, FAKE_SECRET_KEY, FakeS3Target, FaultAction as FakeTargetFault, Operation as FakeTargetOperation,
    RequestRecord,
};
use crate::kms::common::{create_key_with_specific_id, sse_customer_key_md5_base64};
use crate::storage_api::replication_extension::BucketTargetSys;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::list_object_versions::ListObjectVersionsOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, DeleteMarkerEntry, ObjectVersion, ServerSideEncryption,
    VersioningConfiguration,
};
use aws_sdk_s3::{Client, Config};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bytes::Bytes;
use flate2::read::GzDecoder;
use futures::{Stream, StreamExt};
use http::header::CONTENT_ENCODING;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use local_ip_address::local_ip;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{KeyValue, any_value::Value as AnyValue};
use opentelemetry_proto::tonic::metrics::v1::{Metric, metric, number_data_point};
use prost::Message;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    SanType, generate_simple_self_signed,
};
use reqwest::StatusCode;
use rustfs_madmin::{
    AddServiceAccountReq, ListServiceAccountsResp, PeerInfo, PeerSite, ReplicateAddStatus, ReplicateEditStatus,
    ReplicateRemoveStatus, SRRemoveReq, SRResyncOpStatus, SRStatusInfo, SiteReplicationInfo, SyncStatus,
};
use s3s::header::X_AMZ_REPLICATION_STATUS;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::error::Error;
use std::io::Read;
use std::net::IpAddr;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::fs;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep, timeout};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;
type BacklogMetricPoints = Arc<Mutex<BTreeMap<String, BTreeMap<String, (u64, f64)>>>>;

/// A replication source server validates the remote target endpoint, and the e2e
/// target runs on loopback (127.0.0.1), which RustFS's SSRF egress guard rejects by
/// default. This suite opts its source servers into the loopback allowance explicitly
/// so the shared harness (`RustFSTestEnvironment` / the cluster harness) stays
/// fail-closed and every other e2e scenario keeps exercising the production SSRF policy.
const LOOPBACK_REPLICATION_TARGET_ENV: &[(&str, &str)] = &[("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", "true")];

/// Short data-scanner cycle for the failure-recovery tests (backlog#1147 repl-5).
///
/// When a replication target is unreachable, `replicate_object` marks the source
/// object's status FAILED in `xl.meta` (it is NOT queued to the on-disk MRF
/// overflow file, which only backstops worker-queue saturation). The mechanism
/// that re-drives those persisted PENDING/FAILED objects — including after a
/// source restart — is the data scanner's replication heal pass
/// (`heal_replication` -> `queue_replication_heal`). The scanner cycle floors at
/// 1s, so this pins it to the floor to keep convergence within seconds. Combine
/// with [`replication_fast_env`] (health-check / MRF-flush / resync polling) so
/// every recovery loop runs at its minimum interval and each scenario finishes
/// well under the two-minute budget.
///
/// `RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES=1` matters for changes to
/// already-scanned objects (e.g. a delete marker stacked on a replicated key):
/// existing compacted folders are hash-sharded across that many cycles before
/// being rescanned (default 16, `scanner_folder.rs`), so on a long-lived source
/// a failed delete-marker replication may otherwise wait 16 scan cycles before
/// the heal pass revisits it. New keys are unaffected (new folders are always
/// scanned), which is why only restart-free recovery of EXISTING keys needs it.
const FAST_SCANNER_ENV: &[(&str, &str)] = &[
    ("RUSTFS_SCANNER_CYCLE", "1"),
    ("RUSTFS_SCANNER_START_DELAY_SECS", "1"),
    ("RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES", "1"),
];

const REPL17_KMS_KEY_ID: &str = "repl17-local-key";
const REPL17_SSEC_KEY: &str = "01234567890123456789012345678901";
const REPLICATION_FAILED_EVENT: &str = "s3:Replication:OperationFailedReplication";
const REPLICATION_EVENT_MAX_BUFFER_BYTES: usize = 1024 * 1024;
const OTLP_METRICS_BODY_LIMIT: u64 = 4 * 1024 * 1024;
const BUCKET_LABEL: &str = "bucket";
const TOTAL_FAILED_COUNT_METRIC: &str = "rustfs_bucket_replication_total_failed_count";
const CURRENT_BACKLOG_COUNT_METRIC: &str = "rustfs_bucket_replication_current_backlog_count";
const CURRENT_BACKLOG_BYTES_METRIC: &str = "rustfs_bucket_replication_current_backlog_bytes";
const MRF_PENDING_COUNT_METRIC: &str = "rustfs_bucket_replication_mrf_pending_count";
const MRF_PENDING_BYTES_METRIC: &str = "rustfs_bucket_replication_mrf_pending_bytes";

struct ReplicationBacklogMetricCollector {
    endpoint: String,
    values: BacklogMetricPoints,
    task: JoinHandle<()>,
}

impl ReplicationBacklogMetricCollector {
    async fn start() -> Result<Self, Box<dyn Error + Send + Sync>> {
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
                    let _ = http1::Builder::new()
                        .serve_connection(
                            TokioIo::new(stream),
                            service_fn(move |request| handle_backlog_metric_export(request, values.clone())),
                        )
                        .await;
                });
            }
        });

        Ok(Self { endpoint, values, task })
    }

    fn root_endpoint(&self) -> &str {
        self.endpoint.trim_end_matches("/v1/metrics")
    }

    async fn bucket_metric_value(&self, metric: &str, bucket: &str) -> f64 {
        self.values
            .lock()
            .await
            .get(metric)
            .and_then(|buckets| buckets.get(bucket))
            .map(|(_, value)| *value)
            .unwrap_or_default()
    }

    async fn wait_for_bucket_metric(
        &self,
        metric: &str,
        bucket: &str,
        expected: impl Fn(f64) -> bool,
        description: &str,
    ) -> Result<f64, Box<dyn Error + Send + Sync>> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let value = self.bucket_metric_value(metric, bucket).await;
            if expected(value) {
                return Ok(value);
            }
            if tokio::time::Instant::now() >= deadline {
                let snapshot = self.values.lock().await.clone();
                return Err(format!("timed out waiting for {metric} on bucket {bucket} to satisfy {description}; last={value}, snapshot={snapshot:?}").into());
            }
            sleep(Duration::from_millis(200)).await;
        }
    }
}

impl Drop for ReplicationBacklogMetricCollector {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle_backlog_metric_export(
    request: Request<Incoming>,
    values: BacklogMetricPoints,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if request.uri().path() != "/v1/metrics" {
        return Ok(empty_http_response(StatusCode::NOT_FOUND));
    }

    let gzip = request
        .headers()
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("gzip"));
    let Ok(collected) = request.into_body().collect().await else {
        return Ok(empty_http_response(StatusCode::BAD_REQUEST));
    };
    let body = collected.to_bytes();
    if body.len() as u64 > OTLP_METRICS_BODY_LIMIT {
        return Ok(empty_http_response(StatusCode::PAYLOAD_TOO_LARGE));
    }
    let payload = if gzip {
        let mut decoder = GzDecoder::new(body.as_ref());
        let mut decoded = Vec::new();
        if decoder
            .by_ref()
            .take(OTLP_METRICS_BODY_LIMIT + 1)
            .read_to_end(&mut decoded)
            .is_err()
            || decoded.len() as u64 > OTLP_METRICS_BODY_LIMIT
        {
            return Ok(empty_http_response(StatusCode::BAD_REQUEST));
        }
        decoded
    } else {
        body.to_vec()
    };

    match ExportMetricsServiceRequest::decode(payload.as_slice()) {
        Ok(export) => {
            let mut values = values.lock().await;
            record_backlog_metrics(&export, &mut values);
            Ok(empty_http_response(StatusCode::OK))
        }
        Err(_) => Ok(empty_http_response(StatusCode::BAD_REQUEST)),
    }
}

fn empty_http_response(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .expect("static HTTP response is valid")
}

fn record_backlog_metrics(export: &ExportMetricsServiceRequest, values: &mut BTreeMap<String, BTreeMap<String, (u64, f64)>>) {
    for resource_metrics in &export.resource_metrics {
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                record_backlog_metric(metric, values);
            }
        }
    }
}

fn record_backlog_metric(metric: &Metric, values: &mut BTreeMap<String, BTreeMap<String, (u64, f64)>>) {
    if ![
        TOTAL_FAILED_COUNT_METRIC,
        CURRENT_BACKLOG_COUNT_METRIC,
        CURRENT_BACKLOG_BYTES_METRIC,
        MRF_PENDING_COUNT_METRIC,
        MRF_PENDING_BYTES_METRIC,
    ]
    .contains(&metric.name.as_str())
    {
        return;
    }

    let points = match &metric.data {
        Some(metric::Data::Gauge(gauge)) => gauge.data_points.as_slice(),
        Some(metric::Data::Sum(sum)) => sum.data_points.as_slice(),
        _ => return,
    };
    for point in points {
        let Some(bucket) = attribute_string(&point.attributes, BUCKET_LABEL) else {
            continue;
        };
        let Some(value) = number_point_value(point.value.as_ref()) else {
            continue;
        };
        values
            .entry(metric.name.clone())
            .or_default()
            .entry(bucket.to_string())
            .and_modify(|current| {
                if point.time_unix_nano >= current.0 {
                    *current = (point.time_unix_nano, value);
                }
            })
            .or_insert((point.time_unix_nano, value));
    }
}

fn number_point_value(value: Option<&number_data_point::Value>) -> Option<f64> {
    match value? {
        number_data_point::Value::AsDouble(value) => Some(*value),
        number_data_point::Value::AsInt(value) => Some(*value as f64),
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

struct SlowReplicationTargetGuard {
    task: Option<JoinHandle<()>>,
}

impl SlowReplicationTargetGuard {
    async fn bind(address: &str, response_delay: Duration) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let listener = TcpListener::bind(address).await?;
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let _ = http1::Builder::new()
                        .serve_connection(
                            TokioIo::new(stream),
                            service_fn(move |_request| async move {
                                sleep(response_delay).await;
                                Ok::<_, Infallible>(empty_http_response(StatusCode::SERVICE_UNAVAILABLE))
                            }),
                        )
                        .await;
                });
            }
        });
        Ok(Self { task: Some(task) })
    }

    async fn stop(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for SlowReplicationTargetGuard {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

// Mirrors madmin-go `ResyncTargetsInfo`/`ResyncTarget` json tags — the same
// shape `mc replicate resync status` decodes.
#[derive(Debug, Clone, serde::Deserialize)]
struct ReplicationResetStatusResponse {
    #[serde(rename = "target", default)]
    targets: Vec<ReplicationResetStatusTarget>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ReplicationResetStatusTarget {
    #[serde(rename = "arn", default)]
    arn: String,
    #[serde(rename = "resetid", default)]
    reset_id: String,
    #[serde(rename = "resyncStatus", default)]
    status: String,
    #[serde(rename = "replicationCount", default)]
    replicated_count: i64,
    #[serde(rename = "object", default)]
    object: String,
}

fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

fn parse_assume_role_credentials(xml: &str) -> Result<(String, String, String), Box<dyn Error + Send + Sync>> {
    let access_key = extract_xml_tag(xml, "AccessKeyId").ok_or("missing AccessKeyId in AssumeRole response")?;
    let secret_key = extract_xml_tag(xml, "SecretAccessKey").ok_or("missing SecretAccessKey in AssumeRole response")?;
    let session_token = extract_xml_tag(xml, "SessionToken").ok_or("missing SessionToken in AssumeRole response")?;
    Ok((access_key, secret_key, session_token))
}

struct ReplicationTargetOptions<'a> {
    endpoint: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
    target_bucket: &'a str,
    secure: bool,
    skip_tls_verify: bool,
    ca_cert_pem: Option<&'a str>,
}

async fn set_replication_target(
    source_env: &RustFSTestEnvironment,
    source_bucket: &str,
    target_env: &RustFSTestEnvironment,
    target_bucket: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    set_replication_target_with_options(
        source_env,
        source_bucket,
        ReplicationTargetOptions {
            endpoint: &target_env.address,
            access_key: &target_env.access_key,
            secret_key: &target_env.secret_key,
            target_bucket,
            secure: false,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await
}

async fn set_replication_target_with_options(
    source_env: &RustFSTestEnvironment,
    source_bucket: &str,
    options: ReplicationTargetOptions<'_>,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut body = serde_json::json!({
        "endpoint": options.endpoint,
        "credentials": {
            "accessKey": options.access_key,
            "secretKey": options.secret_key
        },
        "targetbucket": options.target_bucket,
        "secure": options.secure,
        "skipTlsVerify": options.skip_tls_verify,
        "type": "replication"
    });
    if let Some(ca_cert_pem) = options.ca_cert_pem {
        body["caCertPem"] = serde_json::Value::String(ca_cert_pem.to_string());
    }
    let url = format!(
        "{}/rustfs/admin/v3/set-remote-target?bucket={}",
        source_env.url,
        urlencoding::encode(source_bucket)
    );
    let response = signed_request(
        http::Method::PUT,
        &url,
        &source_env.access_key,
        &source_env.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("set remote target failed: {status} {body}").into());
    }

    let body = response.bytes().await?;
    let arn: String = serde_json::from_slice(&body)?;
    Ok(arn)
}

async fn send_set_replication_target_request(
    source_env: &RustFSTestEnvironment,
    source_bucket: &str,
    update: bool,
    body: serde_json::Value,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let mut url = format!(
        "{}/rustfs/admin/v3/set-remote-target?bucket={}",
        source_env.url,
        urlencoding::encode(source_bucket)
    );
    if update {
        url.push_str("&update=true");
    }
    signed_request(
        http::Method::PUT,
        &url,
        &source_env.access_key,
        &source_env.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await
}

async fn put_bucket_replication(
    env: &RustFSTestEnvironment,
    bucket: &str,
    target_arn: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    put_bucket_replication_with_delete_statuses(env, bucket, target_arn, "Enabled", None).await
}

async fn put_bucket_replication_with_delete_statuses(
    env: &RustFSTestEnvironment,
    bucket: &str,
    target_arn: &str,
    delete_marker_status: &str,
    version_delete_status: Option<&str>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    put_bucket_replication_with_statuses(env, bucket, target_arn, delete_marker_status, version_delete_status, "Enabled").await
}

async fn put_bucket_replication_with_statuses(
    env: &RustFSTestEnvironment,
    bucket: &str,
    target_arn: &str,
    delete_marker_status: &str,
    version_delete_status: Option<&str>,
    existing_object_status: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let delete_replication = version_delete_status
        .map(|status| format!("<DeleteReplication><Status>{status}</Status></DeleteReplication>"))
        .unwrap_or_default();
    let body = format!(
        r#"<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Role></Role>
  <Rule>
    <ID>rule-1</ID>
    <Priority>1</Priority>
    <Status>Enabled</Status>
    <DeleteMarkerReplication>
      <Status>{delete_marker_status}</Status>
    </DeleteMarkerReplication>
    {delete_replication}
    <ExistingObjectReplication>
      <Status>{existing_object_status}</Status>
    </ExistingObjectReplication>
    <Destination>
      <Bucket>{target_arn}</Bucket>
    </Destination>
  </Rule>
</ReplicationConfiguration>"#
    );
    let url = format!("{}/{bucket}?replication", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body.into_bytes()),
        Some("application/xml"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("put bucket replication failed: {status} {body}").into());
    }

    Ok(())
}

async fn put_bucket_replication_rules(
    env: &RustFSTestEnvironment,
    bucket: &str,
    target_arns: &[&str],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut rules = String::new();
    for (idx, target_arn) in target_arns.iter().enumerate() {
        rules.push_str(&format!(
            r#"
  <Rule>
    <ID>rule-{}</ID>
    <Priority>{}</Priority>
    <Status>Enabled</Status>
    <DeleteMarkerReplication>
      <Status>Enabled</Status>
    </DeleteMarkerReplication>
    <ExistingObjectReplication>
      <Status>Enabled</Status>
    </ExistingObjectReplication>
    <Destination>
      <Bucket>{}</Bucket>
    </Destination>
  </Rule>"#,
            idx + 1,
            idx + 1,
            target_arn
        ));
    }

    let body = format!(
        r#"<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Role></Role>{rules}
</ReplicationConfiguration>"#
    );
    let url = format!("{}/{bucket}?replication", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body.into_bytes()),
        Some("application/xml"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("put bucket replication with multiple rules failed: {status} {body}").into());
    }

    Ok(())
}

async fn delete_bucket_replication(
    env: &RustFSTestEnvironment,
    bucket: &str,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/{bucket}?replication", env.url);
    signed_request(http::Method::DELETE, &url, &env.access_key, &env.secret_key, None, None).await
}

async fn get_bucket_replication(
    env: &RustFSTestEnvironment,
    bucket: &str,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/{bucket}?replication", env.url);
    signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await
}

async fn enable_bucket_versioning(env: &RustFSTestEnvironment, bucket: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    set_bucket_versioning(env, bucket, BucketVersioningStatus::Enabled).await
}

async fn set_bucket_versioning(
    env: &RustFSTestEnvironment,
    bucket: &str,
    status: BucketVersioningStatus,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = env.create_s3_client();
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(VersioningConfiguration::builder().status(status).build())
        .send()
        .await?;
    Ok(())
}

fn insecure_https_client() -> Result<reqwest::Client, Box<dyn Error + Send + Sync>> {
    Ok(reqwest::Client::builder()
        .no_proxy()
        .danger_accept_invalid_certs(true)
        .build()?)
}

fn trusted_https_client(ca_cert_pem: &str) -> Result<reqwest::Client, Box<dyn Error + Send + Sync>> {
    let ca_cert = reqwest::Certificate::from_pem(ca_cert_pem.as_bytes())?;
    Ok(reqwest::Client::builder().no_proxy().add_root_certificate(ca_cert).build()?)
}

async fn new_replication_source_env() -> Result<RustFSTestEnvironment, Box<dyn Error + Send + Sync>> {
    // Reuse the shared harness's portable temp-dir/port setup. This previously built
    // a bespoke `/private/tmp/...` path, which only exists on macOS and is unwritable
    // on the Linux CI runner, so the HTTPS-target tests failed before starting RustFS.
    RustFSTestEnvironment::new().await
}

async fn new_replication_https_target_env() -> Result<RustFSTestEnvironment, Box<dyn Error + Send + Sync>> {
    let mut env = new_replication_source_env().await?;
    let public_ip = local_ip().map_err(|err| std::io::Error::other(format!("resolve local IP failed: {err}")))?;
    let port = env
        .address
        .rsplit(':')
        .next()
        .ok_or_else(|| std::io::Error::other("target env address missing port"))?
        .to_string();
    env.address = format!("0.0.0.0:{port}");
    env.url = format!("https://{public_ip}:{port}");
    Ok(env)
}

async fn generate_self_signed_tls_material(tls_dir: &Path, additional_san: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    fs::create_dir_all(tls_dir).await?;
    let cert = generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string(), additional_san.to_string()])?;
    fs::write(tls_dir.join("rustfs_cert.pem"), cert.cert.pem()).await?;
    fs::write(tls_dir.join("rustfs_key.pem"), cert.signing_key.serialize_pem()).await?;
    Ok(())
}

fn test_certificate_params(common_name: &str) -> CertificateParams {
    let mut params = CertificateParams::default();
    let issued_at = OffsetDateTime::now_utc() - TimeDuration::minutes(5);
    params.not_before = issued_at;
    params.not_after = issued_at + TimeDuration::days(1);
    params.distinguished_name.push(DnType::CountryName, "US");
    params.distinguished_name.push(DnType::OrganizationName, "RustFS");
    params.distinguished_name.push(DnType::CommonName, common_name);
    params
}

async fn generate_private_ca_tls_material(tls_dir: &Path, additional_san: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
    fs::create_dir_all(tls_dir).await?;

    let ca_key = KeyPair::generate()?;
    let mut ca_params = test_certificate_params("RustFS Replication Test CA");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca = CertifiedIssuer::self_signed(ca_params, ca_key)?;

    let server_key = KeyPair::generate()?;
    let mut server_params = test_certificate_params("localhost");
    server_params.is_ca = IsCa::ExplicitNoCa;
    server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature, KeyUsagePurpose::KeyEncipherment];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    server_params
        .subject_alt_names
        .push(SanType::DnsName("localhost".try_into()?));
    server_params
        .subject_alt_names
        .push(SanType::IpAddress("127.0.0.1".parse::<IpAddr>()?));
    match additional_san.parse::<IpAddr>() {
        Ok(ip) => server_params.subject_alt_names.push(SanType::IpAddress(ip)),
        Err(_) => server_params
            .subject_alt_names
            .push(SanType::DnsName(additional_san.try_into()?)),
    }

    let server_cert = server_params.signed_by(&server_key, &ca)?;
    let ca_cert_pem = ca.pem();
    fs::write(tls_dir.join("rustfs_cert.pem"), server_cert.pem()).await?;
    fs::write(tls_dir.join("rustfs_key.pem"), server_key.serialize_pem()).await?;
    fs::write(tls_dir.join("ca.crt"), &ca_cert_pem).await?;

    Ok(ca_cert_pem)
}

async fn start_https_rustfs_server(env: &mut RustFSTestEnvironment, tls_dir: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
    let binary_path = rustfs_binary_path();
    let process = Command::new(&binary_path)
        .env("RUST_LOG", "rustfs=info,rustfs_notify=debug")
        .env("RUSTFS_TLS_PATH", tls_dir)
        .env("RUSTFS_CONSOLE_ENABLE", "false")
        .env("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", "true")
        .args([
            "--address",
            &env.address,
            "--access-key",
            &env.access_key,
            "--secret-key",
            &env.secret_key,
            &env.temp_dir,
        ])
        .spawn()?;
    env.process = Some(process);
    Ok(())
}

async fn wait_for_https_server_ready(
    client: &reqwest::Client,
    env: &RustFSTestEnvironment,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/", env.url);

    for _ in 0..60 {
        match signed_request_with_client(client, http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(_) | Err(_) => sleep(Duration::from_millis(500)).await,
        }
    }

    Err("RustFS HTTPS server failed to become ready within 30 seconds".into())
}

fn assert_untrusted_site_peer_rejected(error: &str, target_url: &str) {
    let error_lower = error.to_ascii_lowercase();
    let certificate_error =
        error.contains("400 Bad Request") && (error_lower.contains("tls") || error_lower.contains("certificate"));
    let https_connect_error =
        error.contains("500 Internal Server Error") && error_lower.contains("failed (connect)") && error.contains(target_url);

    assert!(certificate_error || https_connect_error, "unexpected untrusted HTTPS peer error: {error}");
}

async fn ensure_https_bucket_exists(
    client: &reqwest::Client,
    env: &RustFSTestEnvironment,
    bucket: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let bucket_url = format!("{}/{bucket}/", env.url);
    let response =
        signed_request_with_client(client, http::Method::HEAD, &bucket_url, &env.access_key, &env.secret_key, None, None).await?;

    if response.status() == StatusCode::OK {
        return Ok(());
    }

    let response = signed_request_with_client(
        client,
        http::Method::PUT,
        &bucket_url,
        &env.access_key,
        &env.secret_key,
        Some(Vec::new()),
        None,
    )
    .await?;
    match response.status() {
        StatusCode::OK | StatusCode::CONFLICT => Ok(()),
        status => Err(format!("unexpected HTTPS bucket setup status: {status}").into()),
    }
}

async fn enable_bucket_versioning_over_https(
    client: &reqwest::Client,
    env: &RustFSTestEnvironment,
    bucket: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let body = r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>"#;
    let url = format!("{}/{bucket}?versioning", env.url);
    let response = signed_request_with_client(
        client,
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body.as_bytes().to_vec()),
        Some("application/xml"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("enable HTTPS bucket versioning failed: {status} {body}").into());
    }

    Ok(())
}

async fn wait_for_replicated_object_over_https(
    client: &reqwest::Client,
    env: &RustFSTestEnvironment,
    bucket: &str,
    key: &str,
    expected_body: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let url = format!("{}/{bucket}/{key}", env.url);

    loop {
        let response =
            signed_request_with_client(client, http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await?;

        match response.status() {
            StatusCode::OK => {
                let body = response.text().await?;
                if body == expected_body {
                    return Ok(());
                }
                return Err(format!("replicated HTTPS object body mismatch: expected {expected_body}, got {body}").into());
            }
            StatusCode::NOT_FOUND if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_secs(1)).await;
            }
            status if tokio::time::Instant::now() < deadline => {
                let body = response.text().await.unwrap_or_default();
                if body.contains("NoSuchKey") || body.contains("NoSuchBucket") || body.contains("NotFound") {
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                return Err(format!("unexpected HTTPS replication read status: {status} {body}").into());
            }
            status => {
                let body = response.text().await.unwrap_or_default();
                return Err(format!("HTTPS replicated object was not readable in time: {status} {body}").into());
            }
        }
    }
}

fn create_user_s3_client(env: &RustFSTestEnvironment, access_key: &str, secret_key: &str) -> Client {
    let credentials = Credentials::new(access_key, secret_key, None, None, "e2e-site-replication");
    let config = Config::builder()
        .credentials_provider(credentials)
        .region(Region::new("us-east-1"))
        .endpoint_url(&env.url)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

async fn admin_add_canned_policy(
    env: &RustFSTestEnvironment,
    policy_name: &str,
    policy: &serde_json::Value,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/add-canned-policy?name={}", env.url, policy_name);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(policy.to_string().into_bytes()),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("add canned policy failed: {status} {body}").into());
    }

    Ok(())
}

async fn admin_attach_policy_to_user(
    env: &RustFSTestEnvironment,
    policy_name: &str,
    username: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!(
        "{}/rustfs/admin/v3/set-user-or-group-policy?policyName={}&userOrGroup={}&isGroup=false",
        env.url, policy_name, username
    );
    let response = signed_request(http::Method::PUT, &url, &env.access_key, &env.secret_key, Some(Vec::new()), None).await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("attach policy to user failed: {status} {body}").into());
    }

    Ok(())
}

async fn admin_update_group_members(
    env: &RustFSTestEnvironment,
    group_name: &str,
    members: &[&str],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/update-group-members", env.url);
    let body = serde_json::json!({
        "group": group_name,
        "members": members,
        "isRemove": false,
        "groupStatus": "enabled"
    });
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("update group members failed: {status} {body}").into());
    }

    Ok(())
}

async fn admin_attach_policy_to_group(
    env: &RustFSTestEnvironment,
    policy_name: &str,
    group_name: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!(
        "{}/rustfs/admin/v3/set-user-or-group-policy?policyName={}&userOrGroup={}&isGroup=true",
        env.url, policy_name, group_name
    );
    let response = signed_request(http::Method::PUT, &url, &env.access_key, &env.secret_key, Some(Vec::new()), None).await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("attach policy to group failed: {status} {body}").into());
    }

    Ok(())
}

async fn wait_for_replicated_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    expected_body: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        match client.get_object().bucket(bucket).key(key).send().await {
            Ok(output) => {
                let body = output.body.collect().await?.into_bytes();
                let body = String::from_utf8(body.to_vec())?;
                if body == expected_body {
                    return Ok(());
                }
                return Err(format!("replicated object body mismatch: expected {expected_body}, got {body}").into());
            }
            Err(_err) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(err) => return Err(err.into()),
        }
    }
}

async fn wait_for_replicated_sha256(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    expected_sha256: [u8; 32],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

    loop {
        match client.get_object().bucket(bucket).key(key).send().await {
            Ok(output) => {
                let body = output.body.collect().await?.into_bytes();
                let actual_sha256: [u8; 32] = Sha256::digest(&body).into();
                if actual_sha256 == expected_sha256 {
                    return Ok(());
                }
                return Err(
                    format!("replicated object SHA-256 mismatch: expected {expected_sha256:?}, got {actual_sha256:?}").into(),
                );
            }
            Err(_err) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(err) => return Err(err.into()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ReplicatedVersion {
    key: String,
    version_id: String,
    delete_marker: bool,
    is_latest: bool,
    last_modified: (i64, u32),
    e_tag: Option<String>,
}

async fn list_replication_state(client: &Client, bucket: &str) -> Result<Vec<ReplicatedVersion>, Box<dyn Error + Send + Sync>> {
    let mut state = Vec::new();
    let mut key_marker = None;
    let mut version_id_marker = None;

    loop {
        let output = client
            .list_object_versions()
            .bucket(bucket)
            .set_key_marker(key_marker)
            .set_version_id_marker(version_id_marker)
            .send()
            .await?;

        for version in output.versions() {
            let last_modified = version.last_modified().ok_or("listed object version omitted LastModified")?;
            state.push(ReplicatedVersion {
                key: version.key().ok_or("listed object version omitted key")?.to_string(),
                version_id: version
                    .version_id()
                    .ok_or("listed object version omitted version ID")?
                    .to_string(),
                delete_marker: false,
                is_latest: version.is_latest().unwrap_or(false),
                last_modified: (last_modified.secs(), last_modified.subsec_nanos()),
                e_tag: Some(version.e_tag().ok_or("listed object version omitted ETag")?.to_string()),
            });
        }
        for marker in output.delete_markers() {
            let last_modified = marker.last_modified().ok_or("listed delete marker omitted LastModified")?;
            state.push(ReplicatedVersion {
                key: marker.key().ok_or("listed delete marker omitted key")?.to_string(),
                version_id: marker
                    .version_id()
                    .ok_or("listed delete marker omitted version ID")?
                    .to_string(),
                delete_marker: true,
                is_latest: marker.is_latest().unwrap_or(false),
                last_modified: (last_modified.secs(), last_modified.subsec_nanos()),
                e_tag: None,
            });
        }

        if output.is_truncated() != Some(true) {
            break;
        }
        key_marker = Some(
            output
                .next_key_marker()
                .ok_or("truncated version listing omitted next key marker")?
                .to_string(),
        );
        version_id_marker = output.next_version_id_marker().map(str::to_string);
    }

    state.sort();
    Ok(state)
}

async fn assert_replication_converged(
    source_client: &Client,
    source_bucket: &str,
    target_client: &Client,
    target_bucket: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut consecutive_matches = 0;

    loop {
        let source = list_replication_state(source_client, source_bucket).await?;
        let target = list_replication_state(target_client, target_bucket).await?;
        if source == target {
            consecutive_matches += 1;
            if consecutive_matches == 2 {
                return Ok(());
            }
        } else {
            consecutive_matches = 0;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("replication did not converge in time; source={source:?}, target={target:?}").into());
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_replication_state<F>(
    client: &Client,
    bucket: &str,
    description: &str,
    predicate: F,
) -> Result<Vec<ReplicatedVersion>, Box<dyn Error + Send + Sync>>
where
    F: Fn(&[ReplicatedVersion]) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let state = list_replication_state(client, bucket).await?;
        if predicate(&state) {
            return Ok(state);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("{description}; last target state: {state:?}").into());
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn assert_replication_key_absent(
    client: &Client,
    bucket: &str,
    key: &str,
    observation: Duration,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let deadline = tokio::time::Instant::now() + observation;
    loop {
        let state = list_replication_state(client, bucket).await?;
        assert!(
            state.iter().all(|entry| entry.key != key),
            "unexpected replicated key {bucket}/{key}: {state:?}"
        );
        if tokio::time::Instant::now() >= deadline {
            return Ok(());
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn get_version_body(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    Ok(client
        .get_object()
        .bucket(bucket)
        .key(key)
        .version_id(version_id)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes()
        .to_vec())
}

/// Poll the source object until it reports a not-yet-replicated status.
///
/// A source object with a reachable replication config carries an
/// `x-amz-replication-status` header (surfaced by the SDK as
/// `replication_status()`). While the target is down it must read `PENDING` or
/// `FAILED`; it can never be `COMPLETED`. Used by the failure-recovery tests to
/// prove the outage was actually observed before recovery is driven.
async fn wait_for_source_replication_pending_or_failed(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let head = client.head_object().bucket(bucket).key(key).send().await?;
        match head.replication_status().map(|status| status.as_str()) {
            Some("PENDING") | Some("FAILED") => return Ok(()),
            other => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!("source object {key} never reported PENDING/FAILED; last status={other:?}").into());
                }
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn wait_for_source_replication_status(client: &Client, bucket: &str, key: &str, expected: &str, ssec: bool) -> TestResult {
    let customer_key = BASE64_STANDARD.encode(REPL17_SSEC_KEY);
    let customer_key_md5 = sse_customer_key_md5_base64(REPL17_SSEC_KEY);
    let wait = async {
        loop {
            let request = client.head_object().bucket(bucket).key(key);
            let head = if ssec {
                request
                    .sse_customer_algorithm("AES256")
                    .sse_customer_key(&customer_key)
                    .sse_customer_key_md5(&customer_key_md5)
                    .send()
                    .await?
            } else {
                request.send().await?
            };
            if head.replication_status().map(|status| status.as_str()) == Some(expected) {
                return Ok(());
            }
            sleep(Duration::from_millis(200)).await;
        }
    };

    match timeout(Duration::from_secs(30), wait).await {
        Ok(result) => result,
        Err(_) => Err(format!("source object {key} did not reach replication status {expected} within 30 seconds").into()),
    }
}

async fn wait_for_replication_failure_event_stream<S, E>(stream: S, expected_key: &str, max_wait: Duration) -> TestResult
where
    S: Stream<Item = Result<Bytes, E>>,
    E: Error + Send + Sync + 'static,
{
    let wait = async {
        futures::pin_mut!(stream);
        let mut pending = Vec::new();
        loop {
            let Some(chunk) = stream.next().await else {
                return Err("replication failure event stream ended before the expected event".into());
            };
            let chunk = chunk.map_err(|err| -> Box<dyn Error + Send + Sync> { Box::new(err) })?;
            if chunk.is_empty() {
                continue;
            }
            if pending.len().saturating_add(chunk.len()) > REPLICATION_EVENT_MAX_BUFFER_BYTES {
                return Err("replication failure event buffer exceeded 1 MiB".into());
            }
            pending.extend_from_slice(&chunk);

            while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                let line = pending.drain(..=newline).collect::<Vec<_>>();
                let payload = std::str::from_utf8(&line)?.trim();
                if payload.is_empty() {
                    continue;
                }
                let envelope: serde_json::Value = serde_json::from_str(payload)?;
                let Some(records) = envelope["Records"].as_array() else {
                    continue;
                };
                for record in records {
                    let Some(object_key) = record["s3"]["object"]["key"].as_str() else {
                        continue;
                    };
                    let form_decoded_key = object_key.replace('+', " ");
                    let object_key = urlencoding::decode(&form_decoded_key)
                        .map(|decoded| decoded.into_owned())
                        .unwrap_or_else(|_| object_key.to_string());
                    if object_key == expected_key && record["eventName"].as_str() == Some(REPLICATION_FAILED_EVENT) {
                        return Ok(());
                    }
                }
            }
        }
    };

    match timeout(max_wait, wait).await {
        Ok(result) => result,
        Err(_) => Err(format!("replication failure event for {expected_key} was not received within {max_wait:?}").into()),
    }
}

fn target_history_contains_key(output: &ListObjectVersionsOutput, key: &str) -> bool {
    output.versions().iter().any(|version| version.key() == Some(key))
        || output.delete_markers().iter().any(|marker| marker.key() == Some(key))
}

async fn assert_failed_replication_stays_absent_for(
    source_client: &Client,
    source_bucket: &str,
    target_client: &Client,
    target_bucket: &str,
    key: &str,
    ssec: bool,
    duration: Duration,
) -> TestResult {
    let customer_key = BASE64_STANDARD.encode(REPL17_SSEC_KEY);
    let customer_key_md5 = sse_customer_key_md5_base64(REPL17_SSEC_KEY);
    let wait = async {
        let deadline = tokio::time::Instant::now() + duration;
        loop {
            let source_request = source_client.head_object().bucket(source_bucket).key(key);
            let source = if ssec {
                source_request
                    .sse_customer_algorithm("AES256")
                    .sse_customer_key(&customer_key)
                    .sse_customer_key_md5(&customer_key_md5)
                    .send()
                    .await?
            } else {
                source_request.send().await?
            };
            if source.replication_status().map(|status| status.as_str()) != Some("FAILED") {
                return Err(format!("source replication status for {source_bucket}/{key} did not remain FAILED").into());
            }

            let output = target_client
                .list_object_versions()
                .bucket(target_bucket)
                .prefix(key)
                .send()
                .await?;
            if target_history_contains_key(&output, key) {
                return Err(format!("failed replication created target history for {target_bucket}/{key}").into());
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(());
            }
            sleep(Duration::from_millis(250)).await;
        }
    };

    match timeout(duration + Duration::from_secs(5), wait).await {
        Ok(result) => result,
        Err(_) => Err(format!("timed out while checking failed replication stability for {target_bucket}/{key}").into()),
    }
}

async fn build_sse_replication_pair(
    label: &str,
    source_kms: bool,
    target_kms: bool,
) -> Result<(RustFSTestEnvironment, RustFSTestEnvironment, String, String), Box<dyn Error + Send + Sync>> {
    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut target_env = RustFSTestEnvironment::new().await?;
    let source_kms_key_dir = format!("{}/kms-keys", source_env.temp_dir);
    let target_kms_key_dir = format!("{}/kms-keys", target_env.temp_dir);
    if source_kms {
        fs::create_dir_all(&source_kms_key_dir).await?;
        create_key_with_specific_id(&source_kms_key_dir, REPL17_KMS_KEY_ID).await?;
    }
    // The two sites share a key id but never key material: each side generates
    // its own key, which is exactly the independent-KMS topology managed-SSE
    // replication must survive (target re-encrypts with its own envelope).
    if target_kms {
        fs::create_dir_all(&target_kms_key_dir).await?;
        create_key_with_specific_id(&target_kms_key_dir, REPL17_KMS_KEY_ID).await?;
    }

    let mut source_process_env = replication_fast_env();
    source_process_env.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    source_process_env.extend_from_slice(FAST_SCANNER_ENV);
    source_process_env.extend_from_slice(&[("NO_PROXY", "127.0.0.1,localhost"), ("HTTP_PROXY", ""), ("HTTPS_PROXY", "")]);
    if source_kms {
        source_process_env.extend_from_slice(&[
            ("RUSTFS_KMS_ENABLE", "true"),
            ("RUSTFS_KMS_BACKEND", "local"),
            ("RUSTFS_KMS_KEY_DIR", source_kms_key_dir.as_str()),
            ("RUSTFS_KMS_DEFAULT_KEY_ID", REPL17_KMS_KEY_ID),
            ("RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS", "true"),
            // Per-key KMS authorization is on so this contract is pinned in the
            // configuration replication ships with: the replication worker
            // carries no request identity and must stay exempt.
            ("RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY", "true"),
        ]);
    }
    source_env.start_rustfs_server_with_env(vec![], &source_process_env).await?;

    let mut target_process_env = vec![("NO_PROXY", "127.0.0.1,localhost"), ("HTTP_PROXY", ""), ("HTTPS_PROXY", "")];
    if target_kms {
        target_process_env.extend_from_slice(&[
            ("RUSTFS_KMS_ENABLE", "true"),
            ("RUSTFS_KMS_BACKEND", "local"),
            ("RUSTFS_KMS_KEY_DIR", target_kms_key_dir.as_str()),
            ("RUSTFS_KMS_DEFAULT_KEY_ID", REPL17_KMS_KEY_ID),
            ("RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS", "true"),
            ("RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY", "true"),
        ]);
    }
    target_env
        .start_rustfs_server_without_cleanup_with_env(&target_process_env)
        .await?;

    let source_bucket = format!("repl17-{label}-src");
    let target_bucket = format!("repl17-{label}-dst");
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    source_client.create_bucket().bucket(&source_bucket).send().await?;
    target_client.create_bucket().bucket(&target_bucket).send().await?;
    enable_bucket_versioning(&source_env, &source_bucket).await?;
    enable_bucket_versioning(&target_env, &target_bucket).await?;
    let target_arn = set_replication_target(&source_env, &source_bucket, &target_env, &target_bucket).await?;
    put_bucket_replication(&source_env, &source_bucket, &target_arn).await?;

    Ok((source_env, target_env, source_bucket, target_bucket))
}

async fn assert_managed_sse_replicates_and_reencrypts(label: &str, kms: bool) -> TestResult {
    let (source_env, target_env, source_bucket, target_bucket) = build_sse_replication_pair(label, true, true).await?;
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let key = format!("{label}-contract.txt");
    let body = format!("repl-17 {label} payload").into_bytes();

    let encryption = if kms {
        ServerSideEncryption::AwsKms
    } else {
        ServerSideEncryption::Aes256
    };
    let request = source_client
        .put_object()
        .bucket(&source_bucket)
        .key(&key)
        .body(ByteStream::from(body.clone()))
        .server_side_encryption(encryption.clone());
    let request = if kms {
        request.ssekms_key_id(REPL17_KMS_KEY_ID)
    } else {
        request
    };
    request.send().await?;

    let source = source_client.get_object().bucket(&source_bucket).key(&key).send().await?;
    assert_eq!(source.server_side_encryption(), Some(&encryption));
    let source_etag = source.e_tag().map(str::to_string);
    assert_eq!(source.body.collect().await?.into_bytes().as_ref(), body.as_slice());

    wait_for_source_replication_status(&source_client, &source_bucket, &key, "COMPLETED", false).await?;

    // The target sits on an independent KMS (same key id, different material),
    // so a successful plain GET proves the replica's envelope belongs to the
    // target's KMS: a forwarded source envelope could never unwrap here.
    let replica = target_client.get_object().bucket(&target_bucket).key(&key).send().await?;
    assert_eq!(replica.server_side_encryption(), Some(&encryption));
    let replica_version_id = replica.version_id().map(str::to_string);
    let replica_etag = replica.e_tag().map(str::to_string);
    assert_eq!(replica.body.collect().await?.into_bytes().as_ref(), body.as_slice());

    // The replica must keep the source ETag; otherwise every replication HEAD
    // comparison sees a mismatch and re-replicates the object forever.
    assert_eq!(replica_etag, source_etag, "replica ETag must match the source ETag");

    // Spanning several fast-scanner cycles, the replica must stay the same
    // version: a second version appearing here means the ETag comparison did
    // not converge and the scanner is re-driving the object.
    sleep(Duration::from_secs(5)).await;
    let versions = target_client
        .list_object_versions()
        .bucket(&target_bucket)
        .prefix(&key)
        .send()
        .await?;
    let replica_versions: Vec<_> = versions.versions().iter().filter(|v| v.key() == Some(key.as_str())).collect();
    assert_eq!(replica_versions.len(), 1, "replica must not accumulate versions from re-replication");
    assert_eq!(
        replica_versions[0].version_id().map(str::to_string),
        replica_version_id,
        "replica version must stay stable across scanner cycles"
    );

    Ok(())
}

async fn wait_for_source_delete_marker_replication_failed(
    env: &RustFSTestEnvironment,
    bucket: &str,
    key: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let url = format!(
        "{}/rustfs/admin/v3/replication/diff?bucket={}&prefix={}",
        env.url,
        urlencoding::encode(bucket),
        urlencoding::encode(key)
    );

    loop {
        let response = signed_request(http::Method::POST, &url, &env.access_key, &env.secret_key, None, None).await?;
        if response.status() != StatusCode::OK {
            return Err(format!("replication diff failed with status {}", response.status()).into());
        }
        // The default diff response is a madmin-style stream of bare DiffInfo
        // JSON documents (one per line) with no envelope; assert the envelope
        // is gone so an aggregate-shaped regression fails loudly here.
        let body = response.text().await?;
        let entries = body
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()?;
        for entry in &entries {
            if entry.get("Entries").is_some() {
                return Err(format!("replication diff must stream bare DiffInfo documents, got envelope: {entry}").into());
            }
        }
        let failed = entries.iter().any(|entry| {
            entry["object"].as_str() == Some(key)
                && entry["deletemarker"].as_bool() == Some(true)
                && entry["rStatus"].as_str() == Some("FAILED")
        });
        if failed {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("source delete marker {key} never reported FAILED; last diff={body}").into());
        }
        sleep(Duration::from_millis(200)).await;
    }
}

/// Return the `LastModified` of the (single) delete marker for `key`, if present.
async fn delete_marker_last_modified(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<Option<aws_sdk_s3::primitives::DateTime>, Box<dyn Error + Send + Sync>> {
    let output = client.list_object_versions().bucket(bucket).prefix(key).send().await?;
    Ok(output
        .delete_markers()
        .iter()
        .filter(|marker| marker.key() == Some(key))
        .find_map(|marker| marker.last_modified().cloned()))
}

/// Poll the target until a delete marker for `key` appears, returning its mtime.
async fn wait_for_target_delete_marker(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<aws_sdk_s3::primitives::DateTime, Box<dyn Error + Send + Sync>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(mtime) = delete_marker_last_modified(client, bucket, key).await? {
            return Ok(mtime);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("target never received a delete marker for {key}").into());
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn run_replication_check(
    env: &RustFSTestEnvironment,
    bucket: &str,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/{bucket}?replication-check", env.url);
    signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await
}

async fn remove_replication_target(
    env: &RustFSTestEnvironment,
    bucket: &str,
    arn: &str,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let url = format!(
        "{}/rustfs/admin/v3/remove-remote-target?bucket={}&arn={}",
        env.url,
        urlencoding::encode(bucket),
        urlencoding::encode(arn)
    );
    signed_request(http::Method::DELETE, &url, &env.access_key, &env.secret_key, None, None).await
}

async fn remove_replication_target_request(
    env: &RustFSTestEnvironment,
    bucket: Option<&str>,
    arn: Option<&str>,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let mut url = format!("{}/rustfs/admin/v3/remove-remote-target", env.url);
    let mut separator = '?';

    if let Some(bucket) = bucket {
        url.push(separator);
        separator = '&';
        url.push_str("bucket=");
        url.push_str(&urlencoding::encode(bucket));
    }

    if let Some(arn) = arn {
        url.push(separator);
        url.push_str("arn=");
        url.push_str(&urlencoding::encode(arn));
    }

    signed_request(http::Method::DELETE, &url, &env.access_key, &env.secret_key, None, None).await
}

async fn add_service_account(
    env: &RustFSTestEnvironment,
    signer_access_key: &str,
    signer_secret_key: &str,
    req: &AddServiceAccountReq,
) -> Result<(String, String), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/add-service-account", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        signer_access_key,
        signer_secret_key,
        Some(serde_json::to_vec(req)?),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("add service account failed: {status} {body}").into());
    }

    let body = response.bytes().await?;
    let parsed: serde_json::Value = serde_json::from_slice(&body)?;
    let credentials = parsed
        .get("credentials")
        .ok_or("add service account response missing credentials")?;
    let access_key = credentials
        .get("accessKey")
        .and_then(|value| value.as_str())
        .ok_or("add service account response missing access key")?
        .to_string();
    let secret_key = credentials
        .get("secretKey")
        .and_then(|value| value.as_str())
        .ok_or("add service account response missing secret key")?
        .to_string();

    Ok((access_key, secret_key))
}

async fn add_service_account_with_session_token(
    env: &RustFSTestEnvironment,
    signer_access_key: &str,
    signer_secret_key: &str,
    session_token: &str,
    req: &AddServiceAccountReq,
) -> Result<(String, String), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/add-service-account", env.url);
    let response = signed_request_with_session_token(
        http::Method::PUT,
        &url,
        signer_access_key,
        signer_secret_key,
        session_token,
        Some(serde_json::to_vec(req)?),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("add service account with session token failed: {status} {body}").into());
    }

    let body = response.bytes().await?;
    let parsed: serde_json::Value = serde_json::from_slice(&body)?;
    let credentials = parsed
        .get("credentials")
        .ok_or("add service account response missing credentials")?;
    let access_key = credentials
        .get("accessKey")
        .and_then(|value| value.as_str())
        .ok_or("add service account response missing access key")?
        .to_string();
    let secret_key = credentials
        .get("secretKey")
        .and_then(|value| value.as_str())
        .ok_or("add service account response missing secret key")?
        .to_string();

    Ok((access_key, secret_key))
}

async fn list_service_accounts(
    env: &RustFSTestEnvironment,
    signer_access_key: &str,
    signer_secret_key: &str,
    user: Option<&str>,
) -> Result<ListServiceAccountsResp, Box<dyn Error + Send + Sync>> {
    let mut url = format!("{}/rustfs/admin/v3/list-service-accounts", env.url);
    if let Some(user) = user {
        url.push_str("?user=");
        url.push_str(&urlencoding::encode(user));
    }

    let response = signed_request(http::Method::GET, &url, signer_access_key, signer_secret_key, None, None).await?;
    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("list service accounts failed: {status} {body}").into());
    }

    Ok(response.json().await?)
}

async fn get_account_info(
    env: &RustFSTestEnvironment,
    signer_access_key: &str,
    signer_secret_key: &str,
) -> Result<serde_json::Value, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/accountinfo", env.url);
    let response = signed_request(http::Method::GET, &url, signer_access_key, signer_secret_key, None, None).await?;
    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("account info failed: {status} {body}").into());
    }

    Ok(response.json().await?)
}

async fn wait_for_service_accounts(
    env: &RustFSTestEnvironment,
    signer_access_key: &str,
    signer_secret_key: &str,
    user: Option<&str>,
    expected: &[&str],
) -> Result<ListServiceAccountsResp, Box<dyn Error + Send + Sync>> {
    for _ in 0..20 {
        let resp = list_service_accounts(env, signer_access_key, signer_secret_key, user).await?;
        let access_keys: Vec<&str> = resp.accounts.iter().map(|account| account.access_key.as_str()).collect();
        if expected
            .iter()
            .all(|expected_key| access_keys.iter().any(|actual| actual == expected_key))
        {
            return Ok(resp);
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(format!("service accounts did not reach expected keys {expected:?} on {}", env.address).into())
}

async fn wait_for_object_on_target(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    for _ in 0..40 {
        match client.get_object().bucket(bucket).key(key).send().await {
            Ok(output) => {
                let body = output.body.collect().await?.into_bytes().to_vec();
                return Ok(body);
            }
            Err(err) => {
                if matches!(err.code(), Some("NoSuchKey" | "NotFound" | "NoSuchVersion")) {
                    sleep(Duration::from_millis(250)).await;
                    continue;
                }
                return Err(err.into());
            }
        }
    }

    Err(format!("object {bucket}/{key} was not replicated in time").into())
}

async fn wait_for_bucket_on_target(client: &aws_sdk_s3::Client, bucket: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    for _ in 0..40 {
        match client.head_bucket().bucket(bucket).send().await {
            Ok(_) => return Ok(()),
            Err(err) => {
                if matches!(err.code(), Some("NotFound" | "NoSuchBucket")) {
                    sleep(Duration::from_millis(250)).await;
                    continue;
                }
                return Err(err.into());
            }
        }
    }

    Err(format!("bucket {bucket} was not replicated to the target site in time").into())
}

async fn wait_for_user_get_object(client: &Client, bucket: &str, key: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let mut last_error = None;
    for _ in 0..40 {
        match client.get_object().bucket(bucket).key(key).send().await {
            Ok(output) => {
                let body = output.body.collect().await?.into_bytes().to_vec();
                return Ok(body);
            }
            Err(err) => {
                last_error = Some(err.to_string());
                sleep(Duration::from_millis(250)).await;
            }
        }
    }

    Err(format!(
        "user could not read replicated object {bucket}/{key} in time; last error: {}",
        last_error.unwrap_or_else(|| "unknown".to_string())
    )
    .into())
}

async fn list_replication_targets_request(
    env: &RustFSTestEnvironment,
    bucket: Option<&str>,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let mut url = format!("{}/rustfs/admin/v3/list-remote-targets", env.url);
    if let Some(bucket) = bucket {
        url.push_str("?bucket=");
        url.push_str(&urlencoding::encode(bucket));
    }
    signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await
}

async fn wait_for_remote_target_arn(env: &RustFSTestEnvironment, bucket: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
    for _ in 0..40 {
        let response = list_replication_targets_request(env, Some(bucket)).await?;
        if response.status() == StatusCode::OK {
            let targets: Vec<serde_json::Value> = response.json().await?;
            if let Some(arn) = targets
                .first()
                .and_then(|target| target.get("arn"))
                .and_then(|arn| arn.as_str())
                .filter(|arn| !arn.is_empty())
            {
                return Ok(arn.to_string());
            }
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(format!("site replication did not configure a remote target for bucket {bucket} in time").into())
}

async fn wait_for_remote_target_health_check(
    env: &RustFSTestEnvironment,
    bucket: &str,
    arn: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    for _ in 0..40 {
        let response = list_replication_targets_request(env, Some(bucket)).await?;
        if response.status() == StatusCode::OK {
            let targets: Vec<serde_json::Value> = response.json().await?;
            if targets.iter().any(|target| {
                target.get("arn").and_then(|value| value.as_str()) == Some(arn)
                    && target.get("lastOnline").is_some_and(|value| !value.is_null())
            }) {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(format!("replication target {arn} did not complete a successful health check in time").into())
}

async fn site_replication_add(
    env: &RustFSTestEnvironment,
    sites: &[PeerSite],
) -> Result<ReplicateAddStatus, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/site-replication/add?replicateILMExpiry=false", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(serde_json::to_vec(sites)?),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication add failed: {status} {body}").into());
    }

    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn site_replication_info(env: &RustFSTestEnvironment) -> Result<SiteReplicationInfo, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/site-replication/info", env.url);
    let response = signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication info failed: {status} {body}").into());
    }

    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn site_replication_resync_op(
    env: &RustFSTestEnvironment,
    operation: &str,
    peer: &PeerInfo,
) -> Result<SRResyncOpStatus, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/site-replication/resync/op?operation={operation}", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(serde_json::to_vec(peer)?),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication resync {operation} failed: {status} {body}").into());
    }

    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn site_replication_edit(
    env: &RustFSTestEnvironment,
    query: &str,
    peer: &PeerInfo,
) -> Result<ReplicateEditStatus, Box<dyn Error + Send + Sync>> {
    let url = if query.is_empty() {
        format!("{}/rustfs/admin/v3/site-replication/edit", env.url)
    } else {
        format!("{}/rustfs/admin/v3/site-replication/edit?{query}", env.url)
    };
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(serde_json::to_vec(peer)?),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication edit failed: {status} {body}").into());
    }

    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn site_replication_status(env: &RustFSTestEnvironment, query: &str) -> Result<SRStatusInfo, Box<dyn Error + Send + Sync>> {
    let url = if query.is_empty() {
        format!("{}/rustfs/admin/v3/site-replication/status", env.url)
    } else {
        format!("{}/rustfs/admin/v3/site-replication/status?{query}", env.url)
    };
    let response = signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication status failed: {status} {body}").into());
    }

    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

fn proxy_error_response(error: impl std::fmt::Display) -> Response<Full<bytes::Bytes>> {
    Response::builder()
        .status(reqwest::StatusCode::BAD_GATEWAY)
        .body(Full::new(bytes::Bytes::from(error.to_string())))
        .expect("static proxy response must be valid")
}

async fn forward_replication_proxy_request(
    request: Request<Incoming>,
    backend_url: &str,
    client: &reqwest::Client,
    request_count: &AtomicU64,
    mut replication_enabled: watch::Receiver<bool>,
) -> Response<Full<bytes::Bytes>> {
    let (parts, body) = request.into_parts();
    let is_replication = parts
        .headers
        .get(X_AMZ_REPLICATION_STATUS)
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"REPLICA"));
    if is_replication {
        request_count.fetch_add(1, Ordering::Relaxed);
        while !*replication_enabled.borrow() {
            if replication_enabled.changed().await.is_err() {
                return proxy_error_response("replication gate closed");
            }
        }
    }

    let Some(path_and_query) = parts.uri.path_and_query() else {
        return proxy_error_response("request URI omitted path");
    };
    let body = match body.collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => return proxy_error_response(error),
    };
    let mut forwarded = client.request(parts.method, format!("{backend_url}{path_and_query}"));
    for (name, value) in &parts.headers {
        forwarded = forwarded.header(name, value);
    }
    let response = match forwarded.body(body).send().await {
        Ok(response) => response,
        Err(error) => return proxy_error_response(error),
    };
    let status = response.status();
    let headers = response.headers().clone();
    let body = match response.bytes().await {
        Ok(body) => body,
        Err(error) => return proxy_error_response(error),
    };
    let mut proxied = Response::builder().status(status);
    for (name, value) in &headers {
        proxied = proxied.header(name, value);
    }
    proxied.body(Full::new(body)).expect("upstream response must be valid")
}

async fn start_replication_counting_proxy(
    backend_url: &str,
    tasks: &mut JoinSet<()>,
) -> Result<(String, Arc<AtomicU64>, watch::Sender<bool>), Box<dyn Error + Send + Sync>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_url = format!("http://{}", listener.local_addr()?);
    let backend_url = backend_url.to_string();
    let request_count = Arc::new(AtomicU64::new(0));
    let task_request_count = request_count.clone();
    let (replication_enabled, task_replication_enabled) = watch::channel(true);
    tasks.spawn(async move {
        let client = local_http_client();
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else { break };
                    let backend_url = backend_url.clone();
                    let client = client.clone();
                    let request_count = task_request_count.clone();
                    let replication_enabled = task_replication_enabled.clone();
                    connections.spawn(async move {
                        let service = service_fn(move |request| {
                            let backend_url = backend_url.clone();
                            let client = client.clone();
                            let request_count = request_count.clone();
                            let replication_enabled = replication_enabled.clone();
                            async move {
                                Ok::<_, Infallible>(
                                    forward_replication_proxy_request(
                                        request,
                                        &backend_url,
                                        &client,
                                        &request_count,
                                        replication_enabled,
                                    )
                                    .await,
                                )
                            }
                        });
                        let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
                    });
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    Ok((proxy_url, request_count, replication_enabled))
}

async fn site_replication_remove(
    env: &RustFSTestEnvironment,
    req: &SRRemoveReq,
) -> Result<ReplicateRemoveStatus, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/site-replication/remove", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(serde_json::to_vec(req)?),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication remove failed: {status} {body}").into());
    }

    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn site_replication_state_edit(
    env: &RustFSTestEnvironment,
    body: &rustfs_madmin::SRStateEditReq,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/site-replication/state/edit", env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(serde_json::to_vec(body)?),
        Some("application/json"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("site replication state edit failed: {status} {body}").into());
    }

    Ok(())
}

/// Start a bucket-level replication resync (`PUT ?replication-reset`) and
/// return the target `(arn, reset_id)`, asserting the response carries the
/// madmin `ResyncTargetsInfo` shape (`target[0].arn` / `target[0].resetid`)
/// that `mc replicate resync start` decodes.
async fn start_bucket_replication_reset(
    env: &RustFSTestEnvironment,
    bucket: &str,
) -> Result<(String, String), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/{bucket}?replication-reset", env.url);
    let response = signed_request(http::Method::PUT, &url, &env.access_key, &env.secret_key, None, None).await?;
    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("replication reset start failed: {status} {body}").into());
    }
    let payload: serde_json::Value = response.json().await?;
    let arn = payload["target"][0]["arn"].as_str().unwrap_or_default().to_string();
    let reset_id = payload["target"][0]["resetid"].as_str().unwrap_or_default().to_string();
    if arn.is_empty() || reset_id.is_empty() {
        return Err(format!("replication reset response missing madmin target[0].arn/resetid: {payload}").into());
    }
    Ok((arn, reset_id))
}

async fn get_replication_reset_status(
    env: &RustFSTestEnvironment,
    bucket: &str,
    arn: &str,
) -> Result<ReplicationResetStatusResponse, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/{bucket}?replication-reset-status&arn={}", env.url, urlencoding::encode(arn));
    let response = signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("replication reset status failed: {status} {body}").into());
    }

    Ok(serde_json::from_slice(&response.bytes().await?)?)
}

async fn wait_for_site_replication_enabled(
    env: &RustFSTestEnvironment,
    expected_sites: usize,
) -> Result<SiteReplicationInfo, Box<dyn Error + Send + Sync>> {
    for _ in 0..40 {
        let info = site_replication_info(env).await?;
        if info.enabled
            && info.sites.len() == expected_sites
            && info.sites.iter().all(|peer| peer.sync_state == SyncStatus::Enable)
        {
            return Ok(info);
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(format!("site replication did not reach {expected_sites} sites on {}", env.address).into())
}

async fn wait_for_site_replication_disabled(
    env: &RustFSTestEnvironment,
) -> Result<SiteReplicationInfo, Box<dyn Error + Send + Sync>> {
    wait_for_site_replication_info(env, |info| !info.enabled && info.sites.is_empty()).await
}

async fn assert_site_replication_bucket_detached(
    env: &RustFSTestEnvironment,
    bucket: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let targets_response = list_replication_targets_request(env, Some(bucket)).await?;
    if targets_response.status() != StatusCode::OK {
        return Err(format!("list remote targets failed after site removal: {}", targets_response.status()).into());
    }
    let targets: Vec<serde_json::Value> = targets_response.json().await?;
    if !targets.is_empty() {
        return Err(format!("site removal left remote targets for {bucket}: {targets:?}").into());
    }

    let replication_response = get_bucket_replication(env, bucket).await?;
    if replication_response.status() != StatusCode::NOT_FOUND {
        let status = replication_response.status();
        let body = replication_response.text().await.unwrap_or_default();
        return Err(format!("site removal left replication config for {bucket}: {status} {body}").into());
    }

    Ok(())
}

async fn wait_for_site_replication_info<F>(
    env: &RustFSTestEnvironment,
    predicate: F,
) -> Result<SiteReplicationInfo, Box<dyn Error + Send + Sync>>
where
    F: Fn(&SiteReplicationInfo) -> bool,
{
    // 30s to match wait_for_replication_state: the three-node site tests run
    // several full rustfs processes on one runner, so peer-state propagation
    // can take well over 10s under CI load.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let info = site_replication_info(env).await?;
        if predicate(&info) {
            return Ok(info);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("site replication info did not reach expected state on {}", env.address).into());
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_site_replication_status<F>(
    env: &RustFSTestEnvironment,
    query: &str,
    predicate: F,
) -> Result<SRStatusInfo, Box<dyn Error + Send + Sync>>
where
    F: Fn(&SRStatusInfo) -> bool,
{
    // Same 30s ceiling as wait_for_site_replication_info: the status probes
    // fan out to every peer, so they see the same multi-process CI load.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let status = site_replication_status(env, query).await?;
        if predicate(&status) {
            return Ok(status);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("site replication status did not reach expected state on {}", env.address).into());
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_replication_reset_target<F>(
    env: &RustFSTestEnvironment,
    bucket: &str,
    arn: &str,
    predicate: F,
) -> Result<ReplicationResetStatusTarget, Box<dyn Error + Send + Sync>>
where
    F: Fn(&ReplicationResetStatusTarget) -> bool,
{
    let mut last_seen = None;
    for _ in 0..40 {
        let status = get_replication_reset_status(env, bucket, arn).await?;
        if let Some(target) = status.targets.into_iter().find(|target| target.arn == arn) {
            if predicate(&target) {
                return Ok(target);
            }
            last_seen = Some(target);
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(format!(
        "replication reset target {arn} for bucket {bucket} did not reach expected state; last seen: {:?}",
        last_seen
    )
    .into())
}

async fn build_replication_pair(
    enable_target_versioning: bool,
) -> Result<(RustFSTestEnvironment, RustFSTestEnvironment, String), Box<dyn Error + Send + Sync>> {
    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-check-src";
    let target_bucket = "replication-check-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, source_bucket).await?;
    if enable_target_versioning {
        enable_bucket_versioning(&target_env, target_bucket).await?;
    }

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    Ok((source_env, target_env, source_bucket.to_string()))
}

/// P0-6: CopyObject creates a new object on the destination key, so it must be
/// scheduled for bucket replication exactly like PutObject (MinIO
/// CopyObjectHandler parity). Before the fix the copy path never consulted the
/// replication config: the destination object stayed local forever (its status
/// metadata was inherited wholesale from the source, so the scanner heal pass
/// skipped it too — no PENDING/FAILED marker meant nothing to re-drive).
#[tokio::test]
async fn test_copy_object_replicates_to_target() -> TestResult {
    init_logging();

    let (source_env, target_env, source_bucket) = build_replication_pair(true).await?;
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let target_bucket = "replication-check-dst";

    let src_key = "copy-repl-source.txt";
    let dst_key = "copy-repl-destination.txt";
    let payload = b"copy object replication payload".to_vec();

    source_client
        .put_object()
        .bucket(&source_bucket)
        .key(src_key)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await?;
    assert_eq!(wait_for_object_on_target(&target_client, target_bucket, src_key).await?, payload);
    // Wait for the source object's terminal COMPLETED status so the copy below
    // starts from metadata that carries a stale terminal replication state; the
    // copy must not inherit it (MinIO filterReplicationStatusMetadata parity)
    // and must drive its own PENDING -> COMPLETED cycle.
    wait_for_source_replication_status(&source_client, &source_bucket, src_key, "COMPLETED", false).await?;

    source_client
        .copy_object()
        .bucket(&source_bucket)
        .key(dst_key)
        .copy_source(format!("{source_bucket}/{src_key}"))
        .send()
        .await?;

    assert_eq!(
        wait_for_object_on_target(&target_client, target_bucket, dst_key).await?,
        payload,
        "CopyObject destination must replicate to the remote target"
    );
    wait_for_source_replication_status(&source_client, &source_bucket, dst_key, "COMPLETED", false).await?;

    Ok(())
}

/// P0-6 companion: snowball auto-extract writes each archive member as an
/// independent object; every member must replicate to the remote target like a
/// regular PUT (MinIO PutObjectExtract parity).
#[tokio::test]
async fn test_snowball_extract_replicates_members_to_target() -> TestResult {
    init_logging();

    let (source_env, target_env, source_bucket) = build_replication_pair(true).await?;
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let target_bucket = "replication-check-dst";

    let members: [(&str, &[u8]); 2] = [
        ("snowball/member-one.txt", b"first member payload"),
        ("snowball/member-two.txt", b"second member payload"),
    ];

    let mut builder = tokio_tar::Builder::new(std::io::Cursor::new(Vec::new()));
    for (path, data) in members {
        let mut header = tokio_tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append_data(&mut header, path, std::io::Cursor::new(data)).await?;
    }
    let archive = builder.into_inner().await?.into_inner();

    source_client
        .put_object()
        .bucket(&source_bucket)
        .key("members.tar")
        .metadata("Snowball-Auto-Extract", "true")
        .body(ByteStream::from(archive))
        .send()
        .await?;

    for (key, data) in members {
        assert_eq!(
            wait_for_object_on_target(&target_client, target_bucket, key).await?,
            data,
            "snowball-extracted member {key} must replicate to the remote target"
        );
        wait_for_source_replication_status(&source_client, &source_bucket, key, "COMPLETED", false).await?;
    }

    Ok(())
}

#[tokio::test]
async fn test_replication_check_succeeds_with_remote_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (source_env, target_env, source_bucket) = build_replication_pair(true).await?;
    let response = run_replication_check(&source_env, &source_bucket).await?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await?;
    assert_eq!(payload["Status"], "OK", "{payload}");
    assert_eq!(payload["ActiveMutation"], true);
    assert_eq!(payload["Targets"].as_array().map(Vec::len), Some(1));
    assert_eq!(payload["Targets"][0]["Status"], "OK", "{payload}");
    assert_eq!(payload["Targets"][0]["Phases"]["Put"]["Status"], "OK", "{payload}");
    // A RustFS target adopts the source version id, so the P1-19
    // version-identity probe passes.
    assert_eq!(payload["Targets"][0]["Phases"]["VersionFidelity"]["Status"], "OK", "{payload}");
    // A RustFS target preserves the SSE-C passthrough transport headers and
    // echoes the customer algorithm on the replication-check HEAD (N2).
    assert_eq!(payload["Targets"][0]["Phases"]["SsecPassthrough"]["Status"], "OK", "{payload}");
    assert_eq!(payload["Targets"][0]["Phases"]["DeleteMarker"]["Status"], "OK", "{payload}");
    assert_eq!(payload["Targets"][0]["Phases"]["VersionDelete"]["Status"], "OK", "{payload}");
    assert_eq!(payload["Targets"][0]["Phases"]["Cleanup"]["Status"], "OK", "{payload}");

    let target_client = target_env.create_s3_client();
    let versions = target_client
        .list_object_versions()
        .bucket("replication-check-dst")
        .prefix(".rustfs.sys/replication-check/")
        .send()
        .await?;
    assert!(
        versions.versions().is_empty() && versions.delete_markers().is_empty(),
        "successful check must remove every probe version and delete marker"
    );

    Ok(())
}

#[tokio::test]
async fn test_replication_check_rejects_target_without_object_lock() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-check-lock-src";
    let target_bucket = "replication-check-lock-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client
        .create_bucket()
        .bucket(source_bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    let response = run_replication_check(&source_env, source_bucket).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::OK);
    let payload: serde_json::Value = serde_json::from_str(&body)?;
    assert_eq!(payload["Status"], "FAILED");
    assert_eq!(payload["Targets"][0]["Status"], "FAILED");
    assert_eq!(payload["Targets"][0]["Phases"]["ObjectLock"]["Status"], "FAILED");
    assert!(
        payload["Targets"][0]["Phases"]["ObjectLock"]["Error"]
            .as_str()
            .unwrap_or_default()
            .contains("object lock"),
        "unexpected response: {body}"
    );
    assert_eq!(payload["Targets"][0]["Phases"]["Put"]["Status"], "SKIPPED");

    Ok(())
}

#[tokio::test]
async fn test_set_remote_target_rejects_unversioned_source_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-check-unversioned-src";
    let target_bucket = "replication-check-unversioned-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&target_env, target_bucket).await?;

    let err = set_replication_target(&source_env, source_bucket, &target_env, target_bucket)
        .await
        .expect_err("unversioned source bucket should be rejected during remote target setup");
    let err = err.to_string();

    assert!(err.contains("400 Bad Request"), "unexpected set remote target error: {err}");
    assert!(err.contains("InvalidRequest"), "unexpected set remote target error: {err}");
    assert!(
        err.to_ascii_lowercase().contains("not versioned"),
        "unexpected set remote target error: {err}"
    );

    Ok(())
}

#[tokio::test]
async fn test_replication_check_rejects_unversioned_source_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let bucket = "replication-check-source-unversioned";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;

    let response = run_replication_check(&env, bucket).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("versioning"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
async fn test_replication_check_rejects_missing_replication_config() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let bucket = "replication-check-missing-config";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&env, bucket).await?;

    let response = run_replication_check(&env, bucket).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("ReplicationConfigurationNotFoundError"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
async fn test_replication_check_rejects_invalid_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let response = run_replication_check(&env, "replication-check-no-such-bucket").await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("NoSuchBucket"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
async fn test_set_remote_target_rejects_same_bucket_on_same_deployment() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let bucket = "replication-check-same-target";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&env, bucket).await?;

    let body = serde_json::json!({
        "endpoint": env.address,
        "credentials": {
            "accessKey": env.access_key,
            "secretKey": env.secret_key
        },
        "targetbucket": bucket,
        "secure": false,
        "type": "replication"
    });
    let url = format!("{}/rustfs/admin/v3/set-remote-target?bucket={}", env.url, urlencoding::encode(bucket));
    let response = signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await?;

    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("IncorrectEndpoint"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
async fn test_set_remote_target_rejects_unversioned_target_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-check-src";
    let target_bucket = "replication-check-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;

    let err = set_replication_target(&source_env, source_bucket, &target_env, target_bucket)
        .await
        .expect_err("unversioned target bucket should be rejected during remote target setup");
    assert!(
        err.to_string().contains("Remote target bucket not versioned"),
        "unversioned destination must not be misreported as the source: {err}"
    );

    Ok(())
}

#[tokio::test]
async fn test_set_remote_target_update_requires_arn() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-update-needs-arn-src";
    let target_bucket = "replication-update-needs-arn-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let response = send_set_replication_target_request(
        &source_env,
        source_bucket,
        true,
        serde_json::json!({
            "endpoint": target_env.address,
            "credentials": {
                "accessKey": target_env.access_key,
                "secretKey": target_env.secret_key
            },
            "targetbucket": target_bucket,
            "secure": false,
            "type": "replication"
        }),
    )
    .await?;

    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("arn is required"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
async fn test_set_remote_target_update_rejects_missing_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-update-missing-target-src";
    let target_bucket = "replication-update-missing-target-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let response = send_set_replication_target_request(
        &source_env,
        source_bucket,
        true,
        serde_json::json!({
            "endpoint": target_env.address,
            "credentials": {
                "accessKey": target_env.access_key,
                "secretKey": target_env.secret_key
            },
            "targetbucket": target_bucket,
            "secure": false,
            "type": "replication",
            "arn": "arn:aws:s3:us-east-1:123456789012:replication::missing-target"
        }),
    )
    .await?;

    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("target not found"), "unexpected response: {body}");

    Ok(())
}

async fn send_set_replication_target_update_request(
    source_env: &RustFSTestEnvironment,
    source_bucket: &str,
    ops: &[&str],
    body: serde_json::Value,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let mut url = format!(
        "{}/rustfs/admin/v3/set-remote-target?bucket={}&update=true",
        source_env.url,
        urlencoding::encode(source_bucket)
    );
    for op in ops {
        url.push_str(&format!("&{op}=true"));
    }
    signed_request(
        http::Method::PUT,
        &url,
        &source_env.access_key,
        &source_env.secret_key,
        Some(body.to_string().into_bytes()),
        Some("application/json"),
    )
    .await
}

async fn fetch_single_target(
    env: &RustFSTestEnvironment,
    bucket: &str,
) -> Result<serde_json::Value, Box<dyn Error + Send + Sync>> {
    let response = list_replication_targets_request(env, Some(bucket)).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let mut targets: Vec<serde_json::Value> = response.json().await?;
    assert_eq!(targets.len(), 1, "expected exactly one remote target");
    Ok(targets.remove(0))
}

#[tokio::test]
async fn test_set_remote_target_partial_update_preserves_credentials() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-partial-update-src";
    let target_bucket = "replication-partial-update-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;

    // A sync-only update whose body omits credentials entirely must succeed and
    // leave the stored connection settings untouched.
    let response = send_set_replication_target_update_request(
        &source_env,
        source_bucket,
        &["sync"],
        serde_json::json!({
            "arn": arn,
            "type": "replication",
            "replicationSync": true
        }),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK, "sync-only update failed: {}", response.text().await?);

    let target = fetch_single_target(&source_env, source_bucket).await?;
    assert_eq!(target["replicationSync"], serde_json::json!(true));
    assert_eq!(target["endpoint"], serde_json::json!(target_env.address));
    assert_eq!(target["credentials"]["accessKey"], serde_json::json!(target_env.access_key));

    // An update naming no field groups is a no-op: a body carrying a different
    // endpoint and credentials must not leak into the stored target.
    let response = send_set_replication_target_update_request(
        &source_env,
        source_bucket,
        &[],
        serde_json::json!({
            "arn": arn,
            "type": "replication",
            "endpoint": "203.0.113.1:9000",
            "credentials": { "accessKey": "other-access", "secretKey": "other-secret" },
            "targetbucket": "elsewhere",
            "secure": false,
            "replicationSync": false
        }),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK, "no-op update failed: {}", response.text().await?);

    let target = fetch_single_target(&source_env, source_bucket).await?;
    assert_eq!(
        target["replicationSync"],
        serde_json::json!(true),
        "no-op update must not change sync mode"
    );
    assert_eq!(
        target["endpoint"],
        serde_json::json!(target_env.address),
        "no-op update must not change endpoint"
    );
    assert_eq!(
        target["credentials"]["accessKey"],
        serde_json::json!(target_env.access_key),
        "no-op update must not change credentials"
    );

    Ok(())
}

#[tokio::test]
async fn test_set_remote_target_rejects_invalid_target_url() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let bucket = "replication-invalid-target-url-src";
    let source_client = source_env.create_s3_client();
    source_client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&source_env, bucket).await?;

    let response = send_set_replication_target_request(
        &source_env,
        bucket,
        false,
        serde_json::json!({
            "endpoint": "://invalid-target-url",
            "credentials": {
                "accessKey": "replication",
                "secretKey": "replication"
            },
            "targetbucket": "target-bucket",
            "secure": false,
            "type": "replication"
        }),
    )
    .await?;

    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("invalid target url"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
async fn test_set_remote_target_rejects_self_signed_https_target_without_skip_tls_verify()
-> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = new_replication_source_env()
        .await
        .map_err(|err| std::io::Error::other(format!("create source env failed: {err}")))?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await
        .map_err(|err| std::io::Error::other(format!("start source HTTP server failed: {err}")))?;

    let mut target_env = new_replication_https_target_env()
        .await
        .map_err(|err| std::io::Error::other(format!("create target env failed: {err}")))?;
    let tls_dir = std::path::PathBuf::from(&target_env.temp_dir).join("tls");
    let target_host = target_env
        .url
        .trim_start_matches("https://")
        .split(':')
        .next()
        .ok_or_else(|| std::io::Error::other("target HTTPS URL missing host"))?
        .to_string();
    generate_self_signed_tls_material(&tls_dir, &target_host)
        .await
        .map_err(|err| std::io::Error::other(format!("generate self-signed TLS material failed: {err}")))?;
    start_https_rustfs_server(&mut target_env, &tls_dir)
        .await
        .map_err(|err| std::io::Error::other(format!("start target HTTPS server failed: {err}")))?;
    let https_client =
        insecure_https_client().map_err(|err| std::io::Error::other(format!("build HTTPS client failed: {err}")))?;
    wait_for_https_server_ready(&https_client, &target_env)
        .await
        .map_err(|err| std::io::Error::other(format!("wait for target HTTPS server ready failed: {err}")))?;

    let source_bucket = "replication-self-signed-src";
    let target_bucket = "replication-self-signed-dst";

    let source_client = source_env.create_s3_client();
    source_client
        .create_bucket()
        .bucket(source_bucket)
        .send()
        .await
        .map_err(|err| std::io::Error::other(format!("create source bucket failed: {err}")))?;
    enable_bucket_versioning(&source_env, source_bucket)
        .await
        .map_err(|err| std::io::Error::other(format!("enable source bucket versioning failed: {err}")))?;

    ensure_https_bucket_exists(&https_client, &target_env, target_bucket)
        .await
        .map_err(|err| std::io::Error::other(format!("create target HTTPS bucket failed: {err}")))?;
    enable_bucket_versioning_over_https(&https_client, &target_env, target_bucket)
        .await
        .map_err(|err| std::io::Error::other(format!("enable target HTTPS bucket versioning failed: {err}")))?;

    let err = set_replication_target_with_options(
        &source_env,
        source_bucket,
        ReplicationTargetOptions {
            endpoint: target_env.url.trim_start_matches("https://"),
            access_key: &target_env.access_key,
            secret_key: &target_env.secret_key,
            target_bucket,
            secure: true,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await
    .expect_err("self-signed HTTPS target should fail without skipTlsVerify");
    let err = err.to_string();

    assert!(err.contains("400 Bad Request"), "unexpected HTTPS target setup error: {err}");
    assert!(err.contains("InvalidRequest"), "unexpected HTTPS target setup error: {err}");
    assert!(
        err.to_ascii_lowercase().contains("certificate") || err.to_ascii_lowercase().contains("tls"),
        "unexpected HTTPS target setup error: {err}"
    );

    Ok(())
}

#[tokio::test]
async fn test_set_remote_target_allows_self_signed_https_target_with_skip_tls_verify() -> Result<(), Box<dyn Error + Send + Sync>>
{
    init_logging();

    let mut source_env = new_replication_source_env()
        .await
        .map_err(|err| std::io::Error::other(format!("create source env failed: {err}")))?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await
        .map_err(|err| std::io::Error::other(format!("start source HTTP server failed: {err}")))?;

    let mut target_env = new_replication_https_target_env()
        .await
        .map_err(|err| std::io::Error::other(format!("create target env failed: {err}")))?;
    let tls_dir = std::path::PathBuf::from(&target_env.temp_dir).join("tls");
    let target_host = target_env
        .url
        .trim_start_matches("https://")
        .split(':')
        .next()
        .ok_or_else(|| std::io::Error::other("target HTTPS URL missing host"))?
        .to_string();
    generate_self_signed_tls_material(&tls_dir, &target_host)
        .await
        .map_err(|err| std::io::Error::other(format!("generate self-signed TLS material failed: {err}")))?;
    start_https_rustfs_server(&mut target_env, &tls_dir)
        .await
        .map_err(|err| std::io::Error::other(format!("start target HTTPS server failed: {err}")))?;
    let https_client =
        insecure_https_client().map_err(|err| std::io::Error::other(format!("build HTTPS client failed: {err}")))?;
    wait_for_https_server_ready(&https_client, &target_env)
        .await
        .map_err(|err| std::io::Error::other(format!("wait for target HTTPS server ready failed: {err}")))?;

    let source_bucket = "replication-self-signed-ok-src";
    let target_bucket = "replication-self-signed-ok-dst";
    let object_key = "self-signed-replication.txt";
    let body = "replication over self-signed https should succeed";
    let post_health_check_key = "self-signed-replication-after-health-check.txt";
    let post_health_check_body = "replication should remain available after the target health check";

    let source_client = source_env.create_s3_client();
    source_client
        .create_bucket()
        .bucket(source_bucket)
        .send()
        .await
        .map_err(|err| std::io::Error::other(format!("create source bucket failed: {err}")))?;
    enable_bucket_versioning(&source_env, source_bucket)
        .await
        .map_err(|err| std::io::Error::other(format!("enable source bucket versioning failed: {err}")))?;

    ensure_https_bucket_exists(&https_client, &target_env, target_bucket)
        .await
        .map_err(|err| std::io::Error::other(format!("create target HTTPS bucket failed: {err}")))?;
    enable_bucket_versioning_over_https(&https_client, &target_env, target_bucket)
        .await
        .map_err(|err| std::io::Error::other(format!("enable target HTTPS bucket versioning failed: {err}")))?;

    let target_arn = set_replication_target_with_options(
        &source_env,
        source_bucket,
        ReplicationTargetOptions {
            endpoint: target_env.url.trim_start_matches("https://"),
            access_key: &target_env.access_key,
            secret_key: &target_env.secret_key,
            target_bucket,
            secure: true,
            skip_tls_verify: true,
            ca_cert_pem: None,
        },
    )
    .await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    let response = run_replication_check(&source_env, source_bucket).await?;
    assert_eq!(response.status(), StatusCode::OK);

    source_client
        .put_object()
        .bucket(source_bucket)
        .key(object_key)
        .body(ByteStream::from(body.as_bytes().to_vec()))
        .send()
        .await?;

    wait_for_replicated_object_over_https(&https_client, &target_env, target_bucket, object_key, body).await?;

    wait_for_remote_target_health_check(&source_env, source_bucket, &target_arn).await?;
    source_client
        .put_object()
        .bucket(source_bucket)
        .key(post_health_check_key)
        .body(ByteStream::from(post_health_check_body.as_bytes().to_vec()))
        .send()
        .await?;

    wait_for_replicated_object_over_https(
        &https_client,
        &target_env,
        target_bucket,
        post_health_check_key,
        post_health_check_body,
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_set_remote_target_rejects_private_ca_https_target_without_ca_cert_pem() -> Result<(), Box<dyn Error + Send + Sync>>
{
    init_logging();

    let mut source_env = new_replication_source_env()
        .await
        .map_err(|err| std::io::Error::other(format!("create source env failed: {err}")))?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await
        .map_err(|err| std::io::Error::other(format!("start source HTTP server failed: {err}")))?;

    let mut target_env = new_replication_https_target_env()
        .await
        .map_err(|err| std::io::Error::other(format!("create target env failed: {err}")))?;
    let tls_dir = std::path::PathBuf::from(&target_env.temp_dir).join("tls");
    let target_host = target_env
        .url
        .trim_start_matches("https://")
        .split(':')
        .next()
        .ok_or_else(|| std::io::Error::other("target HTTPS URL missing host"))?
        .to_string();
    let ca_cert_pem = generate_private_ca_tls_material(&tls_dir, &target_host)
        .await
        .map_err(|err| std::io::Error::other(format!("generate private CA TLS material failed: {err}")))?;
    start_https_rustfs_server(&mut target_env, &tls_dir)
        .await
        .map_err(|err| std::io::Error::other(format!("start target HTTPS server failed: {err}")))?;
    let https_client =
        trusted_https_client(&ca_cert_pem).map_err(|err| std::io::Error::other(format!("build HTTPS client failed: {err}")))?;
    wait_for_https_server_ready(&https_client, &target_env)
        .await
        .map_err(|err| std::io::Error::other(format!("wait for target HTTPS server ready failed: {err}")))?;

    let source_bucket = "replication-private-ca-src";
    let target_bucket = "replication-private-ca-dst";

    let source_client = source_env.create_s3_client();
    source_client
        .create_bucket()
        .bucket(source_bucket)
        .send()
        .await
        .map_err(|err| std::io::Error::other(format!("create source bucket failed: {err}")))?;
    enable_bucket_versioning(&source_env, source_bucket)
        .await
        .map_err(|err| std::io::Error::other(format!("enable source bucket versioning failed: {err}")))?;

    ensure_https_bucket_exists(&https_client, &target_env, target_bucket)
        .await
        .map_err(|err| std::io::Error::other(format!("create target HTTPS bucket failed: {err}")))?;
    enable_bucket_versioning_over_https(&https_client, &target_env, target_bucket)
        .await
        .map_err(|err| std::io::Error::other(format!("enable target HTTPS bucket versioning failed: {err}")))?;

    let err = set_replication_target_with_options(
        &source_env,
        source_bucket,
        ReplicationTargetOptions {
            endpoint: target_env.url.trim_start_matches("https://"),
            access_key: &target_env.access_key,
            secret_key: &target_env.secret_key,
            target_bucket,
            secure: true,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await
    .expect_err("private CA HTTPS target should fail without caCertPem");
    let err = err.to_string();

    assert!(err.contains("400 Bad Request"), "unexpected private CA target setup error: {err}");
    assert!(err.contains("InvalidRequest"), "unexpected private CA target setup error: {err}");
    assert!(
        err.to_ascii_lowercase().contains("certificate") || err.to_ascii_lowercase().contains("tls"),
        "unexpected private CA target setup error: {err}"
    );

    Ok(())
}

#[tokio::test]
async fn test_set_remote_target_allows_private_ca_https_target_with_ca_cert_pem() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = new_replication_source_env()
        .await
        .map_err(|err| std::io::Error::other(format!("create source env failed: {err}")))?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await
        .map_err(|err| std::io::Error::other(format!("start source HTTP server failed: {err}")))?;

    let mut target_env = new_replication_https_target_env()
        .await
        .map_err(|err| std::io::Error::other(format!("create target env failed: {err}")))?;
    let tls_dir = std::path::PathBuf::from(&target_env.temp_dir).join("tls");
    let target_host = target_env
        .url
        .trim_start_matches("https://")
        .split(':')
        .next()
        .ok_or_else(|| std::io::Error::other("target HTTPS URL missing host"))?
        .to_string();
    let ca_cert_pem = generate_private_ca_tls_material(&tls_dir, &target_host)
        .await
        .map_err(|err| std::io::Error::other(format!("generate private CA TLS material failed: {err}")))?;
    start_https_rustfs_server(&mut target_env, &tls_dir)
        .await
        .map_err(|err| std::io::Error::other(format!("start target HTTPS server failed: {err}")))?;
    let https_client =
        trusted_https_client(&ca_cert_pem).map_err(|err| std::io::Error::other(format!("build HTTPS client failed: {err}")))?;
    wait_for_https_server_ready(&https_client, &target_env)
        .await
        .map_err(|err| std::io::Error::other(format!("wait for target HTTPS server ready failed: {err}")))?;

    let source_bucket = "replication-private-ca-ok-src";
    let target_bucket = "replication-private-ca-ok-dst";
    let object_key = "private-ca-replication.txt";
    let body = "replication over private ca https should succeed";

    let source_client = source_env.create_s3_client();
    source_client
        .create_bucket()
        .bucket(source_bucket)
        .send()
        .await
        .map_err(|err| std::io::Error::other(format!("create source bucket failed: {err}")))?;
    enable_bucket_versioning(&source_env, source_bucket)
        .await
        .map_err(|err| std::io::Error::other(format!("enable source bucket versioning failed: {err}")))?;

    ensure_https_bucket_exists(&https_client, &target_env, target_bucket)
        .await
        .map_err(|err| std::io::Error::other(format!("create target HTTPS bucket failed: {err}")))?;
    enable_bucket_versioning_over_https(&https_client, &target_env, target_bucket)
        .await
        .map_err(|err| std::io::Error::other(format!("enable target HTTPS bucket versioning failed: {err}")))?;

    let target_arn = set_replication_target_with_options(
        &source_env,
        source_bucket,
        ReplicationTargetOptions {
            endpoint: target_env.url.trim_start_matches("https://"),
            access_key: &target_env.access_key,
            secret_key: &target_env.secret_key,
            target_bucket,
            secure: true,
            skip_tls_verify: false,
            ca_cert_pem: Some(&ca_cert_pem),
        },
    )
    .await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    let response = run_replication_check(&source_env, source_bucket).await?;
    assert_eq!(response.status(), StatusCode::OK);

    source_client
        .put_object()
        .bucket(source_bucket)
        .key(object_key)
        .body(ByteStream::from(body.as_bytes().to_vec()))
        .send()
        .await?;

    wait_for_replicated_object_over_https(&https_client, &target_env, target_bucket, object_key, body).await?;

    Ok(())
}

#[tokio::test]
async fn test_list_remote_targets_rejects_empty_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let response = list_replication_targets_request(&env, Some("")).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("bucket is required"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
async fn test_list_remote_targets_rejects_invalid_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let response = list_replication_targets_request(&env, Some("missing-replication-target-bucket")).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("NoSuchBucket"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
async fn test_remove_remote_target_rejects_missing_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let bucket = "replication-remove-missing-target";
    let target_bucket = "replication-remove-missing-target-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;

    enable_bucket_versioning(&source_env, bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let arn = set_replication_target(&source_env, bucket, &target_env, target_bucket).await?;

    let first_remove = remove_replication_target(&source_env, bucket, &arn).await?;
    assert_eq!(first_remove.status(), StatusCode::NO_CONTENT);

    let response = remove_replication_target(&source_env, bucket, &arn).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("not found"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
async fn test_remove_remote_target_rejects_missing_arn() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let bucket = "replication-remove-missing-arn";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&env, bucket).await?;

    let response = remove_replication_target_request(&env, Some(bucket), None).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("arn is required"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
async fn test_remove_remote_target_rejects_invalid_bucket() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let response = remove_replication_target_request(
        &env,
        Some("missing-replication-remove-bucket"),
        Some("arn:aws:s3:us-east-1:123456789012:replication::missing"),
    )
    .await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("NoSuchBucket"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
async fn test_remove_remote_target_rejects_target_used_by_replication() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (source_env, _target_env, source_bucket) = build_replication_pair(true).await?;
    let targets_url = format!(
        "{}/rustfs/admin/v3/list-remote-targets?bucket={}",
        source_env.url,
        urlencoding::encode(&source_bucket)
    );
    let targets_response = signed_request(
        http::Method::GET,
        &targets_url,
        &source_env.access_key,
        &source_env.secret_key,
        None,
        None,
    )
    .await?;
    assert_eq!(targets_response.status(), StatusCode::OK);
    let targets: Vec<serde_json::Value> = targets_response.json().await?;
    let arn = targets
        .first()
        .and_then(|target| target.get("arn"))
        .and_then(|arn| arn.as_str())
        .ok_or("replication target arn missing")?
        .to_string();

    let response = remove_replication_target(&source_env, &source_bucket, &arn).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("removal disallowed"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
async fn test_delete_bucket_replication_removes_remote_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-delete-config-src";
    let target_bucket = "replication-delete-config-dst";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    let delete_response = delete_bucket_replication(&source_env, source_bucket).await?;
    assert!(
        delete_response.status().is_success(),
        "unexpected delete status: {}",
        delete_response.status()
    );

    let targets_response = list_replication_targets_request(&source_env, Some(source_bucket)).await?;
    assert_eq!(targets_response.status(), StatusCode::OK);
    let targets: Vec<serde_json::Value> = targets_response.json().await?;
    assert!(
        targets
            .iter()
            .all(|target| target.get("arn").and_then(|arn| arn.as_str()) != Some(target_arn.as_str())),
        "deleted replication config left stale target {target_arn}: {targets:?}"
    );

    let recreated_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &recreated_arn).await?;

    Ok(())
}

#[tokio::test]
async fn test_bucket_replication_replicates_put_object_issue_2539() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "issue-2539-src";
    let target_bucket = "issue-2539-dst";
    let object_key = "put-object.txt";
    let body = "bucket replication should copy PutObject payload";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    source_client
        .put_object()
        .bucket(source_bucket)
        .key(object_key)
        .body(ByteStream::from(body.as_bytes().to_vec()))
        .send()
        .await?;

    wait_for_replicated_object(&target_client, target_bucket, object_key, body).await?;

    Ok(())
}

#[tokio::test]
async fn test_bucket_replication_converges_delete_marker_and_version_purge() -> TestResult {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut source_env_vars = replication_fast_env();
    source_env_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    source_env.start_rustfs_server_with_env(vec![], &source_env_vars).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-delete-state-src";
    let target_bucket = "replication-delete-state-dst";
    let object_key = "versioned-object.txt";
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication_with_delete_statuses(&source_env, source_bucket, &target_arn, "Enabled", Some("Enabled")).await?;

    let first_put = source_client
        .put_object()
        .bucket(source_bucket)
        .key(object_key)
        .body(ByteStream::from_static(b"versioned replication payload v1"))
        .send()
        .await?;
    let purged_version_id = first_put
        .version_id()
        .ok_or("first source PUT omitted version ID")?
        .to_string();
    let second_put = source_client
        .put_object()
        .bucket(source_bucket)
        .key(object_key)
        .body(ByteStream::from_static(b"versioned replication payload v2"))
        .send()
        .await?;
    let retained_version_id = second_put
        .version_id()
        .ok_or("second source PUT omitted version ID")?
        .to_string();
    assert_replication_converged(&source_client, source_bucket, &target_client, target_bucket).await?;

    let delete = source_client
        .delete_object()
        .bucket(source_bucket)
        .key(object_key)
        .send()
        .await?;
    let delete_marker_version_id = delete.version_id().ok_or("source DELETE omitted marker version ID")?;
    assert_eq!(delete.delete_marker(), Some(true));
    assert_replication_converged(&source_client, source_bucket, &target_client, target_bucket).await?;
    assert!(
        list_replication_state(&target_client, target_bucket)
            .await?
            .iter()
            .any(|entry| entry.delete_marker && entry.version_id == delete_marker_version_id),
        "target did not preserve the source delete-marker version ID"
    );

    source_client
        .delete_object()
        .bucket(source_bucket)
        .key(object_key)
        .version_id(&purged_version_id)
        .send()
        .await?;
    assert_replication_converged(&source_client, source_bucket, &target_client, target_bucket).await?;
    let target_state = list_replication_state(&target_client, target_bucket).await?;
    assert!(
        target_state.iter().all(|entry| entry.version_id != purged_version_id),
        "target retained the explicitly purged object version"
    );
    assert!(
        target_state
            .iter()
            .any(|entry| !entry.delete_marker && entry.version_id == retained_version_id),
        "target removed the non-selected object version: {target_state:?}"
    );
    let retained = target_client
        .get_object()
        .bucket(target_bucket)
        .key(object_key)
        .version_id(&retained_version_id)
        .send()
        .await?;
    assert_eq!(retained.body.collect().await?.into_bytes().as_ref(), b"versioned replication payload v2");

    Ok(())
}

#[tokio::test]
async fn test_bucket_replication_disabled_delete_marker_does_not_propagate() -> TestResult {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut source_env_vars = replication_fast_env();
    source_env_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    source_env.start_rustfs_server_with_env(vec![], &source_env_vars).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-no-delete-marker-src";
    let target_bucket = "replication-no-delete-marker-dst";
    let object_key = "retained-object.txt";
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication_with_delete_statuses(&source_env, source_bucket, &target_arn, "Disabled", None).await?;

    source_client
        .put_object()
        .bucket(source_bucket)
        .key(object_key)
        .body(ByteStream::from_static(b"delete-marker replication disabled"))
        .send()
        .await?;
    assert_replication_converged(&source_client, source_bucket, &target_client, target_bucket).await?;

    let delete = source_client
        .delete_object()
        .bucket(source_bucket)
        .key(object_key)
        .send()
        .await?;
    let delete_marker_version_id = delete
        .version_id()
        .ok_or("source DELETE omitted marker version ID")?
        .to_string();

    let observation_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let target_state = list_replication_state(&target_client, target_bucket).await?;
        assert!(
            target_state.iter().all(|entry| !entry.delete_marker),
            "disabled delete-marker replication unexpectedly created a target marker: {target_state:?}"
        );
        if tokio::time::Instant::now() >= observation_deadline {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let retained = target_client
        .get_object()
        .bucket(target_bucket)
        .key(object_key)
        .send()
        .await?;
    assert_eq!(
        retained.body.collect().await?.into_bytes().as_ref(),
        b"delete-marker replication disabled"
    );

    source_client
        .delete_object()
        .bucket(source_bucket)
        .key(object_key)
        .version_id(delete_marker_version_id)
        .send()
        .await?;
    assert_replication_converged(&source_client, source_bucket, &target_client, target_bucket).await?;

    Ok(())
}

/// Bounded executable slice for backlog#1620. It deliberately uses real
/// source and target RustFS processes and leaves the full MinIO
/// interoperability profile for a runner that provisions MinIO credentials
/// and a reachable endpoint.
#[tokio::test]
async fn test_bucket_replication_acceptance_matrix_local_dual_targets() -> TestResult {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut source_env_vars = replication_fast_env();
    source_env_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    source_env.start_rustfs_server_with_env(vec![], &source_env_vars).await?;

    let mut target_env_a = RustFSTestEnvironment::new().await?;
    target_env_a
        .start_rustfs_server_without_cleanup_with_env(&source_env_vars)
        .await?;
    let mut target_env_b = RustFSTestEnvironment::new().await?;
    target_env_b
        .start_rustfs_server_without_cleanup_with_env(&source_env_vars)
        .await?;

    let source_bucket = "replication-acceptance-src";
    let target_bucket_a = "replication-acceptance-dst-a";
    let target_bucket_b = "replication-acceptance-dst-b";
    let source_client = source_env.create_s3_client();
    let target_client_a = target_env_a.create_s3_client();
    let target_client_b = target_env_b.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client_a.create_bucket().bucket(target_bucket_a).send().await?;
    target_client_b.create_bucket().bucket(target_bucket_b).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env_a, target_bucket_a).await?;
    enable_bucket_versioning(&target_env_b, target_bucket_b).await?;

    let target_a_arn = set_replication_target(&source_env, source_bucket, &target_env_a, target_bucket_a).await?;
    let target_b_arn = set_replication_target(&source_env, source_bucket, &target_env_b, target_bucket_b).await?;
    let body = format!(
        r#"<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Role></Role>
  <Rule>
    <ID>matrix-prefix</ID>
    <Priority>110</Priority>
    <Status>Enabled</Status>
    <Filter><Prefix>prefix/</Prefix></Filter>
    <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>
    <DeleteReplication><Status>Enabled</Status></DeleteReplication>
    <ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication>
    <SourceSelectionCriteria><ReplicaModifications><Status>Enabled</Status></ReplicaModifications></SourceSelectionCriteria>
    <Destination><Bucket>{target_a_arn}</Bucket></Destination>
  </Rule>
  <Rule>
    <ID>matrix-both-prefix</ID>
    <Priority>120</Priority>
    <Status>Enabled</Status>
    <Filter><Prefix>both/</Prefix></Filter>
    <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>
    <DeleteReplication><Status>Enabled</Status></DeleteReplication>
    <ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication>
    <Destination><Bucket>{target_a_arn}</Bucket></Destination>
  </Rule>
  <Rule>
    <ID>matrix-tag</ID>
    <Priority>130</Priority>
    <Status>Enabled</Status>
    <Filter><Tag><Key>route</Key><Value>tagged</Value></Tag></Filter>
    <DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>
    <DeleteReplication><Status>Enabled</Status></DeleteReplication>
    <ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication>
    <Destination><Bucket>{target_b_arn}</Bucket></Destination>
  </Rule>
  <Rule>
    <ID>matrix-disabled</ID>
    <Priority>140</Priority>
    <Status>Disabled</Status>
    <Filter><Prefix>disabled/</Prefix></Filter>
    <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>
    <DeleteReplication><Status>Enabled</Status></DeleteReplication>
    <ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication>
    <Destination><Bucket>{target_b_arn}</Bucket></Destination>
  </Rule>
  <Rule>
    <ID>matrix-priority-high</ID>
    <Priority>200</Priority>
    <Status>Enabled</Status>
    <Filter><Prefix>priority/</Prefix></Filter>
    <DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>
    <DeleteReplication><Status>Enabled</Status></DeleteReplication>
    <ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication>
    <Destination><Bucket>{target_a_arn}</Bucket></Destination>
  </Rule>
  <Rule>
    <ID>matrix-priority-low</ID>
    <Priority>100</Priority>
    <Status>Enabled</Status>
    <Filter><Prefix>priority/</Prefix></Filter>
    <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>
    <DeleteReplication><Status>Enabled</Status></DeleteReplication>
    <ExistingObjectReplication><Status>Enabled</Status></ExistingObjectReplication>
    <Destination><Bucket>{target_a_arn}</Bucket></Destination>
  </Rule>
</ReplicationConfiguration>"#
    );
    let url = format!("{}/{source_bucket}?replication", source_env.url);
    let response = signed_request(
        http::Method::PUT,
        &url,
        &source_env.access_key,
        &source_env.secret_key,
        Some(body.into_bytes()),
        Some("application/xml"),
    )
    .await?;

    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("put replication acceptance matrix failed: {status} {body}").into());
    }

    let saved_config = get_bucket_replication(&source_env, source_bucket).await?.text().await?;
    for expected in [
        "matrix-prefix",
        "matrix-tag",
        "matrix-disabled",
        "matrix-priority-high",
        "Priority>200",
        "<Status>Disabled</Status>",
        "<Key>route</Key>",
    ] {
        assert!(saved_config.contains(expected), "replication config omitted {expected}: {saved_config}");
    }

    let version_one = source_client
        .put_object()
        .bucket(source_bucket)
        .key("prefix/versions.txt")
        .body(ByteStream::from_static(b"version-one"))
        .send()
        .await?;
    let version_one_id = version_one
        .version_id()
        .ok_or("first matrix PUT omitted version ID")?
        .to_string();
    let version_two = source_client
        .put_object()
        .bucket(source_bucket)
        .key("prefix/versions.txt")
        .body(ByteStream::from_static(b"version-two"))
        .send()
        .await?;
    let version_two_id = version_two
        .version_id()
        .ok_or("second matrix PUT omitted version ID")?
        .to_string();
    wait_for_replication_state(&target_client_a, target_bucket_a, "prefix object did not replicate", |state| {
        state
            .iter()
            .any(|entry| entry.key == "prefix/versions.txt" && entry.version_id == version_two_id)
    })
    .await?;

    let delete_marker = source_client
        .delete_object()
        .bucket(source_bucket)
        .key("prefix/versions.txt")
        .send()
        .await?;
    let delete_marker_id = delete_marker
        .version_id()
        .ok_or("matrix DELETE omitted marker version ID")?
        .to_string();
    wait_for_replication_state(&target_client_a, target_bucket_a, "enabled delete marker did not replicate", |state| {
        state
            .iter()
            .any(|entry| entry.key == "prefix/versions.txt" && entry.delete_marker && entry.version_id == delete_marker_id)
    })
    .await?;

    source_client
        .delete_object()
        .bucket(source_bucket)
        .key("prefix/versions.txt")
        .version_id(&version_one_id)
        .send()
        .await?;
    wait_for_replication_state(&target_client_a, target_bucket_a, "enabled version purge did not replicate", |state| {
        state.iter().all(|entry| entry.version_id != version_one_id)
    })
    .await?;

    source_client
        .put_object()
        .bucket(source_bucket)
        .key("priority/object.txt")
        .body(ByteStream::from_static(b"priority winner"))
        .send()
        .await?;
    wait_for_user_get_object(&target_client_a, target_bucket_a, "priority/object.txt").await?;
    source_client
        .delete_object()
        .bucket(source_bucket)
        .key("priority/object.txt")
        .send()
        .await?;
    sleep(Duration::from_secs(3)).await;
    let priority_state = list_replication_state(&target_client_a, target_bucket_a).await?;
    assert!(
        priority_state
            .iter()
            .any(|entry| entry.key == "priority/object.txt" && !entry.delete_marker),
        "priority rule did not retain the object version: {priority_state:?}"
    );
    assert!(
        priority_state
            .iter()
            .all(|entry| !(entry.key == "priority/object.txt" && entry.delete_marker)),
        "lower-priority delete-marker rule overrode the higher-priority disabled rule: {priority_state:?}"
    );

    source_client
        .put_object()
        .bucket(source_bucket)
        .key("tagged/object.txt")
        .tagging("route=tagged")
        .body(ByteStream::from_static(b"tag filter"))
        .send()
        .await?;
    wait_for_user_get_object(&target_client_b, target_bucket_b, "tagged/object.txt").await?;
    source_client
        .put_object()
        .bucket(source_bucket)
        .key("tagged/no-match.txt")
        .body(ByteStream::from_static(b"not tagged"))
        .send()
        .await?;
    assert_replication_key_absent(&target_client_b, target_bucket_b, "tagged/no-match.txt", Duration::from_secs(3)).await?;

    source_client
        .put_object()
        .bucket(source_bucket)
        .key("both/object.txt")
        .tagging("route=tagged")
        .body(ByteStream::from_static(b"mixed targets"))
        .send()
        .await?;
    tokio::try_join!(
        wait_for_user_get_object(&target_client_a, target_bucket_a, "both/object.txt"),
        wait_for_user_get_object(&target_client_b, target_bucket_b, "both/object.txt"),
    )?;

    source_client
        .put_object()
        .bucket(source_bucket)
        .key("disabled/object.txt")
        .body(ByteStream::from_static(b"disabled"))
        .send()
        .await?;
    assert_replication_key_absent(&target_client_a, target_bucket_a, "disabled/object.txt", Duration::from_secs(3)).await?;
    assert_replication_key_absent(&target_client_b, target_bucket_b, "disabled/object.txt", Duration::from_secs(3)).await?;

    source_client
        .delete_object()
        .bucket(source_bucket)
        .key("tagged/object.txt")
        .send()
        .await?;
    sleep(Duration::from_secs(3)).await;
    let tagged_state = list_replication_state(&target_client_b, target_bucket_b).await?;
    assert!(
        tagged_state
            .iter()
            .any(|entry| entry.key == "tagged/object.txt" && !entry.delete_marker),
        "tag rule should retain the replicated data version: {tagged_state:?}"
    );
    assert!(
        tagged_state
            .iter()
            .all(|entry| !(entry.key == "tagged/object.txt" && entry.delete_marker)),
        "tag rule with disabled delete-marker replication created a marker: {tagged_state:?}"
    );

    // AWS S3 and MinIO both reject suspending versioning on a bucket that
    // carries a replication configuration (InvalidBucketState): suspension
    // would mint null versions that versioned replication can never converge.
    let suspend_err = source_client
        .put_bucket_versioning()
        .bucket(source_bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await
        .expect_err("suspending versioning on a replication source must be rejected");
    assert_eq!(
        suspend_err.as_service_error().and_then(|error| error.code()),
        Some("InvalidBucketState"),
        "suspension on a replication source must fail with InvalidBucketState: {suspend_err:?}"
    );

    // The rejected suspension must leave the versioning + replication state
    // fully intact: a fresh matched PUT still replicates with a real version.
    let post_reject_put = source_client
        .put_object()
        .bucket(source_bucket)
        .key("prefix/after-rejected-suspend.txt")
        .body(ByteStream::from_static(b"still replicating"))
        .send()
        .await?;
    let post_reject_version_id = post_reject_put
        .version_id()
        .ok_or("PUT after rejected suspension omitted version ID")?
        .to_string();
    wait_for_replication_state(
        &target_client_a,
        target_bucket_a,
        "replication stopped after rejected versioning suspension",
        |state| {
            state
                .iter()
                .any(|entry| entry.key == "prefix/after-rejected-suspend.txt" && entry.version_id == post_reject_version_id)
        },
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_single_bucket_multipart_replication_fans_out_to_multiple_targets() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    const PART_SIZE: usize = 5 * 1024 * 1024;
    const PART_COUNT: usize = 3;

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env_a = RustFSTestEnvironment::new().await?;
    target_env_a.start_rustfs_server_without_cleanup(vec![]).await?;

    let mut target_env_b = RustFSTestEnvironment::new().await?;
    target_env_b.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-multipart-fanout-src";
    let target_bucket_a = "replication-multipart-fanout-dst-a";
    let target_bucket_b = "replication-multipart-fanout-dst-b";
    let object_key = "multipart-fanout.bin";

    let source_client = source_env.create_s3_client();
    let target_client_a = target_env_a.create_s3_client();
    let target_client_b = target_env_b.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client_a.create_bucket().bucket(target_bucket_a).send().await?;
    target_client_b.create_bucket().bucket(target_bucket_b).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env_a, target_bucket_a).await?;
    enable_bucket_versioning(&target_env_b, target_bucket_b).await?;

    let target_arn_a = set_replication_target(&source_env, source_bucket, &target_env_a, target_bucket_a).await?;
    let target_arn_b = set_replication_target(&source_env, source_bucket, &target_env_b, target_bucket_b).await?;
    put_bucket_replication_rules(&source_env, source_bucket, &[target_arn_a.as_str(), target_arn_b.as_str()]).await?;

    let created = source_client
        .create_multipart_upload()
        .bucket(source_bucket)
        .key(object_key)
        .content_type("application/x-fanout")
        .metadata("app", "fanout")
        .send()
        .await?;
    let upload_id = created.upload_id().ok_or("missing multipart upload id")?.to_string();
    let mut completed_parts = Vec::with_capacity(PART_COUNT);
    let mut payload = Vec::with_capacity(PART_SIZE * PART_COUNT);

    for part_number in 1..=PART_COUNT {
        let part = vec![u8::try_from(part_number)?; PART_SIZE];
        payload.extend_from_slice(&part);
        let uploaded = source_client
            .upload_part()
            .bucket(source_bucket)
            .key(object_key)
            .upload_id(&upload_id)
            .part_number(i32::try_from(part_number)?)
            .body(ByteStream::from(part))
            .send()
            .await?;
        completed_parts.push(
            CompletedPart::builder()
                .part_number(i32::try_from(part_number)?)
                .set_e_tag(uploaded.e_tag().map(str::to_string))
                .build(),
        );
    }

    let completed = source_client
        .complete_multipart_upload()
        .bucket(source_bucket)
        .key(object_key)
        .upload_id(&upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build())
        .send()
        .await?;
    let source_etag = completed.e_tag().ok_or("completed multipart upload omitted ETag")?;
    let multipart_suffix = format!("-{PART_COUNT}");
    assert!(
        source_etag.trim_matches('"').ends_with(&multipart_suffix),
        "unexpected source multipart ETag: {source_etag}"
    );

    let expected_sha256: [u8; 32] = Sha256::digest(&payload).into();
    tokio::try_join!(
        wait_for_replicated_sha256(&target_client_a, target_bucket_a, object_key, expected_sha256),
        wait_for_replicated_sha256(&target_client_b, target_bucket_b, object_key, expected_sha256),
    )?;

    let target_head_a = target_client_a
        .head_object()
        .bucket(target_bucket_a)
        .key(object_key)
        .send()
        .await?;
    // Multipart replicas carry their metadata through CreateMultipartUpload;
    // this pins the plaintext side of the multipart header fix.
    assert_eq!(target_head_a.content_type(), Some("application/x-fanout"));
    assert_eq!(target_head_a.metadata().and_then(|m| m.get("app").map(String::as_str)), Some("fanout"));
    let target_etag_a = target_head_a.e_tag().ok_or("first target omitted ETag")?.to_string();
    let target_etag_b = target_client_b
        .head_object()
        .bucket(target_bucket_b)
        .key(object_key)
        .send()
        .await?
        .e_tag()
        .ok_or("second target omitted ETag")?
        .to_string();

    assert_eq!(target_etag_a, source_etag);
    assert_eq!(target_etag_b, source_etag);

    Ok(())
}

#[tokio::test]
async fn test_repl17_failure_observation_helpers() -> TestResult {
    let expected_key = "space + percent%.txt";
    let payload = format!(
        "{{\"note\":\"é\",\"Records\":[{{\"eventName\":\"{REPLICATION_FAILED_EVENT}\",\"s3\":{{\"object\":{{\"key\":\"space+%2B+percent%25.txt\"}}}}}}]}}\n"
    );
    let utf8_split = payload.find('é').expect("multibyte fixture must be present") + 1;
    let encoded_plus_split = payload.find("%2B").expect("encoded plus must be present") + 1;
    let chunks = vec![
        Ok::<_, Infallible>(Bytes::copy_from_slice(&payload.as_bytes()[..utf8_split])),
        Ok::<_, Infallible>(Bytes::copy_from_slice(&payload.as_bytes()[utf8_split..encoded_plus_split])),
        Ok::<_, Infallible>(Bytes::copy_from_slice(&payload.as_bytes()[encoded_plus_split..])),
    ];
    wait_for_replication_failure_event_stream(futures::stream::iter(chunks), expected_key, Duration::from_millis(100)).await?;

    let ping_only = futures::stream::unfold((), |_| async {
        sleep(Duration::from_millis(5)).await;
        Some((Ok::<_, Infallible>(Bytes::from_static(b"{}\n")), ()))
    });
    let timeout_error = wait_for_replication_failure_event_stream(ping_only, expected_key, Duration::from_millis(25))
        .await
        .expect_err("ping-only event stream must hit the global deadline");
    assert!(timeout_error.to_string().contains("was not received within"));

    let mut oversized_line = vec![b'x'; REPLICATION_EVENT_MAX_BUFFER_BYTES];
    oversized_line.push(b'\n');
    let oversized = futures::stream::once(async { Ok::<_, Infallible>(Bytes::from(oversized_line)) });
    let oversized_error = wait_for_replication_failure_event_stream(oversized, expected_key, Duration::from_millis(100))
        .await
        .expect_err("oversized event lines must be rejected");
    assert!(oversized_error.to_string().contains("buffer exceeded 1 MiB"));

    let version_output = ListObjectVersionsOutput::builder()
        .versions(ObjectVersion::builder().key(expected_key).build())
        .build();
    assert!(target_history_contains_key(&version_output, expected_key));
    let marker_output = ListObjectVersionsOutput::builder()
        .delete_markers(DeleteMarkerEntry::builder().key(expected_key).build())
        .build();
    assert!(target_history_contains_key(&marker_output, expected_key));
    assert!(!target_history_contains_key(&marker_output, "other-key"));

    Ok(())
}

/// backlog#1147 repl-17 / backlog#1783: SSE-C objects replicate as ciphertext
/// passthrough — the source cannot decrypt them (no customer key server-side),
/// so the stored ciphertext and its encryption metadata travel verbatim and
/// the replica is decryptable only with the original customer key. The
/// backlog#1291 property still holds: never a silent plaintext replica.
#[tokio::test]
async fn test_bucket_replication_sse_c_contract() -> TestResult {
    init_logging();

    let (source_env, target_env, source_bucket, target_bucket) = build_sse_replication_pair("ssec", false, false).await?;
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let key = "ssec-contract.txt";
    let body = b"repl-17 SSE-C payload";
    let customer_key = BASE64_STANDARD.encode(REPL17_SSEC_KEY);
    let customer_key_md5 = sse_customer_key_md5_base64(REPL17_SSEC_KEY);

    source_client
        .put_object()
        .bucket(&source_bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_key_md5)
        .send()
        .await?;

    let source = source_client
        .get_object()
        .bucket(&source_bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_key_md5)
        .send()
        .await?;
    let source_etag = source.e_tag().map(str::to_string);
    assert_eq!(source.body.collect().await?.into_bytes().as_ref(), body);

    wait_for_source_replication_status(&source_client, &source_bucket, key, "COMPLETED", true).await?;

    // The replica is readable only with the original customer key.
    let replica = target_client
        .get_object()
        .bucket(&target_bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_key_md5)
        .send()
        .await?;
    assert_eq!(replica.sse_customer_algorithm(), Some("AES256"));
    let replica_etag = replica.e_tag().map(str::to_string);
    assert_eq!(replica.body.collect().await?.into_bytes().as_ref(), body);
    assert_eq!(replica_etag, source_etag, "replica ETag must match the source ETag");

    // Without the customer key the replica must not be readable — the direct
    // detection point for a silent-plaintext replica (backlog#1291).
    let plain_read = target_client.get_object().bucket(&target_bucket).key(key).send().await;
    assert!(plain_read.is_err(), "SSE-C replica must not be readable without the customer key");

    // A wrong customer key must fail too.
    let wrong_key = BASE64_STANDARD.encode("99999999999999999999999999999999");
    let wrong_key_md5 = sse_customer_key_md5_base64("99999999999999999999999999999999");
    let wrong_read = target_client
        .get_object()
        .bucket(&target_bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&wrong_key)
        .sse_customer_key_md5(&wrong_key_md5)
        .send()
        .await;
    assert!(wrong_read.is_err(), "SSE-C replica must reject a wrong customer key");

    Ok(())
}

/// backlog#1783: SSE-C multipart objects pass through as ciphertext part by
/// part — part boundaries and the encrypted-multipart marker survive so the
/// replica decrypts each part with its part-derived nonce.
#[tokio::test]
async fn test_bucket_replication_sse_c_multipart_passthrough() -> TestResult {
    init_logging();

    const PART_SIZE: usize = 5 * 1024 * 1024;
    const PART_COUNT: usize = 3;

    let (source_env, target_env, source_bucket, target_bucket) = build_sse_replication_pair("ssec-mp", false, false).await?;
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let key = "ssec-mp-contract.bin";
    let customer_key = BASE64_STANDARD.encode(REPL17_SSEC_KEY);
    let customer_key_md5 = sse_customer_key_md5_base64(REPL17_SSEC_KEY);

    let created = source_client
        .create_multipart_upload()
        .bucket(&source_bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_key_md5)
        .send()
        .await?;
    let upload_id = created.upload_id().ok_or("missing multipart upload id")?.to_string();

    let mut completed_parts = Vec::with_capacity(PART_COUNT);
    let mut payload = Vec::with_capacity(PART_SIZE * PART_COUNT);
    for part_number in 1..=PART_COUNT {
        let part = vec![u8::try_from(part_number)?; PART_SIZE];
        payload.extend_from_slice(&part);
        let uploaded = source_client
            .upload_part()
            .bucket(&source_bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(i32::try_from(part_number)?)
            .body(ByteStream::from(part))
            .sse_customer_algorithm("AES256")
            .sse_customer_key(&customer_key)
            .sse_customer_key_md5(&customer_key_md5)
            .send()
            .await?;
        completed_parts.push(
            CompletedPart::builder()
                .part_number(i32::try_from(part_number)?)
                .set_e_tag(uploaded.e_tag().map(str::to_string))
                .build(),
        );
    }
    source_client
        .complete_multipart_upload()
        .bucket(&source_bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build())
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_key_md5)
        .send()
        .await?;

    wait_for_source_replication_status(&source_client, &source_bucket, key, "COMPLETED", true).await?;

    let source_head = source_client
        .head_object()
        .bucket(&source_bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_key_md5)
        .send()
        .await?;
    let replica = target_client
        .get_object()
        .bucket(&target_bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_key_md5)
        .send()
        .await?;
    // The replica must carry the SSE-C marker and the source's multipart ETag
    // (its -N suffix also pins that the part structure survived).
    assert_eq!(replica.sse_customer_algorithm(), Some("AES256"));
    assert_eq!(replica.e_tag(), source_head.e_tag(), "replica must keep the source multipart ETag");
    let replica_version_id = replica.version_id().map(str::to_string);
    assert_eq!(replica.body.collect().await?.into_bytes().as_ref(), payload.as_slice());

    let plain_read = target_client.get_object().bucket(&target_bucket).key(key).send().await;
    assert!(
        plain_read.is_err(),
        "SSE-C multipart replica must not be readable without the customer key"
    );

    // Stability across scanner cycles: convergence must hold for passthrough.
    sleep(Duration::from_secs(5)).await;
    let versions = target_client
        .list_object_versions()
        .bucket(&target_bucket)
        .prefix(key)
        .send()
        .await?;
    let replica_versions: Vec<_> = versions.versions().iter().filter(|v| v.key() == Some(key)).collect();
    assert_eq!(replica_versions.len(), 1, "SSE-C replica must not accumulate versions");
    assert_eq!(replica_versions[0].version_id().map(str::to_string), replica_version_id);

    Ok(())
}

/// N2 (backlog#1675 P1-22): SSE-C passthrough replication to a target that
/// silently drops the `X-Rustfs-Replication-*` transport headers (MinIO-like
/// behavior, modeled by the fake target's drop mode) used to report COMPLETED
/// while the replica had irrecoverably lost its decryption material — the red
/// light this test was born failing on. Fail-closed contract now under test:
/// the first attempt PUTs, HEAD-backs the replica, finds no SSE-C evidence,
/// records the target Unsupported and reports FAILED; a second SSE-C object
/// fails without any PUT reaching the target (capability cache, proven from
/// the target journal); plaintext objects still replicate COMPLETED.
#[tokio::test]
async fn test_ssec_replication_fails_closed_when_target_drops_passthrough_headers() -> TestResult {
    init_logging();

    let target = FakeS3Target::start().await?;
    let target_bucket = "ssec-drop-dst";
    target.create_bucket(target_bucket);
    target.drop_unlisted_replication_headers(true);

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut env_vars = replication_fast_env();
    env_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    env_vars.extend_from_slice(&[("NO_PROXY", "127.0.0.1,localhost"), ("HTTP_PROXY", ""), ("HTTPS_PROXY", "")]);
    source_env.start_rustfs_server_with_env(vec![], &env_vars).await?;

    let source_bucket = "ssec-drop-src";
    let source_client = source_env.create_s3_client();
    source_client.create_bucket().bucket(source_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    let target_arn = set_replication_target_with_options(
        &source_env,
        source_bucket,
        ReplicationTargetOptions {
            endpoint: &target.address(),
            access_key: FAKE_ACCESS_KEY,
            secret_key: FAKE_SECRET_KEY,
            target_bucket,
            secure: false,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    let customer_key = BASE64_STANDARD.encode(REPL17_SSEC_KEY);
    let customer_key_md5 = sse_customer_key_md5_base64(REPL17_SSEC_KEY);
    let put_ssec = |key: &'static str| {
        source_client
            .put_object()
            .bucket(source_bucket)
            .key(key)
            .body(ByteStream::from_static(b"ssec fail-closed payload"))
            .sse_customer_algorithm("AES256")
            .sse_customer_key(&customer_key)
            .sse_customer_key_md5(&customer_key_md5)
            .send()
    };

    // First SSE-C object: the audit must catch the dropped material.
    put_ssec("ssec-first.txt").await?;
    wait_for_source_replication_status(&source_client, source_bucket, "ssec-first.txt", "FAILED", true).await?;

    let requests = target.take_requests();
    let first_put = requests
        .iter()
        .find(|record| record.operation == FakeTargetOperation::PutObject && record.key.as_deref() == Some("ssec-first.txt"))
        .ok_or("the first SSE-C object must have been PUT (capability was Unknown)")?;
    assert!(
        first_put.proxy_headers.ssec_transport_present,
        "the replication PUT must have shipped the SSE-C transport headers the target then dropped"
    );
    assert!(
        requests.iter().any(|record| {
            record.operation == FakeTargetOperation::HeadObject
                && record.key.as_deref() == Some("ssec-first.txt")
                && record.sequence > first_put.sequence
                && record.proxy_headers.replication_check.as_deref() == Some("true")
        }),
        "the post-PUT HEAD-back audit must have run through the replication-check channel; journal: {requests:?}"
    );

    // Second SSE-C object: the cached Unsupported verdict fails it closed
    // before any PUT — including MRF retries of the first object.
    put_ssec("ssec-second.txt").await?;
    wait_for_source_replication_status(&source_client, source_bucket, "ssec-second.txt", "FAILED", true).await?;
    assert!(
        !target.requests().iter().any(|record| {
            record.operation == FakeTargetOperation::PutObject
                && record.key.as_deref() != Some("plain-control.txt")
                && record.proxy_headers.ssec_transport_present
        }),
        "no further SSE-C ciphertext may reach a target recorded Unsupported; journal: {:?}",
        target.requests()
    );

    // The gate is scoped to SSE-C: plaintext replication keeps working.
    source_client
        .put_object()
        .bucket(source_bucket)
        .key("plain-control.txt")
        .body(ByteStream::from_static(b"plaintext control payload"))
        .send()
        .await?;
    wait_for_source_replication_status(&source_client, source_bucket, "plain-control.txt", "COMPLETED", false).await?;
    assert!(target.has_object(target_bucket, "plain-control.txt"));

    target.shutdown().await;
    Ok(())
}

/// N2 (backlog#1675 P1-22): the admin replication-check must expose the same
/// verdict operators would otherwise only learn from failing SSE-C objects —
/// an SsecPassthrough probe phase that fails with the machine-readable
/// `BucketRemoteSsecPassthroughUnsupported` code against a header-dropping
/// target, with no probe residue left behind. The target's overall status
/// stays OK: unlike version-identity drift, dropped passthrough headers are
/// a capability limit, and a plaintext-only deployment against a MinIO-like
/// target must not turn red.
#[tokio::test]
async fn test_replication_check_flags_ssec_passthrough_dropping_target() -> TestResult {
    init_logging();

    let target = FakeS3Target::start().await?;
    let target_bucket = "ssec-check-dst";
    target.create_bucket(target_bucket);
    target.drop_unlisted_replication_headers(true);

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut env_vars = replication_fast_env();
    env_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    env_vars.extend_from_slice(&[("NO_PROXY", "127.0.0.1,localhost"), ("HTTP_PROXY", ""), ("HTTPS_PROXY", "")]);
    source_env.start_rustfs_server_with_env(vec![], &env_vars).await?;

    let source_bucket = "ssec-check-src";
    let source_client = source_env.create_s3_client();
    source_client.create_bucket().bucket(source_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    let target_arn = set_replication_target_with_options(
        &source_env,
        source_bucket,
        ReplicationTargetOptions {
            endpoint: &target.address(),
            access_key: FAKE_ACCESS_KEY,
            secret_key: FAKE_SECRET_KEY,
            target_bucket,
            secure: false,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    let response = run_replication_check(&source_env, source_bucket).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await?;

    assert_eq!(
        payload["Status"], "OK",
        "a capability-only SSE-C failure must not fail the check overall: {payload}"
    );
    let target_report = &payload["Targets"][0];
    assert_eq!(target_report["Status"], "OK", "{payload}");
    let ssec = &target_report["Phases"]["SsecPassthrough"];
    assert_eq!(ssec["Status"], "FAILED", "SsecPassthrough phase must fail: {payload}");
    assert_eq!(
        ssec["Code"], "BucketRemoteSsecPassthroughUnsupported",
        "the failure must carry the machine-readable code: {payload}"
    );
    // Basic replication of plaintext objects works on this target: every other
    // phase passes, so the code is the discriminator operators branch on.
    assert_eq!(target_report["Phases"]["Put"]["Status"], "OK", "{payload}");
    assert_eq!(target_report["Phases"]["VersionFidelity"]["Status"], "OK", "{payload}");
    assert_eq!(target_report["Phases"]["DeleteMarker"]["Status"], "OK", "{payload}");
    assert_eq!(target_report["Phases"]["VersionDelete"]["Status"], "OK", "{payload}");
    assert_eq!(target_report["Phases"]["Cleanup"]["Status"], "OK", "{payload}");

    // The SSE-C probe PUT must have shipped the real transport header names —
    // a mangled or missing header set would fail the phase for the wrong
    // reason and mask a working target.
    let requests = target.requests();
    assert!(
        requests
            .iter()
            .any(|record| record.operation == FakeTargetOperation::PutObject && record.proxy_headers.ssec_transport_present),
        "the SSE-C probe PUT must carry the X-Rustfs-Replication-* transport headers; journal: {requests:?}"
    );

    // No probe residue, including the SSE-C probe version.
    let probe_put = requests
        .into_iter()
        .find(|record| record.operation == FakeTargetOperation::PutObject)
        .ok_or("the probe PUT never reached the fake target")?;
    let probe_key = probe_put.key.ok_or("probe PUT journal record has no key")?;
    assert!(
        target.stored_versions(target_bucket, &probe_key).is_empty(),
        "all probe versions must be cleaned up"
    );

    target.shutdown().await;
    Ok(())
}

/// C1 (backlog#1675 P1-22): heal-path convergence for SSE-C. An SSE-C object
/// whose live replication failed during a target outage must converge through
/// the scanner/heal compensation once the target returns — passing the N2
/// HEAD-back audit against the recovered RustFS target — and the replica must
/// be readable with the customer key.
#[tokio::test]
async fn test_bucket_replication_sse_c_heals_after_target_outage() -> TestResult {
    init_logging();

    let (source_env, mut target_env, source_bucket, target_bucket) =
        build_sse_replication_pair("ssec-heal", false, false).await?;
    let source_client = source_env.create_s3_client();
    let key = "ssec-heal-contract.txt";
    let body = b"repl-22 ssec heal payload".to_vec();
    let customer_key = BASE64_STANDARD.encode(REPL17_SSEC_KEY);
    let customer_key_md5 = sse_customer_key_md5_base64(REPL17_SSEC_KEY);

    // Target outage: the SSE-C write cannot replicate.
    target_env.stop_server();

    source_client
        .put_object()
        .bucket(&source_bucket)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_key_md5)
        .send()
        .await?;

    // The failure is observable on the source (SSE-C HEAD needs the key).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let head = source_client
            .head_object()
            .bucket(&source_bucket)
            .key(key)
            .sse_customer_algorithm("AES256")
            .sse_customer_key(&customer_key)
            .sse_customer_key_md5(&customer_key_md5)
            .send()
            .await?;
        match head.replication_status().map(|status| status.as_str()) {
            Some("PENDING") | Some("FAILED") => break,
            other => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!("source SSE-C object never reported PENDING/FAILED; last status={other:?}").into());
                }
                sleep(Duration::from_millis(200)).await;
            }
        }
    }

    // Recover the target in place; the source scanner re-drives the failure.
    target_env
        .restart_server_preserving_data(vec![], &[("NO_PROXY", "127.0.0.1,localhost"), ("HTTP_PROXY", ""), ("HTTPS_PROXY", "")])
        .await?;

    wait_for_source_replication_status(&source_client, &source_bucket, key, "COMPLETED", true).await?;

    // The healed replica is a REPLICA (status surfaces on HEAD) readable with
    // the customer key.
    let target_client = target_env.create_s3_client();
    let replica_head = target_client
        .head_object()
        .bucket(&target_bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_key_md5)
        .send()
        .await?;
    assert_eq!(
        replica_head.replication_status().map(|status| status.as_str()),
        Some("REPLICA"),
        "the healed copy must carry REPLICA status"
    );
    let replica = target_client
        .get_object()
        .bucket(&target_bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_key_md5)
        .send()
        .await?;
    assert_eq!(replica.sse_customer_algorithm(), Some("AES256"));
    assert_eq!(replica.body.collect().await?.into_bytes().as_ref(), body.as_slice());

    Ok(())
}

/// C1 (backlog#1675 P1-22): existing-object resync for SSE-C. An SSE-C object
/// written BEFORE any replication config must reach the RustFS target through
/// the existing-object resync (`replicate_all` transport, N2-audited), land as
/// a REPLICA, and read back with the customer key.
#[tokio::test]
async fn test_bucket_replication_sse_c_existing_object_resync() -> TestResult {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut source_process_env = replication_fast_env();
    source_process_env.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    source_process_env.extend_from_slice(FAST_SCANNER_ENV);
    source_process_env.extend_from_slice(&[("NO_PROXY", "127.0.0.1,localhost"), ("HTTP_PROXY", ""), ("HTTPS_PROXY", "")]);
    source_env.start_rustfs_server_with_env(vec![], &source_process_env).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env
        .start_rustfs_server_without_cleanup_with_env(&[
            ("NO_PROXY", "127.0.0.1,localhost"),
            ("HTTP_PROXY", ""),
            ("HTTPS_PROXY", ""),
        ])
        .await?;

    let source_bucket = "ssec-existing-src";
    let target_bucket = "ssec-existing-dst";
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    // The SSE-C object exists before any replication wiring.
    let key = "ssec-existing-contract.txt";
    let body = b"repl-22 ssec existing-object payload".to_vec();
    let customer_key = BASE64_STANDARD.encode(REPL17_SSEC_KEY);
    let customer_key_md5 = sse_customer_key_md5_base64(REPL17_SSEC_KEY);
    source_client
        .put_object()
        .bucket(source_bucket)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_key_md5)
        .send()
        .await?;

    // Wire replication (existing-object enabled) and drive a resync.
    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;
    let (reset_arn, reset_id) = start_bucket_replication_reset(&source_env, source_bucket).await?;
    assert_eq!(reset_arn, target_arn);
    let terminal = wait_for_replication_reset_target(&source_env, source_bucket, &target_arn, |status| {
        status.reset_id == reset_id && matches!(status.status.as_str(), "Completed" | "Failed")
    })
    .await?;
    assert_eq!(terminal.status, "Completed", "SSE-C existing-object resync must complete");
    assert!(terminal.replicated_count >= 1, "the existing SSE-C object must have been resynced");

    // The replica is a REPLICA (status surfaces on HEAD) readable with the
    // customer key.
    let replica_head = target_client
        .head_object()
        .bucket(target_bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_key_md5)
        .send()
        .await?;
    assert_eq!(
        replica_head.replication_status().map(|status| status.as_str()),
        Some("REPLICA"),
        "the resynced copy must carry REPLICA status"
    );
    let replica = target_client
        .get_object()
        .bucket(target_bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&customer_key)
        .sse_customer_key_md5(&customer_key_md5)
        .send()
        .await?;
    assert_eq!(replica.sse_customer_algorithm(), Some("AES256"));
    assert_eq!(replica.body.collect().await?.into_bytes().as_ref(), body.as_slice());

    // No plaintext leak: the replica stays unreadable without the key.
    assert!(
        target_client
            .get_object()
            .bucket(target_bucket)
            .key(key)
            .send()
            .await
            .is_err(),
        "SSE-C replica must not be readable without the customer key"
    );

    Ok(())
}

/// backlog#1147 repl-17 / backlog#1783: SSE-S3 objects replicate by decrypting
/// at the source and re-encrypting on the target with the target's own KMS.
/// The property backlog#1291 pinned — never a silent plaintext replica — still
/// holds, but the expectation flips from FAILED to a converged, decryptable
/// replica: COMPLETED status, byte-identical plain GET on the target
/// (independent KMS, so success proves target-owned envelopes), preserved
/// source ETag, and a version that stays stable across scanner cycles.
#[tokio::test]
async fn test_bucket_replication_sse_s3_contract() -> TestResult {
    init_logging();
    assert_managed_sse_replicates_and_reencrypts("sse-s3", false).await
}

/// backlog#1783: when the target site has no KMS, managed-SSE replication must
/// fail closed — replication FAILED, and no plaintext (or any) replica ever
/// materializes on the target.
#[tokio::test]
async fn test_bucket_replication_sse_s3_fails_closed_without_target_kms() -> TestResult {
    init_logging();

    let (source_env, target_env, source_bucket, target_bucket) = build_sse_replication_pair("sse-nokms", true, false).await?;
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let key = "sse-nokms-contract.txt";
    let body = b"repl-17 sse target-without-kms payload".to_vec();

    source_client
        .put_object()
        .bucket(&source_bucket)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;

    wait_for_source_replication_status(&source_client, &source_bucket, key, "FAILED", false).await?;
    assert_failed_replication_stays_absent_for(
        &source_client,
        &source_bucket,
        &target_client,
        &target_bucket,
        key,
        false,
        Duration::from_secs(5),
    )
    .await?;

    let source = source_client.get_object().bucket(&source_bucket).key(key).send().await?;
    assert_eq!(source.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    assert_eq!(source.body.collect().await?.into_bytes().as_ref(), body.as_slice());

    Ok(())
}

/// P1-22 stage 0 → backlog#1783: the existing-object resync path re-drives
/// managed-SSE objects through the same target boundary as live replication.
/// After the live pass completes, a resync over the bucket must converge —
/// the ETag comparison sees the preserved source ETag on the replica and does
/// not rewrite it, so the replica's version stays stable through the resync.
#[tokio::test]
async fn test_bucket_replication_sse_s3_resync_converges() -> TestResult {
    init_logging();

    let (source_env, target_env, source_bucket, target_bucket) = build_sse_replication_pair("sse-resync", true, true).await?;
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let key = "sse-resync-contract.txt";
    let body = b"repl-22 sse resync payload".to_vec();

    source_client
        .put_object()
        .bucket(&source_bucket)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;
    wait_for_source_replication_status(&source_client, &source_bucket, key, "COMPLETED", false).await?;

    let replica = target_client.get_object().bucket(&target_bucket).key(key).send().await?;
    assert_eq!(replica.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    let replica_version_id = replica.version_id().map(str::to_string);
    assert_eq!(replica.body.collect().await?.into_bytes().as_ref(), body.as_slice());

    // Resync: drive the existing-object resync path over the replicated object.
    let (target_arn, reset_id) = start_bucket_replication_reset(&source_env, &source_bucket).await?;
    let terminal = wait_for_replication_reset_target(&source_env, &source_bucket, &target_arn, |target| {
        target.reset_id == reset_id && matches!(target.status.as_str(), "Completed" | "Failed")
    })
    .await?;
    assert_eq!(terminal.reset_id, reset_id);
    assert_eq!(terminal.status, "Completed", "resync over a managed-SSE bucket must complete");

    // The resync pass must not have rewritten the converged replica.
    let versions = target_client
        .list_object_versions()
        .bucket(&target_bucket)
        .prefix(key)
        .send()
        .await?;
    let replica_versions: Vec<_> = versions.versions().iter().filter(|v| v.key() == Some(key)).collect();
    assert_eq!(replica_versions.len(), 1, "resync must not create additional replica versions");
    assert_eq!(replica_versions[0].version_id().map(str::to_string), replica_version_id);

    let source = source_client.get_object().bucket(&source_bucket).key(key).send().await?;
    assert_eq!(source.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    assert_eq!(source.body.collect().await?.into_bytes().as_ref(), body.as_slice());

    Ok(())
}

/// backlog#1147 repl-17 / backlog#1783: SSE-KMS replicates like SSE-S3 — the
/// source key id never crosses sites (only the aws:kms intent), and the target
/// re-encrypts under its own default key. The independent-KMS pair proves the
/// replica's envelope is target-owned.
#[tokio::test]
async fn test_bucket_replication_sse_kms_contract() -> TestResult {
    init_logging();
    assert_managed_sse_replicates_and_reencrypts("sse-kms", true).await
}

/// backlog#1783: managed-SSE multipart objects keep their part structure and
/// their metadata through replication. CreateMultipartUpload on the target
/// carries the full header set (SSE intent, content-type, user metadata) and
/// the completed replica preserves the source's multipart ETag.
#[tokio::test]
async fn test_bucket_replication_sse_s3_multipart_reencrypts() -> TestResult {
    init_logging();

    const PART_SIZE: usize = 5 * 1024 * 1024;
    const PART_COUNT: usize = 3;

    let (source_env, target_env, source_bucket, target_bucket) = build_sse_replication_pair("sse-mp", true, true).await?;
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let key = "sse-mp-contract.bin";

    let created = source_client
        .create_multipart_upload()
        .bucket(&source_bucket)
        .key(key)
        .content_type("application/x-repl17")
        .metadata("app", "repl17")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .send()
        .await?;
    let upload_id = created.upload_id().ok_or("missing multipart upload id")?.to_string();

    let mut completed_parts = Vec::with_capacity(PART_COUNT);
    let mut payload = Vec::with_capacity(PART_SIZE * PART_COUNT);
    for part_number in 1..=PART_COUNT {
        let part = vec![u8::try_from(part_number)?; PART_SIZE];
        payload.extend_from_slice(&part);
        let uploaded = source_client
            .upload_part()
            .bucket(&source_bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(i32::try_from(part_number)?)
            .body(ByteStream::from(part))
            .send()
            .await?;
        completed_parts.push(
            CompletedPart::builder()
                .part_number(i32::try_from(part_number)?)
                .set_e_tag(uploaded.e_tag().map(str::to_string))
                .build(),
        );
    }
    source_client
        .complete_multipart_upload()
        .bucket(&source_bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build())
        .send()
        .await?;

    wait_for_source_replication_status(&source_client, &source_bucket, key, "COMPLETED", false).await?;

    let source_head = source_client.head_object().bucket(&source_bucket).key(key).send().await?;
    let replica = target_client.get_object().bucket(&target_bucket).key(key).send().await?;
    assert_eq!(replica.server_side_encryption(), Some(&ServerSideEncryption::Aes256));
    assert_eq!(replica.e_tag(), source_head.e_tag(), "replica must keep the source multipart ETag");
    assert_eq!(
        replica.last_modified(),
        source_head.last_modified(),
        "replica must keep the source mtime or the multipart HEAD comparison never converges"
    );
    assert_eq!(replica.content_type(), Some("application/x-repl17"));
    assert_eq!(replica.metadata().and_then(|m| m.get("app").map(String::as_str)), Some("repl17"));
    let replica_version_id = replica.version_id().map(str::to_string);
    assert_eq!(replica.body.collect().await?.into_bytes().as_ref(), payload.as_slice());

    // The multipart replica must also stay stable across scanner cycles: a
    // rewritten or additional version means ETag/mtime convergence failed and
    // the scanner keeps re-driving the object.
    sleep(Duration::from_secs(5)).await;
    let versions = target_client
        .list_object_versions()
        .bucket(&target_bucket)
        .prefix(key)
        .send()
        .await?;
    let replica_versions: Vec<_> = versions.versions().iter().filter(|v| v.key() == Some(key)).collect();
    assert_eq!(replica_versions.len(), 1, "multipart replica must not accumulate versions");
    assert_eq!(replica_versions[0].version_id().map(str::to_string), replica_version_id);

    Ok(())
}

/// backlog#1147 repl-5, scenario (a) — target outage + recovery (rustfs#3421 / #2071).
///
/// Kills the replication target mid-workload, asserts the source records the
/// undelivered objects as PENDING/FAILED, then recovers the target with its data
/// directory intact and asserts both sides converge with no data loss. The
/// still-running source's data scanner (short cycle via [`FAST_SCANNER_ENV`])
/// re-drives the failed objects once the target is reachable again.
#[tokio::test]
async fn test_bucket_replication_recovers_after_target_outage() -> TestResult {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut source_env_vars = replication_fast_env();
    source_env_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    source_env_vars.extend_from_slice(FAST_SCANNER_ENV);
    source_env.start_rustfs_server_with_env(vec![], &source_env_vars).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "repl-outage-recovery-src";
    let target_bucket = "repl-outage-recovery-dst";
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    // Baseline object replicated while the target is healthy; it must remain on
    // both sides through the outage (no loss of already-replicated data).
    source_client
        .put_object()
        .bucket(source_bucket)
        .key("before-outage.txt")
        .body(ByteStream::from_static(b"baseline written before the target outage"))
        .send()
        .await?;
    assert_replication_converged(&source_client, source_bucket, &target_client, target_bucket).await?;

    // Target outage: everything written now cannot replicate.
    target_env.stop_server();

    let outage_keys = ["during-outage-1.txt", "during-outage-2.txt"];
    for key in outage_keys {
        source_client
            .put_object()
            .bucket(source_bucket)
            .key(key)
            .body(ByteStream::from(format!("written while the target was down: {key}").into_bytes()))
            .send()
            .await?;
    }

    // The outage is observable on the source: the objects are not yet replicated.
    wait_for_source_replication_pending_or_failed(&source_client, source_bucket, outage_keys[0]).await?;

    // Recover the target in place; the source scanner re-drives the failures.
    target_env.restart_server_preserving_data(vec![], &[]).await?;

    assert_replication_converged(&source_client, source_bucket, &target_client, target_bucket).await?;

    // Explicit no-loss check: baseline + every outage write reached the target.
    let target_state = list_replication_state(&target_client, target_bucket).await?;
    for key in ["before-outage.txt"].into_iter().chain(outage_keys) {
        assert!(
            target_state.iter().any(|entry| entry.key == key && !entry.delete_marker),
            "target missing object {key} after outage recovery; state={target_state:?}"
        );
    }

    Ok(())
}

/// backlog#1610 - black-box bucket replication backlog observability.
///
/// The source exports metrics through the same OTLP path production uses. A slow
/// loopback target keeps replication workers occupied long enough for the metrics
/// runtime to publish non-zero bucket backlog gauges; after the real target
/// returns, replication must converge and the exported current/MRF pending gauges
/// must settle back to zero even though the historical failed counter remains
/// non-zero.
#[tokio::test]
async fn test_bucket_replication_backlog_metrics_observe_outage_and_recovery() -> TestResult {
    init_logging();

    let collector = ReplicationBacklogMetricCollector::start().await?;
    let mut source_env = RustFSTestEnvironment::new().await?;
    let metric_root = collector.root_endpoint().to_string();
    let metric_endpoint = collector.endpoint.clone();
    let mut source_env_vars: Vec<(&str, &str)> = replication_fast_env().into_iter().collect();
    source_env_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    source_env_vars.extend_from_slice(FAST_SCANNER_ENV);
    source_env_vars.extend_from_slice(&[
        ("RUSTFS_OBS_ENDPOINT", metric_root.as_str()),
        ("RUSTFS_OBS_METRIC_ENDPOINT", metric_endpoint.as_str()),
        ("RUSTFS_OBS_METRICS_EXPORT_ENABLED", "true"),
        ("RUSTFS_OBS_TRACES_EXPORT_ENABLED", "false"),
        ("RUSTFS_OBS_LOGS_EXPORT_ENABLED", "false"),
        ("RUSTFS_OBS_METER_INTERVAL", "1"),
        ("RUSTFS_OBS_USE_STDOUT", "false"),
        ("RUSTFS_METRICS_BUCKET_REPLICATION_BANDWIDTH_INTERVAL_SEC", "1"),
    ]);
    source_env.start_rustfs_server_with_env(vec![], &source_env_vars).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "repl-backlog-metrics-src";
    let target_bucket = "repl-backlog-metrics-dst";
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    source_client
        .put_object()
        .bucket(source_bucket)
        .key("before-outage.txt")
        .body(ByteStream::from_static(b"baseline written before backlog metrics outage"))
        .send()
        .await?;
    assert_replication_converged(&source_client, source_bucket, &target_client, target_bucket).await?;

    target_env.stop_server();
    let slow_target = SlowReplicationTargetGuard::bind(&target_env.address, Duration::from_secs(5)).await?;

    let outage_keys = ["metrics-outage-1.txt", "metrics-outage-2.txt", "metrics-outage-3.txt"];
    for key in outage_keys {
        source_client
            .put_object()
            .bucket(source_bucket)
            .key(key)
            .body(ByteStream::from(
                format!("written while backlog metrics target was slow: {key}").into_bytes(),
            ))
            .send()
            .await?;
    }

    let current_backlog = collector
        .wait_for_bucket_metric(
            CURRENT_BACKLOG_COUNT_METRIC,
            source_bucket,
            |value| value >= 1.0,
            "be at least 1 during outage",
        )
        .await?;
    let current_bytes = collector
        .wait_for_bucket_metric(
            CURRENT_BACKLOG_BYTES_METRIC,
            source_bucket,
            |value| value > 0.0,
            "report bytes during outage",
        )
        .await?;
    assert!(
        current_bytes >= current_backlog,
        "backlog bytes should be at least the object count while queued; count={current_backlog}, bytes={current_bytes}"
    );
    let failed_count = collector
        .wait_for_bucket_metric(
            TOTAL_FAILED_COUNT_METRIC,
            source_bucket,
            |value| value >= 1.0,
            "record at least one failed replication attempt during outage",
        )
        .await?;

    slow_target.stop().await;
    target_env.restart_server_preserving_data(vec![], &[]).await?;

    assert_replication_converged(&source_client, source_bucket, &target_client, target_bucket).await?;
    for metric in [
        CURRENT_BACKLOG_COUNT_METRIC,
        CURRENT_BACKLOG_BYTES_METRIC,
        MRF_PENDING_COUNT_METRIC,
        MRF_PENDING_BYTES_METRIC,
    ] {
        collector
            .wait_for_bucket_metric(metric, source_bucket, |value| value == 0.0, "settle back to zero after recovery")
            .await?;
    }
    let final_failed_count = collector.bucket_metric_value(TOTAL_FAILED_COUNT_METRIC, source_bucket).await;
    assert!(
        final_failed_count >= failed_count,
        "historical failed counter should remain non-zero after recovery while current backlog is zero; before={failed_count}, after={final_failed_count}"
    );

    let target_state = list_replication_state(&target_client, target_bucket).await?;
    for key in ["before-outage.txt"].into_iter().chain(outage_keys) {
        assert!(
            target_state.iter().any(|entry| entry.key == key && !entry.delete_marker),
            "target missing object {key} after backlog metrics recovery; state={target_state:?}"
        );
    }

    Ok(())
}

/// backlog#1147 repl-5, scenario (b) — failure state survives a source restart
/// (mirrors backlog#858 delete-decision re-derivation and #859 no-drop).
///
/// Several object writes plus a delete of an already-replicated object all fail
/// while the target is down. The SOURCE is then restarted with the target still
/// unreachable, so replication can only resume from the per-object status
/// persisted in `xl.meta` — nothing in memory survives. Bringing the target back
/// must converge every persisted failure, including the replayed delete marker
/// (whose replication decision is re-derived from the live config).
#[tokio::test]
async fn test_bucket_replication_replays_failed_entries_after_source_restart() -> TestResult {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut source_env_vars = replication_fast_env();
    source_env_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    source_env_vars.extend_from_slice(FAST_SCANNER_ENV);
    source_env.start_rustfs_server_with_env(vec![], &source_env_vars).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "repl-source-restart-src";
    let target_bucket = "repl-source-restart-dst";
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication_with_delete_statuses(&source_env, source_bucket, &target_arn, "Enabled", Some("Enabled")).await?;

    // Replicate an object before the outage so its later deletion produces a
    // delete marker whose replay decision must be re-derived (backlog#858).
    let deleted_key = "deleted-during-outage.txt";
    source_client
        .put_object()
        .bucket(source_bucket)
        .key(deleted_key)
        .body(ByteStream::from_static(b"exists on both sides before deletion"))
        .send()
        .await?;
    assert_replication_converged(&source_client, source_bucket, &target_client, target_bucket).await?;

    // Outage: two fresh objects plus a delete marker all fail to replicate.
    target_env.stop_server();

    let failed_keys = ["failed-1.txt", "failed-2.txt"];
    for key in failed_keys {
        source_client
            .put_object()
            .bucket(source_bucket)
            .key(key)
            .body(ByteStream::from(
                format!("queued for replay while the target was down: {key}").into_bytes(),
            ))
            .send()
            .await?;
    }
    source_client
        .delete_object()
        .bucket(source_bucket)
        .key(deleted_key)
        .send()
        .await?;

    wait_for_source_replication_pending_or_failed(&source_client, source_bucket, failed_keys[0]).await?;

    // Restart the SOURCE while the target is still down: recovery must reload the
    // failure state from disk, since no in-memory queue survives the restart.
    source_env.restart_server_preserving_data(vec![], &source_env_vars).await?;

    // Bring the target back; the restarted source re-drives every persisted
    // failure to convergence.
    target_env.restart_server_preserving_data(vec![], &[]).await?;

    assert_replication_converged(&source_client, source_bucket, &target_client, target_bucket).await?;

    // No entry dropped across the restart (backlog#859) and the delete marker
    // replayed to the target (backlog#858).
    let target_state = list_replication_state(&target_client, target_bucket).await?;
    for key in failed_keys {
        assert!(
            target_state.iter().any(|entry| entry.key == key && !entry.delete_marker),
            "source-restart replay dropped object {key}; state={target_state:?}"
        );
    }
    assert!(
        target_state
            .iter()
            .any(|entry| entry.key == deleted_key && entry.delete_marker),
        "replayed delete marker for {deleted_key} did not reach the target (backlog#858); state={target_state:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_bucket_replication_replayed_delete_marker_preserves_source_mtime_without_source_restart() -> TestResult {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut source_env_vars = replication_fast_env();
    source_env_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    source_env_vars.extend_from_slice(FAST_SCANNER_ENV);
    source_env.start_rustfs_server_with_env(vec![], &source_env_vars).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "repl-dm-mtime-src";
    let target_bucket = "repl-dm-mtime-dst";
    let object_key = "delete-marker-mtime.txt";
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication_with_delete_statuses(&source_env, source_bucket, &target_arn, "Enabled", Some("Enabled")).await?;

    source_client
        .put_object()
        .bucket(source_bucket)
        .key(object_key)
        .body(ByteStream::from_static(b"object to be delete-marked during the outage"))
        .send()
        .await?;
    assert_replication_converged(&source_client, source_bucket, &target_client, target_bucket).await?;

    // Create the delete marker while the target is down so it replicates through
    // the failure-replay path rather than the immediate path. (HEAD on the key
    // now returns the delete marker, so the source status is read from the
    // version listing instead of head_object.)
    target_env.stop_server();
    let delete = source_client
        .delete_object()
        .bucket(source_bucket)
        .key(object_key)
        .send()
        .await?;
    assert_eq!(delete.delete_marker(), Some(true));

    let source_mtime = delete_marker_last_modified(&source_client, source_bucket, object_key)
        .await?
        .ok_or("source has no delete marker after DELETE")?;

    wait_for_source_delete_marker_replication_failed(&source_env, source_bucket, object_key).await?;

    // Widen the gap so a replay-time-stamping regression is unmistakable.
    sleep(Duration::from_secs(3)).await;

    target_env.restart_server_preserving_data(vec![], &[]).await?;

    let target_mtime = wait_for_target_delete_marker(&target_client, target_bucket, object_key).await?;
    assert_eq!(
        target_mtime, source_mtime,
        "replayed delete marker did not preserve the source mtime (backlog#867): source={source_mtime:?}, target={target_mtime:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_sequential_bucket_replication_succeeds_for_multiple_buckets() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    for idx in 1..=5 {
        let source_bucket = format!("replication-multi-src-{idx}");
        let target_bucket = format!("replication-multi-dst-{idx}");
        let object_key = format!("probe-{idx}.txt");
        let body = format!("payload-{idx}");

        source_client.create_bucket().bucket(&source_bucket).send().await?;
        target_client.create_bucket().bucket(&target_bucket).send().await?;
        enable_bucket_versioning(&source_env, &source_bucket).await?;
        enable_bucket_versioning(&target_env, &target_bucket).await?;

        let target_arn = set_replication_target(&source_env, &source_bucket, &target_env, &target_bucket).await?;
        put_bucket_replication(&source_env, &source_bucket, &target_arn).await?;

        source_client
            .put_object()
            .bucket(&source_bucket)
            .key(&object_key)
            .body(ByteStream::from(body.clone().into_bytes()))
            .send()
            .await?;

        wait_for_replicated_object(&target_client, &target_bucket, &object_key, &body).await?;
    }

    Ok(())
}

#[tokio::test]
async fn test_replication_recovers_after_runtime_target_cache_is_cleared() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    let source_bucket = "replication-refresh-src";
    let target_bucket = "replication-refresh-dst";
    let object_key = "probe-refresh.txt";
    let body = "payload-refresh";

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    source_client.create_bucket().bucket(source_bucket).send().await?;
    target_client.create_bucket().bucket(target_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    BucketTargetSys::get().delete(source_bucket).await;

    source_client
        .put_object()
        .bucket(source_bucket)
        .key(object_key)
        .body(ByteStream::from(body.as_bytes().to_vec()))
        .send()
        .await?;

    wait_for_replicated_object(&target_client, target_bucket, object_key, body).await?;

    Ok(())
}

#[tokio::test]
async fn test_site_replication_allows_self_signed_https_with_skip_tls_verify_real_dual_node() -> TestResult {
    init_logging();

    let mut source_env = new_replication_source_env().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = new_replication_https_target_env().await?;
    let tls_dir = std::path::PathBuf::from(&target_env.temp_dir).join("tls");
    let target_host = target_env
        .url
        .trim_start_matches("https://")
        .split(':')
        .next()
        .ok_or_else(|| std::io::Error::other("target HTTPS URL missing host"))?
        .to_string();
    generate_self_signed_tls_material(&tls_dir, &target_host).await?;
    start_https_rustfs_server(&mut target_env, &tls_dir).await?;
    let https_client = insecure_https_client()?;
    wait_for_https_server_ready(&https_client, &target_env).await?;

    let source_site = PeerSite {
        name: "source-site".to_string(),
        endpoint: source_env.url.clone(),
        access_key: source_env.access_key.clone(),
        secret_key: source_env.secret_key.clone(),
        ..Default::default()
    };
    let target_site = PeerSite {
        name: "target-site".to_string(),
        endpoint: target_env.url.clone(),
        access_key: target_env.access_key.clone(),
        secret_key: target_env.secret_key.clone(),
        ..Default::default()
    };

    let add_error = site_replication_add(&source_env, &[source_site.clone(), target_site.clone()])
        .await
        .expect_err("site replication add must reject an untrusted self-signed HTTPS peer");
    let add_error = add_error.to_string();
    assert_untrusted_site_peer_rejected(&add_error, &target_env.url);
    let disabled = wait_for_site_replication_disabled(&source_env).await?;
    assert!(!disabled.enabled && disabled.sites.is_empty());

    let add_status = site_replication_add(
        &source_env,
        &[
            source_site,
            PeerSite {
                skip_tls_verify: true,
                ..target_site
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {add_status:?}");
    let _source_info = wait_for_site_replication_enabled(&source_env, 2).await?;

    let source_client = source_env.create_s3_client();
    let bucket = "site-repl-self-signed-tls";
    let key = "self-signed.txt";
    let body = "site replication over self-signed https";
    source_client.create_bucket().bucket(bucket).send().await?;
    source_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.as_bytes().to_vec()))
        .send()
        .await?;

    wait_for_replicated_object_over_https(&https_client, &target_env, bucket, key, body).await?;

    Ok(())
}

#[tokio::test]
async fn test_site_replication_allows_private_ca_https_with_ca_cert_pem_real_dual_node() -> TestResult {
    init_logging();

    let mut source_env = new_replication_source_env().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = new_replication_https_target_env().await?;
    let tls_dir = std::path::PathBuf::from(&target_env.temp_dir).join("tls");
    let target_host = target_env
        .url
        .trim_start_matches("https://")
        .split(':')
        .next()
        .ok_or_else(|| std::io::Error::other("target HTTPS URL missing host"))?
        .to_string();
    let ca_cert_pem = generate_private_ca_tls_material(&tls_dir, &target_host).await?;
    start_https_rustfs_server(&mut target_env, &tls_dir).await?;
    let https_client = trusted_https_client(&ca_cert_pem)?;
    wait_for_https_server_ready(&https_client, &target_env).await?;

    let source_site = PeerSite {
        name: "source-site".to_string(),
        endpoint: source_env.url.clone(),
        access_key: source_env.access_key.clone(),
        secret_key: source_env.secret_key.clone(),
        ..Default::default()
    };
    let target_site = PeerSite {
        name: "target-site".to_string(),
        endpoint: target_env.url.clone(),
        access_key: target_env.access_key.clone(),
        secret_key: target_env.secret_key.clone(),
        ..Default::default()
    };

    let add_error = site_replication_add(&source_env, &[source_site.clone(), target_site.clone()])
        .await
        .expect_err("site replication add must reject a private CA HTTPS peer without caCertPem");
    let add_error = add_error.to_string();
    assert_untrusted_site_peer_rejected(&add_error, &target_env.url);
    let disabled = wait_for_site_replication_disabled(&source_env).await?;
    assert!(!disabled.enabled && disabled.sites.is_empty());

    let add_status = site_replication_add(
        &source_env,
        &[
            source_site,
            PeerSite {
                ca_cert_pem,
                ..target_site
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {add_status:?}");
    let _source_info = wait_for_site_replication_enabled(&source_env, 2).await?;

    let source_client = source_env.create_s3_client();
    let bucket = "site-repl-private-ca-tls";
    let key = "private-ca.txt";
    let body = "site replication over private ca https";
    source_client.create_bucket().bucket(bucket).send().await?;
    source_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.as_bytes().to_vec()))
        .send()
        .await?;

    wait_for_replicated_object_over_https(&https_client, &target_env, bucket, key, body).await?;

    Ok(())
}

#[tokio::test]
async fn test_site_replication_resync_lifecycle_survives_real_server_restart() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();
    let resync_process_env = [
        ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", "true"),
        ("RUSTFS_REPL_RESYNC_POLL_MAX_MS", "100"),
        // Verbose server logging can block startup when this focused test is run
        // through a captured test process rather than nextest.
        ("RUST_LOG", "error"),
    ];

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env.capture_log_path = Some(format!("{}/server.log", source_env.temp_dir));
    source_env.start_rustfs_server_with_env(vec![], &resync_process_env).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.capture_log_path = Some(format!("{}/server.log", target_env.temp_dir));
    target_env
        .start_rustfs_server_without_cleanup_with_env(&resync_process_env)
        .await?;

    let source_bucket = "site-repl-resync-src";
    const RESYNC_OBJECT_COUNT: usize = 128;

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    // Seed only the joining site so its synchronous backfill must call the initiating
    // site before the add handler has persisted its final enabled state.
    target_client.create_bucket().bucket(source_bucket).send().await?;
    enable_bucket_versioning(&target_env, source_bucket).await?;

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
                ..Default::default()
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
                ..Default::default()
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;
    let remote_peer = source_info
        .sites
        .into_iter()
        .find(|peer| peer.endpoint == target_env.url)
        .ok_or("target peer missing from source site replication info")?;

    // Wait for the initiating site to accept the join callback and configure the
    // backfilled bucket before driving resync in the normal source-to-target direction.
    wait_for_bucket_on_target(&source_client, source_bucket).await?;
    let target_arn = wait_for_remote_target_arn(&source_env, source_bucket).await?;

    for idx in 0..RESYNC_OBJECT_COUNT {
        let size = if idx == 0 { 8 * 1024 * 1024 } else { 8 * 1024 };
        source_client
            .put_object()
            .bucket(source_bucket)
            .key(format!("resync-object-{idx:02}"))
            .body(ByteStream::from(vec![b'x'; size]))
            .send()
            .await?;
    }

    let started = site_replication_resync_op(&source_env, "start", &remote_peer).await?;
    assert_eq!(started.status, "success", "unexpected start result: {:?}", started);
    assert!(
        started.buckets.iter().any(|bucket| {
            bucket.bucket == source_bucket && matches!(bucket.status.as_str(), "started" | "running" | "completed" | "success")
        }),
        "source bucket start status missing: {:?}",
        started
    );
    assert!(!started.resync_id.is_empty(), "start response omitted the resync id: {:?}", started);
    let started_reset_id = started.resync_id.clone();

    assert!(
        matches!(started.state.as_str(), "pending" | "running"),
        "the fixture must keep the first generation active long enough to test duplicate start: {:?}",
        started
    );
    let duplicate_err = site_replication_resync_op(&source_env, "start", &remote_peer)
        .await
        .expect_err("duplicate start must be rejected while a generation is active");
    assert!(
        duplicate_err.to_string().contains("already active"),
        "unexpected duplicate start error: {duplicate_err}"
    );

    let canceled = site_replication_resync_op(&source_env, "cancel", &remote_peer).await?;
    assert_eq!(canceled.status, "success", "unexpected cancel result: {:?}", canceled);
    assert_eq!(canceled.state, "canceled");
    assert!(
        canceled
            .buckets
            .iter()
            .any(|bucket| bucket.bucket == source_bucket && matches!(bucket.status.as_str(), "canceled" | "success")),
        "source bucket cancel status missing: {:?}",
        canceled
    );
    let canceled_again = site_replication_resync_op(&source_env, "cancel", &remote_peer).await?;
    assert_eq!(canceled_again.resync_id, canceled.resync_id, "repeated cancel must be idempotent");
    assert_eq!(canceled_again.state, "canceled");

    let canceled_target =
        wait_for_replication_reset_target(&source_env, source_bucket, &target_arn, |target| target.status == "Canceled").await?;
    assert_eq!(canceled_target.reset_id, started_reset_id);

    let restarted = site_replication_resync_op(&source_env, "start", &remote_peer).await?;
    assert_eq!(restarted.status, "success", "unexpected restart result: {:?}", restarted);
    assert_ne!(restarted.resync_id, started_reset_id);
    assert!(
        matches!(restarted.state.as_str(), "pending" | "running"),
        "the second generation must be active before the process restart: {:?}",
        restarted
    );
    let restarted_reset_id = restarted.resync_id.clone();

    let partial_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let status = site_replication_resync_op(&source_env, "status", &remote_peer).await?;
        let replicated = status.replicated_objects;
        if status.resync_id == restarted_reset_id
            && replicated > 0
            && replicated < u64::try_from(RESYNC_OBJECT_COUNT).expect("test object count should fit u64")
        {
            break;
        }
        if status.state == "completed" || tokio::time::Instant::now() >= partial_deadline {
            return Err(format!("resync did not expose a partial durable checkpoint before restart: {status:?}").into());
        }
        sleep(Duration::from_millis(10)).await;
    }

    source_env.restart_server_preserving_data(vec![], &resync_process_env).await?;
    wait_for_site_replication_enabled(&source_env, 2).await?;

    let after_restart = site_replication_resync_op(&source_env, "status", &remote_peer).await?;
    assert_eq!(
        after_restart.resync_id, restarted_reset_id,
        "server restart changed the durable resync id"
    );
    assert_eq!(after_restart.generation, restarted.generation);
    assert_eq!(after_restart.created_at, restarted.created_at);
    assert!(
        matches!(after_restart.state.as_str(), "pending" | "running" | "completed" | "failed"),
        "unexpected recovered lifecycle state: {:?}",
        after_restart
    );
    assert!(
        after_restart.buckets.iter().any(|bucket| bucket.bucket == source_bucket),
        "durable status lost the source bucket after restart: {:?}",
        after_restart
    );
    let restart_snapshot = get_replication_reset_status(&source_env, source_bucket, &target_arn).await?;
    let restarted_target = wait_for_replication_reset_target(&source_env, source_bucket, &target_arn, |target| {
        target.reset_id == restarted_reset_id
    })
    .await
    .map_err(|err| {
        format!(
            "restart ids: start={} restart={} snapshot={:?}; {err}",
            started_reset_id, restarted_reset_id, restart_snapshot.targets
        )
    })?;
    assert_eq!(restarted_target.reset_id, restarted_reset_id);

    let completion_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let completed = loop {
        let status = site_replication_resync_op(&source_env, "status", &remote_peer).await?;
        match status.state.as_str() {
            "completed" => break status,
            "failed" => return Err(format!("recovered resync failed: {status:?}").into()),
            _ if tokio::time::Instant::now() < completion_deadline => sleep(Duration::from_millis(500)).await,
            _ => return Err(format!("recovered resync did not complete in time: {status:?}").into()),
        }
    };
    assert_eq!(
        completed.replicated_objects,
        u64::try_from(RESYNC_OBJECT_COUNT).expect("test object count should fit u64")
    );

    let replicated = futures::stream::iter(0..RESYNC_OBJECT_COUNT)
        .map(|idx| {
            let target_client = target_client.clone();
            async move {
                let key = format!("resync-object-{idx:02}");
                let body = wait_for_object_on_target(&target_client, source_bucket, &key).await?;
                let expected_size = if idx == 0 { 8 * 1024 * 1024 } else { 8 * 1024 };
                if body != vec![b'x'; expected_size] {
                    return Err(format!("recovered resync object body mismatch for {key}").into());
                }
                Ok::<(), Box<dyn Error + Send + Sync>>(())
            }
        })
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
    for result in replicated {
        result?;
    }

    Ok(())
}

#[tokio::test]
async fn test_site_replication_edit_and_status_peer_state_real_three_node() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut relay_env = RustFSTestEnvironment::new().await?;
    relay_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let relay_client = relay_env.create_s3_client();
    let bucket = "site-repl-edit-endpoint";
    let baseline_key = "before-edit.txt";
    let baseline_payload = b"site replication before endpoint edit".to_vec();
    let moved_key = "after-edit.txt";
    let moved_payload = b"site replication after endpoint edit".to_vec();
    let relayed_key = "after-edit-from-relay.txt";
    let relayed_payload = b"site replication after endpoint edit from relay".to_vec();

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
                ..Default::default()
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
                ..Default::default()
            },
            PeerSite {
                name: "relay-site".to_string(),
                endpoint: relay_env.url.clone(),
                access_key: relay_env.access_key.clone(),
                secret_key: relay_env.secret_key.clone(),
                ..Default::default()
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let source_info = wait_for_site_replication_enabled(&source_env, 3).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 3).await?;
    let _relay_info = wait_for_site_replication_enabled(&relay_env, 3).await?;
    let mut remote_peer = source_info
        .sites
        .into_iter()
        .find(|peer| peer.endpoint == target_env.url)
        .ok_or("target peer missing from source site replication info")?;

    source_client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&source_env, bucket).await?;
    wait_for_bucket_on_target(&target_client, bucket).await?;
    wait_for_bucket_on_target(&relay_client, bucket).await?;
    source_client
        .put_object()
        .bucket(bucket)
        .key(baseline_key)
        .body(ByteStream::from(baseline_payload.clone()))
        .send()
        .await?;
    let replicated_baseline = wait_for_object_on_target(&target_client, bucket, baseline_key).await?;
    assert_eq!(replicated_baseline, baseline_payload);

    let old_target_address = target_env.address.clone();
    let new_target_port = RustFSTestEnvironment::find_available_port().await?;
    let new_target_address = format!("127.0.0.1:{new_target_port}");
    let new_target_url = format!("http://{new_target_address}");
    remote_peer.sync_state = SyncStatus::Enable;
    remote_peer.endpoint = new_target_url.clone();
    let edit_status = site_replication_edit(&source_env, "", &remote_peer).await?;
    assert!(edit_status.success, "unexpected site edit result: {:?}", edit_status);

    let source_after_sync = wait_for_site_replication_info(&source_env, |info| {
        info.sites
            .iter()
            .any(|peer| peer.endpoint == new_target_url && peer.sync_state == SyncStatus::Enable)
    })
    .await?;
    let target_after_sync = wait_for_site_replication_info(&target_env, |info| {
        info.sites
            .iter()
            .any(|peer| peer.endpoint == new_target_url && peer.sync_state == SyncStatus::Enable)
    })
    .await?;
    let relay_after_sync = wait_for_site_replication_info(&relay_env, |info| {
        info.sites
            .iter()
            .any(|peer| peer.endpoint == new_target_url && peer.sync_state == SyncStatus::Enable)
    })
    .await?;
    assert!(
        source_after_sync
            .sites
            .iter()
            .any(|peer| peer.endpoint == new_target_url && peer.sync_state == SyncStatus::Enable)
    );
    assert!(
        target_after_sync
            .sites
            .iter()
            .any(|peer| peer.endpoint == new_target_url && peer.sync_state == SyncStatus::Enable)
    );
    assert!(
        relay_after_sync
            .sites
            .iter()
            .any(|peer| peer.endpoint == new_target_url && peer.sync_state == SyncStatus::Enable)
    );
    assert_eq!(relay_after_sync.sites.len(), 3);

    for (env, unchanged_endpoint) in [(&source_env, &relay_env.address), (&relay_env, &source_env.address)] {
        let mut endpoints_replaced = false;
        let mut last_targets = Vec::new();
        for _ in 0..40 {
            let response = list_replication_targets_request(env, Some(bucket)).await?;
            if response.status() == StatusCode::OK {
                let targets: Vec<serde_json::Value> = response.json().await?;
                let mut endpoints = targets
                    .iter()
                    .filter_map(|target| target.get("endpoint").and_then(|endpoint| endpoint.as_str()))
                    .collect::<Vec<_>>();
                endpoints.sort_unstable();
                let mut expected = vec![new_target_address.as_str(), unchanged_endpoint.as_str()];
                expected.sort_unstable();
                endpoints_replaced = endpoints == expected;
                last_targets = targets;
                if endpoints_replaced {
                    break;
                }
            }
            sleep(Duration::from_millis(250)).await;
        }
        assert!(
            endpoints_replaced,
            "site edit did not replace the bucket target endpoint on {}: {last_targets:?}",
            env.address
        );
    }

    target_env.stop_server();
    let old_endpoint_listener = tokio::net::TcpListener::bind(&old_target_address).await?;
    target_env.address = new_target_address;
    target_env.url = new_target_url.clone();
    target_env
        .start_rustfs_server_without_cleanup_with_env(LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;
    let moved_target_client = target_env.create_s3_client();

    let ilm_edit_status = site_replication_edit(&source_env, "enableILMExpiryReplication=true", &PeerInfo::default()).await?;
    assert!(ilm_edit_status.success, "unexpected ilm edit result: {:?}", ilm_edit_status);

    let source_after_ilm = wait_for_site_replication_info(&source_env, |info| {
        info.sites.len() == 3 && info.sites.iter().all(|peer| peer.replicate_ilm_expiry)
    })
    .await?;
    let target_after_ilm = wait_for_site_replication_info(&target_env, |info| {
        info.sites.len() == 3 && info.sites.iter().all(|peer| peer.replicate_ilm_expiry)
    })
    .await?;
    assert!(source_after_ilm.sites.iter().all(|peer| peer.replicate_ilm_expiry));
    assert!(target_after_ilm.sites.iter().all(|peer| peer.replicate_ilm_expiry));

    let status_query = "peer-state=true";
    let source_status = wait_for_site_replication_status(&source_env, status_query, |status| {
        status.peer_states.len() == 3
            && status
                .peer_states
                .values()
                .all(|state| state.peers.len() == 3 && state.peers.values().all(|peer| peer.replicate_ilm_expiry))
    })
    .await?;
    let target_status = wait_for_site_replication_status(&target_env, status_query, |status| {
        status.peer_states.len() == 3
            && status
                .peer_states
                .values()
                .all(|state| state.peers.len() == 3 && state.peers.values().all(|peer| peer.replicate_ilm_expiry))
    })
    .await?;

    assert_eq!(source_status.peer_states.len(), 3);
    assert_eq!(target_status.peer_states.len(), 3);
    assert!(source_status.peer_states.values().all(|state| state.peers.len() == 3));
    assert!(target_status.peer_states.values().all(|state| state.peers.len() == 3));
    assert!(
        source_status
            .peer_states
            .values()
            .all(|state| state.peers.values().all(|peer| peer.replicate_ilm_expiry))
    );
    assert!(
        target_status
            .peer_states
            .values()
            .all(|state| state.peers.values().all(|peer| peer.replicate_ilm_expiry))
    );

    source_client
        .put_object()
        .bucket(bucket)
        .key(moved_key)
        .body(ByteStream::from(moved_payload.clone()))
        .send()
        .await?;
    relay_client
        .put_object()
        .bucket(bucket)
        .key(relayed_key)
        .body(ByteStream::from(relayed_payload.clone()))
        .send()
        .await?;
    let no_old_endpoint_connection = async {
        match tokio::time::timeout(Duration::from_secs(3), old_endpoint_listener.accept()).await {
            Err(_) => Ok::<(), Box<dyn Error + Send + Sync>>(()),
            Ok(Ok((_, peer_address))) => Err(format!("source contacted the old site endpoint from {peer_address}").into()),
            Ok(Err(err)) => Err(err.into()),
        }
    };
    let (replicated_after_edit, replicated_from_relay, ()) = tokio::try_join!(
        wait_for_object_on_target(&moved_target_client, bucket, moved_key),
        wait_for_object_on_target(&moved_target_client, bucket, relayed_key),
        no_old_endpoint_connection,
    )?;
    assert_eq!(replicated_after_edit, moved_payload);
    assert_eq!(replicated_from_relay, relayed_payload);

    Ok(())
}

#[tokio::test]
async fn test_site_replication_remove_all_real_dual_node() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env
        .start_rustfs_server_without_cleanup_with_env(LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let bucket = "site-repl-remove-all";
    let baseline_key = "before-remove.txt";
    let baseline_payload = b"site replication before remove all".to_vec();
    let post_remove_key = "after-remove.txt";
    let racing_bucket = "site-repl-remove-racing-create";

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
                ..Default::default()
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
                ..Default::default()
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let _source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;

    source_client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&source_env, bucket).await?;
    wait_for_bucket_on_target(&target_client, bucket).await?;
    source_client
        .put_object()
        .bucket(bucket)
        .key(baseline_key)
        .body(ByteStream::from(baseline_payload.clone()))
        .send()
        .await?;
    let replicated_baseline = wait_for_object_on_target(&target_client, bucket, baseline_key).await?;
    assert_eq!(replicated_baseline, baseline_payload);

    let create_racing_bucket = source_client.create_bucket().bucket(racing_bucket).send();
    let remove_req = SRRemoveReq {
        remove_all: true,
        ..Default::default()
    };
    let remove_all = site_replication_remove(&source_env, &remove_req);
    let (create_result, remove_status) = tokio::join!(create_racing_bucket, remove_all);
    create_result?;
    let remove_status = remove_status?;
    assert!(
        !remove_status.status.is_empty() && remove_status.err_detail.is_empty(),
        "unexpected site remove result: {:?}",
        remove_status
    );

    let source_after_remove = wait_for_site_replication_disabled(&source_env).await?;
    let target_after_remove = wait_for_site_replication_disabled(&target_env).await?;

    assert!(!source_after_remove.enabled);
    assert!(source_after_remove.sites.is_empty());
    assert!(!target_after_remove.enabled);
    assert!(target_after_remove.sites.is_empty());

    source_client.head_bucket().bucket(bucket).send().await?;
    source_client.head_bucket().bucket(racing_bucket).send().await?;
    target_client.head_bucket().bucket(bucket).send().await?;
    assert_site_replication_bucket_detached(&source_env, bucket).await?;
    assert_site_replication_bucket_detached(&source_env, racing_bucket).await?;
    match target_client.head_bucket().bucket(racing_bucket).send().await {
        Ok(_) => assert_site_replication_bucket_detached(&target_env, racing_bucket).await?,
        Err(err) if matches!(err.code(), Some("NoSuchBucket" | "NotFound")) => {}
        Err(err) => return Err(err.into()),
    }
    assert_site_replication_bucket_detached(&target_env, bucket).await?;

    source_client
        .put_object()
        .bucket(bucket)
        .key(post_remove_key)
        .body(ByteStream::from_static(b"site replication must stay stopped"))
        .send()
        .await?;
    let absence_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match target_client.get_object().bucket(bucket).key(post_remove_key).send().await {
            Ok(_) => return Err("object reached the removed site replication target".into()),
            Err(err) if matches!(err.code(), Some("NoSuchKey" | "NotFound" | "NoSuchVersion")) => {
                if tokio::time::Instant::now() >= absence_deadline {
                    break;
                }
                sleep(Duration::from_millis(250)).await;
            }
            Err(err) => return Err(err.into()),
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_site_replication_state_edit_fresh_and_stale_real_dual_node() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env
        .start_rustfs_server_without_cleanup_with_env(LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
                ..Default::default()
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
                ..Default::default()
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let target_info = wait_for_site_replication_enabled(&target_env, 2).await?;
    assert!(source_info.sites.iter().all(|peer| !peer.replicate_ilm_expiry));
    assert!(target_info.sites.iter().all(|peer| !peer.replicate_ilm_expiry));

    let target_status =
        wait_for_site_replication_status(&target_env, "peer-state=true", |status| status.peer_states.len() == 2).await?;
    let current_updated_at = target_status
        .peer_states
        .values()
        .find_map(|state| state.updated_at)
        .ok_or("missing target site replication updated_at")?;

    let mut stale_peers = BTreeMap::new();
    for peer in target_info.sites {
        let mut peer = peer;
        peer.replicate_ilm_expiry = true;
        stale_peers.insert(peer.deployment_id.clone(), peer);
    }
    site_replication_state_edit(
        &target_env,
        &rustfs_madmin::SRStateEditReq {
            peers: stale_peers,
            updated_at: Some(current_updated_at - TimeDuration::seconds(1)),
        },
    )
    .await?;

    let target_after_stale = site_replication_info(&target_env).await?;
    let source_after_stale = site_replication_info(&source_env).await?;
    assert!(target_after_stale.sites.iter().all(|peer| !peer.replicate_ilm_expiry));
    assert!(source_after_stale.sites.iter().all(|peer| !peer.replicate_ilm_expiry));

    let mut fresh_peers = BTreeMap::new();
    for peer in target_after_stale.sites {
        let mut peer = peer;
        peer.replicate_ilm_expiry = true;
        fresh_peers.insert(peer.deployment_id.clone(), peer);
    }
    let fresh_updated_at = current_updated_at + TimeDuration::seconds(1);
    site_replication_state_edit(
        &target_env,
        &rustfs_madmin::SRStateEditReq {
            peers: fresh_peers,
            updated_at: Some(fresh_updated_at),
        },
    )
    .await?;

    let target_after_fresh = wait_for_site_replication_info(&target_env, |info| {
        info.sites.len() == 2 && info.sites.iter().all(|peer| peer.replicate_ilm_expiry)
    })
    .await?;
    assert!(target_after_fresh.sites.iter().all(|peer| peer.replicate_ilm_expiry));

    let target_status_after_fresh = wait_for_site_replication_status(&target_env, "peer-state=true", |status| {
        status.peer_states.len() == 2
            && status.peer_states.values().all(|state| {
                state.updated_at == Some(fresh_updated_at) && state.peers.values().all(|peer| peer.replicate_ilm_expiry)
            })
    })
    .await?;
    assert!(target_status_after_fresh.peer_states.values().all(|state| {
        state.updated_at == Some(fresh_updated_at) && state.peers.values().all(|peer| peer.replicate_ilm_expiry)
    }));

    let source_after_fresh = site_replication_info(&source_env).await?;
    assert!(source_after_fresh.sites.iter().all(|peer| !peer.replicate_ilm_expiry));

    Ok(())
}

#[tokio::test]
async fn test_site_replication_replicates_object_with_bucket_versioning_real_dual_node() -> TestResult {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env
        .start_rustfs_server_without_cleanup_with_env(LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let bucket = "site-repl-versioned";
    let key = "hello.txt";
    let payload = b"site replication should replicate after enabling versioning".to_vec();

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
                ..Default::default()
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
                ..Default::default()
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let _source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;

    source_client.create_bucket().bucket(bucket).send().await?;
    let versioning = source_client.get_bucket_versioning().bucket(bucket).send().await?;
    assert_eq!(
        versioning.status(),
        Some(&BucketVersioningStatus::Enabled),
        "site replication did not enable source bucket versioning"
    );
    let replication_response = signed_request(
        http::Method::GET,
        &format!("{}/{bucket}?replication", source_env.url),
        &source_env.access_key,
        &source_env.secret_key,
        None,
        None,
    )
    .await?;
    let replication_status = replication_response.status();
    let replication_body = replication_response.text().await.unwrap_or_default();
    assert_eq!(
        replication_status,
        StatusCode::OK,
        "source bucket replication config missing after site replication setup: {replication_body}"
    );
    source_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await?;

    let replicated = wait_for_object_on_target(&target_client, bucket, key).await?;
    assert_eq!(replicated, payload);

    Ok(())
}

/// Re-applying a site's own replication config must not disable the peer's reverse direction.
///
/// `PutBucketReplication` broadcasts the config to every peer — the console's replication
/// Save button, `mc replicate import`, and a bucket-metadata import all go through it. The
/// receiver used to overwrite its rules with the sender's, whose destination ARN names the
/// receiver itself. No bucket target can satisfy that ARN, so every object written on the
/// receiver was dropped with only a debug line, while `replicate status` still reported
/// "1/1 Buckets in sync" because both configs were byte-identical.
#[tokio::test]
async fn test_site_replication_config_broadcast_keeps_reverse_direction_real_dual_node() -> TestResult {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env
        .start_rustfs_server_without_cleanup_with_env(LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let bucket = "site-repl-config-broadcast";

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "broadcast-source".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
                ..Default::default()
            },
            PeerSite {
                name: "broadcast-target".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
                ..Default::default()
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {add_status:?}");
    wait_for_site_replication_enabled(&source_env, 2).await?;
    wait_for_site_replication_enabled(&target_env, 2).await?;

    source_client.create_bucket().bucket(bucket).send().await?;
    wait_for_bucket_on_target(&target_client, bucket).await?;

    // Both directions work before the broadcast.
    source_client
        .put_object()
        .bucket(bucket)
        .key("from-source.txt")
        .body(ByteStream::from_static(b"written on the initiating site"))
        .send()
        .await?;
    assert_eq!(
        wait_for_object_on_target(&target_client, bucket, "from-source.txt").await?,
        b"written on the initiating site".to_vec(),
    );
    target_client
        .put_object()
        .bucket(bucket)
        .key("from-target.txt")
        .body(ByteStream::from_static(b"written on the joined site"))
        .send()
        .await?;
    assert_eq!(
        wait_for_object_on_target(&source_client, bucket, "from-target.txt").await?,
        b"written on the joined site".to_vec(),
    );

    // Round-trip the source's own config through PutBucketReplication, exactly what the
    // console does when an operator opens the bucket's replication page and saves it.
    let source_config = source_client
        .get_bucket_replication()
        .bucket(bucket)
        .send()
        .await?
        .replication_configuration
        .ok_or("source bucket has no replication configuration")?;
    source_client
        .put_bucket_replication()
        .bucket(bucket)
        .replication_configuration(source_config)
        .send()
        .await?;

    let target_config = wait_for_site_replication_rule(&target_client, bucket).await?;
    let target_deployment_id = site_replication_info(&target_env)
        .await?
        .sites
        .iter()
        .find(|peer| peer.endpoint == target_env.url)
        .map(|peer| peer.deployment_id.clone())
        .ok_or("joined site missing from its own replication info")?;
    for rule in &target_config.rules {
        let destination = rule
            .destination
            .as_ref()
            .map(|destination| destination.bucket.as_str())
            .unwrap_or_default();
        assert!(
            !destination.contains(&target_deployment_id),
            "joined site adopted a rule pointing at itself: {destination}"
        );
    }

    target_client
        .put_object()
        .bucket(bucket)
        .key("from-target-after-broadcast.txt")
        .body(ByteStream::from_static(b"written after the config broadcast"))
        .send()
        .await?;
    assert_eq!(
        wait_for_object_on_target(&source_client, bucket, "from-target-after-broadcast.txt").await?,
        b"written after the config broadcast".to_vec(),
        "config broadcast made replication one-directional"
    );

    Ok(())
}

async fn wait_for_site_replication_rule(
    client: &aws_sdk_s3::Client,
    bucket: &str,
) -> Result<aws_sdk_s3::types::ReplicationConfiguration, Box<dyn Error + Send + Sync>> {
    for _ in 0..40 {
        if let Ok(response) = client.get_bucket_replication().bucket(bucket).send().await
            && let Some(config) = response.replication_configuration
            && !config.rules.is_empty()
        {
            return Ok(config);
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(format!("bucket {bucket} never reported a replication rule").into())
}

#[tokio::test]
async fn test_site_replication_active_active_converges_without_loops_real_dual_node() -> TestResult {
    init_logging();

    match tokio::time::timeout(Duration::from_secs(420), async {
        let mut site_env = replication_fast_env();
        site_env.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);

        let mut site_a_env = RustFSTestEnvironment::new().await?;
        site_a_env.start_rustfs_server_with_env(vec![], &site_env).await?;

        let mut site_b_env = RustFSTestEnvironment::new().await?;
        site_b_env.start_rustfs_server_with_env(vec![], &site_env).await?;

        let mut proxy_tasks = JoinSet::new();
        let (site_a_proxy, site_a_replication_requests, site_a_replication_enabled) =
            start_replication_counting_proxy(&site_a_env.url, &mut proxy_tasks).await?;
        let (site_b_proxy, site_b_replication_requests, site_b_replication_enabled) =
            start_replication_counting_proxy(&site_b_env.url, &mut proxy_tasks).await?;

        let site_a_client = site_a_env.create_s3_client();
        let site_b_client = site_b_env.create_s3_client();
        let bucket = "site-repl-active-active";

        let add_status = site_replication_add(
            &site_a_env,
            &[
                PeerSite {
                    name: "active-site-a".to_string(),
                    endpoint: site_a_env.url.clone(),
                    access_key: site_a_env.access_key.clone(),
                    secret_key: site_a_env.secret_key.clone(),
                    ..Default::default()
                },
                PeerSite {
                    name: "active-site-b".to_string(),
                    endpoint: site_b_env.url.clone(),
                    access_key: site_b_env.access_key.clone(),
                    secret_key: site_b_env.secret_key.clone(),
                    ..Default::default()
                },
            ],
        )
        .await?;
        assert!(add_status.success, "unexpected site add result: {add_status:?}");

        let site_info = wait_for_site_replication_enabled(&site_a_env, 2).await?;
        wait_for_site_replication_enabled(&site_b_env, 2).await?;

        let mut site_a_peer = site_info
            .sites
            .iter()
            .find(|peer| peer.endpoint == site_a_env.url)
            .ok_or("site A peer missing from replication info")?
            .clone();
        site_a_peer.endpoint = site_a_proxy.clone();
        site_a_peer.sync_state = SyncStatus::Enable;
        let site_a_edit = site_replication_edit(&site_a_env, "", &site_a_peer).await?;
        assert!(site_a_edit.success, "unexpected site A endpoint edit: {site_a_edit:?}");

        let mut site_b_peer = site_info
            .sites
            .iter()
            .find(|peer| peer.endpoint == site_b_env.url)
            .ok_or("site B peer missing from replication info")?
            .clone();
        site_b_peer.endpoint = site_b_proxy.clone();
        site_b_peer.sync_state = SyncStatus::Enable;
        let site_b_edit = site_replication_edit(&site_a_env, "", &site_b_peer).await?;
        assert!(site_b_edit.success, "unexpected site B endpoint edit: {site_b_edit:?}");

        for env in [&site_a_env, &site_b_env] {
            wait_for_site_replication_info(env, |info| {
                info.sites.iter().any(|peer| peer.endpoint == site_a_proxy)
                    && info.sites.iter().any(|peer| peer.endpoint == site_b_proxy)
            })
            .await?;
        }

        site_a_client.create_bucket().bucket(bucket).send().await?;
        wait_for_bucket_on_target(&site_b_client, bucket).await?;

        let (site_a_put, site_b_put) = tokio::join!(
            site_a_client
                .put_object()
                .bucket(bucket)
                .key("from-a.txt")
                .body(ByteStream::from_static(b"written on site A"))
                .send(),
            site_b_client
                .put_object()
                .bucket(bucket)
                .key("from-b.txt")
                .body(ByteStream::from_static(b"written on site B"))
                .send(),
        );
        site_a_put?;
        site_b_put?;
        wait_for_replicated_object(&site_b_client, bucket, "from-a.txt", "written on site A").await?;
        wait_for_replicated_object(&site_a_client, bucket, "from-b.txt", "written on site B").await?;

        let pre_conflict_counts = (
            site_a_replication_requests.load(Ordering::Relaxed),
            site_b_replication_requests.load(Ordering::Relaxed),
        );
        assert_eq!(pre_conflict_counts, (1, 1));
        site_a_replication_enabled.send(false)?;
        site_b_replication_enabled.send(false)?;
        let site_a_conflict_version = site_a_client
            .put_object()
            .bucket(bucket)
            .key("conflict.txt")
            .body(ByteStream::from_static(b"conflict from site A"))
            .send()
            .await?
            .version_id()
            .ok_or("site A conflict PUT omitted version ID")?
            .to_string();
        sleep(Duration::from_millis(10)).await;
        let site_b_conflict_version = site_b_client
            .put_object()
            .bucket(bucket)
            .key("conflict.txt")
            .body(ByteStream::from_static(b"conflict from site B"))
            .send()
            .await?
            .version_id()
            .ok_or("site B conflict PUT omitted version ID")?
            .to_string();
        assert_ne!(site_a_conflict_version, site_b_conflict_version);
        tokio::time::timeout(Duration::from_secs(70), async {
            loop {
                let counts = (
                    site_a_replication_requests.load(Ordering::Relaxed),
                    site_b_replication_requests.load(Ordering::Relaxed),
                );
                assert!(counts.0 <= pre_conflict_counts.0 + 1 && counts.1 <= pre_conflict_counts.1 + 1);
                if counts == (pre_conflict_counts.0 + 1, pre_conflict_counts.1 + 1) {
                    break;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| "replication requests did not reach both conflict gates")?;
        let site_a_isolated_conflict = list_replication_state(&site_a_client, bucket)
            .await?
            .into_iter()
            .filter(|version| version.key == "conflict.txt")
            .collect::<Vec<_>>();
        let site_b_isolated_conflict = list_replication_state(&site_b_client, bucket)
            .await?
            .into_iter()
            .filter(|version| version.key == "conflict.txt")
            .collect::<Vec<_>>();
        assert_eq!(site_a_isolated_conflict.len(), 1);
        assert_eq!(site_a_isolated_conflict[0].version_id, site_a_conflict_version);
        assert_eq!(site_b_isolated_conflict.len(), 1);
        assert_eq!(site_b_isolated_conflict[0].version_id, site_b_conflict_version);
        let (api_expected_winner, api_expected_body) = match site_a_isolated_conflict[0]
            .last_modified
            .cmp(&site_b_isolated_conflict[0].last_modified)
        {
            std::cmp::Ordering::Greater => (&site_a_conflict_version, b"conflict from site A".as_slice()),
            std::cmp::Ordering::Less => (&site_b_conflict_version, b"conflict from site B".as_slice()),
            std::cmp::Ordering::Equal => return Err("staggered conflict writes received equal LastModified values".into()),
        };
        site_a_replication_enabled.send(true)?;
        site_b_replication_enabled.send(true)?;
        tokio::time::timeout(
            Duration::from_secs(70),
            assert_replication_converged(&site_a_client, bucket, &site_b_client, bucket),
        )
        .await??;

        for (version_id, expected) in [
            (site_a_conflict_version.as_str(), b"conflict from site A".as_slice()),
            (site_b_conflict_version.as_str(), b"conflict from site B".as_slice()),
        ] {
            assert_eq!(get_version_body(&site_a_client, bucket, "conflict.txt", version_id).await?, expected);
            assert_eq!(get_version_body(&site_b_client, bucket, "conflict.txt", version_id).await?, expected);
        }

        // Newer LastModified wins; the writes are staggered while replication is
        // blocked so this test does not claim a tie-break for equal timestamps.
        let site_a_current = site_a_client
            .get_object()
            .bucket(bucket)
            .key("conflict.txt")
            .send()
            .await?
            .body
            .collect()
            .await?
            .into_bytes();
        let site_b_current = site_b_client
            .get_object()
            .bucket(bucket)
            .key("conflict.txt")
            .send()
            .await?
            .body
            .collect()
            .await?
            .into_bytes();
        assert_eq!(site_a_current, site_b_current);
        let observed_winner_version = if site_a_current.as_ref() == b"conflict from site A" {
            &site_a_conflict_version
        } else if site_a_current.as_ref() == b"conflict from site B" {
            &site_b_conflict_version
        } else {
            return Err(format!("unexpected active-active winner: {site_a_current:?}").into());
        };
        assert_eq!(site_a_current.as_ref(), api_expected_body);
        assert_eq!(observed_winner_version, api_expected_winner);

        site_a_client
            .put_object()
            .bucket(bucket)
            .key("deleted.txt")
            .body(ByteStream::from_static(b"delete me"))
            .send()
            .await?;
        site_a_client.delete_object().bucket(bucket).key("deleted.txt").send().await?;
        tokio::time::timeout(
            Duration::from_secs(70),
            assert_replication_converged(&site_a_client, bucket, &site_b_client, bucket),
        )
        .await??;
        let site_a_deleted = site_a_client
            .get_object()
            .bucket(bucket)
            .key("deleted.txt")
            .send()
            .await
            .expect_err("site A deleted object unexpectedly rebounded");
        let site_b_deleted = site_b_client
            .get_object()
            .bucket(bucket)
            .key("deleted.txt")
            .send()
            .await
            .expect_err("site B deleted object unexpectedly exists");
        assert_eq!(site_a_deleted.as_service_error().and_then(|error| error.code()), Some("NoSuchKey"));
        assert_eq!(site_b_deleted.as_service_error().and_then(|error| error.code()), Some("NoSuchKey"));

        let stable_state = list_replication_state(&site_a_client, bucket).await?;
        assert_eq!(stable_state.iter().filter(|version| version.key == "from-a.txt").count(), 1);
        assert_eq!(stable_state.iter().filter(|version| version.key == "from-b.txt").count(), 1);
        assert_eq!(stable_state.iter().filter(|version| version.key == "conflict.txt").count(), 2);
        assert_eq!(
            stable_state
                .iter()
                .filter(|version| version.key == "conflict.txt" && version.is_latest)
                .count(),
            1
        );
        assert!(
            stable_state.iter().any(|version| version.key == "conflict.txt"
                && version.version_id == *observed_winner_version
                && version.is_latest)
        );
        assert_eq!(stable_state.iter().filter(|version| version.key == "deleted.txt").count(), 2);
        assert_eq!(
            stable_state
                .iter()
                .filter(|version| version.key == "deleted.txt" && version.delete_marker)
                .count(),
            1
        );
        assert_eq!(
            stable_state
                .iter()
                .filter(|version| version.key == "deleted.txt" && !version.delete_marker)
                .count(),
            1
        );
        assert_eq!(
            stable_state
                .iter()
                .filter(|version| version.key == "deleted.txt" && version.delete_marker && version.is_latest)
                .count(),
            1
        );
        assert_eq!(
            stable_state
                .iter()
                .filter(|version| version.key == "deleted.txt" && version.is_latest)
                .count(),
            1
        );

        let baseline_counts = (
            site_a_replication_requests.load(Ordering::Relaxed),
            site_b_replication_requests.load(Ordering::Relaxed),
        );
        assert_eq!(baseline_counts, (2, 4));
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(
            (
                site_a_replication_requests.load(Ordering::Relaxed),
                site_b_replication_requests.load(Ordering::Relaxed),
            ),
            baseline_counts
        );
        assert_eq!(list_replication_state(&site_a_client, bucket).await?, stable_state);
        assert_eq!(list_replication_state(&site_b_client, bucket).await?, stable_state);

        Ok(())
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err("active-active replication test timed out".into()),
    }
}

#[tokio::test]
async fn test_site_replication_replicates_policy_backed_user_access_real_dual_node() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env
        .start_rustfs_server_without_cleanup_with_env(LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let bucket = "site-repl-policy-user";
    let key = "seed.txt";
    let payload = b"site replication policy-backed user access".to_vec();
    let policy_name = "site-repl-readonly";
    let username = "site-repl-user";
    let secret_key = "site-repl-user-secret-key-123456";

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
                ..Default::default()
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
                ..Default::default()
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let _source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;

    source_client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&source_env, bucket).await?;
    source_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await?;

    let replicated = wait_for_object_on_target(&target_client, bucket, key).await?;
    assert_eq!(replicated, payload);

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": [format!("arn:aws:s3:::{bucket}/*")]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                "Resource": [format!("arn:aws:s3:::{bucket}")]
            }
        ]
    });
    admin_add_canned_policy(&source_env, policy_name, &policy).await?;
    admin_create_user(&source_env, username, secret_key).await?;
    admin_attach_policy_to_user(&source_env, policy_name, username).await?;

    let target_user_client = create_user_s3_client(&target_env, username, secret_key);
    let fetched = wait_for_user_get_object(&target_user_client, bucket, key).await?;
    assert_eq!(fetched, payload);

    Ok(())
}

#[tokio::test]
async fn test_site_replication_replicates_group_policy_backed_access_real_dual_node() -> Result<(), Box<dyn Error + Send + Sync>>
{
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env
        .start_rustfs_server_without_cleanup_with_env(LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();
    let bucket = "site-repl-policy-group";
    let key = "seed.txt";
    let payload = b"site replication group-policy-backed user access".to_vec();
    let policy_name = "site-repl-group-readonly";
    let group_name = "site-repl-group";
    let username = "site-repl-group-user";
    let secret_key = "site-repl-group-user-secret-key-12";

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
                ..Default::default()
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
                ..Default::default()
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let _source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;

    source_client.create_bucket().bucket(bucket).send().await?;
    enable_bucket_versioning(&source_env, bucket).await?;
    source_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await?;

    let replicated = wait_for_object_on_target(&target_client, bucket, key).await?;
    assert_eq!(replicated, payload);

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": [format!("arn:aws:s3:::{bucket}/*")]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                "Resource": [format!("arn:aws:s3:::{bucket}")]
            }
        ]
    });
    admin_add_canned_policy(&source_env, policy_name, &policy).await?;
    admin_create_user(&source_env, username, secret_key).await?;
    admin_update_group_members(&source_env, group_name, &[username]).await?;
    admin_attach_policy_to_group(&source_env, policy_name, group_name).await?;

    let target_user_client = create_user_s3_client(&target_env, username, secret_key);
    let fetched = wait_for_user_get_object(&target_user_client, bucket, key).await?;
    assert_eq!(fetched, payload);

    Ok(())
}

#[tokio::test]
async fn test_service_account_policy_from_accountinfo_round_trips_real_single_node() -> TestResult {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let account_info = get_account_info(&env, &env.access_key, &env.secret_key).await?;
    let policy_str = account_info
        .get("policy")
        .and_then(|value| value.as_str())
        .ok_or("account info policy should be a JSON string")?;

    let policy: serde_json::Value = serde_json::from_str(policy_str)?;
    let statements = policy
        .get("Statement")
        .and_then(|value| value.as_array())
        .ok_or("account info policy should include Statement array")?;

    assert!(!statements.is_empty(), "account info policy Statement should not be empty: {policy}");

    let req = AddServiceAccountReq {
        policy: Some(policy),
        target_user: None,
        access_key: "svcacct-info-sample".to_string(),
        secret_key: "svcacct-info-sample-secret-key-123456".to_string(),
        name: Some("svcacct-info-sample".to_string()),
        description: Some("service account created from accountinfo sample policy".to_string()),
        expiration: None,
        comment: None,
    };

    let created = add_service_account(&env, &env.access_key, &env.secret_key, &req).await?;
    assert_eq!(created.0, "svcacct-info-sample");

    let listed =
        wait_for_service_accounts(&env, &env.access_key, &env.secret_key, Some(&env.access_key), &["svcacct-info-sample"])
            .await?;
    assert!(
        listed
            .accounts
            .iter()
            .any(|account| account.access_key == "svcacct-info-sample"),
        "created service account should be listed for parent user: {:?}",
        listed.accounts
    );

    Ok(())
}

#[tokio::test]
async fn test_site_replication_replicates_multiple_service_accounts_real_dual_node() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env
        .start_rustfs_server_without_cleanup_with_env(LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
                ..Default::default()
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
                ..Default::default()
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let _source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;

    let first_req = AddServiceAccountReq {
        policy: None,
        target_user: None,
        access_key: "svc-alpha".to_string(),
        secret_key: "svc-alpha-secret-key-1234567890abcdef".to_string(),
        name: Some("svc-alpha".to_string()),
        description: Some("first replicated service account".to_string()),
        expiration: None,
        comment: None,
    };
    let first = add_service_account(&source_env, &source_env.access_key, &source_env.secret_key, &first_req).await?;

    let target_after_first = wait_for_service_accounts(
        &target_env,
        &target_env.access_key,
        &target_env.secret_key,
        Some(&source_env.access_key),
        &["svc-alpha"],
    )
    .await?;
    assert!(
        target_after_first
            .accounts
            .iter()
            .any(|account| account.access_key == "svc-alpha"),
        "target accounts missing svc-alpha: {:?}",
        target_after_first.accounts
    );

    let second_req = AddServiceAccountReq {
        policy: None,
        target_user: None,
        access_key: "svc-beta".to_string(),
        secret_key: "svc-beta-secret-key-1234567890abcdef1".to_string(),
        name: Some("svc-beta".to_string()),
        description: Some("second replicated service account".to_string()),
        expiration: None,
        comment: None,
    };
    let _second = add_service_account(&source_env, &first.0, &first.1, &second_req).await?;

    let target_after_second = wait_for_service_accounts(
        &target_env,
        &target_env.access_key,
        &target_env.secret_key,
        Some(&source_env.access_key),
        &["svc-alpha", "svc-beta"],
    )
    .await?;
    assert!(
        target_after_second
            .accounts
            .iter()
            .any(|account| account.access_key == "svc-beta"),
        "target accounts missing svc-beta: {:?}",
        target_after_second.accounts
    );

    Ok(())
}

#[tokio::test]
async fn test_site_replication_replicates_service_accounts_created_from_sts_session_real_dual_node() -> TestResult {
    init_logging();

    if !awscurl_available() {
        eprintln!("Skipping STS site replication service-account test because awscurl is unavailable");
        return Ok(());
    }

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env
        .start_rustfs_server_without_cleanup_with_env(LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let add_status = site_replication_add(
        &source_env,
        &[
            PeerSite {
                name: "source-site".to_string(),
                endpoint: source_env.url.clone(),
                access_key: source_env.access_key.clone(),
                secret_key: source_env.secret_key.clone(),
                ..Default::default()
            },
            PeerSite {
                name: "target-site".to_string(),
                endpoint: target_env.url.clone(),
                access_key: target_env.access_key.clone(),
                secret_key: target_env.secret_key.clone(),
                ..Default::default()
            },
        ],
    )
    .await?;
    assert!(add_status.success, "unexpected site add result: {:?}", add_status);

    let _source_info = wait_for_site_replication_enabled(&source_env, 2).await?;
    let _target_info = wait_for_site_replication_enabled(&target_env, 2).await?;

    let assume_role_body = "Action=AssumeRole&Version=2011-06-15&DurationSeconds=3600";
    let sts_xml = awscurl_post_sts_form_urlencoded(
        &format!("{}/", source_env.url.trim_end_matches('/')),
        assume_role_body,
        &source_env.access_key,
        &source_env.secret_key,
    )
    .await?;
    let (sts_access_key, sts_secret_key, sts_session_token) = parse_assume_role_credentials(&sts_xml)?;

    let first_req = AddServiceAccountReq {
        policy: None,
        target_user: None,
        access_key: "svc-sts-alpha".to_string(),
        secret_key: "svc-sts-alpha-secret-key-1234567890".to_string(),
        name: Some("svc-sts-alpha".to_string()),
        description: Some("sts-created replicated service account".to_string()),
        expiration: None,
        comment: None,
    };
    let first =
        add_service_account_with_session_token(&source_env, &sts_access_key, &sts_secret_key, &sts_session_token, &first_req)
            .await?;

    let target_after_first = wait_for_service_accounts(
        &target_env,
        &target_env.access_key,
        &target_env.secret_key,
        Some(&source_env.access_key),
        &["svc-sts-alpha"],
    )
    .await?;
    assert!(
        target_after_first
            .accounts
            .iter()
            .any(|account| account.access_key == "svc-sts-alpha"),
        "target accounts missing svc-sts-alpha: {:?}",
        target_after_first.accounts
    );

    let second_req = AddServiceAccountReq {
        policy: None,
        target_user: None,
        access_key: "svc-sts-beta".to_string(),
        secret_key: "svc-sts-beta-secret-key-1234567890a".to_string(),
        name: Some("svc-sts-beta".to_string()),
        description: Some("second replicated service account from sts-created ak".to_string()),
        expiration: None,
        comment: None,
    };
    let _second = add_service_account(&source_env, &first.0, &first.1, &second_req).await?;

    let target_after_second = wait_for_service_accounts(
        &target_env,
        &target_env.access_key,
        &target_env.secret_key,
        Some(&source_env.access_key),
        &["svc-sts-alpha", "svc-sts-beta"],
    )
    .await?;
    assert!(
        target_after_second
            .accounts
            .iter()
            .any(|account| account.access_key == "svc-sts-beta"),
        "target accounts missing svc-sts-beta: {:?}",
        target_after_second.accounts
    );

    Ok(())
}

/// Poll the fake target journal until `operation` arrives for `key`, then
/// return the `versionId` query value the request carried.
async fn wait_for_target_request_version_id(
    target: &FakeS3Target,
    operation: FakeTargetOperation,
    key: &str,
) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(record) = target
            .requests()
            .into_iter()
            .find(|record| record.operation == operation && record.key.as_deref() == Some(key))
        {
            return Ok(record.version_id);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("fake target never received {operation:?} for {key}; journal: {:?}", target.requests()).into());
        }
        sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn test_bucket_resync_restart_revisits_objects_before_out_of_order_checkpoint() -> TestResult {
    init_logging();

    const OBJECT_COUNT: usize = 128;
    let target = FakeS3Target::start().await?;
    let target_bucket = "resync-checkpoint-dst";
    target.create_bucket(target_bucket);

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut process_env = replication_fast_env();
    process_env.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    process_env.extend_from_slice(&[
        ("NO_PROXY", "127.0.0.1,localhost"),
        ("HTTP_PROXY", ""),
        ("HTTPS_PROXY", ""),
        ("RUST_LOG", "error"),
    ]);
    source_env.start_rustfs_server_with_env(vec![], &process_env).await?;

    let source_bucket = "resync-checkpoint-src";
    let source_client = source_env.create_s3_client();
    source_client.create_bucket().bucket(source_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    let target_arn = set_replication_target_with_options(
        &source_env,
        source_bucket,
        ReplicationTargetOptions {
            endpoint: &target.address(),
            access_key: FAKE_ACCESS_KEY,
            secret_key: FAKE_SECRET_KEY,
            target_bucket,
            secure: false,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    for idx in 0..OBJECT_COUNT {
        source_client
            .put_object()
            .bucket(source_bucket)
            .key(format!("checkpoint-{idx:03}"))
            .body(ByteStream::from(format!("checkpoint payload {idx}").into_bytes()))
            .send()
            .await?;
    }
    wait_for_source_replication_status(&source_client, source_bucket, "checkpoint-127", "COMPLETED", false).await?;
    let initial_replication_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let replicated = target
            .requests()
            .into_iter()
            .filter(|request| request.operation == FakeTargetOperation::PutObject)
            .count();
        if replicated >= OBJECT_COUNT {
            break;
        }
        if tokio::time::Instant::now() >= initial_replication_deadline {
            return Err(format!("initial replication only sent {replicated}/{OBJECT_COUNT} objects").into());
        }
        sleep(Duration::from_millis(100)).await;
    }
    target.clear_bucket_objects(target_bucket);
    target.take_requests();
    target.inject_for_key(
        FakeTargetOperation::PutObject,
        "checkpoint-000",
        FakeTargetFault::Delay(Duration::from_secs(30)),
        100,
    );

    let (reset_arn, reset_id) = start_bucket_replication_reset(&source_env, source_bucket).await?;
    assert_eq!(reset_arn, target_arn);
    let partial = wait_for_replication_reset_target(&source_env, source_bucket, &target_arn, |status| {
        status.reset_id == reset_id
            && status.replicated_count > 0
            && status.replicated_count < i64::try_from(OBJECT_COUNT).expect("test object count should fit i64")
            && status.object.as_str() > "checkpoint-000"
    })
    .await
    .map_err(|err| format!("{err}; target journal: {:?}", target.requests()))?;
    assert!(target.requests().iter().any(|request| {
        request.operation == FakeTargetOperation::PutObject
            && request.key.as_deref() == Some("checkpoint-000")
            && request.fault == Some(FakeTargetFault::Delay(Duration::from_secs(30)))
    }));
    assert!(!target.has_object(target_bucket, "checkpoint-000"));
    source_env.stop_server();
    target.clear_faults();

    source_env.start_rustfs_server_without_cleanup_with_env(&process_env).await?;
    let completed = wait_for_replication_reset_target(&source_env, source_bucket, &target_arn, |status| {
        status.reset_id == reset_id && status.status == "Completed"
    })
    .await?;
    assert_eq!(
        completed.replicated_count,
        i64::try_from(OBJECT_COUNT).expect("test object count should fit i64")
    );
    assert!(
        target.has_object(target_bucket, "checkpoint-000"),
        "restart skipped failed object checkpoint-000 before persisted checkpoint {}",
        partial.object
    );

    target.shutdown().await;
    Ok(())
}

/// P0-5: MinIO derives the replicated version exclusively from the `versionId`
/// query parameter (`putOptsFromReq`); the internal x-*-source-version-id
/// headers do not exist there. Without the query, a MinIO target mints fresh
/// version ids and RustFS -> MinIO replication drifts. PutObject and
/// CreateMultipartUpload (the version is decided at initiate time) must both
/// carry the source version as `?versionId=`.
#[tokio::test]
async fn test_replication_put_and_create_multipart_carry_source_version_id_query() -> TestResult {
    init_logging();

    let target = FakeS3Target::start().await?;
    let target_bucket = "versionid-query-dst";
    target.create_bucket(target_bucket);

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut source_process_env = replication_fast_env();
    source_process_env.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    source_process_env.extend_from_slice(&[("NO_PROXY", "127.0.0.1,localhost"), ("HTTP_PROXY", ""), ("HTTPS_PROXY", "")]);
    source_env.start_rustfs_server_with_env(vec![], &source_process_env).await?;

    let source_bucket = "versionid-query-src";
    let source_client = source_env.create_s3_client();
    source_client.create_bucket().bucket(source_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;

    let target_arn = set_replication_target_with_options(
        &source_env,
        source_bucket,
        ReplicationTargetOptions {
            endpoint: &target.address(),
            access_key: FAKE_ACCESS_KEY,
            secret_key: FAKE_SECRET_KEY,
            target_bucket,
            secure: false,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    // Small object -> replicated through a single PutObject.
    let put = source_client
        .put_object()
        .bucket(source_bucket)
        .key("small.txt")
        .body(ByteStream::from_static(b"versionid query payload"))
        .send()
        .await?;
    let put_source_version = put
        .version_id()
        .ok_or("versioned source PUT must return a version id")?
        .to_string();
    let recorded = wait_for_target_request_version_id(&target, FakeTargetOperation::PutObject, "small.txt").await?;
    assert_eq!(
        recorded.as_deref(),
        Some(put_source_version.as_str()),
        "replication PutObject must carry the source version in the versionId query"
    );

    // Multipart source object -> replicated through CreateMultipartUpload;
    // the target version is fixed at initiate time.
    let create = source_client
        .create_multipart_upload()
        .bucket(source_bucket)
        .key("large.bin")
        .send()
        .await?;
    let upload_id = create
        .upload_id()
        .ok_or("multipart initiate must return an upload id")?
        .to_string();
    let mut completed_parts = Vec::new();
    for (part_number, body) in [(1, vec![b'a'; 5 * 1024 * 1024]), (2, vec![b'b'; 1024])] {
        let uploaded = source_client
            .upload_part()
            .bucket(source_bucket)
            .key("large.bin")
            .upload_id(&upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body))
            .send()
            .await?;
        completed_parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(uploaded.e_tag().unwrap_or_default())
                .build(),
        );
    }
    let complete = source_client
        .complete_multipart_upload()
        .bucket(source_bucket)
        .key("large.bin")
        .upload_id(&upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build())
        .send()
        .await?;
    let multipart_source_version = complete
        .version_id()
        .ok_or("versioned multipart completion must return a version id")?
        .to_string();
    let recorded = wait_for_target_request_version_id(&target, FakeTargetOperation::CreateMultipartUpload, "large.bin").await?;
    assert_eq!(
        recorded.as_deref(),
        Some(multipart_source_version.as_str()),
        "replication CreateMultipartUpload must carry the source version in the versionId query"
    );

    target.shutdown().await;
    Ok(())
}

/// P1-20: inbound replicas never cascade. An object replicated A->B carries
/// x-amz-replication-status=REPLICA on B; `must_replicate` returns an empty
/// decision for replicas, so even an ExistingObjectReplication=Enabled rule
/// configured on B AFTER the replica landed (making it an "existing object"
/// for that rule) must never push it onward — while B's own native objects
/// flow to the onward bucket, proving B's outbound replication and scanner
/// are live.
#[tokio::test]
async fn test_scanner_never_cascades_inbound_replicas() -> TestResult {
    init_logging();

    let mut env_a = RustFSTestEnvironment::new().await?;
    let mut env_a_vars = replication_fast_env();
    env_a_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    env_a.start_rustfs_server_with_env(vec![], &env_a_vars).await?;

    // B becomes a replication source itself, driven by its scanner.
    let mut env_b = RustFSTestEnvironment::new().await?;
    let mut env_b_vars = replication_fast_env();
    env_b_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    env_b_vars.extend_from_slice(FAST_SCANNER_ENV);
    env_b.start_rustfs_server_with_env(vec![], &env_b_vars).await?;

    let bucket_a = "cascade-a-src";
    let bucket_b = "cascade-b-mid";
    let bucket_c = "cascade-a-third";
    let client_a = env_a.create_s3_client();
    let client_b = env_b.create_s3_client();
    client_a.create_bucket().bucket(bucket_a).send().await?;
    client_b.create_bucket().bucket(bucket_b).send().await?;
    client_a.create_bucket().bucket(bucket_c).send().await?;
    enable_bucket_versioning(&env_a, bucket_a).await?;
    enable_bucket_versioning(&env_b, bucket_b).await?;
    enable_bucket_versioning(&env_a, bucket_c).await?;

    // A -> B first: the replica lands on B before B has any outbound rule.
    let arn_ab = set_replication_target(&env_a, bucket_a, &env_b, bucket_b).await?;
    put_bucket_replication(&env_a, bucket_a, &arn_ab).await?;

    let replica_key = "replica-object.txt";
    let replica_payload = "replica payload";
    client_a
        .put_object()
        .bucket(bucket_a)
        .key(replica_key)
        .body(ByteStream::from_static(replica_payload.as_bytes()))
        .send()
        .await?;
    wait_for_replicated_object(&client_b, bucket_b, replica_key, replica_payload).await?;

    // Precondition for the anti-cascade contract: the inbound copy must carry
    // REPLICA status on B. If this fails, the break is in inbound status
    // stamping, not in the scanner guard.
    let inbound = client_b.head_object().bucket(bucket_b).key(replica_key).send().await?;
    assert_eq!(
        inbound.replication_status().map(|status| status.as_str()),
        Some("REPLICA"),
        "inbound replica must be stamped REPLICA on the target"
    );

    // Now wire B's outbound rule; the replica is an "existing object" for it.
    let arn_bc = set_replication_target(&env_b, bucket_b, &env_a, bucket_c).await?;
    put_bucket_replication(&env_b, bucket_b, &arn_bc).await?;

    // B's own native object flows onward through the live path.
    let native_key = "native-control.txt";
    let native_payload = "native control payload";
    client_b
        .put_object()
        .bucket(bucket_b)
        .key(native_key)
        .body(ByteStream::from_static(native_payload.as_bytes()))
        .send()
        .await?;
    wait_for_replicated_object(&client_a, bucket_c, native_key, native_payload).await?;

    // With B's outbound proven, the inbound replica must stay put across
    // multiple fast-scanner cycles.
    assert_replication_key_absent(&client_a, bucket_c, replica_key, Duration::from_secs(6)).await?;

    Ok(())
}

/// P1-19 review follow-up: multipart fixes the target version at initiate
/// and only reports it on completion, so a target can adopt PutObject
/// version ids and still mint its own there — the check must not report OK
/// while multipart deletes and heals would silently miss.
#[tokio::test]
async fn test_replication_check_flags_multipart_only_version_minting_target() -> TestResult {
    init_logging();

    let target = FakeS3Target::start().await?;
    let target_bucket = "multipart-fidelity-dst";
    target.create_bucket(target_bucket);
    // PutObject mirrors the source version id; CreateMultipartUpload does not.
    target.assign_own_multipart_version_ids(true);

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut env_vars = replication_fast_env();
    env_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    env_vars.extend_from_slice(&[("NO_PROXY", "127.0.0.1,localhost"), ("HTTP_PROXY", ""), ("HTTPS_PROXY", "")]);
    source_env.start_rustfs_server_with_env(vec![], &env_vars).await?;

    let source_bucket = "multipart-fidelity-src";
    let source_client = source_env.create_s3_client();
    source_client.create_bucket().bucket(source_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;

    let target_arn = set_replication_target_with_options(
        &source_env,
        source_bucket,
        ReplicationTargetOptions {
            endpoint: &target.address(),
            access_key: FAKE_ACCESS_KEY,
            secret_key: FAKE_SECRET_KEY,
            target_bucket,
            secure: false,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    let response = run_replication_check(&source_env, source_bucket).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await?;

    assert_eq!(payload["Status"], "FAILED", "multipart drift must fail the check: {payload}");
    let target_report = &payload["Targets"][0];
    let fidelity = &target_report["Phases"]["VersionFidelity"];
    assert_eq!(fidelity["Status"], "FAILED", "{payload}");
    assert_eq!(fidelity["Code"], "BucketRemoteTargetVersionMismatch", "{payload}");
    assert!(
        fidelity["Error"]
            .as_str()
            .is_some_and(|error| error.contains("CreateMultipartUpload")),
        "the failure must name the multipart path: {payload}"
    );
    // The PutObject leg mirrored, so it is the multipart probe that failed.
    assert_eq!(target_report["Phases"]["Put"]["Status"], "OK", "{payload}");
    assert_eq!(target_report["Phases"]["DeleteMarker"]["Status"], "SKIPPED", "{payload}");
    assert_eq!(target_report["Phases"]["Cleanup"]["Status"], "OK", "{payload}");

    let probe_key = target
        .requests()
        .into_iter()
        .find(|record| record.operation == FakeTargetOperation::PutObject)
        .and_then(|record| record.key)
        .ok_or("the probe PUT never reached the fake target")?;
    assert!(
        target.stored_versions(target_bucket, &probe_key).is_empty(),
        "both probe versions must be cleaned up on the mismatching target"
    );

    target.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_replication_check_aborts_failed_multipart_probes() -> TestResult {
    init_logging();

    let target = FakeS3Target::start().await?;
    let target_bucket = "multipart-cleanup-dst";
    target.create_bucket(target_bucket);

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut env_vars = replication_fast_env();
    env_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    env_vars.extend_from_slice(&[("NO_PROXY", "127.0.0.1,localhost"), ("HTTP_PROXY", ""), ("HTTPS_PROXY", "")]);
    source_env.start_rustfs_server_with_env(vec![], &env_vars).await?;

    let source_bucket = "multipart-cleanup-src";
    let source_client = source_env.create_s3_client();
    source_client.create_bucket().bucket(source_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;

    let target_arn = set_replication_target_with_options(
        &source_env,
        source_bucket,
        ReplicationTargetOptions {
            endpoint: &target.address(),
            access_key: FAKE_ACCESS_KEY,
            secret_key: FAKE_SECRET_KEY,
            target_bucket,
            secure: false,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    for failed_operation in [FakeTargetOperation::UploadPart, FakeTargetOperation::CompleteMultipartUpload] {
        target.clear_faults();
        target.take_requests();
        target.inject(failed_operation, FakeTargetFault::Status(StatusCode::SERVICE_UNAVAILABLE), 16);

        let response = run_replication_check(&source_env, source_bucket).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let payload: serde_json::Value = response.json().await?;
        assert_eq!(
            payload["Status"], "FAILED",
            "the injected multipart failure must fail the check: {payload}"
        );

        let requests = target.requests();
        assert!(
            requests.iter().any(|request| {
                request.operation == failed_operation
                    && request.fault == Some(FakeTargetFault::Status(StatusCode::SERVICE_UNAVAILABLE))
            }),
            "the check must reach the injected {failed_operation:?} failure: {requests:?}"
        );
        assert!(
            requests
                .iter()
                .any(|request| request.operation == FakeTargetOperation::AbortMultipartUpload),
            "the failed {failed_operation:?} probe must be aborted: {requests:?}"
        );
        assert_eq!(
            target.active_multipart_upload_count(),
            0,
            "the failed {failed_operation:?} probe must not leave multipart state"
        );
        assert_eq!(payload["Targets"][0]["Phases"]["Cleanup"]["Status"], "OK", "{payload}");
    }

    target.clear_faults();
    target.take_requests();
    target.inject(FakeTargetOperation::CompleteMultipartUpload, FakeTargetFault::DisconnectAfterResponse, 16);

    let response = run_replication_check(&source_env, source_bucket).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await?;
    let target_report = &payload["Targets"][0];
    assert_eq!(target_report["Status"], "FAILED", "{payload}");
    assert_eq!(target_report["Phases"]["VersionFidelity"]["Status"], "FAILED", "{payload}");
    assert_eq!(
        target_report["Phases"]["Cleanup"]["Status"], "OK",
        "NoSuchUpload after an ambiguous complete means the multipart artifact is gone: {payload}"
    );
    let requests = target.requests();
    let completed_key = requests
        .iter()
        .find(|request| {
            request.operation == FakeTargetOperation::CompleteMultipartUpload
                && request.fault == Some(FakeTargetFault::DisconnectAfterResponse)
        })
        .and_then(|request| request.key.as_deref())
        .expect("the scripted complete response disconnect must be observed");
    assert!(
        requests
            .iter()
            .any(|request| request.operation == FakeTargetOperation::AbortMultipartUpload),
        "the ambiguous complete must still attempt abort: {requests:?}"
    );
    assert_eq!(target.active_multipart_upload_count(), 0);
    assert!(
        target.stored_versions(target_bucket, completed_key).is_empty(),
        "outer cleanup must remove the object committed before the response disconnect"
    );

    target.clear_faults();
    target.take_requests();
    target.inject(FakeTargetOperation::UploadPart, FakeTargetFault::Status(StatusCode::FORBIDDEN), 16);
    target.inject(
        FakeTargetOperation::AbortMultipartUpload,
        FakeTargetFault::Status(StatusCode::SERVICE_UNAVAILABLE),
        16,
    );

    let response = run_replication_check(&source_env, source_bucket).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await?;
    let target_report = &payload["Targets"][0];
    assert_eq!(target_report["Status"], "FAILED", "{payload}");
    assert_eq!(target_report["Phases"]["VersionFidelity"]["Status"], "FAILED", "{payload}");
    assert_eq!(
        target_report["Phases"]["Cleanup"]["Status"], "FAILED",
        "an unremoved multipart probe must be reported as a cleanup failure: {payload}"
    );
    assert_eq!(
        target_report["Error"], "s3:ReplicateObject permissions missing for replication user",
        "the primary multipart error must remain the target error: {payload}"
    );
    assert_eq!(
        target_report["Phases"]["VersionFidelity"]["Error"], "s3:ReplicateObject permissions missing for replication user",
        "{payload}"
    );
    assert_eq!(
        target_report["Phases"]["Cleanup"]["Error"], "failed to abort multipart replication probe",
        "{payload}"
    );
    assert!(
        target.requests().iter().any(|request| {
            request.operation == FakeTargetOperation::AbortMultipartUpload
                && request.fault == Some(FakeTargetFault::Status(StatusCode::SERVICE_UNAVAILABLE))
        }),
        "the abort failure must be observed"
    );
    assert_eq!(
        target.active_multipart_upload_count(),
        1,
        "the report must match the retained multipart state"
    );

    target.shutdown().await;
    Ok(())
}

/// P1-19 (backlog#1675): the supported replication contract is targets that
/// adopt the source version id (RustFS/MinIO semantics). A target that mints
/// its own version ids silently breaks every version-addressed operation that
/// follows — version deletes and heal re-drives never match, diverging the
/// two sides. replication-check must surface this explicitly: a
/// VersionFidelity phase that compares the probe PUT's response version id
/// against the sent source version id and fails with
/// BucketRemoteTargetVersionMismatch — while still cleaning up the probe
/// object via the version id the target actually assigned.
#[tokio::test]
async fn test_replication_check_flags_version_minting_target() -> TestResult {
    init_logging();

    let target = FakeS3Target::start().await?;
    let target_bucket = "version-fidelity-dst";
    target.create_bucket(target_bucket);
    target.assign_own_version_ids(true);

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut env_vars = replication_fast_env();
    env_vars.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    env_vars.extend_from_slice(&[("NO_PROXY", "127.0.0.1,localhost"), ("HTTP_PROXY", ""), ("HTTPS_PROXY", "")]);
    source_env.start_rustfs_server_with_env(vec![], &env_vars).await?;

    let source_bucket = "version-fidelity-src";
    let source_client = source_env.create_s3_client();
    source_client.create_bucket().bucket(source_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;

    let target_arn = set_replication_target_with_options(
        &source_env,
        source_bucket,
        ReplicationTargetOptions {
            endpoint: &target.address(),
            access_key: FAKE_ACCESS_KEY,
            secret_key: FAKE_SECRET_KEY,
            target_bucket,
            secure: false,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    let response = run_replication_check(&source_env, source_bucket).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await?;

    assert_eq!(
        payload["Status"], "FAILED",
        "a version-minting target must fail the replication check: {payload}"
    );
    let target_report = &payload["Targets"][0];
    assert_eq!(target_report["Status"], "FAILED", "target must be FAILED: {payload}");
    let fidelity = &target_report["Phases"]["VersionFidelity"];
    assert_eq!(fidelity["Status"], "FAILED", "VersionFidelity phase must fail: {payload}");
    assert_eq!(
        fidelity["Code"], "BucketRemoteTargetVersionMismatch",
        "the failure must carry a machine-readable code: {payload}"
    );
    // The probe PUT itself succeeded (fidelity is judged from its response);
    // the later mutation phases are pointless against a drifting target and
    // must be skipped, but cleanup still runs.
    assert_eq!(target_report["Phases"]["Put"]["Status"], "OK", "{payload}");
    assert_eq!(target_report["Phases"]["DeleteMarker"]["Status"], "SKIPPED", "{payload}");
    assert_eq!(target_report["Phases"]["Cleanup"]["Status"], "OK", "{payload}");

    // The probe PUT must carry the source version as `?versionId=` — the
    // exact shape live replication uses (P0-5), and the only shape MinIO
    // consumes. The journal records the query value.
    let probe_put = target
        .requests()
        .into_iter()
        .find(|record| record.operation == FakeTargetOperation::PutObject)
        .ok_or("the probe PUT never reached the fake target")?;
    let probe_query_version = probe_put
        .version_id
        .as_deref()
        .ok_or("the probe PUT must carry a versionId query")?;
    assert!(
        uuid::Uuid::parse_str(probe_query_version).is_ok(),
        "the probe versionId query must be the source uuid, got {probe_query_version}"
    );

    // No probe residue: cleanup must address the version id the target
    // actually assigned, not the source id (which never matched anything).
    let probe_key = probe_put.key.ok_or("probe PUT journal record has no key")?;
    assert!(
        target.stored_versions(target_bucket, &probe_key).is_empty(),
        "the probe object must be cleaned up on the mismatching target"
    );

    Ok(())
}

// --- P1-21 (backlog#1675): delayed delete-marker purge failure handling ---
//
// The fixtures below wire a versioned source bucket to a FakeS3Target with the
// default replication shape: DeleteMarkerReplication=Enabled and
// DeleteReplication omitted. With version-delete replication unconfigured,
// purging the source marker version emits no replication event, and the data
// scanner cannot see a source version that is gone — the delayed purge watcher
// spawned by the marker replication is the ONLY channel that can remove the
// replicated marker from the target.

const DELAYED_PURGE_KEY: &str = "doc.txt";

fn delayed_purge_process_env() -> Vec<(&'static str, &'static str)> {
    let mut env = replication_fast_env();
    env.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    env.extend_from_slice(&[("NO_PROXY", "127.0.0.1,localhost"), ("HTTP_PROXY", ""), ("HTTPS_PROXY", "")]);
    env
}

async fn start_delayed_purge_fixture(
    source_bucket: &str,
    target_bucket: &str,
) -> Result<(FakeS3Target, RustFSTestEnvironment, Client), Box<dyn Error + Send + Sync>> {
    let target = FakeS3Target::start().await?;
    target.create_bucket(target_bucket);

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], &delayed_purge_process_env())
        .await?;

    let source_client = source_env.create_s3_client();
    source_client.create_bucket().bucket(source_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;

    let target_arn = set_replication_target_with_options(
        &source_env,
        source_bucket,
        ReplicationTargetOptions {
            endpoint: &target.address(),
            access_key: FAKE_ACCESS_KEY,
            secret_key: FAKE_SECRET_KEY,
            target_bucket,
            secure: false,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    Ok((target, source_env, source_client))
}

/// PUT an object, stack a delete marker on it, and wait until the fake target
/// stores the marker replica. Returns the source marker version id — the fake
/// target mirrors it because delete replication forwards
/// `x-*-source-version-id`.
///
/// Timing budget for callers: the delayed purge watcher only observes the
/// source for ~4s after the marker replication completes, so the source-side
/// marker-version DELETE must be issued promptly after this returns (the
/// 100ms journal poll below keeps the detection latency small).
async fn replicate_delete_marker(
    target: &FakeS3Target,
    target_bucket: &str,
    source_client: &Client,
    source_bucket: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    source_client
        .put_object()
        .bucket(source_bucket)
        .key(DELAYED_PURGE_KEY)
        .body(ByteStream::from_static(b"delayed purge payload"))
        .send()
        .await?;

    let delete = source_client
        .delete_object()
        .bucket(source_bucket)
        .key(DELAYED_PURGE_KEY)
        .send()
        .await?;
    assert_eq!(delete.delete_marker(), Some(true), "unversioned DELETE must create a marker");
    let marker_version = delete
        .version_id()
        .ok_or("source DELETE omitted the marker version ID")?
        .to_string();

    // Wait for ANY delete marker: a target that mints its own version ids
    // does not mirror the source one.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let replicated = target
            .stored_versions(target_bucket, DELAYED_PURGE_KEY)
            .iter()
            .any(|(_, delete_marker)| *delete_marker);
        if replicated {
            return Ok(marker_version);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(
                format!("fake target never stored the replicated delete marker; journal: {:?}", target.requests()).into(),
            );
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Journal records of purge attempts: target DELETE calls addressing the marker
/// version explicitly. The marker-creation replica DELETE carries no
/// `versionId` query, so the version id is an exact discriminator.
fn delayed_purge_attempts(target: &FakeS3Target, marker_version: &str) -> Vec<RequestRecord> {
    target
        .requests()
        .into_iter()
        .filter(|record| {
            record.operation == FakeTargetOperation::DeleteObject
                && record.key.as_deref() == Some(DELAYED_PURGE_KEY)
                && record.version_id.as_deref() == Some(marker_version)
        })
        .collect()
}

async fn wait_for_target_marker_purged(
    target: &FakeS3Target,
    target_bucket: &str,
    max_wait: Duration,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        let marker_present = target
            .stored_versions(target_bucket, DELAYED_PURGE_KEY)
            .iter()
            .any(|(_, delete_marker)| *delete_marker);
        if !marker_present {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "target delete marker was never purged; target state: {:?}",
                target.stored_versions(target_bucket, DELAYED_PURGE_KEY)
            )
            .into());
        }
        sleep(Duration::from_millis(200)).await;
    }
}

/// P1-21: the delayed purge's single target DELETE currently swallows failures
/// (`let _ =`), so one transient target error strands the replicated marker on
/// the target forever. Contract under test: a failed purge attempt is retried
/// within the watch window and converges once the fault clears.
#[tokio::test]
async fn test_delayed_delete_marker_purge_retries_after_transient_target_failure() -> TestResult {
    init_logging();
    let source_bucket = "delayed-purge-retry-src";
    let target_bucket = "delayed-purge-retry-dst";
    let (target, _source_env, source_client) = start_delayed_purge_fixture(source_bucket, target_bucket).await?;

    let marker_version = replicate_delete_marker(&target, target_bucket, &source_client, source_bucket).await?;

    // Four scripted failures. Fault budget accounting (each journal record
    // consumes one fault, including the SDK's own per-request retries):
    // deleting the marker version fans out over the version-purge replication
    // channel (initial attempt + its fast in-memory MRF retries) plus the
    // delayed purge watcher's single pre-fix attempt — three target DELETE calls
    // in total today, empirically (see the exhaustion test's journal). Four
    // faults outlast all of them, so only a delayed-purge retry in a later
    // watch round can converge. If the SDK retry configuration ever changes,
    // re-derive this budget from a fresh journal capture.
    target.inject(
        FakeTargetOperation::DeleteObject,
        FakeTargetFault::Status(StatusCode::SERVICE_UNAVAILABLE),
        4,
    );

    // Purge the marker at the source. The watcher spawned when the marker
    // replication completed moments ago observes the source marker vanish
    // within its watch window and drives the target purge.
    source_client
        .delete_object()
        .bucket(source_bucket)
        .key(DELAYED_PURGE_KEY)
        .version_id(&marker_version)
        .send()
        .await?;

    // Tight window on purpose: a fixed delayed purge retries on 1s rounds and
    // converges within ~5s, while any straggling backoff retry from the other
    // channels would land later and must not be what turns this test green.
    wait_for_target_marker_purged(&target, target_bucket, Duration::from_secs(15)).await?;

    let attempts = delayed_purge_attempts(&target, &marker_version);
    assert!(
        attempts.len() >= 2,
        "expected the faulted purge attempt plus at least one retry, got: {attempts:?}"
    );
    assert!(
        attempts.iter().any(|record| record.fault.is_none()),
        "expected a clean purge attempt after the fault script drained, got: {attempts:?}"
    );

    target.shutdown().await;
    Ok(())
}

/// P1-21 review follow-up: the watcher must purge the version the TARGET
/// assigned to the replicated marker, not one derived from the source uuid.
/// A target that mints its own version ids answers a source-derived purge
/// with an idempotent 204, which used to look like success and strand the
/// real marker on the target forever.
#[tokio::test]
async fn test_delayed_delete_marker_purge_uses_target_assigned_version() -> TestResult {
    init_logging();
    let source_bucket = "delayed-purge-mint-src";
    let target_bucket = "delayed-purge-mint-dst";
    let (target, _source_env, source_client) = start_delayed_purge_fixture(source_bucket, target_bucket).await?;
    // The target ignores the forwarded source-version-id header and mints its
    // own ids for both the object and the replicated delete marker.
    target.assign_own_version_ids(true);

    let marker_version = replicate_delete_marker(&target, target_bucket, &source_client, source_bucket).await?;

    source_client
        .delete_object()
        .bucket(source_bucket)
        .key(DELAYED_PURGE_KEY)
        .version_id(&marker_version)
        .send()
        .await?;

    // The replicated marker carries a target-minted version id, so nothing
    // but the recorded mapping can address it.
    wait_for_target_marker_purged(&target, target_bucket, Duration::from_secs(25)).await?;

    target.shutdown().await;
    Ok(())
}

/// P1-21: when every watch-window purge attempt fails, the purge intent must
/// survive as a durable MRF entry and replay on the next startup; once the
/// replayed purge succeeds, the entry must be acknowledged instead of being
/// retained as Missed forever.
#[tokio::test]
async fn test_delayed_delete_marker_purge_exhaustion_persists_to_mrf_and_replays_on_restart() -> TestResult {
    init_logging();
    let source_bucket = "delayed-purge-mrf-src";
    let target_bucket = "delayed-purge-mrf-dst";
    let (target, mut source_env, source_client) = start_delayed_purge_fixture(source_bucket, target_bucket).await?;

    let marker_version = replicate_delete_marker(&target, target_bucket, &source_client, source_bucket).await?;

    // Outlast the whole watch window: every in-process purge attempt fails.
    target.inject(
        FakeTargetOperation::DeleteObject,
        FakeTargetFault::Status(StatusCode::SERVICE_UNAVAILABLE),
        64,
    );

    source_client
        .delete_object()
        .bucket(source_bucket)
        .key(DELAYED_PURGE_KEY)
        .version_id(&marker_version)
        .send()
        .await?;

    // Let the watch window drain before restarting. The wall-clock length is
    // not 5x1s: every faulted attempt embeds the SDK's own per-request 503
    // retries (a few seconds each), so instead of a fixed sleep, wait until
    // the faulted attempts stop arriving (the watcher exhausted its rounds and
    // persisted the purge intent), then give the MRF persister its 100ms
    // flush interval.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut last_seen = delayed_purge_attempts(&target, &marker_version).len();
    let mut quiet_since = tokio::time::Instant::now();
    loop {
        sleep(Duration::from_millis(500)).await;
        let seen = delayed_purge_attempts(&target, &marker_version).len();
        if seen != last_seen {
            last_seen = seen;
            quiet_since = tokio::time::Instant::now();
        }
        if last_seen > 0 && quiet_since.elapsed() >= Duration::from_secs(5) {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("purge attempts never quiesced (saw {last_seen}); journal: {:?}", target.requests()).into());
        }
    }
    sleep(Duration::from_secs(1)).await;

    let marker_survives_faults = target
        .stored_versions(target_bucket, DELAYED_PURGE_KEY)
        .iter()
        .any(|(_, delete_marker)| *delete_marker);
    assert!(marker_survives_faults, "scripted faults must have blocked every in-process purge attempt");

    target.clear_faults();
    let attempts_before_restart = delayed_purge_attempts(&target, &marker_version).len();

    // Startup MRF replay must re-drive the purge and clean the target.
    source_env
        .restart_server_preserving_data(vec![], &delayed_purge_process_env())
        .await?;
    wait_for_target_marker_purged(&target, target_bucket, Duration::from_secs(30)).await?;
    let attempts_after_replay = delayed_purge_attempts(&target, &marker_version).len();
    assert!(
        attempts_after_replay > attempts_before_restart,
        "the restart replay must have issued the purge DELETE"
    );

    // The successful replay must acknowledge the MRF entry: another restart may
    // not re-drive the purge again.
    source_env
        .restart_server_preserving_data(vec![], &delayed_purge_process_env())
        .await?;
    sleep(Duration::from_secs(5)).await;
    assert_eq!(
        delayed_purge_attempts(&target, &marker_version).len(),
        attempts_after_replay,
        "acknowledged purge-intent MRF entries must not replay again"
    );

    target.shutdown().await;

    Ok(())
}

// --- P1-20 (backlog#1675): scanner existing-object compensation matrix ---
//
// Every case below inverts the order used by the rest of this file: objects
// are written FIRST and the replication rule arrives afterwards, so the only
// channel that can move the pre-existing objects is the data scanner's
// existing-object resync pass. Negative cells ("never compensated") are
// contracts and are asserted over multiple scanner cycles, always next to a
// replicated control key that proves the scanner and the live path are
// running — an absent key on a dead scanner proves nothing.

/// Envs + buckets only: versioning, the remote target, and the rule variant
/// are wired by each test (the null-version case must PUT before the source
/// bucket becomes versioned). The source runs with FAST_SCANNER_ENV so
/// existing keys are rescanned within seconds instead of 16 dir cycles.
async fn build_scanner_compensation_pair(
    source_bucket: &str,
    target_bucket: &str,
) -> Result<(RustFSTestEnvironment, RustFSTestEnvironment), Box<dyn Error + Send + Sync>> {
    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut source_process_env = replication_fast_env();
    source_process_env.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    source_process_env.extend_from_slice(FAST_SCANNER_ENV);
    source_env.start_rustfs_server_with_env(vec![], &source_process_env).await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env.start_rustfs_server_without_cleanup(vec![]).await?;

    source_env
        .create_s3_client()
        .create_bucket()
        .bucket(source_bucket)
        .send()
        .await?;
    target_env
        .create_s3_client()
        .create_bucket()
        .bucket(target_bucket)
        .send()
        .await?;

    Ok((source_env, target_env))
}

/// P1-20: objects that already exist when a rule with
/// ExistingObjectReplication=Enabled arrives are compensated by the scanner's
/// existing-object resync pass, whatever wrote them — plain PUT, CopyObject,
/// or Snowball auto-extract. The pinned exception is a null-version object
/// (written before the bucket became versioned): the scanner heal gate skips
/// nil-version objects entirely (`scanner_folder.rs` heal_replication), so it
/// must NEVER be compensated.
#[tokio::test]
async fn test_scanner_compensates_existing_objects_across_write_paths() -> TestResult {
    init_logging();
    let source_bucket = "scanner-comp-src";
    let target_bucket = "scanner-comp-dst";
    let (source_env, target_env) = build_scanner_compensation_pair(source_bucket, target_bucket).await?;
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    // Null-version cell: PUT before versioning; the object keeps the nil
    // version id forever.
    let null_key = "pre-versioning-null.txt";
    source_client
        .put_object()
        .bucket(source_bucket)
        .key(null_key)
        .body(ByteStream::from_static(b"null version payload"))
        .send()
        .await?;

    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    // Pre-existing objects from three write paths, all before any replication
    // config exists (their replication status stays Empty).
    let plain_key = "existing-plain.txt";
    let plain_payload = "existing plain payload";
    source_client
        .put_object()
        .bucket(source_bucket)
        .key(plain_key)
        .body(ByteStream::from_static(plain_payload.as_bytes()))
        .send()
        .await?;

    let copy_key = "existing-copy.txt";
    source_client
        .copy_object()
        .bucket(source_bucket)
        .key(copy_key)
        .copy_source(format!("{source_bucket}/{plain_key}"))
        .send()
        .await?;

    let member_key = "snowball/existing-member.txt";
    let member_payload: &[u8] = b"existing snowball member payload";
    let mut builder = tokio_tar::Builder::new(std::io::Cursor::new(Vec::new()));
    let mut header = tokio_tar::Header::new_gnu();
    header.set_size(member_payload.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder
        .append_data(&mut header, member_key, std::io::Cursor::new(member_payload))
        .await?;
    let archive = builder.into_inner().await?.into_inner();
    source_client
        .put_object()
        .bucket(source_bucket)
        .key("existing-members.tar")
        .metadata("Snowball-Auto-Extract", "true")
        .body(ByteStream::from(archive))
        .send()
        .await?;
    // The extracted member must exist locally before the rule arrives, or it
    // would replicate through the live path instead of the scanner.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if source_client
            .head_object()
            .bucket(source_bucket)
            .key(member_key)
            .send()
            .await
            .is_ok()
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("snowball member was never extracted on the source".into());
        }
        sleep(Duration::from_millis(200)).await;
    }

    // Only now wire the remote target and the Enabled rule.
    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    // Control key written after the rule replicates through the live path.
    let control_key = "control-live.txt";
    let control_payload = "control live payload";
    source_client
        .put_object()
        .bucket(source_bucket)
        .key(control_key)
        .body(ByteStream::from_static(control_payload.as_bytes()))
        .send()
        .await?;
    wait_for_replicated_object(&target_client, target_bucket, control_key, control_payload).await?;

    // Scanner compensation for each pre-existing write path.
    wait_for_replicated_object(&target_client, target_bucket, plain_key, plain_payload).await?;
    wait_for_replicated_object(&target_client, target_bucket, copy_key, plain_payload).await?;
    wait_for_replicated_object(&target_client, target_bucket, member_key, std::str::from_utf8(member_payload)?).await?;

    // Null-version contract: with every sibling compensated (scanner proven
    // live), the nil-version object must stay absent across further cycles.
    assert_replication_key_absent(&target_client, target_bucket, null_key, Duration::from_secs(6)).await?;

    Ok(())
}

/// P1-20: ExistingObjectReplication=Disabled is a contract, not a delay — the
/// scanner must NEVER compensate objects that predate the rule, while objects
/// written after the rule replicate normally (the setting only gates the
/// existing-object resync path).
#[tokio::test]
async fn test_scanner_never_compensates_when_existing_object_replication_disabled() -> TestResult {
    init_logging();
    let source_bucket = "scanner-disabled-src";
    let target_bucket = "scanner-disabled-dst";
    let (source_env, mut target_env) = build_scanner_compensation_pair(source_bucket, target_bucket).await?;
    let source_client = source_env.create_s3_client();
    let target_client = target_env.create_s3_client();

    enable_bucket_versioning(&source_env, source_bucket).await?;
    enable_bucket_versioning(&target_env, target_bucket).await?;

    let existing_key = "existing-disabled.txt";
    source_client
        .put_object()
        .bucket(source_bucket)
        .key(existing_key)
        .body(ByteStream::from_static(b"existing disabled payload"))
        .send()
        .await?;

    let target_arn = set_replication_target(&source_env, source_bucket, &target_env, target_bucket).await?;
    put_bucket_replication_with_statuses(&source_env, source_bucket, &target_arn, "Enabled", None, "Disabled").await?;

    // The live path is unaffected by the Disabled existing-object setting.
    let control_key = "control-live.txt";
    let control_payload = "control live payload";
    source_client
        .put_object()
        .bucket(source_bucket)
        .key(control_key)
        .body(ByteStream::from_static(control_payload.as_bytes()))
        .send()
        .await?;
    wait_for_replicated_object(&target_client, target_bucket, control_key, control_payload).await?;

    // Scanner-only witness. A live-path control key alone would let this test
    // pass while the existing-object scanner is disabled or wedged, so make
    // the scanner itself observable: an object whose replication FAILED while
    // the target was down can only be re-driven by the data scanner's
    // replication heal pass (see FAST_SCANNER_ENV), and that pass is NOT
    // gated by ExistingObjectReplication. The witness lives in the same
    // bucket and prefix as the pre-existing key, so a heal pass that reached
    // it necessarily walked the pre-existing key in the same scan.
    let witness_key = "scanner-witness.txt";
    let witness_payload = "scanner witness payload";
    target_env.stop_server();
    source_client
        .put_object()
        .bucket(source_bucket)
        .key(witness_key)
        .body(ByteStream::from_static(witness_payload.as_bytes()))
        .send()
        .await?;
    wait_for_source_replication_status(&source_client, source_bucket, witness_key, "FAILED", false).await?;
    target_env.restart_server_preserving_data(vec![], &[]).await?;
    let target_client = target_env.create_s3_client();
    wait_for_replicated_object(&target_client, target_bucket, witness_key, witness_payload).await?;

    // The scanner demonstrably swept this bucket; the pre-existing key must
    // still be absent, and stay absent over further cycles.
    assert_replication_key_absent(&target_client, target_bucket, existing_key, Duration::from_secs(6)).await?;

    Ok(())
}

/// Shared setup for the P1-5 read-proxy scenarios (backlog#1675): a RustFS
/// source with an enabled replication rule pointing at the fake target, and
/// an object seeded DIRECTLY on the target — it exists remotely but not
/// locally, exactly the active-active replication-lag window the read proxy
/// serves.
async fn start_read_proxy_lab(
    source_bucket: &str,
    target_bucket: &str,
) -> Result<(FakeS3Target, RustFSTestEnvironment, Client, Client), Box<dyn Error + Send + Sync>> {
    let target = FakeS3Target::start().await?;
    target.create_bucket(target_bucket);
    target.assign_own_version_ids(true);

    let mut source_env = RustFSTestEnvironment::new().await?;
    let mut process_env = replication_fast_env();
    process_env.extend_from_slice(LOOPBACK_REPLICATION_TARGET_ENV);
    process_env.extend_from_slice(&[
        ("NO_PROXY", "127.0.0.1,localhost"),
        ("HTTP_PROXY", ""),
        ("HTTPS_PROXY", ""),
        ("RUST_LOG", "error"),
    ]);
    source_env.start_rustfs_server_with_env(vec![], &process_env).await?;

    let source_client = source_env.create_s3_client();
    source_client.create_bucket().bucket(source_bucket).send().await?;
    enable_bucket_versioning(&source_env, source_bucket).await?;
    let target_arn = set_replication_target_with_options(
        &source_env,
        source_bucket,
        ReplicationTargetOptions {
            endpoint: &target.address(),
            access_key: FAKE_ACCESS_KEY,
            secret_key: FAKE_SECRET_KEY,
            target_bucket,
            secure: false,
            skip_tls_verify: false,
            ca_cert_pem: None,
        },
    )
    .await?;
    put_bucket_replication(&source_env, source_bucket, &target_arn).await?;

    let target_client = Client::from_conf(crate::common::build_test_s3_config(
        target.endpoint(),
        FAKE_ACCESS_KEY,
        FAKE_SECRET_KEY,
        None,
        "read-proxy-e2e",
    ));

    Ok((target, source_env, source_client, target_client))
}

/// P1-5 (backlog#1675): during the active-active replication lag window a
/// GET/HEAD for an object the local site does not have yet is proxied to the
/// replication target. Pins the wire contract: the anti-loop
/// `source-proxy-request` marker is sent, the replication worker's
/// `source-replication-check` SSE-C exemption is NEVER sent, client SSE-C
/// headers are forwarded verbatim, and an inbound request that was itself
/// proxied is answered locally (404) without touching the target.
#[tokio::test]
async fn test_get_and_head_proxy_unreplicated_object_to_replication_target() -> TestResult {
    init_logging();

    let source_bucket = "proxy-read-src";
    let target_bucket = "proxy-read-dst";
    let (target, source_env, source_client, target_client) = start_read_proxy_lab(source_bucket, target_bucket).await?;

    let payload = b"proxy payload".to_vec();
    target_client
        .put_object()
        .bucket(target_bucket)
        .key("proxy-only")
        .body(ByteStream::from(payload.clone()))
        .send()
        .await?;
    target.take_requests();

    // a. GET of the locally-missing object is served through the proxy.
    let got = source_client
        .get_object()
        .bucket(source_bucket)
        .key("proxy-only")
        .send()
        .await
        .map_err(|err| format!("proxied GET failed: {}", err.into_service_error()))?;
    assert_eq!(got.content_length, Some(payload.len() as i64));
    let body = got.body.collect().await?.into_bytes();
    assert_eq!(body.as_ref(), payload.as_slice(), "proxied GET must stream the target's body");

    let get_record = target
        .requests()
        .into_iter()
        .find(|record| record.operation == FakeTargetOperation::GetObject && record.key.as_deref() == Some("proxy-only"))
        .ok_or("fake target never received the proxied GET")?;
    assert_eq!(
        get_record.proxy_headers.source_proxy_request.as_deref(),
        Some("true"),
        "proxied GET must carry the anti-loop source-proxy-request marker"
    );
    assert!(
        get_record.proxy_headers.replication_check.is_none(),
        "proxied GET must never carry the replication worker's source-replication-check exemption"
    );
    assert!(
        get_record.proxy_headers.ssec_algorithm.is_none() && !get_record.proxy_headers.ssec_key_present,
        "no client SSE-C headers were sent, so none may be forwarded"
    );

    // a2. Client SSE-C headers travel verbatim to the target (the target owns
    // the real SSE-C decryption; the plaintext fake simply ignores them).
    target.take_requests();
    let ssec_key = "01234567890123456789012345678901";
    let ssec_key_b64 = BASE64_STANDARD.encode(ssec_key);
    let ssec_key_md5 = sse_customer_key_md5_base64(ssec_key);
    let _ = source_client
        .get_object()
        .bucket(source_bucket)
        .key("proxy-only")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&ssec_key_b64)
        .sse_customer_key_md5(&ssec_key_md5)
        .send()
        .await
        .map_err(|err| format!("proxied SSE-C GET failed: {}", err.into_service_error()))?;
    let ssec_record = target
        .requests()
        .into_iter()
        .find(|record| record.operation == FakeTargetOperation::GetObject && record.key.as_deref() == Some("proxy-only"))
        .ok_or("fake target never received the proxied SSE-C GET")?;
    assert_eq!(ssec_record.proxy_headers.ssec_algorithm.as_deref(), Some("AES256"));
    assert!(ssec_record.proxy_headers.ssec_key_present, "SSE-C key header must be forwarded verbatim");
    assert_eq!(ssec_record.proxy_headers.ssec_key_md5.as_deref(), Some(ssec_key_md5.as_str()));
    assert!(ssec_record.proxy_headers.replication_check.is_none());

    // b. HEAD of the locally-missing object is served through the proxy.
    target.take_requests();
    let head = source_client
        .head_object()
        .bucket(source_bucket)
        .key("proxy-only")
        .send()
        .await
        .map_err(|err| format!("proxied HEAD failed: {}", err.into_service_error()))?;
    assert_eq!(head.content_length, Some(payload.len() as i64));
    let head_record = target
        .requests()
        .into_iter()
        .find(|record| record.operation == FakeTargetOperation::HeadObject && record.key.as_deref() == Some("proxy-only"))
        .ok_or("fake target never received the proxied HEAD")?;
    assert_eq!(head_record.proxy_headers.source_proxy_request.as_deref(), Some("true"));
    assert!(head_record.proxy_headers.replication_check.is_none());

    // c. Anti-loop: an inbound request that already carries the proxy marker
    // is answered locally with 404 and never forwarded to the target.
    target.take_requests();
    let err = source_client
        .get_object()
        .bucket(source_bucket)
        .key("proxy-only")
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("x-minio-source-proxy-request", "true");
        })
        .send()
        .await
        .expect_err("anti-loop GET must fail locally instead of proxying");
    let service_err = err.into_service_error();
    assert!(service_err.is_no_such_key(), "anti-loop GET must 404, got: {service_err}");
    assert!(
        !target
            .requests()
            .iter()
            .any(|record| record.operation == FakeTargetOperation::GetObject),
        "anti-loop GET must not reach the replication target; journal: {:?}",
        target.requests()
    );

    // c2. MinIO ProxyHeaderSet parity: the header's mere PRESENCE disables
    // proxying — "false" is exactly what a peer's replication worker sends on
    // its convergence HEADs, and proxying that miss back would fake
    // convergence.
    target.take_requests();
    let err = source_client
        .get_object()
        .bucket(source_bucket)
        .key("proxy-only")
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("x-minio-source-proxy-request", "false");
        })
        .send()
        .await
        .expect_err("proxy-header-set GET must fail locally instead of proxying");
    let service_err = err.into_service_error();
    assert!(service_err.is_no_such_key(), "proxy-header-set GET must 404, got: {service_err}");
    assert!(
        !target
            .requests()
            .iter()
            .any(|record| record.operation == FakeTargetOperation::GetObject),
        "proxy-header-set GET must not reach the replication target; journal: {:?}",
        target.requests()
    );

    // d. The replication worker's own convergence HEAD against the target
    // must carry `source-proxy-request: false` (never proxied back) and the
    // replication-check exemption. Trigger real replication and inspect the
    // fake journal.
    target.take_requests();
    source_client
        .put_object()
        .bucket(source_bucket)
        .key("worker-replicated")
        .body(ByteStream::from_static(b"worker payload"))
        .send()
        .await?;
    wait_for_target_request_version_id(&target, FakeTargetOperation::PutObject, "worker-replicated").await?;
    let worker_head = target
        .requests()
        .into_iter()
        .find(|record| record.operation == FakeTargetOperation::HeadObject && record.key.as_deref() == Some("worker-replicated"))
        .ok_or_else(|| format!("replication worker never HEAD-ed the target; journal: {:?}", target.requests()))?;
    assert_eq!(
        worker_head.proxy_headers.source_proxy_request.as_deref(),
        Some("false"),
        "worker convergence HEAD must send source-proxy-request: false so the target answers locally"
    );
    assert_eq!(
        worker_head.proxy_headers.replication_check.as_deref(),
        Some("true"),
        "worker convergence HEAD keeps the replication-check exemption"
    );

    drop(source_env);
    target.shutdown().await;
    Ok(())
}

/// P1-5 (backlog#1675): GetObjectTagging for an object missing locally is
/// proxied to the replication target with the anti-loop marker, mirroring
/// MinIO `proxyGetTaggingToRepTarget`.
#[tokio::test]
async fn test_get_object_tagging_proxies_unreplicated_object_to_replication_target() -> TestResult {
    init_logging();

    let source_bucket = "proxy-tag-src";
    let target_bucket = "proxy-tag-dst";
    let (target, source_env, source_client, target_client) = start_read_proxy_lab(source_bucket, target_bucket).await?;

    target_client
        .put_object()
        .bucket(target_bucket)
        .key("proxy-tagged")
        .body(ByteStream::from_static(b"tagged payload"))
        .send()
        .await?;
    target_client
        .put_object_tagging()
        .bucket(target_bucket)
        .key("proxy-tagged")
        .tagging(
            aws_sdk_s3::types::Tagging::builder()
                .tag_set(aws_sdk_s3::types::Tag::builder().key("team").value("storage").build()?)
                .build()?,
        )
        .send()
        .await?;
    target.take_requests();

    let tags = source_client
        .get_object_tagging()
        .bucket(source_bucket)
        .key("proxy-tagged")
        .send()
        .await
        .map_err(|err| format!("proxied GetObjectTagging failed: {}", err.into_service_error()))?;
    assert_eq!(tags.tag_set.len(), 1, "proxied tagging read must return the target's tags");
    assert_eq!(tags.tag_set[0].key.as_str(), "team");
    assert_eq!(tags.tag_set[0].value.as_str(), "storage");

    let record = target
        .requests()
        .into_iter()
        .find(|record| record.operation == FakeTargetOperation::GetObjectTagging && record.key.as_deref() == Some("proxy-tagged"))
        .ok_or("fake target never received the proxied GetObjectTagging")?;
    assert_eq!(
        record.proxy_headers.source_proxy_request.as_deref(),
        Some("true"),
        "proxied tagging read must carry the anti-loop marker"
    );
    assert!(record.proxy_headers.replication_check.is_none());

    drop(source_env);
    target.shutdown().await;
    Ok(())
}
