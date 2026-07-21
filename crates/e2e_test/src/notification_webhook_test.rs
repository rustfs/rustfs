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

//! End-to-end regression net for the S3 event-notification pipeline
//! (backlog#1154 peri-1). Proves the full "configure webhook target ->
//! PutBucketNotificationConfiguration -> object operation -> event delivered"
//! chain against a real rustfs binary and a real HTTP receiver, which no other
//! test covers: the target-plugin suites stop at broker integration and the
//! object-lambda suite only exercises the webhook target as a transform
//! function, never as an S3 event sink.
//!
//! Coverage:
//!   * PUT / multipart-complete / DELETE each deliver one event with the correct
//!     eventName, bucket, key, versionId and eTag.
//!   * prefix/suffix filters drop non-matching keys (rule-engine gate).
//!   * an event queued while the target endpoint is unreachable is redelivered
//!     from the on-disk store once the endpoint recovers (store-and-forward).

use crate::common::{RustFSTestEnvironment, init_logging};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Event, FilterRule, FilterRuleName,
    NotificationConfiguration, NotificationConfigurationFilter, QueueConfiguration, S3KeyFilter, VersioningConfiguration,
};
use http::header::{CONTENT_TYPE, HOST};
use local_ip_address::local_ip;
use reqwest::StatusCode;
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use serde_json::Value;
use serial_test::serial;
use std::error::Error;
use std::path::Path;
use std::sync::{
    Arc, Once,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, timeout};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;
type BoxError = Box<dyn Error + Send + Sync>;

/// Region embedded in the target ARN. Only the `id:name` tail of the ARN is used
/// for target lookup (see `process_queue_configurations`), so the region is
/// nominal, but it must be present for `ARN::parse` to succeed.
const NOTIFY_REGION: &str = "us-east-1";

/// Webhook targets are registered as `TargetID { id: <name>, name: "webhook" }`,
/// so the ARN a notification rule references is
/// `arn:rustfs:sqs:<region>:<name>:webhook`.
fn target_arn(target_name: &str) -> String {
    format!("arn:rustfs:sqs:{NOTIFY_REGION}:{target_name}:webhook")
}

// ---------------------------------------------------------------------------
// In-test HTTP event receiver
// ---------------------------------------------------------------------------

/// Reads one HTTP request off the stream, returning its method and body. Handles
/// the target's HEAD reachability probe (no body) and POST event deliveries.
async fn read_http_message(stream: &mut tokio::net::TcpStream) -> Result<(String, Vec<u8>), BoxError> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];

    let header_end = loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err("connection closed before request headers were complete".into());
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(pos) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
    };

    let header_text = std::str::from_utf8(&buffer[..header_end])?;
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().ok_or("missing request line")?;
    let method = request_line.split_whitespace().next().ok_or("missing method")?.to_string();

    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("content-length")
        {
            content_length = value.trim().parse::<usize>().unwrap_or(0);
        }
    }

    let body_offset = header_end + 4;
    while buffer.len().saturating_sub(body_offset) < content_length {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err("connection closed before request body was complete".into());
        }
        buffer.extend_from_slice(&chunk[..read]);
    }

    Ok((method, buffer[body_offset..body_offset + content_length].to_vec()))
}

/// Drives an accepted listener: answers every request 200 OK (so the target's
/// HEAD reachability probe reports it online) and forwards each non-empty POST
/// body, parsed as the S3 event envelope JSON, to `tx`.
fn serve_event_collector(listener: TcpListener, tx: mpsc::UnboundedSender<Value>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Ok(Ok((method, body))) = timeout(Duration::from_secs(5), read_http_message(&mut stream)).await {
                    let _ = stream
                        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                        .await;
                    let _ = stream.shutdown().await;
                    if method == "POST"
                        && !body.is_empty()
                        && let Ok(value) = serde_json::from_slice::<Value>(&body)
                    {
                        let _ = tx.send(value);
                    }
                }
            });
        }
    })
}

/// Binds a fresh collector on a random port. Returns its `/events`
/// endpoint URL, a receiver of parsed event envelopes, and the serving task.
async fn spawn_event_collector() -> Result<(String, mpsc::UnboundedReceiver<Value>, JoinHandle<()>), BoxError> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let port = listener.local_addr()?.port();
    let endpoint_ip = local_ip()?;
    let (tx, rx) = mpsc::unbounded_channel();
    let handle = serve_event_collector(listener, tx);
    Ok((format!("http://{endpoint_ip}.nip.io:{port}/events"), rx, handle))
}

struct HttpsEventCollector {
    endpoint: String,
    running: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    events: mpsc::UnboundedReceiver<Value>,
}

impl HttpsEventCollector {
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn events_mut(&mut self) -> &mut mpsc::UnboundedReceiver<Value> {
        &mut self.events
    }

    fn shutdown(&mut self) -> TestResult {
        self.running.store(false, Ordering::Relaxed);
        if let Ok(parsed) = self.endpoint.parse::<reqwest::Url>()
            && let Some(port) = parsed.port()
        {
            let _ = std::net::TcpStream::connect(("127.0.0.1", port));
        }
        if let Some(handle) = self.handle.take() {
            handle.join().map_err(|_| "https event collector thread panicked")?;
        }
        Ok(())
    }
}

impl Drop for HttpsEventCollector {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn spawn_https_event_collector(ca_path: &Path) -> Result<HttpsEventCollector, BoxError> {
    use rustls::{
        ServerConfig,
        pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
    };
    use std::io::ErrorKind;
    use std::net::TcpListener as StdTcpListener;

    static INSTALL_CRYPTO_PROVIDER: Once = Once::new();
    INSTALL_CRYPTO_PROVIDER.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });

    let listener = StdTcpListener::bind("0.0.0.0:0")?;
    listener.set_nonblocking(true)?;
    let addr = listener.local_addr()?;
    let endpoint_ip = local_ip()?;
    let endpoint_host = format!("{endpoint_ip}.nip.io");

    let rcgen::CertifiedKey { cert, signing_key } = rcgen::generate_simple_self_signed(vec![endpoint_host.clone()])?;
    std::fs::write(ca_path, cert.pem())?;

    let cert_chain = vec![cert.der().clone()];
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    let server_config = Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key_der)?,
    );

    let running = Arc::new(AtomicBool::new(true));
    let server_running = Arc::clone(&running);
    let (tx, events) = mpsc::unbounded_channel();
    let handle = thread::spawn(move || {
        let mut connections = Vec::new();
        while server_running.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let config = Arc::clone(&server_config);
                    let tx = tx.clone();
                    connections.push(thread::spawn(move || {
                        let _ = handle_https_request(stream, config, tx);
                    }));
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(20)),
                Err(_) => break,
            }
        }
        for connection in connections {
            let _ = connection.join();
        }
    });

    Ok(HttpsEventCollector {
        endpoint: format!("https://{endpoint_host}:{}/events", addr.port()),
        running,
        handle: Some(handle),
        events,
    })
}

fn read_sync_http_message<R: std::io::Read>(stream: &mut R) -> Result<(String, Vec<u8>), BoxError> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err("connection closed before request headers were complete".into());
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(pos) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break pos;
        }
    };

    let header_text = std::str::from_utf8(&buffer[..header_end])?;
    let mut lines = header_text.split("\r\n");
    let method = lines
        .next()
        .and_then(|line| line.split_whitespace().next())
        .ok_or("missing request method")?
        .to_string();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("content-length")
        {
            content_length = value.trim().parse()?;
        }
    }

    let body_offset = header_end + 4;
    while buffer.len().saturating_sub(body_offset) < content_length {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err("connection closed before request body was complete".into());
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    Ok((method, buffer[body_offset..body_offset + content_length].to_vec()))
}

fn handle_https_request(
    stream: std::net::TcpStream,
    server_config: Arc<rustls::ServerConfig>,
    tx: mpsc::UnboundedSender<Value>,
) -> Result<(), BoxError> {
    use std::io::Write;

    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let connection = rustls::ServerConnection::new(server_config)?;
    let mut tls_stream = rustls::StreamOwned::new(connection, stream);
    let (method, body) = read_sync_http_message(&mut tls_stream)?;
    let response = "HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
    tls_stream.write_all(response.as_bytes())?;
    tls_stream.flush()?;
    tls_stream.conn.send_close_notify();
    while tls_stream.conn.wants_write() {
        tls_stream.conn.write_tls(&mut tls_stream.sock)?;
    }
    let _ = tls_stream.sock.shutdown(std::net::Shutdown::Write);
    if method == "POST"
        && !body.is_empty()
        && let Ok(event) = serde_json::from_slice(&body)
    {
        let _ = tx.send(event);
    }
    Ok(())
}

/// Decoded object key of the first record in an event envelope.
fn event_key(envelope: &Value) -> Option<String> {
    let raw = envelope["Records"][0]["s3"]["object"]["key"].as_str()?;
    Some(
        urlencoding::decode(raw)
            .map(|d| d.into_owned())
            .unwrap_or_else(|_| raw.to_string()),
    )
}

/// Waits until an event targeting `key` whose `EventName` starts with
/// `event_prefix` arrives, returning the full envelope. Other envelopes are
/// discarded, so a lingering duplicate of an earlier event (delivery is
/// at-least-once) or the create event of a key that is later deleted never
/// satisfies a wait for the opposite event type.
async fn wait_for_event(
    rx: &mut mpsc::UnboundedReceiver<Value>,
    key: &str,
    event_prefix: &str,
    within: Duration,
) -> Result<Value, BoxError> {
    let deadline = Instant::now() + within;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(format!("no {event_prefix}* event for key {key} within {within:?}").into());
        }
        match timeout(remaining, rx.recv()).await {
            Ok(Some(envelope)) => {
                let key_matches = event_key(&envelope).as_deref() == Some(key);
                let name_matches = envelope["EventName"].as_str().is_some_and(|n| n.starts_with(event_prefix));
                if key_matches && name_matches {
                    return Ok(envelope);
                }
            }
            Ok(None) => return Err("event collector channel closed".into()),
            Err(_) => return Err(format!("no {event_prefix}* event for key {key} within {within:?}").into()),
        }
    }
}

/// Collects every event until `stop_key` is seen (plus a short grace window to
/// catch stragglers), or until `max_wait` elapses. Used to prove a negative:
/// non-matching keys must never appear even though a matching sentinel does.
async fn collect_until(
    rx: &mut mpsc::UnboundedReceiver<Value>,
    stop_key: &str,
    max_wait: Duration,
    grace: Duration,
) -> Vec<Value> {
    let hard_deadline = Instant::now() + max_wait;
    let mut soft_deadline: Option<Instant> = None;
    let mut collected = Vec::new();
    loop {
        let deadline = soft_deadline.unwrap_or(hard_deadline).min(hard_deadline);
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, rx.recv()).await {
            Ok(Some(envelope)) => {
                let matched = event_key(&envelope).as_deref() == Some(stop_key);
                collected.push(envelope);
                if matched && soft_deadline.is_none() {
                    soft_deadline = Some(Instant::now() + grace);
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    collected
}

// ---------------------------------------------------------------------------
// Admin target configuration (signed admin HTTP)
// ---------------------------------------------------------------------------

async fn signed_admin_request(
    env: &RustFSTestEnvironment,
    method: http::Method,
    url: &str,
    body: Option<Vec<u8>>,
) -> Result<reqwest::Response, BoxError> {
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("admin URL missing authority")?.to_string();
    let mut builder = http::Request::builder()
        .method(method.clone())
        .uri(uri)
        .header(HOST, authority)
        .header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
    if body.is_some() {
        builder = builder.header(CONTENT_TYPE, "application/json");
    }

    let content_len = body.as_ref().map(|b| b.len() as i64).unwrap_or_default();
    let signed = sign_v4(
        builder.body(Body::empty())?,
        content_len,
        &env.access_key,
        &env.secret_key,
        "",
        "us-east-1",
    );

    let reqwest_method = reqwest::Method::from_bytes(method.as_str().as_bytes())?;
    let mut request = crate::common::local_http_client().request(reqwest_method, url);
    for (name, value) in signed.headers() {
        request = request.header(name, value);
    }
    if let Some(body) = body {
        request = request.body(body);
    }
    Ok(request.send().await?)
}

async fn enable_notify_module(env: &RustFSTestEnvironment) -> TestResult {
    let payload = serde_json::json!({
        "notify_enabled": true,
        "audit_enabled": false,
    });
    let url = format!("{}/rustfs/admin/v3/module-switches", env.url);
    let response = signed_admin_request(env, http::Method::PUT, &url, Some(payload.to_string().into_bytes())).await?;
    if response.status() != StatusCode::OK {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(format!("failed to enable notify module: {status} {text}").into());
    }
    Ok(())
}

/// Registers a webhook notification target with a persistent queue directory, so
/// delivery goes through the durable store-and-forward path.
async fn configure_webhook_target(env: &RustFSTestEnvironment, target_name: &str, endpoint: &str) -> TestResult {
    configure_webhook_target_with_key_values(env, target_name, vec![("endpoint", endpoint.to_string())]).await
}

async fn configure_webhook_target_with_key_values(
    env: &RustFSTestEnvironment,
    target_name: &str,
    mut key_values: Vec<(&str, String)>,
) -> TestResult {
    let queue_dir = format!("{}/notify-queue-{target_name}", env.temp_dir);
    tokio::fs::create_dir_all(&queue_dir).await?;
    if !key_values.iter().any(|(key, _)| *key == "queue_dir") {
        key_values.push(("queue_dir", queue_dir));
    }
    let payload = serde_json::json!({
        "key_values": key_values
            .into_iter()
            .map(|(key, value)| serde_json::json!({ "key": key, "value": value }))
            .collect::<Vec<_>>(),
    });
    let url = format!("{}/rustfs/admin/v3/target/notify_webhook/{target_name}", env.url);
    let response = signed_admin_request(env, http::Method::PUT, &url, Some(payload.to_string().into_bytes())).await?;
    let status = response.status();
    if status != StatusCode::OK {
        let text = response.text().await.unwrap_or_default();
        return Err(format!("failed to configure webhook target {target_name}: {status} {text}").into());
    }
    Ok(())
}

/// Polls the admin target ARN list until the freshly configured target is
/// registered in the runtime, so notification rules and object events do not
/// race target activation.
async fn wait_for_target_registered(env: &RustFSTestEnvironment, target_name: &str) -> TestResult {
    let suffix = format!(":{target_name}:webhook");
    let url = format!("{}/rustfs/admin/v3/target/arns", env.url);
    for _ in 0..80 {
        let response = signed_admin_request(env, http::Method::GET, &url, None).await?;
        if response.status() == StatusCode::OK {
            let arns: Vec<String> = serde_json::from_slice(&response.bytes().await?)?;
            if arns.iter().any(|arn| arn.ends_with(&suffix)) {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("target {target_name} was not registered in admin ARNs").into())
}

async fn wait_for_target_online(env: &RustFSTestEnvironment, target_name: &str) -> TestResult {
    let url = format!("{}/rustfs/admin/v3/target/list", env.url);
    for _ in 0..40 {
        let response = signed_admin_request(env, http::Method::GET, &url, None).await?;
        if response.status() == StatusCode::OK {
            let body: Value = serde_json::from_slice(&response.bytes().await?)?;
            if body["notify_enabled"].as_bool() != Some(true) {
                return Err(format!("admin target list did not report notify_enabled=true: {body}").into());
            }
            let listed = body["notification_endpoints"].as_array().is_some_and(|endpoints| {
                endpoints.iter().any(|endpoint| {
                    endpoint["account_id"].as_str() == Some(target_name)
                        && endpoint["service"].as_str() == Some("webhook")
                        && endpoint["status"].as_str() == Some("online")
                })
            });
            if listed {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("target {target_name} did not become online in admin targets").into())
}

/// Binds a bucket to a webhook target for ObjectCreated:*/ObjectRemoved:* events,
/// filtered to `prefix` + `suffix`.
async fn put_notification_config(client: &Client, bucket: &str, target_name: &str, prefix: &str, suffix: &str) -> TestResult {
    let key_filter = S3KeyFilter::builder()
        .filter_rules(FilterRule::builder().name(FilterRuleName::Prefix).value(prefix).build())
        .filter_rules(FilterRule::builder().name(FilterRuleName::Suffix).value(suffix).build())
        .build();
    let queue = QueueConfiguration::builder()
        .id(format!("{target_name}-rule"))
        .queue_arn(target_arn(target_name))
        .events(Event::from("s3:ObjectCreated:*"))
        .events(Event::from("s3:ObjectRemoved:*"))
        .filter(NotificationConfigurationFilter::builder().key(key_filter).build())
        .build()?;
    let config = NotificationConfiguration::builder().queue_configurations(queue).build();
    client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(config)
        .send()
        .await?;
    Ok(())
}

async fn enable_versioning(client: &Client, bucket: &str) -> TestResult {
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await?;
    Ok(())
}

fn trimmed_etag(value: Option<&str>) -> Option<String> {
    value.map(|e| e.trim_matches('"').to_string())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Regression for rustfs#5052: with the notify module enabled through
/// RUSTFS_NOTIFY_ENABLE, an HTTPS webhook using a configured CA must become
/// online and receive a real S3 event POST.
#[tokio::test]
#[serial]
async fn test_https_webhook_target_delivers_event_with_notify_env_enabled() -> TestResult {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], &[("RUSTFS_NOTIFY_ENABLE", "true")])
        .await?;

    let ca_path = Path::new(&env.temp_dir).join("https-webhook-ca.pem");
    let mut collector = spawn_https_event_collector(&ca_path)?;
    let target = "peri1https";
    let bucket = "peri1-https-events";
    let key = "uploads/https.dat";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;

    configure_webhook_target_with_key_values(
        &env,
        target,
        vec![
            ("endpoint", collector.endpoint().to_string()),
            ("client_ca", ca_path.to_string_lossy().into_owned()),
        ],
    )
    .await?;
    wait_for_target_online(&env, target).await?;
    wait_for_target_registered(&env, target).await?;
    put_notification_config(&client, bucket, target, "uploads/", ".dat").await?;

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"https webhook event body"))
        .send()
        .await?;
    let event = wait_for_event(collector.events_mut(), key, "s3:ObjectCreated:", Duration::from_secs(20)).await?;
    assert_eq!(event["EventName"].as_str(), Some("s3:ObjectCreated:Put"));
    assert_eq!(event["Records"][0]["s3"]["bucket"]["name"].as_str(), Some(bucket));
    assert_eq!(event_key(&event).as_deref(), Some(key));

    env.stop_server();
    collector.shutdown()?;
    Ok(())
}

/// PUT / multipart-complete / DELETE each deliver one event with correct fields,
/// and the prefix/suffix filter drops non-matching keys.
#[tokio::test]
#[serial]
async fn test_webhook_event_delivery_and_filtering() -> TestResult {
    init_logging();

    let (endpoint, mut rx, handle) = spawn_event_collector().await?;

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    enable_notify_module(&env).await?;

    let bucket = "peri1-events";
    let target = "peri1events";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    enable_versioning(&client, bucket).await?;

    configure_webhook_target(&env, target, &endpoint).await?;
    wait_for_target_registered(&env, target).await?;
    put_notification_config(&client, bucket, target, "uploads/", ".dat").await?;

    // --- PUT: ObjectCreated:Put with exact eTag + versionId ------------------
    let put_key = "uploads/report.dat";
    let put = client
        .put_object()
        .bucket(bucket)
        .key(put_key)
        .body(ByteStream::from_static(b"peri-1 put body"))
        .send()
        .await?;
    let put_version = put
        .version_id()
        .ok_or("PUT response missing versionId (versioning not enabled?)")?;

    let created = wait_for_event(&mut rx, put_key, "s3:ObjectCreated:", Duration::from_secs(20)).await?;
    let record = &created["Records"][0];
    let object = &record["s3"]["object"];
    assert_eq!(
        created["EventName"].as_str(),
        Some("s3:ObjectCreated:Put"),
        "envelope EventName: {created}"
    );
    assert!(
        record["eventName"]
            .as_str()
            .is_some_and(|n| n.starts_with("s3:ObjectCreated:")),
        "record eventName: {record}"
    );
    assert_eq!(record["s3"]["bucket"]["name"].as_str(), Some(bucket), "bucket in event: {record}");
    assert_eq!(object["versionId"].as_str(), Some(put_version), "versionId in event: {object}");
    assert_eq!(
        trimmed_etag(object["eTag"].as_str()),
        trimmed_etag(put.e_tag()),
        "eTag in event: {object}"
    );

    // --- Multipart complete: ObjectCreated:CompleteMultipartUpload -----------
    let mp_key = "uploads/multi.dat";
    let created_mp = client.create_multipart_upload().bucket(bucket).key(mp_key).send().await?;
    let upload_id = created_mp.upload_id().ok_or("missing upload id")?;
    let part = client
        .upload_part()
        .bucket(bucket)
        .key(mp_key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"peri-1 multipart part body"))
        .send()
        .await?;
    let complete = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(mp_key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().unwrap_or_default())
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;

    let mp_event = wait_for_event(&mut rx, mp_key, "s3:ObjectCreated:", Duration::from_secs(20)).await?;
    let mp_record = &mp_event["Records"][0];
    assert_eq!(
        mp_event["EventName"].as_str(),
        Some("s3:ObjectCreated:CompleteMultipartUpload"),
        "multipart EventName: {mp_event}"
    );
    assert_eq!(mp_record["s3"]["bucket"]["name"].as_str(), Some(bucket));
    assert_eq!(
        trimmed_etag(mp_record["s3"]["object"]["eTag"].as_str()),
        trimmed_etag(complete.e_tag()),
        "multipart eTag in event: {mp_record}"
    );

    // --- Filter: non-matching prefix and suffix must never be delivered ------
    let wrong_prefix = "logs/report.dat"; // right suffix, wrong prefix
    let wrong_suffix = "uploads/report.txt"; // right prefix, wrong suffix
    let sentinel = "uploads/keep.dat";
    for key in [wrong_prefix, wrong_suffix, sentinel] {
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"filter probe"))
            .send()
            .await?;
    }
    let collected = collect_until(&mut rx, sentinel, Duration::from_secs(20), Duration::from_secs(2)).await;
    let seen: Vec<String> = collected.iter().filter_map(event_key).collect();
    assert!(
        seen.iter().any(|k| k == sentinel),
        "matching sentinel {sentinel} was not delivered; saw {seen:?}"
    );
    assert!(
        !seen.iter().any(|k| k == wrong_prefix),
        "wrong-prefix key {wrong_prefix} bypassed the filter; saw {seen:?}"
    );
    assert!(
        !seen.iter().any(|k| k == wrong_suffix),
        "wrong-suffix key {wrong_suffix} bypassed the filter; saw {seen:?}"
    );

    // --- DELETE on a versioned bucket: ObjectRemoved:* with delete-marker version
    let delete = client.delete_object().bucket(bucket).key(put_key).send().await?;
    let removed = wait_for_event(&mut rx, put_key, "s3:ObjectRemoved:", Duration::from_secs(20)).await?;
    let removed_record = &removed["Records"][0];
    assert!(
        removed["EventName"]
            .as_str()
            .is_some_and(|n| n.starts_with("s3:ObjectRemoved:")),
        "delete EventName: {removed}"
    );
    if let Some(marker_version) = delete.version_id() {
        assert_eq!(
            removed_record["s3"]["object"]["versionId"].as_str(),
            Some(marker_version),
            "delete-marker versionId in event: {removed_record}"
        );
    }

    env.stop_server();
    handle.abort();
    Ok(())
}

/// An event queued while the target endpoint is unreachable survives on the
/// durable store and is redelivered once the endpoint comes back.
#[tokio::test]
#[serial]
#[ignore = "FAILING deterministically on main since it landed (#4821): the target is created but never appears in /rustfs/admin/v3/target/arns, so wait_for_target_registered times out. Quarantined per the flake policy; remove with the fix for rustfs#4852"]
async fn test_webhook_redelivers_event_after_target_recovers() -> TestResult {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;
    enable_notify_module(&env).await?;

    let bucket = "peri1-redeliver";
    let target = "peri1redeliver";
    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;

    // Configure the target while its endpoint is reachable so activation and
    // ARN registration complete deterministically. Registering against a dead
    // endpoint stalls behind the reachability probe's timeout and flakes the
    // registration wait on loaded runners (observed on the merge run of
    // rustfs#4821).
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let port = listener.local_addr()?.port();
    let endpoint_ip = local_ip()?;
    let endpoint = format!("http://{endpoint_ip}.nip.io:{port}/events");
    let (setup_tx, _setup_rx) = mpsc::unbounded_channel();
    let setup_handle = serve_event_collector(listener, setup_tx);

    configure_webhook_target(&env, target, &endpoint).await?;
    wait_for_target_registered(&env, target).await?;
    put_notification_config(&client, bucket, target, "uploads/", ".dat").await?;

    // Take the endpoint down (drops the listener, so connections are refused —
    // a retryable NotConnected), then PUT: the event cannot be delivered and
    // must survive on the durable queue store.
    setup_handle.abort();
    let _ = setup_handle.await;

    let key = "uploads/redeliver.dat";
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"queued while target down"))
        .send()
        .await?;

    // Hold the endpoint down long enough for at least one replay attempt to
    // fail (the replay worker scans the store every 500ms), so recovery below
    // exercises real redelivery rather than a first-attempt success.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Bring the endpoint back on the same port; the replay worker retries with
    // exponential backoff and delivers the queued event.
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let handle = serve_event_collector(listener, tx);

    let redelivered = wait_for_event(&mut rx, key, "s3:ObjectCreated:", Duration::from_secs(45)).await?;
    assert_eq!(
        redelivered["EventName"].as_str(),
        Some("s3:ObjectCreated:Put"),
        "redelivered EventName: {redelivered}"
    );
    assert_eq!(redelivered["Records"][0]["s3"]["bucket"]["name"].as_str(), Some(bucket));

    env.stop_server();
    handle.abort();
    Ok(())
}
