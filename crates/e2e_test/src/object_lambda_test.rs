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

use crate::common::{RustFSTestClusterEnvironment, RustFSTestEnvironment, init_logging, local_http_client};
use aws_sdk_s3::primitives::ByteStream;
use http::header::{CONTENT_TYPE, HOST};
use reqwest::StatusCode;
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::{pre_sign_v4, sign_v4};
use s3s::Body;
use serial_test::serial;
use std::collections::HashMap;
use std::error::Error;
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, timeout};

#[derive(Debug)]
struct CapturedWebhookRequest {
    headers: HashMap<String, String>,
    payload: serde_json::Value,
}

struct WebhookResponseSpec {
    status_line: String,
    body: Vec<u8>,
    headers: Vec<(String, String)>,
    include_auth_headers: bool,
    auth_route_override: Option<String>,
    auth_token_override: Option<String>,
}

fn find_header_terminator(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}

async fn read_http_request(
    stream: &mut tokio::net::TcpStream,
) -> Result<(HashMap<String, String>, Vec<u8>), Box<dyn Error + Send + Sync>> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];

    let header_end = loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err("webhook request ended before headers were fully received".into());
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(pos) = find_header_terminator(&buffer) {
            break pos;
        }
    };

    let header_bytes = &buffer[..header_end];
    let header_text = std::str::from_utf8(header_bytes)?;
    let mut lines = header_text.split("\r\n");
    let _request_line = lines.next().ok_or("missing request line")?;
    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line.split_once(':').ok_or("invalid header line")?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    let content_length = headers
        .get("content-length")
        .ok_or("missing content-length header")?
        .parse::<usize>()?;
    let body_offset = header_end + 4;
    while buffer.len().saturating_sub(body_offset) < content_length {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err("webhook request ended before body was fully received".into());
        }
        buffer.extend_from_slice(&chunk[..read]);
    }

    Ok((headers, buffer[body_offset..body_offset + content_length].to_vec()))
}

async fn spawn_object_lambda_webhook_server() -> Result<
    (
        String,
        oneshot::Receiver<CapturedWebhookRequest>,
        tokio::task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>,
    ),
    Box<dyn Error + Send + Sync>,
> {
    spawn_object_lambda_webhook_server_with_response(WebhookResponseSpec {
        status_line: "200 OK".to_string(),
        body: b"transformed through object lambda".to_vec(),
        headers: vec![("content-type".to_string(), "text/plain".to_string())],
        include_auth_headers: true,
        auth_route_override: None,
        auth_token_override: None,
    })
    .await
}

async fn spawn_object_lambda_webhook_server_with_response(
    response_spec: WebhookResponseSpec,
) -> Result<
    (
        String,
        oneshot::Receiver<CapturedWebhookRequest>,
        tokio::task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>,
    ),
    Box<dyn Error + Send + Sync>,
> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let webhook_url = format!("http://{address}/transform");
    let (request_tx, request_rx) = oneshot::channel();

    let handle = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await?;
            let Ok(Ok((headers, body))) = timeout(Duration::from_secs(2), read_http_request(&mut stream)).await else {
                continue;
            };
            let payload: serde_json::Value = serde_json::from_slice(&body)?;

            let output_route = payload["getObjectContext"]["outputRoute"]
                .as_str()
                .ok_or("missing outputRoute in webhook payload")?
                .to_string();
            let output_token = payload["getObjectContext"]["outputToken"]
                .as_str()
                .ok_or("missing outputToken in webhook payload")?
                .to_string();

            let _ = request_tx.send(CapturedWebhookRequest { headers, payload });

            let mut response_head = format!(
                "HTTP/1.1 {}\r\ncontent-length: {}\r\nconnection: close\r\n",
                response_spec.status_line,
                response_spec.body.len()
            );
            for (name, value) in &response_spec.headers {
                response_head.push_str(&format!("{name}: {value}\r\n"));
            }
            if response_spec.include_auth_headers {
                let auth_route = response_spec.auth_route_override.as_deref().unwrap_or(&output_route);
                let auth_token = response_spec.auth_token_override.as_deref().unwrap_or(&output_token);
                response_head.push_str(&format!("x-amz-request-route: {auth_route}\r\n"));
                response_head.push_str(&format!("x-amz-request-token: {auth_token}\r\n"));
            }
            response_head.push_str("\r\n");
            stream.write_all(response_head.as_bytes()).await?;
            stream.write_all(&response_spec.body).await?;
            stream.shutdown().await?;

            return Ok(());
        }
    });

    Ok((webhook_url, request_rx, handle))
}

async fn read_request_path(stream: &mut tokio::net::TcpStream) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];

    let header_end = loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err("request ended before headers were fully received".into());
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(pos) = find_header_terminator(&buffer) {
            break pos;
        }
    };

    let header_text = std::str::from_utf8(&buffer[..header_end])?;
    let request_line = header_text.lines().next().ok_or("missing request line")?;
    let path = request_line.split_whitespace().nth(1).ok_or("missing request path")?;

    Ok(path.to_string())
}

async fn presigned_get_request(
    url: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
    let signed = pre_sign_v4(
        http::Request::builder()
            .method(http::Method::GET)
            .uri(uri)
            .header(HOST, authority)
            .body(Body::empty())?,
        access_key,
        secret_key,
        "",
        "us-east-1",
        600,
        OffsetDateTime::now_utc(),
    );

    Ok(local_http_client().get(signed.uri().to_string()).send().await?)
}

async fn signed_request(
    method: http::Method,
    url: &str,
    access_key: &str,
    secret_key: &str,
    body: Option<Vec<u8>>,
    content_type: Option<&str>,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
    let mut request = http::Request::builder().method(method.clone()).uri(uri);
    request = request.header(HOST, authority);
    request = request.header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
    if let Some(content_type) = content_type {
        request = request.header(CONTENT_TYPE, content_type);
    }

    let content_len = body.as_ref().map(|body| body.len() as i64).unwrap_or_default();
    let signed = sign_v4(request.body(Body::empty())?, content_len, access_key, secret_key, "", "us-east-1");

    let reqwest_method = reqwest::Method::from_bytes(method.as_str().as_bytes())?;
    let client = local_http_client();
    let mut request_builder = client.request(reqwest_method, url);
    for (name, value) in signed.headers() {
        request_builder = request_builder.header(name, value);
    }
    if let Some(body) = body {
        request_builder = request_builder.body(body);
    }

    Ok(request_builder.send().await?)
}

async fn configure_webhook_target(
    env: &RustFSTestEnvironment,
    target_name: &str,
    endpoint: &str,
    auth_token: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    configure_webhook_target_with_key_values(
        env,
        target_name,
        vec![
            ("endpoint", endpoint.to_string()),
            ("auth_token", auth_token.to_string()),
            ("queue_dir", format!("{}/notify-queue", env.temp_dir)),
        ],
    )
    .await
}

async fn configure_webhook_target_with_key_values(
    env: &RustFSTestEnvironment,
    target_name: &str,
    key_values: Vec<(&str, String)>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let queue_dir = format!("{}/notify-queue", env.temp_dir);
    tokio::fs::create_dir_all(&queue_dir).await?;
    let mut key_values = key_values
        .into_iter()
        .map(|(key, value)| serde_json::json!({ "key": key, "value": value }))
        .collect::<Vec<_>>();
    if !key_values.iter().any(|entry| entry["key"].as_str() == Some("queue_dir")) {
        key_values.push(serde_json::json!({ "key": "queue_dir", "value": queue_dir }));
    }
    let response = send_configure_webhook_target_request(env, target_name, key_values).await?;
    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("failed to configure object lambda webhook target: {status} {body}").into());
    }

    Ok(())
}

async fn send_configure_webhook_target_request(
    env: &RustFSTestEnvironment,
    target_name: &str,
    key_values: Vec<serde_json::Value>,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let payload = serde_json::json!({ "key_values": key_values });
    let url = format!("{}/rustfs/admin/v3/target/notify_webhook/{}", env.url, target_name);
    signed_request(
        http::Method::PUT,
        &url,
        &env.access_key,
        &env.secret_key,
        Some(payload.to_string().into_bytes()),
        Some("application/json"),
    )
    .await
}

async fn list_notification_targets(env: &RustFSTestEnvironment) -> Result<serde_json::Value, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/target/list", env.url);
    let response = signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await?;
    let status = response.status();
    let body = response.bytes().await?;
    if status != StatusCode::OK {
        return Err(format!("failed to list notification targets: {status} {}", String::from_utf8_lossy(body.as_ref())).into());
    }

    Ok(serde_json::from_slice(&body)?)
}

async fn list_target_arns(env: &RustFSTestEnvironment) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/target/arns", env.url);
    let response = signed_request(http::Method::GET, &url, &env.access_key, &env.secret_key, None, None).await?;
    let status = response.status();
    let body = response.bytes().await?;
    if status != StatusCode::OK {
        return Err(format!("failed to list target arns: {status} {}", String::from_utf8_lossy(body.as_ref())).into());
    }

    Ok(serde_json::from_slice(&body)?)
}

async fn delete_webhook_target(env: &RustFSTestEnvironment, target_name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/target/notify_webhook/{target_name}/reset", env.url);
    let response = signed_request(http::Method::DELETE, &url, &env.access_key, &env.secret_key, None, None).await?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if status != StatusCode::OK {
        return Err(format!("failed to delete webhook target {target_name}: {status} {body}").into());
    }

    Ok(())
}

fn notification_target_is_listed(targets: &serde_json::Value, target_name: &str) -> bool {
    notification_target_entry(targets, target_name).is_some()
}

fn notification_target_entry<'a>(targets: &'a serde_json::Value, target_name: &str) -> Option<&'a serde_json::Value> {
    targets["notification_endpoints"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|entry| {
            entry["account_id"].as_str() == Some(target_name)
                && entry["service"]
                    .as_str()
                    .is_some_and(|service| service == "webhook" || service.starts_with("webhook-"))
        })
}

fn notification_target_status<'a>(targets: &'a serde_json::Value, target_name: &str) -> Option<&'a str> {
    notification_target_entry(targets, target_name).and_then(|entry| entry["status"].as_str())
}

async fn wait_for_target_visibility(
    env: &RustFSTestEnvironment,
    target_name: &str,
) -> Result<(serde_json::Value, Vec<String>), Box<dyn Error + Send + Sync>> {
    let mut last_targets = serde_json::Value::Null;
    let mut last_arns = Vec::new();

    for _ in 0..20 {
        last_targets = list_notification_targets(env).await?;
        last_arns = list_target_arns(env).await?;

        if notification_target_is_listed(&last_targets, target_name) {
            return Ok((last_targets, last_arns));
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err(format!("target {target_name} did not become visible in admin APIs; targets={last_targets}, arns={last_arns:?}").into())
}

async fn wait_for_target_absence(
    env: &RustFSTestEnvironment,
    target_name: &str,
) -> Result<(serde_json::Value, Vec<String>), Box<dyn Error + Send + Sync>> {
    let mut last_targets = serde_json::Value::Null;
    let mut last_arns = Vec::new();

    for _ in 0..20 {
        last_targets = list_notification_targets(env).await?;
        last_arns = list_target_arns(env).await?;

        let listed = notification_target_is_listed(&last_targets, target_name);
        let arn_listed = last_arns.iter().any(|arn| arn.ends_with(&format!(":{target_name}:webhook")));
        if !listed && !arn_listed {
            return Ok((last_targets, last_arns));
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err(format!("target {target_name} remained visible in admin APIs; targets={last_targets}, arns={last_arns:?}").into())
}

async fn restart_rustfs_server(env: &mut RustFSTestEnvironment) -> Result<(), Box<dyn Error + Send + Sync>> {
    env.stop_server();
    env.start_rustfs_server_without_cleanup(vec![]).await
}

async fn spawn_http_origin_probe_server() -> Result<
    (
        String,
        mpsc::Receiver<String>,
        tokio::task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>,
    ),
    Box<dyn Error + Send + Sync>,
> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let webhook_url = format!("http://{address}/hook");
    let (path_tx, path_rx) = mpsc::channel(1);

    let handle = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await?;
            let path = timeout(Duration::from_secs(2), read_request_path(&mut stream)).await??;
            let _ = path_tx.try_send(path.clone());
            if path == "/" {
                let response = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                stream.write_all(response).await?;
            }
        }
    });

    Ok((webhook_url, path_rx, handle))
}

async fn read_persisted_server_config(env: &RustFSTestEnvironment) -> String {
    let path = format!("{}/.rustfs.sys/config/config.json", env.temp_dir);
    match tokio::fs::read_to_string(&path).await {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::IsADirectory => {
            let mut entries = Vec::new();
            match tokio::fs::read_dir(&path).await {
                Ok(mut dir) => {
                    while let Ok(Some(entry)) = dir.next_entry().await {
                        entries.push(entry.file_name().to_string_lossy().to_string());
                    }
                    entries.sort();
                    format!("persisted config stored as object directory at {path}; entries={entries:?}")
                }
                Err(dir_err) => format!("persisted config directory exists at {path} but could not be listed: {dir_err}"),
            }
        }
        Err(err) => format!("failed to read persisted config at {path}: {err}"),
    }
}

async fn read_listen_notification_event(
    response: reqwest::Response,
    expected_key: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut response = response;
    let mut pending = String::new();
    loop {
        let chunk = timeout(Duration::from_secs(12), response.chunk()).await??;
        let Some(chunk) = chunk else {
            return Err("listen_notification stream ended before payload".into());
        };
        if chunk.is_empty() {
            continue;
        }
        pending.push_str(&String::from_utf8(chunk.to_vec())?);

        while let Some(newline) = pending.find('\n') {
            let line = pending.drain(..=newline).collect::<String>();
            let payload = line.trim();
            if payload.is_empty() {
                continue;
            }

            let json: serde_json::Value = serde_json::from_str(payload)?;
            let Some(records) = json["Records"].as_array() else {
                continue;
            };
            if records.is_empty() {
                continue;
            }

            let has_expected_key = records.iter().any(|record| {
                let Some(object_key) = record["s3"]["object"]["key"].as_str() else {
                    return false;
                };
                let decoded = urlencoding::decode(object_key)
                    .map(|decoded| decoded.into_owned())
                    .unwrap_or_else(|_| object_key.to_string());
                decoded == expected_key
            });
            if has_expected_key {
                return Ok(payload.to_string());
            }
        }
    }
}

#[tokio::test]
#[serial]
async fn test_notification_target_persists_across_restart_and_delete() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (webhook_url, _request_rx, webhook_handle) = spawn_object_lambda_webhook_server().await?;

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let target_name = "restart-target";
    configure_webhook_target(&env, target_name, &webhook_url, "secret-token").await?;

    let (visible_targets, visible_arns) = wait_for_target_visibility(&env, target_name).await?;
    assert!(notification_target_is_listed(&visible_targets, target_name));
    assert!(
        visible_arns
            .iter()
            .any(|arn| arn.ends_with(&format!(":{target_name}:webhook"))),
        "target ARN missing after initial configure: {visible_arns:?}"
    );

    restart_rustfs_server(&mut env).await?;

    let (targets_after_restart, arns_after_restart) = wait_for_target_visibility(&env, target_name).await?;
    assert!(notification_target_is_listed(&targets_after_restart, target_name));
    assert!(
        arns_after_restart
            .iter()
            .any(|arn| arn.ends_with(&format!(":{target_name}:webhook"))),
        "target ARN missing after restart: {arns_after_restart:?}"
    );

    delete_webhook_target(&env, target_name).await?;
    let (targets_after_delete, arns_after_delete) = wait_for_target_absence(&env, target_name).await?;
    assert!(!notification_target_is_listed(&targets_after_delete, target_name));
    assert!(
        !arns_after_delete
            .iter()
            .any(|arn| arn.ends_with(&format!(":{target_name}:webhook"))),
        "target ARN still visible after delete: {arns_after_delete:?}"
    );

    restart_rustfs_server(&mut env).await?;

    let (targets_after_delete_restart, arns_after_delete_restart) = wait_for_target_absence(&env, target_name).await?;
    assert!(!notification_target_is_listed(&targets_after_delete_restart, target_name));
    assert!(
        !arns_after_delete_restart
            .iter()
            .any(|arn| arn.ends_with(&format!(":{target_name}:webhook"))),
        "target ARN still visible after delete + restart: {arns_after_delete_restart:?}"
    );

    webhook_handle.abort();
    let _ = webhook_handle.await;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_notification_target_with_path_is_online_via_transport_probe() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (webhook_url, mut probe_rx, probe_handle) = spawn_http_origin_probe_server().await?;

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], &[("RUSTFS_NOTIFY_ENABLE", "true")])
        .await?;

    let target_name = "path-probe";
    configure_webhook_target(&env, target_name, &webhook_url, "secret-token").await?;

    let (visible_targets, visible_arns) = wait_for_target_visibility(&env, target_name).await?;
    assert_eq!(notification_target_status(&visible_targets, target_name), Some("online"));
    let observed_path = timeout(Duration::from_secs(10), probe_rx.recv())
        .await
        .map_err(|_| "probe server timed out waiting for a request")?
        .ok_or("probe server did not observe a request")?;
    assert_eq!(observed_path, "/");
    assert!(
        visible_arns
            .iter()
            .any(|arn| arn.ends_with(&format!(":{target_name}:webhook"))),
        "target ARN missing for reachable path endpoint: {visible_arns:?}"
    );

    probe_handle.abort();
    let _ = probe_handle.await;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_get_object_lambda_accepts_presigned_requests() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (webhook_url, request_rx, webhook_handle) = spawn_object_lambda_webhook_server().await?;

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "object-lambda-e2e-presigned";
    let key = "input.txt";
    let object_body = b"hello presigned object lambda";
    let lambda_arn = "arn:rustfs:s3-object-lambda:us-east-1:transformer:webhook";

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(object_body))
        .send()
        .await?;

    configure_webhook_target(&env, "transformer", &webhook_url, "secret-token").await?;
    wait_for_target_visibility(&env, "transformer").await?;

    let lambda_url = format!("{}/{}/{}?lambdaArn={}", env.url, bucket, key, urlencoding::encode(lambda_arn));
    let response = presigned_get_request(&lambda_url, &env.access_key, &env.secret_key).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await?, "transformed through object lambda");

    let captured = timeout(Duration::from_secs(10), request_rx).await??;
    assert_eq!(captured.payload["configuration"]["accessPointArn"].as_str(), Some(lambda_arn));

    webhook_handle.await??;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_get_object_lambda_accepts_named_webhook_target_arn() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (webhook_url, request_rx, webhook_handle) = spawn_object_lambda_webhook_server().await?;

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "object-lambda-e2e-named-target";
    let key = "input.txt";
    let lambda_arn = "arn:rustfs:s3-object-lambda:us-east-1:transformer:webhook-preview";

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"hello object lambda"))
        .send()
        .await?;

    configure_webhook_target(&env, "transformer", &webhook_url, "secret-token").await?;
    wait_for_target_visibility(&env, "transformer").await?;

    let lambda_url = format!("{}/{}/{}?lambdaArn={}", env.url, bucket, key, urlencoding::encode(lambda_arn));
    let response = signed_request(http::Method::GET, &lambda_url, &env.access_key, &env.secret_key, None, None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await?, "transformed through object lambda");

    let captured = timeout(Duration::from_secs(10), request_rx).await??;
    assert_eq!(captured.payload["configuration"]["accessPointArn"].as_str(), Some(lambda_arn));

    webhook_handle.await??;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_get_object_lambda_invokes_runtime_webhook_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (webhook_url, request_rx, webhook_handle) = spawn_object_lambda_webhook_server().await?;

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "object-lambda-e2e";
    let key = "input.txt";
    let object_body = b"hello object lambda";
    let lambda_arn = "arn:rustfs:s3-object-lambda:us-east-1:transformer:webhook";

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(object_body))
        .send()
        .await?;

    configure_webhook_target(&env, "transformer", &webhook_url, "secret-token").await?;
    let (visible_targets, visible_arns) = wait_for_target_visibility(&env, "transformer").await?;

    let lambda_url = format!("{}/{}/{}?lambdaArn={}", env.url, bucket, key, urlencoding::encode(lambda_arn));
    let response = signed_request(http::Method::GET, &lambda_url, &env.access_key, &env.secret_key, None, None).await?;
    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let persisted_config = read_persisted_server_config(&env).await;
        return Err(format!(
            "object lambda request failed: {status} {body}; visible_targets={visible_targets}; visible_arns={visible_arns:?}; persisted_config={persisted_config}"
        )
        .into());
    }
    assert_eq!(
        response.headers().get(CONTENT_TYPE).and_then(|value| value.to_str().ok()),
        Some("text/plain")
    );
    assert_eq!(response.text().await?, "transformed through object lambda");

    let captured = timeout(Duration::from_secs(10), request_rx).await??;
    assert_eq!(captured.headers.get("authorization").map(String::as_str), Some("Bearer secret-token"));
    assert_eq!(captured.headers.get("x-rustfs-object-lambda-bucket").map(String::as_str), Some(bucket));
    assert_eq!(captured.headers.get("x-rustfs-object-lambda-key").map(String::as_str), Some(key));

    assert_eq!(captured.payload["configuration"]["accessPointArn"].as_str(), Some(lambda_arn));
    let expected_request_url = format!("/{bucket}/{key}?lambdaArn={}", urlencoding::encode(lambda_arn));
    assert_eq!(captured.payload["userRequest"]["url"].as_str(), Some(expected_request_url.as_str()));

    let input_s3_url = captured.payload["getObjectContext"]["inputS3Url"]
        .as_str()
        .ok_or("missing inputS3Url in object lambda payload")?;
    assert!(!input_s3_url.contains("lambdaArn="));

    let source_response = local_http_client().get(input_s3_url).send().await?;
    assert_eq!(source_response.status(), StatusCode::OK);
    assert_eq!(source_response.bytes().await?.as_ref(), object_body);

    webhook_handle.await??;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_get_object_lambda_passthroughs_non_success_webhook_response() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (webhook_url, _request_rx, webhook_handle) = spawn_object_lambda_webhook_server_with_response(WebhookResponseSpec {
        status_line: "418 I'm a teapot".to_string(),
        body: b"lambda upstream rejected".to_vec(),
        headers: vec![
            ("content-type".to_string(), "text/plain".to_string()),
            ("x-rustfs-debug".to_string(), "passthrough".to_string()),
            ("x-amz-request-route".to_string(), "should-not-leak".to_string()),
            ("x-amz-request-token".to_string(), "should-not-leak".to_string()),
        ],
        include_auth_headers: false,
        auth_route_override: None,
        auth_token_override: None,
    })
    .await?;

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "object-lambda-e2e-failure";
    let key = "input.txt";
    let lambda_arn = "arn:rustfs:s3-object-lambda:us-east-1:transformer:webhook";

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"hello object lambda"))
        .send()
        .await?;

    configure_webhook_target(&env, "transformer", &webhook_url, "secret-token").await?;
    wait_for_target_visibility(&env, "transformer").await?;

    let lambda_url = format!("{}/{}/{}?lambdaArn={}", env.url, bucket, key, urlencoding::encode(lambda_arn));
    let response = signed_request(http::Method::GET, &lambda_url, &env.access_key, &env.secret_key, None, None).await?;
    assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
    assert_eq!(
        response.headers().get("content-type").and_then(|value| value.to_str().ok()),
        Some("text/plain")
    );
    assert_eq!(
        response.headers().get("x-rustfs-debug").and_then(|value| value.to_str().ok()),
        Some("passthrough")
    );
    assert!(response.headers().get("x-amz-request-route").is_none());
    assert!(response.headers().get("x-amz-request-token").is_none());
    assert_eq!(response.text().await?, "lambda upstream rejected");

    webhook_handle.await??;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_get_object_lambda_rejects_success_response_without_auth_headers() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (webhook_url, _request_rx, webhook_handle) = spawn_object_lambda_webhook_server_with_response(WebhookResponseSpec {
        status_line: "200 OK".to_string(),
        body: b"missing auth headers".to_vec(),
        headers: vec![("content-type".to_string(), "text/plain".to_string())],
        include_auth_headers: false,
        auth_route_override: None,
        auth_token_override: None,
    })
    .await?;

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "object-lambda-e2e-missing-auth";
    let key = "input.txt";
    let lambda_arn = "arn:rustfs:s3-object-lambda:us-east-1:transformer:webhook";

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"hello object lambda"))
        .send()
        .await?;

    configure_webhook_target(&env, "transformer", &webhook_url, "secret-token").await?;
    wait_for_target_visibility(&env, "transformer").await?;

    let lambda_url = format!("{}/{}/{}?lambdaArn={}", env.url, bucket, key, urlencoding::encode(lambda_arn));
    let response = signed_request(http::Method::GET, &lambda_url, &env.access_key, &env.secret_key, None, None).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.text().await?;
    assert!(body.contains("authorization headers"), "unexpected error body: {body}");

    webhook_handle.await??;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_get_object_lambda_rejects_success_response_with_mismatched_auth_headers() -> Result<(), Box<dyn Error + Send + Sync>>
{
    init_logging();

    let (webhook_url, _request_rx, webhook_handle) = spawn_object_lambda_webhook_server_with_response(WebhookResponseSpec {
        status_line: "200 OK".to_string(),
        body: b"mismatched auth headers".to_vec(),
        headers: vec![("content-type".to_string(), "text/plain".to_string())],
        include_auth_headers: true,
        auth_route_override: Some("wrong-route".to_string()),
        auth_token_override: Some("wrong-token".to_string()),
    })
    .await?;

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "object-lambda-e2e-mismatched-auth";
    let key = "input.txt";
    let lambda_arn = "arn:rustfs:s3-object-lambda:us-east-1:transformer:webhook";

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"hello object lambda"))
        .send()
        .await?;

    configure_webhook_target(&env, "transformer", &webhook_url, "secret-token").await?;
    wait_for_target_visibility(&env, "transformer").await?;

    let lambda_url = format!("{}/{}/{}?lambdaArn={}", env.url, bucket, key, urlencoding::encode(lambda_arn));
    let response = signed_request(http::Method::GET, &lambda_url, &env.access_key, &env.secret_key, None, None).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.text().await?;
    assert!(body.contains("authorization headers"), "unexpected error body: {body}");

    webhook_handle.await??;

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_get_object_lambda_rejects_unsupported_target_type() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "object-lambda-e2e-unsupported-target";
    let key = "input.txt";
    let lambda_arn = "arn:rustfs:s3-object-lambda:us-east-1:transformer:mqtt";

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"hello object lambda"))
        .send()
        .await?;

    let lambda_url = format!("{}/{}/{}?lambdaArn={}", env.url, bucket, key, urlencoding::encode(lambda_arn));
    let response = signed_request(http::Method::GET, &lambda_url, &env.access_key, &env.secret_key, None, None).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert!(body.contains("NotImplemented"), "unexpected error body: {body}");
    assert!(
        body.to_ascii_lowercase().contains("target type is not supported"),
        "unexpected error body: {body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_get_object_lambda_rejects_unconfigured_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "object-lambda-e2e-missing-target";
    let key = "input.txt";
    let lambda_arn = "arn:rustfs:s3-object-lambda:us-east-1:transformer:webhook";

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"hello object lambda"))
        .send()
        .await?;

    let lambda_url = format!("{}/{}/{}?lambdaArn={}", env.url, bucket, key, urlencoding::encode(lambda_arn));
    let response = signed_request(http::Method::GET, &lambda_url, &env.access_key, &env.secret_key, None, None).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected error body: {body}");
    assert!(
        body.to_ascii_lowercase().contains("target is not configured"),
        "unexpected error body: {body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_get_object_lambda_rejects_disabled_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "object-lambda-e2e-disabled-target";
    let key = "input.txt";
    let lambda_arn = "arn:rustfs:s3-object-lambda:us-east-1:transformer:webhook";

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"hello object lambda"))
        .send()
        .await?;

    configure_webhook_target_with_key_values(
        &env,
        "transformer",
        vec![
            ("endpoint", "http://127.0.0.1:9/transform".to_string()),
            ("auth_token", "secret-token".to_string()),
            ("enable", "off".to_string()),
        ],
    )
    .await?;
    wait_for_target_visibility(&env, "transformer").await?;

    let lambda_url = format!("{}/{}/{}?lambdaArn={}", env.url, bucket, key, urlencoding::encode(lambda_arn));
    let response = signed_request(http::Method::GET, &lambda_url, &env.access_key, &env.secret_key, None, None).await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected error body: {body}");
    assert!(body.to_ascii_lowercase().contains("target is disabled"), "unexpected error body: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_configure_object_lambda_target_rejects_invalid_endpoint() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "object-lambda-e2e-invalid-endpoint";

    let client = env.create_s3_client();
    client.create_bucket().bucket(bucket).send().await?;
    client
        .put_object()
        .bucket(bucket)
        .key("input.txt")
        .body(ByteStream::from_static(b"hello object lambda"))
        .send()
        .await?;

    let response = send_configure_webhook_target_request(
        &env,
        "transformer",
        vec![
            serde_json::json!({ "key": "endpoint", "value": "://invalid-endpoint" }),
            serde_json::json!({ "key": "auth_token", "value": "secret-token" }),
            serde_json::json!({ "key": "queue_dir", "value": format!("{}/notify-queue", env.temp_dir) }),
        ],
    )
    .await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidArgument"), "unexpected error body: {body}");
    assert!(
        body.to_ascii_lowercase().contains("invalid endpoint url"),
        "unexpected error body: {body}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_configure_object_lambda_notify_webhook_rejects_response_header_timeout_key()
-> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let response = send_configure_webhook_target_request(
        &env,
        "transformer",
        vec![
            serde_json::json!({ "key": "endpoint", "value": "http://127.0.0.1:9/transform" }),
            serde_json::json!({ "key": "auth_token", "value": "secret-token" }),
            serde_json::json!({ "key": "response_header_timeout", "value": "not-a-duration" }),
            serde_json::json!({ "key": "queue_dir", "value": format!("{}/notify-queue", env.temp_dir) }),
        ],
    )
    .await?;
    let status = response.status();
    let body = response.text().await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidArgument"), "unexpected error body: {body}");
    assert!(
        body.to_ascii_lowercase().contains("response_header_timeout"),
        "unexpected error body: {body}"
    );
    assert!(body.to_ascii_lowercase().contains("not allowed"), "unexpected error body: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_listen_notification_emits_after_put_object() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "listen-notification-e2e";
    let key = "logs/app.json";
    let client = env.create_s3_client();

    client.create_bucket().bucket(bucket).send().await?;

    let listen_url = format!(
        "{}/{bucket}?events={}&prefix={}&suffix={}&ping=1",
        env.url,
        urlencoding::encode("s3:ObjectCreated:Put"),
        urlencoding::encode("logs/"),
        urlencoding::encode(".json"),
    );
    let response = signed_request(http::Method::GET, &listen_url, &env.access_key, &env.secret_key, None, None).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );

    let read_task = tokio::spawn(read_listen_notification_event(response, key));

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"listen notification body"))
        .send()
        .await?;

    let payload = timeout(Duration::from_secs(12), read_task).await???;
    assert!(!payload.is_empty(), "listen_notification payload should not be empty");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_listen_notification_emits_on_empty_bucket_when_notify_disabled() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server_with_env(vec![], &[("RUSTFS_NOTIFY_ENABLE", "false")])
        .await?;

    let bucket = "listen-empty-bucket-e2e";
    let key = "seed/object.txt";
    let client = env.create_s3_client();

    client.create_bucket().bucket(bucket).send().await?;

    let listen_url = format!("{}/{bucket}?events={}&ping=1", env.url, urlencoding::encode("s3:ObjectCreated:*"),);
    let response = signed_request(http::Method::GET, &listen_url, &env.access_key, &env.secret_key, None, None).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let read_task = tokio::spawn(read_listen_notification_event(response, key));

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"empty bucket watch body"))
        .send()
        .await?;

    let payload = timeout(Duration::from_secs(12), read_task).await???;
    assert!(!payload.is_empty(), "listen_notification payload should not be empty");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_listen_notification_fans_in_remote_node_events() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut cluster = RustFSTestClusterEnvironment::new(2).await?;
    cluster.start().await?;

    let bucket = "listen-notification-cluster";
    let key = "logs/cluster.json";
    let node0_client = cluster.create_s3_client(0)?;
    let node1_client = cluster.create_s3_client(1)?;

    node0_client.create_bucket().bucket(bucket).send().await?;

    let listen_url = format!(
        "{}/{bucket}?events={}&prefix={}&suffix={}&ping=1",
        cluster.nodes[0].url,
        urlencoding::encode("s3:ObjectCreated:Put"),
        urlencoding::encode("logs/"),
        urlencoding::encode(".json"),
    );
    let response = signed_request(http::Method::GET, &listen_url, &cluster.access_key, &cluster.secret_key, None, None).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let read_task = tokio::spawn(read_listen_notification_event(response, key));

    node1_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"cluster listen notification body"))
        .send()
        .await?;

    let payload = timeout(Duration::from_secs(12), read_task).await???;
    assert!(!payload.is_empty(), "listen_notification cluster payload should not be empty");

    Ok(())
}
