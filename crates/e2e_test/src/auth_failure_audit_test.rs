// Copyright 2026 RustFS Team
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

//! End-to-end authentication/authorization failure audit delivery regression
//! tests (`rustfs/backlog#1151` `sec-15`).
//!
//! Configures a live `rustfs` server with an HTTP audit webhook target pointing
//! at an in-process loopback collector (`127.0.0.1:0`, bypassing proxies) and
//! verifies:
//! 1. Authenticated S3 `AccessDenied` (`GetObject` by an unprivileged user)
//!    emits a structured `AuditEntry` with HTTP 403, `status = "failure"`,
//!    `error = "Access Denied"`, and a non-empty `denialReason` tag.
//! 2. Anonymous S3 `AccessDenied` (`GetObject` on a private bucket) emits a
//!    matching `AuditEntry`.
//! 3. Admin account/MFA `AccessDenied` (`PUT /rustfs/admin/v3/set-user-secret-key`
//!    and `DELETE /rustfs/admin/v3/user/mfa` by a non-admin user) emits
//!    structured `AuditEntry` records with `failureReason = "access_denied"`.
//! 4. Characterization pin for `SignatureDoesNotMatch`: a tampered SigV4
//!    signature returns HTTP `403 SignatureDoesNotMatch` from the gateway
//!    authenticator before request-handler / denial-auditor execution and does
//!    not currently emit an audit webhook record.

use crate::common::{RustFSTestEnvironment, admin_create_user, admin_request, init_logging, local_http_client};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::request_signature_v4::{SIGN_V4_ALGORITHM, get_scope, get_signature, get_signing_key};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::convert::Infallible;
use std::error::Error;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use time::macros::format_description;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::{Duration, timeout};

type BoxError = Box<dyn Error + Send + Sync>;
type TestResult<T = ()> = Result<T, BoxError>;

struct AuditCollector {
    endpoint: String,
    origin: String,
    entries: Arc<Mutex<Vec<Value>>>,
    notify: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for AuditCollector {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl AuditCollector {
    async fn start() -> Result<Self, BoxError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let endpoint = format!("{origin}/audit");
        let entries = Arc::new(Mutex::new(Vec::new()));
        let notify = Arc::new(Notify::new());

        let server_entries = Arc::clone(&entries);
        let server_notify = Arc::clone(&notify);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    Ok((stream, _)) = listener.accept() => {
                        let entries = Arc::clone(&server_entries);
                        let notify = Arc::clone(&server_notify);
                        connections.spawn(async move {
                            let svc = service_fn(move |req: Request<Incoming>| {
                                let entries = Arc::clone(&entries);
                                let notify = Arc::clone(&notify);
                                async move {
                                    if let Ok(collected) = req.into_body().collect().await {
                                        let payload = collected.to_bytes();
                                        if let Ok(doc) = serde_json::from_slice::<Value>(&payload) {
                                            let mut guard = entries.lock().expect("audit collector mutex");
                                            if let Some(records) = doc.get("Records").and_then(Value::as_array) {
                                                guard.extend(records.iter().cloned());
                                            } else {
                                                guard.push(doc);
                                            }
                                            drop(guard);
                                            notify.notify_waiters();
                                        }
                                    }
                                    Ok::<_, Infallible>(Response::new(Empty::<Bytes>::new()))
                                }
                            });
                            let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), svc).await;
                        });
                    }
                    Some(_) = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });

        Ok(Self {
            endpoint,
            origin,
            entries,
            notify,
            task,
        })
    }

    async fn wait_for_entry(&self, label: &str, predicate: impl Fn(&Value) -> bool) -> Result<Value, BoxError> {
        timeout(Duration::from_secs(20), async {
            loop {
                let notified = self.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let guard = self.entries.lock().expect("audit collector mutex");
                    if let Some(found) = guard.iter().find(|entry| predicate(entry)) {
                        return found.clone();
                    }
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| {
            let snapshot = self.entries.lock().expect("audit collector mutex");
            format!(
                "timed out waiting for audit entry ({label}); captured {} entries: {:?}",
                snapshot.len(),
                snapshot
                    .iter()
                    .map(|e| {
                        serde_json::json!({
                            "api": e.get("api"),
                            "error": e.get("error"),
                            "accessKey": e.get("accessKey"),
                            "tags": e.get("tags"),
                            "requestPath": e.get("requestPath"),
                        })
                    })
                    .collect::<Vec<_>>()
            )
            .into()
        })
    }

    fn snapshot(&self) -> Vec<Value> {
        self.entries.lock().expect("audit collector mutex").clone()
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn sha256_hex(data: &[u8]) -> String {
    hex_lower(&Sha256::digest(data))
}

fn build_tampered_sigv4_headers(env: &RustFSTestEnvironment, path: &str) -> (String, String, String) {
    let now = OffsetDateTime::now_utc();
    let fmt = format_description!("[year][month][day]T[hour][minute][second]Z");
    let amz_date = now.format(&fmt).expect("format x-amz-date");
    let canonical_headers = format!("host:{}\nx-amz-content-sha256:{UNSIGNED_PAYLOAD}\nx-amz-date:{amz_date}\n", env.address);
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!("GET\n{path}\n\n{canonical_headers}\n{signed_headers}\n{UNSIGNED_PAYLOAD}");
    let scope = get_scope("us-east-1", now, "s3");
    let string_to_sign = format!("{SIGN_V4_ALGORITHM}\n{amz_date}\n{scope}\n{}", sha256_hex(canonical_request.as_bytes()));
    let signing_key = get_signing_key(&env.secret_key, "us-east-1", now, "s3");
    let mut sig = get_signature(signing_key, &string_to_sign);
    let flipped = if sig.ends_with('a') { 'b' } else { 'a' };
    sig.pop();
    sig.push(flipped);
    let authorization = format!(
        "{SIGN_V4_ALGORITHM} Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={sig}",
        env.access_key
    );
    (authorization, amz_date, UNSIGNED_PAYLOAD.to_string())
}

#[tokio::test]
async fn auth_failure_audit_emits_records_for_s3_and_admin_access_denied() -> TestResult {
    init_logging();
    let collector = AuditCollector::start().await?;
    let mut env = RustFSTestEnvironment::new().await?;
    let queue_dir = format!("{}/audit-queue-sec15", env.temp_dir);
    tokio::fs::create_dir_all(&queue_dir).await?;
    env.start_rustfs_server_without_cleanup_with_env(&[
        ("RUSTFS_AUDIT_ENABLE", "true"),
        ("RUSTFS_AUDIT_WEBHOOK_ENABLE_SEC15", "on"),
        ("RUSTFS_AUDIT_WEBHOOK_ENDPOINT_SEC15", collector.endpoint.as_str()),
        ("RUSTFS_AUDIT_WEBHOOK_QUEUE_DIR_SEC15", queue_dir.as_str()),
        ("RUSTFS_OUTBOUND_ALLOW_ORIGINS", collector.origin.as_str()),
        ("NO_PROXY", "127.0.0.1,localhost"),
        ("no_proxy", "127.0.0.1,localhost"),
    ])
    .await?;
    env.wait_for_server_ready().await?;

    let root = env.create_s3_client();
    let bucket = "sec15-audit-denial-bucket";
    let key = "private/secret.txt";
    let sentinel_key = "private/sentinel.txt";
    root.create_bucket().bucket(bucket).send().await?;
    for obj_key in [key, sentinel_key] {
        root.put_object()
            .bucket(bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"audited-secret"))
            .send()
            .await?;
    }

    let limited_user = "sec15limited";
    let limited_secret = "sec15limitedsecret123";
    let target_user = "sec15target";
    let target_secret = "sec15targetsecret123";
    admin_create_user(&env, limited_user, limited_secret).await?;
    admin_create_user(&env, target_user, target_secret).await?;

    // 1. Authenticated S3 AccessDenied -> emits structured denial AuditEntry.
    let limited_s3 = env.create_s3_client_with_credentials(limited_user, limited_secret);
    let denied_get = limited_s3
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect_err("unprivileged user GetObject must return AccessDenied");
    assert_eq!(denied_get.raw_response().map(|r| r.status().as_u16()), Some(403));
    assert_eq!(denied_get.as_service_error().and_then(ProvideErrorMetadata::code), Some("AccessDenied"));

    let s3_denied_entry = collector
        .wait_for_entry("authenticated S3 GetObject AccessDenied", |entry| {
            entry["api"]["name"].as_str() == Some("s3:GetObject")
                && entry["api"]["bucket"].as_str() == Some(bucket)
                && entry["api"]["object"].as_str() == Some(key)
                && entry["api"]["status_code"].as_i64() == Some(403)
                && entry["accessKey"].as_str() == Some(limited_user)
        })
        .await?;
    assert_eq!(s3_denied_entry["api"]["status"].as_str(), Some("failure"));
    assert_eq!(s3_denied_entry["error"].as_str(), Some("Access Denied"));
    assert!(
        s3_denied_entry["tags"]["denialReason"]
            .as_str()
            .is_some_and(|reason| !reason.is_empty()),
        "S3 AccessDenied audit entry must record tags.denialReason: {s3_denied_entry}"
    );

    // 2. Anonymous S3 AccessDenied -> emits structured denial AuditEntry.
    let anon_resp = local_http_client().get(format!("{}/{bucket}/{key}", env.url)).send().await?;
    let anon_status = anon_resp.status();
    let anon_body = anon_resp.text().await?;
    assert_eq!(anon_status, StatusCode::FORBIDDEN, "anonymous GetObject body: {anon_body}");
    assert!(anon_body.contains("<Code>AccessDenied</Code>"), "body: {anon_body}");

    let anon_denied_entry = collector
        .wait_for_entry("anonymous S3 GetObject AccessDenied", |entry| {
            entry["api"]["name"].as_str() == Some("s3:GetObject")
                && entry["api"]["bucket"].as_str() == Some(bucket)
                && entry["api"]["object"].as_str() == Some(key)
                && entry["api"]["status_code"].as_i64() == Some(403)
                && entry.get("accessKey").and_then(Value::as_str).is_none_or(str::is_empty)
        })
        .await?;
    assert_eq!(anon_denied_entry["api"]["status"].as_str(), Some("failure"));
    assert_eq!(anon_denied_entry["error"].as_str(), Some("Access Denied"));

    // 3. Admin Account/MFA AccessDenied -> emits AccountAuditRecord AuditEntry.
    let (pw_status, pw_body) = admin_request(
        &env.url,
        Method::PUT,
        &format!("/rustfs/admin/v3/set-user-secret-key?accessKey={target_user}"),
        Some(serde_json::json!({"secretKey": "attempted-override-secret-123"}).to_string()),
        limited_user,
        limited_secret,
    )
    .await?;
    assert_eq!(pw_status, StatusCode::FORBIDDEN, "admin set-user-secret-key body: {pw_body}");
    assert!(pw_body.contains("AccessDenied"), "body: {pw_body}");

    let admin_pw_entry = collector
        .wait_for_entry("AdminSetUserSecretKey AccessDenied", |entry| {
            entry["api"]["name"].as_str() == Some("AdminSetUserSecretKey")
                && entry["api"]["status"].as_str() == Some("failure")
                && entry["error"].as_str() == Some("access_denied")
        })
        .await?;
    assert_eq!(admin_pw_entry["accessKey"].as_str(), Some(target_user));
    assert_eq!(admin_pw_entry["tags"]["iamOperation"].as_str(), Some("AdminSetUserSecretKey"));
    assert_eq!(admin_pw_entry["requestPath"].as_str(), Some("/rustfs/admin/v3/set-user-secret-key"));

    let (mfa_status, mfa_body) = admin_request(
        &env.url,
        Method::DELETE,
        &format!("/rustfs/admin/v3/user/mfa?accessKey={target_user}"),
        None,
        limited_user,
        limited_secret,
    )
    .await?;
    assert_eq!(mfa_status, StatusCode::FORBIDDEN, "admin user/mfa reset body: {mfa_body}");
    assert!(mfa_body.contains("AccessDenied"), "body: {mfa_body}");

    let admin_mfa_entry = collector
        .wait_for_entry("AdminResetUserMfa AccessDenied", |entry| {
            entry["api"]["name"].as_str() == Some("AdminResetUserMfa")
                && entry["api"]["status"].as_str() == Some("failure")
                && entry["error"].as_str() == Some("access_denied")
        })
        .await?;
    assert_eq!(admin_mfa_entry["accessKey"].as_str(), Some(target_user));
    assert_eq!(admin_mfa_entry["tags"]["iamOperation"].as_str(), Some("AdminResetUserMfa"));
    assert_eq!(admin_mfa_entry["requestPath"].as_str(), Some("/rustfs/admin/v3/user/mfa"));

    // 4. Characterization for SignatureDoesNotMatch: rejected at the gateway
    // SigV4Authenticator boundary before DenialAuditor/handlers execute.
    let bad_sig_path = format!("/{bucket}/private/bad-sig-probe.txt");
    let (auth, amz_date, payload_hash) = build_tampered_sigv4_headers(&env, &bad_sig_path);
    let sig_resp = local_http_client()
        .get(format!("{}{bad_sig_path}", env.url))
        .header("authorization", auth)
        .header("x-amz-date", amz_date)
        .header("x-amz-content-sha256", payload_hash)
        .send()
        .await?;
    let sig_status = sig_resp.status();
    let sig_body = sig_resp.text().await?;
    assert_eq!(sig_status, StatusCode::FORBIDDEN, "tampered SigV4 must return 403, body: {sig_body}");
    assert!(
        sig_body.contains("<Code>SignatureDoesNotMatch</Code>"),
        "expected SignatureDoesNotMatch, body: {sig_body}"
    );

    // Flush the audit pipeline with a deterministic sentinel AccessDenied request.
    let _ = limited_s3.get_object().bucket(bucket).key(sentinel_key).send().await;
    collector
        .wait_for_entry("sentinel AccessDenied flush", |entry| {
            entry["api"]["object"].as_str() == Some(sentinel_key) && entry["api"]["status_code"].as_i64() == Some(403)
        })
        .await?;

    // Pin current behavior honestly: SigV4Authenticator does not yet emit an
    // AuditEntry for SignatureDoesNotMatch. When production auditing is added
    // for authenticator rejections, update this assertion to assert the entry.
    let sig_entries: Vec<Value> = collector
        .snapshot()
        .into_iter()
        .filter(|entry| {
            entry["api"]["object"].as_str() == Some("private/bad-sig-probe.txt")
                || entry["requestPath"].as_str().is_some_and(|p| p.contains("bad-sig-probe.txt"))
        })
        .collect();
    assert!(
        sig_entries.is_empty(),
        "characterization pin: SigV4Authenticator currently does not emit audit entries for SignatureDoesNotMatch; if production auditing was added, upgrade this assertion: {sig_entries:?}"
    );

    env.stop_server();
    Ok(())
}
