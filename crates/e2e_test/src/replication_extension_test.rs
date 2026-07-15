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
    RustFSTestEnvironment, awscurl_available, awscurl_post_sts_form_urlencoded, init_logging, local_http_client,
    replication_fast_env, rustfs_binary_path,
};
use crate::storage_api::replication_extension::BucketTargetSys;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, VersioningConfiguration};
use aws_sdk_s3::{Client, Config};
use http::header::{CONTENT_TYPE, HOST};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use local_ip_address::local_ip;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    SanType, generate_simple_self_signed,
};
use reqwest::StatusCode;
use rustfs_madmin::{
    AddServiceAccountReq, ListServiceAccountsResp, PeerInfo, PeerSite, ReplicateAddStatus, ReplicateEditStatus,
    ReplicateRemoveStatus, SRRemoveReq, SRResyncOpStatus, SRStatusInfo, SiteReplicationInfo, SyncStatus,
};
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::Body;
use s3s::header::X_AMZ_REPLICATION_STATUS;
use serial_test::serial;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::error::Error;
use std::net::IpAddr;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::fs;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

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

#[derive(Debug, Clone, serde::Deserialize)]
struct ReplicationResetStatusResponse {
    #[serde(rename = "Targets", default)]
    targets: Vec<ReplicationResetStatusTarget>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ReplicationResetStatusTarget {
    #[serde(rename = "Arn", default)]
    arn: String,
    #[serde(rename = "ResetID", default)]
    reset_id: String,
    #[serde(rename = "Status", default)]
    status: String,
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

async fn signed_request_with_client(
    client: &reqwest::Client,
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
    let mut request_builder = client.request(reqwest_method, url);
    for (name, value) in signed.headers() {
        request_builder = request_builder.header(name, value);
    }
    if let Some(body) = body {
        request_builder = request_builder.body(body);
    }

    Ok(request_builder.send().await?)
}

async fn signed_request_with_session_token(
    method: http::Method,
    url: &str,
    access_key: &str,
    secret_key: &str,
    session_token: &str,
    body: Option<Vec<u8>>,
    content_type: Option<&str>,
) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
    let uri = url.parse::<http::Uri>()?;
    let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
    let mut request = http::Request::builder().method(method.clone()).uri(uri);
    request = request.header(HOST, authority);
    request = request.header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
    if !session_token.is_empty() {
        request = request.header("x-amz-security-token", session_token);
    }
    if let Some(content_type) = content_type {
        request = request.header(CONTENT_TYPE, content_type);
    }

    let content_len = body.as_ref().map(|body| body.len() as i64).unwrap_or_default();
    let signed = sign_v4(
        request.body(Body::empty())?,
        content_len,
        access_key,
        secret_key,
        session_token,
        "us-east-1",
    );

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
      <Status>Enabled</Status>
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
    let client = env.create_s3_client();
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

async fn admin_create_user(
    env: &RustFSTestEnvironment,
    username: &str,
    secret_key: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = format!("{}/rustfs/admin/v3/add-user?accessKey={}", env.url, username);
    let body = serde_json::json!({
        "secretKey": secret_key,
        "status": "enabled"
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
        return Err(format!("create user failed: {status} {body}").into());
    }

    Ok(())
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
        if info.enabled && info.sites.len() == expected_sites {
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
    for _ in 0..40 {
        let info = site_replication_info(env).await?;
        if predicate(&info) {
            return Ok(info);
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(format!("site replication info did not reach expected state on {}", env.address).into())
}

async fn wait_for_site_replication_status<F>(
    env: &RustFSTestEnvironment,
    query: &str,
    predicate: F,
) -> Result<SRStatusInfo, Box<dyn Error + Send + Sync>>
where
    F: Fn(&SRStatusInfo) -> bool,
{
    for _ in 0..40 {
        let status = site_replication_status(env, query).await?;
        if predicate(&status) {
            return Ok(status);
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(format!("site replication status did not reach expected state on {}", env.address).into())
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

#[tokio::test]
#[serial]
async fn test_replication_check_succeeds_with_remote_target() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let (_source_env, _target_env, source_bucket) = build_replication_pair(true).await?;
    let response = run_replication_check(&_source_env, &source_bucket).await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.text().await?.is_empty());

    Ok(())
}

#[tokio::test]
#[serial]
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

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("InvalidRequest"), "unexpected response: {body}");
    assert!(body.to_ascii_lowercase().contains("object lock"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
    assert!(body.to_ascii_lowercase().contains("arn is empty"), "unexpected response: {body}");

    Ok(())
}

#[tokio::test]
#[serial]
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

#[tokio::test]
#[serial]
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
#[serial]
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
#[serial]
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

    Ok(())
}

#[tokio::test]
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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

#[tokio::test]
#[serial]
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

    let target_etag_a = target_client_a
        .head_object()
        .bucket(target_bucket_a)
        .key(object_key)
        .send()
        .await?
        .e_tag()
        .ok_or("first target omitted ETag")?
        .to_string();
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

/// backlog#1147 repl-5, scenario (a) — target outage + recovery (rustfs#3421 / #2071).
///
/// Kills the replication target mid-workload, asserts the source records the
/// undelivered objects as PENDING/FAILED, then recovers the target with its data
/// directory intact and asserts both sides converge with no data loss. The
/// still-running source's data scanner (short cycle via [`FAST_SCANNER_ENV`])
/// re-drives the failed objects once the target is reachable again.
#[tokio::test]
#[serial]
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
#[serial]
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

/// backlog#1147 repl-5, scenario (c) — replayed delete marker keeps the source
/// mtime (mirrors backlog#867).
///
/// A delete marker is created while the target is down; the source is then
/// restarted (data preserved) and the target brought back, so the marker
/// replicates through the failure-replay path. The replayed marker must carry
/// the SOURCE's `LastModified`, not the replay time. A deliberate gap before
/// recovery makes any regression (replay-time stamping) obvious.
///
/// The source restart is load-bearing, not just paranoia: on a live
/// (never-restarted) source, the failed delete-marker replication wedges the
/// per-object `/[replicate]/<key>` namespace lock and the marker never
/// replicates even after the target recovers — tracked as backlog#1278. Once
/// that is fixed, a restart-free variant of this scenario should be added.
#[tokio::test]
#[serial]
async fn test_bucket_replication_replayed_delete_marker_preserves_source_mtime() -> TestResult {
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

    // Widen the gap so a replay-time-stamping regression is unmistakable.
    sleep(Duration::from_secs(3)).await;

    // Restart the source (see the doc comment: live-source replay is wedged by
    // backlog#1278), then bring the target back; the restarted source's scanner
    // heal pass replays the failed delete marker.
    source_env.restart_server_preserving_data(vec![], &source_env_vars).await?;
    target_env.restart_server_preserving_data(vec![], &[]).await?;

    let target_mtime = wait_for_target_delete_marker(&target_client, target_bucket, object_key).await?;
    assert_eq!(
        target_mtime, source_mtime,
        "replayed delete marker did not preserve the source mtime (backlog#867): source={source_mtime:?}, target={target_mtime:?}"
    );

    Ok(())
}

#[tokio::test]
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
async fn test_site_replication_resync_start_cancel_restart_real_dual_node() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logging();

    let mut source_env = RustFSTestEnvironment::new().await?;
    source_env
        .start_rustfs_server_with_env(vec![], LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let mut target_env = RustFSTestEnvironment::new().await?;
    target_env
        .start_rustfs_server_without_cleanup_with_env(LOOPBACK_REPLICATION_TARGET_ENV)
        .await?;

    let source_bucket = "site-repl-resync-src";

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

    for idx in 0..32 {
        source_client
            .put_object()
            .bucket(source_bucket)
            .key(format!("resync-object-{idx:02}"))
            .body(ByteStream::from(vec![b'x'; 256 * 1024]))
            .send()
            .await?;
    }

    let started = site_replication_resync_op(&source_env, "start", &remote_peer).await?;
    assert_eq!(started.status, "success", "unexpected start result: {:?}", started);
    assert!(
        started
            .buckets
            .iter()
            .any(|bucket| bucket.bucket == source_bucket && matches!(bucket.status.as_str(), "started" | "success")),
        "source bucket start status missing: {:?}",
        started
    );
    assert!(!started.resync_id.is_empty(), "start response omitted the resync id: {:?}", started);
    let started_reset_id = started.resync_id.clone();

    let canceled = site_replication_resync_op(&source_env, "cancel", &remote_peer).await?;
    assert_eq!(canceled.status, "success", "unexpected cancel result: {:?}", canceled);
    assert!(
        canceled
            .buckets
            .iter()
            .any(|bucket| bucket.bucket == source_bucket && matches!(bucket.status.as_str(), "canceled" | "success")),
        "source bucket cancel status missing: {:?}",
        canceled
    );

    let canceled_target =
        wait_for_replication_reset_target(&source_env, source_bucket, &target_arn, |target| target.status == "Canceled").await?;
    assert_eq!(canceled_target.status, "Canceled");
    assert_eq!(canceled_target.reset_id, started_reset_id);

    let restarted = site_replication_resync_op(&source_env, "start", &remote_peer).await?;
    assert_eq!(restarted.status, "success", "unexpected restart result: {:?}", restarted);
    assert!(
        restarted
            .buckets
            .iter()
            .any(|bucket| bucket.bucket == source_bucket && matches!(bucket.status.as_str(), "started" | "success")),
        "source bucket restart status missing: {:?}",
        restarted
    );
    let restart_snapshot = get_replication_reset_status(&source_env, source_bucket, &target_arn).await?;
    let restarted_target = wait_for_replication_reset_target(&source_env, source_bucket, &target_arn, |target| {
        !target.reset_id.is_empty() && target.reset_id != started_reset_id
    })
    .await
    .map_err(|err| {
        format!(
            "restart ids: start={} restart={} snapshot={:?}; {err}",
            started_reset_id, restarted.resync_id, restart_snapshot.targets
        )
    })?;
    assert_ne!(restarted_target.reset_id, started_reset_id);

    Ok(())
}

#[tokio::test]
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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

#[tokio::test]
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
#[serial]
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
