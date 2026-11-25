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

//! Dual-stack signature admission matrix and historical GHSA security regressions
//! (`rustfs/backlog#2781` T3.10).
//!
//! Each test spawns two isolated `rustfs` server processes (`StackPair`) following the
//! repository's `dual_stack_differential` harness contract:
//! - Side A (`legacy`): runs the released reference binary named by `RUSTFS_E2E_REFERENCE_BINARY`
//!   when set (failing closed on an empty, missing, non-file, or non-executable path), or a
//!   separate server process of the binary under test when unset.
//! - Side B (`gateway`): runs the `rustfs` binary under test on its gateway S3 stack (without the
//!   retired `RUSTFS_S3_STACK` selector, which causes startup refusal).
//!
//! Wire requests are constructed and sent directly over raw HTTP/1.1 TCP sockets (`TcpStream`)
//! using independent RFC-compliant HMAC-SHA256 / HMAC-SHA1 signers so tests control `Host`
//! omission, duplicate `Authorization` headers, empty secret keys, skewed timestamps, wrong
//! service scopes, and multipart `PostObject` forms without client-side SDK/library normalization.

use crate::common::{
    AdminTransport, RustFSTestEnvironment, admin_add_canned_policy_via, admin_attach_user_policy_via, admin_create_user,
    build_test_sts_client, init_logging, parse_assume_role_credentials, rustfs_binary_path,
};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::json;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::fmt::Write as _;
use std::path::PathBuf;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{Duration, OffsetDateTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, BoxError>;

const ENV_REFERENCE_BINARY: &str = "RUSTFS_E2E_REFERENCE_BINARY";
const REGION: &str = "us-east-1";
const PRIVATE_BUCKET: &str = "sec-matrix-private";
const PUBLIC_BUCKET: &str = "sec-matrix-public";
const PROBE_KEY: &str = "probe.txt";
const PROBE_BODY: &[u8] = b"sec-matrix-confidential-payload";
const POST_BOUNDARY: &str = "sec-matrix-form-boundary";

enum ReferenceSide {
    SameBinary,
    Reference(PathBuf),
}

impl ReferenceSide {
    fn from_env() -> Result<Self, BoxError> {
        Self::resolve(std::env::var_os(ENV_REFERENCE_BINARY))
    }

    fn resolve(value: Option<OsString>) -> Result<Self, BoxError> {
        let Some(value) = value else {
            return Ok(Self::SameBinary);
        };
        let path = PathBuf::from(value);
        if path.as_os_str().is_empty() {
            return Err(format!("{ENV_REFERENCE_BINARY} is set but empty").into());
        }
        let metadata = std::fs::metadata(&path)
            .map_err(|err| format!("{ENV_REFERENCE_BINARY} points to {}, which cannot be read: {err}", path.display()))?;
        if !metadata.is_file() {
            return Err(format!("{ENV_REFERENCE_BINARY} points to {}, which is not a file", path.display()).into());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o111 == 0 {
                return Err(format!("{ENV_REFERENCE_BINARY} points to {}, which is not executable", path.display()).into());
            }
        }
        Ok(Self::Reference(path))
    }
}

struct StackPair {
    legacy: RustFSTestEnvironment,
    gateway: RustFSTestEnvironment,
    legacy_label: &'static str,
    is_reference_binary: bool,
}

impl StackPair {
    async fn spawn() -> TestResult<Self> {
        init_logging();
        let side_a = ReferenceSide::from_env()?;
        let is_reference_binary = matches!(side_a, ReferenceSide::Reference(_));
        let legacy_label = if is_reference_binary {
            "legacy-reference"
        } else {
            "legacy-same-binary"
        };
        let server_env = [("RUSTFS_REGION", REGION), ("RUSTFS_CHECK_UPDATE", "false")];
        let start_legacy = async {
            let mut env = RustFSTestEnvironment::new().await?;
            match &side_a {
                ReferenceSide::SameBinary => {
                    let binary = rustfs_binary_path();
                    env.start_rustfs_server_from_binary_without_cleanup(&binary, vec![], &server_env)
                        .await?;
                }
                ReferenceSide::Reference(binary) => {
                    env.start_rustfs_server_from_binary_without_cleanup(binary, vec![], &server_env)
                        .await?;
                }
            }
            Ok::<_, BoxError>(env)
        };
        let start_gateway = async {
            let mut env = RustFSTestEnvironment::new().await?;
            env.start_rustfs_server_without_cleanup_with_env(&server_env).await?;
            Ok::<_, BoxError>(env)
        };
        let (legacy, gateway) = tokio::try_join!(start_legacy, start_gateway)?;
        Ok(Self {
            legacy,
            gateway,
            legacy_label,
            is_reference_binary,
        })
    }

    async fn seed_matrix_buckets(&self) -> TestResult {
        tokio::try_join!(seed_one_env(&self.legacy), seed_one_env(&self.gateway))?;
        Ok(())
    }
}

async fn seed_one_env(env: &RustFSTestEnvironment) -> TestResult {
    let client = env.create_s3_client();
    client.create_bucket().bucket(PRIVATE_BUCKET).send().await?;
    client
        .put_object()
        .bucket(PRIVATE_BUCKET)
        .key(PROBE_KEY)
        .body(ByteStream::from_static(PROBE_BODY))
        .send()
        .await?;

    client.create_bucket().bucket(PUBLIC_BUCKET).send().await?;
    client
        .put_object()
        .bucket(PUBLIC_BUCKET)
        .key(PROBE_KEY)
        .body(ByteStream::from_static(PROBE_BODY))
        .send()
        .await?;
    let public_policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "AllowPublicGetAndPost",
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": [format!("arn:aws:s3:::{PUBLIC_BUCKET}/*")]
        }]
    })
    .to_string();
    client
        .put_bucket_policy()
        .bucket(PUBLIC_BUCKET)
        .policy(public_policy)
        .send()
        .await?;
    Ok(())
}

#[derive(Clone, Debug)]
struct RawHttpRequest {
    method: &'static str,
    target: String,
    include_host: bool,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[derive(Clone, Debug)]
struct WireResponse {
    status: u16,
    body: String,
}

impl WireResponse {
    fn error_code(&self) -> Option<&str> {
        let open = "<Code>";
        let close = "</Code>";
        let start = self.body.find(open)? + open.len();
        let end = self.body[start..].find(close)? + start;
        Some(&self.body[start..end])
    }
}

fn decode_chunked_body(mut raw: &[u8]) -> Result<Vec<u8>, BoxError> {
    let mut out = Vec::new();
    loop {
        let Some(line_end) = raw.windows(2).position(|w| w == b"\r\n") else {
            return Err("malformed chunked HTTP response: missing chunk-size CRLF".into());
        };
        let size_line = std::str::from_utf8(&raw[..line_end])?;
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let chunk_len = usize::from_str_radix(size_hex, 16)?;
        raw = &raw[line_end + 2..];
        if chunk_len == 0 {
            break;
        }
        if raw.len() < chunk_len + 2 {
            return Err("malformed chunked HTTP response: truncated chunk payload".into());
        }
        out.extend_from_slice(&raw[..chunk_len]);
        raw = &raw[chunk_len + 2..];
    }
    Ok(out)
}

async fn send_raw_http1(address: &str, req: &RawHttpRequest) -> TestResult<WireResponse> {
    let mut wire = format!("{} {} HTTP/1.1\r\n", req.method, req.target);
    if req.include_host {
        let _ = write!(wire, "Host: {address}\r\n");
    }
    wire.push_str("Connection: close\r\n");
    let has_content_length = req
        .headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("content-length"));
    if !has_content_length && (!req.body.is_empty() || matches!(req.method, "PUT" | "POST")) {
        let _ = write!(wire, "Content-Length: {}\r\n", req.body.len());
    }
    for (name, value) in &req.headers {
        let _ = write!(wire, "{name}: {value}\r\n");
    }
    wire.push_str("\r\n");

    let (raw_bytes, ended_with_reset) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let mut stream = TcpStream::connect(address).await?;
        stream.write_all(wire.as_bytes()).await?;
        if !req.body.is_empty()
            && let Err(err) = stream.write_all(&req.body).await
            && !matches!(
                err.kind(),
                std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            )
        {
            return Err(err.into());
        }
        let mut buf = Vec::new();
        let mut reset_after_bytes = false;
        loop {
            let mut chunk = [0u8; 4096];
            match stream.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(err)
                    if !buf.is_empty()
                        && matches!(err.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted) =>
                {
                    reset_after_bytes = true;
                    break;
                }
                Err(err) => return Err(err.into()),
            }
        }
        Ok::<_, BoxError>((buf, reset_after_bytes))
    })
    .await
    .map_err(|_| format!("HTTP/1.1 exchange timed out against {address} for {} {}", req.method, req.target))??;

    let Some(split_at) = raw_bytes.windows(4).position(|w| w == b"\r\n\r\n") else {
        return Err(format!("missing HTTP header terminator from {address}").into());
    };
    let head = std::str::from_utf8(&raw_bytes[..split_at])?;
    let body_bytes = &raw_bytes[split_at + 4..];
    let mut lines = head.split("\r\n");
    let status_line = lines.next().ok_or("missing HTTP status line")?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| format!("malformed HTTP status line: {status_line}"))?
        .parse()?;
    let header_lines: Vec<&str> = lines.collect();
    let is_chunked = header_lines.iter().any(|line| {
        line.split_once(':').is_some_and(|(name, val)| {
            name.trim().eq_ignore_ascii_case("transfer-encoding") && val.to_ascii_lowercase().contains("chunked")
        })
    });
    let content_length: Option<usize> = header_lines
        .iter()
        .find_map(|line| {
            let (name, val) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| val.trim().parse::<usize>())
        })
        .transpose()?;
    let decoded_body = if req.method == "HEAD" {
        Vec::new()
    } else if is_chunked {
        decode_chunked_body(body_bytes)?
    } else if let Some(expected_len) = content_length {
        if body_bytes.len() < expected_len {
            return Err(format!(
                "truncated HTTP response body from {address}: expected {expected_len} bytes, got {}",
                body_bytes.len()
            )
            .into());
        }
        body_bytes[..expected_len].to_vec()
    } else if ended_with_reset {
        return Err(format!("connection reset from {address} without Content-Length or chunked framing").into());
    } else {
        body_bytes.to_vec()
    };
    let body = String::from_utf8_lossy(&decoded_body).into_owned();
    Ok(WireResponse { status, body })
}

async fn send_pair<F>(pair: &StackPair, build: F) -> TestResult<(WireResponse, WireResponse)>
where
    F: Fn(&RustFSTestEnvironment) -> RawHttpRequest,
{
    let req_legacy = build(&pair.legacy);
    let req_gateway = build(&pair.gateway);
    tokio::try_join!(
        send_raw_http1(&pair.legacy.address, &req_legacy),
        send_raw_http1(&pair.gateway.address, &req_gateway),
    )
}

fn hex_lower(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex_lower(&Sha256::digest(bytes))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC-SHA256 accepts arbitrary key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hmac_sha1_base64(secret: &str, string_to_sign: &str) -> String {
    let mut mac = Hmac::<Sha1>::new_from_slice(secret.as_bytes()).expect("HMAC-SHA1 accepts arbitrary key length");
    mac.update(string_to_sign.as_bytes());
    base64_simd::STANDARD.encode_to_string(mac.finalize().into_bytes())
}

fn amz_timestamp(at: OffsetDateTime) -> String {
    let fmt = format_description!("[year][month][day]T[hour][minute][second]Z");
    at.format(&fmt).expect("format SigV4 timestamp")
}

fn sigv4_signing_key(secret: &str, day: &str, region: &str, service: &str) -> Vec<u8> {
    let k_secret = format!("AWS4{secret}");
    let k_date = hmac_sha256(k_secret.as_bytes(), day.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn flip_hex_digits(sig: &str) -> String {
    let flipped: String = sig
        .chars()
        .map(|ch| {
            let v = ch.to_digit(16).expect("signature must be hexadecimal");
            char::from_digit(15 - v, 16).expect("hex complement is valid hex")
        })
        .collect();
    assert_ne!(sig, flipped, "complement must mutate signature hex");
    flipped
}

fn aws_uri_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

fn canonical_query_string(pairs: &[(&str, &str)]) -> String {
    let mut encoded: Vec<(String, String)> = pairs.iter().map(|(k, v)| (aws_uri_encode(k), aws_uri_encode(v))).collect();
    encoded.sort_unstable();
    encoded
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

#[derive(Clone)]
struct SigV4Params<'a> {
    method: &'static str,
    path: &'a str,
    query: Vec<(&'a str, &'a str)>,
    host: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
    service: &'a str,
    timestamp: OffsetDateTime,
    extra_signed_headers: Vec<(&'a str, &'a str)>,
    unsigned_headers: Vec<(&'a str, &'a str)>,
    body: Vec<u8>,
}

impl<'a> SigV4Params<'a> {
    fn get_probe(env: &'a RustFSTestEnvironment) -> Self {
        Self {
            method: "GET",
            path: "/sec-matrix-private/probe.txt",
            query: Vec::new(),
            host: &env.address,
            access_key: &env.access_key,
            secret_key: &env.secret_key,
            service: "s3",
            timestamp: OffsetDateTime::now_utc(),
            extra_signed_headers: Vec::new(),
            unsigned_headers: Vec::new(),
            body: Vec::new(),
        }
    }

    fn build_header_signed(&self, tamper_sig: bool, include_host: bool) -> RawHttpRequest {
        let amz_date = amz_timestamp(self.timestamp);
        let day = &amz_date[..8];
        let payload_hash = sha256_hex(&self.body);
        let mut signed = vec![
            ("host", self.host),
            ("x-amz-content-sha256", payload_hash.as_str()),
            ("x-amz-date", amz_date.as_str()),
        ];
        signed.extend(self.extra_signed_headers.iter().copied());
        signed.sort_unstable_by(|a, b| a.0.cmp(b.0));

        let signed_names = signed.iter().map(|(k, _)| *k).collect::<Vec<_>>().join(";");
        let canonical_headers: String = signed.iter().map(|(k, v)| format!("{k}:{}\n", v.trim())).collect();
        let canonical_query = canonical_query_string(&self.query);
        let canonical_req = format!(
            "{}\n{}\n{canonical_query}\n{canonical_headers}\n{signed_names}\n{payload_hash}",
            self.method, self.path
        );
        let scope = format!("{day}/{REGION}/{}/aws4_request", self.service);
        let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", sha256_hex(canonical_req.as_bytes()));
        let key = sigv4_signing_key(self.secret_key, day, REGION, self.service);
        let mut signature = hex_lower(&hmac_sha256(&key, string_to_sign.as_bytes()));
        if tamper_sig {
            signature = flip_hex_digits(&signature);
        }
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_names}, Signature={signature}",
            self.access_key
        );

        let mut headers = vec![
            ("x-amz-date".to_owned(), amz_date),
            ("x-amz-content-sha256".to_owned(), payload_hash),
            ("authorization".to_owned(), authorization),
        ];
        for (name, value) in &self.extra_signed_headers {
            headers.push(((*name).to_owned(), (*value).to_owned()));
        }
        for (name, value) in &self.unsigned_headers {
            headers.push(((*name).to_owned(), (*value).to_owned()));
        }
        let target = if canonical_query.is_empty() {
            self.path.to_owned()
        } else {
            format!("{}?{canonical_query}", self.path)
        };
        RawHttpRequest {
            method: self.method,
            target,
            include_host,
            headers,
            body: self.body.clone(),
        }
    }

    fn build_presigned(&self, expires_secs: u64, tamper_sig: bool, include_host: bool) -> RawHttpRequest {
        let amz_date = amz_timestamp(self.timestamp);
        let day = &amz_date[..8];
        let scope = format!("{day}/{REGION}/{}/aws4_request", self.service);
        let credential = format!("{}/{scope}", self.access_key);
        let expires_str = expires_secs.to_string();

        let mut signed = vec![("host", self.host)];
        signed.extend(self.extra_signed_headers.iter().copied());
        signed.sort_unstable_by(|a, b| a.0.cmp(b.0));
        let signed_names = signed.iter().map(|(k, _)| *k).collect::<Vec<_>>().join(";");
        let canonical_headers: String = signed.iter().map(|(k, v)| format!("{k}:{}\n", v.trim())).collect();

        let mut query = self.query.clone();
        query.extend([
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            ("X-Amz-Credential", credential.as_str()),
            ("X-Amz-Date", amz_date.as_str()),
            ("X-Amz-Expires", expires_str.as_str()),
            ("X-Amz-SignedHeaders", signed_names.as_str()),
        ]);
        let canonical_query = canonical_query_string(&query);
        let canonical_req = format!(
            "{}\n{}\n{canonical_query}\n{canonical_headers}\n{signed_names}\nUNSIGNED-PAYLOAD",
            self.method, self.path
        );
        let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", sha256_hex(canonical_req.as_bytes()));
        let key = sigv4_signing_key(self.secret_key, day, REGION, self.service);
        let mut signature = hex_lower(&hmac_sha256(&key, string_to_sign.as_bytes()));
        if tamper_sig {
            signature = flip_hex_digits(&signature);
        }
        let target = format!("{}?{canonical_query}&X-Amz-Signature={signature}", self.path);
        let mut headers = Vec::new();
        for (name, value) in &self.extra_signed_headers {
            headers.push(((*name).to_owned(), (*value).to_owned()));
        }
        for (name, value) in &self.unsigned_headers {
            headers.push(((*name).to_owned(), (*value).to_owned()));
        }
        RawHttpRequest {
            method: self.method,
            target,
            include_host,
            headers,
            body: self.body.clone(),
        }
    }
}

struct SigV2Params<'a> {
    method: &'static str,
    path: &'a str,
    content_type: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
    tamper_sig: bool,
    include_host: bool,
    body: Vec<u8>,
}

impl<'a> SigV2Params<'a> {
    fn get_probe(env: &'a RustFSTestEnvironment) -> Self {
        Self {
            method: "GET",
            path: "/sec-matrix-private/probe.txt",
            content_type: "",
            access_key: &env.access_key,
            secret_key: &env.secret_key,
            tamper_sig: false,
            include_host: true,
            body: Vec::new(),
        }
    }

    fn build_header(&self, timestamp: OffsetDateTime) -> RawHttpRequest {
        let imf_fixdate =
            format_description!("[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT");
        let date_hdr = timestamp.format(&imf_fixdate).expect("format IMF-fixdate GMT Date");
        let string_to_sign = format!("{}\n\n{}\n{date_hdr}\n{}", self.method, self.content_type, self.path);
        let signing_secret = if self.tamper_sig {
            format!("{}-tampered-v2", self.secret_key)
        } else {
            self.secret_key.to_owned()
        };
        let sig_b64 = hmac_sha1_base64(&signing_secret, &string_to_sign);
        let mut headers = vec![
            ("Date".to_owned(), date_hdr),
            ("Authorization".to_owned(), format!("AWS {}:{sig_b64}", self.access_key)),
        ];
        if !self.content_type.is_empty() {
            headers.push(("Content-Type".to_owned(), self.content_type.to_owned()));
        }
        RawHttpRequest {
            method: self.method,
            target: self.path.to_owned(),
            include_host: self.include_host,
            headers,
            body: self.body.clone(),
        }
    }

    fn build_presigned(&self, expires_raw: &str) -> RawHttpRequest {
        let string_to_sign = format!("{}\n\n{}\n{expires_raw}\n{}", self.method, self.content_type, self.path);
        let signing_secret = if self.tamper_sig {
            format!("{}-tampered-v2-query", self.secret_key)
        } else {
            self.secret_key.to_owned()
        };
        let sig_b64 = hmac_sha1_base64(&signing_secret, &string_to_sign);
        let sig_enc = aws_uri_encode(&sig_b64);
        let ak_enc = aws_uri_encode(self.access_key);
        let exp_enc = aws_uri_encode(expires_raw);
        let query = format!("AWSAccessKeyId={ak_enc}&Expires={exp_enc}&Signature={sig_enc}");
        let mut headers = Vec::new();
        if !self.content_type.is_empty() {
            headers.push(("Content-Type".to_owned(), self.content_type.to_owned()));
        }
        RawHttpRequest {
            method: self.method,
            target: format!("{}?{query}", self.path),
            include_host: self.include_host,
            headers,
            body: self.body.clone(),
        }
    }
}

struct PostFormParams<'a> {
    bucket: &'a str,
    object_key: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
    service: &'a str,
    form_timestamp: OffsetDateTime,
    expiration: OffsetDateTime,
    tamper_sig: bool,
    include_host: bool,
    anonymous_unsigned: bool,
    extra_headers: Vec<(String, String)>,
    file_contents: &'a [u8],
}

impl<'a> PostFormParams<'a> {
    fn valid(env: &'a RustFSTestEnvironment, object_key: &'a str) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            bucket: PRIVATE_BUCKET,
            object_key,
            access_key: &env.access_key,
            secret_key: &env.secret_key,
            service: "s3",
            form_timestamp: now,
            expiration: now + Duration::minutes(5),
            tamper_sig: false,
            include_host: true,
            anonymous_unsigned: false,
            extra_headers: Vec::new(),
            file_contents: b"post-policy-payload",
        }
    }

    fn build(&self) -> RawHttpRequest {
        let amz_date = amz_timestamp(self.form_timestamp);
        let cred_day = amz_date[..8].to_owned();
        let scope = format!("{cred_day}/{REGION}/{}/aws4_request", self.service);
        let credential = format!("{}/{scope}", self.access_key);
        let expiration_str = self.expiration.format(&Rfc3339).expect("format POST policy expiration");

        let mut fields: Vec<(&str, String)> = vec![("bucket", self.bucket.to_owned()), ("key", self.object_key.to_owned())];
        if !self.anonymous_unsigned {
            fields.push(("x-amz-algorithm", "AWS4-HMAC-SHA256".to_owned()));
            fields.push(("x-amz-credential", credential));
            fields.push(("x-amz-date", amz_date));
        }

        let conditions: Vec<serde_json::Value> = fields.iter().map(|(k, v)| json!({ (*k): v })).collect();
        let policy_doc = json!({
            "expiration": expiration_str,
            "conditions": conditions,
        });
        let policy_b64 = base64_simd::STANDARD.encode_to_string(policy_doc.to_string().as_bytes());
        fields.push(("policy", policy_b64.clone()));

        if !self.anonymous_unsigned {
            let signing_key = sigv4_signing_key(self.secret_key, &cred_day, REGION, self.service);
            let mut signature = hex_lower(&hmac_sha256(&signing_key, policy_b64.as_bytes()));
            if self.tamper_sig {
                signature = flip_hex_digits(&signature);
            }
            fields.push(("x-amz-signature", signature));
        }

        let mut body = Vec::new();
        for (name, value) in &fields {
            body.extend_from_slice(
                format!("--{POST_BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n").as_bytes(),
            );
        }
        body.extend_from_slice(
            format!(
                "--{POST_BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"upload.bin\"\r\nContent-Type: application/octet-stream\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(self.file_contents);
        body.extend_from_slice(format!("\r\n--{POST_BOUNDARY}--\r\n").as_bytes());

        let mut headers = vec![("Content-Type".to_owned(), format!("multipart/form-data; boundary={POST_BOUNDARY}"))];
        headers.extend(self.extra_headers.clone());
        RawHttpRequest {
            method: "POST",
            target: format!("/{}", self.bucket),
            include_host: self.include_host,
            headers,
            body,
        }
    }
}

fn assert_dual_stack_rejection(
    scheme: &str,
    case: &str,
    legacy_label: &str,
    legacy: &WireResponse,
    gateway: &WireResponse,
    expected_statuses: &[u16],
    expected_codes: Option<&[&str]>,
) {
    eprintln!(
        "[matrix:{scheme}/{case}] {legacy_label}=({}, {:?}) gateway=({}, {:?})",
        legacy.status,
        legacy.error_code(),
        gateway.status,
        gateway.error_code()
    );
    assert_eq!(
        legacy.status, gateway.status,
        "[{scheme}/{case}] HTTP status drift between {legacy_label} ({}: {:?}) and gateway ({}: {:?})",
        legacy.status, legacy.body, gateway.status, gateway.body
    );
    assert_eq!(
        legacy.error_code(),
        gateway.error_code(),
        "[{scheme}/{case}] S3 <Code> drift between {legacy_label} ({:?}) and gateway ({:?})",
        legacy.body,
        gateway.body
    );
    assert!(
        expected_statuses.contains(&gateway.status),
        "[{scheme}/{case}] expected HTTP status in {expected_statuses:?}, got {} with body: {}",
        gateway.status,
        gateway.body
    );
    assert!(
        !gateway
            .body
            .contains(std::str::from_utf8(PROBE_BODY).expect("ASCII probe payload")),
        "[{scheme}/{case}] rejected response leaked confidential object payload: {}",
        gateway.body
    );
    if let Some(codes) = expected_codes {
        let actual = gateway.error_code().unwrap_or_else(|| {
            panic!(
                "[{scheme}/{case}] expected XML <Code> in {codes:?}, got status {} with body: {:?}",
                gateway.status, gateway.body
            )
        });
        assert!(
            codes.contains(&actual),
            "[{scheme}/{case}] expected XML <Code> in {codes:?}, got {actual:?} in body: {}",
            gateway.body
        );
    }
}

/// Asserts an intentional gateway HTTP/auth tightening (`rustfs/backlog#2684` `R-069` single-`Host`
/// and single-`Authorization` framing, query/form credential-parse normalization, and session-token
/// failure masking):
/// - Against a released `1.0.5` (`s3s`) reference binary (`pair.is_reference_binary`), asserts the
///   exact historical `(status, <Code>)` on Side A and the hardened `(status, <Code>)` on Side B.
/// - In `SameBinary` mode, both sides run `gateway` and must match `expected_gateway`.
fn assert_gateway_hardening_delta(
    pair: &StackPair,
    scheme: &str,
    case: &str,
    legacy: &WireResponse,
    gateway: &WireResponse,
    expected_ref_105: (u16, Option<&str>),
    expected_gateway: (u16, &str),
) {
    let (gw_status, gw_code) = expected_gateway;
    if pair.is_reference_binary {
        eprintln!(
            "[matrix:{scheme}/{case}] {}=({}, {:?}) gateway=({}, {:?}) [1.0.5->gateway delta]",
            pair.legacy_label,
            legacy.status,
            legacy.error_code(),
            gateway.status,
            gateway.error_code()
        );
        assert_eq!(
            (legacy.status, legacy.error_code()),
            expected_ref_105,
            "[{scheme}/{case}] unexpected {} response body: {:?}",
            pair.legacy_label,
            legacy.body
        );
        assert_eq!(
            (gateway.status, gateway.error_code()),
            (gw_status, Some(gw_code)),
            "[{scheme}/{case}] unexpected gateway response body: {:?}",
            gateway.body
        );
        assert!(
            !gateway
                .body
                .contains(std::str::from_utf8(PROBE_BODY).expect("ASCII probe payload")),
            "[{scheme}/{case}] gateway rejected response leaked confidential object payload: {}",
            gateway.body
        );
    } else {
        assert_dual_stack_rejection(scheme, case, pair.legacy_label, legacy, gateway, &[gw_status], Some(&[gw_code]));
    }
}

async fn assert_object_absent_both_stacks(pair: &StackPair, bucket: &str, key: &str, context: &str) -> TestResult {
    for (label, env) in [(pair.legacy_label, &pair.legacy), ("gateway", &pair.gateway)] {
        let err = env
            .create_s3_client()
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect_err(&format!("[{context}] rejected write must not create {bucket}/{key} on {label}"));
        assert_eq!(
            err.raw_response().map(|r| r.status().as_u16()),
            Some(404),
            "[{context}] expected 404 absence on {label}, got {err:?}"
        );
    }
    Ok(())
}

async fn assert_object_absent_after_gateway_rejection(pair: &StackPair, bucket: &str, key: &str, context: &str) -> TestResult {
    if !pair.is_reference_binary {
        return assert_object_absent_both_stacks(pair, bucket, key, context).await;
    }
    let err = pair
        .gateway
        .create_s3_client()
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect_err(&format!("[{context}] gateway rejected write must not create {bucket}/{key}"));
    assert_eq!(
        err.raw_response().map(|r| r.status().as_u16()),
        Some(404),
        "[{context}] expected 404 absence on gateway, got {err:?}"
    );
    Ok(())
}

async fn create_scoped_iam_user(
    env: &RustFSTestEnvironment,
    username: &str,
    secret_key: &str,
    policy_name: &str,
    policy_doc: &serde_json::Value,
) -> TestResult {
    admin_create_user(env, username, secret_key).await?;
    admin_add_canned_policy_via(
        AdminTransport::Signed,
        &env.url,
        &env.access_key,
        &env.secret_key,
        policy_name,
        &policy_doc.to_string(),
    )
    .await?;
    admin_attach_user_policy_via(AdminTransport::Signed, &env.url, &env.access_key, &env.secret_key, policy_name, username)
        .await?;
    Ok(())
}

// ============================================================================
// 1. Signature & Admission Matrix (`^sec_` naming: 6 schemes x 7 perturbations)
// ============================================================================

#[tokio::test]
async fn sec_signature_matrix_sigv4_header() -> TestResult {
    let pair = StackPair::spawn().await?;
    pair.seed_matrix_buckets().await?;
    let scheme = "sigv4_header";

    // (1) valid
    let (leg, gw) = send_pair(&pair, |env| SigV4Params::get_probe(env).build_header_signed(false, true)).await?;
    assert_eq!((leg.status, gw.status), (200, 200), "[{scheme}/valid] must succeed on both stacks");
    assert_eq!(leg.body.as_bytes(), PROBE_BODY);
    assert_eq!(gw.body.as_bytes(), PROBE_BODY);

    // (2) wrong_signature
    let (leg, gw) = send_pair(&pair, |env| SigV4Params::get_probe(env).build_header_signed(true, true)).await?;
    assert_dual_stack_rejection(
        scheme,
        "wrong_signature",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["SignatureDoesNotMatch"]),
    );

    // (3) expired_timestamp (> 15 min past clock skew)
    let past = OffsetDateTime::now_utc() - Duration::minutes(20);
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.timestamp = past;
        p.build_header_signed(false, true)
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "expired_timestamp",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["RequestTimeTooSkewed"]),
    );

    // (4) future_skewed_timestamp (> 15 min future clock skew)
    let future = OffsetDateTime::now_utc() + Duration::minutes(20);
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.timestamp = future;
        p.build_header_signed(false, true)
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "future_skewed_timestamp",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["RequestTimeTooSkewed"]),
    );

    // (5) missing_host (raw HTTP/1.1 without Host header):
    // 1.0.5 (`s3s`) failed later at SigV4 `SignedHeaders=host` (403 SignatureDoesNotMatch);
    // gateway enforces RFC 9112 §3.2 single-Host framing upfront (`R-069`: 400 InvalidRequest).
    let (leg, gw) = send_pair(&pair, |env| SigV4Params::get_probe(env).build_header_signed(false, false)).await?;
    assert_gateway_hardening_delta(
        &pair,
        scheme,
        "missing_host",
        &leg,
        &gw,
        (403, Some("SignatureDoesNotMatch")),
        (400, "InvalidRequest"),
    );

    // (6) duplicate_authorization:
    // 1.0.5 (`s3s`) answered 403 AccessDenied; gateway rejects duplicate Authorization with 400 InvalidRequest.
    let (leg, gw) = send_pair(&pair, |env| {
        let mut req = SigV4Params::get_probe(env).build_header_signed(false, true);
        let dup_auth = req
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
            .expect("signed request carries authorization")
            .1
            .clone();
        req.headers.push(("authorization".to_owned(), dup_auth));
        req
    })
    .await?;
    assert_gateway_hardening_delta(
        &pair,
        scheme,
        "duplicate_authorization",
        &leg,
        &gw,
        (403, Some("AccessDenied")),
        (400, "InvalidRequest"),
    );

    // (7) wrong_service_scope: under rustfs/backlog#2684 (GW-SIG items 1-2,
    // `accept_legacy_rustfs_signing_services`), `s3`, `sts`, and `s3tables` are verified across
    // routes, while any other AWS service scope (`ec2`, `kms`) is refused with 501 NotImplemented.
    for compat_service in ["sts", "s3tables"] {
        let (leg, gw) = send_pair(&pair, |env| {
            let mut p = SigV4Params::get_probe(env);
            p.service = compat_service;
            p.build_header_signed(false, true)
        })
        .await?;
        assert_eq!((leg.status, gw.status), (200, 200), "#2684 GW-SIG {compat_service} parity");
    }
    for bad_service in ["ec2", "kms"] {
        let (leg, gw) = send_pair(&pair, |env| {
            let mut p = SigV4Params::get_probe(env);
            p.service = bad_service;
            p.build_header_signed(false, true)
        })
        .await?;
        assert_dual_stack_rejection(
            scheme,
            &format!("wrong_service_scope_{bad_service}"),
            pair.legacy_label,
            &leg,
            &gw,
            &[501],
            Some(&["NotImplemented"]),
        );
    }

    Ok(())
}

#[tokio::test]
async fn sec_signature_matrix_sigv4_presigned() -> TestResult {
    let pair = StackPair::spawn().await?;
    pair.seed_matrix_buckets().await?;
    let scheme = "sigv4_presigned";

    // (1) valid
    let (leg, gw) = send_pair(&pair, |env| SigV4Params::get_probe(env).build_presigned(300, false, true)).await?;
    assert_eq!((leg.status, gw.status), (200, 200), "[{scheme}/valid] must succeed on both stacks");
    assert_eq!(leg.body.as_bytes(), PROBE_BODY);
    assert_eq!(gw.body.as_bytes(), PROBE_BODY);

    // (2) wrong_signature
    let (leg, gw) = send_pair(&pair, |env| SigV4Params::get_probe(env).build_presigned(300, true, true)).await?;
    assert_dual_stack_rejection(
        scheme,
        "wrong_signature",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["SignatureDoesNotMatch"]),
    );

    // (3) expired_timestamp (signed 1 hour ago with 60s window)
    let expired_start = OffsetDateTime::now_utc() - Duration::hours(1);
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.timestamp = expired_start;
        p.build_presigned(60, false, true)
    })
    .await?;
    assert_dual_stack_rejection(scheme, "expired_timestamp", pair.legacy_label, &leg, &gw, &[403], Some(&["AccessDenied"]));

    // (4) future_skewed_timestamp (X-Amz-Date 20 min in the future)
    let future_start = OffsetDateTime::now_utc() + Duration::minutes(20);
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.timestamp = future_start;
        p.build_presigned(300, false, true)
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "future_skewed_timestamp",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["RequestTimeTooSkewed"]),
    );

    // (5) missing_host: 1.0.5 (`s3s`) answered 403 SignatureDoesNotMatch; gateway enforces 400 InvalidRequest.
    let (leg, gw) = send_pair(&pair, |env| SigV4Params::get_probe(env).build_presigned(300, false, false)).await?;
    assert_gateway_hardening_delta(
        &pair,
        scheme,
        "missing_host",
        &leg,
        &gw,
        (403, Some("SignatureDoesNotMatch")),
        (400, "InvalidRequest"),
    );

    // (6) duplicate_authorization (presigned query + duplicate Authorization headers):
    // 1.0.5 (`s3s`) ignored duplicate Authorization headers when presigned query params were present (200);
    // gateway rejects mixed/duplicate Authorization with 400 InvalidRequest.
    let (leg, gw) = send_pair(&pair, |env| {
        let hdr_req = SigV4Params::get_probe(env).build_header_signed(false, true);
        let auth_val = hdr_req
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
            .expect("header-signed request carries authorization")
            .1
            .clone();
        let mut req = SigV4Params::get_probe(env).build_presigned(300, false, true);
        req.headers.push(("authorization".to_owned(), auth_val.clone()));
        req.headers.push(("authorization".to_owned(), auth_val));
        req
    })
    .await?;
    assert_gateway_hardening_delta(&pair, scheme, "duplicate_authorization", &leg, &gw, (200, None), (400, "InvalidRequest"));

    // (7) wrong_service_scope: `ec2` / `kms` refused with 501 NotImplemented (#2684 GW-SIG items 1-2)
    for bad_service in ["ec2", "kms"] {
        let (leg, gw) = send_pair(&pair, |env| {
            let mut p = SigV4Params::get_probe(env);
            p.service = bad_service;
            p.build_presigned(300, false, true)
        })
        .await?;
        assert_dual_stack_rejection(
            scheme,
            &format!("wrong_service_scope_{bad_service}"),
            pair.legacy_label,
            &leg,
            &gw,
            &[501],
            Some(&["NotImplemented"]),
        );
    }

    Ok(())
}

#[tokio::test]
async fn sec_signature_matrix_sigv2_header() -> TestResult {
    let pair = StackPair::spawn().await?;
    pair.seed_matrix_buckets().await?;
    let scheme = "sigv2_header";
    let now = OffsetDateTime::now_utc();

    // (1) valid (RustFS profile supports SigV2 on S3 object routes; verify parity and payload)
    let (leg, gw) = send_pair(&pair, |env| SigV2Params::get_probe(env).build_header(now)).await?;
    assert_eq!(leg.status, gw.status, "[{scheme}/valid] parity mismatch");
    assert_eq!(gw.status, 200, "[{scheme}/valid] expected 200 on S3 object GET, got {}", gw.body);
    assert_eq!(leg.body.as_bytes(), PROBE_BODY);
    assert_eq!(gw.body.as_bytes(), PROBE_BODY);

    // (2) wrong_signature
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV2Params::get_probe(env);
        p.tamper_sig = true;
        p.build_header(now)
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "wrong_signature",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["SignatureDoesNotMatch"]),
    );

    // (3) expired_timestamp (Date 20 min in the past)
    let past = now - Duration::minutes(20);
    let (leg, gw) = send_pair(&pair, |env| SigV2Params::get_probe(env).build_header(past)).await?;
    assert_dual_stack_rejection(
        scheme,
        "expired_timestamp",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["RequestTimeTooSkewed"]),
    );

    // (4) future_skewed_timestamp (Date 20 min in the future)
    let future = now + Duration::minutes(20);
    let (leg, gw) = send_pair(&pair, |env| SigV2Params::get_probe(env).build_header(future)).await?;
    assert_dual_stack_rejection(
        scheme,
        "future_skewed_timestamp",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["RequestTimeTooSkewed"]),
    );

    // (5) missing_host: 1.0.5 (`hyper` + `s3s`) accepted Host-less SigV2 (200) because SigV2 does not
    // sign Host; gateway enforces RFC 9112 §3.2 Host framing (400 InvalidRequest).
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV2Params::get_probe(env);
        p.include_host = false;
        p.build_header(now)
    })
    .await?;
    assert_gateway_hardening_delta(&pair, scheme, "missing_host", &leg, &gw, (200, None), (400, "InvalidRequest"));

    // (6) duplicate_authorization: 1.0.5 (`s3s`) answered 403 AccessDenied; gateway answers 400 InvalidRequest.
    let (leg, gw) = send_pair(&pair, |env| {
        let mut req = SigV2Params::get_probe(env).build_header(now);
        let auth = req
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
            .expect("SigV2 request carries Authorization")
            .1
            .clone();
        req.headers.push(("Authorization".to_owned(), auth));
        req
    })
    .await?;
    assert_gateway_hardening_delta(
        &pair,
        scheme,
        "duplicate_authorization",
        &leg,
        &gw,
        (403, Some("AccessDenied")),
        (400, "InvalidRequest"),
    );

    // (7) wrong_service_scope: SigV2 credentials sent to STS (`POST /` AssumeRole) and Admin routes must be refused
    let sts_body =
        b"Action=AssumeRole&Version=2011-06-15&RoleArn=arn:aws:iam::123456789012:role/test&RoleSessionName=v2".to_vec();
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV2Params::get_probe(env);
        p.method = "POST";
        p.path = "/";
        p.content_type = "application/x-www-form-urlencoded";
        p.body = sts_body.clone();
        p.build_header(now)
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "wrong_service_scope_sts",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["AccessDenied"]),
    );

    let admin_path = "/rustfs/admin/v3/info";
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV2Params::get_probe(env);
        p.path = admin_path;
        p.build_header(now)
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "wrong_service_scope_admin",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["AccessDenied"]),
    );

    Ok(())
}

#[tokio::test]
async fn sec_signature_matrix_sigv2_presigned() -> TestResult {
    let pair = StackPair::spawn().await?;
    pair.seed_matrix_buckets().await?;
    let scheme = "sigv2_presigned";
    let now_epoch = OffsetDateTime::now_utc().unix_timestamp();

    // (1) valid
    let valid_exp = (now_epoch + 300).to_string();
    let (leg, gw) = send_pair(&pair, |env| SigV2Params::get_probe(env).build_presigned(&valid_exp)).await?;
    assert_eq!((leg.status, gw.status), (200, 200), "[{scheme}/valid] must succeed on both stacks");
    assert_eq!(leg.body.as_bytes(), PROBE_BODY);
    assert_eq!(gw.body.as_bytes(), PROBE_BODY);

    // (2) wrong_signature
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV2Params::get_probe(env);
        p.tamper_sig = true;
        p.build_presigned(&valid_exp)
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "wrong_signature",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["SignatureDoesNotMatch"]),
    );

    // (3) expired_timestamp (Expires 1 hour in the past)
    let expired_exp = (now_epoch - 3600).to_string();
    let (leg, gw) = send_pair(&pair, |env| SigV2Params::get_probe(env).build_presigned(&expired_exp)).await?;
    assert_dual_stack_rejection(scheme, "expired_timestamp", pair.legacy_label, &leg, &gw, &[403], Some(&["AccessDenied"]));

    // (4) future_skewed_timestamp (overflowing epoch beyond 64-bit integer bounds):
    // 1.0.5 (`s3s`) returned 400 InvalidRequest; gateway classifies it as 400 AuthorizationQueryParametersError.
    let overflow_exp = "99999999999999999999";
    let (leg, gw) = send_pair(&pair, |env| SigV2Params::get_probe(env).build_presigned(overflow_exp)).await?;
    assert_gateway_hardening_delta(
        &pair,
        scheme,
        "future_skewed_timestamp",
        &leg,
        &gw,
        (400, Some("InvalidRequest")),
        (400, "AuthorizationQueryParametersError"),
    );

    // (5) missing_host: 1.0.5 (`s3s`) accepted Host-less SigV2 presigned (200); gateway enforces 400 InvalidRequest.
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV2Params::get_probe(env);
        p.include_host = false;
        p.build_presigned(&valid_exp)
    })
    .await?;
    assert_gateway_hardening_delta(&pair, scheme, "missing_host", &leg, &gw, (200, None), (400, "InvalidRequest"));

    // (6) duplicate_authorization (SigV2 presigned query + duplicate Authorization headers):
    // 1.0.5 (`s3s`) ignored duplicate Authorization headers when SigV2 query params were present (200);
    // gateway rejects duplicate Authorization with 400 InvalidRequest.
    let (leg, gw) = send_pair(&pair, |env| {
        let mut req = SigV2Params::get_probe(env).build_presigned(&valid_exp);
        req.headers
            .push(("Authorization".to_owned(), format!("AWS {}:YWJj", env.access_key)));
        req.headers
            .push(("Authorization".to_owned(), format!("AWS {}:ZGVm", env.access_key)));
        req
    })
    .await?;
    assert_gateway_hardening_delta(&pair, scheme, "duplicate_authorization", &leg, &gw, (200, None), (400, "InvalidRequest"));

    // (7) wrong_service_scope: SigV2 presigned sent to STS (`POST /`) and Admin routes must be refused
    let sts_body =
        b"Action=AssumeRole&Version=2011-06-15&RoleArn=arn:aws:iam::123456789012:role/test&RoleSessionName=v2q".to_vec();
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV2Params::get_probe(env);
        p.method = "POST";
        p.path = "/";
        p.content_type = "application/x-www-form-urlencoded";
        p.body = sts_body.clone();
        p.build_presigned(&valid_exp)
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "wrong_service_scope_sts",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["AccessDenied"]),
    );

    let admin_path = "/rustfs/admin/v3/info";
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV2Params::get_probe(env);
        p.path = admin_path;
        p.build_presigned(&valid_exp)
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "wrong_service_scope_admin",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["AccessDenied"]),
    );

    Ok(())
}

#[tokio::test]
async fn sec_signature_matrix_post_policy() -> TestResult {
    let pair = StackPair::spawn().await?;
    pair.seed_matrix_buckets().await?;
    let scheme = "post_policy";

    // (1) valid
    let ok_key = "post-valid.bin";
    let (leg, gw) = send_pair(&pair, |env| PostFormParams::valid(env, ok_key).build()).await?;
    assert_eq!((leg.status, gw.status), (204, 204), "[{scheme}/valid] must return 204 on both stacks");
    assert!(leg.body.is_empty() && gw.body.is_empty(), "204 POST response body must be empty");
    for env in [&pair.legacy, &pair.gateway] {
        let obj = env
            .create_s3_client()
            .get_object()
            .bucket(PRIVATE_BUCKET)
            .key(ok_key)
            .send()
            .await?;
        assert_eq!(obj.body.collect().await?.into_bytes().as_ref(), b"post-policy-payload");
    }

    // (2) wrong_signature
    let bad_sig_key = "post-wrong-sig.bin";
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = PostFormParams::valid(env, bad_sig_key);
        p.tamper_sig = true;
        p.build()
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "wrong_signature",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["SignatureDoesNotMatch"]),
    );
    assert_object_absent_both_stacks(&pair, PRIVATE_BUCKET, bad_sig_key, "post_policy/wrong_signature").await?;

    // (3) expired_timestamp (policy expiration 1 hour in the past)
    // Intentional gateway hardening (`SigV4Authenticator::try_verify`): an expired signed POST
    // policy fails `Presented::read` at credential extraction and normalizes to 403
    // `InvalidAccessKeyId` (whereas legacy `1.0.5` `s3s` answered 403 `AccessDenied`).
    let exp_key = "post-expired.bin";
    let past_exp = OffsetDateTime::now_utc() - Duration::hours(1);
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = PostFormParams::valid(env, exp_key);
        p.expiration = past_exp;
        p.build()
    })
    .await?;
    assert_gateway_hardening_delta(
        &pair,
        scheme,
        "expired_timestamp",
        &leg,
        &gw,
        (403, Some("AccessDenied")),
        (403, "InvalidAccessKeyId"),
    );
    assert_object_absent_both_stacks(&pair, PRIVATE_BUCKET, exp_key, "post_policy/expired_timestamp").await?;

    // (4) future_skewed_timestamp (x-amz-date 20 min in the future -> 403 RequestTimeTooSkewed on both stacks)
    let skew_key = "post-future-skew.bin";
    let future_date = OffsetDateTime::now_utc() + Duration::minutes(20);
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = PostFormParams::valid(env, skew_key);
        p.form_timestamp = future_date;
        p.expiration = future_date + Duration::minutes(5);
        p.build()
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "future_skewed_timestamp",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["RequestTimeTooSkewed"]),
    );
    assert_object_absent_both_stacks(&pair, PRIVATE_BUCKET, skew_key, "post_policy/future_skewed_timestamp").await?;

    // (5) missing_host: 1.0.5 (`s3s`) accepted Host-less PostObject (204); gateway enforces 400 InvalidRequest.
    let no_host_key = "post-missing-host.bin";
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = PostFormParams::valid(env, no_host_key);
        p.include_host = false;
        p.build()
    })
    .await?;
    assert_gateway_hardening_delta(&pair, scheme, "missing_host", &leg, &gw, (204, None), (400, "InvalidRequest"));
    assert_object_absent_after_gateway_rejection(&pair, PRIVATE_BUCKET, no_host_key, "post_policy/missing_host").await?;

    // (6) duplicate_authorization (POST policy upload carrying duplicate Authorization headers):
    // 1.0.5 (`s3s`) ignored duplicate Authorization headers on PostObject (204); gateway enforces 400 InvalidRequest.
    let dup_auth_key = "post-dup-auth.bin";
    let (leg, gw) = send_pair(&pair, |env| {
        let hdr = SigV4Params::get_probe(env).build_header_signed(false, true);
        let auth = hdr
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
            .expect("authorization header")
            .1
            .clone();
        let mut p = PostFormParams::valid(env, dup_auth_key);
        p.extra_headers = vec![("Authorization".to_owned(), auth.clone()), ("Authorization".to_owned(), auth)];
        p.build()
    })
    .await?;
    assert_gateway_hardening_delta(&pair, scheme, "duplicate_authorization", &leg, &gw, (204, None), (400, "InvalidRequest"));
    assert_object_absent_after_gateway_rejection(&pair, PRIVATE_BUCKET, dup_auth_key, "post_policy/duplicate_authorization")
        .await?;

    // (7) wrong_service_scope (`ec2` and `kms` in x-amz-credential of POST policy -> 501 NotImplemented)
    for bad_service in ["ec2", "kms"] {
        let bad_scope_key = format!("post-bad-scope-{bad_service}.bin");
        let (leg, gw) = send_pair(&pair, |env| {
            let mut p = PostFormParams::valid(env, &bad_scope_key);
            p.service = bad_service;
            p.build()
        })
        .await?;
        assert_dual_stack_rejection(
            scheme,
            &format!("wrong_service_scope_{bad_service}"),
            pair.legacy_label,
            &leg,
            &gw,
            &[501],
            Some(&["NotImplemented"]),
        );
        assert_object_absent_both_stacks(&pair, PRIVATE_BUCKET, &bad_scope_key, "post_policy/wrong_service_scope").await?;
    }

    Ok(())
}

#[tokio::test]
async fn sec_signature_matrix_anonymous() -> TestResult {
    let pair = StackPair::spawn().await?;
    pair.seed_matrix_buckets().await?;
    let scheme = "anonymous";
    let private_target = format!("/{PRIVATE_BUCKET}/{PROBE_KEY}");
    let public_target = format!("/{PUBLIC_BUCKET}/{PROBE_KEY}");

    // (1) valid: anonymous GET succeeds on PUBLIC_BUCKET (200) and is denied on PRIVATE_BUCKET (403 AccessDenied)
    let (leg_pub, gw_pub) = send_pair(&pair, |_| RawHttpRequest {
        method: "GET",
        target: public_target.clone(),
        include_host: true,
        headers: Vec::new(),
        body: Vec::new(),
    })
    .await?;
    assert_eq!((leg_pub.status, gw_pub.status), (200, 200), "[{scheme}/valid_public] must succeed");
    assert_eq!(leg_pub.body.as_bytes(), PROBE_BODY);
    assert_eq!(gw_pub.body.as_bytes(), PROBE_BODY);

    let (leg_priv, gw_priv) = send_pair(&pair, |_| RawHttpRequest {
        method: "GET",
        target: private_target.clone(),
        include_host: true,
        headers: Vec::new(),
        body: Vec::new(),
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "valid_private_denied",
        pair.legacy_label,
        &leg_priv,
        &gw_priv,
        &[403],
        Some(&["AccessDenied"]),
    );

    // (2) wrong_signature: unauthenticated PUT against private bucket is rejected without writing
    let unauth_key = "anon-unauth-write.txt";
    let (leg, gw) = send_pair(&pair, |_| RawHttpRequest {
        method: "PUT",
        target: format!("/{PRIVATE_BUCKET}/{unauth_key}"),
        include_host: true,
        headers: Vec::new(),
        body: b"unauth-body".to_vec(),
    })
    .await?;
    assert_dual_stack_rejection(scheme, "wrong_signature", pair.legacy_label, &leg, &gw, &[403], Some(&["AccessDenied"]));
    assert_object_absent_both_stacks(&pair, PRIVATE_BUCKET, unauth_key, "anonymous/wrong_signature").await?;

    // (3) expired_timestamp: anonymous POST Object on PUBLIC_BUCKET with an expired unsigned policy
    let anon_exp_key = "anon-expired-post.bin";
    let past_exp = OffsetDateTime::now_utc() - Duration::hours(1);
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = PostFormParams::valid(env, anon_exp_key);
        p.bucket = PUBLIC_BUCKET;
        p.anonymous_unsigned = true;
        p.expiration = past_exp;
        p.build()
    })
    .await?;
    assert_dual_stack_rejection(scheme, "expired_timestamp", pair.legacy_label, &leg, &gw, &[403], Some(&["AccessDenied"]));
    assert_object_absent_both_stacks(&pair, PUBLIC_BUCKET, anon_exp_key, "anonymous/expired_timestamp").await?;

    // (4) future_skewed_timestamp: anonymous GET with future-skewed x-amz-date on private bucket stays denied
    let future_amz = amz_timestamp(OffsetDateTime::now_utc() + Duration::minutes(20));
    let (leg, gw) = send_pair(&pair, |_| RawHttpRequest {
        method: "GET",
        target: private_target.clone(),
        include_host: true,
        headers: vec![("x-amz-date".to_owned(), future_amz.clone())],
        body: Vec::new(),
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "future_skewed_timestamp",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["AccessDenied"]),
    );

    // (5) missing_host: 1.0.5 (`s3s`) served PUBLIC_BUCKET without Host (200); gateway enforces 400 InvalidRequest.
    let (leg, gw) = send_pair(&pair, |_| RawHttpRequest {
        method: "GET",
        target: public_target.clone(),
        include_host: false,
        headers: Vec::new(),
        body: Vec::new(),
    })
    .await?;
    assert_gateway_hardening_delta(&pair, scheme, "missing_host", &leg, &gw, (200, None), (400, "InvalidRequest"));

    // (6) duplicate_authorization: 1.0.5 (`s3s`) treated duplicate empty Authorization headers on PUBLIC_BUCKET
    // as anonymous (200); gateway rejects duplicate Authorization headers with 400 InvalidRequest.
    let (leg, gw) = send_pair(&pair, |_| RawHttpRequest {
        method: "GET",
        target: public_target.clone(),
        include_host: true,
        headers: vec![
            ("Authorization".to_owned(), String::new()),
            ("Authorization".to_owned(), String::new()),
        ],
        body: Vec::new(),
    })
    .await?;
    assert_gateway_hardening_delta(&pair, scheme, "duplicate_authorization", &leg, &gw, (200, None), (400, "InvalidRequest"));

    // (7) wrong_service_scope: anonymous requests to STS AssumeRole and Admin endpoints are denied
    let sts_body =
        b"Action=AssumeRole&Version=2011-06-15&RoleArn=arn:aws:iam::123456789012:role/test&RoleSessionName=anon".to_vec();
    let (leg, gw) = send_pair(&pair, |_| RawHttpRequest {
        method: "POST",
        target: "/".to_owned(),
        include_host: true,
        headers: vec![("Content-Type".to_owned(), "application/x-www-form-urlencoded".to_owned())],
        body: sts_body.clone(),
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "wrong_service_scope_sts",
        pair.legacy_label,
        &leg,
        &gw,
        &[400],
        Some(&["InvalidRequest"]),
    );

    let (leg, gw) = send_pair(&pair, |_| RawHttpRequest {
        method: "GET",
        target: "/rustfs/admin/v3/info".to_owned(),
        include_host: true,
        headers: Vec::new(),
        body: Vec::new(),
    })
    .await?;
    assert_dual_stack_rejection(
        scheme,
        "wrong_service_scope_admin",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["AccessDenied"]),
    );

    Ok(())
}

#[tokio::test]
async fn sec_signature_matrix_head_error_body_empty() -> TestResult {
    let pair = StackPair::spawn().await?;
    pair.seed_matrix_buckets().await?;
    let probe_path = format!("/{PRIVATE_BUCKET}/{PROBE_KEY}");
    let now = OffsetDateTime::now_utc();
    let past = now - Duration::minutes(20);
    let exp_past = (now.unix_timestamp() - 3600).to_string();

    for (case, build_fn) in [
        (
            "sigv4_header_wrong_sig",
            Box::new(|env: &RustFSTestEnvironment| {
                let mut p = SigV4Params::get_probe(env);
                p.method = "HEAD";
                p.build_header_signed(true, true)
            }) as Box<dyn Fn(&RustFSTestEnvironment) -> RawHttpRequest + Send + Sync>,
        ),
        (
            "sigv4_presigned_expired",
            Box::new(|env: &RustFSTestEnvironment| {
                let mut p = SigV4Params::get_probe(env);
                p.method = "HEAD";
                p.timestamp = now - Duration::hours(1);
                p.build_presigned(60, false, true)
            }),
        ),
        (
            "sigv2_header_skewed",
            Box::new(|env: &RustFSTestEnvironment| {
                let mut p = SigV2Params::get_probe(env);
                p.method = "HEAD";
                p.build_header(past)
            }),
        ),
        (
            "sigv2_presigned_expired",
            Box::new(|env: &RustFSTestEnvironment| {
                let mut p = SigV2Params::get_probe(env);
                p.method = "HEAD";
                p.build_presigned(&exp_past)
            }),
        ),
        (
            "anonymous_private_head",
            Box::new(|_: &RustFSTestEnvironment| RawHttpRequest {
                method: "HEAD",
                target: probe_path.clone(),
                include_host: true,
                headers: Vec::new(),
                body: Vec::new(),
            }),
        ),
    ] {
        let (leg, gw) = send_pair(&pair, |env| build_fn(env)).await?;
        assert_eq!((leg.status, gw.status), (403, 403), "[head/{case}] expected 403 on both stacks");
        assert!(leg.body.is_empty(), "[head/{case}] legacy HEAD error must have empty body");
        assert!(gw.body.is_empty(), "[head/{case}] gateway HEAD error must have empty body");
    }

    Ok(())
}

// ============================================================================
// 2. Historical GHSA Security Regressions (`*_both_stacks` suffix)
// ============================================================================

/// GHSA-xm99-m3gq-83g8 & GHSA-g8w9-qw9q-fghr (#7796 / #7425): unsigned `x-amz-*` headers on
/// header-signed and presigned SigV4 requests must be rejected with 403 `AccessDenied` on both
/// stacks without mutating destination objects or leaking private source objects.
#[tokio::test]
async fn ghsa_unsigned_amz_headers_rejected_both_stacks() -> TestResult {
    let pair = StackPair::spawn().await?;
    let src_bucket = "ghsa-xm99-src";
    let dst_bucket = "ghsa-xm99-dst";
    let src_body = b"ghsa-xm99-private-source-secret";
    let dst_initial = b"ghsa-xm99-original-destination";

    for env in [&pair.legacy, &pair.gateway] {
        let client = env.create_s3_client();
        client.create_bucket().bucket(src_bucket).send().await?;
        client.create_bucket().bucket(dst_bucket).send().await?;
        client
            .put_object()
            .bucket(src_bucket)
            .key("secret.txt")
            .body(ByteStream::from_static(src_body))
            .send()
            .await?;
        client
            .put_object()
            .bucket(dst_bucket)
            .key("target.txt")
            .body(ByteStream::from_static(dst_initial))
            .send()
            .await?;
    }

    let target_path = format!("/{dst_bucket}/target.txt");
    let copy_source = format!("/{src_bucket}/secret.txt");

    // (a) Header-signed SigV4 PutObject replayed with unsigned x-amz-copy-source and x-amz-tagging
    for (case, unsigned_hdr) in [
        ("header_unsigned_copy_source", ("x-amz-copy-source", copy_source.as_str())),
        ("header_unsigned_tagging", ("x-amz-tagging", "classification=public")),
    ] {
        let (leg, gw) = send_pair(&pair, |env| {
            let mut p = SigV4Params::get_probe(env);
            p.method = "PUT";
            p.path = &target_path;
            p.unsigned_headers = vec![unsigned_hdr];
            p.build_header_signed(false, true)
        })
        .await?;
        assert_dual_stack_rejection("ghsa_xm99", case, pair.legacy_label, &leg, &gw, &[403], Some(&["AccessDenied"]));
    }

    // (b) Presigned SigV4 PUT (`SignedHeaders=host`) replayed with unsigned x-amz-copy-source & x-amz-tagging
    let presigned_dst_key = "presigned-unwritten.txt";
    let presigned_dst_path = format!("/{dst_bucket}/{presigned_dst_key}");
    for (case, unsigned_hdr) in [
        ("presigned_put_unsigned_copy_source", ("x-amz-copy-source", copy_source.as_str())),
        ("presigned_put_unsigned_tagging", ("x-amz-tagging", "owner=attacker")),
    ] {
        let (leg, gw) = send_pair(&pair, |env| {
            let mut p = SigV4Params::get_probe(env);
            p.method = "PUT";
            p.path = &presigned_dst_path;
            p.unsigned_headers = vec![unsigned_hdr];
            p.body = b"attacker-body".to_vec();
            p.build_presigned(300, false, true)
        })
        .await?;
        assert_dual_stack_rejection("ghsa_g8w9", case, pair.legacy_label, &leg, &gw, &[403], Some(&["AccessDenied"]));
        assert_object_absent_both_stacks(&pair, dst_bucket, presigned_dst_key, case).await?;
    }

    // (c) Presigned SigV4 GET (`SignedHeaders=host`) replayed with unsigned SSE-C header
    let src_path = format!("/{src_bucket}/secret.txt");
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.method = "GET";
        p.path = &src_path;
        p.unsigned_headers = vec![("x-amz-server-side-encryption-customer-algorithm", "AES256")];
        p.build_presigned(300, false, true)
    })
    .await?;
    assert_dual_stack_rejection(
        "ghsa_g8w9",
        "presigned_get_unsigned_ssec",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["AccessDenied"]),
    );
    assert!(!gw.body.contains("ghsa-xm99-private-source-secret"));

    // Confirm existing target.txt was untouched on both stacks, then prove signed x-amz-copy-source succeeds
    for env in [&pair.legacy, &pair.gateway] {
        let existing = env
            .create_s3_client()
            .get_object()
            .bucket(dst_bucket)
            .key("target.txt")
            .send()
            .await?;
        assert_eq!(existing.body.collect().await?.into_bytes().as_ref(), dst_initial);
    }

    let (leg_ok, gw_ok) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.method = "PUT";
        p.path = &target_path;
        p.extra_signed_headers = vec![("x-amz-copy-source", copy_source.as_str())];
        p.build_header_signed(false, true)
    })
    .await?;
    assert_eq!((leg_ok.status, gw_ok.status), (200, 200), "signed CopyObject control must succeed");

    Ok(())
}

/// CVE-2025-31489 class: requests bearing an unknown `AccessKeyId` (signed with an empty secret or
/// a caller-forged secret) must always return 403 `InvalidAccessKeyId`, and a real `AccessKeyId`
/// signed with an empty secret must return 403 `SignatureDoesNotMatch`, across every scheme.
#[tokio::test]
async fn ghsa_unknown_or_empty_secret_rejected_both_stacks() -> TestResult {
    let pair = StackPair::spawn().await?;
    pair.seed_matrix_buckets().await?;
    let now = OffsetDateTime::now_utc();
    let valid_exp = (now.unix_timestamp() + 300).to_string();

    for (unknown_ak, forged_secret) in [
        ("AKIAUNKNOWNEMPTY0001", ""),
        ("AKIAUNKNOWNFORGED002", "attacker-chosen-secret-key"),
    ] {
        // SigV4 header
        let (leg, gw) = send_pair(&pair, |env| {
            let mut p = SigV4Params::get_probe(env);
            p.access_key = unknown_ak;
            p.secret_key = forged_secret;
            p.build_header_signed(false, true)
        })
        .await?;
        assert_dual_stack_rejection(
            "cve_31489",
            &format!("sigv4_header_{unknown_ak}"),
            pair.legacy_label,
            &leg,
            &gw,
            &[403],
            Some(&["InvalidAccessKeyId"]),
        );

        // SigV4 presigned
        let (leg, gw) = send_pair(&pair, |env| {
            let mut p = SigV4Params::get_probe(env);
            p.access_key = unknown_ak;
            p.secret_key = forged_secret;
            p.build_presigned(300, false, true)
        })
        .await?;
        assert_dual_stack_rejection(
            "cve_31489",
            &format!("sigv4_presigned_{unknown_ak}"),
            pair.legacy_label,
            &leg,
            &gw,
            &[403],
            Some(&["InvalidAccessKeyId"]),
        );

        // SigV2 header (computed directly so empty secret "" still emits `AWS <ak>:<hmac>`)
        let (leg, gw) = send_pair(&pair, |env| {
            let mut p = SigV2Params::get_probe(env);
            p.access_key = unknown_ak;
            p.secret_key = forged_secret;
            p.build_header(now)
        })
        .await?;
        assert_dual_stack_rejection(
            "cve_31489",
            &format!("sigv2_header_{unknown_ak}"),
            pair.legacy_label,
            &leg,
            &gw,
            &[403],
            Some(&["InvalidAccessKeyId"]),
        );

        // SigV2 presigned
        let (leg, gw) = send_pair(&pair, |env| {
            let mut p = SigV2Params::get_probe(env);
            p.access_key = unknown_ak;
            p.secret_key = forged_secret;
            p.build_presigned(&valid_exp)
        })
        .await?;
        assert_dual_stack_rejection(
            "cve_31489",
            &format!("sigv2_presigned_{unknown_ak}"),
            pair.legacy_label,
            &leg,
            &gw,
            &[403],
            Some(&["InvalidAccessKeyId"]),
        );

        // POST policy
        let post_key = format!("forged-{unknown_ak}.bin");
        let (leg, gw) = send_pair(&pair, |env| {
            let mut p = PostFormParams::valid(env, &post_key);
            p.access_key = unknown_ak;
            p.secret_key = forged_secret;
            p.build()
        })
        .await?;
        assert_dual_stack_rejection(
            "cve_31489",
            &format!("post_policy_{unknown_ak}"),
            pair.legacy_label,
            &leg,
            &gw,
            &[403],
            Some(&["InvalidAccessKeyId"]),
        );
        assert_object_absent_both_stacks(&pair, PRIVATE_BUCKET, &post_key, unknown_ak).await?;
    }

    // Known root AccessKeyId signed with an empty secret "" must be rejected with SignatureDoesNotMatch
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.secret_key = "";
        p.build_header_signed(false, true)
    })
    .await?;
    assert_dual_stack_rejection(
        "cve_31489",
        "known_ak_empty_secret_sigv4",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["SignatureDoesNotMatch"]),
    );

    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV2Params::get_probe(env);
        p.secret_key = "";
        p.build_header(now)
    })
    .await?;
    assert_dual_stack_rejection(
        "cve_31489",
        "known_ak_empty_secret_sigv2",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["SignatureDoesNotMatch"]),
    );

    Ok(())
}

/// `rustfs#4845`: a caller granted `s3:PutObject` on the destination bucket without `s3:GetObject`
/// on the source bucket must be denied (`403 AccessDenied`) on both `CopyObject` and
/// `UploadPartCopy` across both stacks.
#[tokio::test]
async fn ghsa_copy_source_authorization_enforced_both_stacks() -> TestResult {
    let pair = StackPair::spawn().await?;
    let src_bucket = "copy-authz-src";
    let dst_bucket = "copy-authz-dst";
    let writer_ak = "copywriteronly";
    let writer_sk = "CopyWriterSecret123!";
    let full_ak = "copyreaderwriter";
    let full_sk = "CopyFullSecret123!";

    let dst_only_policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:PutObject", "s3:AbortMultipartUpload"],
            "Resource": [format!("arn:aws:s3:::{dst_bucket}/*")]
        }]
    });
    let src_and_dst_policy = json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": [format!("arn:aws:s3:::{src_bucket}/*")]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:PutObject"],
                "Resource": [format!("arn:aws:s3:::{dst_bucket}/*")]
            }
        ]
    });

    for env in [&pair.legacy, &pair.gateway] {
        let root = env.create_s3_client();
        root.create_bucket().bucket(src_bucket).send().await?;
        root.create_bucket().bucket(dst_bucket).send().await?;
        root.put_object()
            .bucket(src_bucket)
            .key("confidential.txt")
            .body(ByteStream::from_static(b"source-confidential-bytes"))
            .send()
            .await?;
        create_scoped_iam_user(env, writer_ak, writer_sk, "dst-put-only", &dst_only_policy).await?;
        create_scoped_iam_user(env, full_ak, full_sk, "src-get-dst-put", &src_and_dst_policy).await?;

        // Sanity check: dst-only user CAN PutObject directly to dst_bucket
        let writer_client = env.create_s3_client_with_credentials(writer_ak, writer_sk);
        writer_client
            .put_object()
            .bucket(dst_bucket)
            .key("direct-allowed.txt")
            .body(ByteStream::from_static(b"allowed-direct-put"))
            .send()
            .await?;
    }

    // Attack 1: CopyObject by dst-only user must return 403 AccessDenied on both stacks
    let copy_dst_path = format!("/{dst_bucket}/exfiltrated.txt");
    let copy_src_hdr = format!("/{src_bucket}/confidential.txt");
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.method = "PUT";
        p.path = &copy_dst_path;
        p.access_key = writer_ak;
        p.secret_key = writer_sk;
        p.extra_signed_headers = vec![("x-amz-copy-source", copy_src_hdr.as_str())];
        p.build_header_signed(false, true)
    })
    .await?;
    assert_dual_stack_rejection(
        "issue_4845",
        "copy_object_without_source_get",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["AccessDenied"]),
    );
    assert_object_absent_both_stacks(&pair, dst_bucket, "exfiltrated.txt", "copy_object_without_source_get").await?;

    // Attack 2: UploadPartCopy by dst-only user must also return 403 AccessDenied on both stacks
    for (label, env) in [(pair.legacy_label, &pair.legacy), ("gateway", &pair.gateway)] {
        let writer_client = env.create_s3_client_with_credentials(writer_ak, writer_sk);
        let mpu = writer_client
            .create_multipart_upload()
            .bucket(dst_bucket)
            .key("mpu-exfiltrated.txt")
            .send()
            .await?;
        let upload_id = mpu.upload_id().expect("multipart upload ID");
        let err = writer_client
            .upload_part_copy()
            .bucket(dst_bucket)
            .key("mpu-exfiltrated.txt")
            .upload_id(upload_id)
            .part_number(1)
            .copy_source(format!("{src_bucket}/confidential.txt"))
            .send()
            .await
            .expect_err(&format!("UploadPartCopy without source GetObject must fail on {label}"));
        assert_eq!(err.raw_response().map(|r| r.status().as_u16()), Some(403));
        assert_eq!(err.as_service_error().and_then(ProvideErrorMetadata::code), Some("AccessDenied"));
        writer_client
            .abort_multipart_upload()
            .bucket(dst_bucket)
            .key("mpu-exfiltrated.txt")
            .upload_id(upload_id)
            .send()
            .await?;
    }

    // Positive control: user with GetObject on src + PutObject on dst succeeds on both stacks
    let ok_dst_path = format!("/{dst_bucket}/authorized-copy.txt");
    let (leg_ok, gw_ok) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.method = "PUT";
        p.path = &ok_dst_path;
        p.access_key = full_ak;
        p.secret_key = full_sk;
        p.extra_signed_headers = vec![("x-amz-copy-source", copy_src_hdr.as_str())];
        p.build_header_signed(false, true)
    })
    .await?;
    assert_eq!((leg_ok.status, gw_ok.status), (200, 200), "authorized CopyObject must succeed");

    Ok(())
}

/// GHSA-3ppv-fx5m-m749 (`rustfs#5142`): a caller granted `s3:GetObject` (without
/// `s3:GetObjectVersion`) can read the current object without `versionId`, but must be denied
/// (`403 AccessDenied`) when requesting `GetObject?versionId=...` or versioned `CopyObject`.
#[tokio::test]
async fn ghsa_versioned_get_requires_get_object_version_both_stacks() -> TestResult {
    let pair = StackPair::spawn().await?;
    let bucket = "ghsa-3ppv-versioned";
    let key = "record.txt";
    let v1_body = b"v1-retired-secret-record";
    let v2_body = b"v2-current-public-record";
    let get_only_ak = "getobjectonly";
    let get_only_sk = "GetObjectOnlySecret123!";
    let ver_reader_ak = "getversionreader";
    let ver_reader_sk = "GetVersionSecret123!";

    let get_only_policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": [format!("arn:aws:s3:::{bucket}/*")]
        }]
    });
    let version_policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:GetObjectVersion"],
            "Resource": [format!("arn:aws:s3:::{bucket}/*")]
        }]
    });

    let mut v1_ids = Vec::new();
    for env in [&pair.legacy, &pair.gateway] {
        let root = env.create_s3_client();
        root.create_bucket().bucket(bucket).send().await?;
        root.put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await?;
        let v1 = root
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(v1_body))
            .send()
            .await?;
        let v1_id = v1
            .version_id()
            .ok_or("versioned PutObject must return v1 version_id")?
            .to_owned();
        root.put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(v2_body))
            .send()
            .await?;
        v1_ids.push(v1_id);

        create_scoped_iam_user(env, get_only_ak, get_only_sk, "get-object-only", &get_only_policy).await?;
        create_scoped_iam_user(env, ver_reader_ak, ver_reader_sk, "get-object-version", &version_policy).await?;
    }
    let (v1_legacy, v1_gateway) = (&v1_ids[0], &v1_ids[1]);

    // Sanity check: unversioned GET succeeds with s3:GetObject on both stacks and returns v2
    let obj_path = format!("/{bucket}/{key}");
    let (leg_cur, gw_cur) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.path = &obj_path;
        p.access_key = get_only_ak;
        p.secret_key = get_only_sk;
        p.build_header_signed(false, true)
    })
    .await?;
    assert_eq!((leg_cur.status, gw_cur.status), (200, 200));
    assert_eq!(leg_cur.body.as_bytes(), v2_body);
    assert_eq!(gw_cur.body.as_bytes(), v2_body);

    // Attack 1: GetObject?versionId=<v1> with only s3:GetObject must fail with 403 AccessDenied
    let leg_ver_req = {
        let mut p = SigV4Params::get_probe(&pair.legacy);
        p.path = &obj_path;
        p.query = vec![("versionId", v1_legacy.as_str())];
        p.access_key = get_only_ak;
        p.secret_key = get_only_sk;
        p.build_header_signed(false, true)
    };
    let gw_ver_req = {
        let mut p = SigV4Params::get_probe(&pair.gateway);
        p.path = &obj_path;
        p.query = vec![("versionId", v1_gateway.as_str())];
        p.access_key = get_only_ak;
        p.secret_key = get_only_sk;
        p.build_header_signed(false, true)
    };
    let (leg_ver, gw_ver) = tokio::try_join!(
        send_raw_http1(&pair.legacy.address, &leg_ver_req),
        send_raw_http1(&pair.gateway.address, &gw_ver_req),
    )?;
    assert_dual_stack_rejection(
        "ghsa_3ppv",
        "versioned_get_without_get_object_version",
        pair.legacy_label,
        &leg_ver,
        &gw_ver,
        &[403],
        Some(&["AccessDenied"]),
    );
    assert!(!gw_ver.body.contains("v1-retired-secret-record"));

    // Attack 2: CopyObject with ?versionId=<v1> in x-amz-copy-source must also fail with 403 AccessDenied
    let copy_dst = format!("/{bucket}/copied-v1.txt");
    let leg_src = format!("/{bucket}/{key}?versionId={}", aws_uri_encode(v1_legacy));
    let gw_src = format!("/{bucket}/{key}?versionId={}", aws_uri_encode(v1_gateway));
    let leg_copy_req = {
        let mut p = SigV4Params::get_probe(&pair.legacy);
        p.method = "PUT";
        p.path = &copy_dst;
        p.access_key = get_only_ak;
        p.secret_key = get_only_sk;
        p.extra_signed_headers = vec![("x-amz-copy-source", leg_src.as_str())];
        p.build_header_signed(false, true)
    };
    let gw_copy_req = {
        let mut p = SigV4Params::get_probe(&pair.gateway);
        p.method = "PUT";
        p.path = &copy_dst;
        p.access_key = get_only_ak;
        p.secret_key = get_only_sk;
        p.extra_signed_headers = vec![("x-amz-copy-source", gw_src.as_str())];
        p.build_header_signed(false, true)
    };
    let (leg_cp, gw_cp) = tokio::try_join!(
        send_raw_http1(&pair.legacy.address, &leg_copy_req),
        send_raw_http1(&pair.gateway.address, &gw_copy_req),
    )?;
    assert_dual_stack_rejection(
        "ghsa_3ppv",
        "versioned_copy_without_get_object_version",
        pair.legacy_label,
        &leg_cp,
        &gw_cp,
        &[403],
        Some(&["AccessDenied"]),
    );
    assert_object_absent_both_stacks(&pair, bucket, "copied-v1.txt", "ghsa_3ppv_copy").await?;

    // Positive control: user with s3:GetObjectVersion reads v1 on both stacks
    let leg_ok_req = {
        let mut p = SigV4Params::get_probe(&pair.legacy);
        p.path = &obj_path;
        p.query = vec![("versionId", v1_legacy.as_str())];
        p.access_key = ver_reader_ak;
        p.secret_key = ver_reader_sk;
        p.build_header_signed(false, true)
    };
    let gw_ok_req = {
        let mut p = SigV4Params::get_probe(&pair.gateway);
        p.path = &obj_path;
        p.query = vec![("versionId", v1_gateway.as_str())];
        p.access_key = ver_reader_ak;
        p.secret_key = ver_reader_sk;
        p.build_header_signed(false, true)
    };
    let (leg_ok, gw_ok) = tokio::try_join!(
        send_raw_http1(&pair.legacy.address, &leg_ok_req),
        send_raw_http1(&pair.gateway.address, &gw_ok_req),
    )?;
    assert_eq!((leg_ok.status, gw_ok.status), (200, 200));
    assert_eq!(leg_ok.body.as_bytes(), v1_body);
    assert_eq!(gw_ok.body.as_bytes(), v1_body);

    Ok(())
}

/// GHSA-m77q-r63m-pj89 / STS regression: forged or wrong-secret STS `AssumeRole` calls, unauthorized
/// IAM callers lacking `sts:AssumeRole`, and forged/tampered `X-Amz-Security-Token` session tokens
/// must be rejected with 403 across both stacks.
#[tokio::test]
async fn ghsa_sts_forged_or_wrong_secret_rejected_both_stacks() -> TestResult {
    let pair = StackPair::spawn().await?;
    pair.seed_matrix_buckets().await?;

    let allowed_sts_ak = "stsalloweduser";
    let allowed_sts_sk = "StsAllowedSecret123!";
    let denied_sts_ak = "stsdenieduser";
    let denied_sts_sk = "StsDeniedSecret123!";

    let allow_sts_policy = json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["sts:AssumeRole"],
                "Resource": ["*"]
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": [format!("arn:aws:s3:::{PRIVATE_BUCKET}/*")]
            }
        ]
    });
    let deny_sts_policy = json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": [format!("arn:aws:s3:::{PRIVATE_BUCKET}/*")]
            },
            {
                "Effect": "Deny",
                "Action": ["sts:AssumeRole"],
                "Resource": ["arn:aws:s3:::*"]
            }
        ]
    });

    for env in [&pair.legacy, &pair.gateway] {
        create_scoped_iam_user(env, allowed_sts_ak, allowed_sts_sk, "allow-sts-assume", &allow_sts_policy).await?;
        create_scoped_iam_user(env, denied_sts_ak, denied_sts_sk, "no-sts-assume", &deny_sts_policy).await?;
    }

    let assume_body =
        b"Action=AssumeRole&Version=2011-06-15&RoleArn=arn:aws:iam::123456789012:role/sec&RoleSessionName=probe".to_vec();

    // (a) Wrong secret on STS AssumeRole -> 403 SignatureDoesNotMatch
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.method = "POST";
        p.path = "/";
        p.access_key = allowed_sts_ak;
        p.secret_key = "wrong-sts-secret-key";
        p.service = "sts";
        p.extra_signed_headers = vec![("content-type", "application/x-www-form-urlencoded")];
        p.body = assume_body.clone();
        p.build_header_signed(false, true)
    })
    .await?;
    assert_dual_stack_rejection(
        "ghsa_m77q_sts",
        "assume_role_wrong_secret",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["SignatureDoesNotMatch"]),
    );

    // (b) Tampered signature on STS AssumeRole -> 403 SignatureDoesNotMatch
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.method = "POST";
        p.path = "/";
        p.access_key = allowed_sts_ak;
        p.secret_key = allowed_sts_sk;
        p.service = "sts";
        p.extra_signed_headers = vec![("content-type", "application/x-www-form-urlencoded")];
        p.body = assume_body.clone();
        p.build_header_signed(true, true)
    })
    .await?;
    assert_dual_stack_rejection(
        "ghsa_m77q_sts",
        "assume_role_tampered_sig",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["SignatureDoesNotMatch"]),
    );

    // (c) Unknown access key on STS AssumeRole -> 403 InvalidAccessKeyId
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.method = "POST";
        p.path = "/";
        p.access_key = "AKIASTSUNKNOWN000001";
        p.secret_key = "forged-sts-secret";
        p.service = "sts";
        p.extra_signed_headers = vec![("content-type", "application/x-www-form-urlencoded")];
        p.body = assume_body.clone();
        p.build_header_signed(false, true)
    })
    .await?;
    assert_dual_stack_rejection(
        "ghsa_m77q_sts",
        "assume_role_unknown_key",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["InvalidAccessKeyId"]),
    );

    // (d) Caller without `sts:AssumeRole` permission -> 403 AccessDenied
    let (leg, gw) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.method = "POST";
        p.path = "/";
        p.access_key = denied_sts_ak;
        p.secret_key = denied_sts_sk;
        p.service = "sts";
        p.extra_signed_headers = vec![("content-type", "application/x-www-form-urlencoded")];
        p.body = assume_body.clone();
        p.build_header_signed(false, true)
    })
    .await?;
    assert_dual_stack_rejection(
        "ghsa_m77q_sts",
        "assume_role_unauthorized_user",
        pair.legacy_label,
        &leg,
        &gw,
        &[403],
        Some(&["AccessDenied"]),
    );

    // (e) Positive control: authorized AssumeRole mints usable temporary credentials, whereas a
    // forged/tampered session token or wrong temporary secret is rejected with 403 on both stacks.
    let (leg_mint, gw_mint) = send_pair(&pair, |env| {
        let mut p = SigV4Params::get_probe(env);
        p.method = "POST";
        p.path = "/";
        p.access_key = allowed_sts_ak;
        p.secret_key = allowed_sts_sk;
        p.service = "sts";
        p.extra_signed_headers = vec![("content-type", "application/x-www-form-urlencoded")];
        p.body = assume_body.clone();
        p.build_header_signed(false, true)
    })
    .await?;
    assert_eq!((leg_mint.status, gw_mint.status), (200, 200), "authorized AssumeRole must succeed");
    let (leg_ak, leg_sk, leg_tok) = parse_assume_role_credentials(&leg_mint.body)?;
    let (gw_ak, gw_sk, gw_tok) = parse_assume_role_credentials(&gw_mint.body)?;

    // Valid temporary credentials read PRIVATE_BUCKET/PROBE_KEY on both stacks
    for (env, ak, sk, tok) in [
        (&pair.legacy, &leg_ak, &leg_sk, &leg_tok),
        (&pair.gateway, &gw_ak, &gw_sk, &gw_tok),
    ] {
        let req = {
            let mut p = SigV4Params::get_probe(env);
            p.access_key = ak;
            p.secret_key = sk;
            p.extra_signed_headers = vec![("x-amz-security-token", tok.as_str())];
            p.build_header_signed(false, true)
        };
        let resp = send_raw_http1(&env.address, &req).await?;
        assert_eq!(resp.status, 200, "minted STS session must read authorized S3 object: {}", resp.body);
        assert_eq!(resp.body.as_bytes(), PROBE_BODY);
    }

    // Tampered X-Amz-Security-Token with valid temporary AK/SK -> 403 on both stacks
    let forge_token = |tok: &str| -> String {
        let mut chars: Vec<char> = tok.chars().collect();
        let last = chars.len() - 1;
        chars[last] = if chars[last] == 'a' { 'b' } else { 'a' };
        chars.into_iter().collect()
    };
    let forged_leg_tok = forge_token(&leg_tok);
    let forged_gw_tok = forge_token(&gw_tok);
    let leg_bad_tok_req = {
        let mut p = SigV4Params::get_probe(&pair.legacy);
        p.access_key = &leg_ak;
        p.secret_key = &leg_sk;
        p.extra_signed_headers = vec![("x-amz-security-token", forged_leg_tok.as_str())];
        p.build_header_signed(false, true)
    };
    let gw_bad_tok_req = {
        let mut p = SigV4Params::get_probe(&pair.gateway);
        p.access_key = &gw_ak;
        p.secret_key = &gw_sk;
        p.extra_signed_headers = vec![("x-amz-security-token", forged_gw_tok.as_str())];
        p.build_header_signed(false, true)
    };
    let (leg_tok_rej, gw_tok_rej) = tokio::try_join!(
        send_raw_http1(&pair.legacy.address, &leg_bad_tok_req),
        send_raw_http1(&pair.gateway.address, &gw_bad_tok_req),
    )?;
    assert_gateway_hardening_delta(
        &pair,
        "ghsa_m77q_sts",
        "tampered_session_token_s3",
        &leg_tok_rej,
        &gw_tok_rej,
        (400, Some("InvalidRequest")),
        (403, "InvalidAccessKeyId"),
    );

    // Also verify aws_sdk_sts client surfaces typed 403 SignatureDoesNotMatch on wrong secret
    for env in [&pair.legacy, &pair.gateway] {
        let bad_sts = build_test_sts_client(&env.url, allowed_sts_ak, "wrong-sdk-sts-secret", None, "sec-sts-bad");
        let err = bad_sts
            .assume_role()
            .role_arn("arn:aws:iam::123456789012:role/sec")
            .role_session_name(format!("sdk-{}", Uuid::new_v4().simple()))
            .send()
            .await
            .expect_err("aws-sdk-sts with wrong secret must fail");
        assert_eq!(err.raw_response().map(|r| r.status().as_u16()), Some(403));
        assert_eq!(
            err.as_service_error()
                .and_then(aws_sdk_sts::error::ProvideErrorMetadata::code),
            Some("SignatureDoesNotMatch")
        );
    }

    Ok(())
}
