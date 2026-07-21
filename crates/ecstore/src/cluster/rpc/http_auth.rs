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

//! Internode RPC HMAC authentication.
//!
//! # Security regression coverage
//!
//! GHSA-r5qv-rc46-hv8q (internode RPC authentication must fail closed, fixed in
//! rustfs/rustfs#4402) is anchored by the `ghsa_r5qv_*` tests in the module
//! below, plus the broader negative-signature suite. The advisory class is: a
//! node must never accept an RPC whose auth is missing, malformed, or signed
//! with the default/empty shared secret. Body-bound v2 requests additionally
//! receive process-local replay protection. See
//! `docs/testing/security-regressions.md` for the full advisory -> test map.
//!
//! Advisory: <https://github.com/rustfs/rustfs/security/advisories/GHSA-r5qv-rc46-hv8q>

use crate::cluster::rpc::context_propagation::{inject_request_id_into_http_headers, inject_trace_context_into_http_headers};
use base64::Engine as _;
use base64::engine::general_purpose;
use hmac::{Hmac, KeyInit, Mac};
use http::uri::Authority;
use http::{HeaderMap, HeaderValue, Method, Uri};
#[cfg(test)]
use rustfs_credentials::{DEFAULT_SECRET_KEY, RPC_SECRET_REQUIRED_MESSAGE};
use rustfs_credentials::{RPC_SECRET_REQUIRED_OPERATOR_MESSAGE, try_get_rpc_token};
use sha2::Digest as _;
use sha2::Sha256;
use std::collections::{HashSet, VecDeque};
use std::sync::{LazyLock, Mutex, Once};
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tracing::error;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

const SIGNATURE_HEADER: &str = "x-rustfs-signature";
const TIMESTAMP_HEADER: &str = "x-rustfs-timestamp";
const RPC_AUTH_VERSION_HEADER: &str = "x-rustfs-rpc-auth-version";
const RPC_SIGNATURE_V2_HEADER: &str = "x-rustfs-rpc-signature-v2";
const RPC_NONCE_HEADER: &str = "x-rustfs-rpc-nonce";
pub(crate) const RPC_CONTENT_SHA256_HEADER: &str = "x-rustfs-content-sha256";
const RPC_AUTH_VERSION_V2: &str = "2";
const RPC_RESPONSE_PROOF_DOMAIN: &[u8] = b"rustfs-rpc-response-proof-v1\0";
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
const UNSIGNED_PAYLOAD_NONCE: &str = "unsigned";
const SIGNATURE_VALID_DURATION: i64 = 300; // 5 minutes
const REPLAY_CACHE_RETENTION: Duration = Duration::from_secs(601);
const MAX_REPLAY_PROTECTED_NONCES: usize = 65_536;
pub const TONIC_RPC_PREFIX: &str = "/node_service.NodeService";
static RPC_SECRET_RESOLUTION_LOG_ONCE: Once = Once::new();

#[derive(Default)]
struct RpcNonceCache {
    nonces: HashSet<Uuid>,
    expirations: VecDeque<(Instant, i64, Uuid)>,
    max_wall_time: i64,
}

impl RpcNonceCache {
    fn remove_expired(&mut self, now: Instant, wall_time: i64) {
        while matches!(
            self.expirations.front(),
            Some((expires_at, valid_until, _)) if *expires_at < now && *valid_until < wall_time
        ) {
            let Some((_, _, nonce)) = self.expirations.pop_front() else {
                break;
            };
            self.nonces.remove(&nonce);
        }
    }

    fn check_and_record(
        &mut self,
        nonce: Uuid,
        signed_at: i64,
        now: Instant,
        wall_time: i64,
        expires_at: Instant,
        capacity: usize,
    ) -> std::io::Result<()> {
        self.max_wall_time = self.max_wall_time.max(wall_time);
        if self.max_wall_time.saturating_sub(signed_at) > SIGNATURE_VALID_DURATION {
            return Err(std::io::Error::other("RPC request timestamp expired after clock regression"));
        }
        self.remove_expired(now, self.max_wall_time);
        if self.nonces.contains(&nonce) {
            return Err(std::io::Error::other("RPC request replay detected"));
        }
        if self.nonces.len() >= capacity {
            return Err(std::io::Error::other("RPC replay cache capacity exceeded"));
        }
        self.nonces.insert(nonce);
        self.expirations
            .push_back((expires_at, signed_at.saturating_add(SIGNATURE_VALID_DURATION), nonce));
        Ok(())
    }
}

// This cache is a process-local wire-replay defense only. Mutation handlers
// still need a stable operation ID and coordinator-owned idempotency across
// retries, node failover, and restart.
static LOCAL_RPC_NONCE_CACHE: LazyLock<Mutex<RpcNonceCache>> = LazyLock::new(|| Mutex::new(RpcNonceCache::default()));

/// Get the shared secret for HMAC signing
#[cfg(test)]
fn resolve_shared_secret(env_secret: Option<&str>, global_secret: Option<&str>) -> std::io::Result<String> {
    if let Some(secret) = env_secret.map(str::trim).filter(|secret| !secret.is_empty()) {
        return (secret != DEFAULT_SECRET_KEY)
            .then(|| secret.to_string())
            .ok_or_else(|| std::io::Error::other(RPC_SECRET_REQUIRED_MESSAGE));
    }

    global_secret
        .map(str::trim)
        .filter(|secret| !secret.is_empty() && *secret != DEFAULT_SECRET_KEY)
        .map(ToOwned::to_owned)
        .ok_or_else(|| std::io::Error::other(RPC_SECRET_REQUIRED_MESSAGE))
}

fn get_shared_secret() -> std::io::Result<String> {
    try_get_rpc_token().map_err(|err| {
        RPC_SECRET_RESOLUTION_LOG_ONCE.call_once(|| {
            error!("RPC auth secret resolution failed: {}; {}", err, RPC_SECRET_REQUIRED_OPERATOR_MESSAGE);
        });
        err
    })
}

fn rpc_response_proof_mac(canonical_body: &[u8]) -> std::io::Result<HmacSha256> {
    let secret = get_shared_secret()?;
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).map_err(|_| std::io::Error::other("Invalid RPC HMAC key"))?;
    mac.update(RPC_RESPONSE_PROOF_DOMAIN);
    mac.update(
        &u64::try_from(canonical_body.len())
            .map_err(|_| std::io::Error::other("RPC response proof length cannot be represented"))?
            .to_be_bytes(),
    );
    mac.update(canonical_body);
    Ok(mac)
}

pub fn sign_tonic_rpc_response_proof(canonical_body: &[u8]) -> std::io::Result<Vec<u8>> {
    Ok(rpc_response_proof_mac(canonical_body)?.finalize().into_bytes().to_vec())
}

pub fn verify_tonic_rpc_response_proof(canonical_body: &[u8], proof: &[u8]) -> std::io::Result<()> {
    rpc_response_proof_mac(canonical_body)?
        .verify_slice(proof)
        .map_err(|_| std::io::Error::other("Invalid RPC response proof"))
}

/// Build the canonical payload covered by the RPC HMAC.
fn signature_payload(url: &str, method: &Method, timestamp: i64) -> String {
    let uri: Uri = url.parse().expect("Invalid URL");

    let path_and_query = uri.path_and_query().unwrap();

    let url = path_and_query.to_string();

    format!("{url}|{method}|{timestamp}")
}

fn redacted_rpc_path(url: &str) -> String {
    url.parse::<Uri>()
        .ok()
        .map(|uri| uri.path().to_string())
        .unwrap_or_else(|| "<invalid-rpc-url>".to_string())
}

/// Generate HMAC-SHA256 signature for the given data
fn generate_signature(secret: &str, url: &str, method: &Method, timestamp: i64) -> String {
    let data = signature_payload(url, method, timestamp);
    let mut mac = <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(data.as_bytes());
    let result = mac.finalize();
    general_purpose::STANDARD.encode(result.into_bytes())
}

fn verify_signature(secret: &str, url: &str, method: &Method, timestamp: i64, signature: &str) -> bool {
    let Ok(signature) = general_purpose::STANDARD.decode(signature) else {
        return false;
    };

    let data = signature_payload(url, method, timestamp);
    let mut mac = <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(data.as_bytes());
    mac.verify_slice(&signature).is_ok()
}

#[derive(Clone, Copy)]
struct SignatureV2Scope<'a> {
    audience: &'a str,
    service: &'a str,
    rpc_method: &'a str,
    timestamp: &'a str,
    nonce: &'a str,
    content_sha256: &'a str,
}

fn update_signature_v2(mac: &mut HmacSha256, scope: SignatureV2Scope<'_>) {
    for part in [
        b"rustfs-rpc-auth-v2|".as_slice(),
        scope.audience.as_bytes(),
        b"|/",
        scope.service.as_bytes(),
        b"/",
        scope.rpc_method.as_bytes(),
        b"|POST|",
        scope.timestamp.as_bytes(),
        b"|",
        scope.nonce.as_bytes(),
        b"|",
        scope.content_sha256.as_bytes(),
    ] {
        mac.update(part);
    }
}

fn generate_signature_v2(secret: &str, scope: SignatureV2Scope<'_>) -> std::io::Result<String> {
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()).map_err(|_| std::io::Error::other("Invalid RPC HMAC key"))?;
    update_signature_v2(&mut mac, scope);
    Ok(general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

fn verify_signature_v2(secret: &str, scope: SignatureV2Scope<'_>, signature: &str) -> bool {
    let Ok(signature) = general_purpose::STANDARD.decode(signature) else {
        return false;
    };
    let Ok(mut mac) = <HmacSha256 as KeyInit>::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    update_signature_v2(&mut mac, scope);
    mac.verify_slice(&signature).is_ok()
}

fn valid_content_sha256(value: &str) -> bool {
    value == UNSIGNED_PAYLOAD
        || (value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
}

fn header_value(value: &str, name: &str) -> std::io::Result<HeaderValue> {
    HeaderValue::from_str(value).map_err(|_| std::io::Error::other(format!("Invalid {name} header value")))
}

pub fn normalize_tonic_rpc_audience(value: &str) -> std::io::Result<String> {
    let authority = value
        .parse::<Authority>()
        .map_err(|_| std::io::Error::other("Invalid gRPC peer authority"))?;
    Ok(authority.as_str().to_ascii_lowercase())
}

fn check_timestamp(timestamp: i64) -> std::io::Result<()> {
    let current_time = OffsetDateTime::now_utc().unix_timestamp();
    if current_time.saturating_sub(timestamp) > SIGNATURE_VALID_DURATION
        || timestamp.saturating_sub(current_time) > SIGNATURE_VALID_DURATION
    {
        return Err(std::io::Error::other("Request timestamp expired"));
    }
    Ok(())
}

fn check_and_record_nonce(nonce: Uuid, signed_at: i64) -> std::io::Result<()> {
    let wall_time = OffsetDateTime::now_utc().unix_timestamp();
    let mut cache = LOCAL_RPC_NONCE_CACHE
        .lock()
        .map_err(|_| std::io::Error::other("RPC replay cache unavailable"))?;
    // Take the monotonic timestamp after acquiring the lock so expiration
    // entries remain ordered by the same serialization point as insertion.
    let now = Instant::now();
    let expires_at = now
        .checked_add(REPLAY_CACHE_RETENTION)
        .ok_or_else(|| std::io::Error::other("RPC replay expiry overflow"))?;
    cache.check_and_record(nonce, signed_at, now, wall_time, expires_at, MAX_REPLAY_PROTECTED_NONCES)
}

/// Build headers with authentication signature
pub fn build_auth_headers(url: &str, method: &Method, headers: &mut HeaderMap) -> std::io::Result<()> {
    let auth_headers = gen_signature_headers(url, method)?;

    headers.extend(auth_headers);
    inject_trace_context_into_http_headers(headers);
    inject_request_id_into_http_headers(headers);
    Ok(())
}

pub fn gen_signature_headers(url: &str, method: &Method) -> std::io::Result<HeaderMap> {
    let secret = get_shared_secret()?;
    let timestamp = OffsetDateTime::now_utc().unix_timestamp();

    let signature = generate_signature(&secret, url, method, timestamp);

    let mut headers = HeaderMap::new();
    headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&signature).expect("Invalid header value"));
    headers.insert(
        TIMESTAMP_HEADER,
        HeaderValue::from_str(&timestamp.to_string()).expect("Invalid header value"),
    );

    Ok(headers)
}

/// Generate rolling-upgrade-safe gRPC auth metadata.
///
/// The legacy signature remains present for old servers. New servers prefer the
/// v2 signature and bind it to the destination authority and exact generated
/// gRPC method. A versioned canonical mutation payload can opt into the
/// additional body-digest capability.
pub fn gen_tonic_signature_headers(
    audience: &str,
    service: &str,
    rpc_method: &str,
    content_sha256: Option<&str>,
) -> std::io::Result<HeaderMap> {
    if audience.is_empty() || service.is_empty() || rpc_method.is_empty() || service.contains('/') || rpc_method.contains('/') {
        return Err(std::io::Error::other("Invalid RPC v2 signing scope"));
    }
    let content_sha256 = content_sha256.unwrap_or(UNSIGNED_PAYLOAD);
    if !valid_content_sha256(content_sha256) {
        return Err(std::io::Error::other("Invalid RPC content SHA-256"));
    }

    let secret = get_shared_secret()?;
    let timestamp = OffsetDateTime::now_utc().unix_timestamp();
    let timestamp_header = timestamp.to_string();
    let body_nonce = (content_sha256 != UNSIGNED_PAYLOAD).then(|| Uuid::new_v4().to_string());
    let nonce = body_nonce.as_deref().unwrap_or(UNSIGNED_PAYLOAD_NONCE);
    let legacy_signature = generate_signature(&secret, TONIC_RPC_PREFIX, &Method::GET, timestamp);
    let signature_v2 = generate_signature_v2(
        &secret,
        SignatureV2Scope {
            audience,
            service,
            rpc_method,
            timestamp: &timestamp_header,
            nonce,
            content_sha256,
        },
    )?;

    let mut headers = HeaderMap::new();
    headers.insert(SIGNATURE_HEADER, header_value(&legacy_signature, SIGNATURE_HEADER)?);
    headers.insert(TIMESTAMP_HEADER, header_value(&timestamp_header, TIMESTAMP_HEADER)?);
    headers.insert(RPC_AUTH_VERSION_HEADER, HeaderValue::from_static(RPC_AUTH_VERSION_V2));
    headers.insert(RPC_SIGNATURE_V2_HEADER, header_value(&signature_v2, RPC_SIGNATURE_V2_HEADER)?);
    headers.insert(RPC_NONCE_HEADER, header_value(nonce, RPC_NONCE_HEADER)?);
    headers.insert(RPC_CONTENT_SHA256_HEADER, header_value(content_sha256, RPC_CONTENT_SHA256_HEADER)?);
    Ok(headers)
}

/// Bind a mutation to a versioned, deterministic canonical payload.
///
/// Do not pass a protobuf re-encoding here: unknown fields and map ordering are
/// not a stable mixed-version contract.
pub fn set_tonic_canonical_body_digest<T>(request: &mut tonic::Request<T>, canonical_body: &[u8]) -> std::io::Result<()> {
    let digest = hex_simd::encode_to_string(Sha256::digest(canonical_body), hex_simd::AsciiCase::Lower);
    request
        .metadata_mut()
        .as_mut()
        .insert(RPC_CONTENT_SHA256_HEADER, header_value(&digest, RPC_CONTENT_SHA256_HEADER)?);
    Ok(())
}

pub fn verify_tonic_canonical_body_digest<T>(request: &tonic::Request<T>, canonical_body: &[u8]) -> std::io::Result<()> {
    let version = request
        .metadata()
        .get(RPC_AUTH_VERSION_HEADER)
        .and_then(|value| value.to_str().ok());
    if version != Some(RPC_AUTH_VERSION_V2) {
        return Err(std::io::Error::other("RPC mutation requires v2 authentication"));
    }
    let expected = request
        .metadata()
        .get(RPC_CONTENT_SHA256_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC content SHA-256"))?;
    if expected == UNSIGNED_PAYLOAD || !valid_content_sha256(expected) {
        return Err(std::io::Error::other("RPC body is not bound to the signature"));
    }
    let actual = hex_simd::encode_to_string(Sha256::digest(canonical_body), hex_simd::AsciiCase::Lower);
    if actual != expected {
        return Err(std::io::Error::other("RPC content SHA-256 mismatch"));
    }
    Ok(())
}

fn has_v2_auth_headers(headers: &HeaderMap) -> bool {
    [
        RPC_AUTH_VERSION_HEADER,
        RPC_SIGNATURE_V2_HEADER,
        RPC_NONCE_HEADER,
        RPC_CONTENT_SHA256_HEADER,
    ]
    .iter()
    .any(|name| headers.contains_key(*name))
}

/// Verify gRPC authentication, preferring v2 without downgrade on malformed v2 metadata.
pub fn verify_tonic_rpc_signature(audience: &str, path: &str, headers: &HeaderMap) -> std::io::Result<()> {
    if !has_v2_auth_headers(headers) {
        // RUSTFS_COMPAT_TODO(heal-rpc-auth-v2): accept old peers during rolling upgrades. Remove after the minimum
        // supported RustFS peer version sends v2 authentication on every internode gRPC request.
        return verify_rpc_signature(TONIC_RPC_PREFIX, &Method::GET, headers);
    }

    let path = path
        .strip_prefix('/')
        .ok_or_else(|| std::io::Error::other("Invalid RPC request path"))?;
    let (service, rpc_method) = path
        .split_once('/')
        .filter(|(service, rpc_method)| !service.is_empty() && !rpc_method.is_empty() && !rpc_method.contains('/'))
        .ok_or_else(|| std::io::Error::other("Invalid RPC request path"))?;
    if audience.is_empty() {
        return Err(std::io::Error::other("Missing RPC audience"));
    }

    let version = headers
        .get(RPC_AUTH_VERSION_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC auth version"))?;
    if version != RPC_AUTH_VERSION_V2 {
        return Err(std::io::Error::other("Unsupported RPC auth version"));
    }
    let signature = headers
        .get(RPC_SIGNATURE_V2_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC v2 signature"))?;
    let timestamp_header = headers
        .get(TIMESTAMP_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing timestamp header"))?;
    let timestamp = timestamp_header
        .parse::<i64>()
        .map_err(|_| std::io::Error::other("Invalid timestamp format"))?;
    check_timestamp(timestamp)?;
    let nonce = headers
        .get(RPC_NONCE_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC nonce"))?;
    let content_sha256 = headers
        .get(RPC_CONTENT_SHA256_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing RPC content SHA-256"))?;
    if !valid_content_sha256(content_sha256) {
        return Err(std::io::Error::other("Invalid RPC content SHA-256"));
    }
    let parsed_nonce = if content_sha256 == UNSIGNED_PAYLOAD {
        if nonce != UNSIGNED_PAYLOAD_NONCE {
            return Err(std::io::Error::other("Invalid unsigned RPC nonce"));
        }
        None
    } else {
        let parsed_nonce = Uuid::parse_str(nonce).map_err(|_| std::io::Error::other("Invalid RPC nonce"))?;
        if parsed_nonce.is_nil() {
            return Err(std::io::Error::other("Invalid RPC nonce"));
        }
        Some(parsed_nonce)
    };

    let secret = get_shared_secret()?;
    if !verify_signature_v2(
        &secret,
        SignatureV2Scope {
            audience,
            service,
            rpc_method,
            timestamp: timestamp_header,
            nonce,
            content_sha256,
        },
        signature,
    ) {
        return Err(std::io::Error::other("Invalid RPC v2 signature"));
    }
    if let Some(nonce) = parsed_nonce {
        check_and_record_nonce(nonce, timestamp)?;
    }
    Ok(())
}

/// Verify the request signature for RPC requests
pub fn verify_rpc_signature(url: &str, method: &Method, headers: &HeaderMap) -> std::io::Result<()> {
    // Get signature from header
    let signature = headers
        .get(SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing signature header"))?;

    // Get timestamp from header
    let timestamp_str = headers
        .get(TIMESTAMP_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| std::io::Error::other("Missing timestamp header"))?;

    let timestamp: i64 = timestamp_str
        .parse()
        .map_err(|_| std::io::Error::other("Invalid timestamp format"))?;

    check_timestamp(timestamp)?;

    // Verify signature with constant-time HMAC comparison.
    let secret = get_shared_secret()?;

    if !verify_signature(&secret, url, method, timestamp, signature) {
        let rpc_path = redacted_rpc_path(url);
        error!(
            rpc_path = %rpc_path,
            method = %method,
            timestamp,
            signature_len = signature.len(),
            "verify_rpc_signature: Invalid signature"
        );

        return Err(std::io::Error::other("Invalid signature"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::rpc::context_propagation::REQUEST_ID_HEADER;
    use crate::runtime::sources as runtime_sources;
    use http::{HeaderMap, Method};
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use time::OffsetDateTime;
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    struct CapturedLogs {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    struct CapturedLogWriter {
        buffer: Arc<Mutex<Vec<u8>>>,
    }

    impl CapturedLogs {
        fn contents(&self) -> String {
            let buffer = self
                .buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .clone();
            String::from_utf8(buffer).expect("captured logs should be valid UTF-8")
        }
    }

    impl Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.buffer
                .lock()
                .expect("captured logs mutex should not be poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter {
                buffer: Arc::clone(&self.buffer),
            }
        }
    }

    fn ensure_test_rpc_secret() {
        runtime_sources::ensure_test_rpc_secret();
    }

    /// Security regression for GHSA-r5qv-rc46-hv8q (internode RPC fail-closed,
    /// fixed in rustfs/rustfs#4402): secret resolution must never silently fall
    /// back to a default/empty shared secret. Missing and default secrets both
    /// resolve to an error, so a misconfigured node cannot come up with a
    /// predictable, attacker-known RPC key.
    #[test]
    fn ghsa_r5qv_resolve_shared_secret_rejects_default_fallback() {
        let err = resolve_shared_secret(None, None).expect_err("default fallback must be rejected");
        assert_eq!(err.to_string(), RPC_SECRET_REQUIRED_MESSAGE);

        let err = resolve_shared_secret(None, Some(DEFAULT_SECRET_KEY)).expect_err("default global secret must be rejected");
        assert_eq!(err.to_string(), RPC_SECRET_REQUIRED_MESSAGE);

        let err = resolve_shared_secret(Some(DEFAULT_SECRET_KEY), None).expect_err("default env secret must be rejected");
        assert_eq!(err.to_string(), RPC_SECRET_REQUIRED_MESSAGE);

        let err = resolve_shared_secret(Some("   "), Some("   ")).expect_err("blank secrets must be rejected");
        assert_eq!(err.to_string(), RPC_SECRET_REQUIRED_MESSAGE);
    }

    /// Security regression for GHSA-r5qv-rc46-hv8q: `verify_rpc_signature` must
    /// fail closed for every shape of missing or malformed authentication.
    /// Consolidates the advisory's exact scenario (no valid signature/timestamp
    /// pair => rejected, never silently allowed) into one named test so the
    /// advisory maps to a discoverable regression.
    #[test]
    fn ghsa_r5qv_verify_rpc_signature_fails_closed_on_missing_or_invalid_auth() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;

        // No auth headers at all.
        let empty = HeaderMap::new();
        assert!(
            verify_rpc_signature(url, &method, &empty).is_err(),
            "request with no auth headers must be rejected"
        );

        // Signature header present but garbage; timestamp is current.
        let mut forged = HeaderMap::new();
        let now = OffsetDateTime::now_utc().unix_timestamp();
        forged.insert(SIGNATURE_HEADER, HeaderValue::from_static("not-a-real-signature"));
        forged.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&now.to_string()).unwrap());
        assert!(
            verify_rpc_signature(url, &method, &forged).is_err(),
            "request with a forged signature must be rejected"
        );

        // A validly signed request for a *different* URL must not authorize this one.
        let mut cross = HeaderMap::new();
        build_auth_headers("http://example.com/api/other", &method, &mut cross).expect("auth headers should build");
        assert!(
            verify_rpc_signature(url, &method, &cross).is_err(),
            "a signature bound to a different URL must not authorize this request"
        );

        // Control: a correctly signed request for this URL still succeeds, so the
        // gate is fail-closed rather than fail-everything.
        let mut valid = HeaderMap::new();
        build_auth_headers(url, &method, &mut valid).expect("auth headers should build");
        assert!(
            verify_rpc_signature(url, &method, &valid).is_ok(),
            "a correctly signed request must be accepted"
        );
    }

    #[test]
    fn test_get_shared_secret() {
        ensure_test_rpc_secret();
        let secret = get_shared_secret().expect("test RPC secret should resolve");
        assert!(!secret.is_empty(), "Secret should not be empty");

        let url = "http://node1:7000/rustfs/rpc/read_file_stream?disk=http%3A%2F%2Fnode1%3A7000%2Fdata%2Frustfs3&volume=.rustfs.sys&path=pool.bin%2Fdd0fd773-a962-4265-b543-783ce83953e9%2Fpart.1&offset=0&length=44";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        build_auth_headers(url, &method, &mut headers).expect("auth headers should build");

        let url = "/rustfs/rpc/read_file_stream?disk=http%3A%2F%2Fnode1%3A7000%2Fdata%2Frustfs3&volume=.rustfs.sys&path=pool.bin%2Fdd0fd773-a962-4265-b543-783ce83953e9%2Fpart.1&offset=0&length=44";

        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_ok(), "Valid signature should pass verification");
    }

    #[test]
    fn test_generate_signature_deterministic() {
        let secret = "test-secret";
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let timestamp = 1640995200; // Fixed timestamp

        let signature1 = generate_signature(secret, url, &method, timestamp);
        let signature2 = generate_signature(secret, url, &method, timestamp);

        assert_eq!(signature1, signature2, "Same inputs should produce same signature");
        assert!(!signature1.is_empty(), "Signature should not be empty");
    }

    #[test]
    fn test_generate_signature_different_inputs() {
        let secret = "test-secret";
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let timestamp = 1640995200;

        let signature1 = generate_signature(secret, url, &method, timestamp);
        let signature2 = generate_signature(secret, "http://different.com/api/test2", &method, timestamp);
        let signature3 = generate_signature(secret, url, &Method::POST, timestamp);
        let signature4 = generate_signature(secret, url, &method, timestamp + 1);

        assert_ne!(signature1, signature2, "Different URLs should produce different signatures");
        assert_ne!(signature1, signature3, "Different methods should produce different signatures");
        assert_ne!(signature1, signature4, "Different timestamps should produce different signatures");
    }

    #[test]
    fn test_build_auth_headers() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::POST;
        let mut headers = HeaderMap::new();

        build_auth_headers(url, &method, &mut headers).expect("auth headers should build");

        // Verify headers are present
        assert!(headers.contains_key(SIGNATURE_HEADER), "Should contain signature header");
        assert!(headers.contains_key(TIMESTAMP_HEADER), "Should contain timestamp header");

        // Verify header values are not empty
        let signature = headers.get(SIGNATURE_HEADER).unwrap().to_str().unwrap();
        let timestamp_str = headers.get(TIMESTAMP_HEADER).unwrap().to_str().unwrap();

        assert!(!signature.is_empty(), "Signature should not be empty");
        assert!(!timestamp_str.is_empty(), "Timestamp should not be empty");

        // Verify timestamp is a valid integer
        let timestamp: i64 = timestamp_str.parse().expect("Timestamp should be valid integer");
        let current_time = OffsetDateTime::now_utc().unix_timestamp();

        // Should be within a reasonable range (within 1 second of current time)
        assert!((current_time - timestamp).abs() <= 1, "Timestamp should be close to current time");
    }

    #[test]
    fn test_build_auth_headers_preserves_existing_request_id() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();
        headers.insert(REQUEST_ID_HEADER, HeaderValue::from_static("req-upstream-123"));

        build_auth_headers(url, &method, &mut headers).expect("auth headers should build");

        assert_eq!(headers.get(REQUEST_ID_HEADER).and_then(|v| v.to_str().ok()), Some("req-upstream-123"));
    }

    #[test]
    fn test_build_auth_headers_may_set_request_id_from_trace_id() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        let span = tracing::info_span!("rpc-test-span");
        let _guard = span.enter();
        build_auth_headers(url, &method, &mut headers).expect("auth headers should build");

        if let Some(value) = headers.get(REQUEST_ID_HEADER).and_then(|v| v.to_str().ok()) {
            assert!(!value.is_empty(), "request id should not be empty");
        }
    }

    #[test]
    fn test_verify_rpc_signature_success() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Build headers with valid signature
        build_auth_headers(url, &method, &mut headers).expect("auth headers should build");

        // Verify should succeed
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_ok(), "Valid signature should pass verification");
    }

    #[test]
    fn test_verify_rpc_signature_invalid_signature() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Build headers with valid signature first
        build_auth_headers(url, &method, &mut headers).expect("auth headers should build");

        // Tamper with the signature
        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str("invalid-signature").unwrap());

        // Verify should fail
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Invalid signature should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Invalid signature");
    }

    #[test]
    fn test_verify_signature_uses_hmac_verification() {
        let secret = "test-secret";
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let timestamp = 1640995200;
        let signature = generate_signature(secret, url, &method, timestamp);
        let mut tampered = general_purpose::STANDARD.decode(&signature).unwrap();
        tampered[0] ^= 1;
        let tampered_signature = general_purpose::STANDARD.encode(tampered);

        assert!(verify_signature(secret, url, &method, timestamp, &signature));
        assert!(!verify_signature(secret, url, &method, timestamp, &tampered_signature));
        assert!(!verify_signature(secret, url, &method, timestamp, "invalid-signature"));
    }

    #[test]
    fn walk_dir_capability_is_covered_by_the_signature() {
        let secret = "test-secret";
        let signed_url = concat!(
            "http://node1:9000/rustfs/rpc/walk_dir?disk=disk-a&walk_dir_stream_completion=error-v1",
            "&walk_dir_body_sha256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        let downgraded_url = "http://node1:9000/rustfs/rpc/walk_dir?disk=disk-a";
        let tampered_body_digest = concat!(
            "http://node1:9000/rustfs/rpc/walk_dir?disk=disk-a&walk_dir_stream_completion=error-v1",
            "&walk_dir_body_sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let method = Method::GET;
        let timestamp = 1_640_995_200;
        let signature = generate_signature(secret, signed_url, &method, timestamp);

        assert!(verify_signature(secret, signed_url, &method, timestamp, &signature));
        assert!(!verify_signature(secret, downgraded_url, &method, timestamp, &signature));
        assert!(!verify_signature(secret, tampered_body_digest, &method, timestamp, &signature));
    }

    #[test]
    fn test_invalid_signature_log_contract_excludes_secrets() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test?disk=/sensitive/path&token=private";
        let method = Method::GET;
        let timestamp = OffsetDateTime::now_utc().unix_timestamp();
        let secret = get_shared_secret().expect("test RPC secret should resolve");
        let expected_signature = generate_signature(&secret, url, &method, timestamp);
        let invalid_signature = "invalid-signature";
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::ERROR)
            .with_writer(logs.clone())
            .with_ansi(false)
            .without_time()
            .finish();

        let mut headers = HeaderMap::new();
        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(invalid_signature).unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&timestamp.to_string()).unwrap());

        tracing::subscriber::with_default(subscriber, || {
            let result = verify_rpc_signature(url, &method, &headers);
            assert!(result.is_err(), "Invalid signature should fail verification");
        });

        let captured = logs.contents();
        assert!(captured.contains("Invalid signature"));
        assert!(!captured.contains(&secret));
        assert!(!captured.contains(&expected_signature));
        assert!(!captured.contains(invalid_signature));
        assert!(!captured.contains("sensitive"));
        assert!(!captured.contains("private"));
    }

    #[test]
    fn test_verify_rpc_signature_expired_timestamp() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Set expired timestamp (older than SIGNATURE_VALID_DURATION)
        let expired_timestamp = OffsetDateTime::now_utc().unix_timestamp() - SIGNATURE_VALID_DURATION - 10;
        let secret = get_shared_secret().expect("test RPC secret should resolve");
        let signature = generate_signature(&secret, url, &method, expired_timestamp);

        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&signature).unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&expired_timestamp.to_string()).unwrap());

        // Verify should fail due to expired timestamp
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Expired timestamp should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Request timestamp expired");
    }

    #[test]
    fn test_verify_rpc_signature_future_timestamp_outside_window() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        let future_timestamp = OffsetDateTime::now_utc().unix_timestamp() + SIGNATURE_VALID_DURATION + 10;
        let secret = get_shared_secret().expect("test RPC secret should resolve");
        let signature = generate_signature(&secret, url, &method, future_timestamp);

        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&signature).unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&future_timestamp.to_string()).unwrap());

        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Future timestamp outside valid window should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Request timestamp expired");
    }

    #[test]
    fn test_verify_rpc_signature_missing_signature_header() {
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Add only timestamp header, missing signature
        let timestamp = OffsetDateTime::now_utc().unix_timestamp();
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&timestamp.to_string()).unwrap());

        // Verify should fail
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Missing signature header should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Missing signature header");
    }

    #[test]
    fn test_verify_rpc_signature_missing_timestamp_header() {
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Add only signature header, missing timestamp
        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str("some-signature").unwrap());

        // Verify should fail
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Missing timestamp header should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Missing timestamp header");
    }

    #[test]
    fn test_verify_rpc_signature_invalid_timestamp_format() {
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str("some-signature").unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str("invalid-timestamp").unwrap());

        // Verify should fail
        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Invalid timestamp format should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Invalid timestamp format");
    }

    #[test]
    fn test_verify_rpc_signature_url_mismatch() {
        ensure_test_rpc_secret();
        let original_url = "http://example.com/api/test";
        let different_url = "http://example.com/api/different";
        let method = Method::GET;
        let mut headers = HeaderMap::new();

        // Build headers for one URL
        build_auth_headers(original_url, &method, &mut headers).expect("auth headers should build");

        // Try to verify with a different URL
        let result = verify_rpc_signature(different_url, &method, &headers);
        assert!(result.is_err(), "URL mismatch should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Invalid signature");
    }

    #[test]
    fn test_verify_rpc_signature_method_mismatch() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let original_method = Method::GET;
        let different_method = Method::POST;
        let mut headers = HeaderMap::new();

        // Build headers for one method
        build_auth_headers(url, &original_method, &mut headers).expect("auth headers should build");

        // Try to verify with a different method
        let result = verify_rpc_signature(url, &different_method, &headers);
        assert!(result.is_err(), "Method mismatch should fail verification");

        let error = result.unwrap_err();
        assert_eq!(error.to_string(), "Invalid signature");
    }

    #[test]
    fn test_signature_valid_duration_boundary() {
        ensure_test_rpc_secret();
        let url = "http://example.com/api/test";
        let method = Method::GET;
        let secret = get_shared_secret().expect("test RPC secret should resolve");

        let mut headers = HeaderMap::new();
        let current_time = OffsetDateTime::now_utc().unix_timestamp();
        // Test timestamp just within valid duration
        let valid_timestamp = current_time - SIGNATURE_VALID_DURATION + 1;

        let signature = generate_signature(&secret, url, &method, valid_timestamp);

        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&signature).unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&valid_timestamp.to_string()).unwrap());

        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_ok(), "Timestamp within valid duration should pass");

        // Test timestamp just outside valid duration
        let mut headers = HeaderMap::new();
        let invalid_timestamp = current_time - SIGNATURE_VALID_DURATION - 15;
        let signature = generate_signature(&secret, url, &method, invalid_timestamp);

        headers.insert(SIGNATURE_HEADER, HeaderValue::from_str(&signature).unwrap());
        headers.insert(TIMESTAMP_HEADER, HeaderValue::from_str(&invalid_timestamp.to_string()).unwrap());

        let result = verify_rpc_signature(url, &method, &headers);
        assert!(result.is_err(), "Timestamp outside valid duration should fail");
    }

    #[test]
    fn test_round_trip_authentication() {
        ensure_test_rpc_secret();
        let test_cases = vec![
            ("http://example.com/api/test", Method::GET),
            ("https://api.rustfs.com/v1/bucket", Method::POST),
            ("http://localhost:9000/admin/info", Method::PUT),
            ("https://storage.example.com/path/to/object?query=param", Method::DELETE),
        ];

        for (url, method) in test_cases {
            let mut headers = HeaderMap::new();

            // Build authentication headers
            build_auth_headers(url, &method, &mut headers).expect("auth headers should build");

            // Verify the signature should succeed
            let result = verify_rpc_signature(url, &method, &headers);
            assert!(result.is_ok(), "Round-trip test failed for {method} {url}");
        }
    }

    #[test]
    fn tonic_v2_signature_is_bound_to_exact_method() {
        ensure_test_rpc_secret();
        let headers = gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "Ping", None)
            .expect("tonic auth headers should build");

        assert!(verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/Ping", &headers).is_ok());
        let error = verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/SignalService", &headers)
            .expect_err("signature replayed to a different method must fail");
        assert_eq!(error.to_string(), "Invalid RPC v2 signature");
    }

    #[test]
    fn tonic_v2_signature_is_bound_to_exact_service() {
        ensure_test_rpc_secret();
        let headers = gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "Ping", None)
            .expect("tonic auth headers should build");

        let error = verify_tonic_rpc_signature("node-a:9000", "/other.NodeService/Ping", &headers)
            .expect_err("signature replayed to a different service must fail");
        assert_eq!(error.to_string(), "Invalid RPC v2 signature");
    }

    #[test]
    fn tonic_v2_signature_is_bound_to_destination_audience() {
        ensure_test_rpc_secret();
        let headers = gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "Ping", None)
            .expect("tonic auth headers should build");

        let error = verify_tonic_rpc_signature("node-b:9000", "/node_service.NodeService/Ping", &headers)
            .expect_err("signature replayed to a different node must fail");
        assert_eq!(error.to_string(), "Invalid RPC v2 signature");
    }

    #[test]
    fn malformed_v2_auth_does_not_downgrade_to_valid_legacy_signature() {
        ensure_test_rpc_secret();
        let mut headers = gen_tonic_signature_headers("node-a:9000", "node_service.NodeService", "Ping", None)
            .expect("tonic auth headers should build");
        headers.insert(RPC_SIGNATURE_V2_HEADER, HeaderValue::from_static("invalid"));

        assert!(
            verify_rpc_signature(TONIC_RPC_PREFIX, &Method::GET, &headers).is_ok(),
            "the compatibility signature should remain valid for old servers"
        );
        let error = verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/Ping", &headers)
            .expect_err("new servers must not downgrade malformed v2 auth");
        assert_eq!(error.to_string(), "Invalid RPC v2 signature");
    }

    #[test]
    fn legacy_tonic_signature_remains_accepted_during_rolling_upgrade() {
        ensure_test_rpc_secret();
        let headers = gen_signature_headers(TONIC_RPC_PREFIX, &Method::GET).expect("legacy auth headers should build");

        assert!(verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/Ping", &headers).is_ok());
    }

    #[test]
    fn body_bound_tonic_request_rejects_replay_and_body_tampering() {
        ensure_test_rpc_secret();
        let body = b"heal-control-request";
        let mut request = tonic::Request::new(());
        set_tonic_canonical_body_digest(&mut request, body).expect("canonical body digest should be attached");
        let content_sha256 = request
            .metadata()
            .get(RPC_CONTENT_SHA256_HEADER)
            .and_then(|value| value.to_str().ok());
        let headers =
            gen_tonic_signature_headers("node-a:9000", "node_service.HealControlService", "HealControl", content_sha256)
                .expect("body-bound auth headers should build");
        request.metadata_mut().as_mut().extend(headers.clone());

        assert!(verify_tonic_rpc_signature("node-a:9000", "/node_service.HealControlService/HealControl", &headers).is_ok());
        let replay = verify_tonic_rpc_signature("node-a:9000", "/node_service.HealControlService/HealControl", &headers)
            .expect_err("reusing a body-bound nonce must fail");
        assert_eq!(replay.to_string(), "RPC request replay detected");
        assert!(verify_tonic_canonical_body_digest(&request, body).is_ok());
        let tampered = verify_tonic_canonical_body_digest(&request, b"different-body")
            .expect_err("a different canonical request body must fail");
        assert_eq!(tampered.to_string(), "RPC content SHA-256 mismatch");
    }

    #[test]
    fn tier_mutation_rpc_contract_requires_method_bound_v2_body_digest() {
        ensure_test_rpc_secret();
        let mutation_id = uuid::uuid!("12345678-1234-5678-9abc-def012345678");
        let body = rustfs_protos::canonical_tier_mutation_rpc_body(
            rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
            rustfs_protos::TierMutationRpcPhase::Prepare,
            mutation_id,
            b"canonical-tier-mutation-prepare",
        )
        .expect("small tier mutation body should encode");
        let mut request = tonic::Request::new(());
        set_tonic_canonical_body_digest(&mut request, &body).expect("canonical body digest should be attached");
        let content_sha256 = request
            .metadata()
            .get(RPC_CONTENT_SHA256_HEADER)
            .and_then(|value| value.to_str().ok());
        let headers = gen_tonic_signature_headers(
            "node-a:9000",
            "node_service.TierMutationControlService",
            "PrepareTierMutation",
            content_sha256,
        )
        .expect("body-bound tier mutation auth headers should build");
        request.metadata_mut().as_mut().extend(headers.clone());

        assert!(
            verify_tonic_rpc_signature("node-a:9000", "/node_service.TierMutationControlService/PrepareTierMutation", &headers)
                .is_ok(),
            "tier mutation RPC signature must bind destination, service, method, nonce, and body digest"
        );
        let method_replay =
            verify_tonic_rpc_signature("node-a:9000", "/node_service.TierMutationControlService/CommitTierMutation", &headers)
                .expect_err("prepare auth must not replay to commit");
        assert_eq!(method_replay.to_string(), "Invalid RPC v2 signature");
        let service_replay = verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/PrepareTierMutation", &headers)
            .expect_err("tier mutation auth must not replay to the legacy node service path");
        assert_eq!(service_replay.to_string(), "Invalid RPC v2 signature");
        let tampered_body = rustfs_protos::canonical_tier_mutation_rpc_body(
            rustfs_protos::TIER_MUTATION_RPC_PROTOCOL_VERSION,
            rustfs_protos::TierMutationRpcPhase::Commit,
            mutation_id,
            b"canonical-tier-mutation-prepare",
        )
        .expect("small tier mutation body should encode");
        let tampered =
            verify_tonic_canonical_body_digest(&request, &tampered_body).expect_err("commit body must not match prepare digest");
        assert_eq!(tampered.to_string(), "RPC content SHA-256 mismatch");
    }

    #[test]
    fn partial_v2_metadata_fails_closed() {
        ensure_test_rpc_secret();
        let mut headers = gen_signature_headers(TONIC_RPC_PREFIX, &Method::GET).expect("legacy auth headers should build");
        headers.insert(RPC_AUTH_VERSION_HEADER, HeaderValue::from_static(RPC_AUTH_VERSION_V2));

        let error = verify_tonic_rpc_signature("node-a:9000", "/node_service.NodeService/Ping", &headers)
            .expect_err("partial v2 metadata must not fall back to legacy auth");
        assert_eq!(error.to_string(), "Missing RPC v2 signature");
    }

    #[test]
    fn canonical_mutation_digest_rejects_legacy_only_auth() {
        let mut request = tonic::Request::new(());
        set_tonic_canonical_body_digest(&mut request, b"heal-control-v1\0start").expect("canonical digest should be attached");

        let error = verify_tonic_canonical_body_digest(&request, b"heal-control-v1\0start")
            .expect_err("mutation body verification must also require v2 auth");
        assert_eq!(error.to_string(), "RPC mutation requires v2 authentication");
    }

    #[test]
    fn nonce_cache_expires_by_monotonic_deadline_and_fails_closed_at_capacity() {
        let now = Instant::now();
        let expiry = now.checked_add(REPLAY_CACHE_RETENTION).expect("test expiry should fit");
        let after_expiry = expiry.checked_add(Duration::from_secs(1)).expect("test expiry should fit");
        let nonce_a = Uuid::new_v4();
        let nonce_b = Uuid::new_v4();
        let mut cache = RpcNonceCache::default();

        cache
            .check_and_record(nonce_a, 100, now, 100, expiry, 1)
            .expect("first nonce should be recorded");
        let capacity = cache
            .check_and_record(nonce_b, 100, now, 100, expiry, 1)
            .expect_err("a full replay cache must fail closed");
        assert_eq!(capacity.to_string(), "RPC replay cache capacity exceeded");
        cache
            .check_and_record(nonce_b, 702, after_expiry, 702, after_expiry, 1)
            .expect("expired nonce should release capacity");
        assert!(!cache.nonces.contains(&nonce_a));
        assert!(cache.nonces.contains(&nonce_b));
    }

    #[test]
    fn nonce_cache_rejects_replay_after_wall_clock_regression() {
        let now = Instant::now();
        let expiry = now.checked_add(REPLAY_CACHE_RETENTION).expect("test expiry should fit");
        let after_expiry = expiry.checked_add(Duration::from_secs(1)).expect("test expiry should fit");
        let nonce = Uuid::new_v4();
        let mut cache = RpcNonceCache::default();

        cache
            .check_and_record(nonce, 1_000, now, 1_000, expiry, 2)
            .expect("first nonce should be recorded");
        let replay = cache
            .check_and_record(nonce, 1_000, after_expiry, 900, after_expiry, 2)
            .expect_err("wall clock regression must not make an old signature reusable");
        assert_eq!(replay.to_string(), "RPC request replay detected");

        let stale = cache
            .check_and_record(Uuid::new_v4(), 600, after_expiry, 900, after_expiry, 2)
            .expect_err("the monotonic wall-clock high-water mark must fail closed");
        assert_eq!(stale.to_string(), "RPC request timestamp expired after clock regression");
    }
}
