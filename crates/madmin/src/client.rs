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

//! Admin API HTTP client for heal and scanner management (rustfs/backlog#1869).
//!
//! [`AdminClient`] speaks the `/rustfs/admin/v3` surface with S3 SigV4
//! request signing (the same scheme the server's admin router authenticates),
//! so `mc`-style tooling and automation can drive heal start/query/cancel and
//! read background-heal / scanner status without hand-rolling HTTP.
//!
//! Wire structs in this module mirror the server-side shapes
//! (`rustfs/src/admin/handlers/heal.rs`, `handlers/scanner.rs`,
//! `rustfs-common/src/heal_channel.rs`), following the madmin-go model where
//! the SDK owns its own copies and round-trip tests pin the encoding. Deeply
//! nested status payloads that the server composes from runtime types are
//! carried through as `serde_json::Value` and flattened maps rather than
//! duplicated field-for-field, so the client cannot silently drift on fields
//! it never interprets.

use crate::heal_commands::HealResultItem;
use http::Method;
use serde::{Deserialize, Serialize, de};
use std::time::Duration;

/// Default admin API path prefix on a RustFS endpoint.
pub const DEFAULT_ADMIN_API_PREFIX: &str = "/rustfs/admin";
/// Default SigV4 region when the server has no explicit region configured.
pub const DEFAULT_REGION: &str = "us-east-1";

/// Scan mode for a heal request, mirroring the server's numeric-or-name wire
/// encoding (`0` unknown/default, `1` normal, `2` deep).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HealScanMode {
    /// Server default; behaves as [`HealScanMode::Normal`].
    #[default]
    Unknown,
    /// Metadata-level checks only.
    Normal,
    /// Full bitrot verification while healing.
    Deep,
}

impl HealScanMode {
    fn wire_number(self) -> u8 {
        match self {
            Self::Unknown => 0,
            Self::Normal => 1,
            Self::Deep => 2,
        }
    }

    fn from_wire_number(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Unknown),
            1 => Some(Self::Normal),
            2 => Some(Self::Deep),
            _ => None,
        }
    }

    fn from_wire_name(value: &str) -> Option<Self> {
        match value {
            "unknown" => Some(Self::Unknown),
            "normal" => Some(Self::Normal),
            "deep" => Some(Self::Deep),
            _ => None,
        }
    }
}

impl Serialize for HealScanMode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8(self.wire_number())
    }
}

impl<'de> Deserialize<'de> for HealScanMode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct HealScanModeVisitor;

        impl de::Visitor<'_> for HealScanModeVisitor {
            type Value = HealScanMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a heal scan mode number or name")
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                u8::try_from(value)
                    .ok()
                    .and_then(HealScanMode::from_wire_number)
                    .ok_or_else(|| E::custom(format!("unknown heal scan mode number: {value}")))
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                HealScanMode::from_wire_name(value).ok_or_else(|| E::custom(format!("unknown heal scan mode name: {value}")))
            }
        }

        deserializer.deserialize_any(HealScanModeVisitor)
    }
}

/// Heal options for an admin heal request (mirror of the server body type).
/// Fields default on decode: a client should tolerate a server response whose
/// settings object omits fields it never set.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HealOpts {
    #[serde(default)]
    pub recursive: bool,
    #[serde(rename = "dryRun", default)]
    pub dry_run: bool,
    #[serde(default)]
    pub remove: bool,
    #[serde(default)]
    pub recreate: bool,
    #[serde(rename = "scanMode", default)]
    pub scan_mode: HealScanMode,
    #[serde(rename = "updateParity", default)]
    pub update_parity: bool,
    #[serde(rename = "nolock", default)]
    pub no_lock: bool,
    #[serde(rename = "pool", default)]
    pub pool: Option<usize>,
    #[serde(rename = "set", default)]
    pub set: Option<usize>,
}

/// Successful heal start / path-scoped cancel response.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealStartSuccess {
    pub client_token: String,
    pub client_address: String,
    #[serde(default)]
    pub start_time: String,
}

/// Heal task status response (query, cancel-with-token, start-then-poll).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealTaskStatus {
    /// `running` | `finished` | `stopped` | `notFound`.
    pub summary: String,
    /// Failure detail for stopped tasks; empty otherwise.
    #[serde(rename = "detail", default)]
    pub failure_detail: String,
    #[serde(default)]
    pub start_time: String,
    #[serde(default)]
    pub settings: HealOpts,
    #[serde(default)]
    pub items: Vec<HealResultItem>,
    #[serde(default)]
    pub truncated: bool,
    /// Live progress snapshot; the exact shape is owned by the heal runtime.
    #[serde(default)]
    pub progress: Option<serde_json::Value>,
    /// Canonical heal-owner result. Missing or future states are not repair proof.
    #[serde(default)]
    pub outcome: Option<serde_json::Value>,
    #[serde(default, alias = "next_seq")]
    pub next_seq: Option<u64>,
    #[serde(default, alias = "min_seq")]
    pub min_seq: Option<u64>,
}

/// `POST /v3/background-heal/status` response. Known top-level fields are
/// typed; the flattened heal info and operations matrix pass through verbatim.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackgroundHealStatus {
    /// `disabled` | `uninitialized` | `idle` | `active` | `degraded`.
    pub state: String,
    #[serde(default)]
    pub heal_queue_length: u64,
    #[serde(default)]
    pub heal_active_tasks: u64,
    #[serde(default)]
    pub cluster_status_complete: bool,
    /// Missing on older servers; absent coverage or counts mean unknown.
    #[serde(default)]
    pub coverage: Option<BackgroundHealCoverage>,
    #[serde(default)]
    pub progress: Option<serde_json::Value>,
    /// Remaining wire fields (flattened `BackgroundHealInfo` plus the
    /// priority-by-source operations matrix), carried verbatim.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// Node coverage of a background heal status snapshot. Counters describe only
/// nodes with usable snapshots; unknown peers may still be running heal work.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackgroundHealCoverage {
    #[serde(default)]
    pub expected: Option<usize>,
    #[serde(default)]
    pub responded: Option<usize>,
    #[serde(default)]
    pub unknown: Option<usize>,
    /// Stable reason codes; unknown future codes are preserved verbatim.
    #[serde(default)]
    pub reasons: Vec<String>,
}

/// `GET /v3/scanner/status` response, typed at the fields operators branch
/// on; everything else passes through verbatim.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScannerStatus {
    pub enabled: bool,
    /// `fresh` | `stale` | `unknown`; absent when the scanner never completed
    /// a cycle.
    #[serde(default)]
    pub freshness: Option<ScannerFreshness>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// `POST /v3/scanner/cycle-state/reset` response for the legacy synchronous
/// full-rescan reset path.
#[derive(Debug, Clone, Deserialize)]
pub struct ScannerCycleResetResponse {
    pub status: String,
    pub mode: String,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// `POST /v3/scanner/usage-state/reset` response for the legacy synchronous
/// full-rebuild reset path.
#[derive(Debug, Clone, Deserialize)]
pub struct ScannerUsageStateResetResponse {
    pub status: String,
    pub mode: String,
    pub usage_state: String,
    pub leader_epoch: u64,
    pub next_cycle: u64,
    pub reset_paths: Vec<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct ScannerCycleResetRequest<'a> {
    mode: &'a str,
}

#[derive(Debug, Serialize)]
struct ScannerUsageStateResetRequest<'a> {
    mode: &'a str,
}

/// Freshness block of the scanner status response.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScannerFreshness {
    /// `fresh` | `stale` | `unknown`.
    pub state: String,
}

impl ScannerStatus {
    /// Convenience accessor for the freshness state string.
    pub fn freshness(&self) -> &str {
        self.freshness
            .as_ref()
            .map(|freshness| freshness.state.as_str())
            .unwrap_or("unknown")
    }
}

/// Everything that can go wrong in an admin client call.
#[derive(Debug)]
pub enum AdminClientError {
    /// The endpoint URL could not be parsed.
    InvalidEndpoint(String),
    /// Request build/send failed (DNS, connect, timeout, body read).
    Transport(reqwest::Error),
    /// The server answered a non-2xx status.
    HttpStatus { status: u16, body: String },
    /// The response body did not decode into the expected shape.
    Decode { message: String },
}

impl std::fmt::Display for AdminClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidEndpoint(message) => write!(f, "invalid admin endpoint: {message}"),
            Self::Transport(err) => write!(f, "admin request transport failure: {err}"),
            Self::HttpStatus { status, body } => write!(f, "admin request failed with HTTP {status}: {body}"),
            Self::Decode { message } => write!(f, "admin response decode failure: {message}"),
        }
    }
}

impl std::error::Error for AdminClientError {}

impl From<reqwest::Error> for AdminClientError {
    fn from(err: reqwest::Error) -> Self {
        Self::Transport(err)
    }
}

/// A signed client for a RustFS admin API.
#[derive(Debug, Clone)]
pub struct AdminClient {
    endpoint: reqwest::Url,
    access_key: String,
    secret_key: String,
    session_token: String,
    region: String,
    api_prefix: String,
    http: reqwest::Client,
}

impl AdminClient {
    /// Build a client for `endpoint` (e.g. `http://127.0.0.1:9000`) using root
    /// or admin credentials. Requests are SigV4-signed with the same scheme
    /// the server's admin router authenticates.
    pub fn new(endpoint: &str, access_key: &str, secret_key: &str) -> Result<Self, AdminClientError> {
        let url = reqwest::Url::parse(endpoint).map_err(|err| AdminClientError::InvalidEndpoint(err.to_string()))?;
        if url.host_str().is_none() {
            return Err(AdminClientError::InvalidEndpoint("endpoint has no host".to_string()));
        }
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(AdminClientError::Transport)?;
        Ok(Self {
            endpoint: url,
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            session_token: String::new(),
            region: DEFAULT_REGION.to_string(),
            api_prefix: DEFAULT_ADMIN_API_PREFIX.to_string(),
            http,
        })
    }

    /// Attach an STS session token (signed as `x-amz-security-token`).
    pub fn with_session_token(mut self, session_token: impl Into<String>) -> Self {
        self.session_token = session_token.into();
        self
    }

    /// Override the SigV4 region (defaults to `us-east-1`, matching a
    /// region-less RustFS deployment).
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = region.into();
        self
    }

    /// Override the admin API path prefix (defaults to `/rustfs/admin`).
    pub fn with_api_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.api_prefix = prefix.into();
        self
    }

    /// Start a heal. `bucket` empty and `prefix` empty heals the whole
    /// deployment (requires `recursive` or a `pool`/`set` pair in `opts`,
    /// enforced server-side); a bucket alone heals the bucket (the server
    /// forces `recursive` for bucket heals).
    pub async fn heal_start(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        opts: &HealOpts,
        force_start: bool,
    ) -> Result<HealStartSuccess, AdminClientError> {
        let body = serde_json::to_vec(opts).map_err(|err| AdminClientError::Decode {
            message: err.to_string(),
        })?;
        let mut query = Vec::new();
        if force_start {
            query.push(("forceStart", "true".to_string()));
        }
        self.post_json(&heal_path(bucket, prefix), &query, body).await
    }

    /// Query the status of the heal identified by `client_token` (the token
    /// returned by [`Self::heal_start`]) at the path it was started on.
    pub async fn heal_status(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        client_token: &str,
    ) -> Result<HealTaskStatus, AdminClientError> {
        self.heal_status_since(bucket, prefix, client_token, None).await
    }

    /// Query a retained result window. Missing cursors and outcome remain unknown.
    pub async fn heal_status_since(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        client_token: &str,
        since_seq: Option<u64>,
    ) -> Result<HealTaskStatus, AdminClientError> {
        let mut query = vec![("clientToken", client_token.to_string())];
        if let Some(since_seq) = since_seq {
            query.push(("sinceSeq", since_seq.to_string()));
        }
        self.post_json(&heal_path(bucket, prefix), &query, Vec::new()).await
    }

    /// Stop a heal: with a `client_token` only that task is cancelled and its
    /// final status returned; without one, every heal task at the path is
    /// cancelled (the server answers with a start-success-shaped receipt).
    pub async fn heal_stop(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        client_token: Option<&str>,
    ) -> Result<HealStopOutcome, AdminClientError> {
        let mut query = vec![("forceStop", "true".to_string())];
        if let Some(token) = client_token {
            query.push(("clientToken", token.to_string()));
        }
        match client_token {
            Some(_) => {
                let status: HealTaskStatus = self.post_json(&heal_path(bucket, prefix), &query, Vec::new()).await?;
                Ok(HealStopOutcome::Stopped(Box::new(status)))
            }
            None => {
                let success: HealStartSuccess = self.post_json(&heal_path(bucket, prefix), &query, Vec::new()).await?;
                Ok(HealStopOutcome::PathStopped(success))
            }
        }
    }

    /// Cluster-aggregated background heal status. The route is registered
    /// POST-only on the server, so this must not go out as a GET.
    pub async fn background_heal_status(&self) -> Result<BackgroundHealStatus, AdminClientError> {
        self.post_json("/v3/background-heal/status", &[], Vec::new()).await
    }

    /// Data scanner status (enabled state, freshness, runtime config).
    pub async fn scanner_status(&self) -> Result<ScannerStatus, AdminClientError> {
        self.get_json("/v3/scanner/status").await
    }

    /// Request a legacy synchronous scanner cycle reset (`full-rescan`).
    pub async fn scanner_cycle_state_reset_full_rescan(&self) -> Result<ScannerCycleResetResponse, AdminClientError> {
        let body =
            serde_json::to_vec(&ScannerCycleResetRequest { mode: "full-rescan" }).map_err(|err| AdminClientError::Decode {
                message: err.to_string(),
            })?;
        self.post_json("/v3/scanner/cycle-state/reset", &[], body).await
    }

    /// Request a legacy synchronous scanner usage reset (`full-rebuild`).
    pub async fn scanner_usage_state_reset_full_rebuild(&self) -> Result<ScannerUsageStateResetResponse, AdminClientError> {
        let body = serde_json::to_vec(&ScannerUsageStateResetRequest { mode: "full-rebuild" }).map_err(|err| {
            AdminClientError::Decode {
                message: err.to_string(),
            }
        })?;
        self.post_json("/v3/scanner/usage-state/reset", &[], body).await
    }

    /// ILM expiry worker status. The payload is owned by the expiry
    /// subsystem and still evolving; returned verbatim.
    pub async fn ilm_expiry_status(&self) -> Result<serde_json::Value, AdminClientError> {
        self.get_json("/v3/ilm/expiry/status").await
    }

    /// Durable replacement-recovery status (admin v4). The payload is owned
    /// by the heal runtime; returned verbatim.
    pub async fn replacement_recovery_status(&self) -> Result<serde_json::Value, AdminClientError> {
        self.get_json("/v4/heal/replacement-recovery").await
    }

    /// Signed GET returning a decoded JSON body; escape hatch for endpoints
    /// this client does not wrap yet.
    pub async fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T, AdminClientError> {
        let url = self.url_for(path, &[])?;
        let request = self.sign_and_build(Method::GET, url, Vec::new(), None).await?;
        self.execute(request).await
    }

    /// Signed POST returning a decoded JSON body.
    pub(crate) async fn post_json<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        query: &[(&str, String)],
        body: Vec<u8>,
    ) -> Result<T, AdminClientError> {
        let content_type = if body.is_empty() { None } else { Some("application/json") };
        let url = self.url_for(path, query)?;
        let request = self.sign_and_build(Method::POST, url, body, content_type).await?;
        self.execute(request).await
    }

    pub(crate) fn url_for(&self, path: &str, query: &[(&str, String)]) -> Result<reqwest::Url, AdminClientError> {
        let mut url = self
            .endpoint
            .join(&format!("{}{}", self.api_prefix.trim_end_matches('/'), path))
            .map_err(|err| AdminClientError::InvalidEndpoint(err.to_string()))?;
        if !query.is_empty() {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in query {
                pairs.append_pair(key, value);
            }
        }
        Ok(url)
    }

    /// Build a SigV4-signed request via the same signer the server trusts,
    /// then hand the signed headers to the HTTP client. The signature covers
    /// method, path, query, and an unsigned-payload marker — the same shape
    /// RustFS itself sends for peer admin calls.
    pub(crate) async fn sign_and_build(
        &self,
        method: Method,
        url: reqwest::Url,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<reqwest::Request, AdminClientError> {
        let authority = match (url.host_str(), url.port_or_known_default()) {
            (Some(host), Some(port)) => format!("{host}:{port}"),
            _ => return Err(AdminClientError::InvalidEndpoint("endpoint has no authority".to_string())),
        };
        let mut builder = http::Request::builder()
            .method(method.clone())
            .uri(url.as_str())
            .header(http::header::HOST, &authority)
            .header("x-amz-content-sha256", rustfs_signer::constants::UNSIGNED_PAYLOAD);
        if let Some(content_type) = content_type {
            builder = builder.header(http::header::CONTENT_TYPE, content_type);
        }
        let unsigned = builder
            .body(s3s::Body::empty())
            .map_err(|err| AdminClientError::InvalidEndpoint(format!("build request failed: {err}")))?;
        let signed = rustfs_signer::sign_v4(
            unsigned,
            body.len() as i64,
            &self.access_key,
            &self.secret_key,
            &self.session_token,
            &self.region,
        );

        let mut request = self
            .http
            .request(method, url)
            .body(body)
            .build()
            .map_err(AdminClientError::Transport)?;
        let headers = request.headers_mut();
        for (name, value) in signed.headers().iter() {
            // HOST is owned by the HTTP client; the signed value above was
            // built from the same URL authority, so they always agree.
            if name == http::header::HOST {
                continue;
            }
            headers.insert(name, value.clone());
        }
        Ok(request)
    }

    pub(crate) async fn execute<T: for<'de> Deserialize<'de>>(&self, request: reqwest::Request) -> Result<T, AdminClientError> {
        let response = self.http.execute(request).await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        if !status.is_success() {
            return Err(AdminClientError::HttpStatus {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        serde_json::from_slice(&bytes).map_err(|err| AdminClientError::Decode {
            message: err.to_string(),
        })
    }

    /// Execute a request whose success answer carries no body (`204`).
    pub(crate) async fn execute_no_content(&self, request: reqwest::Request) -> Result<(), AdminClientError> {
        let response = self.http.execute(request).await?;
        let status = response.status();
        if !status.is_success() {
            let bytes = response.bytes().await?;
            return Err(AdminClientError::HttpStatus {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        Ok(())
    }
}

/// Response of [`AdminClient::heal_stop`]: cancelling a single tokened task
/// answers with that task's status, cancelling a whole path answers with a
/// start-success-shaped receipt.
#[derive(Debug, Clone)]
pub enum HealStopOutcome {
    Stopped(Box<HealTaskStatus>),
    PathStopped(HealStartSuccess),
}

fn heal_path(bucket: Option<&str>, prefix: Option<&str>) -> String {
    match (bucket, prefix) {
        (Some(bucket), Some(prefix)) if !bucket.is_empty() && !prefix.is_empty() => {
            format!("/v3/heal/{}/{}", percent_encode_path_segment(bucket), percent_encode_path_segment(prefix))
        }
        (Some(bucket), Some(_)) | (Some(bucket), None) if !bucket.is_empty() => {
            format!("/v3/heal/{}", percent_encode_path_segment(bucket))
        }
        _ => "/v3/heal/".to_string(),
    }
}

/// Encode a single path segment (slashes are content, not separators, inside
/// bucket/prefix path params).
pub(crate) fn percent_encode_path_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        AdminClient, AdminClientError, BackgroundHealStatus, HealOpts, HealScanMode, HealStartSuccess, HealTaskStatus,
        ScannerCycleResetResponse, ScannerStatus, ScannerUsageStateResetResponse, heal_path, percent_encode_path_segment,
    };
    use crate::test_support::TestServer;
    use serde_json::json;

    #[test]
    fn heal_paths_cover_root_bucket_and_prefix() {
        assert_eq!(heal_path(None, None), "/v3/heal/");
        assert_eq!(heal_path(Some(""), Some("")), "/v3/heal/");
        assert_eq!(heal_path(Some("bucket"), None), "/v3/heal/bucket");
        assert_eq!(heal_path(Some("bucket"), Some("pre/fix")), "/v3/heal/bucket/pre%2Ffix");
    }

    #[test]
    fn path_segments_percent_encode_reserved_characters() {
        assert_eq!(percent_encode_path_segment("a b"), "a%20b");
        assert_eq!(percent_encode_path_segment("a/b"), "a%2Fb");
        assert_eq!(percent_encode_path_segment("ü"), "%C3%BC");
    }

    #[test]
    fn heal_opts_round_trip_through_the_server_wire_shape() {
        let opts = HealOpts {
            recursive: true,
            dry_run: false,
            remove: true,
            recreate: false,
            scan_mode: HealScanMode::Deep,
            update_parity: true,
            no_lock: false,
            pool: Some(1),
            set: Some(2),
        };
        let wire = serde_json::to_value(&opts).unwrap();
        assert_eq!(wire["scanMode"], json!(2), "the server body decodes scanMode as a number");
        let back: HealOpts = serde_json::from_value(wire).unwrap();
        assert_eq!(back.scan_mode, HealScanMode::Deep);
        assert_eq!(back.pool, Some(1));
    }

    #[test]
    fn heal_scan_mode_accepts_both_wire_encodings() {
        assert_eq!(serde_json::from_value::<HealScanMode>(json!(1)).unwrap(), HealScanMode::Normal);
        assert_eq!(serde_json::from_value::<HealScanMode>(json!("deep")).unwrap(), HealScanMode::Deep);
        assert!(serde_json::from_value::<HealScanMode>(json!(9)).is_err());
        assert!(serde_json::from_value::<HealScanMode>(json!("sideways")).is_err());
    }

    #[test]
    fn heal_task_status_decodes_the_server_response_shape() {
        let raw = json!({
            "summary": "finished",
            "detail": "",
            "startTime": "2026-08-17T00:00:00Z",
            "settings": {"recursive": false, "scanMode": 1},
            "items": [{
                "resultId": 1, "type": "object", "bucket": "b", "object": "o", "versionId": "", "detail": "",
                "parityBlocks": 2, "dataBlocks": 2, "diskCount": 4, "setCount": 1,
                "before": {"drives": []}, "after": {"drives": []}, "objectSize": 128
            }],
            "truncated": false
        });
        let status: HealTaskStatus = serde_json::from_value(raw).unwrap();
        assert_eq!(status.summary, "finished");
        assert_eq!(status.items.len(), 1);
        assert_eq!(status.settings.scan_mode, HealScanMode::Normal);
        assert!(status.progress.is_none());
    }

    #[test]
    fn outcome_v3_decoder_preserves_canonical_unknown_and_future_fields() {
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/heal-outcome-v3.json")).expect("shared fixtures");
        for case in cases.as_array().expect("cases") {
            let status: HealTaskStatus = serde_json::from_value(case["response"].clone()).expect("optional outcome response");
            assert_eq!(status.outcome.as_ref(), Some(&case["response"]["outcome"]));
            assert_eq!((status.next_seq, status.min_seq), (Some(9), Some(4)));
            assert!(status.truncated);
        }
        let old: HealTaskStatus = serde_json::from_value(json!({"summary":"finished"})).expect("legacy response");
        assert!(old.outcome.is_none() && old.next_seq.is_none() && old.min_seq.is_none());
        let future = json!({"execution":{"state":"future_state"},"newField":7});
        let status: HealTaskStatus =
            serde_json::from_value(json!({"summary":"running","outcome":future})).expect("future outcome remains opaque");
        assert_eq!(status.outcome, Some(future));
    }

    #[test]
    fn background_heal_status_types_known_fields_and_passes_the_rest_through() {
        let raw = json!({
            "state": "active",
            "bitrotStartTime": "t",
            "healQueueLength": 3,
            "healActiveTasks": 1,
            "healOperations": {"queueLength": 3},
            "clusterStatusComplete": true
        });
        let status: BackgroundHealStatus = serde_json::from_value(raw).unwrap();
        assert_eq!(status.state, "active");
        assert_eq!(status.heal_queue_length, 3);
        assert!(status.cluster_status_complete);
        assert!(status.coverage.is_none(), "legacy payloads have unknown coverage");
        assert!(status.extra.contains_key("healOperations"), "unknown nested payloads must pass through");
    }

    #[test]
    fn background_heal_status_missing_coverage_fields_remain_unknown() {
        for raw in [json!({"state": "degraded"}), json!({"state": "degraded", "coverage": {}})] {
            let status: BackgroundHealStatus = serde_json::from_value(raw).expect("partial legacy payload decodes");
            assert!(!status.cluster_status_complete);
            if let Some(coverage) = status.coverage {
                assert_eq!(coverage.expected, None);
                assert_eq!(coverage.responded, None);
                assert_eq!(coverage.unknown, None);
            }
        }
    }

    #[test]
    fn background_heal_status_preserves_future_fields_and_reasons() {
        let raw = json!({
            "state": "degraded", "clusterStatusComplete": false,
            "coverage": {"expected": 3, "responded": 1, "unknown": 2, "reasons": ["future_reason"], "futureCoverage": true},
            "futureStatus": {"value": 7}
        });
        let status: BackgroundHealStatus = serde_json::from_value(raw).expect("future additive fields decode");
        assert_eq!(status.extra["futureStatus"]["value"], 7);
        let coverage = status.coverage.expect("coverage supplied");
        assert_eq!(coverage.expected, Some(3));
        assert_eq!(coverage.responded, Some(1));
        assert_eq!(coverage.unknown, Some(2));
        assert_eq!(coverage.reasons, ["future_reason"]);
    }

    #[test]
    fn scanner_status_defaults_freshness_to_unknown() {
        let raw = json!({"enabled": true, "freshness": {"state": "stale"}, "metrics": {}});
        let status: ScannerStatus = serde_json::from_value(raw).unwrap();
        assert_eq!(status.freshness(), "stale");
        let bare: ScannerStatus = serde_json::from_value(json!({"enabled": false})).unwrap();
        assert_eq!(bare.freshness(), "unknown");
    }

    #[test]
    fn scanner_reset_responses_preserve_future_fields() {
        let cycle: ScannerCycleResetResponse =
            serde_json::from_value(json!({"status": "reset", "mode": "full-rescan", "future": true})).unwrap();
        assert_eq!(cycle.status, "reset");
        assert_eq!(cycle.mode, "full-rescan");
        assert_eq!(cycle.extra["future"], true);

        let usage: ScannerUsageStateResetResponse = serde_json::from_value(json!({
            "status": "reset",
            "mode": "full-rebuild",
            "usage_state": "bootstrap-pending",
            "leader_epoch": 11,
            "next_cycle": 42,
            "reset_paths": [".usage.json"],
            "future": {"accepted": false}
        }))
        .unwrap();
        assert_eq!(usage.status, "reset");
        assert_eq!(usage.mode, "full-rebuild");
        assert_eq!(usage.usage_state, "bootstrap-pending");
        assert_eq!(usage.leader_epoch, 11);
        assert_eq!(usage.next_cycle, 42);
        assert_eq!(usage.reset_paths, [".usage.json"]);
        assert_eq!(usage.extra["future"]["accepted"], false);
    }

    #[test]
    fn invalid_endpoint_is_rejected_without_io() {
        let err = AdminClient::new("not a url", "ak", "sk").unwrap_err();
        assert!(matches!(err, AdminClientError::InvalidEndpoint(_)));
    }

    #[tokio::test]
    async fn signed_requests_carry_sigv4_authorization_and_correct_target() {
        let server = TestServer::spawn(r#"{"clientToken":"token-1","clientAddress":"127.0.0.1:9","startTime":"t"}"#, 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "minioadmin", "minioadmin")
            .expect("client builds against the test server");

        let start: HealStartSuccess = client
            .heal_start(
                Some("bucket"),
                None,
                &HealOpts {
                    recursive: true,
                    ..Default::default()
                },
                false,
            )
            .await
            .expect("signed heal start decodes");

        assert_eq!(start.client_token, "token-1");
        let request = server.recorded();
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/rustfs/admin/v3/heal/bucket");
        assert!(!request.query.contains("forceStart"), "absent flags must not be sent");
        let auth = request.header("authorization").expect("request must be signed");
        assert!(auth.starts_with("AWS4-HMAC-SHA256"), "SigV4 scheme, got: {auth}");
        assert!(auth.contains("Credential=minioadmin/"), "credentials must be in the Authorization header");
        assert_eq!(
            request.header("x-amz-content-sha256").as_deref(),
            Some("UNSIGNED-PAYLOAD"),
            "the client signs the same payload marker RustFS peer calls use"
        );
        assert_eq!(request.header("content-type").as_deref(), Some("application/json"));
        assert!(request.body.contains("\"recursive\":true"));
    }

    #[tokio::test]
    async fn scanner_cycle_reset_posts_legacy_full_rescan_request() {
        let server = TestServer::spawn(r#"{"status":"reset","mode":"full-rescan"}"#, 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();

        let reset = client
            .scanner_cycle_state_reset_full_rescan()
            .await
            .expect("cycle reset response decodes");

        assert_eq!(reset.status, "reset");
        assert_eq!(reset.mode, "full-rescan");
        let request = server.recorded();
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/rustfs/admin/v3/scanner/cycle-state/reset");
        assert_eq!(request.query, "");
        assert!(request.body.contains("\"mode\":\"full-rescan\""));
    }

    #[tokio::test]
    async fn scanner_usage_reset_posts_legacy_full_rebuild_request() {
        let server = TestServer::spawn(
            r#"{"status":"reset","mode":"full-rebuild","usage_state":"bootstrap-pending","leader_epoch":11,"next_cycle":42,"reset_paths":[".usage.json"]}"#,
            200,
        )
        .await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();

        let reset = client
            .scanner_usage_state_reset_full_rebuild()
            .await
            .expect("usage reset response decodes");

        assert_eq!(reset.status, "reset");
        assert_eq!(reset.mode, "full-rebuild");
        assert_eq!(reset.usage_state, "bootstrap-pending");
        assert_eq!(reset.leader_epoch, 11);
        assert_eq!(reset.next_cycle, 42);
        assert_eq!(reset.reset_paths, [".usage.json"]);
        let request = server.recorded();
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/rustfs/admin/v3/scanner/usage-state/reset");
        assert_eq!(request.query, "");
        assert!(request.body.contains("\"mode\":\"full-rebuild\""));
    }

    #[tokio::test]
    async fn query_sends_client_token_on_the_same_path() {
        let body = r#"{"summary":"running","detail":"","settings":{"recursive":false},"items":[],"truncated":false}"#;
        let server = TestServer::spawn(body, 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();

        let status = client
            .heal_status(Some("bucket"), None, "token-1")
            .await
            .expect("status decodes");
        assert_eq!(status.summary, "running");
        let request = server.recorded();
        assert_eq!(request.path, "/rustfs/admin/v3/heal/bucket");
        assert!(request.query.contains("clientToken=token-1"));
        assert!(!request.query.contains("forceStop"));
    }

    #[tokio::test]
    async fn outcome_v3_since_query_preserves_cursor_and_never_sends_force_start() {
        let server = TestServer::spawn(
            r#"{"summary":"running","nextSeq":9,"minSeq":4,"truncated":true,"outcome":{"execution":{"state":"future_state"}}}"#,
            200,
        )
        .await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").expect("test client");
        let status = client
            .heal_status_since(Some("bucket"), None, "token-1", Some(3))
            .await
            .expect("window response");
        assert_eq!((status.next_seq, status.min_seq), (Some(9), Some(4)));
        assert!(status.truncated);
        assert_eq!(status.outcome.expect("future state is preserved")["execution"]["state"], "future_state");
        let request = server.recorded();
        assert!(request.query.contains("sinceSeq=3") && request.query.contains("clientToken=token-1"));
        assert!(!request.query.contains("forceStart") && !request.query.contains("forceStop"));
    }

    #[tokio::test]
    async fn stop_without_token_takes_the_path_cancel_branch() {
        let server = TestServer::spawn(r#"{"clientToken":"path","clientAddress":"c","startTime":"t"}"#, 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();

        let outcome = client.heal_stop(Some("bucket"), None, None).await.expect("path stop decodes");
        assert!(matches!(outcome, super::HealStopOutcome::PathStopped(_)));
        let request = server.recorded();
        assert!(request.query.contains("forceStop=true"));
        assert!(!request.query.contains("clientToken"));
    }

    #[tokio::test]
    async fn stop_with_token_decodes_boxed_task_status() {
        let body = r#"{"summary":"stopped","detail":"","settings":{"recursive":false},"items":[],"truncated":false}"#;
        let server = TestServer::spawn(body, 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();

        let outcome = client
            .heal_stop(Some("bucket"), None, Some("token-1"))
            .await
            .expect("token stop decodes");
        let super::HealStopOutcome::Stopped(status) = outcome else {
            panic!("token stop should return task status");
        };
        assert_eq!(status.summary, "stopped");
        let request = server.recorded();
        assert!(request.query.contains("forceStop=true"));
        assert!(request.query.contains("clientToken=token-1"));
    }

    #[tokio::test]
    async fn background_heal_status_posts_to_the_registered_route() {
        let body = r#"{"state":"idle","healQueueLength":0,"healActiveTasks":0,"clusterStatusComplete":true}"#;
        let server = TestServer::spawn(body, 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();

        let status = client.background_heal_status().await.expect("status decodes");
        assert_eq!(status.state, "idle");
        assert!(status.coverage.is_none(), "older HTTP responses retain unknown coverage");
        let request = server.recorded();
        // The server registers this route POST-only; a GET here answers 405.
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/rustfs/admin/v3/background-heal/status");
        assert_eq!(request.query, "");
    }

    #[tokio::test]
    async fn background_heal_status_decodes_partial_coverage_over_http() {
        let body = r#"{"state":"degraded","healQueueLength":0,"healActiveTasks":0,"clusterStatusComplete":false,"coverage":{"expected":3,"responded":1,"unknown":2,"reasons":["notification_system_unavailable"]},"futureStatus":true}"#;
        let server = TestServer::spawn(body, 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").expect("client builds");
        let status = client
            .background_heal_status()
            .await
            .expect("partial status is a successful response");
        assert_eq!(status.state, "degraded");
        assert!(!status.cluster_status_complete);
        assert_eq!(status.extra["futureStatus"], true);
        let coverage = status.coverage.expect("partial coverage supplied");
        assert_eq!(coverage.expected, Some(3));
        assert_eq!(coverage.responded, Some(1));
        assert_eq!(coverage.unknown, Some(2));
        assert_eq!(coverage.reasons, ["notification_system_unavailable"]);
        let request = server.recorded();
        assert_eq!(request.method, "POST");
        assert_eq!(request.query, "", "reading status must not send heal control parameters");
    }

    #[tokio::test]
    async fn http_error_status_maps_to_a_typed_error_with_body() {
        let server = TestServer::spawn(r#"{"code":"AccessDenied","message":"denied"}"#, 403).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();
        let err = client.scanner_status().await.unwrap_err();
        match err {
            AdminClientError::HttpStatus { status, body } => {
                assert_eq!(status, 403);
                assert!(body.contains("AccessDenied"));
            }
            other => panic!("expected HttpStatus, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn malformed_success_body_maps_to_a_decode_error() {
        let server = TestServer::spawn("not json", 200).await;
        let client = AdminClient::new(&format!("http://{}", server.addr), "ak", "sk").unwrap();
        assert!(matches!(client.scanner_status().await.unwrap_err(), AdminClientError::Decode { .. }));
    }
}
