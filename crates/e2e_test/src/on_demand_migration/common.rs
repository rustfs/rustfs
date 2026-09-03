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

//! Shared environment for on-demand migration (ODM) end-to-end tests.
//!
//! [`OdmTestEnv`] pairs one RustFS server under test with one in-process
//! programmable S3 source ([`FakeS3Target`]). Admin calls target the route
//! convention fixed by the tracking plan
//! (`/rustfs/admin/v3/on-demand-migration/{bucket}`, JSON bodies); the
//! server side lands with ODM-07, so until then the wrappers compile but are
//! not exercised by the harness self-test.

use crate::common::{RustFSTestEnvironment, signed_request};
use crate::fake_s3_target::{
    BucketMode, FAKE_ACCESS_KEY, FAKE_SECRET_KEY, FakeS3Target, FakeS3TargetOptions, Operation, SeedMetadata,
};
use aws_config::retry::RetryConfig;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
use bytes::Bytes;
use serde::Serialize;
use std::fmt;
use std::time::{Duration, Instant};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Module switch the server reads at startup (`false` before GA). The harness
/// turns it on so scenario tests exercise the feature without repeating it.
pub const ODM_MODULE_SWITCH_ENV: &str = "RUSTFS_ON_DEMAND_MIGRATION_ENABLED";
/// The source client shares the replication egress guard, which rejects the
/// fake source's loopback endpoint unless this switch is set.
pub const ALLOW_LOOPBACK_SOURCE_ENV: &str = "RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET";
/// Admin route prefix; the bucket name is appended as one path segment.
pub const ODM_ADMIN_ROUTE: &str = "/rustfs/admin/v3/on-demand-migration";
/// Region the fake source is addressed with (it accepts any SigV4 region).
pub const FAKE_SOURCE_REGION: &str = "us-east-1";

/// Wire form of the bucket-level ODM configuration (ODM-01 model). Every
/// field is public so a scenario can tweak one knob and serialize the rest
/// with the documented defaults.
#[derive(Debug, Clone, Serialize)]
pub struct OdmSourceSpec {
    pub version: u32,
    pub enabled: bool,
    pub source: OdmSource,
    pub filter: OdmFilter,
    pub policy: OdmPolicy,
}

#[derive(Debug, Clone, Serialize)]
pub struct OdmSource {
    pub provider: String,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub path_style: String,
    pub credentials: Option<OdmCredentials>,
    pub tls: OdmTls,
}

#[derive(Clone, Serialize)]
pub struct OdmCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

impl fmt::Debug for OdmCredentials {
    /// Test logs are captured into CI artifacts; keep the secret out of them.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OdmCredentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &"REDACTED")
            .field("session_token", &self.session_token.as_ref().map(|_| "REDACTED"))
            .finish()
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct OdmTls {
    pub skip_verify: bool,
    pub ca_cert_pem: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct OdmFilter {
    pub prefix: Option<String>,
    pub source_prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OdmPolicy {
    pub head: String,
    pub range_get: String,
    pub source_error: String,
    pub respect_local_delete_marker: bool,
    pub preserve_etag: bool,
    pub copy_tags: bool,
    pub emit_events: bool,
    pub negative_cache_ttl_secs: u64,
    pub inline_max_bytes: u64,
    pub multipart_part_size_bytes: u64,
    pub max_concurrent_pulls: u32,
    pub pull_queue_capacity: u32,
    pub source_timeout: OdmSourceTimeout,
    pub bandwidth_limit_bytes_per_sec: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OdmSourceTimeout {
    pub connect_ms: u64,
    pub first_byte_ms: u64,
    pub idle_ms: u64,
}

impl Default for OdmPolicy {
    /// The ODM-01 defaults verbatim.
    fn default() -> Self {
        Self {
            head: "proxy".to_string(),
            range_get: "serve_and_backfill".to_string(),
            source_error: "propagate".to_string(),
            respect_local_delete_marker: true,
            preserve_etag: true,
            copy_tags: false,
            emit_events: true,
            negative_cache_ttl_secs: 30,
            inline_max_bytes: 16 * 1024 * 1024,
            multipart_part_size_bytes: 64 * 1024 * 1024,
            max_concurrent_pulls: 8,
            pull_queue_capacity: 1024,
            source_timeout: OdmSourceTimeout {
                connect_ms: 5_000,
                first_byte_ms: 15_000,
                idle_ms: 30_000,
            },
            bandwidth_limit_bytes_per_sec: None,
        }
    }
}

impl OdmSourceSpec {
    /// Enabled configuration pointing at a bucket on the fake source with the
    /// fixture credentials, path-style addressing, and default policy.
    pub fn for_fake_source(source: &FakeS3Target, source_bucket: impl Into<String>) -> Self {
        Self::new(
            "s3",
            source.endpoint(),
            FAKE_SOURCE_REGION,
            source_bucket,
            FAKE_ACCESS_KEY,
            FAKE_SECRET_KEY,
        )
    }

    /// Enabled configuration pointing at a bucket on a second RustFS server
    /// (see [`start_source_rustfs`]).
    pub fn for_rustfs_source(source: &RustFSTestEnvironment, source_bucket: impl Into<String>) -> Self {
        Self::new(
            "rustfs",
            &source.url,
            FAKE_SOURCE_REGION,
            source_bucket,
            &source.access_key,
            &source.secret_key,
        )
    }

    fn new(
        provider: &str,
        endpoint: &str,
        region: &str,
        source_bucket: impl Into<String>,
        access_key: &str,
        secret_key: &str,
    ) -> Self {
        Self {
            version: 1,
            enabled: true,
            source: OdmSource {
                provider: provider.to_string(),
                endpoint: endpoint.to_string(),
                region: region.to_string(),
                bucket: source_bucket.into(),
                path_style: "path".to_string(),
                credentials: Some(OdmCredentials {
                    access_key: access_key.to_string(),
                    secret_key: secret_key.to_string(),
                    session_token: None,
                }),
                tls: OdmTls::default(),
            },
            filter: OdmFilter::default(),
            policy: OdmPolicy::default(),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("ODM source spec serializes")
    }
}

/// Backfill job control (ODM-12 route shape).
#[derive(Debug, Clone)]
pub enum BackfillOp {
    Start(BackfillRequest),
    Cancel,
    Status,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct BackfillRequest {
    pub prefix: Option<String>,
    pub skip_existing: Option<String>,
    pub dry_run: bool,
}

/// Status plus raw body of an admin call, so a scenario can assert on the
/// HTTP status first and only then parse the JSON.
#[derive(Debug, Clone)]
pub struct AdminResponse {
    pub status: u16,
    pub body: String,
}

impl AdminResponse {
    pub fn json(&self) -> Result<serde_json::Value, BoxError> {
        Ok(serde_json::from_str(&self.body)?)
    }
}

/// Raw S3 response (status, headers, body) for assertions on headers the
/// SDK does not surface, such as `x-rustfs-on-demand-migration`.
#[derive(Debug, Clone)]
pub struct RawResponse {
    pub status: u16,
    pub headers: http::HeaderMap,
    pub body: Bytes,
}

impl RawResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }
}

/// One object to seed into the source.
#[derive(Clone)]
pub struct SeedObject {
    pub key: String,
    pub body: Bytes,
    pub metadata: SeedMetadata,
}

impl SeedObject {
    pub fn new(key: impl Into<String>, body: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            body: body.into(),
            metadata: SeedMetadata::new(),
        }
    }

    pub fn with_metadata(mut self, metadata: SeedMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Process arguments and environment a scenario needs on top of the ODM
/// defaults, plus the fake source's own limits.
#[derive(Debug, Default)]
pub struct OdmEnvOptions<'a> {
    pub source: FakeS3TargetOptions,
    pub args: Vec<&'a str>,
    pub env: Vec<(&'a str, &'a str)>,
    /// Start the server with the file-backed KMS and a default key, so
    /// bucket default encryption (SSE-S3) can be configured.
    pub local_kms: bool,
}

/// Default key id of the [`OdmEnvOptions::local_kms`] backend.
pub const LOCAL_KMS_DEFAULT_KEY_ID: &str = "rustfs-odm-e2e-default-key";

/// RustFS under test plus its fake S3 source.
pub struct OdmTestEnv {
    pub rustfs: RustFSTestEnvironment,
    pub source: FakeS3Target,
    /// S3 client for the RustFS under test.
    pub client: Client,
}

impl OdmTestEnv {
    /// Start a fake source with default limits and a RustFS server with the
    /// ODM module switch enabled.
    pub async fn start() -> Result<Self, BoxError> {
        Self::start_with_options(FakeS3TargetOptions::default()).await
    }

    pub async fn start_with_options(options: FakeS3TargetOptions) -> Result<Self, BoxError> {
        Self::start_with(OdmEnvOptions {
            source: options,
            ..OdmEnvOptions::default()
        })
        .await
    }

    /// Start the pair with extra process arguments and environment for the
    /// server under test (KMS, the usage scanner, replication timing). The
    /// ODM module switch and the loopback-source opt-in are always set; a
    /// caller-supplied entry with the same name wins.
    pub async fn start_with(options: OdmEnvOptions<'_>) -> Result<Self, BoxError> {
        let source = FakeS3Target::start_with_options(options.source).await?;
        let mut rustfs = RustFSTestEnvironment::new().await?;
        let mut env: Vec<(&str, &str)> = vec![(ODM_MODULE_SWITCH_ENV, "true"), (ALLOW_LOOPBACK_SOURCE_ENV, "true")];
        for (name, value) in &options.env {
            match env.iter_mut().find(|(existing, _)| existing == name) {
                Some(entry) => entry.1 = value,
                None => env.push((name, value)),
            }
        }
        let mut args: Vec<&str> = options.args;
        let key_dir = format!("{}/kms-keys", rustfs.temp_dir);
        if options.local_kms {
            tokio::fs::create_dir_all(&key_dir).await?;
            crate::kms::common::create_key_with_specific_id(&key_dir, LOCAL_KMS_DEFAULT_KEY_ID).await?;
            args.extend_from_slice(&[
                "--kms-enable",
                "--kms-backend",
                "local",
                "--kms-key-dir",
                &key_dir,
                "--kms-default-key-id",
                LOCAL_KMS_DEFAULT_KEY_ID,
            ]);
            env.push(("RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS", "true"));
        }
        rustfs.start_rustfs_server_with_env(args, &env).await?;
        let client = rustfs.create_s3_client();
        Ok(Self { rustfs, source, client })
    }

    /// S3 client addressing the fake source directly, for assertions on the
    /// source's own state. Retries are off so a scripted fault is consumed by
    /// exactly the request the test issued.
    pub fn source_client(&self) -> Client {
        fake_source_client(&self.source)
    }

    /// Enabled ODM configuration for `source_bucket` on the fake source.
    pub fn fake_source_spec(&self, source_bucket: impl Into<String>) -> OdmSourceSpec {
        OdmSourceSpec::for_fake_source(&self.source, source_bucket)
    }

    /// `PUT /rustfs/admin/v3/on-demand-migration/{bucket}` with the JSON spec.
    pub async fn configure_source(&self, bucket: &str, spec: &OdmSourceSpec) -> Result<AdminResponse, BoxError> {
        self.admin(http::Method::PUT, &format!("/{bucket}"), Some(spec.to_json()))
            .await
    }

    /// Same as [`Self::configure_source`] with `dry-run=true`: validate and
    /// probe without persisting.
    pub async fn validate_source(&self, bucket: &str, spec: &OdmSourceSpec) -> Result<AdminResponse, BoxError> {
        self.admin(http::Method::PUT, &format!("/{bucket}?dry-run=true"), Some(spec.to_json()))
            .await
    }

    /// `GET .../{bucket}`: redacted configuration, 404 when unconfigured.
    pub async fn get_config(&self, bucket: &str) -> Result<AdminResponse, BoxError> {
        self.admin(http::Method::GET, &format!("/{bucket}"), None).await
    }

    /// `DELETE .../{bucket}`: remove the configuration (idempotent).
    pub async fn disable(&self, bucket: &str) -> Result<AdminResponse, BoxError> {
        self.admin(http::Method::DELETE, &format!("/{bucket}"), None).await
    }

    /// `GET .../{bucket}/status`: runtime snapshot.
    pub async fn status(&self, bucket: &str) -> Result<AdminResponse, BoxError> {
        self.admin(http::Method::GET, &format!("/{bucket}/status"), None).await
    }

    /// Backfill control: `POST .../{bucket}/backfill?op=start|cancel` or
    /// `GET .../{bucket}/backfill` for the checkpoint.
    pub async fn backfill(&self, bucket: &str, op: BackfillOp) -> Result<AdminResponse, BoxError> {
        match op {
            BackfillOp::Start(request) => {
                self.admin(
                    http::Method::POST,
                    &format!("/{bucket}/backfill?op=start"),
                    Some(serde_json::to_value(request)?),
                )
                .await
            }
            BackfillOp::Cancel => {
                self.admin(http::Method::POST, &format!("/{bucket}/backfill?op=cancel"), None)
                    .await
            }
            BackfillOp::Status => self.admin(http::Method::GET, &format!("/{bucket}/backfill"), None).await,
        }
    }

    async fn admin(
        &self,
        method: http::Method,
        path_and_query: &str,
        body: Option<serde_json::Value>,
    ) -> Result<AdminResponse, BoxError> {
        let url = format!("{}{ODM_ADMIN_ROUTE}{path_and_query}", self.rustfs.url);
        let body = body.map(|value| serde_json::to_vec(&value)).transpose()?;
        let content_type = body.is_some().then_some("application/json");
        let response = signed_request(method, &url, &self.rustfs.access_key, &self.rustfs.secret_key, body, content_type).await?;
        Ok(AdminResponse {
            status: response.status().as_u16(),
            body: response.text().await?,
        })
    }

    /// Store objects directly in the fake source (no wire traffic, no journal
    /// entries). Returns the ETags in input order.
    pub fn seed_source(&self, source_bucket: &str, objects: &[SeedObject]) -> Vec<String> {
        objects
            .iter()
            .map(|object| {
                self.source
                    .put_seed_object(source_bucket, object.key.clone(), object.body.clone(), &object.metadata)
            })
            .collect()
    }

    /// Whether `key` is listed by the RustFS under test. Listing is served from
    /// local state only, so this does not trigger a migration the way GET or
    /// HEAD would.
    pub async fn local_key_listed(&self, bucket: &str, key: &str) -> Result<bool, BoxError> {
        let listed = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(key)
            .max_keys(1)
            .send()
            .await?;
        Ok(listed.contents().iter().any(|object| object.key() == Some(key)))
    }

    /// Panics unless `key` is stored locally with exactly `expected` bytes.
    /// Presence is checked through listing first so a missing object fails
    /// here instead of being pulled from the source by the GET.
    pub async fn assert_local_present(&self, bucket: &str, key: &str, expected: &[u8]) {
        assert!(
            self.local_key_listed(bucket, key)
                .await
                .unwrap_or_else(|error| panic!("listing {bucket}/{key} failed: {error}")),
            "{bucket}/{key} must be present locally"
        );
        let body = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap_or_else(|error| panic!("GET {bucket}/{key} failed: {error}"))
            .body
            .collect()
            .await
            .unwrap_or_else(|error| panic!("reading {bucket}/{key} failed: {error}"))
            .into_bytes();
        assert_eq!(body.as_ref(), expected, "{bucket}/{key} local content mismatch");
    }

    /// Polls the listing until `key` is present locally or `timeout` elapses
    /// (background pulls land after the response that triggered them).
    pub async fn wait_local_listed(&self, bucket: &str, key: &str, timeout: Duration) -> Result<bool, BoxError> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.local_key_listed(bucket, key).await? {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Raw signed `GET /{bucket}/{key}` against the RustFS under test.
    pub async fn raw_get(&self, bucket: &str, key: &str) -> Result<RawResponse, BoxError> {
        let url = format!("{}/{bucket}/{key}", self.rustfs.url);
        let response =
            signed_request(http::Method::GET, &url, &self.rustfs.access_key, &self.rustfs.secret_key, None, None).await?;
        Ok(RawResponse {
            status: response.status().as_u16(),
            headers: response.headers().clone(),
            body: response.bytes().await?,
        })
    }

    /// Waits until the runtime consults the source for `bucket`: a config
    /// install is applied asynchronously after the admin call returns. The
    /// probe is a HEAD on a key that exists nowhere, so nothing is pulled and
    /// only that key enters the negative cache. The probe key is per bucket,
    /// so a second bucket, or a reinstalled configuration, waits for its own
    /// state instead of observing an earlier journal entry.
    pub async fn wait_until_source_consulted(&self, bucket: &str) -> Result<(), BoxError> {
        let probe_key = format!("_odm-readiness-probe-{bucket}-{}", uuid::Uuid::new_v4());
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let _ = self.client.head_object().bucket(bucket).key(&probe_key).send().await;
            if self.source.count_requests(Operation::HeadObject, &probe_key) > 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!("on-demand migration runtime for {bucket} did not consult the source in time").into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Creates `bucket` unless it already exists, installs `spec` on it and
    /// returns once the runtime consults the source. Scenarios with a second
    /// bucket, a bucket created with non-default options, or a reinstalled
    /// configuration all go through this.
    pub async fn configure_and_wait(&self, bucket: &str, spec: &OdmSourceSpec) -> Result<(), BoxError> {
        if self.client.head_bucket().bucket(bucket).send().await.is_err() {
            self.rustfs.create_test_bucket(bucket).await?;
        }
        let response = self.configure_source(bucket, spec).await?;
        if response.status != 200 {
            return Err(format!("configure on-demand migration for {bucket}: {} {}", response.status, response.body).into());
        }
        self.wait_until_source_consulted(bucket).await
    }

    /// Raw signed request against the RustFS under test with additional
    /// request headers. `Range`, `If-None-Match` and the anti-loop marker are
    /// not part of the SigV4 signed-header set, so they are attached after
    /// signing exactly as a real client's would be.
    pub async fn raw_object_request(
        &self,
        method: http::Method,
        bucket: &str,
        key: &str,
        headers: &[(&str, &str)],
    ) -> Result<RawResponse, BoxError> {
        let url = format!("{}/{bucket}/{key}", self.rustfs.url);
        let uri = url.parse::<http::Uri>()?;
        let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
        let request = http::Request::builder()
            .method(method.clone())
            .uri(uri)
            .header(http::header::HOST, authority)
            .header("x-amz-content-sha256", rustfs_signer::constants::UNSIGNED_PAYLOAD)
            .body(s3s::Body::empty())?;
        let signed = rustfs_signer::sign_v4(request, 0, &self.rustfs.access_key, &self.rustfs.secret_key, "", "us-east-1");

        let mut builder =
            crate::common::local_http_client().request(reqwest::Method::from_bytes(method.as_str().as_bytes())?, &url);
        for (name, value) in signed.headers() {
            builder = builder.header(name, value);
        }
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let response = builder.send().await?;
        Ok(RawResponse {
            status: response.status().as_u16(),
            headers: response.headers().clone(),
            body: response.bytes().await?,
        })
    }

    /// Parsed `GET .../{bucket}/status` body. Fails when the route does not
    /// answer 200 so a scenario never asserts against an error document.
    pub async fn status_json(&self, bucket: &str) -> Result<serde_json::Value, BoxError> {
        let response = self.status(bucket).await?;
        if response.status != 200 {
            return Err(format!("status for {bucket}: {} {}", response.status, response.body).into());
        }
        response.json()
    }

    /// Reads one counter out of the status document by JSON pointer, e.g.
    /// `/counters/pull_failures_total/queue_full`. Missing runtime state
    /// reads as 0, which is what an operator sees too.
    pub async fn status_counter(&self, bucket: &str, pointer: &str) -> Result<u64, BoxError> {
        Ok(self
            .status_json(bucket)
            .await?
            .pointer(pointer)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0))
    }

    /// Polls [`Self::status_counter`] until it reaches `at_least`. Background
    /// pulls and their failure counters land after the response that started
    /// them, so every counter assertion about them has to wait.
    pub async fn wait_for_status_counter(
        &self,
        bucket: &str,
        pointer: &str,
        at_least: u64,
        timeout: Duration,
    ) -> Result<u64, BoxError> {
        let deadline = Instant::now() + timeout;
        loop {
            let value = self.status_counter(bucket, pointer).await?;
            if value >= at_least {
                return Ok(value);
            }
            if Instant::now() >= deadline {
                return Err(format!("{pointer} for {bucket} stalled at {value}, expected at least {at_least}").into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Highest `inflight_pulls` the status route reported while `work` ran,
    /// sampled every 5 ms. The concurrency ceiling is only observable from
    /// outside while pulls are in flight.
    pub async fn peak_inflight_pulls<F, T>(&self, bucket: &str, work: F) -> Result<(T, u64), BoxError>
    where
        F: std::future::Future<Output = T>,
    {
        let mut peak = 0;
        tokio::pin!(work);
        let output = loop {
            tokio::select! {
                output = &mut work => break output,
                _ = tokio::time::sleep(Duration::from_millis(5)) => {
                    peak = peak.max(self.status_counter(bucket, "/inflight_pulls").await?);
                }
            }
        };
        Ok((output, peak))
    }

    /// Panics if `key` is listed locally.
    pub async fn assert_local_absent(&self, bucket: &str, key: &str) {
        assert!(
            !self
                .local_key_listed(bucket, key)
                .await
                .unwrap_or_else(|error| panic!("listing {bucket}/{key} failed: {error}")),
            "{bucket}/{key} must be absent locally"
        );
    }
}

/// S3 client for the fake source with retries disabled (see
/// [`OdmTestEnv::source_client`]).
pub fn fake_source_client(source: &FakeS3Target) -> Client {
    let credentials = Credentials::new(FAKE_ACCESS_KEY, FAKE_SECRET_KEY, None, None, "odm-fake-source");
    Client::from_conf(
        aws_sdk_s3::Config::builder()
            .credentials_provider(credentials)
            .region(Region::new(FAKE_SOURCE_REGION))
            .endpoint_url(source.endpoint())
            .force_path_style(true)
            .behavior_version_latest()
            .retry_config(RetryConfig::standard().with_max_attempts(1))
            .http_client(SmithyHttpClientBuilder::new().build_http())
            .build(),
    )
}

/// Start a second, fully independent RustFS process (own port, data
/// directory, and default credentials) to act as a real S3 source. It is
/// spawned the same way `reliant::tiering` starts its cold tier; the process
/// is stopped and its directory removed when the returned environment drops.
pub async fn start_source_rustfs() -> Result<RustFSTestEnvironment, BoxError> {
    let mut source = RustFSTestEnvironment::new().await?;
    source.start_rustfs_server_without_cleanup(vec![]).await?;
    Ok(source)
}

/// Like [`start_source_rustfs`], but with on-demand migration enabled on the
/// second server too, so it can be given a source of its own (the anti-loop
/// scenario chains two migrating servers).
pub async fn start_source_rustfs_with_odm() -> Result<RustFSTestEnvironment, BoxError> {
    let mut source = RustFSTestEnvironment::new().await?;
    source
        .start_rustfs_server_without_cleanup_with_env(&[(ODM_MODULE_SWITCH_ENV, "true"), (ALLOW_LOOPBACK_SOURCE_ENV, "true")])
        .await?;
    Ok(source)
}

/// The full scenario fixture every behavior test starts from: a RustFS with
/// `bucket`, an unversioned `source_bucket` on the fake source (a plain
/// migration source), the configuration installed after `adjust` tweaked it,
/// and the runtime proven to consult the source.
pub async fn start_configured_env(
    bucket: &str,
    source_bucket: &str,
    adjust: impl FnOnce(&mut OdmSourceSpec),
) -> Result<OdmTestEnv, BoxError> {
    start_configured_env_with(OdmEnvOptions::default(), bucket, source_bucket, adjust).await
}

/// [`start_configured_env`] with extra process arguments and environment.
pub async fn start_configured_env_with(
    options: OdmEnvOptions<'_>,
    bucket: &str,
    source_bucket: &str,
    adjust: impl FnOnce(&mut OdmSourceSpec),
) -> Result<OdmTestEnv, BoxError> {
    let env = OdmTestEnv::start_with(options).await?;
    env.source.create_bucket_with_mode(source_bucket, BucketMode::Unversioned);
    let mut spec = env.fake_source_spec(source_bucket);
    adjust(&mut spec);
    env.configure_and_wait(bucket, &spec).await?;
    Ok(env)
}
