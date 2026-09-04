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

//! Bucket-level On-Demand Migration configuration: wire model (JSON stored
//! under `on-demand-migration.json`), pure validation, credential redaction,
//! and the publish hook the runtime registers into (rustfs/backlog#2148).
//!
//! The persisted blob is not encrypted; it shares the trust boundary of
//! `bucket-targets.json` and `tier-config.bin`.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::OnceLock;
use url::Url;

/// The only wire version this build reads and writes.
pub const ON_DEMAND_MIGRATION_CONFIG_VERSION: u32 = 1;

const REDACTED: &str = "REDACTED";
const AUTO_REGION: &str = "auto";
const AUTO_REGION_FALLBACK: &str = "us-east-1";

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;

const NEGATIVE_CACHE_TTL_SECS_RANGE: (u64, u64) = (0, 3600);
const INLINE_MAX_BYTES_RANGE: (u64, u64) = (0, 256 * MIB);
const MULTIPART_PART_SIZE_RANGE: (u64, u64) = (5 * MIB, 5 * GIB);
const MAX_CONCURRENT_PULLS_RANGE: (u64, u64) = (1, 256);
const PULL_QUEUE_CAPACITY_RANGE: (u64, u64) = (1, 65_536);
const TIMEOUT_MS_RANGE: (u64, u64) = (100, 10 * 60 * 1000);
const MIN_BANDWIDTH_LIMIT_BYTES_PER_SEC: u64 = 64 * KIB;

/// Bucket-level On-Demand Migration configuration (wire shape, JSON).
///
/// Unknown fields are rejected so a config written by a newer build never
/// silently loses semantics on an older one; missing fields take the
/// documented defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OnDemandMigrationConfig {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub source: SourceConfig,
    #[serde(default)]
    pub filter: FilterConfig,
    #[serde(default)]
    pub policy: PolicyConfig,
}

/// The external S3-compatible source bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub provider: Provider,
    /// `http(s)://host[:port]` with no path or query. Optional only for
    /// [`Provider::Aws`], where it is derived from `region`.
    #[serde(default)]
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket: String,
    #[serde(default)]
    pub path_style: PathStyle,
    /// `None` means anonymous access to a public source bucket.
    #[serde(default)]
    pub credentials: Option<SourceCredentials>,
    #[serde(default)]
    pub tls: TlsConfig,
}

/// Source vendor family. `azure` is deliberately absent from this version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    /// Generic S3-compatible endpoint.
    S3,
    Aws,
    Minio,
    Rustfs,
    R2,
    /// GCS XML interoperability API with HMAC keys.
    Gcs,
}

impl Provider {
    pub fn as_str(&self) -> &'static str {
        match self {
            Provider::S3 => "s3",
            Provider::Aws => "aws",
            Provider::Minio => "minio",
            Provider::Rustfs => "rustfs",
            Provider::R2 => "r2",
            Provider::Gcs => "gcs",
        }
    }

    /// Providers whose SDKs accept `region = "auto"`; RustFS maps it to
    /// `us-east-1` for signing.
    fn accepts_auto_region(&self) -> bool {
        matches!(self, Provider::R2 | Provider::Minio | Provider::Rustfs)
    }
}

impl fmt::Display for Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Bucket addressing style. `auto` is resolved by the source client builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PathStyle {
    #[default]
    Auto,
    Path,
    Virtual,
}

/// Static credentials for the source. `Debug` never prints the secret or
/// the session token.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCredentials {
    pub access_key: String,
    pub secret_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
}

impl SourceCredentials {
    /// A copy safe to return to admin clients or log: the secret key and
    /// the session token (when present) are replaced by `REDACTED`.
    pub fn redacted(&self) -> Self {
        Self {
            access_key: self.access_key.clone(),
            secret_key: REDACTED.to_string(),
            session_token: self.session_token.as_ref().map(|_| REDACTED.to_string()),
        }
    }
}

impl fmt::Debug for SourceCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceCredentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &REDACTED)
            .field("session_token", &self.session_token.as_ref().map(|_| REDACTED))
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    #[serde(default)]
    pub skip_verify: bool,
    #[serde(default)]
    pub ca_cert_pem: Option<String>,
}

/// Key filters. `prefix` restricts which local keys trigger ODM;
/// `source_prefix` is prepended to the local key to form the source key.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilterConfig {
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub source_prefix: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HeadPolicy {
    #[default]
    Proxy,
    LocalOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeGetPolicy {
    #[default]
    ServeAndBackfill,
    ServeOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceErrorPolicy {
    #[default]
    Propagate,
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceTimeout {
    #[serde(default = "default_connect_ms")]
    pub connect_ms: u64,
    #[serde(default = "default_first_byte_ms")]
    pub first_byte_ms: u64,
    #[serde(default = "default_idle_ms")]
    pub idle_ms: u64,
}

impl Default for SourceTimeout {
    fn default() -> Self {
        Self {
            connect_ms: default_connect_ms(),
            first_byte_ms: default_first_byte_ms(),
            idle_ms: default_idle_ms(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyConfig {
    #[serde(default)]
    pub head: HeadPolicy,
    #[serde(default)]
    pub range_get: RangeGetPolicy,
    #[serde(default)]
    pub source_error: SourceErrorPolicy,
    /// Merge the source listing into `ListObjectsV2` so clients see the whole
    /// namespace during the migration (rustfs/backlog#2164). Off by default:
    /// it puts the source in the path of every listing.
    #[serde(default)]
    pub list_through: bool,
    #[serde(default = "default_true")]
    pub respect_local_delete_marker: bool,
    #[serde(default = "default_true")]
    pub preserve_etag: bool,
    #[serde(default)]
    pub copy_tags: bool,
    #[serde(default = "default_true")]
    pub emit_events: bool,
    #[serde(default = "default_negative_cache_ttl_secs")]
    pub negative_cache_ttl_secs: u64,
    #[serde(default = "default_inline_max_bytes")]
    pub inline_max_bytes: u64,
    #[serde(default = "default_multipart_part_size_bytes")]
    pub multipart_part_size_bytes: u64,
    #[serde(default = "default_max_concurrent_pulls")]
    pub max_concurrent_pulls: u32,
    #[serde(default = "default_pull_queue_capacity")]
    pub pull_queue_capacity: u32,
    #[serde(default)]
    pub source_timeout: SourceTimeout,
    #[serde(default)]
    pub bandwidth_limit_bytes_per_sec: Option<u64>,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            head: HeadPolicy::default(),
            range_get: RangeGetPolicy::default(),
            source_error: SourceErrorPolicy::default(),
            list_through: false,
            respect_local_delete_marker: true,
            preserve_etag: true,
            copy_tags: false,
            emit_events: true,
            negative_cache_ttl_secs: default_negative_cache_ttl_secs(),
            inline_max_bytes: default_inline_max_bytes(),
            multipart_part_size_bytes: default_multipart_part_size_bytes(),
            max_concurrent_pulls: default_max_concurrent_pulls(),
            pull_queue_capacity: default_pull_queue_capacity(),
            source_timeout: SourceTimeout::default(),
            bandwidth_limit_bytes_per_sec: None,
        }
    }
}

fn default_version() -> u32 {
    ON_DEMAND_MIGRATION_CONFIG_VERSION
}
fn default_true() -> bool {
    true
}
fn default_negative_cache_ttl_secs() -> u64 {
    30
}
fn default_inline_max_bytes() -> u64 {
    16 * MIB
}
fn default_multipart_part_size_bytes() -> u64 {
    64 * MIB
}
fn default_max_concurrent_pulls() -> u32 {
    8
}
fn default_pull_queue_capacity() -> u32 {
    1024
}
fn default_connect_ms() -> u64 {
    5000
}
fn default_first_byte_ms() -> u64 {
    15_000
}
fn default_idle_ms() -> u64 {
    30_000
}

/// Local facts the pure validator needs to reject self-referencing and
/// replication-loop sources. Endpoints are `scheme://host[:port][/...]`
/// URLs; a bare `host[:port]` is read as `http://host[:port]`.
#[derive(Debug, Clone, Copy)]
pub struct ValidationContext<'a> {
    pub local_bucket: &'a str,
    /// Reserved for the runtime's deployment-identity probe against
    /// `rustfs`/`minio` sources; no static rule consumes it yet.
    pub local_deployment_id: &'a str,
    pub local_endpoints: &'a [String],
    /// `(endpoint, bucket)` of every replication target of `local_bucket`.
    pub replication_target_endpoints: &'a [(String, String)],
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OnDemandMigrationConfigError {
    #[error("on-demand migration config version {0} is not supported; expected {ON_DEMAND_MIGRATION_CONFIG_VERSION}")]
    UnsupportedVersion(u32),
    #[error("on-demand migration config is not valid JSON: {0}")]
    Malformed(String),
    #[error("source endpoint is required for provider {0}")]
    MissingEndpoint(Provider),
    /// Carries only the reason: the endpoint string is operator input that
    /// may embed credentials, so it is never echoed into an error.
    #[error("source endpoint is invalid: {0}")]
    InvalidEndpoint(&'static str),
    #[error("source region must not be empty")]
    EmptyRegion,
    #[error("source region \"auto\" is not supported for provider {0}")]
    AutoRegionUnsupported(Provider),
    #[error("source bucket is invalid: {0}")]
    InvalidBucket(&'static str),
    #[error("source credentials field {0} must not be empty")]
    EmptyCredential(&'static str),
    #[error("source tls.ca_cert_pem is not a PEM certificate")]
    InvalidCaCert,
    #[error("filter.{0} must be null or a non-empty string")]
    EmptyFilterPrefix(&'static str),
    #[error("policy.{field} = {value} is outside {min}..={max}")]
    OutOfRange {
        field: &'static str,
        value: u64,
        min: u64,
        max: u64,
    },
    #[error("policy.bandwidth_limit_bytes_per_sec = {0} is below the minimum of {MIN_BANDWIDTH_LIMIT_BYTES_PER_SEC}")]
    BandwidthLimitTooLow(u64),
    #[error("source endpoint and bucket refer to this bucket on this deployment")]
    SelfReference,
    #[error("source endpoint and bucket match a replication target of this bucket")]
    ReplicationLoop,
}

impl OnDemandMigrationConfig {
    /// Parse the persisted JSON blob. Unknown fields are an error, not a
    /// silent drop.
    pub fn from_json(bytes: &[u8]) -> Result<Self, OnDemandMigrationConfigError> {
        serde_json::from_slice(bytes).map_err(|err| OnDemandMigrationConfigError::Malformed(err.to_string()))
    }

    pub fn to_json(&self) -> Result<Vec<u8>, OnDemandMigrationConfigError> {
        serde_json::to_vec(self).map_err(|err| OnDemandMigrationConfigError::Malformed(err.to_string()))
    }

    /// A copy safe to return to admin clients or log.
    pub fn redacted(&self) -> Self {
        let mut copy = self.clone();
        copy.source.credentials = self.source.credentials.as_ref().map(SourceCredentials::redacted);
        copy
    }

    /// Structural rules first, then the deployment-relative rules that need
    /// `ctx`. Reachability, private-network and SSRF checks belong to the
    /// source client builder, not here.
    pub fn validate(&self, ctx: ValidationContext<'_>) -> Result<(), OnDemandMigrationConfigError> {
        if self.version != ON_DEMAND_MIGRATION_CONFIG_VERSION {
            return Err(OnDemandMigrationConfigError::UnsupportedVersion(self.version));
        }
        self.source.validate()?;
        self.filter.validate()?;
        self.policy.validate()?;

        let source_key = EndpointKey::parse(&self.source.effective_endpoint())
            .ok_or(OnDemandMigrationConfigError::InvalidEndpoint("endpoint has no host"))?;
        let source_bucket = self.source.bucket.as_str();

        if source_bucket == ctx.local_bucket
            && ctx
                .local_endpoints
                .iter()
                .filter_map(|endpoint| EndpointKey::parse(endpoint))
                .any(|local| local == source_key)
        {
            return Err(OnDemandMigrationConfigError::SelfReference);
        }

        if ctx.replication_target_endpoints.iter().any(|(endpoint, bucket)| {
            bucket == source_bucket && EndpointKey::parse(endpoint).is_some_and(|target| target == source_key)
        }) {
            return Err(OnDemandMigrationConfigError::ReplicationLoop);
        }

        Ok(())
    }
}

impl SourceConfig {
    /// The endpoint the client will connect to: the configured one, or for
    /// `aws` the regional default derived from `region`.
    pub fn effective_endpoint(&self) -> String {
        match (&self.endpoint, self.provider) {
            (Some(endpoint), _) => endpoint.clone(),
            (None, Provider::Aws) => format!("https://s3.{}.amazonaws.com", self.region),
            (None, _) => String::new(),
        }
    }

    /// The signing region: `auto` maps to `us-east-1` for the providers that
    /// accept it.
    pub fn effective_region(&self) -> &str {
        if self.region == AUTO_REGION {
            AUTO_REGION_FALLBACK
        } else {
            self.region.as_str()
        }
    }

    fn validate(&self) -> Result<(), OnDemandMigrationConfigError> {
        if self.region.is_empty() {
            return Err(OnDemandMigrationConfigError::EmptyRegion);
        }
        if self.region == AUTO_REGION && !self.provider.accepts_auto_region() {
            return Err(OnDemandMigrationConfigError::AutoRegionUnsupported(self.provider));
        }

        match &self.endpoint {
            Some(endpoint) => validate_endpoint(endpoint)?,
            None if self.provider == Provider::Aws => {
                // Region feeds a hostname: keep it to label characters so a
                // stray "/" or "?" cannot rewrite the derived endpoint.
                if !self.region.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
                    return Err(OnDemandMigrationConfigError::InvalidEndpoint(
                        "region-derived endpoint contains characters outside [A-Za-z0-9-]",
                    ));
                }
            }
            None => return Err(OnDemandMigrationConfigError::MissingEndpoint(self.provider)),
        }

        if self.bucket.is_empty() {
            return Err(OnDemandMigrationConfigError::InvalidBucket("bucket must not be empty"));
        }
        if self.bucket.contains('/') || self.bucket.chars().any(char::is_whitespace) {
            return Err(OnDemandMigrationConfigError::InvalidBucket("bucket must not contain '/' or whitespace"));
        }

        if let Some(credentials) = &self.credentials {
            if credentials.access_key.is_empty() {
                return Err(OnDemandMigrationConfigError::EmptyCredential("access_key"));
            }
            if credentials.secret_key.is_empty() {
                return Err(OnDemandMigrationConfigError::EmptyCredential("secret_key"));
            }
            if credentials.session_token.as_deref().is_some_and(str::is_empty) {
                return Err(OnDemandMigrationConfigError::EmptyCredential("session_token"));
            }
        }

        if let Some(pem) = &self.tls.ca_cert_pem
            && !pem.contains("-----BEGIN CERTIFICATE-----")
        {
            return Err(OnDemandMigrationConfigError::InvalidCaCert);
        }

        Ok(())
    }
}

fn validate_endpoint(endpoint: &str) -> Result<(), OnDemandMigrationConfigError> {
    let invalid = OnDemandMigrationConfigError::InvalidEndpoint;
    let url = Url::parse(endpoint).map_err(|_| invalid("not an absolute URL"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(invalid("scheme must be http or https"));
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err(invalid("endpoint has no host"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(invalid("endpoint must not embed credentials"));
    }
    if !matches!(url.path(), "" | "/") {
        return Err(invalid("endpoint must not have a path"));
    }
    if url.query().is_some() {
        return Err(invalid("endpoint must not have a query"));
    }
    if url.fragment().is_some() {
        return Err(invalid("endpoint must not have a fragment"));
    }
    Ok(())
}

impl FilterConfig {
    fn validate(&self) -> Result<(), OnDemandMigrationConfigError> {
        if self.prefix.as_deref().is_some_and(str::is_empty) {
            return Err(OnDemandMigrationConfigError::EmptyFilterPrefix("prefix"));
        }
        if self.source_prefix.as_deref().is_some_and(str::is_empty) {
            return Err(OnDemandMigrationConfigError::EmptyFilterPrefix("source_prefix"));
        }
        Ok(())
    }
}

impl PolicyConfig {
    fn validate(&self) -> Result<(), OnDemandMigrationConfigError> {
        check_range("negative_cache_ttl_secs", self.negative_cache_ttl_secs, NEGATIVE_CACHE_TTL_SECS_RANGE)?;
        check_range("inline_max_bytes", self.inline_max_bytes, INLINE_MAX_BYTES_RANGE)?;
        check_range("multipart_part_size_bytes", self.multipart_part_size_bytes, MULTIPART_PART_SIZE_RANGE)?;
        check_range("max_concurrent_pulls", u64::from(self.max_concurrent_pulls), MAX_CONCURRENT_PULLS_RANGE)?;
        check_range("pull_queue_capacity", u64::from(self.pull_queue_capacity), PULL_QUEUE_CAPACITY_RANGE)?;
        check_range("source_timeout.connect_ms", self.source_timeout.connect_ms, TIMEOUT_MS_RANGE)?;
        check_range("source_timeout.first_byte_ms", self.source_timeout.first_byte_ms, TIMEOUT_MS_RANGE)?;
        check_range("source_timeout.idle_ms", self.source_timeout.idle_ms, TIMEOUT_MS_RANGE)?;
        if let Some(limit) = self.bandwidth_limit_bytes_per_sec
            && limit < MIN_BANDWIDTH_LIMIT_BYTES_PER_SEC
        {
            return Err(OnDemandMigrationConfigError::BandwidthLimitTooLow(limit));
        }
        Ok(())
    }
}

fn check_range(field: &'static str, value: u64, (min, max): (u64, u64)) -> Result<(), OnDemandMigrationConfigError> {
    if (min..=max).contains(&value) {
        Ok(())
    } else {
        Err(OnDemandMigrationConfigError::OutOfRange { field, value, min, max })
    }
}

/// `host:port` identity of an endpoint for the self-reference and loop
/// rules: the host is case-folded and an omitted port takes the scheme
/// default, so `https://H` and `https://h:443` are the same server. The
/// scheme itself is not compared; the same host and port behind a different
/// scheme is still the same server.
#[derive(Debug, PartialEq, Eq)]
struct EndpointKey {
    host: String,
    port: u16,
}

impl EndpointKey {
    fn parse(raw: &str) -> Option<Self> {
        // A filesystem path (single-node local disk endpoint) is not a
        // server; without this guard `http:///data/disk1` would parse with
        // host `data`.
        if raw.is_empty() || raw.starts_with('/') {
            return None;
        }
        let url = match Url::parse(raw) {
            Ok(url) if matches!(url.scheme(), "http" | "https") => url,
            _ => Url::parse(&format!("http://{raw}")).ok()?,
        };
        let host = url.host_str()?.to_ascii_lowercase();
        let port = url.port_or_known_default()?;
        Some(Self { host, port })
    }
}

/// Signature of the runtime publish hook: called with the bucket name and
/// its parsed config (`None` when absent, cleared, or unreadable) every time
/// the bucket's metadata is installed into or removed from the cache.
pub type ConfigPublishHook = Box<dyn Fn(&str, Option<&OnDemandMigrationConfig>) + Send + Sync>;

/// Registration point for the runtime (`OnDemandMigrationSys`). Until it is
/// set, metadata publishes are no-ops for ODM, so this crate carries no
/// runtime dependency and the config layer stays inert.
pub static ON_DEMAND_MIGRATION_CONFIG_HOOK: OnceLock<ConfigPublishHook> = OnceLock::new();

#[cfg(test)]
mod tests {
    use super::*;

    const FULL_JSON: &str = r#"{
  "version": 1,
  "enabled": true,
  "source": {
    "provider": "s3",
    "endpoint": "https://s3.us-west-1.wasabisys.com",
    "region": "us-west-1",
    "bucket": "legacy-bucket",
    "path_style": "auto",
    "credentials": { "access_key": "AK", "secret_key": "SK", "session_token": null },
    "tls": { "skip_verify": false, "ca_cert_pem": null }
  },
  "filter": { "prefix": null, "source_prefix": null },
  "policy": {
    "head": "proxy",
    "range_get": "serve_and_backfill",
    "source_error": "propagate",
    "list_through": false,
    "respect_local_delete_marker": true,
    "preserve_etag": true,
    "copy_tags": false,
    "emit_events": true,
    "negative_cache_ttl_secs": 30,
    "inline_max_bytes": 16777216,
    "multipart_part_size_bytes": 67108864,
    "max_concurrent_pulls": 8,
    "pull_queue_capacity": 1024,
    "source_timeout": { "connect_ms": 5000, "first_byte_ms": 15000, "idle_ms": 30000 },
    "bandwidth_limit_bytes_per_sec": null
  }
}"#;

    const MINIMAL_JSON: &str = r#"{"source":{"provider":"s3","endpoint":"https://s3.us-west-1.wasabisys.com","region":"us-west-1","bucket":"legacy-bucket"}}"#;

    fn sample() -> OnDemandMigrationConfig {
        OnDemandMigrationConfig::from_json(FULL_JSON.as_bytes()).expect("spec example must parse")
    }

    fn empty_ctx() -> ValidationContext<'static> {
        ValidationContext {
            local_bucket: "local",
            local_deployment_id: "deployment",
            local_endpoints: &[],
            replication_target_endpoints: &[],
        }
    }

    #[test]
    fn json_round_trip_is_lossless_and_minimal_takes_defaults() {
        let full = sample();
        let bytes = full.to_json().unwrap();
        let back = OnDemandMigrationConfig::from_json(&bytes).unwrap();
        assert_eq!(back, full);

        let minimal = OnDemandMigrationConfig::from_json(MINIMAL_JSON.as_bytes()).unwrap();
        let mut expected = full;
        expected.source.credentials = None;
        assert_eq!(minimal, expected, "every omitted field must take the documented default");
        assert_eq!(minimal.version, ON_DEMAND_MIGRATION_CONFIG_VERSION);
        assert!(minimal.enabled);
        assert_eq!(minimal.source.path_style, PathStyle::Auto);
        assert!(minimal.source.credentials.is_none());
        assert_eq!(minimal.source.tls, TlsConfig::default());
        assert_eq!(minimal.filter, FilterConfig::default());
        assert_eq!(minimal.policy, PolicyConfig::default());
        assert_eq!(minimal.policy.inline_max_bytes, 16 * MIB);
        assert_eq!(minimal.policy.multipart_part_size_bytes, 64 * MIB);
        assert_eq!(minimal.policy.source_timeout.first_byte_ms, 15_000);
    }

    #[test]
    fn unknown_fields_are_rejected_at_every_level() {
        for (label, json) in [
            (
                "top",
                r#"{"source":{"provider":"s3","endpoint":"https://h","region":"r","bucket":"b"},"extra":1}"#,
            ),
            (
                "source",
                r#"{"source":{"provider":"s3","endpoint":"https://h","region":"r","bucket":"b","extra":1}}"#,
            ),
            (
                "credentials",
                r#"{"source":{"provider":"s3","endpoint":"https://h","region":"r","bucket":"b","credentials":{"access_key":"a","secret_key":"s","extra":1}}}"#,
            ),
            (
                "policy",
                r#"{"source":{"provider":"s3","endpoint":"https://h","region":"r","bucket":"b"},"policy":{"extra":1}}"#,
            ),
            (
                "timeout",
                r#"{"source":{"provider":"s3","endpoint":"https://h","region":"r","bucket":"b"},"policy":{"source_timeout":{"extra":1}}}"#,
            ),
            (
                "provider enum",
                r#"{"source":{"provider":"azure","endpoint":"https://h","region":"r","bucket":"b"}}"#,
            ),
        ] {
            let err = OnDemandMigrationConfig::from_json(json.as_bytes()).expect_err(label);
            assert!(matches!(err, OnDemandMigrationConfigError::Malformed(_)), "{label}: {err}");
        }
    }

    #[test]
    fn enum_wire_names_match_the_spec() {
        let mut cfg = sample();
        cfg.source.provider = Provider::Gcs;
        cfg.source.path_style = PathStyle::Virtual;
        cfg.policy.head = HeadPolicy::LocalOnly;
        cfg.policy.range_get = RangeGetPolicy::ServeOnly;
        cfg.policy.source_error = SourceErrorPolicy::NotFound;
        let json = String::from_utf8(cfg.to_json().unwrap()).unwrap();
        for expected in [
            r#""provider":"gcs""#,
            r#""path_style":"virtual""#,
            r#""head":"local_only""#,
            r#""range_get":"serve_only""#,
            r#""source_error":"not_found""#,
        ] {
            assert!(json.contains(expected), "missing {expected} in {json}");
        }
    }

    #[test]
    fn spec_example_validates() {
        sample().validate(empty_ctx()).expect("spec example must validate");
    }

    #[test]
    fn version_must_be_one() {
        let mut cfg = sample();
        cfg.version = 2;
        assert_eq!(cfg.validate(empty_ctx()), Err(OnDemandMigrationConfigError::UnsupportedVersion(2)));
    }

    #[test]
    fn aws_derives_endpoint_from_region_but_other_providers_require_it() {
        let mut cfg = sample();
        cfg.source.provider = Provider::Aws;
        cfg.source.endpoint = None;
        cfg.source.region = "eu-central-1".to_string();
        cfg.validate(empty_ctx()).expect("aws may omit the endpoint");
        assert_eq!(cfg.source.effective_endpoint(), "https://s3.eu-central-1.amazonaws.com");

        cfg.source.region = "eu/../evil".to_string();
        assert!(matches!(cfg.validate(empty_ctx()), Err(OnDemandMigrationConfigError::InvalidEndpoint(_))));

        for provider in [Provider::S3, Provider::Minio, Provider::Rustfs, Provider::R2, Provider::Gcs] {
            let mut cfg = sample();
            cfg.source.provider = provider;
            cfg.source.endpoint = None;
            assert_eq!(
                cfg.validate(empty_ctx()),
                Err(OnDemandMigrationConfigError::MissingEndpoint(provider)),
                "{provider}"
            );
        }
    }

    #[test]
    fn endpoint_must_be_a_bare_http_origin() {
        for endpoint in [
            "http://h",
            "https://h",
            "https://h:9000",
            "https://h/",
            "http://10.0.0.5:9000",
            "http://[::1]:9000",
        ] {
            let mut cfg = sample();
            cfg.source.endpoint = Some(endpoint.to_string());
            cfg.validate(empty_ctx()).unwrap_or_else(|err| panic!("{endpoint}: {err}"));
        }
        for endpoint in [
            "h:9000",
            "ftp://h",
            "https://",
            "https://h/bucket",
            "https://h?x=1",
            "https://h#frag",
            "https://user:pw@h",
            "not a url",
            "",
        ] {
            let mut cfg = sample();
            cfg.source.endpoint = Some(endpoint.to_string());
            assert!(
                matches!(cfg.validate(empty_ctx()), Err(OnDemandMigrationConfigError::InvalidEndpoint(_))),
                "{endpoint:?} must be rejected"
            );
        }
    }

    #[test]
    fn region_rules() {
        let mut cfg = sample();
        cfg.source.region = String::new();
        assert_eq!(cfg.validate(empty_ctx()), Err(OnDemandMigrationConfigError::EmptyRegion));

        for provider in [Provider::R2, Provider::Minio, Provider::Rustfs] {
            let mut cfg = sample();
            cfg.source.provider = provider;
            cfg.source.region = "auto".to_string();
            cfg.validate(empty_ctx()).unwrap_or_else(|err| panic!("{provider}: {err}"));
            assert_eq!(cfg.source.effective_region(), "us-east-1");
        }
        for provider in [Provider::S3, Provider::Aws, Provider::Gcs] {
            let mut cfg = sample();
            cfg.source.provider = provider;
            cfg.source.region = "auto".to_string();
            assert_eq!(
                cfg.validate(empty_ctx()),
                Err(OnDemandMigrationConfigError::AutoRegionUnsupported(provider)),
                "{provider}"
            );
        }
        assert_eq!(sample().source.effective_region(), "us-west-1");
    }

    #[test]
    fn bucket_rules() {
        let mut cfg = sample();
        cfg.source.bucket = "ok_bucket.name".to_string();
        cfg.validate(empty_ctx()).unwrap();
        for bucket in ["", "a/b", "a b"] {
            let mut cfg = sample();
            cfg.source.bucket = bucket.to_string();
            assert!(
                matches!(cfg.validate(empty_ctx()), Err(OnDemandMigrationConfigError::InvalidBucket(_))),
                "{bucket:?}"
            );
        }
    }

    #[test]
    fn credentials_rules() {
        let mut cfg = sample();
        cfg.source.credentials = None;
        cfg.validate(empty_ctx()).expect("anonymous public source is allowed");

        let mut cfg = sample();
        cfg.source.credentials = Some(SourceCredentials {
            access_key: "AK".into(),
            secret_key: "SK".into(),
            session_token: Some("tok".into()),
        });
        cfg.validate(empty_ctx()).unwrap();

        for (field, creds) in [
            (
                "access_key",
                SourceCredentials {
                    access_key: String::new(),
                    secret_key: "SK".into(),
                    session_token: None,
                },
            ),
            (
                "secret_key",
                SourceCredentials {
                    access_key: "AK".into(),
                    secret_key: String::new(),
                    session_token: None,
                },
            ),
            (
                "session_token",
                SourceCredentials {
                    access_key: "AK".into(),
                    secret_key: "SK".into(),
                    session_token: Some(String::new()),
                },
            ),
        ] {
            let mut cfg = sample();
            cfg.source.credentials = Some(creds);
            assert_eq!(cfg.validate(empty_ctx()), Err(OnDemandMigrationConfigError::EmptyCredential(field)));
        }
    }

    #[test]
    fn tls_rules() {
        let mut cfg = sample();
        cfg.source.tls = TlsConfig {
            skip_verify: true,
            ca_cert_pem: Some("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n".into()),
        };
        cfg.validate(empty_ctx()).unwrap();

        cfg.source.tls.ca_cert_pem = Some("/etc/ssl/ca.pem".into());
        assert_eq!(cfg.validate(empty_ctx()), Err(OnDemandMigrationConfigError::InvalidCaCert));
    }

    #[test]
    fn filter_rules() {
        let mut cfg = sample();
        cfg.filter = FilterConfig {
            prefix: Some("photos/".into()),
            source_prefix: Some("archive/".into()),
        };
        cfg.validate(empty_ctx()).unwrap();

        cfg.filter.prefix = Some(String::new());
        assert_eq!(cfg.validate(empty_ctx()), Err(OnDemandMigrationConfigError::EmptyFilterPrefix("prefix")));
        cfg.filter.prefix = None;
        cfg.filter.source_prefix = Some(String::new());
        assert_eq!(
            cfg.validate(empty_ctx()),
            Err(OnDemandMigrationConfigError::EmptyFilterPrefix("source_prefix"))
        );
    }

    #[test]
    fn numeric_bounds() {
        type Set = fn(&mut PolicyConfig, u64);
        let cases: [(&str, Set, u64, u64); 8] = [
            ("negative_cache_ttl_secs", |p, v| p.negative_cache_ttl_secs = v, 0, 3600),
            ("inline_max_bytes", |p, v| p.inline_max_bytes = v, 0, 256 * MIB),
            ("multipart_part_size_bytes", |p, v| p.multipart_part_size_bytes = v, 5 * MIB, 5 * GIB),
            ("max_concurrent_pulls", |p, v| p.max_concurrent_pulls = v as u32, 1, 256),
            ("pull_queue_capacity", |p, v| p.pull_queue_capacity = v as u32, 1, 65_536),
            ("source_timeout.connect_ms", |p, v| p.source_timeout.connect_ms = v, 100, 600_000),
            ("source_timeout.first_byte_ms", |p, v| p.source_timeout.first_byte_ms = v, 100, 600_000),
            ("source_timeout.idle_ms", |p, v| p.source_timeout.idle_ms = v, 100, 600_000),
        ];
        for (field, set, min, max) in cases {
            for ok in [min, max] {
                let mut cfg = sample();
                set(&mut cfg.policy, ok);
                cfg.validate(empty_ctx()).unwrap_or_else(|err| panic!("{field}={ok}: {err}"));
            }
            let mut bad = Vec::new();
            if min > 0 {
                bad.push(min - 1);
            }
            bad.push(max + 1);
            for value in bad {
                let mut cfg = sample();
                set(&mut cfg.policy, value);
                assert_eq!(
                    cfg.validate(empty_ctx()),
                    Err(OnDemandMigrationConfigError::OutOfRange { field, value, min, max }),
                    "{field}={value}"
                );
            }
        }

        let mut cfg = sample();
        cfg.policy.bandwidth_limit_bytes_per_sec = Some(64 * KIB);
        cfg.validate(empty_ctx()).unwrap();
        cfg.policy.bandwidth_limit_bytes_per_sec = None;
        cfg.validate(empty_ctx()).unwrap();
        cfg.policy.bandwidth_limit_bytes_per_sec = Some(64 * KIB - 1);
        assert_eq!(
            cfg.validate(empty_ctx()),
            Err(OnDemandMigrationConfigError::BandwidthLimitTooLow(64 * KIB - 1))
        );
    }

    #[test]
    fn self_reference_is_rejected_with_host_case_and_default_port_normalized() {
        let local_endpoints = vec![
            "https://Node-1.Example.com:443/data/disk1".to_string(),
            "http://node-2:9000".to_string(),
        ];
        let ctx = ValidationContext {
            local_bucket: "legacy-bucket",
            local_deployment_id: "deployment",
            local_endpoints: &local_endpoints,
            replication_target_endpoints: &[],
        };

        for endpoint in [
            "https://node-1.example.com",
            "HTTPS://NODE-1.EXAMPLE.COM:443",
            "http://node-2:9000",
        ] {
            let mut cfg = sample();
            cfg.source.endpoint = Some(endpoint.to_string());
            assert_eq!(cfg.validate(ctx), Err(OnDemandMigrationConfigError::SelfReference), "{endpoint}");
        }

        // Same server, different bucket: allowed.
        let mut cfg = sample();
        cfg.source.endpoint = Some("https://node-1.example.com".to_string());
        cfg.source.bucket = "other-bucket".to_string();
        cfg.validate(ctx).unwrap();

        // Same bucket name on a different port or host: allowed.
        for endpoint in ["https://node-1.example.com:9443", "https://node-3.example.com"] {
            let mut cfg = sample();
            cfg.source.endpoint = Some(endpoint.to_string());
            cfg.validate(ctx).unwrap_or_else(|err| panic!("{endpoint}: {err}"));
        }
    }

    #[test]
    fn replication_loop_is_rejected_with_host_case_and_default_port_normalized() {
        let targets = vec![
            ("https://Replica.Example.com".to_string(), "legacy-bucket".to_string()),
            ("http://other:9000".to_string(), "unrelated".to_string()),
        ];
        let ctx = ValidationContext {
            local_bucket: "local",
            local_deployment_id: "deployment",
            local_endpoints: &[],
            replication_target_endpoints: &targets,
        };

        for endpoint in ["https://replica.example.com:443", "https://REPLICA.example.com"] {
            let mut cfg = sample();
            cfg.source.endpoint = Some(endpoint.to_string());
            assert_eq!(cfg.validate(ctx), Err(OnDemandMigrationConfigError::ReplicationLoop), "{endpoint}");
        }

        // Same target host but a different bucket, or the same bucket on a
        // different target: allowed.
        let mut cfg = sample();
        cfg.source.endpoint = Some("https://replica.example.com".to_string());
        cfg.source.bucket = "unrelated".to_string();
        cfg.validate(ctx).unwrap();
        let mut cfg = sample();
        cfg.source.endpoint = Some("http://other:9000".to_string());
        cfg.validate(ctx).unwrap();
    }

    #[test]
    fn aws_derived_endpoint_participates_in_loop_rules() {
        let targets = vec![("https://s3.us-east-2.amazonaws.com".to_string(), "legacy-bucket".to_string())];
        let ctx = ValidationContext {
            local_bucket: "local",
            local_deployment_id: "deployment",
            local_endpoints: &[],
            replication_target_endpoints: &targets,
        };
        let mut cfg = sample();
        cfg.source.provider = Provider::Aws;
        cfg.source.endpoint = None;
        cfg.source.region = "us-east-2".to_string();
        assert_eq!(cfg.validate(ctx), Err(OnDemandMigrationConfigError::ReplicationLoop));
    }

    #[test]
    fn redacted_and_debug_never_expose_secrets() {
        let mut cfg = sample();
        cfg.source.credentials = Some(SourceCredentials {
            access_key: "AKIAEXAMPLE".into(),
            secret_key: "hunter2-secret".into(),
            session_token: Some("session-token-value".into()),
        });

        let redacted = cfg.redacted();
        let creds = redacted.source.credentials.as_ref().unwrap();
        assert_eq!(creds.access_key, "AKIAEXAMPLE");
        assert_eq!(creds.secret_key, "REDACTED");
        assert_eq!(creds.session_token.as_deref(), Some("REDACTED"));
        let redacted_json = String::from_utf8(redacted.to_json().unwrap()).unwrap();
        assert!(!redacted_json.contains("hunter2-secret"));
        assert!(!redacted_json.contains("session-token-value"));

        let debug = format!("{cfg:?}");
        assert!(debug.contains("AKIAEXAMPLE"));
        assert!(!debug.contains("hunter2-secret"), "{debug}");
        assert!(!debug.contains("session-token-value"), "{debug}");
        assert!(debug.contains("REDACTED"));

        // An absent session token stays visibly absent after redaction.
        let mut cfg = sample();
        cfg.source.credentials.as_mut().unwrap().session_token = None;
        assert!(cfg.redacted().source.credentials.unwrap().session_token.is_none());
        assert!(format!("{cfg:?}").contains("session_token: None"));

        // Redaction of an anonymous config is a no-op.
        cfg.source.credentials = None;
        assert_eq!(cfg.redacted(), cfg);
    }

    #[test]
    fn endpoint_key_normalization() {
        let key = |s: &str| EndpointKey::parse(s).unwrap_or_else(|| panic!("{s}"));
        assert_eq!(key("https://h"), key("https://h:443"));
        assert_eq!(key("http://h"), key("http://h:80"));
        assert_eq!(key("https://H.Example.COM"), key("https://h.example.com"));
        assert_eq!(key("https://h:9000/some/path"), key("https://h:9000"));
        assert_eq!(key("h:9000"), key("http://h:9000"));
        assert_ne!(key("https://h"), key("http://h"));
        assert_ne!(key("https://h:9000"), key("https://h:9001"));
        assert!(EndpointKey::parse("").is_none());
        assert!(EndpointKey::parse("/data/disk1").is_none());
    }

    #[test]
    fn error_messages_do_not_carry_credentials() {
        let mut cfg = sample();
        cfg.source.endpoint = Some("https://user:topsecret@h".into());
        let err = cfg.validate(empty_ctx()).unwrap_err();
        assert!(matches!(err, OnDemandMigrationConfigError::InvalidEndpoint(_)));
        let rendered = format!("{err} / {err:?}");
        assert!(!rendered.contains("topsecret"), "{rendered}");
        assert!(!rendered.contains("SK"), "{rendered}");
    }
}
