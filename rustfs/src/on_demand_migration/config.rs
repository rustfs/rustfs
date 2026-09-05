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
//! and persisted-config decoding (rustfs/backlog#2148).
//!
//! The persisted blob is not encrypted; it shares the trust boundary of
//! `bucket-targets.json` and `tier-config.bin`.

use serde::{Deserialize, Serialize};
use std::fmt;
use url::Url;

/// Decode bytes only at the service boundary, preserving typed corruption errors.
pub(super) fn decode_stored_config(
    stored: Option<(Vec<u8>, time::OffsetDateTime)>,
) -> Result<Option<(OnDemandMigrationConfig, time::OffsetDateTime)>, super::storage_api::StorageError> {
    stored
        .map(|(bytes, updated_at)| {
            OnDemandMigrationConfig::from_json(&bytes)
                .map(|config| (config, updated_at))
                .map_err(super::storage_api::StorageError::other)
        })
        .transpose()
}

pub(crate) async fn get_config(
    bucket: &str,
) -> Result<Option<(OnDemandMigrationConfig, time::OffsetDateTime)>, super::storage_api::StorageError> {
    decode_stored_config(super::storage_api::get_on_demand_migration_config(bucket).await?)
}

/// The only wire version this build reads and writes.
pub const ON_DEMAND_MIGRATION_CONFIG_VERSION: u32 = 1;

const REDACTED: &str = "REDACTED";
const AUTO_REGION: &str = "auto";
const AUTO_REGION_FALLBACK: &str = "us-east-1";
/// Public Azure Blob host suffix; the account name is the first label.
pub const AZURE_BLOB_SUFFIX: &str = "blob.core.windows.net";
/// Public Google Cloud Storage endpoint for the native provider.
pub const GCS_DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com";

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
    /// `None` means anonymous access to a public source bucket. Only the
    /// SigV4 providers read it; `azure` and `gcs_native` carry their own
    /// credentials in `azure` / `gcs`.
    #[serde(default)]
    pub credentials: Option<SourceCredentials>,
    #[serde(default)]
    pub tls: TlsConfig,
    /// Required for [`Provider::Azure`] and rejected for every other
    /// provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure: Option<AzureSourceConfig>,
    /// Required for [`Provider::GcsNative`] and rejected for every other
    /// provider. [`Provider::Gcs`] keeps using `credentials` because it
    /// speaks the S3 interoperability API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gcs: Option<GcsSourceConfig>,
}

/// Source vendor family.
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
    /// Native Azure Blob service; parameters in `source.azure`.
    Azure,
    /// Native GCS JSON API with a service-account key; parameters in
    /// `source.gcs`.
    #[serde(rename = "gcs_native")]
    GcsNative,
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
            Provider::Azure => "azure",
            Provider::GcsNative => "gcs_native",
        }
    }

    /// Providers that do not speak S3 and therefore ignore `region`,
    /// `path_style` and `credentials`.
    pub fn is_native(&self) -> bool {
        matches!(self, Provider::Azure | Provider::GcsNative)
    }

    /// Providers whose SDKs accept `region = "auto"`; RustFS maps it to
    /// `us-east-1` for signing. The native providers never sign with a
    /// region, so they accept it as well.
    fn accepts_auto_region(&self) -> bool {
        matches!(self, Provider::R2 | Provider::Minio | Provider::Rustfs) || self.is_native()
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

/// Native Azure Blob source parameters. The container is `source.bucket`,
/// so a config never carries two names for the same container. Exactly one
/// of `account_key` and `sas_token` must be set: the account key signs with
/// Shared Key, the SAS token is appended to every request URL.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AzureSourceConfig {
    /// Storage account name; also derives the default `blob.core.windows.net`
    /// endpoint when `source.endpoint` is absent.
    pub account: String,
    /// Base64 shared key of the storage account.
    #[serde(default)]
    pub account_key: Option<String>,
    /// SAS query string without the leading `?`.
    #[serde(default)]
    pub sas_token: Option<String>,
}

impl AzureSourceConfig {
    /// A copy safe to return to admin clients or log: both secrets are
    /// replaced by `REDACTED`, and whether each is set stays visible.
    pub fn redacted(&self) -> Self {
        Self {
            account: self.account.clone(),
            account_key: self.account_key.as_ref().map(|_| REDACTED.to_string()),
            sas_token: self.sas_token.as_ref().map(|_| REDACTED.to_string()),
        }
    }
}

impl fmt::Debug for AzureSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzureSourceConfig")
            .field("account", &self.account)
            .field("account_key", &self.account_key.as_ref().map(|_| REDACTED))
            .field("sas_token", &self.sas_token.as_ref().map(|_| REDACTED))
            .finish()
    }
}

/// Native Google Cloud Storage source parameters. The bucket is
/// `source.bucket`; only the service-account key lives here.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcsSourceConfig {
    /// Service-account key JSON, verbatim as downloaded from Google Cloud.
    pub service_account_json: String,
}

impl GcsSourceConfig {
    /// A copy safe to return to admin clients or log: the whole key JSON is
    /// a secret (it embeds the private key), so it is replaced wholesale.
    pub fn redacted(&self) -> Self {
        Self {
            service_account_json: REDACTED.to_string(),
        }
    }
}

impl fmt::Debug for GcsSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcsSourceConfig")
            .field("service_account_json", &REDACTED)
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
    #[error("source.{0} is required for provider {1}")]
    MissingProviderBlock(&'static str, Provider),
    #[error("source.{0} is not valid for provider {1}")]
    UnexpectedProviderBlock(&'static str, Provider),
    /// Carries only the reason: the block holds account keys, SAS tokens and
    /// service-account JSON, so no value of it is ever echoed.
    #[error("source.{0} is invalid: {1}")]
    InvalidProviderBlock(&'static str, &'static str),
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
        copy.source.azure = self.source.azure.as_ref().map(AzureSourceConfig::redacted);
        copy.source.gcs = self.source.gcs.as_ref().map(GcsSourceConfig::redacted);
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
            (None, Provider::Azure) => self
                .azure
                .as_ref()
                .map(|azure| format!("https://{}.{AZURE_BLOB_SUFFIX}", azure.account))
                .unwrap_or_default(),
            (None, Provider::GcsNative) => GCS_DEFAULT_ENDPOINT.to_string(),
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
        self.validate_provider_block()?;

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
            // Both native providers derive a fixed endpoint; Azure's is built
            // from the account name, already checked by `validate_provider_block`.
            None if self.provider.is_native() => {}
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

    /// The provider-specific block must be present for exactly its own
    /// provider: a stray `azure` block on an `s3` source would otherwise be
    /// accepted, stored, and silently ignored by the client builder.
    fn validate_provider_block(&self) -> Result<(), OnDemandMigrationConfigError> {
        let missing = OnDemandMigrationConfigError::MissingProviderBlock;
        let unexpected = OnDemandMigrationConfigError::UnexpectedProviderBlock;
        let invalid = OnDemandMigrationConfigError::InvalidProviderBlock;

        if self.provider != Provider::Azure && self.azure.is_some() {
            return Err(unexpected("azure", self.provider));
        }
        if self.provider != Provider::GcsNative && self.gcs.is_some() {
            return Err(unexpected("gcs", self.provider));
        }

        match self.provider {
            Provider::Azure => {
                let azure = self.azure.as_ref().ok_or(missing("azure", self.provider))?;
                if azure.account.is_empty() {
                    return Err(invalid("azure", "account must not be empty"));
                }
                // The account feeds a hostname when the endpoint is derived:
                // keep it to label characters so it cannot rewrite the host.
                if !azure.account.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
                    return Err(invalid("azure", "account contains characters outside [A-Za-z0-9-]"));
                }
                match (azure.account_key.as_deref(), azure.sas_token.as_deref()) {
                    (Some(_), Some(_)) => return Err(invalid("azure", "account_key and sas_token are mutually exclusive")),
                    (None, None) => return Err(invalid("azure", "one of account_key and sas_token is required")),
                    (Some(key), None) => {
                        if key.is_empty() {
                            return Err(invalid("azure", "account_key must not be empty"));
                        }
                        // Decoded here so a mistyped key fails at the admin
                        // boundary instead of on the first source request.
                        if base64_simd::STANDARD.decode_to_vec(key.as_bytes()).is_err() {
                            return Err(invalid("azure", "account_key is not base64"));
                        }
                    }
                    (None, Some(sas)) => {
                        if sas.is_empty() {
                            return Err(invalid("azure", "sas_token must not be empty"));
                        }
                        if sas.starts_with('?') {
                            return Err(invalid("azure", "sas_token must not start with '?'"));
                        }
                        if sas.chars().any(char::is_whitespace) {
                            return Err(invalid("azure", "sas_token must not contain whitespace"));
                        }
                    }
                }
            }
            Provider::GcsNative => {
                let gcs = self.gcs.as_ref().ok_or(missing("gcs", self.provider))?;
                let key: serde_json::Value = serde_json::from_str(&gcs.service_account_json)
                    .map_err(|_| invalid("gcs", "service_account_json is not valid JSON"))?;
                let Some(object) = key.as_object() else {
                    return Err(invalid("gcs", "service_account_json is not a JSON object"));
                };
                if object.get("type").and_then(serde_json::Value::as_str) != Some("service_account") {
                    return Err(invalid("gcs", "service_account_json is not a service_account key"));
                }
                for field in ["client_email", "private_key"] {
                    if object
                        .get(field)
                        .and_then(serde_json::Value::as_str)
                        .is_none_or(str::is_empty)
                    {
                        return Err(invalid("gcs", "service_account_json is missing client_email or private_key"));
                    }
                }
            }
            Provider::S3 | Provider::Aws | Provider::Minio | Provider::Rustfs | Provider::R2 | Provider::Gcs => {}
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

#[cfg(test)]
mod tests {
    use super::*;

    mod before_native_sources {
        include!("../../fixtures/on_demand_migration/source_config_e2a.rs");
    }

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
    fn s3_config_writes_remain_readable_by_the_strict_pre_native_reader() {
        // FULL_JSON is the complete config fixture already present in e2a921bc.
        for provider in ["s3", "aws", "minio", "rustfs", "r2", "gcs"] {
            let mut old_wire: serde_json::Value = serde_json::from_str(FULL_JSON).expect("historical config fixture");
            old_wire["source"]["provider"] = provider.into();
            let config = OnDemandMigrationConfig::from_json(&serde_json::to_vec(&old_wire).expect("historical wire"))
                .expect("current reader accepts the historical source");
            let wire = config.to_json().expect("persist current config");
            let actual: serde_json::Value = serde_json::from_slice(&wire).expect("persisted config JSON");
            let old_source: before_native_sources::SourceConfig = serde_json::from_value(actual["source"].clone())
                .expect("an existing S3 source must remain readable by the strict e2a source consumer");
            assert_eq!(serde_json::to_value(old_source).expect("old reader wire"), old_wire["source"]);
            assert_eq!(actual, old_wire, "provider={provider}: no existing config field or value may change");

            for field in ["azure", "gcs"] {
                let mut rejected = old_wire["source"].clone();
                rejected[field] = serde_json::Value::Null;
                assert!(
                    serde_json::from_value::<before_native_sources::SourceConfig>(rejected).is_err(),
                    "the frozen old reader must reject {field}, even when null"
                );
            }
        }
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
                r#"{"source":{"provider":"swift","endpoint":"https://h","region":"r","bucket":"b"}}"#,
            ),
            (
                "azure block",
                r#"{"source":{"provider":"azure","region":"auto","bucket":"b","azure":{"account":"acct","account_key":"a2V5","extra":1}}}"#,
            ),
            (
                "gcs block",
                r#"{"source":{"provider":"gcs_native","region":"auto","bucket":"b","gcs":{"service_account_json":"{}","extra":1}}}"#,
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
        // The native providers never sign with a region, so "auto" is the
        // honest value to write for them.
        for cfg in [azure_cfg(), gcs_native_cfg()] {
            assert_eq!(cfg.source.region, "auto");
            cfg.validate(empty_ctx())
                .unwrap_or_else(|err| panic!("{}: {err}", cfg.source.provider));
        }
        assert_eq!(sample().source.effective_region(), "us-west-1");
    }

    const SERVICE_ACCOUNT_JSON: &str = r#"{"type":"service_account","project_id":"p","client_email":"a@b.iam.gserviceaccount.com","private_key":"-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----"}"#;

    fn azure_cfg() -> OnDemandMigrationConfig {
        let mut cfg = sample();
        cfg.source.provider = Provider::Azure;
        cfg.source.endpoint = None;
        cfg.source.region = "auto".to_string();
        cfg.source.credentials = None;
        cfg.source.azure = Some(AzureSourceConfig {
            account: "legacyaccount".to_string(),
            account_key: Some("c2VjcmV0LWtleQ==".to_string()),
            sas_token: None,
        });
        cfg
    }

    fn gcs_native_cfg() -> OnDemandMigrationConfig {
        let mut cfg = sample();
        cfg.source.provider = Provider::GcsNative;
        cfg.source.endpoint = None;
        cfg.source.region = "auto".to_string();
        cfg.source.credentials = None;
        cfg.source.gcs = Some(GcsSourceConfig {
            service_account_json: SERVICE_ACCOUNT_JSON.to_string(),
        });
        cfg
    }

    #[test]
    fn native_providers_derive_their_endpoint_and_round_trip_on_the_wire() {
        let azure = azure_cfg();
        assert_eq!(azure.source.effective_endpoint(), "https://legacyaccount.blob.core.windows.net");
        let gcs = gcs_native_cfg();
        assert_eq!(gcs.source.effective_endpoint(), "https://storage.googleapis.com");

        for cfg in [azure_cfg(), gcs_native_cfg()] {
            let json = cfg.to_json().expect("config must serialize");
            assert_eq!(OnDemandMigrationConfig::from_json(&json).expect("config must parse"), cfg);
            let wire: serde_json::Value = serde_json::from_slice(&json).expect("native config JSON");
            let (present, absent, expected) = match cfg.source.provider {
                Provider::Azure => ("azure", "gcs", serde_json::to_value(&cfg.source.azure).expect("Azure block")),
                Provider::GcsNative => ("gcs", "azure", serde_json::to_value(&cfg.source.gcs).expect("GCS block")),
                _ => unreachable!("native fixture"),
            };
            assert!(expected.is_object(), "native credentials must be present");
            assert_eq!(wire["source"][present], expected);
            assert!(wire["source"].get(absent).is_none());
            assert!(
                serde_json::from_value::<before_native_sources::SourceConfig>(wire["source"].clone()).is_err(),
                "native providers still require upgraded readers"
            );
        }
        // The wire labels are part of the admin contract.
        assert!(
            String::from_utf8(azure_cfg().to_json().expect("json"))
                .expect("utf8")
                .contains(r#""provider":"azure""#)
        );
        assert!(
            String::from_utf8(gcs_native_cfg().to_json().expect("json"))
                .expect("utf8")
                .contains(r#""provider":"gcs_native""#)
        );
    }

    #[test]
    fn an_explicit_endpoint_overrides_the_derived_native_one() {
        // Azurite and fake-gcs-server are addressed this way.
        let mut cfg = azure_cfg();
        cfg.source.endpoint = Some("http://azurite.example.com:10000".to_string());
        cfg.validate(empty_ctx()).expect("an explicit native endpoint is allowed");
        assert_eq!(cfg.source.effective_endpoint(), "http://azurite.example.com:10000");

        cfg.source.endpoint = Some("http://azurite.example.com:10000/devstoreaccount1".to_string());
        assert!(
            matches!(cfg.validate(empty_ctx()), Err(OnDemandMigrationConfigError::InvalidEndpoint(_))),
            "a native endpoint is still an origin"
        );
    }

    #[test]
    fn a_provider_block_belongs_to_exactly_its_own_provider() {
        let mut cfg = sample();
        cfg.source.azure = azure_cfg().source.azure;
        assert_eq!(
            cfg.validate(empty_ctx()),
            Err(OnDemandMigrationConfigError::UnexpectedProviderBlock("azure", Provider::S3))
        );

        let mut cfg = sample();
        cfg.source.gcs = gcs_native_cfg().source.gcs;
        assert_eq!(
            cfg.validate(empty_ctx()),
            Err(OnDemandMigrationConfigError::UnexpectedProviderBlock("gcs", Provider::S3))
        );

        let mut cfg = azure_cfg();
        cfg.source.azure = None;
        assert_eq!(
            cfg.validate(empty_ctx()),
            Err(OnDemandMigrationConfigError::MissingProviderBlock("azure", Provider::Azure))
        );

        let mut cfg = gcs_native_cfg();
        cfg.source.gcs = None;
        assert_eq!(
            cfg.validate(empty_ctx()),
            Err(OnDemandMigrationConfigError::MissingProviderBlock("gcs", Provider::GcsNative))
        );
    }

    #[test]
    fn azure_block_rules() {
        let with = |account: &str, key: Option<&str>, sas: Option<&str>| {
            let mut cfg = azure_cfg();
            cfg.source.azure = Some(AzureSourceConfig {
                account: account.to_string(),
                account_key: key.map(str::to_string),
                sas_token: sas.map(str::to_string),
            });
            cfg.validate(empty_ctx())
        };

        with("legacyaccount", None, Some("sv=2021-08-06&sig=abc%3D")).expect("a SAS token is a complete credential");
        with("legacyaccount", Some("c2VjcmV0LWtleQ=="), None).expect("an account key is a complete credential");

        for (label, result) in [
            ("empty account", with("", Some("c2VjcmV0LWtleQ=="), None)),
            // The account becomes the first label of the derived hostname.
            ("account with a dot", with("legacy.account", Some("c2VjcmV0LWtleQ=="), None)),
            ("account with a slash", with("legacy/account", Some("c2VjcmV0LWtleQ=="), None)),
            ("no credential", with("legacyaccount", None, None)),
            ("both credentials", with("legacyaccount", Some("c2VjcmV0LWtleQ=="), Some("sv=1"))),
            ("empty key", with("legacyaccount", Some(""), None)),
            ("key that is not base64", with("legacyaccount", Some("not base64!"), None)),
            ("empty sas", with("legacyaccount", None, Some(""))),
            ("sas with a leading question mark", with("legacyaccount", None, Some("?sv=1"))),
            ("sas with whitespace", with("legacyaccount", None, Some("sv=1 &sig=a"))),
        ] {
            assert!(
                matches!(result, Err(OnDemandMigrationConfigError::InvalidProviderBlock("azure", _))),
                "{label}: {result:?}"
            );
        }
    }

    #[test]
    fn gcs_native_block_requires_a_usable_service_account_key() {
        let with = |json: &str| {
            let mut cfg = gcs_native_cfg();
            cfg.source.gcs = Some(GcsSourceConfig {
                service_account_json: json.to_string(),
            });
            cfg.validate(empty_ctx())
        };

        with(SERVICE_ACCOUNT_JSON).expect("a service-account key is accepted");
        for (label, json) in [
            ("empty", ""),
            ("not json", "not json"),
            ("not an object", "[]"),
            ("wrong type", r#"{"type":"authorized_user","client_email":"a@b","private_key":"k"}"#),
            ("no private key", r#"{"type":"service_account","client_email":"a@b"}"#),
            ("empty client email", r#"{"type":"service_account","client_email":"","private_key":"k"}"#),
        ] {
            let result = with(json);
            assert!(
                matches!(result, Err(OnDemandMigrationConfigError::InvalidProviderBlock("gcs", _))),
                "{label}: {result:?}"
            );
        }
    }

    #[test]
    fn native_secrets_never_survive_redaction_or_debug() {
        let mut azure = azure_cfg();
        azure.source.azure.as_mut().expect("block").sas_token = Some("sv=2021-08-06&sig=top-secret".to_string());
        azure.source.azure.as_mut().expect("block").account_key = None;
        let gcs = gcs_native_cfg();

        for rendered in [
            format!("{:?}", azure.redacted()),
            format!("{azure:?}"),
            String::from_utf8(azure.redacted().to_json().expect("json")).expect("utf8"),
        ] {
            assert!(!rendered.contains("top-secret"), "{rendered}");
            assert!(rendered.contains("legacyaccount"), "the account name is not a secret: {rendered}");
        }
        for rendered in [
            format!("{:?}", gcs.redacted()),
            format!("{gcs:?}"),
            String::from_utf8(gcs.redacted().to_json().expect("json")).expect("utf8"),
        ] {
            assert!(!rendered.contains("PRIVATE KEY-----"), "{rendered}");
            assert!(!rendered.contains("gserviceaccount"), "{rendered}");
        }
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
    /// rustfs/backlog#2148: the accessor reports absence as `Ok(None)` and a
    /// stored payload it cannot parse as a typed error, never as a default
    /// and never as `ConfigNotFound`.
    #[tokio::test]
    async fn get_on_demand_migration_config_distinguishes_absent_from_corrupt() {
        use super::super::storage_api::StorageError as Error;
        use super::super::storage_api::test_support::{
            BUCKET_ON_DEMAND_MIGRATION_CONFIG, BucketMetadata, BucketMetadataSys, isolated_store_over_temp_disks,
        };
        use std::sync::Arc;
        const ODM_JSON: &[u8] = br#"{"source":{"provider":"minio","endpoint":"https://legacy.example.com:9000","region":"auto","bucket":"legacy-bucket","credentials":{"access_key":"AK","secret_key":"SK"}}}"#;

        let (_dirs, ecstore) = isolated_store_over_temp_disks().await;
        let sys = BucketMetadataSys::new(ecstore);
        let bucket = "odm-accessor";

        sys.set(bucket.to_string(), Arc::new(BucketMetadata::new(bucket))).await;
        assert_eq!(
            decode_stored_config(sys.get_on_demand_migration_config(bucket).await.unwrap()).unwrap(),
            None
        );

        let mut corrupt = BucketMetadata::new(bucket);
        corrupt.on_demand_migration_config_json = br#"{"source":{"provider":"s3"},"bogus":1}"#.to_vec();
        sys.set(bucket.to_string(), Arc::new(corrupt)).await;
        let err = decode_stored_config(sys.get_on_demand_migration_config(bucket).await.unwrap())
            .expect_err("corrupt config must not read as a default");
        assert_ne!(err, Error::ConfigNotFound, "corruption must not be reported as absence");
        let typed = match &err {
            Error::Io(io) => io
                .get_ref()
                .and_then(|source| source.downcast_ref::<OnDemandMigrationConfigError>()),
            _ => None,
        };
        assert!(
            matches!(typed, Some(OnDemandMigrationConfigError::Malformed(_))),
            "typed parse error must survive the Result boundary, got: {err:?}"
        );

        let mut valid = BucketMetadata::new(bucket);
        valid
            .update_config(BUCKET_ON_DEMAND_MIGRATION_CONFIG, ODM_JSON.to_vec())
            .unwrap();
        let stamped = valid.on_demand_migration_config_updated_at;
        sys.set(bucket.to_string(), Arc::new(valid)).await;
        let (config, updated_at) = decode_stored_config(sys.get_on_demand_migration_config(bucket).await.unwrap())
            .unwrap()
            .expect("stored config is returned");
        assert_eq!(config, OnDemandMigrationConfig::from_json(ODM_JSON).unwrap());
        assert_eq!(updated_at, stamped);
    }
}
