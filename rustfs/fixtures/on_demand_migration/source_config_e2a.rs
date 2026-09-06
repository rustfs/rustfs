// Strict source reader frozen from e2a921bc1608823c8efec955d7463ab8350a8a01.
// Wire declarations and credential Debug are copied verbatim; runtime methods are omitted.
use serde::{Deserialize, Serialize};
use std::fmt;

const REDACTED: &str = "REDACTED";

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
