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

//! Credential plumbing shared by the Vault KV2 and Transit backends.
//!
//! [`VaultCredentialProvider`] owns the authenticated [`VaultClient`] and hands
//! out per-request snapshots. Backends take a fresh snapshot via
//! [`VaultCredentialProvider::current`] for every Vault call instead of holding
//! a client for their own lifetime: a credential rotation applies to the next
//! call, while calls already in flight finish on the generation they captured
//! (their `Arc` keeps it alive).
//!
//! Lease-bound tokens (AppRole) are kept fresh by a background renewal task
//! (see [`VaultCredentialProvider::spawn_renewal_task`]): it renews at half the
//! lease TTL, falls back to a fresh login when renewal is not possible, and
//! keeps retrying after failures. If the token still reaches the configured
//! safety window before expiry, [`VaultCredentialProvider::current`] fails
//! closed rather than handing out a token that may lapse mid-request.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use sha2::Digest;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use vaultrs::client::{VaultClient, VaultClientSettingsBuilder};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::config::{KmsConfig, VaultAuthMethod, redacted_secret};
use crate::error::{KmsError, Result};
use crate::policy::{self, AttemptError, ErrorClass, OpClass, RetryPolicy};

/// Result of a single authentication attempt, classified for the Auth retry
/// policy.
type AttemptResult<T> = std::result::Result<T, AttemptError>;

/// Cadence for refresh retries after a failed cycle (on top of the bounded
/// retries inside one [`policy::execute`] call).
const DEFAULT_REFRESH_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// Default seconds between token file re-reads for [`TokenFileSource`].
const DEFAULT_TOKEN_FILE_POLL_INTERVAL_SECS: u64 = 30;

// ---------------------------------------------------------------------------
// Metrics
//
// Both gauges describe the one credential generation currently installed, so
// they carry no labels: the Vault address, mount, auth path and token are all
// off limits as label values, and there is exactly one generation to describe.
// The renewal loop republishes them on a bounded cadence while it waits, so a
// scrape landing between refresh cycles never reads a TTL frozen at the last
// refresh, or a fail-closed state that flipped after it.
// ---------------------------------------------------------------------------

/// Gauge: seconds left before the active Vault token expires; `0` once it has.
const METRIC_TOKEN_TTL_SECONDS: &str = "rustfs_kms_vault_token_ttl_seconds";
/// Gauge: `1` while [`VaultCredentialProvider::current`] refuses to hand out
/// the token because it is inside the fail-closed safety window, `0` otherwise.
const METRIC_CREDENTIALS_FAIL_CLOSED: &str = "rustfs_kms_vault_credentials_fail_closed";

/// How often the renewal loop republishes the credential gauges while waiting.
/// Bounds how stale a scrape can be, without any additional Vault traffic.
const CREDENTIAL_GAUGE_INTERVAL: Duration = Duration::from_secs(10);

/// Register metric descriptions once per process.
fn describe_credential_metrics() {
    static DESCRIBE: std::sync::Once = std::sync::Once::new();
    DESCRIBE.call_once(|| {
        metrics::describe_gauge!(
            METRIC_TOKEN_TTL_SECONDS,
            "Seconds remaining before the Vault token backing the KMS backend expires"
        );
        metrics::describe_gauge!(
            METRIC_CREDENTIALS_FAIL_CLOSED,
            "1 while the Vault credential provider refuses to serve its token because the token is inside the fail-closed safety window"
        );
    });
}

/// A crate-owned secret value, zeroized on drop and redacted in Debug output.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub(crate) struct SecretString(String);

impl SecretString {
    pub(crate) fn new(value: String) -> Self {
        Self(value)
    }

    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(redacted_secret(&self.0))
    }
}

/// Expiry attributes of a lease-bound token.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LeaseInfo {
    /// Time-to-live granted at issue or renewal.
    pub(crate) ttl: Duration,
    /// Whether `renew-self` can extend this token.
    pub(crate) renewable: bool,
}

/// A Vault token handed out by a [`TokenSource`].
///
/// The crate's copy of the token is zeroized on drop. This cannot cover the
/// copy `vaultrs` keeps inside its client settings (or the HTTP headers built
/// from it); it bounds how long the token lingers in memory owned by this
/// module.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub(crate) struct TokenLease {
    token: String,
    /// `None` for tokens without an expiry (static configuration tokens and
    /// non-expiring root-like tokens).
    #[zeroize(skip)]
    lease: Option<LeaseInfo>,
}

impl TokenLease {
    pub(crate) fn new(token: String, lease: Option<LeaseInfo>) -> Self {
        Self { token, lease }
    }

    /// Map a Vault auth response onto a lease. A `lease_duration` of zero
    /// means the token never expires, so no lease is tracked and no renewal is
    /// scheduled.
    fn from_auth(auth: vaultrs::api::AuthInfo) -> Self {
        let lease = (auth.lease_duration > 0).then_some(LeaseInfo {
            ttl: Duration::from_secs(auth.lease_duration),
            renewable: auth.renewable,
        });
        Self {
            token: auth.client_token,
            lease,
        }
    }

    /// Expose the raw token for handing to the Vault client builder.
    pub(crate) fn expose(&self) -> &str {
        &self.token
    }

    pub(crate) fn lease_info(&self) -> Option<LeaseInfo> {
        self.lease
    }
}

impl fmt::Debug for TokenLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenLease")
            .field("token", &redacted_secret(&self.token))
            .field("lease", &self.lease)
            .finish()
    }
}

/// Wrap a `vaultrs` failure as a classified attempt failure.
fn attempt_error(operation: &str, error: vaultrs::error::ClientError) -> AttemptError {
    AttemptError {
        class: policy::classify_vaultrs(&error),
        error: KmsError::backend_error(format!("Vault {operation} failed: {error}")),
    }
}

/// Source of Vault authentication tokens.
///
/// Implementations perform one attempt per call; bounded retries and backoff
/// are owned by the caller through [`policy::execute`] with [`OpClass::Auth`].
/// Current sources are [`StaticToken`], [`AppRoleLogin`], and the agent-managed
/// [`TokenFileSource`].
#[async_trait]
pub(crate) trait TokenSource: fmt::Debug + Send + Sync {
    /// One login attempt yielding a token for a new client generation.
    async fn acquire(&self) -> AttemptResult<TokenLease>;

    /// One `renew-self` attempt for the token held by `client`.
    ///
    /// Sources whose tokens cannot be renewed fail fatally; callers fall back
    /// to [`TokenSource::acquire`].
    async fn renew(&self, _client: &VaultClient) -> AttemptResult<TokenLease> {
        Err(AttemptError {
            class: ErrorClass::Fatal,
            error: KmsError::invalid_operation("this token source does not support renewal"),
        })
    }
}

/// Token source for [`VaultAuthMethod::Token`]: always yields the token fixed
/// at configuration time. The token carries no lease, so it is never renewed
/// and never expires from the provider's point of view.
pub(crate) struct StaticToken {
    token: TokenLease,
}

impl StaticToken {
    pub(crate) fn new(token: String) -> Self {
        Self {
            token: TokenLease::new(token, None),
        }
    }
}

#[async_trait]
impl TokenSource for StaticToken {
    async fn acquire(&self) -> AttemptResult<TokenLease> {
        Ok(self.token.clone())
    }
}

impl fmt::Debug for StaticToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TokenLease::fmt already redacts the token value.
        f.debug_struct("StaticToken").field("token", &self.token).finish()
    }
}

/// Token source for [`VaultAuthMethod::AppRole`]: exchanges `role_id` +
/// `secret_id` for a lease-bound token via the AppRole auth engine.
pub(crate) struct AppRoleLogin {
    /// Unauthenticated client used only for the login exchange.
    login_client: VaultClient,
    mount: String,
    role_id: String,
    /// Inline secret_id fallback, used when no file is configured.
    secret_id: SecretString,
    /// Secret-id file, re-read on every login so external rotation of the
    /// secret_id is picked up without a restart. Takes precedence over the
    /// inline value.
    secret_id_file: Option<PathBuf>,
}

impl AppRoleLogin {
    pub(crate) fn new(
        settings: &VaultConnectionSettings,
        mount: String,
        role_id: String,
        secret_id: String,
        secret_id_file: Option<PathBuf>,
    ) -> Result<Self> {
        Ok(Self {
            login_client: settings.build_login_client()?,
            mount,
            role_id,
            secret_id: SecretString::new(secret_id),
            secret_id_file,
        })
    }

    /// Resolve the secret_id for one login attempt.
    ///
    /// File problems are fatal for the attempt (replaying the same read within
    /// one retry cycle cannot help), but the renewal loop keeps retrying on
    /// its cadence, so repairing the file heals the source without a restart.
    async fn resolve_secret_id(&self) -> AttemptResult<SecretString> {
        let Some(path) = &self.secret_id_file else {
            return Ok(self.secret_id.clone());
        };

        let mut raw = tokio::fs::read_to_string(path).await.map_err(|error| AttemptError {
            class: ErrorClass::Fatal,
            error: KmsError::configuration_error(format!("Failed to read AppRole secret_id file {}: {error}", path.display())),
        })?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            raw.zeroize();
            return Err(AttemptError {
                class: ErrorClass::Fatal,
                error: KmsError::configuration_error(format!("AppRole secret_id file {} is empty", path.display())),
            });
        }
        let secret_id = SecretString::new(trimmed.to_string());
        raw.zeroize();
        Ok(secret_id)
    }
}

#[async_trait]
impl TokenSource for AppRoleLogin {
    async fn acquire(&self) -> AttemptResult<TokenLease> {
        let secret_id = self.resolve_secret_id().await?;
        let auth = vaultrs::auth::approle::login(&self.login_client, &self.mount, &self.role_id, secret_id.expose())
            .await
            .map_err(|error| attempt_error("AppRole login", error))?;
        Ok(TokenLease::from_auth(auth))
    }

    async fn renew(&self, client: &VaultClient) -> AttemptResult<TokenLease> {
        let auth = vaultrs::token::renew_self(client, None)
            .await
            .map_err(|error| attempt_error("token renewal", error))?;
        Ok(TokenLease::from_auth(auth))
    }
}

impl fmt::Debug for AppRoleLogin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The login client embeds Vault client settings and must stay out of
        // Debug output; role_id is not a secret in Vault's AppRole model.
        f.debug_struct("AppRoleLogin")
            .field("mount", &self.mount)
            .field("role_id", &self.role_id)
            .field("secret_id", &self.secret_id)
            .field("secret_id_file", &self.secret_id_file)
            .finish_non_exhaustive()
    }
}

/// Token source for [`VaultAuthMethod::Kubernetes`]: exchanges the pod's
/// projected ServiceAccount token for a lease-bound Vault token.
///
/// The JWT is re-read on every login because the kubelet rotates a projected
/// token well inside the pod's lifetime; caching it would strand the source on
/// an expired assertion once the current Vault token can no longer be renewed.
///
/// Unlike [`TokenFileSource`], the file mode is not checked: the kubelet owns
/// the projected token and mounts it world-readable by default, so rejecting
/// group/other bits would refuse every standard pod rather than catch a
/// deployment error.
pub(crate) struct KubernetesLogin {
    /// Unauthenticated client used only for the login exchange.
    login_client: VaultClient,
    mount: String,
    role: String,
    jwt_path: PathBuf,
}

impl KubernetesLogin {
    pub(crate) fn new(settings: &VaultConnectionSettings, mount: String, role: String, jwt_path: PathBuf) -> Result<Self> {
        Ok(Self {
            login_client: settings.build_login_client()?,
            mount,
            role,
            jwt_path,
        })
    }

    /// Read the ServiceAccount token for one login attempt.
    ///
    /// Mirrors [`AppRoleLogin::resolve_secret_id`]: a read failure is fatal for
    /// the attempt but the refresh loop keeps retrying, so a token the kubelet
    /// has not projected yet heals the source without a restart.
    async fn resolve_jwt(&self) -> AttemptResult<SecretString> {
        let mut raw = tokio::fs::read_to_string(&self.jwt_path)
            .await
            .map_err(|error| AttemptError {
                class: ErrorClass::Fatal,
                error: KmsError::configuration_error(format!(
                    "Failed to read Kubernetes ServiceAccount token {}: {error}",
                    self.jwt_path.display()
                )),
            })?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            raw.zeroize();
            return Err(AttemptError {
                class: ErrorClass::Fatal,
                error: KmsError::configuration_error(format!(
                    "Kubernetes ServiceAccount token {} is empty",
                    self.jwt_path.display()
                )),
            });
        }
        let jwt = SecretString::new(trimmed.to_string());
        raw.zeroize();
        Ok(jwt)
    }
}

#[async_trait]
impl TokenSource for KubernetesLogin {
    async fn acquire(&self) -> AttemptResult<TokenLease> {
        let jwt = self.resolve_jwt().await?;
        let auth = vaultrs::auth::kubernetes::login(&self.login_client, &self.mount, &self.role, jwt.expose())
            .await
            .map_err(|error| attempt_error("Kubernetes login", error))?;
        Ok(TokenLease::from_auth(auth))
    }

    async fn renew(&self, client: &VaultClient) -> AttemptResult<TokenLease> {
        let auth = vaultrs::token::renew_self(client, None)
            .await
            .map_err(|error| attempt_error("token renewal", error))?;
        Ok(TokenLease::from_auth(auth))
    }
}

impl fmt::Debug for KubernetesLogin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The login client embeds Vault client settings and must stay out of
        // Debug output; the role name is not a secret, and the JWT is never held.
        f.debug_struct("KubernetesLogin")
            .field("mount", &self.mount)
            .field("role", &self.role)
            .field("jwt_path", &self.jwt_path)
            .finish_non_exhaustive()
    }
}

/// Token source for [`VaultAuthMethod::TokenFile`]: reads an agent-managed
/// token file (for example a Vault Agent auto-auth sink).
///
/// The agent owns the token's lifecycle; this source only tracks the file.
/// Every read grants the token an observed validity of `validity`, so the
/// renewal loop re-reads the file at half that interval and installs a new
/// client generation from whatever the file holds. A file that disappears or
/// turns empty keeps failing the refresh until the fail-closed window trips,
/// and heals the provider as soon as it is restored.
pub(crate) struct TokenFileSource {
    path: PathBuf,
    /// Observed validity granted per successful read; the renewal loop
    /// re-reads at half this value.
    validity: Duration,
    /// Digest of the last token read, to tell agent-driven rotations apart
    /// from plain refreshes in the logs. Never holds the token itself.
    last_digest: std::sync::Mutex<Option<[u8; 32]>>,
}

impl TokenFileSource {
    pub(crate) fn new(path: PathBuf, validity: Duration) -> Self {
        Self {
            path,
            validity,
            last_digest: std::sync::Mutex::new(None),
        }
    }

    fn fatal(error: KmsError) -> AttemptError {
        AttemptError {
            class: ErrorClass::Fatal,
            error,
        }
    }

    /// Reject a token file readable by group or other, mirroring the SFTP
    /// host-key rule: a Vault token grants use of the KMS keys, so a mode
    /// wider than owner-only is a deployment error, not a warning.
    #[cfg(unix)]
    fn check_permissions(&self, metadata: &std::fs::Metadata) -> AttemptResult<()> {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(Self::fatal(KmsError::configuration_error(format!(
                "Vault token file {} has insecure permissions {mode:#o} (group and other permission bits must be unset)",
                self.path.display()
            ))));
        }
        Ok(())
    }
}

#[async_trait]
impl TokenSource for TokenFileSource {
    async fn acquire(&self) -> AttemptResult<TokenLease> {
        // Synchronous reads on purpose: the token file is tiny, this runs at
        // the renewal cadence, and staying off the blocking pool keeps the
        // paused-clock tests of the renewal timing deterministic.
        let metadata = std::fs::metadata(&self.path).map_err(|error| {
            Self::fatal(KmsError::configuration_error(format!(
                "Failed to read Vault token file {}: {error}",
                self.path.display()
            )))
        })?;
        #[cfg(unix)]
        self.check_permissions(&metadata)?;

        let mut raw = std::fs::read_to_string(&self.path).map_err(|error| {
            Self::fatal(KmsError::configuration_error(format!(
                "Failed to read Vault token file {}: {error}",
                self.path.display()
            )))
        })?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            raw.zeroize();
            return Err(Self::fatal(KmsError::configuration_error(format!(
                "Vault token file {} is empty",
                self.path.display()
            ))));
        }

        let digest: [u8; 32] = sha2::Sha256::digest(trimmed.as_bytes()).into();
        let previous = self
            .last_digest
            .lock()
            .expect("token file digest mutex poisoned")
            .replace(digest);
        if previous.is_some_and(|previous| previous != digest) {
            info!(
                path = %self.path.display(),
                modified = ?metadata.modified().ok(),
                "Vault token file rotated; installing a new client generation"
            );
        }

        let token = trimmed.to_string();
        raw.zeroize();
        Ok(TokenLease::new(
            token,
            Some(LeaseInfo {
                ttl: self.validity,
                renewable: false,
            }),
        ))
    }
}

impl fmt::Debug for TokenFileSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The token itself is never stored on the source; only its digest is.
        f.debug_struct("TokenFileSource")
            .field("path", &self.path)
            .field("validity", &self.validity)
            .finish_non_exhaustive()
    }
}

/// Map the configured auth method onto a token source.
pub(crate) fn token_source_for(
    auth_method: &VaultAuthMethod,
    settings: &VaultConnectionSettings,
) -> Result<Box<dyn TokenSource>> {
    match auth_method {
        VaultAuthMethod::Token { token } => Ok(Box::new(StaticToken::new(token.clone()))),
        VaultAuthMethod::AppRole {
            role_id,
            secret_id,
            secret_id_file,
            mount,
            ..
        } => Ok(Box::new(AppRoleLogin::new(
            settings,
            mount.clone(),
            role_id.clone(),
            secret_id.clone(),
            secret_id_file.clone(),
        )?)),
        VaultAuthMethod::Kubernetes {
            role, mount, jwt_path, ..
        } => Ok(Box::new(KubernetesLogin::new(settings, mount.clone(), role.clone(), jwt_path.clone())?)),
        VaultAuthMethod::TokenFile {
            path,
            poll_interval_secs,
            ..
        } => {
            let poll_interval = Duration::from_secs(poll_interval_secs.unwrap_or(DEFAULT_TOKEN_FILE_POLL_INTERVAL_SECS).max(1));
            // Validity is twice the poll interval because the renewal loop
            // fires at half the lease TTL: the file is then re-read once per
            // poll interval, and a file that stops being readable expires the
            // token after roughly two missed polls minus the safety window.
            Ok(Box::new(TokenFileSource::new(path.clone(), poll_interval * 2)))
        }
    }
}

/// Connection parameters shared by every client generation.
#[derive(Debug, Clone)]
pub(crate) struct VaultConnectionSettings {
    pub(crate) address: String,
    pub(crate) namespace: Option<String>,
    /// Per-attempt HTTP timeout applied to the underlying reqwest client.
    pub(crate) attempt_timeout: Duration,
    /// Whether to accept an unverified Vault server certificate. Gated on
    /// `allow_insecure_dev_defaults` by `KmsConfig::validate`.
    pub(crate) skip_tls_verify: bool,
}

impl VaultConnectionSettings {
    /// Build an authenticated client for one generation.
    fn build_client(&self, token: &str) -> Result<VaultClient> {
        let mut settings_builder = VaultClientSettingsBuilder::default();
        settings_builder.address(&self.address);
        // Defense in depth against stalled connections: vaultrs leaves the
        // underlying reqwest client without any timeout by default, so a hung
        // request would otherwise wait forever regardless of the
        // operation-level retry policy.
        settings_builder.timeout(Some(self.attempt_timeout));
        settings_builder.token(token);
        // Always set explicitly: left unset, vaultrs derives this from its own
        // VAULT_SKIP_VERIFY variable, so a stray value in the environment would
        // disable certificate verification behind the KMS configuration and its
        // insecure-defaults gate.
        settings_builder.verify(!self.skip_tls_verify);

        if let Some(namespace) = &self.namespace {
            settings_builder.namespace(Some(namespace.clone()));
        }

        let settings = settings_builder
            .build()
            .map_err(|e| KmsError::backend_error(format!("Failed to build Vault client settings: {e}")))?;

        VaultClient::new(settings).map_err(|e| KmsError::backend_error(format!("Failed to create Vault client: {e}")))
    }

    /// Build the tokenless client used for login exchanges.
    fn build_login_client(&self) -> Result<VaultClient> {
        self.build_client("")
    }
}

/// Refresh and fail-closed tuning for a [`VaultCredentialProvider`].
#[derive(Debug, Clone)]
pub(crate) struct VaultCredentialPolicy {
    /// Retry budget for one login cycle.
    pub(crate) retry: RetryPolicy,
    /// Retry budget for one token-renewal cycle.
    pub(crate) renew_retry: RetryPolicy,
    /// Fail-closed margin: once the current token is within this window of
    /// expiry without a successful refresh, [`VaultCredentialProvider::current`]
    /// refuses to hand it out.
    pub(crate) safety_window: Duration,
    /// Pause between refresh cycles after a failed one.
    pub(crate) retry_interval: Duration,
}

impl VaultCredentialPolicy {
    /// Derive the policy from the KMS configuration.
    ///
    /// The default safety window equals the per-attempt timeout: a request
    /// issued now can stay in flight for up to one attempt timeout, so the
    /// token must outlive at least that.
    pub(crate) fn from_kms_config(
        config: &KmsConfig,
        auth_method: &VaultAuthMethod,
        backend: &'static str,
        endpoint: &str,
        namespace: Option<&str>,
    ) -> Self {
        let retry = RetryPolicy::for_credentials(config, backend, endpoint, namespace, "credentials-login");
        let safety_window = match auth_method {
            VaultAuthMethod::AppRole {
                refresh_safety_window_secs: Some(secs),
                ..
            }
            | VaultAuthMethod::Kubernetes {
                refresh_safety_window_secs: Some(secs),
                ..
            }
            | VaultAuthMethod::TokenFile {
                refresh_safety_window_secs: Some(secs),
                ..
            } => Duration::from_secs(*secs),
            _ => retry.attempt_timeout,
        };
        Self {
            retry,
            renew_retry: RetryPolicy::for_credentials(config, backend, endpoint, namespace, "credentials-renew"),
            safety_window,
            retry_interval: DEFAULT_REFRESH_RETRY_INTERVAL,
        }
    }
}

/// One authenticated client generation.
///
/// Request paths hold the handle (via `Arc`) for the duration of a single
/// Vault call, so a rotation that swaps in a newer generation never tears the
/// client out from under an in-flight request.
pub(crate) struct VaultClientHandle {
    /// Monotonic counter identifying the credential generation this client was
    /// built from; bumped on every successful refresh.
    pub(crate) generation: u64,
    pub(crate) client: VaultClient,
    /// When this generation's token was issued (or last renewed).
    issued_at: Instant,
    /// Lease of this generation's token; `None` when it never expires.
    lease: Option<LeaseInfo>,
}

impl VaultClientHandle {
    /// Absolute expiry of this generation's token.
    ///
    /// `lease.ttl` is built from the `lease_duration` the Vault server sent, so
    /// a value too large to add to `issued_at` would panic on the bare `+`. A
    /// TTL that cannot be represented is indistinguishable from no expiry, so it
    /// collapses to `None` — the same answer already given for the zero-lease
    /// tokens Vault issues, which keeps the token in use and still fully
    /// validated by Vault on every call.
    fn expires_at(&self) -> Option<Instant> {
        self.lease.and_then(|lease| self.issued_at.checked_add(lease.ttl))
    }

    /// When the renewal task should refresh this generation: half the TTL,
    /// leaving the second half as budget for retries before the fail-closed
    /// window is reached.
    ///
    /// Unrepresentable TTLs collapse to `None` as in [`Self::expires_at`],
    /// leaving a token that never expires with nothing to renew.
    fn renew_at(&self) -> Option<Instant> {
        self.lease.and_then(|lease| self.issued_at.checked_add(lease.ttl / 2))
    }
}

impl fmt::Debug for VaultClientHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `VaultClient` embeds its settings, including the token, so it must
        // never appear in Debug output.
        f.debug_struct("VaultClientHandle")
            .field("generation", &self.generation)
            .field("lease", &self.lease)
            .finish_non_exhaustive()
    }
}

/// Owns the authenticated Vault client for a backend and hands out
/// per-request snapshots.
pub(crate) struct VaultCredentialProvider {
    settings: VaultConnectionSettings,
    source: Box<dyn TokenSource>,
    policy: VaultCredentialPolicy,
    current: ArcSwap<VaultClientHandle>,
    /// Serializes refreshes so concurrent triggers coalesce into one login.
    refresh_lock: tokio::sync::Mutex<()>,
}

impl VaultCredentialProvider {
    /// Authenticate with `source` and build the initial client generation.
    pub(crate) async fn new(
        settings: VaultConnectionSettings,
        source: Box<dyn TokenSource>,
        policy: VaultCredentialPolicy,
    ) -> Result<Self> {
        let startup_cancel = CancellationToken::new();
        let lease = policy::execute("vault_login", OpClass::Auth, &policy.retry, &startup_cancel, || source.acquire()).await?;
        let client = settings.build_client(lease.expose())?;
        Ok(Self {
            current: ArcSwap::from_pointee(VaultClientHandle {
                generation: 0,
                client,
                issued_at: Instant::now(),
                lease: lease.lease_info(),
            }),
            settings,
            source,
            policy,
            refresh_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Snapshot the current generation without the expiry gate. Internal use
    /// (renewal scheduling) and tests only; request paths go through
    /// [`VaultCredentialProvider::current`].
    pub(crate) fn snapshot(&self) -> Arc<VaultClientHandle> {
        self.current.load_full()
    }

    /// Snapshot the current client generation for a single request.
    ///
    /// Take one snapshot per Vault call: the returned `Arc` pins the
    /// generation for exactly that call, so a concurrent rotation applies to
    /// the next call without interrupting this one.
    ///
    /// Fails closed when the token is inside the safety window of its expiry:
    /// a request signed with such a token could lapse mid-flight, so refusing
    /// it locally is strictly safer than an unpredictable remote failure.
    pub(crate) fn current(&self) -> Result<Arc<VaultClientHandle>> {
        let handle = self.current.load_full();
        if let Some(expires_at) = handle.expires_at() {
            let now = Instant::now();
            if self.inside_safety_window(now, expires_at) {
                return Err(KmsError::credentials_unavailable(format!(
                    "Vault token (generation {}) is within {:?} of expiry and has not been refreshed; refusing to use it",
                    handle.generation, self.policy.safety_window
                )));
            }
        }
        Ok(handle)
    }

    /// Whether the token expiring at `expires_at` is close enough to refuse.
    ///
    /// `safety_window` reaches here from persisted configuration, so it is not
    /// guaranteed to have passed this version's validation: a window too large
    /// to add to the current instant would panic on the bare `+`. Such a window
    /// means every token is always inside it, so saturating to "refuse" is both
    /// the fail-closed answer and the one the arithmetic was reaching for.
    fn inside_safety_window(&self, now: Instant, expires_at: Instant) -> bool {
        now.checked_add(self.policy.safety_window)
            .is_none_or(|deadline| deadline >= expires_at)
    }

    /// Publish the credential gauges for the generation currently installed.
    ///
    /// The fail-closed gauge re-evaluates the very gate
    /// [`VaultCredentialProvider::current`] applies, so what operators see and
    /// what the request path does cannot drift apart.
    fn record_credential_gauges(&self) {
        let handle = self.current.load();
        let now = Instant::now();
        let fail_closed = match handle.expires_at() {
            Some(expires_at) => {
                metrics::gauge!(METRIC_TOKEN_TTL_SECONDS).set(expires_at.saturating_duration_since(now).as_secs_f64());
                self.inside_safety_window(now, expires_at)
            }
            // A generation without an expiry has no remaining TTL to report
            // and can never lapse, so it can never fail closed either.
            None => false,
        };
        metrics::gauge!(METRIC_CREDENTIALS_FAIL_CLOSED).set(if fail_closed { 1.0 } else { 0.0 });
    }

    /// Wait until `deadline`, republishing the credential gauges on the
    /// observation cadence. Reports `false` when cancellation cut the wait
    /// short.
    async fn wait_publishing_gauges(&self, deadline: Instant, cancel: &CancellationToken) -> bool {
        loop {
            self.record_credential_gauges();
            let now = Instant::now();
            if now >= deadline {
                return true;
            }
            let slice = (deadline - now).min(CREDENTIAL_GAUGE_INTERVAL);
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return false,
                _ = tokio::time::sleep(slice) => {}
            }
        }
    }

    /// Refresh the credentials if generation `observed` is still current.
    ///
    /// Single-flight: concurrent callers serialize on the refresh lock, and a
    /// caller that finds a newer generation already installed returns without
    /// touching Vault. Renewable tokens are renewed in place; anything else
    /// (or a failed renewal) falls back to a fresh login.
    pub(crate) async fn refresh(&self, observed: u64, cancel: &CancellationToken) -> Result<()> {
        let _guard = self.refresh_lock.lock().await;
        let current = self.snapshot();
        if current.generation != observed {
            return Ok(());
        }

        let renewable = current.lease.map(|lease| lease.renewable).unwrap_or(false);
        let renewed = if renewable {
            match policy::execute("vault_token_renew", OpClass::Auth, &self.policy.renew_retry, cancel, || {
                self.source.renew(&current.client)
            })
            .await
            {
                Ok(lease) => Some(lease),
                Err(error @ KmsError::OperationCancelled { .. }) => return Err(error),
                Err(error) => {
                    warn!(
                        generation = current.generation,
                        error = %error,
                        "Vault token renewal failed; falling back to a fresh login"
                    );
                    None
                }
            }
        } else {
            None
        };

        let lease = match renewed {
            Some(lease) => lease,
            None => policy::execute("vault_login", OpClass::Auth, &self.policy.retry, cancel, || self.source.acquire()).await?,
        };

        let client = self.settings.build_client(lease.expose())?;
        self.current.store(Arc::new(VaultClientHandle {
            generation: current.generation + 1,
            client,
            issued_at: Instant::now(),
            lease: lease.lease_info(),
        }));
        Ok(())
    }

    /// Spawn the background renewal task for lease-bound credentials.
    ///
    /// Returns `None` when the current token never expires (static tokens):
    /// there is nothing to renew. The returned handle cancels the task when
    /// dropped, tying the task's lifetime to whoever owns the handle (the
    /// service version that owns this backend).
    pub(crate) fn spawn_renewal_task(self: &Arc<Self>) -> Option<CredentialTaskHandle> {
        self.snapshot().lease?;
        let cancel = CancellationToken::new();
        let join = tokio::spawn(renewal_loop(Arc::clone(self), cancel.clone()));
        Some(CredentialTaskHandle {
            cancel,
            join: std::sync::Mutex::new(Some(join)),
        })
    }
}

impl fmt::Debug for VaultCredentialProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VaultCredentialProvider")
            .field("source", &self.source)
            .field("current", &self.current.load())
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

/// Drive credential refreshes until cancelled.
///
/// Each cycle sleeps until the current generation's renewal point (half TTL),
/// then refreshes. A failed cycle logs, waits `retry_interval`, and tries
/// again immediately (the renewal point is already in the past), so the
/// provider keeps trying to recover even after the fail-closed window has
/// been reached.
///
/// Both waits run through [`VaultCredentialProvider::wait_publishing_gauges`],
/// which is the only place the credential gauges are published: the loop is
/// already the component that tracks token expiry, and doing it here keeps the
/// request path free of any metric work.
async fn renewal_loop(provider: Arc<VaultCredentialProvider>, cancel: CancellationToken) {
    describe_credential_metrics();
    loop {
        let handle = provider.snapshot();
        let Some(renew_at) = handle.renew_at() else {
            // The current generation never expires; nothing left to schedule.
            provider.record_credential_gauges();
            return;
        };
        if !provider.wait_publishing_gauges(renew_at, &cancel).await {
            return;
        }

        match provider.refresh(handle.generation, &cancel).await {
            Ok(()) => {}
            Err(KmsError::OperationCancelled { .. }) => return,
            Err(error) => {
                warn!(
                    generation = handle.generation,
                    error = %error,
                    "Vault credential refresh failed; retrying until the credentials recover"
                );
                let retry_at = Instant::now() + provider.policy.retry_interval;
                if !provider.wait_publishing_gauges(retry_at, &cancel).await {
                    return;
                }
            }
        }
    }
}

/// Owner handle for a spawned renewal task.
///
/// Dropping the handle cancels the task, so hanging it off the service version
/// recycles the task on stop and reconfigure without explicit lifecycle calls.
pub(crate) struct CredentialTaskHandle {
    cancel: CancellationToken,
    join: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl CredentialTaskHandle {
    /// Cancel the renewal task and wait for it to exit.
    pub(crate) async fn shutdown(&self) {
        self.cancel.cancel();
        let join = self.join.lock().expect("credential task join mutex poisoned").take();
        if let Some(join) = join {
            let _ = join.await;
        }
    }
}

impl Drop for CredentialTaskHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DEFAULT_VAULT_KUBERNETES_MOUNT, REDACTED_SECRET};
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    const TEST_TOKEN: &str = "vault-token-debug-leak-canary";
    const TEST_SECRET_ID: &str = "approle-secret-id-leak-canary";

    fn test_settings() -> VaultConnectionSettings {
        VaultConnectionSettings {
            address: "http://127.0.0.1:8200".to_string(),
            namespace: Some("team-namespace".to_string()),
            attempt_timeout: Duration::from_secs(30),
            skip_tls_verify: false,
        }
    }

    /// Tight retry budget so paused-clock tests stay deterministic: one
    /// attempt per cycle, failed cycles spaced by `retry_interval`.
    fn test_policy(safety_window: Duration, retry_interval: Duration) -> VaultCredentialPolicy {
        let retry = RetryPolicy::for_test(
            Duration::from_secs(1),
            Duration::from_secs(1),
            1,
            Duration::from_millis(10),
            Duration::from_millis(10),
        );
        let renew_retry = RetryPolicy::for_test(
            Duration::from_secs(1),
            Duration::from_secs(1),
            1,
            Duration::from_millis(10),
            Duration::from_millis(10),
        );
        VaultCredentialPolicy {
            retry,
            renew_retry,
            safety_window,
            retry_interval,
        }
    }

    #[test]
    fn configured_login_and_renewal_use_reserved_credential_capacity() {
        let config = KmsConfig::default();
        let auth_method = VaultAuthMethod::Token {
            token: TEST_TOKEN.to_string(),
        };
        let credentials = VaultCredentialPolicy::from_kms_config(
            &config,
            &auth_method,
            "vault-kv2",
            "https://credential-policy.example.invalid",
            Some("team-namespace"),
        );
        let operations = RetryPolicy::for_backend(
            &config,
            "vault-kv2",
            "https://credential-policy.example.invalid",
            Some("team-namespace"),
            "operations",
        );

        assert!(credentials.retry.uses_credential_reserve());
        assert!(credentials.renew_retry.uses_credential_reserve());
        assert!(!operations.uses_credential_reserve());
        assert!(credentials.retry.shares_active_capacity_with(&credentials.renew_retry));
        assert!(credentials.retry.shares_active_capacity_with(&operations));
    }

    /// Shared observable state of a [`ScriptedSource`].
    #[derive(Debug, Default)]
    struct ScriptedState {
        login_calls: AtomicU32,
        renew_calls: AtomicU32,
        fail_login: AtomicBool,
        fail_renew: AtomicBool,
    }

    /// Token source with scriptable outcomes for driving the provider without
    /// a Vault server.
    #[derive(Debug)]
    struct ScriptedSource {
        state: Arc<ScriptedState>,
        ttl: Duration,
        renewable: bool,
        login_delay: Duration,
    }

    impl ScriptedSource {
        fn lease(&self) -> Option<LeaseInfo> {
            (!self.ttl.is_zero()).then_some(LeaseInfo {
                ttl: self.ttl,
                renewable: self.renewable,
            })
        }

        fn failure() -> AttemptError {
            AttemptError {
                class: ErrorClass::RetryableStatus,
                error: KmsError::backend_error("scripted auth failure (503)"),
            }
        }
    }

    #[async_trait]
    impl TokenSource for ScriptedSource {
        async fn acquire(&self) -> AttemptResult<TokenLease> {
            if !self.login_delay.is_zero() {
                tokio::time::sleep(self.login_delay).await;
            }
            let call = self.state.login_calls.fetch_add(1, Ordering::SeqCst);
            if self.state.fail_login.load(Ordering::SeqCst) {
                return Err(Self::failure());
            }
            Ok(TokenLease::new(format!("scripted-login-{call}"), self.lease()))
        }

        async fn renew(&self, _client: &VaultClient) -> AttemptResult<TokenLease> {
            let call = self.state.renew_calls.fetch_add(1, Ordering::SeqCst);
            if self.state.fail_renew.load(Ordering::SeqCst) {
                return Err(Self::failure());
            }
            Ok(TokenLease::new(format!("scripted-renew-{call}"), self.lease()))
        }
    }

    async fn scripted_provider(
        ttl: Duration,
        renewable: bool,
        policy: VaultCredentialPolicy,
    ) -> (Arc<VaultCredentialProvider>, Arc<ScriptedState>) {
        let state = Arc::new(ScriptedState::default());
        let source = ScriptedSource {
            state: state.clone(),
            ttl,
            renewable,
            login_delay: Duration::ZERO,
        };
        let provider = VaultCredentialProvider::new(test_settings(), Box::new(source), policy)
            .await
            .expect("scripted provider must build without a live Vault");
        (Arc::new(provider), state)
    }

    async fn static_provider() -> VaultCredentialProvider {
        VaultCredentialProvider::new(
            test_settings(),
            Box::new(StaticToken::new(TEST_TOKEN.to_string())),
            test_policy(Duration::from_secs(10), Duration::from_secs(5)),
        )
        .await
        .expect("static provider must build without a live Vault")
    }

    #[tokio::test]
    async fn test_static_token_snapshots_pin_one_generation() {
        let provider = static_provider().await;

        let first = provider.current().expect("static tokens never expire");
        let second = provider.current().expect("static tokens never expire");

        assert_eq!(first.generation, 0);
        assert!(
            Arc::ptr_eq(&first, &second),
            "without rotation every snapshot must return the same client generation"
        );
    }

    #[tokio::test]
    async fn test_static_token_spawns_no_renewal_task() {
        let provider = Arc::new(static_provider().await);
        assert!(provider.spawn_renewal_task().is_none(), "a token without a lease has nothing to renew");
    }

    #[tokio::test]
    async fn test_static_token_source_yields_configured_token() {
        let settings = test_settings();
        let source = token_source_for(
            &VaultAuthMethod::Token {
                token: TEST_TOKEN.to_string(),
            },
            &settings,
        )
        .expect("token auth must map to a source");

        let lease = source.acquire().await.expect("static acquire cannot fail");
        assert_eq!(lease.expose(), TEST_TOKEN);
        assert!(lease.lease_info().is_none(), "static tokens must not carry a lease");
    }

    #[tokio::test]
    async fn test_approle_auth_method_maps_to_login_source() {
        let settings = test_settings();
        let source = token_source_for(&VaultAuthMethod::approle("role".to_string(), TEST_SECRET_ID.to_string()), &settings)
            .expect("approle auth must map to a login source");

        assert!(format!("{source:?}").contains("AppRoleLogin"));
    }

    #[tokio::test]
    async fn test_kubernetes_auth_method_maps_to_login_source() {
        let settings = test_settings();
        let source = token_source_for(&VaultAuthMethod::kubernetes("rustfs".to_string()), &settings)
            .expect("kubernetes auth must map to a login source");

        assert!(format!("{source:?}").contains("KubernetesLogin"));
    }

    /// `refresh_safety_window_secs` is operator-supplied and reaches the request
    /// path from persisted configuration, so the fail-closed comparison must
    /// survive a window too large to add to the current instant. Before the
    /// checked arithmetic this panicked with "overflow when adding duration to
    /// instant" on the first request after a lease-bearing login.
    #[tokio::test]
    async fn test_current_refuses_rather_than_panics_on_an_unrepresentable_safety_window() {
        let (provider, _state) = scripted_provider(
            Duration::from_secs(60),
            true,
            test_policy(Duration::from_secs(u64::MAX), Duration::from_secs(5)),
        )
        .await;

        let error = provider
            .current()
            .expect_err("a window wider than any lease must refuse the token");
        assert!(
            matches!(error, KmsError::CredentialsUnavailable { .. }),
            "expected CredentialsUnavailable, got {error:?}"
        );
    }

    /// `lease_duration` is a bare u64 straight off the Vault response and forms
    /// the other side of the same comparison, so an absurd one must not panic
    /// either. It is indistinguishable from a non-expiring token, which is how
    /// the zero-lease case already behaves.
    #[tokio::test]
    async fn test_an_unrepresentable_lease_is_treated_as_non_expiring() {
        let (provider, _state) = scripted_provider(
            Duration::from_secs(u64::MAX),
            true,
            test_policy(Duration::from_secs(30), Duration::from_secs(5)),
        )
        .await;

        provider
            .current()
            .expect("a token whose expiry cannot be represented must stay usable");
    }

    /// The configured flag has to reach the HTTP client, not just the config
    /// struct: every generation (authenticated and login) builds its own client,
    /// and a Vault with a self-signed certificate fails the handshake unless
    /// each one carries the setting.
    #[test]
    fn test_skip_tls_verify_reaches_every_vault_client_generation() {
        for skip_tls_verify in [false, true] {
            let settings = VaultConnectionSettings {
                address: "https://vault.example.com:8200".to_string(),
                namespace: None,
                attempt_timeout: Duration::from_secs(30),
                skip_tls_verify,
            };

            let authenticated = settings.build_client(TEST_TOKEN).expect("authenticated client must build");
            assert_eq!(authenticated.settings.verify, !skip_tls_verify);

            let login = settings.build_login_client().expect("login client must build");
            assert_eq!(login.settings.verify, !skip_tls_verify);
        }
    }

    /// vaultrs derives `verify` from its own VAULT_SKIP_VERIFY variable when the
    /// builder leaves it unset, which would disable certificate verification
    /// without passing the KMS insecure-defaults gate.
    #[test]
    fn test_vaultrs_skip_verify_env_cannot_override_the_configured_setting() {
        temp_env::with_var("VAULT_SKIP_VERIFY", Some("true"), || {
            let client = test_settings().build_client(TEST_TOKEN).expect("client must build");
            assert!(
                client.settings.verify,
                "a stray VAULT_SKIP_VERIFY must not disable verification behind the KMS configuration"
            );
        });
    }

    /// The projected token is read fresh per login attempt and trimmed, so a
    /// kubelet rotation is picked up without a restart and a trailing newline
    /// does not corrupt the assertion sent to Vault.
    #[tokio::test]
    async fn test_kubernetes_login_rereads_and_trims_the_service_account_token() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("token");
        tokio::fs::write(&path, "  first-jwt\n").await.expect("write token");

        let login = KubernetesLogin::new(
            &test_settings(),
            DEFAULT_VAULT_KUBERNETES_MOUNT.to_string(),
            "rustfs".to_string(),
            path.clone(),
        )
        .expect("login source must build");

        assert_eq!(login.resolve_jwt().await.expect("first read").expose(), "first-jwt");

        tokio::fs::write(&path, "rotated-jwt").await.expect("rotate token");
        assert_eq!(
            login.resolve_jwt().await.expect("second read").expose(),
            "rotated-jwt",
            "a rotated projected token must be picked up without a restart"
        );
    }

    /// The ServiceAccount token is re-read per attempt, so an unreadable or
    /// empty one fails that attempt without reaching Vault; the refresh loop
    /// keeps retrying, which is what lets a late projection heal the source.
    #[tokio::test]
    async fn test_kubernetes_login_rejects_an_unusable_service_account_token() {
        let dir = tempfile::tempdir().expect("temp dir");
        let missing = dir.path().join("absent-token");
        let empty = dir.path().join("empty-token");
        tokio::fs::write(&empty, "  \n").await.expect("write empty token");

        for (path, expected) in [(missing, "Failed to read"), (empty, "is empty")] {
            let login =
                KubernetesLogin::new(&test_settings(), DEFAULT_VAULT_KUBERNETES_MOUNT.to_string(), "rustfs".to_string(), path)
                    .expect("login source must build");

            let error = login
                .acquire()
                .await
                .expect_err("an unusable ServiceAccount token must fail the attempt");
            assert!(matches!(error.class, ErrorClass::Fatal));
            assert!(error.error.to_string().contains(expected), "got {}", error.error);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_renewal_task_renews_at_half_ttl() {
        let (provider, state) = scripted_provider(
            Duration::from_secs(60),
            true,
            test_policy(Duration::from_secs(10), Duration::from_secs(5)),
        )
        .await;
        let task = provider.spawn_renewal_task().expect("lease-bound tokens need renewal");

        tokio::time::sleep(Duration::from_secs(29)).await;
        assert_eq!(state.renew_calls.load(Ordering::SeqCst), 0, "renewal must not run before half TTL");
        assert_eq!(provider.snapshot().generation, 0);

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(state.renew_calls.load(Ordering::SeqCst), 1, "renewal must run at half TTL");
        assert_eq!(state.login_calls.load(Ordering::SeqCst), 1, "renewable tokens must not re-login");
        assert_eq!(provider.snapshot().generation, 1, "a successful renewal must install a new generation");

        task.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_failed_renewal_falls_back_to_login() {
        let (provider, state) = scripted_provider(
            Duration::from_secs(60),
            true,
            test_policy(Duration::from_secs(10), Duration::from_secs(5)),
        )
        .await;
        state.fail_renew.store(true, Ordering::SeqCst);
        let task = provider.spawn_renewal_task().expect("renewal task");

        tokio::time::sleep(Duration::from_secs(31)).await;
        assert_eq!(state.renew_calls.load(Ordering::SeqCst), 1, "renewal must be attempted first");
        assert_eq!(
            state.login_calls.load(Ordering::SeqCst),
            2,
            "failed renewal must fall back to a fresh login"
        );
        assert_eq!(provider.snapshot().generation, 1);

        task.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_current_fails_closed_inside_safety_window_and_recovers() {
        let (provider, state) = scripted_provider(
            Duration::from_secs(60),
            true,
            test_policy(Duration::from_secs(10), Duration::from_secs(5)),
        )
        .await;
        state.fail_renew.store(true, Ordering::SeqCst);
        state.fail_login.store(true, Ordering::SeqCst);
        let task = provider.spawn_renewal_task().expect("renewal task");

        // Refresh cycles at 30s, 35s, ... keep failing; the token stays usable
        // until 50s (60s TTL minus the 10s safety window).
        tokio::time::sleep(Duration::from_secs(49)).await;
        provider
            .current()
            .expect("token outside the safety window must still be served");

        tokio::time::sleep(Duration::from_secs(2)).await;
        let error = provider
            .current()
            .expect_err("token inside the safety window must be refused");
        assert!(
            matches!(error, KmsError::CredentialsUnavailable { .. }),
            "expected CredentialsUnavailable, got {error:?}"
        );

        // Recovery: after the failed-refresh circuit's cool-down, its single
        // half-open probe succeeds, installs a fresh generation, and the
        // provider serves requests again.
        state.fail_renew.store(false, Ordering::SeqCst);
        state.fail_login.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(31)).await;
        let handle = provider.current().expect("provider must recover after a successful refresh");
        assert!(handle.generation >= 1);

        task.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_shutdown_recycles_renewal_task_promptly() {
        let (provider, _state) = scripted_provider(
            Duration::from_secs(60),
            true,
            test_policy(Duration::from_secs(10), Duration::from_secs(5)),
        )
        .await;
        let task = provider.spawn_renewal_task().expect("renewal task");

        tokio::time::timeout(Duration::from_secs(1), task.shutdown())
            .await
            .expect("cancelled renewal task must exit promptly");
    }

    #[tokio::test(start_paused = true)]
    async fn test_dropping_task_handle_cancels_renewal_task() {
        let (provider, _state) = scripted_provider(
            Duration::from_secs(60),
            true,
            test_policy(Duration::from_secs(10), Duration::from_secs(5)),
        )
        .await;
        let task = provider.spawn_renewal_task().expect("renewal task");
        let cancel_probe = task.cancel.clone();

        drop(task);

        tokio::time::timeout(Duration::from_secs(1), cancel_probe.cancelled())
            .await
            .expect("dropping the handle must cancel the renewal task");
    }

    #[tokio::test(start_paused = true)]
    async fn test_concurrent_refreshes_coalesce_into_one_login() {
        let state = Arc::new(ScriptedState::default());
        let source = ScriptedSource {
            state: state.clone(),
            ttl: Duration::from_secs(60),
            renewable: false,
            login_delay: Duration::from_millis(100),
        };
        let provider = Arc::new(
            VaultCredentialProvider::new(
                test_settings(),
                Box::new(source),
                test_policy(Duration::from_secs(10), Duration::from_secs(5)),
            )
            .await
            .expect("provider"),
        );
        assert_eq!(state.login_calls.load(Ordering::SeqCst), 1, "initial login");

        let cancel = CancellationToken::new();
        let first = {
            let provider = provider.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move { provider.refresh(0, &cancel).await })
        };
        let second = {
            let provider = provider.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move { provider.refresh(0, &cancel).await })
        };
        first.await.expect("join").expect("refresh");
        second.await.expect("join").expect("refresh");

        assert_eq!(
            state.login_calls.load(Ordering::SeqCst),
            2,
            "concurrent refreshes of the same generation must coalesce into one login"
        );
        assert_eq!(provider.snapshot().generation, 1);
    }

    #[tokio::test]
    async fn test_approle_secret_id_file_missing_fails_fatally() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = AppRoleLogin::new(
            &test_settings(),
            "approle".to_string(),
            "role".to_string(),
            String::new(),
            Some(dir.path().join("absent-secret-id")),
        )
        .expect("source");

        let failure = source
            .resolve_secret_id()
            .await
            .expect_err("missing secret_id file must fail the attempt");
        assert_eq!(failure.class, ErrorClass::Fatal);
        assert!(matches!(failure.error, KmsError::ConfigurationError { .. }));
    }

    #[tokio::test]
    async fn test_approle_secret_id_file_empty_fails_fatally() {
        let file = tempfile::NamedTempFile::new().expect("tempfile");
        std::fs::write(file.path(), "  \n\t\n").expect("write whitespace");
        let source = AppRoleLogin::new(
            &test_settings(),
            "approle".to_string(),
            "role".to_string(),
            String::new(),
            Some(file.path().to_path_buf()),
        )
        .expect("source");

        let failure = source
            .resolve_secret_id()
            .await
            .expect_err("effectively empty secret_id file must fail the attempt");
        assert_eq!(failure.class, ErrorClass::Fatal);
        assert!(failure.error.to_string().contains("is empty"));
    }

    #[tokio::test]
    async fn test_approle_secret_id_file_takes_precedence_and_is_trimmed() {
        let file = tempfile::NamedTempFile::new().expect("tempfile");
        std::fs::write(file.path(), format!("  {TEST_SECRET_ID}\n")).expect("write secret");
        let source = AppRoleLogin::new(
            &test_settings(),
            "approle".to_string(),
            "role".to_string(),
            "inline-secret-id-must-lose".to_string(),
            Some(file.path().to_path_buf()),
        )
        .expect("source");

        let secret_id = source.resolve_secret_id().await.expect("readable file must resolve");
        assert_eq!(secret_id.expose(), TEST_SECRET_ID);
    }

    /// Leak regression: the Debug output of every credential-carrying type
    /// must stay free of token and secret_id literals.
    #[tokio::test]
    async fn test_credential_types_debug_redacts_token() {
        let provider = static_provider().await;
        let handle = provider.current().expect("static token");
        let lease = TokenLease::new(
            TEST_TOKEN.to_string(),
            Some(LeaseInfo {
                ttl: Duration::from_secs(60),
                renewable: true,
            }),
        );
        let static_source = StaticToken::new(TEST_TOKEN.to_string());
        let approle_source = AppRoleLogin::new(
            &test_settings(),
            "approle".to_string(),
            "leak-test-role-id".to_string(),
            TEST_SECRET_ID.to_string(),
            None,
        )
        .expect("approle source");

        for rendered in [
            format!("{provider:?}"),
            format!("{handle:?}"),
            format!("{lease:?}"),
            format!("{static_source:?}"),
            format!("{approle_source:?}"),
        ] {
            assert!(!rendered.contains(TEST_TOKEN), "debug output must not leak the vault token: {rendered}");
            assert!(
                !rendered.contains(TEST_SECRET_ID),
                "debug output must not leak the approle secret_id: {rendered}"
            );
        }

        assert!(format!("{lease:?}").contains(REDACTED_SECRET));
        let approle_rendered = format!("{approle_source:?}");
        assert!(approle_rendered.contains("leak-test-role-id"), "role_id is not a secret");
        assert!(approle_rendered.contains(REDACTED_SECRET));
    }

    /// Write a token file with owner-only permissions, as a Vault Agent sink
    /// would.
    fn write_owner_only(path: &std::path::Path, contents: &str) {
        std::fs::write(path, contents).expect("write token file");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).expect("chmod token file");
        }
    }

    async fn token_file_provider(path: std::path::PathBuf, policy: VaultCredentialPolicy) -> Arc<VaultCredentialProvider> {
        Arc::new(
            VaultCredentialProvider::new(test_settings(), Box::new(TokenFileSource::new(path, Duration::from_secs(60))), policy)
                .await
                .expect("token file provider must build without a live Vault"),
        )
    }

    #[tokio::test]
    async fn test_token_file_source_reads_and_trims_token() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("vault-token");
        write_owner_only(&path, &format!("  {TEST_TOKEN}\n"));
        let source = TokenFileSource::new(path, Duration::from_secs(60));

        let lease = source.acquire().await.expect("readable token file must resolve");
        assert_eq!(lease.expose(), TEST_TOKEN);
        let lease_info = lease.lease_info().expect("token file leases carry an observed validity");
        assert_eq!(lease_info.ttl, Duration::from_secs(60));
        assert!(!lease_info.renewable, "agent-managed tokens are refreshed by re-reading, not renew-self");
    }

    #[tokio::test]
    async fn test_token_file_missing_fails_fatally() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = TokenFileSource::new(dir.path().join("absent-token"), Duration::from_secs(60));

        let failure = source.acquire().await.expect_err("missing token file must fail the attempt");
        assert_eq!(failure.class, ErrorClass::Fatal);
        assert!(matches!(failure.error, KmsError::ConfigurationError { .. }));
    }

    #[tokio::test]
    async fn test_token_file_empty_fails_fatally() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("vault-token");
        write_owner_only(&path, "  \n\t\n");
        let source = TokenFileSource::new(path, Duration::from_secs(60));

        let failure = source
            .acquire()
            .await
            .expect_err("effectively empty token file must fail the attempt");
        assert_eq!(failure.class, ErrorClass::Fatal);
        assert!(failure.error.to_string().contains("is empty"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_token_file_rejects_group_or_other_permission_bits() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("vault-token");
        write_owner_only(&path, TEST_TOKEN);
        let source = TokenFileSource::new(path.clone(), Duration::from_secs(60));

        for mode in [0o640u32, 0o604, 0o622, 0o660] {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("chmod");
            let failure = source
                .acquire()
                .await
                .expect_err("token file readable or writable beyond the owner must be rejected");
            assert_eq!(failure.class, ErrorClass::Fatal, "mode {mode:#o}");
            let rendered = failure.error.to_string();
            assert!(rendered.contains("insecure permissions"), "mode {mode:#o}: {rendered}");
            assert!(!rendered.contains(TEST_TOKEN), "permission errors must not echo the token");
        }

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        source.acquire().await.expect("owner-only token file must resolve");
    }

    #[tokio::test(start_paused = true)]
    async fn test_token_file_replacement_installs_new_generation_next_cycle() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("vault-token");
        write_owner_only(&path, "agent-token-v1");
        let provider = token_file_provider(path.clone(), test_policy(Duration::from_secs(10), Duration::from_secs(5))).await;
        let task = provider.spawn_renewal_task().expect("token file credentials are lease-bound");

        tokio::time::sleep(Duration::from_secs(29)).await;
        assert_eq!(provider.snapshot().generation, 0, "no re-read before half the observed validity");

        // Atomic replacement, as a Vault Agent sink writes: temp file + rename.
        let staged = dir.path().join("vault-token.tmp");
        write_owner_only(&staged, "agent-token-v2");
        std::fs::rename(&staged, &path).expect("atomic replace");

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(
            provider.snapshot().generation,
            1,
            "the poll after a rotation must install a new generation"
        );

        // A poll without a rotation still installs a fresh generation: each
        // successful read re-extends the token's observed validity.
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(provider.snapshot().generation, 2);

        task.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_token_file_deletion_fails_closed_and_recovers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("vault-token");
        write_owner_only(&path, "agent-token-v1");
        let provider = token_file_provider(path.clone(), test_policy(Duration::from_secs(10), Duration::from_secs(5))).await;
        let task = provider.spawn_renewal_task().expect("renewal task");

        std::fs::remove_file(&path).expect("remove token file");

        // Polls at 30s, 35s, ... keep failing; the last read keeps the token
        // usable until 50s (60s observed validity minus the 10s safety window).
        tokio::time::sleep(Duration::from_secs(49)).await;
        provider
            .current()
            .expect("token outside the safety window must still be served");

        tokio::time::sleep(Duration::from_secs(2)).await;
        let error = provider
            .current()
            .expect_err("token inside the safety window must be refused");
        assert!(
            matches!(error, KmsError::CredentialsUnavailable { .. }),
            "expected CredentialsUnavailable, got {error:?}"
        );

        // Restoring the file heals the provider on the next retry cycle.
        write_owner_only(&path, "agent-token-v2");
        tokio::time::sleep(Duration::from_secs(6)).await;
        let handle = provider.current().expect("provider must recover once the token file is back");
        assert!(handle.generation >= 1);

        task.shutdown().await;
    }

    #[tokio::test]
    async fn test_token_file_debug_redacts_token() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("vault-token");
        write_owner_only(&path, TEST_TOKEN);
        let source = TokenFileSource::new(path.clone(), Duration::from_secs(60));
        source.acquire().await.expect("token file must resolve");

        let rendered = format!("{source:?}");
        assert!(!rendered.contains(TEST_TOKEN), "debug output must not leak the vault token: {rendered}");
        assert!(rendered.contains("vault-token"), "the file path is not a secret");
    }

    // -- Metric emission ----------------------------------------------------
    //
    // Each test installs a thread-local debugging recorder and drives a
    // paused-clock current-thread runtime inside it, so the renewal loop's
    // gauge publications land on exact virtual timestamps.

    use metrics_util::MetricKind;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    type MetricEntry = (
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    );

    /// Run `test` on a paused current-thread runtime under a debugging
    /// recorder and return one snapshot of everything it emitted.
    ///
    /// A single snapshot per test on purpose: `Snapshotter::snapshot` drains
    /// the recorded state, so taking it per assertion would only show the
    /// first assertion any data.
    fn record_metrics<Out>(
        test: impl FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = Out>>>,
    ) -> (Vec<MetricEntry>, Out) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let out = metrics::with_local_recorder(&recorder, || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .expect("current-thread runtime must build");
            runtime.block_on(test())
        });
        (snapshotter.snapshot().into_vec(), out)
    }

    /// Last value of a gauge, plus the labels it was published with.
    fn gauge(snapshot: &[MetricEntry], name: &str) -> Option<(f64, Vec<String>)> {
        snapshot.iter().find_map(|(composite, _unit, _description, value)| {
            let matches = composite.kind() == MetricKind::Gauge && composite.key().name() == name;
            match (matches, value) {
                (true, DebugValue::Gauge(value)) => Some((
                    value.into_inner(),
                    composite.key().labels().map(|label| label.key().to_string()).collect(),
                )),
                _ => None,
            }
        })
    }

    #[test]
    fn renewal_loop_republishes_token_ttl_while_it_waits() {
        let (snapshot, ()) = record_metrics(|| {
            Box::pin(async {
                let (provider, _state) = scripted_provider(
                    Duration::from_secs(60),
                    true,
                    test_policy(Duration::from_secs(10), Duration::from_secs(5)),
                )
                .await;
                let task = provider.spawn_renewal_task().expect("lease-bound tokens need renewal");

                // Well inside the first half of the lease: nothing has been
                // renewed yet, so only the observation cadence can have moved
                // the gauge off its initial 60s.
                tokio::time::sleep(Duration::from_secs(25)).await;
                task.shutdown().await;
            })
        });

        let (ttl, labels) = gauge(&snapshot, METRIC_TOKEN_TTL_SECONDS).expect("token TTL gauge must be published");
        assert!(
            (ttl - 40.0).abs() < 1.0,
            "expected ~40s left of a 60s lease at the last observation, got {ttl}"
        );
        assert!(labels.is_empty(), "credential gauges must carry no labels, got {labels:?}");
        assert_eq!(
            gauge(&snapshot, METRIC_CREDENTIALS_FAIL_CLOSED).map(|(value, _)| value),
            Some(0.0),
            "a token outside its safety window must not report fail-closed"
        );
    }

    #[test]
    fn fail_closed_gauge_tracks_the_gate_the_request_path_applies() {
        let (snapshot, refused) = record_metrics(|| {
            Box::pin(async {
                let (provider, state) = scripted_provider(
                    Duration::from_secs(60),
                    true,
                    test_policy(Duration::from_secs(10), Duration::from_secs(5)),
                )
                .await;
                state.fail_renew.store(true, Ordering::SeqCst);
                state.fail_login.store(true, Ordering::SeqCst);
                let task = provider.spawn_renewal_task().expect("renewal task");

                // 60s lease minus the 10s safety window: by 51s the provider
                // is refusing the token, and the gauge must already say so.
                tokio::time::sleep(Duration::from_secs(51)).await;
                let refused = provider.current().is_err();
                task.shutdown().await;
                refused
            })
        });

        assert!(refused, "a token inside the safety window must be refused");
        assert_eq!(
            gauge(&snapshot, METRIC_CREDENTIALS_FAIL_CLOSED).map(|(value, _)| value),
            Some(1.0),
            "the gauge must report the same fail-closed state the request path enforces"
        );
        let (ttl, _labels) = gauge(&snapshot, METRIC_TOKEN_TTL_SECONDS).expect("token TTL gauge must be published");
        assert!(
            (ttl - 10.0).abs() < 1.0,
            "expected the TTL gauge to track the lease down into its safety window, got {ttl}"
        );
    }
}
