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
//! a client for their own lifetime: a future credential rotation then applies
//! to the next call, while calls already in flight finish on the generation
//! they captured (their `Arc` keeps it alive).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use vaultrs::client::{VaultClient, VaultClientSettingsBuilder};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::config::{VaultAuthMethod, redacted_secret};
use crate::error::{KmsError, Result};

/// A Vault token handed out by a [`TokenSource`].
///
/// The crate's copy of the token is zeroized on drop. This cannot cover the
/// copy `vaultrs` keeps inside its client settings (or the HTTP headers built
/// from it); it bounds how long the token lingers in memory owned by this
/// module.
///
/// AppRole login (PR-2) will extend this with the lease metadata returned by
/// the login endpoint (`lease_duration`, `renewable`, accessor).
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub(crate) struct TokenLease {
    token: String,
}

impl TokenLease {
    pub(crate) fn new(token: String) -> Self {
        Self { token }
    }

    /// Expose the raw token for handing to the Vault client builder.
    pub(crate) fn expose(&self) -> &str {
        &self.token
    }
}

impl fmt::Debug for TokenLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenLease")
            .field("token", &redacted_secret(&self.token))
            .finish()
    }
}

/// Source of Vault authentication tokens.
///
/// Only [`StaticToken`] exists today. The trait is async and fallible so
/// future sources can perform I/O when acquiring a token without changing the
/// provider:
/// - `AppRoleLogin` (PR-2): performs an `auth/approle/login` round trip and
///   returns the issued token with its lease metadata;
/// - `TokenFile` (PR-3): re-reads an agent-managed token file.
#[async_trait]
pub(crate) trait TokenSource: fmt::Debug + Send + Sync {
    /// Acquire a token for a new client generation.
    ///
    /// Called once at provider construction today; rotation (PR-2) will call
    /// it again for every re-authentication.
    async fn acquire(&self) -> Result<TokenLease>;
}

/// Token source for [`VaultAuthMethod::Token`]: always yields the token fixed
/// at configuration time.
pub(crate) struct StaticToken {
    token: TokenLease,
}

impl StaticToken {
    pub(crate) fn new(token: String) -> Self {
        Self {
            token: TokenLease::new(token),
        }
    }
}

#[async_trait]
impl TokenSource for StaticToken {
    async fn acquire(&self) -> Result<TokenLease> {
        Ok(self.token.clone())
    }
}

impl fmt::Debug for StaticToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TokenLease::fmt already redacts the token value.
        f.debug_struct("StaticToken").field("token", &self.token).finish()
    }
}

/// Map the configured auth method onto a token source.
///
/// AppRole is still rejected at construction time; PR-2 replaces this arm with
/// an `AppRoleLogin` source.
pub(crate) fn token_source_for(auth_method: &VaultAuthMethod) -> Result<Box<dyn TokenSource>> {
    match auth_method {
        VaultAuthMethod::Token { token } => Ok(Box::new(StaticToken::new(token.clone()))),
        VaultAuthMethod::AppRole { .. } => Err(KmsError::backend_error(
            "AppRole authentication not yet implemented. Please use token authentication.",
        )),
    }
}

/// Connection parameters shared by every client generation.
#[derive(Debug, Clone)]
pub(crate) struct VaultConnectionSettings {
    pub(crate) address: String,
    pub(crate) namespace: Option<String>,
    /// Per-attempt HTTP timeout applied to the underlying reqwest client.
    pub(crate) attempt_timeout: Duration,
}

impl VaultConnectionSettings {
    /// Build an authenticated client for one generation.
    fn build_client(&self, token: &TokenLease) -> Result<VaultClient> {
        let mut settings_builder = VaultClientSettingsBuilder::default();
        settings_builder.address(&self.address);
        // Defense in depth against stalled connections: vaultrs leaves the
        // underlying reqwest client without any timeout by default, so a hung
        // request would otherwise wait forever regardless of the
        // operation-level retry policy.
        settings_builder.timeout(Some(self.attempt_timeout));
        settings_builder.token(token.expose());

        if let Some(namespace) = &self.namespace {
            settings_builder.namespace(Some(namespace.clone()));
        }

        let settings = settings_builder
            .build()
            .map_err(|e| KmsError::backend_error(format!("Failed to build Vault client settings: {e}")))?;

        VaultClient::new(settings).map_err(|e| KmsError::backend_error(format!("Failed to create Vault client: {e}")))
    }
}

/// One authenticated client generation.
///
/// Request paths hold the handle (via `Arc`) for the duration of a single
/// Vault call, so a rotation that swaps in a newer generation never tears the
/// client out from under an in-flight request.
pub(crate) struct VaultClientHandle {
    /// Monotonic counter identifying the credential generation this client was
    /// built from. Static tokens never rotate, so only generation 0 exists
    /// today; rotation (PR-2) bumps it on every re-authentication.
    pub(crate) generation: u64,
    pub(crate) client: VaultClient,
}

impl fmt::Debug for VaultClientHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `VaultClient` embeds its settings, including the token, so it must
        // never appear in Debug output.
        f.debug_struct("VaultClientHandle")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

/// Owns the authenticated Vault client for a backend and hands out
/// per-request snapshots.
///
/// The provider keeps neither the settings nor the source after construction
/// because a static token can never be refreshed. Rotation (PR-2) will retain
/// both and add a refresh path that acquires a fresh lease, rebuilds the
/// client, and stores it under a bumped generation.
pub(crate) struct VaultCredentialProvider {
    current: ArcSwap<VaultClientHandle>,
}

impl VaultCredentialProvider {
    /// Authenticate with `source` and build the initial client generation.
    pub(crate) async fn new(settings: VaultConnectionSettings, source: Box<dyn TokenSource>) -> Result<Self> {
        let lease = source.acquire().await?;
        let client = settings.build_client(&lease)?;
        Ok(Self {
            current: ArcSwap::from_pointee(VaultClientHandle { generation: 0, client }),
        })
    }

    /// Snapshot the current client generation.
    ///
    /// Take one snapshot per Vault call: the returned `Arc` pins the
    /// generation for exactly that call, so a concurrent rotation applies to
    /// the next call without interrupting this one.
    pub(crate) fn current(&self) -> Arc<VaultClientHandle> {
        self.current.load_full()
    }
}

impl fmt::Debug for VaultCredentialProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VaultCredentialProvider")
            .field("current", &self.current.load())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::REDACTED_SECRET;

    const TEST_TOKEN: &str = "vault-token-debug-leak-canary";

    fn test_settings() -> VaultConnectionSettings {
        VaultConnectionSettings {
            address: "http://127.0.0.1:8200".to_string(),
            namespace: Some("team-namespace".to_string()),
            attempt_timeout: Duration::from_secs(30),
        }
    }

    /// Building a provider never contacts Vault, so these tests run offline.
    async fn test_provider() -> VaultCredentialProvider {
        VaultCredentialProvider::new(test_settings(), Box::new(StaticToken::new(TEST_TOKEN.to_string())))
            .await
            .expect("provider construction must not require a live Vault")
    }

    #[tokio::test]
    async fn test_static_token_snapshots_pin_one_generation() {
        let provider = test_provider().await;

        let first = provider.current();
        let second = provider.current();

        assert_eq!(first.generation, 0);
        assert!(
            Arc::ptr_eq(&first, &second),
            "without rotation every snapshot must return the same client generation"
        );
    }

    #[tokio::test]
    async fn test_static_token_source_yields_configured_token() {
        let source = token_source_for(&VaultAuthMethod::Token {
            token: TEST_TOKEN.to_string(),
        })
        .expect("token auth must map to a source");

        let lease = source.acquire().await.expect("static acquire cannot fail");
        assert_eq!(lease.expose(), TEST_TOKEN);
    }

    /// Behavior pin: AppRole keeps failing at construction with the same
    /// user-visible message until the login source lands (PR-2).
    #[test]
    fn test_approle_auth_method_still_rejected() {
        let error = token_source_for(&VaultAuthMethod::AppRole {
            role_id: "role".to_string(),
            secret_id: "approle-secret-canary".to_string(),
        })
        .expect_err("approle must stay rejected until the login source lands");

        let rendered = error.to_string();
        assert!(rendered.contains("AppRole authentication not yet implemented"), "got: {rendered}");
        assert!(!rendered.contains("approle-secret-canary"), "error must not echo the secret id");
    }

    /// Leak regression: the Debug output of every credential-carrying type
    /// must stay free of the token literal.
    #[tokio::test]
    async fn test_credential_types_debug_redacts_token() {
        let provider = test_provider().await;
        let handle = provider.current();
        let lease = TokenLease::new(TEST_TOKEN.to_string());
        let source = StaticToken::new(TEST_TOKEN.to_string());

        for rendered in [
            format!("{provider:?}"),
            format!("{handle:?}"),
            format!("{lease:?}"),
            format!("{source:?}"),
        ] {
            assert!(!rendered.contains(TEST_TOKEN), "debug output must not leak the vault token: {rendered}");
        }

        assert!(format!("{lease:?}").contains(REDACTED_SECRET));
    }
}
