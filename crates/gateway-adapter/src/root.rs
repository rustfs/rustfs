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

//! The deployment's root credentials, as every adapter in this crate reads them.
//!
//! Responsible for: [`RootCredentials`], the validated root key pair plus the key STS session
//! tokens are signed with, and [`RootSource`], the one live handle both the credential provider
//! and the policy source read on every request ([`RootCell`] is a settable implementation).
//! NOT responsible for: where the root keys come from or when they change; the binary owns that
//! and publishes each change into its source.
//! Upstream: the binary's runtime credentials. Downstream: `crate::credentials`, `crate::policy`.
//!
//! # Why one source
//!
//! Legacy reads the current root credentials on every request. Two copies taken at assembly could
//! disagree, or go stale after a root change, and a key one adapter still treats as root would be
//! answered with the root secret or judged as the owner. Both adapters therefore hold the same
//! `Arc<dyn RootSource>`, handed to them together by [`crate::IamAdapters::new`].

use std::sync::{Arc, RwLock};

use rustfs_gateway::Credentials;

/// The shortest secret key, in bytes, that any credential in this crate may verify a signature
/// with. The same bound IAM applies to its own credentials; a signature over a shorter (or empty)
/// key is one anybody can compute.
pub const MIN_SECRET_KEY_LEN: usize = 8;

/// Why [`RootCredentials::new`] refused its input. Carries no input text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RootCredentialsError {
    /// The access key id is not one the gateway can hold.
    InvalidAccessKey,
    /// The root secret key is shorter than [`MIN_SECRET_KEY_LEN`].
    WeakSecretKey,
    /// The STS token-signing key is shorter than [`MIN_SECRET_KEY_LEN`].
    WeakSigningKey,
}

impl core::fmt::Display for RootCredentialsError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::InvalidAccessKey => "root access key is not usable",
            Self::WeakSecretKey => "root secret key is shorter than the minimum length",
            Self::WeakSigningKey => "STS token-signing key is shorter than the minimum length",
        })
    }
}

impl std::error::Error for RootCredentialsError {}

/// The root key pair and the STS token-signing key, validated together.
///
/// Implements no `Debug`; the secrets are readable only inside this crate.
pub struct RootCredentials {
    credentials: Credentials,
    sts_signing_key: Box<str>,
}

impl RootCredentials {
    /// Validates the root credentials and the key IAM signs STS session tokens with (today the
    /// root secret, `rustfs_iam::manager::get_token_signing_key`; pass it separately so a future
    /// key separation changes no adapter).
    ///
    /// # Errors
    ///
    /// [`RootCredentialsError`] when either secret is shorter than [`MIN_SECRET_KEY_LEN`] bytes or
    /// the access key id is not one the gateway can hold.
    pub fn new(access_key: &str, secret_key: &str, sts_signing_key: &str) -> Result<Self, RootCredentialsError> {
        if secret_key.len() < MIN_SECRET_KEY_LEN {
            return Err(RootCredentialsError::WeakSecretKey);
        }
        if sts_signing_key.len() < MIN_SECRET_KEY_LEN {
            return Err(RootCredentialsError::WeakSigningKey);
        }
        let credentials =
            Credentials::new(access_key, secret_key.as_bytes()).map_err(|_| RootCredentialsError::InvalidAccessKey)?;
        Ok(Self {
            credentials,
            sts_signing_key: sts_signing_key.into(),
        })
    }

    /// The root access key id, a public identifier.
    #[must_use]
    pub fn access_key(&self) -> &str {
        self.credentials.identity().access_key_id()
    }

    pub(crate) fn credentials(&self) -> &Credentials {
        &self.credentials
    }

    pub(crate) fn sts_signing_key(&self) -> &str {
        &self.sts_signing_key
    }
}

/// The live root credentials, read on every request by both adapters.
pub trait RootSource: Send + Sync + 'static {
    /// The current root credentials; `None` while they are not initialized, which both adapters
    /// answer as unavailable rather than guessing.
    fn current(&self) -> Option<Arc<RootCredentials>>;
}

/// A [`RootSource`] the binary sets at startup and again on every root change.
pub struct RootCell {
    current: RwLock<Option<Arc<RootCredentials>>>,
}

impl RootCell {
    /// A cell holding `initial`.
    #[must_use]
    pub fn new(initial: Option<RootCredentials>) -> Self {
        Self {
            current: RwLock::new(initial.map(Arc::new)),
        }
    }

    /// Replaces the root credentials; every later request reads the new ones.
    pub fn set(&self, root: RootCredentials) {
        *self.current.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(root));
    }

    /// Withdraws the root credentials; every later request is unavailable until [`Self::set`].
    pub fn clear(&self) {
        *self.current.write().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

impl RootSource for RootCell {
    fn current(&self) -> Option<Arc<RootCredentials>> {
        self.current.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Negative — an empty or short root secret is refused at construction, so no source can ever
    /// hand out a root key anybody could sign with; eight bytes is accepted.
    #[test]
    fn root_short_secret_is_refused() {
        for weak in ["", "1234567"] {
            let refused = RootCredentials::new("rootadmin", weak, "signing-key-0001");
            assert!(matches!(refused, Err(RootCredentialsError::WeakSecretKey)), "{} bytes", weak.len());
        }
        assert!(RootCredentials::new("rootadmin", "12345678", "signing-key-0001").is_ok());
    }

    /// Negative — an empty or short STS signing key is refused at construction.
    #[test]
    fn root_short_signing_key_is_refused() {
        for weak in ["", "1234567"] {
            let refused = RootCredentials::new("rootadmin", "root-secret-key-0001", weak);
            assert!(matches!(refused, Err(RootCredentialsError::WeakSigningKey)), "{} bytes", weak.len());
        }
    }
}
