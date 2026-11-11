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
//! tokens are signed with; [`RootSource`], the one live handle both the credential provider and
//! the policy source read on every request; and the effective-secret-length rule every secret in
//! this crate is held to.
//! NOT responsible for: where the root keys come from or when they change. The binary implements
//! [`RootSource`] directly over its live root accessor — a closure is enough — so there is no
//! second copy of root to keep in step.
//! Upstream: the binary's runtime credentials. Downstream: `crate::credentials`, `crate::policy`.
//!
//! # Why one source
//!
//! Legacy reads the current root credentials on every request. Two copies taken at assembly could
//! disagree, or go stale after a root change, and a key one adapter still treats as root would be
//! answered with the root secret or judged as the owner. Both adapters therefore hold the same
//! `Arc<dyn RootSource>`, handed to them together by [`crate::IamAdapters::new`].

use std::sync::Arc;

use rustfs_gateway::Credentials;

/// The shortest effective secret key, in bytes, that any credential in this crate may verify a
/// signature with. The same bound IAM applies to its own credentials; a signature over a shorter
/// (or empty) key is one anybody can compute.
pub const MIN_SECRET_KEY_LEN: usize = 8;

/// The number of bytes of `secret` a signature actually depends on: its length after trailing NUL
/// bytes are removed.
///
/// HMAC pads a key shorter than its block with zero bytes, and SigV4's first key is `"AWS4"`
/// followed by the secret, so trailing NULs add nothing: eight NUL bytes sign exactly like the
/// empty secret, and `"abc"` followed by five NULs signs like `"abc"`. A secret of NULs only has
/// an effective length of zero. An interior NUL is followed by real key bytes, which change the
/// padded key, so it counts like any other byte.
pub(crate) fn effective_secret_len(secret: &[u8]) -> usize {
    secret.iter().rposition(|byte| *byte != 0).map_or(0, |last| last + 1)
}

/// Whether `secret` may verify a signature: its [`effective_secret_len`] is at least
/// [`MIN_SECRET_KEY_LEN`]. The one rule for root, IAM credentials and the STS signing key.
pub(crate) fn is_usable_secret(secret: &[u8]) -> bool {
    effective_secret_len(secret) >= MIN_SECRET_KEY_LEN
}

/// Why [`RootCredentials::new`] refused its input. Carries no input text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RootCredentialsError {
    /// The access key id is not one the gateway can hold.
    InvalidAccessKey,
    /// The root secret key fails the effective-length rule.
    WeakSecretKey,
    /// The STS token-signing key fails the effective-length rule.
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
    /// [`RootCredentialsError`] when either secret fails the effective-length rule (fewer than
    /// [`MIN_SECRET_KEY_LEN`] bytes once trailing NUL bytes are removed) or the access key id is not
    /// one the gateway can hold.
    pub fn new(access_key: &str, secret_key: &str, sts_signing_key: &str) -> Result<Self, RootCredentialsError> {
        if !is_usable_secret(secret_key.as_bytes()) {
            return Err(RootCredentialsError::WeakSecretKey);
        }
        if !is_usable_secret(sts_signing_key.as_bytes()) {
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
///
/// The binary implements it directly over RustFS's live root accessor, so a root change needs no
/// second step. Any `Fn() -> Option<Arc<RootCredentials>>` is a source; for example, over the
/// server context:
///
/// ```ignore
/// let root = move || {
///     let cred = ctx.action_credentials().get()?;
///     let signing_key = ctx.token_signing_key()?;
///     RootCredentials::new(&cred.access_key, &cred.secret_key, &signing_key).ok().map(Arc::new)
/// };
/// let adapters = IamAdapters::new(iam, Arc::new(root));
/// ```
///
/// Root credentials that fail validation read as `None`, which both adapters answer as
/// unavailable.
pub trait RootSource: Send + Sync + 'static {
    /// The current root credentials; `None` while they are not initialized, which both adapters
    /// answer as unavailable rather than guessing.
    fn current(&self) -> Option<Arc<RootCredentials>>;
}

impl<F> RootSource for F
where
    F: Fn() -> Option<Arc<RootCredentials>> + Send + Sync + 'static,
{
    fn current(&self) -> Option<Arc<RootCredentials>> {
        self()
    }
}

/// Test helper: a [`RootSource`] holding fixed root credentials. Production code reads the live
/// accessor instead, so that there is no copy of root to keep in step.
#[cfg(test)]
pub(crate) struct RootCell(Option<Arc<RootCredentials>>);

#[cfg(test)]
impl RootCell {
    pub(crate) fn new(root: Option<RootCredentials>) -> Self {
        Self(root.map(Arc::new))
    }
}

#[cfg(test)]
impl RootSource for RootCell {
    fn current(&self) -> Option<Arc<RootCredentials>> {
        self.0.clone()
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
        for weak in ["", "1234567", "\0\0\0\0\0\0\0\0", "abc\0\0\0\0\0", "abcdefg\0"] {
            let refused = RootCredentials::new("rootadmin", weak, "signing-key-0001");
            assert!(matches!(refused, Err(RootCredentialsError::WeakSecretKey)), "{} bytes", weak.len());
        }
        assert!(RootCredentials::new("rootadmin", "12345678", "signing-key-0001").is_ok());
    }

    /// Positive — an interior NUL is followed by real key bytes and counts: `"abc\0defg"` has an
    /// effective length of eight and is accepted, as is the same secret with trailing NULs.
    #[test]
    fn root_interior_nul_counts_toward_length() {
        for secret in ["abc\0defg", "abc\0defg\0\0"] {
            assert!(RootCredentials::new("rootadmin", secret, "signing-key-0001").is_ok(), "{secret:?}");
            assert!(RootCredentials::new("rootadmin", "root-secret-key-0001", secret).is_ok(), "{secret:?}");
        }
    }

    /// Negative — an empty or short STS signing key is refused at construction.
    #[test]
    fn root_short_signing_key_is_refused() {
        for weak in ["", "1234567", "\0\0\0\0\0\0\0\0", "abc\0\0\0\0\0"] {
            let refused = RootCredentials::new("rootadmin", "root-secret-key-0001", weak);
            assert!(matches!(refused, Err(RootCredentialsError::WeakSigningKey)), "{} bytes", weak.len());
        }
    }
}
