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

use rustfs_appauth::token::{Token, parse_license_with_public_key};
use std::fmt;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

pub type LicenseResult<T> = std::result::Result<T, LicenseError>;
pub type SharedLicenseVerifier = Arc<dyn LicenseVerifier>;

#[derive(Clone, Debug)]
pub enum LicenseError {
    /// Internal license state lock could not be acquired.
    StatePoisoned,
    /// License is required in licensed builds but not provided.
    Missing,
    /// Token decoding or signature check failed.
    Invalid(String),
    /// License expiration check failed.
    #[cfg(feature = "license")]
    Expired { expired_at: u64, now: u64 },
    /// System time could not be read.
    Clock(String),
}

impl fmt::Display for LicenseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LicenseError::StatePoisoned => write!(f, "License state is unavailable"),
            LicenseError::Missing => write!(f, "License is required when building with feature `license`."),
            LicenseError::Invalid(message) => write!(f, "Incorrect license, please contact RustFS. {message}"),
            #[cfg(feature = "license")]
            LicenseError::Expired { expired_at, now } => {
                write!(f, "Incorrect license, please contact RustFS. expired_at={expired_at}, now={now}")
            }
            LicenseError::Clock(message) => write!(f, "Failed to read system time: {message}"),
        }
    }
}

impl std::error::Error for LicenseError {}

impl LicenseError {
    fn into_io(self) -> Error {
        match self {
            LicenseError::StatePoisoned | LicenseError::Clock(_) => Error::other(self.to_string()),
            LicenseError::Missing | LicenseError::Invalid(_) => Error::new(ErrorKind::PermissionDenied, self.to_string()),
            #[cfg(feature = "license")]
            LicenseError::Expired { .. } => Error::new(ErrorKind::PermissionDenied, self.to_string()),
        }
    }
}

#[derive(Clone, Debug, Default)]
enum LicenseStatus {
    /// Internal state has not been evaluated yet.
    #[default]
    Uninitialized,
    /// License has been validated.
    Valid,
    /// License is missing for strict license builds.
    Missing,
    /// License validation failed.
    Invalid(String),
}

impl fmt::Display for LicenseStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uninitialized => write!(f, "uninitialized"),
            Self::Valid => write!(f, "valid"),
            Self::Missing => write!(f, "missing"),
            Self::Invalid(message) => write!(f, "{message}"),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct LicenseState {
    token: Option<Token>,
    status: LicenseStatus,
}

/// Verifier for parsing and validating raw license materials.
pub trait LicenseVerifier: Send + Sync {
    fn validate(&self, raw_license: &str, now: u64) -> LicenseResult<Token>;
}

#[derive(Debug, Default)]
struct AppAuthLicenseVerifier;

impl LicenseVerifier for AppAuthLicenseVerifier {
    fn validate(&self, raw_license: &str, _now: u64) -> LicenseResult<Token> {
        let public_key = license_public_key()?;
        let token =
            parse_license_with_public_key(raw_license, &public_key).map_err(|err| LicenseError::Invalid(err.to_string()))?;

        #[cfg(feature = "license")]
        if token.expired <= _now {
            return Err(LicenseError::Expired {
                expired_at: token.expired,
                now: _now,
            });
        }

        Ok(token)
    }
}

static LICENSE_STATE: OnceLock<RwLock<LicenseState>> = OnceLock::new();
static LICENSE_VERIFIER: OnceLock<SharedLicenseVerifier> = OnceLock::new();

fn license_state() -> &'static RwLock<LicenseState> {
    LICENSE_STATE.get_or_init(|| RwLock::new(LicenseState::default()))
}

fn default_license_verifier() -> SharedLicenseVerifier {
    Arc::new(AppAuthLicenseVerifier)
}

fn license_verifier() -> &'static SharedLicenseVerifier {
    LICENSE_VERIFIER.get_or_init(default_license_verifier)
}

fn now_epoch_secs() -> LicenseResult<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| LicenseError::Clock(err.to_string()))
        .map(|value| value.as_secs())
}

fn normalized_license(raw_license: Option<String>) -> Option<String> {
    raw_license.map(|raw| raw.trim().to_string()).filter(|raw| !raw.is_empty())
}

fn license_public_key() -> LicenseResult<String> {
    let public_key = std::env::var(rustfs_config::ENV_RUSTFS_LICENSE_PUBLIC_KEY)
        .map(|raw| raw.trim().to_string())
        .map_err(|_| {
            LicenseError::Invalid(format!(
                "{} must contain the RSA public key used to verify licenses",
                rustfs_config::ENV_RUSTFS_LICENSE_PUBLIC_KEY
            ))
        })?;

    if public_key.is_empty() {
        return Err(LicenseError::Invalid(format!(
            "{} must contain the RSA public key used to verify licenses",
            rustfs_config::ENV_RUSTFS_LICENSE_PUBLIC_KEY
        )));
    }

    Ok(public_key)
}

fn is_license_token_current(token: &Token, now: u64) -> bool {
    token.expired > now
}

fn strict_build_missing_status() -> LicenseStatus {
    if cfg!(feature = "license") {
        LicenseStatus::Missing
    } else {
        LicenseStatus::Uninitialized
    }
}

fn apply_missing_status(state: &mut LicenseState) {
    state.token = None;
    state.status = strict_build_missing_status();
}

fn apply_invalid_status(state: &mut LicenseState, err: LicenseError) {
    state.token = None;
    state.status = LicenseStatus::Invalid(match err {
        LicenseError::Invalid(message) => message,
        #[cfg(feature = "license")]
        LicenseError::Expired { expired_at, now } => format!("expired at {expired_at}, now {now}"),
        LicenseError::Clock(message) => format!("system clock error: {message}"),
        LicenseError::Missing => "license is required".to_string(),
        LicenseError::StatePoisoned => "license state is unavailable".to_string(),
    });
}

fn apply_valid_status(state: &mut LicenseState, token: Token) {
    state.token = Some(token);
    state.status = LicenseStatus::Valid;
}

/// Replace the global license verifier.
///
/// This is the extension point for OEM/build-time overlays.
/// Returns `false` if the verifier was already initialized.
#[allow(dead_code)]
pub fn set_license_verifier(verifier: SharedLicenseVerifier) -> bool {
    LICENSE_VERIFIER.set(verifier).is_ok()
}

/// Initialize the license in memory.
///
/// This keeps the default API signature stable and is safe to call multiple times.
pub fn initialize_license(raw_license: Option<String>) {
    if let Err(err) = initialize_license_result(raw_license) {
        error!("license initialization failed: {err}");
    }
}

/// Explicit initialization API with typed error return.
pub fn initialize_license_result(raw_license: Option<String>) -> LicenseResult<()> {
    let normalized = normalized_license(raw_license);
    let mut state = license_state().write().map_err(|_| LicenseError::StatePoisoned)?;

    match normalized {
        Some(raw_license) => {
            let now = now_epoch_secs()?;
            match license_verifier().validate(&raw_license, now) {
                Ok(token) => {
                    apply_valid_status(&mut state, token.clone());
                    info!("license loaded, subject: {}", token.name);
                    Ok(())
                }
                Err(err) => {
                    apply_invalid_status(&mut state, err.clone());
                    warn!("license verification failed: {err}");
                    Err(err)
                }
            }
        }
        None => {
            apply_missing_status(&mut state);
            if let LicenseStatus::Missing = state.status {
                Err(LicenseError::Missing)
            } else {
                Ok(())
            }
        }
    }
}

/// Legacy name kept for existing startup code.
pub fn init_license(license: Option<String>) {
    initialize_license(license);
}

/// Return the current license information.
pub fn get_license() -> Option<Token> {
    license_state().read().ok().and_then(|state| state.token.clone())
}

/// New name for compatibility with external integrations.
pub fn current_license() -> Option<Token> {
    get_license()
}

/// Return whether the loaded license token is present and not expired.
pub fn has_valid_license() -> bool {
    let Some(token) = get_license() else {
        return false;
    };
    let Ok(now) = now_epoch_secs() else {
        return false;
    };

    is_license_token_current(&token, now)
}

/// Observe the current license status for observability.
pub fn license_status() -> String {
    license_state()
        .read()
        .ok()
        .map(|state| state.status.to_string())
        .unwrap_or_else(|| LicenseStatus::Uninitialized.to_string())
}

/// Check whether current in-memory license is currently valid.
#[cfg(feature = "license")]
pub fn ensure_license() -> LicenseResult<()> {
    let state = license_state().read().map_err(|_| LicenseError::StatePoisoned)?;
    match &state.status {
        LicenseStatus::Missing => return Err(LicenseError::Missing),
        LicenseStatus::Invalid(message) => return Err(LicenseError::Invalid(message.to_string())),
        LicenseStatus::Uninitialized | LicenseStatus::Valid => {}
    };

    let token = state.token.as_ref().ok_or(LicenseError::Missing)?;
    let now = now_epoch_secs()?;
    if token.expired <= now {
        return Err(LicenseError::Expired {
            expired_at: token.expired,
            now,
        });
    }

    Ok(())
}

#[cfg(not(feature = "license"))]
pub fn ensure_license() -> LicenseResult<()> {
    Ok(())
}

/// Compatibility API for call-sites that still use the legacy name.
pub fn license_check() -> Result<()> {
    ensure_license().map_err(LicenseError::into_io)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::{
        RsaPrivateKey, RsaPublicKey,
        pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding},
    };
    use rustfs_appauth::token::sign_license_token;
    use serial_test::serial;

    #[test]
    fn license_token_current_requires_future_expiration() {
        let token = Token {
            name: "test_app".to_string(),
            expired: 100,
        };

        assert!(is_license_token_current(&token, 99));
        assert!(!is_license_token_current(&token, 100));
        assert!(!is_license_token_current(&token, 101));
    }

    #[test]
    #[serial]
    fn appauth_verifier_rejects_missing_public_key() {
        temp_env::with_var(rustfs_config::ENV_RUSTFS_LICENSE_PUBLIC_KEY, None::<&str>, || {
            assert_license_public_key_error(AppAuthLicenseVerifier.validate("signed-license", 0));
        });
    }

    #[test]
    #[serial]
    fn appauth_verifier_rejects_blank_public_key() {
        temp_env::with_var(rustfs_config::ENV_RUSTFS_LICENSE_PUBLIC_KEY, Some("  \t\n  "), || {
            assert_license_public_key_error(AppAuthLicenseVerifier.validate("signed-license", 0));
        });
    }

    #[test]
    #[serial]
    fn appauth_verifier_accepts_signed_license_with_trimmed_public_key() {
        let mut rng = rand::rng();
        let private_key = RsaPrivateKey::new(&mut rng, 2048).expect("private key should be generated");
        let public_key = RsaPublicKey::from(&private_key);
        let private_key_pem = private_key.to_pkcs8_pem(LineEnding::LF).expect("private key should encode");
        let public_key_pem = public_key
            .to_public_key_pem(LineEnding::LF)
            .expect("public key should encode");
        let expected = Token {
            name: "test_app".to_string(),
            expired: 100,
        };
        let signed_license = sign_license_token(&expected, &private_key_pem).expect("license should sign");
        let public_key_env = format!(" \n{public_key_pem}\t ");

        let actual = temp_env::with_var(rustfs_config::ENV_RUSTFS_LICENSE_PUBLIC_KEY, Some(public_key_env), || {
            AppAuthLicenseVerifier.validate(&signed_license, 0)
        })
        .expect("signed license should validate with env public key");

        assert_eq!(expected.name, actual.name);
        assert_eq!(expected.expired, actual.expired);
    }

    fn assert_license_public_key_error(result: LicenseResult<Token>) {
        let err = result.expect_err("license verification should fail without a public key");
        let LicenseError::Invalid(message) = err else {
            panic!("expected invalid license error, got {err:?}");
        };

        assert!(message.contains(rustfs_config::ENV_RUSTFS_LICENSE_PUBLIC_KEY));
        assert!(message.contains("RSA public key"));
    }
}
