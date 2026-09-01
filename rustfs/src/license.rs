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

use rustfs_crypto::{Token, parse_license_with_public_key};
pub use rustfs_license::{
    LicenseError, LicenseMetadata, LicenseProvider, LicenseResult, LicenseStatus, SERVER_ENTITLEMENT, SharedLicenseProvider,
};
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, warn};

const LOG_COMPONENT_LICENSE: &str = "license";
const LOG_SUBSYSTEM_RUNTIME: &str = "runtime";
const EVENT_LICENSE_INITIALIZATION_FAILED: &str = "license_initialization_failed";

pub type SharedLicenseVerifier = Arc<dyn LicenseVerifier>;

#[derive(Clone, Debug, Default)]
struct LicenseState {
    token: Option<Token>,
    status: LicenseStatus,
}

/// Lower-level verifier used by the default open-source provider.
///
/// OEM integrations that own entitlement decisions should install a
/// [`LicenseProvider`] instead.
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
static LICENSE_PROVIDER: OnceLock<SharedLicenseProvider> = OnceLock::new();

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

#[cfg(test)]
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
        LicenseError::Expired { expired_at, now } => format!("expired at {expired_at}, now {now}"),
        LicenseError::Clock(message) => format!("system clock error: {message}"),
        LicenseError::Missing => "license is required".to_string(),
        LicenseError::StatePoisoned => "license state is unavailable".to_string(),
        LicenseError::Denied { entitlement } => format!("entitlement is not granted: {entitlement}"),
        LicenseError::Unavailable(message) => format!("provider is unavailable: {message}"),
        LicenseError::InvalidEntitlement => "entitlement identifier is invalid".to_string(),
    });
}

fn apply_valid_status(state: &mut LicenseState, token: Token) {
    state.token = Some(token);
    state.status = LicenseStatus::Valid;
}

#[derive(Debug, Default)]
struct OpenSourceLicenseProvider;

impl LicenseProvider for OpenSourceLicenseProvider {
    fn initialize(&self, raw_license: Option<&str>) -> LicenseResult<()> {
        let normalized = raw_license.map(str::trim).filter(|raw| !raw.is_empty());

        let Some(raw_license) = normalized else {
            let mut state = license_state().write().map_err(|_| LicenseError::StatePoisoned)?;
            apply_missing_status(&mut state);
            return if state.status == LicenseStatus::Missing {
                Err(LicenseError::Missing)
            } else {
                Ok(())
            };
        };

        let now = now_epoch_secs()?;
        let result = license_verifier().validate(raw_license, now);
        let mut state = license_state().write().map_err(|_| LicenseError::StatePoisoned)?;
        match result {
            Ok(token) => {
                apply_valid_status(&mut state, token);
                Ok(())
            }
            Err(err) => {
                apply_invalid_status(&mut state, err.clone());
                Err(err)
            }
        }
    }

    fn check(&self, _entitlement: &str) -> LicenseResult<()> {
        #[cfg(not(feature = "license"))]
        return Ok(());

        #[cfg(feature = "license")]
        {
            let state = license_state().read().map_err(|_| LicenseError::StatePoisoned)?;
            match &state.status {
                LicenseStatus::Missing => return Err(LicenseError::Missing),
                LicenseStatus::Invalid(message) => return Err(LicenseError::Invalid(message.clone())),
                LicenseStatus::Unavailable => {
                    return Err(LicenseError::Unavailable("license state is unavailable".to_string()));
                }
                LicenseStatus::Uninitialized | LicenseStatus::Valid => {}
            }

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
    }

    fn status(&self) -> LicenseStatus {
        license_state()
            .read()
            .map(|state| state.status.clone())
            .unwrap_or(LicenseStatus::Unavailable)
    }

    fn metadata(&self) -> Option<LicenseMetadata> {
        license_state().read().ok().and_then(|state| {
            state.token.as_ref().map(|token| LicenseMetadata {
                subject: token.name.clone(),
                expires_at: Some(token.expired),
            })
        })
    }
}

fn default_license_provider() -> SharedLicenseProvider {
    Arc::new(OpenSourceLicenseProvider)
}

fn license_provider() -> &'static SharedLicenseProvider {
    LICENSE_PROVIDER.get_or_init(default_license_provider)
}

/// Replace the verifier used by the default open-source provider.
///
/// Returns `false` if the verifier was already initialized.
pub fn set_license_verifier(verifier: SharedLicenseVerifier) -> bool {
    LICENSE_VERIFIER.set(verifier).is_ok()
}

/// Replace the global license provider before any other license API is used.
///
/// Returns `false` if the provider was already initialized.
pub fn set_license_provider(provider: SharedLicenseProvider) -> bool {
    LICENSE_PROVIDER.set(provider).is_ok()
}

/// Initialize the license in memory.
///
/// This keeps the default API signature stable and is safe to call multiple times.
pub fn initialize_license(raw_license: Option<String>) {
    if let Err(err) = initialize_license_result(raw_license) {
        match err {
            LicenseError::Missing
            | LicenseError::Invalid(_)
            | LicenseError::Expired { .. }
            | LicenseError::Denied { .. }
            | LicenseError::InvalidEntitlement => {
                warn!(
                    event = EVENT_LICENSE_INITIALIZATION_FAILED,
                    component = LOG_COMPONENT_LICENSE,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    error = %err,
                    "License initialization failed"
                );
            }
            LicenseError::StatePoisoned | LicenseError::Clock(_) | LicenseError::Unavailable(_) => {
                error!(
                    event = EVENT_LICENSE_INITIALIZATION_FAILED,
                    component = LOG_COMPONENT_LICENSE,
                    subsystem = LOG_SUBSYSTEM_RUNTIME,
                    error = %err,
                    "License initialization failed"
                );
            }
        }
    }
}

/// Explicit initialization API with typed error return.
pub fn initialize_license_result(raw_license: Option<String>) -> LicenseResult<()> {
    let normalized = normalized_license(raw_license);
    license_provider().initialize(normalized.as_deref())
}

/// Legacy name kept for existing startup code.
pub fn init_license(license: Option<String>) {
    initialize_license(license);
}

/// Return the current license information.
pub fn get_license() -> Option<Token> {
    let metadata = license_provider().metadata()?;
    Some(Token {
        name: metadata.subject,
        expired: metadata.expires_at?,
    })
}

/// New name for compatibility with external integrations.
pub fn current_license() -> Option<Token> {
    get_license()
}

/// Return whether the loaded license token is present and not expired.
pub fn has_valid_license() -> bool {
    let provider = license_provider();
    if provider.status() != LicenseStatus::Valid || provider.check(SERVER_ENTITLEMENT).is_err() {
        return false;
    }

    match provider.metadata().and_then(|metadata| metadata.expires_at) {
        Some(expires_at) => now_epoch_secs().is_ok_and(|now| expires_at > now),
        None => true,
    }
}

/// Observe the current license status for observability.
pub fn license_status() -> String {
    license_provider().status().to_string()
}

/// Check a normalized entitlement identifier against the active provider.
pub fn check_entitlement(entitlement: &str) -> LicenseResult<()> {
    if entitlement.is_empty() || entitlement.trim() != entitlement {
        return Err(LicenseError::InvalidEntitlement);
    }

    license_provider().check(entitlement)
}

/// Check whether the existing server-wide license entitlement is granted.
pub fn ensure_license() -> LicenseResult<()> {
    check_entitlement(SERVER_ENTITLEMENT)
}

fn license_error_into_io(err: LicenseError) -> Error {
    match err {
        LicenseError::StatePoisoned | LicenseError::Clock(_) | LicenseError::Unavailable(_) => Error::other(err.to_string()),
        LicenseError::Missing
        | LicenseError::Invalid(_)
        | LicenseError::Expired { .. }
        | LicenseError::Denied { .. }
        | LicenseError::InvalidEntitlement => Error::new(ErrorKind::PermissionDenied, err.to_string()),
    }
}

/// Compatibility API for call-sites that still use the legacy name.
pub fn license_check() -> Result<()> {
    ensure_license().map_err(license_error_into_io)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::{
        RsaPrivateKey, RsaPublicKey,
        pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding},
    };
    use rustfs_crypto::sign_license_token;
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
    fn entitlement_identifier_must_be_normalized() {
        assert_eq!(check_entitlement(""), Err(LicenseError::InvalidEntitlement));
        assert_eq!(check_entitlement(" rustfs.server"), Err(LicenseError::InvalidEntitlement));
        assert_eq!(check_entitlement("rustfs.server "), Err(LicenseError::InvalidEntitlement));
    }

    #[test]
    fn provider_errors_keep_legacy_io_error_kinds() {
        let missing = license_error_into_io(LicenseError::Missing);
        let unavailable = license_error_into_io(LicenseError::Unavailable("runtime failure".to_string()));

        assert_eq!(missing.kind(), ErrorKind::PermissionDenied);
        assert_eq!(unavailable.kind(), ErrorKind::Other);
    }

    #[test]
    #[serial]
    #[cfg(not(feature = "license"))]
    fn open_source_provider_accepts_missing_license() {
        let provider = OpenSourceLicenseProvider;

        assert_eq!(provider.initialize(None), Ok(()));
        assert_eq!(provider.status(), LicenseStatus::Uninitialized);
        assert_eq!(provider.check(SERVER_ENTITLEMENT), Ok(()));
    }

    #[test]
    #[serial]
    #[cfg(feature = "license")]
    fn strict_provider_rejects_missing_license() {
        let provider = OpenSourceLicenseProvider;

        assert_eq!(provider.initialize(None), Err(LicenseError::Missing));
        assert_eq!(provider.status(), LicenseStatus::Missing);
        assert_eq!(provider.check(SERVER_ENTITLEMENT), Err(LicenseError::Missing));
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
