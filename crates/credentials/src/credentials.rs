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

use crate::{DEFAULT_SECRET_KEY, ENV_RPC_SECRET, IAM_POLICY_CLAIM_NAME_SA, INHERITED_POLICY_TYPE};
use rand::{Rng, RngExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::fmt;
use std::io::Error;
use std::sync::{LazyLock, OnceLock};
use time::OffsetDateTime;

/// Global active credentials
static GLOBAL_ACTIVE_CRED: OnceLock<Credentials> = OnceLock::new();

/// Global RPC authentication token
pub static GLOBAL_RUSTFS_RPC_SECRET: OnceLock<String> = OnceLock::new();

/// Public error returned when RPC authentication is not safely configured.
pub const RPC_SECRET_REQUIRED_MESSAGE: &str = "RPC authentication secret is not configured";

/// Operator-facing guidance for configuring RPC authentication safely.
pub const RPC_SECRET_REQUIRED_OPERATOR_MESSAGE: &str = "RUSTFS_RPC_SECRET must be set to a non-default value or RUSTFS_SECRET_KEY must be changed from the default for RPC authentication";

/// Error type for credentials operations
#[derive(Debug)]
pub enum CredentialsError {
    /// Credentials already initialized
    AlreadyInitialized,
    /// Failed to generate access key
    AccessKeyGenerationFailed(Error),
    /// Failed to generate secret key
    SecretKeyGenerationFailed(Error),
}

impl fmt::Display for CredentialsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CredentialsError::AlreadyInitialized => write!(f, "Credentials already initialized"),
            CredentialsError::AccessKeyGenerationFailed(e) => write!(f, "Failed to generate access key: {}", e),
            CredentialsError::SecretKeyGenerationFailed(e) => write!(f, "Failed to generate secret key: {}", e),
        }
    }
}

impl std::error::Error for CredentialsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CredentialsError::AccessKeyGenerationFailed(e) => Some(e),
            CredentialsError::SecretKeyGenerationFailed(e) => Some(e),
            _ => None,
        }
    }
}

/// Initialize the global action credentials
///
/// # Arguments
/// * `ak` - Optional access key
/// * `sk` - Optional secret key
///
/// # Returns
/// * `Result<(), CredentialsError>` - Ok if successful, Err if already initialized or generation failed
pub fn init_global_action_credentials(ak: Option<String>, sk: Option<String>) -> Result<(), CredentialsError> {
    let ak = match ak {
        Some(k) => k,
        None => gen_access_key(20).map_err(CredentialsError::AccessKeyGenerationFailed)?,
    };

    let sk = match sk {
        Some(k) => k,
        None => gen_secret_key(32).map_err(CredentialsError::SecretKeyGenerationFailed)?,
    };

    let cred = Credentials {
        access_key: ak,
        secret_key: sk,
        ..Default::default()
    };

    GLOBAL_ACTIVE_CRED.set(cred).map_err(|_| CredentialsError::AlreadyInitialized)
}

/// Get the global action credentials
pub fn get_global_action_cred() -> Option<Credentials> {
    GLOBAL_ACTIVE_CRED.get().cloned()
}

/// Get the global secret key
///
/// # Returns
/// * `Option<String>` - The global secret key, if set
///
pub fn get_global_secret_key_opt() -> Option<String> {
    GLOBAL_ACTIVE_CRED.get().map(|cred| cred.secret_key.clone())
}

/// Get the global secret key
///
/// # Returns
/// * `String` - The global secret key, or empty string if not set
///
pub fn get_global_secret_key() -> String {
    GLOBAL_ACTIVE_CRED
        .get()
        .map(|cred| cred.secret_key.clone())
        .unwrap_or_default()
}

/// Get the global access key
///
/// # Returns
/// * `Option<String>` - The global access key, if set
///
pub fn get_global_access_key_opt() -> Option<String> {
    GLOBAL_ACTIVE_CRED.get().map(|cred| cred.access_key.clone())
}

/// Get the global access key
///
/// # Returns
/// * `String` - The global access key, or empty string if not set
///
pub fn get_global_access_key() -> String {
    GLOBAL_ACTIVE_CRED
        .get()
        .map(|cred| cred.access_key.clone())
        .unwrap_or_default()
}

/// Generates a random access key of the specified length.
///
/// # Arguments
/// * `length` - The length of the access key to generate
///
/// # Returns
/// * `Result<String>` - A result containing the generated access key or an error if the length is too short
///
/// # Errors
/// This function will return an error if the specified length is less than 3.
///
/// Examples
/// ```no_run
/// use rustfs_credentials::gen_access_key;
///
/// let access_key = gen_access_key(16).unwrap();
/// println!("Generated access key: {}", access_key);
/// ```
///
pub fn gen_access_key(length: usize) -> std::io::Result<String> {
    const ALPHA_NUMERIC_TABLE: [char; 36] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
        'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    ];

    if length < 3 {
        return Err(Error::other("access key length is too short"));
    }

    let mut result = String::with_capacity(length);
    let mut rng = rand::rng();

    for _ in 0..length {
        result.push(ALPHA_NUMERIC_TABLE[rng.random_range(0..ALPHA_NUMERIC_TABLE.len())]);
    }

    Ok(result)
}

/// Generates a random secret key of the specified length.
///
/// # Arguments
/// * `length` - The length of the secret key to generate
///
/// # Returns
/// * `Result<String>` - A result containing the generated secret key or an error if the length is too short
///
/// # Errors
/// This function will return an error if the specified length is less than 8.
///
/// # Examples
/// ```no_run
/// use rustfs_credentials::gen_secret_key;
///
/// let secret_key = gen_secret_key(32).unwrap();
/// println!("Generated secret key: {}", secret_key);
/// ```
///
pub fn gen_secret_key(length: usize) -> std::io::Result<String> {
    use base64_simd::URL_SAFE_NO_PAD;

    if length < 8 {
        return Err(Error::other("secret key length is too short"));
    }
    let mut rng = rand::rng();

    let mut key = vec![0u8; URL_SAFE_NO_PAD.estimated_decoded_length(length)];
    rng.fill_bytes(&mut key);

    // URL_SAFE_NO_PAD uses "-" and "_" instead of "+" and "/", so "/" never
    // appears in the output. The .replace("/", "+") was a dead no-op.
    let encoded = URL_SAFE_NO_PAD.encode_to_string(&key);

    Ok(encoded)
}

/// Get the RPC authentication token from environment variable
///
/// # Returns
/// * `String` - The RPC authentication token
///
fn resolve_rpc_secret(env_secret: Option<&str>, global_secret: Option<&str>) -> Option<String> {
    if let Some(secret) = env_secret.map(str::trim).filter(|secret| !secret.is_empty()) {
        return (secret != DEFAULT_SECRET_KEY).then(|| secret.to_string());
    }

    global_secret
        .map(str::trim)
        .filter(|secret| !secret.is_empty() && *secret != DEFAULT_SECRET_KEY)
        .map(ToOwned::to_owned)
}

pub fn try_get_rpc_token() -> std::io::Result<String> {
    if let Some(secret) = GLOBAL_RUSTFS_RPC_SECRET.get() {
        return resolve_rpc_secret(None, Some(secret)).ok_or_else(|| Error::other(RPC_SECRET_REQUIRED_MESSAGE));
    }

    let env_secret = env::var(ENV_RPC_SECRET).ok();
    let global_secret = get_global_secret_key_opt();
    let secret = resolve_rpc_secret(env_secret.as_deref(), global_secret.as_deref())
        .ok_or_else(|| Error::other(RPC_SECRET_REQUIRED_MESSAGE))?;

    match GLOBAL_RUSTFS_RPC_SECRET.set(secret.clone()) {
        Ok(()) => Ok(secret),
        Err(_) => GLOBAL_RUSTFS_RPC_SECRET
            .get()
            .and_then(|stored| resolve_rpc_secret(None, Some(stored)))
            .ok_or_else(|| Error::other(RPC_SECRET_REQUIRED_MESSAGE)),
    }
}

#[deprecated(note = "use try_get_rpc_token to handle missing RPC secrets explicitly")]
pub fn get_rpc_token() -> String {
    try_get_rpc_token().expect(RPC_SECRET_REQUIRED_MESSAGE)
}

/// A wrapper struct for masking sensitive strings in Debug implementations.
/// It avoids allocating a new String just for formatting.
pub struct Masked<'a>(pub Option<&'a str>);

impl<'a> fmt::Debug for Masked<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => Ok(()),
            Some(s) => {
                let len = s.chars().count();
                if len == 0 {
                    Ok(())
                } else if len == 1 {
                    write!(f, "***")
                } else if len == 2 {
                    let first = s.chars().next().ok_or(fmt::Error)?;
                    write!(f, "{}***|{}", first, len)
                } else {
                    let first = s.chars().next().ok_or(fmt::Error)?;
                    let last = s.chars().last().ok_or(fmt::Error)?;
                    write!(f, "{}***{}|{}", first, last, len)
                }
            }
        }
    }
}

impl<'a> fmt::Display for Masked<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// Credentials structure
///
/// Fields:
/// - access_key: Access key string
/// - secret_key: Secret key string
/// - session_token: Session token string
/// - expiration: Optional expiration time as OffsetDateTime
/// - status: Status string (e.g., "active", "off")
/// - parent_user: Parent user string
/// - groups: Optional list of groups
/// - claims: Optional map of claims
/// - name: Optional name string
/// - description: Optional description string
///
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Credentials {
    #[serde(rename = "accessKey", alias = "access_key", default)]
    pub access_key: String,
    #[serde(rename = "secretKey", alias = "secret_key", default)]
    pub secret_key: String,
    #[serde(rename = "sessionToken", alias = "session_token", default)]
    pub session_token: String,
    #[serde(default, with = "crate::serde_datetime::option")]
    pub expiration: Option<OffsetDateTime>,
    #[serde(default)]
    pub status: String,
    #[serde(rename = "parentUser", alias = "parent_user", default)]
    pub parent_user: String,
    #[serde(default)]
    pub groups: Option<Vec<String>>,
    #[serde(default)]
    pub claims: Option<HashMap<String, Value>>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &Masked(Some(&self.secret_key)))
            .field("session_token", &Masked(Some(&self.session_token)))
            .field("expiration", &self.expiration)
            .field("status", &self.status)
            .field("parent_user", &self.parent_user)
            .field("groups", &self.groups)
            .field("claims", &self.claims)
            .field("name", &self.name)
            .field("description", &self.description)
            .finish()
    }
}

impl Credentials {
    /// Returns a reference to this credential's claims, or a shared empty map
    /// when the credential has no claims attached. Avoids per-call allocation
    /// at call sites that need an `&HashMap<String, Value>`.
    pub fn claims_or_empty(&self) -> &HashMap<String, Value> {
        static EMPTY: LazyLock<HashMap<String, Value>> = LazyLock::new(HashMap::new);
        match &self.claims {
            Some(c) => c,
            None => &EMPTY,
        }
    }

    pub fn is_expired(&self) -> bool {
        if self.expiration.is_none() {
            return false;
        }

        self.expiration
            .as_ref()
            .map(|e| OffsetDateTime::now_utc() > *e)
            .unwrap_or(false)
    }

    pub fn is_temp(&self) -> bool {
        !self.session_token.is_empty() && !self.is_expired()
    }

    pub fn is_service_account(&self) -> bool {
        self.claims
            .as_ref()
            .map(|x| x.get(IAM_POLICY_CLAIM_NAME_SA).is_some_and(|_| !self.parent_user.is_empty()))
            .unwrap_or_default()
    }

    pub fn is_implied_policy(&self) -> bool {
        if self.is_service_account() {
            return self
                .claims
                .as_ref()
                .map(|x| x.get(IAM_POLICY_CLAIM_NAME_SA).is_some_and(|v| v == INHERITED_POLICY_TYPE))
                .unwrap_or_default();
        }

        false
    }

    pub fn is_valid(&self) -> bool {
        if self.status == "off" {
            return false;
        }

        self.access_key.len() >= 3 && self.secret_key.len() >= 8 && !self.is_expired()
    }

    pub fn is_owner(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IAM_POLICY_CLAIM_NAME_SA, INHERITED_POLICY_TYPE};
    use time::Duration;

    #[test]
    fn test_credentials_is_expired() {
        let mut cred = Credentials::default();
        assert!(!cred.is_expired());

        cred.expiration = Some(OffsetDateTime::now_utc() + Duration::hours(1));
        assert!(!cred.is_expired());

        cred.expiration = Some(OffsetDateTime::now_utc() - Duration::hours(1));
        assert!(cred.is_expired());
    }

    #[test]
    fn test_credentials_is_temp() {
        let mut cred = Credentials::default();
        assert!(!cred.is_temp());

        cred.session_token = "token".to_string();
        assert!(cred.is_temp());

        cred.expiration = Some(OffsetDateTime::now_utc() - Duration::hours(1));
        assert!(!cred.is_temp());
    }

    #[test]
    fn test_credentials_is_service_account() {
        let mut cred = Credentials::default();
        assert!(!cred.is_service_account());

        let mut claims = HashMap::new();
        claims.insert(IAM_POLICY_CLAIM_NAME_SA.to_string(), Value::String("policy".to_string()));
        cred.claims = Some(claims);
        cred.parent_user = "parent".to_string();
        assert!(cred.is_service_account());
    }

    #[test]
    fn test_credentials_is_implied_policy() {
        let mut cred = Credentials::default();
        assert!(!cred.is_implied_policy());

        let mut claims = HashMap::new();
        claims.insert(IAM_POLICY_CLAIM_NAME_SA.to_string(), Value::String(INHERITED_POLICY_TYPE.to_string()));
        cred.claims = Some(claims);
        cred.parent_user = "parent".to_string();
        assert!(cred.is_implied_policy());
    }

    #[test]
    fn test_credentials_is_valid() {
        let mut cred = Credentials::default();
        assert!(!cred.is_valid());

        cred.access_key = "abc".to_string();
        cred.secret_key = "12345678".to_string();
        assert!(cred.is_valid());

        cred.status = "off".to_string();
        assert!(!cred.is_valid());
    }

    #[test]
    fn test_credentials_is_owner() {
        let cred = Credentials::default();
        assert!(!cred.is_owner());
    }

    #[test]
    fn test_global_credentials_flow() {
        // Since OnceLock can only be set once, we put together all globally related tests
        // If it has already been initialized (possibly from other tests), we verify the results directly
        if get_global_action_cred().is_none() {
            // Verify that the initial state is empty
            assert!(get_global_access_key_opt().is_none());
            assert_eq!(get_global_access_key(), "");
            assert!(get_global_secret_key_opt().is_none());
            assert_eq!(get_global_secret_key(), "");

            // Initialize
            let test_ak = "test_access_key".to_string();
            let test_sk = "test_secret_key_123456".to_string();
            init_global_action_credentials(Some(test_ak), Some(test_sk)).ok();
        }

        // Verify the state after initialization
        let cred = get_global_action_cred().expect("Global credentials should be set");
        assert!(!cred.access_key.is_empty());
        assert!(!cred.secret_key.is_empty());

        assert!(get_global_access_key_opt().is_some());
        assert!(!get_global_access_key().is_empty());
        assert!(get_global_secret_key_opt().is_some());
        assert!(!get_global_secret_key().is_empty());
    }

    #[test]
    fn test_init_global_credentials_auto_gen() {
        // If it hasn't already been initialized, the test automatically generates logic
        if get_global_action_cred().is_none() {
            init_global_action_credentials(None, None).ok();
            let ak = get_global_access_key();
            let sk = get_global_secret_key();
            assert_eq!(ak.len(), 20);
            assert_eq!(sk.len(), 32);
        }
    }

    #[test]
    fn test_gen_secret_key_uses_url_safe_base64_without_padding() {
        let key = gen_secret_key(32).expect("secret key should generate");

        assert_eq!(key.len(), 32);
        assert!(!key.contains('/'));
        assert!(!key.contains('+'));
        assert!(!key.contains('='));
    }

    #[test]
    fn test_resolve_rpc_secret_rejects_default_fallback() {
        assert!(resolve_rpc_secret(None, None).is_none());
        assert!(resolve_rpc_secret(None, Some(DEFAULT_SECRET_KEY)).is_none());
        assert!(resolve_rpc_secret(Some(DEFAULT_SECRET_KEY), Some("custom-global-secret")).is_none());
    }

    #[test]
    fn test_rpc_secret_public_error_omits_configuration_details() {
        assert!(!RPC_SECRET_REQUIRED_MESSAGE.contains("RUSTFS_"));
        assert!(!RPC_SECRET_REQUIRED_MESSAGE.contains(DEFAULT_SECRET_KEY));
        assert!(RPC_SECRET_REQUIRED_OPERATOR_MESSAGE.contains("RUSTFS_RPC_SECRET"));
    }

    #[allow(deprecated)]
    #[test]
    fn test_get_rpc_token_preserves_string_return_type() {
        fn assert_string_return(_: fn() -> String) {}

        assert_string_return(get_rpc_token);
    }

    #[test]
    fn test_resolve_rpc_secret_accepts_non_default_secret() {
        assert_eq!(resolve_rpc_secret(Some("custom-rpc-secret"), None).as_deref(), Some("custom-rpc-secret"));
        assert_eq!(
            resolve_rpc_secret(None, Some("custom-global-secret")).as_deref(),
            Some("custom-global-secret")
        );
    }

    #[test]
    fn test_resolve_rpc_secret_trims_and_falls_back_from_blank_env() {
        assert_eq!(
            resolve_rpc_secret(Some("  custom-rpc-secret  "), None).as_deref(),
            Some("custom-rpc-secret")
        );
        assert_eq!(
            resolve_rpc_secret(Some("  "), Some("  custom-global-secret  ")).as_deref(),
            Some("custom-global-secret")
        );
        assert_eq!(
            resolve_rpc_secret(Some("  "), Some("custom-global-secret")).as_deref(),
            Some("custom-global-secret")
        );
        let padded_default_secret = format!("  {}  ", DEFAULT_SECRET_KEY);
        assert!(resolve_rpc_secret(Some(padded_default_secret.as_str()), Some("custom-global-secret")).is_none());
    }

    #[test]
    fn test_masked_debug() {
        // Test None
        assert_eq!(format!("{:?}", Masked(None)), "");

        // Test empty string
        assert_eq!(format!("{:?}", Masked(Some(""))), "");

        // Test length 1
        assert_eq!(format!("{:?}", Masked(Some("a"))), "***");

        // Test length 2
        assert_eq!(format!("{:?}", Masked(Some("ab"))), "a***|2");

        // Test length 3
        assert_eq!(format!("{:?}", Masked(Some("abc"))), "a***c|3");

        // Test length 4
        assert_eq!(format!("{:?}", Masked(Some("abcd"))), "a***d|4");

        // Test longer string
        assert_eq!(format!("{:?}", Masked(Some("secretpassword"))), "s***d|14");

        // Test Unicode input should not panic and should keep character boundary
        assert_eq!(format!("{:?}", Masked(Some("中"))), "***");
        assert_eq!(format!("{:?}", Masked(Some("中文"))), "中***|2");
        assert_eq!(format!("{:?}", Masked(Some("中文测试"))), "中***试|4");
    }

    #[test]
    fn test_credentials_debug_masks_sensitive_fields() {
        let cred = Credentials {
            access_key: "debug-access-key".to_string(),
            secret_key: "debug-secret-key".to_string(),
            session_token: "debug-session-token".to_string(),
            parent_user: "parent-user".to_string(),
            ..Default::default()
        };

        let output = format!("{cred:?}");

        assert!(output.contains("debug-access-key"));
        assert!(output.contains("parent-user"));
        assert!(!output.contains("debug-secret-key"));
        assert!(!output.contains("debug-session-token"));
    }

    #[test]
    fn test_credentials_expiration_serialize_as_rfc3339() {
        use time::OffsetDateTime;

        let c = Credentials {
            access_key: "ak".to_string(),
            secret_key: "sk12345678".to_string(),
            expiration: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };

        let json = serde_json::to_string(&c).expect("serialize");
        assert!(
            json.contains('T') && (json.contains('Z') || json.contains("+00:00")),
            "Credentials expiration should be RFC3339; got: {}",
            json
        );
    }

    #[test]
    fn test_credentials_deserialize_minio_style_rfc3339_expiration() {
        let minio_style = r#"{"accessKey":"ak","secretKey":"sk12345678","expiration":"2025-03-07T12:00:00Z"}"#;
        let c: Credentials = serde_json::from_str(minio_style).expect("deserialize");
        assert_eq!(c.access_key, "ak");
        assert!(c.expiration.is_some());
    }
}
