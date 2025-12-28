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

use crate::{DEFAULT_SECRET_KEY, ENV_GRPC_AUTH_TOKEN, IAM_POLICY_CLAIM_NAME_SA, INHERITED_POLICY_TYPE};
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::io::Error;
use std::sync::OnceLock;
use time::OffsetDateTime;

/// Global active credentials
static GLOBAL_ACTIVE_CRED: OnceLock<Credentials> = OnceLock::new();

/// Global gRPC authentication token
static GLOBAL_GRPC_AUTH_TOKEN: OnceLock<String> = OnceLock::new();

/// Initialize the global action credentials
///
/// # Arguments
/// * `ak` - Optional access key
/// * `sk` - Optional secret key
///
/// # Returns
/// * `Result<(), Box<Credentials>>` - Ok if successful, Err with existing credentials if already initialized
///
/// # Panics
/// This function panics if automatic credential generation fails when `ak` or `sk`
/// are `None`, for example if the random number generator fails while calling
/// `gen_access_key` or `gen_secret_key`.
pub fn init_global_action_credentials(ak: Option<String>, sk: Option<String>) -> Result<(), Box<Credentials>> {
    let ak = ak.unwrap_or_else(|| gen_access_key(20).expect("Failed to generate access key"));
    let sk = sk.unwrap_or_else(|| gen_secret_key(32).expect("Failed to generate secret key"));

    let cred = Credentials {
        access_key: ak,
        secret_key: sk,
        ..Default::default()
    };

    GLOBAL_ACTIVE_CRED.set(cred).map_err(|e| {
        Box::new(Credentials {
            access_key: e.access_key.clone(),
            ..Default::default()
        })
    })
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

    let encoded = URL_SAFE_NO_PAD.encode_to_string(&key);
    let key_str = encoded.replace("/", "+");

    Ok(key_str)
}

/// Get the gRPC authentication token from environment variable
///
/// # Returns
/// * `String` - The gRPC authentication token
///
pub fn get_grpc_token() -> String {
    GLOBAL_GRPC_AUTH_TOKEN
        .get_or_init(|| {
            env::var(ENV_GRPC_AUTH_TOKEN)
                .unwrap_or_else(|_| get_global_secret_key_opt().unwrap_or_else(|| DEFAULT_SECRET_KEY.to_string()))
        })
        .clone()
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
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: String,
    pub expiration: Option<OffsetDateTime>,
    pub status: String,
    pub parent_user: String,
    pub groups: Option<Vec<String>>,
    pub claims: Option<HashMap<String, Value>>,
    pub name: Option<String>,
    pub description: Option<String>,
}

impl Credentials {
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
            init_global_action_credentials(Some(test_ak.clone()), Some(test_sk.clone())).ok();
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
}
