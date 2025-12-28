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

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::OnceLock;
use time::OffsetDateTime;

const IAM_POLICY_CLAIM_NAME_SA: &str = "sa-policy";
const INHERITED_POLICY_TYPE: &str = "inherited-policy";

/// Global active credentials
static GLOBAL_ACTIVE_CRED: OnceLock<Credentials> = OnceLock::new();

/// Initialize the global action credentials
///
/// # Arguments
/// * `ak` - Optional access key
/// * `sk` - Optional secret key
///
/// # Returns
/// * None
///
pub fn init_global_action_credentials(ak: Option<String>, sk: Option<String>) {
    let ak = {
        if let Some(k) = ak {
            k
        } else {
            rustfs_utils::string::gen_access_key(20).unwrap_or_default()
        }
    };

    let sk = {
        if let Some(k) = sk {
            k
        } else {
            rustfs_utils::string::gen_secret_key(32).unwrap_or_default()
        }
    };

    GLOBAL_ACTIVE_CRED
        .set(Credentials {
            access_key: ak,
            secret_key: sk,
            ..Default::default()
        })
        .unwrap();
}

/// Get the global action credentials
pub fn get_global_action_cred() -> Option<Credentials> {
    GLOBAL_ACTIVE_CRED.get().cloned()
}

/// Get the global access key
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
pub fn get_global_access_ke() -> String {
    GLOBAL_ACTIVE_CRED
        .get()
        .map(|cred| cred.access_key.clone())
        .unwrap_or_default()
}

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
