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

mod credentials;

pub use credentials::*;

use rustfs_credentials::Credentials;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct UserIdentity {
    pub version: i64,
    pub credentials: Credentials,
    pub update_at: Option<OffsetDateTime>,
}

impl UserIdentity {
    /// Create a new UserIdentity
    ///
    /// # Arguments
    /// * `credentials` - Credentials object
    ///
    /// # Returns
    /// * UserIdentity
    pub fn new(credentials: Credentials) -> Self {
        UserIdentity {
            version: 1,
            credentials,
            update_at: Some(OffsetDateTime::now_utc()),
        }
    }

    /// Add an SSH public key to user identity for SFTP authentication
    pub fn add_ssh_public_key(&mut self, public_key: &str) {
        self.credentials
            .claims
            .get_or_insert_with(HashMap::new)
            .insert("ssh_public_keys".to_string(), json!([public_key]));
    }

    /// Get all SSH public keys for user identity
    pub fn get_ssh_public_keys(&self) -> Vec<String> {
        self.credentials
            .claims
            .as_ref()
            .and_then(|claims| claims.get("ssh_public_keys"))
            .and_then(|keys| keys.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).map(String::from).collect())
            .unwrap_or_default()
    }
}

impl From<Credentials> for UserIdentity {
    fn from(value: Credentials) -> Self {
        UserIdentity {
            version: 1,
            credentials: value,
            update_at: Some(OffsetDateTime::now_utc()),
        }
    }
}
