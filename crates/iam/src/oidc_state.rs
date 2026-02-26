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

use moka::future::Cache;
use std::time::Duration;
use tracing::warn;

const OIDC_STATE_CAPACITY: u64 = 10_000;
const OIDC_STATE_CAPACITY_WARNING: u64 = 9_000;
const OIDC_STATE_CAPACITY_CRITICAL: u64 = 10_000;

/// Stores the PKCE verifier and nonce for an in-flight OIDC authorization flow.
#[derive(Debug, Clone)]
pub struct OidcAuthSession {
    pub provider_id: String,
    pub pkce_verifier: String,
    pub nonce: String,
    pub redirect_after: Option<String>,
}

/// TTL cache for OIDC auth state (PKCE verifiers + nonces) during the authorization flow.
/// Entries expire after 5 minutes and are single-use (removed on retrieval).
#[derive(Clone)]
pub struct OidcStateStore {
    cache: Cache<String, OidcAuthSession>,
}

impl OidcStateStore {
    pub fn new() -> Self {
        let cache = Cache::builder()
            .max_capacity(OIDC_STATE_CAPACITY)
            .time_to_live(Duration::from_secs(300)) // 5 minute TTL
            .build();
        Self { cache }
    }

    /// Store a new auth session keyed by the OAuth2 `state` parameter.
    pub async fn insert(&self, state: String, session: OidcAuthSession) {
        self.cache.insert(state, session).await;
        self.cache.run_pending_tasks().await;
        let size = self.cache.entry_count();
        if size >= OIDC_STATE_CAPACITY_CRITICAL {
            warn!("OIDC state store reached configured capacity ({size}/{OIDC_STATE_CAPACITY})");
        } else if size >= OIDC_STATE_CAPACITY_WARNING {
            warn!("OIDC state store approaching capacity ({size}/{OIDC_STATE_CAPACITY})");
        }
    }

    /// Retrieve and remove an auth session (single-use). Returns None if expired or not found.
    /// Uses `remove` which returns the value and removes it in a single operation.
    pub async fn take(&self, state: &str) -> Option<OidcAuthSession> {
        self.cache.remove(state).await
    }

    /// Check if a state key exists (without consuming it).
    pub async fn contains(&self, state: &str) -> bool {
        self.cache.get(state).await.is_some()
    }
}

impl Default for OidcStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_state_store_insert_and_take() {
        let store = OidcStateStore::new();
        let session = OidcAuthSession {
            provider_id: "okta".to_string(),
            pkce_verifier: "verifier123".to_string(),
            nonce: "nonce456".to_string(),
            redirect_after: Some("/dashboard".to_string()),
        };

        store.insert("state_abc".to_string(), session.clone()).await;
        assert!(store.contains("state_abc").await);

        let retrieved = store.take("state_abc").await;
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.provider_id, "okta");
        assert_eq!(retrieved.pkce_verifier, "verifier123");
        assert_eq!(retrieved.nonce, "nonce456");
        assert_eq!(retrieved.redirect_after, Some("/dashboard".to_string()));

        // Single-use: second take should return None
        assert!(store.take("state_abc").await.is_none());
    }

    #[tokio::test]
    async fn test_state_store_missing_key() {
        let store = OidcStateStore::new();
        assert!(store.take("nonexistent").await.is_none());
        assert!(!store.contains("nonexistent").await);
    }

    #[tokio::test]
    async fn test_state_store_multiple_entries() {
        let store = OidcStateStore::new();

        for i in 0..5 {
            let session = OidcAuthSession {
                provider_id: format!("provider_{i}"),
                pkce_verifier: format!("verifier_{i}"),
                nonce: format!("nonce_{i}"),
                redirect_after: None,
            };
            store.insert(format!("state_{i}"), session).await;
        }

        for i in 0..5 {
            let retrieved = store.take(&format!("state_{i}")).await;
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().provider_id, format!("provider_{i}"));
        }

        // All consumed
        for i in 0..5 {
            assert!(store.take(&format!("state_{i}")).await.is_none());
        }
    }
}
