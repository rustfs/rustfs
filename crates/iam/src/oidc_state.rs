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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;

const OIDC_STATE_CAPACITY: u64 = 10_000;
const OIDC_STATE_CAPACITY_WARNING: u64 = 9_000;
const OIDC_STATE_CAPACITY_CRITICAL: u64 = 10_000;
const OIDC_STATE_CAPACITY_LOG_INTERVAL_SECS: u64 = 60;

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
    last_capacity_log_at: Arc<AtomicU64>,
}

impl OidcStateStore {
    pub fn new() -> Self {
        let cache = Cache::builder()
            .max_capacity(OIDC_STATE_CAPACITY)
            .time_to_live(Duration::from_secs(300)) // 5 minute TTL
            .build();
        Self {
            cache,
            last_capacity_log_at: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Store a new auth session keyed by the OAuth2 `state` parameter.
    pub async fn insert(&self, state: String, session: OidcAuthSession) {
        self.cache.insert(state, session).await;
        let size = self.cache.entry_count();

        if !Self::should_log_capacity_warning(&self.last_capacity_log_at) {
            return;
        }

        if size >= OIDC_STATE_CAPACITY_CRITICAL {
            self.cache.run_pending_tasks().await;
            warn!("OIDC state store reached configured capacity ({size}/{OIDC_STATE_CAPACITY})");
            return;
        }

        if size >= OIDC_STATE_CAPACITY_WARNING {
            self.cache.run_pending_tasks().await;
            warn!("OIDC state store approaching capacity ({size}/{OIDC_STATE_CAPACITY})");
        }
    }

    fn now_unix_secs() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
    }

    fn should_log_capacity_warning(last_log_at: &AtomicU64) -> bool {
        let now = Self::now_unix_secs();
        let last = last_log_at.load(Ordering::Acquire);
        if now.saturating_sub(last) < OIDC_STATE_CAPACITY_LOG_INTERVAL_SECS {
            return false;
        }
        last_log_at
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
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
