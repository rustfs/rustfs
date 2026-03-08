// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Container Synchronization Support for Swift API
//!
//! This module implements bidirectional container synchronization between
//! Swift clusters, enabling disaster recovery, geo-replication, and
//! multi-region deployments.
//!
//! # Configuration
//!
//! Container sync is configured via container metadata:
//!
//! ```bash
//! # Set sync target and key
//! swift post my-container \
//!   -H "X-Container-Sync-To: https://remote-swift.example.com/v1/AUTH_remote/backup-container" \
//!   -H "X-Container-Sync-Key: mysecretkey123"
//! ```
//!
//! # Sync Process
//!
//! 1. Background worker periodically scans containers with sync configuration
//! 2. Compares local objects with remote container
//! 3. Syncs new/updated objects to remote
//! 4. Uses timestamp-based conflict resolution
//! 5. Retries failed syncs with exponential backoff
//!
//! # Conflict Resolution
//!
//! - **Last-Write-Wins**: Most recent timestamp wins
//! - **Bidirectional**: Both clusters can accept writes
//! - **Eventual Consistency**: Objects converge to same state
//!
//! # Security
//!
//! - Shared secret key (X-Container-Sync-Key) for authentication
//! - HTTPS recommended for remote connections
//! - Per-container isolation

use super::{SwiftError, SwiftResult};
use std::collections::HashMap;
use tracing::{debug, warn};

/// Container sync configuration
#[derive(Debug, Clone, PartialEq)]
pub struct SyncConfig {
    /// Target Swift URL (e.g., "https://remote/v1/AUTH_account/container")
    pub sync_to: String,

    /// Shared secret key for authentication
    pub sync_key: String,

    /// Enable/disable sync
    pub enabled: bool,
}

impl SyncConfig {
    /// Parse sync configuration from container metadata
    pub fn from_metadata(metadata: &HashMap<String, String>) -> SwiftResult<Option<Self>> {
        // Check for sync target
        let sync_to = match metadata.get("x-container-sync-to") {
            Some(url) if !url.is_empty() => url.clone(),
            _ => return Ok(None),
        };

        // Get sync key
        let sync_key = metadata
            .get("x-container-sync-key")
            .ok_or_else(|| SwiftError::BadRequest("X-Container-Sync-Key required when X-Container-Sync-To is set".to_string()))?
            .clone();

        if sync_key.is_empty() {
            return Err(SwiftError::BadRequest("X-Container-Sync-Key cannot be empty".to_string()));
        }

        Ok(Some(SyncConfig {
            sync_to,
            sync_key,
            enabled: true,
        }))
    }

    /// Convert to container metadata headers
    pub fn to_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert("x-container-sync-to".to_string(), self.sync_to.clone());
        metadata.insert("x-container-sync-key".to_string(), self.sync_key.clone());
        metadata
    }

    /// Validate sync target URL
    pub fn validate(&self) -> SwiftResult<()> {
        // Parse URL to ensure it's valid
        if !self.sync_to.starts_with("http://") && !self.sync_to.starts_with("https://") {
            return Err(SwiftError::BadRequest("X-Container-Sync-To must be a valid HTTP(S) URL".to_string()));
        }

        // Warn if using HTTP instead of HTTPS
        if self.sync_to.starts_with("http://") {
            warn!("Container sync using unencrypted HTTP - consider using HTTPS");
        }

        // Validate key length (recommend at least 16 characters)
        if self.sync_key.len() < 16 {
            warn!("Container sync key is short (<16 chars) - recommend longer key");
        }

        Ok(())
    }
}

/// Sync status for a container
#[derive(Debug, Clone)]
pub struct SyncStatus {
    /// Last successful sync timestamp (Unix seconds)
    pub last_sync: Option<u64>,

    /// Number of objects successfully synced
    pub objects_synced: u64,

    /// Number of sync failures
    pub sync_failures: u64,

    /// Last sync error message
    pub last_error: Option<String>,

    /// Objects currently in sync queue
    pub queue_size: u64,
}

impl SyncStatus {
    /// Create new empty sync status
    pub fn new() -> Self {
        SyncStatus {
            last_sync: None,
            objects_synced: 0,
            sync_failures: 0,
            last_error: None,
            queue_size: 0,
        }
    }

    /// Record successful sync
    pub fn record_success(&mut self, timestamp: u64, objects_count: u64) {
        self.last_sync = Some(timestamp);
        self.objects_synced += objects_count;
        self.last_error = None;
    }

    /// Record sync failure
    pub fn record_failure(&mut self, error_msg: String) {
        self.sync_failures += 1;
        self.last_error = Some(error_msg);
    }
}

impl Default for SyncStatus {
    fn default() -> Self {
        Self::new()
    }
}

/// Sync queue entry for an object that needs syncing
#[derive(Debug, Clone)]
pub struct SyncQueueEntry {
    /// Object name
    pub object: String,

    /// Object ETag for change detection
    pub etag: String,

    /// Last modified timestamp
    pub last_modified: u64,

    /// Retry count
    pub retry_count: u32,

    /// Next retry time (Unix seconds)
    pub next_retry: u64,
}

impl SyncQueueEntry {
    /// Create new sync queue entry
    pub fn new(object: String, etag: String, last_modified: u64) -> Self {
        SyncQueueEntry {
            object,
            etag,
            last_modified,
            retry_count: 0,
            next_retry: 0,
        }
    }

    /// Calculate next retry time with exponential backoff
    pub fn schedule_retry(&mut self, current_time: u64) {
        self.retry_count += 1;

        // Exponential backoff: 1m, 2m, 4m, 8m, 16m, max 1 hour
        let backoff_seconds = std::cmp::min(60 * (1 << (self.retry_count - 1)), 3600);
        self.next_retry = current_time + backoff_seconds;

        debug!("Scheduled retry #{} for '{}' at +{}s", self.retry_count, self.object, backoff_seconds);
    }

    /// Check if ready for retry
    pub fn ready_for_retry(&self, current_time: u64) -> bool {
        current_time >= self.next_retry
    }

    /// Check if max retries exceeded
    pub fn max_retries_exceeded(&self) -> bool {
        self.retry_count >= 10 // Max 10 retries
    }
}

/// Conflict resolution strategy
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictResolution {
    /// Use object with most recent timestamp
    LastWriteWins,
    /// Always prefer local object
    LocalWins,
    /// Always prefer remote object
    RemoteWins,
}

/// Compare timestamps for conflict resolution
pub fn resolve_conflict(local_timestamp: u64, remote_timestamp: u64, strategy: ConflictResolution) -> bool {
    match strategy {
        ConflictResolution::LastWriteWins => local_timestamp >= remote_timestamp,
        ConflictResolution::LocalWins => true,
        ConflictResolution::RemoteWins => false,
    }
}

/// Extract container name from sync target URL
///
/// Example: "https://remote/v1/AUTH_account/container" -> "container"
pub fn extract_target_container(sync_to: &str) -> SwiftResult<String> {
    let url_path = sync_to
        .strip_prefix("http://")
        .or_else(|| sync_to.strip_prefix("https://"))
        .ok_or_else(|| SwiftError::BadRequest("Invalid sync URL".to_string()))?;

    // Find the path after the host
    let path_start = url_path
        .find('/')
        .ok_or_else(|| SwiftError::BadRequest("Invalid sync URL: missing path".to_string()))?;

    let path = &url_path[path_start..];

    // Expected format: /v1/{account}/{container}
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    if parts.len() < 3 {
        return Err(SwiftError::BadRequest(
            "Invalid sync URL: expected format /v1/{account}/{container}".to_string(),
        ));
    }

    Ok(parts[2].to_string())
}

/// Generate sync signature for authentication
///
/// Uses HMAC-SHA1 of the request path with shared secret
pub fn generate_sync_signature(path: &str, key: &str) -> String {
    use hmac::{Hmac, KeyInit, Mac};
    use sha1::Sha1;

    type HmacSha1 = Hmac<Sha1>;

    let mut mac = HmacSha1::new_from_slice(key.as_bytes()).unwrap_or_else(|_| panic!("HMAC key error"));

    mac.update(path.as_bytes());

    let result = mac.finalize();
    hex::encode(result.into_bytes())
}

/// Verify sync signature
pub fn verify_sync_signature(path: &str, key: &str, signature: &str) -> bool {
    let expected = generate_sync_signature(path, key);
    expected == signature
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_config_from_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert(
            "x-container-sync-to".to_string(),
            "https://remote.example.com/v1/AUTH_remote/backup".to_string(),
        );
        metadata.insert("x-container-sync-key".to_string(), "mysecretkey123".to_string());

        let config = SyncConfig::from_metadata(&metadata).unwrap();
        assert!(config.is_some());

        let config = config.unwrap();
        assert_eq!(config.sync_to, "https://remote.example.com/v1/AUTH_remote/backup");
        assert_eq!(config.sync_key, "mysecretkey123");
        assert!(config.enabled);
    }

    #[test]
    fn test_sync_config_from_metadata_no_sync() {
        let metadata = HashMap::new();
        let config = SyncConfig::from_metadata(&metadata).unwrap();
        assert!(config.is_none());
    }

    #[test]
    fn test_sync_config_from_metadata_missing_key() {
        let mut metadata = HashMap::new();
        metadata.insert("x-container-sync-to".to_string(), "https://example.com/v1/AUTH_test/backup".to_string());

        let result = SyncConfig::from_metadata(&metadata);
        assert!(result.is_err());
    }

    #[test]
    fn test_sync_config_validation() {
        let config = SyncConfig {
            sync_to: "https://example.com/v1/AUTH_test/backup".to_string(),
            sync_key: "verylongsecretkey123".to_string(),
            enabled: true,
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sync_config_validation_invalid_url() {
        let config = SyncConfig {
            sync_to: "invalid-url".to_string(),
            sync_key: "secretkey".to_string(),
            enabled: true,
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_sync_status_record_success() {
        let mut status = SyncStatus::new();
        assert_eq!(status.objects_synced, 0);

        status.record_success(1000, 5);
        assert_eq!(status.last_sync, Some(1000));
        assert_eq!(status.objects_synced, 5);
        assert_eq!(status.last_error, None);
    }

    #[test]
    fn test_sync_status_record_failure() {
        let mut status = SyncStatus::new();

        status.record_failure("Connection timeout".to_string());
        assert_eq!(status.sync_failures, 1);
        assert_eq!(status.last_error, Some("Connection timeout".to_string()));
    }

    #[test]
    fn test_sync_queue_entry_retry_backoff() {
        let mut entry = SyncQueueEntry::new("test.txt".to_string(), "abc123".to_string(), 1000);

        assert_eq!(entry.retry_count, 0);

        entry.schedule_retry(2000);
        assert_eq!(entry.retry_count, 1);
        assert_eq!(entry.next_retry, 2060); // 2000 + 60 (1 minute)

        entry.schedule_retry(2060);
        assert_eq!(entry.retry_count, 2);
        assert_eq!(entry.next_retry, 2180); // 2060 + 120 (2 minutes)
    }

    #[test]
    fn test_sync_queue_entry_ready_for_retry() {
        let mut entry = SyncQueueEntry::new("test.txt".to_string(), "abc123".to_string(), 1000);
        entry.schedule_retry(2000);

        assert!(!entry.ready_for_retry(2000));
        assert!(!entry.ready_for_retry(2059));
        assert!(entry.ready_for_retry(2060));
        assert!(entry.ready_for_retry(3000));
    }

    #[test]
    fn test_sync_queue_entry_max_retries() {
        let mut entry = SyncQueueEntry::new("test.txt".to_string(), "abc123".to_string(), 1000);

        for i in 0..10 {
            assert!(!entry.max_retries_exceeded());
            entry.schedule_retry(1000 + i * 60);
        }

        assert!(entry.max_retries_exceeded());
    }

    #[test]
    fn test_resolve_conflict_last_write_wins() {
        // Local newer
        assert!(resolve_conflict(2000, 1000, ConflictResolution::LastWriteWins));

        // Remote newer
        assert!(!resolve_conflict(1000, 2000, ConflictResolution::LastWriteWins));

        // Same timestamp (local wins by default in our implementation)
        assert!(resolve_conflict(1000, 1000, ConflictResolution::LastWriteWins));
    }

    #[test]
    fn test_resolve_conflict_strategies() {
        assert!(resolve_conflict(1000, 2000, ConflictResolution::LocalWins));
        assert!(!resolve_conflict(2000, 1000, ConflictResolution::RemoteWins));
    }

    #[test]
    fn test_extract_target_container() {
        let url = "https://remote.example.com/v1/AUTH_test/backup-container";
        let container = extract_target_container(url).unwrap();
        assert_eq!(container, "backup-container");

        let url2 = "http://localhost:8080/v1/AUTH_local/my-container";
        let container2 = extract_target_container(url2).unwrap();
        assert_eq!(container2, "my-container");
    }

    #[test]
    fn test_extract_target_container_invalid() {
        assert!(extract_target_container("invalid-url").is_err());
        assert!(extract_target_container("https://example.com/invalid").is_err());
    }

    #[test]
    fn test_generate_sync_signature() {
        let path = "/v1/AUTH_test/container/object.txt";
        let key = "mysecretkey";

        let sig1 = generate_sync_signature(path, key);
        let sig2 = generate_sync_signature(path, key);

        // Signature should be deterministic
        assert_eq!(sig1, sig2);
        assert_eq!(sig1.len(), 40); // SHA1 = 20 bytes = 40 hex chars

        // Different key produces different signature
        let sig3 = generate_sync_signature(path, "differentkey");
        assert_ne!(sig1, sig3);
    }

    #[test]
    fn test_verify_sync_signature() {
        let path = "/v1/AUTH_test/container/object.txt";
        let key = "mysecretkey";

        let signature = generate_sync_signature(path, key);
        assert!(verify_sync_signature(path, key, &signature));

        // Wrong signature
        assert!(!verify_sync_signature(path, key, "wrongsignature"));

        // Wrong key
        assert!(!verify_sync_signature(path, "wrongkey", &signature));
    }
}
