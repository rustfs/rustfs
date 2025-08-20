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
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, error, info};

use crate::version;

/// Update check related errors
#[derive(Error, Debug)]
pub enum UpdateCheckError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("Version parsing failed: {0}")]
    VersionParseError(String),

    #[error("Invalid version response: {0}")]
    InvalidResponse(String),
}

/// Version information structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    /// Version number
    pub version: String,
    /// Release date
    pub release_date: Option<String>,
    /// Release notes
    pub release_notes: Option<String>,
    /// Download URL
    pub download_url: Option<String>,
}

/// Update check result
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateCheckResult {
    /// Whether update is available
    pub update_available: bool,
    /// Current version
    pub current_version: String,
    /// Latest version information
    pub latest_version: Option<VersionInfo>,
    /// Check time
    pub check_time: chrono::DateTime<chrono::Utc>,
}

/// Version checker
pub struct VersionChecker {
    /// HTTP client
    client: reqwest::Client,
    /// Version server URL
    version_url: String,
    /// Request timeout
    timeout: Duration,
}

impl Default for VersionChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionChecker {
    /// Create a new version checker
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent(format!("RustFS/{}", get_current_version()))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            client,
            version_url: "https://version.rustfs.com/latest.json".to_string(),
            timeout: Duration::from_secs(10),
        }
    }

    /// Create version checker with custom configuration
    #[allow(dead_code)]
    pub fn with_config(url: String, timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .user_agent(format!("RustFS/{}", get_current_version()))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            client,
            version_url: url,
            timeout,
        }
    }

    /// Check for updates
    pub async fn check_for_updates(&self) -> Result<UpdateCheckResult, UpdateCheckError> {
        let current_version = get_current_version();
        debug!("Checking for updates, current version: {}", current_version);

        // Send HTTP GET request to get latest version information
        let response = self.client.get(&self.version_url).timeout(self.timeout).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            error!("Version check request failed, status code: {}, response: {}", status, error_text);
            return Err(UpdateCheckError::InvalidResponse(format!(
                "HTTP status code: {status}, response: {error_text}"
            )));
        }

        // Parse response
        let response_bytes = response.bytes().await?;
        let version_info: VersionInfo = match serde_json::from_slice(&response_bytes) {
            Ok(v) => v,
            Err(e) => {
                let error_text = String::from_utf8_lossy(&response_bytes);
                error!("Version check request failed, response: {}", e);
                return Err(UpdateCheckError::InvalidResponse(format!(
                    "JSON parsing failed: {e}, response: {error_text}"
                )));
            }
        };

        debug!("Retrieved latest version information: {:?}", version_info);

        // Compare versions using version.rs functions
        let update_available = version::is_newer_version(&current_version, &version_info.version)
            .map_err(|e| UpdateCheckError::VersionParseError(e.to_string()))?;

        let result = UpdateCheckResult {
            update_available,
            current_version,
            latest_version: Some(version_info),
            check_time: chrono::Utc::now(),
        };

        if result.update_available {
            info!(
                "New version available: {} -> {}",
                result.current_version,
                result.latest_version.as_ref().unwrap().version
            );
        } else {
            info!("Current version is up to date: {}", result.current_version);
        }

        Ok(result)
    }
}

/// Get current version number
pub fn get_current_version() -> String {
    version::get_version()
}

/// Convenience function for async update checking
pub async fn check_updates() -> Result<UpdateCheckResult, UpdateCheckError> {
    let checker = VersionChecker::new();
    checker.check_for_updates().await
}

/// Update check with custom URL
#[allow(dead_code)]
pub async fn check_updates_with_url(url: String) -> Result<UpdateCheckResult, UpdateCheckError> {
    let checker = VersionChecker::with_config(url, Duration::from_secs(10));
    checker.check_for_updates().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_current_version() {
        let version = get_current_version();
        assert!(!version.is_empty());
        debug!("Current version: {version}");
    }

    #[test]
    fn test_update_check_result() {
        use chrono::Utc;

        // Test creating UpdateCheckResult with update available
        let version_info = VersionInfo {
            version: "1.2.0".to_string(),
            release_date: Some("2024-01-15T10:00:00Z".to_string()),
            release_notes: Some("Bug fixes and new features".to_string()),
            download_url: Some("https://github.com/rustfs/rustfs/releases/tag/v1.2.0".to_string()),
        };

        let check_time = Utc::now();
        let result = UpdateCheckResult {
            update_available: true,
            current_version: "1.1.0".to_string(),
            latest_version: Some(version_info.clone()),
            check_time,
        };

        debug!("Update check result: {:?}", serde_json::to_string(&result).unwrap());

        // Test fields
        assert!(result.update_available);
        assert_eq!(result.current_version, "1.1.0");
        assert!(result.latest_version.is_some());
        assert_eq!(result.check_time, check_time);

        // Test latest version info
        if let Some(latest) = &result.latest_version {
            assert_eq!(latest.version, "1.2.0");
            assert_eq!(latest.release_date, Some("2024-01-15T10:00:00Z".to_string()));
            assert_eq!(latest.release_notes, Some("Bug fixes and new features".to_string()));
            assert_eq!(
                latest.download_url,
                Some("https://github.com/rustfs/rustfs/releases/tag/v1.2.0".to_string())
            );
        }

        // Test Clone functionality
        let cloned_result = result.clone();
        assert_eq!(cloned_result.update_available, result.update_available);
        assert_eq!(cloned_result.current_version, result.current_version);
        assert_eq!(cloned_result.check_time, result.check_time);

        // Test Debug functionality (should not panic)
        let debug_output = format!("{result:?}");
        assert!(debug_output.contains("UpdateCheckResult"));
        assert!(debug_output.contains("1.1.0"));
        assert!(debug_output.contains("1.2.0"));

        // Test creating UpdateCheckResult with no update available
        let no_update_result = UpdateCheckResult {
            update_available: false,
            current_version: "1.2.0".to_string(),
            latest_version: Some(VersionInfo {
                version: "1.2.0".to_string(),
                release_date: Some("2024-01-15T10:00:00Z".to_string()),
                release_notes: None,
                download_url: None,
            }),
            check_time: Utc::now(),
        };

        assert!(!no_update_result.update_available);
        assert_eq!(no_update_result.current_version, "1.2.0");

        // Test creating UpdateCheckResult with None latest_version (error case)
        let error_result = UpdateCheckResult {
            update_available: false,
            current_version: "1.1.0".to_string(),
            latest_version: None,
            check_time: Utc::now(),
        };

        assert!(!error_result.update_available);
        assert!(error_result.latest_version.is_none());

        debug!("UpdateCheckResult tests passed successfully");
    }

    #[test]
    fn test_version_info() {
        // Test VersionInfo structure
        let version_info = VersionInfo {
            version: "2.0.0".to_string(),
            release_date: Some("2024-02-01T12:00:00Z".to_string()),
            release_notes: Some("Major release with breaking changes".to_string()),
            download_url: Some("https://github.com/rustfs/rustfs/releases/tag/v2.0.0".to_string()),
        };

        // Test fields
        assert_eq!(version_info.version, "2.0.0");
        assert_eq!(version_info.release_date, Some("2024-02-01T12:00:00Z".to_string()));
        assert_eq!(version_info.release_notes, Some("Major release with breaking changes".to_string()));
        assert_eq!(
            version_info.download_url,
            Some("https://github.com/rustfs/rustfs/releases/tag/v2.0.0".to_string())
        );

        // Test Clone functionality
        let cloned_info = version_info.clone();
        assert_eq!(cloned_info.version, version_info.version);
        assert_eq!(cloned_info.release_date, version_info.release_date);
        assert_eq!(cloned_info.release_notes, version_info.release_notes);
        assert_eq!(cloned_info.download_url, version_info.download_url);

        // Test Debug functionality
        let debug_output = format!("{version_info:?}");
        assert!(debug_output.contains("VersionInfo"));
        assert!(debug_output.contains("2.0.0"));

        // Test minimal VersionInfo with only version
        let minimal_info = VersionInfo {
            version: "1.0.0".to_string(),
            release_date: None,
            release_notes: None,
            download_url: None,
        };

        assert_eq!(minimal_info.version, "1.0.0");
        assert!(minimal_info.release_date.is_none());
        assert!(minimal_info.release_notes.is_none());
        assert!(minimal_info.download_url.is_none());

        // Test JSON serialization/deserialization
        let json_string = serde_json::to_string(&version_info).unwrap();
        debug!("Serialized version info: {json_string}");
        assert!(json_string.contains("2.0.0"));
        assert!(json_string.contains("Major release"));

        let deserialized: VersionInfo = serde_json::from_str(&json_string).unwrap();
        assert_eq!(deserialized.version, version_info.version);
        assert_eq!(deserialized.release_notes, version_info.release_notes);

        debug!("VersionInfo tests passed successfully");
    }

    #[test]
    fn test_version_functions_integration() {
        // Test that version functions from version.rs work correctly
        assert_eq!(version::clean_version("refs/tags/1.0.0-alpha.17"), "1.0.0-alpha.17");
        assert_eq!(version::clean_version("v1.0.0"), "1.0.0");

        // Test version comparison
        assert!(version::is_newer_version("1.0.0", "1.0.1").unwrap());
        assert!(!version::is_newer_version("1.0.1", "1.0.0").unwrap());

        // Test version parsing using parse_version
        assert_eq!(version::parse_version("1.0.0").unwrap(), (1, 0, 0, None));
        assert_eq!(version::parse_version("2.1.3-alpha.1").unwrap(), (2, 1, 3, Some("alpha.1".to_string())));

        debug!("Version functions integration tests passed successfully");
    }
}
