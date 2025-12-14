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

//! Point-in-time file history retrieval.
//!
//! This module provides functionality for retrieving object versions
//! at a specific point in time, enabling users to view file history
//! and recover previous states of objects.

use crate::store_api::ObjectInfo;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Information about an object version for history display
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectVersionHistory {
    /// The version ID of this object version
    pub version_id: Option<String>,
    /// When this version was created/modified
    pub mod_time: Option<OffsetDateTime>,
    /// Size of this version in bytes
    pub size: i64,
    /// ETag of this version
    pub etag: Option<String>,
    /// Whether this version is a delete marker
    pub is_delete_marker: bool,
    /// Whether this is the latest/current version
    pub is_latest: bool,
    /// Storage class of this version
    pub storage_class: Option<String>,
}

impl ObjectVersionHistory {
    /// Create a new ObjectVersionHistory from an ObjectInfo
    pub fn from_object_info(info: &ObjectInfo, is_latest: bool) -> Self {
        Self {
            version_id: info.version_id.map(|v| v.to_string()),
            mod_time: info.mod_time,
            size: info.size,
            etag: info.etag.clone(),
            is_delete_marker: info.delete_marker,
            is_latest,
            storage_class: info.storage_class.clone(),
        }
    }
}

/// Response structure for file history queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileHistoryResponse {
    /// Bucket name
    pub bucket: String,
    /// Object key
    pub object: String,
    /// List of all versions, ordered by mod_time descending (newest first)
    pub versions: Vec<ObjectVersionHistory>,
    /// Total number of versions
    pub total_versions: usize,
}

impl FileHistoryResponse {
    /// Create a new FileHistoryResponse
    pub fn new(bucket: String, object: String, versions: Vec<ObjectVersionHistory>) -> Self {
        let total_versions = versions.len();
        Self {
            bucket,
            object,
            versions,
            total_versions,
        }
    }
}

/// Find the version of an object that was current at the specified datetime.
///
/// This function searches through all versions of an object and returns
/// the most recent version that was created before or at the target datetime.
///
/// # Arguments
///
/// * `versions` - List of all object versions (should be sorted by mod_time descending)
/// * `at_time` - The target datetime to query
///
/// # Returns
///
/// * `Some(ObjectInfo)` - The version that was current at the specified time
/// * `None` - No version existed at that time (object didn't exist yet)
///
/// # Note
///
/// If the version found is a delete marker, it means the object was deleted
/// at that point in time.
pub fn find_version_at_time(versions: &[ObjectInfo], at_time: OffsetDateTime) -> Option<&ObjectInfo> {
    // Versions should be sorted by mod_time descending (newest first)
    // Find the first version where mod_time <= at_time
    for version in versions {
        if let Some(mod_time) = version.mod_time {
            if mod_time <= at_time {
                return Some(version);
            }
        }
    }
    None
}

/// Check if an object existed (and was not deleted) at a specific point in time
///
/// # Arguments
///
/// * `versions` - List of all object versions
/// * `at_time` - The target datetime to query
///
/// # Returns
///
/// * `true` - Object existed and was not deleted at that time
/// * `false` - Object didn't exist or was deleted at that time
pub fn object_existed_at_time(versions: &[ObjectInfo], at_time: OffsetDateTime) -> bool {
    match find_version_at_time(versions, at_time) {
        Some(version) => !version.delete_marker,
        None => false,
    }
}

/// Get the history of versions for display purposes
///
/// Converts a list of ObjectInfo versions into ObjectVersionHistory structs
/// suitable for API responses.
pub fn get_version_history(versions: Vec<ObjectInfo>) -> Vec<ObjectVersionHistory> {
    let mut history: Vec<ObjectVersionHistory> = Vec::with_capacity(versions.len());
    let mut is_first = true;

    for version in versions {
        let hist = ObjectVersionHistory::from_object_info(&version, is_first);
        history.push(hist);
        is_first = false;
    }

    history
}

/// Sort versions by modification time, newest first
pub fn sort_versions_by_time(versions: &mut [ObjectInfo]) {
    versions.sort_by(|a, b| {
        let a_time = a.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH);
        let b_time = b.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH);
        b_time.cmp(&a_time)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;
    use uuid::Uuid;

    fn create_test_version(mod_time: OffsetDateTime, is_delete_marker: bool) -> ObjectInfo {
        ObjectInfo {
            bucket: "test-bucket".to_string(),
            name: "test-object".to_string(),
            mod_time: Some(mod_time),
            size: 1024,
            version_id: Some(Uuid::new_v4()),
            delete_marker: is_delete_marker,
            ..Default::default()
        }
    }

    #[test]
    fn test_find_version_at_time_exact_match() {
        let t1 = datetime!(2025-01-01 10:00:00 UTC);
        let t2 = datetime!(2025-01-02 10:00:00 UTC);
        let t3 = datetime!(2025-01-03 10:00:00 UTC);

        let versions = vec![
            create_test_version(t3, false),
            create_test_version(t2, false),
            create_test_version(t1, false),
        ];

        // Query at exact time of t2
        let result = find_version_at_time(&versions, t2);
        assert!(result.is_some());
        assert_eq!(result.unwrap().mod_time, Some(t2));
    }

    #[test]
    fn test_find_version_at_time_between_versions() {
        let t1 = datetime!(2025-01-01 10:00:00 UTC);
        let t2 = datetime!(2025-01-03 10:00:00 UTC);

        let versions = vec![create_test_version(t2, false), create_test_version(t1, false)];

        // Query at time between t1 and t2 - should return t1
        let query_time = datetime!(2025-01-02 10:00:00 UTC);
        let result = find_version_at_time(&versions, query_time);
        assert!(result.is_some());
        assert_eq!(result.unwrap().mod_time, Some(t1));
    }

    #[test]
    fn test_find_version_at_time_before_first_version() {
        let t1 = datetime!(2025-01-02 10:00:00 UTC);

        let versions = vec![create_test_version(t1, false)];

        // Query before object existed
        let query_time = datetime!(2025-01-01 10:00:00 UTC);
        let result = find_version_at_time(&versions, query_time);
        assert!(result.is_none());
    }

    #[test]
    fn test_object_existed_at_time_with_delete_marker() {
        let t1 = datetime!(2025-01-01 10:00:00 UTC);
        let t2 = datetime!(2025-01-02 10:00:00 UTC); // delete marker
        let t3 = datetime!(2025-01-03 10:00:00 UTC);

        let versions = vec![
            create_test_version(t3, false),
            create_test_version(t2, true), // Delete marker
            create_test_version(t1, false),
        ];

        // At t1, object existed
        assert!(object_existed_at_time(&versions, t1));

        // At t2 (delete marker), object was deleted
        assert!(!object_existed_at_time(&versions, t2));

        // At t3, object exists again
        assert!(object_existed_at_time(&versions, t3));

        // Between t1 and t2, use t1 version
        let between = datetime!(2025-01-01 15:00:00 UTC);
        assert!(object_existed_at_time(&versions, between));
    }

    #[test]
    fn test_get_version_history() {
        let t1 = datetime!(2025-01-01 10:00:00 UTC);
        let t2 = datetime!(2025-01-02 10:00:00 UTC);

        let versions = vec![create_test_version(t2, false), create_test_version(t1, false)];

        let history = get_version_history(versions);
        assert_eq!(history.len(), 2);
        assert!(history[0].is_latest);
        assert!(!history[1].is_latest);
    }

    #[test]
    fn test_sort_versions_by_time() {
        let t1 = datetime!(2025-01-01 10:00:00 UTC);
        let t2 = datetime!(2025-01-02 10:00:00 UTC);
        let t3 = datetime!(2025-01-03 10:00:00 UTC);

        let mut versions = vec![
            create_test_version(t1, false),
            create_test_version(t3, false),
            create_test_version(t2, false),
        ];

        sort_versions_by_time(&mut versions);

        assert_eq!(versions[0].mod_time, Some(t3));
        assert_eq!(versions[1].mod_time, Some(t2));
        assert_eq!(versions[2].mod_time, Some(t1));
    }
}
