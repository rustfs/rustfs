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

//! Object Symlinks Support for Swift API
//!
//! This module implements symlink objects that reference other objects,
//! similar to filesystem symlinks. Symlinks allow creating multiple references
//! to the same object without duplicating data.
//!
//! # Configuration
//!
//! Symlinks are created via the X-Object-Symlink-Target header:
//!
//! ```bash
//! # Create symlink to object in same container
//! curl -X PUT "http://swift.example.com/v1/AUTH_test/container/link" \
//!   -H "X-Auth-Token: $TOKEN" \
//!   -H "X-Object-Symlink-Target: object.txt"
//!
//! # Create symlink to object in different container
//! curl -X PUT "http://swift.example.com/v1/AUTH_test/container/link" \
//!   -H "X-Auth-Token: $TOKEN" \
//!   -H "X-Object-Symlink-Target: other-container/object.txt"
//! ```
//!
//! # Symlink Resolution
//!
//! When a GET or HEAD request is made on a symlink:
//! 1. The symlink target is extracted from metadata
//! 2. The target object is retrieved (following symlink chain if needed)
//! 3. Target object content is returned with X-Symlink-Target header
//! 4. Loop detection prevents infinite recursion (max 5 hops)
//!
//! # Example Usage
//!
//! ```bash
//! # Upload original object
//! swift upload container file.txt
//!
//! # Create symlink
//! swift upload container link.txt \
//!   -H "X-Object-Symlink-Target: file.txt" \
//!   --object-name link.txt
//!
//! # Download via symlink (returns file.txt content)
//! swift download container link.txt
//! # Response includes: X-Symlink-Target: container/file.txt
//! ```

use super::{SwiftError, SwiftResult};
use std::collections::HashSet;
use tracing::{debug, warn};

/// Maximum symlink follow depth to prevent infinite loops
const MAX_SYMLINK_DEPTH: u8 = 5;

/// Symlink path used for loop detection
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SymlinkPath {
    pub account: String,
    pub container: String,
    pub object: String,
}

impl SymlinkPath {
    pub fn new(account: &str, container: &str, object: &str) -> Self {
        Self {
            account: account.to_string(),
            container: container.to_string(),
            object: object.to_string(),
        }
    }

    pub fn from_strs(account: &str, container: &str, object: &str) -> Self {
        Self::new(account, container, object)
    }
}

/// Parsed symlink target
#[derive(Debug, Clone, PartialEq)]
pub struct SymlinkTarget {
    /// Target container (None = same container as symlink)
    pub container: Option<String>,

    /// Target object name
    pub object: String,
}

impl SymlinkTarget {
    /// Parse symlink target from header value
    ///
    /// Formats:
    /// - "object" - Same container
    /// - "container/object" - Different container
    pub fn parse(value: &str) -> SwiftResult<Self> {
        let value = value.trim();

        if value.is_empty() {
            return Err(SwiftError::BadRequest("X-Object-Symlink-Target cannot be empty".to_string()));
        }

        // Check for container/object format
        if let Some(slash_pos) = value.find('/') {
            let container = value[..slash_pos].to_string();
            let object = value[slash_pos + 1..].to_string();

            if container.is_empty() || object.is_empty() {
                return Err(SwiftError::BadRequest(
                    "Invalid symlink target format: container and object cannot be empty".to_string(),
                ));
            }

            Ok(SymlinkTarget {
                container: Some(container),
                object,
            })
        } else {
            // Same container
            Ok(SymlinkTarget {
                container: None,
                object: value.to_string(),
            })
        }
    }

    /// Format symlink target for header value
    ///
    /// Returns: "container/object" or "object"
    pub fn to_header_value(&self, current_container: &str) -> String {
        match &self.container {
            Some(container) => format!("{}/{}", container, self.object),
            None => format!("{}/{}", current_container, self.object),
        }
    }

    /// Resolve container name (use current container if not specified)
    pub fn resolve_container<'a>(&'a self, current_container: &'a str) -> &'a str {
        self.container.as_deref().unwrap_or(current_container)
    }
}

/// Extract symlink target from request headers
pub fn extract_symlink_target(headers: &http::HeaderMap) -> SwiftResult<Option<SymlinkTarget>> {
    if let Some(target_header) = headers.get("x-object-symlink-target") {
        let target_str = target_header
            .to_str()
            .map_err(|_| SwiftError::BadRequest("Invalid X-Object-Symlink-Target header".to_string()))?;

        let target = SymlinkTarget::parse(target_str)?;
        debug!("Extracted symlink target: container={:?}, object={}", target.container, target.object);
        Ok(Some(target))
    } else {
        Ok(None)
    }
}

/// Check if object is a symlink by examining metadata
pub fn is_symlink(metadata: &std::collections::HashMap<String, String>) -> bool {
    metadata.contains_key("x-object-symlink-target")
}

/// Get symlink target from object metadata
pub fn get_symlink_target(metadata: &std::collections::HashMap<String, String>) -> SwiftResult<Option<SymlinkTarget>> {
    if let Some(target_value) = metadata.get("x-object-symlink-target") {
        Ok(Some(SymlinkTarget::parse(target_value)?))
    } else {
        Ok(None)
    }
}

/// Validate symlink depth to prevent infinite loops
pub fn validate_symlink_depth(depth: u8) -> SwiftResult<()> {
    if depth >= MAX_SYMLINK_DEPTH {
        return Err(SwiftError::Conflict(format!(
            "Symlink loop detected or max depth exceeded (depth: {})",
            depth
        )));
    }
    Ok(())
}

/// Check if a symlink path has been visited before (circular reference detection)
pub fn check_circular_reference(visited: &HashSet<SymlinkPath>, account: &str, container: &str, object: &str) -> SwiftResult<()> {
    let path = SymlinkPath::new(account, container, object);

    if visited.contains(&path) {
        warn!(
            account = %account,
            container = %container,
            object = %object,
            "Circular symlink reference detected"
        );
        return Err(SwiftError::Conflict(format!(
            "Circular symlink reference detected: {}/{}/{}",
            account, container, object
        )));
    }

    Ok(())
}

/// Validate symlink depth and check for circular references
pub fn validate_symlink_access(
    visited: &HashSet<SymlinkPath>,
    depth: u8,
    account: &str,
    container: &str,
    object: &str,
) -> SwiftResult<()> {
    // Check depth limit first
    validate_symlink_depth(depth)?;

    // Check for circular references
    check_circular_reference(visited, account, container, object)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_symlink_target_same_container() {
        let target = SymlinkTarget::parse("object.txt").unwrap();
        assert_eq!(target.container, None);
        assert_eq!(target.object, "object.txt");
    }

    #[test]
    fn test_parse_symlink_target_different_container() {
        let target = SymlinkTarget::parse("other-container/object.txt").unwrap();
        assert_eq!(target.container, Some("other-container".to_string()));
        assert_eq!(target.object, "object.txt");
    }

    #[test]
    fn test_parse_symlink_target_with_slashes() {
        let target = SymlinkTarget::parse("container/path/to/object.txt").unwrap();
        assert_eq!(target.container, Some("container".to_string()));
        assert_eq!(target.object, "path/to/object.txt");
    }

    #[test]
    fn test_parse_symlink_target_empty() {
        let result = SymlinkTarget::parse("");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_symlink_target_empty_container() {
        let result = SymlinkTarget::parse("/object.txt");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_symlink_target_empty_object() {
        let result = SymlinkTarget::parse("container/");
        assert!(result.is_err());
    }

    #[test]
    fn test_to_header_value_same_container() {
        let target = SymlinkTarget {
            container: None,
            object: "object.txt".to_string(),
        };
        assert_eq!(target.to_header_value("my-container"), "my-container/object.txt");
    }

    #[test]
    fn test_to_header_value_different_container() {
        let target = SymlinkTarget {
            container: Some("other-container".to_string()),
            object: "object.txt".to_string(),
        };
        assert_eq!(target.to_header_value("my-container"), "other-container/object.txt");
    }

    #[test]
    fn test_resolve_container_same() {
        let target = SymlinkTarget {
            container: None,
            object: "object.txt".to_string(),
        };
        assert_eq!(target.resolve_container("my-container"), "my-container");
    }

    #[test]
    fn test_resolve_container_different() {
        let target = SymlinkTarget {
            container: Some("other-container".to_string()),
            object: "object.txt".to_string(),
        };
        assert_eq!(target.resolve_container("my-container"), "other-container");
    }

    #[test]
    fn test_extract_symlink_target_present() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-object-symlink-target", "target.txt".parse().unwrap());

        let result = extract_symlink_target(&headers).unwrap();
        assert!(result.is_some());

        let target = result.unwrap();
        assert_eq!(target.container, None);
        assert_eq!(target.object, "target.txt");
    }

    #[test]
    fn test_extract_symlink_target_absent() {
        let headers = http::HeaderMap::new();
        let result = extract_symlink_target(&headers).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_is_symlink_true() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("x-object-symlink-target".to_string(), "target.txt".to_string());
        assert!(is_symlink(&metadata));
    }

    #[test]
    fn test_is_symlink_false() {
        let metadata = std::collections::HashMap::new();
        assert!(!is_symlink(&metadata));
    }

    #[test]
    fn test_get_symlink_target_present() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("x-object-symlink-target".to_string(), "container/target.txt".to_string());

        let result = get_symlink_target(&metadata).unwrap();
        assert!(result.is_some());

        let target = result.unwrap();
        assert_eq!(target.container, Some("container".to_string()));
        assert_eq!(target.object, "target.txt");
    }

    #[test]
    fn test_get_symlink_target_absent() {
        let metadata = std::collections::HashMap::new();
        let result = get_symlink_target(&metadata).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_validate_symlink_depth_ok() {
        assert!(validate_symlink_depth(0).is_ok());
        assert!(validate_symlink_depth(4).is_ok());
    }

    #[test]
    fn test_validate_symlink_depth_exceeded() {
        assert!(validate_symlink_depth(5).is_err());
        assert!(validate_symlink_depth(10).is_err());
    }

    #[test]
    fn test_symlink_path_creation() {
        let path = SymlinkPath::new("account1", "container1", "object1");
        assert_eq!(path.account, "account1");
        assert_eq!(path.container, "container1");
        assert_eq!(path.object, "object1");
    }

    #[test]
    fn test_symlink_path_equality() {
        let path1 = SymlinkPath::new("account1", "container1", "object1");
        let path2 = SymlinkPath::new("account1", "container1", "object1");
        let path3 = SymlinkPath::new("account2", "container1", "object1");

        assert_eq!(path1, path2);
        assert_ne!(path1, path3);
    }

    #[test]
    fn test_check_circular_reference_not_visited() {
        let visited = HashSet::new();
        assert!(check_circular_reference(&visited, "acc", "cont", "obj").is_ok());
    }

    #[test]
    fn test_check_circular_reference_visited() {
        let mut visited = HashSet::new();
        visited.insert(SymlinkPath::new("acc", "cont", "obj"));

        let result = check_circular_reference(&visited, "acc", "cont", "obj");
        assert!(result.is_err());

        if let Err(SwiftError::Conflict(msg)) = result {
            assert!(msg.contains("Circular symlink reference detected"));
            assert!(msg.contains("acc/cont/obj"));
        } else {
            panic!("Expected Conflict error");
        }
    }

    #[test]
    fn test_check_circular_reference_different_path() {
        let mut visited = HashSet::new();
        visited.insert(SymlinkPath::new("acc1", "cont1", "obj1"));

        // Different path should not trigger circular reference error
        assert!(check_circular_reference(&visited, "acc2", "cont2", "obj2").is_ok());
    }

    #[test]
    fn test_validate_symlink_access_success() {
        let visited = HashSet::new();
        assert!(validate_symlink_access(&visited, 0, "acc", "cont", "obj").is_ok());
        assert!(validate_symlink_access(&visited, 4, "acc", "cont", "obj").is_ok());
    }

    #[test]
    fn test_validate_symlink_access_depth_exceeded() {
        let visited = HashSet::new();
        assert!(validate_symlink_access(&visited, 5, "acc", "cont", "obj").is_err());
        assert!(validate_symlink_access(&visited, 10, "acc", "cont", "obj").is_err());
    }

    #[test]
    fn test_validate_symlink_access_circular_reference() {
        let mut visited = HashSet::new();
        visited.insert(SymlinkPath::new("acc", "cont", "obj"));

        let result = validate_symlink_access(&visited, 0, "acc", "cont", "obj");
        assert!(result.is_err());

        if let Err(SwiftError::Conflict(msg)) = result {
            assert!(msg.contains("Circular symlink reference detected"));
        } else {
            panic!("Expected Conflict error");
        }
    }

    #[test]
    fn test_validate_symlink_access_both_checks() {
        let mut visited = HashSet::new();
        visited.insert(SymlinkPath::new("acc", "cont", "obj"));

        // Should fail due to circular reference even though depth is OK
        assert!(validate_symlink_access(&visited, 0, "acc", "cont", "obj").is_err());

        // Should fail due to depth even though no circular reference
        assert!(validate_symlink_access(&visited, 6, "acc2", "cont2", "obj2").is_err());
    }
}
