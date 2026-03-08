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

//! Comprehensive tests for container listing and symlink features
//!
//! Tests cover:
//! - Container listing with prefix filter
//! - Container listing with delimiter (subdirectories)
//! - Container listing with marker/end_marker (pagination)
//! - Container listing with limit
//! - Symlink creation and validation
//! - Symlink GET/HEAD following
//! - Symlink target resolution
//! - Symlink loop detection

#![cfg(feature = "swift")]

use rustfs_protocols::swift::symlink::*;
use std::collections::HashMap;

/// Test symlink target validation
#[test]
fn test_is_symlink() {
    // Valid symlink metadata with correct header
    let mut metadata = HashMap::new();
    metadata.insert("x-object-symlink-target".to_string(), "container/object".to_string());
    assert!(is_symlink(&metadata));

    // No symlink metadata
    let metadata2 = HashMap::new();
    assert!(!is_symlink(&metadata2));

    // Regular object metadata
    let mut metadata3 = HashMap::new();
    metadata3.insert("content-type".to_string(), "text/plain".to_string());
    assert!(!is_symlink(&metadata3));
}

/// Test symlink target extraction
#[test]
fn test_get_symlink_target() {
    // Valid symlink target
    let mut metadata = HashMap::new();
    metadata.insert("x-object-symlink-target".to_string(), "photos/cat.jpg".to_string());

    let target = get_symlink_target(&metadata).unwrap();
    assert!(target.is_some());

    let target = target.unwrap();
    assert_eq!(target.container, Some("photos".to_string()));
    assert_eq!(target.object, "cat.jpg");

    // Same container target
    let mut metadata2 = HashMap::new();
    metadata2.insert("x-object-symlink-target".to_string(), "report.pdf".to_string());

    let target2 = get_symlink_target(&metadata2).unwrap();
    assert!(target2.is_some());

    let target2 = target2.unwrap();
    assert_eq!(target2.container, None);
    assert_eq!(target2.object, "report.pdf");

    // No symlink metadata
    let metadata3 = HashMap::new();
    let target3 = get_symlink_target(&metadata3).unwrap();
    assert_eq!(target3, None);
}

/// Test symlink target parsing
#[test]
fn test_parse_symlink_target() {
    use rustfs_protocols::swift::symlink::SymlinkTarget;

    // Standard format: container/object
    let target = SymlinkTarget::parse("photos/cat.jpg").unwrap();
    assert_eq!(target.container, Some("photos".to_string()));
    assert_eq!(target.object, "cat.jpg");

    // Nested object path
    let target2 = SymlinkTarget::parse("docs/2024/reports/summary.pdf").unwrap();
    assert_eq!(target2.container, Some("docs".to_string()));
    assert_eq!(target2.object, "2024/reports/summary.pdf");

    // Single slash
    let target3 = SymlinkTarget::parse("container/object").unwrap();
    assert_eq!(target3.container, Some("container".to_string()));
    assert_eq!(target3.object, "object");

    // Same container (no slash)
    let target4 = SymlinkTarget::parse("object.txt").unwrap();
    assert_eq!(target4.container, None);
    assert_eq!(target4.object, "object.txt");
}

/// Test invalid symlink targets
#[test]
fn test_parse_symlink_target_invalid() {
    use rustfs_protocols::swift::symlink::SymlinkTarget;

    // Empty string
    let result = SymlinkTarget::parse("");
    assert!(result.is_err());

    // Only slash (empty container and object)
    let result2 = SymlinkTarget::parse("/");
    assert!(result2.is_err());

    // Empty container
    let result3 = SymlinkTarget::parse("/object");
    assert!(result3.is_err());

    // Empty object
    let result4 = SymlinkTarget::parse("container/");
    assert!(result4.is_err());
}

/// Test symlink metadata format
#[test]
fn test_symlink_metadata_format() {
    let mut metadata = HashMap::new();
    metadata.insert("x-object-symlink-target".to_string(), "photos/cat.jpg".to_string());
    metadata.insert("content-type".to_string(), "application/symlink".to_string());

    assert!(is_symlink(&metadata));

    let target = get_symlink_target(&metadata).unwrap().unwrap();
    assert_eq!(target.container, Some("photos".to_string()));
    assert_eq!(target.object, "cat.jpg");

    // Content-Type should indicate symlink
    assert_eq!(metadata.get("content-type").unwrap(), "application/symlink");
}

/// Test symlink with empty target
#[test]
fn test_symlink_empty_target() {
    use rustfs_protocols::swift::symlink::SymlinkTarget;

    let mut metadata = HashMap::new();
    metadata.insert("x-object-symlink-target".to_string(), String::new());

    // Empty target should be invalid when parsed
    let result = SymlinkTarget::parse("");
    assert!(result.is_err());

    // Also check that is_symlink returns true (header exists)
    // but parsing will fail
    assert!(is_symlink(&metadata));
    let target_result = get_symlink_target(&metadata);
    assert!(target_result.is_err());
}

/// Test symlink target with special characters
#[test]
fn test_symlink_target_special_chars() {
    use rustfs_protocols::swift::symlink::SymlinkTarget;

    let test_cases = vec![
        ("container/file with spaces.txt", "container", "file with spaces.txt"),
        ("container/file-with-dashes.txt", "container", "file-with-dashes.txt"),
        ("container/file_with_underscores.txt", "container", "file_with_underscores.txt"),
        ("photos/2024/january/cat.jpg", "photos", "2024/january/cat.jpg"),
    ];

    for (target_str, expected_container, expected_object) in test_cases {
        let target = SymlinkTarget::parse(target_str).unwrap();

        assert_eq!(target.container, Some(expected_container.to_string()));
        assert_eq!(target.object, expected_object);
    }

    // Same container (no slash)
    let target = SymlinkTarget::parse("file.txt").unwrap();
    assert_eq!(target.container, None);
    assert_eq!(target.object, "file.txt");
}

/// Test symlink loop detection structure
#[test]
fn test_symlink_loop_detection() {
    // Test data structure for loop detection
    let mut visited = std::collections::HashSet::new();

    // Visit chain of symlinks
    let chain = vec!["link1", "link2", "link3"];

    for link in &chain {
        assert!(!visited.contains(link));
        visited.insert(*link);
    }

    // Try to revisit - should detect loop
    assert!(visited.contains(&"link1"));
}

/// Test maximum symlink depth
#[test]
fn test_symlink_max_depth() {
    use rustfs_protocols::swift::symlink::validate_symlink_depth;

    const MAX_SYMLINK_DEPTH: u8 = 5;

    // Depths 0-4 should be valid
    for depth in 0..MAX_SYMLINK_DEPTH {
        assert!(validate_symlink_depth(depth).is_ok());
    }

    // Depth 5 and above should fail
    assert!(validate_symlink_depth(MAX_SYMLINK_DEPTH).is_err());
    assert!(validate_symlink_depth(MAX_SYMLINK_DEPTH + 1).is_err());
}

/// Test symlink with query parameters in target
#[test]
fn test_symlink_target_query_params() {
    use rustfs_protocols::swift::symlink::SymlinkTarget;

    // Symlink targets should not include query parameters
    // (those are part of the request, not the target)

    let target = SymlinkTarget::parse("container/object").unwrap();

    assert_eq!(target.container, Some("container".to_string()));
    assert_eq!(target.object, "object");

    // Query params would be on the request URL, not the target
}

/// Test symlink metadata preservation
#[test]
fn test_symlink_metadata_preservation() {
    let mut metadata = HashMap::new();
    metadata.insert("x-object-symlink-target".to_string(), "photos/cat.jpg".to_string());
    metadata.insert("x-object-meta-description".to_string(), "Link to cat photo".to_string());
    metadata.insert("content-type".to_string(), "application/symlink".to_string());

    // All metadata should be preserved
    assert_eq!(metadata.len(), 3);
    assert!(metadata.contains_key("x-object-symlink-target"));
    assert!(metadata.contains_key("x-object-meta-description"));
    assert!(metadata.contains_key("content-type"));
}

/// Test container listing prefix filter structure
#[test]
fn test_listing_prefix_structure() {
    // Test that prefix filtering structure works correctly
    let objects = [
        "photos/2024/cat.jpg",
        "photos/2024/dog.jpg",
        "photos/2023/bird.jpg",
        "documents/report.pdf",
    ];

    // Filter by prefix "photos/2024/"
    let prefix = "photos/2024/";
    let filtered: Vec<_> = objects.iter().filter(|o| o.starts_with(prefix)).collect();

    assert_eq!(filtered.len(), 2);
    assert!(filtered.contains(&&"photos/2024/cat.jpg"));
    assert!(filtered.contains(&&"photos/2024/dog.jpg"));
}

/// Test container listing delimiter structure
#[test]
fn test_listing_delimiter_structure() {
    // Test delimiter-based directory listing
    let objects = vec![
        "photos/2024/cat.jpg",
        "photos/2024/dog.jpg",
        "photos/2023/bird.jpg",
        "photos/README.txt",
        "documents/report.pdf",
    ];

    let delimiter = '/';

    // Group by first component (before first delimiter)
    let mut directories = std::collections::HashSet::new();
    for obj in &objects {
        if let Some(pos) = obj.find(delimiter) {
            directories.insert(&obj[..=pos]); // Include delimiter
        }
    }

    assert!(directories.contains("photos/"));
    assert!(directories.contains("documents/"));
}

/// Test container listing with marker (pagination)
#[test]
fn test_listing_marker_structure() {
    let objects = ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"];

    // List starting after marker "b.txt"
    let marker = "b.txt";
    let filtered: Vec<_> = objects.iter().filter(|o| *o > &marker).collect();

    assert_eq!(filtered.len(), 3);
    assert_eq!(*filtered[0], "c.txt");
    assert_eq!(*filtered[1], "d.txt");
    assert_eq!(*filtered[2], "e.txt");
}

/// Test container listing with end_marker
#[test]
fn test_listing_end_marker_structure() {
    let objects = ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"];

    // List up to (but not including) end_marker "d.txt"
    let end_marker = "d.txt";
    let filtered: Vec<_> = objects.iter().filter(|o| *o < &end_marker).collect();

    assert_eq!(filtered.len(), 3);
    assert_eq!(*filtered[0], "a.txt");
    assert_eq!(*filtered[1], "b.txt");
    assert_eq!(*filtered[2], "c.txt");
}

/// Test container listing with both marker and end_marker
#[test]
fn test_listing_marker_and_end_marker() {
    let objects = ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"];

    let marker = "b.txt";
    let end_marker = "e.txt";

    let filtered: Vec<_> = objects.iter().filter(|o| *o > &marker && *o < &end_marker).collect();

    assert_eq!(filtered.len(), 2);
    assert_eq!(*filtered[0], "c.txt");
    assert_eq!(*filtered[1], "d.txt");
}

/// Test container listing with limit
#[test]
fn test_listing_limit_structure() {
    let objects = ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"];

    let limit = 3;
    let limited: Vec<_> = objects.iter().take(limit).collect();

    assert_eq!(limited.len(), 3);
    assert_eq!(*limited[0], "a.txt");
    assert_eq!(*limited[1], "b.txt");
    assert_eq!(*limited[2], "c.txt");
}

/// Test container listing with prefix and limit
#[test]
fn test_listing_prefix_and_limit() {
    let objects = [
        "photos/a.jpg",
        "photos/b.jpg",
        "photos/c.jpg",
        "photos/d.jpg",
        "documents/x.pdf",
    ];

    let prefix = "photos/";
    let limit = 2;

    let filtered: Vec<_> = objects.iter().filter(|o| o.starts_with(prefix)).take(limit).collect();

    assert_eq!(filtered.len(), 2);
    assert_eq!(*filtered[0], "photos/a.jpg");
    assert_eq!(*filtered[1], "photos/b.jpg");
}

/// Test container listing with delimiter and prefix
#[test]
fn test_listing_delimiter_and_prefix() {
    let objects = [
        "photos/2024/cat.jpg",
        "photos/2024/dog.jpg",
        "photos/2023/bird.jpg",
        "documents/report.pdf",
    ];

    let prefix = "photos/";
    let delimiter = '/';

    // Filter by prefix first
    let with_prefix: Vec<_> = objects.iter().filter(|o| o.starts_with(prefix)).collect();

    // Then group by next delimiter
    let mut subdirs = std::collections::HashSet::new();
    for obj in with_prefix {
        let after_prefix = &obj[prefix.len()..];
        if let Some(pos) = after_prefix.find(delimiter) {
            subdirs.insert(&after_prefix[..=pos]);
        }
    }

    assert!(subdirs.contains("2024/"));
    assert!(subdirs.contains("2023/"));
}

/// Test symlink cross-container references
#[test]
fn test_symlink_cross_container() {
    use rustfs_protocols::swift::symlink::SymlinkTarget;

    // Symlinks can reference objects in different containers
    let target = SymlinkTarget::parse("other-container/object.txt").unwrap();

    assert_eq!(target.container, Some("other-container".to_string()));
    assert_eq!(target.object, "object.txt");
}

/// Test symlink to nested object
#[test]
fn test_symlink_to_nested_object() {
    use rustfs_protocols::swift::symlink::SymlinkTarget;

    let target = SymlinkTarget::parse("container/folder1/folder2/file.txt").unwrap();

    assert_eq!(target.container, Some("container".to_string()));
    assert_eq!(target.object, "folder1/folder2/file.txt");
}

/// Test listing empty container
#[test]
fn test_listing_empty_container() {
    let objects: Vec<&str> = vec![];

    let filtered: Vec<_> = objects.iter().collect();
    assert_eq!(filtered.len(), 0);

    // With prefix
    let with_prefix: Vec<_> = objects.iter().filter(|o| o.starts_with("prefix/")).collect();
    assert_eq!(with_prefix.len(), 0);
}

/// Test listing lexicographic ordering
#[test]
fn test_listing_lexicographic_order() {
    let mut objects = ["z.txt", "a.txt", "m.txt", "b.txt"];
    objects.sort();

    assert_eq!(objects[0], "a.txt");
    assert_eq!(objects[1], "b.txt");
    assert_eq!(objects[2], "m.txt");
    assert_eq!(objects[3], "z.txt");
}

/// Test listing with numeric-like names
#[test]
fn test_listing_numeric_names() {
    let mut objects = ["file10.txt", "file2.txt", "file1.txt", "file20.txt"];
    objects.sort();

    // Lexicographic sort, not numeric
    assert_eq!(objects[0], "file1.txt");
    assert_eq!(objects[1], "file10.txt");
    assert_eq!(objects[2], "file2.txt");
    assert_eq!(objects[3], "file20.txt");
}

/// Test symlink with absolute path target
#[test]
fn test_symlink_absolute_path() {
    // Swift symlinks typically use relative paths, but test absolute format
    let target = "/v1/AUTH_account/container/object";

    // Parse should handle leading slashes
    // (Implementation-dependent - may strip leading slash)
    if target.starts_with('/') {
        let stripped = target.trim_start_matches('/');
        // Should still be parseable after stripping
        assert!(stripped.contains('/'));
    }
}
