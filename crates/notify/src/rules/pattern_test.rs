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

//! Pattern matching tests for notification filter rules
//!
//! This module contains tests for the pattern matching logic used in
//! bucket notification filters, specifically testing prefix and suffix
//! filter rules as used in S3 bucket notifications.

use super::pattern;

#[cfg(test)]
mod bug_reproduction_tests {
    use super::*;

    /// Test case from bug report:
    /// - Prefix: "uploads/"
    /// - Suffix: ".csv"
    /// - Object key: "uploads/test.csv"
    #[test]
    fn test_uploads_csv_pattern() {
        let pattern = pattern::new_pattern(Some("uploads/"), Some(".csv"));
        assert_eq!(pattern, "uploads/*.csv");
        assert!(pattern::match_simple(&pattern, "uploads/test.csv"));
        assert!(pattern::match_simple(&pattern, "uploads/subdir/file.csv"));
        assert!(!pattern::match_simple(&pattern, "uploads/test.txt"));
    }

    /// Test prefix only
    #[test]
    fn test_prefix_only() {
        let pattern = pattern::new_pattern(Some("images/"), None);
        assert_eq!(pattern, "images/*");
        assert!(pattern::match_simple(&pattern, "images/photo.jpg"));
        assert!(pattern::match_simple(&pattern, "images/subdir/photo.png"));
        assert!(!pattern::match_simple(&pattern, "documents/photo.jpg"));
    }

    /// Test suffix only
    #[test]
    fn test_suffix_only() {
        let pattern = pattern::new_pattern(None, Some(".jpg"));
        assert_eq!(pattern, "*.jpg");
        assert!(pattern::match_simple(&pattern, "photo.jpg"));
        assert!(pattern::match_simple(&pattern, "uploads/photo.jpg"));
        assert!(pattern::match_simple(&pattern, "a/b/photo.jpg"));
        assert!(!pattern::match_simple(&pattern, "photo.png"));
    }

    /// Test empty pattern (no filter)
    #[test]
    fn test_empty_pattern() {
        let pattern = pattern::new_pattern(None, None);
        assert_eq!(pattern, "");
        // Empty pattern should not match anything
        assert!(!pattern::match_simple(&pattern, "anything"));
    }

    /// Test complex patterns
    #[test]
    fn test_complex_patterns() {
        // Pattern with both prefix and suffix
        let pattern = pattern::new_pattern(Some("data/uploads/"), Some(".csv"));
        assert_eq!(pattern, "data/uploads/*.csv");
        assert!(pattern::match_simple(&pattern, "data/uploads/test.csv"));
        assert!(!pattern::match_simple(&pattern, "data/test.csv"));

        // Pattern with prefix containing special characters
        let pattern = pattern::new_pattern(Some("special-chars_123/"), Some(".txt"));
        assert_eq!(pattern, "special-chars_123/*.txt");
        assert!(pattern::match_simple(&pattern, "special-chars_123/file.txt"));
    }

    /// Test edge cases
    #[test]
    fn test_edge_cases() {
        // Empty prefix and non-empty suffix
        let pattern = pattern::new_pattern(Some(""), Some(".csv"));
        assert_eq!(pattern, "*.csv");

        // Non-empty prefix and empty suffix
        let pattern = pattern::new_pattern(Some("uploads/"), Some(""));
        assert_eq!(pattern, "uploads/*");

        // Pattern ending with star in prefix
        let pattern = pattern::new_pattern(Some("uploads*"), Some(".csv"));
        assert_eq!(pattern, "uploads*.csv");

        // Pattern starting with star in suffix
        let pattern = pattern::new_pattern(Some("uploads/"), Some("*csv"));
        assert_eq!(pattern, "uploads/*csv");
    }

    /// Test the exact scenario from bug report
    #[test]
    fn test_bug_report_exact_scenario() {
        // Configuration: prefix="uploads/", suffix=".csv"
        let prefix = Some("uploads/");
        let suffix = Some(".csv");
        let object_key = "uploads/test.csv";

        // Generate pattern from filter rules
        let pattern = pattern::new_pattern(prefix, suffix);
        println!("Generated pattern: '{}'", pattern);

        // Verify pattern is correct
        assert_eq!(pattern, "uploads/*.csv");

        // Test matching
        let matches = pattern::match_simple(&pattern, object_key);
        println!("Pattern '{}' matches key '{}': {}", pattern, object_key, matches);

        assert!(matches, "Pattern '{}' should match object key '{}'", pattern, object_key);

        // Test other keys
        assert!(pattern::match_simple(&pattern, "uploads/file1.csv"));
        assert!(pattern::match_simple(&pattern, "uploads/nested/file.csv"));
        assert!(!pattern::match_simple(&pattern, "uploads/file.txt"));
        assert!(!pattern::match_simple(&pattern, "files/test.csv"));
    }

    /// Test with multiple slashes in path
    #[test]
    fn test_multiple_slashes() {
        let pattern = pattern::new_pattern(Some("a/b/c/"), Some(".csv"));
        assert_eq!(pattern, "a/b/c/*.csv");
        assert!(pattern::match_simple(&pattern, "a/b/c/test.csv"));
        assert!(!pattern::match_simple(&pattern, "a/b/test.csv"));
    }

    /// Test wildcard behavior
    #[test]
    fn test_wildcard_behavior() {
        // Test that * matches any characters including slashes
        let pattern = "uploads/*.csv";
        assert!(pattern::match_simple(pattern, "uploads/test.csv"));
        assert!(pattern::match_simple(pattern, "uploads/subdir/test.csv"));
        assert!(pattern::match_simple(pattern, "uploads/a/b/c/test.csv"));

        // Test that multiple wildcards work
        let pattern = "a*b*c.csv";
        assert!(pattern::match_simple(pattern, "a123b456c.csv"));
        assert!(pattern::match_simple(pattern, "abc.csv"));
    }

    /// Test match_all pattern
    #[test]
    fn test_match_all_pattern() {
        // S3 uses "*" to match all objects
        let pattern = "*";
        assert!(pattern::match_simple(pattern, "anything"));
        assert!(pattern::match_simple(pattern, "uploads/test.csv"));
        assert!(pattern::match_simple(pattern, ""));
    }
}
