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

//! Tests for PatternRules and RulesMap
//!
//! This module contains comprehensive tests for the rule matching logic
//! used in bucket notifications, specifically testing the flow from
//! configuration to event matching.

use super::*;
use rustfs_s3_common::EventName;
use rustfs_targets::arn::TargetID;

#[cfg(test)]
mod pattern_rules_tests {
    use super::*;

    /// Test basic pattern rules matching
    #[test]
    fn test_pattern_rules_basic_matching() {
        let mut rules = PatternRules::new();
        let target_id = TargetID::new("test-id".to_string(), "webhook".to_string());

        // Add a rule for *.csv files
        rules.add("*.csv".to_string(), target_id.clone());

        // Test matching
        assert!(rules.match_simple("test.csv"));
        assert!(rules.match_simple("uploads/test.csv"));
        assert!(!rules.match_simple("test.txt"));

        // Test match_targets
        let targets = rules.match_targets("test.csv");
        assert!(targets.contains(&target_id));

        let targets = rules.match_targets("test.txt");
        assert!(!targets.contains(&target_id));
    }

    /// Test multiple patterns with different targets
    #[test]
    fn test_multiple_patterns() {
        let mut rules = PatternRules::new();
        let csv_target = TargetID::new("csv-target".to_string(), "webhook".to_string());
        let jpg_target = TargetID::new("jpg-target".to_string(), "webhook".to_string());

        rules.add("*.csv".to_string(), csv_target.clone());
        rules.add("*.jpg".to_string(), jpg_target.clone());

        // CSV files match csv_target
        let targets = rules.match_targets("test.csv");
        assert!(targets.contains(&csv_target));
        assert!(!targets.contains(&jpg_target));

        // JPG files match jpg_target
        let targets = rules.match_targets("test.jpg");
        assert!(!targets.contains(&csv_target));
        assert!(targets.contains(&jpg_target));

        // Both patterns match for nested paths
        let targets = rules.match_targets("uploads/test.csv");
        assert!(targets.contains(&csv_target));

        let targets = rules.match_targets("images/photo.jpg");
        assert!(targets.contains(&jpg_target));
    }

    /// Test prefix patterns
    #[test]
    fn test_prefix_patterns() {
        let mut rules = PatternRules::new();
        let target_id = TargetID::new("prefix-target".to_string(), "webhook".to_string());

        rules.add("uploads/*".to_string(), target_id.clone());
        rules.add("images/*".to_string(), target_id);

        assert!(rules.match_simple("uploads/test.csv"));
        assert!(rules.match_simple("uploads/subdir/test.csv"));
        assert!(rules.match_simple("images/photo.jpg"));
        assert!(!rules.match_simple("documents/test.txt"));
    }

    /// Test prefix + suffix pattern (bug scenario)
    #[test]
    fn test_prefix_suffix_pattern_bug_scenario() {
        let mut rules = PatternRules::new();
        let target_id = TargetID::new("webhook-target".to_string(), "webhook".to_string());

        // This is the pattern generated from prefix="uploads/", suffix=".csv"
        rules.add("uploads/*.csv".to_string(), target_id.clone());

        // These should match
        assert!(rules.match_simple("uploads/test.csv"));
        assert!(rules.match_simple("uploads/file1.csv"));
        assert!(rules.match_simple("uploads/nested/path/file.csv"));

        // These should not match
        assert!(!rules.match_simple("uploads/test.txt"));
        assert!(!rules.match_simple("files/test.csv"));

        // Verify match_targets returns correct targets
        let targets = rules.match_targets("uploads/test.csv");
        assert_eq!(targets.len(), 1);
        assert!(targets.contains(&target_id));
    }

    /// Test match_all pattern
    #[test]
    fn test_match_all_pattern() {
        let mut rules = PatternRules::new();
        let target_id = TargetID::new("all-target".to_string(), "webhook".to_string());

        rules.add("*".to_string(), target_id.clone());

        assert!(rules.match_simple("anything"));
        assert!(rules.match_simple("uploads/test.csv"));
        assert!(rules.match_simple(""));

        let targets = rules.match_targets("any_key");
        assert!(targets.contains(&target_id));
    }

    /// Test empty rules
    #[test]
    fn test_empty_rules() {
        let rules = PatternRules::new();

        assert!(rules.is_empty());
        assert!(!rules.match_simple("anything"));

        let targets = rules.match_targets("test.csv");
        assert!(targets.is_empty());
    }

    /// Test union operation
    #[test]
    fn test_union() {
        let mut rules1 = PatternRules::new();
        let mut rules2 = PatternRules::new();

        let target1 = TargetID::new("target1".to_string(), "webhook".to_string());
        let target2 = TargetID::new("target2".to_string(), "webhook".to_string());

        rules1.add("*.csv".to_string(), target1.clone());
        rules2.add("*.jpg".to_string(), target2);

        let combined = rules1.union(&rules2);

        assert!(combined.match_simple("test.csv"));
        assert!(combined.match_simple("test.jpg"));
        assert!(!combined.match_simple("test.png"));

        let targets = combined.match_targets("test.csv");
        assert!(targets.contains(&target1));
    }

    /// Test difference operation
    #[test]
    fn test_difference() {
        let mut rules1 = PatternRules::new();
        let mut rules2 = PatternRules::new();

        let target1 = TargetID::new("target1".to_string(), "webhook".to_string());
        let target2 = TargetID::new("target2".to_string(), "webhook".to_string());

        // Add same target to multiple patterns in rules1
        rules1.add("*.csv".to_string(), target1.clone());
        rules1.add("*.jpg".to_string(), target1.clone());
        rules1.add("*.txt".to_string(), target1);

        // Add different target to .jpg pattern in rules2
        rules2.add("*.jpg".to_string(), target2);

        let diff = rules1.difference(&rules2);

        // After difference:
        // *.csv: target1 remains (pattern doesn't exist in rules2)
        // *.jpg: target1 remains (target1 != target2, so it's not removed)
        // *.txt: target1 remains (pattern doesn't exist in rules2)
        assert!(diff.match_simple("test.csv"));
        assert!(diff.match_simple("test.jpg"));
        assert!(diff.match_simple("test.txt"));
    }

    /// Test difference operation removing same target
    #[test]
    fn test_difference_same_target() {
        let mut rules1 = PatternRules::new();
        let mut rules2 = PatternRules::new();

        let target1 = TargetID::new("target1".to_string(), "webhook".to_string());

        // Add same target to multiple patterns in rules1
        rules1.add("*.csv".to_string(), target1.clone());
        rules1.add("*.jpg".to_string(), target1.clone());
        rules1.add("*.txt".to_string(), target1.clone());

        // Add same target to .jpg pattern in rules2
        rules2.add("*.jpg".to_string(), target1);

        let diff = rules1.difference(&rules2);

        // After difference:
        // *.csv: target1 remains (pattern doesn't exist in rules2)
        // *.jpg: target1 is removed (same target in both)
        // *.txt: target1 remains (pattern doesn't exist in rules2)
        assert!(diff.match_simple("test.csv"));
        assert!(!diff.match_simple("test.jpg"));
        assert!(diff.match_simple("test.txt"));
    }

    /// Test remove_pattern
    #[test]
    fn test_remove_pattern() {
        let mut rules = PatternRules::new();
        let target_id = TargetID::new("test-target".to_string(), "webhook".to_string());

        rules.add("*.csv".to_string(), target_id.clone());
        rules.add("*.jpg".to_string(), target_id);

        assert!(rules.match_simple("test.csv"));
        assert!(rules.match_simple("test.jpg"));

        // Remove .csv pattern
        assert!(rules.remove_pattern("*.csv"));

        assert!(!rules.match_simple("test.csv"));
        assert!(rules.match_simple("test.jpg"));

        // Remove non-existent pattern
        assert!(!rules.remove_pattern("*.png"));
    }
}

#[cfg(test)]
mod rules_map_tests {
    use super::*;

    /// Test basic rule addition and matching
    #[test]
    fn test_rules_map_basic() {
        let mut rules_map = RulesMap::new();
        let target_id = TargetID::new("test-target".to_string(), "webhook".to_string());

        // Add rule for ObjectCreatedPut with pattern *
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*".to_string(), target_id.clone());

        // Check has_subscriber
        assert!(rules_map.has_subscriber(&EventName::ObjectCreatedPut));
        assert!(!rules_map.has_subscriber(&EventName::ObjectRemovedDelete));

        // Check match_rules
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "any_key");
        assert!(targets.contains(&target_id));

        // Check event with different name doesn't match
        let targets = rules_map.match_rules(EventName::ObjectRemovedDelete, "any_key");
        assert!(!targets.contains(&target_id));
    }

    /// Test compound event expansion
    #[test]
    fn test_compound_event_expansion() {
        let mut rules_map = RulesMap::new();
        let target_id = TargetID::new("test-target".to_string(), "webhook".to_string());

        // Add rule for ObjectCreatedAll (compound event)
        rules_map.add_rule_config(&[EventName::ObjectCreatedAll], "*.csv".to_string(), target_id.clone());

        // ObjectCreatedAll should be expanded to all ObjectCreated events
        assert!(rules_map.has_subscriber(&EventName::ObjectCreatedAll));
        assert!(rules_map.has_subscriber(&EventName::ObjectCreatedPut));
        assert!(rules_map.has_subscriber(&EventName::ObjectCreatedPost));
        assert!(rules_map.has_subscriber(&EventName::ObjectCreatedCopy));

        // All should match .csv files
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "test.csv");
        assert!(targets.contains(&target_id));

        let targets = rules_map.match_rules(EventName::ObjectCreatedPost, "test.csv");
        assert!(targets.contains(&target_id));
    }

    /// Test exact bug scenario
    #[test]
    fn test_bug_scenario_exact_flow() {
        let mut rules_map = RulesMap::new();
        let target_id = TargetID::new("webhook-primary".to_string(), "webhook".to_string());

        // Configuration: Events=["s3:ObjectCreated:*"], prefix="uploads/", suffix=".csv"
        // This generates pattern "uploads/*.csv"
        rules_map.add_rule_config(&[EventName::ObjectCreatedAll], "uploads/*.csv".to_string(), target_id.clone());

        // When ObjectCreatedPut event occurs with key "uploads/test.csv"
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "uploads/test.csv");

        // Should match
        assert!(!targets.is_empty(), "Targets should not be empty for matching event");
        assert!(targets.contains(&target_id), "Should find webhook-primary target");

        // Test non-matching key
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "uploads/test.txt");
        assert!(targets.is_empty(), "Targets should be empty for non-matching key");

        // Test matching different suffix
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "uploads/subdir/file.csv");
        assert!(!targets.is_empty(), "Nested CSV files should also match");
        assert!(targets.contains(&target_id));
    }

    /// Test multiple patterns for same event
    #[test]
    fn test_multiple_patterns_same_event() {
        let mut rules_map = RulesMap::new();
        let csv_target = TargetID::new("csv-target".to_string(), "webhook".to_string());
        let jpg_target = TargetID::new("jpg-target".to_string(), "webhook".to_string());

        // Add different patterns for same event
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*.csv".to_string(), csv_target.clone());
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*.jpg".to_string(), jpg_target.clone());

        // Both patterns should work
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "test.csv");
        assert!(targets.contains(&csv_target));
        assert!(!targets.contains(&jpg_target));

        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "test.jpg");
        assert!(!targets.contains(&csv_target));
        assert!(targets.contains(&jpg_target));
    }

    /// Test prefix only filter
    #[test]
    fn test_prefix_only_filter() {
        let mut rules_map = RulesMap::new();
        let target_id = TargetID::new("test-target".to_string(), "webhook".to_string());

        // Configuration: prefix="images/", no suffix
        // Pattern should be "images/*"
        rules_map.add_rule_config(&[EventName::ObjectCreatedAll], "images/*".to_string(), target_id.clone());

        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "images/photo.jpg");
        assert!(targets.contains(&target_id));

        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "uploads/photo.jpg");
        assert!(!targets.contains(&target_id));
    }

    /// Test suffix only filter
    #[test]
    fn test_suffix_only_filter() {
        let mut rules_map = RulesMap::new();
        let target_id = TargetID::new("test-target".to_string(), "webhook".to_string());

        // Configuration: no prefix, suffix=".pdf"
        // Pattern should be "*.pdf"
        rules_map.add_rule_config(&[EventName::ObjectCreatedAll], "*.pdf".to_string(), target_id.clone());

        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "document.pdf");
        assert!(targets.contains(&target_id));

        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "document.txt");
        assert!(!targets.contains(&target_id));
    }

    /// Test empty pattern (no filter)
    #[test]
    fn test_no_filter() {
        let mut rules_map = RulesMap::new();
        let target_id = TargetID::new("test-target".to_string(), "webhook".to_string());

        // Configuration: no prefix, no suffix
        // Pattern should default to "*" (match all)
        rules_map.add_rule_config(&[EventName::ObjectCreatedAll], "".to_string(), target_id.clone());

        // All keys should match
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "anything.csv");
        assert!(targets.contains(&target_id));

        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "uploads/test.txt");
        assert!(targets.contains(&target_id));
    }

    /// Test different event types
    #[test]
    fn test_different_event_types() {
        let mut rules_map = RulesMap::new();
        let put_target = TargetID::new("put-target".to_string(), "webhook".to_string());
        let delete_target = TargetID::new("delete-target".to_string(), "webhook".to_string());

        // Add rules for different event types
        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*.csv".to_string(), put_target.clone());
        rules_map.add_rule_config(&[EventName::ObjectRemovedDelete], "*.csv".to_string(), delete_target.clone());

        // Put event should match put_target
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "test.csv");
        assert!(targets.contains(&put_target));
        assert!(!targets.contains(&delete_target));

        // Delete event should match delete_target
        let targets = rules_map.match_rules(EventName::ObjectRemovedDelete, "test.csv");
        assert!(!targets.contains(&put_target));
        assert!(targets.contains(&delete_target));
    }

    /// Test remove_map
    #[test]
    fn test_remove_map() {
        let mut rules_map1 = RulesMap::new();
        let mut rules_map2 = RulesMap::new();

        let target1 = TargetID::new("target1".to_string(), "webhook".to_string());
        let target2 = TargetID::new("target2".to_string(), "webhook".to_string());

        rules_map1.add_rule_config(&[EventName::ObjectCreatedPut], "*.csv".to_string(), target1.clone());
        rules_map1.add_rule_config(&[EventName::ObjectCreatedPost], "*.jpg".to_string(), target1.clone());

        rules_map2.add_rule_config(&[EventName::ObjectCreatedPut], "*.csv".to_string(), target2.clone());

        // Remove rules_map2 from rules_map1
        rules_map1.remove_map(&rules_map2);

        // ObjectCreatedPut rule should still exist with target1 (different from target2)
        let targets = rules_map1.match_rules(EventName::ObjectCreatedPut, "test.csv");
        assert!(targets.contains(&target1));
        assert!(!targets.contains(&target2));

        // ObjectCreatedPost rule should still exist
        let targets = rules_map1.match_rules(EventName::ObjectCreatedPost, "test.jpg");
        assert!(targets.contains(&target1));
    }

    /// Test remove_map removing same target
    #[test]
    fn test_remove_map_same_target() {
        let mut rules_map1 = RulesMap::new();
        let mut rules_map2 = RulesMap::new();

        let target1 = TargetID::new("target1".to_string(), "webhook".to_string());

        rules_map1.add_rule_config(&[EventName::ObjectCreatedPut], "*.csv".to_string(), target1.clone());
        rules_map1.add_rule_config(&[EventName::ObjectCreatedPost], "*.jpg".to_string(), target1.clone());

        rules_map2.add_rule_config(&[EventName::ObjectCreatedPut], "*.csv".to_string(), target1.clone());

        // Remove rules_map2 from rules_map1
        rules_map1.remove_map(&rules_map2);

        // ObjectCreatedPut rule should be removed (same target and pattern)
        let targets = rules_map1.match_rules(EventName::ObjectCreatedPut, "test.csv");
        assert!(targets.is_empty());

        // ObjectCreatedPost rule should still exist
        let targets = rules_map1.match_rules(EventName::ObjectCreatedPost, "test.jpg");
        assert!(targets.contains(&target1));
    }

    /// Test contains_target_id
    #[test]
    fn test_contains_target_id() {
        let mut rules_map = RulesMap::new();
        let target_id = TargetID::new("test-target".to_string(), "webhook".to_string());

        assert!(!rules_map.contains_target_id(&target_id));

        rules_map.add_rule_config(&[EventName::ObjectCreatedPut], "*.csv".to_string(), target_id.clone());

        assert!(rules_map.contains_target_id(&target_id));
    }
}
