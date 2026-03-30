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

//! Integration tests for BucketNotificationConfig
//!
//! This module contains tests that simulate the exact flow from XML configuration
//! to event matching, including filter rules with prefix and suffix.

use super::*;
use rustfs_s3_common::EventName;
use rustfs_targets::arn::{ARN, TargetID};
use std::io::Cursor;

#[cfg(test)]
mod integration_tests {
    use super::*;

    fn create_test_arn() -> ARN {
        ARN {
            partition: "rustfs".to_string(),
            service: "sqs".to_string(),
            region: "".to_string(),
            target_id: TargetID::new("primary".to_string(), "webhook".to_string()),
        }
    }

    #[test]
    fn test_create_arn() {
        let arn = create_test_arn();

        assert_eq!(arn.partition, "rustfs");
        assert_eq!(arn.service, "sqs");
        assert_eq!(arn.region, "");
        assert_eq!(arn.target_id, TargetID::new("primary".to_string(), "webhook".to_string()));
    }

    /// Test exact bug scenario from the bug report
    #[test]
    fn test_bug_report_exact_scenario_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<NotificationConfiguration>
    <QueueConfiguration>
        <Id>test-queue</Id>
        <Queue>arn:rustfs:sqs:ap-northeast-1:primary:webhook</Queue>
        <Event>s3:ObjectCreated:*</Event>
        <Filter>
            <S3Key>
                <FilterRule>
                    <Name>prefix</Name>
                    <Value>uploads/</Value>
                </FilterRule>
                <FilterRule>
                    <Name>suffix</Name>
                    <Value>.csv</Value>
                </FilterRule>
            </S3Key>
        </Filter>
    </QueueConfiguration>
</NotificationConfiguration>"#;

        let current_region = "ap-northeast-1";
        let arn_list = vec!["arn:rustfs:sqs:ap-northeast-1:primary:webhook".to_string()];

        let config = BucketNotificationConfig::from_xml(Cursor::new(xml.as_bytes()), current_region, &arn_list).unwrap();

        // Verify pattern is "uploads/*.csv"
        let rules_map = config.get_rules_map();

        // Debug: Print the patterns stored
        println!("\nPatterns stored in rules_map:");
        for (event_name, pattern_rules) in rules_map.inner().iter() {
            for pattern in pattern_rules.inner().keys() {
                println!("  Event: {:?}, Pattern: '{}'", event_name, pattern);
            }
        }

        // The event should be registered for ObjectCreatedAll and all its expansions
        assert!(rules_map.has_subscriber(&EventName::ObjectCreatedAll));
        assert!(rules_map.has_subscriber(&EventName::ObjectCreatedPut));

        // Test matching "uploads/test.csv" with ObjectCreatedPut
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "uploads/test.csv");

        println!("Matching 'uploads/test.csv' with ObjectCreatedPut: {} targets", targets.len());
        for target in &targets {
            println!("  Target: {:?}", target);
        }

        // Should find the target
        assert!(!targets.is_empty(), "Should find at least one target");

        // Test matching "uploads/subdir/file.csv" - should also match
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "uploads/subdir/file.csv");
        assert!(!targets.is_empty(), "Nested CSV files should match");

        // Test matching "uploads/test.txt" - should NOT match
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "uploads/test.txt");
        assert!(targets.is_empty(), "TXT files should not match");

        // Test matching "files/test.csv" - should NOT match (wrong prefix)
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "files/test.csv");
        assert!(targets.is_empty(), "Files in wrong prefix should not match");
    }

    /// Test prefix only filter
    #[test]
    fn test_prefix_only_filter_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<NotificationConfiguration>
    <QueueConfiguration>
        <Id>test-queue</Id>
        <Queue>arn:rustfs:sqs:ap-northeast-1:primary:webhook</Queue>
        <Event>s3:ObjectCreated:*</Event>
        <Filter>
            <S3Key>
                <FilterRuleList>
                    <FilterRule>
                        <Name>prefix</Name>
                        <Value>images/</Value>
                    </FilterRule>
                </FilterRuleList>
            </S3Key>
        </Filter>
    </QueueConfiguration>
</NotificationConfiguration>"#;

        let current_region = "ap-northeast-1";
        let arn_list = vec!["arn:rustfs:sqs:ap-northeast-1:primary:webhook".to_string()];

        let config = BucketNotificationConfig::from_xml(Cursor::new(xml.as_bytes()), current_region, &arn_list).unwrap();

        let rules_map = config.get_rules_map();

        // Debug: Print the patterns stored
        println!("\nPatterns stored in rules_map:");
        for (event_name, pattern_rules) in rules_map.inner().iter() {
            for pattern in pattern_rules.inner().keys() {
                println!("  Event: {:?}, Pattern: '{}'", event_name, pattern);
            }
        }

        // Test matching "images/photo.jpg"
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "images/photo.jpg");
        println!("Matching 'images/photo.jpg': {} targets", targets.len());
        assert!(!targets.is_empty(), "Files in images/ should match");

        // Test matching "uploads/photo.jpg" - should NOT match
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "uploads/photo.jpg");
        println!("Matching 'uploads/photo.jpg': {} targets", targets.len());
        assert!(targets.is_empty(), "Files not in images/ should not match");
    }

    /// Test suffix only filter
    #[test]
    fn test_suffix_only_filter_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<NotificationConfiguration>
    <QueueConfiguration>
        <Id>test-queue</Id>
        <Queue>arn:rustfs:sqs:ap-northeast-1:primary:webhook</Queue>
        <Event>s3:ObjectCreated:*</Event>
        <Filter>
            <S3Key>
                <FilterRuleList>
                    <FilterRule>
                        <Name>suffix</Name>
                        <Value>.pdf</Value>
                    </FilterRule>
                </FilterRuleList>
            </S3Key>
        </Filter>
    </QueueConfiguration>
</NotificationConfiguration>"#;

        let current_region = "ap-northeast-1";
        let arn_list = vec!["arn:rustfs:sqs:ap-northeast-1:primary:webhook".to_string()];

        let config = BucketNotificationConfig::from_xml(Cursor::new(xml.as_bytes()), current_region, &arn_list).unwrap();

        let rules_map = config.get_rules_map();

        // Test matching "document.pdf" - should match
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "document.pdf");
        assert!(!targets.is_empty(), "PDF files should match");

        // Test matching "document.txt" - should NOT match
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "document.txt");
        assert!(targets.is_empty(), "Non-PDF files should not match");
    }

    /// Test no filter (match all)
    #[test]
    fn test_no_filter_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<NotificationConfiguration>
    <QueueConfiguration>
        <Id>test-queue</Id>
        <Queue>arn:rustfs:sqs:ap-northeast-1:primary:webhook</Queue>
        <Event>s3:ObjectCreated:*</Event>
    </QueueConfiguration>
</NotificationConfiguration>"#;

        let current_region = "ap-northeast-1";
        let arn_list = vec!["arn:rustfs:sqs:ap-northeast-1:primary:webhook".to_string()];

        let config = BucketNotificationConfig::from_xml(Cursor::new(xml.as_bytes()), current_region, &arn_list).unwrap();

        let rules_map = config.get_rules_map();

        // All files should match
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "anything.csv");
        assert!(!targets.is_empty(), "All files should match");

        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "uploads/test.txt");
        assert!(!targets.is_empty(), "All files should match");
    }

    /// Test specific event type instead of compound
    #[test]
    fn test_specific_event_type_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<NotificationConfiguration>
    <QueueConfiguration>
        <Id>test-queue</Id>
        <Queue>arn:rustfs:sqs:ap-northeast-1:primary:webhook</Queue>
        <Event>s3:ObjectCreated:Put</Event>
        <Filter>
            <S3Key>
                <FilterRule>
                    <Name>prefix</Name>
                    <Value>uploads/</Value>
                </FilterRule>
                <FilterRule>
                    <Name>suffix</Name>
                    <Value>.csv</Value>
                </FilterRule>
            </S3Key>
        </Filter>
    </QueueConfiguration>
</NotificationConfiguration>"#;

        let current_region = "ap-northeast-1";
        let arn_list = vec!["arn:rustfs:sqs:ap-northeast-1:primary:webhook".to_string()];

        let config = BucketNotificationConfig::from_xml(Cursor::new(xml.as_bytes()), current_region, &arn_list).unwrap();

        let rules_map = config.get_rules_map();

        // Debug: Print all events that have subscribers
        println!("\nAll events with subscribers:");
        for (event_name, _pattern_rules) in rules_map.inner().iter() {
            println!("  Event: {:?}", event_name);
        }

        // Note: has_subscriber uses bitmask logic, so has_subscriber(&ObjectCreatedAll) returns true
        // if any ObjectCreated* event is registered. This is intentional for efficiency.
        // Only ObjectCreatedPut was explicitly registered in the XML.

        // Should have subscriber for ObjectCreatedPut
        assert!(rules_map.has_subscriber(&EventName::ObjectCreatedPut));

        // Should NOT have subscriber for ObjectCreatedPost (only Put was configured)
        assert!(!rules_map.has_subscriber(&EventName::ObjectCreatedPost));

        // Test matching with Put event
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "uploads/test.csv");
        assert!(!targets.is_empty(), "Put event should match");
    }

    /// Test URL-encoded object keys
    #[test]
    fn test_url_encoded_keys() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<NotificationConfiguration>
    <QueueConfiguration>
        <Id>test-queue</Id>
        <Queue>arn:rustfs:sqs:ap-northeast-1:primary:webhook</Queue>
        <Event>s3:ObjectCreated:*</Event>
        <Filter>
            <S3Key>
                <FilterRule>
                    <Name>prefix</Name>
                    <Value>uploads/</Value>
                </FilterRule>
                <FilterRule>
                    <Name>suffix</Name>
                    <Value>.csv</Value>
                </FilterRule>
            </S3Key>
        </Filter>
    </QueueConfiguration>
</NotificationConfiguration>"#;

        let current_region = "ap-northeast-1";
        let arn_list = vec!["arn:rustfs:sqs:ap-northeast-1:primary:webhook".to_string()];

        let config = BucketNotificationConfig::from_xml(Cursor::new(xml.as_bytes()), current_region, &arn_list).unwrap();

        let rules_map = config.get_rules_map();

        // Test with URL-encoded space: "uploads/my%20file.csv"
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "uploads/my%20file.csv");
        // This may or may not match depending on encoding
        println!("URL-encoded key 'uploads/my%20file.csv' matched: {}", !targets.is_empty());

        // Test with unencoded version
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "uploads/my file.csv");
        assert!(!targets.is_empty(), "Unencoded key should match");
    }

    /// Test multiple queue configurations
    #[test]
    fn test_multiple_queue_configs_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<NotificationConfiguration>
    <QueueConfiguration>
        <Id>csv-queue</Id>
        <Queue>arn:rustfs:sqs::primary:webhook-csv</Queue>
        <Event>s3:ObjectCreated:*</Event>
        <Filter>
            <S3Key>
                <FilterRule>
                    <Name>prefix</Name>
                    <Value>uploads/</Value>
                </FilterRule>
                <FilterRule>
                    <Name>suffix</Name>
                    <Value>.csv</Value>
                </FilterRule>
            </S3Key>
        </Filter>
    </QueueConfiguration>
    <QueueConfiguration>
        <Id>jpg-queue</Id>
        <Queue>arn:rustfs:sqs::primary:webhook-jpg</Queue>
        <Event>s3:ObjectCreated:*</Event>
        <Filter>
            <S3Key>
                <FilterRuleList>
                    <FilterRule>
                        <Name>prefix</Name>
                        <Value>images/</Value>
                    </FilterRule>
                    <FilterRule>
                        <Name>suffix</Name>
                        <Value>.jpg</Value>
                    </FilterRule>
                </FilterRuleList>
            </S3Key>
        </Filter>
    </QueueConfiguration>
</NotificationConfiguration>"#;

        let current_region = "ap-northeast-1";
        let arn_list = vec![
            "arn:rustfs:sqs:ap-northeast-1:primary:webhook-csv".to_string(),
            "arn:rustfs:sqs:ap-northeast-1:primary:webhook-jpg".to_string(),
        ];

        let config = BucketNotificationConfig::from_xml(Cursor::new(xml.as_bytes()), current_region, &arn_list).unwrap();

        let rules_map = config.get_rules_map();

        // Test CSV files
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "uploads/test.csv");
        println!("Targets for 'uploads/test.csv': {:?}", targets);
        assert!(!targets.is_empty(), "CSV files should match");
        // Should have csv target (ARN: arn:rustfs:sqs::primary:webhook-csv)
        let csv_target = TargetID::new("primary".to_string(), "webhook-csv".to_string());
        assert!(targets.contains(&csv_target), "Should find CSV webhook, got: {:?}", targets);

        // Test JPG files
        let targets = rules_map.match_rules(EventName::ObjectCreatedPut, "images/photo.jpg");
        assert!(!targets.is_empty(), "JPG files should match");
        // Should have jpg target (ARN: arn:rustfs:sqs:primary:webhook-jpg)
        let jpg_target = TargetID::new("primary".to_string(), "webhook-jpg".to_string());
        assert!(targets.contains(&jpg_target), "Should find JPG webhook");
    }

    /// Test compound event type expansion
    #[test]
    fn test_compound_event_expansion_integration() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<NotificationConfiguration>
    <QueueConfiguration>
        <Id>test-queue</Id>
        <Queue>arn:rustfs:sqs:ap-northeast-1:primary:webhook</Queue>
        <Event>s3:ObjectCreated:*</Event>
        <Filter>
            <S3Key>
                <FilterRuleList>
                    <FilterRule>
                        <Name>prefix</Name>
                        <Value>data/</Value>
                    </FilterRule>
                </FilterRuleList>
            </S3Key>
        </Filter>
    </QueueConfiguration>
</NotificationConfiguration>"#;

        let current_region = "ap-northeast-1";
        let arn_list = vec!["arn:rustfs:sqs:ap-northeast-1:primary:webhook".to_string()];

        let config = BucketNotificationConfig::from_xml(Cursor::new(xml.as_bytes()), current_region, &arn_list).unwrap();

        let rules_map = config.get_rules_map();

        // AWS ObjectCreated:* should only include object creation operations.
        let event_types = [
            EventName::ObjectCreatedPut,
            EventName::ObjectCreatedPost,
            EventName::ObjectCreatedCopy,
            EventName::ObjectCreatedCompleteMultipartUpload,
        ];

        for event_type in event_types {
            assert!(rules_map.has_subscriber(&event_type), "Event {:?} should have subscribers", event_type);

            // All should match "data/file.csv"
            let targets = rules_map.match_rules(event_type, "data/file.csv");
            assert!(!targets.is_empty(), "Event {:?} should match", event_type);
        }

        assert!(!rules_map.has_subscriber(&EventName::ObjectTaggingPut));
        assert!(!rules_map.has_subscriber(&EventName::ObjectTaggingDelete));
    }
}
