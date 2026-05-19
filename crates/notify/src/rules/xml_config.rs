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

use crate::rules::pattern;
use hashbrown::HashSet;
use rustfs_s3_common::EventName;
use rustfs_targets::arn::{ARN, ArnError, TargetIDError};
use serde::{Deserialize, Serialize};
use std::io::Read;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseConfigError {
    #[error("XML parsing error:{0}")]
    XmlError(#[from] quick_xml::errors::serialize::DeError),
    #[error("Invalid filter value:{0}")]
    InvalidFilterValue(String),
    #[error("Invalid filter name: {0}, only 'prefix' or 'suffix' is allowed")]
    InvalidFilterName(String),
    #[error("There can only be one 'prefix' in the filter rule")]
    DuplicatePrefixFilter,
    #[error("There can only be one 'suffix' in the filter rule")]
    DuplicateSuffixFilter,
    #[error("Missing event name")]
    MissingEventName,
    #[error("Duplicate event name:{0}")]
    DuplicateEventName(String), // EventName is usually an enum, and here String is used to represent its text
    #[error("Repeated queue configuration: ID={0:?}, ARN={1}")]
    DuplicateQueueConfiguration(Option<String>, String),
    #[error("Unsupported configuration types (e.g. Lambda, Topic)")]
    UnsupportedConfiguration,
    #[error("ARN not found:{0}")]
    ArnNotFound(String),
    #[error("Unknown area:{0}")]
    UnknownRegion(String),
    #[error("ARN parsing error:{0}")]
    ArnParseError(#[from] ArnError),
    #[error("TargetID parsing error:{0}")]
    TargetIDParseError(#[from] TargetIDError),
    #[error("IO Error:{0}")]
    IoError(#[from] std::io::Error),
    #[error("Region mismatch: Configure region {config_region}, current region {current_region}")]
    RegionMismatch { config_region: String, current_region: String },
    #[error("ARN {0} Not found in the provided list")]
    ArnValidation(String),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FilterRule {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Value")]
    pub value: String,
}

impl FilterRule {
    fn validate(&self) -> Result<(), ParseConfigError> {
        if !self.name.eq_ignore_ascii_case("prefix") && !self.name.eq_ignore_ascii_case("suffix") {
            return Err(ParseConfigError::InvalidFilterName(self.name.clone()));
        }
        // ValidateFilterRuleValue from Go:
        // no "." or ".." path segments, <= 1024 chars, valid UTF-8, no '\'.
        for segment in self.value.split('/') {
            if segment == "." || segment == ".." {
                return Err(ParseConfigError::InvalidFilterValue(self.value.clone()));
            }
        }
        if self.value.len() > 1024 || self.value.contains('\\') || std::str::from_utf8(self.value.as_bytes()).is_err() {
            return Err(ParseConfigError::InvalidFilterValue(self.value.clone()));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Clone, Default, PartialEq, Eq)]
pub struct S3KeyFilter {
    #[serde(rename = "FilterRule", default, skip_serializing_if = "Vec::is_empty")]
    pub filter_rule_list: Vec<FilterRule>,
}

impl S3KeyFilter {
    /// Validate filter rules for duplicates.
    /// According to AWS S3 documentation, there can be at most one prefix
    /// and one suffix filter rule per queue configuration.
    pub fn validate(&self) -> Result<(), ParseConfigError> {
        let mut has_prefix = false;
        let mut has_suffix = false;
        for rule in &self.filter_rule_list {
            rule.validate()?;
            if rule.name.eq_ignore_ascii_case("prefix") {
                if has_prefix {
                    return Err(ParseConfigError::DuplicatePrefixFilter);
                }
                has_prefix = true;
            } else if rule.name.eq_ignore_ascii_case("suffix") {
                if has_suffix {
                    return Err(ParseConfigError::DuplicateSuffixFilter);
                }
                has_suffix = true;
            }
        }
        Ok(())
    }

    /// Check if filter rule list is empty.
    pub fn is_empty(&self) -> bool {
        self.filter_rule_list.is_empty()
    }

    /// Generate pattern string from filter rules.
    /// This method extracts prefix and suffix values from filter rules
    /// and generates a wildcard pattern string for matching object keys.
    pub fn pattern(&self) -> String {
        let mut prefix_val: Option<&str> = None;
        let mut suffix_val: Option<&str> = None;

        for rule in &self.filter_rule_list {
            if rule.name.eq_ignore_ascii_case("prefix") {
                prefix_val = Some(&rule.value);
            } else if rule.name.eq_ignore_ascii_case("suffix") {
                suffix_val = Some(&rule.value);
            }
        }
        pattern::new_pattern(prefix_val, suffix_val)
    }
}

#[derive(Debug, serde::Deserialize)]
struct S3KeyContent {
    #[serde(rename = "FilterRule", default)]
    filter_rule_list: Vec<FilterRule>,
    #[serde(rename = "FilterRuleList", default)]
    filter_rule_list_wrapper: Option<S3KeyFilterRuleList>,
}

#[derive(Debug, serde::Deserialize)]
struct S3KeyFilterRuleList {
    #[serde(rename = "FilterRule", default)]
    filter_rule_list: Vec<FilterRule>,
}

impl S3KeyContent {
    /// Get all filter rules from this S3Key content, handling both direct FilterRule
    /// and FilterRuleList wrapper structures
    fn get_filter_rules(&self) -> Vec<FilterRule> {
        // If we have a FilterRuleList wrapper, use that
        if let Some(wrapper) = &self.filter_rule_list_wrapper
            && !wrapper.filter_rule_list.is_empty()
        {
            return wrapper.filter_rule_list.clone();
        }
        // Otherwise use direct FilterRule list
        self.filter_rule_list.clone()
    }
}

/// Custom deserializer for S3KeyFilter to handle Filter element correctly.
/// AWS S3 XML structure: <Filter><S3Key><FilterRule>...</FilterRule></S3Key></Filter>
impl<'de> Deserialize<'de> for S3KeyFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct S3KeyFilterVisitor {
            filter_rules: Vec<FilterRule>,
        }

        impl S3KeyFilterVisitor {
            fn new() -> Self {
                Self {
                    filter_rules: Vec::new(),
                }
            }
        }

        impl<'de> serde::de::Visitor<'de> for S3KeyFilterVisitor {
            type Value = S3KeyFilter;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(
                    formatter,
                    "an S3Key filter map with an `S3Key` element containing one or more \
 `FilterRule` children (e.g. <Filter><S3Key><FilterRule>...</FilterRule></S3Key></Filter>)"
                )
            }

            fn visit_map<V>(mut self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "S3Key" => {
                            // Parse S3Key content which contains FilterRule(s)
                            let s3key_content: S3KeyContent = map.next_value()?;
                            self.filter_rules = s3key_content.get_filter_rules();
                        }
                        _ => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }

                Ok(S3KeyFilter {
                    filter_rule_list: self.filter_rules,
                })
            }
        }

        deserializer.deserialize_map(S3KeyFilterVisitor::new())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct QueueConfig {
    #[serde(rename = "Id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "Queue")] // This is ARN in XML
    pub arn: ARN,
    #[serde(rename = "Event", default)] // XML has multiple <Event> tags
    pub events: Vec<EventName>, // EventName needs to handle XML (de)serialization if not string
    #[serde(rename = "Filter", default, skip_serializing_if = "s3key_filter_is_empty")]
    pub filter: S3KeyFilter,
}

fn s3key_filter_is_empty(f: &S3KeyFilter) -> bool {
    f.filter_rule_list.is_empty()
}

impl QueueConfig {
    pub fn validate(&self, region: &str, arn_list: &[String]) -> Result<(), ParseConfigError> {
        if self.events.is_empty() {
            return Err(ParseConfigError::MissingEventName);
        }
        let mut event_set = HashSet::new();
        for event in &self.events {
            // EventName::to_string() or similar for uniqueness check
            if !event_set.insert(event.to_string()) {
                return Err(ParseConfigError::DuplicateEventName(event.to_string()));
            }
        }
        self.filter.validate()?;

        // Validate ARN (similar to Go's Queue.Validate)
        // The Go code checks targetList.Exists(q.ARN.TargetID)
        // Here we check against a provided arn_list
        let _config_arn_str = self.arn.to_string();
        if !self.arn.region.is_empty() && self.arn.region != region {
            return Err(ParseConfigError::UnknownRegion(self.arn.region.clone()));
        }

        // Construct the ARN string that would be in arn_list
        // The arn_list contains ARNs like "arn:rustfs:sqs:REGION:ID:NAME"
        // We need to ensure self.arn (potentially with region adjusted) is in arn_list
        let effective_arn = ARN {
            target_id: self.arn.target_id.clone(),
            region: if self.arn.region.is_empty() {
                region.to_string()
            } else {
                self.arn.region.clone()
            },
            service: self.arn.service.clone(),     // or default "sqs"
            partition: self.arn.partition.clone(), // or default "rustfs"
        };

        if !arn_list.contains(&effective_arn.to_string()) {
            return Err(ParseConfigError::ArnNotFound(effective_arn.to_string()));
        }
        Ok(())
    }

    /// Sets the region if it's not already set in the ARN.
    pub fn set_region_if_empty(&mut self, region: &str) {
        if self.arn.region.is_empty() {
            self.arn.region = region.to_string();
        }
    }
}

/// Corresponding to the `lambda` structure in the Go code.
/// Used to parse <CloudFunction> ARN from inside the <CloudFunctionConfiguration> tag.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
pub struct LambdaConfigDetail {
    #[serde(rename = "CloudFunction")]
    pub arn: String,
    // According to AWS S3 documentation, <CloudFunctionConfiguration> usually also contains Id, Event, Filter
    // But in order to strictly correspond to the Go `lambda` structure provided, only ARN is included here.
    // If full support is required, additional fields can be added.
    // For example:
    // #[serde(rename = "Id", skip_serializing_if = "Option::is_none")]
    // pub id: Option<String>,
    // #[serde(rename = "Event", default, skip_serializing_if = "Vec::is_empty")]
    // pub events: Vec<EventName>,
    // #[serde(rename = "Filter", default, skip_serializing_if = "S3KeyFilterIsEmpty")]
    // pub filter: S3KeyFilter,
}

/// Corresponding to the `topic` structure in the Go code.
/// Used to parse <Topic> ARN from inside the <TopicConfiguration> tag.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
pub struct TopicConfigDetail {
    #[serde(rename = "Topic")]
    pub arn: String,
    // Similar to LambdaConfigDetail, it can be extended to include fields such as Id, Event, Filter, etc.
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(rename = "NotificationConfiguration")]
pub struct NotificationConfiguration {
    #[serde(rename = "xmlns", skip_serializing_if = "Option::is_none")]
    pub xmlns: Option<String>,
    #[serde(rename = "QueueConfiguration", default, skip_serializing_if = "Vec::is_empty")]
    pub queue_list: Vec<QueueConfig>,
    #[serde(
        rename = "CloudFunctionConfiguration", // Tags for each lambda configuration item in XML
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub lambda_list: Vec<LambdaConfigDetail>, // Modify: Use a new structure

    #[serde(
        rename = "TopicConfiguration", // Tags for each topic configuration item in XML
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub topic_list: Vec<TopicConfigDetail>, // Modify: Use a new structure
}

impl NotificationConfiguration {
    pub fn from_reader<R: Read + std::io::BufRead>(reader: R) -> Result<Self, ParseConfigError> {
        let config: NotificationConfiguration = quick_xml::de::from_reader(reader)?;
        Ok(config)
    }

    pub fn validate(&self, current_region: &str, arn_list: &[String]) -> Result<(), ParseConfigError> {
        // Verification logic remains the same: if lambda_list or topic_list is not empty, it is considered an unsupported configuration
        if !self.lambda_list.is_empty() || !self.topic_list.is_empty() {
            return Err(ParseConfigError::UnsupportedConfiguration);
        }

        let mut unique_queues = HashSet::new();
        for queue_config in &self.queue_list {
            queue_config.validate(current_region, arn_list)?;
            let queue_key = (
                queue_config.id.clone(),
                queue_config.arn.to_string(), // Assuming that the ARN structure implements Display or ToString
            );
            if !unique_queues.insert(queue_key.clone()) {
                return Err(ParseConfigError::DuplicateQueueConfiguration(queue_key.0, queue_key.1));
            }
        }
        Ok(())
    }

    pub fn set_defaults(&mut self, region: &str) {
        for queue_config in &mut self.queue_list {
            queue_config.set_region_if_empty(region);
        }
        if self.xmlns.is_none() {
            self.xmlns = Some("http://s3.amazonaws.com/doc/2006-03-01/".to_string());
        }
        // Note: If LambdaConfigDetail and TopicConfigDetail contain information such as regions in the future,
        // You may also need to set the default value here. But according to the current definition, they only contain ARN strings.
    }
}
