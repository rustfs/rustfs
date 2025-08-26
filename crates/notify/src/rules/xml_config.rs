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

use super::pattern;
use rustfs_targets::EventName;
use rustfs_targets::arn::{ARN, ArnError, TargetIDError};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
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
        if self.name != "prefix" && self.name != "suffix" {
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

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct FilterRuleList {
    #[serde(rename = "FilterRule", default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<FilterRule>,
}

impl FilterRuleList {
    pub fn validate(&self) -> Result<(), ParseConfigError> {
        let mut has_prefix = false;
        let mut has_suffix = false;
        for rule in &self.rules {
            rule.validate()?;
            if rule.name == "prefix" {
                if has_prefix {
                    return Err(ParseConfigError::DuplicatePrefixFilter);
                }
                has_prefix = true;
            } else if rule.name == "suffix" {
                if has_suffix {
                    return Err(ParseConfigError::DuplicateSuffixFilter);
                }
                has_suffix = true;
            }
        }
        Ok(())
    }

    pub fn pattern(&self) -> String {
        let mut prefix_val: Option<&str> = None;
        let mut suffix_val: Option<&str> = None;

        for rule in &self.rules {
            if rule.name == "prefix" {
                prefix_val = Some(&rule.value);
            } else if rule.name == "suffix" {
                suffix_val = Some(&rule.value);
            }
        }
        pattern::new_pattern(prefix_val, suffix_val)
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct S3KeyFilter {
    #[serde(rename = "FilterRuleList", default, skip_serializing_if = "FilterRuleList::is_empty")]
    pub filter_rule_list: FilterRuleList,
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
        self.filter.filter_rule_list.validate()?;

        // Validate ARN (similar to Go's Queue.Validate)
        // The Go code checks targetList.Exists(q.ARN.TargetID)
        // Here we check against a provided arn_list
        let _config_arn_str = self.arn.to_arn_string();
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

        if !arn_list.contains(&effective_arn.to_arn_string()) {
            return Err(ParseConfigError::ArnNotFound(effective_arn.to_arn_string()));
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
                queue_config.arn.to_arn_string(), // Assuming that the ARN structure implements Display or ToString
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
