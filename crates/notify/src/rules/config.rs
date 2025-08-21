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

use super::rules_map::RulesMap;
use super::xml_config::ParseConfigError as BucketNotificationConfigError;
use crate::rules::NotificationConfiguration;
use crate::rules::pattern_rules;
use crate::rules::target_id_set;
use rustfs_targets::EventName;
use rustfs_targets::arn::TargetID;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Read;

/// Configuration for bucket notifications.
/// This struct now holds the parsed and validated rules in the new RulesMap format.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketNotificationConfig {
    pub region: String,  // Region where this config is applicable
    pub rules: RulesMap, // The new, more detailed RulesMap
}

impl BucketNotificationConfig {
    pub fn new(region: &str) -> Self {
        BucketNotificationConfig {
            region: region.to_string(),
            rules: RulesMap::new(),
        }
    }

    /// Adds a rule to the configuration.
    /// This method allows adding a rule with a specific event and target ID.
    pub fn add_rule(
        &mut self,
        event_names: &[EventName], // Assuming event_names is a list of event names
        pattern: String,           // The object key pattern for the rule
        target_id: TargetID,       // The target ID for the notification
    ) {
        self.rules.add_rule_config(event_names, pattern, target_id);
    }

    /// Parses notification configuration from XML.
    /// `arn_list` is a list of valid ARN strings for validation.
    pub fn from_xml<R: Read + std::io::BufRead>(
        reader: R,
        current_region: &str,
        arn_list: &[String],
    ) -> Result<Self, BucketNotificationConfigError> {
        let mut parsed_config = NotificationConfiguration::from_reader(reader)?;

        // Set defaults (region in ARNs if empty, xmlns) before validation
        parsed_config.set_defaults(current_region);

        // Validate the parsed configuration
        parsed_config.validate(current_region, arn_list)?;

        let mut rules_map = RulesMap::new();
        for queue_conf in parsed_config.queue_list {
            // The ARN in queue_conf should now have its region set if it was originally empty.
            // Ensure TargetID can be cloned or extracted correctly.
            let target_id = queue_conf.arn.target_id.clone();
            let pattern_str = queue_conf.filter.filter_rule_list.pattern();
            rules_map.add_rule_config(&queue_conf.events, pattern_str, target_id);
        }

        Ok(BucketNotificationConfig {
            region: current_region.to_string(), // Config is for the current_region
            rules: rules_map,
        })
    }

    /// Validates the *current* BucketNotificationConfig.
    /// This might be redundant if construction always implies validation.
    /// However, Go's Config has a Validate method.
    /// The primary validation now happens during `from_xml` via `NotificationConfiguration::validate`.
    /// This method could re-check against an updated arn_list or region if needed.
    pub fn validate(&self, current_region: &str, arn_list: &[String]) -> Result<(), BucketNotificationConfigError> {
        if self.region != current_region {
            return Err(BucketNotificationConfigError::RegionMismatch {
                config_region: self.region.clone(),
                current_region: current_region.to_string(),
            });
        }

        // Iterate through the rules in self.rules and validate their TargetIDs against arn_list
        // This requires RulesMap to expose its internal structure or provide an iterator
        for (_event_name, pattern_rules) in self.rules.inner().iter() {
            for (_pattern, target_id_set) in pattern_rules.inner().iter() {
                // Assuming PatternRules has inner()
                for target_id in target_id_set {
                    // Construct the ARN string for this target_id and self.region
                    let arn_to_check = target_id.to_arn(&self.region); // Assuming TargetID has to_arn
                    if !arn_list.contains(&arn_to_check.to_arn_string()) {
                        return Err(BucketNotificationConfigError::ArnNotFound(arn_to_check.to_arn_string()));
                    }
                }
            }
        }
        Ok(())
    }

    // Expose the RulesMap for the notifier
    pub fn get_rules_map(&self) -> &RulesMap {
        &self.rules
    }

    /// Sets the region for the configuration
    pub fn set_region(&mut self, region: &str) {
        self.region = region.to_string();
    }
}

// Add a helper to PatternRules if not already present
impl pattern_rules::PatternRules {
    pub fn inner(&self) -> &HashMap<String, target_id_set::TargetIdSet> {
        &self.rules
    }
}
