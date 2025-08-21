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

use super::pattern_rules::PatternRules;
use super::target_id_set::TargetIdSet;
use rustfs_targets::EventName;
use rustfs_targets::arn::TargetID;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// RulesMap - Rule mapping organized by event name。
/// `event.RulesMap` (map[Name]Rules) in the corresponding Go code
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RulesMap {
    map: HashMap<EventName, PatternRules>,
    /// A bitmask that represents the union of all event types in this map.
    /// Used for quick checks in `has_subscriber`.
    total_events_mask: u64,
}

impl RulesMap {
    /// Create a new, empty RulesMap.
    pub fn new() -> Self {
        Default::default()
    }

    /// Add a rule configuration to the map.
    ///
    /// This method handles composite event names (such as `s3:ObjectCreated:*`), expanding them as
    /// Multiple specific event types and add rules for each event type.
    ///
    /// # Parameters
    /// * `event_names` - List of event names associated with this rule.
    /// * `pattern` - Matching pattern for object keys. If empty, the default is `*` (match all).
    /// * `target_id` - The target ID of the notification.
    pub fn add_rule_config(&mut self, event_names: &[EventName], pattern: String, target_id: TargetID) {
        let effective_pattern = if pattern.is_empty() {
            "*".to_string() // Match all by default
        } else {
            pattern
        };

        for event_name_spec in event_names {
            // Expand compound event types, for example ObjectCreatedAll -> [ObjectCreatedPut, ObjectCreatedPost, ...]
            for expanded_event_name in event_name_spec.expand() {
                // Make sure EventName::expand() returns Vec<EventName>
                self.map
                    .entry(expanded_event_name)
                    .or_default()
                    .add(effective_pattern.clone(), target_id.clone());
                // Update the total_events_mask to include this event type
                self.total_events_mask |= expanded_event_name.mask();
            }
        }
    }

    /// Merge another RulesMap.
    /// `RulesMap.Add(rulesMap2 RulesMap) corresponding to Go
    pub fn add_map(&mut self, other_map: &Self) {
        for (event_name, other_pattern_rules) in &other_map.map {
            let self_pattern_rules = self.map.entry(*event_name).or_default();
            // PatternRules::union Returns the new PatternRules, we need to modify the existing ones
            let merged_rules = self_pattern_rules.union(other_pattern_rules);
            *self_pattern_rules = merged_rules;
        }
        // Directly merge two masks.
        self.total_events_mask |= other_map.total_events_mask;
    }

    /// Remove another rule defined in the RulesMap from the current RulesMap.
    ///
    /// After the rule is removed, `total_events_mask` is recalculated to ensure its accuracy.
    pub fn remove_map(&mut self, other_map: &Self) {
        let mut events_to_remove = Vec::new();
        for (event_name, self_pattern_rules) in &mut self.map {
            if let Some(other_pattern_rules) = other_map.map.get(event_name) {
                *self_pattern_rules = self_pattern_rules.difference(other_pattern_rules);
                if self_pattern_rules.is_empty() {
                    events_to_remove.push(*event_name);
                }
            }
        }
        for event_name in events_to_remove {
            self.map.remove(&event_name);
        }
        // After removing the rule, recalculate total_events_mask.
        self.recalculate_mask();
    }

    /// Checks whether any configured rules exist for a given event type.
    ///
    /// This method uses a bitmask for a quick check of O(1) complexity.
    /// `event_name` can be a compound type, such as `ObjectCreatedAll`.
    pub fn has_subscriber(&self, event_name: &EventName) -> bool {
        // event_name.mask() will handle compound events correctly
        (self.total_events_mask & event_name.mask()) != 0
    }

    /// Rules matching the given event and object keys and return all matching target IDs.
    ///
    /// # Notice
    /// The `event_name` parameter should be a specific, non-compound event type.
    /// Because this is taken from the `Event` object that actually occurs.
    pub fn match_rules(&self, event_name: EventName, object_key: &str) -> TargetIdSet {
        // Use bitmask to quickly determine whether there is a matching rule
        if (self.total_events_mask & event_name.mask()) == 0 {
            return TargetIdSet::new(); // No matching rules
        }

        // First try to directly match the event name
        if let Some(pattern_rules) = self.map.get(&event_name) {
            let targets = pattern_rules.match_targets(object_key);
            if !targets.is_empty() {
                return targets;
            }
        }
        // Go's RulesMap[eventName] is directly retrieved, and if it does not exist, it is empty Rules.
        // Rust's HashMap::get returns Option. If the event name does not exist, there is no rule.
        // Compound events (such as ObjectCreatedAll) have been expanded as a single event when add_rule_config.
        // Therefore, a single event name should be used when querying.
        // If event_name itself is a single type, look it up directly.
        // If event_name is a compound type, Go's logic is expanded when added.
        // Here match_rules should receive events that may already be single.
        // If the caller passes in a compound event, it should expand itself or handle this function first.
        // Assume that event_name is already a specific event that can be used for searching.
        self.map
            .get(&event_name)
            .map_or_else(TargetIdSet::new, |pr| pr.match_targets(object_key))
    }

    /// Check if RulesMap is empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns a clone of internal rules for use in scenarios such as BucketNotificationConfig::validate.
    pub fn inner(&self) -> &HashMap<EventName, PatternRules> {
        &self.map
    }

    /// A private helper function that recalculates `total_events_mask` based on the content of the current `map`.
    /// Called after the removal operation to ensure the accuracy of the mask.
    fn recalculate_mask(&mut self) {
        let mut new_mask = 0u64;
        for event_name in self.map.keys() {
            new_mask |= event_name.mask();
        }
        self.total_events_mask = new_mask;
    }

    /// Remove rules and optimize performance
    #[allow(dead_code)]
    pub fn remove_rule(&mut self, event_name: &EventName, pattern: &str) {
        if let Some(pattern_rules) = self.map.get_mut(event_name) {
            pattern_rules.rules.remove(pattern);
            if pattern_rules.is_empty() {
                self.map.remove(event_name);
            }
        }
        self.recalculate_mask(); // Delay calculation mask
    }

    /// Batch Delete Rules
    #[allow(dead_code)]
    pub fn remove_rules(&mut self, event_names: &[EventName]) {
        for event_name in event_names {
            self.map.remove(event_name);
        }
        self.recalculate_mask(); // Unified calculation of mask after batch processing
    }

    /// Update rules and optimize performance
    #[allow(dead_code)]
    pub fn update_rule(&mut self, event_name: EventName, pattern: String, target_id: TargetID) {
        self.map.entry(event_name).or_default().add(pattern, target_id);
        self.total_events_mask |= event_name.mask(); // Update only the relevant bitmask
    }
}
