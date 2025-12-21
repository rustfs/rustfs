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

use crate::rules::{PatternRules, TargetIdSet};
use hashbrown::HashMap;
use rustfs_targets::EventName;
use rustfs_targets::arn::TargetID;
use serde::{Deserialize, Serialize};

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
    ///
    /// # Returns
    /// A new instance of RulesMap with an empty map and a total_events_mask set to 0.
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
    ///
    /// # Parameters
    /// * `other_map` - The other RulesMap to be merged into the current one.
    pub fn add_map(&mut self, other_map: &Self) {
        for (event_name, other_pattern_rules) in &other_map.map {
            self.map.entry(*event_name).or_default().union_in_place(other_pattern_rules);
        }
        // Directly merge two masks.
        self.total_events_mask |= other_map.total_events_mask;
    }

    /// Remove another rule defined in the RulesMap from the current RulesMap.
    ///
    /// After the rule is removed, `total_events_mask` is recalculated to ensure its accuracy.
    ///
    /// # Parameters
    /// * `other_map` - The other RulesMap containing rules to be removed from the current one.
    pub fn remove_map(&mut self, other_map: &Self) {
        let mut events_to_remove = Vec::new();
        for (event_name, self_pattern_rules) in &mut self.map {
            if let Some(other_pattern_rules) = other_map.map.get(event_name) {
                self_pattern_rules.difference_in_place(other_pattern_rules);
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
    ///
    /// # Parameters
    /// * `event_name` - The event name to check for subscribers.
    pub fn has_subscriber(&self, event_name: &EventName) -> bool {
        // event_name.mask() will handle compound events correctly
        (self.total_events_mask & event_name.mask()) != 0
    }

    /// Rules matching the given event and object keys and return all matching target IDs.
    ///
    /// # Notice
    /// The `event_name` parameter should be a specific, non-compound event type.
    /// Because this is taken from the `Event` object that actually occurs.
    ///
    /// # Parameters
    /// * `event_name` - The specific event name to match against.
    /// * `object_key` - The object key to match against the patterns in the rules.
    ///
    /// # Returns
    /// * A set of TargetIDs that match the given event and object key.
    pub fn match_rules(&self, event_name: EventName, object_key: &str) -> TargetIdSet {
        // Use bitmask to quickly determine whether there is a matching rule
        if (self.total_events_mask & event_name.mask()) == 0 {
            return TargetIdSet::new(); // No matching rules
        }

        // In Go, RulesMap[eventName] returns empty rules if the key doesn't exist.
        // Rust's HashMap::get returns Option, so missing key means no rules.
        // Compound events like ObjectCreatedAll are expanded into specific events during add_rule_config.
        // Thus, queries should use specific event names.
        // If event_name is compound, expansion happens at addition time.
        // match_rules assumes event_name is already a specific event for lookup.
        // Callers should expand compound events before calling this method.
        self.map
            .get(&event_name)
            .map_or_else(TargetIdSet::new, |pr| pr.match_targets(object_key))
    }

    /// Check if RulesMap is empty.
    ///
    /// # Returns
    /// * `true` if there are no rules in the map; `false` otherwise
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Determine whether the current RulesMap contains the specified TargetID (referenced by any event / pattern).
    ///
    /// # Parameters
    /// * `target_id` - The TargetID to check for existence within the RulesMap
    ///
    /// # Returns
    /// * `true` if the TargetID exists in any of the PatternRules; `false` otherwise.
    pub fn contains_target_id(&self, target_id: &TargetID) -> bool {
        self.map.values().any(|pr| pr.contains_target_id(target_id))
    }

    /// Returns a clone of internal rules for use in scenarios such as BucketNotificationConfig::validate.
    ///
    /// # Returns
    /// A reference to the internal HashMap of EventName to PatternRules.
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
    ///
    /// # Parameters
    /// * `event_name` - The EventName from which to remove the rule.
    /// * `pattern` - The pattern of the rule to be removed.
    #[allow(dead_code)]
    pub fn remove_rule(&mut self, event_name: &EventName, pattern: &str) {
        let mut remove_event = false;

        if let Some(pattern_rules) = self.map.get_mut(event_name) {
            pattern_rules.remove_pattern(pattern);
            if pattern_rules.is_empty() {
                remove_event = true;
            }
        }

        if remove_event {
            self.map.remove(event_name);
        }

        self.recalculate_mask(); // Delay calculation mask
    }

    /// Batch Delete Rules and Optimize Performance
    ///
    /// # Parameters
    /// * `event_names` - A slice of EventNames to be removed.
    #[allow(dead_code)]
    pub fn remove_rules(&mut self, event_names: &[EventName]) {
        for event_name in event_names {
            self.map.remove(event_name);
        }
        self.recalculate_mask(); // Unified calculation of mask after batch processing
    }

    /// Update rules and optimize performance
    ///
    /// # Parameters
    /// * `event_name` - The EventName to update.
    /// * `pattern` - The pattern of the rule to be updated.
    /// * `target_id` - The TargetID to be added.
    #[allow(dead_code)]
    pub fn update_rule(&mut self, event_name: EventName, pattern: String, target_id: TargetID) {
        self.map.entry(event_name).or_default().add(pattern, target_id);
        self.total_events_mask |= event_name.mask(); // Update only the relevant bitmask
    }

    /// Iterate all EventName keys contained in this RulesMap.
    ///
    /// Used by snapshot compilation to compute bucket event_mask.
    ///
    /// # Returns
    /// An iterator over all EventName keys in the RulesMap.
    #[inline]
    pub fn iter_events(&self) -> impl Iterator<Item = EventName> + '_ {
        // `inner()` is already used by config.rs, so we reuse it here.
        // If the key type is `EventName`, `.copied()` is the cheapest way to return values.
        self.inner().keys().copied()
    }
}
