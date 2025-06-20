use super::pattern_rules::PatternRules;
use super::target_id_set::TargetIdSet;
use crate::arn::TargetID;
use crate::event::EventName;
use std::collections::HashMap;

/// RulesMap - Rule mapping organized by event name。
/// `event.RulesMap` (map[Name]Rules) in the corresponding Go code
#[derive(Debug, Clone, Default)]
pub struct RulesMap {
    map: HashMap<EventName, PatternRules>,
}

impl RulesMap {
    pub fn new() -> Self {
        Default::default()
    }

    /// Add rule configuration.
    /// event_names: A set of event names。
    /// pattern: Object key pattern.
    /// target_id: Notify the target.
    ///
    /// This method expands the composite event name.
    pub fn add_rule_config(&mut self, event_names: &[EventName], pattern: String, target_id: TargetID) {
        let mut effective_pattern = pattern;
        if effective_pattern.is_empty() {
            effective_pattern = "*".to_string(); // Match all by default
        }

        for event_name_spec in event_names {
            for expanded_event_name in event_name_spec.expand() {
                // Make sure EventName::expand() returns Vec<EventName>
                self.map
                    .entry(expanded_event_name)
                    .or_default()
                    .add(effective_pattern.clone(), target_id.clone());
            }
        }
    }

    /// Merge another RulesMap.
    /// `RulesMap.Add(rulesMap2 RulesMap) corresponding to Go
    pub fn add_map(&mut self, other_map: &Self) {
        for (event_name, other_pattern_rules) in &other_map.map {
            let self_pattern_rules = self.map.entry(*event_name).or_default();
            // PatternRules::union 返回新的 PatternRules，我们需要修改现有的
            let merged_rules = self_pattern_rules.union(other_pattern_rules);
            *self_pattern_rules = merged_rules;
        }
    }

    /// Remove another rule defined in the RulesMap from the current RulesMap.
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
    }

    ///Rules matching the given event name and object key, returning all matching TargetIDs.
    pub fn match_rules(&self, event_name: EventName, object_key: &str) -> TargetIdSet {
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

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns a clone of internal rules for use in scenarios such as BucketNotificationConfig::validate.
    pub fn inner(&self) -> &HashMap<EventName, PatternRules> {
        &self.map
    }
}
