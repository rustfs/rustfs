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
    pub fn add_rule_config(
        &mut self,
        event_names: &[EventName],
        pattern: String,
        target_id: TargetID,
    ) {
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

    /// 从当前 RulesMap 中移除另一个 RulesMap 中定义的规则。
    /// 对应 Go 的 `RulesMap.Remove(rulesMap2 RulesMap)`
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

    /// 匹配给定事件名称和对象键的规则，返回所有匹配的 TargetID。
    pub fn match_rules(&self, event_name: EventName, object_key: &str) -> TargetIdSet {
        // 首先尝试直接匹配事件名称
        if let Some(pattern_rules) = self.map.get(&event_name) {
            let targets = pattern_rules.match_targets(object_key);
            if !targets.is_empty() {
                return targets;
            }
        }
        // Go 的 RulesMap[eventName] 直接获取，如果不存在则为空 Rules。
        // Rust 的 HashMap::get 返回 Option。如果事件名不存在，则没有规则。
        // 复合事件（如 ObjectCreatedAll）在 add_rule_config 时已展开为单一事件。
        // 因此，查询时应使用单一事件名称。
        // 如果 event_name 本身就是单一类型，则直接查找。
        // 如果 event_name 是复合类型，Go 的逻辑是在添加时展开。
        // 这里的 match_rules 应该接收已经可能是单一的事件。
        // 如果调用者传入的是复合事件，它应该先自行展开或此函数处理。
        // 假设 event_name 已经是具体的、可用于查找的事件。
        self.map
            .get(&event_name)
            .map_or_else(TargetIdSet::new, |pr| pr.match_targets(object_key))
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// 返回内部规则的克隆，用于 BucketNotificationConfig::validate 等场景。
    pub fn inner(&self) -> &HashMap<EventName, PatternRules> {
        &self.map
    }
}
