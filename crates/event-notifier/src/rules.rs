// src/rules.rs
use crate::event_name::EventName;
use crate::target::TargetID;
use std::collections::{HashMap, HashSet};
use wildmatch::WildMatch;

/// TargetIDSet represents a set of target IDs
#[derive(Debug, Clone, Default)]
pub struct TargetIDSet(HashSet<TargetID>);

impl TargetIDSet {
    pub fn new() -> Self {
        Self(HashSet::new())
    }

    pub fn insert(&mut self, target_id: TargetID) -> bool {
        self.0.insert(target_id)
    }

    pub fn contains(&self, target_id: &TargetID) -> bool {
        self.0.contains(target_id)
    }

    pub fn remove(&mut self, target_id: &TargetID) -> bool {
        self.0.remove(target_id)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }

    pub fn iter(&self) -> impl Iterator<Item = &TargetID> {
        self.0.iter()
    }

    pub fn extend<I: IntoIterator<Item = TargetID>>(&mut self, iter: I) {
        self.0.extend(iter)
    }

    pub fn union(&self, other: &TargetIDSet) -> TargetIDSet {
        let mut result = self.clone();
        result.extend(other.0.iter().cloned());
        result
    }

    pub fn difference(&self, other: &TargetIDSet) -> TargetIDSet {
        let mut result = TargetIDSet::new();
        for id in self.0.iter() {
            if !other.contains(id) {
                result.insert(id.clone());
            }
        }
        result
    }
}

impl FromIterator<TargetID> for TargetIDSet {
    fn from_iter<I: IntoIterator<Item = TargetID>>(iter: I) -> Self {
        let mut set = TargetIDSet::new();
        set.extend(iter);
        set
    }
}

#[derive(Debug, Clone)]
pub struct Rules {
    patterns: HashMap<String, TargetIDSet>,
}

impl Rules {
    pub fn new() -> Self {
        Self {
            patterns: HashMap::new(),
        }
    }

    pub fn add(&mut self, pattern: String, target_id: TargetID) {
        let entry = self.patterns.entry(pattern).or_insert_with(TargetIDSet::new);
        entry.insert(target_id);
    }

    pub fn match_object(&self, object_name: &str) -> bool {
        for pattern in self.patterns.keys() {
            if WildMatch::new(pattern).matches(object_name) {
                return true;
            }
        }
        false
    }

    pub fn match_targets(&self, object_name: &str) -> TargetIDSet {
        let mut target_ids = TargetIDSet::new();

        for (pattern, ids) in &self.patterns {
            if WildMatch::new(pattern).matches(object_name) {
                target_ids.extend(ids.iter().cloned());
            }
        }

        target_ids
    }
}

#[derive(Debug, Default)]
pub struct RulesMap {
    rules: HashMap<EventName, Rules>,
}

impl RulesMap {
    pub fn new() -> Self {
        Self { rules: HashMap::new() }
    }

    pub fn add(&mut self, events: &[EventName], pattern: String, target_id: TargetID) {
        for &event in events {
            for expanded_event in event.expand() {
                let rules = self.rules.entry(expanded_event).or_insert_with(Rules::new);
                rules.add(pattern.clone(), target_id.clone());
            }
        }
    }

    pub fn match_simple(&self, event: EventName, object_name: &str) -> bool {
        match self.rules.get(&event) {
            Some(rules) => rules.match_object(object_name),
            None => false,
        }
    }

    pub fn match_targets(&self, event: EventName, object_name: &str) -> TargetIDSet {
        match self.rules.get(&event) {
            Some(rules) => rules.match_targets(object_name),
            None => TargetIDSet::new(),
        }
    }
}

impl Clone for RulesMap {
    fn clone(&self) -> Self {
        Self {
            rules: self.rules.clone(),
        }
    }
}

impl Default for Rules {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_name::EventName;

    #[test]
    fn test_rules() {
        let mut rules = Rules::new();
        rules.add("*.txt".to_string(), "target1".to_string());
        rules.add("*.jpg".to_string(), "target2".to_string());

        assert!(rules.match_object("file.txt"));
        assert!(!rules.match_object("file.pdf"));
        assert!(rules.match_object("image.jpg"));
        assert!(!rules.match_object("image.png"));

        let target_ids = rules.match_targets("file.txt");
        assert_eq!(target_ids.len(), 1);
        assert!(target_ids.iter().any(|id| id == "target1"));

        let mut rules_map = RulesMap::new();
        let target_id = "target1".to_string();
        // 添加规则
        rules_map.add(&[EventName::ObjectCreatedAll], String::from("images/*"), target_id);

        // 匹配对象
        let matches = rules_map.match_simple(EventName::ObjectCreatedPut, "images/photo.jpg"); // returns true

        // 获取匹配的目标
        let target_ids = rules_map.match_targets(EventName::ObjectCreatedPut, "images/photo.jpg");
    }
}
