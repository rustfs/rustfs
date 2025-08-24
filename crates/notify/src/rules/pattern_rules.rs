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
use super::target_id_set::TargetIdSet;
use rustfs_targets::arn::TargetID;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// PatternRules - Event rule that maps object name patterns to TargetID collections.
/// `event.Rules` (map[string]TargetIDSet) in the Go code
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PatternRules {
    pub(crate) rules: HashMap<String, TargetIdSet>,
}

impl PatternRules {
    pub fn new() -> Self {
        Default::default()
    }

    /// Add rules: Pattern and Target ID.
    /// If the schema already exists, add target_id to the existing TargetIdSet.
    pub fn add(&mut self, pattern: String, target_id: TargetID) {
        self.rules.entry(pattern).or_default().insert(target_id);
    }

    /// Checks if there are any rules that match the given object name.
    pub fn match_simple(&self, object_name: &str) -> bool {
        self.rules.keys().any(|p| pattern::match_simple(p, object_name))
    }

    /// Returns all TargetIDs that match the object name.
    pub fn match_targets(&self, object_name: &str) -> TargetIdSet {
        let mut matched_targets = TargetIdSet::new();
        for (pattern_str, target_set) in &self.rules {
            if pattern::match_simple(pattern_str, object_name) {
                matched_targets.extend(target_set.iter().cloned());
            }
        }
        matched_targets
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Merge another PatternRules.
    /// Corresponding to Go's `Rules.Union`.
    pub fn union(&self, other: &Self) -> Self {
        let mut new_rules = self.clone();
        for (pattern, their_targets) in &other.rules {
            let our_targets = new_rules.rules.entry(pattern.clone()).or_default();
            our_targets.extend(their_targets.iter().cloned());
        }
        new_rules
    }

    /// Calculate the difference from another PatternRules.
    /// Corresponding to Go's `Rules.Difference`.
    pub fn difference(&self, other: &Self) -> Self {
        let mut result_rules = HashMap::new();
        for (pattern, self_targets) in &self.rules {
            match other.rules.get(pattern) {
                Some(other_targets) => {
                    let diff_targets: TargetIdSet = self_targets.difference(other_targets).cloned().collect();
                    if !diff_targets.is_empty() {
                        result_rules.insert(pattern.clone(), diff_targets);
                    }
                }
                None => {
                    // If there is no pattern in other, self_targets are all retained
                    result_rules.insert(pattern.clone(), self_targets.clone());
                }
            }
        }
        PatternRules { rules: result_rules }
    }
}
