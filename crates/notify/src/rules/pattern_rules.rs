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

use crate::rules::TargetIdSet;
use crate::rules::pattern;
use hashbrown::HashMap;
use rayon::prelude::*;
use rustfs_targets::arn::TargetID;
use serde::{Deserialize, Serialize};

/// PatternRules - Event rule that maps object name patterns to TargetID collections.
/// `event.Rules` (map[string]TargetIDSet) in the Go code
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PatternRules {
    pub(crate) rules: HashMap<String, TargetIdSet>,
}

impl PatternRules {
    /// Create a new, empty PatternRules.
    pub fn new() -> Self {
        Default::default()
    }

    /// Add rules: Pattern and Target ID.
    /// If the schema already exists, add target_id to the existing TargetIdSet.
    ///
    /// # Arguments
    /// * `pattern` - The object name pattern.
    /// * `target_id` - The TargetID to associate with the pattern.
    pub fn add(&mut self, pattern: String, target_id: TargetID) {
        self.rules.entry(pattern).or_default().insert(target_id);
    }

    /// Checks if there are any rules that match the given object name.
    ///
    /// # Arguments
    /// * `object_name` - The object name to match against the patterns.
    ///
    /// # Returns
    /// `true` if any pattern matches the object name, otherwise `false`.
    pub fn match_simple(&self, object_name: &str) -> bool {
        self.rules.keys().any(|p| pattern::match_simple(p, object_name))
    }

    /// Returns all TargetIDs that match the object name.
    ///
    /// Performance optimization points:
    /// 1) Small collections are serialized directly to avoid rayon scheduling/merging overhead
    /// 2) When hitting, no longer temporarily allocate TargetIdSet for each rule, but directly extend
    ///
    /// # Arguments
    /// * `object_name` - The object name to match against the patterns.
    ///
    /// # Returns
    /// A TargetIdSet containing all TargetIDs that match the object name.
    pub fn match_targets(&self, object_name: &str) -> TargetIdSet {
        let n = self.rules.len();
        if n == 0 {
            return TargetIdSet::new();
        }

        // Experience Threshold: Serial is usually faster below this value (can be adjusted after benchmarking)
        const PAR_THRESHOLD: usize = 128;

        if n < PAR_THRESHOLD {
            let mut out = TargetIdSet::new();
            for (pattern_str, target_set) in self.rules.iter() {
                if pattern::match_simple(pattern_str, object_name) {
                    out.extend(target_set.iter().cloned());
                }
            }
            return out;
        }
        // Parallel path: Each thread accumulates a local set and finally merges it to reduce frequent allocations
        self.rules
            .par_iter()
            .fold(TargetIdSet::new, |mut local, (pattern_str, target_set)| {
                if pattern::match_simple(pattern_str, object_name) {
                    local.extend(target_set.iter().cloned());
                }
                local
            })
            .reduce(TargetIdSet::new, |mut acc, set| {
                acc.extend(set);
                acc
            })
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Merge another PatternRules.
    /// Corresponding to Go's `Rules.Union`.
    /// # Arguments
    /// * `other` - The PatternRules to merge with.
    ///
    /// # Returns
    /// A new PatternRules containing the union of both.
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
    /// The result contains only the patterns and TargetIDs that are in `self` but not in `other`.
    ///
    /// # Arguments
    /// * `other` - The PatternRules to compare against.
    ///
    /// # Returns
    /// A new PatternRules containing the difference.
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

    /// Merge another PatternRules into self in place.
    /// Corresponding to Go's `Rules.UnionInPlace`.
    /// # Arguments
    /// * `other` - The PatternRules to merge with.
    pub fn union_in_place(&mut self, other: &Self) {
        for (pattern, their_targets) in &other.rules {
            self.rules
                .entry(pattern.clone())
                .or_default()
                .extend(their_targets.iter().cloned());
        }
    }

    /// Calculate the difference from another PatternRules in place.
    /// Corresponding to Go's `Rules.DifferenceInPlace`.
    /// The result contains only the patterns and TargetIDs that are in `self` but not in `other`.
    /// # Arguments
    /// * `other` - The PatternRules to compare against.
    pub fn difference_in_place(&mut self, other: &Self) {
        self.rules.retain(|pattern, self_targets| {
            if let Some(other_targets) = other.rules.get(pattern) {
                // Remove other_targets from self_targets
                self_targets.retain(|tid| !other_targets.contains(tid));
            }
            !self_targets.is_empty()
        });
    }

    /// Remove a pattern and its associated TargetID set from the PatternRules.
    ///
    /// # Arguments
    /// * `pattern` - The pattern to remove.
    pub fn remove_pattern(&mut self, pattern: &str) -> bool {
        self.rules.remove(pattern).is_some()
    }

    /// Determine whether the current PatternRules contains the specified TargetID (referenced by any pattern).
    ///
    /// # Parameters
    /// * `target_id` - The TargetID to check for existence within the PatternRules
    ///
    /// # Returns
    /// * `true` if the TargetID exists in any of the patterns; `false` otherwise.
    pub fn contains_target_id(&self, target_id: &TargetID) -> bool {
        self.rules.values().any(|set| set.contains(target_id))
    }

    /// Expose the internal rules for use in scenarios such as BucketNotificationConfig::validate.
    ///
    /// # Returns
    /// A reference to the internal HashMap of patterns to TargetIdSets.
    pub fn inner(&self) -> &HashMap<String, TargetIdSet> {
        &self.rules
    }
}
