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

use rustfs_targets::EventName;
use std::sync::Arc;

/// Let the rules structure provide "what events it is subscribed to".
/// This way BucketRulesSnapshot does not need to know the internal shape of rules.
pub trait RuleEvents {
    fn subscribed_events(&self) -> &[EventName];
}

/// Let the rules container provide the ability to iterate over all rules (abstracting only to the minimum necessary).
pub trait RulesContainer {
    type Rule: RuleEvents + ?Sized;
    fn iter_rules<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Self::Rule> + 'a>;

    /// Fast empty judgment for snapshots (fix missing `rules.is_empty()`)
    fn is_empty(&self) -> bool {
        self.iter_rules().next().is_none()
    }
}

/// Represents a bucket's notification subscription view snapshot (immutable).
///
/// - `event_mask`: Quickly determine whether there is a subscription to a certain type of event (bitset/flags).
/// - `rules`: precise rule mapping (prefix/suffix/pattern -> targets).
///
/// The read path only reads this snapshot to ensure consistency.
#[derive(Debug, Clone)]
pub struct BucketRulesSnapshot<R>
where
    R: RulesContainer + ?Sized,
{
    pub event_mask: u64,
    pub rules: Arc<R>,
}

impl<R> BucketRulesSnapshot<R>
where
    R: RulesContainer + ?Sized,
{
    /// Create an empty snapshot with no subscribed events and no rules.
    ///
    /// # Arguments
    /// * `rules` - An Arc to a rules container (can be an empty container).
    ///
    /// # Returns
    /// An instance of `BucketRulesSnapshot` with an empty event mask.
    #[inline]
    pub fn empty(rules: Arc<R>) -> Self {
        Self { event_mask: 0, rules }
    }

    /// Check if the snapshot has any subscribers for the specified event.
    ///
    /// # Arguments
    /// * `event` - The event name to check for subscriptions.
    ///
    /// # Returns
    /// `true` if there are subscribers for the event, `false` otherwise.
    #[inline]
    pub fn has_event(&self, event: &EventName) -> bool {
        (self.event_mask & event.mask()) != 0
    }

    /// Check if the snapshot is empty (no subscribed events or rules).
    ///
    /// # Returns
    /// `true` if the snapshot is empty, `false` otherwise.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.event_mask == 0 || self.rules.is_empty()
    }

    /// [debug] Assert that `event_mask` is consistent with the event declared in `rules`.
    ///
    /// Constraints:
    /// - only runs in debug builds (release incurs no cost).
    /// - If the rule contains compound events (\*All / Everything), rely on `EventName::mask()` to automatically expand.
    #[inline]
    pub fn debug_assert_mask_consistent(&self) {
        #[cfg(debug_assertions)]
        {
            let mut recomputed = 0u64;
            for rule in self.rules.iter_rules() {
                for ev in rule.subscribed_events() {
                    recomputed |= ev.mask();
                }
            }

            debug_assert!(
                recomputed == self.event_mask,
                "BucketRulesSnapshot.event_mask inconsistent: stored={:#x}, recomputed={:#x}",
                self.event_mask,
                recomputed
            );
        }
    }
}

/// Unify trait-object snapshot types (fix Sized / missing generic arguments)
pub type DynRulesContainer = dyn RulesContainer<Rule = dyn RuleEvents> + Send + Sync;

/// Expose Arc form to facilitate sharing.
pub type BucketSnapshotRef = Arc<BucketRulesSnapshot<DynRulesContainer>>;
