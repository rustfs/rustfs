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

//! Per-bucket attribution of namespace commits for the scanner.
//!
//! A namespace commit is registered here under its bucket (and object, when it
//! has one) before its first disk effect. When its last owner drops, after its
//! last disk task, the bucket is stamped with a monotonic end sequence. The
//! scanner reads the sequence before a walk and, after the walk, asks for every
//! bucket that is still in flight or ended past that floor: those buckets may
//! have been walked mid-commit and stay grey instead of vetoing the cycle.
//! Commits that cannot be attributed (unscoped owners, or an ended card that
//! did not fit the table) are reported as such and keep the whole-cycle
//! barrier.

use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Distinct buckets the ended table may hold. An end that does not fit is
/// reported as unattributed to any walk that overlaps it.
pub(crate) const MAX_NAMESPACE_COMMIT_CARD_BUCKETS: usize = 65_536;

/// Objects kept per bucket before the card collapses to the whole bucket. It
/// matches the scanner's top-level-entry bound for one dirty bucket.
pub(crate) const MAX_NAMESPACE_COMMIT_CARD_OBJECTS_PER_BUCKET: usize = 128;

/// Object names kept across all ended cards. Past it, a bucket that would add
/// another name collapses to the whole bucket, so a scanner that stops
/// draining cannot grow the table beyond its bucket bound.
pub(crate) const MAX_NAMESPACE_COMMIT_CARD_OBJECTS: usize = 65_536;

/// The part of a bucket that a namespace commit may have changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScannerNamespaceCommitCardScope {
    WholeBucket,
    Objects(BTreeSet<String>),
}

/// Buckets whose namespace commits overlap a scanner walk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScannerNamespaceCommitCards {
    pub buckets: BTreeMap<String, ScannerNamespaceCommitCardScope>,
    /// A commit overlapping the walk could not be attributed to a bucket.
    pub unattributed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct NamespaceCommitKey {
    bucket: String,
    object: Option<String>,
}

impl NamespaceCommitKey {
    pub(crate) fn new(bucket: &str, object: Option<&str>) -> Self {
        Self {
            bucket: bucket.to_string(),
            object: object.filter(|object| !object.is_empty()).map(str::to_string),
        }
    }
}

#[derive(Debug)]
struct EndedCard {
    last_end: u64,
    scope: ScannerNamespaceCommitCardScope,
}

#[derive(Debug, Default)]
pub(crate) struct NamespaceCommitCardTable {
    end_sequence: u64,
    in_flight: HashMap<NamespaceCommitKey, usize>,
    unscoped: usize,
    ended: BTreeMap<String, EndedCard>,
    /// Object names held by `ended`.
    ended_objects: usize,
    /// End sequence of the latest end that did not fit `ended`.
    overflow_end: u64,
}

fn scope_objects(scope: &ScannerNamespaceCommitCardScope) -> usize {
    match scope {
        ScannerNamespaceCommitCardScope::WholeBucket => 0,
        ScannerNamespaceCommitCardScope::Objects(objects) => objects.len(),
    }
}

fn merge_scope(scope: &mut ScannerNamespaceCommitCardScope, object: Option<&String>) {
    match (&mut *scope, object) {
        (ScannerNamespaceCommitCardScope::WholeBucket, _) => {}
        (ScannerNamespaceCommitCardScope::Objects(_), None) => *scope = ScannerNamespaceCommitCardScope::WholeBucket,
        (ScannerNamespaceCommitCardScope::Objects(objects), Some(object)) => {
            objects.insert(object.clone());
            if objects.len() > MAX_NAMESPACE_COMMIT_CARD_OBJECTS_PER_BUCKET {
                *scope = ScannerNamespaceCommitCardScope::WholeBucket;
            }
        }
    }
}

fn empty_scope() -> ScannerNamespaceCommitCardScope {
    ScannerNamespaceCommitCardScope::Objects(BTreeSet::new())
}

impl NamespaceCommitCardTable {
    pub(crate) fn begin(&mut self, key: Option<&NamespaceCommitKey>) {
        match key {
            Some(key) => *self.in_flight.entry(key.clone()).or_default() += 1,
            None => self.unscoped = self.unscoped.saturating_add(1),
        }
    }

    pub(crate) fn end(&mut self, key: Option<&NamespaceCommitKey>) {
        let Some(key) = key else {
            self.unscoped = self.unscoped.saturating_sub(1);
            return;
        };
        if let Some(count) = self.in_flight.get_mut(key) {
            *count -= 1;
            if *count == 0 {
                self.in_flight.remove(key);
            }
        }
        // Saturation only merges later ends into the same sequence; a floor
        // read at `u64::MAX` then reports every card, which is conservative.
        self.end_sequence = self.end_sequence.saturating_add(1);
        let end = self.end_sequence;
        if !self.ended.contains_key(&key.bucket) && self.ended.len() >= MAX_NAMESPACE_COMMIT_CARD_BUCKETS {
            self.overflow_end = end;
            return;
        }
        let card = self.ended.entry(key.bucket.clone()).or_insert_with(|| EndedCard {
            last_end: end,
            scope: empty_scope(),
        });
        card.last_end = end;
        let before = scope_objects(&card.scope);
        if self.ended_objects >= MAX_NAMESPACE_COMMIT_CARD_OBJECTS
            && key.object.as_ref().is_some_and(
                |object| matches!(&card.scope, ScannerNamespaceCommitCardScope::Objects(objects) if !objects.contains(object)),
            )
        {
            card.scope = ScannerNamespaceCommitCardScope::WholeBucket;
        } else {
            merge_scope(&mut card.scope, key.object.as_ref());
        }
        self.ended_objects = self.ended_objects - before + scope_objects(&card.scope);
    }

    /// Sequence a walk records before it starts reading.
    pub(crate) fn floor(&self) -> u64 {
        self.end_sequence
    }

    /// Whether an in-flight commit cannot be attributed to a bucket.
    pub(crate) fn unscoped_in_flight(&self) -> bool {
        self.unscoped != 0
    }

    /// Every bucket with a commit in flight or ended after `floor`. Ended cards
    /// at or below `floor` ended before the walk that recorded it started and
    /// are pruned: one collector runs per instance and its floors only grow.
    pub(crate) fn overlapping(&mut self, floor: u64) -> ScannerNamespaceCommitCards {
        self.ended.retain(|_, card| card.last_end > floor || floor == u64::MAX);
        self.ended_objects = self.ended.values().map(|card| scope_objects(&card.scope)).sum();
        let mut buckets = self
            .ended
            .iter()
            .map(|(bucket, card)| (bucket.clone(), card.scope.clone()))
            .collect::<BTreeMap<_, _>>();
        for key in self.in_flight.keys() {
            merge_scope(buckets.entry(key.bucket.clone()).or_insert_with(empty_scope), key.object.as_ref());
        }
        for scope in buckets.values_mut() {
            if matches!(scope, ScannerNamespaceCommitCardScope::Objects(objects) if objects.is_empty()) {
                *scope = ScannerNamespaceCommitCardScope::WholeBucket;
            }
        }
        ScannerNamespaceCommitCards {
            buckets,
            unattributed: self.unscoped != 0 || self.overflow_end > floor || floor == u64::MAX,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn objects(names: &[&str]) -> ScannerNamespaceCommitCardScope {
        ScannerNamespaceCommitCardScope::Objects(names.iter().map(|name| name.to_string()).collect())
    }

    #[test]
    fn a_walk_sees_commits_in_flight_or_ended_after_its_floor() {
        let mut table = NamespaceCommitCardTable::default();
        let before = NamespaceCommitKey::new("before", Some("a"));
        table.begin(Some(&before));
        table.end(Some(&before));
        let hot = NamespaceCommitKey::new("hot", Some("a/1"));
        table.begin(Some(&hot));
        table.begin(Some(&hot));

        let floor = table.floor();
        let during = NamespaceCommitKey::new("during", Some("b/2"));
        table.begin(Some(&during));
        table.end(Some(&during));
        table.end(Some(&hot));

        let overlapping = table.overlapping(floor);
        assert!(!overlapping.unattributed);
        assert_eq!(
            overlapping.buckets,
            BTreeMap::from([
                ("during".to_string(), objects(&["b/2"])),
                ("hot".to_string(), objects(&["a/1"])),
            ]),
            "a commit that ended before the floor is not reported"
        );

        let next_floor = table.floor();
        assert_eq!(
            table.overlapping(next_floor).buckets,
            BTreeMap::from([("hot".to_string(), objects(&["a/1"]))]),
            "the second owner keeps the commit in flight"
        );
        table.end(Some(&hot));
        assert_eq!(table.overlapping(next_floor).buckets.len(), 1, "it ended after the floor");
        assert!(table.overlapping(table.floor()).buckets.is_empty());
        assert!(table.ended.is_empty(), "cards below the latest floor are pruned");
    }

    #[test]
    fn bucket_level_commits_and_object_overflow_collapse_to_the_whole_bucket() {
        let mut table = NamespaceCommitCardTable::default();
        for index in 0..=MAX_NAMESPACE_COMMIT_CARD_OBJECTS_PER_BUCKET {
            let key = NamespaceCommitKey::new("wide", Some(&format!("object-{index}")));
            table.begin(Some(&key));
            table.end(Some(&key));
        }
        let bucket_heal = NamespaceCommitKey::new("healed", None);
        table.begin(Some(&bucket_heal));
        table.end(Some(&bucket_heal));
        let empty_object = NamespaceCommitKey::new("empty", Some(""));
        table.begin(Some(&empty_object));

        assert_eq!(
            table.overlapping(0).buckets,
            BTreeMap::from([
                ("empty".to_string(), ScannerNamespaceCommitCardScope::WholeBucket),
                ("healed".to_string(), ScannerNamespaceCommitCardScope::WholeBucket),
                ("wide".to_string(), ScannerNamespaceCommitCardScope::WholeBucket),
            ])
        );
    }

    #[test]
    fn object_names_are_bounded_across_the_table() {
        let mut table = NamespaceCommitCardTable::default();
        let buckets = MAX_NAMESPACE_COMMIT_CARD_OBJECTS / MAX_NAMESPACE_COMMIT_CARD_OBJECTS_PER_BUCKET;
        for bucket in 0..buckets {
            for object in 0..MAX_NAMESPACE_COMMIT_CARD_OBJECTS_PER_BUCKET {
                let key = NamespaceCommitKey::new(&format!("bucket-{bucket}"), Some(&format!("object-{object}")));
                table.begin(Some(&key));
                table.end(Some(&key));
            }
        }
        assert_eq!(table.ended_objects, MAX_NAMESPACE_COMMIT_CARD_OBJECTS);
        let repeat = NamespaceCommitKey::new("bucket-0", Some("object-0"));
        table.begin(Some(&repeat));
        table.end(Some(&repeat));
        assert_eq!(table.ended_objects, MAX_NAMESPACE_COMMIT_CARD_OBJECTS, "a known name adds nothing");
        let extra = NamespaceCommitKey::new("extra", Some("object"));
        table.begin(Some(&extra));
        table.end(Some(&extra));
        let wider = NamespaceCommitKey::new("bucket-1", Some("new"));
        table.begin(Some(&wider));
        table.end(Some(&wider));
        assert_eq!(
            table.ended_objects,
            MAX_NAMESPACE_COMMIT_CARD_OBJECTS - MAX_NAMESPACE_COMMIT_CARD_OBJECTS_PER_BUCKET
        );
        let cards = table.overlapping(0);
        assert_eq!(cards.buckets["extra"], ScannerNamespaceCommitCardScope::WholeBucket);
        assert_eq!(cards.buckets["bucket-1"], ScannerNamespaceCommitCardScope::WholeBucket);
        assert!(!cards.unattributed, "collapsing a scope loses no attribution");
    }

    #[test]
    fn unscoped_owners_and_ended_overflow_are_unattributed() {
        let mut table = NamespaceCommitCardTable::default();
        table.begin(None);
        assert!(table.unscoped_in_flight());
        assert!(table.overlapping(table.floor()).unattributed);
        table.end(None);
        assert!(!table.unscoped_in_flight());
        assert!(!table.overlapping(table.floor()).unattributed);

        for index in 0..=MAX_NAMESPACE_COMMIT_CARD_BUCKETS {
            let key = NamespaceCommitKey::new(&format!("bucket-{index}"), None);
            table.begin(Some(&key));
            table.end(Some(&key));
        }
        let overlapping = table.overlapping(0);
        assert!(overlapping.unattributed, "a dropped ended card cannot be attributed");
        assert_eq!(overlapping.buckets.len(), MAX_NAMESPACE_COMMIT_CARD_BUCKETS);
        let after = table.overlapping(table.floor());
        assert!(!after.unattributed, "an overflow before the floor does not overlap the walk");
        assert!(after.buckets.is_empty());
    }
}
