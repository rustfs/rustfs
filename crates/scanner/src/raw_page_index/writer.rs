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

use super::{
    RawEnumerationPageBuilder, RawEnumerationPageIndex, RawEnumerationPageIndexError, RawEnumerationPageIndexInner,
    RawEnumerationPageIndexState, owner_entry_is_valid,
};
use crate::data_usage_define::clone_cache_value;
use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use std::collections::BTreeSet;
use std::hash::{BuildHasher, RandomState};

/// Keys live in immutable pages. Resizing the lookup table moves only
/// their locations, and a hash collision always compares the original names.
#[derive(Default)]
struct SealedNames {
    slots: HashTable<(usize, usize)>,
    hasher: RandomState,
}

impl SealedNames {
    fn ordinal(&self, inner: &RawEnumerationPageIndexInner, name: &str) -> Option<usize> {
        let &(page, slot) = self
            .slots
            .find(self.hasher.hash_one(name), |&(page, slot)| inner.pages[page].entries[slot] == name)?;
        // Page validation binds entries_start to the actual number of preceding names.
        Some(inner.pages[page].entries_start as usize + slot)
    }

    fn insert(&mut self, inner: &RawEnumerationPageIndexInner, position: (usize, usize)) -> bool {
        let (page, slot) = position;
        let name = &inner.pages[page].entries[slot];
        match self.slots.entry(
            self.hasher.hash_one(name),
            |&(page, slot)| &inner.pages[page].entries[slot] == name,
            |&(page, slot)| self.hasher.hash_one(&inner.pages[page].entries[slot]),
        ) {
            Entry::Vacant(entry) => {
                entry.insert(position);
                true
            }
            Entry::Occupied(_) => false,
        }
    }
}

/// Exclusive, validated owner of an index during one directory enumeration.
/// The auxiliary sets are never serialized: restored indexes must cross the
/// validation boundary again before any pages can be trusted or extended.
pub(crate) struct RawEnumerationPageWriter {
    inner: RawEnumerationPageIndexInner,
    sealed: SealedNames,
    restored_committed_entries: usize,
    pending: BTreeSet<String>,
    observed_complete: Vec<bool>,
    observed_count: usize,
    complete_source_changed: bool,
    observations: usize,
    revalidate_after_entries: usize,
}

impl RawEnumerationPageWriter {
    pub(crate) fn new(index: RawEnumerationPageIndex) -> Result<Self, RawEnumerationPageIndexError> {
        let RawEnumerationPageIndexState::Supported(inner) = index.state else {
            return Err(RawEnumerationPageIndexError::Unsupported);
        };
        let mut sealed = SealedNames::default();
        inner.validate_membership(|name, location| match location {
            Some(position) => sealed.insert(&inner, position),
            None => sealed.ordinal(&inner, name).is_none(),
        })?;
        let committed_entries = sealed.slots.len();
        let entry_count = committed_entries + inner.building.as_ref().map_or(0, |page| page.entries.len());
        let restored_committed_entries = if inner.generation > 0 { committed_entries } else { 0 };
        Ok(Self {
            inner,
            sealed,
            restored_committed_entries,
            pending: BTreeSet::new(),
            observed_complete: Vec::new(),
            observed_count: 0,
            complete_source_changed: false,
            observations: 0,
            revalidate_after_entries: entry_count,
        })
    }

    /// A parked native stream delivers the unread tail first. Incomplete
    /// membership can immediately append that tail; terminal page verification
    /// still requires observing the entire current directory.
    pub(crate) fn continue_native_stream(&mut self) {
        if !self.inner.complete && !self.inner.building.as_ref().is_some_and(|page| page.terminal) {
            self.observations = self.revalidate_after_entries;
        }
    }

    pub(crate) fn indexed_entry_count(&self) -> usize {
        self.sealed.slots.len() + self.inner.building.as_ref().map_or(0, |page| page.entries.len())
    }

    pub(crate) fn is_replay(&self, entry: &str, continues_stream: bool) -> bool {
        self.restored_committed_entries != 0
            && ((!continues_stream && self.observations < self.restored_committed_entries)
                || self
                    .sealed
                    .ordinal(&self.inner, entry)
                    .is_some_and(|ordinal| ordinal < self.restored_committed_entries))
    }

    pub(crate) fn record_entry(&mut self, entry: &str) -> Result<(), RawEnumerationPageIndexError> {
        if !owner_entry_is_valid(entry) {
            return Err(RawEnumerationPageIndexError::InvalidEntry);
        }
        self.observations = self.observations.saturating_add(1);
        let position = self.sealed.ordinal(&self.inner, entry).or_else(|| {
            let building = self.inner.building.as_ref()?;
            let slot = building.entries.binary_search_by(|name| name.as_str().cmp(entry)).ok()?;
            Some(self.sealed.slots.len() + slot)
        });
        if self.inner.complete || self.inner.building.as_ref().is_some_and(|page| page.terminal) {
            if let Some(ordinal) = position {
                // An unchanged terminal builder keeps its sorted ordinals when sealed.
                // A foreign name already invalidates the complete-source check.
                if self.observed_complete.len() <= ordinal {
                    self.observed_complete.resize(ordinal + 1, false);
                }
                if !self.observed_complete[ordinal] {
                    self.observed_complete[ordinal] = true;
                    self.observed_count += 1;
                }
            } else {
                self.complete_source_changed = true;
            }
        }
        if self.inner.complete {
            if self.observations >= self.revalidate_after_entries
                && (self.observed_count != self.indexed_entry_count() || self.complete_source_changed)
            {
                return Err(RawEnumerationPageIndexError::IdentityMismatch);
            }
            return Ok(());
        }
        if position.is_none() {
            self.pending.insert(entry.to_owned());
        }
        // A restored directory can be enumerated in a different order. Keep
        // the old observation floor and append at most one new entry per call,
        // in the same order as normalization of the cumulative source.
        if self.observations < self.revalidate_after_entries {
            return Ok(());
        }
        if self
            .inner
            .building
            .as_ref()
            .is_some_and(|page| page.entries.len() >= self.inner.page_entry_limit)
        {
            return self.commit_page();
        }
        if let Some(entry) = self.pending.pop_first() {
            let building = self.inner.building.get_or_insert_with(|| RawEnumerationPageBuilder {
                page_index: u64::try_from(self.inner.pages.len()).unwrap_or(u64::MAX),
                entries_start: u64::try_from(self.sealed.slots.len()).unwrap_or(u64::MAX),
                entries: Vec::new(),
                terminal: false,
            });
            // The building page is normalized (strictly increasing) on every
            // path that reaches here, so a positional insert keeps it so.
            let position = building.entries.binary_search(&entry).unwrap_or_else(|position| position);
            building.entries.insert(position, entry);
            building.terminal = false;
            self.inner.generation = self.inner.generation.saturating_add(1);
        }
        if self
            .inner
            .building
            .as_ref()
            .is_some_and(|page| page.terminal || page.entries.len() >= self.inner.page_entry_limit)
        {
            self.commit_page()?;
        }
        Ok(())
    }

    fn commit_page(&mut self) -> Result<(), RawEnumerationPageIndexError> {
        self.inner.commit_building_page(self.sealed.slots.len())?;
        let page = self.inner.pages.len() - 1;
        for slot in 0..self.inner.pages[page].entries.len() {
            if !self.sealed.insert(&self.inner, (page, slot)) {
                return Err(RawEnumerationPageIndexError::CorruptIndex);
            }
        }
        Ok(())
    }

    pub(crate) fn checkpoint(&self) -> Result<RawEnumerationPageIndex, RawEnumerationPageIndexError> {
        let mut inner = clone_cache_value(&self.inner);
        if inner.building.is_some() {
            inner.commit_building_page(self.sealed.slots.len())?;
        }
        Ok(RawEnumerationPageIndex {
            state: RawEnumerationPageIndexState::Supported(inner),
        })
    }
}

#[cfg(test)]
mod tests;
