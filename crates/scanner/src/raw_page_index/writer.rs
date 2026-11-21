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
use std::collections::{BTreeSet, HashMap, HashSet};

/// Exclusive, validated owner of an index during one directory enumeration.
/// The auxiliary sets are never serialized: restored indexes must cross the
/// validation boundary again before any pages can be trusted or extended.
pub(crate) struct RawEnumerationPageWriter {
    inner: RawEnumerationPageIndexInner,
    // True only for names in sealed pages at the start of this walk.
    indexed: HashMap<String, bool>,
    restored_committed_entries: usize,
    pending: BTreeSet<String>,
    observed_complete: HashSet<String>,
    complete_source_changed: bool,
    observations: usize,
    revalidate_after_entries: usize,
    committed_entries: usize,
}

impl RawEnumerationPageWriter {
    pub(crate) fn new(index: RawEnumerationPageIndex) -> Result<Self, RawEnumerationPageIndexError> {
        let RawEnumerationPageIndexState::Supported(inner) = index.state else {
            return Err(RawEnumerationPageIndexError::Unsupported);
        };
        let mut indexed = HashMap::new();
        inner.validate_membership(|name, sealed| indexed.insert(name.to_owned(), sealed).is_none())?;
        let entry_count = indexed.len();
        let committed_entries = entry_count - inner.building.as_ref().map_or(0, |page| page.entries.len());
        let restored_committed_entries = if inner.generation > 0 { committed_entries } else { 0 };
        Ok(Self {
            inner,
            indexed,
            restored_committed_entries,
            pending: BTreeSet::new(),
            observed_complete: HashSet::new(),
            complete_source_changed: false,
            observations: 0,
            revalidate_after_entries: entry_count,
            committed_entries,
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
        self.indexed.len()
    }

    pub(crate) fn is_replay(&self, entry: &str, continues_stream: bool) -> bool {
        self.restored_committed_entries != 0
            && ((!continues_stream && self.observations < self.restored_committed_entries)
                || self.indexed.get(entry) == Some(&true))
    }

    pub(crate) fn record_entry(&mut self, entry: &str) -> Result<(), RawEnumerationPageIndexError> {
        if !owner_entry_is_valid(entry) {
            return Err(RawEnumerationPageIndexError::InvalidEntry);
        }
        self.observations = self.observations.saturating_add(1);
        if self.inner.complete || self.inner.building.as_ref().is_some_and(|page| page.terminal) {
            if self.indexed.contains_key(entry) {
                self.observed_complete.insert(entry.to_owned());
            } else {
                self.complete_source_changed = true;
            }
        }
        if self.inner.complete {
            if self.observations >= self.revalidate_after_entries
                && (self.observed_complete.len() != self.indexed.len() || self.complete_source_changed)
            {
                return Err(RawEnumerationPageIndexError::IdentityMismatch);
            }
            return Ok(());
        }
        if !self.indexed.contains_key(entry) {
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
                entries_start: u64::try_from(self.committed_entries).unwrap_or(u64::MAX),
                entries: Vec::new(),
                terminal: false,
            });
            self.indexed.insert(clone_cache_value(&entry), false);
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
        self.inner.commit_building_page(self.committed_entries)?;
        self.committed_entries = self.indexed.len();
        Ok(())
    }

    pub(crate) fn checkpoint(&self) -> Result<RawEnumerationPageIndex, RawEnumerationPageIndexError> {
        let mut inner = clone_cache_value(&self.inner);
        if inner.building.is_some() {
            inner.commit_building_page(self.committed_entries)?;
        }
        Ok(RawEnumerationPageIndex {
            state: RawEnumerationPageIndexState::Supported(inner),
        })
    }
}

#[cfg(test)]
mod tests;
