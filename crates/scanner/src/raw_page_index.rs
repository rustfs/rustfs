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

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

const RAW_PAGE_INDEX_VERSION: u16 = 1;
const RAW_PAGE_ENTRY_MAX_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawEnumerationPageIndex {
    state: RawEnumerationPageIndexState,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum RawEnumerationPageIndexState {
    Unsupported,
    Supported(RawEnumerationPageIndexInner),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RawEnumerationPageIndexInner {
    generation: u64,
    parent: String,
    page_entry_limit: usize,
    pages: Vec<RawEnumerationPage>,
    building: Option<RawEnumerationPageBuilder>,
    complete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawEnumerationPage {
    version: u16,
    parent: String,
    page_index: u64,
    entries_start: u64,
    entries: Vec<String>,
    terminal: bool,
    digest: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RawEnumerationPageBuilder {
    page_index: u64,
    entries_start: u64,
    entries: Vec<String>,
    terminal: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RawEnumerationPageBuildOutcome {
    pub status: RawEnumerationPageOwnerStatus,
    pub ready_to_commit: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RawEnumerationPageOwnerStatus {
    Unsupported,
    Building {
        generation: u64,
        parent: String,
        page_index: u64,
        indexed_entries: u64,
        buffered_entries: usize,
    },
    Ready {
        generation: u64,
        parent: String,
        committed_pages: usize,
        indexed_entries: u64,
        complete: bool,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RawEnumerationPageIndexError {
    #[error("raw enumeration page owner is unsupported")]
    Unsupported,
    #[error("raw enumeration page owner generation changed")]
    StaleGeneration,
    #[error("raw enumeration page identity changed before committed coverage")]
    IdentityMismatch,
    #[error("raw enumeration page owner requires a non-empty parent")]
    EmptyParent,
    #[error("raw enumeration page entry limit must be non-zero")]
    EmptyPage,
    #[error("raw enumeration page build budget must be non-zero")]
    EmptyBudget,
    #[error("raw enumeration page entry is invalid")]
    InvalidEntry,
    #[error("raw enumeration page has no staged entries")]
    EmptyCommit,
    #[error("raw enumeration page index is corrupt")]
    CorruptIndex,
}

impl RawEnumerationPageIndex {
    pub fn unsupported() -> Self {
        Self {
            state: RawEnumerationPageIndexState::Unsupported,
        }
    }

    pub fn new(parent: impl Into<String>, page_entry_limit: usize) -> Result<Self, RawEnumerationPageIndexError> {
        let parent = parent.into();
        if parent.is_empty() {
            return Err(RawEnumerationPageIndexError::EmptyParent);
        }
        if page_entry_limit == 0 {
            return Err(RawEnumerationPageIndexError::EmptyPage);
        }
        Ok(Self {
            state: RawEnumerationPageIndexState::Supported(RawEnumerationPageIndexInner {
                generation: 0,
                parent,
                page_entry_limit,
                pages: Vec::new(),
                building: None,
                complete: false,
            }),
        })
    }

    pub fn generation(&self) -> Option<u64> {
        match &self.state {
            RawEnumerationPageIndexState::Unsupported => None,
            RawEnumerationPageIndexState::Supported(inner) => Some(inner.generation),
        }
    }

    pub fn status(&self) -> RawEnumerationPageOwnerStatus {
        match &self.state {
            RawEnumerationPageIndexState::Unsupported => RawEnumerationPageOwnerStatus::Unsupported,
            RawEnumerationPageIndexState::Supported(inner) => inner.status(),
        }
    }

    pub fn ingest_owner_entries<I>(
        &mut self,
        entries: I,
        max_new_entries: usize,
        expected_generation: u64,
    ) -> Result<RawEnumerationPageBuildOutcome, RawEnumerationPageIndexError>
    where
        I: IntoIterator<Item = String>,
    {
        self.ingest_owner_entries_inner(entries, max_new_entries, expected_generation, true)
    }

    pub fn ingest_partial_owner_entries<I>(
        &mut self,
        entries: I,
        max_new_entries: usize,
        expected_generation: u64,
    ) -> Result<RawEnumerationPageBuildOutcome, RawEnumerationPageIndexError>
    where
        I: IntoIterator<Item = String>,
    {
        self.ingest_owner_entries_inner(entries, max_new_entries, expected_generation, false)
    }

    fn ingest_owner_entries_inner<I>(
        &mut self,
        entries: I,
        max_new_entries: usize,
        expected_generation: u64,
        source_complete: bool,
    ) -> Result<RawEnumerationPageBuildOutcome, RawEnumerationPageIndexError>
    where
        I: IntoIterator<Item = String>,
    {
        if max_new_entries == 0 {
            return Err(RawEnumerationPageIndexError::EmptyBudget);
        }
        let RawEnumerationPageIndexState::Supported(inner) = &mut self.state else {
            return Err(RawEnumerationPageIndexError::Unsupported);
        };
        if inner.generation != expected_generation {
            return Err(RawEnumerationPageIndexError::StaleGeneration);
        }
        let entries = normalize_owner_entries(entries)?;
        let committed_entries = inner.validated_committed_entries()?;
        if inner.complete {
            if !entry_sets_match(&entries, &committed_entries) {
                return Err(RawEnumerationPageIndexError::IdentityMismatch);
            }
            return Ok(RawEnumerationPageBuildOutcome {
                status: inner.status(),
                ready_to_commit: false,
            });
        }

        if source_complete && !entries_contain_all(&entries, &committed_entries) {
            return Err(RawEnumerationPageIndexError::IdentityMismatch);
        }

        let mut indexed_entries = committed_entries.clone();
        if let Some(building) = &inner.building {
            building.validate(
                u64::try_from(inner.pages.len()).unwrap_or(u64::MAX),
                u64::try_from(committed_entries.len()).unwrap_or(u64::MAX),
                inner.page_entry_limit,
            )?;
            if source_complete && !entries_contain_all(&entries, &building.entries) {
                return Err(RawEnumerationPageIndexError::IdentityMismatch);
            }
            indexed_entries.extend(building.entries.iter().cloned());
        }
        let indexed_entries = normalize_owner_entries(indexed_entries)?;
        if source_complete && entry_sets_match(&entries, &indexed_entries) && inner.building.is_none() {
            inner.complete = true;
            inner.generation = inner.generation.saturating_add(1);
            return Ok(RawEnumerationPageBuildOutcome {
                status: inner.status(),
                ready_to_commit: false,
            });
        }

        let mut drop_empty_building = false;
        let ready_to_commit = {
            let page_index = u64::try_from(inner.pages.len()).unwrap_or(u64::MAX);
            let entries_start = u64::try_from(committed_entries.len()).unwrap_or(u64::MAX);
            let building = inner.building.get_or_insert_with(|| RawEnumerationPageBuilder {
                page_index,
                entries_start,
                entries: Vec::new(),
                terminal: false,
            });
            if building.entries.len() >= inner.page_entry_limit {
                true
            } else {
                let remaining_page_slots = inner.page_entry_limit.saturating_sub(building.entries.len());
                let append_entries = entries
                    .iter()
                    .filter(|entry| indexed_entries.binary_search(entry).is_err())
                    .take(max_new_entries.min(remaining_page_slots))
                    .cloned()
                    .collect::<Vec<_>>();
                let append_count = append_entries.len();
                if append_count == 0 {
                    if source_complete && !building.terminal {
                        building.terminal = true;
                        inner.generation = inner.generation.saturating_add(1);
                    } else if building.entries.is_empty() {
                        drop_empty_building = true;
                    }
                } else {
                    building.entries.extend(append_entries);
                    building.entries.sort();
                    building.entries.dedup();
                    let source_entries_indexed = source_complete && {
                        let mut indexed_after_append = indexed_entries;
                        indexed_after_append.extend(building.entries.iter().cloned());
                        let indexed_after_append = normalize_owner_entries(indexed_after_append)?;
                        entry_sets_match(&indexed_after_append, &entries)
                    };
                    building.terminal = source_entries_indexed;
                    inner.generation = inner.generation.saturating_add(1);
                }
                !building.entries.is_empty() && (building.terminal || building.entries.len() >= inner.page_entry_limit)
            }
        };
        if drop_empty_building {
            inner.building = None;
        }

        Ok(RawEnumerationPageBuildOutcome {
            status: inner.status(),
            ready_to_commit,
        })
    }

    pub fn commit_building_page(&mut self, expected_generation: u64) -> Result<RawEnumerationPage, RawEnumerationPageIndexError> {
        let RawEnumerationPageIndexState::Supported(inner) = &mut self.state else {
            return Err(RawEnumerationPageIndexError::Unsupported);
        };
        if inner.generation != expected_generation {
            return Err(RawEnumerationPageIndexError::StaleGeneration);
        }
        let entries_start = inner.validated_committed_entries()?.len();
        let Some(building) = inner.building.as_ref() else {
            return Err(RawEnumerationPageIndexError::EmptyCommit);
        };
        if building.entries.is_empty() {
            return Err(RawEnumerationPageIndexError::EmptyCommit);
        }
        building.validate(
            u64::try_from(inner.pages.len()).unwrap_or(u64::MAX),
            u64::try_from(entries_start).unwrap_or(u64::MAX),
            inner.page_entry_limit,
        )?;
        let Some(building) = inner.building.take() else {
            return Err(RawEnumerationPageIndexError::EmptyCommit);
        };
        let page = RawEnumerationPage::new(&inner.parent, building);
        inner.complete = page.terminal;
        inner.pages.push(page.clone());
        inner.generation = inner.generation.saturating_add(1);
        Ok(page)
    }

    pub fn page(&self, page_index: u64, expected_digest: [u8; 32]) -> Result<&RawEnumerationPage, RawEnumerationPageIndexError> {
        let RawEnumerationPageIndexState::Supported(inner) = &self.state else {
            return Err(RawEnumerationPageIndexError::Unsupported);
        };
        let Some(page_index) = usize::try_from(page_index).ok() else {
            return Err(RawEnumerationPageIndexError::IdentityMismatch);
        };
        let Some(page) = inner.pages.get(page_index) else {
            return Err(RawEnumerationPageIndexError::IdentityMismatch);
        };
        page.validate(&inner.parent, u64::try_from(page_index).unwrap_or(u64::MAX), page.entries_start)?;
        if page.digest != expected_digest {
            return Err(RawEnumerationPageIndexError::IdentityMismatch);
        }
        Ok(page)
    }

    pub fn committed_entries(&self) -> Result<Vec<String>, RawEnumerationPageIndexError> {
        match &self.state {
            RawEnumerationPageIndexState::Unsupported => Ok(Vec::new()),
            RawEnumerationPageIndexState::Supported(inner) => inner.validated_committed_entries(),
        }
    }

    pub fn indexed_entries(&self) -> Result<Vec<String>, RawEnumerationPageIndexError> {
        match &self.state {
            RawEnumerationPageIndexState::Unsupported => Ok(Vec::new()),
            RawEnumerationPageIndexState::Supported(inner) => inner.validated_indexed_entries(),
        }
    }
}

impl RawEnumerationPageIndexInner {
    fn status(&self) -> RawEnumerationPageOwnerStatus {
        let indexed_entries = u64::try_from(self.indexed_entries()).unwrap_or(u64::MAX);
        if let Some(building) = &self.building {
            RawEnumerationPageOwnerStatus::Building {
                generation: self.generation,
                parent: self.parent.clone(),
                page_index: building.page_index,
                indexed_entries,
                buffered_entries: building.entries.len(),
            }
        } else {
            RawEnumerationPageOwnerStatus::Ready {
                generation: self.generation,
                parent: self.parent.clone(),
                committed_pages: self.pages.len(),
                indexed_entries,
                complete: self.complete,
            }
        }
    }

    fn validated_committed_entries(&self) -> Result<Vec<String>, RawEnumerationPageIndexError> {
        let mut entries = Vec::new();
        for (page_index, page) in self.pages.iter().enumerate() {
            page.validate(
                &self.parent,
                u64::try_from(page_index).unwrap_or(u64::MAX),
                u64::try_from(entries.len()).unwrap_or(u64::MAX),
            )?;
            if page.terminal && page_index + 1 != self.pages.len() {
                return Err(RawEnumerationPageIndexError::CorruptIndex);
            }
            entries.extend(page.entries.iter().cloned());
        }
        if self.complete && self.pages.last().is_some_and(|page| !page.terminal) {
            return Err(RawEnumerationPageIndexError::CorruptIndex);
        }
        let mut unique_entries = entries.clone();
        unique_entries.sort();
        unique_entries.dedup();
        if unique_entries.len() != entries.len() {
            return Err(RawEnumerationPageIndexError::CorruptIndex);
        }
        Ok(entries)
    }

    fn validated_indexed_entries(&self) -> Result<Vec<String>, RawEnumerationPageIndexError> {
        let mut entries = self.validated_committed_entries()?;
        if let Some(building) = &self.building {
            building.validate(
                u64::try_from(self.pages.len()).unwrap_or(u64::MAX),
                u64::try_from(entries.len()).unwrap_or(u64::MAX),
                self.page_entry_limit,
            )?;
            entries.extend(building.entries.iter().cloned());
        }
        Ok(entries)
    }

    fn indexed_entries(&self) -> usize {
        self.pages.iter().map(|page| page.entries.len()).sum::<usize>()
            + self.building.as_ref().map_or(0, |building| building.entries.len())
    }
}

impl RawEnumerationPageBuilder {
    fn validate(&self, page_index: u64, entries_start: u64, page_entry_limit: usize) -> Result<(), RawEnumerationPageIndexError> {
        if self.page_index != page_index
            || self.entries_start != entries_start
            || self.entries.is_empty()
            || self.entries.len() > page_entry_limit
            || !entries_are_normalized(&self.entries)
        {
            return Err(RawEnumerationPageIndexError::CorruptIndex);
        }
        Ok(())
    }
}

impl RawEnumerationPage {
    pub fn version(&self) -> u16 {
        self.version
    }

    pub fn parent(&self) -> &str {
        &self.parent
    }

    pub fn page_index(&self) -> u64 {
        self.page_index
    }

    pub fn entries_start(&self) -> u64 {
        self.entries_start
    }

    pub fn entries(&self) -> &[String] {
        &self.entries
    }

    pub fn terminal(&self) -> bool {
        self.terminal
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    fn new(parent: &str, building: RawEnumerationPageBuilder) -> Self {
        let digest = raw_page_digest(parent, &building);
        Self {
            version: RAW_PAGE_INDEX_VERSION,
            parent: parent.to_string(),
            page_index: building.page_index,
            entries_start: building.entries_start,
            entries: building.entries,
            terminal: building.terminal,
            digest,
        }
    }

    fn validate(&self, parent: &str, page_index: u64, entries_start: u64) -> Result<(), RawEnumerationPageIndexError> {
        let builder = RawEnumerationPageBuilder {
            page_index: self.page_index,
            entries_start: self.entries_start,
            entries: self.entries.clone(),
            terminal: self.terminal,
        };
        if self.version != RAW_PAGE_INDEX_VERSION
            || self.parent != parent
            || self.page_index != page_index
            || self.entries_start != entries_start
            || self.entries.is_empty()
            || !entries_are_normalized(&self.entries)
            || self.digest != raw_page_digest(parent, &builder)
        {
            return Err(RawEnumerationPageIndexError::CorruptIndex);
        }
        Ok(())
    }
}

fn normalize_owner_entries<I>(entries: I) -> Result<Vec<String>, RawEnumerationPageIndexError>
where
    I: IntoIterator<Item = String>,
{
    let mut entries = entries.into_iter().map(validate_owner_entry).collect::<Result<Vec<_>, _>>()?;
    entries.sort();
    entries.dedup();
    Ok(entries)
}

fn validate_owner_entry(entry: String) -> Result<String, RawEnumerationPageIndexError> {
    if !owner_entry_is_valid(&entry) {
        return Err(RawEnumerationPageIndexError::InvalidEntry);
    }
    Ok(entry)
}

fn owner_entry_is_valid(entry: &str) -> bool {
    !entry.is_empty() && entry != "." && entry != ".." && !entry.contains('/') && entry.len() <= RAW_PAGE_ENTRY_MAX_BYTES
}

fn entries_are_normalized(entries: &[String]) -> bool {
    entries.iter().all(|entry| owner_entry_is_valid(entry))
        && entries
            .windows(2)
            .all(|window| window.first().zip(window.get(1)).is_some_and(|(left, right)| left < right))
}

fn entries_contain_all(entries: &[String], required: &[String]) -> bool {
    required.iter().all(|entry| entries.binary_search(entry).is_ok())
}

fn entry_sets_match(left: &[String], right: &[String]) -> bool {
    left.len() == right.len() && entries_contain_all(left, right)
}

fn raw_page_digest(parent: &str, building: &RawEnumerationPageBuilder) -> [u8; 32] {
    let mut digest = Sha256::new();
    update_digest(&mut digest, b"version", &RAW_PAGE_INDEX_VERSION.to_le_bytes());
    update_digest(&mut digest, b"parent", parent.as_bytes());
    update_digest(&mut digest, b"page_index", &building.page_index.to_le_bytes());
    update_digest(&mut digest, b"entries_start", &building.entries_start.to_le_bytes());
    update_digest(&mut digest, b"terminal", &[u8::from(building.terminal)]);
    for entry in &building.entries {
        update_digest(&mut digest, b"entry", entry.as_bytes());
    }
    digest.finalize().into()
}

fn update_digest(digest: &mut Sha256, label: &[u8], value: &[u8]) {
    digest.update(label);
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_le_bytes());
    digest.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    fn indexed_entries(status: &RawEnumerationPageOwnerStatus) -> u64 {
        match status {
            RawEnumerationPageOwnerStatus::Unsupported => 0,
            RawEnumerationPageOwnerStatus::Building { indexed_entries, .. }
            | RawEnumerationPageOwnerStatus::Ready { indexed_entries, .. } => *indexed_entries,
        }
    }

    #[test]
    fn unsupported_owner_reports_unsupported_without_building_pages() {
        let mut owner = RawEnumerationPageIndex::unsupported();

        assert_eq!(owner.status(), RawEnumerationPageOwnerStatus::Unsupported);
        assert_eq!(
            owner.ingest_owner_entries(entries(&["entry-a"]), 1, 0),
            Err(RawEnumerationPageIndexError::Unsupported)
        );
        assert_eq!(owner.commit_building_page(0), Err(RawEnumerationPageIndexError::Unsupported));
    }

    #[test]
    fn owner_page_builds_monotonically_across_small_budget_restarts() {
        let source = entries(&["entry-c", "entry-a", "entry-e", "entry-b", "entry-d"]);
        let mut owner = RawEnumerationPageIndex::new("bucket", 2).expect("page owner should initialize");
        let mut last_indexed_entries = 0;

        for _ in 0..8 {
            let mut restarted_owner = owner.clone();
            let generation = restarted_owner
                .generation()
                .expect("supported owner should expose generation");
            let outcome = restarted_owner
                .ingest_owner_entries(source.clone(), 1, generation)
                .expect("owner page build should accept stable source entries");
            let now_indexed_entries = indexed_entries(&outcome.status);
            assert!(
                now_indexed_entries >= last_indexed_entries,
                "owner-backed page build must not move coverage backward across restart"
            );
            last_indexed_entries = now_indexed_entries;
            if outcome.ready_to_commit {
                let generation = restarted_owner
                    .generation()
                    .expect("supported owner should expose generation");
                restarted_owner
                    .commit_building_page(generation)
                    .expect("ready page should commit under matching generation");
            }
            owner = restarted_owner;
            if let RawEnumerationPageOwnerStatus::Ready { complete: true, .. } = owner.status() {
                break;
            }
        }

        assert_eq!(
            owner.status(),
            RawEnumerationPageOwnerStatus::Ready {
                generation: 8,
                parent: "bucket".to_string(),
                committed_pages: 3,
                indexed_entries: 5,
                complete: true,
            }
        );
        assert_eq!(
            owner.committed_entries().expect("committed entries should validate"),
            entries(&["entry-a", "entry-b", "entry-c", "entry-d", "entry-e"])
        );
    }

    #[test]
    fn page_digest_identity_guards_consumption_and_source_drift() {
        let mut owner = RawEnumerationPageIndex::new("bucket", 2).expect("page owner should initialize");
        let source = entries(&["entry-a", "entry-b", "entry-c"]);
        let generation = owner.generation().expect("supported owner should expose generation");
        let outcome = owner
            .ingest_owner_entries(source, 2, generation)
            .expect("first page build should succeed");
        assert!(outcome.ready_to_commit);
        let generation = owner.generation().expect("supported owner should expose generation");
        let page = owner.commit_building_page(generation).expect("first page should commit");

        assert_eq!(
            owner
                .page(page.page_index, page.digest)
                .expect("committed page should be readable"),
            &page
        );
        let mut wrong_digest = page.digest;
        wrong_digest[0] ^= 0xff;
        assert_eq!(
            owner.page(page.page_index, wrong_digest),
            Err(RawEnumerationPageIndexError::IdentityMismatch)
        );

        let generation = owner.generation().expect("supported owner should expose generation");
        assert_eq!(
            owner.ingest_owner_entries(entries(&["entry-a", "entry-x", "entry-c"]), 1, generation),
            Err(RawEnumerationPageIndexError::IdentityMismatch)
        );
        assert_eq!(
            owner
                .committed_entries()
                .expect("committed entries should validate after source drift rejection"),
            entries(&["entry-a", "entry-b"])
        );
    }

    #[test]
    fn page_commit_cas_failure_and_precommit_crash_do_not_publish_coverage() {
        let mut owner = RawEnumerationPageIndex::new("bucket", 2).expect("page owner should initialize");
        let source = entries(&["entry-a", "entry-b", "entry-c"]);
        let generation = owner.generation().expect("supported owner should expose generation");
        let outcome = owner
            .ingest_owner_entries(source, 2, generation)
            .expect("page build should stage entries");
        assert!(outcome.ready_to_commit);
        assert_eq!(
            owner
                .committed_entries()
                .expect("uncommitted staged entries should not publish coverage"),
            Vec::<String>::new()
        );

        assert_eq!(owner.commit_building_page(generation), Err(RawEnumerationPageIndexError::StaleGeneration));
        assert_eq!(
            owner
                .committed_entries()
                .expect("failed CAS should not corrupt committed coverage"),
            Vec::<String>::new()
        );

        let mut restarted_owner = owner.clone();
        let generation = restarted_owner
            .generation()
            .expect("supported owner should expose generation");
        restarted_owner
            .commit_building_page(generation)
            .expect("persisted staged page should commit after restart with fresh CAS generation");
        assert_eq!(
            restarted_owner
                .committed_entries()
                .expect("restarted committed entries should validate"),
            entries(&["entry-a", "entry-b"])
        );
    }

    #[test]
    fn serialized_building_page_resumes_and_commits_after_restart() {
        let source = entries(&["entry-a", "entry-b", "entry-c"]);
        let mut owner = RawEnumerationPageIndex::new("bucket", 2).expect("page owner should initialize");
        let generation = owner.generation().expect("supported owner should expose generation");
        let outcome = owner
            .ingest_owner_entries(source.clone(), 1, generation)
            .expect("first budgeted page build should stage one entry");
        assert_eq!(
            outcome.status,
            RawEnumerationPageOwnerStatus::Building {
                generation: 1,
                parent: "bucket".to_string(),
                page_index: 0,
                indexed_entries: 1,
                buffered_entries: 1,
            }
        );
        assert!(!outcome.ready_to_commit);

        let encoded = rmp_serde::to_vec(&owner).expect("building page index should encode");
        let mut decoded: RawEnumerationPageIndex = rmp_serde::from_slice(&encoded).expect("building page index should decode");

        let generation = decoded.generation().expect("decoded owner should expose generation");
        let outcome = decoded
            .ingest_owner_entries(source, 1, generation)
            .expect("decoded page owner should resume from staged coverage");
        assert!(outcome.ready_to_commit);
        let generation = decoded.generation().expect("decoded owner should expose generation");
        let page = decoded
            .commit_building_page(generation)
            .expect("decoded ready page should commit");
        assert_eq!(page.entries(), entries(&["entry-a", "entry-b"]));
        assert_eq!(
            decoded
                .page(page.page_index(), page.digest())
                .expect("committed decoded page should validate by digest")
                .entries(),
            entries(&["entry-a", "entry-b"])
        );
    }

    #[test]
    fn partial_owner_source_does_not_mark_terminal_before_completion() {
        let mut owner = RawEnumerationPageIndex::new("bucket", 2).expect("page owner should initialize");
        let generation = owner.generation().expect("supported owner should expose generation");
        let outcome = owner
            .ingest_partial_owner_entries(entries(&["entry-a"]), 1, generation)
            .expect("partial source should stage one entry");
        assert_eq!(
            outcome.status,
            RawEnumerationPageOwnerStatus::Building {
                generation: 1,
                parent: "bucket".to_string(),
                page_index: 0,
                indexed_entries: 1,
                buffered_entries: 1,
            }
        );
        assert!(!outcome.ready_to_commit);
        assert_eq!(
            owner.indexed_entries().expect("building page entries should validate"),
            entries(&["entry-a"])
        );

        let encoded = rmp_serde::to_vec(&owner).expect("partial owner should encode");
        let mut restarted: RawEnumerationPageIndex = rmp_serde::from_slice(&encoded).expect("partial owner should decode");
        let generation = restarted.generation().expect("restarted owner should expose generation");
        let outcome = restarted
            .ingest_owner_entries(entries(&["entry-a", "entry-b"]), 1, generation)
            .expect("complete source should finish resumed building page");
        assert!(outcome.ready_to_commit);
        let generation = restarted.generation().expect("finished owner should expose generation");
        let page = restarted
            .commit_building_page(generation)
            .expect("terminal resumed page should commit");
        assert!(page.terminal());
        assert_eq!(page.entries(), entries(&["entry-a", "entry-b"]));
    }

    #[test]
    fn complete_owner_source_missing_committed_entry_fails_closed() {
        let mut owner = RawEnumerationPageIndex::new("bucket", 2).expect("page owner should initialize");
        let generation = owner.generation().expect("supported owner should expose generation");
        owner
            .ingest_owner_entries(entries(&["entry-a", "entry-b"]), 2, generation)
            .expect("initial complete source should build a committed page");
        let generation = owner.generation().expect("supported owner should expose generation");
        owner
            .commit_building_page(generation)
            .expect("initial committed page should validate");

        let generation = owner.generation().expect("supported owner should expose generation");
        assert_eq!(
            owner.ingest_owner_entries(entries(&["entry-a", "entry-c"]), 2, generation),
            Err(RawEnumerationPageIndexError::IdentityMismatch),
            "only complete source identity can prove a previously committed raw entry disappeared"
        );
    }

    #[test]
    fn terminal_marker_advances_generation_before_commit() {
        let initial_source = entries(&["entry-a", "entry-b", "entry-c"]);
        let current_source = entries(&["entry-a", "entry-b"]);
        let mut owner = RawEnumerationPageIndex::new("bucket", 3).expect("page owner should initialize");
        let generation = owner.generation().expect("supported owner should expose generation");
        let outcome = owner
            .ingest_owner_entries(initial_source, 2, generation)
            .expect("budgeted page build should stage all source entries");
        assert_eq!(owner.generation(), Some(1));
        assert!(!outcome.ready_to_commit);

        let generation = owner.generation().expect("supported owner should expose generation");
        let outcome = owner
            .ingest_owner_entries(current_source, 1, generation)
            .expect("terminal-only build step should complete the staged page");
        assert!(outcome.ready_to_commit);
        assert_eq!(
            owner.generation(),
            Some(2),
            "terminal marker is a persisted builder state change and must advance the CAS generation"
        );
        assert_eq!(owner.commit_building_page(generation), Err(RawEnumerationPageIndexError::StaleGeneration));

        let generation = owner.generation().expect("supported owner should expose generation");
        let page = owner
            .commit_building_page(generation)
            .expect("fresh generation should commit terminal page");
        assert!(page.terminal());
    }

    #[test]
    fn deserialized_corrupt_page_digest_fails_closed() {
        let source = entries(&["entry-a", "entry-b", "entry-c"]);
        let mut owner = RawEnumerationPageIndex::new("bucket", 2).expect("page owner should initialize");
        let generation = owner.generation().expect("supported owner should expose generation");
        let outcome = owner
            .ingest_owner_entries(source.clone(), 2, generation)
            .expect("page build should stage entries");
        assert!(outcome.ready_to_commit);
        let generation = owner.generation().expect("supported owner should expose generation");
        let page = owner.commit_building_page(generation).expect("ready page should commit");

        let encoded = rmp_serde::to_vec(&owner).expect("committed page index should encode");
        let mut decoded: RawEnumerationPageIndex = rmp_serde::from_slice(&encoded).expect("committed page index should decode");
        let RawEnumerationPageIndexState::Supported(inner) = &mut decoded.state else {
            panic!("decoded owner should be supported");
        };
        inner.pages[0].digest[0] ^= 0xff;

        assert_eq!(decoded.committed_entries(), Err(RawEnumerationPageIndexError::CorruptIndex));
        assert_eq!(
            decoded.page(page.page_index(), page.digest()),
            Err(RawEnumerationPageIndexError::CorruptIndex)
        );
        assert_eq!(
            decoded.ingest_owner_entries(source, 1, decoded.generation().expect("decoded owner should expose generation")),
            Err(RawEnumerationPageIndexError::CorruptIndex)
        );

        let mut complete_owner = RawEnumerationPageIndex::new("bucket", 2).expect("complete owner should initialize");
        let generation = complete_owner.generation().expect("complete owner should expose generation");
        let outcome = complete_owner
            .ingest_owner_entries(entries(&["entry-a", "entry-b"]), 2, generation)
            .expect("terminal page build should stage entries");
        assert!(outcome.ready_to_commit);
        let generation = complete_owner.generation().expect("complete owner should expose generation");
        complete_owner
            .commit_building_page(generation)
            .expect("terminal page should commit");
        let encoded = rmp_serde::to_vec(&complete_owner).expect("complete page index should encode");
        let mut decoded_complete: RawEnumerationPageIndex =
            rmp_serde::from_slice(&encoded).expect("complete page index should decode");
        let RawEnumerationPageIndexState::Supported(inner) = &mut decoded_complete.state else {
            panic!("decoded complete owner should be supported");
        };
        inner.pages[0].digest[0] ^= 0xff;
        assert_eq!(
            decoded_complete.ingest_owner_entries(
                entries(&["entry-a", "entry-b"]),
                1,
                decoded_complete
                    .generation()
                    .expect("decoded complete owner should expose generation")
            ),
            Err(RawEnumerationPageIndexError::CorruptIndex)
        );
    }

    #[test]
    fn deserialized_corrupt_building_page_fails_closed_before_append() {
        let source = entries(&["entry-a", "entry-b", "entry-c"]);
        let mut owner = RawEnumerationPageIndex::new("bucket", 2).expect("page owner should initialize");
        let generation = owner.generation().expect("supported owner should expose generation");
        owner
            .ingest_owner_entries(source.clone(), 1, generation)
            .expect("page build should stage one entry");

        let encoded = rmp_serde::to_vec(&owner).expect("building page index should encode");
        let mut decoded: RawEnumerationPageIndex = rmp_serde::from_slice(&encoded).expect("building page index should decode");
        let RawEnumerationPageIndexState::Supported(inner) = &mut decoded.state else {
            panic!("decoded building owner should be supported");
        };
        inner.building.as_mut().expect("building page should be present").page_index = 9;

        assert_eq!(
            decoded.ingest_owner_entries(source, 1, decoded.generation().expect("decoded owner should expose generation")),
            Err(RawEnumerationPageIndexError::CorruptIndex)
        );
        assert_eq!(
            decoded.commit_building_page(decoded.generation().expect("decoded owner should expose generation")),
            Err(RawEnumerationPageIndexError::CorruptIndex)
        );
    }

    #[test]
    fn deserialized_duplicate_entries_across_pages_fail_closed() {
        let mut owner = RawEnumerationPageIndex::new("bucket", 1).expect("page owner should initialize");
        for source in [entries(&["entry-b"]), entries(&["entry-b", "entry-a"])] {
            let generation = owner.generation().expect("owner should expose generation");
            let outcome = owner
                .ingest_partial_owner_entries(source, 1, generation)
                .expect("single entry page should build");
            assert!(outcome.ready_to_commit);
            let generation = owner.generation().expect("ready page should expose generation");
            owner
                .commit_building_page(generation)
                .expect("single entry page should commit");
        }

        let encoded = rmp_serde::to_vec(&owner).expect("page index should encode");
        let mut decoded: RawEnumerationPageIndex = rmp_serde::from_slice(&encoded).expect("page index should decode");
        let RawEnumerationPageIndexState::Supported(inner) = &mut decoded.state else {
            panic!("decoded owner should be supported");
        };
        inner.pages[1] = inner.pages[0].clone();

        assert_eq!(decoded.committed_entries(), Err(RawEnumerationPageIndexError::CorruptIndex));
        assert_eq!(decoded.indexed_entries(), Err(RawEnumerationPageIndexError::CorruptIndex));
        assert_eq!(
            decoded.ingest_partial_owner_entries(
                entries(&["entry-a", "entry-b"]),
                1,
                decoded.generation().expect("decoded owner should expose generation")
            ),
            Err(RawEnumerationPageIndexError::CorruptIndex)
        );
    }

    #[test]
    fn empty_owner_source_becomes_ready_without_empty_page_commit() {
        let mut owner = RawEnumerationPageIndex::new("bucket", 2).expect("page owner should initialize");
        let generation = owner.generation().expect("supported owner should expose generation");
        let outcome = owner
            .ingest_owner_entries(Vec::new(), 1, generation)
            .expect("empty owner source should be a valid complete index");

        assert_eq!(
            outcome.status,
            RawEnumerationPageOwnerStatus::Ready {
                generation: 1,
                parent: "bucket".to_string(),
                committed_pages: 0,
                indexed_entries: 0,
                complete: true,
            }
        );
        assert!(!outcome.ready_to_commit);
        assert_eq!(owner.commit_building_page(1), Err(RawEnumerationPageIndexError::EmptyCommit));
    }

    #[test]
    fn complete_owner_rejects_source_drift_after_restart() {
        let mut owner = RawEnumerationPageIndex::new("bucket", 2).expect("page owner should initialize");
        let generation = owner.generation().expect("supported owner should expose generation");
        let outcome = owner
            .ingest_owner_entries(entries(&["entry-a", "entry-b"]), 2, generation)
            .expect("terminal page build should stage entries");
        assert!(outcome.ready_to_commit);
        let generation = owner.generation().expect("supported owner should expose generation");
        owner.commit_building_page(generation).expect("terminal page should commit");

        let encoded = rmp_serde::to_vec(&owner).expect("complete owner should encode");
        let mut restarted_owner: RawEnumerationPageIndex = rmp_serde::from_slice(&encoded).expect("complete owner should decode");
        let generation = restarted_owner
            .generation()
            .expect("restarted owner should expose generation");

        assert_eq!(
            restarted_owner.ingest_owner_entries(entries(&["entry-a", "entry-b", "entry-c"]), 1, generation),
            Err(RawEnumerationPageIndexError::IdentityMismatch)
        );
    }

    #[test]
    fn owner_page_rejects_invalid_boundaries_without_advancing_generation() {
        assert_eq!(RawEnumerationPageIndex::new("", 2), Err(RawEnumerationPageIndexError::EmptyParent));
        assert_eq!(RawEnumerationPageIndex::new("bucket", 0), Err(RawEnumerationPageIndexError::EmptyPage));

        let mut owner = RawEnumerationPageIndex::new("bucket", 1).expect("page owner should initialize");
        assert_eq!(
            owner.ingest_owner_entries(entries(&["entry-a"]), 0, 0),
            Err(RawEnumerationPageIndexError::EmptyBudget)
        );
        assert_eq!(
            owner.ingest_owner_entries(entries(&["nested/name"]), 1, 0),
            Err(RawEnumerationPageIndexError::InvalidEntry)
        );
        assert_eq!(owner.generation(), Some(0));

        let oversized = "x".repeat(RAW_PAGE_ENTRY_MAX_BYTES + 1);
        assert_eq!(
            owner.ingest_owner_entries(vec![oversized], 1, 0),
            Err(RawEnumerationPageIndexError::InvalidEntry)
        );
        assert_eq!(owner.generation(), Some(0));

        let exact_boundary = "x".repeat(RAW_PAGE_ENTRY_MAX_BYTES);
        let outcome = owner
            .ingest_owner_entries(vec![exact_boundary.clone()], 1, 0)
            .expect("max-sized direct entry should be accepted");
        assert!(outcome.ready_to_commit);
        let page = owner.commit_building_page(1).expect("max-sized direct entry should commit");
        assert_eq!(page.entries(), &[exact_boundary]);
    }
}
