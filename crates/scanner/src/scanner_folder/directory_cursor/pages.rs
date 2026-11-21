// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io;
use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const PAGE_NAMES: usize = 256;
const PAGE_NAME_BYTES: usize = 32 * 1024;

/// One contiguous UTF-8 string per run. Names from the filesystem cannot
/// contain NUL. Checkpoint clones share both the payload and its local index.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct NamePage(Arc<PackedNames>);

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PackedNames {
    text: Box<str>,
    digest: [u8; 32],
    #[serde(skip)]
    starts: OnceLock<Box<[usize]>>,
    #[serde(skip)]
    valid: OnceLock<bool>,
}

impl NamePage {
    fn hash(text: &str) -> [u8; 32] {
        Sha256::new()
            .chain_update(b"rustfs-scanner-directory-names-v1")
            .chain_update(text.as_bytes())
            .finalize()
            .into()
    }

    fn new(mut names: Vec<String>) -> Self {
        names.sort_unstable();
        let mut text = String::with_capacity(names.iter().map(|n| n.len() + 1).sum());
        for name in names {
            text.push_str(&name);
            text.push('\0');
        }
        Self(Arc::new(PackedNames {
            digest: Self::hash(&text),
            text: text.into_boxed_str(),
            starts: OnceLock::new(),
            valid: OnceLock::new(),
        }))
    }

    fn starts(&self) -> &[usize] {
        self.0.starts.get_or_init(|| {
            self.names()
                .scan(0, |offset, name| {
                    let start = *offset;
                    *offset += name.len() + 1;
                    Some(start)
                })
                .collect()
        })
    }

    fn at(&self, offset: usize) -> &str {
        self.0.text[offset..].split('\0').next().unwrap_or_default()
    }

    fn get(&self, index: usize) -> Option<&str> {
        self.starts().get(index).map(|&offset| self.at(offset))
    }

    fn after(&self, last: &str) -> usize {
        self.starts().partition_point(|&offset| self.at(offset) <= last)
    }

    pub(super) fn contains(&self, name: &str) -> bool {
        self.starts().binary_search_by(|&offset| self.at(offset).cmp(name)).is_ok()
    }

    pub(super) fn digest(&self) -> &[u8; 32] {
        &self.0.digest
    }

    pub(super) fn names(&self) -> impl Iterator<Item = &str> {
        self.0.text.split_terminator('\0')
    }

    pub(super) fn valid(&self) -> bool {
        *self.0.valid.get_or_init(|| {
            let text = &self.0.text;
            text.ends_with('\0')
                && text.len() <= PAGE_NAME_BYTES + PAGE_NAMES + 4096
                && self.names().take(PAGE_NAMES + 1).count() <= PAGE_NAMES
                && self.names().all(super::valid_component)
                && self.names().is_sorted()
                && self.0.digest == Self::hash(text)
        })
    }
}

/// Immutable sorted runs are shared with checkpoint snapshots. The merge
/// retains one head per run. At a cold resume each run seeks to the first
/// name after the authenticated completed child using binary search.
#[derive(Default)]
pub(super) struct NamePages {
    pages: Vec<NamePage>,
    building: Vec<String>,
    name_bytes: usize,
    count: usize,
    heads: BinaryHeap<Reverse<(String, usize, usize)>>,
    ready: bool,
    pending: Option<String>,
    through: Option<String>,
}

impl NamePages {
    pub(super) fn len(&self) -> usize {
        self.count
    }
    pub(super) fn ready(&self) -> bool {
        self.ready
    }
    pub(super) fn through(&self) -> Option<&str> {
        self.through.as_deref()
    }

    pub(super) fn add(&mut self, name: String) {
        self.name_bytes = self.name_bytes.saturating_add(name.len());
        self.count = self.count.saturating_add(1);
        self.building.push(name);
    }

    fn flush(&mut self) {
        if self.building.is_empty() {
            return;
        }
        let page = NamePage::new(std::mem::take(&mut self.building));
        if let Some(first) = page.get(0) {
            self.heads.push(Reverse((first.to_owned(), self.pages.len(), 0)));
        }
        self.pages.push(page);
        self.name_bytes = 0;
    }

    pub(super) async fn flush_full(&mut self) -> io::Result<()> {
        if self.building.len() >= PAGE_NAMES || self.name_bytes >= PAGE_NAME_BYTES {
            self.flush();
        }
        Ok(())
    }

    pub(super) fn finish(&mut self) {
        self.flush();
        self.ready = true;
    }

    pub(super) fn snapshot(&self) -> Vec<NamePage> {
        let mut pages = self.pages.clone();
        if !self.building.is_empty() {
            pages.push(NamePage::new(self.building.clone()));
        }
        pages
    }

    pub(super) fn restore(pages: &[NamePage], ready: bool, through: Option<&str>) -> Self {
        let mut names = Self {
            pages: pages.to_vec(),
            count: pages.iter().map(|p| p.starts().len()).sum(),
            ready,
            ..Self::default()
        };
        if !ready
            && names
                .pages
                .last()
                .is_some_and(|page| page.starts().len() < PAGE_NAMES && page.0.text.len() < PAGE_NAME_BYTES)
            && let Some(page) = names.pages.pop()
        {
            names.building = page.names().map(str::to_owned).collect();
            names.name_bytes = names.building.iter().map(String::len).sum();
        }
        names.seek_after(through);
        names
    }

    pub(super) fn seek_after(&mut self, through: Option<&str>) {
        self.pending = None;
        self.through = through.map(str::to_owned);
        self.heads.clear();
        for (page_index, page) in self.pages.iter().enumerate() {
            let start = through.map_or(0, |last| page.after(last));
            if let Some(next) = page.get(start) {
                self.heads.push(Reverse((next.to_owned(), page_index, start)));
            }
        }
    }

    pub(super) fn rewind(&mut self) {
        self.seek_after(None);
    }

    pub(super) async fn next(&mut self) -> io::Result<Option<String>> {
        if self.pending.is_some() {
            return Ok(self.pending.clone());
        }
        while let Some(Reverse((name, page, index))) = self.heads.pop() {
            if let Some(next) = self.pages[page].get(index + 1) {
                self.heads.push(Reverse((next.to_owned(), page, index + 1)));
            }
            if self.through.as_ref() == Some(&name) {
                continue;
            }
            self.pending = Some(name.clone());
            return Ok(Some(name));
        }
        Ok(None)
    }

    pub(super) fn acknowledge(&mut self) -> bool {
        if let Some(name) = self.pending.take() {
            self.through = Some(name);
            return true;
        }
        false
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_membership_shares_names_and_rejects_corrupt_payloads() {
        let page = NamePage::new((0..PAGE_NAMES).rev().map(|i| format!("obj-{i:06}")).collect());
        assert!(page.valid());
        let shared = page.clone();
        assert!(Arc::ptr_eq(&page.0, &shared.0));
        let encoded = rmp_serde::to_vec_named(&page).expect("serialize packed names");
        assert!(encoded.len() < PAGE_NAMES * 12, "checkpoint stores one delimited string per page");
        let restored: NamePage = rmp_serde::from_slice(&encoded).expect("cold name page");
        assert!(restored.valid());
        assert!(restored.contains("obj-000137"));
        assert_eq!(restored.get(restored.after("obj-000137")), Some("obj-000138"));
        for text in ["", "obj", "a\0\0b\0", "b\0a\0", "../other\0"] {
            let malformed = NamePage(Arc::new(PackedNames {
                text: text.into(),
                digest: NamePage::hash(text),
                starts: OnceLock::new(),
                valid: OnceLock::new(),
            }));
            assert!(!malformed.valid(), "invalid membership accepted: {text:?}");
        }
        let mut bytes = encoded;
        let offset = bytes
            .windows(10)
            .position(|value| value == b"obj-000137")
            .expect("encoded name");
        bytes[offset] = b'x';
        assert!(!rmp_serde::from_slice::<NamePage>(&bytes).expect("decodable payload").valid());
    }

    #[tokio::test]
    async fn interleaved_pages_resume_at_each_unacknowledged_name() {
        let mut pages = NamePages::default();
        let mut expected = std::collections::BTreeSet::new();
        // Each consecutive sorted run interleaves with every other run. One
        // duplicate straddles two runs and a partial run remains in memory.
        for run in 0..5 {
            for index in (0..PAGE_NAMES).rev() {
                let name = format!("item-{index:04}-{run:02}");
                expected.insert(name.clone());
                pages.add(name);
                pages.flush_full().await.expect("append sorted run");
            }
        }
        pages.add("item-0000-02".to_owned());
        pages.finish();
        for name in &expected {
            assert_eq!(pages.next().await.expect("scheduled child").as_ref(), Some(name));
            assert_eq!(pages.next().await.expect("retry after cancellation").as_ref(), Some(name));
            assert!(pages.acknowledge());
        }
        assert_eq!(pages.next().await.expect("sealed EOF"), None);
        pages.rewind();
        assert_eq!(pages.next().await.expect("fresh coverage").as_ref(), expected.first());
    }
}
