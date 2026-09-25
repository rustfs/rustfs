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

use super::*;
use crate::raw_page_index::RAW_PAGE_DIGEST_ENTRIES;

fn checkpoint(mut index: RawEnumerationPageIndex) -> RawEnumerationPageIndex {
    if let RawEnumerationPageIndexState::Supported(inner) = &index.state
        && inner.building.is_some()
    {
        index
            .commit_building_page(index.generation().expect("supported index generation"))
            .expect("reference checkpoint must commit its partial page");
    }
    index
}

fn compare_with_cumulative_ingestion(initial: RawEnumerationPageIndex, source: &[&str]) {
    let mut reference = initial.clone();
    let floor = reference.indexed_entries().expect("reference entries").len();
    let mut writer = RawEnumerationPageWriter::new(initial).expect("validated writer");
    let mut observed = Vec::new();
    for entry in source {
        observed.push((*entry).to_owned());
        let expected = if observed.len() < floor {
            Ok(())
        } else {
            reference
                .ingest_partial_owner_entries(observed.clone(), 1, reference.generation().expect("reference generation"))
                .and_then(|outcome| {
                    if outcome.ready_to_commit {
                        reference.commit_building_page(reference.generation().expect("commit generation"))?;
                    }
                    Ok(())
                })
        };
        let actual = writer.record_entry(entry);
        assert_eq!(actual, expected, "observation {observed:?}");
        if actual.is_err() {
            return;
        }
        let expected = checkpoint(reference.clone());
        let actual = writer.checkpoint().expect("incremental checkpoint");
        assert_eq!(actual, expected, "page identity and generation at {observed:?}");
        assert_eq!(
            rmp_serde::to_vec(&actual).expect("encode incremental index"),
            rmp_serde::to_vec(&expected).expect("encode reference index"),
            "persisted format must remain unchanged"
        );
        assert_eq!(writer.indexed_entry_count(), actual.indexed_entries().expect("validate output").len());
    }
}

#[test]
fn raw_enumeration_writer_matches_cumulative_pages_with_duplicates_and_restarts() {
    for limit in [1, 2, 3, 128] {
        let source = ["c", "a", "b", "b", "f", "d", "e", "g"];
        let empty = RawEnumerationPageIndex::new("bucket/metadata", limit).expect("empty index");
        compare_with_cumulative_ingestion(empty.clone(), &source);
        let mut writer = RawEnumerationPageWriter::new(empty).expect("writer");
        for prefix in 0..source.len() {
            writer.record_entry(source[prefix]).expect("entry before interruption");
            let saved = writer.checkpoint().expect("save interrupted enumeration");
            let encoded = rmp_serde::to_vec(&saved).expect("encode checkpoint");
            let restored = rmp_serde::from_slice(&encoded).expect("restore checkpoint");
            compare_with_cumulative_ingestion(restored, &["h", "g", "f", "e", "d", "c", "b", "a", "i", "j"]);
        }
    }
}

#[test]
fn raw_enumeration_writer_preserves_restored_building_and_terminal_pages() {
    for complete in [false, true] {
        for budget in [1, 2] {
            let mut index = RawEnumerationPageIndex::new("bucket", 2).expect("index");
            let source = ["a".to_owned(), "b".to_owned()];
            if complete {
                index.ingest_owner_entries(source, budget, 0).expect("complete source");
            } else {
                index.ingest_partial_owner_entries(source, budget, 0).expect("partial source");
            }
            compare_with_cumulative_ingestion(index.clone(), &["b", "a", "a", "c", "d"]);
            compare_with_cumulative_ingestion(checkpoint(index), &["b", "a", "a", "c", "d"]);
        }
    }
}

#[test]
fn raw_enumeration_writer_keeps_identity_check_when_terminal_builder_commits() {
    for limit in [2, 3] {
        let mut index = RawEnumerationPageIndex::new("bucket", limit).expect("index");
        index
            .ingest_owner_entries(["a".to_owned(), "b".to_owned()], 2, 0)
            .expect("terminal building page");
        compare_with_cumulative_ingestion(index.clone(), &["b", "a", "b", "c"]);
        compare_with_cumulative_ingestion(index, &["foreign", "a", "b", "c"]);
    }
}

#[test]
fn raw_enumeration_writer_rejects_complete_source_drift_after_observation_floor() {
    let mut index = RawEnumerationPageIndex::new("bucket", 2).expect("index");
    index
        .ingest_owner_entries(["a".to_owned(), "b".to_owned()], 2, 0)
        .expect("complete source");
    let index = checkpoint(index);
    for source in [["foreign", "a"], ["a", "foreign"], ["a", "a"], ["b", "a"]] {
        compare_with_cumulative_ingestion(index.clone(), &source);
    }
}

#[test]
fn raw_enumeration_writer_rejects_corruption_at_restore() {
    let mut writer = RawEnumerationPageWriter::new(RawEnumerationPageIndex::new("bucket", 2).expect("index")).expect("writer");
    for entry in ["a", "b", "c"] {
        writer.record_entry(entry).expect("valid entry");
    }
    let valid = writer.checkpoint().expect("checkpoint");
    for case in 0..7 {
        let mut corrupt = valid.clone();
        let RawEnumerationPageIndexState::Supported(inner) = &mut corrupt.state else {
            panic!("supported checkpoint");
        };
        match case {
            0 => inner.pages[0].digest[0] ^= 1,
            1 => inner.pages[1].entries_start = 0,
            2 => inner.pages[1] = inner.pages[0].clone(),
            3 => inner.page_entry_limit = 0,
            4 => inner.complete = true,
            5 => inner.parent.clear(),
            6 => {
                inner.building = Some(RawEnumerationPageBuilder {
                    page_index: 2,
                    entries_start: 3,
                    entries: vec!["a".to_owned()],
                    terminal: false,
                });
            }
            _ => unreachable!(),
        }
        let bytes = rmp_serde::to_vec(&corrupt).expect("encode damaged checkpoint");
        let restored = rmp_serde::from_slice(&bytes).expect("decode untrusted checkpoint");
        assert!(
            matches!(RawEnumerationPageWriter::new(restored), Err(RawEnumerationPageIndexError::CorruptIndex)),
            "corruption case {case} must not become trusted runtime state"
        );
    }
}

#[test]
fn raw_enumeration_writer_rejects_invalid_names_without_publishing_them() {
    for entry in ["", ".", "..", "nested/name", &"x".repeat(16 * 1024 + 1)] {
        let mut writer =
            RawEnumerationPageWriter::new(RawEnumerationPageIndex::new("bucket", 128).expect("index")).expect("writer");
        let before = writer.checkpoint().expect("empty checkpoint");
        assert_eq!(writer.record_entry(entry), Err(RawEnumerationPageIndexError::InvalidEntry));
        assert_eq!(writer.checkpoint().expect("checkpoint after rejection"), before);
        assert_eq!(writer.indexed_entry_count(), 0);
    }
}

#[test]
fn raw_enumeration_writer_hashes_only_new_pages_after_restore() {
    let count = 4096;
    let mut writer = RawEnumerationPageWriter::new(RawEnumerationPageIndex::new("bucket", 128).expect("index")).expect("writer");
    RAW_PAGE_DIGEST_ENTRIES.set(0);
    for i in (0..count).rev() {
        writer.record_entry(&format!("entry-{i:06}")).expect("append entry");
    }
    assert_eq!(
        RAW_PAGE_DIGEST_ENTRIES.get(),
        count,
        "each committed entry is hashed once during enumeration"
    );
    let saved = writer.checkpoint().expect("checkpoint");
    assert_eq!(saved.indexed_entries().expect("validated checkpoint").len(), count);
    let mut resumed = RawEnumerationPageWriter::new(saved).expect("restore large checkpoint");
    RAW_PAGE_DIGEST_ENTRIES.set(0);
    for i in 0..count * 2 {
        resumed.record_entry(&format!("entry-{i:06}")).expect("resume and append");
    }
    assert_eq!(resumed.indexed_entry_count(), count * 2);
    assert_eq!(
        RAW_PAGE_DIGEST_ENTRIES.get(),
        count,
        "restored pages must not be rehashed for each entry or commit"
    );
}

#[test]
fn raw_enumeration_writer_keeps_snapshot_independent_of_later_appends() {
    let mut writer = RawEnumerationPageWriter::new(RawEnumerationPageIndex::new("bucket", 128).expect("index")).expect("writer");
    for i in 0..127 {
        writer.record_entry(&format!("entry-{i:03}")).expect("append before boundary");
    }
    let partial = writer.checkpoint().expect("partial page checkpoint");
    writer.record_entry("entry-127").expect("full page");
    let full = writer.checkpoint().expect("full page checkpoint");
    writer.record_entry("entry-128").expect("next page");
    assert_eq!(partial.indexed_entries().expect("old partial snapshot").len(), 127);
    assert_eq!(full.indexed_entries().expect("old full snapshot").len(), 128);
    assert_eq!(
        writer
            .checkpoint()
            .expect("new snapshot")
            .indexed_entries()
            .expect("new entries")
            .len(),
        129
    );
    assert_ne!(partial, full);
}
