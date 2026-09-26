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

#[tokio::test]
async fn raw_enumeration_reuses_validated_parent_and_skips_unrelated_index() {
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir),
    };
    scanner.old_cache.info.name = "bucket".to_owned();
    scanner.old_cache.info.scan_progress = Some(crate::DataUsageScanProgress {
        started_plan: crate::DataUsageScanPlanDigest([1; 32]),
        requested_plan: crate::DataUsageScanPlanDigest([1; 32]),
    });
    scanner.old_cache.info.source = Some(crate::DataUsageCacheSource::new(0, 0));
    scanner.old_cache.info.scan_identity = Some(crate::DataUsageScanIdentity {
        version: 1,
        bucket_incarnation: Uuid::from_u128(1),
        set_layout: crate::DataUsageScanPlanDigest([2; 32]),
        publication_epoch: 1,
        tier_registry_generation: 0,
        scan_mode: HealScanMode::Normal,
    });
    let mut saved =
        RawEnumerationPageWriter::new(RawEnumerationPageIndex::new("bucket/metadata", 128).expect("index")).expect("writer");
    for i in 0..1024 {
        saved.record_entry(&format!("entry-{i:04}")).expect("initial entry");
    }
    scanner.old_cache.info.scan_raw_enumeration_page_index = Some(saved.checkpoint().expect("saved index"));
    scanner.record_raw_enumeration_entry("bucket/metadata", "entry-0000");
    RAW_PAGE_DIGEST_ENTRIES.set(0);
    for i in 1..1024 {
        scanner.record_raw_enumeration_entry("bucket/metadata", &format!("entry-{i:04}"));
    }
    assert_eq!(
        RAW_PAGE_DIGEST_ENTRIES.get(),
        0,
        "the same parent must not revalidate its restored index per entry"
    );
    let progress = scanner.raw_enumeration_progress.last().expect("active progress");
    assert_eq!(progress.checkpointable_entry_count(), 1024);
    assert!(progress.has_checkpointable_page_index());
    assert_eq!(
        RAW_PAGE_DIGEST_ENTRIES.get(),
        0,
        "progress selection must use cached counts, not snapshot/validation"
    );
    for i in 0..32 {
        let parent = format!("bucket/other-{i}");
        assert!(scanner.raw_enumeration_committed_entry_oracle(&parent).is_empty());
        scanner.record_raw_enumeration_entry(&parent, "object");
        scanner.finish_raw_enumeration_parent(&parent);
    }
    assert_eq!(
        RAW_PAGE_DIGEST_ENTRIES.get(),
        0,
        "unrelated directories must not validate the saved metadata index"
    );
    let (_, checkpoint) = scanner.take_raw_enumeration_resume_state();
    assert_eq!(
        checkpoint
            .expect("metadata checkpoint")
            .indexed_entries()
            .expect("valid restored output")
            .len(),
        1024
    );
}

#[test]
fn raw_enumeration_progress_invalid_entry_discards_index_but_retains_diagnostic_cursor() {
    let mut progress = RawEnumerationProgress::new("bucket", None);
    progress.record_entry("valid");
    progress.record_entry("invalid/name");
    progress.record_entry("later");
    assert_eq!(progress.checkpointable_entry_count(), 0);
    assert!(progress.page_index().is_none());
    assert_eq!(progress.cursor().expect("diagnostic cursor").entries_seen, 3);
}
