// Copyright 2026 RustFS Team
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
use crate::sleeper::SCANNER_IDLE_MODE;

struct IdleGuard(bool);

impl IdleGuard {
    fn enabled() -> Self {
        Self(SCANNER_IDLE_MODE.swap(true, Ordering::Relaxed))
    }
}

impl Drop for IdleGuard {
    fn drop(&mut self) {
        SCANNER_IDLE_MODE.store(self.0, Ordering::Relaxed);
    }
}

#[tokio::test]
#[serial]
async fn pacing_preserves_real_folder_versions_bytes_checkpoints_and_maintenance() {
    let _idle = IdleGuard::enabled();
    let mut totals = Vec::new();
    let pending = pending_heal(PendingScannerHealKind::Object, "bucket", Some("unvisited"), None, 0, 1);
    for factor in [0.0, 0.01] {
        let (mut scanner, temp_dir) = build_test_scanner().await;
        let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir.clone());
        scanner.old_cache.info.name = "bucket".to_string();
        scanner.new_cache.info.name = "bucket".to_string();
        scanner.new_cache.info.pending_heals.push(pending.clone());
        scanner.is_erasure_mode = true;
        scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Default).for_disk_with_budget_for_tests(
            &temp_dir,
            crate::sleeper::ScannerDiskIoBudgetLimits {
                iops: Some(1_000_000),
                bytes_per_sec: Some(1_000_000),
            },
        );
        scanner
            .sleeper
            .update_from_runtime_config(factor, Duration::from_secs(1), true, 64);
        for object in 0..16 {
            let name = format!("prefix/object-{object:03}");
            let mut metadata = FileMeta::new();
            for (version, size) in [(1, 3), (2, 5)] {
                let mut info = FileInfo::new(&name, 4, 2);
                info.volume = "bucket".to_string();
                info.version_id = Some(Uuid::from_u128(version));
                info.versioned = true;
                info.size = size;
                info.mod_time = Some(OffsetDateTime::from_unix_timestamp(1_700_000_000 + size).expect("fixture timestamp"));
                metadata.add_version(info).expect("add metadata version");
            }
            write_test_object_metadata_bytes(&temp_dir, "bucket", &name, &metadata.marshal_msg().expect("encode versions")).await;
        }
        let mut entry = DataUsageEntry::default();
        scanner
            .scan_folder(
                CancellationToken::new(),
                CachedFolder {
                    name: "bucket".to_string(),
                    parent: None,
                    object_heal_prob_div: 1,
                },
                &mut entry,
            )
            .await
            .expect("complete real folder scan");
        let total = scanner.new_cache.size_recursive(&hash_path("bucket").0).expect("root totals");
        assert_eq!(total.objects, 16);
        assert_eq!(total.versions, 32);
        assert_eq!(total.size, 128);
        assert_eq!(total.failed_objects, 0);
        assert_eq!(scanner.checkpoint_objects, 16);
        assert_eq!(scanner.new_cache.info.pending_heals, vec![pending.clone()]);
        assert_eq!(scanner.pacing_debt.charged_objects, 16);
        assert_eq!(scanner.pacing_debt.pending(), Duration::ZERO, "bucket completion settles the tail");
        if factor > 0.0 {
            assert!(scanner.pacing_debt.batches > 0, "the real object path must execute pacing");
        }
        let status = crate::sleeper::scanner_disk_io_budget_status()
            .into_iter()
            .find(|status| status.disk == temp_dir.to_string_lossy())
            .expect("real walker disk charges");
        assert!(status.charged_ops >= 16 * 2);
        assert!(status.charged_bytes > 0);
        totals.push((
            serde_json::to_value(total).expect("serialize totals"),
            status.charged_ops,
            status.charged_bytes,
        ));
    }
    assert_eq!(totals[0], totals[1], "batching must preserve full accounting and every disk charge");
}

#[tokio::test]
#[serial]
async fn pacing_settles_failed_metadata_work_at_bucket_completion() {
    let _idle = IdleGuard::enabled();
    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard::new(60, 100, &mut scanner, temp_dir.clone());
    scanner.old_cache.info.name = "bucket".to_string();
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Default);
    scanner
        .sleeper
        .update_from_runtime_config(0.01, Duration::from_secs(1), true, 64);
    write_test_object_metadata_bytes(&temp_dir, "bucket", "broken", &[]).await;
    let mut entry = DataUsageEntry::default();
    scanner
        .scan_folder(
            CancellationToken::new(),
            CachedFolder {
                name: "bucket".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut entry,
        )
        .await
        .expect("corrupt metadata remains a recorded failure");
    let total = scanner
        .new_cache
        .size_recursive(&hash_path("bucket").0)
        .expect("failed root totals");
    assert_eq!(total.objects, 0);
    assert_eq!(total.failed_objects, 1);
    assert_eq!(scanner.new_cache.info.failed_objects.len(), 1);
    assert_eq!(scanner.checkpoint_objects, 0);
    assert_eq!(scanner.pacing_debt.charged_objects, 1, "failure work must enter the same pacer");
    assert_eq!(scanner.pacing_debt.batches, 1);
    assert_eq!(scanner.pacing_debt.pending(), Duration::ZERO);
}
