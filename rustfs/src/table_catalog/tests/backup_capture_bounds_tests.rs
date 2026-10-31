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

#[derive(Debug, PartialEq, Eq)]
struct BackupCaptureObjectFingerprint {
    size_bytes: usize,
    sha256: String,
    etag: String,
    mod_time: Option<OffsetDateTime>,
}

#[derive(Debug, PartialEq, Eq)]
struct BackupCapturePersistedState {
    objects: BTreeMap<(String, String), BackupCaptureObjectFingerprint>,
    puts: BTreeMap<(String, String), usize>,
    deletes: BTreeMap<(String, String), usize>,
}

async fn backup_capture_persisted_state(backend: &TestCatalogObjectBackend) -> BackupCapturePersistedState {
    let state = backend.state.lock().await;
    // Retain fingerprints rather than another copy of the bounded large fixtures.
    let objects = state
        .objects
        .iter()
        .map(|(key, object)| {
            let sha256 =
                hex_simd::encode_to_string(<sha2::Sha256 as sha2::Digest>::digest(&object.data), hex_simd::AsciiCase::Lower);
            (
                key.clone(),
                BackupCaptureObjectFingerprint {
                    size_bytes: object.data.len(),
                    sha256,
                    etag: object.etag.clone(),
                    mod_time: object.mod_time,
                },
            )
        })
        .collect();
    BackupCapturePersistedState {
        objects,
        puts: state.put_attempts.clone(),
        deletes: state.delete_attempts.clone(),
    }
}

#[tokio::test]
async fn backup_capture_rejects_malformed_pages_before_publishing_any_object() {
    for scenario in [
        "foreign-prefix",
        "duplicate-object",
        "reverse-order",
        "oversized",
        "empty-truncated",
        "repeated-cursor",
    ] {
        let (backend, _, _) = backup_fixture().await;
        let paths = TableCatalogObjectPaths::default();
        let prefix = paths.table_bucket_root_prefix("analytics");
        let before = backup_capture_persisted_state(&backend).await;
        let existing = before
            .objects
            .keys()
            .filter(|(bucket, object)| bucket == RUSTFS_META_BUCKET && object.starts_with(&prefix))
            .map(|(_, object)| object.clone())
            .collect::<Vec<_>>();
        assert!(existing.len() >= 2, "the healthy fixture provides two real sorted catalog objects");
        let first = existing[0].clone();
        let second = existing[1].clone();
        let pages = match scenario {
            "foreign-prefix" => vec![TableCatalogObjectListPage {
                objects: vec![paths.table_bucket_entry_path("unrelated")],
                is_truncated: false,
            }],
            "duplicate-object" => vec![TableCatalogObjectListPage {
                objects: vec![first.clone(), first],
                is_truncated: false,
            }],
            "reverse-order" => vec![TableCatalogObjectListPage {
                objects: vec![second, first],
                is_truncated: false,
            }],
            "oversized" => vec![TableCatalogObjectListPage {
                objects: (0..=TABLE_CATALOG_LIST_MAX_KEYS)
                    .map(|index| format!("{prefix}oversized/{index:04}.json"))
                    .collect(),
                is_truncated: false,
            }],
            "empty-truncated" => vec![TableCatalogObjectListPage {
                objects: Vec::new(),
                is_truncated: true,
            }],
            "repeated-cursor" => vec![
                TableCatalogObjectListPage {
                    objects: vec![first.clone()],
                    is_truncated: true,
                },
                TableCatalogObjectListPage {
                    objects: vec![first],
                    is_truncated: false,
                },
            ],
            _ => unreachable!(),
        };
        let page_key = (RUSTFS_META_BUCKET.to_string(), prefix);
        backend
            .state
            .lock()
            .await
            .list_page_overrides
            .entry(page_key.clone())
            .or_default()
            .extend(pages);

        let error = ObjectTableCatalogStore::new(backend.clone())
            .create_object_catalog_backup("analytics", None)
            .await
            .expect_err("malformed backend pages must not become a backup");
        let expected = if matches!(scenario, "oversized" | "empty-truncated") {
            "invalid catalog backup listing page"
        } else {
            "catalog backup listing escaped its prefix or did not advance"
        };
        assert!(
            matches!(&error, TableCatalogStoreError::Invalid(message) if message == expected),
            "{scenario}: {error}"
        );
        assert_eq!(
            backup_capture_persisted_state(&backend).await,
            before,
            "{scenario} mutated an object, ETag or PUT/DELETE attempt count"
        );
        assert!(
            backend
                .state
                .lock()
                .await
                .list_page_overrides
                .get(&page_key)
                .expect("injected page queue")
                .is_empty(),
            "{scenario} did not consume the injected listing boundary"
        );
    }
}

#[tokio::test]
async fn backup_capture_stops_reading_when_the_encoded_payload_budget_is_exhausted() {
    let (backend, _, _) = backup_fixture().await;
    let prefix = TableCatalogObjectPaths::default().table_bucket_root_prefix("analytics");
    let first_path = format!("{prefix}zz-backup-budget-0.bin");
    let second_path = format!("{prefix}zz-backup-budget-1.bin");
    let tail_path = format!("{prefix}zz-backup-budget-2-tail.bin");
    // Byte 120 occupies four JSON bytes per array element. Each payload is just
    // below 8 MiB to keep its individual encoding below 32 MiB; the existing
    // valid catalog plus both payloads exceeds the shared 64 MiB budget.
    let payload_bytes = 8 * 1024 * 1024 - 256;
    backend
        .seed_object(RUSTFS_META_BUCKET, &first_path, vec![b'x'; payload_bytes])
        .await;
    backend
        .seed_object(RUSTFS_META_BUCKET, &second_path, vec![b'x'; payload_bytes])
        .await;
    backend
        .seed_object(RUSTFS_META_BUCKET, &tail_path, b"unreachable tail".to_vec())
        .await;
    backend.fail_next_read(RUSTFS_META_BUCKET, &tail_path).await;
    let before = backup_capture_persisted_state(&backend).await;
    let before_reads = backend.state.lock().await.read_attempts.clone();

    let error = ObjectTableCatalogStore::new(backend.clone())
        .create_object_catalog_backup("analytics", None)
        .await
        .expect_err("incremental payload accounting must stop before the unread tail");
    assert!(
        matches!(
            &error,
            TableCatalogStoreError::Invalid(message)
                if message == "catalog backup payload exceeds its encoded byte budget"
        ),
        "expected the incremental byte budget, not the injected read or metadata validation error: {error}"
    );
    assert_eq!(
        backup_capture_persisted_state(&backend).await,
        before,
        "budget rejection changed persistent object bytes, ETags or write attempts"
    );
    let state = backend.state.lock().await;
    for object in [&first_path, &second_path] {
        let key = (RUSTFS_META_BUCKET.to_string(), object.clone());
        assert_eq!(
            state.read_attempts.get(&key).copied().unwrap_or_default(),
            before_reads.get(&key).copied().unwrap_or_default() + 1,
            "the collector did not measure the expected bounded payload: {object}"
        );
    }
    let tail_key = (RUSTFS_META_BUCKET.to_string(), tail_path);
    let tail_reads = before_reads.get(&tail_key).copied().unwrap_or_default();
    assert_eq!(
        state.read_attempts.get(&tail_key).copied().unwrap_or_default(),
        tail_reads,
        "the collector read beyond the exhausted byte budget"
    );
    assert!(
        state
            .fail_read_attempts
            .get(&tail_key)
            .is_some_and(|attempts| attempts.contains(&(tail_reads + 1))),
        "the unread tail fault was unexpectedly consumed"
    );
}
