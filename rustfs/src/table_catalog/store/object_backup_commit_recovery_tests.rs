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
use crate::table_catalog::test_support::TestCatalogObjectBackend;

fn fixture() -> (
    ObjectTableCatalogStore<TestCatalogObjectBackend>,
    Vec<ObjectCatalogBackupObject>,
    TableEntry,
) {
    let backup: ObjectCatalogBackup =
        serde_json::from_slice(include_bytes!("../tests/fixtures/object-catalog-backup-v1.json")).expect("existing v1 fixture");
    let entry = serde_json::from_slice(
        &backup
            .objects
            .iter()
            .find(|object| object.object.ends_with(TABLE_ENTRY_FILE))
            .unwrap()
            .data,
    )
    .expect("fixture table owner");
    let store = ObjectTableCatalogStore::new(TestCatalogObjectBackend::default());
    store
        .validate_catalog_backup_metadata_identities("analytics", &backup.objects)
        .expect("real current metadata is valid before testing recovery policy");
    (store, backup.objects, entry)
}

fn object(path: String, data: Vec<u8>) -> ObjectCatalogBackupObject {
    ObjectCatalogBackupObject {
        bucket: RUSTFS_META_BUCKET.to_string(),
        object: path,
        size_bytes: u64::try_from(data.len()).unwrap(),
        etag: Some("fixture-etag".to_string()),
        sha256: backup_sha256(&data),
        data,
    }
}

fn current_commit(entry: &TableEntry) -> CommitLogEntry {
    CommitLogEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        commit_id: "current-commit".to_string(),
        idempotency_key: Some("current-request".to_string()),
        table_id: entry.table_id.clone(),
        operation: "append".to_string(),
        expected_version_token: "base-token".to_string(),
        new_version_token: entry.version_token.clone(),
        previous_metadata_location: "tables/table-id/metadata/00000.metadata.json".to_string(),
        new_metadata_location: entry.metadata_location.clone(),
        requirements: Vec::new(),
        status: CommitLogStatus::Committed,
        writer: Some("fixture-writer".to_string()),
        created_at: Some("2026-10-31T00:00:00Z".to_string()),
        updated_at: Some("2026-10-31T00:00:01Z".to_string()),
    }
}

fn log_path(commit: &CommitLogEntry) -> String {
    TableCatalogObjectPaths::default().commit_log_entry_path("analytics", &commit.table_id, &commit.commit_id)
}

fn index_path(commit: &CommitLogEntry) -> String {
    TableCatalogObjectPaths::default().commit_idempotency_entry_path(
        "analytics",
        &commit.table_id,
        commit.idempotency_key.as_deref().expect("fixture index key"),
    )
}

fn put_record(objects: &mut Vec<ObjectCatalogBackupObject>, path: String, commit: &CommitLogEntry) {
    objects.retain(|object| object.object != path);
    objects.push(object(path, serde_json::to_vec(commit).unwrap()));
}

fn publish(objects: &mut Vec<ObjectCatalogBackupObject>, commit: &CommitLogEntry) {
    put_record(objects, log_path(commit), commit);
    if commit.idempotency_key.is_some() {
        put_record(objects, index_path(commit), commit);
    }
}

fn assert_recovery(result: TableCatalogStoreResult<()>, state: &str) {
    let error = result.expect_err("unstable commit state must be rejected");
    assert!(
        matches!(&error, TableCatalogStoreError::Conflict(message)
        if message.contains("commit recovery required") && message.contains(state)),
        "{error}"
    );
}

#[test]
fn backup_commit_policy_accepts_the_original_v1_snapshot_with_valid_metadata() {
    let (store, objects, _) = fixture();
    let before = objects.clone();
    store.validate_backup_commit_recovery("analytics", &objects).unwrap();
    assert_eq!(objects, before);
}

#[test]
fn backup_commit_policy_accepts_finalized_current_commits_with_or_without_idempotency() {
    for idempotent in [false, true] {
        let (store, mut objects, entry) = fixture();
        let mut commit = current_commit(&entry);
        if !idempotent {
            commit.idempotency_key = None;
        }
        publish(&mut objects, &commit);
        let before = objects.clone();
        store.validate_backup_commit_recovery("analytics", &objects).unwrap();
        assert_eq!(objects, before);
    }
}

#[test]
fn backup_commit_policy_rejects_post_cas_finalization_without_mutating_source_bytes() {
    let (store, mut objects, entry) = fixture();
    let mut staged = current_commit(&entry);
    staged.status = CommitLogStatus::Staged;
    publish(&mut objects, &staged);
    let before = objects.clone();
    assert_recovery(store.validate_backup_commit_recovery("analytics", &objects), "FinalizationRequired");
    assert_eq!(objects, before);
}

#[test]
fn backup_commit_policy_rejects_staging_before_the_table_pointer_advanced() {
    let (store, mut objects, entry) = fixture();
    let mut staged = current_commit(&entry);
    staged.status = CommitLogStatus::Staged;
    staged.expected_version_token = entry.version_token;
    staged.previous_metadata_location = entry.metadata_location;
    staged.new_version_token = "next-token".to_string();
    staged.new_metadata_location = "tables/table-id/metadata/00002.metadata.json".to_string();
    publish(&mut objects, &staged);
    assert_recovery(store.validate_backup_commit_recovery("analytics", &objects), "StagedBeforeTableUpdate");
}

#[test]
fn backup_commit_policy_rejects_missing_and_stale_idempotency_receipts() {
    for missing in [false, true] {
        let (store, mut objects, entry) = fixture();
        let commit = current_commit(&entry);
        publish(&mut objects, &commit);
        if missing {
            objects.retain(|object| object.object != index_path(&commit));
        } else {
            let mut stale = commit.clone();
            stale.status = CommitLogStatus::Staged;
            put_record(&mut objects, index_path(&commit), &stale);
        }
        assert_recovery(
            store.validate_backup_commit_recovery("analytics", &objects),
            "IdempotencyIndexRepairRequired",
        );
    }
}

#[test]
fn backup_commit_policy_keeps_manual_review_failures_distinct() {
    for failure in ["conflicting-index", "failed-log", "unreachable-log"] {
        let (store, mut objects, entry) = fixture();
        let mut commit = current_commit(&entry);
        if failure == "failed-log" {
            commit.status = CommitLogStatus::Failed;
        } else if failure == "unreachable-log" {
            commit.new_version_token = "unreachable-token".to_string();
            commit.new_metadata_location = "tables/table-id/metadata/00099.metadata.json".to_string();
        }
        publish(&mut objects, &commit);
        if failure == "conflicting-index" {
            let mut conflicting = commit.clone();
            conflicting.operation = "overwrite".to_string();
            put_record(&mut objects, index_path(&commit), &conflicting);
        }
        assert_recovery(store.validate_backup_commit_recovery("analytics", &objects), "ManualReview");
    }
}

#[test]
fn backup_commit_policy_uses_the_existing_history_proof_for_older_staged_commits() {
    for older_status in [CommitLogStatus::Committed, CommitLogStatus::Staged] {
        let (store, mut objects, entry) = fixture();
        let mut latest = current_commit(&entry);
        latest.expected_version_token = "intermediate-token".to_string();
        latest.previous_metadata_location = "tables/table-id/metadata/00002.metadata.json".to_string();
        let mut older = current_commit(&entry);
        older.commit_id = "historical-commit".to_string();
        older.idempotency_key = Some("historical-request".to_string());
        older.new_version_token = latest.expected_version_token.clone();
        older.new_metadata_location = latest.previous_metadata_location.clone();
        older.status = older_status.clone();
        publish(&mut objects, &older);
        publish(&mut objects, &latest);
        let result = store.validate_backup_commit_recovery("analytics", &objects);
        if older_status == CommitLogStatus::Committed {
            result.expect("healthy history retains earlier finalized receipts");
        } else {
            assert_recovery(result, "FinalizationRequired");
        }
    }
}

#[test]
fn backup_commit_policy_detects_an_idempotency_receipt_without_its_commit() {
    let (store, mut objects, entry) = fixture();
    let commit = current_commit(&entry);
    publish(&mut objects, &commit);
    objects.retain(|object| object.object != log_path(&commit));
    assert_recovery(
        store.validate_backup_commit_recovery("analytics", &objects),
        "idempotency receipt has no identical commit log",
    );
}

#[test]
fn backup_commit_policy_reuses_owner_and_lookup_path_validation() {
    for changed in ["table-id", "commit-id", "idempotency-key", "version"] {
        let (store, mut objects, entry) = fixture();
        let commit = current_commit(&entry);
        publish(&mut objects, &commit);
        let mut corrupt = commit.clone();
        let path = match changed {
            "table-id" => {
                corrupt.table_id = "another-table-id".to_string();
                log_path(&commit)
            }
            "commit-id" => {
                corrupt.commit_id = "another-commit".to_string();
                log_path(&commit)
            }
            "idempotency-key" => {
                corrupt.idempotency_key = Some("another-request".to_string());
                index_path(&commit)
            }
            "version" => {
                corrupt.version = 0;
                log_path(&commit)
            }
            _ => unreachable!(),
        };
        put_record(&mut objects, path, &corrupt);
        assert!(matches!(
            store.validate_backup_commit_recovery("analytics", &objects),
            Err(TableCatalogStoreError::Invalid(_))
        ));
    }
}

#[test]
fn backup_commit_policy_uses_the_renamed_active_pointer_instead_of_the_source_tombstone() {
    let (store, mut objects, mut destination) = fixture();
    objects.retain(|object| !object.object.ends_with(TABLE_ENTRY_FILE));
    let mut source = destination.clone();
    source.state = TableCatalogEntryState::Deleted;
    source.version_token = "old-token".to_string();
    source.metadata_location = "tables/table-id/metadata/00000.metadata.json".to_string();
    source.generation = source.generation.saturating_sub(1);
    destination.table = "renamed_orders".to_string();
    for entry in [&source, &destination] {
        let path = TableCatalogObjectPaths::default().table_entry_path(
            "analytics",
            &Namespace::parse(&entry.namespace).unwrap(),
            &IdentifierSegment::parse(&entry.table).unwrap(),
        );
        objects.push(object(path, serde_json::to_vec(entry).unwrap()));
    }
    store
        .validate_catalog_backup_metadata_identities("analytics", &objects)
        .expect("renamed active pointer still uses the original valid metadata");
    publish(&mut objects, &current_commit(&destination));
    let before = objects.clone();
    store
        .validate_backup_commit_recovery("analytics", &objects)
        .expect("source tombstone is not current commit authority");
    assert_eq!(objects, before);
}

#[test]
fn backup_commit_policy_rejects_two_active_pointers_for_one_table_id() {
    let (store, mut objects, mut duplicate) = fixture();
    duplicate.table = "duplicate_owner".to_string();
    let path = TableCatalogObjectPaths::default().table_entry_path(
        "analytics",
        &Namespace::parse(&duplicate.namespace).unwrap(),
        &IdentifierSegment::parse(&duplicate.table).unwrap(),
    );
    objects.push(object(path, serde_json::to_vec(&duplicate).unwrap()));
    assert_recovery(
        store.validate_backup_commit_recovery("analytics", &objects),
        "more than one active catalog pointer",
    );
}

#[test]
fn backup_commit_policy_preserves_paired_history_left_after_an_object_table_drop() {
    let (store, mut objects, entry) = fixture();
    publish(&mut objects, &current_commit(&entry));
    objects.retain(|object| !object.object.ends_with(TABLE_ENTRY_FILE) && object.bucket == RUSTFS_META_BUCKET);
    let before = objects.clone();
    store
        .validate_backup_commit_recovery("analytics", &objects)
        .expect("backups can retain paired history from an earlier drop");
    assert_eq!(objects, before);
}

#[test]
fn backup_commit_policy_checks_both_receipt_directions_even_without_an_active_table() {
    for keep_log in [false, true] {
        let (store, mut objects, entry) = fixture();
        let commit = current_commit(&entry);
        publish(&mut objects, &commit);
        objects.retain(|object| !object.object.ends_with(TABLE_ENTRY_FILE) && object.bucket == RUSTFS_META_BUCKET);
        let missing = if keep_log { index_path(&commit) } else { log_path(&commit) };
        objects.retain(|object| object.object != missing);
        assert_recovery(store.validate_backup_commit_recovery("analytics", &objects), "receipt");
    }
}

#[test]
fn backup_commit_policy_preserves_proven_superseded_receipts_without_rewriting_them() {
    for idempotent in [false, true] {
        let (store, mut objects, entry) = fixture();
        let winner = current_commit(&entry);
        let loser = CommitLogEntry {
            commit_id: "superseded-commit".to_string(),
            idempotency_key: idempotent.then(|| "superseded-request".to_string()),
            new_version_token: "superseded-token".to_string(),
            new_metadata_location: "tables/table-id/metadata/loser.metadata.json".to_string(),
            status: CommitLogStatus::Staged,
            ..winner.clone()
        };
        publish(&mut objects, &winner);
        publish(&mut objects, &loser);
        let before = objects.clone();
        store
            .validate_backup_commit_recovery("analytics", &objects)
            .expect("a proven losing request is not pending recovery");
        assert_eq!(objects, before, "sealing must retain the original STAGED audit bytes");
    }
}

#[test]
fn backup_commit_policy_never_accepts_a_loser_without_intact_winner_and_index_proof() {
    for failure in ["missing-winner", "wrong-base", "conflicting-index"] {
        let (store, mut objects, entry) = fixture();
        let winner = current_commit(&entry);
        let mut loser = CommitLogEntry {
            commit_id: "superseded-commit".to_string(),
            idempotency_key: Some("superseded-request".to_string()),
            new_version_token: "superseded-token".to_string(),
            new_metadata_location: "tables/table-id/metadata/loser.metadata.json".to_string(),
            status: CommitLogStatus::Staged,
            ..winner.clone()
        };
        if failure == "wrong-base" {
            loser.expected_version_token = "unknown-base-token".to_string();
        }
        publish(&mut objects, &winner);
        publish(&mut objects, &loser);
        if failure == "missing-winner" {
            objects.retain(|object| object.object != log_path(&winner) && object.object != index_path(&winner));
        } else if failure == "conflicting-index" {
            let mut conflicting = loser.clone();
            conflicting.operation = "overwrite".to_string();
            put_record(&mut objects, index_path(&loser), &conflicting);
        }
        let before = objects.clone();
        assert_recovery(store.validate_backup_commit_recovery("analytics", &objects), "ManualReview");
        assert_eq!(objects, before);
    }
}
