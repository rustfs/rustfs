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

struct StagedLoserFixture {
    backend: TestCatalogObjectBackend,
    store: ObjectTableCatalogStore<TestCatalogObjectBackend>,
    loser: TableCommitRequest,
    winner: TableCommitRequest,
    committed: TableCommitResult,
}

async fn staged_loser_fixture(interrupt_winner_finalization: bool) -> StagedLoserFixture {
    let backend = TestCatalogObjectBackend::default();
    let store = ObjectTableCatalogStore::new(backend.clone());
    let bucket = "analytics";
    let namespace = Namespace::parse("sales").unwrap();
    let table = IdentifierSegment::parse("orders").unwrap();
    let initial = test_table_entry(
        bucket,
        &namespace,
        &table,
        default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
    );
    store.put_table_bucket(test_bucket_entry(bucket)).await.unwrap();
    store
        .create_namespace(test_namespace_entry(bucket, &namespace))
        .await
        .unwrap();
    store.create_table(initial.clone()).await.unwrap();
    for name in ["00001", "loser", "winner", "reloaded"] {
        let location = default_table_metadata_file_path(&namespace, &table, &format!("{name}.metadata.json"));
        let mut metadata = super::super::test_support::table_metadata_json(&initial.table_uuid, &initial.warehouse_location);
        metadata["properties"] = serde_json::json!({"writer": name});
        backend
            .seed_object(bucket, &location, serde_json::to_vec(&metadata).unwrap())
            .await;
    }
    let paths = TableCatalogObjectPaths::default();
    let table_path = paths.table_entry_path(bucket, &namespace, &table);
    backend.fail_next_put(RUSTFS_META_BUCKET, &table_path).await;
    let loser = TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        commit_id: "paused-writer-a0".to_string(),
        idempotency_key: Some("a0-request".to_string()),
        operation: "set-properties".to_string(),
        expected_version_token: initial.version_token.clone(),
        expected_metadata_location: initial.metadata_location.clone(),
        new_metadata_location: default_table_metadata_file_path(&namespace, &table, "loser.metadata.json"),
        requirements: Vec::new(),
        writer: None,
    };
    assert_matches!(store.commit_table(loser.clone()).await, Err(TableCatalogStoreError::Internal(_)));
    let winner = TableCommitRequest {
        commit_id: "winner-c1".to_string(),
        idempotency_key: Some("c1-request".to_string()),
        new_metadata_location: default_table_metadata_file_path(&namespace, &table, "winner.metadata.json"),
        ..loser.clone()
    };
    if interrupt_winner_finalization {
        let winner_log = paths.commit_log_entry_path(bucket, &initial.table_id, &winner.commit_id);
        backend.fail_put_attempt(RUSTFS_META_BUCKET, &winner_log, 2).await;
    }
    let committed = store.commit_table(winner.clone()).await.unwrap();
    StagedLoserFixture {
        backend,
        store,
        loser,
        winner,
        committed,
    }
}

#[tokio::test]
async fn superseded_staged_commit_recovers_without_rewriting_receipts_or_pointer() {
    let StagedLoserFixture {
        backend,
        store,
        loser,
        winner,
        committed,
    } = staged_loser_fixture(false).await;
    let bucket = loser.table_bucket.as_str();
    let before = backend.state.lock().await.objects.clone();

    let recovered = store.recover_table_commits(bucket, "sales", "orders").await.unwrap();
    assert_eq!(recovered.manual_review_count, 0, "a durable same-base winner proves the staged loser");
    assert_eq!(recovered.finalized_count, 0, "a losing writer must never be finalized as committed");
    let lost = recovered
        .commits
        .iter()
        .find(|entry| entry.commit_id == loser.commit_id)
        .unwrap();
    assert_eq!(lost.status, CommitLogStatus::Staged);
    assert_eq!(serde_json::to_value(&lost.recovery_state).unwrap(), "SUPERSEDED");
    assert_eq!(lost.idempotency_index_status, TableCommitIdempotencyIndexStatus::Matches);
    let after = backend.state.lock().await.objects.clone();
    assert_eq!(after.len(), before.len());
    for (path, original) in &before {
        let actual = after.get(path).unwrap();
        assert_eq!(actual.data, original.data, "recovery changed bytes at {path:?}");
        assert_eq!(actual.etag, original.etag, "recovery rewrote {path:?}");
    }

    let restarted = ObjectTableCatalogStore::new(backend.clone());
    let diagnostics = restarted.diagnose_table_catalog(bucket, "sales", "orders", 0).await.unwrap();
    assert_eq!(diagnostics.recovery_status, TableCatalogRecoveryStatus::Healthy);
    assert_matches!(restarted.commit_table(loser.clone()).await, Err(TableCatalogStoreError::Conflict(_)));
    let mut stale_request = loser.clone();
    stale_request.commit_id = "stale-new-request".to_string();
    stale_request.idempotency_key = Some("stale-new-key".to_string());
    assert_matches!(restarted.commit_table(stale_request).await, Err(TableCatalogStoreError::Conflict(_)));
    let mut different_payload = loser.clone();
    different_payload.operation = "replace".to_string();
    assert_matches!(
        restarted.commit_table(different_payload.clone()).await,
        Err(TableCatalogStoreError::Conflict(_))
    );
    assert_eq!(
        restarted.load_table(bucket, "sales", "orders").await.unwrap(),
        Some(committed.table.clone())
    );
    assert_eq!(restarted.commit_table(winner).await.unwrap().table, committed.table);

    let reloaded = TableCommitRequest {
        commit_id: "reloaded-writer-a1".to_string(),
        idempotency_key: Some("a1-request".to_string()),
        expected_version_token: committed.table.version_token.clone(),
        expected_metadata_location: committed.table.metadata_location.clone(),
        new_metadata_location: default_table_metadata_file_path(
            &Namespace::parse("sales").unwrap(),
            &IdentifierSegment::parse("orders").unwrap(),
            "reloaded.metadata.json",
        ),
        ..different_payload
    };
    let next = restarted.commit_table(reloaded.clone()).await.unwrap();
    assert_eq!(next.table.generation, committed.table.generation + 1);
    assert_eq!(restarted.commit_table(reloaded).await.unwrap().table, next.table);
    let restarted = ObjectTableCatalogStore::new(backend);
    assert_eq!(
        restarted
            .diagnose_table_catalog(bucket, "sales", "orders", 0)
            .await
            .unwrap()
            .recovery_status,
        TableCatalogRecoveryStatus::Healthy
    );
}

#[tokio::test]
async fn superseded_staged_commit_waits_for_durable_winner_finalization() {
    let StagedLoserFixture {
        backend,
        store,
        loser,
        committed,
        ..
    } = staged_loser_fixture(true).await;
    let paths = TableCatalogObjectPaths::default();
    let log_path = paths.commit_log_entry_path(&loser.table_bucket, &committed.table.table_id, &loser.commit_id);
    let before = backend.read_object(RUSTFS_META_BUCKET, &log_path).await.unwrap().unwrap();
    let planned = store
        .plan_table_commit_recovery("analytics", "sales", "orders")
        .await
        .unwrap();
    assert_eq!(planned.manual_review_count, 1, "a merely staged winner is insufficient proof");
    assert_eq!(planned.finalization_required_count, 1);

    let recovered = store.recover_table_commits("analytics", "sales", "orders").await.unwrap();
    assert_eq!(recovered.finalized_count, 1);
    assert_eq!(recovered.manual_review_count, 0);
    let loser_state = recovered
        .commits
        .iter()
        .find(|entry| entry.commit_id == loser.commit_id)
        .unwrap();
    assert_eq!(loser_state.recovery_state, TableCommitRecoveryState::Superseded);
    assert_eq!(loser_state.status, CommitLogStatus::Staged);
    let after = backend.read_object(RUSTFS_META_BUCKET, &log_path).await.unwrap().unwrap();
    assert_eq!(after.data, before.data);
    assert_eq!(after.etag, before.etag);
    assert_eq!(store.load_table("analytics", "sales", "orders").await.unwrap(), Some(committed.table));
    assert_eq!(
        store
            .diagnose_table_catalog("analytics", "sales", "orders", 0)
            .await
            .unwrap()
            .recovery_status,
        TableCatalogRecoveryStatus::Healthy
    );
}

#[tokio::test]
async fn superseded_staged_commit_requires_retained_winner_proof_after_restart() {
    let StagedLoserFixture {
        backend,
        store,
        loser,
        winner,
        committed,
    } = staged_loser_fixture(false).await;
    let healthy = store.recover_table_commits("analytics", "sales", "orders").await.unwrap();
    assert_eq!(healthy.manual_review_count, 0);
    let winner_path =
        TableCatalogObjectPaths::default().commit_log_entry_path("analytics", &committed.table.table_id, &winner.commit_id);
    assert!(
        backend
            .state
            .lock()
            .await
            .objects
            .remove(&(RUSTFS_META_BUCKET.to_string(), winner_path))
            .is_some()
    );
    let restarted = ObjectTableCatalogStore::new(backend);
    let before = restarted.load_table("analytics", "sales", "orders").await.unwrap();
    let report = restarted.recover_table_commits("analytics", "sales", "orders").await.unwrap();
    assert_eq!(report.manual_review_count, 1);
    assert_eq!(report.finalized_count, 0);
    assert_eq!(report.commits[0].recovery_state, TableCommitRecoveryState::ManualReview);
    assert_eq!(report.commits[0].status, CommitLogStatus::Staged);
    assert_matches!(restarted.commit_table(loser).await, Err(TableCatalogStoreError::Conflict(_)));
    assert_eq!(restarted.load_table("analytics", "sales", "orders").await.unwrap(), before);
    let diagnostics = restarted
        .diagnose_table_catalog("analytics", "sales", "orders", 0)
        .await
        .unwrap();
    assert_eq!(diagnostics.recovery_status, TableCatalogRecoveryStatus::ManualReviewRequired);
    assert!(
        diagnostics
            .recommended_actions
            .contains(&TableCatalogRecoveryAction::ReviewCommitLog)
    );
}

#[tokio::test]
async fn superseded_staged_commit_does_not_repair_a_conflicting_index() {
    let StagedLoserFixture {
        backend,
        store,
        loser,
        committed,
        ..
    } = staged_loser_fixture(false).await;
    let mut conflicting = store
        .get_commit_by_id("analytics", &committed.table.table_id, &loser.commit_id)
        .await
        .unwrap()
        .unwrap();
    conflicting.new_version_token = "contradictory-token".to_string();
    let path = TableCatalogObjectPaths::default().commit_idempotency_entry_path(
        "analytics",
        &committed.table.table_id,
        loser.idempotency_key.as_deref().unwrap(),
    );
    backend
        .seed_object(RUSTFS_META_BUCKET, &path, serde_json::to_vec(&conflicting).unwrap())
        .await;
    let before = backend.state.lock().await.objects.clone();
    let report = store.recover_table_commits("analytics", "sales", "orders").await.unwrap();
    assert_eq!(report.manual_review_count, 1);
    assert_eq!(report.finalized_count, 0);
    let loser_state = report
        .commits
        .iter()
        .find(|entry| entry.commit_id == loser.commit_id)
        .unwrap();
    assert_eq!(loser_state.recovery_state, TableCommitRecoveryState::ManualReview);
    assert_eq!(loser_state.idempotency_index_status, TableCommitIdempotencyIndexStatus::Conflicting);
    let after = backend.state.lock().await.objects.clone();
    assert_eq!(before.len(), after.len());
    for (key, original) in &before {
        let actual = after.get(key).unwrap();
        assert_eq!(actual.data, original.data);
        assert_eq!(actual.etag, original.etag);
    }
    assert_matches!(store.commit_table(loser).await, Err(TableCatalogStoreError::Conflict(_)));
}

fn history_fixture() -> (TableEntry, CommitLogEntry, CommitLogEntry) {
    let namespace = Namespace::parse("sales").unwrap();
    let table_name = IdentifierSegment::parse("orders").unwrap();
    let metadata = |name| default_table_metadata_file_path(&namespace, &table_name, name);
    let mut table = test_table_entry("analytics", &namespace, &table_name, metadata("winner.metadata.json"));
    table.version_token = "winner-token".to_string();
    table.generation = 2;
    let loser = CommitLogEntry {
        version: TABLE_CATALOG_ENTRY_VERSION,
        commit_id: "loser".to_string(),
        idempotency_key: None,
        table_id: table.table_id.clone(),
        operation: "set-properties".to_string(),
        expected_version_token: "base-token".to_string(),
        new_version_token: "loser-token".to_string(),
        previous_metadata_location: metadata("base.metadata.json"),
        new_metadata_location: metadata("loser.metadata.json"),
        requirements: Vec::new(),
        status: CommitLogStatus::Staged,
        writer: None,
        created_at: None,
        updated_at: None,
    };
    let winner = CommitLogEntry {
        commit_id: "winner".to_string(),
        new_version_token: table.version_token.clone(),
        new_metadata_location: table.metadata_location.clone(),
        status: CommitLogStatus::Committed,
        ..loser.clone()
    };
    (table, loser, winner)
}

fn classify_history(
    table: &TableEntry,
    target: &CommitLogEntry,
    commits: &[CommitLogEntry],
    indexed: Option<&CommitLogEntry>,
) -> TableCommitRecoveryEntry {
    let history = TableCommitHistoryIndex::new(table, commits.iter());
    table_commit_recovery_entry_from_history(table, target, indexed, &history)
}

#[test]
fn superseded_history_requires_a_reachable_finalized_same_base_winner() {
    for case in [
        "staged-winner",
        "failed-winner",
        "wrong-base-token",
        "wrong-base-location",
        "foreign-table",
        "disconnected-winner",
        "same-commit-id",
    ] {
        let (mut table, loser, mut winner) = history_fixture();
        match case {
            "staged-winner" => winner.status = CommitLogStatus::Staged,
            "failed-winner" => winner.status = CommitLogStatus::Failed,
            "wrong-base-token" => winner.expected_version_token = "other-base-token".to_string(),
            "wrong-base-location" => winner.previous_metadata_location = "other/base.metadata.json".to_string(),
            "foreign-table" => winner.table_id = "other-table-id".to_string(),
            "disconnected-winner" => {
                table.metadata_location = "other/current.metadata.json".to_string();
                table.version_token = "other-current-token".to_string();
            }
            "same-commit-id" => winner.commit_id.clone_from(&loser.commit_id),
            _ => unreachable!(),
        }
        let recovery = classify_history(&table, &loser, &[loser.clone(), winner], None);
        assert_eq!(recovery.recovery_state, TableCommitRecoveryState::ManualReview, "{case}");
    }
}

#[test]
fn superseded_history_is_stable_after_later_commits() {
    let (mut table, loser, winner) = history_fixture();
    let immediate = classify_history(&table, &loser, &[loser.clone(), winner.clone()], None);
    assert_eq!(immediate.recovery_state, TableCommitRecoveryState::Superseded);
    assert!(immediate.reason.contains(&winner.commit_id));
    let later = CommitLogEntry {
        commit_id: "later".to_string(),
        previous_metadata_location: table.metadata_location.clone(),
        expected_version_token: table.version_token.clone(),
        new_metadata_location: "later/current.metadata.json".to_string(),
        new_version_token: "later-token".to_string(),
        ..winner.clone()
    };
    table.metadata_location.clone_from(&later.new_metadata_location);
    table.version_token.clone_from(&later.new_version_token);
    table.generation += 1;
    let after = classify_history(&table, &loser, &[loser.clone(), winner, later], None);
    assert_eq!(after.recovery_state, TableCommitRecoveryState::Superseded);
    assert_eq!(after.reason, immediate.reason);
}

#[test]
fn superseded_history_rejects_failed_or_committed_losers() {
    for status in [CommitLogStatus::Failed, CommitLogStatus::Committed] {
        let (table, mut loser, winner) = history_fixture();
        loser.status = status;
        let recovery = classify_history(&table, &loser, &[loser.clone(), winner], None);
        assert_eq!(recovery.recovery_state, TableCommitRecoveryState::ManualReview);
    }
}

#[test]
fn superseded_history_rejects_ambiguity_cycles_and_reused_tokens() {
    for case in [
        "ambiguous-current",
        "cycle",
        "reused-history-token",
        "loser-token-in-history",
        "off-history-ambiguity",
    ] {
        let (mut table, mut loser, winner) = history_fixture();
        let mut extra = CommitLogEntry {
            commit_id: "extra".to_string(),
            ..winner.clone()
        };
        let mut commits = vec![loser.clone(), winner.clone()];
        match case {
            "ambiguous-current" => commits.push(extra),
            "cycle" => {
                extra.previous_metadata_location.clone_from(&winner.new_metadata_location);
                extra.expected_version_token.clone_from(&winner.new_version_token);
                extra.new_metadata_location.clone_from(&winner.previous_metadata_location);
                extra.new_version_token.clone_from(&winner.expected_version_token);
                commits.push(extra);
            }
            "reused-history-token" => {
                extra.previous_metadata_location.clone_from(&winner.new_metadata_location);
                extra.expected_version_token.clone_from(&winner.new_version_token);
                extra.new_metadata_location = "later/current.metadata.json".to_string();
                extra.new_version_token.clone_from(&winner.expected_version_token);
                table.metadata_location.clone_from(&extra.new_metadata_location);
                table.version_token.clone_from(&extra.new_version_token);
                commits.push(extra);
            }
            "loser-token-in-history" => {
                loser.new_version_token.clone_from(&winner.expected_version_token);
                commits[0] = loser.clone();
            }
            "off-history-ambiguity" => {
                extra.new_metadata_location = "unknown/metadata.json".to_string();
                extra.new_version_token = "unknown-token".to_string();
                commits.push(extra.clone());
                extra.commit_id = "another-extra".to_string();
                commits.push(extra);
            }
            _ => unreachable!(),
        }
        assert_eq!(
            classify_history(&table, &loser, &commits, None).recovery_state,
            TableCommitRecoveryState::ManualReview,
            "{case}"
        );
    }
}

#[test]
fn superseded_history_requires_an_intact_idempotency_binding() {
    let (table, mut loser, winner) = history_fixture();
    loser.idempotency_key = Some("loser-request".to_string());
    let commits = [loser.clone(), winner];
    assert_eq!(
        classify_history(&table, &loser, &commits, Some(&loser)).recovery_state,
        TableCommitRecoveryState::Superseded
    );
    let missing = classify_history(&table, &loser, &commits, None);
    assert_eq!(missing.recovery_state, TableCommitRecoveryState::ManualReview);
    assert_eq!(missing.idempotency_index_status, TableCommitIdempotencyIndexStatus::Missing);
    let mut stale = loser.clone();
    stale.status = CommitLogStatus::Committed;
    let state = classify_history(&table, &loser, &commits, Some(&stale));
    assert_eq!(state.recovery_state, TableCommitRecoveryState::ManualReview);
    assert_eq!(state.idempotency_index_status, TableCommitIdempotencyIndexStatus::Stale);
    let mut conflicting = loser.clone();
    conflicting.commit_id = "another-request".to_string();
    let state = classify_history(&table, &loser, &commits, Some(&conflicting));
    assert_eq!(state.recovery_state, TableCommitRecoveryState::ManualReview);
    assert_eq!(state.idempotency_index_status, TableCommitIdempotencyIndexStatus::Conflicting);
}

#[test]
fn superseded_history_never_relabels_a_committed_staged_ancestor() {
    let (table, mut ancestor, winner) = history_fixture();
    ancestor.new_metadata_location.clone_from(&winner.previous_metadata_location);
    ancestor.new_version_token.clone_from(&winner.expected_version_token);
    ancestor.previous_metadata_location = "older/base.metadata.json".to_string();
    ancestor.expected_version_token = "older-base-token".to_string();
    let recovery = classify_history(&table, &ancestor, &[ancestor.clone(), winner], None);
    assert_eq!(recovery.recovery_state, TableCommitRecoveryState::FinalizationRequired);
}
