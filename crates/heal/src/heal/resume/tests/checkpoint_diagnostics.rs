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

async fn diagnostic_row(disk: &DiskStore, task_id: &str) -> serde_json::Value {
    let rows = CheckpointManager::blocked_records(disk)
        .await
        .expect("read-only checkpoint inventory");
    serde_json::to_value(
        rows.into_iter()
            .find(|row| row.task_id == task_id)
            .expect("blocked task listed"),
    )
    .expect("bounded diagnostic encoding")
}

#[tokio::test]
async fn checkpoint_diagnostics_verify_historical_bytes_without_unblocking() {
    for legacy in LEGACY_SIGNED_CHECKPOINTS {
        let (temp, disk) = schema_test_disk().await;
        let task_id = "00000000-0000-4000-8000-000000000002";
        let path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
        disk.write_all(RUSTFS_META_BUCKET, &path, legacy.as_bytes().to_vec().into())
            .await
            .expect("historical checkpoint");
        disk.write_all(RUSTFS_META_BUCKET, &blocked_marker_path(task_id), b"blocked".to_vec().into())
            .await
            .expect("legacy marker");
        for _ in 0..2 {
            let row = diagnostic_row(&disk, task_id).await;
            let validation = &row["validation"];
            assert_eq!(validation["stage"], "validated");
            assert_eq!(validation["signaturePresent"], true);
            assert_eq!(validation["signatureValid"], true);
            assert_eq!(validation["taskIdentityMatches"], true);
            assert_eq!(validation["legacyRewind"], true);
            assert_eq!(validation["readerAccepted"], true);
            assert_eq!(validation["recoveryStatus"], "candidate_execution_unauthorized");
            assert_eq!(validation["ownerFence"], "unknown");
            assert_eq!(validation["executionAuthorized"], false);
            assert_eq!(
                disk.read_all(RUSTFS_META_BUCKET, &path)
                    .await
                    .expect("checkpoint retained")
                    .as_ref(),
                legacy.as_bytes()
            );
            assert_eq!(
                disk.read_all(RUSTFS_META_BUCKET, &blocked_marker_path(task_id))
                    .await
                    .expect("marker retained")
                    .as_ref(),
                b"blocked"
            );
            assert!(row.to_string().len() < 1024, "the diagnostic contains a summary only");
        }
        temp.close().expect("remove diagnostic fixture");
    }
}

#[tokio::test]
async fn checkpoint_diagnostics_keep_current_schemas_and_cross_task_identity_separate() {
    for admin in [false, true] {
        let (temp, disk) = schema_test_disk().await;
        let task_id = ResumeUtils::generate_task_id();
        let manager = CheckpointManager::new(disk.clone(), task_id.clone())
            .await
            .expect("current checkpoint writer");
        if admin {
            manager
                .set_admin(AdminErasureCheckpoint {
                    set_disk_id: "pool_0_set_0".to_string(),
                    options: Default::default(),
                    buckets: [("private-bucket".to_string(), Some(uuid::Uuid::new_v4()))].into(),
                    outcome: Default::default(),
                    completed: false,
                })
                .await
                .expect("current schema eight checkpoint");
        }
        manager
            .add_processed_object("private-customer-key".to_string())
            .await
            .expect("private object identity");
        manager.update_position(1, 2).await.expect("persist current checkpoint");
        let path = format!("{BUCKET_META_PREFIX}/{task_id}_{RESUME_CHECKPOINT_FILE}");
        let bytes = disk.read_all(RUSTFS_META_BUCKET, &path).await.expect("writer bytes");
        disk.write_all(RUSTFS_META_BUCKET, &blocked_marker_path(&task_id), b"blocked".to_vec().into())
            .await
            .expect("current marker");
        let row = diagnostic_row(&disk, &task_id).await;
        assert_eq!(row["validation"]["schemaVersion"], if admin { 8 } else { 7 });
        assert_eq!(row["validation"]["signatureValid"], true);
        assert_eq!(row["validation"]["legacyRewind"], false);
        assert!(!row.to_string().contains("private-customer-key"));
        assert!(!row.to_string().contains("private-bucket"));
        let other = ResumeUtils::generate_task_id();
        let other_path = format!("{BUCKET_META_PREFIX}/{other}_{RESUME_CHECKPOINT_FILE}");
        disk.write_all(RUSTFS_META_BUCKET, &other_path, bytes.clone())
            .await
            .expect("cross-task evidence");
        disk.write_all(RUSTFS_META_BUCKET, &blocked_marker_path(&other), b"blocked".to_vec().into())
            .await
            .expect("cross-task marker");
        let other_row = diagnostic_row(&disk, &other).await;
        assert_eq!(other_row["validation"]["stage"], "identity_conflict");
        assert_eq!(other_row["validation"]["taskIdentityMatches"], false);
        assert_eq!(other_row["validation"]["readerAccepted"], false);
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, &path)
                .await
                .expect("original evidence")
                .as_ref(),
            bytes.as_ref()
        );
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, &other_path)
                .await
                .expect("cross-task evidence")
                .as_ref(),
            bytes.as_ref()
        );
        temp.close().expect("remove current diagnostic fixture");
    }
}

async fn inspect_bytes_unchanged(disk: &DiskStore, task: &str, bytes: Vec<u8>) -> serde_json::Value {
    let path = format!("{BUCKET_META_PREFIX}/{task}_{RESUME_CHECKPOINT_FILE}");
    disk.write_all(RUSTFS_META_BUCKET, &path, bytes.clone().into())
        .await
        .expect("diagnostic input");
    disk.write_all(RUSTFS_META_BUCKET, &blocked_marker_path(task), b"blocked".to_vec().into())
        .await
        .expect("diagnostic isolation marker");
    let row = diagnostic_row(disk, task).await;
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, &path)
            .await
            .expect("snapshot unchanged")
            .as_ref(),
        bytes
    );
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, &blocked_marker_path(task))
            .await
            .expect("marker unchanged")
            .as_ref(),
        b"blocked"
    );
    assert_eq!(row["validation"]["ownerFence"], "unknown");
    assert_eq!(row["validation"]["executionAuthorized"], false);
    row
}

#[tokio::test]
async fn checkpoint_diagnostics_distinguish_rejection_stages_without_exposing_payloads() {
    let (temp, disk) = schema_test_disk().await;
    let task = "00000000-0000-4000-8000-000000000002";
    for (bytes, stage, signature_valid) in [
        (b"private-invalid-json".to_vec(), "malformed", serde_json::Value::Null),
        (
            LEGACY_SIGNED_CHECKPOINTS[0]
                .replace("\"current_object_index\":500", "\"current_object_index\":501")
                .into_bytes(),
            "digest_mismatch",
            serde_json::json!(false),
        ),
        (
            LEGACY_SIGNED_CHECKPOINTS[0]
                .replace("\"schema_version\":5", "\"schema_version\":99")
                .into_bytes(),
            "future_schema",
            serde_json::Value::Null,
        ),
        (
            LEGACY_SIGNED_CHECKPOINTS[0]
                .replace("\"schema_version\":5", "\"schema_version\":5,\"schema_version\":6")
                .into_bytes(),
            "malformed",
            serde_json::Value::Null,
        ),
        (
            LEGACY_SIGNED_CHECKPOINTS[0]
                .replace(
                    "\"checkpoint_time\":1700000000",
                    "\"checkpoint_time\":1700000000,\"dangling_delete_retry_not_before\":18446744073709551615",
                )
                .into_bytes(),
            "invalid_deadline",
            serde_json::Value::Null,
        ),
    ] {
        let row = inspect_bytes_unchanged(&disk, task, bytes).await;
        assert_eq!(row["validation"]["stage"], stage);
        assert_eq!(row["validation"]["signatureValid"], signature_valid);
        assert_eq!(row["validation"]["readerAccepted"], false);
        assert_eq!(row["validation"]["recoveryStatus"], "rejected");
        assert!(!row.to_string().contains("private-invalid-json"));
    }
    for admin in [false, true] {
        let task = ResumeUtils::generate_task_id();
        let manager = CheckpointManager::new(disk.clone(), task.to_string())
            .await
            .expect("current negative fixture");
        if admin {
            manager
                .set_admin(AdminErasureCheckpoint {
                    set_disk_id: "pool_0_set_0".to_string(),
                    options: Default::default(),
                    buckets: [("private-bucket".to_string(), Some(uuid::Uuid::new_v4()))].into(),
                    outcome: Default::default(),
                    completed: false,
                })
                .await
                .expect("admin negative fixture");
        }
        let path = format!("{BUCKET_META_PREFIX}/{task}_{RESUME_CHECKPOINT_FILE}");
        let bytes = disk.read_all(RUSTFS_META_BUCKET, &path).await.expect("modern bytes");
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).expect("modern fixture");
        value["integrity_digest"] = serde_json::Value::Null;
        let row = inspect_bytes_unchanged(&disk, &task, serde_json::to_vec(&value).expect("missing digest fixture")).await;
        assert_eq!(row["validation"]["stage"], "signature_missing");
        assert_eq!(row["validation"]["signaturePresent"], false);
        assert!(row["validation"]["signatureValid"].is_null(), "missing is not a performed verification");
        assert_eq!(row["validation"]["readerAccepted"], false);
    }
    temp.close().expect("remove rejection stage fixture");
}

#[tokio::test]
async fn checkpoint_diagnostics_preserve_legacy_sidecar_and_unsigned_rewind() {
    use sha2::{Digest, Sha256};
    let (temp, disk) = schema_test_disk().await;
    let task = "00000000-0000-4000-8000-000000000002";
    let mut value: serde_json::Value = serde_json::from_str(LEGACY_SIGNED_CHECKPOINTS[0]).expect("legacy fixture");
    value["integrity_digest"] = serde_json::Value::Null;
    let bytes = serde_json::to_vec(&value).expect("unsigned legacy fixture");
    let row = inspect_bytes_unchanged(&disk, task, bytes.clone()).await;
    assert_eq!(row["validation"]["signaturePresent"], false);
    assert!(row["validation"]["signatureValid"].is_null());
    assert_eq!(row["validation"]["readerAccepted"], true);
    assert_eq!(row["validation"]["legacyRewind"], true);
    let digest_path = format!("{BUCKET_META_PREFIX}/{task}_ahm_checkpoint.sha256");
    for (digest, expected) in [
        (base64_simd::STANDARD.encode_to_string(Sha256::digest(&bytes)).into_bytes(), true),
        (b"wrong-sidecar-digest".to_vec(), false),
    ] {
        disk.write_all(RUSTFS_META_BUCKET, &digest_path, digest.clone().into())
            .await
            .expect("legacy sidecar");
        let row = inspect_bytes_unchanged(&disk, task, bytes.clone()).await;
        assert_eq!(row["validation"]["signaturePresent"], true);
        assert_eq!(row["validation"]["signatureValid"], expected);
        assert_eq!(row["validation"]["readerAccepted"], expected);
        assert_eq!(
            disk.read_all(RUSTFS_META_BUCKET, &digest_path)
                .await
                .expect("sidecar retained")
                .as_ref(),
            digest
        );
    }
    temp.close().expect("remove legacy sidecar fixture");
}

#[tokio::test]
async fn checkpoint_diagnostics_never_parse_truncated_or_budget_exhausted_snapshots() {
    let (temp, disk) = schema_test_disk().await;
    let task = ResumeUtils::generate_task_id();
    let manager = CheckpointManager::new(disk.clone(), task.clone())
        .await
        .expect("boundary fixture");
    let path = format!("{BUCKET_META_PREFIX}/{task}_{RESUME_CHECKPOINT_FILE}");
    let mut bytes = disk
        .read_all(RUSTFS_META_BUCKET, &path)
        .await
        .expect("boundary writer bytes")
        .to_vec();
    bytes.resize(1024 * 1024, b' ');
    let row = inspect_bytes_unchanged(&disk, &task, bytes.clone()).await;
    assert_eq!(row["validation"]["readerAccepted"], true, "exact byte limit remains readable");
    bytes.push(b' ');
    let row = inspect_bytes_unchanged(&disk, &task, bytes).await;
    assert_eq!(row["validation"]["stage"], "too_large");
    assert!(row["validation"]["readerAccepted"].is_null());
    assert!(row["validation"]["signatureValid"].is_null());
    drop(manager);
    temp.close().expect("remove snapshot size fixture");

    let (temp, disk) = schema_test_disk().await;
    for _ in 0..5 {
        let task = ResumeUtils::generate_task_id();
        let manager = CheckpointManager::new(disk.clone(), task.clone())
            .await
            .expect("aggregate budget fixture");
        let path = format!("{BUCKET_META_PREFIX}/{task}_{RESUME_CHECKPOINT_FILE}");
        let mut bytes = disk
            .read_all(RUSTFS_META_BUCKET, &path)
            .await
            .expect("budget writer bytes")
            .to_vec();
        bytes.resize(1024 * 1024, b' ');
        disk.write_all(RUSTFS_META_BUCKET, &path, bytes.into())
            .await
            .expect("exact-limit snapshot");
        disk.write_all(RUSTFS_META_BUCKET, &blocked_marker_path(&task), b"blocked".to_vec().into())
            .await
            .expect("budget marker");
        drop(manager);
    }
    let rows = serde_json::to_value(CheckpointManager::blocked_records(&disk).await.expect("bounded inventory"))
        .expect("bounded summary");
    let rows = rows.as_array().expect("rows");
    assert_eq!(rows.len(), 5, "budget exhaustion cannot omit isolated responsibilities");
    assert_eq!(rows.iter().filter(|row| row["validation"]["readerAccepted"] == true).count(), 4);
    let exhausted = rows
        .iter()
        .find(|row| row["validation"]["stage"] == "budget_exhausted")
        .expect("explicit budget unknown");
    assert!(exhausted["validation"]["readerAccepted"].is_null());
    assert!(exhausted["validation"]["signatureValid"].is_null());
    temp.close().expect("remove aggregate budget fixture");
}

#[tokio::test]
async fn checkpoint_diagnostics_keep_missing_and_unreadable_evidence_unknown() {
    let (temp, disk) = schema_test_disk().await;
    let task = ResumeUtils::generate_task_id();
    disk.write_all(RUSTFS_META_BUCKET, &blocked_marker_path(&task), b"blocked".to_vec().into())
        .await
        .expect("missing snapshot marker");
    let row = diagnostic_row(&disk, &task).await;
    assert_eq!(row["validation"]["stage"], "snapshot_missing");
    assert!(row["validation"]["readerAccepted"].is_null());
    let mut legacy: serde_json::Value = serde_json::from_str(LEGACY_SIGNED_CHECKPOINTS[0]).expect("sidecar read fixture");
    legacy["task_id"] = serde_json::json!(task);
    legacy["integrity_digest"] = serde_json::Value::Null;
    let path = format!("{BUCKET_META_PREFIX}/{task}_{RESUME_CHECKPOINT_FILE}");
    let bytes = serde_json::to_vec(&legacy).expect("sidecar-only fixture");
    disk.write_all(RUSTFS_META_BUCKET, &path, bytes.clone().into())
        .await
        .expect("legacy sidecar-only snapshot");
    let sidecar = temp
        .path()
        .join(RUSTFS_META_BUCKET)
        .join(format!("{BUCKET_META_PREFIX}/{task}_ahm_checkpoint.sha256"));
    tokio::fs::create_dir(&sidecar)
        .await
        .expect("unreadable digest is a directory");
    let row = diagnostic_row(&disk, &task).await;
    assert_eq!(row["validation"]["stage"], "read_unavailable");
    assert!(row["validation"]["signaturePresent"].is_null());
    assert!(row["validation"]["signatureValid"].is_null());
    assert!(row["validation"]["readerAccepted"].is_null());
    assert_eq!(row["validation"]["recoveryStatus"], "unknown");
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, &path)
            .await
            .expect("checkpoint retained")
            .as_ref(),
        bytes
    );
    assert_eq!(
        disk.read_all(RUSTFS_META_BUCKET, &blocked_marker_path(&task))
            .await
            .expect("marker retained")
            .as_ref(),
        b"blocked"
    );
    assert!(sidecar.is_dir(), "read errors cannot replace the original evidence");
    temp.close().expect("remove unavailable evidence fixture");
}

#[tokio::test]
async fn checkpoint_diagnostics_record_budget_does_not_omit_blocked_tasks() {
    let (temp, disk) = schema_test_disk().await;
    for _ in 0..65 {
        let task = ResumeUtils::generate_task_id();
        disk.write_all(RUSTFS_META_BUCKET, &blocked_marker_path(&task), b"blocked".to_vec().into())
            .await
            .expect("record budget marker");
        let path = format!("{BUCKET_META_PREFIX}/{task}_{RESUME_CHECKPOINT_FILE}");
        disk.write_all(RUSTFS_META_BUCKET, &path, b"malformed".to_vec().into())
            .await
            .expect("record budget snapshot");
    }
    let rows = serde_json::to_value(
        CheckpointManager::blocked_records(&disk)
            .await
            .expect("record budget inventory"),
    )
    .expect("budget rows");
    let rows = rows.as_array().expect("rows");
    assert_eq!(rows.len(), 65);
    assert_eq!(rows.iter().filter(|row| row["validation"]["stage"] == "malformed").count(), 64);
    let exhausted = rows
        .iter()
        .find(|row| row["validation"]["stage"] == "budget_exhausted")
        .expect("record inspection limit");
    assert!(exhausted["validation"]["readerAccepted"].is_null());
    assert!(exhausted["validation"]["signatureValid"].is_null());
    temp.close().expect("remove record budget fixture");
}
