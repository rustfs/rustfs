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
    (ObjectTableCatalogStore::new(TestCatalogObjectBackend::default()), backup.objects, entry)
}

fn report(entry: &TableEntry, job_id: &str, status: TableMetadataMaintenanceJobStatus) -> TableMetadataMaintenanceReport {
    serde_json::from_value(serde_json::json!({
        "job": {
            "job_id": job_id,
            "table_bucket": entry.table_bucket,
            "namespace": entry.namespace,
            "table": entry.table,
            "table_id": entry.table_id,
            "status": status,
            "worker_id": "retained-worker",
            "lease_id": "retained-lease",
            "attempt": 1,
            "heartbeat_at": "2000-01-01T00:00:00Z",
            "started_at": "2000-01-01T00:00:00Z",
            "finished_at": "2000-01-01T00:01:00Z",
            "current_metadata_location": entry.metadata_location,
            "current_generation": entry.generation,
            "retain_recent_metadata_files": 2,
            "safety_window_seconds": 900,
            "cleanup_watermark_unix_seconds": 1,
            "quarantine-enabled": true,
            "quarantine-retention-seconds": 86400,
            "quarantined_object_count": 1
        },
        "current_metadata_location": entry.metadata_location,
        "retained_metadata_locations": [entry.metadata_location],
        "cleanup_candidate_locations": [],
        "deletable_metadata_locations": []
    }))
    .expect("valid maintenance report")
}

fn object(path: String, data: Vec<u8>) -> ObjectCatalogBackupObject {
    ObjectCatalogBackupObject {
        bucket: RUSTFS_META_BUCKET.to_string(),
        object: path,
        size_bytes: u64::try_from(data.len()).unwrap(),
        etag: Some("retained-etag".to_string()),
        sha256: backup_sha256(&data),
        data,
    }
}

fn report_paths(entry: &TableEntry, job_id: &str) -> [String; 3] {
    let paths = TableCatalogObjectPaths::default();
    let namespace = Namespace::parse(&entry.namespace).unwrap();
    let table = IdentifierSegment::parse(&entry.table).unwrap();
    [
        paths.table_maintenance_job_path(&entry.table_bucket, &namespace, &table, &entry.table_id, job_id),
        paths.table_maintenance_latest_job_path(&entry.table_bucket, &namespace, &table, &entry.table_id),
        paths.table_maintenance_current_job_path(&entry.table_bucket, &namespace, &table, &entry.table_id),
    ]
}

fn publish(objects: &mut Vec<ObjectCatalogBackupObject>, entry: &TableEntry, report: &TableMetadataMaintenanceReport) {
    for path in report_paths(entry, &report.job.job_id) {
        objects.retain(|existing| existing.object != path);
        objects.push(object(path, serde_json::to_vec(report).unwrap()));
    }
}

fn assert_recovery_required(result: TableCatalogStoreResult<ObjectCatalogBackupMaintenanceSummary>) {
    let error = result.expect_err("unstable maintenance must fail closed");
    assert!(
        matches!(&error, TableCatalogStoreError::Conflict(message) if message.contains("maintenance recovery required")),
        "{error}"
    );
}

#[test]
fn backup_without_maintenance_keeps_v1_objects_unchanged() {
    let (store, objects, _) = fixture();
    let before = objects.clone();
    assert_eq!(
        store
            .validate_backup_maintenance_authority("analytics", &objects)
            .unwrap()
            .object_count,
        0
    );
    assert_eq!(objects, before);
}

#[test]
fn backup_rejects_current_queued_and_running_even_after_their_timestamps_expired() {
    for status in [
        TableMetadataMaintenanceJobStatus::Queued,
        TableMetadataMaintenanceJobStatus::Running,
    ] {
        let (store, mut objects, entry) = fixture();
        let mut active = report(&entry, "active-job", status);
        active.job.finished_at = None;
        publish(&mut objects, &entry, &active);
        let before = objects.clone();
        assert_recovery_required(store.validate_backup_maintenance_authority("analytics", &objects));
        assert_eq!(objects, before, "lease, worker, attempt or aliases were silently rewritten");
    }
}

#[test]
fn stable_authority_keeps_historical_running_jobs_configs_and_quarantine_bytes() {
    let (store, mut objects, entry) = fixture();
    let stable = report(&entry, "new-job", TableMetadataMaintenanceJobStatus::Successful);
    publish(&mut objects, &entry, &stable);
    let historical = report(&entry, "old-job", TableMetadataMaintenanceJobStatus::Running);
    objects.push(object(
        report_paths(&entry, &historical.job.job_id)[0].clone(),
        serde_json::to_vec(&historical).unwrap(),
    ));
    let paths = TableCatalogObjectPaths::default();
    let namespace = Namespace::parse(&entry.namespace).unwrap();
    let table = IdentifierSegment::parse(&entry.table).unwrap();
    let mut config = TableMaintenanceConfig::default();
    config.quarantine_enabled = true;
    config.quarantine_retention_seconds = 86400;
    for path in [
        paths.table_bucket_maintenance_config_path("analytics"),
        paths.table_maintenance_config_path("analytics", &namespace, &table, &entry.table_id),
    ] {
        objects.push(object(path, serde_json::to_vec(&config).unwrap()));
    }
    // Additive audit/quarantine fields remain raw bytes, including across aliases.
    for path in report_paths(&entry, &stable.job.job_id) {
        let saved = objects.iter_mut().find(|object| object.object == path).unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&saved.data).unwrap();
        value["future-quarantine-audit"] = serde_json::json!({"original-worker": "retained-worker", "receipt": [1, 2, 3]});
        *saved = object(path, serde_json::to_vec(&value).unwrap());
    }
    let before = objects.clone();
    assert_eq!(
        store
            .validate_backup_maintenance_authority("analytics", &objects)
            .unwrap()
            .object_count,
        6
    );
    assert_eq!(objects, before);
}

#[test]
fn backup_requires_every_current_report_view() {
    for missing in 0..3 {
        let (store, mut objects, entry) = fixture();
        let stable = report(&entry, "stable-job", TableMetadataMaintenanceJobStatus::Successful);
        publish(&mut objects, &entry, &stable);
        let missing = report_paths(&entry, &stable.job.job_id)[missing].clone();
        objects.retain(|object| object.object != missing);
        assert_recovery_required(store.validate_backup_maintenance_authority("analytics", &objects));
    }
}

#[test]
fn backup_rejects_different_attempt_or_lease_even_with_the_same_job_id() {
    for changed_field in ["attempt", "lease"] {
        let (store, mut objects, entry) = fixture();
        let stable = report(&entry, "stable-job", TableMetadataMaintenanceJobStatus::Successful);
        publish(&mut objects, &entry, &stable);
        let mut changed = stable.clone();
        if changed_field == "attempt" {
            changed.job.attempt += 1;
        } else {
            changed.job.lease_id = "another-lease".to_string();
        }
        let latest = report_paths(&entry, &stable.job.job_id)[1].clone();
        *objects.iter_mut().find(|object| object.object == latest).unwrap() =
            object(latest, serde_json::to_vec(&changed).unwrap());
        assert_recovery_required(store.validate_backup_maintenance_authority("analytics", &objects));
    }
}

#[test]
fn backup_does_not_repair_partially_published_terminal_receipts() {
    let (store, mut objects, entry) = fixture();
    let running = report(&entry, "same-attempt", TableMetadataMaintenanceJobStatus::Running);
    publish(&mut objects, &entry, &running);
    let terminal = report(&entry, "same-attempt", TableMetadataMaintenanceJobStatus::Successful);
    let named = report_paths(&entry, &terminal.job.job_id)[0].clone();
    *objects.iter_mut().find(|object| object.object == named).unwrap() = object(named, serde_json::to_vec(&terminal).unwrap());
    let before = objects.clone();
    assert_recovery_required(store.validate_backup_maintenance_authority("analytics", &objects));
    assert_eq!(objects, before);
}

#[test]
fn backup_validates_report_owner_and_named_job_path() {
    for changed in ["bucket", "owner", "job-path"] {
        let (store, mut objects, entry) = fixture();
        let stable = report(&entry, "stable-job", TableMetadataMaintenanceJobStatus::Successful);
        publish(&mut objects, &entry, &stable);
        let named = report_paths(&entry, &stable.job.job_id)[0].clone();
        let saved = objects.iter_mut().find(|object| object.object == named).unwrap();
        let mut changed_report = stable.clone();
        match changed {
            "bucket" => changed_report.job.table_bucket = "another-bucket".to_string(),
            "owner" => changed_report.job.table_id = "another-table-id".to_string(),
            "job-path" => changed_report.job.job_id = "another-job".to_string(),
            _ => unreachable!(),
        }
        *saved = object(named, serde_json::to_vec(&changed_report).unwrap());
        assert!(matches!(
            store.validate_backup_maintenance_authority("analytics", &objects),
            Err(TableCatalogStoreError::Invalid(_))
        ));
    }
}

#[test]
fn backup_rejects_an_unknown_sidecar_under_the_current_owner() {
    let (store, mut objects, entry) = fixture();
    let config = TableCatalogObjectPaths::default().table_maintenance_config_path(
        "analytics",
        &Namespace::parse(&entry.namespace).unwrap(),
        &IdentifierSegment::parse(&entry.table).unwrap(),
        &entry.table_id,
    );
    let path = format!("{}unknown.json", config.trim_end_matches(MAINTENANCE_CONFIG_FILE));
    objects.push(object(
        path,
        serde_json::to_vec(&report(&entry, "job", TableMetadataMaintenanceJobStatus::Successful)).unwrap(),
    ));
    assert!(matches!(
        store.validate_backup_maintenance_authority("analytics", &objects),
        Err(TableCatalogStoreError::Invalid(_))
    ));
}

#[test]
fn superseded_incarnation_sidecars_do_not_gain_authority_on_restore() {
    let (store, mut objects, mut old_entry) = fixture();
    old_entry.table_id = "superseded-incarnation".to_string();
    let old = report(&old_entry, "old-running", TableMetadataMaintenanceJobStatus::Running);
    publish(&mut objects, &old_entry, &old);
    let before = objects.clone();
    assert_eq!(
        store
            .validate_backup_maintenance_authority("analytics", &objects)
            .unwrap()
            .object_count,
        3
    );
    assert_eq!(objects, before);
}

#[test]
fn isolated_running_history_for_a_retained_owner_requires_explicit_recovery() {
    let (store, mut objects, entry) = fixture();
    let running = report(&entry, "unresolved-job", TableMetadataMaintenanceJobStatus::Running);
    objects.push(object(
        report_paths(&entry, &running.job.job_id)[0].clone(),
        serde_json::to_vec(&running).unwrap(),
    ));
    assert_recovery_required(store.validate_backup_maintenance_authority("analytics", &objects));
}

#[test]
fn a_deleted_catalog_owner_still_cannot_restore_a_discoverable_running_lease() {
    let (store, mut objects, mut entry) = fixture();
    entry.state = TableCatalogEntryState::Deleted;
    let path = objects
        .iter()
        .find(|object| object.object.ends_with(TABLE_ENTRY_FILE))
        .unwrap()
        .object
        .clone();
    *objects.iter_mut().find(|object| object.object == path).unwrap() = object(path, serde_json::to_vec(&entry).unwrap());
    publish(
        &mut objects,
        &entry,
        &report(&entry, "retained-running", TableMetadataMaintenanceJobStatus::Running),
    );
    assert_recovery_required(store.validate_backup_maintenance_authority("analytics", &objects));
}

#[test]
fn hierarchical_namespaces_and_a_table_named_maintenance_keep_canonical_paths() {
    let (store, mut objects, mut entry) = fixture();
    objects.retain(|object| !object.object.ends_with(TABLE_ENTRY_FILE));
    entry.namespace = "sales.history".to_string();
    entry.table = "maintenance".to_string();
    let path = TableCatalogObjectPaths::default().table_entry_path(
        "analytics",
        &Namespace::parse(&entry.namespace).unwrap(),
        &IdentifierSegment::parse(&entry.table).unwrap(),
    );
    objects.push(object(path, serde_json::to_vec(&entry).unwrap()));
    publish(
        &mut objects,
        &entry,
        &report(&entry, "stable", TableMetadataMaintenanceJobStatus::Successful),
    );
    assert_eq!(
        store
            .validate_backup_maintenance_authority("analytics", &objects)
            .unwrap()
            .object_count,
        3
    );
}
