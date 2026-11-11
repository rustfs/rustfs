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

use super::super::object::validate_table_maintenance_report_owner;
use super::*;

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct ObjectCatalogBackupMaintenanceSummary {
    pub(super) object_count: usize,
}

#[derive(Default)]
struct BackupMaintenanceOwner {
    named: BTreeMap<String, TableMetadataMaintenanceReport>,
    latest: Option<TableMetadataMaintenanceReport>,
    current: Option<TableMetadataMaintenanceReport>,
}

impl<B> ObjectTableCatalogStore<B>
where
    B: TableCatalogObjectBackend,
{
    /// Counts recognized sidecars without granting authority to a candidate.
    pub(super) fn backup_maintenance_object_count(&self, table_bucket: &str, objects: &[ObjectCatalogBackupObject]) -> usize {
        let config = self.paths.table_bucket_maintenance_config_path(table_bucket);
        objects
            .iter()
            .filter(|object| {
                object.bucket == self.catalog_bucket()
                    && (object.object == config
                        || self
                            .paths
                            .historical_table_maintenance_path(table_bucket, &object.object)
                            .is_some())
            })
            .count()
    }

    /// Validates maintenance authority in an already captured catalog snapshot.
    ///
    /// Callers hold the backup/publication fences and validate the artifact's
    /// ordinary entry, size, hash and metadata contracts separately. This pure
    /// check never expires a lease or repairs aliases. It preserves all source
    /// bytes, including audit, quarantine and historical worker identities.
    pub(super) fn validate_backup_maintenance_authority(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
    ) -> TableCatalogStoreResult<ObjectCatalogBackupMaintenanceSummary> {
        let mut owners = BTreeSet::new();
        let mut seen_paths = BTreeSet::new();
        for object in objects.iter().filter(|object| object.bucket == self.catalog_bucket()) {
            if !seen_paths.insert(object.object.as_str()) {
                return Err(TableCatalogStoreError::Invalid("catalog backup has a duplicate object path".to_string()));
            }
            if !object.object.ends_with(TABLE_ENTRY_FILE) {
                continue;
            }
            let entry: TableEntry = serde_json::from_slice(&object.data)
                .map_err(|_| TableCatalogStoreError::Invalid("invalid maintenance catalog owner in backup".to_string()))?;
            validate_table_entry_object(&self.paths, &object.object, &entry)?;
            if entry.table_bucket != table_bucket {
                return Err(TableCatalogStoreError::Invalid(
                    "backup maintenance owner belongs to another bucket".to_string(),
                ));
            }
            if matches!(entry.state, TableCatalogEntryState::Renaming | TableCatalogEntryState::Deleting) {
                return Err(backup_maintenance_recovery_required("catalog owner has a pending table operation"));
            }
            // read_table_with_etag_unlocked does not filter Deleted entries.
            // Retained owners must therefore also fence their current lease.
            let namespace = parse_namespace_for_store(&entry.namespace)?;
            let table = parse_table_for_store(&entry.table)?;
            let config = self
                .paths
                .table_maintenance_config_path(table_bucket, &namespace, &table, &entry.table_id);
            owners.insert(config.trim_end_matches(MAINTENANCE_CONFIG_FILE).to_string());
        }

        let mut summary = ObjectCatalogBackupMaintenanceSummary::default();
        let mut reports = BTreeMap::<String, BackupMaintenanceOwner>::new();
        let bucket_config = self.paths.table_bucket_maintenance_config_path(table_bucket);
        for object in objects.iter().filter(|object| object.bucket == self.catalog_bucket()) {
            if object.object == bucket_config {
                validate_backup_maintenance_config(&object.data)?;
                summary.object_count += 1;
                continue;
            }
            if object.object.ends_with(TABLE_ENTRY_FILE)
                || object.object.ends_with(NAMESPACE_ENTRY_FILE)
                || object.object.ends_with(VIEW_ENTRY_FILE)
            {
                continue;
            }
            let owned_prefix = owners
                .range(..=object.object.clone())
                .next_back()
                .filter(|prefix| object.object.starts_with(prefix.as_str()));
            let path = match owned_prefix {
                Some(prefix) => Some((prefix.as_str(), &object.object[prefix.len()..])),
                None => self.paths.historical_table_maintenance_path(table_bucket, &object.object),
            };
            let Some((prefix, suffix)) = path else {
                continue;
            };
            summary.object_count += 1;
            if suffix == MAINTENANCE_CONFIG_FILE {
                validate_backup_maintenance_config(&object.data)?;
                continue;
            }
            let report: TableMetadataMaintenanceReport = serde_json::from_slice(&object.data)
                .map_err(|_| TableCatalogStoreError::Invalid("invalid maintenance report in backup".to_string()))?;
            if report.job.table_id.is_empty() {
                return Err(TableCatalogStoreError::Invalid(
                    "backup maintenance report has no table identity".to_string(),
                ));
            }
            let namespace = parse_namespace_for_store(&report.job.namespace)?;
            let table = parse_table_for_store(&report.job.table)?;
            validate_table_maintenance_report_owner(&report, table_bucket, &namespace, &table, &report.job.table_id)?;
            let owner = reports.entry(prefix.to_string()).or_default();
            let expected = match suffix {
                MAINTENANCE_CURRENT_JOB_FILE => {
                    owner.current = Some(report.clone());
                    self.paths
                        .table_maintenance_current_job_path(table_bucket, &namespace, &table, &report.job.table_id)
                }
                MAINTENANCE_LATEST_JOB_FILE => {
                    owner.latest = Some(report.clone());
                    self.paths
                        .table_maintenance_latest_job_path(table_bucket, &namespace, &table, &report.job.table_id)
                }
                _ if suffix.starts_with(&format!("{MAINTENANCE_JOB_ROOT}/")) => {
                    owner.named.insert(report.job.job_id.clone(), report.clone());
                    self.paths.table_maintenance_job_path(
                        table_bucket,
                        &namespace,
                        &table,
                        &report.job.table_id,
                        &report.job.job_id,
                    )
                }
                _ => {
                    return Err(TableCatalogStoreError::Invalid(
                        "unexpected maintenance object path in backup".to_string(),
                    ));
                }
            };
            if object.object != expected {
                return Err(TableCatalogStoreError::Invalid(
                    "backup maintenance report does not match its object path".to_string(),
                ));
            }
        }

        for (prefix, owner) in reports {
            if !owners.contains(&prefix) {
                // No restored catalog owner can discover this incarnation.
                // Keep its historical bytes without granting execution authority.
                continue;
            }
            let current = owner
                .current
                .as_ref()
                .ok_or_else(|| backup_maintenance_recovery_required("maintenance reports have no current authority"))?;
            let latest = owner
                .latest
                .as_ref()
                .ok_or_else(|| backup_maintenance_recovery_required("current maintenance authority has no latest report"))?;
            let named = owner
                .named
                .get(&current.job.job_id)
                .ok_or_else(|| backup_maintenance_recovery_required("current maintenance authority has no named report"))?;
            if current != latest || current != named {
                return Err(backup_maintenance_recovery_required(
                    "named, latest and current maintenance reports are not consistently published",
                ));
            }
            if matches!(
                current.job.status,
                TableMetadataMaintenanceJobStatus::Queued | TableMetadataMaintenanceJobStatus::Running
            ) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "maintenance recovery required before backup or restore: {}/{}/{} job {} attempt {} is {:?}; finish or recover the current job and capture a new stable backup",
                    current.job.table_bucket,
                    current.job.namespace,
                    current.job.table,
                    current.job.job_id,
                    current.job.attempt,
                    current.job.status,
                )));
            }
            // Only aliases grant authority. A historical named Running job with
            // another id cannot heartbeat or reclaim this stable current job.
        }
        Ok(summary)
    }
}

fn backup_maintenance_recovery_required(reason: &str) -> TableCatalogStoreError {
    TableCatalogStoreError::Conflict(format!(
        "maintenance recovery required before backup or restore: {reason}; inspect the named, latest and current reports, recover their existing authority, then capture a stable backup"
    ))
}

fn validate_backup_maintenance_config(data: &[u8]) -> TableCatalogStoreResult<()> {
    let config: TableMaintenanceConfig = serde_json::from_slice(data)
        .map_err(|_| TableCatalogStoreError::Invalid("invalid maintenance config in backup".to_string()))?;
    validate_table_maintenance_config(&config)
}

#[cfg(test)]
#[path = "object_backup_maintenance_tests.rs"]
mod tests;
