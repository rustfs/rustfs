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

use super::super::*;

pub(crate) fn validate_catalog_entry_version(kind: &str, version: u16) -> TableCatalogStoreResult<()> {
    if version != TABLE_CATALOG_ENTRY_VERSION {
        return Err(TableCatalogStoreError::Invalid(format!("unsupported {kind} entry version")));
    }
    Ok(())
}

fn validate_table_maintenance_config_version(version: u16) -> TableCatalogStoreResult<()> {
    if version != TABLE_MAINTENANCE_CONFIG_VERSION {
        return Err(TableCatalogStoreError::Invalid(
            "unsupported table maintenance config entry version".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_table_maintenance_config(config: &TableMaintenanceConfig) -> TableCatalogStoreResult<()> {
    validate_table_maintenance_config_version(config.version)?;
    if config.worker_lease_timeout_seconds == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "worker-lease-timeout-seconds must be greater than zero".to_string(),
        ));
    }
    if config.worker_lease_timeout_seconds > TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_MAX_SECONDS {
        return Err(TableCatalogStoreError::Invalid(format!(
            "worker-lease-timeout-seconds cannot exceed {TABLE_MAINTENANCE_WORKER_LEASE_TIMEOUT_MAX_SECONDS}"
        )));
    }
    if config.max_retry_attempts > 10 {
        return Err(TableCatalogStoreError::Invalid("max-retry-attempts cannot exceed 10".to_string()));
    }
    if config.max_retry_attempts > 0 && config.retry_initial_backoff_seconds == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "retry-initial-backoff-seconds must be greater than zero when retry is enabled".to_string(),
        ));
    }
    if config.max_retry_attempts > 0 && config.retry_initial_backoff_seconds > TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS {
        return Err(TableCatalogStoreError::Invalid(format!(
            "retry-initial-backoff-seconds cannot exceed {TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS}"
        )));
    }
    if config.max_retry_attempts > 0 && config.retry_max_backoff_seconds > TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS {
        return Err(TableCatalogStoreError::Invalid(format!(
            "retry-max-backoff-seconds cannot exceed {TABLE_MAINTENANCE_RETRY_BACKOFF_MAX_SECONDS}"
        )));
    }
    if config.max_retry_attempts > 0 && config.retry_max_backoff_seconds < config.retry_initial_backoff_seconds {
        return Err(TableCatalogStoreError::Invalid(
            "retry-max-backoff-seconds must be greater than or equal to retry-initial-backoff-seconds".to_string(),
        ));
    }
    if config.quarantine_enabled && config.quarantine_retention_seconds == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "quarantine-retention-seconds must be greater than zero when quarantine is enabled".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_table_snapshot_expiration_config(config: &TableSnapshotExpirationConfig) -> TableCatalogStoreResult<()> {
    if config.min_snapshots_to_keep == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "min-snapshots-to-keep must be greater than zero".to_string(),
        ));
    }
    if config.max_snapshot_age_ms < 0 {
        return Err(TableCatalogStoreError::Invalid("max-snapshot-age-ms cannot be negative".to_string()));
    }
    Ok(())
}

pub(crate) fn validate_table_compaction_planning_config(config: &TableCompactionPlanningConfig) -> TableCatalogStoreResult<()> {
    if config.target_file_size_bytes == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "target-file-size-bytes must be greater than zero".to_string(),
        ));
    }
    if config.small_file_threshold_bytes == 0 {
        return Err(TableCatalogStoreError::Invalid(
            "small-file-threshold-bytes must be greater than zero".to_string(),
        ));
    }
    if config.small_file_threshold_bytes > config.target_file_size_bytes {
        return Err(TableCatalogStoreError::Invalid(
            "small-file-threshold-bytes cannot exceed target-file-size-bytes".to_string(),
        ));
    }
    if config.min_input_files < 2 {
        return Err(TableCatalogStoreError::Invalid("min-input-files must be at least two".to_string()));
    }
    if config.max_rewrite_bytes_per_job < config.target_file_size_bytes {
        return Err(TableCatalogStoreError::Invalid(
            "max-rewrite-bytes-per-job must be at least target-file-size-bytes".to_string(),
        ));
    }
    Ok(())
}
