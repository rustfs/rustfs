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

use crate::table_catalog::*;

mod compaction;
mod object;
mod planner;
mod recovery;

pub(crate) use planner::*;
pub(crate) use recovery::*;

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
