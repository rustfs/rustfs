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

use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogBackupStatus {
    Created,
    AlreadyPresent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogBackupReport {
    pub table_bucket: String,
    pub backup_id: String,
    pub status: TableCatalogBackupStatus,
    pub backup_path: String,
    pub source_snapshot_etag: String,
    pub source_snapshot_version: u16,
    pub catalog_fingerprint: String,
    pub object_count: usize,
    pub verified_object_count: usize,
    pub verified_bytes: u64,
    pub maintenance_object_count: usize,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TableCatalogRestoreStatus {
    Restored,
    AlreadyRestored,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TableCatalogRestoreReport {
    pub table_bucket: String,
    pub backup_id: String,
    pub status: TableCatalogRestoreStatus,
    pub previous_snapshot_etag: Option<String>,
    pub restored_snapshot_etag: String,
    pub object_count: usize,
    pub maintenance_object_count: usize,
}
