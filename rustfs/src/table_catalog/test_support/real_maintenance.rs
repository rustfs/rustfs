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

use crate::table_catalog::{
    ObjectTableCatalogStore, TableCatalogObjectBackend, TableCatalogStoreError, TableCatalogStoreResult,
    TableMetadataMaintenanceReport,
};
use std::time::Duration;
use tokio::sync::oneshot;

/// Exposes the real plan/delete boundary to an integration test without retaining publication locks while paused.
pub(crate) async fn plan_then_delete_after<B: TableCatalogObjectBackend>(
    store: &ObjectTableCatalogStore<B>,
    bucket: &str,
    namespace: &str,
    table: &str,
    retain_recent_metadata_files: usize,
    planned: oneshot::Sender<TableMetadataMaintenanceReport>,
    resume: oneshot::Receiver<()>,
) -> TableCatalogStoreResult<TableMetadataMaintenanceReport> {
    let report = store
        .plan_table_metadata_maintenance(bucket, namespace, table, retain_recent_metadata_files)
        .await?;
    planned
        .send(report.clone())
        .map_err(|_| TableCatalogStoreError::Internal("maintenance test plan receiver closed".to_string()))?;
    tokio::time::timeout(Duration::from_secs(30), resume)
        .await
        .map_err(|_| TableCatalogStoreError::Internal("maintenance test resume timed out".to_string()))?
        .map_err(|_| TableCatalogStoreError::Internal("maintenance test resume sender closed".to_string()))?;
    store
        .delete_table_metadata_maintenance_report(bucket, namespace, table, report)
        .await
}
