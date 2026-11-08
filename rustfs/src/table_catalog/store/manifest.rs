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

pub(super) fn table_catalog_backing_manifest(
    paths: &TableCatalogObjectPaths,
    namespace: &Namespace,
    table: &IdentifierSegment,
    entry: &TableEntry,
    commit_recovery: &TableCommitRecoveryReport,
) -> TableCatalogBackingManifest {
    let recovery_required = commit_recovery.staged_before_table_update_count > 0
        || commit_recovery.finalization_required_count > 0
        || commit_recovery.idempotency_repair_required_count > 0;
    let manual_review_required = commit_recovery.manual_review_count > 0;
    let wal_status = if manual_review_required {
        TableCatalogWalStatus::ManualReviewRequired
    } else if recovery_required {
        TableCatalogWalStatus::RecoveryRequired
    } else {
        TableCatalogWalStatus::Recoverable
    };
    TableCatalogBackingManifest {
        version: TABLE_CATALOG_BACKING_MANIFEST_VERSION,
        current: TableCatalogBackingProfile {
            kind: TableCatalogBackingKind::ObjectBacked,
            authority: TableCatalogAuthority::RustfsSysObject,
            consistency: TableCatalogConsistencyMode::ConditionalObjectCas,
            durability: TableCatalogDurabilityMode::StagedCommitLogBeforePointerUpdate,
            current_pointer_path: paths.table_entry_path(&entry.table_bucket, namespace, table),
            wal: TableCatalogWalState {
                status: wal_status,
                commit_log_prefix: paths.commit_log_entries_prefix(&entry.table_bucket, &entry.table_id),
                idempotency_index_prefix: paths.commit_idempotency_entries_prefix(&entry.table_bucket, &entry.table_id),
                committed_generation: entry.generation,
                staged_before_table_update_count: commit_recovery.staged_before_table_update_count,
                finalization_required_count: commit_recovery.finalization_required_count,
                idempotency_repair_required_count: commit_recovery.idempotency_repair_required_count,
                manual_review_count: commit_recovery.manual_review_count,
            },
            snapshot: TableCatalogSnapshotState {
                export_api: "GET /iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/catalog/export".to_string(),
                includes_table_bucket: true,
                includes_namespace: true,
                includes_table_pointer: true,
                includes_backing_manifest: true,
                snapshot_etag: None,
                snapshot_version: None,
            },
        },
        ha: TableCatalogHaPolicy {
            writer_region_model: TableCatalogHaWriterModel::SingleActiveWriterRegion,
            read_replica_strategy: TableCatalogReadReplicaStrategy::ReadOnlyReplicasForListAndLoad,
            commit_read_requirement: TableCatalogCommitReadRequirement::LinearizableLeaderRead,
            active_active_supported: false,
            failover_requires_operator_promotion: true,
        },
        scale_validation: TableCatalogScaleValidation {
            status: TableCatalogScaleValidationStatus::MatrixPublished,
            benchmark_required: true,
            required_scenarios: vec![
                TableCatalogScaleValidationScenario::ConcurrentCommitCas,
                TableCatalogScaleValidationScenario::CommitLogRecoveryReplay,
                TableCatalogScaleValidationScenario::ReadReplicaStaleReadGuard,
                TableCatalogScaleValidationScenario::ClientConformanceMatrix,
            ],
        },
    }
}
