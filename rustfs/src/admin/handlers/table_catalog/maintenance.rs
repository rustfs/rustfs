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
use crate::admin::operation::{AdminRequest, AdminResponse, Operation};
use rustfs_s3_types::s3_error;

pub struct RestTableMetadataMaintenanceHandler {}

#[async_trait::async_trait]
impl Operation for RestTableMetadataMaintenanceHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RunTableMaintenanceAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let request = read_json_body::<TableMetadataMaintenanceRequest>(req.take_body()).await?;
        let metadata_backend = table_catalog_backend_from_envelope(req.envelope())?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let response =
            configured_table_metadata_maintenance_response(&store, &metadata_backend, &warehouse, &namespace, &table, request)
                .await?;
        build_json_response(StatusCode::OK, &response)
    }
}

async fn configured_table_metadata_maintenance_response<B>(
    store: &crate::table_catalog::ConfiguredTableCatalogStore<B>,
    metadata_backend: &B,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: TableMetadataMaintenanceRequest,
) -> S3Result<crate::table_catalog::TableMetadataMaintenanceReport>
where
    B: crate::table_catalog::TableCatalogObjectBackend + Clone,
{
    match store {
        crate::table_catalog::ConfiguredTableCatalogStore::ObjectBacked(store) => {
            table_metadata_maintenance_response(store, metadata_backend, bucket, namespace, table, request).await
        }
        crate::table_catalog::ConfiguredTableCatalogStore::DurableStrong(store) => {
            if request.delete || request.commit_snapshot_expiration || request.commit_compaction {
                return Err(s3_error!(
                    InvalidRequest,
                    "metadata maintenance deletion and snapshot expiration or compaction commits are not supported with durable-strong table catalog backing"
                ));
            }
            store
                .run_table_metadata_maintenance_planning(
                    bucket,
                    &namespace.public_name(),
                    table,
                    request.retain_recent_metadata_files,
                    request.snapshot_expiration,
                    request.compaction,
                )
                .await
                .map_err(catalog_store_error)
        }
    }
}

pub struct GetTableMaintenanceConfigHandler {}

#[async_trait::async_trait]
impl Operation for GetTableMaintenanceConfigHandler {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableLifecycleAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let response = store
            .get_table_maintenance_config(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct PutTableMaintenanceConfigHandler {}

#[async_trait::async_trait]
impl Operation for PutTableMaintenanceConfigHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableLifecycleAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let request = read_json_body::<crate::table_catalog::TableMaintenanceConfig>(req.take_body()).await?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let response = store
            .put_table_maintenance_config(&warehouse, &namespace.public_name(), &table, request)
            .await
            .map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct GetTableMaintenanceJobHandler {}

#[async_trait::async_trait]
impl Operation for GetTableMaintenanceJobHandler {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let job = job_id_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableLifecycleAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let Some(response) = store
            .get_table_metadata_maintenance_report(&warehouse, &namespace.public_name(), &table, &job)
            .await
            .map_err(catalog_store_error)?
        else {
            return Err(s3_error!(InvalidRequest, "maintenance job not found"));
        };
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct GetTableMaintenanceSchedulerHandler {}

#[async_trait::async_trait]
impl Operation for GetTableMaintenanceSchedulerHandler {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableLifecycleAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let response = store
            .get_table_maintenance_scheduler_report(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RunTableMaintenanceWorkerHandler {}

#[async_trait::async_trait]
impl Operation for RunTableMaintenanceSchedulerHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RunTableMaintenanceAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let request = read_json_body_or_default::<TableMaintenanceSchedulerRunRequest>(req.take_body()).await?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let response = store
            .run_table_maintenance_scheduler_once(
                &warehouse,
                &namespace.public_name(),
                &table,
                request.scheduler_id().to_string(),
            )
            .await
            .map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RunTableMaintenanceSchedulerHandler {}

#[async_trait::async_trait]
impl Operation for RunTableMaintenanceWorkerHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RunTableMaintenanceAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let request = read_json_body::<TableMaintenanceWorkerRunRequest>(req.take_body()).await?;
        let condition = request.condition()?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let response = match condition {
            Some(condition) => {
                store
                    .run_table_metadata_maintenance_worker_once_conditionally(
                        &warehouse,
                        &namespace.public_name(),
                        &table,
                        request.worker_id().to_string(),
                        &condition,
                    )
                    .await
            }
            None => {
                store
                    .run_table_metadata_maintenance_worker_once(
                        &warehouse,
                        &namespace.public_name(),
                        &table,
                        request.worker_id().to_string(),
                    )
                    .await
            }
        }
        .map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct HeartbeatTableMaintenanceJobHandler {}

#[async_trait::async_trait]
impl Operation for HeartbeatTableMaintenanceJobHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let job = job_id_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RunTableMaintenanceAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let request = read_json_body::<TableMaintenanceHeartbeatRequest>(req.take_body()).await?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let response = store
            .heartbeat_table_metadata_maintenance_job(
                &warehouse,
                &namespace.public_name(),
                &table,
                &job,
                &request.lease_id,
                &request.worker_id,
            )
            .await
            .map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct TableMaintenanceQuarantineHandler {}

#[async_trait::async_trait]
impl Operation for TableMaintenanceQuarantineHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let job = job_id_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RunTableMaintenanceAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let request = read_json_body::<crate::table_catalog::TableMaintenanceQuarantineOperationRequest>(req.take_body()).await?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let response = store
            .apply_table_maintenance_quarantine_operation(&warehouse, &namespace.public_name(), &table, &job, request)
            .await
            .map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}
