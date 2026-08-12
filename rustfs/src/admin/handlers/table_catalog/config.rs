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

pub struct GetCatalogConfigHandler {}

#[async_trait::async_trait]
impl Operation for GetCatalogConfigHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_table_catalog_request(&req, AdminAction::GetTableCatalogAction).await?;
        let warehouse = warehouse_from_config_query(&req.uri)?;
        build_json_response(StatusCode::OK, &catalog_config_response(warehouse.as_deref())?)
    }
}

pub struct EnableTableBucketHandler {}

#[async_trait::async_trait]
impl Operation for EnableTableBucketHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableBucketAction).await?;
        let backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let object_store = runtime_sources::object_store_from_req(&req)
            .ok_or_else(|| table_catalog_internal_error("request object store is not initialized"))?;
        let store = table_catalog_store_from_backend(backend.clone())?;
        let publication = TableCommitObjectBackend::preauthorized(backend);
        let response = enable_table_bucket_response(&store, &publication, object_store.as_ref(), &warehouse).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct GetTableBucketHandler {}

#[async_trait::async_trait]
impl Operation for GetTableBucketHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableBucketAction).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
        let enabled = table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let response = table_bucket_response(&store, &warehouse, enabled).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct GetTableCatalogMigrationHandler {}

#[async_trait::async_trait]
impl Operation for GetTableCatalogMigrationHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableCatalogAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_object_store_from_extensions(&req.extensions)?;
        let started = Instant::now();
        let result = store
            .plan_durable_strong_backing_migration(&warehouse)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("migration", &warehouse, "", "", started, &result);
        let response = result?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct MaterializeTableCatalogMigrationHandler {}

#[async_trait::async_trait]
impl Operation for MaterializeTableCatalogMigrationHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        authorize_table_catalog_request(&req, AdminAction::MigrateTableCatalogAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_object_store_from_extensions(&req.extensions)?;
        let started = Instant::now();
        let result = store
            .materialize_durable_strong_backing_migration(&warehouse)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("migration-materialize", &warehouse, "", "", started, &result);
        let response = result?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct CancelTableCatalogMigrationHandler {}

#[async_trait::async_trait]
impl Operation for CancelTableCatalogMigrationHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        authorize_table_catalog_request(&req, AdminAction::MigrateTableCatalogAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_object_store_from_extensions(&req.extensions)?;
        let started = Instant::now();
        let result = store
            .cancel_durable_strong_backing_migration(&warehouse)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("migration-cancel", &warehouse, "", "", started, &result);
        let response = result?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct ExternalCatalogBridgeHandler {}

#[async_trait::async_trait]
impl Operation for ExternalCatalogBridgeHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_object_store_from_extensions(&req.extensions)?;
        let response = external_catalog_bridge_response(&store, &warehouse, &namespace, &table).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct PutExternalCatalogBridgeHandler {}

#[async_trait::async_trait]
impl Operation for PutExternalCatalogBridgeHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let request = read_json_body::<ExternalCatalogBridgeRequest>(req.input).await?;
        let store = table_catalog_object_store_from_extensions(&req.extensions)?;
        let response = put_external_catalog_bridge_response(&store, &warehouse, &namespace, &table, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct SyncExternalCatalogBridgeHandler {}

#[async_trait::async_trait]
impl Operation for SyncExternalCatalogBridgeHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        let principal =
            authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableMetadataLocationAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let metadata_backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let store = table_catalog_object_store_from_extensions(&req.extensions)?;
        if store
            .load_table(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error)?
            .is_none()
        {
            authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
        }
        install_table_catalog_s3_request_info(&mut req, &principal)?;
        let request = read_json_body::<ExternalCatalogBridgeSyncRequest>(std::mem::take(&mut req.input)).await?;
        let table_bucket_enabled = table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let commit_backend = TableCommitObjectBackend::for_request(metadata_backend, req);
        let result = sync_external_catalog_bridge_response(
            &store,
            &commit_backend,
            &warehouse,
            &namespace,
            &table,
            request,
            table_bucket_enabled,
        )
        .await;
        let response = commit_backend.finish(result).await?;
        build_json_response(StatusCode::OK, &response)
    }
}
