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

pub struct RestListTablesHandler {}

#[async_trait::async_trait]
impl Operation for RestListTablesHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let response = list_tables_response(&store, &warehouse, &namespace, &req.uri).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestCreateTableHandler {}

#[async_trait::async_trait]
impl Operation for RestCreateTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CreateTableAction).await?;
        let request = read_json_body::<CreateTableRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let table_bucket_enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        let response =
            create_table_response(&store, &metadata_backend, &warehouse, &namespace, request, table_bucket_enabled).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestRegisterTableHandler {}

#[async_trait::async_trait]
impl Operation for RestRegisterTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
        let request = read_json_body::<RegisterTableRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let table_bucket_enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        let response =
            register_table_response(&store, &metadata_backend, &warehouse, &namespace, request, table_bucket_enabled).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestLoadTableHandler {}

#[async_trait::async_trait]
impl Operation for RestLoadTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response = load_table_response(&store, &metadata_backend, &warehouse, &namespace, &table).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestTableExistsHandler {}

#[async_trait::async_trait]
impl Operation for RestTableExistsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        Ok(empty_response(table_exists_status(&store, &warehouse, &namespace, &table).await?))
    }
}

pub struct RestCommitTableHandler {}

#[async_trait::async_trait]
impl Operation for RestCommitTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body::<RestCommitTableRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response = commit_table_response(&store, &metadata_backend, &warehouse, &namespace, &table, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestDropTableHandler {}

#[async_trait::async_trait]
impl Operation for RestDropTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::DeleteTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        drop_table_in_store(&store, &warehouse, &namespace, &table).await?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}

pub struct GetTableMetadataLocationHandler {}

#[async_trait::async_trait]
impl Operation for GetTableMetadataLocationHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataLocationAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let response = get_table_metadata_location_response(&store, &warehouse, &namespace, &table).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct UpdateTableMetadataLocationHandler {}

#[async_trait::async_trait]
impl Operation for UpdateTableMetadataLocationHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableMetadataLocationAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body::<UpdateTableMetadataLocationRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response =
            update_table_metadata_location_response(&store, &metadata_backend, &warehouse, &namespace, &table, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct ExportTableCatalogHandler {}

#[async_trait::async_trait]
impl Operation for ExportTableCatalogHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let started = Instant::now();
        let result = store
            .export_table_catalog_entry(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("export", &warehouse, &namespace.public_name(), &table, started, &result);
        let response = result?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct ImportTableCatalogHandler {}

#[async_trait::async_trait]
impl Operation for ImportTableCatalogHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
        let request = read_json_body::<CatalogImportRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let table_bucket_enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        let response =
            catalog_import_response(&store, &metadata_backend, &warehouse, &namespace, &table, request, table_bucket_enabled)
                .await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct GetTableCatalogDiagnosticsHandler {}

#[async_trait::async_trait]
impl Operation for GetTableCatalogDiagnosticsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let config = store
            .get_table_maintenance_config(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error)?;
        let started = Instant::now();
        let result = store
            .diagnose_table_catalog(&warehouse, &namespace.public_name(), &table, config.retain_recent_metadata_files)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result(
            "diagnostics",
            &warehouse,
            &namespace.public_name(),
            &table,
            started,
            &result,
        );
        let response = result?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RecoverTableCatalogHandler {}

#[async_trait::async_trait]
impl Operation for RecoverTableCatalogHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let store = table_catalog_store()?;
        let started = Instant::now();
        let result = store
            .recover_table_commits(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("recovery", &warehouse, &namespace.public_name(), &table, started, &result);
        let response = result?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RollbackTableCatalogHandler {}

#[async_trait::async_trait]
impl Operation for RollbackTableCatalogHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let request = read_json_body::<RollbackTableRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response = rollback_table_response(&store, &metadata_backend, &warehouse, &namespace, &table, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}
