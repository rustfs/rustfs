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
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
        let response = list_tables_response(&store, &warehouse, &namespace, &req.uri).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestRenameTableHandler {}

#[async_trait::async_trait]
impl Operation for RestRenameTableHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let principal = table_catalog_request_principal(&req).await?;
        let request = read_bounded_json_body::<RenameTableRequest>(
            &req.headers,
            std::mem::take(&mut req.input),
            RENAME_TABLE_BODY_MAX_SIZE,
            RENAME_TABLE_BODY_TIMEOUT,
            "rename table",
        )
        .await?;
        let (source_namespace, source_table) = table_identifier_from_request(request.source)?;
        let (destination_namespace, destination_table) = table_identifier_from_request(request.destination)?;

        let source_resource = TableCatalogResource::table(&warehouse, &source_namespace, source_table.as_str());
        authorize_table_catalog_resource_for_principal(&req, &principal, &source_resource, AdminAction::SetTableAction).await?;
        let destination_resource = TableCatalogResource::table(&warehouse, &destination_namespace, destination_table.as_str());
        authorize_table_catalog_resource_for_principal(&req, &principal, &destination_resource, AdminAction::SetTableAction)
            .await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;

        let store = table_catalog_store_from_extensions(&req.extensions)?;
        store
            .rename_table(
                &warehouse,
                &source_namespace.public_name(),
                source_table.as_str(),
                &destination_namespace.public_name(),
                destination_table.as_str(),
            )
            .await
            .map_err(catalog_store_error)?;
        Ok(empty_response(StatusCode::NO_CONTENT))
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
        let metadata_backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let table_bucket_enabled = table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let commit_backend = TableCommitObjectBackend::preauthorized(metadata_backend);
        let response =
            create_table_response(&store, &commit_backend, &warehouse, &namespace, request, table_bucket_enabled).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestRegisterTableHandler {}

#[async_trait::async_trait]
impl Operation for RestRegisterTableHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        let principal = authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
        install_table_catalog_s3_request_info(&mut req, &principal)?;
        let request = read_json_body::<RegisterTableRequest>(std::mem::take(&mut req.input)).await?;
        let metadata_backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let table_bucket_enabled = table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let commit_backend = TableCommitObjectBackend::for_request(metadata_backend, req);
        let result =
            register_table_response(&store, &commit_backend, &warehouse, &namespace, request, table_bucket_enabled).await;
        let response = commit_backend.finish(result).await?;
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
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let metadata_backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let snapshot_selection = rest_table_snapshot_selection_from_query(&req.uri)?;
        let mut response = load_table_response(&store, &metadata_backend, &warehouse, &namespace, &table).await?;
        apply_rest_table_snapshot_selection(&mut response.metadata, snapshot_selection);
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
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
        Ok(empty_response(table_exists_status(&store, &warehouse, &namespace, &table).await?))
    }
}

pub struct RestCommitTableHandler {}

#[async_trait::async_trait]
impl Operation for RestCommitTableHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        let principal = authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        install_table_catalog_s3_request_info(&mut req, &principal)?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let request = read_rest_commit_table_request(std::mem::take(&mut req.input)).await?;
        let metadata_backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let commit_backend = TableCommitObjectBackend::for_request(metadata_backend, req);
        let result = commit_table_response(&store, &commit_backend, &warehouse, &namespace, &table, request).await;
        let response = commit_backend.finish(result).await?;
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
        let purge_requested = rest_purge_requested_from_query(&req.uri)?;
        if purge_requested {
            return Err(iceberg_rest_error(
                ICEBERG_ERROR_UNSUPPORTED_OPERATION,
                StatusCode::NOT_ACCEPTABLE,
                "purgeRequested=true is not supported",
            ));
        }
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
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
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
        let response = get_table_metadata_location_response(&store, &warehouse, &namespace, &table).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct UpdateTableMetadataLocationHandler {}

#[async_trait::async_trait]
impl Operation for UpdateTableMetadataLocationHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        let principal =
            authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableMetadataLocationAction).await?;
        install_table_catalog_s3_request_info(&mut req, &principal)?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let request = read_json_body::<UpdateTableMetadataLocationRequest>(std::mem::take(&mut req.input)).await?;
        let metadata_backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let commit_backend = TableCommitObjectBackend::for_request(metadata_backend, req);
        let result =
            update_table_metadata_location_response(&store, &commit_backend, &warehouse, &namespace, &table, request).await;
        let response = commit_backend.finish(result).await?;
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
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
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
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        let principal = authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
        install_table_catalog_s3_request_info(&mut req, &principal)?;
        let request = read_json_body::<CatalogImportRequest>(std::mem::take(&mut req.input)).await?;
        let metadata_backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let table_bucket_enabled = table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let commit_backend = TableCommitObjectBackend::for_request(metadata_backend, req);
        let result =
            catalog_import_response(&store, &commit_backend, &warehouse, &namespace, &table, request, table_bucket_enabled).await;
        let response = commit_backend.finish(result).await?;
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
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
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
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
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
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        let principal = authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        install_table_catalog_s3_request_info(&mut req, &principal)?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let request = read_json_body::<RollbackTableRequest>(std::mem::take(&mut req.input)).await?;
        let metadata_backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let commit_backend = TableCommitObjectBackend::for_request(metadata_backend, req);
        let result = rollback_table_response(&store, &commit_backend, &warehouse, &namespace, &table, request).await;
        let response = commit_backend.finish(result).await?;
        build_json_response(StatusCode::OK, &response)
    }
}
