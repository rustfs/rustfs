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

pub struct RestListViewsHandler {}

#[async_trait::async_trait]
impl Operation for RestListViewsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
        let response = list_views_response(&store, &warehouse, &namespace, &req.uri).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestCreateViewHandler {}

#[async_trait::async_trait]
impl Operation for RestCreateViewHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::CreateTableAction).await?;
        let request = read_json_body::<CreateViewRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let table_bucket_enabled = table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let publication_backend = TableCommitObjectBackend::preauthorized(metadata_backend);
        let response =
            create_view_response(&store, &publication_backend, &warehouse, &namespace, request, table_bucket_enabled).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestLoadViewHandler {}

#[async_trait::async_trait]
impl Operation for RestLoadViewHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let view = view_name_from_params(&params)?;
        let resource = TableCatalogResource::view(&warehouse, &namespace, &view);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let metadata_backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response = load_view_response(&store, &metadata_backend, &warehouse, &namespace, &view).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestViewExistsHandler {}

#[async_trait::async_trait]
impl Operation for RestViewExistsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let view = view_name_from_params(&params)?;
        let resource = TableCatalogResource::view(&warehouse, &namespace, &view);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
        Ok(empty_response(view_exists_status(&store, &warehouse, &namespace, &view).await?))
    }
}

pub struct RestReplaceViewHandler {}

#[async_trait::async_trait]
impl Operation for RestReplaceViewHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let view = view_name_from_params(&params)?;
        let resource = TableCatalogResource::view(&warehouse, &namespace, &view);
        let principal = authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        install_table_catalog_s3_request_info(&mut req, &principal)?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let request = read_rest_commit_view_request(std::mem::take(&mut req.input)).await?;
        let metadata_backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let commit_backend = TableCommitObjectBackend::for_request(metadata_backend, req);
        let result = replace_view_response(&store, &commit_backend, &warehouse, &namespace, &view, request).await;
        let response = commit_backend.finish(result).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestDropViewHandler {}

#[async_trait::async_trait]
impl Operation for RestDropViewHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let view = view_name_from_params(&params)?;
        let resource = TableCatalogResource::view(&warehouse, &namespace, &view);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::DeleteTableAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
        drop_view_in_store(&store, &warehouse, &namespace, &view).await?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}
