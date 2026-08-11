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

pub struct ListTableRefsHandler {}

#[async_trait::async_trait]
impl Operation for ListTableRefsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let metadata_backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let response = table_refs_response(&store, &metadata_backend, &warehouse, &namespace, &table).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct PutTableRefHandler {}

#[async_trait::async_trait]
impl Operation for PutTableRefHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let ref_name = ref_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        let principal = authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        install_table_catalog_s3_request_info(&mut req, &principal)?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let request = read_json_body::<PutTableRefRequest>(std::mem::take(&mut req.input)).await?;
        let metadata_backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let commit_backend = TableCommitObjectBackend::for_request(metadata_backend, req);
        let result = put_table_ref_response(&store, &commit_backend, &warehouse, &namespace, &table, &ref_name, request).await;
        let response = commit_backend.finish(result).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct DeleteTableRefHandler {}

#[async_trait::async_trait]
impl Operation for DeleteTableRefHandler {
    async fn call(&self, mut req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let ref_name = ref_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        let principal = authorize_table_catalog_resource_request(&req, &resource, AdminAction::CommitTableAction).await?;
        install_table_catalog_s3_request_info(&mut req, &principal)?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let request = read_json_body_or_default::<DeleteTableRefRequest>(std::mem::take(&mut req.input)).await?;
        let metadata_backend = table_catalog_backend_from_extensions(&req.extensions)?;
        let store = table_catalog_store_from_backend(metadata_backend.clone())?;
        let commit_backend = TableCommitObjectBackend::for_request(metadata_backend, req);
        let result = delete_table_ref_response(&store, &commit_backend, &warehouse, &namespace, &table, &ref_name, request).await;
        let response = commit_backend.finish(result).await?;
        build_json_response(StatusCode::OK, &response)
    }
}
