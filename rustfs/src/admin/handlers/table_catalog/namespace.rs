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

pub struct RestListNamespacesHandler {}

#[async_trait::async_trait]
impl Operation for RestListNamespacesHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let parent = rest_namespace_parent_from_query(&req.uri)?;
        let resource = match &parent {
            Some(parent) => TableCatalogResource::namespace(&warehouse, parent),
            None => TableCatalogResource::warehouse(&warehouse),
        };
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableNamespaceAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
        let response = list_namespaces_response(&store, &warehouse, parent.as_ref(), &req.uri).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestCreateNamespaceHandler {}

#[async_trait::async_trait]
impl Operation for RestCreateNamespaceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableNamespaceAction).await?;
        let request = read_bounded_json_body::<CreateNamespaceRequest>(
            &req.headers,
            req.input,
            NAMESPACE_REQUEST_BODY_MAX_SIZE,
            NAMESPACE_REQUEST_BODY_TIMEOUT,
            "namespace creation",
        )
        .await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
        let table_bucket_enabled = table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let response = create_namespace_response(&store, &warehouse, request, table_bucket_enabled).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestGetNamespaceHandler {}

#[async_trait::async_trait]
impl Operation for RestGetNamespaceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableNamespaceAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
        let response = get_namespace_response(&store, &warehouse, &namespace).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestDropNamespaceHandler {}

#[async_trait::async_trait]
impl Operation for RestDropNamespaceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::DeleteTableNamespaceAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
        drop_namespace_in_store(&store, &warehouse, &namespace.public_name()).await?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}

pub struct RestUpdateNamespacePropertiesHandler {}

#[async_trait::async_trait]
impl Operation for RestUpdateNamespacePropertiesHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::UpdateTableNamespacePropertiesAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let request = read_bounded_json_body::<UpdateNamespacePropertiesRequest>(
            &req.headers,
            req.input,
            NAMESPACE_REQUEST_BODY_MAX_SIZE,
            NAMESPACE_REQUEST_BODY_TIMEOUT,
            "namespace properties",
        )
        .await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
        let response = update_namespace_properties_response(&store, &warehouse, &namespace, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestNamespaceExistsHandler {}

#[async_trait::async_trait]
impl Operation for RestNamespaceExistsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableNamespaceAction).await?;
        ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;
        let store = table_catalog_store_from_extensions(&req.extensions)?;
        Ok(empty_response(namespace_exists_status(&store, &warehouse, &namespace).await?))
    }
}
