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

pub struct RestLoadCredentialsHandler {}

#[async_trait::async_trait]
impl Operation for RestLoadCredentialsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableCredentialsAction).await?;
        ensure_table_bucket_enabled(&warehouse).await?;
        let principal = table_catalog_request_principal(&req).await?;
        let store = table_catalog_store()?;
        let issuer = IamTableCredentialIssuer::from_env();
        let response = load_credentials_response(&store, &warehouse, &namespace, &table, &issuer, Some(&principal)).await?;
        build_json_response(StatusCode::OK, &response)
    }
}
