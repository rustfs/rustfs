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

use crate::admin::{
    auth::validate_admin_request,
    router::{AdminOperation, Operation, S3Router},
};
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{RemoteAddr, TABLE_CATALOG_PREFIX};
use crate::table_catalog::DEFAULT_WAREHOUSE_ID;
use http::{HeaderMap, HeaderValue, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{Body, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::Serialize;
use std::collections::BTreeMap;

const JSON_CONTENT_TYPE: &str = "application/json";
const WAREHOUSE_PROPERTY: &str = "warehouse";
const UNSUPPORTED_OPERATION_TYPE: &str = "UnsupportedOperationException";
const UNSUPPORTED_OPERATION_STATUS: StatusCode = StatusCode::NOT_ACCEPTABLE;

static GET_CONFIG_HANDLER: GetCatalogConfigHandler = GetCatalogConfigHandler {};
static LIST_NAMESPACES_HANDLER: UnsupportedCatalogOperationHandler = UnsupportedCatalogOperationHandler {
    action: AdminAction::GetTableNamespaceAction,
    operation: "list namespaces",
};
static CREATE_NAMESPACE_HANDLER: UnsupportedCatalogOperationHandler = UnsupportedCatalogOperationHandler {
    action: AdminAction::SetTableNamespaceAction,
    operation: "create namespace",
};
static GET_NAMESPACE_HANDLER: UnsupportedCatalogOperationHandler = UnsupportedCatalogOperationHandler {
    action: AdminAction::GetTableNamespaceAction,
    operation: "get namespace",
};
static DROP_NAMESPACE_HANDLER: UnsupportedCatalogOperationHandler = UnsupportedCatalogOperationHandler {
    action: AdminAction::DeleteTableNamespaceAction,
    operation: "drop namespace",
};
static LIST_TABLES_HANDLER: UnsupportedCatalogOperationHandler = UnsupportedCatalogOperationHandler {
    action: AdminAction::GetTableAction,
    operation: "list tables",
};
static CREATE_TABLE_HANDLER: UnsupportedCatalogOperationHandler = UnsupportedCatalogOperationHandler {
    action: AdminAction::CreateTableAction,
    operation: "create table",
};
static REGISTER_TABLE_HANDLER: UnsupportedCatalogOperationHandler = UnsupportedCatalogOperationHandler {
    action: AdminAction::RegisterTableAction,
    operation: "register table",
};
static LOAD_TABLE_HANDLER: UnsupportedCatalogOperationHandler = UnsupportedCatalogOperationHandler {
    action: AdminAction::GetTableAction,
    operation: "load table",
};
static COMMIT_TABLE_HANDLER: UnsupportedCatalogOperationHandler = UnsupportedCatalogOperationHandler {
    action: AdminAction::CommitTableAction,
    operation: "commit table",
};
static DROP_TABLE_HANDLER: UnsupportedCatalogOperationHandler = UnsupportedCatalogOperationHandler {
    action: AdminAction::DeleteTableAction,
    operation: "drop table",
};

#[derive(Debug, Serialize)]
struct CatalogConfigResponse {
    defaults: BTreeMap<&'static str, &'static str>,
    overrides: BTreeMap<&'static str, &'static str>,
    endpoints: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: RestCatalogError,
}

#[derive(Debug, Serialize)]
struct RestCatalogError {
    message: String,
    #[serde(rename = "type")]
    error_type: &'static str,
    code: u16,
}

pub fn register_table_catalog_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{TABLE_CATALOG_PREFIX}/config").as_str(),
        AdminOperation(&GET_CONFIG_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{TABLE_CATALOG_PREFIX}/{{warehouse}}/namespaces").as_str(),
        AdminOperation(&LIST_NAMESPACES_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{TABLE_CATALOG_PREFIX}/{{warehouse}}/namespaces").as_str(),
        AdminOperation(&CREATE_NAMESPACE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{TABLE_CATALOG_PREFIX}/{{warehouse}}/namespaces/{{namespace}}").as_str(),
        AdminOperation(&GET_NAMESPACE_HANDLER),
    )?;
    r.insert(
        Method::DELETE,
        format!("{TABLE_CATALOG_PREFIX}/{{warehouse}}/namespaces/{{namespace}}").as_str(),
        AdminOperation(&DROP_NAMESPACE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{TABLE_CATALOG_PREFIX}/{{warehouse}}/namespaces/{{namespace}}/tables").as_str(),
        AdminOperation(&LIST_TABLES_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{TABLE_CATALOG_PREFIX}/{{warehouse}}/namespaces/{{namespace}}/tables").as_str(),
        AdminOperation(&CREATE_TABLE_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{TABLE_CATALOG_PREFIX}/{{warehouse}}/namespaces/{{namespace}}/register").as_str(),
        AdminOperation(&REGISTER_TABLE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{TABLE_CATALOG_PREFIX}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}").as_str(),
        AdminOperation(&LOAD_TABLE_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{TABLE_CATALOG_PREFIX}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}").as_str(),
        AdminOperation(&COMMIT_TABLE_HANDLER),
    )?;
    r.insert(
        Method::DELETE,
        format!("{TABLE_CATALOG_PREFIX}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}").as_str(),
        AdminOperation(&DROP_TABLE_HANDLER),
    )?;

    Ok(())
}

fn catalog_config_response() -> CatalogConfigResponse {
    CatalogConfigResponse {
        defaults: BTreeMap::from([(WAREHOUSE_PROPERTY, DEFAULT_WAREHOUSE_ID)]),
        overrides: BTreeMap::new(),
        endpoints: Vec::new(),
    }
}

fn build_json_response<T: Serialize>(status: StatusCode, body: &T) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(body).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(JSON_CONTENT_TYPE));
    Ok(S3Response::with_headers((status, Body::from(data)), headers))
}

async fn authorize_table_catalog_request(req: &S3Request<Body>, action: AdminAction) -> S3Result<()> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    validate_admin_request(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(action)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
    )
    .await
}

fn unsupported_response(operation: &str) -> S3Result<S3Response<(StatusCode, Body)>> {
    build_json_response(
        UNSUPPORTED_OPERATION_STATUS,
        &ErrorResponse {
            error: RestCatalogError {
                message: format!("Iceberg REST Catalog {operation} is not implemented yet"),
                error_type: UNSUPPORTED_OPERATION_TYPE,
                code: UNSUPPORTED_OPERATION_STATUS.as_u16(),
            },
        },
    )
}

pub struct GetCatalogConfigHandler {}

#[async_trait::async_trait]
impl Operation for GetCatalogConfigHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_table_catalog_request(&req, AdminAction::GetTableCatalogAction).await?;
        build_json_response(StatusCode::OK, &catalog_config_response())
    }
}

pub struct UnsupportedCatalogOperationHandler {
    action: AdminAction,
    operation: &'static str,
}

#[async_trait::async_trait]
impl Operation for UnsupportedCatalogOperationHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_table_catalog_request(&req, self.action).await?;
        unsupported_response(self.operation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_config_response_uses_default_warehouse_without_unsupported_endpoints() {
        let response = catalog_config_response();

        assert_eq!(response.defaults.get(WAREHOUSE_PROPERTY), Some(&DEFAULT_WAREHOUSE_ID));
        assert!(response.overrides.is_empty());
        assert!(response.endpoints.is_empty());
    }

    #[test]
    fn unsupported_response_uses_rest_catalog_unsupported_status() {
        let response = unsupported_response("create table").expect("unsupported response should build");

        assert_eq!(response.output.0, UNSUPPORTED_OPERATION_STATUS);
    }

    #[test]
    fn table_catalog_handlers_require_table_admin_actions() {
        let src = include_str!("table_catalog.rs");

        assert!(src.contains("AdminAction::GetTableCatalogAction"));
        assert!(src.contains("AdminAction::GetTableNamespaceAction"));
        assert!(src.contains("AdminAction::SetTableNamespaceAction"));
        assert!(src.contains("AdminAction::DeleteTableNamespaceAction"));
        assert!(src.contains("AdminAction::CreateTableAction"));
        assert!(src.contains("AdminAction::RegisterTableAction"));
        assert!(src.contains("AdminAction::CommitTableAction"));
        assert!(src.contains("AdminAction::DeleteTableAction"));
    }
}
