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
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_ecstore::{bucket::metadata_sys, new_object_layer_fn, store::ECStore};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::{Body, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use uuid::Uuid;

const JSON_CONTENT_TYPE: &str = "application/json";
const WAREHOUSE_PROPERTY: &str = "warehouse";
const TABLE_CATALOG_ENDPOINTS: &[&str] = &[
    "GET /{warehouse}/namespaces",
    "POST /{warehouse}/namespaces",
    "GET /{warehouse}/namespaces/{namespace}",
    "DELETE /{warehouse}/namespaces/{namespace}",
    "GET /{warehouse}/namespaces/{namespace}/tables",
    "POST /{warehouse}/namespaces/{namespace}/register",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}",
    "DELETE /{warehouse}/namespaces/{namespace}/tables/{table}",
];

static GET_CONFIG_HANDLER: GetCatalogConfigHandler = GetCatalogConfigHandler {};
static LIST_NAMESPACES_HANDLER: RestListNamespacesHandler = RestListNamespacesHandler {};
static CREATE_NAMESPACE_HANDLER: RestCreateNamespaceHandler = RestCreateNamespaceHandler {};
static GET_NAMESPACE_HANDLER: RestGetNamespaceHandler = RestGetNamespaceHandler {};
static DROP_NAMESPACE_HANDLER: RestDropNamespaceHandler = RestDropNamespaceHandler {};
static LIST_TABLES_HANDLER: RestListTablesHandler = RestListTablesHandler {};
static CREATE_TABLE_HANDLER: RestCreateTableHandler = RestCreateTableHandler {};
static REGISTER_TABLE_HANDLER: RestRegisterTableHandler = RestRegisterTableHandler {};
static LOAD_TABLE_HANDLER: RestLoadTableHandler = RestLoadTableHandler {};
static COMMIT_TABLE_HANDLER: RestCommitTableHandler = RestCommitTableHandler {};
static DROP_TABLE_HANDLER: RestDropTableHandler = RestDropTableHandler {};

#[derive(Debug, Serialize)]
struct CatalogConfigResponse {
    defaults: BTreeMap<&'static str, &'static str>,
    overrides: BTreeMap<&'static str, &'static str>,
    endpoints: Vec<&'static str>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateNamespaceRequest {
    namespace: Vec<String>,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterTableRequest {
    name: String,
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    #[serde(default)]
    overwrite: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTableRequest {
    name: String,
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestCommitTableRequest {
    #[serde(rename = "commit-id")]
    commit_id: String,
    #[serde(default, rename = "idempotency-key")]
    idempotency_key: Option<String>,
    operation: String,
    #[serde(rename = "expected-version-token")]
    expected_version_token: String,
    #[serde(rename = "expected-metadata-location")]
    expected_metadata_location: String,
    #[serde(rename = "new-metadata-location")]
    new_metadata_location: String,
    #[serde(default)]
    requirements: Vec<serde_json::Value>,
    #[serde(default)]
    writer: Option<String>,
}

#[derive(Debug, Serialize)]
struct RestNamespaceResponse {
    namespace: Vec<String>,
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct RestListNamespacesResponse {
    namespaces: Vec<Vec<String>>,
}

#[derive(Debug, Serialize)]
struct RestTableIdentifier {
    namespace: Vec<String>,
    name: String,
}

#[derive(Debug, Serialize)]
struct RestListTablesResponse {
    identifiers: Vec<RestTableIdentifier>,
}

#[derive(Debug, Serialize)]
struct RestLoadTableResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    metadata: serde_json::Value,
    config: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct RestCommitTableResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    #[serde(rename = "version-token")]
    version_token: String,
    generation: u64,
    #[serde(rename = "commit-id")]
    commit_id: String,
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
        endpoints: TABLE_CATALOG_ENDPOINTS.to_vec(),
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

async fn read_json_body<T: DeserializeOwned>(mut input: Body) -> S3Result<T> {
    let body = input
        .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
        .await
        .map_err(|err| s3_error!(InvalidRequest, "failed to read request body: {}", err))?;
    if body.is_empty() {
        return Err(s3_error!(InvalidRequest, "request body is required"));
    }
    serde_json::from_slice(&body).map_err(|err| s3_error!(InvalidRequest, "invalid JSON: {}", err))
}

fn warehouse_from_params(params: &Params<'_, '_>) -> S3Result<String> {
    let warehouse = params.get("warehouse").unwrap_or("");
    if warehouse.is_empty() {
        return Err(s3_error!(InvalidRequest, "warehouse is required"));
    }
    Ok(warehouse.to_string())
}

fn namespace_from_params(params: &Params<'_, '_>) -> S3Result<crate::table_catalog::Namespace> {
    let namespace = params.get("namespace").unwrap_or("");
    crate::table_catalog::Namespace::parse(namespace).map_err(|err| s3_error!(InvalidRequest, "invalid namespace: {}", err))
}

fn table_name_from_params(params: &Params<'_, '_>) -> S3Result<String> {
    let table = params.get("table").unwrap_or("");
    crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    Ok(table.to_string())
}

fn table_catalog_backend() -> S3Result<crate::table_catalog::EcStoreTableCatalogObjectBackend<ECStore>> {
    let store = new_object_layer_fn().ok_or_else(|| s3_error!(InternalError, "object store not initialized"))?;
    Ok(crate::table_catalog::EcStoreTableCatalogObjectBackend::new(store))
}

fn table_catalog_store() -> S3Result<crate::table_catalog::EcStoreTableCatalogStore<ECStore>> {
    let backend = table_catalog_backend()?;
    Ok(crate::table_catalog::ObjectTableCatalogStore::new(backend))
}

async fn table_bucket_enabled_from_metadata(bucket: &str) -> S3Result<bool> {
    let metadata = metadata_sys::get(bucket)
        .await
        .map_err(|err| s3_error!(InvalidRequest, "failed to load table bucket metadata for {bucket}: {}", err))?;
    Ok(metadata.table_bucket_enabled())
}

fn table_bucket_entry_from_metadata_marker(bucket: &str) -> crate::table_catalog::TableBucketEntry {
    crate::table_catalog::TableBucketEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        catalog_type: crate::table_catalog::TABLE_BUCKET_CATALOG_TYPE.to_string(),
        warehouse_root: format!("s3://{bucket}/"),
        state: crate::table_catalog::TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    }
}

async fn ensure_table_bucket_entry<S>(store: &S, bucket: &str, table_bucket_enabled: bool) -> S3Result<()>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    if !table_bucket_enabled {
        return Err(s3_error!(InvalidRequest, "bucket {bucket} is not table-enabled"));
    }
    if store.get_table_bucket(bucket).await.map_err(catalog_store_error)?.is_some() {
        return Ok(());
    }
    store
        .put_table_bucket(table_bucket_entry_from_metadata_marker(bucket))
        .await
        .map_err(catalog_store_error)
}

fn namespace_segments(namespace: &crate::table_catalog::Namespace) -> Vec<String> {
    namespace
        .segments()
        .iter()
        .map(|segment| segment.as_str().to_string())
        .collect()
}

fn namespace_from_segments(segments: &[String]) -> S3Result<crate::table_catalog::Namespace> {
    if segments.is_empty() {
        return Err(s3_error!(InvalidRequest, "namespace cannot be empty"));
    }

    let namespace = segments.join(".");
    crate::table_catalog::Namespace::parse(&namespace).map_err(|err| s3_error!(InvalidRequest, "invalid namespace: {}", err))
}

fn namespace_response_from_entry(entry: crate::table_catalog::NamespaceEntry) -> S3Result<RestNamespaceResponse> {
    let namespace = crate::table_catalog::Namespace::parse(&entry.namespace)
        .map_err(|err| s3_error!(InternalError, "persisted namespace entry is invalid: {}", err))?;
    Ok(RestNamespaceResponse {
        namespace: namespace_segments(&namespace),
        properties: entry.properties,
    })
}

fn list_namespaces_response_from_entries(
    entries: Vec<crate::table_catalog::NamespaceEntry>,
) -> S3Result<RestListNamespacesResponse> {
    let namespaces = entries
        .into_iter()
        .map(|entry| {
            let namespace = crate::table_catalog::Namespace::parse(&entry.namespace)
                .map_err(|err| s3_error!(InternalError, "persisted namespace entry is invalid: {}", err))?;
            Ok(namespace_segments(&namespace))
        })
        .collect::<S3Result<Vec<_>>>()?;
    Ok(RestListNamespacesResponse { namespaces })
}

fn list_tables_response_from_entries(entries: Vec<crate::table_catalog::TableEntry>) -> S3Result<RestListTablesResponse> {
    let identifiers = entries
        .into_iter()
        .map(|entry| {
            let namespace = crate::table_catalog::Namespace::parse(&entry.namespace)
                .map_err(|err| s3_error!(InternalError, "persisted table entry namespace is invalid: {}", err))?;
            Ok(RestTableIdentifier {
                namespace: namespace_segments(&namespace),
                name: entry.table,
            })
        })
        .collect::<S3Result<Vec<_>>>()?;
    Ok(RestListTablesResponse { identifiers })
}

fn load_table_response_from_entry(entry: crate::table_catalog::TableEntry, metadata: serde_json::Value) -> RestLoadTableResponse {
    RestLoadTableResponse {
        metadata_location: entry.metadata_location,
        metadata,
        config: BTreeMap::from([("warehouse-location".to_string(), entry.warehouse_location)]),
    }
}

fn commit_table_response_from_result(result: crate::table_catalog::TableCommitResult) -> RestCommitTableResponse {
    RestCommitTableResponse {
        metadata_location: result.table.metadata_location,
        version_token: result.table.version_token,
        generation: result.table.generation,
        commit_id: result.commit_log.commit_id,
    }
}

fn table_commit_request_from_rest_request(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: RestCommitTableRequest,
) -> crate::table_catalog::TableCommitRequest {
    crate::table_catalog::TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        commit_id: request.commit_id,
        idempotency_key: request.idempotency_key,
        operation: request.operation,
        expected_version_token: request.expected_version_token,
        expected_metadata_location: request.expected_metadata_location,
        new_metadata_location: request.new_metadata_location,
        requirements: request.requirements,
        writer: request.writer,
    }
}

fn table_entry_from_register_request(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    request: RegisterTableRequest,
) -> S3Result<crate::table_catalog::TableEntry> {
    if request.overwrite {
        return Err(s3_error!(NotImplemented, "register table overwrite is not supported"));
    }
    let table = crate::table_catalog::IdentifierSegment::parse(request.name)
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    if !crate::table_catalog::is_valid_table_metadata_location(namespace, &table, &request.metadata_location) {
        return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
    }

    let table_id = Uuid::new_v4().to_string();
    Ok(crate::table_catalog::TableEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id: table_id.clone(),
        table_uuid: Uuid::new_v4().to_string(),
        format: "ICEBERG".to_string(),
        format_version: 2,
        warehouse_location: format!("s3://{bucket}/tables/{table_id}"),
        metadata_location: request.metadata_location,
        version_token: format!("token-{}", Uuid::new_v4()),
        generation: 1,
        state: crate::table_catalog::TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    })
}

fn table_entry_from_create_table_request(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    request: CreateTableRequest,
) -> S3Result<crate::table_catalog::TableEntry> {
    let properties = request.properties;
    let mut entry = table_entry_from_register_request(
        bucket,
        namespace,
        RegisterTableRequest {
            name: request.name,
            metadata_location: request.metadata_location,
            overwrite: false,
        },
    )?;
    entry.properties = properties;
    Ok(entry)
}

fn namespace_entry_from_create_request(
    bucket: &str,
    request: CreateNamespaceRequest,
) -> S3Result<crate::table_catalog::NamespaceEntry> {
    let namespace = namespace_from_segments(&request.namespace)?;
    Ok(crate::table_catalog::NamespaceEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        namespace_id: namespace.storage_id(),
        state: crate::table_catalog::TableCatalogEntryState::Active,
        properties: request.properties,
        created_at: None,
        updated_at: None,
    })
}

fn catalog_store_error(err: crate::table_catalog::TableCatalogStoreError) -> s3s::S3Error {
    match err {
        crate::table_catalog::TableCatalogStoreError::NotFound(message) => {
            s3_error!(InvalidRequest, "{message}")
        }
        crate::table_catalog::TableCatalogStoreError::Conflict(message) => {
            s3_error!(PreconditionFailed, "{message}")
        }
        crate::table_catalog::TableCatalogStoreError::Invalid(message) => {
            s3_error!(InvalidRequest, "{message}")
        }
        crate::table_catalog::TableCatalogStoreError::Internal(message) => {
            s3_error!(InternalError, "{message}")
        }
    }
}

async fn create_namespace_response<S>(
    store: &S,
    bucket: &str,
    request: CreateNamespaceRequest,
    table_bucket_enabled: bool,
) -> S3Result<RestNamespaceResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let entry = namespace_entry_from_create_request(bucket, request)?;
    ensure_table_bucket_entry(store, bucket, table_bucket_enabled).await?;
    store.create_namespace(entry.clone()).await.map_err(catalog_store_error)?;
    namespace_response_from_entry(entry)
}

async fn list_namespaces_response<S>(store: &S, bucket: &str) -> S3Result<RestListNamespacesResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let entries = store.list_namespaces(bucket).await.map_err(catalog_store_error)?;
    list_namespaces_response_from_entries(entries)
}

async fn get_namespace_response<S>(
    store: &S,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
) -> S3Result<RestNamespaceResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(entry) = store
        .get_namespace(bucket, &namespace.public_name())
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(s3_error!(InvalidRequest, "namespace not found"));
    };
    namespace_response_from_entry(entry)
}

async fn drop_namespace_in_store<S>(store: &S, bucket: &str, namespace: &str) -> S3Result<()>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    store.drop_namespace(bucket, namespace).await.map_err(catalog_store_error)
}

async fn register_table_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    request: RegisterTableRequest,
    table_bucket_enabled: bool,
) -> S3Result<RestLoadTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let entry = table_entry_from_register_request(bucket, namespace, request)?;
    ensure_table_bucket_entry(store, bucket, table_bucket_enabled).await?;
    let metadata = read_table_metadata_json(metadata_backend, bucket, &entry.metadata_location).await?;
    store.register_table(entry.clone()).await.map_err(catalog_store_error)?;
    Ok(load_table_response_from_entry(entry, metadata))
}

async fn create_table_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    request: CreateTableRequest,
    table_bucket_enabled: bool,
) -> S3Result<RestLoadTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let entry = table_entry_from_create_table_request(bucket, namespace, request)?;
    ensure_table_bucket_entry(store, bucket, table_bucket_enabled).await?;
    let metadata = read_table_metadata_json(metadata_backend, bucket, &entry.metadata_location).await?;
    store.create_table(entry.clone()).await.map_err(catalog_store_error)?;
    Ok(load_table_response_from_entry(entry, metadata))
}

async fn read_table_metadata_json(
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    metadata_location: &str,
) -> S3Result<serde_json::Value> {
    let Some(object) = metadata_backend
        .read_object(bucket, metadata_location)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(s3_error!(InvalidRequest, "table metadata object not found: {metadata_location}"));
    };
    let metadata = serde_json::from_slice::<serde_json::Value>(&object.data)
        .map_err(|err| s3_error!(InvalidRequest, "failed to parse table metadata JSON: {}", err))?;
    if !metadata.is_object() {
        return Err(s3_error!(InvalidRequest, "table metadata JSON must be an object"));
    }
    Ok(metadata)
}

async fn list_tables_response<S>(
    store: &S,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
) -> S3Result<RestListTablesResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let entries = store
        .list_tables(bucket, &namespace.public_name())
        .await
        .map_err(catalog_store_error)?;
    list_tables_response_from_entries(entries)
}

async fn load_table_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
) -> S3Result<RestLoadTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(entry) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(s3_error!(InvalidRequest, "table not found"));
    };
    let metadata = read_table_metadata_json(metadata_backend, bucket, &entry.metadata_location).await?;
    Ok(load_table_response_from_entry(entry, metadata))
}

async fn commit_table_response<S>(
    store: &S,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: RestCommitTableRequest,
) -> S3Result<RestCommitTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let request = table_commit_request_from_rest_request(bucket, namespace, table, request);
    let result = store.commit_table(request).await.map_err(catalog_store_error)?;
    Ok(commit_table_response_from_result(result))
}

async fn drop_table_in_store<S>(store: &S, bucket: &str, namespace: &crate::table_catalog::Namespace, table: &str) -> S3Result<()>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    store
        .drop_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)
}

pub struct GetCatalogConfigHandler {}

#[async_trait::async_trait]
impl Operation for GetCatalogConfigHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_table_catalog_request(&req, AdminAction::GetTableCatalogAction).await?;
        build_json_response(StatusCode::OK, &catalog_config_response())
    }
}

pub struct RestListNamespacesHandler {}

#[async_trait::async_trait]
impl Operation for RestListNamespacesHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_table_catalog_request(&req, AdminAction::GetTableNamespaceAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let store = table_catalog_store()?;
        let response = list_namespaces_response(&store, &warehouse).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestCreateNamespaceHandler {}

#[async_trait::async_trait]
impl Operation for RestCreateNamespaceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_table_catalog_request(&req, AdminAction::SetTableNamespaceAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let request = read_json_body::<CreateNamespaceRequest>(req.input).await?;
        let store = table_catalog_store()?;
        let table_bucket_enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        let response = create_namespace_response(&store, &warehouse, request, table_bucket_enabled).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestGetNamespaceHandler {}

#[async_trait::async_trait]
impl Operation for RestGetNamespaceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_table_catalog_request(&req, AdminAction::GetTableNamespaceAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let store = table_catalog_store()?;
        let response = get_namespace_response(&store, &warehouse, &namespace).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestDropNamespaceHandler {}

#[async_trait::async_trait]
impl Operation for RestDropNamespaceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_table_catalog_request(&req, AdminAction::DeleteTableNamespaceAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let store = table_catalog_store()?;
        drop_namespace_in_store(&store, &warehouse, &namespace.public_name()).await?;
        Ok(S3Response::new((StatusCode::NO_CONTENT, Body::default())))
    }
}

pub struct RestListTablesHandler {}

#[async_trait::async_trait]
impl Operation for RestListTablesHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_table_catalog_request(&req, AdminAction::GetTableAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let store = table_catalog_store()?;
        let response = list_tables_response(&store, &warehouse, &namespace).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestCreateTableHandler {}

#[async_trait::async_trait]
impl Operation for RestCreateTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_table_catalog_request(&req, AdminAction::CreateTableAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let request = read_json_body::<CreateTableRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone());
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
        authorize_table_catalog_request(&req, AdminAction::RegisterTableAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let request = read_json_body::<RegisterTableRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone());
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
        authorize_table_catalog_request(&req, AdminAction::GetTableAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let metadata_backend = table_catalog_backend()?;
        let store = crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone());
        let response = load_table_response(&store, &metadata_backend, &warehouse, &namespace, &table).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestCommitTableHandler {}

#[async_trait::async_trait]
impl Operation for RestCommitTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_table_catalog_request(&req, AdminAction::CommitTableAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let request = read_json_body::<RestCommitTableRequest>(req.input).await?;
        let store = table_catalog_store()?;
        let response = commit_table_response(&store, &warehouse, &namespace, &table, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestDropTableHandler {}

#[async_trait::async_trait]
impl Operation for RestDropTableHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_table_catalog_request(&req, AdminAction::DeleteTableAction).await?;
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let store = table_catalog_store()?;
        drop_table_in_store(&store, &warehouse, &namespace, &table).await?;
        Ok(S3Response::new((StatusCode::NO_CONTENT, Body::default())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_catalog::TableCatalogStore;
    use std::sync::Arc;

    #[test]
    fn catalog_config_response_lists_mvp_rest_endpoints() {
        let response = catalog_config_response();

        assert_eq!(response.defaults.get(WAREHOUSE_PROPERTY), Some(&DEFAULT_WAREHOUSE_ID));
        assert!(response.overrides.is_empty());
        assert!(response.endpoints.contains(&"GET /{warehouse}/namespaces"));
        assert!(response.endpoints.contains(&"POST /{warehouse}/namespaces"));
        assert!(
            response
                .endpoints
                .contains(&"POST /{warehouse}/namespaces/{namespace}/register")
        );
        assert!(
            !response
                .endpoints
                .contains(&"POST /{warehouse}/namespaces/{namespace}/tables")
        );
        assert!(
            response
                .endpoints
                .contains(&"GET /{warehouse}/namespaces/{namespace}/tables/{table}")
        );
        assert!(
            !response
                .endpoints
                .contains(&"POST /{warehouse}/namespaces/{namespace}/tables/{table}")
        );
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

    #[test]
    fn rest_catalog_mvp_routes_use_implemented_handlers() {
        fn assert_operation<T: Operation>() {}

        let _: &RestListNamespacesHandler = &LIST_NAMESPACES_HANDLER;
        let _: &RestCreateNamespaceHandler = &CREATE_NAMESPACE_HANDLER;
        let _: &RestGetNamespaceHandler = &GET_NAMESPACE_HANDLER;
        let _: &RestDropNamespaceHandler = &DROP_NAMESPACE_HANDLER;
        let _: &RestListTablesHandler = &LIST_TABLES_HANDLER;
        let _: &RestCreateTableHandler = &CREATE_TABLE_HANDLER;
        let _: &RestRegisterTableHandler = &REGISTER_TABLE_HANDLER;
        let _: &RestLoadTableHandler = &LOAD_TABLE_HANDLER;
        let _: &RestCommitTableHandler = &COMMIT_TABLE_HANDLER;
        let _: &RestDropTableHandler = &DROP_TABLE_HANDLER;

        assert_operation::<RestListNamespacesHandler>();
        assert_operation::<RestCreateNamespaceHandler>();
        assert_operation::<RestGetNamespaceHandler>();
        assert_operation::<RestDropNamespaceHandler>();
        assert_operation::<RestListTablesHandler>();
        assert_operation::<RestCreateTableHandler>();
        assert_operation::<RestRegisterTableHandler>();
        assert_operation::<RestLoadTableHandler>();
        assert_operation::<RestCommitTableHandler>();
        assert_operation::<RestDropTableHandler>();
    }

    #[test]
    fn create_namespace_request_uses_rest_namespace_segments_and_properties() {
        let request: CreateNamespaceRequest = serde_json::from_value(serde_json::json!({
            "namespace": ["analytics", "daily_events"],
            "properties": {
                "owner": "lakehouse"
            }
        }))
        .expect("request should parse");
        let namespace = namespace_from_segments(&request.namespace).expect("namespace should be valid");
        let response = namespace_response_from_entry(crate::table_catalog::NamespaceEntry {
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "warehouse".to_string(),
            namespace: namespace.public_name(),
            namespace_id: namespace.storage_id(),
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: request.properties,
            created_at: None,
            updated_at: None,
        })
        .expect("namespace response should build");

        assert_eq!(namespace.public_name(), "analytics.daily_events");
        assert_eq!(response.namespace, vec!["analytics".to_string(), "daily_events".to_string()]);
        assert_eq!(response.properties.get("owner").map(String::as_str), Some("lakehouse"));
    }

    #[test]
    fn list_tables_response_uses_rest_identifier_shape() {
        let namespace = crate::table_catalog::Namespace::parse("analytics.daily_events").expect("namespace should parse");
        let response = list_tables_response_from_entries(vec![crate::table_catalog::TableEntry {
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "warehouse".to_string(),
            namespace: namespace.public_name(),
            table: "events".to_string(),
            table_id: "table-id".to_string(),
            table_uuid: "table-uuid".to_string(),
            format: "ICEBERG".to_string(),
            format_version: 2,
            warehouse_location: "s3://warehouse/tables/table-id".to_string(),
            metadata_location:
                ".rustfs-table/warehouses/default/namespaces/analytics/daily_events/tables/events/metadata/00001.metadata.json"
                    .to_string(),
            version_token: "token-v1".to_string(),
            generation: 1,
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        }])
        .expect("table list response should build");

        assert_eq!(
            response.identifiers[0].namespace,
            vec!["analytics".to_string(), "daily_events".to_string()]
        );
        assert_eq!(response.identifiers[0].name, "events");
    }

    #[test]
    fn register_table_request_builds_initial_table_entry() {
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let request: RegisterTableRequest = serde_json::from_value(serde_json::json!({
            "name": "events",
            "metadata-location": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json",
            "overwrite": false
        }))
        .expect("request should parse");

        let entry = table_entry_from_register_request("warehouse", &namespace, request).expect("table entry should build");

        assert_eq!(entry.table_bucket, "warehouse");
        assert_eq!(entry.namespace, "analytics");
        assert_eq!(entry.table, "events");
        assert_eq!(
            entry.metadata_location,
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json"
        );
        assert!(entry.properties.is_empty());
        assert_eq!(entry.generation, 1);
        assert!(!entry.version_token.is_empty());
    }

    #[test]
    fn load_table_response_includes_rest_metadata_payload() {
        let metadata = serde_json::json!({
            "format-version": 2,
            "table-uuid": "table-uuid",
            "location": "s3://warehouse/tables/table-id"
        });
        let response = load_table_response_from_entry(
            crate::table_catalog::TableEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: "warehouse".to_string(),
                namespace: "analytics".to_string(),
                table: "events".to_string(),
                table_id: "table-id".to_string(),
                table_uuid: "table-uuid".to_string(),
                format: "ICEBERG".to_string(),
                format_version: 2,
                warehouse_location: "s3://warehouse/tables/table-id".to_string(),
                metadata_location:
                    ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json".to_string(),
                version_token: "token-v1".to_string(),
                generation: 1,
                state: crate::table_catalog::TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            },
            metadata.clone(),
        );

        assert_eq!(response.metadata, metadata);
    }

    #[test]
    fn commit_table_request_uses_rest_commit_fields() {
        let request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "commit-id": "commit-1",
            "idempotency-key": "retry-1",
            "operation": "append",
            "expected-version-token": "token-v1",
            "expected-metadata-location": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json",
            "new-metadata-location": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json",
            "requirements": [
                {
                    "type": "assert-current-snapshot-id",
                    "snapshot-id": 10
                }
            ],
            "writer": "pyiceberg"
        }))
        .expect("commit request should parse");

        assert_eq!(request.commit_id, "commit-1");
        assert_eq!(request.idempotency_key.as_deref(), Some("retry-1"));
        assert_eq!(request.operation, "append");
        assert_eq!(request.expected_version_token, "token-v1");
        assert_eq!(
            request.new_metadata_location,
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json"
        );
        assert_eq!(request.requirements.len(), 1);
        assert_eq!(request.writer.as_deref(), Some("pyiceberg"));
    }

    #[derive(Default)]
    struct TestTableCatalogStore {
        table_buckets: tokio::sync::Mutex<Vec<crate::table_catalog::TableBucketEntry>>,
        namespaces: tokio::sync::Mutex<Vec<crate::table_catalog::NamespaceEntry>>,
        tables: tokio::sync::Mutex<Vec<crate::table_catalog::TableEntry>>,
        commits: tokio::sync::Mutex<Vec<crate::table_catalog::CommitLogEntry>>,
    }

    #[derive(Clone, Default)]
    struct TestTableCatalogObjectBackend {
        objects: Arc<tokio::sync::Mutex<BTreeMap<(String, String), crate::table_catalog::TableCatalogObject>>>,
    }

    impl TestTableCatalogObjectBackend {
        async fn put_json(&self, bucket: &str, object: &str, value: serde_json::Value) {
            let data = serde_json::to_vec(&value).expect("metadata JSON should serialize");
            self.objects.lock().await.insert(
                (bucket.to_string(), object.to_string()),
                crate::table_catalog::TableCatalogObject {
                    data,
                    etag: Some("etag".to_string()),
                },
            );
        }
    }

    #[async_trait::async_trait]
    impl crate::table_catalog::TableCatalogObjectBackend for TestTableCatalogObjectBackend {
        async fn read_object(
            &self,
            bucket: &str,
            object: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::TableCatalogObject>> {
            Ok(self
                .objects
                .lock()
                .await
                .get(&(bucket.to_string(), object.to_string()))
                .cloned())
        }

        async fn object_exists(&self, bucket: &str, object: &str) -> crate::table_catalog::TableCatalogStoreResult<bool> {
            Ok(self
                .objects
                .lock()
                .await
                .contains_key(&(bucket.to_string(), object.to_string())))
        }

        async fn put_object(
            &self,
            bucket: &str,
            object: &str,
            data: Vec<u8>,
            _precondition: crate::table_catalog::TableCatalogPutPrecondition,
        ) -> crate::table_catalog::TableCatalogStoreResult<()> {
            self.objects.lock().await.insert(
                (bucket.to_string(), object.to_string()),
                crate::table_catalog::TableCatalogObject {
                    data,
                    etag: Some("etag".to_string()),
                },
            );
            Ok(())
        }

        async fn delete_object(&self, bucket: &str, object: &str) -> crate::table_catalog::TableCatalogStoreResult<()> {
            self.objects.lock().await.remove(&(bucket.to_string(), object.to_string()));
            Ok(())
        }

        async fn list_objects(&self, bucket: &str, prefix: &str) -> crate::table_catalog::TableCatalogStoreResult<Vec<String>> {
            Ok(self
                .objects
                .lock()
                .await
                .keys()
                .filter(|(object_bucket, object)| object_bucket == bucket && object.starts_with(prefix))
                .map(|(_, object)| object.clone())
                .collect())
        }

        async fn acquire_write_lock(
            &self,
            _bucket: &str,
            _object: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Box<dyn Send>> {
            Ok(Box::new(()))
        }
    }

    #[async_trait::async_trait]
    impl crate::table_catalog::TableCatalogStore for TestTableCatalogStore {
        async fn get_table_bucket(
            &self,
            table_bucket: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::TableBucketEntry>> {
            Ok(self
                .table_buckets
                .lock()
                .await
                .iter()
                .find(|entry| entry.table_bucket == table_bucket)
                .cloned())
        }

        async fn put_table_bucket(
            &self,
            entry: crate::table_catalog::TableBucketEntry,
        ) -> crate::table_catalog::TableCatalogStoreResult<()> {
            let mut table_buckets = self.table_buckets.lock().await;
            table_buckets.retain(|existing| existing.table_bucket != entry.table_bucket);
            table_buckets.push(entry);
            Ok(())
        }

        async fn create_namespace(
            &self,
            entry: crate::table_catalog::NamespaceEntry,
        ) -> crate::table_catalog::TableCatalogStoreResult<()> {
            if self.get_table_bucket(&entry.table_bucket).await?.is_none() {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "table bucket {}",
                    entry.table_bucket
                )));
            }
            self.namespaces.lock().await.push(entry);
            Ok(())
        }

        async fn list_namespaces(
            &self,
            table_bucket: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Vec<crate::table_catalog::NamespaceEntry>> {
            Ok(self
                .namespaces
                .lock()
                .await
                .iter()
                .filter(|entry| entry.table_bucket == table_bucket)
                .cloned()
                .collect())
        }

        async fn get_namespace(
            &self,
            table_bucket: &str,
            namespace: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::NamespaceEntry>> {
            Ok(self
                .namespaces
                .lock()
                .await
                .iter()
                .find(|entry| entry.table_bucket == table_bucket && entry.namespace == namespace)
                .cloned())
        }

        async fn drop_namespace(&self, table_bucket: &str, namespace: &str) -> crate::table_catalog::TableCatalogStoreResult<()> {
            self.namespaces
                .lock()
                .await
                .retain(|entry| !(entry.table_bucket == table_bucket && entry.namespace == namespace));
            Ok(())
        }

        async fn create_table(
            &self,
            entry: crate::table_catalog::TableEntry,
        ) -> crate::table_catalog::TableCatalogStoreResult<()> {
            if self.get_table_bucket(&entry.table_bucket).await?.is_none() {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "table bucket {}",
                    entry.table_bucket
                )));
            }
            if self.get_namespace(&entry.table_bucket, &entry.namespace).await?.is_none() {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "namespace {}/{}",
                    entry.table_bucket, entry.namespace
                )));
            }
            self.tables.lock().await.push(entry);
            Ok(())
        }

        async fn register_table(
            &self,
            entry: crate::table_catalog::TableEntry,
        ) -> crate::table_catalog::TableCatalogStoreResult<()> {
            if self.get_table_bucket(&entry.table_bucket).await?.is_none() {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "table bucket {}",
                    entry.table_bucket
                )));
            }
            if self.get_namespace(&entry.table_bucket, &entry.namespace).await?.is_none() {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "namespace {}/{}",
                    entry.table_bucket, entry.namespace
                )));
            }
            self.tables.lock().await.push(entry);
            Ok(())
        }

        async fn list_tables(
            &self,
            table_bucket: &str,
            namespace: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Vec<crate::table_catalog::TableEntry>> {
            Ok(self
                .tables
                .lock()
                .await
                .iter()
                .filter(|entry| entry.table_bucket == table_bucket && entry.namespace == namespace)
                .cloned()
                .collect())
        }

        async fn load_table(
            &self,
            table_bucket: &str,
            namespace: &str,
            table: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::TableEntry>> {
            Ok(self
                .tables
                .lock()
                .await
                .iter()
                .find(|entry| entry.table_bucket == table_bucket && entry.namespace == namespace && entry.table == table)
                .cloned())
        }

        async fn commit_table(
            &self,
            request: crate::table_catalog::TableCommitRequest,
        ) -> crate::table_catalog::TableCatalogStoreResult<crate::table_catalog::TableCommitResult> {
            let mut tables = self.tables.lock().await;
            let Some(index) = tables.iter().position(|entry| {
                entry.table_bucket == request.table_bucket && entry.namespace == request.namespace && entry.table == request.table
            }) else {
                return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                    "table {}/{}/{}",
                    request.table_bucket, request.namespace, request.table
                )));
            };

            let current = tables[index].clone();
            if current.version_token != request.expected_version_token {
                return Err(crate::table_catalog::TableCatalogStoreError::Conflict(
                    "current table version token does not match expected token".to_string(),
                ));
            }
            if current.metadata_location != request.expected_metadata_location {
                return Err(crate::table_catalog::TableCatalogStoreError::Conflict(
                    "current table metadata location does not match expected location".to_string(),
                ));
            }

            let mut next = current.clone();
            next.metadata_location = request.new_metadata_location.clone();
            next.version_token = "token-committed".to_string();
            next.generation = next.generation.saturating_add(1);
            tables[index] = next.clone();
            drop(tables);

            let commit_log = crate::table_catalog::CommitLogEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                commit_id: request.commit_id,
                idempotency_key: request.idempotency_key,
                table_id: current.table_id,
                operation: request.operation,
                expected_version_token: request.expected_version_token,
                new_version_token: next.version_token.clone(),
                previous_metadata_location: request.expected_metadata_location,
                new_metadata_location: request.new_metadata_location,
                requirements: request.requirements,
                status: crate::table_catalog::CommitLogStatus::Committed,
                writer: request.writer,
                created_at: None,
                updated_at: None,
            };
            self.commits.lock().await.push(commit_log.clone());

            Ok(crate::table_catalog::TableCommitResult { table: next, commit_log })
        }

        async fn drop_table(
            &self,
            table_bucket: &str,
            namespace: &str,
            table: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<()> {
            self.tables
                .lock()
                .await
                .retain(|entry| !(entry.table_bucket == table_bucket && entry.namespace == namespace && entry.table == table));
            Ok(())
        }

        async fn get_commit_by_id(
            &self,
            _table_bucket: &str,
            _table_id: &str,
            _commit_id: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::CommitLogEntry>> {
            Ok(None)
        }

        async fn get_commit_by_idempotency_key(
            &self,
            _table_bucket: &str,
            _table_id: &str,
            _idempotency_key: &str,
        ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::CommitLogEntry>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn ensure_table_bucket_entry_seeds_enabled_bucket_before_namespace_create() {
        let store = TestTableCatalogStore::default();

        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        let table_bucket = store
            .get_table_bucket("warehouse")
            .await
            .expect("table bucket lookup should succeed")
            .expect("table bucket entry should exist");

        assert_eq!(table_bucket.table_bucket, "warehouse");
        assert_eq!(table_bucket.catalog_type, crate::table_catalog::TABLE_BUCKET_CATALOG_TYPE);
        assert_eq!(table_bucket.warehouse_root, "s3://warehouse/");
    }

    #[tokio::test]
    async fn ensure_table_bucket_entry_rejects_bucket_without_table_marker() {
        let store = TestTableCatalogStore::default();

        assert!(ensure_table_bucket_entry(&store, "warehouse", false).await.is_err());
        assert!(
            store
                .get_table_bucket("warehouse")
                .await
                .expect("table bucket lookup should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn namespace_helpers_call_catalog_store() {
        let store = TestTableCatalogStore::default();
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        let create = create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::from([("owner".to_string(), "lakehouse".to_string())]),
            },
            true,
        )
        .await
        .expect("namespace should be created");

        assert_eq!(create.namespace, vec!["analytics".to_string()]);
        assert_eq!(create.properties.get("owner").map(String::as_str), Some("lakehouse"));

        let list = list_namespaces_response(&store, "warehouse")
            .await
            .expect("namespace list should load");
        assert_eq!(list.namespaces, vec![vec!["analytics".to_string()]]);

        drop_namespace_in_store(&store, "warehouse", "analytics")
            .await
            .expect("namespace should drop");
        let list = list_namespaces_response(&store, "warehouse")
            .await
            .expect("namespace list should load after drop");
        assert!(list.namespaces.is_empty());
    }

    #[tokio::test]
    async fn table_helpers_call_catalog_store() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");

        let metadata_location =
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        metadata_backend
            .put_json(
                "warehouse",
                metadata_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "table-uuid",
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;
        let register = register_table_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: metadata_location.to_string(),
                overwrite: false,
            },
            true,
        )
        .await
        .expect("table should register");

        assert_eq!(register.metadata_location, metadata_location);
        assert_eq!(register.metadata["format-version"], 2);

        let list = list_tables_response(&store, "warehouse", &namespace)
            .await
            .expect("table list should load");
        assert_eq!(list.identifiers[0].name, "events");

        let load = load_table_response(&store, &metadata_backend, "warehouse", &namespace, "events")
            .await
            .expect("table should load");
        assert_eq!(load.metadata_location, metadata_location);
        assert_eq!(load.metadata["table-uuid"], "table-uuid");

        let current = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let next_metadata_location =
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
        metadata_backend
            .put_json(
                "warehouse",
                next_metadata_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "table-uuid",
                    "location": "s3://warehouse/tables/table-id",
                    "last-sequence-number": 2
                }),
            )
            .await;
        let commit = commit_table_response(
            &store,
            "warehouse",
            &namespace,
            "events",
            RestCommitTableRequest {
                commit_id: "commit-1".to_string(),
                idempotency_key: Some("retry-1".to_string()),
                operation: "append".to_string(),
                expected_version_token: current.version_token.clone(),
                expected_metadata_location: current.metadata_location.clone(),
                new_metadata_location: next_metadata_location.to_string(),
                requirements: Vec::new(),
                writer: Some("pyiceberg".to_string()),
            },
        )
        .await
        .expect("table commit should succeed");
        assert_eq!(commit.metadata_location, next_metadata_location);
        assert_eq!(commit.version_token, "token-committed");
        assert_eq!(commit.generation, current.generation + 1);
        assert_eq!(commit.commit_id, "commit-1");

        let committed = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("committed table lookup should succeed")
            .expect("committed table should exist");
        assert_eq!(committed.metadata_location, next_metadata_location);

        drop_table_in_store(&store, "warehouse", &namespace, "events")
            .await
            .expect("table should drop");
        assert!(
            load_table_response(&store, &metadata_backend, "warehouse", &namespace, "events")
                .await
                .is_err()
        );
    }
}
