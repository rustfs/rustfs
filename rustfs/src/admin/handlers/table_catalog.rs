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
    auth::{AdminResourceScope, validate_admin_request, validate_admin_request_with_bucket_object},
    router::{AdminOperation, Operation, S3Router},
};
use crate::app::context::resolve_object_store_handle;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{RemoteAddr, TABLE_CATALOG_COMPAT_PREFIX, TABLE_CATALOG_PREFIX};
use crate::table_catalog::{DEFAULT_WAREHOUSE_ID, TableCatalogStore};
use http::{HeaderMap, HeaderValue, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_ecstore::{
    bucket::{metadata::table_catalog_path_hash, metadata_sys},
    store::ECStore,
};
use rustfs_iam::{manager::get_token_signing_key, sys::SESSION_POLICY_NAME};
use rustfs_policy::{
    auth::get_new_credentials_with_metadata,
    policy::{
        Policy,
        action::{Action, AdminAction},
    },
};
use s3s::{Body, S3Request, S3Response, S3Result, header::CONTENT_TYPE, s3_error};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, HashMap};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

const JSON_CONTENT_TYPE: &str = "application/json";
const ENV_TABLE_CATALOG_CREDENTIAL_VENDING: &str = "RUSTFS_TABLE_CATALOG_CREDENTIAL_VENDING";
const ENV_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS: &str = "RUSTFS_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS";
const DEFAULT_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS: i64 = 15 * 60;
const MIN_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS: i64 = 60;
const MAX_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS: i64 = 60 * 60;
const WAREHOUSE_PROPERTY: &str = "warehouse";
const CATALOG_ENDPOINT_PREFIX_CONFIG_KEY: &str = "rustfs.catalog-endpoint-prefix";
const CATALOG_COMPAT_ENDPOINT_PREFIX_CONFIG_KEY: &str = "rustfs.catalog-compat-endpoint-prefix";
const CREDENTIAL_VENDING_CONFIG_KEY: &str = "rustfs.credential-vending";
const CREDENTIAL_VENDING_REASON_CONFIG_KEY: &str = "rustfs.credential-vending-reason";
const CREDENTIAL_SCOPE_CONFIG_KEY: &str = "rustfs.credential-scope";
const CREDENTIAL_SCOPE_PREFIX_CONFIG_KEY: &str = "rustfs.credential-scope-prefix";
const CREDENTIAL_MODE_CONFIG_KEY: &str = "rustfs.credential-mode";
const CREDENTIAL_EXPIRATION_CONFIG_KEY: &str = "rustfs.credential-expiration-unix-seconds";
const CREDENTIAL_VENDING_UNSUPPORTED: &str = "unsupported";
const CREDENTIAL_VENDING_SUPPORTED: &str = "supported";
const CREDENTIAL_VENDING_UNSUPPORTED_REASON: &str = "temporary-credentials-not-implemented";
const CREDENTIAL_SCOPE_WAREHOUSE_PREFIX: &str = "warehouse-prefix";
const CREDENTIAL_SCOPE_TABLE_PREFIX: &str = "table-prefix";
const CREDENTIAL_MODE_CLIENT_PROVIDED: &str = "client-provided-s3-credentials-required";
const CREDENTIAL_MODE_CATALOG_VENDED: &str = "catalog-vended-temporary-credentials";
const S3_ACCESS_KEY_ID_CONFIG_KEY: &str = "s3.access-key-id";
const S3_SECRET_ACCESS_KEY_CONFIG_KEY: &str = "s3.secret-access-key";
const S3_SESSION_TOKEN_CONFIG_KEY: &str = "s3.session-token";
const TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT: &str = "namespaces";
const TABLE_CATALOG_TABLE_RESOURCE_ROOT: &str = "tables";
const TABLE_CATALOG_ENDPOINTS: &[&str] = &[
    "GET /v1/{prefix}/namespaces",
    "POST /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "DELETE /v1/{prefix}/namespaces/{namespace}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/register",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "PUT /buckets/{warehouse}",
    "GET /buckets/{warehouse}",
    "GET /{warehouse}/namespaces",
    "POST /{warehouse}/namespaces",
    "GET /{warehouse}/namespaces/{namespace}",
    "HEAD /{warehouse}/namespaces/{namespace}",
    "DELETE /{warehouse}/namespaces/{namespace}",
    "GET /{warehouse}/namespaces/{namespace}/tables",
    "POST /{warehouse}/namespaces/{namespace}/tables",
    "POST /{warehouse}/namespaces/{namespace}/register",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}",
    "HEAD /{warehouse}/namespaces/{namespace}/tables/{table}",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/credentials",
    "POST /{warehouse}/namespaces/{namespace}/tables/{table}",
    "DELETE /{warehouse}/namespaces/{namespace}/tables/{table}",
    "POST /{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/metadata",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location",
    "PUT /{warehouse}/namespaces/{namespace}/tables/{table}/metadata-location",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/config",
    "PUT /{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/config",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/maintenance/jobs/{job}",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/export",
    "POST /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/import",
    "GET /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/diagnostics",
    "POST /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/rollback",
];

static GET_CONFIG_HANDLER: GetCatalogConfigHandler = GetCatalogConfigHandler {};
static ENABLE_TABLE_BUCKET_HANDLER: EnableTableBucketHandler = EnableTableBucketHandler {};
static GET_TABLE_BUCKET_HANDLER: GetTableBucketHandler = GetTableBucketHandler {};
static LIST_NAMESPACES_HANDLER: RestListNamespacesHandler = RestListNamespacesHandler {};
static CREATE_NAMESPACE_HANDLER: RestCreateNamespaceHandler = RestCreateNamespaceHandler {};
static GET_NAMESPACE_HANDLER: RestGetNamespaceHandler = RestGetNamespaceHandler {};
static NAMESPACE_EXISTS_HANDLER: RestNamespaceExistsHandler = RestNamespaceExistsHandler {};
static DROP_NAMESPACE_HANDLER: RestDropNamespaceHandler = RestDropNamespaceHandler {};
static LIST_TABLES_HANDLER: RestListTablesHandler = RestListTablesHandler {};
static CREATE_TABLE_HANDLER: RestCreateTableHandler = RestCreateTableHandler {};
static REGISTER_TABLE_HANDLER: RestRegisterTableHandler = RestRegisterTableHandler {};
static LOAD_TABLE_HANDLER: RestLoadTableHandler = RestLoadTableHandler {};
static TABLE_EXISTS_HANDLER: RestTableExistsHandler = RestTableExistsHandler {};
static LOAD_CREDENTIALS_HANDLER: RestLoadCredentialsHandler = RestLoadCredentialsHandler {};
static COMMIT_TABLE_HANDLER: RestCommitTableHandler = RestCommitTableHandler {};
static DROP_TABLE_HANDLER: RestDropTableHandler = RestDropTableHandler {};
static GET_TABLE_METADATA_LOCATION_HANDLER: GetTableMetadataLocationHandler = GetTableMetadataLocationHandler {};
static UPDATE_TABLE_METADATA_LOCATION_HANDLER: UpdateTableMetadataLocationHandler = UpdateTableMetadataLocationHandler {};
static TABLE_METADATA_MAINTENANCE_HANDLER: RestTableMetadataMaintenanceHandler = RestTableMetadataMaintenanceHandler {};
static GET_TABLE_MAINTENANCE_CONFIG_HANDLER: GetTableMaintenanceConfigHandler = GetTableMaintenanceConfigHandler {};
static PUT_TABLE_MAINTENANCE_CONFIG_HANDLER: PutTableMaintenanceConfigHandler = PutTableMaintenanceConfigHandler {};
static GET_TABLE_MAINTENANCE_JOB_HANDLER: GetTableMaintenanceJobHandler = GetTableMaintenanceJobHandler {};
static EXPORT_TABLE_CATALOG_HANDLER: ExportTableCatalogHandler = ExportTableCatalogHandler {};
static IMPORT_TABLE_CATALOG_HANDLER: ImportTableCatalogHandler = ImportTableCatalogHandler {};
static GET_TABLE_CATALOG_DIAGNOSTICS_HANDLER: GetTableCatalogDiagnosticsHandler = GetTableCatalogDiagnosticsHandler {};
static ROLLBACK_TABLE_CATALOG_HANDLER: RollbackTableCatalogHandler = RollbackTableCatalogHandler {};

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
    #[serde(default)]
    location: Option<String>,
    schema: serde_json::Value,
    #[serde(default, rename = "partition-spec")]
    partition_spec: Option<serde_json::Value>,
    #[serde(default, rename = "write-order")]
    write_order: Option<serde_json::Value>,
    #[serde(default, rename = "stage-create")]
    stage_create: bool,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestCommitTableRequest {
    #[serde(default, rename = "identifier")]
    _identifier: Option<serde_json::Value>,
    #[serde(default, rename = "commit-id")]
    commit_id: Option<String>,
    #[serde(default, rename = "idempotency-key")]
    idempotency_key: Option<String>,
    #[serde(default)]
    operation: Option<String>,
    #[serde(default, rename = "expected-version-token")]
    expected_version_token: Option<String>,
    #[serde(default, rename = "expected-metadata-location")]
    expected_metadata_location: Option<String>,
    #[serde(default, rename = "new-metadata-location")]
    new_metadata_location: Option<String>,
    #[serde(default)]
    requirements: Vec<serde_json::Value>,
    #[serde(default)]
    updates: Vec<serde_json::Value>,
    #[serde(default)]
    writer: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableMetadataMaintenanceRequest {
    #[serde(default, rename = "retain-recent-metadata-files")]
    retain_recent_metadata_files: usize,
    #[serde(default)]
    delete: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateTableMetadataLocationRequest {
    #[serde(rename = "metadata-location", alias = "metadataLocation")]
    metadata_location: String,
    #[serde(rename = "version-token", alias = "versionToken")]
    version_token: String,
    #[serde(default, rename = "commit-id", alias = "commitId")]
    commit_id: Option<String>,
    #[serde(default, rename = "idempotency-key", alias = "idempotencyKey")]
    idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogImportRequest {
    #[serde(rename = "metadata-location", alias = "metadataLocation")]
    metadata_location: String,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RollbackTableRequest {
    #[serde(rename = "metadata-location", alias = "metadataLocation")]
    metadata_location: String,
    #[serde(rename = "version-token", alias = "versionToken")]
    version_token: String,
    #[serde(default, rename = "commit-id", alias = "commitId")]
    commit_id: Option<String>,
    #[serde(default, rename = "idempotency-key", alias = "idempotencyKey")]
    idempotency_key: Option<String>,
}

#[derive(Debug, Serialize)]
struct TableBucketResponse {
    #[serde(rename = "table-bucket")]
    table_bucket: String,
    enabled: bool,
    #[serde(rename = "catalog-type")]
    catalog_type: String,
    warehouse: String,
    #[serde(rename = "warehouse-location")]
    warehouse_location: String,
    #[serde(rename = "catalog-uri")]
    catalog_uri: String,
    #[serde(rename = "compat-catalog-uri")]
    compat_catalog_uri: String,
    #[serde(rename = "credential-vending")]
    credential_vending: &'static str,
    #[serde(rename = "credential-scope")]
    credential_scope: &'static str,
    #[serde(rename = "credential-scope-prefix")]
    credential_scope_prefix: String,
    #[serde(rename = "catalog-entry-present")]
    catalog_entry_present: bool,
    properties: BTreeMap<String, String>,
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
struct RestStorageCredential {
    prefix: String,
    config: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct TableCredentialScope {
    scope_prefix: String,
    object_prefix: String,
}

#[derive(Debug, Clone)]
struct TableCredentialIssueRequest<'a> {
    entry: &'a crate::table_catalog::TableEntry,
    principal: Option<&'a rustfs_credentials::Credentials>,
    scope_prefix: String,
    object_prefix: String,
}

#[derive(Debug, Clone)]
struct IssuedTableCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    expiration: OffsetDateTime,
}

#[async_trait::async_trait]
trait TableCredentialIssuer: Sync {
    fn enabled(&self) -> bool {
        true
    }

    async fn issue_table_credentials(&self, request: TableCredentialIssueRequest<'_>)
    -> S3Result<Option<IssuedTableCredentials>>;
}

#[cfg(test)]
struct DisabledTableCredentialIssuer;

#[cfg(test)]
#[async_trait::async_trait]
impl TableCredentialIssuer for DisabledTableCredentialIssuer {
    fn enabled(&self) -> bool {
        false
    }

    async fn issue_table_credentials(
        &self,
        _request: TableCredentialIssueRequest<'_>,
    ) -> S3Result<Option<IssuedTableCredentials>> {
        Ok(None)
    }
}

struct IamTableCredentialIssuer {
    enabled: bool,
    ttl_seconds: i64,
}

impl IamTableCredentialIssuer {
    fn from_env() -> Self {
        Self {
            enabled: table_credential_vending_enabled(),
            ttl_seconds: table_credential_ttl_seconds(),
        }
    }
}

#[async_trait::async_trait]
impl TableCredentialIssuer for IamTableCredentialIssuer {
    fn enabled(&self) -> bool {
        self.enabled
    }

    async fn issue_table_credentials(
        &self,
        request: TableCredentialIssueRequest<'_>,
    ) -> S3Result<Option<IssuedTableCredentials>> {
        if !self.enabled {
            return Ok(None);
        }

        let Some(principal) = request.principal else {
            return Err(s3_error!(InvalidRequest, "authentication required for table credentials"));
        };
        if principal.is_temp() || principal.is_service_account() {
            return Err(s3_error!(
                AccessDenied,
                "table credential vending does not allow chained temporary credentials"
            ));
        }

        let policy = table_credential_session_policy(&request.entry.table_bucket, &request.object_prefix)?;
        let policy_buf = serde_json::to_vec(&policy)
            .map_err(|err| s3_error!(InternalError, "failed to serialize table credential session policy: {}", err))?;
        let expiration = OffsetDateTime::now_utc().saturating_add(Duration::seconds(self.ttl_seconds));
        let mut claims: HashMap<String, serde_json::Value> = principal.claims.clone().unwrap_or_default();
        claims.insert(
            "exp".to_string(),
            serde_json::Value::Number(serde_json::Number::from(expiration.unix_timestamp())),
        );
        claims.insert("parent".to_string(), serde_json::Value::String(principal.access_key.clone()));
        claims.insert(
            SESSION_POLICY_NAME.to_string(),
            serde_json::Value::String(base64_simd::URL_SAFE_NO_PAD.encode_to_string(&policy_buf)),
        );
        claims.insert(
            "rustfs:table-bucket".to_string(),
            serde_json::Value::String(request.entry.table_bucket.clone()),
        );
        claims.insert("rustfs:table-id".to_string(), serde_json::Value::String(request.entry.table_id.clone()));
        claims.insert(
            "rustfs:credential-scope-prefix".to_string(),
            serde_json::Value::String(request.scope_prefix.clone()),
        );

        let secret = get_token_signing_key().ok_or_else(|| s3_error!(InternalError, "token signing key not initialized"))?;
        let mut credential = get_new_credentials_with_metadata(&claims, &secret)
            .map_err(|err| s3_error!(InternalError, "failed to generate table credentials: {}", err))?;
        bind_table_credential_parent(&mut credential, principal);

        let iam_store = rustfs_iam::get().map_err(|_| s3_error!(InternalError, "iam not init"))?;
        iam_store
            .set_temp_user(&credential.access_key, &credential, None)
            .await
            .map_err(|_| s3_error!(InternalError, "failed to store table credentials"))?;

        Ok(Some(IssuedTableCredentials {
            access_key_id: credential.access_key,
            secret_access_key: credential.secret_key,
            session_token: credential.session_token,
            expiration,
        }))
    }
}

fn bind_table_credential_parent(credential: &mut rustfs_credentials::Credentials, principal: &rustfs_credentials::Credentials) {
    credential.parent_user = principal.access_key.clone();
}

#[derive(Debug, Serialize)]
struct RestLoadTableResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    metadata: serde_json::Value,
    config: BTreeMap<String, String>,
    #[serde(rename = "storage-credentials")]
    storage_credentials: Vec<RestStorageCredential>,
}

#[derive(Debug, Serialize)]
struct RestLoadCredentialsResponse {
    #[serde(rename = "storage-credentials")]
    storage_credentials: Vec<RestStorageCredential>,
}

#[derive(Debug, Serialize)]
struct RestCommitTableResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    metadata: serde_json::Value,
    #[serde(rename = "version-token")]
    version_token: String,
    generation: u64,
    #[serde(rename = "commit-id")]
    commit_id: String,
}

#[derive(Debug, Serialize)]
struct TableMetadataLocationResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    #[serde(rename = "version-token")]
    version_token: String,
    generation: u64,
    #[serde(rename = "warehouse-location")]
    warehouse_location: String,
}

pub fn register_table_catalog_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    for prefix in [TABLE_CATALOG_PREFIX, TABLE_CATALOG_COMPAT_PREFIX] {
        register_table_catalog_prefix_routes(r, prefix)?;
    }

    Ok(())
}

fn register_table_catalog_prefix_routes(r: &mut S3Router<AdminOperation>, prefix: &str) -> std::io::Result<()> {
    r.insert(Method::GET, format!("{prefix}/config").as_str(), AdminOperation(&GET_CONFIG_HANDLER))?;
    r.insert(
        Method::PUT,
        format!("{prefix}/buckets/{{warehouse}}").as_str(),
        AdminOperation(&ENABLE_TABLE_BUCKET_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/buckets/{{warehouse}}").as_str(),
        AdminOperation(&GET_TABLE_BUCKET_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces").as_str(),
        AdminOperation(&LIST_NAMESPACES_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces").as_str(),
        AdminOperation(&CREATE_NAMESPACE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}").as_str(),
        AdminOperation(&GET_NAMESPACE_HANDLER),
    )?;
    r.insert(
        Method::HEAD,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}").as_str(),
        AdminOperation(&NAMESPACE_EXISTS_HANDLER),
    )?;
    r.insert(
        Method::DELETE,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}").as_str(),
        AdminOperation(&DROP_NAMESPACE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables").as_str(),
        AdminOperation(&LIST_TABLES_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables").as_str(),
        AdminOperation(&CREATE_TABLE_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/register").as_str(),
        AdminOperation(&REGISTER_TABLE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}").as_str(),
        AdminOperation(&LOAD_TABLE_HANDLER),
    )?;
    r.insert(
        Method::HEAD,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}").as_str(),
        AdminOperation(&TABLE_EXISTS_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/credentials").as_str(),
        AdminOperation(&LOAD_CREDENTIALS_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}").as_str(),
        AdminOperation(&COMMIT_TABLE_HANDLER),
    )?;
    r.insert(
        Method::DELETE,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}").as_str(),
        AdminOperation(&DROP_TABLE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/metadata-location").as_str(),
        AdminOperation(&GET_TABLE_METADATA_LOCATION_HANDLER),
    )?;
    r.insert(
        Method::PUT,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/metadata-location").as_str(),
        AdminOperation(&UPDATE_TABLE_METADATA_LOCATION_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/maintenance/metadata").as_str(),
        AdminOperation(&TABLE_METADATA_MAINTENANCE_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/maintenance/config").as_str(),
        AdminOperation(&GET_TABLE_MAINTENANCE_CONFIG_HANDLER),
    )?;
    r.insert(
        Method::PUT,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/maintenance/config").as_str(),
        AdminOperation(&PUT_TABLE_MAINTENANCE_CONFIG_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/maintenance/jobs/{{job}}").as_str(),
        AdminOperation(&GET_TABLE_MAINTENANCE_JOB_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/catalog/export").as_str(),
        AdminOperation(&EXPORT_TABLE_CATALOG_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/catalog/import").as_str(),
        AdminOperation(&IMPORT_TABLE_CATALOG_HANDLER),
    )?;
    r.insert(
        Method::GET,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/catalog/diagnostics").as_str(),
        AdminOperation(&GET_TABLE_CATALOG_DIAGNOSTICS_HANDLER),
    )?;
    r.insert(
        Method::POST,
        format!("{prefix}/{{warehouse}}/namespaces/{{namespace}}/tables/{{table}}/catalog/rollback").as_str(),
        AdminOperation(&ROLLBACK_TABLE_CATALOG_HANDLER),
    )?;

    Ok(())
}

fn catalog_config_response() -> CatalogConfigResponse {
    CatalogConfigResponse {
        defaults: BTreeMap::from([
            (WAREHOUSE_PROPERTY, DEFAULT_WAREHOUSE_ID),
            (CATALOG_ENDPOINT_PREFIX_CONFIG_KEY, TABLE_CATALOG_PREFIX),
            (CATALOG_COMPAT_ENDPOINT_PREFIX_CONFIG_KEY, TABLE_CATALOG_COMPAT_PREFIX),
        ]),
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

fn empty_response(status: StatusCode) -> S3Response<(StatusCode, Body)> {
    S3Response::new((status, Body::default()))
}

fn exists_status(exists: bool) -> StatusCode {
    if exists {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
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

#[derive(Debug, Clone)]
struct TableCatalogResource<'a> {
    warehouse: &'a str,
    namespace: Option<String>,
    table: Option<String>,
}

impl<'a> TableCatalogResource<'a> {
    fn warehouse(warehouse: &'a str) -> Self {
        Self {
            warehouse,
            namespace: None,
            table: None,
        }
    }

    fn namespace(warehouse: &'a str, namespace: &crate::table_catalog::Namespace) -> Self {
        Self {
            warehouse,
            namespace: Some(namespace.storage_id()),
            table: None,
        }
    }

    fn table(warehouse: &'a str, namespace: &crate::table_catalog::Namespace, table: &str) -> Self {
        Self {
            warehouse,
            namespace: Some(namespace.storage_id()),
            table: Some(table.to_string()),
        }
    }

    fn object_path(&self) -> Option<String> {
        match (&self.namespace, &self.table) {
            (Some(namespace), Some(table)) => Some(format!(
                "{TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT}/{namespace}/{TABLE_CATALOG_TABLE_RESOURCE_ROOT}/{table}"
            )),
            (Some(namespace), None) => Some(format!("{TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT}/{namespace}")),
            (None, _) => None,
        }
    }
}

async fn authorize_table_catalog_resource_request(
    req: &S3Request<Body>,
    resource: &TableCatalogResource<'_>,
    action: AdminAction,
) -> S3Result<()> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    let object_path = resource.object_path();
    validate_admin_request_with_bucket_object(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(action)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        AdminResourceScope::bucket_object(resource.warehouse, object_path.as_deref().unwrap_or("")),
    )
    .await
}

async fn table_catalog_request_principal(req: &S3Request<Body>) -> S3Result<rustfs_credentials::Credentials> {
    let Some(input_cred) = &req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };
    let (cred, _owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
    Ok(cred)
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

fn job_id_from_params(params: &Params<'_, '_>) -> S3Result<String> {
    let job = params.get("job").unwrap_or("");
    if job.is_empty() {
        return Err(s3_error!(InvalidRequest, "maintenance job id is required"));
    }
    Ok(job.to_string())
}

fn table_catalog_backend() -> S3Result<crate::table_catalog::EcStoreTableCatalogObjectBackend<ECStore>> {
    let store = resolve_object_store_handle().ok_or_else(|| s3_error!(InternalError, "object store not initialized"))?;
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

async fn enable_table_bucket_marker(bucket: &str) -> S3Result<()> {
    let marker = crate::table_catalog::table_bucket_marker_json()
        .map_err(|err| s3_error!(InternalError, "failed to serialize table bucket marker: {}", err))?;
    metadata_sys::update(bucket, crate::table_catalog::TABLE_BUCKET_MARKER_CONFIG, marker)
        .await
        .map(|_| ())
        .map_err(|err| s3_error!(InvalidRequest, "failed to enable table bucket {bucket}: {}", err))
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

async fn table_bucket_response<S>(store: &S, bucket: &str, enabled: bool) -> S3Result<TableBucketResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let entry = store.get_table_bucket(bucket).await.map_err(catalog_store_error)?;
    let (catalog_type, warehouse_location, properties, catalog_entry_present) = match entry {
        Some(entry) => (entry.catalog_type, entry.warehouse_root, entry.properties, true),
        None => (
            crate::table_catalog::TABLE_BUCKET_CATALOG_TYPE.to_string(),
            format!("s3://{bucket}/"),
            BTreeMap::new(),
            false,
        ),
    };

    Ok(TableBucketResponse {
        table_bucket: bucket.to_string(),
        enabled,
        catalog_type,
        warehouse: bucket.to_string(),
        warehouse_location: warehouse_location.clone(),
        catalog_uri: format!("{TABLE_CATALOG_PREFIX}/{bucket}"),
        compat_catalog_uri: format!("{TABLE_CATALOG_COMPAT_PREFIX}/{bucket}"),
        credential_vending: CREDENTIAL_VENDING_UNSUPPORTED,
        credential_scope: CREDENTIAL_SCOPE_WAREHOUSE_PREFIX,
        credential_scope_prefix: warehouse_location,
        catalog_entry_present,
        properties,
    })
}

async fn enable_table_bucket_response<S>(store: &S, bucket: &str) -> S3Result<TableBucketResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    ensure_table_bucket_entry(store, bucket, true).await?;
    enable_table_bucket_marker(bucket).await?;
    table_bucket_response(store, bucket, true).await
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

fn table_credential_vending_enabled() -> bool {
    std::env::var(ENV_TABLE_CATALOG_CREDENTIAL_VENDING)
        .ok()
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "on" | "enabled"))
        .unwrap_or(false)
}

fn table_credential_ttl_seconds() -> i64 {
    std::env::var(ENV_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .map(|seconds| seconds.clamp(MIN_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS, MAX_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS))
        .unwrap_or(DEFAULT_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS)
}

fn table_credential_scope(entry: &crate::table_catalog::TableEntry) -> S3Result<TableCredentialScope> {
    let location = entry
        .warehouse_location
        .strip_prefix("s3://")
        .ok_or_else(|| s3_error!(InvalidRequest, "table warehouse location must be an s3 URI"))?;
    let (bucket, object_prefix) = location
        .split_once('/')
        .ok_or_else(|| s3_error!(InvalidRequest, "table warehouse location must include an object prefix"))?;
    if bucket != entry.table_bucket {
        return Err(s3_error!(InvalidRequest, "table warehouse location must be inside the table bucket"));
    }
    let object_prefix = normalize_table_credential_object_prefix(object_prefix)?;
    Ok(TableCredentialScope {
        scope_prefix: format!("s3://{bucket}/{object_prefix}"),
        object_prefix,
    })
}

fn normalize_table_credential_object_prefix(object_prefix: &str) -> S3Result<String> {
    let object_prefix = object_prefix.strip_suffix('/').unwrap_or(object_prefix);
    if object_prefix.is_empty() {
        return Err(s3_error!(InvalidRequest, "table credential scope prefix is empty"));
    }
    if object_prefix.contains('\\') {
        return Err(s3_error!(
            InvalidRequest,
            "table credential scope prefix contains an invalid path separator"
        ));
    }
    if object_prefix
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(s3_error!(
            InvalidRequest,
            "table credential scope prefix contains an invalid path segment"
        ));
    }

    let mut normalized = object_prefix.to_string();
    normalized.push('/');
    Ok(normalized)
}

fn table_credential_session_policy(bucket: &str, object_prefix: &str) -> S3Result<Policy> {
    let object_prefix = normalize_table_credential_object_prefix(object_prefix)?;
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts"
                ],
                "Resource": [
                    format!("arn:aws:s3:::{bucket}/{object_prefix}*")
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetBucketLocation"
                ],
                "Resource": [
                    format!("arn:aws:s3:::{bucket}")
                ]
            }
        ]
    });
    let data = serde_json::to_vec(&policy)
        .map_err(|err| s3_error!(InternalError, "failed to serialize table credential policy: {}", err))?;
    Policy::parse_config(&data).map_err(|err| s3_error!(InvalidRequest, "invalid table credential policy: {}", err))
}

fn storage_credential_from_issued(scope: TableCredentialScope, issued: IssuedTableCredentials) -> RestStorageCredential {
    let mut config = BTreeMap::new();
    config.insert(S3_ACCESS_KEY_ID_CONFIG_KEY.to_string(), issued.access_key_id);
    config.insert(S3_SECRET_ACCESS_KEY_CONFIG_KEY.to_string(), issued.secret_access_key);
    config.insert(S3_SESSION_TOKEN_CONFIG_KEY.to_string(), issued.session_token);
    config.insert(CREDENTIAL_VENDING_CONFIG_KEY.to_string(), CREDENTIAL_VENDING_SUPPORTED.to_string());
    config.insert(CREDENTIAL_MODE_CONFIG_KEY.to_string(), CREDENTIAL_MODE_CATALOG_VENDED.to_string());
    config.insert(CREDENTIAL_SCOPE_CONFIG_KEY.to_string(), CREDENTIAL_SCOPE_TABLE_PREFIX.to_string());
    config.insert(CREDENTIAL_SCOPE_PREFIX_CONFIG_KEY.to_string(), scope.scope_prefix.clone());
    config.insert(
        CREDENTIAL_EXPIRATION_CONFIG_KEY.to_string(),
        issued.expiration.unix_timestamp().to_string(),
    );
    RestStorageCredential {
        prefix: scope.scope_prefix,
        config,
    }
}

fn load_table_response_from_entry(entry: crate::table_catalog::TableEntry, metadata: serde_json::Value) -> RestLoadTableResponse {
    let mut config = BTreeMap::new();
    let warehouse_location = entry.warehouse_location.clone();
    config.insert("warehouse-location".to_string(), warehouse_location.clone());
    config.insert(CREDENTIAL_VENDING_CONFIG_KEY.to_string(), CREDENTIAL_VENDING_UNSUPPORTED.to_string());
    config.insert(
        CREDENTIAL_VENDING_REASON_CONFIG_KEY.to_string(),
        CREDENTIAL_VENDING_UNSUPPORTED_REASON.to_string(),
    );
    config.insert(CREDENTIAL_SCOPE_CONFIG_KEY.to_string(), CREDENTIAL_SCOPE_TABLE_PREFIX.to_string());
    config.insert(CREDENTIAL_SCOPE_PREFIX_CONFIG_KEY.to_string(), warehouse_location);
    config.insert(CREDENTIAL_MODE_CONFIG_KEY.to_string(), CREDENTIAL_MODE_CLIENT_PROVIDED.to_string());

    RestLoadTableResponse {
        metadata_location: entry.metadata_location,
        metadata,
        config,
        storage_credentials: Vec::new(),
    }
}

async fn load_credentials_response_from_entry(
    entry: &crate::table_catalog::TableEntry,
    issuer: &dyn TableCredentialIssuer,
    principal: Option<&rustfs_credentials::Credentials>,
) -> S3Result<RestLoadCredentialsResponse> {
    if !issuer.enabled() {
        return Ok(RestLoadCredentialsResponse {
            storage_credentials: Vec::new(),
        });
    }
    let scope = table_credential_scope(entry)?;
    let request = TableCredentialIssueRequest {
        entry,
        principal,
        scope_prefix: scope.scope_prefix.clone(),
        object_prefix: scope.object_prefix.clone(),
    };
    let storage_credentials = match issuer.issue_table_credentials(request).await? {
        Some(issued) => vec![storage_credential_from_issued(scope, issued)],
        None => Vec::new(),
    };
    Ok(RestLoadCredentialsResponse { storage_credentials })
}

fn commit_table_response_from_result(
    result: crate::table_catalog::TableCommitResult,
    metadata: serde_json::Value,
) -> RestCommitTableResponse {
    RestCommitTableResponse {
        metadata_location: result.table.metadata_location,
        metadata,
        version_token: result.table.version_token,
        generation: result.table.generation,
        commit_id: result.commit_log.commit_id,
    }
}

fn table_metadata_location_response_from_entry(entry: crate::table_catalog::TableEntry) -> TableMetadataLocationResponse {
    TableMetadataLocationResponse {
        metadata_location: entry.metadata_location,
        version_token: entry.version_token,
        generation: entry.generation,
        warehouse_location: entry.warehouse_location,
    }
}

fn table_commit_request_from_rest_request(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: RestCommitTableRequest,
) -> S3Result<crate::table_catalog::TableCommitRequest> {
    Ok(crate::table_catalog::TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        commit_id: request.commit_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
        idempotency_key: request.idempotency_key,
        operation: request.operation.unwrap_or_else(|| "commit".to_string()),
        expected_version_token: request
            .expected_version_token
            .ok_or_else(|| s3_error!(InvalidRequest, "legacy commit requires expected-version-token"))?,
        expected_metadata_location: request
            .expected_metadata_location
            .ok_or_else(|| s3_error!(InvalidRequest, "legacy commit requires expected-metadata-location"))?,
        new_metadata_location: request
            .new_metadata_location
            .ok_or_else(|| s3_error!(InvalidRequest, "legacy commit requires new-metadata-location"))?,
        requirements: request.requirements,
        writer: request.writer,
    })
}

fn validate_table_location_in_bucket(bucket: &str, location: &str) -> S3Result<()> {
    if !location.starts_with(&format!("s3://{bucket}/")) {
        return Err(s3_error!(InvalidRequest, "table location must be inside the table bucket"));
    }
    Ok(())
}

fn metadata_table_uuid(metadata: &serde_json::Value) -> S3Result<&str> {
    metadata
        .get("table-uuid")
        .and_then(serde_json::Value::as_str)
        .filter(|uuid| !uuid.is_empty())
        .ok_or_else(|| s3_error!(InvalidRequest, "table metadata is missing table-uuid"))
}

fn metadata_format_version(metadata: &serde_json::Value) -> S3Result<u16> {
    let version = metadata
        .get("format-version")
        .and_then(serde_json::Value::as_u64)
        .filter(|version| *version > 0)
        .ok_or_else(|| s3_error!(InvalidRequest, "table metadata is missing format-version"))?;
    u16::try_from(version).map_err(|_| s3_error!(InvalidRequest, "table metadata format-version is too large"))
}

fn metadata_table_location(metadata: &serde_json::Value) -> S3Result<&str> {
    metadata
        .get("location")
        .and_then(serde_json::Value::as_str)
        .filter(|location| !location.is_empty())
        .ok_or_else(|| s3_error!(InvalidRequest, "table metadata is missing location"))
}

fn validate_metadata_table_location_in_bucket(bucket: &str, metadata: &serde_json::Value) -> S3Result<()> {
    let location = metadata_table_location(metadata)?;
    validate_table_location_in_bucket(bucket, location)
}

fn validate_metadata_matches_current_metadata(
    current_metadata: &serde_json::Value,
    target_metadata: &serde_json::Value,
) -> S3Result<()> {
    let expected_table_uuid = metadata_table_uuid(current_metadata)?;
    metadata_format_version(current_metadata)?;
    let target_table_uuid = metadata_table_uuid(target_metadata)?;
    metadata_format_version(target_metadata)?;
    if target_table_uuid != expected_table_uuid {
        return Err(s3_error!(
            InvalidRequest,
            "table metadata table-uuid does not match current table metadata"
        ));
    }
    Ok(())
}

fn adopt_registered_metadata_identity(
    entry: &mut crate::table_catalog::TableEntry,
    metadata: &serde_json::Value,
) -> S3Result<()> {
    entry.table_uuid = metadata_table_uuid(metadata)?.to_string();
    entry.format_version = metadata_format_version(metadata)?;
    entry.warehouse_location = metadata_table_location(metadata)?.to_string();
    Ok(())
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

fn table_entry_from_import_request(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: CatalogImportRequest,
) -> S3Result<crate::table_catalog::TableEntry> {
    let table = crate::table_catalog::IdentifierSegment::parse(table.to_string())
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
        properties: request.properties,
        created_at: None,
        updated_at: None,
    })
}

fn table_entry_from_create_table_request(
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    request: CreateTableRequest,
) -> S3Result<(crate::table_catalog::TableEntry, serde_json::Value)> {
    if request.stage_create {
        return Err(s3_error!(NotImplemented, "stage-create is not supported"));
    }

    let table = crate::table_catalog::IdentifierSegment::parse(request.name)
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let table_id = Uuid::new_v4().to_string();
    let table_uuid = Uuid::new_v4().to_string();
    let warehouse_location = request.location.unwrap_or_else(|| format!("s3://{bucket}/tables/{table_id}"));
    validate_table_location_in_bucket(bucket, &warehouse_location)?;
    let metadata_location = crate::table_catalog::default_table_metadata_file_path(namespace, &table, "00001.metadata.json");

    let mut entry = crate::table_catalog::TableEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.as_str().to_string(),
        table_id,
        table_uuid,
        format: "ICEBERG".to_string(),
        format_version: 2,
        warehouse_location,
        metadata_location,
        version_token: format!("token-{}", Uuid::new_v4()),
        generation: 1,
        state: crate::table_catalog::TableCatalogEntryState::Active,
        properties: request.properties,
        created_at: None,
        updated_at: None,
    };
    let metadata = initial_table_metadata_json(
        &entry,
        request.schema,
        request.partition_spec,
        request.write_order,
        entry.properties.clone(),
    )?;
    entry.format_version = metadata
        .get("format-version")
        .and_then(serde_json::Value::as_u64)
        .and_then(|version| u16::try_from(version).ok())
        .unwrap_or(2);
    Ok((entry, metadata))
}

fn initial_table_metadata_json(
    entry: &crate::table_catalog::TableEntry,
    mut schema: serde_json::Value,
    partition_spec: Option<serde_json::Value>,
    write_order: Option<serde_json::Value>,
    properties: BTreeMap<String, String>,
) -> S3Result<serde_json::Value> {
    let schema_object = schema
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "schema must be a JSON object"))?;
    schema_object
        .entry("schema-id".to_string())
        .or_insert_with(|| serde_json::Value::from(0));
    let schema_id = schema_object
        .get("schema-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "schema-id must be an integer"))?;
    let last_column_id = max_field_id(&schema);

    let mut spec = partition_spec.unwrap_or_else(|| {
        serde_json::json!({
            "spec-id": 0,
            "fields": []
        })
    });
    let spec_object = spec
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "partition-spec must be a JSON object"))?;
    spec_object
        .entry("spec-id".to_string())
        .or_insert_with(|| serde_json::Value::from(0));
    spec_object
        .entry("fields".to_string())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    let spec_id = spec_object
        .get("spec-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "partition spec-id must be an integer"))?;
    let last_partition_id = max_partition_field_id(&spec);

    let mut sort_order = write_order.unwrap_or_else(|| {
        serde_json::json!({
            "order-id": 0,
            "fields": []
        })
    });
    let sort_order_object = sort_order
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "write-order must be a JSON object"))?;
    sort_order_object
        .entry("order-id".to_string())
        .or_insert_with(|| serde_json::Value::from(0));
    sort_order_object
        .entry("fields".to_string())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    let sort_order_id = sort_order_object
        .get("order-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "sort order-id must be an integer"))?;

    Ok(serde_json::json!({
        "format-version": entry.format_version,
        "table-uuid": entry.table_uuid,
        "location": entry.warehouse_location,
        "last-sequence-number": 0,
        "last-updated-ms": current_time_millis(),
        "last-column-id": last_column_id,
        "schemas": [schema],
        "current-schema-id": schema_id,
        "partition-specs": [spec],
        "default-spec-id": spec_id,
        "last-partition-id": last_partition_id,
        "sort-orders": [sort_order],
        "default-sort-order-id": sort_order_id,
        "properties": properties,
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {}
    }))
}

fn current_time_millis() -> i64 {
    let now = OffsetDateTime::now_utc();
    now.unix_timestamp()
        .saturating_mul(1000)
        .saturating_add(i64::from(now.millisecond()))
}

fn max_field_id(value: &serde_json::Value) -> i64 {
    let mut max_id = 0;
    collect_max_field_id(value, &mut max_id);
    max_id
}

fn collect_max_field_id(value: &serde_json::Value, max_id: &mut i64) {
    match value {
        serde_json::Value::Object(object) => {
            if let Some(id) = object.get("id").and_then(serde_json::Value::as_i64) {
                *max_id = (*max_id).max(id);
            }
            for child in object.values() {
                collect_max_field_id(child, max_id);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                collect_max_field_id(child, max_id);
            }
        }
        _ => {}
    }
}

fn max_partition_field_id(value: &serde_json::Value) -> i64 {
    let mut max_id = 999;
    let Some(fields) = value.get("fields").and_then(serde_json::Value::as_array) else {
        return max_id;
    };
    for field in fields {
        if let Some(field_id) = field.get("field-id").and_then(serde_json::Value::as_i64) {
            max_id = max_id.max(field_id);
        }
    }
    max_id
}

fn standard_commit_ids(commit_id: Option<String>) -> (String, String) {
    match commit_id {
        Some(commit_id) => match Uuid::parse_str(&commit_id) {
            Ok(uuid) => {
                let commit_id = uuid.to_string();
                (commit_id.clone(), commit_id)
            }
            Err(_) => {
                let metadata_file_token = table_catalog_path_hash(&commit_id);
                (commit_id, metadata_file_token)
            }
        },
        None => {
            let commit_id = Uuid::new_v4().to_string();
            (commit_id.clone(), commit_id)
        }
    }
}

fn next_metadata_file_name(generation: u64, metadata_file_token: &str) -> String {
    format!("{generation:05}-{metadata_file_token}.metadata.json")
}

fn validate_table_commit_requirements(metadata: &serde_json::Value, requirements: &[serde_json::Value]) -> S3Result<()> {
    for requirement in requirements {
        let requirement_type = requirement
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| s3_error!(InvalidRequest, "commit requirement type is required"))?;
        match requirement_type {
            "assert-create" => {
                return Err(s3_error!(PreconditionFailed, "commit requirement failed: table already exists"));
            }
            "assert-table-uuid" => {
                let expected = requirement
                    .get("uuid")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| s3_error!(InvalidRequest, "assert-table-uuid requires uuid"))?;
                let actual = metadata
                    .get("table-uuid")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| s3_error!(InvalidRequest, "current table metadata is missing table-uuid"))?;
                if actual != expected {
                    return Err(s3_error!(PreconditionFailed, "commit requirement failed: table uuid changed"));
                }
            }
            "assert-current-schema-id" => {
                validate_i64_requirement(metadata, requirement, "current-schema-id", "current schema id")?;
            }
            "assert-default-spec-id" => {
                validate_i64_requirement(metadata, requirement, "default-spec-id", "default spec id")?;
            }
            "assert-default-sort-order-id" => {
                validate_i64_requirement(metadata, requirement, "default-sort-order-id", "default sort order id")?;
            }
            "assert-last-assigned-field-id" => {
                validate_i64_requirement_with_metadata_key(
                    metadata,
                    requirement,
                    "last-assigned-field-id",
                    "last-column-id",
                    "last assigned field id",
                )?;
            }
            "assert-last-assigned-partition-id" => {
                validate_i64_requirement_with_metadata_key(
                    metadata,
                    requirement,
                    "last-assigned-partition-id",
                    "last-partition-id",
                    "last assigned partition id",
                )?;
            }
            "assert-ref-snapshot-id" => validate_ref_snapshot_requirement(metadata, requirement)?,
            _ => return Err(s3_error!(NotImplemented, "unsupported commit requirement: {requirement_type}")),
        }
    }
    Ok(())
}

fn validate_i64_requirement(
    metadata: &serde_json::Value,
    requirement: &serde_json::Value,
    key: &str,
    label: &str,
) -> S3Result<()> {
    validate_i64_requirement_with_metadata_key(metadata, requirement, key, key, label)
}

fn validate_i64_requirement_with_metadata_key(
    metadata: &serde_json::Value,
    requirement: &serde_json::Value,
    requirement_key: &str,
    metadata_key: &str,
    label: &str,
) -> S3Result<()> {
    let expected = requirement
        .get(requirement_key)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "{requirement_key} must be an integer"))?;
    let actual = metadata
        .get(metadata_key)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "current table metadata is missing {metadata_key}"))?;
    if actual != expected {
        return Err(s3_error!(PreconditionFailed, "commit requirement failed: {label} changed"));
    }
    Ok(())
}

fn validate_ref_snapshot_requirement(metadata: &serde_json::Value, requirement: &serde_json::Value) -> S3Result<()> {
    let ref_name = requirement
        .get("ref")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "assert-ref-snapshot-id requires ref"))?;
    let refs = metadata.get("refs").and_then(serde_json::Value::as_object);
    let actual = refs
        .and_then(|refs| refs.get(ref_name))
        .and_then(|reference| reference.get("snapshot-id"))
        .and_then(serde_json::Value::as_i64);
    if requirement.get("snapshot-id").is_some_and(serde_json::Value::is_null) {
        if actual.is_some() {
            return Err(s3_error!(PreconditionFailed, "commit requirement failed: snapshot ref exists"));
        }
        return Ok(());
    }
    let expected = requirement
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "assert-ref-snapshot-id requires snapshot-id"))?;
    if actual != Some(expected) {
        return Err(s3_error!(PreconditionFailed, "commit requirement failed: snapshot ref changed"));
    }
    Ok(())
}

fn apply_table_commit_updates(
    mut metadata: serde_json::Value,
    updates: &[serde_json::Value],
    previous_metadata_location: &str,
) -> S3Result<serde_json::Value> {
    if !metadata.is_object() {
        return Err(s3_error!(InvalidRequest, "current table metadata must be a JSON object"));
    }

    for update in updates {
        let action = update
            .get("action")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| s3_error!(InvalidRequest, "table update action is required"))?;
        match action {
            "assign-uuid" => apply_assign_uuid_update(&mut metadata, update)?,
            "upgrade-format-version" => apply_upgrade_format_version_update(&mut metadata, update)?,
            "add-schema" => apply_add_schema_update(&mut metadata, update)?,
            "set-current-schema" => apply_set_current_schema_update(&mut metadata, update)?,
            "add-spec" => apply_add_spec_update(&mut metadata, update)?,
            "set-default-spec" => apply_set_default_spec_update(&mut metadata, update)?,
            "add-sort-order" => apply_add_sort_order_update(&mut metadata, update)?,
            "set-default-sort-order" => apply_set_default_sort_order_update(&mut metadata, update)?,
            "add-snapshot" => apply_add_snapshot_update(&mut metadata, update)?,
            "set-snapshot-ref" => apply_set_snapshot_ref_update(&mut metadata, update)?,
            "remove-snapshots" => apply_remove_snapshots_update(&mut metadata, update)?,
            "remove-snapshot-ref" => apply_remove_snapshot_ref_update(&mut metadata, update)?,
            "set-location" => apply_set_location_update(&mut metadata, update)?,
            "set-properties" => apply_set_properties_update(&mut metadata, update)?,
            "remove-properties" => apply_remove_properties_update(&mut metadata, update)?,
            _ => return Err(s3_error!(NotImplemented, "unsupported table update: {action}")),
        }
    }

    append_previous_metadata_log(&mut metadata, previous_metadata_location)?;
    metadata_object_mut(&mut metadata)?.insert("last-updated-ms".to_string(), serde_json::Value::from(current_time_millis()));
    Ok(metadata)
}

fn apply_assign_uuid_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let uuid = update
        .get("uuid")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "assign-uuid requires uuid"))?;
    let object = metadata_object_mut(metadata)?;
    if let Some(existing) = object.get("table-uuid").and_then(serde_json::Value::as_str)
        && existing != uuid
    {
        return Err(s3_error!(PreconditionFailed, "cannot reassign table uuid"));
    }
    object.insert("table-uuid".to_string(), serde_json::Value::String(uuid.to_string()));
    Ok(())
}

fn apply_upgrade_format_version_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let version = update
        .get("format-version")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "upgrade-format-version requires format-version"))?;
    let current = metadata
        .get("format-version")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_default();
    if version < current {
        return Err(s3_error!(InvalidRequest, "format-version cannot be downgraded"));
    }
    metadata_object_mut(metadata)?.insert("format-version".to_string(), serde_json::Value::from(version));
    Ok(())
}

fn apply_add_schema_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let mut schema = update
        .get("schema")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-schema requires schema"))?;
    if !schema.is_object() {
        return Err(s3_error!(InvalidRequest, "add-schema schema must be a JSON object"));
    }
    if schema.get("schema-id").is_none() {
        let next_id = next_array_object_i64(metadata, "schemas", "schema-id")?;
        schema
            .as_object_mut()
            .ok_or_else(|| s3_error!(InvalidRequest, "add-schema schema must be a JSON object"))?
            .insert("schema-id".to_string(), serde_json::Value::from(next_id));
    }
    let last_column_id = max_field_id(&schema);
    ensure_array_field(metadata, "schemas")?.push(schema);
    let object = metadata_object_mut(metadata)?;
    let current_last = object
        .get("last-column-id")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_default();
    object.insert("last-column-id".to_string(), serde_json::Value::from(current_last.max(last_column_id)));
    Ok(())
}

fn apply_set_current_schema_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let requested_id = update
        .get("schema-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-current-schema requires schema-id"))?;
    let schema_id = if requested_id == -1 {
        last_array_object_i64(metadata, "schemas", "schema-id")?
    } else {
        requested_id
    };
    metadata_object_mut(metadata)?.insert("current-schema-id".to_string(), serde_json::Value::from(schema_id));
    Ok(())
}

fn apply_add_spec_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let mut spec = update
        .get("spec")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-spec requires spec"))?;
    if !spec.is_object() {
        return Err(s3_error!(InvalidRequest, "add-spec spec must be a JSON object"));
    }
    if spec.get("spec-id").is_none() {
        let next_id = next_array_object_i64(metadata, "partition-specs", "spec-id")?;
        spec.as_object_mut()
            .ok_or_else(|| s3_error!(InvalidRequest, "add-spec spec must be a JSON object"))?
            .insert("spec-id".to_string(), serde_json::Value::from(next_id));
    }
    let last_partition_id = max_partition_field_id(&spec);
    ensure_array_field(metadata, "partition-specs")?.push(spec);
    let object = metadata_object_mut(metadata)?;
    let current_last = object
        .get("last-partition-id")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(999);
    object.insert(
        "last-partition-id".to_string(),
        serde_json::Value::from(current_last.max(last_partition_id)),
    );
    Ok(())
}

fn apply_set_default_spec_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let requested_id = update
        .get("spec-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-default-spec requires spec-id"))?;
    let spec_id = if requested_id == -1 {
        last_array_object_i64(metadata, "partition-specs", "spec-id")?
    } else {
        requested_id
    };
    metadata_object_mut(metadata)?.insert("default-spec-id".to_string(), serde_json::Value::from(spec_id));
    Ok(())
}

fn apply_add_sort_order_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let mut sort_order = update
        .get("sort-order")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-sort-order requires sort-order"))?;
    if !sort_order.is_object() {
        return Err(s3_error!(InvalidRequest, "add-sort-order sort-order must be a JSON object"));
    }
    if sort_order.get("order-id").is_none() {
        let next_id = next_array_object_i64(metadata, "sort-orders", "order-id")?;
        sort_order
            .as_object_mut()
            .ok_or_else(|| s3_error!(InvalidRequest, "add-sort-order sort-order must be a JSON object"))?
            .insert("order-id".to_string(), serde_json::Value::from(next_id));
    }
    ensure_array_field(metadata, "sort-orders")?.push(sort_order);
    Ok(())
}

fn apply_set_default_sort_order_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let requested_id = update
        .get("sort-order-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-default-sort-order requires sort-order-id"))?;
    let sort_order_id = if requested_id == -1 {
        last_array_object_i64(metadata, "sort-orders", "order-id")?
    } else {
        requested_id
    };
    metadata_object_mut(metadata)?.insert("default-sort-order-id".to_string(), serde_json::Value::from(sort_order_id));
    Ok(())
}

fn apply_add_snapshot_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let snapshot = update
        .get("snapshot")
        .cloned()
        .ok_or_else(|| s3_error!(InvalidRequest, "add-snapshot requires snapshot"))?;
    let snapshot_id = snapshot
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot-id must be an integer"))?;
    let sequence_number = snapshot
        .get("sequence-number")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "snapshot sequence-number must be an integer"))?;
    let timestamp_ms = snapshot
        .get("timestamp-ms")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_else(current_time_millis);
    ensure_array_field(metadata, "snapshots")?.push(snapshot);
    let object = metadata_object_mut(metadata)?;
    let current_sequence_number = object
        .get("last-sequence-number")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_default();
    object.insert(
        "last-sequence-number".to_string(),
        serde_json::Value::from(current_sequence_number.max(sequence_number)),
    );
    object.insert("current-snapshot-id".to_string(), serde_json::Value::from(snapshot_id));
    ensure_array_field(metadata, "snapshot-log")?.push(serde_json::json!({
        "timestamp-ms": timestamp_ms,
        "snapshot-id": snapshot_id
    }));
    Ok(())
}

fn apply_set_snapshot_ref_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let ref_name = update
        .get("ref-name")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-snapshot-ref requires ref-name"))?;
    let snapshot_id = update
        .get("snapshot-id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-snapshot-ref requires snapshot-id"))?;
    let reference = update
        .as_object()
        .ok_or_else(|| s3_error!(InvalidRequest, "set-snapshot-ref must be a JSON object"))?
        .iter()
        .filter(|(key, _)| key.as_str() != "action" && key.as_str() != "ref-name")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<serde_json::Map<_, _>>();
    ensure_object_field(metadata, "refs")?.insert(ref_name.to_string(), serde_json::Value::Object(reference));
    if ref_name == "main" {
        metadata_object_mut(metadata)?.insert("current-snapshot-id".to_string(), serde_json::Value::from(snapshot_id));
    }
    Ok(())
}

fn apply_remove_snapshots_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let ids = update
        .get("snapshot-ids")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| s3_error!(InvalidRequest, "remove-snapshots requires snapshot-ids"))?
        .iter()
        .filter_map(serde_json::Value::as_i64)
        .collect::<std::collections::BTreeSet<_>>();
    ensure_array_field(metadata, "snapshots")?.retain(|snapshot| {
        snapshot
            .get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .is_none_or(|snapshot_id| !ids.contains(&snapshot_id))
    });
    ensure_array_field(metadata, "snapshot-log")?.retain(|log| {
        log.get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .is_none_or(|snapshot_id| !ids.contains(&snapshot_id))
    });
    Ok(())
}

fn apply_remove_snapshot_ref_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let ref_name = update
        .get("ref-name")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "remove-snapshot-ref requires ref-name"))?;
    ensure_object_field(metadata, "refs")?.remove(ref_name);
    Ok(())
}

fn apply_set_location_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let location = update
        .get("location")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-location requires location"))?;
    metadata_object_mut(metadata)?.insert("location".to_string(), serde_json::Value::String(location.to_string()));
    Ok(())
}

fn apply_set_properties_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let updates = update
        .get("updates")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| s3_error!(InvalidRequest, "set-properties requires updates"))?;
    let properties = ensure_object_field(metadata, "properties")?;
    for (key, value) in updates {
        let value = value
            .as_str()
            .ok_or_else(|| s3_error!(InvalidRequest, "table property values must be strings"))?;
        properties.insert(key.clone(), serde_json::Value::String(value.to_string()));
    }
    Ok(())
}

fn apply_remove_properties_update(metadata: &mut serde_json::Value, update: &serde_json::Value) -> S3Result<()> {
    let removals = update
        .get("removals")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| s3_error!(InvalidRequest, "remove-properties requires removals"))?;
    let properties = ensure_object_field(metadata, "properties")?;
    for removal in removals {
        let key = removal
            .as_str()
            .ok_or_else(|| s3_error!(InvalidRequest, "property removals must be strings"))?;
        properties.remove(key);
    }
    Ok(())
}

fn append_previous_metadata_log(metadata: &mut serde_json::Value, previous_metadata_location: &str) -> S3Result<()> {
    ensure_array_field(metadata, "metadata-log")?.push(serde_json::json!({
        "timestamp-ms": current_time_millis(),
        "metadata-file": previous_metadata_location
    }));
    Ok(())
}

fn metadata_object_mut(metadata: &mut serde_json::Value) -> S3Result<&mut serde_json::Map<String, serde_json::Value>> {
    metadata
        .as_object_mut()
        .ok_or_else(|| s3_error!(InvalidRequest, "table metadata must be a JSON object"))
}

fn ensure_array_field<'a>(metadata: &'a mut serde_json::Value, key: &str) -> S3Result<&'a mut Vec<serde_json::Value>> {
    let object = metadata_object_mut(metadata)?;
    object
        .entry(key.to_string())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    object
        .get_mut(key)
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| s3_error!(InvalidRequest, "metadata field {key} must be an array"))
}

fn ensure_object_field<'a>(
    metadata: &'a mut serde_json::Value,
    key: &str,
) -> S3Result<&'a mut serde_json::Map<String, serde_json::Value>> {
    let object = metadata_object_mut(metadata)?;
    object
        .entry(key.to_string())
        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    object
        .get_mut(key)
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| s3_error!(InvalidRequest, "metadata field {key} must be an object"))
}

fn next_array_object_i64(metadata: &serde_json::Value, array_key: &str, id_key: &str) -> S3Result<i64> {
    Ok(last_array_object_i64(metadata, array_key, id_key)?.saturating_add(1))
}

fn last_array_object_i64(metadata: &serde_json::Value, array_key: &str, id_key: &str) -> S3Result<i64> {
    let values = metadata
        .get(array_key)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| s3_error!(InvalidRequest, "metadata field {array_key} must be an array"))?;
    values
        .iter()
        .filter_map(|value| value.get(id_key).and_then(serde_json::Value::as_i64))
        .max()
        .ok_or_else(|| s3_error!(InvalidRequest, "metadata field {array_key} has no {id_key}"))
}

fn table_commit_operation(metadata: &serde_json::Value) -> String {
    metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .and_then(|snapshots| snapshots.last())
        .and_then(|snapshot| snapshot.get("summary"))
        .and_then(|summary| summary.get("operation"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("commit")
        .to_string()
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

async fn namespace_exists_status<S>(store: &S, bucket: &str, namespace: &crate::table_catalog::Namespace) -> S3Result<StatusCode>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let exists = store
        .get_namespace(bucket, &namespace.public_name())
        .await
        .map_err(catalog_store_error)?
        .is_some();
    Ok(exists_status(exists))
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
    let mut entry = table_entry_from_register_request(bucket, namespace, request)?;
    ensure_table_bucket_entry(store, bucket, table_bucket_enabled).await?;
    let metadata = read_table_metadata_json(metadata_backend, bucket, &entry.metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &metadata)?;
    adopt_registered_metadata_identity(&mut entry, &metadata)?;
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
    let (entry, metadata) = table_entry_from_create_table_request(bucket, namespace, request)?;
    ensure_table_bucket_entry(store, bucket, table_bucket_enabled).await?;
    let metadata_data = serde_json::to_vec(&metadata)
        .map_err(|err| s3_error!(InternalError, "failed to serialize initial table metadata: {}", err))?;
    metadata_backend
        .put_object(
            bucket,
            &entry.metadata_location,
            metadata_data,
            crate::table_catalog::TableCatalogPutPrecondition::IfAbsent,
        )
        .await
        .map_err(catalog_store_error)?;
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

async fn table_exists_status<S>(
    store: &S,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
) -> S3Result<StatusCode>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let exists = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
        .is_some();
    Ok(exists_status(exists))
}

async fn load_credentials_response<S>(
    store: &S,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    issuer: &dyn TableCredentialIssuer,
    principal: Option<&rustfs_credentials::Credentials>,
) -> S3Result<RestLoadCredentialsResponse>
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
    load_credentials_response_from_entry(&entry, issuer, principal).await
}

async fn get_table_metadata_location_response<S>(
    store: &S,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
) -> S3Result<TableMetadataLocationResponse>
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
    Ok(table_metadata_location_response_from_entry(entry))
}

async fn update_table_metadata_location_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: UpdateTableMetadataLocationRequest,
) -> S3Result<TableMetadataLocationResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(current) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(s3_error!(InvalidRequest, "table not found"));
    };
    let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    if !crate::table_catalog::is_valid_table_metadata_location(namespace, &table_name, &request.metadata_location) {
        return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
    }
    let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &current_metadata)?;
    let target_metadata = read_table_metadata_json(metadata_backend, bucket, &request.metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
    validate_metadata_matches_current_metadata(&current_metadata, &target_metadata)?;
    let commit_request = crate::table_catalog::TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        commit_id: request.commit_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
        idempotency_key: request.idempotency_key,
        operation: "update-metadata-location".to_string(),
        expected_version_token: request.version_token,
        expected_metadata_location: current.metadata_location,
        new_metadata_location: request.metadata_location,
        requirements: Vec::new(),
        writer: Some("rustfs-metadata-location-api".to_string()),
    };
    let result = store.commit_table(commit_request).await.map_err(catalog_store_error)?;
    Ok(table_metadata_location_response_from_entry(result.table))
}

async fn commit_table_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: RestCommitTableRequest,
) -> S3Result<RestCommitTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    if request.new_metadata_location.is_none() {
        return standard_commit_table_response(store, metadata_backend, bucket, namespace, table, request).await;
    }

    let request = table_commit_request_from_rest_request(bucket, namespace, table, request)?;
    let Some(current) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(s3_error!(InvalidRequest, "table not found"));
    };
    let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    if !crate::table_catalog::is_valid_table_metadata_location(namespace, &table_name, &request.new_metadata_location) {
        return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
    }
    let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &current_metadata)?;
    let target_metadata = read_table_metadata_json(metadata_backend, bucket, &request.new_metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
    validate_metadata_matches_current_metadata(&current_metadata, &target_metadata)?;
    let result = store.commit_table(request).await.map_err(catalog_store_error)?;
    Ok(commit_table_response_from_result(result, target_metadata))
}

async fn standard_commit_table_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: RestCommitTableRequest,
) -> S3Result<RestCommitTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(current) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(s3_error!(InvalidRequest, "table not found"));
    };
    let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
    validate_table_commit_requirements(&current_metadata, &request.requirements)?;
    let expected_metadata = current_metadata.clone();
    let next_metadata = apply_table_commit_updates(current_metadata, &request.updates, &current.metadata_location)?;
    validate_metadata_table_location_in_bucket(bucket, &next_metadata)?;
    validate_metadata_matches_current_metadata(&expected_metadata, &next_metadata)?;
    let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    let (commit_id, metadata_file_token) = standard_commit_ids(request.commit_id);
    let next_generation = current.generation.saturating_add(1);
    let next_metadata_location = crate::table_catalog::default_table_metadata_file_path(
        namespace,
        &table_name,
        &next_metadata_file_name(next_generation, &metadata_file_token),
    );
    let next_metadata_data = serde_json::to_vec(&next_metadata)
        .map_err(|err| s3_error!(InternalError, "failed to serialize table metadata update: {}", err))?;
    metadata_backend
        .put_object(
            bucket,
            &next_metadata_location,
            next_metadata_data,
            crate::table_catalog::TableCatalogPutPrecondition::IfAbsent,
        )
        .await
        .map_err(catalog_store_error)?;

    let commit_request = crate::table_catalog::TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        commit_id,
        idempotency_key: request.idempotency_key,
        operation: request.operation.unwrap_or_else(|| table_commit_operation(&next_metadata)),
        expected_version_token: current.version_token,
        expected_metadata_location: current.metadata_location,
        new_metadata_location: next_metadata_location,
        requirements: request.requirements,
        writer: request.writer,
    };
    let result = store.commit_table(commit_request).await.map_err(catalog_store_error)?;
    Ok(commit_table_response_from_result(result, next_metadata))
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

async fn table_metadata_maintenance_response<B>(
    store: &crate::table_catalog::ObjectTableCatalogStore<B>,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: TableMetadataMaintenanceRequest,
) -> S3Result<crate::table_catalog::TableMetadataMaintenanceReport>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    let report = if request.delete {
        store
            .delete_table_metadata_maintenance_candidates(
                bucket,
                &namespace.public_name(),
                table,
                request.retain_recent_metadata_files,
            )
            .await
            .map_err(catalog_store_error)
    } else {
        store
            .plan_table_metadata_maintenance(bucket, &namespace.public_name(), table, request.retain_recent_metadata_files)
            .await
            .map_err(catalog_store_error)
    }?;
    store
        .put_table_metadata_maintenance_report(&report)
        .await
        .map_err(catalog_store_error)?;
    Ok(report)
}

async fn catalog_import_response<B>(
    store: &crate::table_catalog::ObjectTableCatalogStore<B>,
    metadata_backend: &B,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: CatalogImportRequest,
    table_bucket_enabled: bool,
) -> S3Result<RestLoadTableResponse>
where
    B: crate::table_catalog::TableCatalogObjectBackend,
{
    ensure_table_bucket_entry(store, bucket, table_bucket_enabled).await?;
    let mut entry = table_entry_from_import_request(bucket, namespace, table, request)?;
    let metadata = read_table_metadata_json(metadata_backend, bucket, &entry.metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &metadata)?;
    adopt_registered_metadata_identity(&mut entry, &metadata)?;
    store.register_table(entry.clone()).await.map_err(catalog_store_error)?;
    Ok(load_table_response_from_entry(entry, metadata))
}

async fn rollback_table_response<S>(
    store: &S,
    metadata_backend: &impl crate::table_catalog::TableCatalogObjectBackend,
    bucket: &str,
    namespace: &crate::table_catalog::Namespace,
    table: &str,
    request: RollbackTableRequest,
) -> S3Result<RestCommitTableResponse>
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let Some(current) = store
        .load_table(bucket, &namespace.public_name(), table)
        .await
        .map_err(catalog_store_error)?
    else {
        return Err(s3_error!(InvalidRequest, "table not found"));
    };
    let table_name = crate::table_catalog::IdentifierSegment::parse(table.to_string())
        .map_err(|err| s3_error!(InvalidRequest, "invalid table name: {}", err))?;
    if !crate::table_catalog::is_valid_table_metadata_location(namespace, &table_name, &request.metadata_location) {
        return Err(s3_error!(InvalidRequest, "metadata location must be inside the table metadata directory"));
    }
    let current_metadata = read_table_metadata_json(metadata_backend, bucket, &current.metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &current_metadata)?;
    let target_metadata = read_table_metadata_json(metadata_backend, bucket, &request.metadata_location).await?;
    validate_metadata_table_location_in_bucket(bucket, &target_metadata)?;
    validate_metadata_matches_current_metadata(&current_metadata, &target_metadata)?;
    let commit_request = crate::table_catalog::TableCommitRequest {
        table_bucket: bucket.to_string(),
        namespace: namespace.public_name(),
        table: table.to_string(),
        commit_id: request.commit_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
        idempotency_key: request.idempotency_key,
        operation: "rollback".to_string(),
        expected_version_token: request.version_token,
        expected_metadata_location: current.metadata_location,
        new_metadata_location: request.metadata_location,
        requirements: Vec::new(),
        writer: Some("rustfs-catalog-rollback-api".to_string()),
    };
    let result = store.commit_table(commit_request).await.map_err(catalog_store_error)?;
    Ok(commit_table_response_from_result(result, target_metadata))
}

pub struct GetCatalogConfigHandler {}

#[async_trait::async_trait]
impl Operation for GetCatalogConfigHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize_table_catalog_request(&req, AdminAction::GetTableCatalogAction).await?;
        build_json_response(StatusCode::OK, &catalog_config_response())
    }
}

pub struct EnableTableBucketHandler {}

#[async_trait::async_trait]
impl Operation for EnableTableBucketHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableBucketAction).await?;
        let store = table_catalog_store()?;
        let response = enable_table_bucket_response(&store, &warehouse).await?;
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
        let store = table_catalog_store()?;
        let enabled = table_bucket_enabled_from_metadata(&warehouse).await?;
        let response = table_bucket_response(&store, &warehouse, enabled).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestListNamespacesHandler {}

#[async_trait::async_trait]
impl Operation for RestListNamespacesHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableNamespaceAction).await?;
        let store = table_catalog_store()?;
        let response = list_namespaces_response(&store, &warehouse).await?;
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
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableNamespaceAction).await?;
        let store = table_catalog_store()?;
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
        let store = table_catalog_store()?;
        drop_namespace_in_store(&store, &warehouse, &namespace.public_name()).await?;
        Ok(empty_response(StatusCode::NO_CONTENT))
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
        let store = table_catalog_store()?;
        Ok(empty_response(namespace_exists_status(&store, &warehouse, &namespace).await?))
    }
}

pub struct RestListTablesHandler {}

#[async_trait::async_trait]
impl Operation for RestListTablesHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableAction).await?;
        let store = table_catalog_store()?;
        let response = list_tables_response(&store, &warehouse, &namespace).await?;
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
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let resource = TableCatalogResource::namespace(&warehouse, &namespace);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
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
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone());
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
        let store = table_catalog_store()?;
        Ok(empty_response(table_exists_status(&store, &warehouse, &namespace, &table).await?))
    }
}

pub struct RestLoadCredentialsHandler {}

#[async_trait::async_trait]
impl Operation for RestLoadCredentialsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableCredentialsAction).await?;
        let principal = table_catalog_request_principal(&req).await?;
        let store = table_catalog_store()?;
        let issuer = IamTableCredentialIssuer::from_env();
        let response = load_credentials_response(&store, &warehouse, &namespace, &table, &issuer, Some(&principal)).await?;
        build_json_response(StatusCode::OK, &response)
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
        let request = read_json_body::<RestCommitTableRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone());
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
        let request = read_json_body::<UpdateTableMetadataLocationRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone());
        let response =
            update_table_metadata_location_response(&store, &metadata_backend, &warehouse, &namespace, &table, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct RestTableMetadataMaintenanceHandler {}

#[async_trait::async_trait]
impl Operation for RestTableMetadataMaintenanceHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RunTableMaintenanceAction).await?;
        let request = read_json_body::<TableMetadataMaintenanceRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend);
        let response = table_metadata_maintenance_response(&store, &warehouse, &namespace, &table, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct GetTableMaintenanceConfigHandler {}

#[async_trait::async_trait]
impl Operation for GetTableMaintenanceConfigHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableLifecycleAction).await?;
        let store = table_catalog_store()?;
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
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableLifecycleAction).await?;
        let request = read_json_body::<crate::table_catalog::TableMaintenanceConfig>(req.input).await?;
        let store = table_catalog_store()?;
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
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let job = job_id_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableLifecycleAction).await?;
        let store = table_catalog_store()?;
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

pub struct ExportTableCatalogHandler {}

#[async_trait::async_trait]
impl Operation for ExportTableCatalogHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        let store = table_catalog_store()?;
        let response = store
            .export_table_catalog_entry(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error)?;
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
        let store = crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone());
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
        let store = table_catalog_store()?;
        let config = store
            .get_table_maintenance_config(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error)?;
        let response = store
            .diagnose_table_catalog(&warehouse, &namespace.public_name(), &table, config.retain_recent_metadata_files)
            .await
            .map_err(catalog_store_error)?;
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
        let request = read_json_body::<RollbackTableRequest>(req.input).await?;
        let metadata_backend = table_catalog_backend()?;
        let store = crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone());
        let response = rollback_table_response(&store, &metadata_backend, &warehouse, &namespace, &table, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_catalog::{TableCatalogObjectBackend, TableCatalogStore};
    use std::sync::Arc;

    #[test]
    fn catalog_config_response_lists_standard_rest_endpoints() {
        let response = catalog_config_response();

        assert_eq!(response.defaults.get(WAREHOUSE_PROPERTY), Some(&DEFAULT_WAREHOUSE_ID));
        assert_eq!(response.defaults.get(CATALOG_ENDPOINT_PREFIX_CONFIG_KEY), Some(&TABLE_CATALOG_PREFIX));
        assert_eq!(
            response.defaults.get(CATALOG_COMPAT_ENDPOINT_PREFIX_CONFIG_KEY),
            Some(&TABLE_CATALOG_COMPAT_PREFIX)
        );
        assert!(response.overrides.is_empty());
        assert!(response.endpoints.contains(&"GET /v1/{prefix}/namespaces"));
        assert!(response.endpoints.contains(&"HEAD /v1/{prefix}/namespaces/{namespace}"));
        assert!(
            response
                .endpoints
                .contains(&"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}")
        );
        assert!(
            response
                .endpoints
                .contains(&"HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}")
        );
        assert!(
            response
                .endpoints
                .contains(&"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials")
        );
        assert!(response.endpoints.contains(&"GET /{warehouse}/namespaces"));
        assert!(response.endpoints.contains(&"POST /{warehouse}/namespaces"));
        assert!(response.endpoints.contains(&"HEAD /{warehouse}/namespaces/{namespace}"));
        assert!(
            response
                .endpoints
                .contains(&"POST /{warehouse}/namespaces/{namespace}/register")
        );
        assert!(
            response
                .endpoints
                .contains(&"POST /{warehouse}/namespaces/{namespace}/tables")
        );
        assert!(
            response
                .endpoints
                .contains(&"GET /{warehouse}/namespaces/{namespace}/tables/{table}")
        );
        assert!(
            response
                .endpoints
                .contains(&"HEAD /{warehouse}/namespaces/{namespace}/tables/{table}")
        );
        assert!(
            response
                .endpoints
                .contains(&"POST /{warehouse}/namespaces/{namespace}/tables/{table}")
        );
        assert!(
            response
                .endpoints
                .contains(&"GET /{warehouse}/namespaces/{namespace}/tables/{table}/credentials")
        );
    }

    #[test]
    fn table_catalog_handlers_require_table_admin_actions() {
        let src = include_str!("table_catalog.rs");

        assert!(
            operation_block(src, "GetCatalogConfigHandler")
                .contains("authorize_table_catalog_request(&req, AdminAction::GetTableCatalogAction).await?;")
        );
        assert!(
            src.contains("validate_admin_request_with_bucket_object("),
            "catalog resource auth should pass namespace/table scope into IAM object matching"
        );

        for (handler, action) in [
            ("EnableTableBucketHandler", "AdminAction::SetTableBucketAction"),
            ("GetTableBucketHandler", "AdminAction::GetTableBucketAction"),
            ("RestListNamespacesHandler", "AdminAction::GetTableNamespaceAction"),
            ("RestCreateNamespaceHandler", "AdminAction::SetTableNamespaceAction"),
            ("RestGetNamespaceHandler", "AdminAction::GetTableNamespaceAction"),
            ("RestNamespaceExistsHandler", "AdminAction::GetTableNamespaceAction"),
            ("RestDropNamespaceHandler", "AdminAction::DeleteTableNamespaceAction"),
            ("RestListTablesHandler", "AdminAction::GetTableAction"),
            ("RestCreateTableHandler", "AdminAction::CreateTableAction"),
            ("RestRegisterTableHandler", "AdminAction::RegisterTableAction"),
            ("RestLoadTableHandler", "AdminAction::GetTableMetadataAction"),
            ("RestTableExistsHandler", "AdminAction::GetTableAction"),
            ("RestLoadCredentialsHandler", "AdminAction::GetTableCredentialsAction"),
            ("RestCommitTableHandler", "AdminAction::CommitTableAction"),
            ("RestDropTableHandler", "AdminAction::DeleteTableAction"),
            ("GetTableMetadataLocationHandler", "AdminAction::GetTableMetadataLocationAction"),
            ("UpdateTableMetadataLocationHandler", "AdminAction::SetTableMetadataLocationAction"),
            ("RestTableMetadataMaintenanceHandler", "AdminAction::RunTableMaintenanceAction"),
            ("GetTableMaintenanceConfigHandler", "AdminAction::GetTableLifecycleAction"),
            ("PutTableMaintenanceConfigHandler", "AdminAction::SetTableLifecycleAction"),
            ("GetTableMaintenanceJobHandler", "AdminAction::GetTableLifecycleAction"),
            ("ExportTableCatalogHandler", "AdminAction::GetTableMetadataAction"),
            ("ImportTableCatalogHandler", "AdminAction::RegisterTableAction"),
            ("GetTableCatalogDiagnosticsHandler", "AdminAction::GetTableMetadataAction"),
            ("RollbackTableCatalogHandler", "AdminAction::CommitTableAction"),
        ] {
            let block = operation_block(src, handler);
            assert!(
                block.contains(&format!("authorize_table_catalog_resource_request(&req, &resource, {action}).await?;")),
                "{handler} should require {action} with catalog resource auth"
            );
            assert!(
                !block.contains("authorize_table_catalog_request(&req,"),
                "{handler} must not use unscoped table catalog authorization"
            );
            assert!(
                !block.contains("authorize_table_catalog_warehouse_request(&req, &warehouse,"),
                "{handler} should not bypass catalog resource auth"
            );
        }

        for (handler, action) in [
            ("RestLoadTableHandler", "AdminAction::GetTableMetadataAction"),
            ("RestTableExistsHandler", "AdminAction::GetTableAction"),
            ("RestLoadCredentialsHandler", "AdminAction::GetTableCredentialsAction"),
            ("RestCommitTableHandler", "AdminAction::CommitTableAction"),
            ("RestDropTableHandler", "AdminAction::DeleteTableAction"),
            ("GetTableMetadataLocationHandler", "AdminAction::GetTableMetadataLocationAction"),
            ("UpdateTableMetadataLocationHandler", "AdminAction::SetTableMetadataLocationAction"),
            ("RestTableMetadataMaintenanceHandler", "AdminAction::RunTableMaintenanceAction"),
            ("GetTableMaintenanceConfigHandler", "AdminAction::GetTableLifecycleAction"),
            ("PutTableMaintenanceConfigHandler", "AdminAction::SetTableLifecycleAction"),
            ("GetTableMaintenanceJobHandler", "AdminAction::GetTableLifecycleAction"),
            ("ExportTableCatalogHandler", "AdminAction::GetTableMetadataAction"),
            ("ImportTableCatalogHandler", "AdminAction::RegisterTableAction"),
            ("GetTableCatalogDiagnosticsHandler", "AdminAction::GetTableMetadataAction"),
            ("RollbackTableCatalogHandler", "AdminAction::CommitTableAction"),
        ] {
            let block = operation_block(src, handler);
            assert!(
                block.contains("TableCatalogResource::table(&warehouse, &namespace, &table)"),
                "{handler} should build a table-aware catalog resource"
            );
            assert!(
                block.contains(&format!("authorize_table_catalog_resource_request(&req, &resource, {action}).await?;")),
                "{handler} should authorize against the table-aware catalog resource"
            );
        }
    }

    #[test]
    fn table_catalog_resource_builds_policy_object_scope() {
        let namespace = crate::table_catalog::Namespace::parse("analytics.daily_events").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");

        assert_eq!(TableCatalogResource::warehouse("warehouse-a").object_path(), None);
        assert_eq!(
            TableCatalogResource::namespace("warehouse-a", &namespace)
                .object_path()
                .as_deref(),
            Some("namespaces/analytics/daily_events")
        );
        assert_eq!(
            TableCatalogResource::table("warehouse-a", &namespace, table.as_str())
                .object_path()
                .as_deref(),
            Some("namespaces/analytics/daily_events/tables/events")
        );
    }

    fn operation_block<'a>(src: &'a str, handler: &str) -> &'a str {
        let marker = format!("impl Operation for {handler}");
        let block = src.split_once(&marker).expect("handler impl should exist").1;
        let end = block
            .find("\npub struct ")
            .or_else(|| block.find("\n#[cfg(test)]"))
            .unwrap_or(block.len());
        &block[..end]
    }

    #[test]
    fn rest_catalog_mvp_routes_use_implemented_handlers() {
        fn assert_operation<T: Operation>() {}

        let _: &EnableTableBucketHandler = &ENABLE_TABLE_BUCKET_HANDLER;
        let _: &GetTableBucketHandler = &GET_TABLE_BUCKET_HANDLER;
        let _: &RestListNamespacesHandler = &LIST_NAMESPACES_HANDLER;
        let _: &RestCreateNamespaceHandler = &CREATE_NAMESPACE_HANDLER;
        let _: &RestGetNamespaceHandler = &GET_NAMESPACE_HANDLER;
        let _: &RestNamespaceExistsHandler = &NAMESPACE_EXISTS_HANDLER;
        let _: &RestDropNamespaceHandler = &DROP_NAMESPACE_HANDLER;
        let _: &RestListTablesHandler = &LIST_TABLES_HANDLER;
        let _: &RestCreateTableHandler = &CREATE_TABLE_HANDLER;
        let _: &RestRegisterTableHandler = &REGISTER_TABLE_HANDLER;
        let _: &RestLoadTableHandler = &LOAD_TABLE_HANDLER;
        let _: &RestTableExistsHandler = &TABLE_EXISTS_HANDLER;
        let _: &RestLoadCredentialsHandler = &LOAD_CREDENTIALS_HANDLER;
        let _: &RestCommitTableHandler = &COMMIT_TABLE_HANDLER;
        let _: &RestDropTableHandler = &DROP_TABLE_HANDLER;
        let _: &GetTableMetadataLocationHandler = &GET_TABLE_METADATA_LOCATION_HANDLER;
        let _: &UpdateTableMetadataLocationHandler = &UPDATE_TABLE_METADATA_LOCATION_HANDLER;
        let _: &RestTableMetadataMaintenanceHandler = &TABLE_METADATA_MAINTENANCE_HANDLER;
        let _: &GetTableMaintenanceConfigHandler = &GET_TABLE_MAINTENANCE_CONFIG_HANDLER;
        let _: &PutTableMaintenanceConfigHandler = &PUT_TABLE_MAINTENANCE_CONFIG_HANDLER;
        let _: &GetTableMaintenanceJobHandler = &GET_TABLE_MAINTENANCE_JOB_HANDLER;
        let _: &ExportTableCatalogHandler = &EXPORT_TABLE_CATALOG_HANDLER;
        let _: &ImportTableCatalogHandler = &IMPORT_TABLE_CATALOG_HANDLER;
        let _: &GetTableCatalogDiagnosticsHandler = &GET_TABLE_CATALOG_DIAGNOSTICS_HANDLER;
        let _: &RollbackTableCatalogHandler = &ROLLBACK_TABLE_CATALOG_HANDLER;

        assert_operation::<EnableTableBucketHandler>();
        assert_operation::<GetTableBucketHandler>();
        assert_operation::<RestListNamespacesHandler>();
        assert_operation::<RestCreateNamespaceHandler>();
        assert_operation::<RestGetNamespaceHandler>();
        assert_operation::<RestNamespaceExistsHandler>();
        assert_operation::<RestDropNamespaceHandler>();
        assert_operation::<RestListTablesHandler>();
        assert_operation::<RestCreateTableHandler>();
        assert_operation::<RestRegisterTableHandler>();
        assert_operation::<RestLoadTableHandler>();
        assert_operation::<RestTableExistsHandler>();
        assert_operation::<RestLoadCredentialsHandler>();
        assert_operation::<RestCommitTableHandler>();
        assert_operation::<RestDropTableHandler>();
        assert_operation::<GetTableMetadataLocationHandler>();
        assert_operation::<UpdateTableMetadataLocationHandler>();
        assert_operation::<RestTableMetadataMaintenanceHandler>();
        assert_operation::<GetTableMaintenanceConfigHandler>();
        assert_operation::<PutTableMaintenanceConfigHandler>();
        assert_operation::<GetTableMaintenanceJobHandler>();
        assert_operation::<ExportTableCatalogHandler>();
        assert_operation::<ImportTableCatalogHandler>();
        assert_operation::<GetTableCatalogDiagnosticsHandler>();
        assert_operation::<RollbackTableCatalogHandler>();
    }

    #[test]
    fn table_metadata_maintenance_request_uses_conservative_defaults() {
        let request: TableMetadataMaintenanceRequest =
            serde_json::from_value(serde_json::json!({})).expect("default maintenance request should parse");

        assert_eq!(request.retain_recent_metadata_files, 0);
        assert!(!request.delete);
    }

    #[test]
    fn table_metadata_maintenance_request_accepts_delete_mode() {
        let request: TableMetadataMaintenanceRequest = serde_json::from_value(serde_json::json!({
            "retain-recent-metadata-files": 2,
            "delete": true
        }))
        .expect("metadata maintenance request should parse");

        assert_eq!(request.retain_recent_metadata_files, 2);
        assert!(request.delete);
    }

    #[tokio::test]
    async fn table_bucket_response_reports_catalog_discovery_without_credentials() {
        let store = TestTableCatalogStore::default();
        ensure_table_bucket_entry(&store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");

        let response = table_bucket_response(&store, "warehouse", true)
            .await
            .expect("bucket response should build");

        assert_eq!(response.table_bucket, "warehouse");
        assert!(response.enabled);
        assert_eq!(response.catalog_type, crate::table_catalog::TABLE_BUCKET_CATALOG_TYPE);
        assert_eq!(response.warehouse_location, "s3://warehouse/");
        assert_eq!(response.catalog_uri, "/iceberg/v1/warehouse");
        assert_eq!(response.compat_catalog_uri, "/_iceberg/v1/warehouse");
        assert_eq!(response.credential_vending, CREDENTIAL_VENDING_UNSUPPORTED);
        assert_eq!(response.credential_scope, "warehouse-prefix");
        assert_eq!(response.credential_scope_prefix, "s3://warehouse/");
        assert!(response.catalog_entry_present);
    }

    #[test]
    fn table_catalog_ingress_requests_reject_unknown_fields() {
        assert_rejects_unknown_field::<CreateNamespaceRequest>(
            "CreateNamespaceRequest",
            serde_json::json!({
                "namespace": ["analytics"],
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<RegisterTableRequest>(
            "RegisterTableRequest",
            serde_json::json!({
                "name": "events",
                "metadata-location": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json",
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<CreateTableRequest>(
            "CreateTableRequest",
            serde_json::json!({
                "name": "events",
                "schema": {},
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<RestCommitTableRequest>(
            "RestCommitTableRequest",
            serde_json::json!({
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<TableMetadataMaintenanceRequest>(
            "TableMetadataMaintenanceRequest",
            serde_json::json!({
                "delete": true,
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<UpdateTableMetadataLocationRequest>(
            "UpdateTableMetadataLocationRequest",
            serde_json::json!({
                "metadata-location": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json",
                "version-token": "token-v1",
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<CatalogImportRequest>(
            "CatalogImportRequest",
            serde_json::json!({
                "metadata-location": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json",
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<RollbackTableRequest>(
            "RollbackTableRequest",
            serde_json::json!({
                "metadata-location": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json",
                "version-token": "token-v2",
                "unexpected": true
            }),
        );
        assert_rejects_unknown_field::<crate::table_catalog::TableMaintenanceConfig>(
            "TableMaintenanceConfig",
            serde_json::json!({
                "version": 1,
                "unexpected": true
            }),
        );
    }

    fn assert_rejects_unknown_field<T>(target: &str, value: serde_json::Value)
    where
        T: serde::de::DeserializeOwned,
    {
        let err = match serde_json::from_value::<T>(value) {
            Ok(_) => panic!("{target} should reject unknown fields"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("unknown field"),
            "{target} should reject unknown fields, got: {err}"
        );
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

    #[tokio::test]
    async fn namespace_exists_status_uses_head_rest_semantics() {
        let store = TestTableCatalogStore::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");

        assert_eq!(
            namespace_exists_status(&store, "warehouse", &namespace)
                .await
                .expect("missing namespace check should succeed"),
            StatusCode::NOT_FOUND
        );

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

        assert_eq!(
            namespace_exists_status(&store, "warehouse", &namespace)
                .await
                .expect("existing namespace check should succeed"),
            StatusCode::NO_CONTENT
        );
    }

    #[tokio::test]
    async fn table_exists_status_uses_head_rest_semantics() {
        let store = TestTableCatalogStore::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");

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

        assert_eq!(
            table_exists_status(&store, "warehouse", &namespace, "events")
                .await
                .expect("missing table check should succeed"),
            StatusCode::NOT_FOUND
        );

        store
            .create_table(crate::table_catalog::TableEntry {
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
                    ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json"
                        .to_string(),
                version_token: "token-v1".to_string(),
                generation: 1,
                state: crate::table_catalog::TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            })
            .await
            .expect("table should be created");

        assert_eq!(
            table_exists_status(&store, "warehouse", &namespace, "events")
                .await
                .expect("existing table check should succeed"),
            StatusCode::NO_CONTENT
        );
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
    fn create_table_request_accepts_standard_iceberg_rest_shape() {
        let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
            "name": "events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {
                        "id": 1,
                        "name": "id",
                        "required": true,
                        "type": "long"
                    }
                ]
            },
            "partition-spec": {
                "spec-id": 0,
                "fields": []
            },
            "write-order": {
                "order-id": 0,
                "fields": []
            },
            "properties": {
                "write.format.default": "parquet"
            }
        }))
        .expect("standard create table request should parse");

        assert_eq!(request.name, "events");
    }

    #[test]
    fn commit_table_request_accepts_standard_iceberg_rest_shape() {
        let request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [
                {
                    "type": "assert-table-uuid",
                    "uuid": "table-uuid"
                }
            ],
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "lakehouse"
                    }
                }
            ]
        }))
        .expect("standard commit table request should parse");

        assert_eq!(request.requirements.len(), 1);
    }

    #[test]
    fn standard_commit_ids_use_uuid_for_metadata_file_when_provided() {
        let commit_id = "11111111-1111-4111-8111-111111111111";
        assert_eq!(
            standard_commit_ids(Some(commit_id.to_string())),
            (commit_id.to_string(), commit_id.to_string())
        );
    }

    #[test]
    fn standard_commit_ids_generate_metadata_hash_for_non_uuid_client_id() {
        let (commit_id, metadata_file_token) = standard_commit_ids(Some("commit-1".to_string()));

        assert_eq!(commit_id, "commit-1");
        assert_ne!(metadata_file_token, commit_id);
        assert_eq!(metadata_file_token, table_catalog_path_hash("commit-1"));
    }

    #[tokio::test]
    async fn create_table_response_writes_initial_metadata_for_standard_request() {
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

        let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
            "name": "events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {
                        "id": 1,
                        "name": "id",
                        "required": true,
                        "type": "long"
                    }
                ]
            },
            "properties": {
                "write.format.default": "parquet"
            }
        }))
        .expect("standard create table request should parse");

        let response = create_table_response(&store, &metadata_backend, "warehouse", &namespace, request, true)
            .await
            .expect("table should be created");

        assert_eq!(
            response.metadata_location,
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json"
        );
        assert_eq!(response.metadata["format-version"], 2);
        assert_eq!(response.metadata["current-schema-id"], 0);
        assert_eq!(response.metadata["default-spec-id"], 0);
        assert_eq!(response.metadata["default-sort-order-id"], 0);
        assert_eq!(
            response.metadata["properties"]["write.format.default"],
            serde_json::Value::String("parquet".to_string())
        );
        let entry = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert_eq!(response.metadata["table-uuid"], entry.table_uuid);
        assert!(
            metadata_backend
                .object_exists("warehouse", &response.metadata_location)
                .await
                .expect("metadata object lookup should succeed")
        );
    }

    #[tokio::test]
    async fn standard_commit_applies_updates_and_writes_next_metadata() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_uuid = created.metadata["table-uuid"]
            .as_str()
            .expect("created metadata should have table uuid")
            .to_string();

        let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "requirements": [
                {
                    "type": "assert-table-uuid",
                    "uuid": table_uuid
                },
                {
                    "type": "assert-current-schema-id",
                    "current-schema-id": 0
                }
            ],
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "lakehouse"
                    }
                },
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1234,
                        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
                        "summary": {
                            "operation": "append"
                        }
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": 10,
                    "type": "branch"
                }
            ]
        }))
        .expect("standard commit table request should parse");

        let commit = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", commit_request)
            .await
            .expect("standard commit should succeed");

        let metadata_file_prefix = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-";
        let metadata_file_suffix = ".metadata.json";
        let generated_commit_id = commit
            .metadata_location
            .strip_prefix(metadata_file_prefix)
            .and_then(|file| file.strip_suffix(metadata_file_suffix))
            .expect("standard commit metadata file should include a UUID suffix");
        Uuid::parse_str(generated_commit_id).expect("metadata file suffix should be a UUID");
        assert_eq!(commit.commit_id, generated_commit_id);
        assert_eq!(commit.metadata["properties"]["owner"], serde_json::Value::String("lakehouse".to_string()));
        assert_eq!(commit.metadata["current-snapshot-id"], 10);
        assert_eq!(commit.metadata["last-sequence-number"], 1);
        assert_eq!(commit.metadata["refs"]["main"]["snapshot-id"], 10);
        assert_eq!(
            commit.metadata["metadata-log"][0]["metadata-file"],
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json"
        );
        let committed = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("committed table lookup should succeed")
            .expect("committed table should exist");
        assert_eq!(committed.metadata_location, commit.metadata_location);
        assert!(
            metadata_backend
                .object_exists("warehouse", &commit.metadata_location)
                .await
                .expect("committed metadata lookup should succeed")
        );
    }

    #[tokio::test]
    async fn standard_commit_uses_client_uuid_commit_id_in_metadata_file_name() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        create_standard_events_table(&store, &metadata_backend, &namespace).await;

        let commit_id = "11111111-1111-4111-8111-111111111111";
        let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "commit-id": commit_id,
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "lakehouse"
                    }
                }
            ]
        }))
        .expect("standard commit table request should parse");
        let commit = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", commit_request)
            .await
            .expect("standard commit should succeed");

        assert_eq!(commit.commit_id, commit_id);
        assert_eq!(
            commit.metadata_location,
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-11111111-1111-4111-8111-111111111111.metadata.json"
        );
        assert!(
            metadata_backend
                .object_exists("warehouse", &commit.metadata_location)
                .await
                .expect("committed metadata lookup should succeed")
        );
    }

    #[tokio::test]
    async fn standard_commit_accepts_non_uuid_client_commit_id_without_using_it_in_metadata_file_name() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        create_standard_events_table(&store, &metadata_backend, &namespace).await;

        let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "commit-id": "commit-1",
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "lakehouse"
                    }
                }
            ]
        }))
        .expect("standard commit table request should parse");
        let commit = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", commit_request)
            .await
            .expect("standard commit should succeed");

        let metadata_file_prefix = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-";
        let metadata_file_suffix = ".metadata.json";
        let metadata_file_token = commit
            .metadata_location
            .strip_prefix(metadata_file_prefix)
            .and_then(|file| file.strip_suffix(metadata_file_suffix))
            .expect("standard commit metadata file should include a safe token suffix");
        assert_eq!(commit.commit_id, "commit-1");
        assert_ne!(metadata_file_token, commit.commit_id);
        assert_eq!(metadata_file_token, table_catalog_path_hash("commit-1"));
    }

    #[tokio::test]
    async fn standard_commit_ignores_generation_only_orphan_metadata_file() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        create_standard_events_table(&store, &metadata_backend, &namespace).await;
        metadata_backend
            .put_json(
                "warehouse",
                ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json",
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "orphan",
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;

        let commit_id = "22222222-2222-4222-8222-222222222222";
        let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "commit-id": commit_id,
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "lakehouse"
                    }
                }
            ]
        }))
        .expect("standard commit table request should parse");
        let commit = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", commit_request)
            .await
            .expect("standard commit should not collide with generation-only orphan");

        assert_eq!(
            commit.metadata_location,
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-22222222-2222-4222-8222-222222222222.metadata.json"
        );
        assert_eq!(commit.metadata["properties"]["owner"], "lakehouse");
    }

    #[tokio::test]
    async fn concurrent_standard_commits_write_distinct_metadata_files_before_pointer_conflict() {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        create_standard_events_table(&store, &metadata_backend, &namespace).await;

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let metadata_backend = TestTableCatalogObjectBackend {
            objects: Arc::clone(&metadata_backend.objects),
            put_object_barrier: Some(barrier),
        };
        let first_commit_id = "33333333-3333-4333-8333-333333333333";
        let second_commit_id = "44444444-4444-4444-8444-444444444444";
        let first_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "commit-id": first_commit_id,
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "first"
                    }
                }
            ]
        }))
        .expect("first standard commit table request should parse");
        let second_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "commit-id": second_commit_id,
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "second"
                    }
                }
            ]
        }))
        .expect("second standard commit table request should parse");

        let (first, second) = tokio::join!(
            commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", first_request),
            commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", second_request)
        );
        let success_count = [first.is_ok(), second.is_ok()].into_iter().filter(|ok| *ok).count();

        assert_eq!(success_count, 1);
        assert!(
            metadata_backend
                .object_exists(
                    "warehouse",
                    ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-33333333-3333-4333-8333-333333333333.metadata.json"
                )
                .await
                .expect("first metadata object lookup should succeed")
        );
        assert!(
            metadata_backend
                .object_exists(
                    "warehouse",
                    ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-44444444-4444-4444-8444-444444444444.metadata.json"
                )
                .await
                .expect("second metadata object lookup should succeed")
        );
    }

    #[tokio::test]
    async fn standard_commit_accepts_legacy_catalog_uuid_when_current_metadata_matches() {
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
        let current_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        let legacy_entry = table_entry_from_register_request(
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: current_location.to_string(),
                overwrite: false,
            },
        )
        .expect("table entry should build");
        assert_ne!(legacy_entry.table_uuid, "metadata-table-uuid");
        store
            .register_table(legacy_entry.clone())
            .await
            .expect("legacy table entry should register");
        metadata_backend
            .put_json(
                "warehouse",
                current_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "metadata-table-uuid",
                    "location": "s3://warehouse/tables/table-id",
                    "properties": {}
                }),
            )
            .await;

        let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {
                        "owner": "lakehouse"
                    }
                }
            ]
        }))
        .expect("standard commit table request should parse");
        let committed = commit_table_response(&store, &metadata_backend, "warehouse", &namespace, "events", commit_request)
            .await
            .expect("legacy catalog uuid should not block standard commit");

        assert_eq!(committed.metadata["table-uuid"], "metadata-table-uuid");
        assert_eq!(committed.metadata["properties"]["owner"], "lakehouse");
        assert_eq!(committed.generation, legacy_entry.generation + 1);
    }

    #[tokio::test]
    async fn metadata_location_api_accepts_legacy_catalog_uuid_when_target_matches_current_metadata() {
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
        let current_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        let legacy_entry = table_entry_from_register_request(
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: current_location.to_string(),
                overwrite: false,
            },
        )
        .expect("table entry should build");
        assert_ne!(legacy_entry.table_uuid, "metadata-table-uuid");
        store
            .register_table(legacy_entry.clone())
            .await
            .expect("legacy table entry should register");
        metadata_backend
            .put_json(
                "warehouse",
                current_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "metadata-table-uuid",
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;
        let next_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
        metadata_backend
            .put_json(
                "warehouse",
                next_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "metadata-table-uuid",
                    "location": "s3://warehouse/tables/table-id",
                    "last-sequence-number": 2
                }),
            )
            .await;

        let updated = update_table_metadata_location_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            "events",
            UpdateTableMetadataLocationRequest {
                metadata_location: next_location.to_string(),
                version_token: legacy_entry.version_token,
                commit_id: Some("commit-1".to_string()),
                idempotency_key: None,
            },
        )
        .await
        .expect("legacy catalog uuid should not block metadata-location update");

        assert_eq!(updated.metadata_location, next_location);
        assert_eq!(updated.generation, legacy_entry.generation + 1);
    }

    #[tokio::test]
    async fn table_metadata_maintenance_helper_runs_dry_run_and_delete() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        let old = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        let current = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

        seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current.clone()).await;
        backend
            .put_json_with_mod_time(bucket, &old, serde_json::json!({}), Some(OffsetDateTime::UNIX_EPOCH))
            .await;
        backend
            .put_json_with_mod_time(
                bucket,
                &current,
                serde_json::json!({
                    "metadata-log": []
                }),
                Some(OffsetDateTime::UNIX_EPOCH),
            )
            .await;

        let default_config = store
            .get_table_maintenance_config(bucket, "analytics", "events")
            .await
            .expect("default maintenance config should load");
        assert_eq!(default_config, crate::table_catalog::TableMaintenanceConfig::default());
        let config = store
            .put_table_maintenance_config(
                bucket,
                "analytics",
                "events",
                crate::table_catalog::TableMaintenanceConfig {
                    version: crate::table_catalog::TABLE_MAINTENANCE_CONFIG_VERSION,
                    retain_recent_metadata_files: 2,
                    delete_enabled: true,
                    background_enabled: false,
                },
            )
            .await
            .expect("maintenance config should persist");
        assert_eq!(config.retain_recent_metadata_files, 2);
        assert!(config.delete_enabled);
        assert!(
            store
                .put_table_maintenance_config(
                    bucket,
                    "analytics",
                    "events",
                    crate::table_catalog::TableMaintenanceConfig {
                        version: crate::table_catalog::TABLE_MAINTENANCE_CONFIG_VERSION,
                        retain_recent_metadata_files: 2,
                        delete_enabled: true,
                        background_enabled: true,
                    },
                )
                .await
                .is_err()
        );

        let dry_run = table_metadata_maintenance_response(
            &store,
            bucket,
            &namespace,
            "events",
            TableMetadataMaintenanceRequest {
                retain_recent_metadata_files: 0,
                delete: false,
            },
        )
        .await
        .expect("metadata maintenance dry-run should succeed");
        assert_eq!(dry_run.cleanup_candidate_locations, vec![old.clone()]);
        assert_eq!(dry_run.deletable_metadata_locations, vec![old.clone()]);
        let stored_dry_run = store
            .get_table_metadata_maintenance_report(bucket, "analytics", "events", &dry_run.job.job_id)
            .await
            .expect("maintenance job lookup should succeed")
            .expect("maintenance job should be stored");
        assert_eq!(stored_dry_run, dry_run);
        assert!(
            backend
                .object_exists(bucket, &old)
                .await
                .expect("old metadata lookup should succeed")
        );

        let deleted = table_metadata_maintenance_response(
            &store,
            bucket,
            &namespace,
            "events",
            TableMetadataMaintenanceRequest {
                retain_recent_metadata_files: 0,
                delete: true,
            },
        )
        .await
        .expect("metadata maintenance delete should succeed");
        assert_eq!(deleted.cleanup_candidate_locations, vec![old.clone()]);
        assert_eq!(deleted.deletable_metadata_locations, vec![old.clone()]);
        assert!(
            !backend
                .object_exists(bucket, &old)
                .await
                .expect("old metadata lookup should succeed after delete")
        );
    }

    #[test]
    fn commit_requirements_reject_mismatched_table_uuid() {
        let metadata = serde_json::json!({
            "table-uuid": "actual-table-uuid"
        });
        let requirements = vec![serde_json::json!({
            "type": "assert-table-uuid",
            "uuid": "stale-table-uuid"
        })];

        assert!(validate_table_commit_requirements(&metadata, &requirements).is_err());
    }

    #[test]
    fn table_updates_reject_unknown_actions() {
        let metadata = serde_json::json!({
            "metadata-log": []
        });
        let updates = vec![serde_json::json!({
            "action": "rewrite-everything"
        })];

        assert!(apply_table_commit_updates(metadata, &updates, "metadata/00001.metadata.json").is_err());
    }

    #[test]
    fn table_location_updates_must_stay_inside_bucket() {
        let metadata = serde_json::json!({
            "location": "s3://warehouse/tables/table-id",
            "metadata-log": []
        });
        let updates = vec![serde_json::json!({
            "action": "set-location",
            "location": "s3://other-warehouse/tables/table-id"
        })];

        let updated = apply_table_commit_updates(metadata, &updates, "metadata/00001.metadata.json")
            .expect("set-location should update metadata before boundary validation");

        assert!(validate_metadata_table_location_in_bucket("warehouse", &updated).is_err());
    }

    #[test]
    fn load_table_response_includes_rest_metadata_payload() {
        let metadata = serde_json::json!({
            "format-version": 2,
            "table-uuid": "table-uuid",
            "location": "s3://warehouse/tables/table-id"
        });
        let response = load_table_response_from_entry(table_entry_for_credentials(), metadata.clone());

        assert_eq!(response.metadata, metadata);
        assert!(response.storage_credentials.is_empty());
        assert_eq!(response.config.get("rustfs.credential-vending"), Some(&"unsupported".to_string()));
        assert_eq!(
            response.config.get("rustfs.credential-vending-reason"),
            Some(&"temporary-credentials-not-implemented".to_string())
        );
        assert_eq!(response.config.get("rustfs.credential-scope"), Some(&"table-prefix".to_string()));
        assert_eq!(
            response.config.get("rustfs.credential-scope-prefix"),
            Some(&"s3://warehouse/tables/table-id".to_string())
        );
        assert_eq!(
            response.config.get("rustfs.credential-mode"),
            Some(&"client-provided-s3-credentials-required".to_string())
        );
        assert!(!response.config.contains_key("s3.access-key-id"));
        assert!(!response.config.contains_key("s3.secret-access-key"));
        assert!(!response.config.contains_key("s3.session-token"));
    }

    fn table_entry_for_credentials() -> crate::table_catalog::TableEntry {
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
            metadata_location: ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json"
                .to_string(),
            version_token: "token-v1".to_string(),
            generation: 1,
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        }
    }

    #[tokio::test]
    async fn disabled_table_credential_issuer_keeps_credentials_empty() {
        let issuer = DisabledTableCredentialIssuer;
        let response = load_credentials_response_from_entry(&table_entry_for_credentials(), &issuer, None)
            .await
            .expect("disabled issuer should build an empty response");

        assert!(response.storage_credentials.is_empty());
    }

    #[tokio::test]
    async fn disabled_table_credential_issuer_skips_scope_validation() {
        let issuer = DisabledTableCredentialIssuer;
        let mut entry = table_entry_for_credentials();
        entry.warehouse_location = "s3://warehouse/".to_string();

        let response = load_credentials_response_from_entry(&entry, &issuer, None)
            .await
            .expect("disabled issuer should not validate credential scopes");

        assert!(response.storage_credentials.is_empty());
    }

    struct TestTableCredentialIssuer;

    #[async_trait::async_trait]
    impl TableCredentialIssuer for TestTableCredentialIssuer {
        async fn issue_table_credentials(
            &self,
            request: TableCredentialIssueRequest<'_>,
        ) -> S3Result<Option<IssuedTableCredentials>> {
            assert_eq!(request.entry.table_bucket, "warehouse");
            assert_eq!(request.scope_prefix, "s3://warehouse/tables/table-id/");
            assert_eq!(request.object_prefix, "tables/table-id/");
            Ok(Some(IssuedTableCredentials {
                access_key_id: "temporary-access-key".to_string(),
                secret_access_key: "temporary-secret-key".to_string(),
                session_token: "temporary-session-token".to_string(),
                expiration: OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("test timestamp should be valid"),
            }))
        }
    }

    #[tokio::test]
    async fn credential_issuer_returns_temporary_scoped_storage_credentials() {
        let issuer = TestTableCredentialIssuer;
        let principal = rustfs_credentials::Credentials {
            access_key: "parent-access-key".to_string(),
            secret_key: "parent-secret-key".to_string(),
            ..Default::default()
        };

        let response = load_credentials_response_from_entry(&table_entry_for_credentials(), &issuer, Some(&principal))
            .await
            .expect("issuer should build a scoped credential response");

        assert_eq!(response.storage_credentials.len(), 1);
        let credential = &response.storage_credentials[0];
        assert_eq!(credential.prefix, "s3://warehouse/tables/table-id/");
        assert_eq!(credential.config.get("s3.access-key-id"), Some(&"temporary-access-key".to_string()));
        assert_eq!(credential.config.get("s3.secret-access-key"), Some(&"temporary-secret-key".to_string()));
        assert_eq!(credential.config.get("s3.session-token"), Some(&"temporary-session-token".to_string()));
        assert_eq!(
            credential.config.get("rustfs.credential-mode"),
            Some(&"catalog-vended-temporary-credentials".to_string())
        );
        assert_eq!(
            credential.config.get("rustfs.credential-scope-prefix"),
            Some(&"s3://warehouse/tables/table-id/".to_string())
        );
        assert_eq!(
            credential.config.get("rustfs.credential-expiration-unix-seconds"),
            Some(&"1800000000".to_string())
        );
        assert!(!credential.config.contains_key("rustfs.credential-vending-reason"));
    }

    #[test]
    fn table_credentials_do_not_snapshot_parent_groups() {
        let principal = rustfs_credentials::Credentials {
            access_key: "parent-access-key".to_string(),
            groups: Some(vec!["analytics-writers".to_string()]),
            ..Default::default()
        };
        let mut credential = rustfs_credentials::Credentials::default();

        bind_table_credential_parent(&mut credential, &principal);

        assert_eq!(credential.parent_user, "parent-access-key");
        assert!(credential.groups.is_none());
    }

    #[tokio::test]
    async fn table_credential_session_policy_is_limited_to_table_prefix() {
        let policy =
            table_credential_session_policy("warehouse", "tables/table-id/").expect("table credential policy should build");
        let groups = None;
        let conditions = std::collections::HashMap::new();
        let claims = std::collections::HashMap::new();

        assert!(
            policy
                .is_allowed(&rustfs_policy::policy::Args {
                    account: "temporary-access-key",
                    groups: &groups,
                    action: Action::S3Action(rustfs_policy::policy::action::S3Action::GetObjectAction),
                    bucket: "warehouse",
                    conditions: &conditions,
                    is_owner: false,
                    object: "tables/table-id/data/file.parquet",
                    claims: &claims,
                    deny_only: false,
                })
                .await
        );
        assert!(
            policy
                .is_allowed(&rustfs_policy::policy::Args {
                    account: "temporary-access-key",
                    groups: &groups,
                    action: Action::S3Action(rustfs_policy::policy::action::S3Action::GetBucketLocationAction),
                    bucket: "warehouse",
                    conditions: &conditions,
                    is_owner: false,
                    object: "",
                    claims: &claims,
                    deny_only: false,
                })
                .await
        );
        assert!(
            !policy
                .is_allowed(&rustfs_policy::policy::Args {
                    account: "temporary-access-key",
                    groups: &groups,
                    action: Action::S3Action(rustfs_policy::policy::action::S3Action::GetObjectAction),
                    bucket: "warehouse",
                    conditions: &conditions,
                    is_owner: false,
                    object: "tables/other/data/file.parquet",
                    claims: &claims,
                    deny_only: false,
                })
                .await
        );
        assert!(
            !policy
                .is_allowed(&rustfs_policy::policy::Args {
                    account: "temporary-access-key",
                    groups: &groups,
                    action: Action::S3Action(rustfs_policy::policy::action::S3Action::PutObjectAction),
                    bucket: "other-warehouse",
                    conditions: &conditions,
                    is_owner: false,
                    object: "tables/table-id/data/file.parquet",
                    claims: &claims,
                    deny_only: false,
                })
                .await
        );
    }

    #[test]
    fn table_credential_scope_rejects_cross_bucket_or_unsafe_prefix() {
        let mut entry = table_entry_for_credentials();
        entry.warehouse_location = "s3://other-warehouse/tables/table-id".to_string();
        assert!(table_credential_scope(&entry).is_err());

        let mut entry = table_entry_for_credentials();
        entry.warehouse_location = "s3://warehouse/tables/../table-id".to_string();
        assert!(table_credential_scope(&entry).is_err());
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

        assert_eq!(request.commit_id.as_deref(), Some("commit-1"));
        assert_eq!(request.idempotency_key.as_deref(), Some("retry-1"));
        assert_eq!(request.operation.as_deref(), Some("append"));
        assert_eq!(request.expected_version_token.as_deref(), Some("token-v1"));
        assert_eq!(
            request.new_metadata_location.as_deref(),
            Some(".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json")
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
        fail_put_table_bucket: tokio::sync::Mutex<bool>,
    }

    #[derive(Clone, Default)]
    struct TestTableCatalogObjectBackend {
        objects: Arc<tokio::sync::Mutex<BTreeMap<(String, String), crate::table_catalog::TableCatalogObject>>>,
        put_object_barrier: Option<Arc<tokio::sync::Barrier>>,
    }

    impl TestTableCatalogObjectBackend {
        async fn put_json(&self, bucket: &str, object: &str, value: serde_json::Value) {
            self.put_json_with_mod_time(bucket, object, value, None).await;
        }

        async fn put_json_with_mod_time(
            &self,
            bucket: &str,
            object: &str,
            value: serde_json::Value,
            mod_time: Option<OffsetDateTime>,
        ) {
            let data = serde_json::to_vec(&value).expect("metadata JSON should serialize");
            self.objects.lock().await.insert(
                (bucket.to_string(), object.to_string()),
                crate::table_catalog::TableCatalogObject {
                    data,
                    etag: Some("etag".to_string()),
                    mod_time,
                },
            );
        }
    }

    async fn create_standard_events_table(
        store: &TestTableCatalogStore,
        metadata_backend: &TestTableCatalogObjectBackend,
        namespace: &crate::table_catalog::Namespace,
    ) -> RestLoadTableResponse {
        ensure_table_bucket_entry(store, "warehouse", true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            store,
            "warehouse",
            CreateNamespaceRequest {
                namespace: namespace.public_name().split('.').map(str::to_string).collect(),
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let create_request: CreateTableRequest = serde_json::from_value(serde_json::json!({
            "name": "events",
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {
                        "id": 1,
                        "name": "id",
                        "required": true,
                        "type": "long"
                    }
                ]
            }
        }))
        .expect("standard create table request should parse");
        create_table_response(store, metadata_backend, "warehouse", namespace, create_request, true)
            .await
            .expect("table should be created")
    }

    async fn seed_object_table_for_metadata_maintenance(
        store: &crate::table_catalog::ObjectTableCatalogStore<TestTableCatalogObjectBackend>,
        backend: &TestTableCatalogObjectBackend,
        bucket: &str,
        namespace: &crate::table_catalog::Namespace,
        table: &crate::table_catalog::IdentifierSegment,
        current_metadata_location: String,
    ) {
        store
            .put_table_bucket(crate::table_catalog::TableBucketEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: bucket.to_string(),
                catalog_type: crate::table_catalog::TABLE_BUCKET_CATALOG_TYPE.to_string(),
                warehouse_root: format!("s3://{bucket}/"),
                state: crate::table_catalog::TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            })
            .await
            .expect("table bucket entry should seed");
        store
            .create_namespace(crate::table_catalog::NamespaceEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                namespace_id: namespace.storage_id(),
                state: crate::table_catalog::TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            })
            .await
            .expect("namespace entry should seed");
        store
            .create_table(crate::table_catalog::TableEntry {
                version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: bucket.to_string(),
                namespace: namespace.public_name(),
                table: table.as_str().to_string(),
                table_id: "table-id".to_string(),
                table_uuid: "table-uuid".to_string(),
                format: "ICEBERG".to_string(),
                format_version: 2,
                warehouse_location: format!("s3://{bucket}/tables/table-id"),
                metadata_location: current_metadata_location,
                version_token: "token-v1".to_string(),
                generation: 1,
                state: crate::table_catalog::TableCatalogEntryState::Active,
                properties: BTreeMap::new(),
                created_at: None,
                updated_at: None,
            })
            .await
            .expect("table entry should seed");
        backend
            .put_json(bucket, "unrelated/ignored.json", serde_json::json!({}))
            .await;
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
            precondition: crate::table_catalog::TableCatalogPutPrecondition,
        ) -> crate::table_catalog::TableCatalogStoreResult<()> {
            let key = (bucket.to_string(), object.to_string());
            let mut objects = self.objects.lock().await;
            let result = if matches!(precondition, crate::table_catalog::TableCatalogPutPrecondition::IfAbsent)
                && objects.contains_key(&key)
            {
                Err(crate::table_catalog::TableCatalogStoreError::Conflict(format!(
                    "object already exists: {object}"
                )))
            } else {
                objects.insert(
                    key,
                    crate::table_catalog::TableCatalogObject {
                        data,
                        etag: Some("etag".to_string()),
                        mod_time: None,
                    },
                );
                Ok(())
            };
            drop(objects);
            if let Some(barrier) = &self.put_object_barrier {
                barrier.wait().await;
            }
            result
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
            let mut fail_put_table_bucket = self.fail_put_table_bucket.lock().await;
            if *fail_put_table_bucket {
                *fail_put_table_bucket = false;
                return Err(crate::table_catalog::TableCatalogStoreError::Internal(
                    "injected table bucket write failure".to_string(),
                ));
            }
            drop(fail_put_table_bucket);

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
    async fn enable_table_bucket_response_fails_before_marker_when_catalog_entry_fails() {
        let store = TestTableCatalogStore::default();
        *store.fail_put_table_bucket.lock().await = true;

        assert!(enable_table_bucket_response(&store, "warehouse").await.is_err());
        assert!(
            store
                .get_table_bucket("warehouse")
                .await
                .expect("table bucket lookup should succeed")
                .is_none()
        );
        assert!(!*store.fail_put_table_bucket.lock().await);
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
            &metadata_backend,
            "warehouse",
            &namespace,
            "events",
            RestCommitTableRequest {
                commit_id: Some("commit-1".to_string()),
                idempotency_key: Some("retry-1".to_string()),
                operation: Some("append".to_string()),
                expected_version_token: Some(current.version_token.clone()),
                expected_metadata_location: Some(current.metadata_location.clone()),
                new_metadata_location: Some(next_metadata_location.to_string()),
                requirements: Vec::new(),
                updates: Vec::new(),
                _identifier: None,
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

    #[tokio::test]
    async fn register_table_response_adopts_metadata_table_uuid() {
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
                    "table-uuid": "metadata-table-uuid",
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;

        register_table_response(
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

        let entry = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert_eq!(entry.table_uuid, "metadata-table-uuid");
    }

    #[tokio::test]
    async fn register_table_response_rejects_metadata_without_format_version() {
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
                    "table-uuid": "metadata-table-uuid",
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;

        assert!(
            register_table_response(
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
            .is_err()
        );
        assert!(
            store
                .load_table("warehouse", "analytics", "events")
                .await
                .expect("table lookup should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn metadata_location_api_loads_and_updates_current_pointer() {
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
        let current_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        let entry = table_entry_from_register_request(
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: current_location.to_string(),
                overwrite: false,
            },
        )
        .expect("table entry should build");
        let table_uuid = entry.table_uuid.clone();
        store.register_table(entry).await.expect("table should register");
        metadata_backend
            .put_json(
                "warehouse",
                current_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": table_uuid,
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;
        let current = get_table_metadata_location_response(&store, "warehouse", &namespace, "events")
            .await
            .expect("metadata location should load");
        let next_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
        metadata_backend
            .put_json(
                "warehouse",
                next_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": table_uuid,
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;

        let updated = update_table_metadata_location_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            "events",
            UpdateTableMetadataLocationRequest {
                metadata_location: next_location.to_string(),
                version_token: current.version_token.clone(),
                commit_id: Some("commit-1".to_string()),
                idempotency_key: Some("retry-1".to_string()),
            },
        )
        .await
        .expect("metadata location should update");

        assert_eq!(updated.metadata_location, next_location);
        assert_eq!(updated.generation, current.generation + 1);
        assert_ne!(updated.version_token, current.version_token);
    }

    #[tokio::test]
    async fn metadata_location_api_rejects_invalid_target_metadata_before_commit() {
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
        let current_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        store
            .register_table(
                table_entry_from_register_request(
                    "warehouse",
                    &namespace,
                    RegisterTableRequest {
                        name: "events".to_string(),
                        metadata_location: current_location.to_string(),
                        overwrite: false,
                    },
                )
                .expect("table entry should build"),
            )
            .await
            .expect("table should register");
        let current = get_table_metadata_location_response(&store, "warehouse", &namespace, "events")
            .await
            .expect("metadata location should load");
        let invalid_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
        metadata_backend
            .put_json(
                "warehouse",
                invalid_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "table-uuid",
                    "location": "s3://other-warehouse/tables/table-id"
                }),
            )
            .await;

        assert!(
            update_table_metadata_location_response(
                &store,
                &metadata_backend,
                "warehouse",
                &namespace,
                "events",
                UpdateTableMetadataLocationRequest {
                    metadata_location: invalid_location.to_string(),
                    version_token: current.version_token,
                    commit_id: Some("commit-1".to_string()),
                    idempotency_key: None,
                },
            )
            .await
            .is_err()
        );
        let unchanged = get_table_metadata_location_response(&store, "warehouse", &namespace, "events")
            .await
            .expect("metadata location should still load");
        assert_eq!(unchanged.metadata_location, current_location);
        assert_eq!(unchanged.generation, current.generation);
    }

    #[tokio::test]
    async fn metadata_location_api_rejects_mismatched_table_uuid_before_commit() {
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
        let current_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        metadata_backend
            .put_json(
                "warehouse",
                current_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "table-uuid",
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;
        register_table_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: current_location.to_string(),
                overwrite: false,
            },
            true,
        )
        .await
        .expect("table should register");
        let current = get_table_metadata_location_response(&store, "warehouse", &namespace, "events")
            .await
            .expect("metadata location should load");
        let mismatched_location =
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
        metadata_backend
            .put_json(
                "warehouse",
                mismatched_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "other-table-uuid",
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;

        assert!(
            update_table_metadata_location_response(
                &store,
                &metadata_backend,
                "warehouse",
                &namespace,
                "events",
                UpdateTableMetadataLocationRequest {
                    metadata_location: mismatched_location.to_string(),
                    version_token: current.version_token,
                    commit_id: Some("commit-1".to_string()),
                    idempotency_key: None,
                },
            )
            .await
            .is_err()
        );
        let unchanged = get_table_metadata_location_response(&store, "warehouse", &namespace, "events")
            .await
            .expect("metadata location should still load");
        assert_eq!(unchanged.metadata_location, current_location);
        assert_eq!(unchanged.generation, current.generation);
    }

    #[tokio::test]
    async fn catalog_import_and_rollback_use_register_and_commit_paths() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        ensure_table_bucket_entry(&store, bucket, true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            bucket,
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let imported_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        backend
            .put_json(
                bucket,
                &imported_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "table-uuid",
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;

        let imported = catalog_import_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            CatalogImportRequest {
                metadata_location: imported_location.clone(),
                properties: BTreeMap::from([("owner".to_string(), "lakehouse".to_string())]),
            },
            true,
        )
        .await
        .expect("catalog import should register table");
        assert_eq!(imported.metadata_location, imported_location);
        let current = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        assert_eq!(current.properties.get("owner").map(String::as_str), Some("lakehouse"));

        let rollback_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        backend
            .put_json(
                bucket,
                &rollback_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "table-uuid",
                    "location": "s3://warehouse/tables/table-id",
                    "last-sequence-number": 2
                }),
            )
            .await;
        let rollback = rollback_table_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            RollbackTableRequest {
                metadata_location: rollback_location.clone(),
                version_token: current.version_token,
                commit_id: Some("rollback-1".to_string()),
                idempotency_key: None,
            },
        )
        .await
        .expect("rollback should commit selected metadata");

        assert_eq!(rollback.metadata_location, rollback_location);
        assert_eq!(rollback.commit_id, "rollback-1");
    }

    #[tokio::test]
    async fn rollback_rejects_invalid_target_metadata_before_commit() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        ensure_table_bucket_entry(&store, bucket, true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            bucket,
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let current_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        backend
            .put_json(
                bucket,
                &current_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "table-uuid",
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;
        catalog_import_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            CatalogImportRequest {
                metadata_location: current_location.clone(),
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("catalog import should register table");
        let current = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");

        let invalid_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        backend
            .put_json(
                bucket,
                &invalid_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "table-uuid",
                    "location": "s3://other-warehouse/tables/table-id"
                }),
            )
            .await;

        assert!(
            rollback_table_response(
                &store,
                &backend,
                bucket,
                &namespace,
                "events",
                RollbackTableRequest {
                    metadata_location: invalid_location,
                    version_token: current.version_token,
                    commit_id: Some("rollback-1".to_string()),
                    idempotency_key: None,
                },
            )
            .await
            .is_err()
        );
        let unchanged = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");

        assert_eq!(unchanged.metadata_location, current_location);
        assert_eq!(unchanged.generation, current.generation);
    }

    #[tokio::test]
    async fn rollback_rejects_mismatched_table_uuid_before_commit() {
        let backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
        let bucket = "warehouse";
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
        ensure_table_bucket_entry(&store, bucket, true)
            .await
            .expect("table bucket entry should be seeded");
        create_namespace_response(
            &store,
            bucket,
            CreateNamespaceRequest {
                namespace: vec!["analytics".to_string()],
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("namespace should be created");
        let current_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
        backend
            .put_json(
                bucket,
                &current_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "table-uuid",
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;
        catalog_import_response(
            &store,
            &backend,
            bucket,
            &namespace,
            "events",
            CatalogImportRequest {
                metadata_location: current_location.clone(),
                properties: BTreeMap::new(),
            },
            true,
        )
        .await
        .expect("catalog import should register table");
        let current = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");

        let mismatched_location =
            crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
        backend
            .put_json(
                bucket,
                &mismatched_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "other-table-uuid",
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;

        assert!(
            rollback_table_response(
                &store,
                &backend,
                bucket,
                &namespace,
                "events",
                RollbackTableRequest {
                    metadata_location: mismatched_location,
                    version_token: current.version_token,
                    commit_id: Some("rollback-1".to_string()),
                    idempotency_key: None,
                },
            )
            .await
            .is_err()
        );
        let unchanged = store
            .load_table(bucket, "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, current_location);
        assert_eq!(unchanged.generation, current.generation);
    }

    #[tokio::test]
    async fn legacy_commit_rejects_mismatched_table_uuid_before_commit() {
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
        let current_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
        metadata_backend
            .put_json(
                "warehouse",
                current_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "table-uuid",
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;
        register_table_response(
            &store,
            &metadata_backend,
            "warehouse",
            &namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: current_location.to_string(),
                overwrite: false,
            },
            true,
        )
        .await
        .expect("table should register");
        let current = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let mismatched_location =
            ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
        metadata_backend
            .put_json(
                "warehouse",
                mismatched_location,
                serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "other-table-uuid",
                    "location": "s3://warehouse/tables/table-id"
                }),
            )
            .await;

        assert!(
            commit_table_response(
                &store,
                &metadata_backend,
                "warehouse",
                &namespace,
                "events",
                RestCommitTableRequest {
                    commit_id: Some("commit-1".to_string()),
                    idempotency_key: None,
                    operation: Some("append".to_string()),
                    expected_version_token: Some(current.version_token.clone()),
                    expected_metadata_location: Some(current.metadata_location.clone()),
                    new_metadata_location: Some(mismatched_location.to_string()),
                    requirements: Vec::new(),
                    updates: Vec::new(),
                    _identifier: None,
                    writer: Some("pyiceberg".to_string()),
                },
            )
            .await
            .is_err()
        );
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, current_location);
        assert_eq!(unchanged.generation, current.generation);
    }
}
