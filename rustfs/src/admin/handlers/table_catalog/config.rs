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
use crate::admin::operation::{AdminRequest, AdminResponse, Operation};

pub struct GetCatalogConfigHandler {}

#[async_trait::async_trait]
impl Operation for GetCatalogConfigHandler {
    async fn call(&self, req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        authorize_table_catalog_request(&req, AdminAction::GetTableCatalogAction).await?;
        let warehouse = warehouse_from_config_query(req.uri())?;
        build_json_response(StatusCode::OK, &catalog_config_response(warehouse.as_deref())?)
    }
}

pub struct EnableTableBucketHandler {}

#[async_trait::async_trait]
impl Operation for EnableTableBucketHandler {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableBucketAction).await?;
        let app_context = runtime_sources::app_context_from_envelope(req.envelope());
        let backend = table_catalog_backend_from_envelope(req.envelope())?;
        let object_store = runtime_sources::object_store_from_envelope(req.envelope())
            .ok_or_else(|| table_catalog_internal_error("request object store is not initialized"))?;
        let store = table_catalog_store_from_backend(backend.clone())?;
        let publication = TableCommitObjectBackend::preauthorized(backend);
        let response = enable_table_bucket_response(&store, &publication, object_store.as_ref(), &warehouse).await?;
        crate::app::bucket_usecase::notify_table_bucket_metadata_reload(warehouse.clone(), "enable table bucket", app_context)
            .await;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct DisableTableBucketHandler {}

#[async_trait::async_trait]
impl Operation for DisableTableBucketHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        let principal = authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableBucketAction).await?;
        let cleanup = table_bucket_cleanup_requested_from_query(req.uri())?;
        let app_context = runtime_sources::app_context_from_envelope(req.envelope());
        let backend = table_catalog_backend_from_envelope(req.envelope())?;
        let object_store = runtime_sources::object_store_from_envelope(req.envelope())
            .ok_or_else(|| table_catalog_internal_error("request object store is not initialized"))?;
        let store = table_catalog_store_from_backend(backend.clone())?;
        install_table_catalog_s3_request_info(&mut req, &principal)?;
        let publication = TableCommitObjectBackend::for_request(backend, req);
        let response = if cleanup {
            cleanup_empty_table_bucket_response(&store, &publication, object_store.as_ref(), &warehouse).await?
        } else {
            disable_table_bucket_response(&store, &publication, object_store.as_ref(), &warehouse).await?
        };
        crate::app::bucket_usecase::notify_table_bucket_metadata_reload(warehouse.clone(), "disable table bucket", app_context)
            .await;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct GetTableBucketHandler {}

#[async_trait::async_trait]
impl Operation for GetTableBucketHandler {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableBucketAction).await?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let enabled = table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let response = table_bucket_response(&store, &warehouse, enabled).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct CreateTableCatalogBackupHandler {}

#[async_trait::async_trait]
impl Operation for CreateTableCatalogBackupHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::MigrateTableCatalogAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let request = read_json_body_or_default::<CatalogBackupRequest>(req.take_body()).await?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let started = Instant::now();
        let result = store
            .create_catalog_backup(&warehouse, request.expected_snapshot_etag.as_deref())
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("catalog-backup", &warehouse, "", "", started, &result);
        let response = result?;
        build_json_response(StatusCode::OK, &response)
    }
}

/// Returns the saved artifact bytes without applying or re-encoding its catalog.
pub struct ExportTableCatalogBackupHandler {}

#[async_trait::async_trait]
impl Operation for ExportTableCatalogBackupHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::MigrateTableCatalogAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let request = read_json_body::<CatalogBackupExportRequest>(req.take_body()).await?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let started = Instant::now();
        let result = store
            .export_catalog_backup(&warehouse, &request.backup_id)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("catalog-backup-export", &warehouse, "", "", started, &result);
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(JSON_CONTENT_TYPE));
        headers.insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-store, private"));
        headers.insert(http::header::PRAGMA, HeaderValue::from_static("no-cache"));
        headers.insert(http::header::EXPIRES, HeaderValue::from_static("0"));
        Ok(AdminResponse::with_headers(StatusCode::OK, Body::from(result?), headers))
    }
}

/// Stores a validated candidate artifact; catalog publication is a separate restore.
pub struct ImportTableCatalogBackupHandler {}

#[async_trait::async_trait]
impl Operation for ImportTableCatalogBackupHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::MigrateTableCatalogAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let data = read_limited_body(
            req.take_body(),
            CATALOG_BACKUP_IMPORT_BODY_MAX_SIZE,
            TABLE_CATALOG_REQUEST_BODY_TIMEOUT,
            Some("catalog backup import"),
        )
        .await?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let started = Instant::now();
        let result = store
            .import_catalog_backup(&warehouse, data.to_vec())
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("catalog-backup-import", &warehouse, "", "", started, &result);
        build_sensitive_json_response(StatusCode::OK, &result?)
    }
}

/// Prepares one verified small object while leaving catalog publication to restore.
pub struct PrepareTableCatalogBackupObjectHandler {}

#[async_trait::async_trait]
impl Operation for PrepareTableCatalogBackupObjectHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::MigrateTableCatalogAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let body = req.take_body();
        let request = read_bounded_json_body::<CatalogBackupObjectPrepareRequest>(
            req.headers(),
            body,
            CATALOG_BACKUP_PREPARE_BODY_MAX_SIZE,
            TABLE_CATALOG_REQUEST_BODY_TIMEOUT,
            "catalog backup object preparation",
        )
        .await?;
        if request.data_base64.len() > CATALOG_BACKUP_PREPARE_BASE64_MAX_SIZE {
            return Err(S3Error::from(ApiError::invalid_request(
                "data-base64 exceeds the encoded preparation limit",
            )));
        }
        let data = base64_simd::STANDARD
            .decode_to_vec(request.data_base64.as_bytes())
            .map_err(|_| S3Error::from(ApiError::invalid_request("data-base64 must be standard padded Base64")))?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let started = Instant::now();
        let result = store
            .prepare_catalog_backup_object(&warehouse, &request.backup_id, &request.object, data)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("catalog-backup-prepare", &warehouse, "", "", started, &result);
        build_sensitive_json_response(StatusCode::OK, &result?)
    }
}

pub struct RestoreTableCatalogBackupHandler {}

#[async_trait::async_trait]
impl Operation for RestoreTableCatalogBackupHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::MigrateTableCatalogAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let request = read_json_body::<CatalogRestoreRequest>(req.take_body()).await?;
        let store = table_catalog_store_from_envelope(req.envelope())?;
        let started = Instant::now();
        let result = store
            .restore_catalog_backup_with_id(
                &warehouse,
                &request.backup_id,
                request.expected_snapshot_etag.as_deref(),
                request.allow_replace,
                request.restore_id.as_deref(),
            )
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("catalog-restore", &warehouse, "", "", started, &result);
        let response = result?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct BackfillTableWarehouseIndexHandler {}

#[async_trait::async_trait]
impl Operation for BackfillTableWarehouseIndexHandler {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::MigrateTableCatalogAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let store = table_catalog_object_store_from_envelope(req.envelope())?;
        let started = Instant::now();
        let result = store
            .backfill_table_warehouse_index(&warehouse)
            .await
            .map_err(catalog_store_error);
        record_table_catalog_admin_operation_result("warehouse-index-backfill", &warehouse, "", "", started, &result);
        result?;
        Ok(owned_empty_response(StatusCode::NO_CONTENT))
    }
}

pub struct ExternalCatalogBridgeHandler {}

#[async_trait::async_trait]
impl Operation for ExternalCatalogBridgeHandler {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableMetadataAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let store = table_catalog_object_store_from_envelope(req.envelope())?;
        let response = external_catalog_bridge_response(&store, &warehouse, &namespace, &table).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct PutExternalCatalogBridgeHandler {}

#[async_trait::async_trait]
impl Operation for PutExternalCatalogBridgeHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let request = read_json_body::<ExternalCatalogBridgeRequest>(req.take_body()).await?;
        let store = table_catalog_object_store_from_envelope(req.envelope())?;
        let response = put_external_catalog_bridge_response(&store, &warehouse, &namespace, &table, request).await?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct SyncExternalCatalogBridgeHandler {}

#[async_trait::async_trait]
impl Operation for SyncExternalCatalogBridgeHandler {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let namespace = namespace_from_params(&params)?;
        let table = table_name_from_params(&params)?;
        let resource = TableCatalogResource::table(&warehouse, &namespace, &table);
        let principal =
            authorize_table_catalog_resource_request(&req, &resource, AdminAction::SetTableMetadataLocationAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let metadata_backend = table_catalog_backend_from_envelope(req.envelope())?;
        let store = table_catalog_object_store_from_envelope(req.envelope())?;
        if store
            .load_table(&warehouse, &namespace.public_name(), &table)
            .await
            .map_err(catalog_store_error)?
            .is_none()
        {
            authorize_table_catalog_resource_request(&req, &resource, AdminAction::RegisterTableAction).await?;
        }
        install_table_catalog_s3_request_info(&mut req, &principal)?;
        let request = read_json_body::<ExternalCatalogBridgeSyncRequest>(req.take_body()).await?;
        let table_bucket_enabled = table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
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

pub struct GetTableDirectoryHandler {}

#[async_trait::async_trait]
impl Operation for GetTableDirectoryHandler {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::GetTableCatalogAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let store = table_catalog_object_store_from_envelope(req.envelope())?;
        let response = store.table_directory_status(&warehouse).await.map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct AdvanceTableDirectoryHandler {}

#[async_trait::async_trait]
impl Operation for AdvanceTableDirectoryHandler {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::MigrateTableCatalogAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let store = table_catalog_object_store_from_envelope(req.envelope())?;
        let response = store.advance_table_directory(&warehouse).await.map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}

pub struct DisableTableDirectoryHandler {}

#[async_trait::async_trait]
impl Operation for DisableTableDirectoryHandler {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let warehouse = warehouse_from_params(&params)?;
        let resource = TableCatalogResource::warehouse(&warehouse);
        authorize_table_catalog_resource_request(&req, &resource, AdminAction::MigrateTableCatalogAction).await?;
        ensure_table_bucket_enabled_from_envelope(req.envelope(), &warehouse).await?;
        let store = table_catalog_object_store_from_envelope(req.envelope())?;
        let response = store.disable_table_directory(&warehouse).await.map_err(catalog_store_error)?;
        build_json_response(StatusCode::OK, &response)
    }
}
