use super::*;
use crate::admin::runtime_sources::{AppContext, IamInterface, KmsInterface, ServerContextSlot};
use crate::table_catalog::{TableCatalogObjectBackend, TableCatalogStore};
use datafusion::{
    arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    parquet::arrow::ArrowWriter,
};
use std::sync::Arc;

use crate::table_catalog::test_support::{
    TestCatalogObjectBackend as TestTableCatalogObjectBackend, TestCatalogObjectRecord, TestCatalogPublishPause,
    TestTableCatalogStore, manifest_avro_bytes as test_manifest_avro_bytes,
    manifest_avro_bytes_with_nullable_sequences as test_manifest_avro_bytes_with_nullable_sequences,
    manifest_list_avro_bytes as test_manifest_list_avro_bytes, manifest_list_avro_entries as test_manifest_list_avro_entries,
    table_metadata_json as test_table_metadata_json,
};
use rustfs_iam::store::{Store as _, UserType};
use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};

struct RequestIam {
    handle: Arc<rustfs_iam::sys::IamSys<rustfs_iam::store::object::ObjectStore>>,
}

impl IamInterface for RequestIam {
    fn handle(&self) -> Arc<rustfs_iam::sys::IamSys<rustfs_iam::store::object::ObjectStore>> {
        self.handle.clone()
    }

    fn is_ready(&self) -> bool {
        true
    }
}

struct RequestKms;

impl KmsInterface for RequestKms {
    fn handle(&self) -> Arc<rustfs_kms::KmsServiceManager> {
        Arc::new(rustfs_kms::KmsServiceManager::new())
    }
}

#[tokio::test]
async fn table_catalog_authentication_and_credentials_use_the_request_context() {
    let (_temp_dir, _disk_paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
    rustfs_iam::store::object::ObjectStore::new(store.clone())
        .save_iam_config(
            serde_json::json!({"version": 1}),
            format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
        )
        .await
        .expect("request IAM format should be seeded");
    let iam = rustfs_iam::build_iam_sys(store.clone())
        .await
        .expect("request IAM should initialize");
    let user_access_key = "request-table-user";
    let user_secret_key = "request-table-user-secret-key";
    let policy_name = "request-table-get";
    iam.create_user(
        user_access_key,
        &AddOrUpdateUserReq {
            secret_key: user_secret_key.to_string(),
            policy: None,
            status: AccountStatus::Enabled,
        },
    )
    .await
    .expect("request IAM user should be created");
    iam.set_policy(
        policy_name,
        Policy::parse_config(br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["admin:GetTable"]}]}"#)
            .expect("request table policy should parse"),
    )
    .await
    .expect("request table policy should be stored");
    iam.policy_db_set(user_access_key, UserType::Reg, false, policy_name)
        .await
        .expect("request table policy should be attached");
    let context = Arc::new(AppContext::new(
        store.clone(),
        Arc::new(RequestIam { handle: iam.clone() }),
        Arc::new(RequestKms),
    ));
    let credentials = rustfs_credentials::Credentials {
        access_key: "request-root-access-key".to_string(),
        secret_key: "request-root-secret-key".to_string(),
        status: "on".to_string(),
        ..Default::default()
    };
    assert!(context.publish_action_credentials(credentials.clone()));
    let slot = ServerContextSlot::new();
    assert!(slot.install(context));
    let mut extensions = http::Extensions::new();
    extensions.insert(slot);
    let request = S3Request {
        input: Body::empty(),
        method: Method::GET,
        uri: "/table-context".parse().expect("test URI"),
        headers: HeaderMap::new(),
        extensions,
        credentials: Some(s3s::auth::Credentials {
            access_key: user_access_key.to_string(),
            secret_key: s3s::auth::SecretKey::from(user_secret_key.to_string()),
        }),
        region: None,
        service: None,
        trailing_headers: None,
    };

    let principal = table_catalog_request_principal(&request)
        .await
        .expect("request IAM credentials should authenticate");
    assert!(!principal.owner);
    assert!(Arc::ptr_eq(&principal.iam_store, &iam));
    authorize_table_catalog_request(&request, AdminAction::GetTableAction)
        .await
        .expect("unscoped authorization should use the request IAM");
    let resource = TableCatalogResource::warehouse("request-warehouse");
    authorize_table_catalog_resource_request(&request, &resource, AdminAction::GetTableAction)
        .await
        .expect("resource authorization should use the request IAM");
    let issuer = IamTableCredentialIssuer::from_request(&request).expect("credential issuer should use the request context");
    assert!(Arc::ptr_eq(&issuer.iam_store, &iam));
    assert_eq!(issuer.token_signing_key.as_deref(), Some("request-root-secret-key"));
    let resolved_store =
        runtime_sources::object_store_from_extensions(&request.extensions).expect("request object store should resolve");
    assert!(Arc::ptr_eq(&resolved_store, &store));
}

#[test]
#[serial_test::serial]
fn catalog_config_response_lists_standard_rest_endpoints() {
    let response = temp_env::with_var_unset(crate::table_catalog::ENV_TABLE_CATALOG_BACKING, || catalog_config_response(None))
        .expect("catalog config should build");

    assert_eq!(response.defaults.get(WAREHOUSE_PROPERTY).map(String::as_str), Some(DEFAULT_WAREHOUSE_ID));
    assert_eq!(
        response.defaults.get(CATALOG_ENDPOINT_PREFIX_CONFIG_KEY).map(String::as_str),
        Some(TABLE_CATALOG_PREFIX)
    );
    assert_eq!(
        response
            .defaults
            .get(CATALOG_COMPAT_ENDPOINT_PREFIX_CONFIG_KEY)
            .map(String::as_str),
        Some(TABLE_CATALOG_COMPAT_PREFIX)
    );
    assert_eq!(
        response.defaults.get(CATALOG_BACKING_CONFIG_KEY).map(String::as_str),
        Some(crate::table_catalog::TABLE_CATALOG_BACKING_OBJECT)
    );
    assert!(!response.defaults.contains_key(PREFIX_PROPERTY));
    assert_eq!(
        response.overrides.get(NAMESPACE_SEPARATOR_PROPERTY).map(String::as_str),
        Some(REST_NAMESPACE_SEPARATOR_URL_ENCODED)
    );
    assert!(
        !response
            .endpoints
            .contains(&"POST /v1/{prefix}/namespaces/{namespace}/properties")
    );
    assert!(!response.endpoints.contains(&"POST /v1/{prefix}/tables/rename"));
    assert_eq!(response.admin_discovery.runtime_capabilities, "/rustfs/admin/v4/runtime/capabilities");
    assert_eq!(response.admin_discovery.cluster_snapshot, "/rustfs/admin/v4/cluster/snapshot");
    assert_eq!(response.admin_discovery.extensions_catalog, "/rustfs/admin/v4/extensions/catalog");
    assert_eq!(response.endpoints.as_slice(), TABLE_CATALOG_ENDPOINTS);
    assert!(response.endpoints.iter().all(|endpoint| endpoint.contains("/v1/{prefix}/")));
}

#[test]
#[serial_test::serial]
fn catalog_config_response_reports_durable_strong_backing_override() {
    let response = temp_env::with_var(
        crate::table_catalog::ENV_TABLE_CATALOG_BACKING,
        Some(crate::table_catalog::TABLE_CATALOG_BACKING_DURABLE_STRONG),
        || catalog_config_response(None),
    )
    .expect("catalog config should build");

    assert_eq!(
        response.overrides.get(CATALOG_BACKING_CONFIG_KEY).map(String::as_str),
        Some(crate::table_catalog::TABLE_CATALOG_BACKING_DURABLE_STRONG)
    );
    assert!(
        response
            .endpoints
            .contains(&"POST /v1/{prefix}/namespaces/{namespace}/properties")
    );
    assert!(response.endpoints.contains(&"POST /v1/{prefix}/tables/rename"));
}

#[test]
#[serial_test::serial]
fn catalog_config_response_uses_requested_warehouse_as_standard_prefix() {
    let response = temp_env::with_var_unset(crate::table_catalog::ENV_TABLE_CATALOG_BACKING, || {
        catalog_config_response(Some("analytics"))
    })
    .expect("catalog config should build");

    assert_eq!(response.defaults.get(PREFIX_PROPERTY).map(String::as_str), Some("analytics"));
}

#[test]
fn warehouse_config_query_rejects_empty_and_repeated_values() {
    let uri = "/iceberg/v1/config?warehouse=analytics".parse().expect("URI");
    assert_eq!(warehouse_from_config_query(&uri).expect("warehouse query"), Some("analytics".to_string()));

    let uri = "/iceberg/v1/config?warehouse=".parse().expect("URI");
    assert!(warehouse_from_config_query(&uri).is_err());

    let uri = "/iceberg/v1/config?warehouse=one&warehouse=two".parse().expect("URI");
    assert!(warehouse_from_config_query(&uri).is_err());
}

#[test]
fn drop_table_purge_query_is_explicit_and_strict() {
    for (uri, expected) in [
        ("/iceberg/v1/analytics/namespaces/sales/tables/orders", false),
        ("/iceberg/v1/analytics/namespaces/sales/tables/orders?purgeRequested=false", false),
        ("/iceberg/v1/analytics/namespaces/sales/tables/orders?purgeRequested=true", true),
        ("/iceberg/v1/analytics/namespaces/sales/tables/orders?purgeRequested=False", false),
        ("/iceberg/v1/analytics/namespaces/sales/tables/orders?purgeRequested=True", true),
    ] {
        assert_eq!(
            rest_purge_requested_from_query(&uri.parse().expect("URI")).expect("purge query should parse"),
            expected
        );
    }

    for uri in [
        "/iceberg/v1/analytics/namespaces/sales/tables/orders?purgeRequested=1",
        "/iceberg/v1/analytics/namespaces/sales/tables/orders?purgeRequested=true&purgeRequested=false",
    ] {
        let error = rest_purge_requested_from_query(&uri.parse().expect("URI")).expect_err("invalid purge query should fail");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
        assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));
    }
}

#[test]
fn catalog_conflicts_use_operation_specific_iceberg_errors() {
    let already_exists = catalog_store_already_exists_error(crate::table_catalog::TableCatalogStoreError::Conflict(
        "table already exists".to_string(),
    ));
    assert_eq!(already_exists.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_ALREADY_EXISTS.into()));
    assert_eq!(already_exists.status_code(), Some(StatusCode::CONFLICT));

    let namespace_not_empty = catalog_store_namespace_drop_error(crate::table_catalog::TableCatalogStoreError::Conflict(
        "namespace is not empty".to_string(),
    ));
    assert_eq!(namespace_not_empty.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_NAMESPACE_NOT_EMPTY.into()));
    assert_eq!(namespace_not_empty.status_code(), Some(StatusCode::CONFLICT));

    let namespace_not_found = catalog_store_namespace_drop_error(crate::table_catalog::TableCatalogStoreError::NotFound(
        "namespace not found".to_string(),
    ));
    assert_eq!(namespace_not_found.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_NO_SUCH_NAMESPACE.into()));
    assert_eq!(namespace_not_found.status_code(), Some(StatusCode::NOT_FOUND));

    let unsupported = catalog_store_error(crate::table_catalog::TableCatalogStoreError::Unsupported(
        "operation is unavailable".to_string(),
    ));
    assert_eq!(unsupported.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNSUPPORTED_OPERATION.into()));
    assert_eq!(unsupported.status_code(), Some(StatusCode::NOT_ACCEPTABLE));

    for (error, expected_code, expected_status) in [
        (
            crate::table_catalog::TableCatalogStoreError::NamespaceNotFound("namespace not found".to_string()),
            ICEBERG_ERROR_NO_SUCH_NAMESPACE,
            StatusCode::NOT_FOUND,
        ),
        (
            crate::table_catalog::TableCatalogStoreError::TableNotFound("table not found".to_string()),
            ICEBERG_ERROR_NO_SUCH_TABLE,
            StatusCode::NOT_FOUND,
        ),
        (
            crate::table_catalog::TableCatalogStoreError::AlreadyExists("destination exists".to_string()),
            ICEBERG_ERROR_ALREADY_EXISTS,
            StatusCode::CONFLICT,
        ),
    ] {
        let mapped = catalog_store_error(error);
        assert_eq!(mapped.code(), &S3ErrorCode::Custom(expected_code.into()));
        assert_eq!(mapped.status_code(), Some(expected_status));
    }
}

#[test]
fn table_catalog_admin_operation_result_labels_are_stable() {
    let success: Result<(), ()> = Ok(());
    let failure: Result<(), ()> = Err(());

    assert_eq!(table_catalog_admin_operation_result_label(&success), "success");
    assert_eq!(table_catalog_admin_operation_result_label(&failure), "failure");
}

#[test]
fn table_catalog_handlers_require_table_admin_actions() {
    let src = table_catalog_handler_source();

    assert!(
        operation_block(&src, "GetCatalogConfigHandler")
            .contains("authorize_table_catalog_request(&req, AdminAction::GetTableCatalogAction).await?;")
    );
    assert!(
        src.contains("validate_admin_action_with_bucket_object_for_iam("),
        "catalog resource auth should pass namespace/table scope into IAM object matching"
    );

    for (handler, action) in [
        ("EnableTableBucketHandler", "AdminAction::SetTableBucketAction"),
        ("GetTableBucketHandler", "AdminAction::GetTableBucketAction"),
        ("GetTableCatalogMigrationHandler", "AdminAction::GetTableCatalogAction"),
        ("RestListNamespacesHandler", "AdminAction::GetTableNamespaceAction"),
        ("RestCreateNamespaceHandler", "AdminAction::SetTableNamespaceAction"),
        ("RestGetNamespaceHandler", "AdminAction::GetTableNamespaceAction"),
        ("RestNamespaceExistsHandler", "AdminAction::GetTableNamespaceAction"),
        (
            "RestUpdateNamespacePropertiesHandler",
            "AdminAction::UpdateTableNamespacePropertiesAction",
        ),
        ("RestDropNamespaceHandler", "AdminAction::DeleteTableNamespaceAction"),
        ("RestListTablesHandler", "AdminAction::GetTableAction"),
        ("RestCreateTableHandler", "AdminAction::CreateTableAction"),
        ("RestRegisterTableHandler", "AdminAction::RegisterTableAction"),
        ("RestListViewsHandler", "AdminAction::GetTableMetadataAction"),
        ("RestCreateViewHandler", "AdminAction::CreateTableAction"),
        ("RestLoadTableHandler", "AdminAction::GetTableMetadataAction"),
        ("RestTableExistsHandler", "AdminAction::GetTableAction"),
        ("RestLoadCredentialsHandler", "AdminAction::GetTableCredentialsAction"),
        ("RestCommitTableHandler", "AdminAction::CommitTableAction"),
        ("RestDropTableHandler", "AdminAction::DeleteTableAction"),
        ("RestLoadViewHandler", "AdminAction::GetTableMetadataAction"),
        ("RestReplaceViewHandler", "AdminAction::CommitTableAction"),
        ("RestDropViewHandler", "AdminAction::DeleteTableAction"),
        ("ListTableRefsHandler", "AdminAction::GetTableMetadataAction"),
        ("GetTableMetadataLocationHandler", "AdminAction::GetTableMetadataLocationAction"),
        ("UpdateTableMetadataLocationHandler", "AdminAction::SetTableMetadataLocationAction"),
        ("RestTableMetadataMaintenanceHandler", "AdminAction::RunTableMaintenanceAction"),
        ("GetTableMaintenanceConfigHandler", "AdminAction::GetTableLifecycleAction"),
        ("PutTableMaintenanceConfigHandler", "AdminAction::SetTableLifecycleAction"),
        ("GetTableMaintenanceJobHandler", "AdminAction::GetTableLifecycleAction"),
        ("GetTableMaintenanceSchedulerHandler", "AdminAction::GetTableLifecycleAction"),
        ("RunTableMaintenanceSchedulerHandler", "AdminAction::RunTableMaintenanceAction"),
        ("TableMaintenanceQuarantineHandler", "AdminAction::RunTableMaintenanceAction"),
        ("ExportTableCatalogHandler", "AdminAction::GetTableMetadataAction"),
        ("ImportTableCatalogHandler", "AdminAction::RegisterTableAction"),
        ("ExternalCatalogBridgeHandler", "AdminAction::GetTableMetadataAction"),
        ("PutExternalCatalogBridgeHandler", "AdminAction::RegisterTableAction"),
        ("SyncExternalCatalogBridgeHandler", "AdminAction::SetTableMetadataLocationAction"),
        ("GetTableCatalogDiagnosticsHandler", "AdminAction::GetTableMetadataAction"),
        ("RecoverTableCatalogHandler", "AdminAction::CommitTableAction"),
        ("RollbackTableCatalogHandler", "AdminAction::CommitTableAction"),
    ] {
        let block = operation_block(&src, handler);
        assert!(
            block.contains(&format!("authorize_table_catalog_resource_request(&req, &resource, {action})")),
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

    let sync_bridge_block = operation_block(&src, "SyncExternalCatalogBridgeHandler");
    assert!(
        sync_bridge_block.contains("AdminAction::RegisterTableAction"),
        "external catalog sync should require register authorization before creating a missing table"
    );
    assert!(
        sync_bridge_block.contains(".load_table(&warehouse, &namespace.public_name(), &table)"),
        "external catalog sync should branch authorization on current table existence"
    );

    let rename_block = operation_block(&src, "RestRenameTableHandler");
    assert_eq!(rename_block.matches("table_catalog_request_principal(&req).await?;").count(), 1);
    assert_eq!(
        rename_block
            .matches("authorize_table_catalog_resource_for_principal(")
            .count(),
        2
    );
    assert_eq!(rename_block.matches("AdminAction::SetTableAction").count(), 2);

    let migration_block = operation_block(&src, "GetTableCatalogMigrationHandler");
    assert!(
        migration_block.contains("TableCatalogResource::warehouse(&warehouse)"),
        "catalog migration dry-run should authorize against the warehouse resource"
    );
    for handler in [
        "MaterializeTableCatalogMigrationHandler",
        "CancelTableCatalogMigrationHandler",
    ] {
        let block = operation_block(&src, handler);
        assert!(
            block.contains("authorize_table_catalog_request(&req, AdminAction::MigrateTableCatalogAction).await?;"),
            "{handler} should require the global catalog migration action"
        );
        assert!(
            !block.contains("authorize_table_catalog_resource_request("),
            "{handler} must not imply that a global backing cutover is warehouse-scoped"
        );
    }

    for (handler, action) in [
        ("RestLoadTableHandler", "AdminAction::GetTableMetadataAction"),
        ("RestTableExistsHandler", "AdminAction::GetTableAction"),
        ("RestLoadCredentialsHandler", "AdminAction::GetTableCredentialsAction"),
        ("RestCommitTableHandler", "AdminAction::CommitTableAction"),
        ("RestDropTableHandler", "AdminAction::DeleteTableAction"),
        ("ListTableRefsHandler", "AdminAction::GetTableMetadataAction"),
        ("GetTableMetadataLocationHandler", "AdminAction::GetTableMetadataLocationAction"),
        ("UpdateTableMetadataLocationHandler", "AdminAction::SetTableMetadataLocationAction"),
        ("RestTableMetadataMaintenanceHandler", "AdminAction::RunTableMaintenanceAction"),
        ("GetTableMaintenanceConfigHandler", "AdminAction::GetTableLifecycleAction"),
        ("PutTableMaintenanceConfigHandler", "AdminAction::SetTableLifecycleAction"),
        ("GetTableMaintenanceJobHandler", "AdminAction::GetTableLifecycleAction"),
        ("GetTableMaintenanceSchedulerHandler", "AdminAction::GetTableLifecycleAction"),
        ("RunTableMaintenanceSchedulerHandler", "AdminAction::RunTableMaintenanceAction"),
        ("TableMaintenanceQuarantineHandler", "AdminAction::RunTableMaintenanceAction"),
        ("ExportTableCatalogHandler", "AdminAction::GetTableMetadataAction"),
        ("ImportTableCatalogHandler", "AdminAction::RegisterTableAction"),
        ("ExternalCatalogBridgeHandler", "AdminAction::GetTableMetadataAction"),
        ("PutExternalCatalogBridgeHandler", "AdminAction::RegisterTableAction"),
        ("SyncExternalCatalogBridgeHandler", "AdminAction::SetTableMetadataLocationAction"),
        ("GetTableCatalogDiagnosticsHandler", "AdminAction::GetTableMetadataAction"),
        ("RecoverTableCatalogHandler", "AdminAction::CommitTableAction"),
        ("RollbackTableCatalogHandler", "AdminAction::CommitTableAction"),
    ] {
        let block = operation_block(&src, handler);
        assert!(
            block.contains("TableCatalogResource::table(&warehouse, &namespace, &table)"),
            "{handler} should build a table-aware catalog resource"
        );
        assert!(
            block.contains(&format!("authorize_table_catalog_resource_request(&req, &resource, {action})")),
            "{handler} should authorize against the table-aware catalog resource"
        );
    }
}

#[test]
fn standard_rest_handlers_wire_strict_response_guards() {
    let src = table_catalog_handler_source();
    let drop_table = operation_block(&src, "RestDropTableHandler");
    assert!(drop_table.contains("rest_purge_requested_from_query(&req.uri)?"));
    assert!(drop_table.contains("if purge_requested"));
    assert!(drop_table.contains("StatusCode::NOT_ACCEPTABLE"));

    let load_table = operation_block(&src, "RestLoadTableHandler");
    assert!(load_table.contains("rest_table_snapshot_selection_from_query(&req.uri)?"));
    assert!(load_table.contains("apply_rest_table_snapshot_selection(&mut response.metadata, snapshot_selection);"));

    let credentials = operation_block(&src, "RestLoadCredentialsHandler");
    assert!(credentials.contains("build_sensitive_json_response(StatusCode::OK, &response)"));
}

#[test]
fn table_bucket_handlers_resolve_state_from_the_request_context() {
    let src = table_catalog_handler_source();
    let enable = operation_block(&src, "EnableTableBucketHandler");
    assert!(enable.contains("table_catalog_backend_from_extensions(&req.extensions)?;"));
    assert!(enable.contains("runtime_sources::object_store_from_req(&req)"));

    let get = operation_block(&src, "GetTableBucketHandler");
    assert!(get.contains("table_catalog_store_from_extensions(&req.extensions)?;"));
    assert!(get.contains("table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;"));
}

#[test]
fn table_catalog_list_handlers_parse_standard_pagination() {
    let src = table_catalog_handler_source();
    for (handler, helper_call) in [
        (
            "RestListNamespacesHandler",
            "list_namespaces_response(&store, &warehouse, parent.as_ref(), &req.uri).await?",
        ),
        (
            "RestListTablesHandler",
            "list_tables_response(&store, &warehouse, &namespace, &req.uri).await?",
        ),
        (
            "RestListViewsHandler",
            "list_views_response(&store, &warehouse, &namespace, &req.uri).await?",
        ),
    ] {
        let block = operation_block(&src, handler);
        assert!(
            block.contains(helper_call),
            "{handler} should pass the request URI to its paginated list helper"
        );
    }
}

#[test]
fn namespace_write_handlers_bound_request_bodies() {
    let src = table_catalog_handler_source();
    for handler in ["RestCreateNamespaceHandler", "RestUpdateNamespacePropertiesHandler"] {
        let block = operation_block(&src, handler);
        assert!(
            block.contains("read_bounded_json_body::<"),
            "{handler} should enforce the namespace request body limit and timeout"
        );
    }
}

#[test]
fn table_pointer_write_handlers_install_commit_publication_guard() {
    let src = table_catalog_handler_source();
    for handler in [
        "RestRegisterTableHandler",
        "RestCommitTableHandler",
        "UpdateTableMetadataLocationHandler",
        "ImportTableCatalogHandler",
        "PutTableRefHandler",
        "DeleteTableRefHandler",
        "RestReplaceViewHandler",
        "RollbackTableCatalogHandler",
        "SyncExternalCatalogBridgeHandler",
    ] {
        let block = operation_block(&src, handler);
        assert!(
            block.contains("install_table_catalog_s3_request_info(&mut req, &principal)?;"),
            "{handler} should install exact-key S3 authorization context"
        );
        assert!(
            block.contains("TableCommitObjectBackend::for_request(metadata_backend, req)"),
            "{handler} should use the guarded table commit backend"
        );
        assert!(
            block.contains("commit_backend.finish(result).await?"),
            "{handler} should preserve exact-key authorization errors"
        );
    }
}

#[test]
fn table_catalog_handlers_require_enabled_table_bucket_marker_before_catalog_state() {
    let src = table_catalog_handler_source();

    for handler in [
        "GetTableCatalogMigrationHandler",
        "MaterializeTableCatalogMigrationHandler",
        "CancelTableCatalogMigrationHandler",
        "RestListNamespacesHandler",
        "RestCreateNamespaceHandler",
        "RestGetNamespaceHandler",
        "RestNamespaceExistsHandler",
        "RestUpdateNamespacePropertiesHandler",
        "RestDropNamespaceHandler",
        "RestRenameTableHandler",
        "RestListTablesHandler",
        "RestCreateTableHandler",
        "RestRegisterTableHandler",
        "RestListViewsHandler",
        "RestCreateViewHandler",
        "RestLoadTableHandler",
        "RestTableExistsHandler",
        "RestLoadCredentialsHandler",
        "RestCommitTableHandler",
        "RestDropTableHandler",
        "RestLoadViewHandler",
        "RestViewExistsHandler",
        "RestReplaceViewHandler",
        "RestDropViewHandler",
        "ListTableRefsHandler",
        "PutTableRefHandler",
        "DeleteTableRefHandler",
        "GetTableMetadataLocationHandler",
        "UpdateTableMetadataLocationHandler",
        "RestTableMetadataMaintenanceHandler",
        "GetTableMaintenanceConfigHandler",
        "PutTableMaintenanceConfigHandler",
        "GetTableMaintenanceJobHandler",
        "GetTableMaintenanceSchedulerHandler",
        "RunTableMaintenanceSchedulerHandler",
        "RunTableMaintenanceWorkerHandler",
        "HeartbeatTableMaintenanceJobHandler",
        "TableMaintenanceQuarantineHandler",
        "ExportTableCatalogHandler",
        "ImportTableCatalogHandler",
        "ExternalCatalogBridgeHandler",
        "PutExternalCatalogBridgeHandler",
        "SyncExternalCatalogBridgeHandler",
        "GetTableCatalogDiagnosticsHandler",
        "RecoverTableCatalogHandler",
        "RollbackTableCatalogHandler",
    ] {
        let block = operation_block(&src, handler);
        assert!(
            block.contains("ensure_table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;")
                || block.contains("table_bucket_enabled_from_extensions(&req.extensions, &warehouse).await?;"),
            "{handler} should require the table bucket metadata marker before catalog state access"
        );
    }
}

#[test]
fn enable_table_bucket_response_fences_before_marker_and_catalog_entry() {
    let src = table_catalog_handler_source();
    let block = function_block(&src, "async fn enable_table_bucket_response");
    let publication_fence = block
        .find("TableCommitPublication::begin_table_bucket(publication, bucket)")
        .expect("enable should acquire the table-bucket publication fence");
    let marker_write = block
        .find("enable_table_bucket_marker(object_store, bucket).await?;")
        .expect("enable should write the metadata marker");
    let catalog_entry_write = block
        .find("ensure_table_bucket_entry(store, bucket, true).await?;")
        .expect("enable should write the catalog entry");

    assert!(
        publication_fence < marker_write && marker_write < catalog_entry_write,
        "enable should fence object mutations before writing the metadata marker and catalog entry"
    );
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

fn table_catalog_handler_source() -> String {
    [
        include_str!("mod.rs"),
        include_str!("config.rs"),
        include_str!("credentials.rs"),
        include_str!("maintenance.rs"),
        include_str!("namespace.rs"),
        include_str!("refs.rs"),
        include_str!("routes.rs"),
        include_str!("table.rs"),
        include_str!("view.rs"),
    ]
    .join("\n")
}

fn function_block<'a>(src: &'a str, signature: &str) -> &'a str {
    let block = src.split_once(signature).expect("function should exist").1;
    let end = block
        .find("\nfn ")
        .or_else(|| block.find("\nasync fn "))
        .unwrap_or(block.len());
    &block[..end]
}

#[test]
fn rest_catalog_mvp_routes_use_implemented_handlers() {
    fn assert_operation<T: Operation>() {}

    let _: &EnableTableBucketHandler = &ENABLE_TABLE_BUCKET_HANDLER;
    let _: &GetTableBucketHandler = &GET_TABLE_BUCKET_HANDLER;
    let _: &GetTableCatalogMigrationHandler = &GET_TABLE_CATALOG_MIGRATION_HANDLER;
    let _: &MaterializeTableCatalogMigrationHandler = &MATERIALIZE_TABLE_CATALOG_MIGRATION_HANDLER;
    let _: &CancelTableCatalogMigrationHandler = &CANCEL_TABLE_CATALOG_MIGRATION_HANDLER;
    let _: &RestListNamespacesHandler = &LIST_NAMESPACES_HANDLER;
    let _: &RestCreateNamespaceHandler = &CREATE_NAMESPACE_HANDLER;
    let _: &RestGetNamespaceHandler = &GET_NAMESPACE_HANDLER;
    let _: &RestNamespaceExistsHandler = &NAMESPACE_EXISTS_HANDLER;
    let _: &RestUpdateNamespacePropertiesHandler = &UPDATE_NAMESPACE_PROPERTIES_HANDLER;
    let _: &RestDropNamespaceHandler = &DROP_NAMESPACE_HANDLER;
    let _: &RestListTablesHandler = &LIST_TABLES_HANDLER;
    let _: &RestCreateTableHandler = &CREATE_TABLE_HANDLER;
    let _: &RestRegisterTableHandler = &REGISTER_TABLE_HANDLER;
    let _: &RestListViewsHandler = &LIST_VIEWS_HANDLER;
    let _: &RestCreateViewHandler = &CREATE_VIEW_HANDLER;
    let _: &RestLoadTableHandler = &LOAD_TABLE_HANDLER;
    let _: &RestTableExistsHandler = &TABLE_EXISTS_HANDLER;
    let _: &RestLoadCredentialsHandler = &LOAD_CREDENTIALS_HANDLER;
    let _: &RestCommitTableHandler = &COMMIT_TABLE_HANDLER;
    let _: &RestDropTableHandler = &DROP_TABLE_HANDLER;
    let _: &RestRenameTableHandler = &RENAME_TABLE_HANDLER;
    let _: &RestLoadViewHandler = &LOAD_VIEW_HANDLER;
    let _: &RestReplaceViewHandler = &REPLACE_VIEW_HANDLER;
    let _: &RestDropViewHandler = &DROP_VIEW_HANDLER;
    let _: &ListTableRefsHandler = &LIST_TABLE_REFS_HANDLER;
    let _: &GetTableMetadataLocationHandler = &GET_TABLE_METADATA_LOCATION_HANDLER;
    let _: &UpdateTableMetadataLocationHandler = &UPDATE_TABLE_METADATA_LOCATION_HANDLER;
    let _: &RestTableMetadataMaintenanceHandler = &TABLE_METADATA_MAINTENANCE_HANDLER;
    let _: &GetTableMaintenanceConfigHandler = &GET_TABLE_MAINTENANCE_CONFIG_HANDLER;
    let _: &PutTableMaintenanceConfigHandler = &PUT_TABLE_MAINTENANCE_CONFIG_HANDLER;
    let _: &GetTableMaintenanceJobHandler = &GET_TABLE_MAINTENANCE_JOB_HANDLER;
    let _: &GetTableMaintenanceSchedulerHandler = &GET_TABLE_MAINTENANCE_SCHEDULER_HANDLER;
    let _: &RunTableMaintenanceSchedulerHandler = &RUN_TABLE_MAINTENANCE_SCHEDULER_HANDLER;
    let _: &TableMaintenanceQuarantineHandler = &TABLE_MAINTENANCE_QUARANTINE_HANDLER;
    let _: &ExportTableCatalogHandler = &EXPORT_TABLE_CATALOG_HANDLER;
    let _: &ImportTableCatalogHandler = &IMPORT_TABLE_CATALOG_HANDLER;
    let _: &ExternalCatalogBridgeHandler = &EXTERNAL_CATALOG_BRIDGE_HANDLER;
    let _: &PutExternalCatalogBridgeHandler = &PUT_EXTERNAL_CATALOG_BRIDGE_HANDLER;
    let _: &SyncExternalCatalogBridgeHandler = &SYNC_EXTERNAL_CATALOG_BRIDGE_HANDLER;
    let _: &GetTableCatalogDiagnosticsHandler = &GET_TABLE_CATALOG_DIAGNOSTICS_HANDLER;
    let _: &RecoverTableCatalogHandler = &RECOVER_TABLE_CATALOG_HANDLER;
    let _: &RollbackTableCatalogHandler = &ROLLBACK_TABLE_CATALOG_HANDLER;

    assert_operation::<EnableTableBucketHandler>();
    assert_operation::<GetTableBucketHandler>();
    assert_operation::<GetTableCatalogMigrationHandler>();
    assert_operation::<MaterializeTableCatalogMigrationHandler>();
    assert_operation::<CancelTableCatalogMigrationHandler>();
    assert_operation::<RestListNamespacesHandler>();
    assert_operation::<RestCreateNamespaceHandler>();
    assert_operation::<RestGetNamespaceHandler>();
    assert_operation::<RestNamespaceExistsHandler>();
    assert_operation::<RestUpdateNamespacePropertiesHandler>();
    assert_operation::<RestDropNamespaceHandler>();
    assert_operation::<RestListTablesHandler>();
    assert_operation::<RestCreateTableHandler>();
    assert_operation::<RestRegisterTableHandler>();
    assert_operation::<RestListViewsHandler>();
    assert_operation::<RestCreateViewHandler>();
    assert_operation::<RestLoadTableHandler>();
    assert_operation::<RestTableExistsHandler>();
    assert_operation::<RestLoadCredentialsHandler>();
    assert_operation::<RestCommitTableHandler>();
    assert_operation::<RestDropTableHandler>();
    assert_operation::<RestRenameTableHandler>();
    assert_operation::<RestLoadViewHandler>();
    assert_operation::<RestReplaceViewHandler>();
    assert_operation::<RestDropViewHandler>();
    assert_operation::<ListTableRefsHandler>();
    assert_operation::<GetTableMetadataLocationHandler>();
    assert_operation::<UpdateTableMetadataLocationHandler>();
    assert_operation::<RestTableMetadataMaintenanceHandler>();
    assert_operation::<GetTableMaintenanceConfigHandler>();
    assert_operation::<PutTableMaintenanceConfigHandler>();
    assert_operation::<GetTableMaintenanceJobHandler>();
    assert_operation::<GetTableMaintenanceSchedulerHandler>();
    assert_operation::<RunTableMaintenanceSchedulerHandler>();
    assert_operation::<RunTableMaintenanceWorkerHandler>();
    assert_operation::<HeartbeatTableMaintenanceJobHandler>();
    assert_operation::<TableMaintenanceQuarantineHandler>();
    assert_operation::<ExportTableCatalogHandler>();
    assert_operation::<ImportTableCatalogHandler>();
    assert_operation::<ExternalCatalogBridgeHandler>();
    assert_operation::<PutExternalCatalogBridgeHandler>();
    assert_operation::<SyncExternalCatalogBridgeHandler>();
    assert_operation::<GetTableCatalogDiagnosticsHandler>();
    assert_operation::<RecoverTableCatalogHandler>();
    assert_operation::<RollbackTableCatalogHandler>();
}

#[test]
fn table_metadata_maintenance_request_uses_conservative_defaults() {
    let request: TableMetadataMaintenanceRequest =
        serde_json::from_value(serde_json::json!({})).expect("default maintenance request should parse");

    assert_eq!(request.retain_recent_metadata_files, 0);
    assert!(!request.delete);
    assert!(request.snapshot_expiration.is_none());
    assert!(!request.commit_snapshot_expiration);
    assert!(request.compaction.is_none());
    assert!(!request.commit_compaction);
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
    assert!(request.snapshot_expiration.is_none());
    assert!(!request.commit_snapshot_expiration);
    assert!(request.compaction.is_none());
    assert!(!request.commit_compaction);
}

#[test]
fn table_metadata_maintenance_request_accepts_snapshot_and_compaction_plans() {
    let request: TableMetadataMaintenanceRequest = serde_json::from_value(serde_json::json!({
        "commit-snapshot-expiration": true,
        "snapshot-expiration": {
            "min-snapshots-to-keep": 2,
            "max-snapshot-age-ms": 3600000
        },
        "commit-compaction": true,
        "compaction": {
            "target-file-size-bytes": 536870912,
            "small-file-threshold-bytes": 67108864,
            "min-input-files": 5,
            "max-rewrite-bytes-per-job": 10737418240u64
        }
    }))
    .expect("metadata maintenance request should parse maintenance planning config");

    let snapshot_expiration = request
        .snapshot_expiration
        .expect("snapshot expiration config should be present");
    assert_eq!(snapshot_expiration.min_snapshots_to_keep, 2);
    assert_eq!(snapshot_expiration.max_snapshot_age_ms, 3_600_000);
    assert!(request.commit_snapshot_expiration);
    assert!(request.commit_compaction);
    let compaction = request.compaction.expect("compaction config should be present");
    assert_eq!(compaction.target_file_size_bytes, 536_870_912);
    assert_eq!(compaction.small_file_threshold_bytes, 67_108_864);
    assert_eq!(compaction.min_input_files, 5);
    assert_eq!(compaction.max_rewrite_bytes_per_job, 10_737_418_240);
}

#[test]
fn table_maintenance_scheduler_run_request_uses_stable_default_scheduler_id() {
    let request: TableMaintenanceSchedulerRunRequest =
        serde_json::from_value(serde_json::json!({})).expect("scheduler run request should parse");

    assert_eq!(request.scheduler_id(), "rustfs-maintenance-scheduler");
}

#[tokio::test]
async fn table_maintenance_scheduler_run_request_empty_body_uses_default_scheduler_id() {
    let request: TableMaintenanceSchedulerRunRequest = read_json_body_or_default(Body::empty())
        .await
        .expect("bodyless scheduler run should use the default scheduler id");

    assert_eq!(request.scheduler_id(), "rustfs-maintenance-scheduler");
}

#[test]
fn table_maintenance_scheduler_run_request_accepts_scheduler_id() {
    let request: TableMaintenanceSchedulerRunRequest = serde_json::from_value(serde_json::json!({
        "scheduler-id": "scheduler-a"
    }))
    .expect("scheduler run request should parse scheduler id");

    assert_eq!(request.scheduler_id(), "scheduler-a");
}

#[test]
fn table_maintenance_worker_run_request_uses_stable_default_worker_id() {
    let request: TableMaintenanceWorkerRunRequest =
        serde_json::from_value(serde_json::json!({})).expect("worker run request should parse");

    assert_eq!(request.worker_id(), "rustfs-maintenance-worker");
}

#[test]
fn table_maintenance_worker_run_request_accepts_worker_id() {
    let request: TableMaintenanceWorkerRunRequest = serde_json::from_value(serde_json::json!({
        "worker-id": "worker-a"
    }))
    .expect("worker run request should parse worker id");

    assert_eq!(request.worker_id(), "worker-a");
}

#[test]
fn table_maintenance_heartbeat_request_requires_lease_id() {
    let err = serde_json::from_value::<TableMaintenanceHeartbeatRequest>(serde_json::json!({
        "worker-id": "worker-a"
    }))
    .expect_err("heartbeat request should require lease id");

    assert!(err.to_string().contains("lease-id"));
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
    assert_rejects_unknown_field::<UpdateNamespacePropertiesRequest>(
        "UpdateNamespacePropertiesRequest",
        serde_json::json!({
            "updates": {},
            "unexpected": true
        }),
    );
    assert_rejects_unknown_field::<RenameTableRequest>(
        "RenameTableRequest",
        serde_json::json!({
            "source": {"namespace": ["analytics"], "name": "events"},
            "destination": {"namespace": ["curated"], "name": "events_v2"},
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
    assert_rejects_unknown_field::<ExternalCatalogBridgeRequest>(
        "ExternalCatalogBridgeRequest",
        serde_json::json!({
            "catalog": "polaris",
            "external-namespace": "analytics",
            "external-table": "events",
            "unexpected": true
        }),
    );
    assert_rejects_unknown_field::<ExternalCatalogBridgeSyncRequest>(
        "ExternalCatalogBridgeSyncRequest",
        serde_json::json!({
            "catalog": "glue",
            "external-namespace": "analytics",
            "external-table": "events",
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

#[test]
fn rename_table_request_uses_standard_identifiers_and_strict_serde() {
    let request: RenameTableRequest = serde_json::from_value(serde_json::json!({
        "source": {"namespace": ["analytics", "raw"], "name": "events"},
        "destination": {"namespace": ["analytics", "curated"], "name": "events_v2"}
    }))
    .expect("rename request should parse");
    assert_eq!(request.source.namespace, vec!["analytics", "raw"]);
    assert_eq!(request.source.name, "events");
    assert_eq!(request.destination.namespace, vec!["analytics", "curated"]);
    assert_eq!(request.destination.name, "events_v2");

    assert_rejects_unknown_field::<RenameTableRequest>(
        "RenameTableRequest.source",
        serde_json::json!({
            "source": {"namespace": ["analytics"], "name": "events", "unexpected": true},
            "destination": {"namespace": ["curated"], "name": "events_v2"}
        }),
    );
}

#[tokio::test]
async fn rename_table_body_rejects_declared_and_streamed_oversize_payloads() {
    let mut oversized_headers = HeaderMap::new();
    oversized_headers.insert(
        http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&(RENAME_TABLE_BODY_MAX_SIZE + 1).to_string()).expect("content length should parse"),
    );
    let declared = read_bounded_json_body::<RenameTableRequest>(
        &oversized_headers,
        Body::empty(),
        RENAME_TABLE_BODY_MAX_SIZE,
        RENAME_TABLE_BODY_TIMEOUT,
        "rename table",
    )
    .await
    .expect_err("oversized declared body should fail before reading");
    assert_eq!(declared.code(), &S3ErrorCode::InvalidRequest);

    let streamed = read_bounded_json_body::<RenameTableRequest>(
        &HeaderMap::new(),
        Body::from(vec![b' '; RENAME_TABLE_BODY_MAX_SIZE + 1]),
        RENAME_TABLE_BODY_MAX_SIZE,
        RENAME_TABLE_BODY_TIMEOUT,
        "rename table",
    )
    .await
    .expect_err("oversized streamed body should fail");
    assert_eq!(streamed.code(), &S3ErrorCode::InvalidRequest);
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
    assert!(namespace_from_segments(&["analytics.daily_events".to_string()]).is_err());
}

#[tokio::test]
async fn invalid_namespace_properties_fail_before_catalog_state_changes() {
    let store = TestTableCatalogStore::default();
    let error = create_namespace_response(
        &store,
        "warehouse",
        CreateNamespaceRequest {
            namespace: vec!["analytics".to_string()],
            properties: BTreeMap::from([(
                "owner".to_string(),
                "x".repeat(crate::table_catalog::NAMESPACE_PROPERTY_VALUE_MAX_LEN + 1),
            )]),
        },
        true,
    )
    .await
    .expect_err("oversized namespace property must fail");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
    assert!(store.table_buckets.lock().await.is_empty());
    assert!(store.namespaces.lock().await.is_empty());
}

#[test]
fn namespace_parent_query_accepts_standard_and_legacy_path_separators() {
    let path_namespace = namespace_from_path_value("accounting%1Ftax").expect("encoded path namespace should parse");
    assert_eq!(path_namespace.public_name(), "accounting.tax");
    let lowercase_path_namespace =
        namespace_from_path_value("accounting%1ftax").expect("lowercase encoded path namespace should parse");
    assert_eq!(lowercase_path_namespace.public_name(), "accounting.tax");
    let legacy_path_namespace = namespace_from_path_value("accounting.tax").expect("legacy dotted path namespace should parse");
    assert_eq!(legacy_path_namespace.public_name(), "accounting.tax");
    assert!(namespace_from_path_value("accounting%2Etax").is_err());
    assert!(namespace_from_path_value("accounting.tax%2Epaid").is_err());
    assert!(namespace_from_path_value("accounting%2Ftax").is_err());
    assert!(namespace_from_path_value("%FF").is_err());

    let uri = "/iceberg/v1/analytics/namespaces?parent=accounting%1Ftax"
        .parse()
        .expect("URI");
    let parent = rest_namespace_parent_from_query(&uri)
        .expect("parent query should parse")
        .expect("parent should be present");
    assert_eq!(parent.public_name(), "accounting.tax");

    let uri = "/iceberg/v1/analytics/namespaces?parent=".parse().expect("URI");
    assert!(
        rest_namespace_parent_from_query(&uri)
            .expect("empty parent should parse")
            .is_none()
    );

    let uri = "/iceberg/v1/analytics/namespaces?parent=one&parent=two".parse().expect("URI");
    let error = rest_namespace_parent_from_query(&uri).expect_err("repeated parent should fail");
    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
    assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));
}

#[test]
fn namespace_property_update_uses_standard_shape_and_rejects_invalid_key_sets() {
    let request: UpdateNamespacePropertiesRequest = serde_json::from_value(serde_json::json!({
        "removals": ["retention"],
        "updates": {"owner": "platform"}
    }))
    .expect("namespace property update should parse");
    let update = namespace_properties_update_from_request(request).expect("disjoint property update should validate");
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let mut entry = crate::table_catalog::NamespaceEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: "warehouse".to_string(),
        namespace: namespace.public_name(),
        namespace_id: namespace.storage_id(),
        state: crate::table_catalog::TableCatalogEntryState::Active,
        properties: BTreeMap::from([("retention".to_string(), "30d".to_string())]),
        created_at: None,
        updated_at: None,
    };
    let result = update.apply_to(&mut entry);
    assert_eq!(result.removed, vec!["retention".to_string()]);
    assert_eq!(result.updated, vec!["owner".to_string()]);
    assert_eq!(entry.properties.get("owner").map(String::as_str), Some("platform"));

    let duplicate = namespace_properties_update_from_request(UpdateNamespacePropertiesRequest {
        removals: vec!["owner".to_string(), "owner".to_string()],
        updates: BTreeMap::new(),
    })
    .expect_err("duplicate removals should fail");
    assert_eq!(duplicate.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
    assert_eq!(duplicate.status_code(), Some(StatusCode::BAD_REQUEST));

    let overlap = namespace_properties_update_from_request(UpdateNamespacePropertiesRequest {
        removals: vec!["owner".to_string()],
        updates: BTreeMap::from([("owner".to_string(), "platform".to_string())]),
    })
    .expect_err("overlapping property sets should fail");
    assert_eq!(overlap.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNPROCESSABLE_ENTITY.into()));
    assert_eq!(overlap.status_code(), Some(StatusCode::UNPROCESSABLE_ENTITY));
}

#[tokio::test]
async fn namespace_property_update_body_is_bounded_and_required() {
    let mut oversized_headers = HeaderMap::new();
    oversized_headers.insert(
        http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&(NAMESPACE_REQUEST_BODY_MAX_SIZE + 1).to_string()).expect("content length should parse"),
    );
    let error = read_bounded_json_body::<UpdateNamespacePropertiesRequest>(
        &oversized_headers,
        Body::empty(),
        NAMESPACE_REQUEST_BODY_MAX_SIZE,
        NAMESPACE_REQUEST_BODY_TIMEOUT,
        "namespace properties",
    )
    .await
    .expect_err("oversized declared body should fail before reading");
    assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);

    let error = read_bounded_json_body::<UpdateNamespacePropertiesRequest>(
        &HeaderMap::new(),
        Body::from(vec![b' '; NAMESPACE_REQUEST_BODY_MAX_SIZE + 1]),
        NAMESPACE_REQUEST_BODY_MAX_SIZE,
        NAMESPACE_REQUEST_BODY_TIMEOUT,
        "namespace properties",
    )
    .await
    .expect_err("oversized streamed body should fail");
    assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);

    let error = read_bounded_json_body::<UpdateNamespacePropertiesRequest>(
        &HeaderMap::new(),
        Body::empty(),
        NAMESPACE_REQUEST_BODY_MAX_SIZE,
        NAMESPACE_REQUEST_BODY_TIMEOUT,
        "namespace properties",
    )
    .await
    .expect_err("empty body should fail");
    assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);

    let request = read_bounded_json_body::<UpdateNamespacePropertiesRequest>(
        &HeaderMap::new(),
        Body::from(r#"{"updates":{"owner":"platform"}}"#.to_string()),
        NAMESPACE_REQUEST_BODY_MAX_SIZE,
        NAMESPACE_REQUEST_BODY_TIMEOUT,
        "namespace properties",
    )
    .await
    .expect("bounded request should parse");
    assert_eq!(request.updates.get("owner").map(String::as_str), Some("platform"));

    let mut maximum_properties = BTreeMap::new();
    for index in 0..15 {
        maximum_properties.insert(format!("k{index:02}"), "v".repeat(crate::table_catalog::NAMESPACE_PROPERTY_VALUE_MAX_LEN));
    }
    let used = maximum_properties
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum::<usize>();
    let final_key = "k15".to_string();
    maximum_properties.insert(
        final_key.clone(),
        "v".repeat(crate::table_catalog::NAMESPACE_PROPERTIES_MAX_TOTAL_BYTES - used - final_key.len()),
    );
    let maximum_body = serde_json::to_vec(&serde_json::json!({"updates": maximum_properties}))
        .expect("maximum namespace properties should encode");
    assert!(maximum_body.len() > crate::table_catalog::NAMESPACE_PROPERTIES_MAX_TOTAL_BYTES);
    assert!(maximum_body.len() < NAMESPACE_REQUEST_BODY_MAX_SIZE);
    let request = read_bounded_json_body::<UpdateNamespacePropertiesRequest>(
        &HeaderMap::new(),
        Body::from(maximum_body),
        NAMESPACE_REQUEST_BODY_MAX_SIZE,
        NAMESPACE_REQUEST_BODY_TIMEOUT,
        "namespace properties",
    )
    .await
    .expect("maximum valid namespace properties body should parse");
    crate::table_catalog::validate_namespace_properties(&request.updates)
        .expect("maximum valid namespace properties should remain within the domain limit");
}

#[tokio::test(start_paused = true)]
async fn generic_json_body_readers_time_out_stalled_streams() {
    let required_stream = futures::stream::pending::<Result<http_body::Frame<Bytes>, std::io::Error>>();
    let required = tokio::spawn(read_json_body::<serde_json::Value>(Body::http_body(http_body_util::StreamBody::new(
        required_stream,
    ))));
    let optional_stream = futures::stream::pending::<Result<http_body::Frame<Bytes>, std::io::Error>>();
    let optional = tokio::spawn(read_json_body_or_default::<serde_json::Value>(Body::http_body(
        http_body_util::StreamBody::new(optional_stream),
    )));
    tokio::task::yield_now().await;
    tokio::time::advance(TABLE_CATALOG_REQUEST_BODY_TIMEOUT).await;

    let required_error = required
        .await
        .expect("required body task should complete")
        .expect_err("stalled required body should time out");
    let optional_error = optional
        .await
        .expect("optional body task should complete")
        .expect_err("stalled optional body should time out");

    assert_eq!(required_error.code(), &S3ErrorCode::InvalidRequest);
    assert_eq!(optional_error.code(), &S3ErrorCode::InvalidRequest);
}

#[test]
fn list_tables_response_uses_rest_identifier_shape() {
    let namespace = crate::table_catalog::Namespace::parse("analytics.daily_events").expect("namespace should parse");
    let response = list_tables_response_from_entries(
        vec![crate::table_catalog::TableEntry {
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
        }],
        None,
    )
    .expect("table list response should build");

    assert_eq!(
        response.identifiers[0].namespace,
        vec!["analytics".to_string(), "daily_events".to_string()]
    );
    assert_eq!(response.identifiers[0].name, "events");
    assert!(response.next_page_token.is_none());
}

#[test]
fn rest_pagination_round_trips_context_bound_tokens() {
    let context = RestPageContext {
        resource: TABLE_CATALOG_TABLE_RESOURCE_ROOT,
        warehouse: "warehouse",
        namespace: Some("analytics"),
    };
    let first_request = "/?pageSize=2".parse::<http::Uri>().expect("first page URI should parse");
    let first_pagination = rest_pagination_from_query(&first_request, context).expect("pageSize should start pagination");
    let (cursor, limit) = first_pagination.page_request().expect("pageSize should enable pagination");
    assert_eq!(cursor, None);
    assert_eq!(limit.get(), 2);
    let next_page_token = first_pagination
        .next_page_token(Some("strong:beta".to_string()))
        .expect("page token should encode")
        .expect("page token should be present");
    let second_request = format!("/?pageSize=2&pageToken={next_page_token}")
        .parse::<http::Uri>()
        .expect("second page URI should parse");
    let second_pagination = rest_pagination_from_query(&second_request, context).expect("continuation token should decode");
    let (cursor, limit) = second_pagination
        .page_request()
        .expect("continuation should remain paginated");
    assert_eq!(cursor, Some("strong:beta"));
    assert_eq!(limit.get(), 2);
    assert!(
        second_pagination
            .next_page_token(None)
            .expect("terminal token should build")
            .is_none()
    );

    let default_size_request = format!("/?pageToken={next_page_token}")
        .parse::<http::Uri>()
        .expect("continuation URI should parse without pageSize");
    let default_size_pagination =
        rest_pagination_from_query(&default_size_request, context).expect("continuation should use the default page size");
    let (cursor, limit) = default_size_pagination
        .page_request()
        .expect("continuation should remain paginated");
    assert_eq!(cursor, Some("strong:beta"));
    assert_eq!(limit.get(), REST_DEFAULT_PAGE_SIZE);

    for other_context in [
        RestPageContext {
            resource: TABLE_CATALOG_VIEW_RESOURCE_ROOT,
            warehouse: "warehouse",
            namespace: Some("analytics"),
        },
        RestPageContext {
            resource: TABLE_CATALOG_TABLE_RESOURCE_ROOT,
            warehouse: "other-warehouse",
            namespace: Some("analytics"),
        },
        RestPageContext {
            resource: TABLE_CATALOG_TABLE_RESOURCE_ROOT,
            warehouse: "warehouse",
            namespace: Some("other-namespace"),
        },
    ] {
        let expected_context = rest_page_context_fingerprint(other_context);
        let error = decode_rest_page_token(&next_page_token, &expected_context).expect_err("cross-context token should fail");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
        assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));
    }
}

#[test]
fn rest_pagination_rejects_invalid_query_parameters() {
    let context = RestPageContext {
        resource: TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT,
        warehouse: "warehouse",
        namespace: None,
    };
    for uri in [
        "/?pageSize=0",
        "/?pageSize=one",
        "/?pageSize=1&pageSize=2",
        "/?pageToken=first&pageToken=second",
    ] {
        let uri = uri.parse::<http::Uri>().expect("invalid pagination URI should still parse");
        let error = rest_pagination_from_query(&uri, context).expect_err("invalid pagination query should fail");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()), "{uri}");
        assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST), "{uri}");
    }

    let oversized_token = "a".repeat(REST_PAGE_TOKEN_MAX_LENGTH + 1);
    let oversized_uri = format!("/?pageToken={oversized_token}")
        .parse::<http::Uri>()
        .expect("oversized token URI should parse");
    let error = rest_pagination_from_query(&oversized_uri, context).expect_err("oversized token should fail");
    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
    assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));

    let empty_token = "/?pageToken=".parse::<http::Uri>().expect("empty token URI should parse");
    let pagination = rest_pagination_from_query(&empty_token, context).expect("empty token should start the first page");
    let (cursor, limit) = pagination.page_request().expect("empty token should enable pagination");
    assert_eq!(cursor, None);
    assert_eq!(limit.get(), REST_DEFAULT_PAGE_SIZE);

    let capped_size = "/?pageSize=5000"
        .parse::<http::Uri>()
        .expect("large page size URI should parse");
    let pagination = rest_pagination_from_query(&capped_size, context).expect("large page size should be capped");
    let (cursor, limit) = pagination.page_request().expect("pageSize alone should enable pagination");
    assert_eq!(cursor, None);
    assert_eq!(limit.get(), REST_MAX_PAGE_SIZE);

    let unpaginated = rest_pagination_from_query(&"/".parse().expect("URI should parse"), context)
        .expect("request without pagination should parse");
    assert!(unpaginated.page_request().is_none());
}

#[test]
fn rest_pagination_rejects_malformed_token_payloads() {
    let context = RestPageContext {
        resource: TABLE_CATALOG_NAMESPACE_RESOURCE_ROOT,
        warehouse: "warehouse",
        namespace: None,
    };
    let context_fingerprint = rest_page_context_fingerprint(context);
    let encoded_json =
        |value: serde_json::Value| base64_encode_url_safe_no_pad(&serde_json::to_vec(&value).expect("test token should encode"));
    let malformed_tokens = [
        "*".to_string(),
        base64_encode_url_safe_no_pad(b"not-json"),
        encoded_json(serde_json::json!({
            "version": REST_PAGE_TOKEN_VERSION,
            "context": context_fingerprint,
            "cursor": "strong:alpha",
            "unknown": true
        })),
        encoded_json(serde_json::json!({
            "version": REST_PAGE_TOKEN_VERSION + 1,
            "context": context_fingerprint,
            "cursor": "strong:alpha"
        })),
        encoded_json(serde_json::json!({
            "version": REST_PAGE_TOKEN_VERSION,
            "context": context_fingerprint,
            "cursor": ""
        })),
    ];

    for token in malformed_tokens {
        let uri = format!(
            "/?pageToken={}",
            url::form_urlencoded::byte_serialize(token.as_bytes()).collect::<String>()
        )
        .parse::<http::Uri>()
        .expect("malformed token URI should parse");
        let error = rest_pagination_from_query(&uri, context).expect_err("malformed token should fail");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
        assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));
    }
}

#[tokio::test]
async fn namespace_listing_returns_direct_children_and_scopes_pagination_to_parent() {
    let store = TestTableCatalogStore::default();
    for name in [
        "accounting",
        "accounting.tax.paid",
        "accounting.ledger",
        "analytics",
        "analytics.daily",
        "sales",
    ] {
        let namespace = crate::table_catalog::Namespace::parse(name).expect("namespace should parse");
        store.namespaces.lock().await.push(crate::table_catalog::NamespaceEntry {
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "warehouse".to_string(),
            namespace: namespace.public_name(),
            namespace_id: namespace.storage_id(),
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        });
    }

    let root_uri = "/".parse::<http::Uri>().expect("root namespace URI should parse");
    let root = list_namespaces_response(&store, "warehouse", None, &root_uri)
        .await
        .expect("root namespace list should load");
    assert_eq!(
        root.namespaces,
        vec![
            vec!["accounting".to_string()],
            vec!["analytics".to_string()],
            vec!["sales".to_string()]
        ]
    );

    let parent = crate::table_catalog::Namespace::parse("accounting").expect("parent namespace should parse");
    let first_uri = "/?pageSize=1".parse::<http::Uri>().expect("first page URI should parse");
    let first = list_namespaces_response(&store, "warehouse", Some(&parent), &first_uri)
        .await
        .expect("first child page should load");
    assert_eq!(first.namespaces, vec![vec!["accounting".to_string(), "ledger".to_string()]]);
    let token = first.next_page_token.expect("child continuation token should exist");
    let second_uri = format!("/?pageSize=1&pageToken={token}")
        .parse::<http::Uri>()
        .expect("second page URI should parse");
    let second = list_namespaces_response(&store, "warehouse", Some(&parent), &second_uri)
        .await
        .expect("second child page should load");
    assert_eq!(second.namespaces, vec![vec!["accounting".to_string(), "tax".to_string()]]);
    assert!(second.next_page_token.is_none());

    let different_parent = crate::table_catalog::Namespace::parse("analytics").expect("parent namespace should parse");
    let mismatched_uri = format!("/?pageSize=1&pageToken={token}")
        .parse::<http::Uri>()
        .expect("mismatched page URI should parse");
    assert!(
        list_namespaces_response(&store, "warehouse", Some(&different_parent), &mismatched_uri)
            .await
            .is_err()
    );

    let missing = crate::table_catalog::Namespace::parse("missing").expect("missing namespace should parse");
    let error = list_namespaces_response(&store, "warehouse", Some(&missing), &root_uri)
        .await
        .expect_err("missing parent should return an Iceberg error");
    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_NO_SUCH_NAMESPACE.into()));
    assert_eq!(error.status_code(), Some(StatusCode::NOT_FOUND));

    let leaf = crate::table_catalog::Namespace::parse("sales").expect("leaf namespace should parse");
    let empty = list_namespaces_response(&store, "warehouse", Some(&leaf), &root_uri)
        .await
        .expect("existing leaf namespace should return an empty child list");
    assert!(empty.namespaces.is_empty());
    assert!(empty.next_page_token.is_none());
}

#[tokio::test]
async fn rest_list_pagination_covers_namespaces_tables_and_views() {
    let store = TestTableCatalogStore::default();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    for name in ["beta", "alpha"] {
        store.namespaces.lock().await.push(crate::table_catalog::NamespaceEntry {
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "warehouse".to_string(),
            namespace: name.to_string(),
            namespace_id: name.to_string(),
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        });
        store.tables.lock().await.push(crate::table_catalog::TableEntry {
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "warehouse".to_string(),
            namespace: namespace.public_name(),
            table: name.to_string(),
            table_id: format!("table-{name}"),
            table_uuid: format!("table-uuid-{name}"),
            format: "ICEBERG".to_string(),
            format_version: 2,
            warehouse_location: format!("s3://warehouse/tables/table-{name}"),
            metadata_location: format!("s3://warehouse/tables/table-{name}/metadata/00001.metadata.json"),
            version_token: "token-v1".to_string(),
            generation: 1,
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        });
        store.views.lock().await.push(crate::table_catalog::ViewEntry {
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: "warehouse".to_string(),
            namespace: namespace.public_name(),
            view: name.to_string(),
            view_id: format!("view-{name}"),
            view_uuid: format!("view-uuid-{name}"),
            format: "ICEBERG_VIEW".to_string(),
            format_version: 1,
            warehouse_location: format!("s3://warehouse/views/view-{name}"),
            metadata_location: format!("s3://warehouse/views/view-{name}/metadata/00001.view.json"),
            version_token: "token-v1".to_string(),
            generation: 1,
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: BTreeMap::new(),
            created_at: None,
            updated_at: None,
        });
    }

    let first_uri = "/?pageSize=1".parse::<http::Uri>().expect("first page URI should parse");
    let namespaces = list_namespaces_response(&store, "warehouse", None, &first_uri)
        .await
        .expect("namespace first page should load");
    assert_eq!(namespaces.namespaces, vec![vec!["alpha".to_string()]]);
    let namespace_token = namespaces.next_page_token.expect("namespace continuation should exist");
    let namespace_uri = format!("/?pageSize=1&pageToken={namespace_token}")
        .parse::<http::Uri>()
        .expect("namespace continuation URI should parse");
    let namespaces = list_namespaces_response(&store, "warehouse", None, &namespace_uri)
        .await
        .expect("namespace second page should load");
    assert_eq!(namespaces.namespaces, vec![vec!["beta".to_string()]]);
    assert!(namespaces.next_page_token.is_none());

    let tables = list_tables_response(&store, "warehouse", &namespace, &first_uri)
        .await
        .expect("table first page should load");
    assert_eq!(tables.identifiers.len(), 1);
    assert_eq!(tables.identifiers[0].name, "alpha");
    let table_token = tables.next_page_token.expect("table continuation should exist");
    let table_uri = format!("/?pageSize=1&pageToken={table_token}")
        .parse::<http::Uri>()
        .expect("table continuation URI should parse");
    let tables = list_tables_response(&store, "warehouse", &namespace, &table_uri)
        .await
        .expect("table second page should load");
    assert_eq!(tables.identifiers.len(), 1);
    assert_eq!(tables.identifiers[0].name, "beta");
    assert!(tables.next_page_token.is_none());

    let views = list_views_response(&store, "warehouse", &namespace, &first_uri)
        .await
        .expect("view first page should load");
    assert_eq!(views.identifiers.len(), 1);
    assert_eq!(views.identifiers[0].name, "alpha");
    let view_token = views.next_page_token.expect("view continuation should exist");
    let view_uri = format!("/?pageSize=1&pageToken={view_token}")
        .parse::<http::Uri>()
        .expect("view continuation URI should parse");
    let views = list_views_response(&store, "warehouse", &namespace, &view_uri)
        .await
        .expect("view second page should load");
    assert_eq!(views.identifiers.len(), 1);
    assert_eq!(views.identifiers[0].name, "beta");
    assert!(views.next_page_token.is_none());

    for uri in ["/", "/?pageSize=2"] {
        let uri = uri.parse::<http::Uri>().expect("list URI should parse");
        let namespaces = list_namespaces_response(&store, "warehouse", None, &uri)
            .await
            .expect("namespace exact page should load");
        let tables = list_tables_response(&store, "warehouse", &namespace, &uri)
            .await
            .expect("table exact page should load");
        let views = list_views_response(&store, "warehouse", &namespace, &uri)
            .await
            .expect("view exact page should load");
        assert_eq!(namespaces.namespaces.len(), 2);
        assert!(namespaces.next_page_token.is_none());
        assert_eq!(tables.identifiers.len(), 2);
        assert!(tables.next_page_token.is_none());
        assert_eq!(views.identifiers.len(), 2);
        assert!(views.next_page_token.is_none());
    }
}

#[test]
fn list_responses_expose_null_next_page_token_at_end() {
    for value in [
        serde_json::to_value(RestListNamespacesResponse {
            namespaces: vec![vec!["analytics".to_string()]],
            next_page_token: None,
        })
        .expect("namespace response should serialize"),
        serde_json::to_value(RestListTablesResponse {
            identifiers: Vec::new(),
            next_page_token: None,
        })
        .expect("table response should serialize"),
        serde_json::to_value(RestListViewsResponse {
            identifiers: Vec::new(),
            next_page_token: None,
        })
        .expect("view response should serialize"),
    ] {
        assert!(value.get("next-page-token").is_some_and(serde_json::Value::is_null));
    }
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
            metadata_location: ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json"
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
            "metadata-location": "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json",
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
fn create_table_assigns_positive_ids_to_spark_schema() {
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "name": "events",
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 0, "name": "id", "required": false, "type": "long"},
                {"id": 1, "name": "payload", "required": false, "type": "string"}
            ]
        },
        "partition-spec": {"spec-id": 0, "fields": []},
        "properties": {"owner": "spark"}
    }))
    .expect("Spark create table request should parse");

    let (_, metadata) = table_entry_from_create_table_request("warehouse", &namespace, request)
        .expect("catalog should assign positive field IDs");

    assert_eq!(metadata["schemas"][0]["fields"][0]["id"], 1);
    assert_eq!(metadata["schemas"][0]["fields"][1]["id"], 2);
    assert_eq!(metadata["last-column-id"], 2);
}

#[test]
fn create_table_assigns_fresh_id_to_negative_temporary_field_id() {
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "name": "events",
        "schema": {
            "type": "struct",
            "identifier-field-ids": [-1],
            "fields": [{"id": -1, "name": "id", "required": true, "type": "long"}]
        },
        "partition-spec": {
            "fields": [{"source-id": -1, "name": "id", "transform": "identity"}]
        },
        "write-order": {
            "fields": [{
                "source-id": -1,
                "transform": "identity",
                "direction": "asc",
                "null-order": "nulls-first"
            }]
        }
    }))
    .expect("create table request with a negative temporary field ID should parse");

    let (_, metadata) = table_entry_from_create_table_request("warehouse", &namespace, request)
        .expect("catalog should replace the negative temporary field ID");

    assert_eq!(metadata["schemas"][0]["fields"][0]["id"], 1);
    assert_eq!(metadata["schemas"][0]["identifier-field-ids"], serde_json::json!([1]));
    assert_eq!(metadata["partition-specs"][0]["fields"][0]["source-id"], 1);
    assert_eq!(metadata["sort-orders"][0]["fields"][0]["source-id"], 1);
    assert_eq!(metadata["last-column-id"], 1);
}

#[test]
fn create_table_request_honors_supported_format_version_property() {
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "name": "events_v1",
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        },
        "properties": {"format-version": "1"}
    }))
    .expect("v1 create table request should parse");

    let (entry, metadata) =
        table_entry_from_create_table_request("warehouse", &namespace, request).expect("v1 table metadata should be created");

    assert_eq!(entry.format_version, 1);
    assert_eq!(metadata["format-version"], 1);
    assert!(!entry.properties.contains_key("format-version"));
    assert!(metadata["properties"].get("format-version").is_none());
    assert!(metadata.get("schema").is_some());
    assert!(metadata.get("partition-spec").is_some());
    assert!(metadata.get("schemas").is_some());
    assert_eq!(metadata["current-schema-id"], 0);
    assert!(metadata.get("partition-specs").is_some());
    assert!(metadata.get("sort-orders").is_some());
    assert_eq!(metadata["sort-orders"][0]["order-id"], 0);
    assert_eq!(metadata["default-sort-order-id"], 0);
    assert!(metadata.get("last-sequence-number").is_none());
}

#[test]
fn catalog_assigns_read_only_schema_spec_and_sort_order_ids() {
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "name": "events",
        "schema": {
            "type": "struct",
            "schema-id": 41,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        },
        "partition-spec": {
            "spec-id": 42,
            "fields": [{"source-id": 1, "name": "id", "transform": "identity"}]
        },
        "write-order": {
            "order-id": 43,
            "fields": [{
                "source-id": 1,
                "transform": "identity",
                "direction": "asc",
                "null-order": "nulls-first"
            }]
        },
        "properties": {}
    }))
    .expect("create table request should parse");
    let (_, metadata) =
        table_entry_from_create_table_request("warehouse", &namespace, request).expect("table metadata should be created");

    assert_eq!(metadata["schemas"][0]["schema-id"], 0);
    assert_eq!(metadata["current-schema-id"], 0);
    assert_eq!(metadata["partition-specs"][0]["spec-id"], 0);
    assert_eq!(metadata["partition-specs"][0]["fields"][0]["field-id"], 1000);
    assert_eq!(metadata["default-spec-id"], 0);
    assert_eq!(metadata["last-partition-id"], 1000);
    assert_eq!(metadata["sort-orders"][0]["order-id"], 1);
    assert_eq!(metadata["default-sort-order-id"], 1);

    let updated = apply_table_commit_updates_at(
        metadata,
        &[
            serde_json::json!({
                "action": "add-schema",
                "schema": {
                    "type": "struct",
                    "schema-id": 41,
                    "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
                }
            }),
            serde_json::json!({"action": "set-current-schema", "schema-id": -1}),
            serde_json::json!({
                "action": "add-spec",
                "spec": {
                    "spec-id": 42,
                    "fields": [
                        {"source-id": 1, "name": "id", "transform": "identity"},
                        {"source-id": 1, "name": "id_bucket", "transform": "bucket[16]"}
                    ]
                }
            }),
            serde_json::json!({"action": "set-default-spec", "spec-id": -1}),
            serde_json::json!({
                "action": "add-sort-order",
                "sort-order": {"order-id": 43, "fields": []}
            }),
            serde_json::json!({"action": "set-default-sort-order", "sort-order-id": -1}),
        ],
        "s3://warehouse/tables/table-id/metadata/v1.metadata.json",
        2,
    )
    .expect("catalog-assigned metadata IDs should apply");

    assert_eq!(updated["schemas"][1]["schema-id"], 1);
    assert_eq!(updated["current-schema-id"], 1);
    assert_eq!(updated["partition-specs"][1]["spec-id"], 1);
    assert_eq!(updated["partition-specs"][1]["fields"][0]["field-id"], 1000);
    assert_eq!(updated["partition-specs"][1]["fields"][1]["field-id"], 1001);
    assert_eq!(updated["default-spec-id"], 1);
    assert_eq!(updated["last-partition-id"], 1001);
    assert_eq!(updated["sort-orders"].as_array().map(Vec::len), Some(2));
    assert_eq!(updated["sort-orders"][0]["order-id"], 1);
    assert_eq!(updated["sort-orders"][1]["order-id"], 0);
    assert_eq!(updated["default-sort-order-id"], 0);
}

#[test]
fn create_table_assigns_fresh_schema_field_ids_and_rewrites_references() {
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "name": "events",
        "schema": {
            "type": "struct",
            "schema-id": 41,
            "identifier-field-ids": [0],
            "fields": [
                {"id": 0, "name": "id", "required": true, "type": "long"},
                {
                    "id": 10,
                    "name": "details",
                    "required": false,
                    "type": {
                        "type": "struct",
                        "fields": [{"id": 11, "name": "category", "required": false, "type": "string"}]
                    }
                },
                {
                    "id": 20,
                    "name": "tags",
                    "required": false,
                    "type": {
                        "type": "list",
                        "element-id": 21,
                        "element-required": false,
                        "element": "string"
                    }
                },
                {
                    "id": 30,
                    "name": "attributes",
                    "required": false,
                    "type": {
                        "type": "map",
                        "key-id": 31,
                        "key": "string",
                        "value-id": 32,
                        "value-required": false,
                        "value": {
                            "type": "struct",
                            "fields": [{"id": 33, "name": "score", "required": false, "type": "int"}]
                        }
                    }
                }
            ]
        },
        "partition-spec": {
            "spec-id": 42,
            "fields": [{"source-id": 0, "name": "id", "transform": "identity"}]
        },
        "write-order": {
            "order-id": 43,
            "fields": [{
                "source-id": 11,
                "transform": "identity",
                "direction": "asc",
                "null-order": "nulls-first"
            }]
        }
    }))
    .expect("create table request should parse");

    let (_, metadata) =
        table_entry_from_create_table_request("warehouse", &namespace, request).expect("catalog should assign fresh field IDs");

    let schema = &metadata["schemas"][0];
    assert_eq!(schema["fields"][0]["id"], 1);
    assert_eq!(schema["fields"][1]["id"], 2);
    assert_eq!(schema["fields"][2]["id"], 3);
    assert_eq!(schema["fields"][3]["id"], 4);
    assert_eq!(schema["fields"][1]["type"]["fields"][0]["id"], 5);
    assert_eq!(schema["fields"][2]["type"]["element-id"], 6);
    assert_eq!(schema["fields"][3]["type"]["key-id"], 7);
    assert_eq!(schema["fields"][3]["type"]["value-id"], 8);
    assert_eq!(schema["fields"][3]["type"]["value"]["fields"][0]["id"], 9);
    assert_eq!(schema["identifier-field-ids"], serde_json::json!([1]));
    assert_eq!(metadata["last-column-id"], 9);
    assert_eq!(metadata["partition-specs"][0]["fields"][0]["source-id"], 1);
    assert_eq!(metadata["sort-orders"][0]["fields"][0]["source-id"], 5);
}

#[test]
fn create_table_rejects_duplicate_temporary_schema_field_ids() {
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "name": "events",
        "schema": {
            "type": "struct",
            "fields": [
                {"id": 0, "name": "id", "required": false, "type": "long"},
                {"id": 0, "name": "payload", "required": false, "type": "string"}
            ]
        }
    }))
    .expect("create table request should parse");

    let error = table_entry_from_create_table_request("warehouse", &namespace, request)
        .expect_err("duplicate temporary field IDs must be rejected");

    assert_eq!(error.message(), Some("duplicate create schema field id 0"));
}

#[test]
fn create_table_rejects_excessive_schema_nesting() {
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let mut field_type = serde_json::Value::from("long");
    for element_id in 1..=crate::table_catalog::ICEBERG_MAX_SCHEMA_NESTING_DEPTH + 1 {
        field_type = serde_json::json!({
            "type": "list",
            "element-id": element_id,
            "element-required": false,
            "element": field_type
        });
    }
    let request = CreateTableRequest {
        name: "events".to_string(),
        location: None,
        schema: serde_json::json!({
            "type": "struct",
            "fields": [{"id": 0, "name": "nested", "required": false, "type": field_type}]
        }),
        partition_spec: None,
        write_order: None,
        stage_create: false,
        properties: BTreeMap::new(),
    };

    let error = table_entry_from_create_table_request("warehouse", &namespace, request)
        .expect_err("excessively nested create schemas must be rejected");

    assert_eq!(error.message(), Some("create schema exceeds the maximum nesting depth"));
}

#[test]
fn standard_commit_binds_new_specs_and_sort_orders_to_current_schema() {
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "name": "events",
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        },
        "properties": {}
    }))
    .expect("create table request should parse");
    let (_, metadata) =
        table_entry_from_create_table_request("warehouse", &namespace, request).expect("table metadata should be created");
    let schema_updates = [
        serde_json::json!({
            "action": "add-schema",
            "schema": {"type": "struct", "schema-id": 1, "fields": []}
        }),
        serde_json::json!({"action": "set-current-schema", "schema-id": -1}),
    ];

    let mut spec_updates = schema_updates.to_vec();
    spec_updates.push(serde_json::json!({
        "action": "add-spec",
        "spec": {
            "spec-id": 1,
            "fields": [{"source-id": 1, "name": "id", "transform": "identity"}]
        }
    }));
    apply_table_commit_updates_at(
        metadata.clone(),
        &spec_updates,
        "s3://warehouse/tables/table-id/metadata/v1.metadata.json",
        2,
    )
    .expect_err("a new partition spec must bind to the current schema");
    spec_updates[2]["spec"]["fields"][0]["transform"] = serde_json::Value::from("void");
    apply_table_commit_updates_at(
        metadata.clone(),
        &spec_updates,
        "s3://warehouse/tables/table-id/metadata/v1.metadata.json",
        2,
    )
    .expect("a void partition field may retain a source removed from the current schema");

    let mut sort_updates = schema_updates.to_vec();
    sort_updates.push(serde_json::json!({
        "action": "add-sort-order",
        "sort-order": {
            "order-id": 1,
            "fields": [{
                "source-id": 1,
                "transform": "identity",
                "direction": "asc",
                "null-order": "nulls-first"
            }]
        }
    }));
    apply_table_commit_updates_at(metadata, &sort_updates, "s3://warehouse/tables/table-id/metadata/v1.metadata.json", 2)
        .expect_err("a new sort order must bind to the current schema");

    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "name": "events_v1",
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        },
        "properties": {"format-version": "1"}
    }))
    .expect("v1 create table request should parse");
    let (_, v1_metadata) =
        table_entry_from_create_table_request("warehouse", &namespace, request).expect("v1 table metadata should be created");
    spec_updates[2]["spec"]["fields"][0]["transform"] = serde_json::Value::from("identity");
    apply_table_commit_updates_at(
        v1_metadata.clone(),
        &spec_updates,
        "s3://warehouse/tables/table-id/metadata/v1.metadata.json",
        2,
    )
    .expect_err("a new v1 partition spec must bind to the updated current schema");
    apply_table_commit_updates_at(
        v1_metadata.clone(),
        &sort_updates,
        "s3://warehouse/tables/table-id/metadata/v1.metadata.json",
        2,
    )
    .expect_err("a new v1 sort order must bind to the updated current schema");

    let mut singular_v1_metadata = v1_metadata.clone();
    let singular_v1_object = singular_v1_metadata
        .as_object_mut()
        .expect("v1 table metadata should be an object");
    for field in [
        "schemas",
        "current-schema-id",
        "partition-specs",
        "default-spec-id",
        "last-partition-id",
        "sort-orders",
        "default-sort-order-id",
    ] {
        singular_v1_object.remove(field);
    }
    let singular_v1_updates = [
        serde_json::json!({
            "action": "add-schema",
            "schema": {
                "type": "struct",
                "schema-id": 1,
                "fields": [{"id": 2, "name": "category", "required": true, "type": "string"}]
            }
        }),
        serde_json::json!({
            "action": "add-spec",
            "spec": {
                "spec-id": 1,
                "fields": [{"source-id": 1, "name": "id", "transform": "identity"}]
            }
        }),
    ];
    let singular_v1_updated = apply_table_commit_updates_at(
        singular_v1_metadata,
        &singular_v1_updates,
        "s3://warehouse/tables/table-id/metadata/v1.metadata.json",
        2,
    )
    .expect("a singular v1 table may add schema history without changing its current schema");
    assert_eq!(singular_v1_updated["current-schema-id"], 0);
    assert_eq!(singular_v1_updated["schema"]["schema-id"], 0);

    let valid_v1_updates = [
        serde_json::json!({
            "action": "add-schema",
            "schema": {
                "type": "struct",
                "schema-id": 1,
                "fields": [{"id": 2, "name": "category", "required": true, "type": "string"}]
            }
        }),
        serde_json::json!({"action": "set-current-schema", "schema-id": -1}),
        serde_json::json!({
            "action": "add-spec",
            "spec": {
                "spec-id": 1,
                "fields": [{"source-id": 2, "name": "category", "transform": "identity"}]
            }
        }),
    ];
    apply_table_commit_updates_at(
        v1_metadata,
        &valid_v1_updates,
        "s3://warehouse/tables/table-id/metadata/v1.metadata.json",
        2,
    )
    .expect("a new v1 partition spec may bind to a field in the updated current schema");
}

#[test]
fn last_added_table_ids_require_a_preceding_add_update() {
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "name": "events",
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        },
        "properties": {}
    }))
    .expect("create table request should parse");
    let (_, metadata) =
        table_entry_from_create_table_request("warehouse", &namespace, request).expect("table metadata should be created");
    let invalid_update_sequences = [
        vec![
            serde_json::json!({"action": "set-current-schema", "schema-id": -1}),
            serde_json::json!({
                "action": "add-schema",
                "schema": {"type": "struct", "schema-id": 1, "fields": []}
            }),
        ],
        vec![
            serde_json::json!({"action": "set-default-spec", "spec-id": -1}),
            serde_json::json!({
                "action": "add-spec",
                "spec": {"spec-id": 1, "fields": []}
            }),
        ],
        vec![
            serde_json::json!({"action": "set-default-sort-order", "sort-order-id": -1}),
            serde_json::json!({
                "action": "add-sort-order",
                "sort-order": {"order-id": 1, "fields": []}
            }),
        ],
    ];

    for updates in invalid_update_sequences {
        let error = apply_table_commit_updates_at(
            metadata.clone(),
            &updates,
            "s3://warehouse/tables/table-id/metadata/v1.metadata.json",
            2,
        )
        .expect_err("a later add update must not satisfy an earlier -1 reference");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert!(
            error
                .message()
                .is_some_and(|message| message.contains("requires a preceding"))
        );
    }
}

#[test]
fn create_table_counts_collection_ids_in_last_column_id() {
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "name": "events",
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {
                    "id": 1,
                    "name": "items",
                    "required": true,
                    "type": {
                        "type": "list",
                        "element-id": 7,
                        "element-required": true,
                        "element": "long"
                    }
                },
                {
                    "id": 2,
                    "name": "lookup",
                    "required": true,
                    "type": {
                        "type": "map",
                        "key-id": 8,
                        "key": "string",
                        "value-id": 9,
                        "value-required": false,
                        "value": "string"
                    }
                }
            ]
        }
    }))
    .expect("create table request should parse");

    let (_, metadata) =
        table_entry_from_create_table_request("warehouse", &namespace, request).expect("table metadata should be created");

    let schema = &metadata["schemas"][0];
    assert_eq!(schema["fields"][0]["type"]["element-id"], 3);
    assert_eq!(schema["fields"][1]["type"]["key-id"], 4);
    assert_eq!(schema["fields"][1]["type"]["value-id"], 5);
    assert_eq!(metadata["last-column-id"], 5);
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

#[tokio::test]
async fn commit_request_readers_require_standard_arrays_and_preserve_legacy_pointer_shapes() {
    let table_error = read_rest_commit_table_request(Body::from("{}".to_string()))
        .await
        .expect_err("standard table commits must include requirements and updates");
    assert_eq!(table_error.code(), &S3ErrorCode::InvalidRequest);

    let table = read_rest_commit_table_request(Body::from(r#"{"requirements":[],"updates":[]}"#.to_string()))
        .await
        .expect("standard table commits may provide empty requirements and updates");
    assert!(table.requirements.is_empty());
    assert!(table.updates.is_empty());

    let legacy_table = read_rest_commit_table_request(Body::from(
        r#"{"expected-version-token":"token-v1","expected-metadata-location":"s3://warehouse/tables/table-id/metadata/v1.metadata.json","new-metadata-location":"s3://warehouse/tables/table-id/metadata/v2.metadata.json"}"#
            .to_string(),
    ))
    .await
    .expect("legacy pointer commits may omit standard arrays");
    assert_eq!(
        legacy_table.new_metadata_location.as_deref(),
        Some("s3://warehouse/tables/table-id/metadata/v2.metadata.json")
    );

    let mixed_table = read_rest_commit_table_request(Body::from(
        r#"{"expected-version-token":"token-v1","new-metadata-location":"s3://warehouse/tables/table-id/metadata/v2.metadata.json","requirements":[],"updates":[{"action":"set-properties","updates":{"owner":"lakehouse"}}]}"#
            .to_string(),
    ))
    .await
    .expect_err("legacy pointer commits must not silently discard standard updates");
    assert_eq!(mixed_table.code(), &S3ErrorCode::InvalidRequest);

    let view_error = read_rest_commit_view_request(Body::from("{}".to_string()))
        .await
        .expect_err("standard view commits must include updates");
    assert_eq!(view_error.code(), &S3ErrorCode::InvalidRequest);

    let view = read_rest_commit_view_request(Body::from(r#"{"updates":[]}"#.to_string()))
        .await
        .expect("standard view commits may omit requirements");
    assert!(view.requirements.is_empty());
    assert!(view.updates.is_empty());

    let legacy_view = read_rest_commit_view_request(Body::from(
        r#"{"commit-id":"legacy-view-update","new-metadata-location":"s3://warehouse/views/view-id/metadata/v2.metadata.json"}"#
            .to_string(),
    ))
    .await
    .expect("legacy view pointer commits may omit standard updates");
    assert_eq!(legacy_view._commit_id.as_deref(), Some("legacy-view-update"));
}

#[test]
fn unsupported_create_and_register_modes_return_iceberg_errors() {
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let register_error = table_entry_from_register_request(
        "warehouse",
        &namespace,
        RegisterTableRequest {
            name: "events".to_string(),
            metadata_location: "s3://warehouse/metadata/00001.metadata.json".to_string(),
            overwrite: true,
        },
    )
    .expect_err("register overwrite should remain unsupported");
    assert_eq!(register_error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNSUPPORTED_OPERATION.into()));
    assert_eq!(register_error.status_code(), Some(StatusCode::NOT_ACCEPTABLE));

    let create_request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "name": "events",
        "schema": {"type": "struct", "schema-id": 0, "fields": []},
        "stage-create": true
    }))
    .expect("stage-create request should parse");
    let create_error = table_entry_from_create_table_request("warehouse", &namespace, create_request)
        .expect_err("staged create should remain unsupported");
    assert_eq!(create_error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNSUPPORTED_OPERATION.into()));
    assert_eq!(create_error.status_code(), Some(StatusCode::NOT_ACCEPTABLE));
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

#[test]
fn format_upgrade_assigns_v1_snapshot_sequences_and_rejects_v3() {
    let mut metadata = serde_json::json!({
        "format-version": 1,
        "table-uuid": "table-uuid",
        "location": "s3://warehouse/tables/table-id",
        "last-updated-ms": 1,
        "last-column-id": 1,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        },
        "partition-spec": [{"source-id": 1, "name": "id", "transform": "identity"}],
        "snapshots": [{
            "snapshot-id": 10,
            "timestamp-ms": 1,
            "manifests": ["s3://warehouse/tables/table-id/metadata/manifest-10.avro"],
            "summary": {"operation": "append"}
        }]
    });

    apply_upgrade_format_version_update(
        &mut metadata,
        &serde_json::json!({"action": "upgrade-format-version", "format-version": 2}),
    )
    .expect("v1 metadata should upgrade to v2");
    crate::table_catalog::synchronize_table_metadata_version_fields(&mut metadata)
        .expect("upgraded metadata fields should synchronize");

    assert_eq!(metadata["snapshots"][0]["sequence-number"], 0);
    assert_eq!(metadata["partition-specs"][0]["fields"][0]["field-id"], 1000);
    assert_eq!(metadata["last-partition-id"], 1000);
    crate::table_catalog::validate_supported_table_metadata(&metadata).expect("upgraded metadata should satisfy the v2 contract");

    let error = apply_upgrade_format_version_update(
        &mut metadata,
        &serde_json::json!({"action": "upgrade-format-version", "format-version": 3}),
    )
    .expect_err("format version 3 is not supported");
    assert_eq!(error.status_code(), Some(StatusCode::NOT_ACCEPTABLE));
}

#[tokio::test]
async fn create_table_response_writes_initial_metadata_for_standard_request() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
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

    let commit_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let response = create_table_response(&store, &commit_backend, "warehouse", &namespace, request, true)
        .await
        .expect("table should be created");

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
    assert_eq!(
        response.metadata_location,
        format!(
            "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001-{}.metadata.json",
            entry.table_id
        )
    );
    assert_eq!(response.metadata["table-uuid"], entry.table_uuid);
    assert!(
        metadata_backend
            .object_exists("warehouse", &entry.metadata_location)
            .await
            .expect("metadata object lookup should succeed")
    );
}

#[tokio::test]
async fn create_table_holds_bucket_fence_from_metadata_write_through_registration() {
    let store = Arc::new(TestTableCatalogStore::default());
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let metadata_backend = TestTableCatalogObjectBackend {
        put_object_barrier: Some(Arc::clone(&barrier)),
        ..TestTableCatalogObjectBackend::content_addressed()
    };
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    ensure_table_bucket_entry(store.as_ref(), "warehouse", true)
        .await
        .expect("table bucket entry should be seeded");
    create_namespace_response(
        store.as_ref(),
        "warehouse",
        CreateNamespaceRequest {
            namespace: vec!["analytics".to_string()],
            properties: BTreeMap::new(),
        },
        true,
    )
    .await
    .expect("namespace should be created");
    let request = serde_json::from_value::<CreateTableRequest>(serde_json::json!({
        "name": "events",
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        }
    }))
    .expect("create table request should parse");

    let create_store = Arc::clone(&store);
    let create_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let create_namespace = namespace.clone();
    let create = tokio::spawn(async move {
        create_table_response(create_store.as_ref(), &create_backend, "warehouse", &create_namespace, request, true).await
    });
    tokio::time::timeout(StdDuration::from_secs(2), async {
        while metadata_backend.state.lock().await.objects.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("initial metadata write should reach its publication pause");

    let bucket_lock = crate::table_catalog::default_table_bucket_publication_lock_path();
    assert!(
        metadata_backend.write_lock_is_held("warehouse", &bucket_lock).await,
        "first publication must fence data-plane writers before initial metadata is visible"
    );
    metadata_backend.lock_attempts.lock().await.clear();
    let writer_backend = metadata_backend.clone();
    let writer_lock = bucket_lock.clone();
    let writer = tokio::spawn(async move {
        crate::table_catalog::TableCatalogObjectBackend::acquire_read_lock(&writer_backend, "warehouse", &writer_lock).await
    });
    metadata_backend.wait_for_lock_attempts(1).await;
    assert!(!writer.is_finished(), "a first-publication writer must wait until registration finishes");

    barrier.wait().await;
    tokio::time::timeout(StdDuration::from_secs(2), create)
        .await
        .expect("table creation should complete")
        .expect("table creation task should join")
        .expect("table creation should succeed");
    tokio::time::timeout(StdDuration::from_secs(2), writer)
        .await
        .expect("writer should continue after registration")
        .expect("writer task should join")
        .expect("writer lock acquisition should succeed");
    assert!(
        store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .is_some(),
        "table must become visible before the bucket fence is released"
    );
}

#[tokio::test]
async fn create_view_holds_publication_fences_from_metadata_write_through_registration() {
    let pause = TestCatalogPublishPause::default();
    let store = Arc::new(TestTableCatalogStore {
        create_view_pause: Some(pause.clone()),
        ..Default::default()
    });
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let metadata_backend = TestTableCatalogObjectBackend {
        put_object_barrier: Some(Arc::clone(&barrier)),
        ..Default::default()
    };
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    ensure_table_bucket_entry(store.as_ref(), "warehouse", true)
        .await
        .expect("table bucket entry should be seeded");
    create_namespace_response(
        store.as_ref(),
        "warehouse",
        CreateNamespaceRequest {
            namespace: vec!["analytics".to_string()],
            properties: BTreeMap::new(),
        },
        true,
    )
    .await
    .expect("namespace should be created");
    let request = serde_json::from_value::<CreateViewRequest>(serde_json::json!({
        "name": "recent_events",
        "schema": {"type": "struct", "fields": []},
        "view-version": {
            "version-id": 1,
            "timestamp-ms": 1,
            "schema-id": 0,
            "summary": {},
            "default-namespace": ["analytics"],
            "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]
        }
    }))
    .expect("create view request should parse");

    let create_store = Arc::clone(&store);
    let create_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let create_namespace = namespace.clone();
    let create = tokio::spawn(async move {
        create_view_response(create_store.as_ref(), &create_backend, "warehouse", &create_namespace, request, true).await
    });
    tokio::time::timeout(StdDuration::from_secs(2), async {
        while metadata_backend.state.lock().await.objects.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("initial view metadata write should reach its publication pause");

    let bucket_lock = crate::table_catalog::default_table_bucket_publication_lock_path();
    assert!(
        metadata_backend.write_lock_is_held("warehouse", &bucket_lock).await,
        "view publication must fence data-plane writers before initial metadata is visible"
    );
    barrier.wait().await;
    tokio::time::timeout(StdDuration::from_secs(2), pause.wait_started())
        .await
        .expect("view creation should reach catalog publication");

    let view_name = crate::table_catalog::IdentifierSegment::parse("recent_events").expect("view should parse");
    let view_lock = crate::table_catalog::default_table_publication_lock_path(&namespace, &view_name);
    assert!(
        metadata_backend.write_lock_is_held("warehouse", &bucket_lock).await,
        "view creation must retain the bucket fence until catalog publication"
    );
    assert!(
        metadata_backend.write_lock_is_held("warehouse", &view_lock).await,
        "view creation must hold the view publication fence before registration"
    );
    assert!(
        store
            .load_view("warehouse", "analytics", "recent_events")
            .await
            .expect("view lookup should succeed")
            .is_none(),
        "the view must remain invisible before catalog publication"
    );

    metadata_backend.lock_attempts.lock().await.clear();
    let writer_backend = metadata_backend.clone();
    let writer_lock = bucket_lock.clone();
    let writer = tokio::spawn(async move {
        crate::table_catalog::TableCatalogObjectBackend::acquire_read_lock(&writer_backend, "warehouse", &writer_lock).await
    });
    metadata_backend.wait_for_lock_attempts(1).await;
    assert!(!writer.is_finished(), "a data-plane writer must wait for view registration");

    pause.release();
    tokio::time::timeout(StdDuration::from_secs(2), create)
        .await
        .expect("view creation should complete")
        .expect("view creation task should join")
        .expect("view creation should succeed");
    tokio::time::timeout(StdDuration::from_secs(2), writer)
        .await
        .expect("writer should continue after view registration")
        .expect("writer task should join")
        .expect("writer lock acquisition should succeed");
    assert!(
        store
            .load_view("warehouse", "analytics", "recent_events")
            .await
            .expect("view lookup should succeed")
            .is_some(),
        "the view must become visible after catalog publication"
    );
}

#[tokio::test]
async fn create_table_response_recreates_dropped_identifier_without_overwriting_retained_metadata() {
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone());
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let first_entry = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("first table lookup should succeed")
        .expect("first table should exist");
    let retained_initial = metadata_backend
        .read_object("warehouse", &first_entry.metadata_location)
        .await
        .expect("first metadata lookup should succeed")
        .expect("first metadata should exist");

    let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "commit-id": "11111111-1111-4111-8111-111111111111",
        "updates": [
            {
                "action": "set-properties",
                "updates": {
                    "owner": "first-table"
                }
            }
        ]
    }))
    .expect("standard commit request should parse");
    standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        commit_request,
    )
    .await
    .expect("first table metadata commit should succeed");
    let first_committed_entry = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("committed table lookup should succeed")
        .expect("committed table should exist");

    drop_table_in_store(&store, "warehouse", &namespace, "events")
        .await
        .expect("first table should drop");
    drop_namespace_in_store(&store, "warehouse", "analytics")
        .await
        .expect("first namespace should drop");
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
    .expect("namespace should be recreated");

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
    .expect("recreate table request should parse");
    let commit_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let second = create_table_response(&store, &commit_backend, "warehouse", &namespace, create_request, true)
        .await
        .expect("dropped table identifier should be reusable");
    let second_entry = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("recreated table lookup should succeed")
        .expect("recreated table should exist");

    assert_ne!(first_entry.table_id, second_entry.table_id);
    assert_ne!(first_entry.table_uuid, second_entry.table_uuid);
    assert_ne!(first_entry.metadata_location, second_entry.metadata_location);
    assert_ne!(first_committed_entry.metadata_location, second_entry.metadata_location);
    assert_eq!(second.metadata["table-uuid"], second_entry.table_uuid);
    assert_eq!(second.metadata["metadata-log"], serde_json::json!([]));
    assert_eq!(
        metadata_backend
            .read_object("warehouse", &first_entry.metadata_location)
            .await
            .expect("retained metadata lookup should succeed")
            .expect("retained metadata should still exist")
            .data,
        retained_initial.data
    );
    assert!(
        metadata_backend
            .object_exists("warehouse", &first_committed_entry.metadata_location)
            .await
            .expect("committed metadata lookup should succeed")
    );
    assert!(
        metadata_backend
            .object_exists("warehouse", &second_entry.metadata_location)
            .await
            .expect("recreated metadata lookup should succeed")
    );

    let second_commit = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        standard_property_commit_request("11111111-1111-4111-8111-111111111111", &second_entry.table_uuid, "second-table"),
    )
    .await
    .expect("recreated table should use a scoped metadata path when retained metadata occupies the normal path");
    assert!(second_commit.metadata_location.ends_with(&table_scoped_metadata_file_name(
        2,
        &second_entry.table_id,
        "11111111-1111-4111-8111-111111111111"
    )));
    assert_eq!(second_commit.metadata["table-uuid"], second_entry.table_uuid);
    assert_eq!(second_commit.metadata["properties"]["owner"], "second-table");
}

#[tokio::test]
async fn concurrent_create_table_responses_keep_one_catalog_winner_with_distinct_metadata() {
    let catalog_backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(catalog_backend);
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
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
    let create_request = || {
        serde_json::from_value::<CreateTableRequest>(serde_json::json!({
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
        .expect("concurrent create table request should parse")
    };

    let first_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let second_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let (first, second) = tokio::join!(
        create_table_response(&store, &first_backend, "warehouse", &namespace, create_request(), true,),
        create_table_response(&store, &second_backend, "warehouse", &namespace, create_request(), true,)
    );
    assert_ne!(first.is_ok(), second.is_ok(), "exactly one concurrent create should succeed");
    let (winner, loser) = match (first, second) {
        (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => (winner, loser),
        _ => unreachable!("success count was checked above"),
    };
    assert!(format!("{loser:?}").contains("AlreadyExistsException"));

    let tables = store
        .list_tables("warehouse", "analytics")
        .await
        .expect("table listing should succeed");
    assert_eq!(tables.len(), 1);
    let winner_entry = &tables[0];
    assert_eq!(
        winner.metadata_location,
        table_metadata_location_for_client("warehouse", &winner_entry.metadata_location)
    );
    let metadata_prefix = winner_entry
        .metadata_location
        .rsplit_once('/')
        .map(|(prefix, _)| format!("{prefix}/"))
        .expect("metadata location should contain a file name");
    let metadata_objects = metadata_backend
        .list_objects("warehouse", &metadata_prefix)
        .await
        .expect("metadata listing should succeed");
    assert_eq!(metadata_objects.len(), 2);
    assert_ne!(metadata_objects[0], metadata_objects[1]);

    let mut table_uuids = Vec::with_capacity(metadata_objects.len());
    for metadata_object in metadata_objects {
        let metadata = metadata_backend
            .read_object("warehouse", &metadata_object)
            .await
            .expect("metadata lookup should succeed")
            .expect("metadata object should exist");
        let metadata: serde_json::Value = serde_json::from_slice(&metadata.data).expect("metadata object should contain JSON");
        table_uuids.push(
            metadata["table-uuid"]
                .as_str()
                .expect("metadata should contain table uuid")
                .to_string(),
        );
    }
    table_uuids.sort();
    table_uuids.dedup();
    assert_eq!(table_uuids.len(), 2);
    assert!(table_uuids.contains(&winner_entry.table_uuid));
}

#[tokio::test]
async fn standard_commit_applies_updates_and_writes_next_metadata() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_uuid = created.metadata["table-uuid"]
        .as_str()
        .expect("created metadata should have table uuid")
        .to_string();
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_snapshot_manifest(&metadata_backend, "warehouse", &manifest_list, 10, 1, &[(&data_file, 0, 1, 10, 1)]).await;

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
                    "manifest-list": manifest_list,
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

    let commit = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        commit_request,
    )
    .await
    .expect("standard commit should succeed");

    let metadata_file_prefix =
        "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-";
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
    assert_eq!(commit.metadata["metadata-log"][0]["metadata-file"], created.metadata_location);
    assert_eq!(commit.metadata["metadata-log"][0]["timestamp-ms"], created.metadata["last-updated-ms"]);
    let committed = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("committed table lookup should succeed")
        .expect("committed table should exist");
    assert_eq!(
        table_metadata_location_for_client("warehouse", &committed.metadata_location),
        commit.metadata_location
    );
    assert!(
        metadata_backend
            .object_exists("warehouse", &committed.metadata_location)
            .await
            .expect("committed metadata lookup should succeed")
    );
}

#[test]
fn table_metadata_file_name_scoping_is_bounded_and_identity_sensitive() {
    let metadata_file_token = "a".repeat(64);
    let table_id = "11111111-1111-4111-8111-111111111111";
    let first = table_scoped_metadata_file_name(u64::MAX, table_id, &metadata_file_token);
    let second = table_scoped_metadata_file_name(u64::MAX, "22222222-2222-4222-8222-222222222222", &metadata_file_token);
    let third = table_scoped_metadata_file_name(u64::MAX, table_id, &"b".repeat(64));
    let alias_commit_id = format!("table-metadata:{}:{table_id}{metadata_file_token}", table_id.len());
    let (_, alias_token) = standard_commit_ids(Some(alias_commit_id));
    let normal_alias = next_metadata_file_name(u64::MAX, &alias_token);

    assert_eq!(
        table_scoped_metadata_file_name(2, table_id, &metadata_file_token),
        "00002-table-accc41bda78e38e3814a0d4a09a66e47256c24d88dde9b8c1ea57db0e434c599.metadata.json"
    );
    assert_ne!(first, second);
    assert_ne!(first, third);
    assert_ne!(first, normal_alias);
    assert!(first.len() <= crate::table_catalog::TABLE_METADATA_FILE_NAME_MAX_LEN);
    let scoped_token = first
        .strip_prefix(&format!("{}-table-", u64::MAX))
        .and_then(|file_name| file_name.strip_suffix(".metadata.json"))
        .expect("scoped metadata file should use the generated file name shape");
    assert_eq!(scoped_token.len(), 64);
    assert!(
        scoped_token
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    );
}

#[tokio::test]
async fn renamed_and_recreated_tables_with_the_same_commit_id_use_disjoint_metadata_files() {
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::StrongTableCatalogStore::new(metadata_backend.clone());
    let source_namespace = crate::table_catalog::Namespace::parse("analytics").expect("source namespace should parse");
    let destination_namespace = crate::table_catalog::Namespace::parse("curated").expect("destination namespace should parse");
    create_standard_events_table(&store, &metadata_backend, &source_namespace).await;
    create_namespace_response(
        &store,
        "warehouse",
        CreateNamespaceRequest {
            namespace: vec!["curated".to_string()],
            properties: BTreeMap::new(),
        },
        true,
    )
    .await
    .expect("destination namespace should be created");
    store
        .rename_table("warehouse", "analytics", "events", "curated", "events_v2")
        .await
        .expect("table should rename");

    let recreate_request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "name": "events",
        "schema": {"type": "struct", "schema-id": 0, "fields": []}
    }))
    .expect("recreate table request should parse");
    create_table_response(
        &store,
        &TableCommitObjectBackend::trusted(metadata_backend.clone()),
        "warehouse",
        &source_namespace,
        recreate_request,
        true,
    )
    .await
    .expect("source identifier should be reusable");

    let renamed = store
        .load_table("warehouse", "curated", "events_v2")
        .await
        .expect("renamed table lookup should succeed")
        .expect("renamed table should exist");
    let recreated = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("recreated table lookup should succeed")
        .expect("recreated table should exist");
    assert_ne!(renamed.table_id, recreated.table_id);

    let commit_id = "11111111-1111-4111-8111-111111111111";
    let renamed_commit = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &destination_namespace,
        "events_v2",
        standard_property_commit_request(commit_id, &renamed.table_uuid, "curated"),
    )
    .await
    .expect("renamed table commit should succeed");
    let recreated_commit = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &source_namespace,
        "events",
        standard_property_commit_request(commit_id, &recreated.table_uuid, "analytics"),
    )
    .await
    .expect("recreated table commit should succeed");

    assert_ne!(renamed_commit.metadata_location, recreated_commit.metadata_location);
    assert!(
        renamed_commit
            .metadata_location
            .ends_with(&next_metadata_file_name(2, commit_id))
    );
    let original_metadata_root = crate::table_catalog::default_table_metadata_dir_path(
        &source_namespace,
        &crate::table_catalog::IdentifierSegment::parse("events").expect("source table should parse"),
    );
    assert!(
        renamed_commit
            .metadata_location
            .starts_with(&format!("s3://warehouse/{original_metadata_root}/"))
    );
    assert!(
        !renamed_commit
            .metadata_location
            .contains("/namespaces/curated/tables/events_v2/")
    );
    assert!(
        recreated_commit
            .metadata_location
            .ends_with(&table_scoped_metadata_file_name(2, &recreated.table_id, commit_id))
    );
    assert_eq!(recreated_commit.metadata["table-uuid"], recreated.table_uuid);
    assert_eq!(recreated_commit.metadata["properties"]["owner"], "analytics");
    for metadata_location in [renamed_commit.metadata_location, recreated_commit.metadata_location] {
        let object_key = test_snapshot_object_key("warehouse", &metadata_location);
        assert!(
            metadata_backend
                .object_exists("warehouse", &object_key)
                .await
                .expect("metadata lookup should succeed")
        );
    }
}

#[tokio::test]
async fn standard_commit_reuses_matching_normal_metadata_orphan() {
    let (store, metadata_backend, namespace, current, request, primary_location, fallback_location) =
        standard_commit_primary_fixture("target", "target").await;

    let committed = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect("matching prewritten metadata should be reusable");

    assert_eq!(
        committed.metadata_location,
        table_metadata_location_for_client("warehouse", &primary_location)
    );
    assert!(
        !metadata_backend
            .object_exists("warehouse", &fallback_location)
            .await
            .expect("fallback metadata lookup should succeed")
    );
    let committed_entry = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    assert_eq!(committed_entry.generation, current.generation + 1);
    let persisted = read_table_metadata_json(&metadata_backend, "warehouse", &primary_location)
        .await
        .expect("persisted metadata should load");
    assert_eq!(committed.metadata, persisted);
}

#[tokio::test]
async fn standard_commit_rejects_mutated_retry_after_normal_metadata_orphan() {
    let (store, metadata_backend, namespace, current, request, _primary_location, fallback_location) =
        standard_commit_primary_fixture("original", "mutated").await;

    let error = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect_err("a reused commit id must not accept a different payload");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_COMMIT_FAILED.into()));
    assert_eq!(error.status_code(), Some(StatusCode::CONFLICT));
    assert_events_table_entry_unchanged(&store, &current).await;
    assert!(
        !metadata_backend
            .object_exists("warehouse", &fallback_location)
            .await
            .expect("fallback metadata lookup should succeed")
    );
}

#[tokio::test]
async fn standard_commit_rejects_malformed_normal_metadata_orphan() {
    let (store, metadata_backend, namespace, current, request, primary_location, fallback_location) =
        standard_commit_primary_fixture("target", "target").await;
    metadata_backend
        .put_json("warehouse", &primary_location, serde_json::json!({}))
        .await;

    let error = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect_err("malformed prewritten metadata must fail closed");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
    assert_eq!(error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert_events_table_entry_unchanged(&store, &current).await;
    assert!(
        !metadata_backend
            .object_exists("warehouse", &fallback_location)
            .await
            .expect("fallback metadata lookup should succeed")
    );
}

#[tokio::test]
async fn standard_commit_rejects_same_uuid_metadata_without_timestamp_as_server_state() {
    let (store, metadata_backend, namespace, current, request, primary_location, fallback_location) =
        standard_commit_primary_fixture("target", "target").await;
    let mut persisted = read_table_metadata_json(&metadata_backend, "warehouse", &primary_location)
        .await
        .expect("persisted metadata should load");
    persisted
        .as_object_mut()
        .expect("metadata should be an object")
        .remove("last-updated-ms");
    metadata_backend.put_json("warehouse", &primary_location, persisted).await;

    let error = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect_err("same-uuid metadata without a timestamp must fail as server state");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
    assert_eq!(error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert_eq!(error.message(), Some("existing generated metadata is invalid"));
    assert_events_table_entry_unchanged(&store, &current).await;
    assert!(
        !metadata_backend
            .object_exists("warehouse", &fallback_location)
            .await
            .expect("fallback metadata lookup should succeed")
    );
}

#[tokio::test]
async fn standard_commit_rejects_same_uuid_unsupported_metadata_as_server_state() {
    let (store, metadata_backend, namespace, current, request, primary_location, fallback_location) =
        standard_commit_primary_fixture("target", "target").await;
    let mut persisted = read_table_metadata_json(&metadata_backend, "warehouse", &primary_location)
        .await
        .expect("persisted metadata should load");
    persisted["format-version"] = serde_json::json!(3);
    metadata_backend.put_json("warehouse", &primary_location, persisted).await;

    let error = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect_err("same-uuid unsupported metadata must fail as server state");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
    assert_eq!(error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert_eq!(error.message(), Some("existing generated metadata is invalid"));
    assert_events_table_entry_unchanged(&store, &current).await;
    assert!(
        !metadata_backend
            .object_exists("warehouse", &fallback_location)
            .await
            .expect("fallback metadata lookup should succeed")
    );
}

#[tokio::test]
async fn standard_commit_rejects_unparseable_normal_metadata_orphan_as_server_state() {
    let (store, metadata_backend, namespace, current, request, primary_location, fallback_location) =
        standard_commit_primary_fixture("target", "target").await;
    metadata_backend
        .put_bytes("warehouse", &primary_location, b"{".to_vec())
        .await;

    let error = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect_err("unparseable prewritten metadata must fail as server state");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
    assert_eq!(error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert_events_table_entry_unchanged(&store, &current).await;
    assert!(
        !metadata_backend
            .object_exists("warehouse", &fallback_location)
            .await
            .expect("fallback metadata lookup should succeed")
    );
}

#[tokio::test]
async fn standard_commit_rejects_unsupported_normal_metadata_orphan_as_server_state() {
    let (store, metadata_backend, namespace, current, mut current_metadata, request, fallback_location) =
        standard_commit_foreign_primary_fixture().await;
    current_metadata["table-uuid"] = serde_json::Value::String(Uuid::new_v4().to_string());
    current_metadata["format-version"] = serde_json::json!(3);
    let primary_location = crate::table_catalog::table_metadata_file_path_for_entry(
        &current,
        &next_metadata_file_name(2, "11111111-1111-4111-8111-111111111111"),
    )
    .expect("primary metadata path should be valid");
    metadata_backend
        .put_json("warehouse", &primary_location, current_metadata)
        .await;

    let error = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect_err("unsupported generated metadata must fail as server state");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
    assert_eq!(error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert_events_table_entry_unchanged(&store, &current).await;
    assert!(
        !metadata_backend
            .object_exists("warehouse", &fallback_location)
            .await
            .expect("fallback metadata lookup should succeed")
    );
}

#[tokio::test]
async fn standard_commit_rejects_same_uuid_metadata_from_different_lineage() {
    let (store, metadata_backend, namespace, current, request, primary_location, fallback_location) =
        standard_commit_primary_fixture("target", "target").await;
    let mut persisted_metadata = read_table_metadata_json(&metadata_backend, "warehouse", &primary_location)
        .await
        .expect("persisted metadata should load");
    persisted_metadata["metadata-log"][0]["metadata-file"] =
        serde_json::json!("s3://warehouse/foreign/metadata/00001.metadata.json");
    metadata_backend
        .put_json("warehouse", &primary_location, persisted_metadata)
        .await;

    let error = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect_err("same-uuid metadata from another lineage must fail closed");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_COMMIT_FAILED.into()));
    assert_eq!(error.status_code(), Some(StatusCode::CONFLICT));
    assert_events_table_entry_unchanged(&store, &current).await;
    assert!(
        !metadata_backend
            .object_exists("warehouse", &fallback_location)
            .await
            .expect("fallback metadata lookup should succeed")
    );
}

#[tokio::test]
async fn standard_commit_recovers_matching_table_scoped_metadata_orphan() {
    let (store, metadata_backend, namespace, current, current_metadata, request, fallback_location) =
        standard_commit_foreign_primary_fixture().await;
    let previous_metadata_location = table_metadata_location_for_client("warehouse", &current.metadata_location);
    let timestamp_ms = current_metadata["last-updated-ms"]
        .as_i64()
        .expect("current metadata should include last-updated-ms")
        .saturating_add(1);
    let matching_metadata =
        apply_table_commit_updates_at(current_metadata, &request.updates, &previous_metadata_location, timestamp_ms)
            .expect("matching fallback metadata should build");
    metadata_backend
        .put_json("warehouse", &fallback_location, matching_metadata)
        .await;

    let committed = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect("a matching fallback orphan should be recoverable");

    assert_eq!(
        committed.metadata_location,
        table_metadata_location_for_client("warehouse", &fallback_location)
    );
    assert_eq!(committed.metadata["properties"]["owner"], "target");
    let committed_entry = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    assert_eq!(committed_entry.generation, current.generation + 1);
}

#[tokio::test]
async fn concurrent_identical_commits_reuse_table_scoped_metadata_winner() {
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::StrongTableCatalogStore::new(metadata_backend.clone());
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let current = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let mut foreign_metadata = read_table_metadata_json(&metadata_backend, "warehouse", &current.metadata_location)
        .await
        .expect("current metadata should load");
    foreign_metadata["table-uuid"] = serde_json::Value::String(Uuid::new_v4().to_string());
    let commit_id = "11111111-1111-4111-8111-111111111111";
    let primary_location =
        crate::table_catalog::table_metadata_file_path_for_entry(&current, &next_metadata_file_name(2, commit_id))
            .expect("primary metadata path should be valid");
    metadata_backend
        .put_json("warehouse", &primary_location, foreign_metadata)
        .await;

    let barrier_backend = TestTableCatalogObjectBackend {
        put_object_barrier: Some(Arc::new(tokio::sync::Barrier::new(2))),
        ..metadata_backend.clone()
    };
    let first_backend = trusted_table_commit_backend(&barrier_backend);
    let second_backend = trusted_table_commit_backend(&barrier_backend);
    let (first, second) = tokio::join!(
        standard_commit_table_response(
            &store,
            &first_backend,
            "warehouse",
            &namespace,
            "events",
            standard_property_commit_request(commit_id, &current.table_uuid, "target"),
        ),
        standard_commit_table_response(
            &store,
            &second_backend,
            "warehouse",
            &namespace,
            "events",
            standard_property_commit_request(commit_id, &current.table_uuid, "target"),
        )
    );
    let first = first.expect("first identical commit should succeed");
    let second = second.expect("second identical commit should replay the winner");

    assert_eq!(first.commit_id, second.commit_id);
    assert_eq!(first.metadata_location, second.metadata_location);
    assert_eq!(first.metadata, second.metadata);
    assert!(
        first
            .metadata_location
            .ends_with(&table_scoped_metadata_file_name(2, &current.table_id, commit_id))
    );
    let committed = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    assert_eq!(committed.generation, current.generation + 1);
}

#[tokio::test]
async fn standard_commit_rejects_mismatched_table_scoped_metadata_orphan() {
    let (store, metadata_backend, namespace, current, current_metadata, request, fallback_location) =
        standard_commit_foreign_primary_fixture().await;
    let previous_metadata_location = table_metadata_location_for_client("warehouse", &current.metadata_location);
    let timestamp_ms = current_metadata["last-updated-ms"]
        .as_i64()
        .expect("current metadata should include last-updated-ms")
        .saturating_add(1);
    let mismatched_request =
        standard_property_commit_request("11111111-1111-4111-8111-111111111111", &current.table_uuid, "different");
    let mismatched_metadata =
        apply_table_commit_updates_at(current_metadata, &mismatched_request.updates, &previous_metadata_location, timestamp_ms)
            .expect("mismatched fallback metadata should build");
    metadata_backend
        .put_json("warehouse", &fallback_location, mismatched_metadata)
        .await;

    let error = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect_err("a fallback orphan for another payload must fail closed");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_COMMIT_FAILED.into()));
    assert_eq!(error.status_code(), Some(StatusCode::CONFLICT));
    assert_events_table_entry_unchanged(&store, &current).await;
}

#[tokio::test]
async fn standard_commit_rejects_foreign_table_scoped_metadata_as_server_state() {
    let (store, metadata_backend, namespace, current, mut current_metadata, request, fallback_location) =
        standard_commit_foreign_primary_fixture().await;
    current_metadata["table-uuid"] = serde_json::Value::String(Uuid::new_v4().to_string());
    metadata_backend
        .put_json("warehouse", &fallback_location, current_metadata)
        .await;

    let error = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect_err("a table-scoped path owned by another table must fail as server state");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
    assert_eq!(error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert_events_table_entry_unchanged(&store, &current).await;
}

#[tokio::test]
async fn standard_commit_reports_missing_generated_metadata_as_server_state() {
    let (store, metadata_backend, namespace, current, current_metadata, request, fallback_location) =
        standard_commit_foreign_primary_fixture().await;
    let previous_metadata_location = table_metadata_location_for_client("warehouse", &current.metadata_location);
    let timestamp_ms = current_metadata["last-updated-ms"]
        .as_i64()
        .expect("current metadata should include last-updated-ms")
        .saturating_add(1);
    let matching_metadata =
        apply_table_commit_updates_at(current_metadata, &request.updates, &previous_metadata_location, timestamp_ms)
            .expect("matching fallback metadata should build");
    metadata_backend
        .put_json("warehouse", &fallback_location, matching_metadata)
        .await;
    *metadata_backend.missing_read_object_path.lock().await = Some(fallback_location.clone());

    let error = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect_err("a generated object that disappears before readback must fail as server state");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
    assert_eq!(error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert_eq!(error.message(), Some("generated metadata object is missing"));
    assert_events_table_entry_unchanged(&store, &current).await;
    assert!(
        metadata_backend
            .object_exists("warehouse", &fallback_location)
            .await
            .expect("fallback metadata lookup should succeed")
    );
}

#[tokio::test]
async fn standard_commit_redacts_generated_metadata_read_failures() {
    let (store, metadata_backend, namespace, current, _current_metadata, request, fallback_location) =
        standard_commit_foreign_primary_fixture().await;
    *metadata_backend.fail_read_object_path.lock().await = Some(fallback_location);

    let error = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect_err("generated metadata read failures must be redacted");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
    assert_eq!(error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert_eq!(error.message(), Some("existing generated metadata is invalid"));
    assert!(!error.message().is_some_and(|message| message.contains("private")));
    assert_events_table_entry_unchanged(&store, &current).await;
}

#[tokio::test]
async fn standard_commit_propagates_fallback_write_failure() {
    let (store, metadata_backend, namespace, current, _current_metadata, request, fallback_location) =
        standard_commit_foreign_primary_fixture().await;
    *metadata_backend.fail_put_object_path.lock().await = Some(fallback_location.clone());

    let error = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect_err("fallback write failures must be propagated");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
    assert_eq!(error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert_events_table_entry_unchanged(&store, &current).await;
    assert!(
        !metadata_backend
            .object_exists("warehouse", &fallback_location)
            .await
            .expect("fallback metadata lookup should succeed")
    );
}

#[tokio::test]
async fn standard_commit_rejects_fallback_readback_mismatch() {
    let (store, metadata_backend, namespace, current, _current_metadata, request, fallback_location) =
        standard_commit_foreign_primary_fixture().await;
    *metadata_backend.corrupt_put_object_path.lock().await = Some(fallback_location);

    let error = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect_err("fallback readback changes must prevent catalog publication");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_COMMIT_FAILED.into()));
    assert_eq!(error.status_code(), Some(StatusCode::CONFLICT));
    assert_events_table_entry_unchanged(&store, &current).await;
}

#[tokio::test]
async fn standard_commit_uses_client_uuid_commit_id_in_metadata_file_name() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    create_standard_events_table(&store, &metadata_backend, &namespace).await;

    let commit_id = "11111111-1111-4111-8111-111111111111";
    let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
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
    let commit = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        commit_request,
    )
    .await
    .expect("standard commit should succeed");

    assert_eq!(commit.commit_id, commit_id);
    assert_eq!(
        commit.metadata_location,
        "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-11111111-1111-4111-8111-111111111111.metadata.json"
    );
    let committed = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("committed table lookup should succeed")
        .expect("committed table should exist");
    assert!(
        metadata_backend
            .object_exists("warehouse", &committed.metadata_location)
            .await
            .expect("committed metadata lookup should succeed")
    );
}

#[tokio::test]
async fn standard_commit_accepts_non_uuid_client_commit_id_without_using_it_in_metadata_file_name() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    create_standard_events_table(&store, &metadata_backend, &namespace).await;

    let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
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
    let commit = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        commit_request,
    )
    .await
    .expect("standard commit should succeed");

    let metadata_file_prefix =
        "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-";
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
async fn commit_publication_uses_idempotency_key_as_retry_identity() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let idempotency_key = Uuid::now_v7().to_string();
    let request = serde_json::from_value(serde_json::json!({
        "idempotency-key": idempotency_key,
        "requirements": [],
        "updates": [{
            "action": "set-properties",
            "updates": {"owner": "lakehouse"}
        }]
    }))
    .expect("standard commit request should parse");

    let commit = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect("standard commit should succeed");

    assert_eq!(commit.commit_id, idempotency_key);
    assert!(commit.metadata_location.contains(&idempotency_key));
}

#[tokio::test]
async fn commit_publication_replays_historical_standard_commit_across_backings() {
    for mode in [
        crate::table_catalog::TableCatalogBackingMode::ObjectBacked,
        crate::table_catalog::TableCatalogBackingMode::DurableStrong,
    ] {
        let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
        let store = crate::table_catalog::ConfiguredTableCatalogStore::new_for_test(metadata_backend.clone(), mode);
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let first_request = serde_json::json!({
            "commit-id": "commit-a",
            "idempotency-key": "request-a",
            "writer": "test-client",
            "requirements": [],
            "updates": [{
                "action": "set-properties",
                "updates": {"owner": "first"}
            }]
        });
        let first = standard_commit_table_response(
            &store,
            &trusted_table_commit_backend(&metadata_backend),
            "warehouse",
            &namespace,
            "events",
            serde_json::from_value(first_request.clone()).expect("first commit request should parse"),
        )
        .await
        .expect("first commit should update the table");

        if mode == crate::table_catalog::TableCatalogBackingMode::ObjectBacked {
            let current = store
                .load_table("warehouse", "analytics", "events")
                .await
                .expect("first committed table lookup should succeed")
                .expect("first committed table should exist");
            let mut historical = store
                .get_commit_by_id("warehouse", &current.table_id, "commit-a")
                .await
                .expect("historical commit lookup should succeed")
                .expect("historical commit should exist");
            historical.requirements = vec![metadata_digest_requirement(&first.metadata).expect("metadata digest should build")];
            let paths = crate::table_catalog::TableCatalogObjectPaths::default();
            let commit_path = paths.commit_log_entry_path("warehouse", &current.table_id, "commit-a");
            let idempotency_path = paths.commit_idempotency_entry_path("warehouse", &current.table_id, "request-a");
            let historical = serde_json::to_value(historical).expect("historical commit should serialize");
            for path in [commit_path, idempotency_path] {
                metadata_backend
                    .put_json(crate::admin::storage_api::RUSTFS_META_BUCKET, &path, historical.clone())
                    .await;
            }
        }

        let store = crate::table_catalog::ConfiguredTableCatalogStore::new_for_test(metadata_backend.clone(), mode);
        let second = standard_commit_table_response(
            &store,
            &trusted_table_commit_backend(&metadata_backend),
            "warehouse",
            &namespace,
            "events",
            serde_json::from_value(serde_json::json!({
                "commit-id": "commit-b",
                "idempotency-key": "request-b",
                "requirements": [],
                "updates": [{
                    "action": "set-properties",
                    "updates": {"owner": "current"}
                }]
            }))
            .expect("second commit request should parse"),
        )
        .await
        .expect("second commit should advance the table");
        let current = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("current table lookup should succeed")
            .expect("current table should exist");

        let replay = standard_commit_table_response(
            &store,
            &trusted_table_commit_backend(&metadata_backend),
            "warehouse",
            &namespace,
            "events",
            serde_json::from_value(first_request.clone()).expect("first commit retry should parse"),
        )
        .await
        .expect("an exact old commit retry should succeed");

        assert_eq!(replay.commit_id, "commit-a", "{mode:?}");
        assert_eq!(replay.metadata_location, second.metadata_location, "{mode:?}");
        assert_eq!(replay.version_token, second.version_token, "{mode:?}");
        assert_eq!(replay.generation, second.generation, "{mode:?}");
        assert_eq!(replay.metadata["properties"]["owner"], "current", "{mode:?}");
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup after replay should succeed")
            .expect("table should remain present");
        assert_eq!(unchanged.metadata_location, current.metadata_location, "{mode:?}");
        assert_eq!(unchanged.version_token, current.version_token, "{mode:?}");

        let mut mutated_request = first_request;
        mutated_request["updates"][0]["updates"]["owner"] = serde_json::Value::String("mutated".to_string());
        let error = standard_commit_table_response(
            &store,
            &trusted_table_commit_backend(&metadata_backend),
            "warehouse",
            &namespace,
            "events",
            serde_json::from_value(mutated_request).expect("mutated retry should parse"),
        )
        .await
        .expect_err("mutating updates under a persisted idempotency key must conflict");
        assert_eq!(error.status_code(), Some(StatusCode::CONFLICT), "{mode:?}");
    }
}

#[tokio::test]
async fn staged_standard_commit_retry_revalidates_referenced_objects() {
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone());
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let current = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let table_name = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_snapshot_manifest(&metadata_backend, "warehouse", &manifest_list, 10, 1, &[(&data_file, 0, 1, 10, 1)]).await;
    let commit_id = "11111111-1111-4111-8111-111111111111";
    let request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "commit-id": commit_id,
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": manifest_list,
                    "summary": {"operation": "append"}
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
    .expect("standard commit request should parse");
    let previous_metadata = read_table_metadata_json(&metadata_backend, "warehouse", &current.metadata_location)
        .await
        .expect("current metadata should load");
    let target_metadata = apply_table_commit_updates_at(
        previous_metadata,
        &request.updates,
        &table_metadata_location_for_client("warehouse", &current.metadata_location),
        2000,
    )
    .expect("target metadata should build");
    let (_, metadata_file_token) = standard_commit_ids(Some(commit_id.to_string()));
    let target_location = crate::table_catalog::default_table_metadata_file_path(
        &namespace,
        &table_name,
        &next_metadata_file_name(current.generation.saturating_add(1), &metadata_file_token),
    );
    metadata_backend
        .put_json("warehouse", &target_location, target_metadata.clone())
        .await;
    let staged = crate::table_catalog::CommitLogEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        commit_id: commit_id.to_string(),
        idempotency_key: None,
        table_id: current.table_id.clone(),
        operation: table_commit_operation(&target_metadata),
        expected_version_token: current.version_token.clone(),
        new_version_token: "staged-token".to_string(),
        previous_metadata_location: current.metadata_location.clone(),
        new_metadata_location: target_location,
        requirements: Vec::new(),
        status: crate::table_catalog::CommitLogStatus::Staged,
        writer: None,
        created_at: None,
        updated_at: None,
    };
    let commit_path =
        crate::table_catalog::TableCatalogObjectPaths::default().commit_log_entry_path("warehouse", &current.table_id, commit_id);
    metadata_backend
        .put_json(
            crate::admin::storage_api::RUSTFS_META_BUCKET,
            &commit_path,
            serde_json::to_value(staged).expect("staged commit should serialize"),
        )
        .await;
    let data_object = test_snapshot_object_key("warehouse", &data_file);
    crate::table_catalog::TableCatalogObjectBackend::delete_object(&metadata_backend, "warehouse", &data_object)
        .await
        .expect("referenced data object should be removed before retry");
    let authorized = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let commit_backend = TableCommitObjectBackend::test(metadata_backend, Arc::clone(&authorized), None);

    let result = standard_commit_table_response(&store, &commit_backend, "warehouse", &namespace, "events", request).await;
    commit_backend
        .finish(result)
        .await
        .expect_err("a staged retry must reject a now-missing referenced object");

    assert!(authorized.lock().await.contains(&(data_object, S3Action::GetObjectAction)));
    let unchanged = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should remain");
    assert_eq!(unchanged.metadata_location, current.metadata_location);
    assert_eq!(unchanged.version_token, current.version_token);
}

#[tokio::test]
async fn commit_publication_denies_generated_metadata_write_before_pointer_advance() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let before = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let commit_id = "11111111-1111-4111-8111-111111111111";
    let table_name = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let next_metadata_location = crate::table_catalog::default_table_metadata_file_path(
        &namespace,
        &table_name,
        &next_metadata_file_name(before.generation.saturating_add(1), commit_id),
    );
    let request = serde_json::from_value(serde_json::json!({
        "commit-id": commit_id,
        "requirements": [],
        "updates": [{
            "action": "set-properties",
            "updates": {"owner": "lakehouse"}
        }]
    }))
    .expect("commit request should parse");
    let authorized = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let commit_backend =
        TableCommitObjectBackend::test(metadata_backend.clone(), Arc::clone(&authorized), Some(next_metadata_location.clone()));

    let result = commit_table_response(&store, &commit_backend, "warehouse", &namespace, "events", request).await;
    let error = commit_backend
        .finish(result)
        .await
        .expect_err("denied metadata write should fail the commit");

    assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
    assert!(
        authorized
            .lock()
            .await
            .contains(&(next_metadata_location.clone(), S3Action::PutObjectAction))
    );
    let after = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should remain");
    assert_eq!(after.metadata_location, before.metadata_location);
    assert_eq!(after.version_token, before.version_token);
    assert_eq!(after.generation, before.generation);
    assert!(
        !metadata_backend
            .object_exists("warehouse", &next_metadata_location)
            .await
            .expect("metadata object lookup should succeed")
    );
}

#[tokio::test]
async fn commit_publication_authorizes_referenced_objects() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let entry = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let manifest = format!("{table_location}/metadata/manifest-snap-10.avro");
    let data_file = format!("{table_location}/data/part-10.parquet");
    let statistics_file = format!("{table_location}/metadata/stats-10.puffin");
    let partition_statistics_file = format!("{table_location}/metadata/partition-stats-10.parquet");
    let statistics_bytes = b"PFA1PFA1".to_vec();
    let partition_statistics_bytes = test_parquet_i32_bytes(&[1]);
    seed_test_snapshot_manifest(&metadata_backend, "warehouse", &manifest_list, 10, 1, &[(&data_file, 0, 1, 10, 1)]).await;
    metadata_backend
        .put_bytes(
            "warehouse",
            &test_snapshot_object_key("warehouse", &statistics_file),
            statistics_bytes.clone(),
        )
        .await;
    metadata_backend
        .put_bytes(
            "warehouse",
            &test_snapshot_object_key("warehouse", &partition_statistics_file),
            partition_statistics_bytes.clone(),
        )
        .await;
    let request = serde_json::from_value(serde_json::json!({
        "commit-id": "22222222-2222-4222-8222-222222222222",
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": manifest_list,
                    "summary": {"operation": "append"}
                }
            },
            {
                "action": "set-snapshot-ref",
                "ref-name": "main",
                "snapshot-id": 10,
                "type": "branch"
            },
            {
                "action": "set-statistics",
                "statistics": {
                    "snapshot-id": 10,
                    "statistics-path": statistics_file,
                    "file-size-in-bytes": statistics_bytes.len(),
                    "file-footer-size-in-bytes": 0,
                    "blob-metadata": []
                }
            },
            {
                "action": "set-partition-statistics",
                "partition-statistics": {
                    "snapshot-id": 10,
                    "statistics-path": partition_statistics_file,
                    "file-size-in-bytes": partition_statistics_bytes.len()
                }
            }
        ]
    }))
    .expect("standard commit request should parse");
    let authorized = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let commit_backend = TableCommitObjectBackend::test(metadata_backend.clone(), Arc::clone(&authorized), None);

    let result = commit_table_response(&store, &commit_backend, "warehouse", &namespace, "events", request).await;
    commit_backend
        .finish(result)
        .await
        .expect("authorized standard commit should succeed");

    let authorizations = authorized.lock().await;
    let expected_reads = [
        entry.metadata_location,
        test_snapshot_object_key("warehouse", &manifest_list),
        test_snapshot_object_key("warehouse", &manifest),
        test_snapshot_object_key("warehouse", &data_file),
        test_snapshot_object_key("warehouse", &statistics_file),
        test_snapshot_object_key("warehouse", &partition_statistics_file),
    ];
    for object in expected_reads {
        assert!(
            authorizations
                .iter()
                .any(|(authorized_object, action)| authorized_object == &object && *action == S3Action::GetObjectAction),
            "missing GetObject authorization for {object}"
        );
    }
}

#[tokio::test]
async fn commit_publication_denies_referenced_data_read_before_pointer_advance() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let before = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_snapshot_manifest(&metadata_backend, "warehouse", &manifest_list, 10, 1, &[(&data_file, 0, 1, 10, 1)]).await;
    let request = serde_json::from_value(serde_json::json!({
        "commit-id": "33333333-3333-4333-8333-333333333333",
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": manifest_list,
                    "summary": {"operation": "append"}
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
    .expect("standard commit request should parse");
    let denied_object = test_snapshot_object_key("warehouse", &data_file);
    let authorized = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let commit_backend = TableCommitObjectBackend::test(metadata_backend, Arc::clone(&authorized), Some(denied_object.clone()));

    let result = commit_table_response(&store, &commit_backend, "warehouse", &namespace, "events", request).await;
    let error = commit_backend
        .finish(result)
        .await
        .expect_err("denied referenced data read should fail the commit");

    assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
    assert!(authorized.lock().await.contains(&(denied_object, S3Action::GetObjectAction)));
    let after = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should remain");
    assert_eq!(after.metadata_location, before.metadata_location);
    assert_eq!(after.version_token, before.version_token);
    assert_eq!(after.generation, before.generation);
}

#[tokio::test]
async fn commit_publication_holds_referenced_object_locks_until_pointer_publish() {
    let pause = TestCatalogPublishPause::default();
    let store = Arc::new(TestTableCatalogStore {
        commit_table_pause: Some(pause.clone()),
        ..Default::default()
    });
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(store.as_ref(), &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let manifest = format!("{table_location}/metadata/manifest-snap-10.avro");
    let data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_snapshot_manifest(&metadata_backend, "warehouse", &manifest_list, 10, 1, &[(&data_file, 0, 1, 10, 1)]).await;
    let request = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": manifest_list,
                    "summary": {"operation": "append"}
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
    .expect("standard commit request should parse");
    let commit_store = Arc::clone(&store);
    let commit_namespace = namespace.clone();
    let commit_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let commit = tokio::spawn(async move {
        let result =
            commit_table_response(commit_store.as_ref(), &commit_backend, "warehouse", &commit_namespace, "events", request)
                .await;
        commit_backend.finish(result).await
    });

    tokio::time::timeout(StdDuration::from_secs(2), pause.wait_started())
        .await
        .expect("commit should reach catalog publication");
    let table_name = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let publication_lock = crate::table_catalog::default_table_publication_lock_path(&namespace, &table_name);
    assert!(
        metadata_backend.write_lock_is_held("warehouse", &publication_lock).await,
        "table publication fence must remain held during catalog publication"
    );
    assert!(
        !metadata_backend
            .write_lock_is_held("warehouse", &crate::table_catalog::default_table_bucket_publication_lock_path(),)
            .await,
        "same-warehouse commits must not serialize unrelated tables through the table-bucket fence"
    );
    for location in [&manifest_list, &manifest] {
        let object = test_snapshot_object_key("warehouse", location);
        assert!(
            metadata_backend.write_lock_is_held("warehouse", &object).await,
            "snapshot metadata lock must remain held during catalog publication: {location}"
        );
    }
    let data_object = test_snapshot_object_key("warehouse", &data_file);
    assert!(
        !metadata_backend.write_lock_is_held("warehouse", &data_object).await,
        "live data files must not consume one retained publication lock per object"
    );
    metadata_backend.lock_attempts.lock().await.clear();
    let data_plane_backend = metadata_backend.clone();
    let data_plane_publication_lock = publication_lock.clone();
    let data_plane = tokio::spawn(async move {
        crate::table_catalog::TableCatalogObjectBackend::acquire_read_lock(
            &data_plane_backend,
            "warehouse",
            &data_plane_publication_lock,
        )
        .await
    });
    metadata_backend.wait_for_lock_attempts(1).await;
    assert!(!data_plane.is_finished(), "a data-plane mutation must wait for catalog publication");
    pause.release();
    tokio::time::timeout(StdDuration::from_secs(2), commit)
        .await
        .expect("commit task should complete")
        .expect("commit task should join")
        .expect("standard commit should succeed");
    tokio::time::timeout(StdDuration::from_secs(2), data_plane)
        .await
        .expect("data-plane publication guard should unblock")
        .expect("data-plane guard task should join")
        .expect("data-plane guard acquisition should succeed");
    assert!(
        !metadata_backend.write_lock_is_held("warehouse", &publication_lock).await,
        "table publication fence must be released after catalog publication"
    );
    for location in [&manifest_list, &manifest] {
        let object = test_snapshot_object_key("warehouse", location);
        assert!(
            !metadata_backend.write_lock_is_held("warehouse", &object).await,
            "snapshot metadata lock must be released after catalog publication: {location}"
        );
    }
}

#[tokio::test]
async fn rolling_upgrade_commit_retains_legacy_data_file_guard_until_publication_completes() {
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let data_file = "tables/table-id/data/part-00001.parquet";
    metadata_backend.put_bytes("warehouse", data_file, b"data".to_vec()).await;
    let commit_backend = TableCommitObjectBackend::rolling_upgrade(metadata_backend.clone());
    assert!(
        crate::table_catalog::TableCatalogObjectBackend::object_exists(&commit_backend, "warehouse", data_file)
            .await
            .expect("data-file discovery should succeed")
    );

    crate::table_catalog::TableCatalogObjectBackend::prepare_table_commit_publication(
        &commit_backend,
        "warehouse",
        "analytics",
        "events",
    )
    .await
    .expect("rolling-upgrade publication should prepare");
    assert!(
        metadata_backend.write_lock_is_held("warehouse", data_file).await,
        "old-node object writers must remain fenced until every node uses table publication locks"
    );

    let old_writer_backend = metadata_backend.clone();
    let old_writer = tokio::spawn(async move {
        crate::table_catalog::TableCatalogObjectBackend::acquire_write_lock(&old_writer_backend, "warehouse", data_file).await
    });
    metadata_backend.wait_for_lock_attempts(3).await;
    assert!(!old_writer.is_finished(), "an old-node object writer must wait for catalog publication");

    crate::table_catalog::TableCatalogObjectBackend::complete_table_commit_publication(&commit_backend);
    tokio::time::timeout(StdDuration::from_secs(2), old_writer)
        .await
        .expect("old-node writer should continue after publication")
        .expect("old-node writer task should join")
        .expect("old-node writer lock acquisition should succeed");
}

#[tokio::test]
async fn rolling_upgrade_initial_publication_fences_old_and_new_data_plane_writers() {
    let pause = TestCatalogPublishPause::default();
    let store = Arc::new(TestTableCatalogStore {
        register_table_pause: Some(pause.clone()),
        ..Default::default()
    });
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    ensure_table_bucket_entry(store.as_ref(), "warehouse", true)
        .await
        .expect("table bucket entry should seed");
    create_namespace_response(
        store.as_ref(),
        "warehouse",
        CreateNamespaceRequest {
            namespace: vec!["analytics".to_string()],
            properties: BTreeMap::new(),
        },
        true,
    )
    .await
    .expect("namespace should seed");

    let data_file = "tables/table-id/data/part-00001.parquet";
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    metadata_backend.put_bytes("warehouse", data_file, b"data".to_vec()).await;
    let publication_backend = TableCommitObjectBackend::rolling_upgrade(metadata_backend.clone());
    assert!(
        crate::table_catalog::TableCatalogObjectBackend::object_exists(&publication_backend, "warehouse", data_file)
            .await
            .expect("data-file discovery should succeed")
    );
    let entry = crate::table_catalog::TableEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: "warehouse".to_string(),
        namespace: namespace.public_name(),
        table: "events".to_string(),
        table_id: "table-id".to_string(),
        table_uuid: "table-uuid".to_string(),
        format: "ICEBERG".to_string(),
        format_version: 2,
        warehouse_location: "s3://warehouse/tables/table-id".to_string(),
        metadata_location: "s3://warehouse/tables/table-id/metadata/v1.metadata.json".to_string(),
        version_token: "token-v1".to_string(),
        generation: 1,
        state: crate::table_catalog::TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    };
    let register_store = Arc::clone(&store);
    let register_backend = publication_backend.clone();
    let register = tokio::spawn(async move { register_store.register_table_with_publication(entry, &register_backend).await });

    tokio::time::timeout(StdDuration::from_secs(2), pause.wait_started())
        .await
        .expect("registration should reach catalog publication");
    let bucket_publication_lock = crate::table_catalog::default_table_bucket_publication_lock_path();
    assert!(
        metadata_backend
            .write_lock_is_held("warehouse", &bucket_publication_lock)
            .await,
        "initial publication must retain the bucket fence before the table entry is visible"
    );
    assert!(
        metadata_backend.write_lock_is_held("warehouse", data_file).await,
        "rolling-upgrade publication must retain the exact object guard for old-node writers"
    );
    assert!(
        store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .is_none(),
        "the table must still be invisible while the registration is paused"
    );

    metadata_backend.lock_attempts.lock().await.clear();
    let new_writer_backend = metadata_backend.clone();
    let new_writer_lock = bucket_publication_lock.clone();
    let new_writer = tokio::spawn(async move {
        crate::table_catalog::TableCatalogObjectBackend::acquire_read_lock(&new_writer_backend, "warehouse", &new_writer_lock)
            .await
    });
    let old_writer_backend = metadata_backend.clone();
    let old_writer = tokio::spawn(async move {
        crate::table_catalog::TableCatalogObjectBackend::acquire_write_lock(&old_writer_backend, "warehouse", data_file).await
    });
    metadata_backend.wait_for_lock_attempts(2).await;
    assert!(
        !new_writer.is_finished(),
        "a new-node first-publication writer must wait on the table-bucket fence"
    );
    assert!(!old_writer.is_finished(), "an old-node writer must wait on the exact object guard");

    pause.release();
    tokio::time::timeout(StdDuration::from_secs(2), register)
        .await
        .expect("registration should complete after the pause is released")
        .expect("registration task should join")
        .expect("registration should succeed");
    tokio::time::timeout(StdDuration::from_secs(2), new_writer)
        .await
        .expect("new-node writer should continue after registration")
        .expect("new-node writer task should join")
        .expect("new-node writer guard acquisition should succeed");
    tokio::time::timeout(StdDuration::from_secs(2), old_writer)
        .await
        .expect("old-node writer should continue after registration")
        .expect("old-node writer task should join")
        .expect("old-node writer guard acquisition should succeed");
}

#[tokio::test]
async fn warehouse_relocation_holds_bucket_fence_before_catalog_publication() {
    let pause = TestCatalogPublishPause::default();
    let store = Arc::new(TestTableCatalogStore {
        commit_table_pause: Some(pause.clone()),
        ..Default::default()
    });
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(store.as_ref(), &metadata_backend, &namespace).await;
    let current = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let mut target_metadata = created.metadata;
    target_metadata["location"] = serde_json::json!("s3://warehouse/tables/relocated-table-id");
    let table_bucket_fence_required =
        table_warehouse_location_changes(&current, &target_metadata).expect("relocated warehouse location should be valid");
    assert!(table_bucket_fence_required);

    let commit_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let commit_store = Arc::clone(&store);
    let commit = tokio::spawn(async move {
        publish_table_commit(
            commit_store.as_ref(),
            &commit_backend,
            table_bucket_fence_required,
            crate::table_catalog::TableCommitRequest {
                table_bucket: "warehouse".to_string(),
                namespace: "analytics".to_string(),
                table: "events".to_string(),
                commit_id: "relocate-commit".to_string(),
                idempotency_key: None,
                operation: "set-location".to_string(),
                expected_version_token: current.version_token,
                expected_metadata_location: current.metadata_location,
                new_metadata_location: "tables/catalog/analytics/events/metadata/00002.metadata.json".to_string(),
                requirements: Vec::new(),
                writer: Some("test".to_string()),
            },
        )
        .await
    });

    tokio::time::timeout(StdDuration::from_secs(2), pause.wait_started())
        .await
        .expect("relocation should reach catalog publication");
    let bucket_publication_lock = crate::table_catalog::default_table_bucket_publication_lock_path();
    assert!(
        metadata_backend
            .write_lock_is_held("warehouse", &bucket_publication_lock)
            .await,
        "warehouse relocation must retain the table-bucket fence before pointer publication"
    );
    metadata_backend.lock_attempts.lock().await.clear();
    let writer_backend = metadata_backend.clone();
    let writer_lock = bucket_publication_lock.clone();
    let writer = tokio::spawn(async move {
        crate::table_catalog::TableCatalogObjectBackend::acquire_read_lock(&writer_backend, "warehouse", &writer_lock).await
    });
    metadata_backend.wait_for_lock_attempts(1).await;
    assert!(
        !writer.is_finished(),
        "a writer in the new warehouse prefix must wait for relocation publication"
    );

    pause.release();
    tokio::time::timeout(StdDuration::from_secs(2), commit)
        .await
        .expect("relocation commit should complete")
        .expect("relocation task should join")
        .expect("relocation commit should succeed");
    tokio::time::timeout(StdDuration::from_secs(2), writer)
        .await
        .expect("writer should continue after relocation publication")
        .expect("writer task should join")
        .expect("writer guard acquisition should succeed");
}

#[tokio::test]
async fn commit_publication_lock_order_remains_compatible_with_old_maintenance_nodes() {
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let current_metadata = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let new_metadata = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let store = Arc::new(crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone()));
    seed_object_table_for_metadata_maintenance(
        store.as_ref(),
        &metadata_backend,
        "warehouse",
        &namespace,
        &table,
        current_metadata.clone(),
    )
    .await;
    metadata_backend
        .put_json("warehouse", &new_metadata, serde_json::json!({}))
        .await;
    let commit_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    crate::table_catalog::TableCatalogObjectBackend::read_object(&commit_backend, "warehouse", &new_metadata)
        .await
        .expect("new metadata discovery should succeed")
        .expect("new metadata should exist");

    let table_path = crate::table_catalog::TableCatalogObjectPaths::default().table_entry_path("warehouse", &namespace, &table);
    let old_table_guard = crate::table_catalog::TableCatalogObjectBackend::acquire_write_lock(
        &metadata_backend,
        crate::admin::storage_api::RUSTFS_META_BUCKET,
        &table_path,
    )
    .await
    .expect("old maintenance node should acquire the table lock first");
    metadata_backend.lock_attempts.lock().await.clear();
    let commit_store = Arc::clone(&store);
    let publication_backend = commit_backend.clone();
    let commit_new_metadata = new_metadata.clone();
    let commit = tokio::spawn(async move {
        commit_store
            .commit_table_with_publication(
                crate::table_catalog::TableCommitRequest {
                    table_bucket: "warehouse".to_string(),
                    namespace: "analytics".to_string(),
                    table: "events".to_string(),
                    commit_id: "mixed-version-lock-order".to_string(),
                    idempotency_key: None,
                    operation: "append".to_string(),
                    expected_version_token: "token-v1".to_string(),
                    expected_metadata_location: current_metadata,
                    new_metadata_location: commit_new_metadata,
                    requirements: Vec::new(),
                    writer: Some("rolling-upgrade-test".to_string()),
                },
                &publication_backend,
            )
            .await
    });
    metadata_backend.wait_for_lock_attempts(2).await;
    let migration_lock = crate::table_catalog::TableCatalogObjectPaths::default().backing_migration_fence_lock_path("warehouse");
    assert_eq!(
        metadata_backend.lock_attempts.lock().await.as_slice(),
        &[
            (crate::admin::storage_api::RUSTFS_META_BUCKET.to_string(), migration_lock),
            (crate::admin::storage_api::RUSTFS_META_BUCKET.to_string(), table_path),
        ]
    );

    let old_object_guard = tokio::time::timeout(
        StdDuration::from_secs(2),
        crate::table_catalog::TableCatalogObjectBackend::acquire_write_lock(&metadata_backend, "warehouse", &new_metadata),
    )
    .await
    .expect("new commit must not hold object locks while waiting for the old table lock")
    .expect("old maintenance node should acquire the object lock");
    drop(old_object_guard);
    drop(old_table_guard);
    tokio::time::timeout(StdDuration::from_secs(2), commit)
        .await
        .expect("commit should complete after old maintenance releases its locks")
        .expect("commit task should join")
        .expect("commit should succeed");
}

#[tokio::test]
async fn commit_publication_acquires_discovered_object_locks_in_key_order() {
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let first = "metadata/a.json";
    let last = "metadata/z.json";
    metadata_backend.put_bytes("warehouse", first, b"a".to_vec()).await;
    metadata_backend.put_bytes("warehouse", last, b"z".to_vec()).await;
    let commit_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());

    crate::table_catalog::TableCatalogObjectBackend::read_object(&commit_backend, "warehouse", last)
        .await
        .expect("last object discovery should succeed");
    let first_writer = crate::table_catalog::TableCatalogObjectBackend::acquire_write_lock(&metadata_backend, "warehouse", first)
        .await
        .expect("first object writer should acquire its lock");
    tokio::time::timeout(
        StdDuration::from_secs(2),
        crate::table_catalog::TableCatalogObjectBackend::read_object(&commit_backend, "warehouse", first),
    )
    .await
    .expect("discovery must not retain the last object's lock")
    .expect("first object discovery should succeed");

    metadata_backend.lock_attempts.lock().await.clear();
    let publication_backend = commit_backend.clone();
    let publication = tokio::spawn(async move {
        crate::table_catalog::TableCatalogObjectBackend::prepare_table_commit_publication(
            &publication_backend,
            "warehouse",
            "analytics",
            "events",
        )
        .await
    });
    metadata_backend.wait_for_lock_attempts(2).await;
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let publication_lock = crate::table_catalog::default_table_publication_lock_path(&namespace, &table);
    assert_eq!(
        metadata_backend.lock_attempts.lock().await.as_slice(),
        &[
            ("warehouse".to_string(), publication_lock.clone()),
            ("warehouse".to_string(), first.to_string()),
        ]
    );

    let last_writer = tokio::time::timeout(
        StdDuration::from_secs(2),
        crate::table_catalog::TableCatalogObjectBackend::acquire_write_lock(&metadata_backend, "warehouse", last),
    )
    .await
    .expect("publication waiting on the first key must not retain the last key")
    .expect("last object writer should acquire its lock");
    assert_eq!(
        metadata_backend.lock_attempts.lock().await.pop(),
        Some(("warehouse".to_string(), last.to_string()))
    );
    drop(last_writer);
    drop(first_writer);
    tokio::time::timeout(StdDuration::from_secs(2), publication)
        .await
        .expect("ordered publication locking should complete")
        .expect("publication task should join")
        .expect("publication preparation should succeed");
    assert_eq!(
        metadata_backend.lock_attempts.lock().await.as_slice(),
        &[
            ("warehouse".to_string(), publication_lock),
            ("warehouse".to_string(), first.to_string()),
            ("warehouse".to_string(), last.to_string()),
        ]
    );
    crate::table_catalog::TableCatalogObjectBackend::complete_table_commit_publication(&commit_backend);
}

#[tokio::test]
async fn commit_publication_revalidates_objects_after_ordered_lock_acquisition() {
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let object = "metadata/current.json";
    metadata_backend.put_bytes("warehouse", object, b"before".to_vec()).await;
    let commit_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    crate::table_catalog::TableCatalogObjectBackend::read_object(&commit_backend, "warehouse", object)
        .await
        .expect("object discovery should succeed");
    metadata_backend.put_bytes("warehouse", object, b"after".to_vec()).await;

    let error = crate::table_catalog::TableCatalogObjectBackend::prepare_table_commit_publication(
        &commit_backend,
        "warehouse",
        "analytics",
        "events",
    )
    .await
    .expect_err("changed object must fail publication preparation");
    assert!(matches!(error, crate::table_catalog::TableCatalogStoreError::Conflict(_)));
}

#[tokio::test]
async fn commit_publication_binds_fingerprint_to_returned_bytes() {
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let object = "metadata/current.json";
    let original = b"original".to_vec();
    let replacement = b"replacement".to_vec();
    let original_etag = hex_sha256(&original, str::to_string);
    metadata_backend.state.lock().await.objects.insert(
        ("warehouse".to_string(), object.to_string()),
        TestCatalogObjectRecord {
            data: replacement,
            etag: original_etag.clone(),
            mod_time: None,
        },
    );
    let commit_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    crate::table_catalog::TableCatalogObjectBackend::read_object(&commit_backend, "warehouse", object)
        .await
        .expect("replacement bytes should be discovered")
        .expect("replacement object should exist");
    metadata_backend.state.lock().await.objects.insert(
        ("warehouse".to_string(), object.to_string()),
        TestCatalogObjectRecord {
            data: original,
            etag: original_etag,
            mod_time: None,
        },
    );

    let error = crate::table_catalog::TableCatalogObjectBackend::prepare_table_commit_publication(
        &commit_backend,
        "warehouse",
        "analytics",
        "events",
    )
    .await
    .expect_err("restoring the statted version after reading replacement bytes must fail publication");

    assert!(matches!(error, crate::table_catalog::TableCatalogStoreError::Conflict(_)));
}

#[tokio::test]
async fn standard_commit_publishes_more_than_ten_thousand_live_files() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should include the table location");
    let manifest_list = format!("{table_location}/metadata/boundary-list.avro");
    let manifest = format!("{table_location}/metadata/boundary-manifest.avro");
    let data_file_count = 10_001;
    let data_files = (0..data_file_count)
        .map(|index| format!("{table_location}/data/part-{index:05}.parquet"))
        .collect::<Vec<_>>();
    let manifest_files = data_files.iter().map(|file| (file.as_str(), 0, 1, 20, 1)).collect::<Vec<_>>();
    let manifest_bytes = test_manifest_avro_bytes(&manifest_files);
    metadata_backend
        .put_bytes(
            "warehouse",
            &test_snapshot_object_key("warehouse", &manifest_list),
            test_manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())], 1, 20),
        )
        .await;
    metadata_backend
        .put_bytes("warehouse", &test_snapshot_object_key("warehouse", &manifest), manifest_bytes)
        .await;
    {
        let mut state = metadata_backend.state.lock().await;
        let data = vec![1];
        let etag = hex_sha256(&data, str::to_string);
        for file in &data_files {
            state.objects.insert(
                ("warehouse".to_string(), test_snapshot_object_key("warehouse", file)),
                TestCatalogObjectRecord {
                    data: data.clone(),
                    etag: etag.clone(),
                    mod_time: None,
                },
            );
        }
    }
    let request = serde_json::from_value(serde_json::json!({
        "commit-id": "publication-boundary",
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 20,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": manifest_list,
                    "summary": {"operation": "append"}
                }
            },
            {
                "action": "set-snapshot-ref",
                "ref-name": "main",
                "snapshot-id": 20,
                "type": "branch"
            }
        ]
    }))
    .expect("boundary commit request should parse");
    let commit_backend = TableCommitObjectBackend::trusted(metadata_backend);

    let result = standard_commit_table_response(&store, &commit_backend, "warehouse", &namespace, "events", request).await;
    let response = commit_backend
        .finish(result)
        .await
        .expect("a valid snapshot with more than ten thousand live files should publish");

    assert_eq!(response.metadata["current-snapshot-id"], 20);
    assert_eq!(
        store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("committed table lookup should succeed")
            .expect("committed table should exist")
            .metadata_location,
        table_metadata_location_for_catalog("warehouse", &response.metadata_location)
            .expect("response metadata location should map to the catalog object")
    );
}

#[tokio::test]
async fn commit_publication_rejects_recreated_object_observed_by_exists() {
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let object = "data/part-00001.parquet";
    metadata_backend.put_bytes("warehouse", object, b"before".to_vec()).await;
    let commit_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    assert!(
        crate::table_catalog::TableCatalogObjectBackend::object_exists(&commit_backend, "warehouse", object)
            .await
            .expect("object discovery should succeed")
    );
    crate::table_catalog::TableCatalogObjectBackend::delete_object(&metadata_backend, "warehouse", object)
        .await
        .expect("object delete should succeed");
    metadata_backend.put_bytes("warehouse", object, b"after".to_vec()).await;

    let error = crate::table_catalog::TableCatalogObjectBackend::prepare_table_commit_publication(
        &commit_backend,
        "warehouse",
        "analytics",
        "events",
    )
    .await
    .expect_err("a recreated object with different bytes must fail publication preparation");

    assert!(matches!(error, crate::table_catalog::TableCatalogStoreError::Conflict(_)));
}

#[tokio::test]
async fn standard_commit_ignores_generation_only_orphan_metadata_file() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
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
        "requirements": [],
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
    let commit = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        commit_request,
    )
    .await
    .expect("standard commit should not collide with generation-only orphan");

    assert_eq!(
        commit.metadata_location,
        "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002-22222222-2222-4222-8222-222222222222.metadata.json"
    );
    assert_eq!(commit.metadata["properties"]["owner"], "lakehouse");
}

#[tokio::test]
async fn concurrent_standard_commits_write_distinct_metadata_files_before_pointer_conflict() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    create_standard_events_table(&store, &metadata_backend, &namespace).await;

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let metadata_backend = TestTableCatalogObjectBackend {
        state: Arc::clone(&metadata_backend.state),
        put_object_barrier: Some(barrier),
        ..TestTableCatalogObjectBackend::content_addressed()
    };
    let first_commit_id = "33333333-3333-4333-8333-333333333333";
    let second_commit_id = "44444444-4444-4444-8444-444444444444";
    let first_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
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
        "requirements": [],
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

    let first_backend = trusted_table_commit_backend(&metadata_backend);
    let second_backend = trusted_table_commit_backend(&metadata_backend);
    let (first, second) = tokio::join!(
        commit_table_response(&store, &first_backend, "warehouse", &namespace, "events", first_request),
        commit_table_response(&store, &second_backend, "warehouse", &namespace, "events", second_request)
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
async fn standard_commit_rejects_unbound_legacy_catalog_identity() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
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
            test_table_metadata_json("metadata-table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;

    let commit_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
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
    let error = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        commit_request,
    )
    .await
    .expect_err("a legacy catalog identity that does not match persisted metadata must fail closed");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
    assert_eq!(error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert_events_table_entry_unchanged(&store, &legacy_entry).await;
}

#[tokio::test]
async fn metadata_location_api_rejects_unbound_legacy_catalog_identity() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
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
            test_table_metadata_json("metadata-table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    let next_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
    let mut next_metadata = test_table_metadata_json("metadata-table-uuid", "s3://warehouse/tables/table-id");
    next_metadata["last-sequence-number"] = serde_json::Value::from(2);
    metadata_backend.put_json("warehouse", next_location, next_metadata).await;

    let error = update_table_metadata_location_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        UpdateTableMetadataLocationRequest {
            metadata_location: next_location.to_string(),
            version_token: legacy_entry.version_token.clone(),
            commit_id: Some("commit-1".to_string()),
            idempotency_key: None,
        },
    )
    .await
    .expect_err("metadata-location updates must reject unbound legacy catalog identity");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
    assert_eq!(error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert_events_table_entry_unchanged(&store, &legacy_entry).await;
}

#[tokio::test]
async fn table_metadata_maintenance_helper_runs_dry_run_and_delete() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
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
                "current-snapshot-id": 20,
                "metadata-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 10,
                        "timestamp-ms": 1000,
                        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro"
                    },
                    {
                        "snapshot-id": 20,
                        "timestamp-ms": 2000,
                        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-20.avro"
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 20,
                        "type": "branch"
                    }
                }
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
                ..Default::default()
            },
        )
        .await
        .expect("maintenance config should persist");
    assert_eq!(config.retain_recent_metadata_files, 2);
    assert!(config.delete_enabled);
    let background_config = store
        .put_table_maintenance_config(
            bucket,
            "analytics",
            "events",
            crate::table_catalog::TableMaintenanceConfig {
                version: crate::table_catalog::TABLE_MAINTENANCE_CONFIG_VERSION,
                retain_recent_metadata_files: 2,
                delete_enabled: true,
                background_enabled: true,
                ..Default::default()
            },
        )
        .await
        .expect("background maintenance config should persist");
    assert!(background_config.background_enabled);

    let dry_run = table_metadata_maintenance_response(
        &store,
        &backend,
        bucket,
        &namespace,
        "events",
        TableMetadataMaintenanceRequest {
            retain_recent_metadata_files: 0,
            delete: false,
            snapshot_expiration: Some(crate::table_catalog::TableSnapshotExpirationConfig {
                min_snapshots_to_keep: 1,
                max_snapshot_age_ms: 1,
            }),
            commit_snapshot_expiration: false,
            compaction: Some(crate::table_catalog::TableCompactionPlanningConfig {
                target_file_size_bytes: 512 * 1024 * 1024,
                small_file_threshold_bytes: 64 * 1024 * 1024,
                min_input_files: 2,
                max_rewrite_bytes_per_job: 1024 * 1024 * 1024,
            }),
            commit_compaction: false,
        },
    )
    .await
    .expect("metadata maintenance dry-run should succeed");
    assert_eq!(dry_run.cleanup_candidate_locations, vec![old.clone()]);
    assert_eq!(dry_run.deletable_metadata_locations, vec![old.clone()]);
    let snapshot_expiration = dry_run
        .snapshot_expiration
        .as_ref()
        .expect("dry-run report should include snapshot expiration planning");
    assert_eq!(snapshot_expiration.expiration_candidate_count, 1);
    assert_eq!(snapshot_expiration.current_snapshot_id, Some(20));
    let compaction = dry_run
        .compaction
        .as_ref()
        .expect("dry-run report should include compaction planning");
    assert_eq!(
        compaction.status,
        crate::table_catalog::TableCompactionPlanningStatus::ManualReviewRequired
    );
    assert_eq!(compaction.manual_review_count, 1);
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
        &backend,
        bucket,
        &namespace,
        "events",
        TableMetadataMaintenanceRequest {
            retain_recent_metadata_files: 0,
            delete: true,
            snapshot_expiration: None,
            commit_snapshot_expiration: false,
            compaction: None,
            commit_compaction: false,
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
    let publication_lock = crate::table_catalog::default_table_publication_lock_path(&namespace, &table);
    assert!(
        backend
            .lock_attempts
            .lock()
            .await
            .contains(&(bucket.to_string(), publication_lock)),
        "metadata deletion should enter through the table publication fence"
    );
}

#[tokio::test]
async fn table_metadata_maintenance_helper_commits_snapshot_expiration() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
    let bucket = "warehouse";
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let current = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current.clone()).await;
    backend
        .put_json_with_mod_time(
            bucket,
            &current,
            serde_json::json!({
                "format-version": 2,
                "table-uuid": "table-uuid",
                "location": "s3://warehouse/tables/table-id",
                "last-sequence-number": 2,
                "last-updated-ms": 2000,
                "last-column-id": 1,
                "schemas": [{
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
                }],
                "current-schema-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "sort-orders": [{"order-id": 0, "fields": []}],
                "default-sort-order-id": 0,
                "current-snapshot-id": 20,
                "metadata-log": [],
                "snapshot-log": [
                    {
                        "timestamp-ms": 1000,
                        "snapshot-id": 10
                    },
                    {
                        "timestamp-ms": 2000,
                        "snapshot-id": 20
                    }
                ],
                "snapshots": [
                    {
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "timestamp-ms": 1000,
                        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
                        "summary": {"operation": "append"}
                    },
                    {
                        "snapshot-id": 20,
                        "sequence-number": 2,
                        "timestamp-ms": 2000,
                        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-20.avro",
                        "summary": {"operation": "append"}
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 20,
                        "type": "branch"
                    }
                }
            }),
            Some(OffsetDateTime::UNIX_EPOCH),
        )
        .await;
    backend.lock_attempts.lock().await.clear();

    let report = table_metadata_maintenance_response(
        &store,
        &backend,
        bucket,
        &namespace,
        "events",
        TableMetadataMaintenanceRequest {
            retain_recent_metadata_files: 0,
            delete: false,
            snapshot_expiration: Some(crate::table_catalog::TableSnapshotExpirationConfig {
                min_snapshots_to_keep: 1,
                max_snapshot_age_ms: 1,
            }),
            commit_snapshot_expiration: true,
            compaction: None,
            commit_compaction: false,
        },
    )
    .await
    .expect("snapshot expiration commit should succeed");

    let snapshot_expiration = report
        .snapshot_expiration
        .as_ref()
        .expect("maintenance report should include snapshot expiration");
    assert_eq!(snapshot_expiration.expired_snapshot_ids, vec![10]);
    let committed_location = snapshot_expiration
        .committed_metadata_location
        .as_ref()
        .expect("snapshot expiration commit should report committed metadata")
        .clone();
    assert_ne!(committed_location, current);

    let entry = store
        .load_table(bucket, "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    assert_eq!(entry.metadata_location, committed_location);
    assert_eq!(entry.generation, 2);

    let committed_object = backend
        .read_object(bucket, &entry.metadata_location)
        .await
        .expect("committed metadata lookup should succeed")
        .expect("committed metadata object should exist");
    let committed_metadata =
        serde_json::from_slice::<serde_json::Value>(&committed_object.data).expect("committed metadata should be valid JSON");
    let snapshots = committed_metadata
        .get("snapshots")
        .and_then(serde_json::Value::as_array)
        .expect("committed metadata should contain snapshots");
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].get("snapshot-id").and_then(serde_json::Value::as_i64), Some(20));
    let snapshot_log = committed_metadata
        .get("snapshot-log")
        .and_then(serde_json::Value::as_array)
        .expect("committed metadata should contain snapshot-log");
    assert_eq!(snapshot_log.len(), 1);
    assert_eq!(
        committed_metadata["metadata-log"][0]["metadata-file"],
        serde_json::Value::String(table_metadata_location_for_client(bucket, &current))
    );
    assert!(
        backend
            .object_exists(bucket, &current)
            .await
            .expect("previous metadata lookup should succeed")
    );
    let publication_lock = crate::table_catalog::default_table_publication_lock_path(&namespace, &table);
    assert!(
        backend
            .lock_attempts
            .lock()
            .await
            .contains(&(bucket.to_string(), publication_lock)),
        "internal snapshot expiration commits should enter through the table publication fence"
    );
    assert!(
        backend.lock_attempts.lock().await.contains(&(bucket.to_string(), current)),
        "snapshot expiration must retain the metadata observation through pointer publication"
    );
}

#[tokio::test]
async fn table_metadata_maintenance_helper_commits_compaction_through_publication_observer() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
    let bucket = "warehouse";
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let metadata_dir = crate::table_catalog::default_table_metadata_dir_path(&namespace, &table);
    let data_dir = crate::table_catalog::default_table_data_dir_path(&namespace, &table);
    let current = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let manifest_list = format!("{metadata_dir}/snap-20.avro");
    let manifest = format!("{metadata_dir}/manifest-20.avro");
    let left_data = format!("{data_dir}/part-left.parquet");
    let right_data = format!("{data_dir}/part-right.parquet");
    let manifest_bytes = test_manifest_avro_bytes(&[(&left_data, 0, 0, 20, 7), (&right_data, 0, 0, 20, 7)]);

    seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current.clone()).await;
    backend
        .put_bytes(
            bucket,
            &manifest_list,
            test_manifest_list_avro_bytes(&[(&manifest, manifest_bytes.len())], 7, 20),
        )
        .await;
    backend.put_bytes(bucket, &manifest, manifest_bytes).await;
    backend.put_bytes(bucket, &left_data, test_parquet_i32_bytes(&[1, 2])).await;
    backend.put_bytes(bucket, &right_data, test_parquet_i32_bytes(&[3, 4])).await;
    backend
        .put_json(
            bucket,
            &current,
            serde_json::json!({
                "format-version": 2,
                "table-uuid": "table-uuid",
                "location": "s3://warehouse/tables/table-id",
                "last-sequence-number": 7,
                "last-updated-ms": 2000,
                "metadata-log": [],
                "snapshots": [{
                    "snapshot-id": 20,
                    "sequence-number": 7,
                    "timestamp-ms": 2000,
                    "manifest-list": manifest_list,
                    "summary": {"operation": "append"}
                }],
                "current-snapshot-id": 20,
                "refs": {
                    "main": {
                        "snapshot-id": 20,
                        "type": "branch"
                    }
                }
            }),
        )
        .await;
    backend.lock_attempts.lock().await.clear();

    let report = table_metadata_maintenance_response(
        &store,
        &backend,
        bucket,
        &namespace,
        "events",
        TableMetadataMaintenanceRequest {
            retain_recent_metadata_files: 0,
            delete: false,
            snapshot_expiration: None,
            commit_snapshot_expiration: false,
            compaction: Some(crate::table_catalog::TableCompactionPlanningConfig {
                target_file_size_bytes: 64 * 1024,
                small_file_threshold_bytes: 64 * 1024,
                min_input_files: 2,
                max_rewrite_bytes_per_job: 128 * 1024,
            }),
            commit_compaction: true,
        },
    )
    .await
    .expect("compaction maintenance commit should succeed");

    let compaction = report.compaction.expect("maintenance report should include compaction");
    assert_eq!(compaction.status, crate::table_catalog::TableCompactionPlanningStatus::Committed);
    let lock_attempts = backend.lock_attempts.lock().await;
    for observed_object in [current, manifest_list, manifest, left_data, right_data] {
        assert!(
            lock_attempts.contains(&(bucket.to_string(), observed_object.clone())),
            "compaction must retain its content observation through pointer publication: {observed_object}"
        );
    }
}

#[tokio::test]
async fn table_metadata_maintenance_helper_rejects_snapshot_expiration_manual_review_commit() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
    let bucket = "warehouse";
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let current = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");

    seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current.clone()).await;
    backend
        .put_json_with_mod_time(
            bucket,
            &current,
            serde_json::json!({
                "format-version": 2,
                "table-uuid": "table-uuid",
                "location": "s3://warehouse/tables/table-id",
                "current-snapshot-id": 20,
                "metadata-log": [],
                "snapshot-log": [],
                "snapshots": [
                    {
                        "snapshot-id": 10,
                        "timestamp-ms": 1000,
                        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro"
                    },
                    {
                        "snapshot-id": 20,
                        "timestamp-ms": 2000,
                        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-20.avro"
                    }
                ],
                "refs": {
                    "main": {
                        "snapshot-id": 20,
                        "type": "branch"
                    },
                    "audit": {
                        "snapshot-id": 10,
                        "type": "tag"
                    }
                }
            }),
            Some(OffsetDateTime::UNIX_EPOCH),
        )
        .await;

    let result = table_metadata_maintenance_response(
        &store,
        &backend,
        bucket,
        &namespace,
        "events",
        TableMetadataMaintenanceRequest {
            retain_recent_metadata_files: 0,
            delete: false,
            snapshot_expiration: Some(crate::table_catalog::TableSnapshotExpirationConfig {
                min_snapshots_to_keep: 1,
                max_snapshot_age_ms: 1,
            }),
            commit_snapshot_expiration: true,
            compaction: None,
            commit_compaction: false,
        },
    )
    .await;

    assert!(result.is_err());
    let entry = store
        .load_table(bucket, "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    assert_eq!(entry.metadata_location, current);
    assert_eq!(entry.generation, 1);
}

#[tokio::test]
async fn table_metadata_maintenance_helper_rejects_stale_snapshot_expiration_plan() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
    let bucket = "warehouse";
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let current = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let next = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00003.metadata.json");

    let metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": "table-uuid",
        "location": "s3://warehouse/tables/table-id",
        "current-snapshot-id": 20,
        "metadata-log": [],
        "snapshot-log": [],
        "snapshots": [
            {
                "snapshot-id": 10,
                "timestamp-ms": 1000,
                "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro"
            },
            {
                "snapshot-id": 20,
                "timestamp-ms": 2000,
                "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-20.avro"
            }
        ],
        "refs": {
            "main": {
                "snapshot-id": 20,
                "type": "branch"
            }
        }
    });
    seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current.clone()).await;
    backend
        .put_json_with_mod_time(bucket, &current, metadata.clone(), Some(OffsetDateTime::UNIX_EPOCH))
        .await;
    backend
        .put_json_with_mod_time(bucket, &next, metadata, Some(OffsetDateTime::UNIX_EPOCH))
        .await;

    let stale_plan = store
        .plan_table_snapshot_expiration(
            bucket,
            "analytics",
            "events",
            crate::table_catalog::TableSnapshotExpirationConfig {
                min_snapshots_to_keep: 1,
                max_snapshot_age_ms: 1,
            },
        )
        .await
        .expect("snapshot expiration plan should build");
    store
        .commit_table(crate::table_catalog::TableCommitRequest {
            table_bucket: bucket.to_string(),
            namespace: namespace.public_name(),
            table: table.as_str().to_string(),
            commit_id: "advance-pointer".to_string(),
            idempotency_key: None,
            operation: "append".to_string(),
            expected_version_token: "token-v1".to_string(),
            expected_metadata_location: current,
            new_metadata_location: next,
            requirements: Vec::new(),
            writer: Some("test".to_string()),
        })
        .await
        .expect("pointer advance should succeed");

    let result = commit_table_snapshot_expiration_response(&store, &backend, bucket, &namespace, "events", stale_plan).await;

    assert!(result.is_err());
    let entry = store
        .load_table(bucket, "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    assert_eq!(entry.generation, 2);
}

#[tokio::test]
async fn table_metadata_maintenance_helper_rejects_delete_with_snapshot_expiration_commit() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");

    let result = table_metadata_maintenance_response(
        &store,
        &backend,
        "warehouse",
        &namespace,
        "events",
        TableMetadataMaintenanceRequest {
            retain_recent_metadata_files: 0,
            delete: true,
            snapshot_expiration: Some(crate::table_catalog::TableSnapshotExpirationConfig {
                min_snapshots_to_keep: 1,
                max_snapshot_age_ms: 1,
            }),
            commit_snapshot_expiration: true,
            compaction: None,
            commit_compaction: false,
        },
    )
    .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn table_refs_response_reports_current_and_user_defined_refs() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
    let bucket = "warehouse";
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let current = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current.clone()).await;
    let mut metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
    metadata["last-sequence-number"] = serde_json::Value::from(2);
    metadata["snapshots"] = serde_json::json!([
        {
            "snapshot-id": 9,
            "sequence-number": 1,
            "timestamp-ms": 1,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-9.avro",
            "summary": {"operation": "append"}
        },
        {
            "snapshot-id": 10,
            "parent-snapshot-id": 9,
            "sequence-number": 2,
            "timestamp-ms": 2,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
            "summary": {"operation": "append"}
        }
    ]);
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["snapshot-log"] = serde_json::json!([
        {"timestamp-ms": 1, "snapshot-id": 9},
        {"timestamp-ms": 2, "snapshot-id": 10}
    ]);
    metadata["refs"] = serde_json::json!({
        "main": {"snapshot-id": 10, "type": "branch"},
        "audit": {"snapshot-id": 9, "type": "tag"}
    });
    backend
        .put_json_with_mod_time(bucket, &current, metadata, Some(OffsetDateTime::UNIX_EPOCH))
        .await;

    let response = table_refs_response(&store, &backend, bucket, &namespace, "events")
        .await
        .expect("refs response should load");

    assert_eq!(response.current_snapshot_id, Some(10));
    assert_eq!(response.protected_ref_count, 1);
    assert_eq!(response.user_defined_ref_count, 1);
    assert!(response.refs.contains_key("main"));
    assert!(response.refs.contains_key("audit"));
}

#[tokio::test]
async fn external_catalog_bridge_response_lists_supported_operator_bridges() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(backend);
    let bucket = "warehouse";
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
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
    let response = external_catalog_bridge_response(&store, bucket, &namespace, "events")
        .await
        .expect("bridge response should load");

    assert_eq!(response.status, "bridge-unconfigured");
    assert!(response.unsupported_bridges.is_empty());
    assert_eq!(response.capabilities.len(), 4);
    assert!(
        response
            .capabilities
            .iter()
            .any(|bridge| bridge.catalog == "polaris" && bridge.status == "operator-sync-supported")
    );
}

#[tokio::test]
async fn external_catalog_bridge_persists_identity_and_boundary() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(backend);
    let bucket = "warehouse";
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
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

    let configured = put_external_catalog_bridge_response(
        &store,
        bucket,
        &namespace,
        "events",
        ExternalCatalogBridgeRequest {
            catalog: "polaris".to_string(),
            external_catalog_id: Some("prod-catalog".to_string()),
            external_namespace: "sales.analytics".to_string(),
            external_table: "orders".to_string(),
            external_table_uuid: Some("table-uuid".to_string()),
            metadata_location: None,
            external_version_token: Some("polaris-v7".to_string()),
            policy_mode: Some("rustfs-authoritative".to_string()),
            credential_mode: Some("rustfs-table-credentials".to_string()),
            sync_mode: Some("manual".to_string()),
            properties: BTreeMap::from([("owner".to_string(), "lakehouse".to_string())]),
        },
    )
    .await
    .expect("bridge should configure");

    assert_eq!(configured.status, "bridge-configured");
    let bridge = configured.bridge.as_ref().expect("bridge state should be returned");
    assert_eq!(bridge.catalog, "polaris");
    assert_eq!(bridge.external_catalog_id.as_deref(), Some("prod-catalog"));
    assert_eq!(bridge.external_namespace, "sales.analytics");
    assert_eq!(bridge.external_table, "orders");
    assert_eq!(bridge.external_version_token.as_deref(), Some("polaris-v7"));
    assert_eq!(bridge.policy_mode, "rustfs-authoritative");
    assert_eq!(bridge.credential_mode, "rustfs-table-credentials");
    assert_eq!(bridge.sync_mode, "manual");

    let loaded = external_catalog_bridge_response(&store, bucket, &namespace, "events")
        .await
        .expect("bridge response should load");
    assert_eq!(loaded.status, "bridge-configured");
    assert!(
        loaded
            .capabilities
            .iter()
            .any(|capability| capability.catalog == "polaris" && capability.status == "operator-sync-supported")
    );
}

#[tokio::test]
async fn external_catalog_bridge_sync_registers_missing_table_from_snapshot() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
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
    let metadata_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    backend
        .put_json(
            bucket,
            &metadata_location,
            test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    let commit_backend = trusted_table_commit_backend(&backend);

    let synced = sync_external_catalog_bridge_response(
        &store,
        &commit_backend,
        bucket,
        &namespace,
        "events",
        ExternalCatalogBridgeSyncRequest {
            catalog: "glue".to_string(),
            external_catalog_id: Some("aws-glue-prod".to_string()),
            external_namespace: "sales".to_string(),
            external_table: "orders".to_string(),
            external_table_uuid: Some("table-uuid".to_string()),
            metadata_location: metadata_location.clone(),
            external_version_token: Some("glue-version-1".to_string()),
            expected_version_token: None,
            expected_metadata_location: None,
            commit_id: Some("external-sync-1".to_string()),
            idempotency_key: Some("external-sync-idempotency-1".to_string()),
            policy_mode: Some("rustfs-authoritative".to_string()),
            credential_mode: Some("rustfs-table-credentials".to_string()),
            rollback_strategy: Some("retain-current-pointer".to_string()),
            properties: BTreeMap::new(),
        },
        true,
    )
    .await
    .expect("external sync should register missing table");

    assert_eq!(synced.action, "registered");
    assert_eq!(
        synced.table.metadata_location,
        table_metadata_location_for_client(bucket, &metadata_location)
    );
    let bridge = synced.bridge.bridge.as_ref().expect("bridge state should be returned");
    assert_eq!(bridge.catalog, "glue");
    assert_eq!(bridge.last_sync_status.as_deref(), Some("synced"));
    assert_eq!(bridge.last_synced_metadata_location.as_deref(), Some(metadata_location.as_str()));
    assert_eq!(bridge.rollback_strategy, "retain-current-pointer");
    let current = store
        .load_table(bucket, "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should be registered");
    assert_eq!(current.metadata_location, metadata_location);
    assert_eq!(current.table_uuid, "table-uuid");
}

#[tokio::test]
async fn external_catalog_bridge_sync_commits_existing_table_pointer() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
    let bucket = "warehouse";
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let current_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let next_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current_location.clone()).await;
    let mut current_metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
    current_metadata["last-sequence-number"] = serde_json::Value::from(1);
    backend.put_json(bucket, &current_location, current_metadata).await;
    let mut next_metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
    next_metadata["last-sequence-number"] = serde_json::Value::from(2);
    backend.put_json(bucket, &next_location, next_metadata).await;
    let commit_backend = trusted_table_commit_backend(&backend);

    let synced = sync_external_catalog_bridge_response(
        &store,
        &commit_backend,
        bucket,
        &namespace,
        "events",
        ExternalCatalogBridgeSyncRequest {
            catalog: "hive-metastore".to_string(),
            external_catalog_id: Some("hms-prod".to_string()),
            external_namespace: "sales".to_string(),
            external_table: "orders".to_string(),
            external_table_uuid: Some("table-uuid".to_string()),
            metadata_location: table_metadata_location_for_client(bucket, &next_location),
            external_version_token: Some("hms-version-2".to_string()),
            expected_version_token: Some("token-v1".to_string()),
            expected_metadata_location: Some(table_metadata_location_for_client(bucket, &current_location)),
            commit_id: Some("external-sync-2".to_string()),
            idempotency_key: Some("external-sync-idempotency-2".to_string()),
            policy_mode: Some("rustfs-authoritative".to_string()),
            credential_mode: Some("rustfs-table-credentials".to_string()),
            rollback_strategy: Some("retain-current-pointer".to_string()),
            properties: BTreeMap::new(),
        },
        true,
    )
    .await
    .expect("external sync should commit existing table");

    assert_eq!(synced.action, "committed");
    assert_eq!(synced.table.metadata_location, table_metadata_location_for_client(bucket, &next_location));
    let current = store
        .load_table(bucket, "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    assert_eq!(current.metadata_location, next_location);
    assert_eq!(current.generation, 2);
}

#[tokio::test]
async fn external_catalog_bridge_sync_denies_metadata_reads_before_pointer_publish() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
    let bucket = "warehouse";
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let current_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let next_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current_location.clone()).await;
    backend
        .put_json(
            bucket,
            &current_location,
            test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    backend
        .put_json(
            bucket,
            &next_location,
            test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    let authorized = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let commit_backend = TableCommitObjectBackend::test(backend, Arc::clone(&authorized), Some(next_location.clone()));

    let result = sync_external_catalog_bridge_response(
        &store,
        &commit_backend,
        bucket,
        &namespace,
        "events",
        ExternalCatalogBridgeSyncRequest {
            catalog: "hive-metastore".to_string(),
            external_catalog_id: Some("hms-prod".to_string()),
            external_namespace: "sales".to_string(),
            external_table: "orders".to_string(),
            external_table_uuid: Some("table-uuid".to_string()),
            metadata_location: next_location.clone(),
            external_version_token: Some("hms-version-2".to_string()),
            expected_version_token: Some("token-v1".to_string()),
            expected_metadata_location: Some(current_location.clone()),
            commit_id: Some("external-sync-denied".to_string()),
            idempotency_key: None,
            policy_mode: Some("rustfs-authoritative".to_string()),
            credential_mode: Some("rustfs-table-credentials".to_string()),
            rollback_strategy: Some("retain-current-pointer".to_string()),
            properties: BTreeMap::new(),
        },
        true,
    )
    .await;
    let error = commit_backend
        .finish(result)
        .await
        .expect_err("denied external metadata read must fail before pointer publication");

    assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
    assert!(authorized.lock().await.contains(&(next_location, S3Action::GetObjectAction)));
    let current = store
        .load_table(bucket, "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should remain");
    assert_eq!(current.metadata_location, current_location);
    assert_eq!(current.version_token, "token-v1");
}

#[tokio::test]
async fn external_catalog_bridge_sync_conflicts_leave_pointer_unchanged() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
    let bucket = "warehouse";
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let current_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let next_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current_location.clone()).await;
    backend
        .put_json(
            bucket,
            &current_location,
            test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    backend
        .put_json(
            bucket,
            &next_location,
            test_table_metadata_json("different-table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    let commit_backend = trusted_table_commit_backend(&backend);

    let result = sync_external_catalog_bridge_response(
        &store,
        &commit_backend,
        bucket,
        &namespace,
        "events",
        ExternalCatalogBridgeSyncRequest {
            catalog: "dlf".to_string(),
            external_catalog_id: Some("dlf-prod".to_string()),
            external_namespace: "sales".to_string(),
            external_table: "orders".to_string(),
            external_table_uuid: Some("different-table-uuid".to_string()),
            metadata_location: next_location.clone(),
            external_version_token: Some("dlf-version-2".to_string()),
            expected_version_token: Some("token-v1".to_string()),
            expected_metadata_location: Some(current_location.clone()),
            commit_id: Some("external-sync-3".to_string()),
            idempotency_key: Some("external-sync-idempotency-3".to_string()),
            policy_mode: Some("rustfs-authoritative".to_string()),
            credential_mode: Some("rustfs-table-credentials".to_string()),
            rollback_strategy: Some("retain-current-pointer".to_string()),
            properties: BTreeMap::new(),
        },
        true,
    )
    .await;

    assert!(result.is_err());
    let current = store
        .load_table(bucket, "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    assert_eq!(current.metadata_location, current_location);
    assert_eq!(current.version_token, "token-v1");
}

#[test]
fn snapshot_conflict_requirements_validate_snapshot_ref_id() {
    let metadata = serde_json::json!({
        "current-snapshot-id": 10,
        "refs": {"main": {"type": "branch", "snapshot-id": 10}}
    });

    let matching = vec![serde_json::json!({
        "type": "assert-ref-snapshot-id",
        "ref": "main",
        "snapshot-id": 10
    })];
    validate_table_commit_requirements(&metadata, &matching).expect("matching current snapshot should pass");

    let stale = vec![serde_json::json!({
        "type": "assert-ref-snapshot-id",
        "ref": "main",
        "snapshot-id": 9
    })];
    assert!(validate_table_commit_requirements(&metadata, &stale).is_err());

    let no_snapshot_metadata = serde_json::json!({});
    let create_like = vec![serde_json::json!({
        "type": "assert-ref-snapshot-id",
        "ref": "main",
        "snapshot-id": null
    })];
    validate_table_commit_requirements(&no_snapshot_metadata, &create_like)
        .expect("null current snapshot requirement should pass when no current snapshot exists");
}

#[test]
fn snapshot_conflict_rejects_unknown_parent_or_stale_sequence_number() {
    let mut metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["last-sequence-number"] = serde_json::Value::from(4);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 4,
        "timestamp-ms": 1234,
        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
        "summary": {"operation": "append"}
    }]);
    metadata["refs"] = serde_json::json!({"main": {"snapshot-id": 10, "type": "branch"}});

    let unknown_parent = vec![serde_json::json!({
        "action": "add-snapshot",
        "snapshot": {
            "snapshot-id": 11,
            "parent-snapshot-id": 9,
            "sequence-number": 5,
            "timestamp-ms": 2234,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-11.avro",
            "summary": {
                "operation": "append"
            }
        }
    })];
    let error = apply_table_commit_updates(metadata.clone(), &unknown_parent, "metadata/00001.metadata.json")
        .expect_err("unknown snapshot parents must fail");
    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_COMMIT_FAILED.into()));
    assert_eq!(error.status_code(), Some(StatusCode::CONFLICT));
    assert_eq!(error.message(), Some("snapshot parent does not exist"));

    let stale_sequence = vec![serde_json::json!({
        "action": "add-snapshot",
        "snapshot": {
            "snapshot-id": 11,
            "parent-snapshot-id": 10,
            "sequence-number": 4,
            "timestamp-ms": 2234,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-11.avro",
            "summary": {
                "operation": "append"
            }
        }
    })];
    let error = apply_table_commit_updates(metadata.clone(), &stale_sequence, "metadata/00001.metadata.json")
        .expect_err("snapshot sequence numbers must advance");
    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_COMMIT_FAILED.into()));
    assert_eq!(error.status_code(), Some(StatusCode::CONFLICT));
    assert_eq!(error.message(), Some("snapshot sequence number must advance"));

    let stale_root_sequence = vec![serde_json::json!({
        "action": "add-snapshot",
        "snapshot": {
            "snapshot-id": 11,
            "sequence-number": 4,
            "timestamp-ms": 2234,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-11.avro",
            "summary": {
                "operation": "append"
            }
        }
    })];
    let error = apply_table_commit_updates(metadata, &stale_root_sequence, "metadata/00001.metadata.json")
        .expect_err("root snapshot sequence numbers must advance");
    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_COMMIT_FAILED.into()));
    assert_eq!(error.status_code(), Some(StatusCode::CONFLICT));
    assert_eq!(error.message(), Some("snapshot sequence number must advance"));
}

#[test]
fn snapshot_updates_move_only_the_declared_reference() {
    let mut metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["last-sequence-number"] = serde_json::Value::from(4);
    metadata["snapshots"] = serde_json::json!([
        {
            "snapshot-id": 9,
            "sequence-number": 3,
            "timestamp-ms": 1000,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-9.avro",
            "summary": {"operation": "append"}
        },
        {
            "snapshot-id": 10,
            "parent-snapshot-id": 9,
            "sequence-number": 4,
            "timestamp-ms": 1234,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
            "summary": {"operation": "append"}
        }
    ]);
    metadata["refs"] = serde_json::json!({"main": {"snapshot-id": 10, "type": "branch"}});
    metadata["snapshot-log"] = serde_json::json!([{"timestamp-ms": 1234, "snapshot-id": 10}]);
    let add_snapshot = serde_json::json!({
        "action": "add-snapshot",
        "snapshot": {
            "snapshot-id": 11,
            "parent-snapshot-id": 9,
            "sequence-number": 5,
            "timestamp-ms": 2234,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-11.avro",
            "summary": {"operation": "append"}
        }
    });

    let added =
        apply_table_commit_updates_at(metadata, std::slice::from_ref(&add_snapshot), "metadata/00001.metadata.json", 3000)
            .expect("a snapshot may branch from any retained parent");
    assert_eq!(added["current-snapshot-id"], 10);
    assert_eq!(added["refs"]["main"]["snapshot-id"], 10);
    assert_eq!(added["snapshot-log"].as_array().map(Vec::len), Some(1));

    let branch = apply_table_commit_updates_at(
        added,
        &[serde_json::json!({
            "action": "set-snapshot-ref",
            "ref-name": "audit",
            "snapshot-id": 11,
            "type": "branch"
        })],
        "metadata/00002.metadata.json",
        3001,
    )
    .expect("a non-main branch should be updated");
    assert_eq!(branch["current-snapshot-id"], 10);
    assert_eq!(branch["refs"]["main"]["snapshot-id"], 10);
    assert_eq!(branch["refs"]["audit"]["snapshot-id"], 11);
    assert_eq!(branch["snapshot-log"].as_array().map(Vec::len), Some(1));

    let main = apply_table_commit_updates_at(
        branch,
        &[serde_json::json!({
            "action": "set-snapshot-ref",
            "ref-name": "main",
            "snapshot-id": 11,
            "type": "branch"
        })],
        "metadata/00003.metadata.json",
        3002,
    )
    .expect("main should move to a retained snapshot");
    assert_eq!(main["current-snapshot-id"], 11);
    assert_eq!(main["snapshot-log"].as_array().map(Vec::len), Some(2));
    assert_eq!(main["snapshot-log"][1], serde_json::json!({"timestamp-ms": 3002, "snapshot-id": 11}));

    let unchanged = apply_table_commit_updates_at(
        main,
        &[serde_json::json!({
            "action": "set-snapshot-ref",
            "ref-name": "main",
            "snapshot-id": 11,
            "type": "branch"
        })],
        "metadata/00004.metadata.json",
        3003,
    )
    .expect("replaying an unchanged main reference should be a no-op for snapshot history");
    assert_eq!(unchanged["snapshot-log"].as_array().map(Vec::len), Some(2));
}

#[test]
fn newly_added_main_snapshot_uses_its_snapshot_timestamp_in_history() {
    let mut metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
    metadata["current-snapshot-id"] = serde_json::Value::from(-1);
    let updated = apply_table_commit_updates_at(
        metadata,
        &[
            serde_json::json!({
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 11,
                    "sequence-number": 1,
                    "timestamp-ms": 2234,
                    "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-11.avro",
                    "summary": {"operation": "append"}
                }
            }),
            serde_json::json!({
                "action": "set-snapshot-ref",
                "ref-name": "main",
                "snapshot-id": 11,
                "type": "branch"
            }),
        ],
        "metadata/00001.metadata.json",
        3000,
    )
    .expect("new snapshot and main reference should apply");

    assert_eq!(updated["current-snapshot-id"], 11);
    assert_eq!(updated["snapshot-log"], serde_json::json!([{"timestamp-ms": 2234, "snapshot-id": 11}]));
}

#[test]
fn only_v1_snapshots_may_omit_sequence_number() {
    let v1 = serde_json::json!({
        "format-version": 1,
        "table-uuid": "table-uuid",
        "location": "s3://warehouse/tables/table-id",
        "last-updated-ms": 1,
        "last-column-id": 1,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        },
        "partition-spec": [],
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": []
    });
    let v1_updated = apply_table_commit_updates_at(
        v1,
        &[serde_json::json!({
            "action": "add-snapshot",
            "snapshot": {
                "snapshot-id": 10,
                "timestamp-ms": 2234,
                "manifests": ["s3://warehouse/tables/table-id/metadata/manifest-10.avro"],
                "summary": {"operation": "append"}
            }
        })],
        "metadata/00001.metadata.json",
        3000,
    )
    .expect("an Iceberg v1 zero sequence snapshot may omit sequence-number");
    assert!(v1_updated["snapshots"][0].get("sequence-number").is_none());
    assert!(v1_updated.get("last-sequence-number").is_none());

    let v2 = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
    let v2_error = apply_table_commit_updates_at(
        v2,
        &[serde_json::json!({
            "action": "add-snapshot",
            "snapshot": {
                "snapshot-id": 10,
                "timestamp-ms": 2234,
                "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
                "summary": {"operation": "append"}
            }
        })],
        "metadata/00001.metadata.json",
        3000,
    )
    .expect_err("new Iceberg v2 snapshots must include sequence-number");
    assert_eq!(v2_error.code(), &S3ErrorCode::InvalidRequest);
    assert_eq!(v2_error.message(), Some("Iceberg v2 snapshot sequence-number is required"));
}

#[test]
fn snapshot_updates_reject_non_integer_parent_ids() {
    let metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
    let error = apply_table_commit_updates_at(
        metadata,
        &[serde_json::json!({
            "action": "add-snapshot",
            "snapshot": {
                "snapshot-id": 10,
                "parent-snapshot-id": "invalid",
                "sequence-number": 1,
                "timestamp-ms": 2234,
                "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
                "summary": {"operation": "append"}
            }
        })],
        "metadata/00001.metadata.json",
        3000,
    )
    .expect_err("snapshot parent IDs must be integers");

    assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn standard_commit_accepts_multiple_ordered_snapshots() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::default();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let first_manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let first_manifest = format!("{table_location}/metadata/manifest-snap-10.avro");
    let first_data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_snapshot_manifest(
        &metadata_backend,
        "warehouse",
        &first_manifest_list,
        10,
        1,
        &[(&first_data_file, 0, 1, 10, 1)],
    )
    .await;
    let second_manifest_list = format!("{table_location}/metadata/snap-11.avro");
    let second_manifest = format!("{table_location}/metadata/manifest-11.avro");
    let second_data_file = format!("{table_location}/data/part-11.parquet");
    seed_test_manifest(&metadata_backend, "warehouse", &second_manifest, &[(&second_data_file, 0, 1, 11, 2)]).await;
    seed_test_manifest_list_entries(
        &metadata_backend,
        "warehouse",
        &second_manifest_list,
        &[(&first_manifest, 1, 10), (&second_manifest, 2, 11)],
    )
    .await;
    let request = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": first_manifest_list,
                    "summary": {"operation": "append"}
                }
            },
            {
                "action": "set-snapshot-ref",
                "ref-name": "main",
                "snapshot-id": 10,
                "type": "branch"
            },
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 11,
                    "parent-snapshot-id": 10,
                    "sequence-number": 2,
                    "timestamp-ms": 2234,
                    "manifest-list": second_manifest_list,
                    "summary": {"operation": "append"}
                }
            },
            {
                "action": "set-snapshot-ref",
                "ref-name": "main",
                "snapshot-id": 11,
                "type": "branch"
            }
        ]
    }))
    .expect("multi-snapshot commit request should parse");

    let committed = standard_commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request,
    )
    .await
    .expect("ordered intermediate snapshots should commit");

    assert_eq!(committed.metadata["snapshots"].as_array().map(Vec::len), Some(2));
    assert_eq!(committed.metadata["current-snapshot-id"], 11);
    assert_eq!(committed.metadata["last-sequence-number"], 2);
    assert_eq!(
        committed.metadata["snapshot-log"],
        serde_json::json!([{"timestamp-ms": 2234, "snapshot-id": 11}])
    );
}

#[test]
fn snapshot_conflict_rejects_unknown_snapshot_operations() {
    let mut metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
    metadata["current-snapshot-id"] = serde_json::Value::from(10);
    metadata["last-sequence-number"] = serde_json::Value::from(4);
    metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 4,
        "timestamp-ms": 1234,
        "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
        "summary": {"operation": "append"}
    }]);
    metadata["refs"] = serde_json::json!({"main": {"snapshot-id": 10, "type": "branch"}});

    let updates = vec![serde_json::json!({
        "action": "add-snapshot",
        "snapshot": {
            "snapshot-id": 11,
            "parent-snapshot-id": 10,
            "sequence-number": 5,
            "timestamp-ms": 2234,
            "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-11.avro",
            "summary": {
                "operation": "unknown"
            }
        }
    })];
    let error = apply_table_commit_updates(metadata, &updates, "metadata/00001.metadata.json")
        .expect_err("unknown snapshot operations must fail");
    assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
    assert_eq!(error.message(), Some("unsupported snapshot operation: unknown"));
}

#[tokio::test]
async fn row_level_conflict_allows_overwrite_when_deleted_file_is_current() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let current_manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let old_data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_snapshot_manifest(
        &metadata_backend,
        "warehouse",
        &current_manifest_list,
        10,
        1,
        &[(&old_data_file, 0, 1, 10, 1)],
    )
    .await;
    let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": current_manifest_list,
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
    .expect("append request should parse");
    commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        append_request,
    )
    .await
    .expect("append commit should succeed");

    let overwrite_manifest_list = format!("{table_location}/metadata/snap-11.avro");
    let replacement_data_file = format!("{table_location}/data/part-11.parquet");
    seed_test_snapshot_manifest(
        &metadata_backend,
        "warehouse",
        &overwrite_manifest_list,
        11,
        2,
        &[(&old_data_file, 0, 2, 11, 2), (&replacement_data_file, 0, 1, 11, 2)],
    )
    .await;
    let overwrite_request_json = serde_json::json!({
        "requirements": [
            {
                "type": "assert-ref-snapshot-id",
                "ref": "main",
                "snapshot-id": 10
            }
        ],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 11,
                    "parent-snapshot-id": 10,
                    "sequence-number": 2,
                    "timestamp-ms": 2234,
                    "manifest-list": overwrite_manifest_list,
                    "summary": {
                        "operation": "overwrite"
                    }
                }
            },
            {
                "action": "set-snapshot-ref",
                "ref-name": "main",
                "snapshot-id": 11,
                "type": "branch"
            }
        ]
    });
    let stale_overwrite_request: RestCommitTableRequest =
        serde_json::from_value(overwrite_request_json.clone()).expect("stale overwrite request should parse");

    let error = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        stale_overwrite_request,
    )
    .await
    .expect_err("deleted file sequence must not change");
    assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);

    seed_test_snapshot_manifest(
        &metadata_backend,
        "warehouse",
        &overwrite_manifest_list,
        11,
        2,
        &[(&old_data_file, 0, 2, 11, 1), (&replacement_data_file, 0, 1, 11, 2)],
    )
    .await;
    let overwrite_request: RestCommitTableRequest =
        serde_json::from_value(overwrite_request_json).expect("overwrite request should parse");

    let commit = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        overwrite_request,
    )
    .await
    .expect("overwrite commit should pass manifest conflict validation");

    assert_eq!(commit.metadata["current-snapshot-id"], 11);
    assert_eq!(commit.metadata["last-sequence-number"], 2);
}

#[tokio::test]
async fn row_level_conflict_rejects_embedded_manifests_for_v2_snapshot() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let manifest = format!("{table_location}/metadata/manifest-10.avro");
    let current = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifests": [
                        manifest.clone()
                    ],
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
    .expect("append request should parse");

    let error = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        append_request,
    )
    .await
    .expect_err("new v2 snapshots must use a manifest list");

    assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
    let unchanged = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should still exist");
    assert_eq!(unchanged.metadata_location, current.metadata_location);
    assert_eq!(unchanged.version_token, current.version_token);
    assert_eq!(unchanged.generation, current.generation);
}

#[tokio::test]
async fn row_level_conflict_inherits_manifest_list_sequence_numbers() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let manifest = format!("{table_location}/metadata/manifest-10.avro");
    let data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_manifest_with_nullable_sequences(&metadata_backend, "warehouse", &manifest, &[(&data_file, 0, 1, 10, None)]).await;
    seed_test_manifest_list(&metadata_backend, "warehouse", &manifest_list, &[&manifest], 1, 10).await;
    let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": manifest_list,
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
    .expect("append request should parse");

    let commit = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        append_request,
    )
    .await
    .expect("manifest entry should inherit sequence numbers from manifest list");

    assert_eq!(commit.metadata["current-snapshot-id"], 10);
    assert_eq!(commit.metadata["last-sequence-number"], 1);
}

#[tokio::test]
async fn row_level_conflict_allows_inherited_manifests_on_append() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let first_manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let first_manifest = format!("{table_location}/metadata/manifest-10.avro");
    let first_data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_manifest(&metadata_backend, "warehouse", &first_manifest, &[(&first_data_file, 0, 1, 10, 1)]).await;
    seed_test_manifest_list(&metadata_backend, "warehouse", &first_manifest_list, &[&first_manifest], 1, 10).await;
    let first_append: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": first_manifest_list,
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
    .expect("first append request should parse");
    commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        first_append,
    )
    .await
    .expect("first append should commit");

    let second_manifest_list = format!("{table_location}/metadata/snap-11.avro");
    let second_manifest = format!("{table_location}/metadata/manifest-11.avro");
    let second_data_file = format!("{table_location}/data/part-11.parquet");
    seed_test_manifest(&metadata_backend, "warehouse", &second_manifest, &[(&second_data_file, 0, 1, 11, 2)]).await;
    seed_test_manifest_list_entries(
        &metadata_backend,
        "warehouse",
        &second_manifest_list,
        &[(&first_manifest, 1, 10), (&second_manifest, 2, 11)],
    )
    .await;
    let second_append: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [
            {
                "type": "assert-ref-snapshot-id",
                "ref": "main",
                "snapshot-id": 10
            }
        ],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 11,
                    "parent-snapshot-id": 10,
                    "sequence-number": 2,
                    "timestamp-ms": 2234,
                    "manifest-list": second_manifest_list,
                    "summary": {
                        "operation": "append"
                    }
                }
            },
            {
                "action": "set-snapshot-ref",
                "ref-name": "main",
                "snapshot-id": 11,
                "type": "branch"
            }
        ]
    }))
    .expect("second append request should parse");

    let commit = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        second_append,
    )
    .await
    .expect("append should preserve inherited manifests");

    assert_eq!(commit.metadata["current-snapshot-id"], 11);
    assert_eq!(commit.metadata["last-sequence-number"], 2);
}

#[tokio::test]
async fn row_level_conflict_rejects_changed_inherited_manifest_identity() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let first_manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let first_manifest = format!("{table_location}/metadata/manifest-10.avro");
    let first_data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_manifest(&metadata_backend, "warehouse", &first_manifest, &[(&first_data_file, 0, 1, 10, 1)]).await;
    seed_test_manifest_list(&metadata_backend, "warehouse", &first_manifest_list, &[&first_manifest], 1, 10).await;
    let first_append: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": first_manifest_list,
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
    .expect("first append request should parse");
    commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        first_append,
    )
    .await
    .expect("first append should commit");
    let current = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");

    let second_manifest_list = format!("{table_location}/metadata/snap-11.avro");
    seed_test_manifest_list(&metadata_backend, "warehouse", &second_manifest_list, &[&first_manifest], 2, 10).await;
    let second_append: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [
            {
                "type": "assert-ref-snapshot-id",
                "ref": "main",
                "snapshot-id": 10
            }
        ],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 11,
                    "parent-snapshot-id": 10,
                    "sequence-number": 2,
                    "timestamp-ms": 2234,
                    "manifest-list": second_manifest_list,
                    "summary": {
                        "operation": "append"
                    }
                }
            }
        ]
    }))
    .expect("second append request should parse");

    let error = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        second_append,
    )
    .await
    .expect_err("inherited manifest identity must not change");

    assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
    let unchanged = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should still exist");
    assert_eq!(unchanged.metadata_location, current.metadata_location);
    assert_eq!(unchanged.version_token, current.version_token);
    assert_eq!(unchanged.generation, current.generation);
}

/// Table-driven coverage for stale or historical manifest sequence failures.
#[tokio::test]
async fn row_level_conflict_rejects_stale_or_historical_manifest_sequences() {
    // (case, manifest-list sequence, data-file suffix, manifest-entry snapshot id, expected failure)
    let cases: &[(&str, i64, &str, i64, &str)] = &[
        (
            "stale-new-manifest-sequence",
            1,
            "11",
            11,
            "new manifest sequence must match the committed snapshot",
        ),
        (
            "stale-added-entry-sequence",
            2,
            "11",
            11,
            "added file sequence must match the new manifest",
        ),
        (
            "historical-change-in-new-manifest",
            2,
            "10",
            10,
            "new manifest must not claim a historical changed entry",
        ),
    ];

    for (case, manifest_list_sequence, data_file_suffix, entry_snapshot_id, failure) in cases {
        let store = TestTableCatalogStore::default();
        let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
        let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
        let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
        let table_location = created.metadata["location"]
            .as_str()
            .expect("created metadata should have table location");
        let current = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist");
        let manifest_list = format!("{table_location}/metadata/snap-11.avro");
        let manifest = format!("{table_location}/metadata/manifest-11.avro");
        let data_file = format!("{table_location}/data/part-{data_file_suffix}.parquet");
        seed_test_manifest(&metadata_backend, "warehouse", &manifest, &[(&data_file, 0, 1, *entry_snapshot_id, 1)]).await;
        seed_test_manifest_list(&metadata_backend, "warehouse", &manifest_list, &[&manifest], *manifest_list_sequence, 11).await;
        let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": 11,
                        "sequence-number": 2,
                        "timestamp-ms": 2234,
                        "manifest-list": manifest_list,
                        "summary": {
                            "operation": "append"
                        }
                    }
                }
            ]
        }))
        .expect("append request should parse");

        let Err(error) = commit_table_response(
            &store,
            &trusted_table_commit_backend(&metadata_backend),
            "warehouse",
            &namespace,
            "events",
            append_request,
        )
        .await
        else {
            panic!("[{case}] {failure}");
        };

        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest, "[{case}] {failure}");
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, current.metadata_location, "[{case}] {failure}");
        assert_eq!(unchanged.version_token, current.version_token, "[{case}] {failure}");
        assert_eq!(unchanged.generation, current.generation, "[{case}] {failure}");
    }
}

#[tokio::test]
async fn row_level_conflict_allows_add_only_overwrite_snapshot() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let current_manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let current_data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_snapshot_manifest(
        &metadata_backend,
        "warehouse",
        &current_manifest_list,
        10,
        1,
        &[(&current_data_file, 0, 1, 10, 1)],
    )
    .await;
    let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": current_manifest_list,
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
    .expect("append request should parse");
    commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        append_request,
    )
    .await
    .expect("append commit should succeed");

    let overwrite_manifest_list = format!("{table_location}/metadata/snap-11.avro");
    let added_data_file = format!("{table_location}/data/part-11.parquet");
    seed_test_snapshot_manifest(
        &metadata_backend,
        "warehouse",
        &overwrite_manifest_list,
        11,
        2,
        &[(&added_data_file, 0, 1, 11, 2)],
    )
    .await;
    let overwrite_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [
            {
                "type": "assert-ref-snapshot-id",
                "ref": "main",
                "snapshot-id": 10
            }
        ],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 11,
                    "parent-snapshot-id": 10,
                    "sequence-number": 2,
                    "timestamp-ms": 2234,
                    "manifest-list": overwrite_manifest_list,
                    "summary": {
                        "operation": "overwrite"
                    }
                }
            },
            {
                "action": "set-snapshot-ref",
                "ref-name": "main",
                "snapshot-id": 11,
                "type": "branch"
            }
        ]
    }))
    .expect("overwrite request should parse");

    let commit = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        overwrite_request,
    )
    .await
    .expect("add-only overwrite should pass conflict validation");

    assert_eq!(commit.metadata["current-snapshot-id"], 11);
    assert_eq!(commit.metadata["last-sequence-number"], 2);
}

#[tokio::test]
async fn row_level_conflict_rejects_delete_of_non_current_file() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let current_manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let current_data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_snapshot_manifest(
        &metadata_backend,
        "warehouse",
        &current_manifest_list,
        10,
        1,
        &[(&current_data_file, 0, 1, 10, 1)],
    )
    .await;
    let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": current_manifest_list,
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
    .expect("append request should parse");
    commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        append_request,
    )
    .await
    .expect("append commit should succeed");
    let committed = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");

    let stale_data_file = format!("{table_location}/data/stale.parquet");
    let stale_key = test_snapshot_object_key("warehouse", &stale_data_file);
    metadata_backend.put_bytes("warehouse", &stale_key, b"stale".to_vec()).await;
    let overwrite_manifest_list = format!("{table_location}/metadata/snap-11.avro");
    seed_test_snapshot_manifest(
        &metadata_backend,
        "warehouse",
        &overwrite_manifest_list,
        11,
        2,
        &[(&stale_data_file, 0, 2, 11, 2)],
    )
    .await;
    let overwrite_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [
            {
                "type": "assert-ref-snapshot-id",
                "ref": "main",
                "snapshot-id": 10
            }
        ],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 11,
                    "parent-snapshot-id": 10,
                    "sequence-number": 2,
                    "timestamp-ms": 2234,
                    "manifest-list": overwrite_manifest_list,
                    "summary": {
                        "operation": "overwrite"
                    }
                }
            }
        ]
    }))
    .expect("overwrite request should parse");

    let error = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        overwrite_request,
    )
    .await
    .expect_err("stale row-level delete should conflict");

    assert_eq!(error.code(), &s3s::S3ErrorCode::Custom(ICEBERG_ERROR_COMMIT_FAILED.into()));
    assert_eq!(error.status_code(), Some(StatusCode::CONFLICT));
    let unchanged = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should still exist");
    assert_eq!(unchanged.metadata_location, committed.metadata_location);
    assert_eq!(unchanged.version_token, committed.version_token);
    assert_eq!(unchanged.generation, committed.generation);
}

#[tokio::test]
async fn row_level_conflict_rejects_append_with_delete_files() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let current = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let delete_file = format!("{table_location}/delete/delete-10.parquet");
    seed_test_snapshot_manifest(&metadata_backend, "warehouse", &manifest_list, 10, 1, &[(&delete_file, 1, 1, 10, 1)]).await;
    let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": manifest_list,
                    "summary": {
                        "operation": "append"
                    }
                }
            }
        ]
    }))
    .expect("append request should parse");

    let error = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        append_request,
    )
    .await
    .expect_err("append must not add delete files");

    assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
    let unchanged = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should still exist");
    assert_eq!(unchanged.metadata_location, current.metadata_location);
    assert_eq!(unchanged.version_token, current.version_token);
    assert_eq!(unchanged.generation, current.generation);
}

#[tokio::test]
async fn row_level_conflict_rejects_missing_manifest_before_pointer_update() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let current_manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let current_data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_snapshot_manifest(
        &metadata_backend,
        "warehouse",
        &current_manifest_list,
        10,
        1,
        &[(&current_data_file, 0, 1, 10, 1)],
    )
    .await;
    let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": current_manifest_list,
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
    .expect("append request should parse");
    commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        append_request,
    )
    .await
    .expect("append commit should succeed");
    let committed = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let missing_manifest_list = format!("{table_location}/metadata/missing-snap-11.avro");
    let overwrite_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [
            {
                "type": "assert-ref-snapshot-id",
                "ref": "main",
                "snapshot-id": 10
            }
        ],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 11,
                    "parent-snapshot-id": 10,
                    "sequence-number": 2,
                    "timestamp-ms": 2234,
                    "manifest-list": missing_manifest_list,
                    "summary": {
                        "operation": "overwrite"
                    }
                }
            }
        ]
    }))
    .expect("overwrite request should parse");

    let error = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        overwrite_request,
    )
    .await
    .expect_err("missing manifest-list should fail before pointer update");

    assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
    let unchanged = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should still exist");
    assert_eq!(unchanged.metadata_location, committed.metadata_location);
    assert_eq!(unchanged.version_token, committed.version_token);
    assert_eq!(unchanged.generation, committed.generation);
}

#[tokio::test]
async fn row_level_conflict_rejects_manifest_outside_table_warehouse() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let current_manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let current_data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_snapshot_manifest(
        &metadata_backend,
        "warehouse",
        &current_manifest_list,
        10,
        1,
        &[(&current_data_file, 0, 1, 10, 1)],
    )
    .await;
    let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": current_manifest_list,
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
    .expect("append request should parse");
    commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        append_request,
    )
    .await
    .expect("append commit should succeed");
    let committed = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let outside_manifest_list = "s3://warehouse/tables/other-table/metadata/snap-11.avro";
    let overwrite_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [
            {
                "type": "assert-ref-snapshot-id",
                "ref": "main",
                "snapshot-id": 10
            }
        ],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 11,
                    "parent-snapshot-id": 10,
                    "sequence-number": 2,
                    "timestamp-ms": 2234,
                    "manifest-list": outside_manifest_list,
                    "summary": {
                        "operation": "overwrite"
                    }
                }
            }
        ]
    }))
    .expect("overwrite request should parse");

    let error = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        overwrite_request,
    )
    .await
    .expect_err("outside manifest-list should fail before pointer update");

    assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
    let unchanged = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should still exist");
    assert_eq!(unchanged.metadata_location, committed.metadata_location);
    assert_eq!(unchanged.version_token, committed.version_token);
    assert_eq!(unchanged.generation, committed.generation);
}

#[tokio::test]
async fn statistics_updates_reject_unpublished_objects_before_pointer_update() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::default();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_snapshot_manifest(&metadata_backend, "warehouse", &manifest_list, 10, 1, &[(&data_file, 0, 1, 10, 1)]).await;
    let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": manifest_list,
                    "summary": {"operation": "append"}
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
    .expect("append request should parse");
    commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        append_request,
    )
    .await
    .expect("append commit should succeed");
    let committed = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let outside_statistics_file = "s3://warehouse/tables/other-table/metadata/stats-10.puffin";
    metadata_backend
        .put_bytes(
            "warehouse",
            &test_snapshot_object_key("warehouse", outside_statistics_file),
            b"outside-stats".to_vec(),
        )
        .await;

    for (commit_id, statistics_file) in [
        (
            "55555555-5555-4555-8555-555555555551",
            format!("{table_location}/metadata/missing-stats-10.puffin"),
        ),
        ("55555555-5555-4555-8555-555555555552", outside_statistics_file.to_string()),
    ] {
        let request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
            "commit-id": commit_id,
            "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 10}],
            "updates": [{
                "action": "set-statistics",
                "statistics": {
                    "snapshot-id": 10,
                    "statistics-path": statistics_file,
                    "file-size-in-bytes": 5,
                    "file-footer-size-in-bytes": 0,
                    "blob-metadata": []
                }
            }]
        }))
        .expect("statistics request should parse");

        let error = commit_table_response(
            &store,
            &trusted_table_commit_backend(&metadata_backend),
            "warehouse",
            &namespace,
            "events",
            request,
        )
        .await
        .expect_err("unpublished statistics object should fail before pointer update");

        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        let unchanged = store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should still exist");
        assert_eq!(unchanged.metadata_location, committed.metadata_location);
        assert_eq!(unchanged.version_token, committed.version_token);
        assert_eq!(unchanged.generation, committed.generation);
    }
}

#[tokio::test]
async fn bodyless_ref_delete_uses_default_request_options() {
    let request: DeleteTableRefRequest = read_json_body_or_default(Body::empty())
        .await
        .expect("bodyless ref delete should use default request options");

    assert!(request.expected_snapshot_id.is_none());
    assert!(!request.force);
    assert!(request.commit_id.is_none());
    assert!(request.idempotency_key.is_none());
    assert!(request.writer.is_none());
}

#[test]
fn unknown_commit_requirements_and_updates_are_bad_requests() {
    let unknown_requirement = vec![serde_json::json!({"type": "unknown-requirement"})];
    let nonstandard_table_requirement = vec![serde_json::json!({"type": "assert-current-snapshot-id", "snapshot-id": 10})];
    let unknown_update = vec![serde_json::json!({"action": "unknown-update"})];
    let table_requirement_error = validate_table_commit_requirements(&serde_json::json!({}), &unknown_requirement)
        .expect_err("unknown table requirement should fail");
    let nonstandard_table_requirement_error =
        validate_table_commit_requirements(&serde_json::json!({"current-snapshot-id": 10}), &nonstandard_table_requirement)
            .expect_err("nonstandard table requirement should fail");
    let table_update_error = apply_table_commit_updates(serde_json::json!({}), &unknown_update, "metadata/00001.metadata.json")
        .expect_err("unknown table update should fail");
    let view_requirement_error = validate_view_commit_requirements(&serde_json::json!({}), &unknown_requirement)
        .expect_err("unknown view requirement should fail");
    let nonstandard_view_requirement_error = validate_view_commit_requirements(
        &serde_json::json!({"current-version-id": 1}),
        &[serde_json::json!({"type": "assert-current-view-version-id", "current-view-version-id": 1})],
    )
    .expect_err("nonstandard view requirement should fail");
    let nonstandard_view_update_error = apply_view_commit_updates_at(
        serde_json::json!({}),
        &[serde_json::json!({"action": "set-current-schema", "schema-id": 1})],
        0,
    )
    .expect_err("nonstandard view update should fail");
    assert_eq!(
        nonstandard_view_update_error.message(),
        Some("unsupported view update: set-current-schema")
    );
    let view_update_error =
        apply_view_commit_updates_at(serde_json::json!({}), &unknown_update, 0).expect_err("unknown view update should fail");

    for error in [
        table_requirement_error,
        nonstandard_table_requirement_error,
        table_update_error,
        view_requirement_error,
        nonstandard_view_requirement_error,
        nonstandard_view_update_error,
        view_update_error,
    ] {
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
    }
}

#[test]
fn failed_commit_requirements_use_iceberg_conflict_errors() {
    let table_error = validate_table_commit_requirements(
        &serde_json::json!({"table-uuid": "current"}),
        &[serde_json::json!({"type": "assert-table-uuid", "uuid": "stale"})],
    )
    .expect_err("stale table requirement should fail");
    let view_error = validate_view_commit_requirements(
        &serde_json::json!({"view-uuid": "current"}),
        &[serde_json::json!({"type": "assert-view-uuid", "uuid": "stale"})],
    )
    .expect_err("stale view requirement should fail");

    for error in [table_error, view_error] {
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_COMMIT_FAILED.into()));
        assert_eq!(error.status_code(), Some(StatusCode::CONFLICT));
    }
}

#[test]
fn commit_identifier_must_match_the_resource_url() {
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let matching = RestTableIdentifier {
        namespace: vec!["analytics".to_string()],
        name: "events".to_string(),
    };
    validate_rest_commit_identifier(Some(&matching), &namespace, "events").expect("matching identifier should be accepted");

    for identifier in [
        RestTableIdentifier {
            namespace: vec!["staging".to_string()],
            name: "events".to_string(),
        },
        RestTableIdentifier {
            namespace: vec!["analytics".to_string()],
            name: "other".to_string(),
        },
    ] {
        assert_eq!(
            validate_rest_commit_identifier(Some(&identifier), &namespace, "events")
                .expect_err("identifier mismatch should fail")
                .code(),
            &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into())
        );
    }
}

#[tokio::test]
async fn mismatched_commit_identifiers_leave_catalog_pointers_unchanged() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::default();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_before = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let table_error = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        RestCommitTableRequest {
            identifier: Some(RestTableIdentifier {
                namespace: vec!["analytics".to_string()],
                name: "other".to_string(),
            }),
            commit_id: None,
            idempotency_key: None,
            operation: None,
            expected_version_token: None,
            expected_metadata_location: None,
            new_metadata_location: None,
            requirements: Vec::new(),
            updates: vec![serde_json::json!({"action": "set-properties", "updates": {"owner": "bad"}})],
            writer: None,
        },
    )
    .await
    .expect_err("mismatched table identifier should fail");
    assert_eq!(table_error.status_code(), Some(StatusCode::BAD_REQUEST));
    assert_eq!(
        store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .expect("table should exist"),
        table_before
    );

    create_standard_recent_events_view(&store, &metadata_backend, &namespace).await;
    let view_before = store
        .load_view("warehouse", "analytics", "recent_events")
        .await
        .expect("view lookup should succeed")
        .expect("view should exist");
    let view_error = replace_view_response(
        &store,
        &metadata_backend,
        "warehouse",
        &namespace,
        "recent_events",
        RestCommitViewRequest {
            identifier: Some(RestTableIdentifier {
                namespace: vec!["analytics".to_string()],
                name: "other".to_string(),
            }),
            _commit_id: None,
            expected_version_token: None,
            expected_metadata_location: None,
            new_metadata_location: None,
            requirements: Vec::new(),
            updates: vec![serde_json::json!({"action": "set-properties", "updates": {"owner": "bad"}})],
        },
    )
    .await
    .expect_err("mismatched view identifier should fail");
    assert_eq!(view_error.status_code(), Some(StatusCode::BAD_REQUEST));
    assert_eq!(
        store
            .load_view("warehouse", "analytics", "recent_events")
            .await
            .expect("view lookup should succeed")
            .expect("view should exist"),
        view_before
    );
}

#[test]
fn table_updates_apply_standard_statistics_and_metadata_cleanup_actions() {
    let metadata = serde_json::json!({
        "last-updated-ms": 1,
        "schemas": [
            {"type": "struct", "schema-id": 0, "fields": []},
            {"type": "struct", "schema-id": 1, "fields": []}
        ],
        "current-schema-id": 0,
        "partition-specs": [
            {"spec-id": 0, "fields": []},
            {"spec-id": 1, "fields": []}
        ],
        "default-spec-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
        "snapshots": [{"snapshot-id": 10, "schema-id": 0}],
        "current-snapshot-id": 10,
        "refs": {"main": {"type": "branch", "snapshot-id": 10}},
        "metadata-log": []
    });
    let updated = apply_table_commit_updates_at(
        metadata,
        &[
            serde_json::json!({
                "action": "set-statistics",
                "statistics": {
                    "snapshot-id": 10,
                    "statistics-path": "s3://warehouse/tables/table-id/metadata/stats.puffin",
                    "file-size-in-bytes": 128,
                    "file-footer-size-in-bytes": 16,
                    "blob-metadata": [{
                        "type": "apache-datasketches-theta-v1",
                        "snapshot-id": 10,
                        "sequence-number": 1,
                        "fields": [1],
                        "properties": {"compression-codec": "zstd"}
                    }]
                }
            }),
            serde_json::json!({
                "action": "set-partition-statistics",
                "partition-statistics": {
                    "snapshot-id": 10,
                    "statistics-path": "s3://warehouse/tables/table-id/metadata/partition-stats.parquet",
                    "file-size-in-bytes": 64
                }
            }),
            serde_json::json!({"action": "remove-partition-specs", "spec-ids": [1]}),
            serde_json::json!({"action": "remove-schemas", "schema-ids": [1]}),
        ],
        "metadata/00001.metadata.json",
        100,
    )
    .expect("standard table updates should apply");
    assert_eq!(updated["statistics"][0]["snapshot-id"], 10);
    assert_eq!(updated["partition-statistics"][0]["snapshot-id"], 10);
    assert_eq!(updated["partition-specs"].as_array().map(Vec::len), Some(1));
    assert_eq!(updated["schemas"].as_array().map(Vec::len), Some(1));
    let removed = apply_table_commit_updates_at(
        updated,
        &[
            serde_json::json!({"action": "remove-statistics", "snapshot-id": 10}),
            serde_json::json!({"action": "remove-partition-statistics", "snapshot-id": 10}),
        ],
        "metadata/00002.metadata.json",
        101,
    )
    .expect("standard table removals should apply");
    assert!(removed["statistics"].as_array().is_some_and(Vec::is_empty));
    assert!(removed["partition-statistics"].as_array().is_some_and(Vec::is_empty));
}

#[test]
fn remove_snapshots_rejects_mixed_snapshot_id_types() {
    let metadata = serde_json::json!({
        "snapshots": [{"snapshot-id": 10}, {"snapshot-id": 11}],
        "snapshot-log": [{"snapshot-id": 10}, {"snapshot-id": 11}],
        "metadata-log": []
    });
    let error = apply_table_commit_updates_at(
        metadata,
        &[serde_json::json!({"action": "remove-snapshots", "snapshot-ids": [10, "bad"]})],
        "metadata/00001.metadata.json",
        100,
    )
    .expect_err("mixed snapshot id types must fail before removing snapshots");

    assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
}

#[test]
fn remove_snapshots_removes_associated_statistics_entries() {
    let metadata = serde_json::json!({
        "last-updated-ms": 1,
        "snapshots": [{"snapshot-id": 10}, {"snapshot-id": 11}],
        "snapshot-log": [{"snapshot-id": 10}, {"snapshot-id": 11}],
        "statistics": [
            {"snapshot-id": 10, "statistics-path": "s3://warehouse/stats-10.puffin"},
            {"snapshot-id": 11, "statistics-path": "s3://warehouse/stats-11.puffin"}
        ],
        "partition-statistics": [
            {"snapshot-id": 10, "statistics-path": "s3://warehouse/partition-stats-10.parquet"},
            {"snapshot-id": 11, "statistics-path": "s3://warehouse/partition-stats-11.parquet"}
        ],
        "metadata-log": []
    });
    let updated = apply_table_commit_updates_at(
        metadata,
        &[serde_json::json!({"action": "remove-snapshots", "snapshot-ids": [10]})],
        "metadata/00001.metadata.json",
        100,
    )
    .expect("snapshot expiration should remove associated statistics entries");

    for field in ["snapshots", "snapshot-log", "statistics", "partition-statistics"] {
        assert_eq!(updated[field].as_array().map(Vec::len), Some(1));
        assert_eq!(updated[field][0]["snapshot-id"], 11);
    }
}

#[test]
fn removing_snapshots_clears_dangling_references_and_current_snapshot() {
    let metadata = serde_json::json!({
        "last-updated-ms": 1,
        "current-snapshot-id": 10,
        "snapshots": [{"snapshot-id": 10}, {"snapshot-id": 11}],
        "refs": {
            "main": {"snapshot-id": 10, "type": "branch"},
            "release": {"snapshot-id": 10, "type": "tag"},
            "audit": {"snapshot-id": 11, "type": "branch"}
        },
        "snapshot-log": [{"snapshot-id": 10}, {"snapshot-id": 11}],
        "metadata-log": []
    });
    let updated = apply_table_commit_updates_at(
        metadata,
        &[serde_json::json!({"action": "remove-snapshots", "snapshot-ids": [10]})],
        "metadata/00001.metadata.json",
        100,
    )
    .expect("snapshot removal should clean references");

    assert_eq!(updated["current-snapshot-id"], -1);
    assert!(updated["refs"].get("main").is_none());
    assert!(updated["refs"].get("release").is_none());
    assert_eq!(updated["refs"]["audit"]["snapshot-id"], 11);
}

#[test]
fn removing_an_intermediate_snapshot_truncates_earlier_history() {
    let metadata = serde_json::json!({
        "last-updated-ms": 1,
        "current-snapshot-id": 12,
        "snapshots": [{"snapshot-id": 10}, {"snapshot-id": 11}, {"snapshot-id": 12}],
        "refs": {"main": {"snapshot-id": 12, "type": "branch"}},
        "snapshot-log": [
            {"timestamp-ms": 10, "snapshot-id": 10},
            {"timestamp-ms": 11, "snapshot-id": 11},
            {"timestamp-ms": 12, "snapshot-id": 12}
        ],
        "metadata-log": []
    });
    let updated = apply_table_commit_updates_at(
        metadata,
        &[serde_json::json!({"action": "remove-snapshots", "snapshot-ids": [11]})],
        "metadata/00001.metadata.json",
        100,
    )
    .expect("snapshot removal should preserve valid time-travel history only");

    assert_eq!(updated["snapshot-log"], serde_json::json!([{"timestamp-ms": 12, "snapshot-id": 12}]));
}

#[test]
fn removing_main_snapshot_reference_clears_current_snapshot() {
    let metadata = serde_json::json!({
        "last-updated-ms": 1,
        "current-snapshot-id": 10,
        "snapshots": [{"snapshot-id": 10}],
        "refs": {"main": {"snapshot-id": 10, "type": "branch"}},
        "snapshot-log": [{"snapshot-id": 10}],
        "metadata-log": []
    });
    let updated = apply_table_commit_updates_at(
        metadata,
        &[serde_json::json!({"action": "remove-snapshot-ref", "ref-name": "main"})],
        "metadata/00001.metadata.json",
        100,
    )
    .expect("main reference removal should succeed");

    assert_eq!(updated["current-snapshot-id"], -1);
    assert!(updated["refs"].get("main").is_none());
}

#[test]
fn table_statistics_updates_reject_malformed_standard_files() {
    let metadata = serde_json::json!({"metadata-log": []});
    for update in [
        serde_json::json!({
            "action": "set-statistics",
            "statistics": {
                "snapshot-id": 10,
                "statistics-path": "s3://warehouse/stats.puffin",
                "file-footer-size-in-bytes": 1,
                "blob-metadata": []
            }
        }),
        serde_json::json!({
            "action": "set-statistics",
            "statistics": {
                "snapshot-id": 10,
                "statistics-path": "s3://warehouse/stats.puffin",
                "file-size-in-bytes": 1,
                "file-footer-size-in-bytes": 2,
                "blob-metadata": []
            }
        }),
        serde_json::json!({
            "action": "set-partition-statistics",
            "partition-statistics": {
                "snapshot-id": 10,
                "statistics-path": "s3://warehouse/partition-stats.parquet"
            }
        }),
        serde_json::json!({
            "action": "set-statistics",
            "snapshot-id": 11,
            "statistics": {
                "snapshot-id": 10,
                "statistics-path": "s3://warehouse/stats.puffin",
                "file-size-in-bytes": 1,
                "file-footer-size-in-bytes": 0,
                "blob-metadata": []
            }
        }),
    ] {
        let error = apply_table_commit_updates_at(metadata.clone(), &[update], "metadata/00001.metadata.json", 100)
            .expect_err("malformed statistics updates must fail");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
        assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));
    }
}

#[test]
fn table_encryption_key_updates_require_format_version_three() {
    for update in [
        serde_json::json!({
            "action": "add-encryption-key",
            "encryption-key": {"key-id": "key-1", "encrypted-key-metadata": "AQID"}
        }),
        serde_json::json!({"action": "remove-encryption-key", "key-id": "key-1"}),
    ] {
        let error = apply_table_commit_updates_at(
            test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
            &[update],
            "metadata/00001.metadata.json",
            100,
        )
        .expect_err("Iceberg v2 tables must reject v3 encryption-key updates");
        assert_eq!(error.status_code(), Some(StatusCode::NOT_ACCEPTABLE));
    }
}

#[test]
fn table_location_updates_must_stay_inside_bucket() {
    let metadata = serde_json::json!({
        "location": "s3://warehouse/tables/table-id",
        "last-updated-ms": 1,
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
fn create_view_request_accepts_standard_iceberg_rest_shape() {
    let request: CreateViewRequest = serde_json::from_value(serde_json::json!({
        "name": "recent_events",
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
        "view-version": {
            "version-id": 1,
            "timestamp-ms": 1,
            "schema-id": 0,
            "summary": {
                "engine-name": "spark",
                "engine-version": "3.5.0"
            },
            "default-catalog": "warehouse",
            "default-namespace": ["analytics"],
            "representations": [
                {
                    "type": "sql",
                    "sql": "SELECT id FROM analytics.events WHERE ts >= current_date()",
                    "dialect": "spark"
                }
            ]
        },
        "properties": {
            "comment": "recent event ids"
        }
    }))
    .expect("standard create view request should parse");

    assert_eq!(request.name, "recent_events");
    assert_eq!(request.properties.get("comment").map(String::as_str), Some("recent event ids"));
}

#[test]
fn view_versions_use_the_created_schema_and_resolve_minus_one() {
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let request: CreateViewRequest = serde_json::from_value(serde_json::json!({
        "name": "recent_events",
        "schema": {"type": "struct", "schema-id": 3, "fields": []},
        "view-version": {
            "version-id": 1,
            "timestamp-ms": 1,
            "schema-id": 99,
            "summary": {"engine-name": "spark"},
            "default-catalog": "warehouse",
            "default-namespace": ["analytics"],
            "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]
        },
        "properties": {}
    }))
    .expect("view request should parse");
    let (_, metadata) = view_entry_from_create_view_request("warehouse", &namespace, request)
        .expect("create should resolve the current schema placeholder");
    assert_eq!(metadata["schemas"][0]["schema-id"], 0);
    assert_eq!(metadata["versions"][0]["schema-id"], 0);

    let updated = apply_view_commit_updates_at(
        metadata,
        &[
            serde_json::json!({
                "action": "add-schema",
                "schema": {"type": "struct", "schema-id": 99, "fields": []}
            }),
            serde_json::json!({
                "action": "add-view-version",
                "view-version": {
                    "version-id": 2,
                    "timestamp-ms": 2,
                    "schema-id": -1,
                    "summary": {"engine-name": "spark"},
                    "default-catalog": "warehouse",
                    "default-namespace": ["analytics"],
                    "representations": [{"type": "sql", "sql": "SELECT 2", "dialect": "spark"}]
                }
            }),
        ],
        2,
    )
    .expect("view commit should resolve the current schema placeholder");
    assert_eq!(updated["schemas"][1]["schema-id"], 1);
    assert_eq!(updated["versions"][1]["schema-id"], 1);
    assert_eq!(updated["versions"][1]["timestamp-ms"], 2);
    assert!(updated.get("last-updated-ms").is_none());
    assert!(updated.get("metadata-log").is_none());
    assert!(updated.get("last-column-id").is_none());
}

#[test]
fn current_view_version_minus_one_selects_the_last_added_version() {
    let metadata = serde_json::json!({
        "format-version": 1,
        "view-uuid": "view-uuid",
        "location": "s3://warehouse/views/view-id",
        "current-version-id": 5,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
        "versions": [{
            "version-id": 5,
            "timestamp-ms": 1,
            "schema-id": 0,
            "summary": {},
            "default-namespace": ["analytics"],
            "representations": [{"type": "sql", "sql": "SELECT 5", "dialect": "spark"}]
        }],
        "version-log": [{"version-id": 5, "timestamp-ms": 1}],
        "properties": {}
    });
    let updated = apply_view_commit_updates_at(
        metadata,
        &[
            serde_json::json!({
                "action": "add-view-version",
                "view-version": {
                    "version-id": 3,
                    "timestamp-ms": 2,
                    "schema-id": 0,
                    "summary": {},
                    "default-namespace": ["analytics"],
                    "representations": [{"type": "sql", "sql": "SELECT 3", "dialect": "spark"}]
                }
            }),
            serde_json::json!({"action": "set-current-view-version", "view-version-id": -1}),
        ],
        2,
    )
    .expect("minus one should resolve to the last added view version");

    assert_eq!(updated["current-version-id"], 3);
    assert_eq!(
        updated["version-log"]
            .as_array()
            .and_then(|log| log.last())
            .map(|entry| &entry["version-id"]),
        Some(&serde_json::Value::from(3))
    );
    assert_eq!(
        updated["version-log"]
            .as_array()
            .and_then(|log| log.last())
            .map(|entry| &entry["timestamp-ms"]),
        Some(&serde_json::Value::from(2))
    );
}

#[test]
fn last_added_view_ids_require_a_preceding_add_update() {
    let metadata = serde_json::json!({
        "format-version": 1,
        "view-uuid": "view-uuid",
        "location": "s3://warehouse/views/view-id",
        "current-version-id": 1,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
        "versions": [{
            "version-id": 1,
            "timestamp-ms": 1,
            "schema-id": 0,
            "summary": {},
            "default-namespace": ["analytics"],
            "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]
        }],
        "version-log": [{"version-id": 1, "timestamp-ms": 1}],
        "properties": {}
    });
    let version = serde_json::json!({
        "action": "add-view-version",
        "view-version": {
            "version-id": 2,
            "timestamp-ms": 2,
            "schema-id": 0,
            "summary": {},
            "default-namespace": ["analytics"],
            "representations": [{"type": "sql", "sql": "SELECT 2", "dialect": "spark"}]
        }
    });
    let invalid_update_sequences = [
        vec![serde_json::json!({
            "action": "add-view-version",
            "view-version": {
                "version-id": 2,
                "timestamp-ms": 2,
                "schema-id": -1,
                "summary": {},
                "default-namespace": ["analytics"],
                "representations": [{"type": "sql", "sql": "SELECT 2", "dialect": "spark"}]
            }
        })],
        vec![serde_json::json!({"action": "set-current-view-version", "view-version-id": -1})],
        vec![
            serde_json::json!({"action": "set-current-view-version", "view-version-id": -1}),
            version,
        ],
    ];

    for updates in invalid_update_sequences {
        let error = apply_view_commit_updates_at(metadata.clone(), &updates, 3)
            .expect_err("historical or later view objects must not satisfy a -1 reference");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert!(
            error
                .message()
                .is_some_and(|message| message.contains("requires a preceding"))
        );
    }
}

#[test]
fn view_history_uses_added_version_time_and_skips_current_version_noops() {
    let metadata = serde_json::json!({
        "format-version": 1,
        "view-uuid": "view-uuid",
        "location": "s3://warehouse/views/view-id",
        "current-version-id": 1,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
        "versions": [{
            "version-id": 1,
            "timestamp-ms": 1,
            "schema-id": 0,
            "summary": {},
            "default-namespace": ["analytics"],
            "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]
        }],
        "version-log": [{"version-id": 1, "timestamp-ms": 1}],
        "properties": {}
    });
    let unchanged = apply_view_commit_updates_at(
        metadata.clone(),
        &[serde_json::json!({"action": "set-current-view-version", "view-version-id": 1})],
        100,
    )
    .expect("setting the current view version again should be a no-op");
    assert_eq!(unchanged["version-log"], metadata["version-log"]);

    let updated = apply_view_commit_updates_at(
        metadata,
        &[
            serde_json::json!({
                "action": "add-view-version",
                "view-version": {
                    "version-id": 2,
                    "timestamp-ms": 20,
                    "schema-id": 0,
                    "summary": {},
                    "default-namespace": ["analytics"],
                    "representations": [{"type": "sql", "sql": "SELECT 2", "dialect": "spark"}]
                }
            }),
            serde_json::json!({
                "action": "add-view-version",
                "view-version": {
                    "version-id": 3,
                    "timestamp-ms": 30,
                    "schema-id": 0,
                    "summary": {},
                    "default-namespace": ["analytics"],
                    "representations": [{"type": "sql", "sql": "SELECT 3", "dialect": "spark"}]
                }
            }),
            serde_json::json!({"action": "set-current-view-version", "view-version-id": 2}),
        ],
        100,
    )
    .expect("an explicitly selected version added in this commit should use its own timestamp");
    assert_eq!(updated["current-version-id"], 2);
    assert_eq!(
        updated["version-log"].as_array().and_then(|log| log.last()),
        Some(&serde_json::json!({
            "timestamp-ms": 20,
            "version-id": 2
        }))
    );
}

#[test]
fn view_requests_and_metadata_require_standard_fields() {
    let request_without_properties = serde_json::from_value::<CreateViewRequest>(serde_json::json!({
        "name": "recent_events",
        "schema": {"type": "struct", "schema-id": 0, "fields": []},
        "view-version": {
            "version-id": 1,
            "timestamp-ms": 1,
            "schema-id": 0,
            "summary": {},
            "default-namespace": ["analytics"],
            "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]
        }
    }))
    .expect("the Java REST serializer omits empty view properties");
    assert!(request_without_properties.properties.is_empty());

    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let request = serde_json::from_value::<CreateViewRequest>(serde_json::json!({
        "name": "recent_events",
        "schema": {"type": "struct", "schema-id": 0, "fields": []},
        "view-version": {
            "version-id": 1,
            "schema-id": 0,
            "summary": {},
            "default-namespace": ["analytics"],
            "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]
        },
        "properties": {}
    }))
    .expect("request shape should parse before metadata validation");
    let error = view_entry_from_create_view_request("warehouse", &namespace, request)
        .expect_err("view-version timestamp-ms must be required");
    assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);

    let malformed_schema_request = serde_json::from_value::<CreateViewRequest>(serde_json::json!({
        "name": "recent_events",
        "schema": {},
        "view-version": {
            "version-id": 1,
            "timestamp-ms": 1,
            "schema-id": 0,
            "summary": {},
            "default-namespace": ["analytics"],
            "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]
        },
        "properties": {}
    }))
    .expect("request shape should parse before schema validation");
    view_entry_from_create_view_request("warehouse", &namespace, malformed_schema_request)
        .expect_err("create view must reject schemas missing type and fields");

    let metadata = serde_json::json!({
        "format-version": 1,
        "view-uuid": "view-uuid",
        "location": "s3://warehouse/views/view-id",
        "current-version-id": 1,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
        "versions": [{
            "version-id": 1,
            "timestamp-ms": 1,
            "schema-id": 0,
            "summary": {},
            "default-namespace": ["analytics"],
            "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]
        }],
        "version-log": [{"version-id": 1, "timestamp-ms": 1}],
        "properties": {}
    });
    let mut missing_current = metadata.clone();
    missing_current
        .as_object_mut()
        .expect("metadata should be an object")
        .remove("current-version-id");
    validate_supported_view_metadata(&missing_current).expect_err("current-version-id must be required");

    let mut unsupported_representation = metadata.clone();
    unsupported_representation["versions"][0]["representations"][0]["type"] = serde_json::Value::from("python");
    validate_supported_view_metadata(&unsupported_representation).expect_err("non-SQL view representations must be rejected");

    let mut duplicate_dialect = metadata.clone();
    duplicate_dialect["versions"][0]["representations"] = serde_json::json!([
        {"type": "sql", "sql": "SELECT 1", "dialect": "spark"},
        {"type": "sql", "sql": "SELECT 2", "dialect": "SPARK"}
    ]);
    validate_supported_view_metadata(&duplicate_dialect).expect_err("a view version must not contain duplicate SQL dialects");

    let mut malformed_schema = metadata.clone();
    malformed_schema["schemas"] = serde_json::json!([{"schema-id": 0}]);
    validate_supported_view_metadata(&malformed_schema).expect_err("view schemas must include type and fields");

    let updated = apply_view_commit_updates_at(
        metadata.clone(),
        &[serde_json::json!({"action": "set-properties", "updates": {"owner": "analytics"}})],
        2,
    )
    .expect("standard view metadata without table-only timestamps must remain mutable");
    assert_eq!(updated["properties"]["owner"], "analytics");
    assert!(updated.get("last-updated-ms").is_none());
    assert!(updated.get("metadata-log").is_none());

    let missing_version_id = apply_view_commit_updates_at(
        metadata,
        &[serde_json::json!({
            "action": "add-view-version",
            "view-version": {
                "timestamp-ms": 2,
                "schema-id": 0,
                "summary": {},
                "default-namespace": ["analytics"],
                "representations": [{"type": "sql", "sql": "SELECT 2", "dialect": "spark"}]
            }
        })],
        2,
    )
    .expect_err("add-view-version must not synthesize version-id");
    assert_eq!(missing_version_id.code(), &S3ErrorCode::InvalidRequest);
}

#[test]
fn view_commit_rejects_unsupported_format_version_upgrade() {
    let metadata = serde_json::json!({
        "format-version": 1,
        "view-uuid": "view-uuid",
        "location": "s3://warehouse/views/view-id",
        "schemas": [{"schema-id": 0, "type": "struct", "fields": []}],
        "versions": [{
            "version-id": 1,
            "timestamp-ms": 1,
            "schema-id": 0,
            "summary": {"engine-name": "spark"},
            "default-namespace": ["analytics"],
            "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]
        }],
        "current-version-id": 1,
        "version-log": [],
        "metadata-log": [],
        "properties": {}
    });

    let error = apply_view_commit_updates_at(
        metadata,
        &[serde_json::json!({"action": "upgrade-format-version", "format-version": 2})],
        0,
    )
    .expect_err("Iceberg view format-version 2 is unsupported");

    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_UNSUPPORTED_OPERATION.into()));
    assert_eq!(error.status_code(), Some(StatusCode::NOT_ACCEPTABLE));
}

#[test]
fn create_view_request_accepts_deep_warehouse_location() {
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let deep_prefix = (0..80).map(|index| format!("level-{index}")).collect::<Vec<_>>().join("/");
    let location = format!("s3://warehouse/{deep_prefix}");
    assert!(crate::table_catalog::validate_table_warehouse_location("warehouse", &location).is_err());
    let request: CreateViewRequest = serde_json::from_value(serde_json::json!({
        "name": "recent_events",
        "location": location,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": []
        },
        "view-version": {
            "version-id": 1,
            "timestamp-ms": 1,
            "schema-id": 0,
            "summary": {
                "engine-name": "spark"
            },
            "default-catalog": "warehouse",
            "default-namespace": ["analytics"],
            "representations": [
                {
                    "type": "sql",
                    "sql": "SELECT 1",
                    "dialect": "spark"
                }
            ]
        },
        "properties": {}
    }))
    .expect("deep create view request should parse");

    let (entry, metadata) = view_entry_from_create_view_request("warehouse", &namespace, request)
        .expect("view location should not inherit table warehouse index depth limits");

    assert_eq!(entry.warehouse_location, format!("s3://warehouse/{deep_prefix}"));
    validate_metadata_view_location_in_bucket("warehouse", &metadata)
        .expect("view metadata location should not inherit table warehouse index depth limits");
}

#[tokio::test]
async fn view_catalog_responses_persist_replace_and_drop_view_metadata() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
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

    let create_request: CreateViewRequest = serde_json::from_value(serde_json::json!({
        "name": "recent_events",
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
        "view-version": {
            "version-id": 1,
            "timestamp-ms": 1,
            "schema-id": 0,
            "summary": {
                "engine-name": "spark"
            },
            "default-catalog": "warehouse",
            "default-namespace": ["analytics"],
            "representations": [
                {
                    "type": "sql",
                    "sql": "SELECT id FROM analytics.events",
                    "dialect": "spark"
                }
            ]
        },
        "properties": {}
    }))
    .expect("standard create view request should parse");

    let create_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let created = create_view_response(&store, &create_backend, "warehouse", &namespace, create_request, true)
        .await
        .expect("view should be created");
    assert_eq!(created.metadata["format-version"], 1);
    assert_eq!(created.metadata["current-version-id"], 1);
    assert_eq!(created.metadata["versions"][0]["representations"][0]["dialect"], "spark");
    let created_metadata_key = table_metadata_location_for_catalog("warehouse", &created.metadata_location)
        .expect("created metadata location should normalize for the object backend");
    assert!(
        metadata_backend
            .object_exists("warehouse", &created_metadata_key)
            .await
            .expect("view metadata object lookup should succeed")
    );

    let unpaginated_uri = "/".parse::<http::Uri>().expect("list URI should parse");
    let listed = list_views_response(&store, "warehouse", &namespace, &unpaginated_uri)
        .await
        .expect("views should list");
    assert_eq!(listed.identifiers.len(), 1);
    assert_eq!(listed.identifiers[0].name, "recent_events");

    let loaded = load_view_response(&store, &metadata_backend, "warehouse", &namespace, "recent_events")
        .await
        .expect("view should load");
    assert_eq!(loaded.metadata_location, created.metadata_location);

    let metadata_directory = created_metadata_key
        .rsplit_once('/')
        .map(|(directory, _)| directory)
        .expect("created metadata key should have a directory");
    let invalid_target_key = format!("{metadata_directory}/invalid.metadata.json");
    let mut invalid_target = created.metadata.clone();
    invalid_target["format-version"] = serde_json::Value::from(2);
    metadata_backend
        .put_json("warehouse", &invalid_target_key, invalid_target)
        .await;
    let invalid_replace = serde_json::from_value::<RestCommitViewRequest>(serde_json::json!({
        "new-metadata-location": table_metadata_location_for_client("warehouse", &invalid_target_key),
        "updates": []
    }))
    .expect("external replace request should parse");
    let invalid_replace_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let error = replace_view_response(
        &store,
        &invalid_replace_backend,
        "warehouse",
        &namespace,
        "recent_events",
        invalid_replace,
    )
    .await
    .expect_err("unsupported external view metadata must fail before pointer publication");
    assert_eq!(error.status_code(), Some(StatusCode::NOT_ACCEPTABLE));
    assert_eq!(
        store
            .load_view("warehouse", "analytics", "recent_events")
            .await
            .expect("view lookup should succeed")
            .expect("view should remain registered")
            .metadata_location,
        created_metadata_key
    );

    let replace_request: RestCommitViewRequest = serde_json::from_value(serde_json::json!({
        "updates": [
            {
                "action": "add-view-version",
                "view-version": {
                    "version-id": 2,
                    "timestamp-ms": 2,
                    "schema-id": 0,
                    "summary": {
                        "engine-name": "spark"
                    },
                    "default-catalog": "warehouse",
                    "default-namespace": ["analytics"],
                    "representations": [
                        {
                            "type": "sql",
                            "sql": "SELECT id FROM analytics.events WHERE id > 10",
                            "dialect": "spark"
                        }
                    ]
                }
            },
            {
                "action": "set-current-view-version",
                "view-version-id": 2
            }
        ]
    }))
    .expect("replace view request should parse");
    let replace_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let replaced = replace_view_response(&store, &replace_backend, "warehouse", &namespace, "recent_events", replace_request)
        .await
        .expect("view should replace");
    assert_ne!(replaced.metadata_location, created.metadata_location);
    assert_eq!(replaced.metadata["current-version-id"], 2);
    assert_eq!(
        replaced.metadata["version-log"]
            .as_array()
            .expect("version log should be an array")
            .len(),
        2
    );

    drop_view_in_store(&store, "warehouse", &namespace, "recent_events")
        .await
        .expect("view should drop");
    let listed = list_views_response(&store, "warehouse", &namespace, &unpaginated_uri)
        .await
        .expect("views should list after drop");
    assert!(listed.identifiers.is_empty());

    let recreate_request: CreateViewRequest = serde_json::from_value(serde_json::json!({
        "name": "recent_events",
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
        "view-version": {
            "version-id": 1,
            "timestamp-ms": 1,
            "schema-id": 0,
            "summary": {
                "engine-name": "spark"
            },
            "default-catalog": "warehouse",
            "default-namespace": ["analytics"],
            "representations": [
                {
                    "type": "sql",
                    "sql": "SELECT id FROM analytics.events WHERE id > 100",
                    "dialect": "spark"
                }
            ]
        },
        "properties": {}
    }))
    .expect("standard recreate view request should parse");
    let recreate_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let recreated = create_view_response(&store, &recreate_backend, "warehouse", &namespace, recreate_request, true)
        .await
        .expect("dropped view name should be reusable");
    assert_ne!(recreated.metadata_location, created.metadata_location);
}

#[tokio::test]
async fn replace_view_holds_target_metadata_and_view_fences_until_pointer_publish() {
    let pause = TestCatalogPublishPause::default();
    let store = Arc::new(TestTableCatalogStore {
        replace_view_pause: Some(pause.clone()),
        ..Default::default()
    });
    let metadata_backend = TestTableCatalogObjectBackend::default();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_recent_events_view(store.as_ref(), &metadata_backend, &namespace).await;
    let current_metadata_key = table_metadata_location_for_catalog("warehouse", &created.metadata_location)
        .expect("created metadata location should normalize");
    let metadata_directory = current_metadata_key
        .rsplit_once('/')
        .map(|(directory, _)| directory)
        .expect("created metadata key should have a directory");
    let target_metadata_key = format!("{metadata_directory}/external.metadata.json");
    metadata_backend
        .put_json("warehouse", &target_metadata_key, created.metadata.clone())
        .await;
    let request = serde_json::from_value::<RestCommitViewRequest>(serde_json::json!({
        "new-metadata-location": table_metadata_location_for_client("warehouse", &target_metadata_key),
        "requirements": [],
        "updates": []
    }))
    .expect("replace view request should parse");

    let replace_store = Arc::clone(&store);
    let replace_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let replace_namespace = namespace.clone();
    let replace = tokio::spawn(async move {
        let result = replace_view_response(
            replace_store.as_ref(),
            &replace_backend,
            "warehouse",
            &replace_namespace,
            "recent_events",
            request,
        )
        .await;
        replace_backend.finish(result).await
    });
    tokio::time::timeout(StdDuration::from_secs(2), pause.wait_started())
        .await
        .expect("view replacement should reach catalog publication");

    let view_name = crate::table_catalog::IdentifierSegment::parse("recent_events").expect("view should parse");
    let view_lock = crate::table_catalog::default_table_publication_lock_path(&namespace, &view_name);
    let bucket_lock = crate::table_catalog::default_table_bucket_publication_lock_path();
    assert!(
        !metadata_backend.write_lock_is_held("warehouse", &bucket_lock).await,
        "view replacement without warehouse relocation must not serialize the table bucket"
    );
    assert!(
        metadata_backend.write_lock_is_held("warehouse", &view_lock).await,
        "view replacement must retain its publication fence until pointer publication"
    );
    assert!(
        metadata_backend.write_lock_is_held("warehouse", &target_metadata_key).await,
        "target view metadata must remain stable until pointer publication"
    );

    metadata_backend.lock_attempts.lock().await.clear();
    let writer_backend = metadata_backend.clone();
    let writer_target = target_metadata_key.clone();
    let writer = tokio::spawn(async move {
        crate::table_catalog::TableCatalogObjectBackend::acquire_write_lock(&writer_backend, "warehouse", &writer_target).await
    });
    metadata_backend.wait_for_lock_attempts(1).await;
    assert!(!writer.is_finished(), "a target metadata writer must wait for pointer publication");

    pause.release();
    let replaced = tokio::time::timeout(StdDuration::from_secs(2), replace)
        .await
        .expect("view replacement should complete")
        .expect("view replacement task should join")
        .expect("view replacement should succeed");
    tokio::time::timeout(StdDuration::from_secs(2), writer)
        .await
        .expect("target metadata writer should continue after publication")
        .expect("target metadata writer task should join")
        .expect("target metadata writer lock acquisition should succeed");
    assert_eq!(
        table_metadata_location_for_catalog("warehouse", &replaced.metadata_location)
            .expect("replaced metadata location should normalize"),
        target_metadata_key
    );
}

#[tokio::test]
async fn replace_view_holds_table_bucket_fence_for_warehouse_relocation() {
    let pause = TestCatalogPublishPause::default();
    let store = Arc::new(TestTableCatalogStore {
        replace_view_pause: Some(pause.clone()),
        ..Default::default()
    });
    let metadata_backend = TestTableCatalogObjectBackend::default();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_recent_events_view(store.as_ref(), &metadata_backend, &namespace).await;
    let current_metadata_key = table_metadata_location_for_catalog("warehouse", &created.metadata_location)
        .expect("created metadata location should normalize");
    let metadata_directory = current_metadata_key
        .rsplit_once('/')
        .map(|(directory, _)| directory)
        .expect("created metadata key should have a directory");
    let target_metadata_key = format!("{metadata_directory}/relocated.metadata.json");
    let mut target_metadata = created.metadata;
    target_metadata["location"] = serde_json::Value::String("s3://warehouse/views/relocated".to_string());
    metadata_backend
        .put_json("warehouse", &target_metadata_key, target_metadata)
        .await;
    let request = serde_json::from_value::<RestCommitViewRequest>(serde_json::json!({
        "new-metadata-location": table_metadata_location_for_client("warehouse", &target_metadata_key),
        "requirements": [],
        "updates": []
    }))
    .expect("replace view request should parse");

    let replace_store = Arc::clone(&store);
    let replace_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let replace_namespace = namespace.clone();
    let replace = tokio::spawn(async move {
        let result = replace_view_response(
            replace_store.as_ref(),
            &replace_backend,
            "warehouse",
            &replace_namespace,
            "recent_events",
            request,
        )
        .await;
        replace_backend.finish(result).await
    });
    tokio::time::timeout(StdDuration::from_secs(2), pause.wait_started())
        .await
        .expect("view replacement should reach catalog publication");

    let bucket_lock = crate::table_catalog::default_table_bucket_publication_lock_path();
    assert!(
        metadata_backend.write_lock_is_held("warehouse", &bucket_lock).await,
        "view warehouse relocation must retain the table-bucket publication fence"
    );

    pause.release();
    tokio::time::timeout(StdDuration::from_secs(2), replace)
        .await
        .expect("view replacement should complete")
        .expect("view replacement task should join")
        .expect("view replacement should succeed");
}

#[tokio::test]
async fn external_view_metadata_replacement_repairs_legacy_incomplete_metadata() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::default();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_recent_events_view(&store, &metadata_backend, &namespace).await;
    let current = store
        .load_view("warehouse", "analytics", "recent_events")
        .await
        .expect("view lookup should succeed")
        .expect("view should exist");
    let mut incomplete = created.metadata.clone();
    incomplete
        .as_object_mut()
        .expect("view metadata should be an object")
        .remove("versions");
    metadata_backend
        .put_json("warehouse", &current.metadata_location, incomplete)
        .await;
    load_view_response(&store, &metadata_backend, "warehouse", &namespace, "recent_events")
        .await
        .expect_err("legacy incomplete view metadata must not be served");

    let metadata_directory = current
        .metadata_location
        .rsplit_once('/')
        .map(|(directory, _)| directory)
        .expect("current metadata location should have a directory");
    let target = format!("{metadata_directory}/repaired.metadata.json");
    metadata_backend
        .put_json("warehouse", &target, created.metadata.clone())
        .await;
    let request = serde_json::from_value::<RestCommitViewRequest>(serde_json::json!({
        "new-metadata-location": table_metadata_location_for_client("warehouse", &target),
        "requirements": [{"type": "assert-view-uuid", "uuid": current.view_uuid}],
        "updates": []
    }))
    .expect("view repair request should parse");
    let publication_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let repaired = replace_view_response(&store, &publication_backend, "warehouse", &namespace, "recent_events", request)
        .await
        .expect("a valid external metadata target should repair legacy incomplete metadata");

    assert_eq!(repaired.metadata, created.metadata);
    load_view_response(&store, &metadata_backend, "warehouse", &namespace, "recent_events")
        .await
        .expect("the repaired view should load");
}

#[tokio::test]
async fn table_ref_write_responses_use_commit_guard_and_protect_deletes() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_snapshot_manifest(&metadata_backend, "warehouse", &manifest_list, 10, 1, &[(&data_file, 0, 1, 10, 1)]).await;

    let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 10,
                    "sequence-number": 1,
                    "timestamp-ms": 1234,
                    "manifest-list": manifest_list,
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
    .expect("append request should parse");
    commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        append_request,
    )
    .await
    .expect("append should commit");

    let ref_request_json = serde_json::json!({
        "snapshot-id": 10,
        "type": "tag",
        "max-ref-age-ms": 86400000,
        "expected-snapshot-id": null
    });
    let before = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let denied_authorizations = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let denied_backend = TableCommitObjectBackend::test(
        metadata_backend.clone(),
        Arc::clone(&denied_authorizations),
        Some(before.metadata_location.clone()),
    );
    let denied_request: PutTableRefRequest =
        serde_json::from_value(ref_request_json.clone()).expect("denied ref put request should parse");
    let denied =
        put_table_ref_response(&store, &denied_backend, "warehouse", &namespace, "events", "audit", denied_request).await;
    let error = denied_backend
        .finish(denied)
        .await
        .expect_err("ref writes must honor exact object authorization");
    assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
    assert!(
        denied_authorizations
            .lock()
            .await
            .contains(&(before.metadata_location.clone(), S3Action::GetObjectAction))
    );
    let after_denial = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should remain");
    assert_eq!(after_denial.metadata_location, before.metadata_location);
    assert_eq!(after_denial.version_token, before.version_token);
    assert_eq!(after_denial.generation, before.generation);

    let ref_request: PutTableRefRequest = serde_json::from_value(ref_request_json).expect("ref put request should parse");
    let ref_authorizations = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let ref_backend = TableCommitObjectBackend::test(metadata_backend.clone(), Arc::clone(&ref_authorizations), None);
    let result = put_table_ref_response(&store, &ref_backend, "warehouse", &namespace, "events", "audit", ref_request).await;
    ref_backend.finish(result).await.expect("ref put should commit");
    assert!(
        ref_authorizations
            .lock()
            .await
            .iter()
            .any(|(_, action)| *action == S3Action::PutObjectAction)
    );

    let refs = table_refs_response(&store, &metadata_backend, "warehouse", &namespace, "events")
        .await
        .expect("refs should load");
    assert_eq!(refs.refs["audit"]["type"], "tag");
    assert_eq!(refs.refs["audit"]["max-ref-age-ms"], 86400000);

    let delete_without_force: DeleteTableRefRequest =
        serde_json::from_value(serde_json::json!({})).expect("ref delete request should parse");
    let error = delete_table_ref_response(
        &store,
        &metadata_backend,
        "warehouse",
        &namespace,
        "events",
        "audit",
        delete_without_force,
    )
    .await
    .expect_err("retention refs should require force delete");
    assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);

    let force_delete: DeleteTableRefRequest =
        serde_json::from_value(serde_json::json!({ "force": true })).expect("ref force delete should parse");
    let delete_authorizations = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let delete_backend = TableCommitObjectBackend::test(metadata_backend.clone(), Arc::clone(&delete_authorizations), None);
    let result =
        delete_table_ref_response(&store, &delete_backend, "warehouse", &namespace, "events", "audit", force_delete).await;
    delete_backend.finish(result).await.expect("force delete should commit");
    assert!(
        delete_authorizations
            .lock()
            .await
            .iter()
            .any(|(_, action)| *action == S3Action::PutObjectAction)
    );
    let refs = table_refs_response(&store, &metadata_backend, "warehouse", &namespace, "events")
        .await
        .expect("refs should load after delete");
    assert!(!refs.refs.contains_key("audit"));

    let main_delete: DeleteTableRefRequest =
        serde_json::from_value(serde_json::json!({ "force": true })).expect("main delete request should parse");
    let error = delete_table_ref_response(&store, &metadata_backend, "warehouse", &namespace, "events", "main", main_delete)
        .await
        .expect_err("main ref should remain protected");
    assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidRequest);
}

#[test]
fn load_table_response_includes_rest_metadata_payload() {
    let metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": "table-uuid",
        "location": "s3://warehouse/tables/table-id",
        "metadata-log": [
            {
                "timestamp-ms": 1,
                "metadata-file": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00000.metadata.json"
            },
            {
                "timestamp-ms": 2,
                "metadata-file": "s3://warehouse/external/metadata/00000.metadata.json"
            }
        ]
    });
    let response = load_table_response_from_entry(table_entry_for_credentials(), metadata);

    assert_eq!(
        response.metadata_location,
        "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json"
    );
    assert_eq!(
        response.metadata["metadata-log"][0]["metadata-file"],
        "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00000.metadata.json"
    );
    assert_eq!(
        response.metadata["metadata-log"][1]["metadata-file"],
        "s3://warehouse/external/metadata/00000.metadata.json"
    );
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

#[test]
fn load_table_response_preserves_format_v4_relative_metadata_log() {
    let metadata = serde_json::json!({
        "format-version": 4,
        "table-uuid": "table-uuid",
        "metadata-log": [
            {
                "timestamp-ms": 1,
                "metadata-file": "metadata/00000.metadata.json"
            },
            {
                "timestamp-ms": 2,
                "metadata-file": ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00000.metadata.json"
            }
        ]
    });

    let response = load_table_response_from_entry(table_entry_for_credentials(), metadata);

    assert_eq!(response.metadata["metadata-log"][0]["metadata-file"], "metadata/00000.metadata.json");
    assert_eq!(
        response.metadata["metadata-log"][1]["metadata-file"],
        "s3://warehouse/.rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00000.metadata.json"
    );
}

#[test]
fn load_table_snapshot_selection_validates_and_filters_refs() {
    assert_eq!(
        rest_table_snapshot_selection_from_query(&"/".parse().expect("URI should parse"))
            .expect("omitted snapshots selection should parse"),
        RestTableSnapshotSelection::All
    );
    assert_eq!(
        rest_table_snapshot_selection_from_query(&"/?snapshots=all".parse().expect("URI should parse"))
            .expect("all snapshots selection should parse"),
        RestTableSnapshotSelection::All
    );
    assert_eq!(
        rest_table_snapshot_selection_from_query(&"/?snapshots=refs".parse().expect("URI should parse"))
            .expect("referenced snapshots selection should parse"),
        RestTableSnapshotSelection::Refs
    );
    for uri in ["/?snapshots=unknown", "/?snapshots=all&snapshots=refs"] {
        let error = rest_table_snapshot_selection_from_query(&uri.parse().expect("URI should parse"))
            .expect_err("invalid snapshots selections must fail");
        assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_BAD_REQUEST.into()));
    }

    let metadata = serde_json::json!({
        "snapshots": [
            {"snapshot-id": 1},
            {"snapshot-id": 2},
            {"snapshot-id": 3}
        ],
        "refs": {
            "audit": {"type": "tag", "snapshot-id": 1},
            "main": {"type": "branch", "snapshot-id": 3}
        }
    });
    let mut all = metadata.clone();
    apply_rest_table_snapshot_selection(&mut all, RestTableSnapshotSelection::All);
    assert_eq!(all["snapshots"].as_array().map(Vec::len), Some(3));

    let mut referenced = metadata;
    apply_rest_table_snapshot_selection(&mut referenced, RestTableSnapshotSelection::Refs);
    assert_eq!(
        referenced["snapshots"]
            .as_array()
            .expect("snapshots should remain an array")
            .iter()
            .filter_map(|snapshot| snapshot["snapshot-id"].as_i64())
            .collect::<Vec<_>>(),
        vec![1, 3]
    );

    let mut implicit_main = serde_json::json!({
        "current-snapshot-id": 2,
        "snapshots": [
            {"snapshot-id": 1},
            {"snapshot-id": 2}
        ]
    });
    apply_rest_table_snapshot_selection(&mut implicit_main, RestTableSnapshotSelection::Refs);
    assert_eq!(
        implicit_main["snapshots"]
            .as_array()
            .expect("snapshots should remain an array")
            .iter()
            .filter_map(|snapshot| snapshot["snapshot-id"].as_i64())
            .collect::<Vec<_>>(),
        vec![2]
    );
}

#[test]
fn table_metadata_location_for_catalog_accepts_only_the_table_bucket() {
    let object_key = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";

    assert_eq!(
        table_metadata_location_for_catalog("warehouse", &format!("s3://warehouse/{object_key}"))
            .expect("same-bucket metadata location should normalize"),
        object_key
    );
    assert!(table_metadata_location_for_catalog("warehouse", &format!("s3://other/{object_key}")).is_err());
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
    assert_eq!(
        response.config.get(CREDENTIAL_VENDING_CONFIG_KEY),
        Some(&CREDENTIAL_VENDING_UNSUPPORTED.to_string())
    );
    assert_eq!(
        response.config.get(CREDENTIAL_MODE_CONFIG_KEY),
        Some(&CREDENTIAL_MODE_CLIENT_PROVIDED.to_string())
    );
    assert_eq!(
        response.config.get(CREDENTIAL_VENDING_REASON_CONFIG_KEY),
        Some(&"credential-vending-disabled".to_string())
    );
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
    assert_eq!(
        response.config.get(CREDENTIAL_VENDING_CONFIG_KEY),
        Some(&CREDENTIAL_VENDING_UNSUPPORTED.to_string())
    );
    assert!(!response.config.contains_key(CREDENTIAL_SCOPE_PREFIX_CONFIG_KEY));
}

struct UnavailableTableCredentialIssuer;

#[async_trait::async_trait]
impl TableCredentialIssuer for UnavailableTableCredentialIssuer {
    async fn issue_table_credentials(
        &self,
        request: TableCredentialIssueRequest<'_>,
    ) -> S3Result<Option<IssuedTableCredentials>> {
        assert_eq!(request.scope_prefix, "s3://warehouse/tables/table-id/");
        Ok(None)
    }
}

#[tokio::test]
async fn unavailable_table_credential_issuer_reports_fallback_scope() {
    let issuer = UnavailableTableCredentialIssuer;
    let response = load_credentials_response_from_entry(&table_entry_for_credentials(), &issuer, None)
        .await
        .expect("unavailable issuer should build a fallback response");

    assert!(response.storage_credentials.is_empty());
    assert_eq!(
        response.config.get(CREDENTIAL_VENDING_CONFIG_KEY),
        Some(&CREDENTIAL_VENDING_UNSUPPORTED.to_string())
    );
    assert_eq!(
        response.config.get(CREDENTIAL_MODE_CONFIG_KEY),
        Some(&CREDENTIAL_MODE_CLIENT_PROVIDED.to_string())
    );
    assert_eq!(
        response.config.get(CREDENTIAL_VENDING_REASON_CONFIG_KEY),
        Some(&CREDENTIAL_VENDING_UNSUPPORTED_REASON.to_string())
    );
    assert_eq!(
        response.config.get(CREDENTIAL_SCOPE_CONFIG_KEY),
        Some(&CREDENTIAL_SCOPE_TABLE_PREFIX.to_string())
    );
    assert_eq!(
        response.config.get(CREDENTIAL_SCOPE_PREFIX_CONFIG_KEY),
        Some(&"s3://warehouse/tables/table-id/".to_string())
    );
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

    assert_eq!(
        response.config.get(CREDENTIAL_VENDING_CONFIG_KEY),
        Some(&CREDENTIAL_VENDING_SUPPORTED.to_string())
    );
    assert_eq!(
        response.config.get(CREDENTIAL_MODE_CONFIG_KEY),
        Some(&CREDENTIAL_MODE_CATALOG_VENDED.to_string())
    );
    assert!(!response.config.contains_key(S3_ACCESS_KEY_ID_CONFIG_KEY));
    assert!(!response.config.contains_key(S3_SECRET_ACCESS_KEY_CONFIG_KEY));
    assert!(!response.config.contains_key(S3_SESSION_TOKEN_CONFIG_KEY));
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

#[tokio::test]
async fn credential_response_serializes_sensitive_config_only_inside_storage_credentials() {
    let issuer = TestTableCredentialIssuer;
    let response = load_credentials_response_from_entry(&table_entry_for_credentials(), &issuer, None)
        .await
        .expect("issuer should build a scoped credential response");

    let value = serde_json::to_value(&response).expect("credential response should serialize");

    assert_eq!(
        value["config"][CREDENTIAL_VENDING_CONFIG_KEY],
        serde_json::Value::String(CREDENTIAL_VENDING_SUPPORTED.to_string())
    );
    assert!(value["config"].get(S3_ACCESS_KEY_ID_CONFIG_KEY).is_none());
    assert!(value["config"].get(S3_SECRET_ACCESS_KEY_CONFIG_KEY).is_none());
    assert!(value["config"].get(S3_SESSION_TOKEN_CONFIG_KEY).is_none());
    assert_eq!(
        value["storage-credentials"][0]["config"][S3_ACCESS_KEY_ID_CONFIG_KEY],
        serde_json::Value::String("temporary-access-key".to_string())
    );
}

#[test]
fn credential_http_response_disables_caching() {
    let response = build_sensitive_json_response(StatusCode::OK, &serde_json::json!({"storage-credentials": []}))
        .expect("sensitive response should build");

    assert_eq!(
        response.headers.get(http::header::CACHE_CONTROL),
        Some(&HeaderValue::from_static("no-store, private"))
    );
    assert_eq!(response.headers.get(http::header::PRAGMA), Some(&HeaderValue::from_static("no-cache")));
    assert_eq!(response.headers.get(http::header::EXPIRES), Some(&HeaderValue::from_static("0")));
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
    let policy = table_credential_session_policy(&table_entry_for_credentials(), "tables/table-id/")
        .expect("table credential policy should build");
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
        policy
            .is_allowed(&rustfs_policy::policy::Args {
                account: "temporary-access-key",
                groups: &groups,
                action: Action::AdminAction(rustfs_policy::policy::action::AdminAction::SetTableMetadataAction),
                bucket: "warehouse",
                conditions: &conditions,
                is_owner: false,
                object: "namespaces/analytics/tables/events",
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

#[tokio::test]
async fn table_credential_session_policy_includes_table_resource_actions() {
    let policy = table_credential_session_policy(&table_entry_for_credentials(), "tables/table-id/")
        .expect("table credential policy should build");
    let groups = None;
    let conditions = std::collections::HashMap::new();
    let claims = std::collections::HashMap::new();

    assert!(
        policy
            .is_allowed(&rustfs_policy::policy::Args {
                account: "temporary-access-key",
                groups: &groups,
                action: Action::AdminAction(rustfs_policy::policy::action::AdminAction::GetTableMetadataAction),
                bucket: "warehouse",
                conditions: &conditions,
                is_owner: false,
                object: "namespaces/analytics/tables/events",
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
                action: Action::AdminAction(rustfs_policy::policy::action::AdminAction::SetTableMetadataAction),
                bucket: "warehouse",
                conditions: &conditions,
                is_owner: false,
                object: "namespaces/analytics/tables/orders",
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
                action: Action::AdminAction(rustfs_policy::policy::action::AdminAction::CreateTableAction),
                bucket: "warehouse",
                conditions: &conditions,
                is_owner: false,
                object: "namespaces/analytics/tables/events",
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
                    "type": "assert-ref-snapshot-id",
                    "ref": "main",
                    "snapshot-id": 10
                }
            ],
            "updates": [],
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

#[test]
fn rest_commit_item_counts_are_bounded_before_processing() {
    let allowed = vec![serde_json::Value::Null; TABLE_CATALOG_COMMIT_REQUIREMENT_MAX_COUNT];
    validate_rest_commit_item_counts(&allowed, &[]).expect("the documented requirement limit should be accepted");
    let too_many_requirements = vec![serde_json::Value::Null; TABLE_CATALOG_COMMIT_REQUIREMENT_MAX_COUNT + 1];
    assert_eq!(
        validate_rest_commit_item_counts(&too_many_requirements, &[])
            .expect_err("excess requirements must be rejected")
            .code(),
        &S3ErrorCode::InvalidRequest
    );
    let too_many_updates = vec![serde_json::Value::Null; TABLE_CATALOG_COMMIT_UPDATE_MAX_COUNT + 1];
    assert_eq!(
        validate_rest_commit_item_counts(&[], &too_many_updates)
            .expect_err("excess updates must be rejected")
            .code(),
        &S3ErrorCode::InvalidRequest
    );
}

fn trusted_table_commit_backend(
    backend: &TestTableCatalogObjectBackend,
) -> TableCommitObjectBackend<TestTableCatalogObjectBackend> {
    TableCommitObjectBackend::trusted(backend.clone())
}

fn test_snapshot_object_key(bucket: &str, location: &str) -> String {
    crate::table_catalog::table_catalog_object_key_from_location(bucket, location)
        .expect("test snapshot object location should be valid")
}

fn test_parquet_i32_bytes(values: &[i32]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(Arc::clone(&schema) as SchemaRef, vec![Arc::new(Int32Array::from(values.to_vec()))])
        .expect("parquet test batch should build");
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).expect("parquet writer should build");
        writer.write(&batch).expect("parquet batch should write");
        writer.close().expect("parquet writer should close");
    }
    bytes
}

async fn seed_test_manifest_list(
    backend: &TestTableCatalogObjectBackend,
    bucket: &str,
    manifest_list_location: &str,
    manifest_locations: &[&str],
    sequence_number: i64,
    snapshot_id: i64,
) {
    let manifest_list_key = test_snapshot_object_key(bucket, manifest_list_location);
    let mut manifests = Vec::with_capacity(manifest_locations.len());
    for manifest_location in manifest_locations {
        let manifest_key = test_snapshot_object_key(bucket, manifest_location);
        let manifest_length = backend
            .state
            .lock()
            .await
            .objects
            .get(&(bucket.to_string(), manifest_key))
            .map(|object| object.data.len())
            .expect("test manifest must be seeded before its manifest list");
        manifests.push((*manifest_location, manifest_length));
    }
    backend
        .put_bytes(
            bucket,
            &manifest_list_key,
            test_manifest_list_avro_bytes(&manifests, sequence_number, snapshot_id),
        )
        .await;
}

async fn seed_test_manifest_list_entries(
    backend: &TestTableCatalogObjectBackend,
    bucket: &str,
    manifest_list_location: &str,
    manifest_entries: &[(&str, i64, i64)],
) {
    let manifest_list_key = test_snapshot_object_key(bucket, manifest_list_location);
    let mut manifests = Vec::with_capacity(manifest_entries.len());
    for (manifest_location, sequence_number, snapshot_id) in manifest_entries {
        let manifest_key = test_snapshot_object_key(bucket, manifest_location);
        let manifest_length = backend
            .state
            .lock()
            .await
            .objects
            .get(&(bucket.to_string(), manifest_key))
            .map(|object| object.data.len())
            .expect("test manifest must be seeded before its manifest list");
        manifests.push((*manifest_location, manifest_length, *sequence_number, *snapshot_id));
    }
    backend
        .put_bytes(bucket, &manifest_list_key, test_manifest_list_avro_entries(&manifests))
        .await;
}

async fn seed_test_snapshot_manifest(
    backend: &TestTableCatalogObjectBackend,
    bucket: &str,
    manifest_list_location: &str,
    snapshot_id: i64,
    sequence_number: i64,
    files: &[(&str, i32, i32, i64, i64)],
) {
    let manifest_location = manifest_list_location
        .rsplit_once('/')
        .map(|(prefix, name)| format!("{prefix}/manifest-{name}"))
        .expect("manifest list location should include a file name");
    let manifest_key = test_snapshot_object_key(bucket, &manifest_location);
    let manifest_list_key = test_snapshot_object_key(bucket, manifest_list_location);
    let manifest_bytes = test_manifest_avro_bytes(files);
    backend
        .put_bytes(
            bucket,
            &manifest_list_key,
            test_manifest_list_avro_bytes(&[(&manifest_location, manifest_bytes.len())], sequence_number, snapshot_id),
        )
        .await;
    backend.put_bytes(bucket, &manifest_key, manifest_bytes).await;
    seed_test_manifest_data_files(backend, bucket, files).await;
}

async fn seed_test_manifest_with_nullable_sequences(
    backend: &TestTableCatalogObjectBackend,
    bucket: &str,
    manifest_location: &str,
    files: &[(&str, i32, i32, i64, Option<i64>)],
) {
    let manifest_key = test_snapshot_object_key(bucket, manifest_location);
    backend
        .put_bytes(bucket, &manifest_key, test_manifest_avro_bytes_with_nullable_sequences(files))
        .await;
    let data_files = files
        .iter()
        .map(|(file_path, content, status, snapshot_id, sequence_number)| {
            (*file_path, *content, *status, *snapshot_id, sequence_number.unwrap_or_default())
        })
        .collect::<Vec<_>>();
    seed_test_manifest_data_files(backend, bucket, &data_files).await;
}

async fn seed_test_manifest(
    backend: &TestTableCatalogObjectBackend,
    bucket: &str,
    manifest_location: &str,
    files: &[(&str, i32, i32, i64, i64)],
) {
    let manifest_key = test_snapshot_object_key(bucket, manifest_location);
    backend
        .put_bytes(bucket, &manifest_key, test_manifest_avro_bytes(files))
        .await;
    seed_test_manifest_data_files(backend, bucket, files).await;
}

async fn seed_test_manifest_data_files(
    backend: &TestTableCatalogObjectBackend,
    bucket: &str,
    files: &[(&str, i32, i32, i64, i64)],
) {
    for (file_path, _, status, _, _) in files {
        if *status != 2 {
            let object_key = test_snapshot_object_key(bucket, file_path);
            backend.put_bytes(bucket, &object_key, b"data".to_vec()).await;
        }
    }
}

async fn create_standard_events_table<S>(
    store: &S,
    metadata_backend: &TestTableCatalogObjectBackend,
    namespace: &crate::table_catalog::Namespace,
) -> RestLoadTableResponse
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
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
    let commit_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    create_table_response(store, &commit_backend, "warehouse", namespace, create_request, true)
        .await
        .expect("table should be created")
}

async fn create_standard_recent_events_view<S>(
    store: &S,
    metadata_backend: &TestTableCatalogObjectBackend,
    namespace: &crate::table_catalog::Namespace,
) -> RestLoadViewResponse
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    ensure_table_bucket_entry(store, "warehouse", true)
        .await
        .expect("table bucket entry should be seeded");
    if store
        .get_namespace("warehouse", &namespace.public_name())
        .await
        .expect("namespace lookup should succeed")
        .is_none()
    {
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
    }
    let request: CreateViewRequest = serde_json::from_value(serde_json::json!({
        "name": "recent_events",
        "schema": {"type": "struct", "fields": []},
        "view-version": {
            "version-id": 1,
            "timestamp-ms": 1,
            "schema-id": 0,
            "summary": {},
            "default-namespace": ["analytics"],
            "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]
        },
        "properties": {}
    }))
    .expect("standard create view request should parse");
    let publication_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    create_view_response(store, &publication_backend, "warehouse", namespace, request, true)
        .await
        .expect("view should be created")
}

fn standard_property_commit_request(commit_id: &str, table_uuid: &str, owner: &str) -> RestCommitTableRequest {
    serde_json::from_value(serde_json::json!({
        "commit-id": commit_id,
        "requirements": [{"type": "assert-table-uuid", "uuid": table_uuid}],
        "updates": [{"action": "set-properties", "updates": {"owner": owner}}]
    }))
    .expect("property commit request should parse")
}

async fn standard_commit_foreign_primary_fixture() -> (
    TestTableCatalogStore,
    TestTableCatalogObjectBackend,
    crate::table_catalog::Namespace,
    crate::table_catalog::TableEntry,
    serde_json::Value,
    RestCommitTableRequest,
    String,
) {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let current = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let current_metadata = read_table_metadata_json(&metadata_backend, "warehouse", &current.metadata_location)
        .await
        .expect("current metadata should load");
    let commit_id = "11111111-1111-4111-8111-111111111111";
    let request = standard_property_commit_request(commit_id, &current.table_uuid, "target");
    let primary_location =
        crate::table_catalog::table_metadata_file_path_for_entry(&current, &next_metadata_file_name(2, commit_id))
            .expect("primary metadata path should be valid");
    let mut foreign_metadata = current_metadata.clone();
    foreign_metadata["table-uuid"] = serde_json::Value::String(Uuid::new_v4().to_string());
    metadata_backend
        .put_json("warehouse", &primary_location, foreign_metadata)
        .await;
    let fallback_location = crate::table_catalog::table_metadata_file_path_for_entry(
        &current,
        &table_scoped_metadata_file_name(2, &current.table_id, commit_id),
    )
    .expect("fallback metadata path should be valid");
    (store, metadata_backend, namespace, current, current_metadata, request, fallback_location)
}

async fn standard_commit_primary_fixture(
    persisted_owner: &str,
    requested_owner: &str,
) -> (
    TestTableCatalogStore,
    TestTableCatalogObjectBackend,
    crate::table_catalog::Namespace,
    crate::table_catalog::TableEntry,
    RestCommitTableRequest,
    String,
    String,
) {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let current = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let current_metadata = read_table_metadata_json(&metadata_backend, "warehouse", &current.metadata_location)
        .await
        .expect("current metadata should load");
    let commit_id = "11111111-1111-4111-8111-111111111111";
    let persisted_request = standard_property_commit_request(commit_id, &current.table_uuid, persisted_owner);
    let requested = standard_property_commit_request(commit_id, &current.table_uuid, requested_owner);
    let previous_metadata_location = table_metadata_location_for_client("warehouse", &current.metadata_location);
    let timestamp_ms = current_metadata["last-updated-ms"]
        .as_i64()
        .expect("current metadata should include last-updated-ms")
        .saturating_add(1);
    let persisted_metadata =
        apply_table_commit_updates_at(current_metadata, &persisted_request.updates, &previous_metadata_location, timestamp_ms)
            .expect("persisted metadata should build");
    let primary_location =
        crate::table_catalog::table_metadata_file_path_for_entry(&current, &next_metadata_file_name(2, commit_id))
            .expect("primary metadata path should be valid");
    metadata_backend
        .put_json("warehouse", &primary_location, persisted_metadata)
        .await;
    let fallback_location = crate::table_catalog::table_metadata_file_path_for_entry(
        &current,
        &table_scoped_metadata_file_name(2, &current.table_id, commit_id),
    )
    .expect("fallback metadata path should be valid");
    (
        store,
        metadata_backend,
        namespace,
        current,
        requested,
        primary_location,
        fallback_location,
    )
}

async fn assert_events_table_entry_unchanged(store: &TestTableCatalogStore, expected: &crate::table_catalog::TableEntry) {
    let actual = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    assert_eq!(actual.metadata_location, expected.metadata_location);
    assert_eq!(actual.generation, expected.generation);
    assert_eq!(actual.version_token, expected.version_token);
}

async fn seed_events_registration_target<S>(
    store: &S,
    metadata_backend: &TestTableCatalogObjectBackend,
) -> (crate::table_catalog::Namespace, String)
where
    S: crate::table_catalog::TableCatalogStore + ?Sized,
{
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    ensure_table_bucket_entry(store, "warehouse", true)
        .await
        .expect("table bucket entry should be seeded");
    create_namespace_response(
        store,
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
        ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json".to_string();
    metadata_backend
        .put_json(
            "warehouse",
            &metadata_location,
            test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    (namespace, metadata_location)
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
async fn ensure_table_bucket_entry_propagates_catalog_entry_failure() {
    let store = TestTableCatalogStore::default();
    *store.fail_put_table_bucket.lock().await = true;

    assert!(ensure_table_bucket_entry(&store, "warehouse", true).await.is_err());
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

    let unpaginated_uri = "/".parse::<http::Uri>().expect("list URI should parse");
    let list = list_namespaces_response(&store, "warehouse", None, &unpaginated_uri)
        .await
        .expect("namespace list should load");
    assert_eq!(list.namespaces, vec![vec!["analytics".to_string()]]);

    let update = update_namespace_properties_response(
        &store,
        "warehouse",
        &crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse"),
        UpdateNamespacePropertiesRequest {
            removals: vec!["owner".to_string(), "missing".to_string()],
            updates: BTreeMap::from([("retention".to_string(), "30d".to_string())]),
        },
    )
    .await
    .expect("namespace properties should update");
    assert_eq!(update.updated, vec!["retention".to_string()]);
    assert_eq!(update.removed, vec!["owner".to_string()]);
    assert_eq!(update.missing, vec!["missing".to_string()]);
    let updated = get_namespace_response(
        &store,
        "warehouse",
        &crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse"),
    )
    .await
    .expect("updated namespace should load");
    assert_eq!(updated.properties.get("retention").map(String::as_str), Some("30d"));
    assert!(!updated.properties.contains_key("owner"));

    drop_namespace_in_store(&store, "warehouse", "analytics")
        .await
        .expect("namespace should drop");
    let list = list_namespaces_response(&store, "warehouse", None, &unpaginated_uri)
        .await
        .expect("namespace list should load after drop");
    assert!(list.namespaces.is_empty());
}

#[tokio::test]
async fn table_helpers_call_catalog_store() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
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

    let metadata_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
    metadata_backend
        .put_json(
            "warehouse",
            metadata_location,
            test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    let register = register_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
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

    let client_metadata_location = table_metadata_location_for_client("warehouse", metadata_location);
    assert_eq!(register.metadata_location, client_metadata_location);
    assert_eq!(register.metadata["format-version"], 2);

    let unpaginated_uri = "/".parse::<http::Uri>().expect("list URI should parse");
    let list = list_tables_response(&store, "warehouse", &namespace, &unpaginated_uri)
        .await
        .expect("table list should load");
    assert_eq!(list.identifiers[0].name, "events");

    let load = load_table_response(&store, &metadata_backend, "warehouse", &namespace, "events")
        .await
        .expect("table should load");
    assert_eq!(load.metadata_location, client_metadata_location);
    assert_eq!(load.metadata["table-uuid"], "table-uuid");

    let current = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let next_metadata_location =
        ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
    let mut next_metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
    next_metadata["last-sequence-number"] = serde_json::Value::from(2);
    metadata_backend
        .put_json("warehouse", next_metadata_location, next_metadata)
        .await;
    let client_requirements = vec![serde_json::json!({
        "type": "assert-table-uuid",
        "uuid": "table-uuid"
    })];
    let commit = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        RestCommitTableRequest {
            commit_id: Some("commit-1".to_string()),
            idempotency_key: Some("retry-1".to_string()),
            operation: Some("append".to_string()),
            expected_version_token: Some(current.version_token.clone()),
            expected_metadata_location: Some(table_metadata_location_for_client("warehouse", &current.metadata_location)),
            new_metadata_location: Some(table_metadata_location_for_client("warehouse", next_metadata_location)),
            requirements: client_requirements.clone(),
            updates: Vec::new(),
            identifier: None,
            writer: Some("pyiceberg".to_string()),
        },
    )
    .await
    .expect("table commit should succeed");
    assert_eq!(
        commit.metadata_location,
        table_metadata_location_for_client("warehouse", next_metadata_location)
    );
    assert_eq!(commit.version_token, "token-committed");
    assert_eq!(commit.generation, current.generation + 1);
    assert_eq!(commit.commit_id, "commit-1");
    assert_eq!(
        store
            .commits
            .lock()
            .await
            .last()
            .expect("commit log should be recorded")
            .requirements,
        client_requirements,
        "persisted requirements must remain compatible with clients that do not know RustFS-private requirements"
    );

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
async fn load_table_rejects_invalid_persisted_metadata() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::default();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let metadata_key = table_metadata_location_for_catalog("warehouse", &created.metadata_location)
        .expect("created metadata location should normalize");
    let mut invalid_metadata = created.metadata;
    invalid_metadata["location"] = serde_json::Value::from("s3://other-warehouse/tables/table-id");
    metadata_backend.put_json("warehouse", &metadata_key, invalid_metadata).await;

    let error = load_table_response(&store, &metadata_backend, "warehouse", &namespace, "events")
        .await
        .expect_err("load table must reject persisted metadata outside the table bucket");
    assert_eq!(error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
    assert_eq!(error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
}

#[test]
fn table_format_upgrade_accepts_historical_v1_metadata_and_rejects_downgrade() {
    let entry = crate::table_catalog::TableEntry {
        version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
        table_bucket: "warehouse".to_string(),
        namespace: "analytics".to_string(),
        table: "events".to_string(),
        table_id: "table-id".to_string(),
        table_uuid: "table-uuid".to_string(),
        format: "ICEBERG".to_string(),
        format_version: 2,
        warehouse_location: "s3://warehouse/tables/table-id".to_string(),
        metadata_location: "tables/table-id/metadata/00002.metadata.json".to_string(),
        version_token: "token-v2".to_string(),
        generation: 2,
        state: crate::table_catalog::TableCatalogEntryState::Active,
        properties: BTreeMap::new(),
        created_at: None,
        updated_at: None,
    };
    let historical_v1 = serde_json::json!({
        "format-version": 1,
        "table-uuid": "table-uuid",
        "location": "s3://warehouse/tables/table-id",
        "last-updated-ms": 1,
        "last-column-id": 1,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        },
        "partition-spec": [],
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": []
    });
    let current_v2 = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");

    validate_persisted_table_metadata(&entry, &historical_v1, false)
        .expect("historical v1 metadata should remain readable after a v2 upgrade");
    validate_persisted_table_metadata(&entry, &historical_v1, true)
        .expect_err("the current pointer must match the catalog format version");
    let mut legacy_entry = entry;
    legacy_entry.format_version = 1;
    validate_persisted_table_metadata(&legacy_entry, &current_v2, true)
        .expect("a current v2 metadata file committed before format persistence must remain readable");
    validate_persisted_table_metadata(&legacy_entry, &current_v2, false)
        .expect("post-upgrade v2 metadata must remain readable as a historical commit base");
    validate_metadata_identity_matches_current_metadata(&historical_v1, &current_v2).expect("a v1 table may upgrade to v2");
    validate_metadata_identity_matches_current_metadata(&current_v2, &historical_v1)
        .expect_err("a v2 table must not downgrade to v1");
}

#[tokio::test]
async fn load_responses_reject_persisted_metadata_for_another_catalog_identity() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::default();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let table = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_entry = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let mut foreign_table_metadata = table.metadata;
    foreign_table_metadata["table-uuid"] = serde_json::Value::from("foreign-table-uuid");
    metadata_backend
        .put_json("warehouse", &table_entry.metadata_location, foreign_table_metadata)
        .await;

    let table_error = load_table_response(&store, &metadata_backend, "warehouse", &namespace, "events")
        .await
        .expect_err("load table must bind persisted metadata to the catalog identity");
    assert_eq!(table_error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
    assert_eq!(table_error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));

    let view = create_standard_recent_events_view(&store, &metadata_backend, &namespace).await;
    let view_entry = store
        .load_view("warehouse", "analytics", "recent_events")
        .await
        .expect("view lookup should succeed")
        .expect("view should exist");
    let mut foreign_view_metadata = view.metadata;
    foreign_view_metadata["view-uuid"] = serde_json::Value::from("foreign-view-uuid");
    metadata_backend
        .put_json("warehouse", &view_entry.metadata_location, foreign_view_metadata)
        .await;

    let view_error = load_view_response(&store, &metadata_backend, "warehouse", &namespace, "recent_events")
        .await
        .expect_err("load view must bind persisted metadata to the catalog identity");
    assert_eq!(view_error.code(), &S3ErrorCode::Custom(ICEBERG_ERROR_REST.into()));
    assert_eq!(view_error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
}

#[tokio::test]
async fn register_table_response_adopts_metadata_table_uuid() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
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
    let metadata_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
    metadata_backend
        .put_json(
            "warehouse",
            metadata_location,
            test_table_metadata_json("metadata-table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;

    register_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
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
async fn register_table_denies_metadata_read_before_catalog_publication() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let (namespace, metadata_location) = seed_events_registration_target(&store, &metadata_backend).await;
    let authorized = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let guarded_backend =
        TableCommitObjectBackend::test(metadata_backend, Arc::clone(&authorized), Some(metadata_location.clone()));

    let result = register_table_response(
        &store,
        &guarded_backend,
        "warehouse",
        &namespace,
        RegisterTableRequest {
            name: "events".to_string(),
            metadata_location: metadata_location.clone(),
            overwrite: false,
        },
        true,
    )
    .await;
    let error = guarded_backend
        .finish(result)
        .await
        .expect_err("denied registration metadata read must fail before catalog publication");

    assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
    assert!(
        authorized
            .lock()
            .await
            .contains(&(metadata_location, S3Action::GetObjectAction))
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
async fn catalog_import_denies_metadata_read_before_catalog_publication() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let (namespace, metadata_location) = seed_events_registration_target(&store, &metadata_backend).await;
    let authorized = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let guarded_backend =
        TableCommitObjectBackend::test(metadata_backend, Arc::clone(&authorized), Some(metadata_location.clone()));

    let result = catalog_import_response(
        &store,
        &guarded_backend,
        "warehouse",
        &namespace,
        "events",
        CatalogImportRequest {
            metadata_location: metadata_location.clone(),
            properties: BTreeMap::new(),
        },
        true,
    )
    .await;
    let error = guarded_backend
        .finish(result)
        .await
        .expect_err("denied import metadata read must fail before catalog publication");

    assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
    assert!(
        authorized
            .lock()
            .await
            .contains(&(metadata_location, S3Action::GetObjectAction))
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
async fn register_table_rejects_metadata_replaced_before_catalog_publication() {
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let store = Arc::new(crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone()));
    let (namespace, metadata_location) = seed_events_registration_target(store.as_ref(), &metadata_backend).await;
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let table_path = crate::table_catalog::TableCatalogObjectPaths::default().table_entry_path("warehouse", &namespace, &table);
    let table_guard = crate::table_catalog::TableCatalogObjectBackend::acquire_write_lock(
        &metadata_backend,
        crate::admin::storage_api::RUSTFS_META_BUCKET,
        &table_path,
    )
    .await
    .expect("catalog publication gate should acquire its lock");
    metadata_backend.lock_attempts.lock().await.clear();
    let guarded_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let register_store = Arc::clone(&store);
    let register_backend = guarded_backend.clone();
    let register_namespace = namespace.clone();
    let register_location = metadata_location.clone();
    let registration = tokio::spawn(async move {
        let result = register_table_response(
            register_store.as_ref(),
            &register_backend,
            "warehouse",
            &register_namespace,
            RegisterTableRequest {
                name: "events".to_string(),
                metadata_location: register_location,
                overwrite: false,
            },
            true,
        )
        .await;
        register_backend.finish(result).await
    });
    metadata_backend.wait_for_lock_attempts(3).await;
    let writer_guard = tokio::time::timeout(
        StdDuration::from_secs(2),
        crate::table_catalog::TableCatalogObjectBackend::acquire_write_lock(&metadata_backend, "warehouse", &metadata_location),
    )
    .await
    .expect("metadata validation must release its read lock before catalog publication")
    .expect("concurrent metadata writer should acquire its lock");
    metadata_backend
        .put_json(
            "warehouse",
            &metadata_location,
            test_table_metadata_json("replacement-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    drop(writer_guard);
    drop(table_guard);

    tokio::time::timeout(StdDuration::from_secs(2), registration)
        .await
        .expect("registration should finish after the writer releases its lock")
        .expect("registration task should join")
        .expect_err("replaced metadata must not be registered");
    assert!(
        store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn catalog_import_rejects_metadata_replaced_before_catalog_publication() {
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let store = Arc::new(crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone()));
    let (namespace, metadata_location) = seed_events_registration_target(store.as_ref(), &metadata_backend).await;
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let table_path = crate::table_catalog::TableCatalogObjectPaths::default().table_entry_path("warehouse", &namespace, &table);
    let table_guard = crate::table_catalog::TableCatalogObjectBackend::acquire_write_lock(
        &metadata_backend,
        crate::admin::storage_api::RUSTFS_META_BUCKET,
        &table_path,
    )
    .await
    .expect("catalog publication gate should acquire its lock");
    metadata_backend.lock_attempts.lock().await.clear();
    let guarded_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    let import_store = Arc::clone(&store);
    let import_backend = guarded_backend.clone();
    let import_namespace = namespace.clone();
    let import_location = metadata_location.clone();
    let import = tokio::spawn(async move {
        let result = catalog_import_response(
            import_store.as_ref(),
            &import_backend,
            "warehouse",
            &import_namespace,
            "events",
            CatalogImportRequest {
                metadata_location: import_location,
                properties: BTreeMap::new(),
            },
            true,
        )
        .await;
        import_backend.finish(result).await
    });
    metadata_backend.wait_for_lock_attempts(3).await;
    let writer_guard = tokio::time::timeout(
        StdDuration::from_secs(2),
        crate::table_catalog::TableCatalogObjectBackend::acquire_write_lock(&metadata_backend, "warehouse", &metadata_location),
    )
    .await
    .expect("metadata validation must release its read lock before catalog publication")
    .expect("concurrent metadata writer should acquire its lock");
    metadata_backend
        .put_json(
            "warehouse",
            &metadata_location,
            test_table_metadata_json("replacement-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    drop(writer_guard);
    drop(table_guard);

    tokio::time::timeout(StdDuration::from_secs(2), import)
        .await
        .expect("import should finish after the writer releases its lock")
        .expect("import task should join")
        .expect_err("replaced metadata must not be imported");
    assert!(
        store
            .load_table("warehouse", "analytics", "events")
            .await
            .expect("table lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn register_table_response_rejects_metadata_without_format_version() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
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
    let metadata_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json";
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
            &trusted_table_commit_backend(&metadata_backend),
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
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
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
    let warehouse_location = entry.warehouse_location.clone();
    store.register_table(entry).await.expect("table should register");
    metadata_backend
        .put_json("warehouse", current_location, test_table_metadata_json(&table_uuid, &warehouse_location))
        .await;
    let current = get_table_metadata_location_response(&store, "warehouse", &namespace, "events")
        .await
        .expect("metadata location should load");
    assert_eq!(
        current.metadata_location,
        table_metadata_location_for_client("warehouse", current_location)
    );
    let next_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
    metadata_backend
        .put_json("warehouse", next_location, test_table_metadata_json(&table_uuid, &warehouse_location))
        .await;

    let updated = update_table_metadata_location_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        UpdateTableMetadataLocationRequest {
            metadata_location: table_metadata_location_for_client("warehouse", next_location),
            version_token: current.version_token.clone(),
            commit_id: Some("commit-1".to_string()),
            idempotency_key: Some("retry-1".to_string()),
        },
    )
    .await
    .expect("metadata location should update");

    assert_eq!(updated.metadata_location, table_metadata_location_for_client("warehouse", next_location));
    assert_eq!(updated.generation, current.generation + 1);
    assert_ne!(updated.version_token, current.version_token);
}

#[tokio::test]
async fn metadata_location_api_accepts_gzip_table_metadata() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let current = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let target_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json.gz");
    metadata_backend
        .put_gzip_json("warehouse", &target_location, created.metadata)
        .await;

    let updated = update_table_metadata_location_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        UpdateTableMetadataLocationRequest {
            metadata_location: table_metadata_location_for_client("warehouse", &target_location),
            version_token: current.version_token,
            commit_id: Some("gzip-metadata".to_string()),
            idempotency_key: None,
        },
    )
    .await
    .expect("gzip table metadata should commit");

    assert_eq!(
        updated.metadata_location,
        table_metadata_location_for_client("warehouse", &target_location)
    );
}

#[tokio::test]
async fn metadata_location_api_validates_snapshot_graph_before_commit() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let current = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let target_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002-graph.metadata.json");
    let manifest_list = format!("{}/metadata/snap-10.avro", current.warehouse_location);
    let data_file = format!("{}/data/part-10.parquet", current.warehouse_location);
    let mut target_metadata = created.metadata;
    target_metadata["last-sequence-number"] = serde_json::Value::from(1);
    target_metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 1,
        "timestamp-ms": 10,
        "manifest-list": manifest_list,
        "summary": {"operation": "append"}
    }]);
    target_metadata["current-snapshot-id"] = serde_json::Value::from(10);
    target_metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    metadata_backend
        .put_json("warehouse", &target_location, target_metadata)
        .await;
    let request = || UpdateTableMetadataLocationRequest {
        metadata_location: table_metadata_location_for_client("warehouse", &target_location),
        version_token: current.version_token.clone(),
        commit_id: Some("graph-commit".to_string()),
        idempotency_key: Some("graph-replay".to_string()),
    };

    let error = update_table_metadata_location_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request(),
    )
    .await
    .expect_err("missing manifest-list must fail before pointer publication");
    assert_eq!(error.message(), Some("snapshot manifest-list object is missing"));
    let unchanged = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should remain present");
    assert_eq!(unchanged.metadata_location, current.metadata_location);

    seed_test_snapshot_manifest(&metadata_backend, "warehouse", &manifest_list, 10, 1, &[(&data_file, 0, 1, 10, 1)]).await;
    update_table_metadata_location_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        request(),
    )
    .await
    .expect("complete snapshot graph should commit");
}

#[tokio::test]
async fn metadata_location_api_validates_relocated_snapshot_graph_under_target_warehouse() {
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(metadata_backend.clone());
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let current = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let target_location =
        crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002-relocated.metadata.json");
    let target_warehouse = "s3://warehouse/tables/relocated-table-id";
    let manifest_list = format!("{target_warehouse}/metadata/snap-10.avro");
    let data_file = format!("{target_warehouse}/data/part-10.parquet");
    let mut target_metadata = created.metadata;
    target_metadata["location"] = serde_json::Value::String(target_warehouse.to_string());
    target_metadata["last-sequence-number"] = serde_json::Value::from(1);
    target_metadata["snapshots"] = serde_json::json!([{
        "snapshot-id": 10,
        "sequence-number": 1,
        "timestamp-ms": 10,
        "manifest-list": manifest_list,
        "summary": {"operation": "append"}
    }]);
    target_metadata["current-snapshot-id"] = serde_json::Value::from(10);
    target_metadata["refs"] = serde_json::json!({"main": {"type": "branch", "snapshot-id": 10}});
    metadata_backend
        .put_json("warehouse", &target_location, target_metadata)
        .await;
    seed_test_snapshot_manifest(&metadata_backend, "warehouse", &manifest_list, 10, 1, &[(&data_file, 0, 1, 10, 1)]).await;

    update_table_metadata_location_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        UpdateTableMetadataLocationRequest {
            metadata_location: table_metadata_location_for_client("warehouse", &target_location),
            version_token: current.version_token,
            commit_id: Some("relocate-graph".to_string()),
            idempotency_key: None,
        },
    )
    .await
    .expect("snapshot objects under the target warehouse should validate");

    let relocated = store
        .load_table("warehouse", "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    assert_eq!(relocated.warehouse_location, target_warehouse);
}

#[tokio::test]
async fn metadata_location_api_rejects_invalid_target_metadata_before_commit() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
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
            test_table_metadata_json(&table_uuid, "s3://warehouse/tables/table-id"),
        )
        .await;
    let current = get_table_metadata_location_response(&store, "warehouse", &namespace, "events")
        .await
        .expect("metadata location should load");
    let invalid_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
    metadata_backend
        .put_json(
            "warehouse",
            invalid_location,
            test_table_metadata_json(&table_uuid, "s3://other-warehouse/tables/table-id"),
        )
        .await;

    assert!(
        update_table_metadata_location_response(
            &store,
            &trusted_table_commit_backend(&metadata_backend),
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
    assert_eq!(
        unchanged.metadata_location,
        table_metadata_location_for_client("warehouse", current_location)
    );
    assert_eq!(unchanged.generation, current.generation);
}

#[tokio::test]
async fn metadata_location_api_rejects_mismatched_table_uuid_before_commit() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
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
            test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    register_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
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
    let mismatched_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
    metadata_backend
        .put_json(
            "warehouse",
            mismatched_location,
            test_table_metadata_json("other-table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;

    assert!(
        update_table_metadata_location_response(
            &store,
            &trusted_table_commit_backend(&metadata_backend),
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
    assert_eq!(
        unchanged.metadata_location,
        table_metadata_location_for_client("warehouse", current_location)
    );
    assert_eq!(unchanged.generation, current.generation);
}

#[tokio::test]
async fn catalog_import_and_rollback_use_register_and_commit_paths() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
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
            test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;

    let imported = catalog_import_response(
        &store,
        &trusted_table_commit_backend(&backend),
        bucket,
        &namespace,
        "events",
        CatalogImportRequest {
            metadata_location: table_metadata_location_for_client(bucket, &imported_location),
            properties: BTreeMap::from([("owner".to_string(), "lakehouse".to_string())]),
        },
        true,
    )
    .await
    .expect("catalog import should register table");
    assert_eq!(imported.metadata_location, table_metadata_location_for_client(bucket, &imported_location));
    let current = store
        .load_table(bucket, "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should exist");
    assert_eq!(current.properties.get("owner").map(String::as_str), Some("lakehouse"));

    let imported_again = catalog_import_response(
        &store,
        &trusted_table_commit_backend(&backend),
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
    .expect("repeated catalog import should be idempotent");
    assert_eq!(
        imported_again.metadata_location,
        table_metadata_location_for_client(bucket, &imported_location)
    );

    let rollback_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    let mut rollback_metadata = test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id");
    rollback_metadata["last-sequence-number"] = serde_json::Value::from(2);
    backend.put_json(bucket, &rollback_location, rollback_metadata).await;
    let rollback = rollback_table_response(
        &store,
        &trusted_table_commit_backend(&backend),
        bucket,
        &namespace,
        "events",
        RollbackTableRequest {
            metadata_location: table_metadata_location_for_client(bucket, &rollback_location),
            version_token: current.version_token,
            commit_id: Some("rollback-1".to_string()),
            idempotency_key: None,
        },
    )
    .await
    .expect("rollback should commit selected metadata");

    assert_eq!(rollback.metadata_location, table_metadata_location_for_client(bucket, &rollback_location));
    assert_eq!(rollback.commit_id, "rollback-1");
}

#[tokio::test]
async fn rollback_denies_metadata_reads_before_pointer_publish() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
    let bucket = "warehouse";
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let current_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    let rollback_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current_location.clone()).await;
    backend
        .put_json(
            bucket,
            &current_location,
            test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    backend
        .put_json(
            bucket,
            &rollback_location,
            test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    let authorized = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let commit_backend = TableCommitObjectBackend::test(backend, Arc::clone(&authorized), Some(rollback_location.clone()));

    let result = rollback_table_response(
        &store,
        &commit_backend,
        bucket,
        &namespace,
        "events",
        RollbackTableRequest {
            metadata_location: rollback_location.clone(),
            version_token: "token-v1".to_string(),
            commit_id: Some("rollback-denied".to_string()),
            idempotency_key: None,
        },
    )
    .await;
    let error = commit_backend
        .finish(result)
        .await
        .expect_err("denied rollback metadata read must fail before pointer publication");

    assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
    assert!(
        authorized
            .lock()
            .await
            .contains(&(rollback_location, S3Action::GetObjectAction))
    );
    let current = store
        .load_table(bucket, "analytics", "events")
        .await
        .expect("table lookup should succeed")
        .expect("table should remain");
    assert_eq!(current.metadata_location, current_location);
    assert_eq!(current.version_token, "token-v1");
}

#[tokio::test]
async fn rollback_rejects_invalid_target_metadata_before_commit() {
    let backend = TestTableCatalogObjectBackend::content_addressed();
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
            test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    catalog_import_response(
        &store,
        &trusted_table_commit_backend(&backend),
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
            &trusted_table_commit_backend(&backend),
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
    let backend = TestTableCatalogObjectBackend::content_addressed();
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
            test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    catalog_import_response(
        &store,
        &trusted_table_commit_backend(&backend),
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

    let mismatched_location = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00002.metadata.json");
    backend
        .put_json(
            bucket,
            &mismatched_location,
            test_table_metadata_json("other-table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;

    assert!(
        rollback_table_response(
            &store,
            &trusted_table_commit_backend(&backend),
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
    let metadata_backend = TestTableCatalogObjectBackend::content_addressed();
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
            test_table_metadata_json("table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    register_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
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
    let mismatched_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
    metadata_backend
        .put_json(
            "warehouse",
            mismatched_location,
            test_table_metadata_json("other-table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;

    assert!(
        commit_table_response(
            &store,
            &trusted_table_commit_backend(&metadata_backend),
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
                identifier: None,
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
