use super::*;
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
    assert_eq!(response.admin_discovery.runtime_capabilities, "/rustfs/admin/v4/runtime/capabilities");
    assert_eq!(response.admin_discovery.cluster_snapshot, "/rustfs/admin/v4/cluster/snapshot");
    assert_eq!(response.admin_discovery.extensions_catalog, "/rustfs/admin/v4/extensions/catalog");
    assert!(response.endpoints.contains(&"GET /v1/{prefix}/namespaces"));
    assert!(response.endpoints.contains(&"GET /{warehouse}/catalog/migration"));
    assert!(response.endpoints.contains(&"POST /{warehouse}/catalog/migration"));
    assert!(response.endpoints.contains(&"DELETE /{warehouse}/catalog/migration"));
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
    assert!(response.endpoints.contains(&"GET /{warehouse}/namespaces/{namespace}/views"));
    assert!(response.endpoints.contains(&"POST /{warehouse}/namespaces/{namespace}/views"));
    assert!(
        response
            .endpoints
            .contains(&"HEAD /{warehouse}/namespaces/{namespace}/views/{view}")
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
    assert!(
        response
            .endpoints
            .contains(&"GET /{warehouse}/namespaces/{namespace}/views/{view}")
    );
    assert!(
        response
            .endpoints
            .contains(&"GET /{warehouse}/namespaces/{namespace}/tables/{table}/refs")
    );
    assert!(
        response
            .endpoints
            .contains(&"PUT /{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}")
    );
    assert!(
        response
            .endpoints
            .contains(&"DELETE /{warehouse}/namespaces/{namespace}/tables/{table}/refs/{ref}")
    );
    assert!(
        response
            .endpoints
            .contains(&"GET /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external")
    );
    assert!(
        response
            .endpoints
            .contains(&"PUT /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external")
    );
    assert!(
        response
            .endpoints
            .contains(&"POST /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/external/sync")
    );
    assert!(
        response
            .endpoints
            .contains(&"POST /{warehouse}/namespaces/{namespace}/tables/{table}/catalog/recovery")
    );
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
        src.contains("validate_admin_request_with_bucket_object("),
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
            block.contains("ensure_table_bucket_enabled(&warehouse).await?;")
                || block.contains("table_bucket_enabled_from_metadata(&warehouse).await?;"),
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
        .find("enable_table_bucket_marker(bucket).await?;")
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
    assert!(metadata.get("last-sequence-number").is_none());
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
        "partition-spec": [],
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
        while metadata_backend.objects.lock().await.is_empty() {
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
async fn create_table_response_recreates_dropped_identifier_without_overwriting_retained_metadata() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
}

#[tokio::test]
async fn concurrent_create_table_responses_keep_one_catalog_winner_with_distinct_metadata() {
    let catalog_backend = TestTableCatalogObjectBackend::default();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(catalog_backend);
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
        let metadata_backend = TestTableCatalogObjectBackend::default();
        let store = crate::table_catalog::ConfiguredTableCatalogStore::new(metadata_backend.clone(), mode);
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

        let store = crate::table_catalog::ConfiguredTableCatalogStore::new(metadata_backend.clone(), mode);
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    seed_test_snapshot_manifest(&metadata_backend, "warehouse", &manifest_list, 10, 1, &[(&data_file, 0, 1, 10, 1)]).await;
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
    let object = "metadata/current.json";
    let original = b"original".to_vec();
    let replacement = b"replacement".to_vec();
    let original_etag = hex_sha256(&original, str::to_string);
    metadata_backend.objects.lock().await.insert(
        ("warehouse".to_string(), object.to_string()),
        crate::table_catalog::TableCatalogObject {
            data: replacement,
            etag: Some(original_etag.clone()),
            mod_time: None,
        },
    );
    let commit_backend = TableCommitObjectBackend::trusted(metadata_backend.clone());
    crate::table_catalog::TableCatalogObjectBackend::read_object(&commit_backend, "warehouse", object)
        .await
        .expect("replacement bytes should be discovered")
        .expect("replacement object should exist");
    metadata_backend.objects.lock().await.insert(
        ("warehouse".to_string(), object.to_string()),
        crate::table_catalog::TableCatalogObject {
            data: original,
            etag: Some(original_etag),
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    metadata_backend
        .put_bytes(
            "warehouse",
            &test_snapshot_object_key("warehouse", &manifest_list),
            test_manifest_list_avro_bytes(&[&manifest], 1, 20),
        )
        .await;
    metadata_backend
        .put_bytes(
            "warehouse",
            &test_snapshot_object_key("warehouse", &manifest),
            test_manifest_avro_bytes(&manifest_files),
        )
        .await;
    {
        let mut objects = metadata_backend.objects.lock().await;
        let data = vec![1];
        let etag = hex_sha256(&data, str::to_string);
        for file in &data_files {
            objects.insert(
                ("warehouse".to_string(), test_snapshot_object_key("warehouse", file)),
                crate::table_catalog::TableCatalogObject {
                    data: data.clone(),
                    etag: Some(etag.clone()),
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    create_standard_events_table(&store, &metadata_backend, &namespace).await;

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let metadata_backend = TestTableCatalogObjectBackend {
        objects: Arc::clone(&metadata_backend.objects),
        put_object_barrier: Some(barrier),
        ..Default::default()
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
            test_table_metadata_json("metadata-table-uuid", "s3://warehouse/tables/table-id"),
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
    let committed = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        commit_request,
    )
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
            test_table_metadata_json("metadata-table-uuid", "s3://warehouse/tables/table-id"),
        )
        .await;
    let next_location = ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00002.metadata.json";
    let mut next_metadata = test_table_metadata_json("metadata-table-uuid", "s3://warehouse/tables/table-id");
    next_metadata["last-sequence-number"] = serde_json::Value::from(2);
    metadata_backend.put_json("warehouse", next_location, next_metadata).await;

    let updated = update_table_metadata_location_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
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

    assert_eq!(updated.metadata_location, table_metadata_location_for_client("warehouse", next_location));
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
    let backend = TestTableCatalogObjectBackend::default();
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
    let backend = TestTableCatalogObjectBackend::default();
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

    seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current.clone()).await;
    backend
        .put_bytes(bucket, &manifest_list, test_manifest_list_avro_bytes(&[&manifest], 7, 20))
        .await;
    backend
        .put_bytes(
            bucket,
            &manifest,
            test_manifest_avro_bytes(&[(&left_data, 0, 0, 20, 7), (&right_data, 0, 0, 20, 7)]),
        )
        .await;
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
    let backend = TestTableCatalogObjectBackend::default();
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
    let backend = TestTableCatalogObjectBackend::default();
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
    let backend = TestTableCatalogObjectBackend::default();
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
    let backend = TestTableCatalogObjectBackend::default();
    let store = crate::table_catalog::ObjectTableCatalogStore::new(backend.clone());
    let bucket = "warehouse";
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let table = crate::table_catalog::IdentifierSegment::parse("events").expect("table should parse");
    let current = crate::table_catalog::default_table_metadata_file_path(&namespace, &table, "00001.metadata.json");
    seed_object_table_for_metadata_maintenance(&store, &backend, bucket, &namespace, &table, current.clone()).await;
    backend
        .put_json_with_mod_time(
            bucket,
            &current,
            serde_json::json!({
                "current-snapshot-id": 10,
                "refs": {
                    "main": {
                        "snapshot-id": 10,
                        "type": "branch"
                    },
                    "audit": {
                        "snapshot-id": 9,
                        "type": "tag"
                    }
                }
            }),
            Some(OffsetDateTime::UNIX_EPOCH),
        )
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
    let backend = TestTableCatalogObjectBackend::default();
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
    let backend = TestTableCatalogObjectBackend::default();
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
    let backend = TestTableCatalogObjectBackend::default();
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
    let backend = TestTableCatalogObjectBackend::default();
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
    let backend = TestTableCatalogObjectBackend::default();
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
fn snapshot_conflict_requirements_validate_current_snapshot_id() {
    let metadata = serde_json::json!({
        "current-snapshot-id": 10
    });

    let matching = vec![serde_json::json!({
        "type": "assert-current-snapshot-id",
        "snapshot-id": 10
    })];
    validate_table_commit_requirements(&metadata, &matching).expect("matching current snapshot should pass");

    let stale = vec![serde_json::json!({
        "type": "assert-current-snapshot-id",
        "snapshot-id": 9
    })];
    assert!(validate_table_commit_requirements(&metadata, &stale).is_err());

    let no_snapshot_metadata = serde_json::json!({});
    let create_like = vec![serde_json::json!({
        "type": "assert-current-snapshot-id",
        "snapshot-id": null
    })];
    validate_table_commit_requirements(&no_snapshot_metadata, &create_like)
        .expect("null current snapshot requirement should pass when no current snapshot exists");
}

#[test]
fn snapshot_conflict_rejects_stale_parent_or_sequence_number() {
    let metadata = serde_json::json!({
        "current-snapshot-id": 10,
        "last-sequence-number": 4,
        "snapshots": [
            {
                "snapshot-id": 10,
                "sequence-number": 4,
                "timestamp-ms": 1234,
                "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
                "summary": {
                    "operation": "append"
                }
            }
        ],
        "snapshot-log": [],
        "metadata-log": []
    });

    let stale_parent = vec![serde_json::json!({
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
    assert!(apply_table_commit_updates(metadata.clone(), &stale_parent, "metadata/00001.metadata.json").is_err());

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
    assert!(apply_table_commit_updates(metadata, &stale_sequence, "metadata/00001.metadata.json").is_err());
}

#[test]
fn snapshot_conflict_rejects_unknown_snapshot_operations() {
    let metadata = serde_json::json!({
        "current-snapshot-id": 10,
        "last-sequence-number": 4,
        "snapshots": [
            {
                "snapshot-id": 10,
                "sequence-number": 4,
                "timestamp-ms": 1234,
                "manifest-list": "s3://warehouse/tables/table-id/metadata/snap-10.avro",
                "summary": {
                    "operation": "append"
                }
            }
        ],
        "snapshot-log": [],
        "metadata-log": []
    });

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
    assert!(apply_table_commit_updates(metadata, &updates, "metadata/00001.metadata.json").is_err());
}

#[tokio::test]
async fn row_level_conflict_allows_overwrite_when_deleted_file_is_current() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
                "type": "assert-current-snapshot-id",
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
async fn row_level_conflict_allows_v1_manifest_snapshot() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::default();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let manifest = format!("{table_location}/metadata/manifest-10.avro");
    let data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_manifest(&metadata_backend, "warehouse", &manifest, &[(&data_file, 0, 1, 10, 1)]).await;
    let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
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

    let commit = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        append_request,
    )
    .await
    .expect("v1 manifests snapshot should commit");

    assert_eq!(commit.metadata["current-snapshot-id"], 10);
    assert_eq!(commit.metadata["last-sequence-number"], 1);

    let second_manifest_list = format!("{table_location}/metadata/snap-11.avro");
    let second_manifest = format!("{table_location}/metadata/manifest-11.avro");
    let second_data_file = format!("{table_location}/data/part-11.parquet");
    let second_manifest_list_key = test_snapshot_object_key("warehouse", &second_manifest_list);
    metadata_backend
        .put_bytes(
            "warehouse",
            &second_manifest_list_key,
            test_manifest_list_avro_entries(&[(&manifest, 1, 10), (&second_manifest, 2, 11)]),
        )
        .await;
    seed_test_manifest(&metadata_backend, "warehouse", &second_manifest, &[(&second_data_file, 0, 1, 11, 2)]).await;
    let second_append: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [
            {
                "type": "assert-current-snapshot-id",
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

    let upgraded = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        second_append,
    )
    .await
    .expect("manifest-list snapshot should inherit a legacy manifest with unknown provenance");

    assert_eq!(upgraded.metadata["current-snapshot-id"], 11);
    assert_eq!(upgraded.metadata["last-sequence-number"], 2);
}

#[tokio::test]
async fn row_level_conflict_inherits_manifest_list_sequence_numbers() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::default();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let manifest = format!("{table_location}/metadata/manifest-10.avro");
    let data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_manifest_list(&metadata_backend, "warehouse", &manifest_list, &[&manifest], 1, 10).await;
    seed_test_manifest_with_nullable_sequences(&metadata_backend, "warehouse", &manifest, &[(&data_file, 0, 1, 10, None)]).await;
    let append_request: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let first_manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let first_manifest = format!("{table_location}/metadata/manifest-10.avro");
    let first_data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_manifest_list(&metadata_backend, "warehouse", &first_manifest_list, &[&first_manifest], 1, 10).await;
    seed_test_manifest(&metadata_backend, "warehouse", &first_manifest, &[(&first_data_file, 0, 1, 10, 1)]).await;
    let first_append: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
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
    let second_manifest_list_key = test_snapshot_object_key("warehouse", &second_manifest_list);
    metadata_backend
        .put_bytes(
            "warehouse",
            &second_manifest_list_key,
            test_manifest_list_avro_entries(&[(&first_manifest, 1, 10), (&second_manifest, 2, 11)]),
        )
        .await;
    seed_test_manifest(&metadata_backend, "warehouse", &second_manifest, &[(&second_data_file, 0, 1, 11, 2)]).await;
    let second_append: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
        "requirements": [
            {
                "type": "assert-current-snapshot-id",
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
    let namespace = crate::table_catalog::Namespace::parse("analytics").expect("namespace should parse");
    let created = create_standard_events_table(&store, &metadata_backend, &namespace).await;
    let table_location = created.metadata["location"]
        .as_str()
        .expect("created metadata should have table location");
    let first_manifest_list = format!("{table_location}/metadata/snap-10.avro");
    let first_manifest = format!("{table_location}/metadata/manifest-10.avro");
    let first_data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_manifest_list(&metadata_backend, "warehouse", &first_manifest_list, &[&first_manifest], 1, 10).await;
    seed_test_manifest(&metadata_backend, "warehouse", &first_manifest, &[(&first_data_file, 0, 1, 10, 1)]).await;
    let first_append: RestCommitTableRequest = serde_json::from_value(serde_json::json!({
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
                "type": "assert-current-snapshot-id",
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

#[tokio::test]
async fn row_level_conflict_rejects_stale_new_manifest_sequence() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let data_file = format!("{table_location}/data/part-11.parquet");
    seed_test_manifest_list(&metadata_backend, "warehouse", &manifest_list, &[&manifest], 1, 11).await;
    seed_test_manifest(&metadata_backend, "warehouse", &manifest, &[(&data_file, 0, 1, 11, 1)]).await;
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

    let error = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        append_request,
    )
    .await
    .expect_err("new manifest sequence must match the committed snapshot");

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
async fn row_level_conflict_rejects_stale_added_entry_sequence() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let data_file = format!("{table_location}/data/part-11.parquet");
    seed_test_manifest_list(&metadata_backend, "warehouse", &manifest_list, &[&manifest], 2, 11).await;
    seed_test_manifest(&metadata_backend, "warehouse", &manifest, &[(&data_file, 0, 1, 11, 1)]).await;
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

    let error = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        append_request,
    )
    .await
    .expect_err("added file sequence must match the new manifest");

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
async fn row_level_conflict_rejects_historical_change_in_new_manifest() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let data_file = format!("{table_location}/data/part-10.parquet");
    seed_test_manifest_list(&metadata_backend, "warehouse", &manifest_list, &[&manifest], 2, 11).await;
    seed_test_manifest(&metadata_backend, "warehouse", &manifest, &[(&data_file, 0, 1, 10, 1)]).await;
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

    let error = commit_table_response(
        &store,
        &trusted_table_commit_backend(&metadata_backend),
        "warehouse",
        &namespace,
        "events",
        append_request,
    )
    .await
    .expect_err("new manifest must not claim a historical changed entry");

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
async fn row_level_conflict_allows_add_only_overwrite_snapshot() {
    let store = TestTableCatalogStore::default();
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
                "type": "assert-current-snapshot-id",
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
                "type": "assert-current-snapshot-id",
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

    assert_eq!(error.code(), &s3s::S3ErrorCode::PreconditionFailed);
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
                "type": "assert-current-snapshot-id",
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
                "type": "assert-current-snapshot-id",
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
        }
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
        }
    }))
    .expect("standard create view request should parse");

    let created = create_view_response(&store, &metadata_backend, "warehouse", &namespace, create_request, true)
        .await
        .expect("view should be created");
    assert_eq!(created.metadata["format-version"], 1);
    assert_eq!(created.metadata["current-version-id"], 1);
    assert_eq!(created.metadata["versions"][0]["representations"][0]["dialect"], "spark");
    assert!(
        metadata_backend
            .object_exists("warehouse", &created.metadata_location)
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
    let replace_request: RestCommitViewRequest = serde_json::from_value(serde_json::json!({
        "updates": [
            {
                "action": "add-view-version",
                "view-version": {
                    "version-id": 2,
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
    let replaced = replace_view_response(&store, &metadata_backend, "warehouse", &namespace, "recent_events", replace_request)
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
        }
    }))
    .expect("standard recreate view request should parse");
    let recreated = create_view_response(&store, &metadata_backend, "warehouse", &namespace, recreate_request, true)
        .await
        .expect("dropped view name should be reusable");
    assert_ne!(recreated.metadata_location, created.metadata_location);
}

#[tokio::test]
async fn table_ref_write_responses_use_commit_guard_and_protect_deletes() {
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
    views: tokio::sync::Mutex<Vec<crate::table_catalog::ViewEntry>>,
    commits: tokio::sync::Mutex<Vec<crate::table_catalog::CommitLogEntry>>,
    fail_put_table_bucket: tokio::sync::Mutex<bool>,
    register_table_pause: Option<TestCatalogPublishPause>,
    commit_table_pause: Option<TestCatalogPublishPause>,
}

#[derive(Clone, Default)]
struct TestCatalogPublishPause {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl TestCatalogPublishPause {
    async fn wait_started(&self) {
        self.started.notified().await;
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

type TestTableCatalogObjectLocks = Arc<tokio::sync::Mutex<BTreeMap<(String, String), Arc<tokio::sync::Mutex<()>>>>>;

#[derive(Clone, Default)]
struct TestTableCatalogObjectBackend {
    objects: Arc<tokio::sync::Mutex<BTreeMap<(String, String), crate::table_catalog::TableCatalogObject>>>,
    put_object_barrier: Option<Arc<tokio::sync::Barrier>>,
    locks: TestTableCatalogObjectLocks,
    lock_attempts: Arc<tokio::sync::Mutex<Vec<(String, String)>>>,
}

impl TestTableCatalogObjectBackend {
    async fn put_bytes(&self, bucket: &str, object: &str, data: Vec<u8>) {
        let etag = hex_sha256(&data, str::to_string);
        self.objects.lock().await.insert(
            (bucket.to_string(), object.to_string()),
            crate::table_catalog::TableCatalogObject {
                data,
                etag: Some(etag),
                mod_time: None,
            },
        );
    }

    async fn put_json(&self, bucket: &str, object: &str, value: serde_json::Value) {
        self.put_json_with_mod_time(bucket, object, value, None).await;
    }

    async fn put_gzip_json(&self, bucket: &str, object: &str, value: serde_json::Value) {
        use std::io::Write;

        let data = serde_json::to_vec(&value).expect("metadata JSON should serialize");
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&data).expect("metadata JSON should compress");
        self.put_bytes(bucket, object, encoder.finish().expect("metadata gzip stream should finish"))
            .await;
    }

    async fn put_json_with_mod_time(
        &self,
        bucket: &str,
        object: &str,
        value: serde_json::Value,
        mod_time: Option<OffsetDateTime>,
    ) {
        let data = serde_json::to_vec(&value).expect("metadata JSON should serialize");
        let etag = hex_sha256(&data, str::to_string);
        self.objects.lock().await.insert(
            (bucket.to_string(), object.to_string()),
            crate::table_catalog::TableCatalogObject {
                data,
                etag: Some(etag),
                mod_time,
            },
        );
    }

    async fn write_lock_is_held(&self, bucket: &str, object: &str) -> bool {
        let lock = self
            .locks
            .lock()
            .await
            .get(&(bucket.to_string(), object.to_string()))
            .cloned();
        lock.is_some_and(|lock| lock.try_lock_owned().is_err())
    }

    async fn wait_for_lock_attempts(&self, count: usize) {
        tokio::time::timeout(StdDuration::from_secs(2), async {
            loop {
                if self.lock_attempts.lock().await.len() >= count {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("lock acquisition attempts should be observable");
    }
}

fn trusted_table_commit_backend(
    backend: &TestTableCatalogObjectBackend,
) -> TableCommitObjectBackend<TestTableCatalogObjectBackend> {
    TableCommitObjectBackend::trusted(backend.clone())
}

fn test_table_metadata_json(table_uuid: &str, location: &str) -> serde_json::Value {
    serde_json::json!({
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": 1,
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
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {}
    })
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

fn test_manifest_list_avro_bytes(manifest_paths: &[&str], sequence_number: i64, snapshot_id: i64) -> Vec<u8> {
    let manifests = manifest_paths
        .iter()
        .map(|manifest_path| (*manifest_path, 0, sequence_number, snapshot_id))
        .collect::<Vec<_>>();
    test_manifest_list_avro_entries_with_partition_specs(&manifests)
}

fn test_manifest_list_avro_entries(manifests: &[(&str, i64, i64)]) -> Vec<u8> {
    let manifests = manifests
        .iter()
        .map(|(manifest_path, sequence_number, snapshot_id)| (*manifest_path, 0, *sequence_number, *snapshot_id))
        .collect::<Vec<_>>();
    test_manifest_list_avro_entries_with_partition_specs(&manifests)
}

fn test_manifest_list_avro_entries_with_partition_specs(manifests: &[(&str, i32, i64, i64)]) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(
        r#"
            {
              "type": "record",
              "name": "manifest_file",
              "fields": [
                {"name": "manifest_path", "type": "string"},
                {"name": "manifest_length", "type": "long"},
                {"name": "partition_spec_id", "type": "int"},
                {"name": "content", "type": "int"},
                {"name": "sequence_number", "type": "long"},
                {"name": "min_sequence_number", "type": "long"},
                {"name": "added_snapshot_id", "type": "long"},
                {"name": "added_files_count", "type": "int"},
                {"name": "existing_files_count", "type": "int"},
                {"name": "deleted_files_count", "type": "int"},
                {"name": "added_rows_count", "type": "long"},
                {"name": "existing_rows_count", "type": "long"},
                {"name": "deleted_rows_count", "type": "long"}
              ]
            }
            "#,
    )
    .expect("manifest list avro schema should parse");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    for (manifest_path, partition_spec_id, sequence_number, snapshot_id) in manifests {
        writer
            .append(apache_avro::types::Value::Record(vec![
                (
                    "manifest_path".to_string(),
                    apache_avro::types::Value::String((*manifest_path).to_string()),
                ),
                ("manifest_length".to_string(), apache_avro::types::Value::Long(1)),
                ("partition_spec_id".to_string(), apache_avro::types::Value::Int(*partition_spec_id)),
                ("content".to_string(), apache_avro::types::Value::Int(0)),
                ("sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                ("min_sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                ("added_snapshot_id".to_string(), apache_avro::types::Value::Long(*snapshot_id)),
                ("added_files_count".to_string(), apache_avro::types::Value::Int(1)),
                ("existing_files_count".to_string(), apache_avro::types::Value::Int(0)),
                ("deleted_files_count".to_string(), apache_avro::types::Value::Int(0)),
                ("added_rows_count".to_string(), apache_avro::types::Value::Long(1)),
                ("existing_rows_count".to_string(), apache_avro::types::Value::Long(0)),
                ("deleted_rows_count".to_string(), apache_avro::types::Value::Long(0)),
            ]))
            .expect("manifest list record should append");
    }
    writer.into_inner().expect("manifest list avro bytes should flush")
}

fn test_manifest_avro_bytes(files: &[(&str, i32, i32, i64, i64)]) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(
        r#"
            {
              "type": "record",
              "name": "manifest_entry",
              "fields": [
                {"name": "status", "type": "int"},
                {"name": "snapshot_id", "type": "long"},
                {"name": "sequence_number", "type": "long"},
                {"name": "file_sequence_number", "type": "long"},
                {
                  "name": "data_file",
                  "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                      {"name": "content", "type": "int"},
                      {"name": "file_path", "type": "string"},
                      {"name": "record_count", "type": "long"},
                      {"name": "file_size_in_bytes", "type": "long"}
                    ]
                  }
                }
              ]
            }
            "#,
    )
    .expect("manifest avro schema should parse");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    for (file_path, content, status, snapshot_id, sequence_number) in files {
        writer
            .append(apache_avro::types::Value::Record(vec![
                ("status".to_string(), apache_avro::types::Value::Int(*status)),
                ("snapshot_id".to_string(), apache_avro::types::Value::Long(*snapshot_id)),
                ("sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                ("file_sequence_number".to_string(), apache_avro::types::Value::Long(*sequence_number)),
                (
                    "data_file".to_string(),
                    apache_avro::types::Value::Record(vec![
                        ("content".to_string(), apache_avro::types::Value::Int(*content)),
                        ("file_path".to_string(), apache_avro::types::Value::String((*file_path).to_string())),
                        ("record_count".to_string(), apache_avro::types::Value::Long(1)),
                        ("file_size_in_bytes".to_string(), apache_avro::types::Value::Long(1)),
                    ]),
                ),
            ]))
            .expect("manifest record should append");
    }
    writer.into_inner().expect("manifest avro bytes should flush")
}

fn test_nullable_long(value: Option<i64>) -> apache_avro::types::Value {
    match value {
        Some(value) => apache_avro::types::Value::Union(1, Box::new(apache_avro::types::Value::Long(value))),
        None => apache_avro::types::Value::Union(0, Box::new(apache_avro::types::Value::Null)),
    }
}

fn test_manifest_avro_bytes_with_nullable_sequences(files: &[(&str, i32, i32, i64, Option<i64>)]) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(
        r#"
            {
              "type": "record",
              "name": "manifest_entry",
              "fields": [
                {"name": "status", "type": "int"},
                {"name": "snapshot_id", "type": "long"},
                {"name": "sequence_number", "type": ["null", "long"], "default": null},
                {"name": "file_sequence_number", "type": ["null", "long"], "default": null},
                {
                  "name": "data_file",
                  "type": {
                    "type": "record",
                    "name": "data_file",
                    "fields": [
                      {"name": "content", "type": "int"},
                      {"name": "file_path", "type": "string"},
                      {"name": "record_count", "type": "long"},
                      {"name": "file_size_in_bytes", "type": "long"}
                    ]
                  }
                }
              ]
            }
            "#,
    )
    .expect("manifest avro schema should parse");
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    for (file_path, content, status, snapshot_id, sequence_number) in files {
        writer
            .append(apache_avro::types::Value::Record(vec![
                ("status".to_string(), apache_avro::types::Value::Int(*status)),
                ("snapshot_id".to_string(), apache_avro::types::Value::Long(*snapshot_id)),
                ("sequence_number".to_string(), test_nullable_long(*sequence_number)),
                ("file_sequence_number".to_string(), test_nullable_long(*sequence_number)),
                (
                    "data_file".to_string(),
                    apache_avro::types::Value::Record(vec![
                        ("content".to_string(), apache_avro::types::Value::Int(*content)),
                        ("file_path".to_string(), apache_avro::types::Value::String((*file_path).to_string())),
                        ("record_count".to_string(), apache_avro::types::Value::Long(1)),
                        ("file_size_in_bytes".to_string(), apache_avro::types::Value::Long(1)),
                    ]),
                ),
            ]))
            .expect("manifest record should append");
    }
    writer.into_inner().expect("manifest avro bytes should flush")
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
    backend
        .put_bytes(
            bucket,
            &manifest_list_key,
            test_manifest_list_avro_bytes(manifest_locations, sequence_number, snapshot_id),
        )
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
    backend
        .put_bytes(
            bucket,
            &manifest_list_key,
            test_manifest_list_avro_bytes(&[&manifest_location], sequence_number, snapshot_id),
        )
        .await;
    backend
        .put_bytes(bucket, &manifest_key, test_manifest_avro_bytes(files))
        .await;
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
            let etag = hex_sha256(&data, str::to_string);
            objects.insert(
                key,
                crate::table_catalog::TableCatalogObject {
                    data,
                    etag: Some(etag),
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
        bucket: &str,
        object: &str,
    ) -> crate::table_catalog::TableCatalogStoreResult<Box<dyn Send>> {
        self.lock_attempts.lock().await.push((bucket.to_string(), object.to_string()));
        let lock = {
            let mut locks = self.locks.lock().await;
            locks
                .entry((bucket.to_string(), object.to_string()))
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        Ok(Box::new(lock.lock_owned().await))
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

    async fn update_namespace_properties(
        &self,
        table_bucket: &str,
        namespace: &str,
        update: crate::table_catalog::NamespacePropertiesUpdate,
    ) -> crate::table_catalog::TableCatalogStoreResult<crate::table_catalog::NamespacePropertiesUpdateResult> {
        let mut namespaces = self.namespaces.lock().await;
        let entry = namespaces
            .iter_mut()
            .find(|entry| entry.table_bucket == table_bucket && entry.namespace == namespace)
            .ok_or_else(|| {
                crate::table_catalog::TableCatalogStoreError::NotFound(format!("namespace {table_bucket}/{namespace}"))
            })?;
        Ok(update.apply_to(entry))
    }

    async fn drop_namespace(&self, table_bucket: &str, namespace: &str) -> crate::table_catalog::TableCatalogStoreResult<()> {
        self.namespaces
            .lock()
            .await
            .retain(|entry| !(entry.table_bucket == table_bucket && entry.namespace == namespace));
        Ok(())
    }

    async fn create_table(&self, entry: crate::table_catalog::TableEntry) -> crate::table_catalog::TableCatalogStoreResult<()> {
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

    async fn register_table(&self, entry: crate::table_catalog::TableEntry) -> crate::table_catalog::TableCatalogStoreResult<()> {
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
        if let Some(pause) = &self.register_table_pause {
            pause.started.notify_one();
            pause.release.notified().await;
        }
        self.tables.lock().await.push(entry);
        Ok(())
    }

    async fn register_table_with_publication(
        &self,
        entry: crate::table_catalog::TableEntry,
        publication: &(dyn crate::table_catalog::TableCommitPublication + Sync),
    ) -> crate::table_catalog::TableCatalogStoreResult<()> {
        publication.begin_table_bucket(&entry.table_bucket).await?;
        if !publication.holds_table_bucket(&entry.table_bucket) {
            return Err(crate::table_catalog::TableCatalogStoreError::Internal(
                "table registration requires a table-bucket publication fence".to_string(),
            ));
        }
        let _publication_completion = crate::table_catalog::TableCommitPublicationCompletion::new(publication);
        publication
            .prepare(&entry.table_bucket, &entry.namespace, &entry.table)
            .await?;
        if !publication.holds_table(&entry.table_bucket, &entry.namespace, &entry.table) {
            return Err(crate::table_catalog::TableCatalogStoreError::Internal(
                "table registration requires a table publication fence".to_string(),
            ));
        }
        self.register_table(entry).await
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

    async fn list_all_tables(
        &self,
        table_bucket: &str,
    ) -> crate::table_catalog::TableCatalogStoreResult<Vec<crate::table_catalog::TableEntry>> {
        Ok(self
            .tables
            .lock()
            .await
            .iter()
            .filter(|entry| entry.table_bucket == table_bucket)
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
        if let Some(pause) = &self.commit_table_pause {
            pause.started.notify_one();
            pause.release.notified().await;
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

    async fn commit_table_with_publication(
        &self,
        request: crate::table_catalog::TableCommitRequest,
        publication: &(dyn crate::table_catalog::TableCommitPublication + Sync),
    ) -> crate::table_catalog::TableCatalogStoreResult<crate::table_catalog::TableCommitResult> {
        publication
            .prepare(&request.table_bucket, &request.namespace, &request.table)
            .await?;
        if !publication.holds_table(&request.table_bucket, &request.namespace, &request.table) {
            return Err(crate::table_catalog::TableCatalogStoreError::Internal(
                "table commit requires a table publication fence".to_string(),
            ));
        }
        let _publication_completion = crate::table_catalog::TableCommitPublicationCompletion::new(publication);
        self.commit_table(request).await
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

    async fn create_view(&self, entry: crate::table_catalog::ViewEntry) -> crate::table_catalog::TableCatalogStoreResult<()> {
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
        self.views.lock().await.push(entry);
        Ok(())
    }

    async fn list_views(
        &self,
        table_bucket: &str,
        namespace: &str,
    ) -> crate::table_catalog::TableCatalogStoreResult<Vec<crate::table_catalog::ViewEntry>> {
        Ok(self
            .views
            .lock()
            .await
            .iter()
            .filter(|entry| entry.table_bucket == table_bucket && entry.namespace == namespace)
            .cloned()
            .collect())
    }

    async fn load_view(
        &self,
        table_bucket: &str,
        namespace: &str,
        view: &str,
    ) -> crate::table_catalog::TableCatalogStoreResult<Option<crate::table_catalog::ViewEntry>> {
        Ok(self
            .views
            .lock()
            .await
            .iter()
            .find(|entry| entry.table_bucket == table_bucket && entry.namespace == namespace && entry.view == view)
            .cloned())
    }

    async fn replace_view(
        &self,
        request: crate::table_catalog::ViewCommitRequest,
    ) -> crate::table_catalog::TableCatalogStoreResult<crate::table_catalog::ViewCommitResult> {
        let mut views = self.views.lock().await;
        let Some(index) = views.iter().position(|entry| {
            entry.table_bucket == request.table_bucket && entry.namespace == request.namespace && entry.view == request.view
        }) else {
            return Err(crate::table_catalog::TableCatalogStoreError::NotFound(format!(
                "view {}/{}/{}",
                request.table_bucket, request.namespace, request.view
            )));
        };
        let current = views[index].clone();
        if current.version_token != request.expected_version_token {
            return Err(crate::table_catalog::TableCatalogStoreError::Conflict(
                "current view version token does not match expected token".to_string(),
            ));
        }
        if current.metadata_location != request.expected_metadata_location {
            return Err(crate::table_catalog::TableCatalogStoreError::Conflict(
                "current view metadata location does not match expected location".to_string(),
            ));
        }
        let mut next = current;
        next.metadata_location = request.new_metadata_location;
        next.version_token = "token-view-committed".to_string();
        next.generation = next.generation.saturating_add(1);
        views[index] = next.clone();
        Ok(crate::table_catalog::ViewCommitResult { view: next })
    }

    async fn drop_view(
        &self,
        table_bucket: &str,
        namespace: &str,
        view: &str,
    ) -> crate::table_catalog::TableCatalogStoreResult<()> {
        self.views
            .lock()
            .await
            .retain(|entry| !(entry.table_bucket == table_bucket && entry.namespace == namespace && entry.view == view));
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
            _identifier: None,
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
            test_table_metadata_json(&table_uuid, "s3://warehouse/tables/table-id"),
        )
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
        .put_json(
            "warehouse",
            next_location,
            test_table_metadata_json(&table_uuid, "s3://warehouse/tables/table-id"),
        )
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let metadata_backend = TestTableCatalogObjectBackend::default();
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
    let backend = TestTableCatalogObjectBackend::default();
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
