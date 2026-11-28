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

//! The table data plane's share of S3 authorization, on the owned request: which object actions
//! map to table metadata actions, which ones need the publication fence, how table listings hide
//! unauthorized objects and seal their cursors, how catalog failures are answered, and how a
//! request reuses the table resolution and fences it already holds.
//!
//! Responsible for: `storage::authorization::table_data_plane` and the multipart authorization
//! functions' use of the publication fence, driven through `RequestEnvelope`.
//! Not responsible for: the table catalog's own storage, or the listing use cases that consume
//! the listing snapshot.
//! Upstream: the owned request fixture, the gating test stores, and the table catalog.
//! Downstream: `storage::authorization` and `storage::access_owned`.

use crate::storage::access_owned::object_commit::{
    CopySourceObject, authorize_complete_multipart_upload, authorize_upload_part_copy,
};
use crate::storage::access_owned::object_write::authorize_upload_part;
use crate::storage::authorization::tests::fixture::{request, server_context};
use crate::storage::authorization::{
    AuthorizationRequest, TABLE_DATA_PLANE_LIST_CURSOR_MAX_ENCODED_LEN, TableDataPlaneListAccess,
    TableDataPlaneListCursorPosition, TableDataPlaneListResource, TableDataPlanePublicationGuards,
    require_owned_reserved_table_object, retain_table_bucket_publication_guard, table_catalog_backend_for_data_plane,
    table_data_plane_admin_action, table_data_plane_content_mutation, table_data_plane_resource_for_request,
    table_publication_guard_error,
};
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use http::Method;
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::S3ErrorCode;
use serial_test::serial;
use std::sync::Arc;

#[test]
fn table_data_plane_admin_action_maps_s3_object_operations() {
    assert_eq!(
        table_data_plane_admin_action(Action::S3Action(S3Action::GetObjectAction)),
        Some(rustfs_policy::policy::action::AdminAction::GetTableMetadataAction)
    );
    assert_eq!(
        table_data_plane_admin_action(Action::S3Action(S3Action::PutObjectAction)),
        Some(rustfs_policy::policy::action::AdminAction::SetTableMetadataAction)
    );
    assert_eq!(
        table_data_plane_admin_action(Action::S3Action(S3Action::PutObjectTaggingAction)),
        Some(rustfs_policy::policy::action::AdminAction::SetTableMetadataAction)
    );
    assert_eq!(
        table_data_plane_admin_action(Action::S3Action(S3Action::DeleteObjectAction)),
        Some(rustfs_policy::policy::action::AdminAction::SetTableMetadataAction)
    );
    assert_eq!(
        table_data_plane_admin_action(Action::S3Action(S3Action::GetObjectTaggingAction)),
        Some(rustfs_policy::policy::action::AdminAction::GetTableMetadataAction)
    );
    assert_eq!(table_data_plane_admin_action(Action::S3Action(S3Action::ListBucketAction)), None);
}

fn table_list_access(allowed: bool) -> TableDataPlaneListAccess {
    TableDataPlaneListAccess {
        bucket: "analytics".to_string(),
        principal_fingerprint: "principal".to_string(),
        snapshot_fingerprint: "snapshot".to_string(),
        cursor_key: [0x42; 32],
        resources: vec![TableDataPlaneListResource {
            resource: crate::table_catalog::TableDataPlaneResource {
                table_bucket: "analytics".to_string(),
                namespace: "sales".to_string(),
                table: "orders".to_string(),
                table_id: "table-id".to_string(),
                warehouse_object_prefix: "tables/table-id/".to_string(),
            },
            allowed,
        }],
    }
}

/// The listing authorization hands its table snapshot to the use case through the request it
/// was given.
#[test]
fn table_list_access_reaches_the_use_case_from_either_edge() {
    let access = TableDataPlaneListAccess::test_listing_fixture("bucket");

    let mut envelope = request(Method::GET).build();
    assert!(envelope.table_list_access().is_none());

    AuthorizationRequest::install_table_list_access(&mut envelope, access);

    assert!(envelope.table_list_access().is_some());
}

#[test]
fn table_list_access_hides_unauthorized_table_objects_and_reserved_metadata() {
    let denied = table_list_access(false);
    assert!(!denied.allows_object("tables/table-id/data/file.parquet"));
    assert!(
        !denied.allows_object(".rustfs-table/warehouses/default/namespaces/sales/tables/orders/metadata/00001.metadata.json")
    );
    assert!(!denied.allows_object(".rustfs-table/private/catalog.json"));
    assert!(denied.allows_object("ordinary/file.txt"));

    let allowed = table_list_access(true);
    assert!(allowed.allows_object("tables/table-id/data/file.parquet"));
    assert!(
        !allowed.allows_object(".rustfs-table/warehouses/default/namespaces/sales/tables/orders/metadata/00001.metadata.json")
    );
    assert!(!allowed.allows_object(".rustfs-table/private/catalog.json"));
}

#[test]
fn table_list_cursor_is_confidential_and_bound_to_the_request_snapshot() {
    let access = table_list_access(true);
    let marker = "tables/hidden/data/file.parquet";
    let token = access
        .encode_cursor(
            "ListObjectsV2",
            "tables/",
            Some("/"),
            1000,
            TableDataPlaneListCursorPosition {
                marker: Some(marker.to_string()),
                version_marker: None,
                common_prefix: None,
            },
        )
        .expect("cursor should encode");
    assert!(!token.contains(marker));
    let decoded = access
        .decode_cursor("ListObjectsV2", "tables/", Some("/"), 1000, &token)
        .expect("cursor should decode");
    assert_eq!(decoded.marker(), Some(marker));

    let mut changed = access.clone();
    changed.snapshot_fingerprint = "changed".to_string();
    assert!(
        changed
            .decode_cursor("ListObjectsV2", "tables/", Some("/"), 1000, &token)
            .is_err()
    );
    assert!(
        access
            .decode_cursor("ListObjectsV2", "other/", Some("/"), 1000, &token)
            .is_err()
    );
    let mut rotated_key = access.clone();
    rotated_key.cursor_key[0] ^= 1;
    assert!(
        rotated_key
            .decode_cursor("ListObjectsV2", "tables/", Some("/"), 1000, &token)
            .is_err()
    );
    assert!(
        access
            .decode_cursor(
                "ListObjectsV2",
                "tables/",
                Some("/"),
                1000,
                &"x".repeat(TABLE_DATA_PLANE_LIST_CURSOR_MAX_ENCODED_LEN + 1),
            )
            .is_err()
    );
}

#[test]
fn reserved_table_metadata_requires_an_active_table_owner() {
    let error = require_owned_reserved_table_object::<()>(
        ".rustfs-table/warehouses/default/namespaces/analytics/tables/events/metadata/00001.metadata.json",
        None,
    )
    .expect_err("unowned reserved metadata must fail closed");
    assert_eq!(error.code(), &S3ErrorCode::AccessDenied);

    assert!(
        require_owned_reserved_table_object("objects/data.parquet", None::<()>)
            .unwrap()
            .is_none()
    );
    assert!(
        require_owned_reserved_table_object(".rustfs-table/metadata.json", Some(()))
            .unwrap()
            .is_some()
    );
}

#[test]
fn table_data_plane_publication_guard_covers_content_mutations_only() {
    for action in [
        S3Action::PutObjectAction,
        S3Action::DeleteObjectAction,
        S3Action::DeleteObjectVersionAction,
        S3Action::RestoreObjectAction,
        S3Action::ReplicateObjectAction,
        S3Action::ReplicateDeleteAction,
        S3Action::PutObjectFanOutAction,
    ] {
        assert!(table_data_plane_content_mutation(Action::S3Action(action)));
    }
    for action in [
        S3Action::GetObjectAction,
        S3Action::PutObjectTaggingAction,
        S3Action::DeleteObjectTaggingAction,
        S3Action::ListBucketAction,
    ] {
        assert!(!table_data_plane_content_mutation(Action::S3Action(action)));
    }
}

/// The message `ApiError::error_code_to_message` writes for `ServiceUnavailable`.
const SERVICE_UNAVAILABLE_MESSAGE: &str = "The service is unavailable. Please retry.";
/// The message `ApiError::error_code_to_message` writes for `InternalError`.
const INTERNAL_ERROR_MESSAGE: &str = "We encountered an internal error, please try again.";

/// Whether the error carries a `Retry-After` response header, which is the only place the
/// gateway's error rendering (`gateway_handlers::error::handler_error`) takes it from.
fn carries_retry_after(err: &rustfs_s3_types::S3Error) -> bool {
    err.headers()
        .is_some_and(|headers| headers.contains_key(http::header::RETRY_AFTER))
}

#[test]
fn table_publication_guard_unavailable_errors_are_retryable() {
    use crate::table_catalog::TableCatalogStoreError;

    for message in [
        "failed to acquire catalog migration lock: Quorum not reached: required 2, achieved 1",
        "failed to acquire catalog migration lock: lock acquisition timed out after 5s",
        "peer unavailable",
        "",
    ] {
        let error = TableCatalogStoreError::Unavailable(message.to_string());
        let err = table_publication_guard_error(error);
        assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable, "{message}");
        assert_eq!(
            err.status_code().or_else(|| err.code().status_code()),
            Some(http::StatusCode::SERVICE_UNAVAILABLE)
        );
        assert_eq!(err.message(), Some(SERVICE_UNAVAILABLE_MESSAGE));
        assert!(!carries_retry_after(&err));
    }
}

#[test]
fn table_publication_guard_non_retryable_errors_stay_internal() {
    use crate::table_catalog::TableCatalogStoreError;

    let message = "temporarily unavailable: timeout: quorum not reached".to_string();
    for error in [
        TableCatalogStoreError::Internal(message.clone()),
        TableCatalogStoreError::Invalid(message.clone()),
        TableCatalogStoreError::Unsupported(message.clone()),
        TableCatalogStoreError::NotFound(message.clone()),
        TableCatalogStoreError::NamespaceNotFound(message.clone()),
        TableCatalogStoreError::TableNotFound(message.clone()),
        TableCatalogStoreError::AlreadyExists(message.clone()),
        TableCatalogStoreError::Conflict(message),
    ] {
        let err = table_publication_guard_error(error);
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(
            err.status_code().or_else(|| err.code().status_code()),
            Some(http::StatusCode::INTERNAL_SERVER_ERROR)
        );
        assert_eq!(err.message(), Some(INTERNAL_ERROR_MESSAGE));
        assert!(!carries_retry_after(&err));
    }
}

#[tokio::test]
#[serial]
async fn table_data_plane_cold_lookup_does_not_wait_for_backup_while_holding_publication() {
    use crate::table_catalog::{TableCatalogObjectBackend as _, TableCatalogStore as _};
    let (_temp_dir, _disk_paths, object_store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
    let bucket = "cold-authorization";
    object_store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create table bucket");
    let server_ctx = server_context(object_store.clone());
    let mut request_envelope = request(Method::PUT).server_context(&server_ctx).build();
    let backend = table_catalog_backend_for_data_plane(&request_envelope).expect("request backend");
    let catalog = crate::table_catalog::ConfiguredTableCatalogStore::new_for_test(
        backend.clone(),
        crate::table_catalog::TableCatalogBackingMode::ObjectBacked,
    );
    catalog
        .put_table_bucket(crate::table_catalog::TableBucketEntry {
            vector_bucket_binding: None,
            version: crate::table_catalog::TABLE_CATALOG_ENTRY_VERSION,
            table_bucket: bucket.to_string(),
            catalog_type: crate::table_catalog::TABLE_BUCKET_CATALOG_TYPE.to_string(),
            warehouse_root: format!("s3://{bucket}/"),
            state: crate::table_catalog::TableCatalogEntryState::Active,
            properties: Default::default(),
            active_rename_id: None,
            created_at: None,
            updated_at: None,
        })
        .await
        .expect("newly enabled catalog entry");
    retain_table_bucket_publication_guard(&mut request_envelope, bucket)
        .await
        .expect("S3 owns publication first");
    let backup_guard = backend
        .acquire_write_lock(
            crate::admin::storage_api::RUSTFS_META_BUCKET,
            &crate::table_catalog::TableCatalogObjectPaths::default().catalog_backup_fence_lock_path(bucket),
        )
        .await
        .expect("backup owns its catalog fence");
    let resource = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        table_data_plane_resource_for_request(&mut request_envelope, bucket, "ordinary-object", true),
    )
    .await
    .expect("cold S3 lookup must not wait for the backup catalog fence")
    .expect("resolve newly enabled table bucket");
    assert!(resource.is_none());
    assert!(
        request_envelope
            .table_publication_guards()
            .expect("retained publication")
            .state
            .lock()
            .missing_resources
            .contains(&(bucket.to_string(), "ordinary-object".to_string()))
    );
    drop(backup_guard);
    drop(request_envelope);
    let mut read_request = request(Method::GET).server_context(&server_ctx).build();
    table_data_plane_resource_for_request(&mut read_request, bucket, "ordinary-object", true)
        .await
        .expect("unfenced reads retain normal index initialization");
    assert!(
        backend
            .read_object(
                crate::admin::storage_api::RUSTFS_META_BUCKET,
                &crate::table_catalog::TableCatalogObjectPaths::default().warehouse_index_state_path(bucket),
            )
            .await
            .expect("read initialized index marker")
            .is_some()
    );
}

#[tokio::test]
#[serial]
async fn multipart_parts_skip_table_publication_fence_but_completion_retains_it() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let server_ctx = server_context(Arc::clone(&store));
    let bucket = format!("multipart-table-fence-{}", uuid::Uuid::new_v4());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("test bucket should be created");
    let policy_json = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:GetObject","s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}}]}}"#
    );
    let mut metadata = (*crate::storage::get_bucket_metadata(&bucket)
        .await
        .expect("new bucket metadata should be cached"))
    .clone();
    metadata.policy_config = Some(serde_json::from_str(&policy_json).expect("test bucket policy should parse"));
    metadata.policy_config_json = policy_json.into_bytes();
    crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
        .await
        .expect("test bucket policy should be published");

    // UploadPart: upload id `upload-id`, part number 1.
    let mut part_req = request(Method::PUT).req_info().server_context(&server_ctx).build();
    authorize_upload_part(&mut part_req, &server_ctx, &bucket, "object")
        .await
        .expect("anonymous UploadPart should be authorized by the test policy");
    assert!(
        part_req.table_publication_guards().is_none(),
        "staged part data must not retain a table publication fence"
    );

    // UploadPartCopy: upload id `upload-id`, part number 2, source `{bucket}/source`.
    let mut copy_part_req = request(Method::PUT).req_info().server_context(&server_ctx).build();
    authorize_upload_part_copy(
        &mut copy_part_req,
        &server_ctx,
        &bucket,
        "object",
        Some(CopySourceObject {
            bucket: &bucket,
            key: "source",
            version_id: None,
        }),
    )
    .await
    .expect("anonymous UploadPartCopy should be authorized by the test policy");
    assert!(
        copy_part_req.table_publication_guards().is_none(),
        "staged copied part data must not retain a table publication fence"
    );

    // CompleteMultipartUpload: upload id `upload-id`, an empty part list, no `If-Match`.
    let mut complete_req = request(Method::POST).req_info().server_context(&server_ctx).build();
    authorize_complete_multipart_upload(&mut complete_req, &server_ctx, &bucket, "object", false)
        .await
        .expect("anonymous CompleteMultipartUpload should be authorized by the test policy");
    let bucket_fence = (bucket, crate::table_catalog::default_table_bucket_publication_lock_path());
    let guards = complete_req
        .table_publication_guards()
        .expect("completion should retain a table publication fence");
    assert!(guards.state.lock().keys.contains(&bucket_fence));
}

#[tokio::test]
async fn table_data_plane_request_reuses_table_resource_for_distinct_object_while_commit_waits() {
    let resource = crate::table_catalog::TableDataPlaneResource {
        table_bucket: "warehouse".to_string(),
        namespace: "analytics".to_string(),
        table: "events".to_string(),
        table_id: "table-id".to_string(),
        warehouse_object_prefix: "tables/table-id/".to_string(),
    };
    let cached_key = ("warehouse".to_string(), "tables/table-id/".to_string());
    let requested_key = ("warehouse".to_string(), "tables/table-id/data/part-00002.parquet".to_string());
    let retained = TableDataPlanePublicationGuards::default();
    retained.state.lock().resources.insert(cached_key, resource.clone());

    let publication_lock = Arc::new(tokio::sync::Mutex::new(()));
    let request_publication_guard = Arc::clone(&publication_lock).lock_owned().await;
    retained.state.lock().guards.push(Box::new(request_publication_guard));
    let mut req = request(Method::PUT).extension(retained.clone()).build();

    let catalog_lock = Arc::new(tokio::sync::Mutex::new(()));
    let commit_started = Arc::new(tokio::sync::Notify::new());
    let commit_catalog_lock = Arc::clone(&catalog_lock);
    let commit_publication_lock = Arc::clone(&publication_lock);
    let commit_started_signal = Arc::clone(&commit_started);
    let commit = tokio::spawn(async move {
        let _catalog_guard = commit_catalog_lock.lock().await;
        commit_started_signal.notify_one();
        let _publication_guard = commit_publication_lock.lock().await;
    });
    commit_started.notified().await;

    let resolved = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        table_data_plane_resource_for_request(&mut req, &requested_key.0, &requested_key.1, true),
    )
    .await
    .expect("cached table resolution must not wait for the catalog lock")
    .expect("cached table resolution should succeed");
    assert_eq!(resolved, Some(resource));

    retained.state.lock().guards.clear();
    tokio::time::timeout(std::time::Duration::from_secs(1), commit)
        .await
        .expect("commit should continue after the request releases its publication guard")
        .expect("commit task should join");
}

#[tokio::test]
async fn table_data_plane_request_reuses_reserved_warehouse_resource() {
    let resource = crate::table_catalog::TableDataPlaneResource {
        table_bucket: "warehouse".to_string(),
        namespace: "analytics".to_string(),
        table: "events".to_string(),
        table_id: "table-id".to_string(),
        warehouse_object_prefix: ".rustfs-table/custom-warehouse/events/".to_string(),
    };
    let retained = TableDataPlanePublicationGuards::default();
    retained.state.lock().resources.insert(
        (resource.table_bucket.clone(), resource.warehouse_object_prefix.clone()),
        resource.clone(),
    );
    let mut req = request(Method::GET).extension(retained).build();

    let resolved = table_data_plane_resource_for_request(
        &mut req,
        "warehouse",
        ".rustfs-table/custom-warehouse/events/data/part-00001.parquet",
        true,
    )
    .await
    .expect("a reserved-prefix warehouse object should use ordinary table resolution");

    assert_eq!(resolved, Some(resource));
}

#[tokio::test]
async fn table_data_plane_request_reuses_absence_while_bucket_publication_guard_is_held() {
    let key = ("warehouse".to_string(), "tables/new-table/data/file.parquet".to_string());
    let bucket_fence = (key.0.clone(), crate::table_catalog::default_table_bucket_publication_lock_path());
    let retained = TableDataPlanePublicationGuards::default();
    {
        let mut state = retained.state.lock();
        state.keys.insert(bucket_fence);
        state.missing_resources.insert(key.clone());
    }

    let publication_lock = Arc::new(tokio::sync::Mutex::new(()));
    let request_publication_guard = Arc::clone(&publication_lock).lock_owned().await;
    retained.state.lock().guards.push(Box::new(request_publication_guard));
    let mut req = request(Method::PUT).extension(retained.clone()).build();

    let commit_publication_lock = Arc::clone(&publication_lock);
    let commit = tokio::spawn(async move {
        let _publication_guard = commit_publication_lock.lock().await;
    });
    tokio::task::yield_now().await;

    let resolved = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        table_data_plane_resource_for_request(&mut req, &key.0, &key.1, true),
    )
    .await
    .expect("cached absence must not reread the catalog while publication waits")
    .expect("cached absence should resolve successfully");
    assert!(resolved.is_none());

    retained.state.lock().guards.clear();
    tokio::time::timeout(std::time::Duration::from_secs(1), commit)
        .await
        .expect("publication should continue after the request releases its bucket guard")
        .expect("publication task should join");
}
