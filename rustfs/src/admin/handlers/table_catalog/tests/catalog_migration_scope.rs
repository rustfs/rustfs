// Copyright 2026 RustFS Team
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
use crate::admin::storage_api::RUSTFS_META_BUCKET;
use crate::storage::storage_api::contract::bucket::BucketOperations as _;
use crate::table_catalog::TableCatalogObject;
use rustfs_iam::store::Store as _;

const SCOPED_WAREHOUSE: &str = "migration-scope-owned";
const FOREIGN_WAREHOUSE: &str = "migration-scope-foreign";
const UNMARKED_WAREHOUSE: &str = "migration-scope-unmarked";
const SCOPED_USER: &str = "catalog-migration-scoped-user";
const SCOPED_SECRET: &str = "catalog-migration-scoped-user-secret";

#[derive(Clone, Copy, Debug)]
enum CatalogMigrationScopeOperation {
    Backfill,
    Backup,
    Restore,
}

impl CatalogMigrationScopeOperation {
    fn suffix(self) -> &'static str {
        match self {
            Self::Backfill => "/catalog/warehouse-index/backfill",
            Self::Backup => "/catalog/backup",
            Self::Restore => "/catalog/restore",
        }
    }
}

struct CatalogMigrationScopeFixture {
    _temp_dir: tempfile::TempDir,
    context: Arc<AppContext>,
}

impl CatalogMigrationScopeFixture {
    async fn new() -> Self {
        let (temp_dir, _disk_paths, object_store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        for bucket in [SCOPED_WAREHOUSE, FOREIGN_WAREHOUSE, UNMARKED_WAREHOUSE] {
            object_store
                .make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("create isolated scope fixture bucket");
        }
        rustfs_iam::store::object::ObjectStore::new(object_store.clone())
            .save_iam_config(
                serde_json::json!({"version": 1}),
                format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
            )
            .await
            .expect("seed isolated IAM format");
        let iam = rustfs_iam::build_iam_sys(object_store.clone())
            .await
            .expect("build isolated request IAM");
        let context = Arc::new(AppContext::new(
            object_store.clone(),
            Arc::new(RequestIam { handle: iam.clone() }),
            Arc::new(RequestKms),
        ));
        // Preserve the existing process-global winner. When this test publishes
        // first, use the same root as the existing load-table fixture.
        let mut root = rustfs_credentials::get_global_action_cred().unwrap_or_else(|| rustfs_credentials::Credentials {
            access_key: "load-table-root-access-key".to_string(),
            secret_key: "load-table-root-secret-key".to_string(),
            ..Default::default()
        });
        root.status = "on".to_string();
        assert!(context.publish_action_credentials(root));
        iam.create_user(
            SCOPED_USER,
            &AddOrUpdateUserReq {
                secret_key: SCOPED_SECRET.to_string(),
                policy: None,
                status: AccountStatus::Enabled,
            },
        )
        .await
        .expect("create a regular non-owner migration user");
        let policy = serde_json::to_vec(&serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "admin:MigrateTableCatalog",
                "Resource": format!("arn:aws:s3:::{SCOPED_WAREHOUSE}")
            }]
        }))
        .expect("encode warehouse-limited migration policy");
        iam.set_policy(
            "catalog-migration-resource-scope",
            Policy::parse_config(&policy).expect("parse warehouse-limited migration policy"),
        )
        .await
        .expect("store migration policy");
        iam.policy_db_set(SCOPED_USER, UserType::Reg, false, "catalog-migration-resource-scope")
            .await
            .expect("attach migration policy to the regular user");

        let setup = table_catalog_handler_request(context.clone(), SCOPED_USER, SCOPED_SECRET);
        let backend = table_catalog_backend_from_envelope(setup.envelope()).expect("resolve the isolated catalog backend");
        let catalog = table_catalog_store_from_backend(backend).expect("resolve object catalog");
        for bucket in [SCOPED_WAREHOUSE, FOREIGN_WAREHOUSE] {
            enable_table_bucket_marker(object_store.as_ref(), bucket)
                .await
                .expect("enable fixture table bucket marker");
            ensure_table_bucket_entry(&catalog, bucket, true)
                .await
                .expect("seed fixture catalog bucket");
        }
        Self {
            _temp_dir: temp_dir,
            context,
        }
    }

    fn request(&self, prefix: &str, warehouse: &str, operation: CatalogMigrationScopeOperation, body: &[u8]) -> AdminRequest {
        table_catalog_request_builder(
            self.context.clone(),
            Some((SCOPED_USER, SCOPED_SECRET)),
            Method::POST,
            &format!("{prefix}/{warehouse}{}", operation.suffix()),
        )
        .header("content-type", "application/json")
        .body(Body::from(body.to_vec()))
        .build()
    }

    async fn inventory(&self) -> BTreeMap<(String, String), TableCatalogObject> {
        let setup = table_catalog_handler_request(self.context.clone(), SCOPED_USER, SCOPED_SECRET);
        let backend = table_catalog_backend_from_envelope(setup.envelope()).expect("resolve inventory backend");
        let mut result = BTreeMap::new();
        for bucket in [RUSTFS_META_BUCKET, SCOPED_WAREHOUSE, FOREIGN_WAREHOUSE, UNMARKED_WAREHOUSE] {
            for object in backend.list_objects(bucket, "").await.expect("list isolated fixture objects") {
                let stored = backend
                    .read_object(bucket, &object)
                    .await
                    .expect("read isolated fixture object")
                    .expect("listed fixture object exists");
                result.insert((bucket.to_string(), object), stored);
            }
        }
        result
    }
}

async fn call_catalog_migration_scope_handler(
    request: AdminRequest,
    prefix: &str,
    operation: CatalogMigrationScopeOperation,
) -> S3Result<AdminResponse> {
    let mut router = matchit::Router::new();
    router
        .insert(format!("{prefix}/{{warehouse}}{}", operation.suffix()), ())
        .expect("insert the real catalog route shape");
    let path = request.uri().path().to_string();
    let matched = router.at(&path).expect("match the warehouse parameter");
    match operation {
        CatalogMigrationScopeOperation::Backfill => BackfillTableWarehouseIndexHandler {}.call(request, matched.params).await,
        CatalogMigrationScopeOperation::Backup => CreateTableCatalogBackupHandler {}.call(request, matched.params).await,
        CatalogMigrationScopeOperation::Restore => RestoreTableCatalogBackupHandler {}.call(request, matched.params).await,
    }
}

async fn catalog_migration_scope_response_json(response: AdminResponse) -> serde_json::Value {
    let (status, _, bytes) = response_parts(response).await;
    assert_eq!(status, StatusCode::OK);
    serde_json::from_slice(&bytes).expect("parse successful catalog response JSON")
}

#[tokio::test]
#[serial_test::serial]
async fn catalog_backfill_handler_enforces_warehouse_scope_before_marker_or_store_access() {
    let fixture = CatalogMigrationScopeFixture::new().await;
    for prefix in [TABLE_CATALOG_PREFIX, TABLE_CATALOG_COMPAT_PREFIX] {
        let operation = CatalogMigrationScopeOperation::Backfill;
        let request = fixture.request(prefix, SCOPED_WAREHOUSE, operation, b"");
        let principal = authorize_table_catalog_resource_request(
            &request,
            &TableCatalogResource::warehouse(SCOPED_WAREHOUSE),
            AdminAction::MigrateTableCatalogAction,
        )
        .await
        .expect("the scoped policy allows its own warehouse");
        assert!(!principal.owner, "scope checks must use the regular user rather than owner bypass");
        let response = call_catalog_migration_scope_handler(request, prefix, operation)
            .await
            .expect("authorized warehouse-index backfill succeeds");
        assert_eq!(response.into_parts().0, StatusCode::NO_CONTENT);

        for warehouse in [FOREIGN_WAREHOUSE, UNMARKED_WAREHOUSE] {
            let before = fixture.inventory().await;
            let request = fixture.request(prefix, warehouse, operation, b"{body must not be consumed");
            let error = call_catalog_migration_scope_handler(request, prefix, operation)
                .await
                .expect_err("a different warehouse must be denied before marker or store access");
            assert_eq!(error.code(), &S3ErrorCode::AccessDenied, "{prefix} {warehouse}");
            assert_eq!(error.status_code(), Some(StatusCode::FORBIDDEN), "{prefix} {warehouse}");
            assert_eq!(fixture.inventory().await, before, "denied backfill changed persistent state");
        }
    }
}

#[tokio::test]
#[serial_test::serial]
async fn catalog_backup_restore_enforce_migration_scope_and_replay_own_receipts() {
    let fixture = CatalogMigrationScopeFixture::new().await;
    for (index, prefix) in [TABLE_CATALOG_PREFIX, TABLE_CATALOG_COMPAT_PREFIX].into_iter().enumerate() {
        let request = fixture.request(prefix, SCOPED_WAREHOUSE, CatalogMigrationScopeOperation::Backup, b"{}");
        let principal = table_catalog_request_principal(&request)
            .await
            .expect("authenticate the scoped backup caller");
        assert!(!principal.owner, "backup and restore must not use owner bypass");
        let response = call_catalog_migration_scope_handler(request, prefix, CatalogMigrationScopeOperation::Backup)
            .await
            .expect("the scoped user can create its own object catalog backup");
        let backup = catalog_migration_scope_response_json(response).await;
        assert_eq!(backup["table_bucket"], SCOPED_WAREHOUSE);
        assert!(matches!(backup["status"].as_str(), Some("CREATED" | "ALREADY_PRESENT")));
        assert!(backup["object_count"].as_u64().is_some_and(|count| count > 0));
        let backup_id = backup["backup_id"].as_str().expect("successful backup id").to_string();
        assert_eq!(backup_id.len(), 64);
        assert!(
            backup_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        let watermark = backup["source_snapshot_etag"]
            .as_str()
            .expect("successful backup source watermark");
        assert!(!watermark.is_empty());
        let restore_id = format!("scoped-handler-restore-{index}");
        let restore_body = serde_json::to_vec(&serde_json::json!({
            "backup-id": backup_id,
            "restore-id": restore_id,
            "expected-snapshot-etag": watermark,
            "allow-replace": false
        }))
        .expect("bind the restore request to the successful backup and explicit restore id");
        let request = fixture.request(prefix, SCOPED_WAREHOUSE, CatalogMigrationScopeOperation::Restore, &restore_body);
        let response = call_catalog_migration_scope_handler(request, prefix, CatalogMigrationScopeOperation::Restore)
            .await
            .expect("the scoped user can restore the unchanged catalog from its own backup");
        let restored = catalog_migration_scope_response_json(response).await;
        assert_eq!(restored["table_bucket"], SCOPED_WAREHOUSE);
        assert_eq!(restored["backup_id"], backup_id);
        assert_eq!(restored["status"], "RESTORED");
        assert_eq!(restored["object_count"], backup["object_count"]);
        assert_eq!(restored["maintenance_object_count"], backup["maintenance_object_count"]);

        let setup = table_catalog_handler_request(fixture.context.clone(), SCOPED_USER, SCOPED_SECRET);
        let backend = table_catalog_backend_from_envelope(setup.envelope()).expect("resolve restore control backend");
        let state_path = crate::table_catalog::TableCatalogObjectPaths::default().catalog_restore_state_path(SCOPED_WAREHOUSE);
        let barrier = backend
            .read_object(RUSTFS_META_BUCKET, &state_path)
            .await
            .expect("read identified restore barrier")
            .expect("identified restore barrier exists");
        let barrier: serde_json::Value = serde_json::from_slice(&barrier.data).expect("identified restore barrier JSON");
        assert_eq!(barrier["restore_id"], restore_id, "handler did not forward the explicit restore id");
        assert_eq!(barrier["backup_id"], backup_id);
        assert_eq!(barrier["state"], "COMPLETED");

        let before_replay = fixture.inventory().await;
        let request = fixture.request(prefix, SCOPED_WAREHOUSE, CatalogMigrationScopeOperation::Restore, &restore_body);
        let response = call_catalog_migration_scope_handler(request, prefix, CatalogMigrationScopeOperation::Restore)
            .await
            .expect("the same restore id replays the completed receipt");
        let mut replay = catalog_migration_scope_response_json(response).await;
        assert_eq!(replay["status"], "ALREADY_RESTORED");
        replay["status"] = restored["status"].clone();
        assert_eq!(replay, restored, "replay changed the original receipt");
        assert_eq!(fixture.inventory().await, before_replay, "completed replay changed persistent state");

        for warehouse in [FOREIGN_WAREHOUSE, UNMARKED_WAREHOUSE] {
            for operation in [
                CatalogMigrationScopeOperation::Backup,
                CatalogMigrationScopeOperation::Restore,
            ] {
                let before = fixture.inventory().await;
                let request = fixture.request(prefix, warehouse, operation, b"{body must not be consumed");
                let error = call_catalog_migration_scope_handler(request, prefix, operation)
                    .await
                    .expect_err("foreign warehouse must fail authorization before malformed JSON or marker checks");
                assert_eq!(error.code(), &S3ErrorCode::AccessDenied, "{prefix} {warehouse} {operation:?}");
                assert_eq!(error.status_code(), Some(StatusCode::FORBIDDEN), "{prefix} {warehouse} {operation:?}");
                assert_eq!(fixture.inventory().await, before, "foreign backup/restore changed persistent state");
            }
        }
    }
}
