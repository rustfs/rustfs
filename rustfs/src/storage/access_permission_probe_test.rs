// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Table permission probes use the real catalog resource and IAM, without publishing objects.

use super::{
    Action, AuthorizationRequest, ECStore, ReqInfo, RequestedObjectLockRetention, S3Action, S3ErrorCode, authorize_request_core,
};
use crate::app::{RequestCredentials, RequestEnvelope, RequestHead};
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::storage::storage_api::contract::object::ObjectOperations as _;
use crate::storage::storage_api::runtime_sources_consumer::{AppContext, IamInterface, ServerContextSlot};
use crate::table_catalog::{
    Namespace, ObjectTableCatalogStore, TABLE_BUCKET_CATALOG_TYPE, TABLE_BUCKET_MARKER_CONFIG, TABLE_CATALOG_ENTRY_VERSION,
    TableBucketEntry, TableCatalogEntryState, TableCatalogObjectBackend as _, TableCatalogObjectPaths, TableCatalogStore as _,
    TableEntry, default_table_metadata_file_path, synthetic_namespace_entry, table_bucket_marker_json,
};
use http::{HeaderMap, Method};
use rustfs_credentials::Credentials;
use rustfs_iam::{
    store::{Store as _, UserType, object::ObjectStore},
    sys::IamSys,
};
use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
use rustfs_policy::policy::Policy;
use serde_json::{Value, json};
use std::sync::Arc;

struct ReadyIam(Arc<IamSys<ObjectStore>>);

impl IamInterface for ReadyIam {
    fn handle(&self) -> Arc<IamSys<ObjectStore>> {
        self.0.clone()
    }

    fn is_ready(&self) -> bool {
        true
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    store: Arc<ECStore>,
    iam: Arc<IamSys<ObjectStore>>,
    envelope: RequestEnvelope,
    cred: Credentials,
    bucket: String,
    object: String,
}

impl Fixture {
    async fn new() -> Self {
        let (directory, _paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        ObjectStore::new(store.clone())
            .save_iam_config(
                json!({"version": 1}),
                format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
            )
            .await
            .expect("seed isolated IAM format");
        let iam = rustfs_iam::build_iam_sys(store.clone())
            .await
            .expect("initialize isolated IAM");
        iam.create_user(
            "table-probe-user",
            &AddOrUpdateUserReq {
                secret_key: "table-probe-fixture-secret".into(),
                policy: None,
                status: AccountStatus::Enabled,
            },
        )
        .await
        .expect("create ordinary permission caller");
        let cred = iam.get_user("table-probe-user").await.expect("resolve caller").credentials;
        let bucket = format!("table-probe-{}", uuid::Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create table bucket");
        store
            .update_bucket_metadata_config(
                &bucket,
                TABLE_BUCKET_MARKER_CONFIG,
                table_bucket_marker_json().expect("serialize table marker"),
            )
            .await
            .expect("enable table bucket");
        let backend = crate::table_catalog::EcStoreTableCatalogObjectBackend::new(store.clone());
        let catalog = ObjectTableCatalogStore::new(backend);
        catalog
            .put_table_bucket(TableBucketEntry {
                version: TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: bucket.clone(),
                catalog_type: TABLE_BUCKET_CATALOG_TYPE.into(),
                warehouse_root: format!("s3://{bucket}/"),
                state: TableCatalogEntryState::Active,
                properties: Default::default(),
                active_rename_id: None,
                created_at: None,
                updated_at: None,
            })
            .await
            .expect("create catalog warehouse");
        let namespace = Namespace::parse("sales").expect("namespace");
        let table = crate::table_catalog::IdentifierSegment::parse("orders").expect("table name");
        catalog
            .create_namespace(synthetic_namespace_entry(&bucket, &namespace))
            .await
            .expect("create catalog namespace");
        catalog
            .create_table(TableEntry {
                version: TABLE_CATALOG_ENTRY_VERSION,
                table_bucket: bucket.clone(),
                namespace: namespace.public_name(),
                table: table.as_str().into(),
                table_id: "table-id".into(),
                table_uuid: "table-uuid".into(),
                format: "ICEBERG".into(),
                format_version: 2,
                warehouse_location: format!("s3://{bucket}/tables/table-id"),
                metadata_location: default_table_metadata_file_path(&namespace, &table, "00001.metadata.json"),
                version_token: "token-v1".into(),
                generation: 1,
                state: TableCatalogEntryState::Active,
                properties: Default::default(),
                created_at: None,
                updated_at: None,
            })
            .await
            .expect("register table warehouse location");
        let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
        let slot = ServerContextSlot::new();
        assert!(slot.install(Arc::new(AppContext::new(store.clone(), Arc::new(ReadyIam(iam.clone())), ambient.kms()))));
        let mut extensions = http::Extensions::new();
        extensions.insert(slot);
        let envelope = RequestEnvelope::from_parts(
            RequestHead {
                method: Method::POST,
                uri: "/rustfs/admin/v4/account/permissions".parse().expect("query URI"),
                headers: HeaderMap::new(),
                credentials: Some(RequestCredentials::without_secret_key(cred.access_key.clone(), None)),
                region: None,
                service: None,
            },
            extensions,
        );
        Self {
            _directory: directory,
            store,
            iam,
            envelope,
            cred,
            bucket,
            object: "tables/table-id/data/part-00001.parquet".into(),
        }
    }

    fn table_statement(&self, effect: &str, condition: Option<Value>) -> Value {
        let mut statement = json!({
            "Effect": effect,
            "Action": ["admin:GetTableMetadata", "admin:SetTableMetadata"],
            "Resource": format!("arn:aws:s3:::{}/namespaces/sales/tables/orders", self.bucket)
        });
        if let Some(condition) = condition {
            statement["Condition"] = condition;
        }
        statement
    }

    async fn set_policy(&self, table_statements: Vec<Value>) {
        let mut statements = vec![json!({
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:PutObjectRetention"],
            "Resource": format!("arn:aws:s3:::{}/*", self.bucket)
        })];
        statements.extend(table_statements);
        let body = serde_json::to_vec(&json!({"Version": "2012-10-17", "Statement": statements})).expect("policy JSON");
        let policy = Policy::parse_config(&body).expect("table permission policy");
        self.iam
            .set_policy("table-probe-policy", policy)
            .await
            .expect("store permission policy");
        self.iam
            .policy_db_set(&self.cred.access_key, UserType::Reg, false, "table-probe-policy")
            .await
            .expect("attach permission policy");
    }

    fn probe(&self, action: S3Action) -> RequestEnvelope {
        let method = if action == S3Action::GetObjectAction {
            Method::GET
        } else {
            Method::PUT
        };
        self.envelope.fork_for_permission_probe(
            method,
            format!("/{}/{}", self.bucket, self.object).parse().expect("object URI"),
            ReqInfo {
                cred: Some(self.cred.clone()),
                bucket: Some(self.bucket.clone()),
                object: Some(self.object.clone()),
                suppress_denial_log: true,
                suppress_denial_audit: true,
                ..Default::default()
            },
        )
    }
}

#[test]
fn table_permission_probe_uses_resolved_resources_and_three_state_conditions() {
    crate::app::gating_test_env::run_large_stack_test("table-permission-conditions", || async {
        let fixture = Fixture::new().await;
        let backend = crate::table_catalog::EcStoreTableCatalogObjectBackend::new(fixture.store.clone());
        let index_state = TableCatalogObjectPaths::default().warehouse_index_state_path(&fixture.bucket);
        let catalog_bucket = crate::storage::storage_api::ecstore_disk::RUSTFS_META_BUCKET;
        assert!(
            backend
                .read_object(catalog_bucket, &index_state)
                .await
                .expect("read cold warehouse index state")
                .is_none(),
            "the fixture must exercise a cold index"
        );
        let unknown = fixture.table_statement("Allow", Some(json!({"Null": {"aws:UserAgent": "true"}})));
        fixture.set_policy(vec![unknown.clone()]).await;
        for action in [S3Action::GetObjectAction, S3Action::PutObjectAction] {
            let mut probe = fixture.probe(action);
            let error = authorize_request_core(&mut probe, Action::S3Action(action), None, false)
                .await
                .expect_err("unknown table condition must not authorize the S3 object");
            assert_eq!(error.code(), &S3ErrorCode::NotImplemented);
            assert!(probe.publication_guards().is_none(), "permission probes cannot retain publication locks");
        }
        fixture
            .set_policy(vec![unknown.clone(), fixture.table_statement("Deny", None)])
            .await;
        let error = authorize_request_core(
            &mut fixture.probe(S3Action::GetObjectAction),
            Action::S3Action(S3Action::GetObjectAction),
            None,
            false,
        )
        .await
        .expect_err("known table deny resolves the unknown grant");
        assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
        assert!(
            backend
                .read_object(catalog_bucket, &index_state)
                .await
                .expect("read index state after queries")
                .is_none(),
            "permission queries must not backfill or persist warehouse index state"
        );
        fixture
            .set_policy(vec![unknown, fixture.table_statement("Allow", None)])
            .await;
        authorize_request_core(
            &mut fixture.probe(S3Action::GetObjectAction),
            Action::S3Action(S3Action::GetObjectAction),
            None,
            false,
        )
        .await
        .expect("known grant for the resolved catalog table authorizes the S3 object");
        let mut other_table = fixture.table_statement("Allow", None);
        other_table["Resource"] = json!(format!("arn:aws:s3:::{}/namespaces/sales/tables/other", fixture.bucket));
        fixture.set_policy(vec![other_table]).await;
        let error = authorize_request_core(
            &mut fixture.probe(S3Action::GetObjectAction),
            Action::S3Action(S3Action::GetObjectAction),
            None,
            false,
        )
        .await
        .expect_err("a different table resource cannot authorize the resolved owner");
        assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
    });
}

#[test]
fn table_permission_probe_preserves_known_retention_absence() {
    crate::app::gating_test_env::run_large_stack_test("table-permission-retention", || async {
        let fixture = Fixture::new().await;
        fixture
            .set_policy(vec![
                fixture.table_statement("Allow", Some(json!({"Null": {"s3:object-lock-retain-until-date": "true"}}))),
            ])
            .await;
        let action = Action::S3Action(S3Action::PutObjectRetentionAction);
        let mut reset = fixture.probe(S3Action::PutObjectRetentionAction);
        reset.install_requested_retention(RequestedObjectLockRetention {
            mode: Some("GOVERNANCE".into()),
            retain_until: None,
        });
        authorize_request_core(&mut reset, action, None, false)
            .await
            .expect("the declared retention reset is known absent for the mapped table gate");
        let error = authorize_request_core(&mut fixture.probe(S3Action::PutObjectRetentionAction), action, None, false)
            .await
            .expect_err("omitted retention remains an unknown table condition");
        assert_eq!(error.code(), &S3ErrorCode::NotImplemented);
        let mut set_date = fixture.probe(S3Action::PutObjectRetentionAction);
        set_date.install_requested_retention(RequestedObjectLockRetention {
            mode: Some("GOVERNANCE".into()),
            retain_until: Some(time::OffsetDateTime::now_utc() + time::Duration::days(1)),
        });
        let error = authorize_request_core(&mut set_date, action, None, false)
            .await
            .expect_err("a supplied retention date does not satisfy known absence");
        assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
        assert!(
            fixture
                .store
                .get_object_info(&fixture.bucket, &fixture.object, &Default::default())
                .await
                .is_err(),
            "queries cannot publish table data"
        );
    });
}
