// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn deleted_action(fixture: &SdkWorkerFixture, resource: VectorMaintenanceResourceV1, cursor: Option<String>) -> Action {
    Action::VectorDeletedInspect {
        runtime_token: fixture.runtime_token.clone(),
        resource,
        cursor,
    }
}

#[test]
#[serial_test::serial]
fn deleted_observation_rejects_active_changed_identity_and_unknown_cursor() {
    run_large_stack_test("deleted-resource-observation", || async {
        let fixture = SdkWorkerFixture::new().await;
        assert!(
            call(&fixture.worker, deleted_action(&fixture, fixture.resource.clone(), None))
                .await
                .is_err()
        );
        fixture
            .client
            .delete_index()
            .vector_bucket_name(&fixture.bucket)
            .index_name("worker.index")
            .send()
            .await
            .expect("delete the original Index through SDK");
        let observed = call(&fixture.worker, deleted_action(&fixture, fixture.resource.clone(), None))
            .await
            .expect("inspect exact Deleted resource");
        assert_eq!(observed["state"], "DELETED");
        assert_eq!(observed["resource"], serde_json::to_value(&fixture.resource).expect("resource JSON"));
        assert_eq!(observed["source_intent"], observed["observed_object"]);
        assert!(!observed["history"].as_array().expect("original history").is_empty());
        assert!(observed["next_cursor"].is_null());
        assert!(
            call(
                &fixture.worker,
                Action::VectorInspect {
                    runtime_token: fixture.runtime_token.clone(),
                    resource: fixture.resource.clone(),
                    cursor: None,
                }
            )
            .await
            .is_err(),
            "the original live-resource inspector must retain its admission rule"
        );
        for field in ["table_id", "bucket_incarnation"] {
            let mut changed = serde_json::to_value(&fixture.resource).expect("resource JSON");
            if field == "table_id" {
                changed[field] = json!(Uuid::new_v4());
            } else {
                changed["bucket"][field] = json!(Uuid::new_v4());
            }
            let changed = serde_json::from_value(changed).expect("syntactically valid changed identity");
            assert!(call(&fixture.worker, deleted_action(&fixture, changed, None)).await.is_err());
        }
        assert!(
            call(
                &fixture.worker,
                deleted_action(&fixture, fixture.resource.clone(), Some("unissued".into()))
            )
            .await
            .is_err()
        );
        let after = call(&fixture.worker, deleted_action(&fixture, fixture.resource.clone(), None))
            .await
            .expect("original Deleted evidence remains readable");
        for field in ["resource", "source_intent", "observed_object", "current", "history", "jobs"] {
            assert_eq!(observed[field], after[field], "read-only observation changed {field}");
        }
    });
}

#[test]
#[serial_test::serial]
fn deleted_observation_rechecks_current_policy() {
    run_large_stack_test("deleted-resource-policy", || async {
        let fixture = SdkWorkerFixture::new().await;
        fixture
            .client
            .delete_index()
            .vector_bucket_name(&fixture.bucket)
            .index_name("worker.index")
            .send()
            .await
            .expect("delete the original Index");
        call(&fixture.worker, deleted_action(&fixture, fixture.resource.clone(), None))
            .await
            .expect("authorized Deleted observation");
        let bucket = fixture.resource.bucket().to_handle().expect("original bucket handle");
        let policy = json!({"Version":"2012-10-17","Statement":[{
            "Effect":"Deny","Principal":"*","Action":"s3vectors:DeleteVectors",
            "Resource":format!("arn:aws:s3vectors:{}::bucket/{}/index/worker.index",bucket.region(),fixture.bucket)
        }]})
        .to_string();
        fixture
            .client
            .put_vector_bucket_policy()
            .vector_bucket_name(&fixture.bucket)
            .policy(policy)
            .send()
            .await
            .expect("deny the original observation account");
        let error = call(&fixture.worker, deleted_action(&fixture, fixture.resource.clone(), None))
            .await
            .expect_err("current resource policy must deny the same observation");
        assert!(
            matches!(&error,
                WorkerError::Maintenance(VectorMaintenanceExecutionError::Catalog(TableCatalogStoreError::Io {
                    classification: CatalogIoErrorKind::Denied,
                    context: "authorize Vector maintenance preparation",
                    source,
                })) if source.downcast_ref::<crate::error::ApiError>().is_some_and(|error|
                    error.code == rustfs_s3_types::S3ErrorCode::AccessDenied)
            ),
            "expected the original typed maintenance policy denial and AccessDenied source: {error:?}",
        );
    });
}

#[test]
#[serial_test::serial]
fn table_sentinel_reads_store_and_rejects_changed_lifetime() {
    run_large_stack_test("table-sentinel-observation", || async {
        let fixture = SdkWorkerFixture::new().await;
        let created = call(&fixture.worker, Action::TableSentinelCreate)
            .await
            .expect("create actual Table sentinel");
        let token = created["sentinel_token"].as_str().expect("sentinel token");
        let read = call(
            &fixture.worker,
            Action::TableSentinelRead {
                sentinel_token: token.into(),
            },
        )
        .await
        .expect("reread Table sentinel from its store");
        assert_eq!(read, created["observation"]);
        assert_eq!(read["kind"], "Table");
        assert!(read["parent"]["vector_bucket_binding"].is_null());
        assert_eq!(read["entry"]["format"], "ICEBERG");
        assert_eq!(read["objects"].as_array().expect("actual objects").len(), 5);
        let other = Worker {
            store: fixture.worker.store.clone(),
            server_ctx: fixture.worker.server_ctx.clone(),
            node_id: "table-sentinel-observer".into(),
            nonce: Uuid::new_v4().to_string(),
            boot_id: Uuid::new_v4(),
            started: Instant::now(),
            state: Mutex::default(),
            shutdown: CancellationToken::new(),
            vector_http: tokio::sync::Mutex::new(None),
        };
        assert!(
            call(
                &other,
                Action::TableSentinelRead {
                    sentinel_token: token.into()
                }
            )
            .await
            .is_err()
        );
        let reopened = call(
            &other,
            Action::TableSentinelOpen {
                identity: serde_json::from_value(read["identity"].clone()).expect("observed identity"),
            },
        )
        .await
        .expect("open only after independent persisted identity validation");
        assert_eq!(reopened["observation"], read);
        for field in ["bucket_incarnation", "fixture_id"] {
            let mut identity = read["identity"].clone();
            identity[field] = json!(Uuid::new_v4());
            assert!(
                call(
                    &fixture.worker,
                    Action::TableSentinelOpen {
                        identity: serde_json::from_value(identity).expect("changed identity"),
                    }
                )
                .await
                .is_err()
            );
        }
        assert!(
            call(
                &fixture.worker,
                Action::TableSentinelRead {
                    sentinel_token: "unissued".into()
                }
            )
            .await
            .is_err()
        );
        assert_eq!(
            call(
                &fixture.worker,
                Action::TableSentinelRead {
                    sentinel_token: token.into()
                }
            )
            .await
            .expect("unchanged original Table remains readable"),
            read
        );
    });
}

#[test]
#[serial_test::serial]
fn table_sentinel_missing_data_is_an_error() {
    run_large_stack_test("table-sentinel-missing-data", || async {
        let fixture = SdkWorkerFixture::new().await;
        let created = call(&fixture.worker, Action::TableSentinelCreate)
            .await
            .expect("create actual Table sentinel");
        let observation = &created["observation"];
        let bucket = observation["identity"]["storage_bucket_id"]
            .as_str()
            .expect("owned storage bucket");
        let data = observation["objects"]
            .as_array()
            .expect("observed objects")
            .iter()
            .find(|object| object["key"].as_str().is_some_and(|key| key.ends_with("/sentinel.bin")))
            .expect("exact observed sentinel data");
        assert_eq!(data["bucket"], bucket);
        // Remove only the owned sentinel data to exercise missing-evidence rejection.
        EcStoreTableCatalogObjectBackend::new(fixture.worker.store.clone())
            .delete_object(bucket, data["key"].as_str().expect("exact data key"))
            .await
            .expect("remove the selected sentinel data object");
        let error = call(
            &fixture.worker,
            Action::TableSentinelRead {
                sentinel_token: created["sentinel_token"].as_str().expect("sentinel token").into(),
            },
        )
        .await
        .expect_err("missing actual data cannot be replaced with remembered bytes");
        assert!(matches!(error, WorkerError::Invalid(ref message) if message == "Table sentinel data evidence is missing"));
    });
}
