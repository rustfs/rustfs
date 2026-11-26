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

//! Tag- and condition-dependent decisions of the owned object authorization functions.
//!
//! Responsible for: the tag evidence a local miss leaves for the replication proxy, the read
//! permission a specific `If-Match` write requires, the version permission an include-deleted
//! listing requires, and the source and destination tag decisions of CopyObject, including the
//! fresh decision the copy use case makes against the source it actually reads.
//! Not responsible for: bucket-generation snapshots (see `generation_guards.rs`), or the policy
//! evaluation itself.
//! Upstream: the owned request fixture in `storage::authorization::tests::fixture`.
//! Downstream: `storage::access_owned::{object_read, object_meta, object_write, object_commit,
//! listing_multipart}` and the CopyObject use case.

use crate::app::RequestEnvelope;
use crate::app::object_usecase::{CopyObjectWriteInput, DefaultObjectUsecase};
use crate::storage::access_owned::listing_multipart::authorize_bucket_listing;
use crate::storage::access_owned::object_commit::{
    CopyObjectAuthorization, CopySourceObject, authorize_complete_multipart_upload, authorize_copy_object,
};
use crate::storage::access_owned::object_meta::{authorize_object_tagging_request, authorize_put_object_tagging};
use crate::storage::access_owned::object_read::{authorize_get_object, authorize_head_object};
use crate::storage::access_owned::object_write::{PutObjectAuthorization, authorize_put_object};
use crate::storage::authorization::tests::fixture::{TestKms, UnreadyIam, request, server_context};
use crate::storage::req_info::ReqInfo;
use crate::storage::request_markers::RequestedObjectTags;
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use crate::storage::storage_api::contract::object::{ObjectIO as _, ObjectOperations as _};
use crate::storage::storage_api::runtime_sources_consumer::{AppContext, IamInterface, ServerContextSlot};
use http::Method;
use rustfs_gateway_types::CopySourceValue;
use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
use rustfs_policy::policy::action::S3Action;
use rustfs_s3_types::S3ErrorCode;
use serial_test::serial;
use std::{collections::HashMap, sync::Arc};

/// The copy use case input the legacy edge built from a CopyObject request that carries only
/// these fields.
fn copy_write_input(
    bucket: &str,
    key: &str,
    copy_source: CopySourceValue,
    metadata_directive: Option<&str>,
    tagging: Option<&str>,
    tagging_directive: Option<&str>,
) -> CopyObjectWriteInput {
    CopyObjectWriteInput {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        copy_source: Some(copy_source),
        version_id: None,
        specific_if_none_match: false,
        server_side_encryption: None,
        ssekms_key_id: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        copy_source_sse_customer_algorithm: None,
        copy_source_sse_customer_key: None,
        copy_source_sse_customer_key_md5: None,
        metadata_directive: metadata_directive.map(str::to_owned),
        metadata: None,
        tagging: tagging.map(str::to_owned),
        tagging_directive: tagging_directive.map(str::to_owned),
        copy_source_if_match: None,
        copy_source_if_none_match: None,
        copy_source_dates: Default::default(),
        cache_control: None,
        content_disposition: None,
        content_encoding: None,
        content_language: None,
        content_type: None,
        expires: None,
        website_redirect_location: None,
        object_lock_legal_hold_status: None,
        object_lock_mode: None,
        object_lock_retain_until_date: None,
        storage_class: None,
        checksum_algorithm: None,
    }
}

/// CopyObject's authorization facts for a request naming only a bucket-path source, tags and
/// a tagging directive.
fn copy_facts<'a>(
    bucket: &'a str,
    key: &'a str,
    source: CopySourceObject<'a>,
    tagging: Option<&'a str>,
    tagging_directive: Option<&'a str>,
) -> CopyObjectAuthorization<'a> {
    CopyObjectAuthorization {
        bucket,
        key,
        source: Some(source),
        query_version: None,
        tagging,
        tagging_directive,
        retain_until: None,
        legal_hold: false,
        retention: false,
    }
}

#[tokio::test]
#[serial]
async fn tag_dependent_access_hooks_preserve_local_miss_for_replication_proxy() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let slot = server_context(Arc::clone(&store));
    let bucket = format!("tag-proxy-boundary-{}", uuid::Uuid::new_v4());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket");
    let policy_json = format!(
        r#"{{"Version":"2012-10-17","Statement":[
            {{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:GetObject","s3:GetObjectTagging","s3:PutObjectTagging","s3:DeleteObjectTagging"],"Resource":["arn:aws:s3:::{bucket}/*"]}},
            {{"Effect":"Deny","Principal":{{"AWS":"*"}},"Action":["s3:GetObject","s3:GetObjectTagging","s3:PutObjectTagging","s3:DeleteObjectTagging"],"Resource":["arn:aws:s3:::{bucket}/*"],"Condition":{{"StringEquals":{{"s3:ExistingObjectTag/protected":"yes"}}}}}}
        ]}}"#
    );
    let policy_bytes = policy_json.into_bytes();
    store
        .update_bucket_metadata_config(
            &bucket,
            crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
            policy_bytes,
        )
        .await
        .expect("publish policy");
    let check = |envelope: &RequestEnvelope| {
        let evidence = envelope
            .object_tag_conditions()
            .expect("authorization must retain tag dependency");
        assert!(evidence.values.is_empty(), "missing local object has no tags");
        assert!(
            !crate::app::object_meta::tagging::proxy_allowed(envelope),
            "local absence cannot authorize a tagged replica"
        );
    };

    let mut envelope = request(Method::GET).req_info().server_context(&slot).build();
    authorize_get_object(&slot, &mut envelope, &bucket, "missing", None)
        .await
        .expect("local miss does not match the tag deny");
    check(&envelope);

    let mut envelope = request(Method::HEAD).req_info().server_context(&slot).build();
    authorize_head_object(&slot, &mut envelope, &bucket, "missing", None)
        .await
        .expect("local miss does not match the tag deny");
    check(&envelope);

    let mut envelope = request(Method::GET).req_info().server_context(&slot).build();
    authorize_object_tagging_request(&mut envelope, &bucket, "missing", None, S3Action::GetObjectTaggingAction)
        .await
        .expect("local miss does not match the tag deny");
    check(&envelope);

    let mut envelope = request(Method::DELETE).req_info().server_context(&slot).build();
    authorize_object_tagging_request(&mut envelope, &bucket, "missing", None, S3Action::DeleteObjectTaggingAction)
        .await
        .expect("local miss does not match the tag deny");
    check(&envelope);

    let mut envelope = request(Method::PUT).req_info().server_context(&slot).build();
    authorize_put_object_tagging(&mut envelope, &slot, &bucket, "missing", None, RequestedObjectTags(Vec::new()))
        .await
        .expect("local miss does not match the tag deny");
    check(&envelope);
}

#[tokio::test]
#[serial]
async fn conditional_writes_require_read_permission_only_for_specific_etags() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let slot = server_context(Arc::clone(&store));
    for readable in [false, true] {
        let bucket = format!("etag-oracle-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        let read_effect = if readable { "Allow" } else { "Deny" };
        let policy_json = format!(
            r#"{{"Version":"2012-10-17","Statement":[
                {{"Effect":"Allow","Principal":{{"AWS":"*"}},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::{bucket}/*"]}},
                {{"Effect":"{read_effect}","Principal":{{"AWS":"*"}},"Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":["arn:aws:s3:::{bucket}/*"]}}
            ]}}"#
        );
        let policy_bytes = policy_json.into_bytes();
        store
            .update_bucket_metadata_config(
                &bucket,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                policy_bytes,
            )
            .await
            .expect("publish policy");
        for condition in ["absent", "wildcard", "dto", "header"] {
            let specific = matches!(condition, "dto" | "header");
            // The decoded `If-Match` value is specific only for a strong ETag; the wildcard and
            // the header-only case leave the decoded value non-specific.
            let specific_dto_etag = condition == "dto";
            let mut put = request(Method::PUT).req_info().server_context(&slot);
            let mut complete = request(Method::POST).req_info().server_context(&slot);
            if condition == "header" {
                put = put.header("if-match", "\"guessed-etag\"");
                complete = complete.header("if-match", "\"guessed-etag\"");
            }
            let mut put = put.build();
            let mut complete = complete.build();
            let put_facts = PutObjectAuthorization {
                bucket: &bucket,
                key: "object",
                version_id: None,
                retain_until: None,
                specific_etag: specific_dto_etag,
                legal_hold: false,
                retention: false,
                post: false,
            };
            for result in [
                authorize_put_object(&mut put, &slot, put_facts, |_| Ok(())).await,
                authorize_complete_multipart_upload(&mut complete, &slot, &bucket, "object", specific_dto_etag).await,
            ] {
                if specific && !readable {
                    assert_eq!(
                        result
                            .expect_err("a write-only caller must not compare content fingerprints")
                            .code(),
                        &S3ErrorCode::AccessDenied
                    );
                } else {
                    result.expect("readable or wildcard/ordinary writes remain authorized");
                }
            }
        }
    }
}

#[tokio::test]
#[serial]
async fn include_deleted_listing_enforces_version_permissions() {
    use rustfs_iam::store::Store as _;
    use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
    struct ReadyIam(Arc<IamSys<ObjectStore>>);
    impl IamInterface for ReadyIam {
        fn handle(&self) -> Arc<IamSys<ObjectStore>> {
            self.0.clone()
        }
        fn is_ready(&self) -> bool {
            true
        }
    }
    let (_directory, _paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
    ObjectStore::new(store.clone())
        .save_iam_config(
            serde_json::json!({"version":1}),
            format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
        )
        .await
        .expect("seed IAM format");
    let iam = rustfs_iam::build_iam_sys(store.clone()).await.expect("initialize IAM");
    iam.create_user(
        "list-reader",
        &AddOrUpdateUserReq {
            secret_key: "list-reader-secret".to_string(),
            policy: None,
            status: AccountStatus::Enabled,
        },
    )
    .await
    .expect("create reader");
    let user = iam.get_user("list-reader").await.expect("reader identity").credentials;
    iam.set_policy(
        "list-only",
        rustfs_policy::policy::Policy::parse_config(
            br#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::*"}]}"#,
        )
        .expect("list policy"),
    )
    .await
    .expect("store identity policy");
    iam.policy_db_set("list-reader", rustfs_iam::store::UserType::Reg, false, "list-only")
        .await
        .expect("attach list policy");
    let bucket = format!("deleted-list-auth-{}", uuid::Uuid::new_v4());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket");
    let slot = ServerContextSlot::new();
    assert!(slot.install(Arc::new(AppContext::new(Arc::clone(&store), Arc::new(ReadyIam(iam)), Arc::new(TestKms)))));
    for (grant_versions, deny_versions) in [(false, false), (true, false), (true, true)] {
        let mut statements = vec![serde_json::json!({
            "Effect": "Allow", "Principal": {"AWS": "*"}, "Action": ["s3:ListBucket"], "Resource": format!("arn:aws:s3:::{bucket}")
        })];
        if grant_versions {
            statements.push(serde_json::json!({
                "Effect": "Allow", "Principal": {"AWS": "*"}, "Action": ["s3:ListBucketVersions"], "Resource": format!("arn:aws:s3:::{bucket}")
            }));
        }
        if deny_versions {
            statements.push(serde_json::json!({
                "Effect": "Deny", "Principal": {"AWS": "*"}, "Action": ["s3:ListBucketVersions"], "Resource": format!("arn:aws:s3:::{bucket}")
            }));
        }
        let policy = serde_json::json!({"Version": "2012-10-17", "Statement": statements});
        store
            .update_bucket_metadata_config(
                &bucket,
                crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                serde_json::to_vec(&policy).unwrap(),
            )
            .await
            .expect("publish listing policy");
        for header in ["x-rustfs-include-deleted", "x-minio-include-deleted"] {
            for (value, authenticated) in [("true", true), ("false", true), ("true", false), ("false", false)] {
                let mut envelope = request(Method::GET)
                    .server_context(&slot)
                    .extension(ReqInfo {
                        bucket: Some(bucket.clone()),
                        cred: authenticated.then(|| user.clone()),
                        ..Default::default()
                    })
                    .header(header, value)
                    .build();
                let result = authorize_bucket_listing(&mut envelope, &bucket, S3Action::ListBucketAction).await;
                if value == "true" && (deny_versions || (authenticated && !grant_versions)) {
                    assert_eq!(
                        result.expect_err("version visibility requires permission").code(),
                        &S3ErrorCode::AccessDenied,
                        "{header}={value}, authenticated={authenticated}"
                    );
                } else {
                    result.expect("authorized listings remain available");
                }
            }
        }
    }
}

#[test]
fn copy_tags_authorize_source_and_effective_destination_tags() {
    crate::app::gating_test_env::run_large_stack_test("copy-tag-authorization", || async {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let slot = ServerContextSlot::new();
        let context = Arc::new(AppContext::new(store.clone(), Arc::new(UnreadyIam), Arc::new(TestKms)));
        assert!(slot.install(context.clone()));
        let bucket = format!("copy-tags-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket");
        let mut reader = crate::storage::PutObjReader::from_vec(b"confidential source".to_vec());
        let source = store
            .put_object(
                &bucket,
                "source",
                &mut reader,
                &crate::storage::ObjectOptions {
                    versioned: true,
                    user_defined: HashMap::from([(
                        rustfs_utils::http::AMZ_OBJECT_TAGGING.to_string(),
                        "visibility=private".to_string(),
                    )]),
                    ..Default::default()
                },
            )
            .await
            .expect("tagged historical source");
        let historical = source.version_id.expect("versioned source").to_string();
        let mut reader = crate::storage::PutObjReader::from_vec(b"current source".to_vec());
        store
            .put_object(
                &bucket,
                "source",
                &mut reader,
                &crate::storage::ObjectOptions {
                    versioned: true,
                    user_defined: HashMap::from([(
                        rustfs_utils::http::AMZ_OBJECT_TAGGING.to_string(),
                        "visibility=private".to_string(),
                    )]),
                    ..Default::default()
                },
            )
            .await
            .expect("current tagged source");
        for (key, tags) in [("source-null", "visibility=private"), ("source-empty", "")] {
            let mut reader = crate::storage::PutObjReader::from_vec(b"unversioned source".to_vec());
            store
                .put_object(
                    &bucket,
                    key,
                    &mut reader,
                    &crate::storage::ObjectOptions {
                        user_defined: HashMap::from([(rustfs_utils::http::AMZ_OBJECT_TAGGING.to_string(), tags.to_string())]),
                        ..Default::default()
                    },
                )
                .await
                .expect("unversioned source");
        }
        for (index, (directive, replacement, explicit_version, tag_permission, deny_public, expected)) in [
            (None, None, None, "missing", false, false),
            (Some("COPY"), None, None, "deny", false, false),
            (None, None, None, "current", false, true),
            (Some("COPY"), None, Some("historical"), "current", false, false),
            (Some("COPY"), None, Some("historical"), "version", false, true),
            (Some("COPY"), None, Some("null"), "current", false, false),
            (Some("COPY"), None, Some("null"), "version", false, true),
            (None, None, None, "empty", false, true),
            (Some("REPLACE"), Some("visibility=private"), None, "deny", false, true),
            (Some("REPLACE"), Some(""), None, "deny", false, true),
            (Some("REPLACE"), Some("visibility=public"), None, "deny", true, false),
        ]
        .into_iter()
        .enumerate()
        {
            let mut statements = vec![
                serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":format!("arn:aws:s3:::{bucket}/source*")}),
                serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:PutObject","s3:PutObjectTagging"],"Resource":format!("arn:aws:s3:::{bucket}/copied-*")}),
            ];
            if !matches!(tag_permission, "missing" | "empty") {
                let action = if tag_permission == "version" {
                    "s3:GetObjectVersionTagging"
                } else {
                    "s3:GetObjectTagging"
                };
                statements.push(serde_json::json!({"Effect":if tag_permission == "deny" { "Deny" } else { "Allow" },"Principal":{"AWS":"*"},"Action":action,"Resource":format!("arn:aws:s3:::{bucket}/source*")}));
            }
            if deny_public {
                statements.push(serde_json::json!({"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{bucket}/*"),"Condition":{"StringEquals":{"s3:RequestObjectTag/visibility":"public"}}}));
            }
            let policy = serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":statements})).expect("policy");
            store
                .update_bucket_metadata_config(
                    &bucket,
                    crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                    policy,
                )
                .await
                .expect("policy publication");
            let destination = format!("copied-{index}");
            let source_key = if tag_permission == "empty" {
                "source-empty"
            } else if explicit_version == Some("null") {
                "source-null"
            } else {
                "source"
            };
            let source_version = explicit_version.map(|version| {
                if version == "historical" {
                    historical.clone()
                } else {
                    "null".to_owned()
                }
            });
            let mut envelope = request(Method::PUT).req_info().server_context(&slot).build();
            let facts = copy_facts(
                &bucket,
                &destination,
                CopySourceObject {
                    bucket: &bucket,
                    key: source_key,
                    version_id: source_version.as_deref(),
                },
                replacement,
                directive,
            );
            let access = authorize_copy_object(&mut envelope, &slot, facts).await;
            if !expected {
                assert_eq!(
                    access.expect_err("copy must enforce tagging and destination policy").code(),
                    &S3ErrorCode::AccessDenied,
                    "case {index}"
                );
                assert!(
                    store
                        .get_object_info(&bucket, &destination, &crate::storage::ObjectOptions::default())
                        .await
                        .is_err()
                );
                continue;
            }
            access.expect("authorized copy");
            let input = copy_write_input(
                &bucket,
                &destination,
                CopySourceValue::Bucket {
                    bucket: bucket.clone(),
                    key: source_key.to_owned(),
                    version_id: source_version,
                },
                None,
                replacement,
                directive,
            );
            DefaultObjectUsecase::with_context(Some(context.clone()))
                .execute_copy_object_owned(envelope, input)
                .await
                .expect("copy preserves allowed tags");
            assert_eq!(
                store
                    .get_object_tags(&bucket, &destination, &crate::storage::ObjectOptions::default())
                    .await
                    .expect("destination tags"),
                replacement.unwrap_or(if tag_permission == "empty" { "" } else { "visibility=private" })
            );
        }
    });
}

#[test]
fn copy_tags_rechecks_actual_source_after_access_snapshot() {
    crate::app::gating_test_env::run_large_stack_test("copy-tag-snapshot", || async {
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let slot = ServerContextSlot::new();
        let context = Arc::new(AppContext::new(store.clone(), Arc::new(UnreadyIam), Arc::new(TestKms)));
        assert!(slot.install(context.clone()));
        for mode in ["tag-change", "new-tags", "allowed-tags", "source-tag-change", "self-copy"] {
            let bucket = format!("copy-snapshot-{}", uuid::Uuid::new_v4());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket");
            let destination = if mode == "self-copy" { "source" } else { "destination" };
            let initial = if mode == "new-tags" { "" } else { "visibility=private" };
            let mut reader = crate::storage::PutObjReader::from_vec(b"source".to_vec());
            store
                .put_object(
                    &bucket,
                    "source",
                    &mut reader,
                    &crate::storage::ObjectOptions {
                        user_defined: HashMap::from([(rustfs_utils::http::AMZ_OBJECT_TAGGING.to_string(), initial.to_string())]),
                        ..Default::default()
                    },
                )
                .await
                .expect("source");
            let mut statements = vec![
                serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/source")}),
                serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:PutObject","s3:PutObjectTagging"],"Resource":format!("arn:aws:s3:::{bucket}/{destination}")}),
                serde_json::json!({"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:PutObject","Resource":format!("arn:aws:s3:::{bucket}/*"),"Condition":{"StringEquals":{"s3:RequestObjectTag/visibility":"public"}}}),
            ];
            if mode != "new-tags" {
                statements.push(serde_json::json!({"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObjectTagging","Resource":format!("arn:aws:s3:::{bucket}/source")}));
                // A conditional Allow must work at the preliminary hook as well.
                statements[1]["Condition"] = serde_json::json!({"StringEquals":{"s3:RequestObjectTag/visibility":"private"}});
                if mode == "source-tag-change" {
                    statements[1].as_object_mut().expect("statement").remove("Condition");
                    statements.remove(2);
                    statements.last_mut().expect("tag read statement")["Condition"] =
                        serde_json::json!({"StringEquals":{"s3:ExistingObjectTag/visibility":"private"}});
                } else if mode == "self-copy" {
                    statements[1]["Condition"]["StringEquals"]["s3:ExistingObjectTag/visibility"] = serde_json::json!("private");
                    statements.last_mut().expect("tag read statement")["Condition"] =
                        serde_json::json!({"StringEquals":{"s3:ExistingObjectTag/visibility":"private"}});
                }
            }
            store
                .update_bucket_metadata_config(
                    &bucket,
                    crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                    serde_json::to_vec(&serde_json::json!({"Version":"2012-10-17","Statement":statements})).expect("policy"),
                )
                .await
                .expect("publish policy");
            let mut envelope = request(Method::PUT).req_info().server_context(&slot).build();
            let facts = copy_facts(
                &bucket,
                destination,
                CopySourceObject {
                    bucket: &bucket,
                    key: "source",
                    version_id: None,
                },
                None,
                None,
            );
            authorize_copy_object(&mut envelope, &slot, facts)
                .await
                .expect("preliminary snapshot permits copy");
            if !matches!(mode, "allowed-tags" | "self-copy") {
                store
                    .put_object_tags(&bucket, "source", "visibility=public", &crate::storage::ObjectOptions::default())
                    .await
                    .expect("change source after access hook");
            }
            let input = copy_write_input(
                &bucket,
                destination,
                CopySourceValue::Bucket {
                    bucket: bucket.clone(),
                    key: "source".to_owned(),
                    version_id: None,
                },
                (mode == "self-copy").then_some("REPLACE"),
                None,
                None,
            );
            let probe = crate::storage::helper::MutationCompletionProbe::install(&bucket);
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                DefaultObjectUsecase::with_context(Some(context.clone())).execute_copy_object_owned(envelope, input),
            )
            .await
            .expect("copy authorization must not reacquire its own object lock");
            if matches!(mode, "allowed-tags" | "self-copy") {
                result.expect("unchanged allowed tags complete");
                {
                    let events = probe.events.lock().unwrap();
                    assert_eq!(events.len(), 1, "one committed copy event");
                    assert_eq!(events[0].event_name, rustfs_s3_types::EventName::ObjectCreatedCopy);
                    assert_eq!(events[0].object.name, destination);
                }
                assert_eq!(
                    store
                        .get_object_tags(&bucket, destination, &crate::storage::ObjectOptions::default())
                        .await
                        .expect("copied tags"),
                    "visibility=private"
                );
            } else {
                assert_eq!(
                    result.expect_err("actual source tags require fresh authorization").code(),
                    &S3ErrorCode::AccessDenied
                );
                assert!(
                    store
                        .get_object_info(&bucket, "destination", &crate::storage::ObjectOptions::default())
                        .await
                        .is_err(),
                    "denial must precede destination publication"
                );
                assert!(probe.events.lock().unwrap().is_empty(), "refused copy cannot emit a success event");
            }
        }
    });
}
