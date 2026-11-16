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

//! Bucket configuration mutation tests over the shared use case and durable metadata.

use super::*;
use crate::app::bucket_usecase::test_support::build_request;
use crate::app::gating_test_env::{run_large_stack_test, shared_gating_ambient};
use crate::app::runtime_sources::ServerContextSlot;
use crate::app::storage_api::bucket_usecase::helper::MutationCompletionProbe;
use crate::app::storage_api::owned_parity::authorize_bucket_config_mutation;
use crate::site_replication::hooks::BUCKET_META_HOOK_CALLS;
use http::Method;
use rustfs_credentials::Credentials;
use rustfs_s3_types::{S3ErrorCode, S3Result};
use serial_test::serial;
use std::{cell::RefCell, sync::Mutex};

#[derive(Clone, Copy)]
enum Config {
    Encryption,
    Policy,
    Tagging,
}

impl Config {
    async fn seed(self, bucket: &str) {
        match self {
            Self::Encryption => {
                metadata_sys::update(
                    bucket,
                    BUCKET_SSECONFIG,
                    b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>\
<SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"
                        .to_vec(),
                )
                .await
            }
            Self::Policy => {
                metadata_sys::update(bucket, BUCKET_POLICY_CONFIG, br#"{"Version":"2012-10-17","Statement":[]}"#.to_vec()).await
            }
            Self::Tagging => {
                let tags = rustfs_gateway_types::cors_tagging::parse_tagging(
                    b"<Tagging><TagSet><Tag><Key>team</Key><Value>storage</Value></Tag></TagSet></Tagging>",
                )
                .expect("valid tagging fixture");
                metadata_sys::update_s3_tagging_config(bucket, Some(tags), None).await
            }
        }
        .expect("seed bucket configuration");
    }

    async fn stored(self, bucket: &str) -> Vec<u8> {
        let metadata = metadata_sys::get_global_bucket_metadata_sys()
            .expect("initialized metadata system")
            .read()
            .await
            .get_config_from_disk(bucket)
            .await
            .expect("read bucket metadata from disk");
        match self {
            Self::Encryption => metadata.encryption_config_xml,
            Self::Policy => metadata.policy_config_json,
            Self::Tagging => metadata.tagging_config_xml,
        }
    }

    async fn put(self, usecase: &DefaultBucketUsecase, envelope: RequestEnvelope, bucket: &str) -> S3Result<()> {
        match self {
            Self::Encryption => {
                let config = rustfs_gateway_types::persistence::parse_bucket_encryption(ENCRYPTION_WRITE).unwrap();
                usecase.execute_put_bucket_encryption(envelope, bucket, Ok(config)).await
            }
            Self::Policy => usecase.execute_put_bucket_policy(envelope, bucket, POLICY_WRITE).await,
            Self::Tagging => {
                let tags = rustfs_gateway_types::cors_tagging::parse_tagging(TAGGING_WRITE).unwrap();
                usecase.execute_put_bucket_tagging(envelope, bucket, tags).await
            }
        }
    }

    async fn execute(self, usecase: &DefaultBucketUsecase, envelope: RequestEnvelope, bucket: &str) -> S3Result<()> {
        match self {
            Self::Encryption => usecase.execute_delete_bucket_encryption(envelope, bucket).await,
            Self::Policy => usecase.execute_delete_bucket_policy(envelope, bucket).await,
            Self::Tagging => usecase.execute_delete_bucket_tagging(envelope, bucket).await,
        }
    }
}

const ENCRYPTION_WRITE: &[u8] = b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>fixture-key</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>";
const POLICY_WRITE: &str = "{ \"Version\": \"2012-10-17\", \"Statement\": [] }  ";
const TAGGING_WRITE: &[u8] = b"<Tagging><TagSet><Tag><Key>team</Key><Value>gateway</Value></Tag></TagSet></Tagging>";

async fn check_mutation(config: Config, stale: bool, write: bool) {
    let (subresource, action, operation, reload, replicated, audit_status) = match (config, write) {
        (Config::Encryption, false) => (
            "encryption",
            S3Action::PutBucketEncryptionAction,
            S3Operation::DeleteBucketEncryption,
            "delete bucket encryption",
            "sse-config",
            204,
        ),
        (Config::Policy, false) => (
            "policy",
            S3Action::DeleteBucketPolicyAction,
            S3Operation::DeleteBucketPolicy,
            "delete bucket policy",
            "policy",
            200,
        ),
        (Config::Tagging, false) => (
            "tagging",
            S3Action::PutBucketTaggingAction,
            S3Operation::DeleteBucketTagging,
            "delete bucket tagging",
            "tags",
            200,
        ),
        (Config::Encryption, true) => (
            "encryption",
            S3Action::PutBucketEncryptionAction,
            S3Operation::PutBucketEncryption,
            "put bucket encryption",
            "sse-config",
            200,
        ),
        (Config::Policy, true) => (
            "policy",
            S3Action::PutBucketPolicyAction,
            S3Operation::PutBucketPolicy,
            "put bucket policy",
            "policy",
            200,
        ),
        (Config::Tagging, true) => (
            "tagging",
            S3Action::PutBucketTaggingAction,
            S3Operation::PutBucketTagging,
            "put bucket tagging",
            "tags",
            200,
        ),
    };
    let context = shared_gating_ambient().await;
    let store = context.object_store();
    let bucket = format!("config-mutation-{}", uuid::Uuid::new_v4().simple());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket");
    let server = ServerContextSlot::new();
    assert!(server.install(Arc::clone(&context)));
    let mut request = build_request((), if write { Method::PUT } else { Method::DELETE });
    request.uri = format!("/{bucket}?{subresource}").parse().expect("request URI");
    request.extensions.insert(Arc::clone(&server));
    request.extensions.insert(ReqInfo {
        cred: Some(Credentials {
            access_key: "config-delete-owner".to_owned(),
            ..Default::default()
        }),
        is_owner: true,
        ..Default::default()
    });
    request.extensions.insert(request_context::RequestContext {
        request_id: "bucket-config-delete".to_owned(),
        x_amz_request_id: "bucket-config-delete".to_owned(),
        trace_id: None,
        span_id: None,
        start_time: std::time::Instant::now(),
    });
    let mut envelope = RequestEnvelope::from_s3s(&request);
    authorize_bucket_config_mutation(&server, &mut envelope, &bucket, Action::S3Action(action))
        .await
        .expect("capture authorized bucket incarnation");
    if stale {
        store
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("delete authorized bucket");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("recreate bucket");
    }
    config.seed(&bucket).await;
    let seeded = config.stored(&bucket).await;
    assert!(!seeded.is_empty(), "durable configuration fixture");

    let before = rustfs_scanner::scanner_dirty_usage_bucket_generation(&bucket);
    let maintenance_before = rustfs_scanner::scanner_maintenance_generation();
    let audits = MutationCompletionProbe::install(&bucket);
    let reloads = Arc::new(Mutex::new(Vec::new()));
    let observe = Arc::clone(&reloads);
    let usecase = DefaultBucketUsecase::with_context(Some(context));
    let (result, hooks) = BUCKET_META_HOOK_CALLS
        .scope(RefCell::default(), async {
            let result = BUCKET_METADATA_RELOAD_PROBE
                .scope(
                    (
                        bucket.clone(),
                        Box::new(move |completed, label, maintenance| {
                            let replication_calls = BUCKET_META_HOOK_CALLS.with(|calls| calls.borrow().len());
                            observe
                                .lock()
                                .expect("reload entries")
                                .push((completed, label, maintenance, replication_calls));
                        }),
                    ),
                    async {
                        if write {
                            config.put(&usecase, envelope, &bucket).await
                        } else {
                            config.execute(&usecase, envelope, &bucket).await
                        }
                    },
                )
                .await;
            (result, BUCKET_META_HOOK_CALLS.with(RefCell::take))
        })
        .await;

    let stored = config.stored(&bucket).await;
    let after = rustfs_scanner::scanner_dirty_usage_bucket_generation(&bucket);
    let reloads = reloads.lock().expect("reload entries");
    let entries = audits.audits.lock().expect("audit entries");
    assert!(audits.events.lock().expect("event entries").is_empty());
    let audit = entries
        .iter()
        .map(|entry| (entry.api.name.as_deref(), entry.api.status_code))
        .collect::<Vec<_>>();
    if stale {
        assert_eq!(
            (
                result.err().map(|err| err.code().clone()),
                stored,
                reloads.len(),
                hooks.len(),
                after,
                audit
            ),
            (
                Some(S3ErrorCode::NoSuchBucket),
                seeded,
                0,
                0,
                before,
                vec![(Some(operation.as_str()), Some(404))]
            ),
            "{operation:?} must refuse the stale authorization and preserve replacement bytes"
        );
    } else {
        result.expect("authorized mutation");
        if write {
            match config {
                Config::Encryption => {
                    let actual = rustfs_gateway_types::persistence::parse_bucket_encryption(&stored).unwrap();
                    let expected = rustfs_gateway_types::persistence::parse_bucket_encryption(ENCRYPTION_WRITE).unwrap();
                    assert_eq!(actual, expected);
                }
                Config::Policy => assert_eq!(stored, POLICY_WRITE.as_bytes()),
                Config::Tagging => {
                    let actual = rustfs_gateway_types::cors_tagging::parse_tagging(&stored).unwrap();
                    let expected = rustfs_gateway_types::cors_tagging::parse_tagging(TAGGING_WRITE).unwrap();
                    assert_eq!(actual, expected);
                }
            }
        } else {
            assert!(stored.is_empty(), "{operation:?} must remove the durable configuration");
        }
        assert_eq!(*reloads, [(false, reload, false, 0), (true, reload, false, 0)]);
        let [hook] = hooks.as_slice() else { panic!("{operation:?}: expected one replication call") };
        assert_eq!((&hook.bucket, hook.item_type.as_str()), (&bucket, replicated));
        assert_eq!(hook.scanner_maintenance_generation, maintenance_before);
        assert_eq!(rustfs_scanner::scanner_maintenance_generation(), maintenance_before);
        if matches!(config, Config::Tagging) {
            assert!(after > hook.scanner_generation);
        } else {
            assert_eq!(after, hook.scanner_generation);
        }
        assert_eq!(audit, [(Some(operation.as_str()), Some(audit_status))]);
        assert_eq!(entries[0].request_id.as_deref(), Some("bucket-config-delete"));
    }
}

#[test]
#[serial]
fn encryption_delete_fences_recreated_bucket_and_orders_callbacks() {
    run_large_stack_test("delete-encryption", || async {
        check_mutation(Config::Encryption, false, false).await;
        check_mutation(Config::Encryption, true, false).await;
    });
}

#[test]
#[serial]
fn policy_delete_fences_recreated_bucket_and_orders_callbacks() {
    run_large_stack_test("delete-policy", || async {
        check_mutation(Config::Policy, false, false).await;
        check_mutation(Config::Policy, true, false).await;
    });
}

#[test]
#[serial]
fn tagging_delete_fences_recreated_bucket_and_orders_callbacks() {
    run_large_stack_test("delete-tagging", || async {
        check_mutation(Config::Tagging, false, false).await;
        check_mutation(Config::Tagging, true, false).await;
    });
}

#[test]
#[serial]
fn encryption_put_fences_recreated_bucket_and_orders_callbacks() {
    run_large_stack_test("put-encryption", || async {
        check_mutation(Config::Encryption, false, true).await;
        check_mutation(Config::Encryption, true, true).await;
    });
}

#[test]
#[serial]
fn policy_put_fences_recreated_bucket_and_orders_callbacks() {
    run_large_stack_test("put-policy", || async {
        check_mutation(Config::Policy, false, true).await;
        check_mutation(Config::Policy, true, true).await;
    });
}

#[test]
#[serial]
fn tagging_put_fences_recreated_bucket_and_orders_callbacks() {
    run_large_stack_test("put-tagging", || async {
        check_mutation(Config::Tagging, false, true).await;
        check_mutation(Config::Tagging, true, true).await;
    });
}
