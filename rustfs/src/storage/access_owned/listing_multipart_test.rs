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

//! Multipart initiation policy decisions take precedence over generation-read failures.

use super::{CreateMultipartAuthorization, authorize_create_multipart_upload};
use crate::app::RequestEnvelope;
use crate::runtime_sources::{AppContext, ServerContextSlot};
use crate::storage::access::tests::{TestKms, UnreadyIam, build_request, ensure_req_info};
use crate::storage::access::{
    AuthorizationRequest, BucketGenerationGuard, CreateMultipartUploadInput, FS, ObjectLockLegalHoldStatus, ObjectLockMode,
    S3Access as _,
};
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use http::{HeaderValue, Method};
use std::sync::Arc;
use time::OffsetDateTime;

#[test]
#[serial_test::serial]
fn create_multipart_authorization_surfaces_generation_errors_after_policy_checks() {
    crate::app::gating_test_env::run_large_stack_test("create-multipart-authorization-order", || async {
        const ACTIONS: &[&str] = &[
            "s3:PutObject",
            "s3:ReplicateObject",
            "s3:PutObjectLegalHold",
            "s3:PutObjectRetention",
        ];
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let slot = ServerContextSlot::new();
        let fs = FS::with_server_ctx(slot.clone());
        assert!(slot.install(Arc::new(AppContext::new(store.clone(), Arc::new(UnreadyIam), Arc::new(TestKms)))));
        let bucket = format!("create-auth-order-{}", uuid::Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create test bucket");
        for excluded in ACTIONS.iter().copied().map(Some).chain([None]) {
            let policy = serde_json::json!({"Version":"2012-10-17","Statement":[{
                "Effect":"Allow","Principal":{"AWS":"*"},
                "Action":ACTIONS.iter().copied().filter(|action| Some(*action) != excluded).collect::<Vec<_>>(),
                "Resource":format!("arn:aws:s3:::{bucket}/object")
            }]});
            store
                .update_bucket_metadata_config(
                    &bucket,
                    crate::storage::storage_api::ecstore_bucket::metadata::BUCKET_POLICY_CONFIG,
                    serde_json::to_vec(&policy).unwrap(),
                )
                .await
                .expect("publish policy");
            let retain_until = OffsetDateTime::now_utc() + time::Duration::days(1);
            let mut legacy = build_request(
                CreateMultipartUploadInput {
                    bucket: bucket.clone(),
                    key: "object".to_owned(),
                    object_lock_legal_hold_status: Some(ObjectLockLegalHoldStatus::from_static("OFF")),
                    object_lock_mode: Some(ObjectLockMode::from_static("GOVERNANCE")),
                    object_lock_retain_until_date: Some(retain_until.into()),
                    ..Default::default()
                },
                Method::POST,
            );
            legacy
                .headers
                .insert("x-rustfs-source-replication-request", HeaderValue::from_static("true"));
            legacy.extensions.insert(slot.clone());
            ensure_req_info(&mut legacy);
            legacy.install_bucket_generation(BucketGenerationGuard {
                bucket: "other-bucket".to_owned(),
                incarnation_id: uuid::Uuid::new_v4(),
            });
            let mut owned = RequestEnvelope::from_s3s(&legacy);
            let legacy = fs
                .create_multipart_upload(&mut legacy)
                .await
                .expect_err("policy or generation refusal");
            let owned = authorize_create_multipart_upload(
                &slot,
                &mut owned,
                CreateMultipartAuthorization {
                    bucket: &bucket,
                    key: "object",
                    query_version: None,
                    retain_until: Some(retain_until),
                    legal_hold_present: true,
                    retention_present: true,
                },
            )
            .await
            .expect_err("policy or generation refusal");
            let expected = if excluded.is_some() { "AccessDenied" } else { "InternalError" };
            assert_eq!(legacy.code().as_str(), expected, "legacy {excluded:?}");
            assert_eq!(owned.code().as_str(), expected, "owned {excluded:?}");
        }
    });
}
