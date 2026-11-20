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

//! The condition values the owned authorization core builds from one `RequestEnvelope`.
//!
//! Responsible for: list query conditions, request object tag conditions, the verified client
//! facts, the internal-object condition filter, the remaining-retention-days condition, the
//! existing-object-tag hint and merge, and the owner's bucket-policy Deny bypass.
//! Not responsible for: running an operation's `authorize_*` function against stored policies
//! (`storage::access_owned` tests), or the legacy request type.
//! Upstream: `crate::storage::authorization::tests::fixture`. Downstream: `storage::authorization`
//! conditions and decision, `crate::auth::facts`, and the bucket policy system.

use crate::app::RequestEnvelope;
use crate::app::object_meta::tagging::proxy_allowed;
use crate::auth::OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION;
use crate::auth::facts::{AuthzPrincipal, AuthzTarget, list_query_pairs, merge_list_query_conditions};
use crate::shared_types::RemoteAddr;
use crate::storage::StorageError;
use crate::storage::authorization::tests::fixture::{request, server_context};
use crate::storage::authorization::{
    BucketPolicyExistingObjectTagHint, BucketPolicyRawLoadErrorKind, ObjectTagConditions, authorization_conditions,
    authorization_conditions_with_internal, classify_bucket_policy_raw_load_error, load_bucket_policy_existing_object_tag_hint,
    maybe_merge_object_tag_conditions, merge_request_object_tag_conditions, owner_can_bypass_policy_deny,
    remaining_retention_days, request_authz_facts, set_requested_object_lock_retain_until,
};
use crate::storage::storage_api::contract::bucket::{BucketOperations as _, MakeBucketOptions};
use http::{HeaderMap, HeaderValue, Method};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_policy::policy::{BucketPolicy, bucket_policy_uses_existing_object_tag_conditions};
use rustfs_s3_types::S3ErrorCode;
use serial_test::serial;
use std::collections::HashMap;
use time::OffsetDateTime;

/// The conditions an anonymous principal's decision about `action` evaluates for `req`.
fn remaining_retention_days_conditions(req: &RequestEnvelope, action: Action) -> HashMap<String, Vec<String>> {
    let credentials = rustfs_credentials::Credentials::default();
    let principal = AuthzPrincipal::new(&credentials, "");
    let facts = request_authz_facts(
        req,
        AuthzTarget {
            action,
            bucket: "",
            object: "",
            version_id: None,
            location_constraint: None,
        },
    );
    authorization_conditions(req, &principal, &facts).expect("conditions should build")
}

/// A PUT that asks to retain its object for `retain_for` from now.
fn request_retaining_for(retain_for: time::Duration) -> RequestEnvelope {
    let mut req = request(Method::PUT).build();
    let retain_until = OffsetDateTime::now_utc() + retain_for;
    set_requested_object_lock_retain_until(&mut req, Some(retain_until));
    req
}

/// Object tag conditions must use keys like ExistingObjectTag/<tag-key> so that
/// bucket policy conditions (e.g. s3:ExistingObjectTag/security) are evaluated correctly.
#[test]
fn test_merge_list_bucket_query_conditions_extracts_supported_keys() {
    let mut conditions = HashMap::new();
    merge_list_query_conditions(
        Action::S3Action(S3Action::ListBucketAction),
        &list_query_pairs(Some("prefix=photos%2F2024%2F&delimiter=%2F&max-keys=10&encoding-type=url")),
        &mut conditions,
    );

    assert_eq!(conditions.get("prefix"), Some(&vec!["photos/2024/".to_string()]));
    assert_eq!(conditions.get("delimiter"), Some(&vec!["/".to_string()]));
    assert_eq!(conditions.get("max-keys"), Some(&vec!["10".to_string()]));
    assert!(!conditions.contains_key("encoding-type"));
}

#[test]
fn test_merge_list_bucket_query_conditions_preserves_empty_prefix_signal() {
    let mut conditions = HashMap::new();
    merge_list_query_conditions(
        Action::S3Action(S3Action::ListBucketVersionsAction),
        &list_query_pairs(Some("prefix=&delimiter=%2F")),
        &mut conditions,
    );

    assert_eq!(conditions.get("prefix"), Some(&vec![String::new()]));
    assert_eq!(conditions.get("delimiter"), Some(&vec!["/".to_string()]));
}

#[test]
fn test_merge_list_bucket_query_conditions_ignores_non_list_actions() {
    let mut conditions = HashMap::new();
    merge_list_query_conditions(
        Action::S3Action(S3Action::GetObjectAction),
        &list_query_pairs(Some("prefix=photos%2F2024%2F&delimiter=%2F&max-keys=10")),
        &mut conditions,
    );

    assert!(conditions.is_empty());
}

#[test]
fn listing_conditions_reject_repeated_query_and_ignore_headers() {
    for action in [
        S3Action::ListBucketAction,
        S3Action::ListBucketVersionsAction,
        S3Action::ListBucketMultipartUploadsAction,
    ] {
        for key in ["prefix", "delimiter", "max-keys"] {
            let action = Action::S3Action(action);
            let credentials = rustfs_credentials::Credentials::default();
            let conditions_of = |req: &RequestEnvelope| {
                let principal = AuthzPrincipal::new(&credentials, "");
                let facts = request_authz_facts(
                    req,
                    AuthzTarget {
                        action,
                        bucket: "bucket",
                        object: "",
                        version_id: None,
                        location_constraint: None,
                    },
                );
                authorization_conditions(req, &principal, &facts)
            };
            // Every request carries the header; only the applied query may describe the listing.
            let req = request(Method::GET).header(key, "allowed").build();
            let conditions = conditions_of(&req).unwrap();
            assert!(!conditions.contains_key(key), "{key} must come from the applied query");
            let req = request(Method::GET)
                .uri(&format!("/bucket?{key}=actual"))
                .header(key, "allowed")
                .build();
            let conditions = conditions_of(&req).unwrap();
            assert_eq!(conditions.get(key), Some(&vec!["actual".to_string()]));
            let req = request(Method::GET)
                .uri(&format!("/bucket?{key}=actual&{key}=allowed"))
                .header(key, "allowed")
                .build();
            let error = conditions_of(&req).unwrap_err();
            assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        }
    }
}

#[test]
fn test_merge_request_object_tag_conditions_applies_only_to_put_object() {
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-tagging", HeaderValue::from_static("classification=restricted&label=copy%20test"));

    let mut source_conditions = HashMap::new();
    merge_request_object_tag_conditions(Action::S3Action(S3Action::GetObjectAction), &headers, &mut source_conditions)
        .expect("GetObject condition construction should succeed");
    assert!(
        source_conditions.is_empty(),
        "destination request tags must not affect CopyObject source authorization"
    );

    let mut destination_conditions = HashMap::new();
    merge_request_object_tag_conditions(Action::S3Action(S3Action::PutObjectAction), &headers, &mut destination_conditions)
        .expect("PutObject condition construction should succeed");
    assert_eq!(
        destination_conditions.get("RequestObjectTag/classification"),
        Some(&vec!["restricted".to_string()])
    );
    assert_eq!(destination_conditions.get("RequestObjectTag/label"), Some(&vec!["copy test".to_string()]));
    assert_eq!(
        destination_conditions.get("RequestObjectTagKeys"),
        Some(&vec!["classification".to_string(), "label".to_string()])
    );
}

#[test]
fn access_facts_preserve_verified_client_context() {
    use rustfs_trusted_proxies::{ClientInfo, ValidationMode};

    let trusted = ClientInfo::from_trusted_proxy(
        "203.0.113.9".parse().expect("client IP"),
        None,
        Some("https".to_owned()),
        "10.0.0.2".parse().expect("proxy IP"),
        1,
        ValidationMode::Lenient,
        Vec::new(),
    );
    for (client_info, expected_ip, expected_secure) in [(None, "192.0.2.10", "false"), (Some(trusted), "203.0.113.9", "true")] {
        let mut builder = request(Method::GET)
            .header("x-forwarded-for", "198.51.100.1")
            .header("x-forwarded-proto", "https")
            .extension(Some(RemoteAddr("192.0.2.10:443".parse().expect("peer address"))));
        if let Some(client_info) = client_info {
            builder = builder.extension(client_info);
        }
        let req = builder.build();
        let conditions = remaining_retention_days_conditions(&req, Action::S3Action(S3Action::GetObjectAction));
        assert_eq!(conditions.get("SourceIp"), Some(&vec![expected_ip.to_owned()]));
        assert_eq!(conditions.get("SecureTransport"), Some(&vec![expected_secure.to_owned()]));
        assert!(req.remote_addr().is_some(), "facts must only borrow the recorded peer");
    }
}

/// AWS does not document how it rounds partial days. MinIO computes
/// `ceil(hours / 24)` in `enforceRetentionBypassForPut`
/// (cmd/bucket-object-lock.go), and RustFS keeps that rounding so a policy
/// moved from MinIO evaluates the same way.
#[test]
fn remaining_retention_days_matches_s3_ceil_rounding() {
    let now = OffsetDateTime::now_utc();
    let cases = [
        (time::Duration::NANOSECOND, 1),
        (time::Duration::HOUR, 1),
        (time::Duration::DAY - time::Duration::SECOND, 1),
        (time::Duration::DAY, 1),
        (time::Duration::DAY + time::Duration::SECOND, 2),
        (time::Duration::days(29) + time::Duration::hours(12), 30),
        (time::Duration::days(30), 30),
        (time::Duration::days(30) + time::Duration::SECOND, 31),
    ];

    for (retain_for, expected) in cases {
        let minio_days = (retain_for.as_seconds_f64() / 3600.0 / 24.0).ceil() as i64;
        assert_eq!(minio_days, expected, "MinIO reference for {retain_for}");
        assert_eq!(remaining_retention_days(now + retain_for, now), expected, "retain for {retain_for}");
    }
}

#[test]
fn remaining_retention_days_is_zero_for_dates_not_in_future() {
    let now = OffsetDateTime::now_utc();

    assert_eq!(remaining_retention_days(now, now), 0);
    assert_eq!(remaining_retention_days(now - time::Duration::NANOSECOND, now), 0);
    assert_eq!(remaining_retention_days(now - time::Duration::days(3), now), 0);
}

#[test]
fn requested_retain_until_sets_remaining_retention_days_condition() {
    let req = request_retaining_for(time::Duration::days(10) - time::Duration::HOUR);

    let conditions = remaining_retention_days_conditions(&req, Action::S3Action(S3Action::PutObjectAction));

    assert_eq!(
        conditions.get(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION),
        Some(&vec!["10".to_string()])
    );
}

#[test]
fn request_without_retention_has_no_remaining_retention_days_condition() {
    let action = Action::S3Action(S3Action::PutObjectAction);
    let req = request(Method::PUT).build();
    assert_eq!(
        remaining_retention_days_conditions(&req, action).get(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION),
        None
    );

    let mut req = request_retaining_for(time::Duration::days(5));
    set_requested_object_lock_retain_until(&mut req, None);
    assert_eq!(
        remaining_retention_days_conditions(&req, action).get(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION),
        None
    );
}

#[test]
fn remaining_retention_days_condition_ignores_client_header() {
    let action = Action::S3Action(S3Action::PutObjectRetentionAction);
    let mut req = request(Method::PUT)
        .header(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION, "1")
        .build();
    assert_eq!(
        remaining_retention_days_conditions(&req, action).get(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION),
        None
    );

    let retain_until = OffsetDateTime::now_utc() + time::Duration::days(400);
    set_requested_object_lock_retain_until(&mut req, Some(retain_until));
    assert_eq!(
        remaining_retention_days_conditions(&req, action).get(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION),
        Some(&vec!["400".to_string()])
    );
}

#[test]
fn internal_object_authorization_ignores_unapplied_request_options() {
    let req = request(Method::POST)
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20260801/us-east-1/s3tables/aws4_request",
        )
        .header("user-agent", "pyiceberg/test")
        .header("x-amz-server-side-encryption", "AES256")
        .header("x-amz-tagging", "classification=%ZZ")
        .build();

    let credentials = rustfs_credentials::Credentials::default();
    let principal = AuthzPrincipal::new(&credentials, "");
    let facts = request_authz_facts(
        &req,
        AuthzTarget {
            action: Action::S3Action(S3Action::PutObjectAction),
            bucket: "",
            object: "",
            version_id: None,
            location_constraint: None,
        },
    );
    // The owned edge reaches this branch through `authorize_owned_internal_object_request`.
    let conditions = authorization_conditions_with_internal(&req, &principal, &facts, true)
        .expect("internal object authorization conditions should build");

    assert_eq!(conditions.get("authType"), Some(&vec!["REST-HEADER".to_string()]));
    assert_eq!(conditions.get("signatureversion"), Some(&vec!["AWS4-HMAC-SHA256".to_string()]));
    assert_eq!(conditions.get("UserAgent"), Some(&vec!["pyiceberg/test".to_string()]));
    assert!(!conditions.contains_key("authorization"));
    assert!(!conditions.contains_key("x-amz-server-side-encryption"));
    assert!(!conditions.contains_key("RequestObjectTag/classification"));
    assert!(!conditions.contains_key("RequestObjectTagKeys"));
}

#[tokio::test]
async fn test_required_existing_object_tags_are_merged_into_authorization_conditions() {
    let (_temp_dir, _disk_paths, store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
    let slot = server_context(store);
    let mut req = request(Method::GET)
        .server_context(&slot)
        .extension(ObjectTagConditions::new(
            "bucket",
            "tagged-object",
            None,
            HashMap::from([
                ("ExistingObjectTag/security".to_string(), vec!["restricted".to_string()]),
                ("ExistingObjectTag/project".to_string(), vec!["webapp".to_string()]),
            ]),
        ))
        .build();
    let mut conditions = HashMap::new();
    conditions.insert("delimiter".to_string(), vec!["/".to_string()]);

    maybe_merge_object_tag_conditions(
        &mut req,
        Action::S3Action(S3Action::GetObjectAction),
        "bucket",
        "tagged-object",
        None,
        &mut conditions,
        true,
    )
    .await
    .expect("merge required existing object tags into authorization conditions");

    assert_eq!(conditions.get("ExistingObjectTag/security"), Some(&vec!["restricted".to_string()]));
    assert!(!proxy_allowed(&req));
    assert_eq!(conditions.get("ExistingObjectTag/project"), Some(&vec!["webapp".to_string()]));
    assert_eq!(conditions.get("delimiter"), Some(&vec!["/".to_string()]));
}

#[test]
fn test_bucket_policy_existing_object_tag_condition_key_detection() {
    let condition_key_policy = r#"{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Principal":"*",
    "Action":["s3:GetObject"],
    "Resource":["arn:aws:s3:::bucket/*"],
    "Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}
  }]
}"#;
    let policy: BucketPolicy = serde_json::from_str(condition_key_policy).expect("valid bucket policy JSON");
    assert!(
        bucket_policy_uses_existing_object_tag_conditions(&policy),
        "ExistingObjectTag in condition key must be detected"
    );

    let value_only_policy = r#"{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Principal":"*",
    "Action":["s3:GetObject"],
    "Resource":["arn:aws:s3:::bucket/*"],
    "Condition":{"StringEquals":{"s3:prefix":"ExistingObjectTag/security"}}
  }]
}"#;
    let policy: BucketPolicy = serde_json::from_str(value_only_policy).expect("valid bucket policy JSON");
    assert!(
        !bucket_policy_uses_existing_object_tag_conditions(&policy),
        "ExistingObjectTag text in values should not trigger tag dependency"
    );
}

#[test]
fn test_unparsable_bucket_policy_json_implies_conservative_existing_object_tag_fetch() {
    // Matches `load_bucket_policy_existing_object_tag_hint`: unparsable policy => conservative tag fetch.
    let malformed = r#"{"Version":"2012-10-17","Statement":[INVALID]}"#;
    assert!(serde_json::from_str::<BucketPolicy>(malformed).is_err());
    let conservative_fetch = serde_json::from_str::<BucketPolicy>(malformed)
        .map(|p| bucket_policy_uses_existing_object_tag_conditions(&p))
        .unwrap_or(true);
    assert!(conservative_fetch);

    // Invalid JSON that still contains real ExistingObjectTag condition keys (trailing comma).
    let malformed_with_tag_keys =
        r#"{"Version":"2012-10-17","Statement":[{"Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}},]}"#;
    assert!(serde_json::from_str::<BucketPolicy>(malformed_with_tag_keys).is_err());
    let conservative_with_tag_keys = serde_json::from_str::<BucketPolicy>(malformed_with_tag_keys)
        .map(|p| bucket_policy_uses_existing_object_tag_conditions(&p))
        .unwrap_or(true);
    assert!(conservative_with_tag_keys);
}

#[tokio::test]
#[serial]
async fn existing_tag_hint_reuses_parsed_metadata_and_fails_closed_without_it() {
    let store = crate::app::gating_test_env::shared_gating_ecstore().await;
    let bucket = format!("cached-policy-hint-{}", uuid::Uuid::new_v4());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create hint bucket");
    let document = serde_json::json!({"Version":"2012-10-17","Statement":[{
        "Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":format!("arn:aws:s3:::{bucket}/*"),
        "Condition":{"StringEquals":{"s3:ExistingObjectTag/classification":"secret"}}
    }]});
    let mut metadata = (*crate::storage::get_bucket_metadata(&bucket).await.expect("bucket metadata")).clone();
    metadata.policy_config =
        Some(serde_json::from_slice(&serde_json::to_vec(&document).expect("policy JSON")).expect("parsed policy"));
    // A cached parsed snapshot is the authority used by actual policy
    // evaluation; the tag hint must consult that same snapshot.
    metadata.policy_config_json = b"invalid raw JSON".to_vec();
    crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata.clone())
        .await
        .expect("publish parsed snapshot");
    assert!(matches!(
        load_bucket_policy_existing_object_tag_hint(&store, &bucket, Action::S3Action(S3Action::GetObjectAction)).await,
        BucketPolicyExistingObjectTagHint::Parsed(_)
    ));
    metadata.policy_config = None;
    crate::storage::storage_api::set_bucket_metadata(bucket.clone(), metadata)
        .await
        .expect("publish unreadable snapshot");
    assert!(matches!(
        load_bucket_policy_existing_object_tag_hint(&store, &bucket, Action::S3Action(S3Action::GetObjectAction)).await,
        BucketPolicyExistingObjectTagHint::ConservativeTagRequired
    ));
}

#[test]
fn test_classify_bucket_policy_raw_load_error() {
    assert_eq!(
        classify_bucket_policy_raw_load_error(&StorageError::ConfigNotFound),
        BucketPolicyRawLoadErrorKind::PolicyMissing
    );
    assert_eq!(
        classify_bucket_policy_raw_load_error(&StorageError::BucketNotFound("b".to_string())),
        BucketPolicyRawLoadErrorKind::BucketMissing
    );
    assert_eq!(
        classify_bucket_policy_raw_load_error(&StorageError::Io(std::io::Error::other("boom"))),
        BucketPolicyRawLoadErrorKind::Other
    );
}

/// Owner can bypass bucket policy Deny only for the three policy management APIs (per AWS S3).
#[test]
fn test_owner_can_bypass_policy_deny_only_for_policy_apis() {
    // Owner + policy management actions -> bypass allowed
    assert!(owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::GetBucketPolicyAction)));
    assert!(owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::PutBucketPolicyAction)));
    assert!(owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::DeleteBucketPolicyAction)));

    // Owner + other actions -> no bypass (still subject to bucket policy Deny)
    assert!(!owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::ListBucketAction)));
    assert!(!owner_can_bypass_policy_deny(true, &Action::S3Action(S3Action::GetObjectAction)));

    // Non-owner -> no bypass for any action
    assert!(!owner_can_bypass_policy_deny(false, &Action::S3Action(S3Action::GetBucketPolicyAction)));
    assert!(!owner_can_bypass_policy_deny(
        false,
        &Action::S3Action(S3Action::DeleteBucketPolicyAction)
    ));
}
