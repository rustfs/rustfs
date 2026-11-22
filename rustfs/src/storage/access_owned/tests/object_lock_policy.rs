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

//! Which request values an operation's owned authorization hands the policy engine, and what a
//! stored identity or bucket policy decides on them: versioned tagging actions, the
//! `s3:ObjectCreationOperation` condition, legal hold, tagging, retention and POST form values
//! taken from the applied body instead of the wire headers, the decoded copy source, the
//! remaining-retention-days limit, and the replication proxy's tag-evidence gate.
//!
//! Responsible for: running the `storage::access_owned` functions on a `RequestEnvelope` with the
//! facts the gateway edge decodes, then evaluating the resulting conditions.
//! Not responsible for: decoding those facts from the wire (the gateway handlers own that), or
//! the condition builders in isolation (`storage::authorization` tests).
//! Upstream: `crate::storage::authorization::tests::fixture`. Downstream: `storage::access_owned`,
//! `storage::authorization` conditions, and the identity and bucket policy systems.

use crate::app::RequestEnvelope;
use crate::app::object_meta::tagging::proxy_allowed;
use crate::auth::facts::{AuthzPrincipal, AuthzTarget};
use crate::auth::{OBJECT_CREATION_OPERATION_CONDITION, OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION, get_condition_values};
use crate::storage::access_owned::object_commit::{
    CopyObjectAuthorization, CopySourceObject, authorize_copy_object, authorize_upload_part_copy,
};
use crate::storage::access_owned::object_meta::{authorize_put_object_legal_hold, authorize_put_object_tagging};
use crate::storage::access_owned::object_write::{
    PostObjectFormPolicy, PostObjectPolicyFields, PutObjectAuthorization, authorize_put_object,
};
use crate::storage::authorization::tests::fixture::request;
use crate::storage::authorization::{
    AuthorizationRequest, ObjectTagConditions, authorization_conditions, authorization_conditions_with_internal,
    request_authz_facts, set_requested_object_lock_retain_until,
};
use crate::storage::authorization_actions::versioned_tagging_action;
use crate::storage::request_markers::{
    MultipartStagingOperation, RequestedObjectLegalHold, RequestedObjectLockRetention, RequestedObjectTags,
};
use crate::storage::storage_api::runtime_sources_consumer::ServerContextSlot;
use http::{HeaderValue, Method};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_policy::policy::{Args, BucketPolicy, BucketPolicyArgs, Policy};
use std::collections::HashMap;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

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

async fn assert_requested_value_denied(action: Action, key: &str, value: &str, conditions: &HashMap<String, Vec<String>>) {
    let document = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": action, "Resource": "arn:aws:s3:::bucket/*"},
            {"Effect": "Deny", "Action": action, "Resource": "arn:aws:s3:::bucket/*",
             "Condition": {"StringEquals": {key: value}}}
        ]
    });
    let policy = Policy::parse_config(&serde_json::to_vec(&document).unwrap()).unwrap();
    let claims = HashMap::new();
    let args = Args {
        account: "writer",
        groups: &None,
        action,
        bucket: "bucket",
        conditions,
        is_owner: false,
        object: "object",
        claims: &claims,
        deny_only: false,
    };
    assert!(!policy.is_allowed(&args).await, "the applied request value must trigger Deny");
}

#[tokio::test]
async fn versioned_tagging_requires_version_permissions() {
    let policy = Policy::parse_config(br#"{
        "Version":"2012-10-17",
        "Statement":[{"Effect":"Allow","Action":["s3:GetObjectTagging","s3:PutObjectTagging","s3:DeleteObjectTagging"],"Resource":"arn:aws:s3:::bucket/*"}]
    }"#).expect("current-object tagging policy should parse");
    let conditions = HashMap::new();
    let claims = HashMap::new();
    let groups = None;
    for (current, versioned) in [
        (S3Action::GetObjectTaggingAction, S3Action::GetObjectVersionTaggingAction),
        (S3Action::PutObjectTaggingAction, S3Action::PutObjectVersionTaggingAction),
        (S3Action::DeleteObjectTaggingAction, S3Action::DeleteObjectVersionTaggingAction),
    ] {
        for version in [None, Some("null"), Some("0194e0f1-0000-7000-8000-000000000000")] {
            let action = versioned_tagging_action(current, version);
            assert_eq!(action, Action::S3Action(if version.is_some() { versioned } else { current }));
            let args = Args {
                account: "writer",
                groups: &groups,
                action,
                bucket: "bucket",
                conditions: &conditions,
                is_owner: false,
                object: "object",
                claims: &claims,
                deny_only: false,
            };
            assert_eq!(policy.is_allowed(&args).await, version.is_none(), "version selector {version:?}");
        }
    }
}

#[test]
fn object_creation_operation_classifies_only_multipart_staging() {
    for (operation, staging) in [
        ("CreateMultipartUpload", true),
        ("UploadPart", true),
        ("UploadPartCopy", true),
        ("PutObject", false),
        ("CopyObject", false),
        ("CompleteMultipartUpload", false),
        ("PostObject", false),
        ("GetObject", false),
        ("InternalObjectWrite", false),
    ] {
        assert_eq!(MultipartStagingOperation::for_operation(operation).is_some(), staging, "{operation}");
    }
}

#[test]
fn object_creation_operation_ignores_request_and_claim_spoofing() {
    for forged in ["false", "true"] {
        for claim in [
            "ObjectCreationOperation",
            "objectcreationoperation",
            "OBJECTCREATIONOPERATION",
            "ldapObjectCreationOperation",
            "ldapldapObjectCreationOperation",
        ] {
            let credentials = rustfs_credentials::Credentials {
                claims: Some(HashMap::from([(claim.to_string(), serde_json::json!(forged))])),
                ..Default::default()
            };
            let spoofed = || {
                request(Method::GET)
                    .uri(&format!("/bucket/key?ObjectCreationOperation={forged}&uploadId=fake&partNumber=1"))
                    .header("objectcreationoperation", forged)
            };
            assert!(
                !get_condition_values(spoofed().build().headers(), &credentials, None, None, None)
                    .keys()
                    .any(|key| key.eq_ignore_ascii_case(OBJECT_CREATION_OPERATION_CONDITION))
            );
            for staging in [false, true] {
                let req = if staging {
                    spoofed().extension(MultipartStagingOperation).build()
                } else {
                    spoofed().build()
                };
                // The envelope never carries the legacy internal marker; the internal-object
                // edge (`authorize_owned_internal_object_request`) passes the flag explicitly.
                for internal in [false, true] {
                    for action in [
                        S3Action::PutObjectAction,
                        S3Action::GetObjectAction,
                        S3Action::GetObjectVersionAction,
                    ] {
                        let principal = AuthzPrincipal::new(&credentials, "");
                        let facts = request_authz_facts(
                            &req,
                            AuthzTarget {
                                action: Action::S3Action(action),
                                bucket: "",
                                object: "",
                                version_id: None,
                                location_constraint: None,
                            },
                        );
                        let conditions = authorization_conditions_with_internal(&req, &principal, &facts, internal)
                            .expect("conditions should build");
                        let values = conditions
                            .iter()
                            .filter(|(key, _)| key.eq_ignore_ascii_case(OBJECT_CREATION_OPERATION_CONDITION))
                            .collect::<Vec<_>>();
                        if action == S3Action::PutObjectAction {
                            assert_eq!(values.len(), 1, "one canonical server-derived value");
                            assert_eq!(values[0].0, OBJECT_CREATION_OPERATION_CONDITION);
                            assert_eq!(
                                values[0].1,
                                &vec![(internal || !staging).to_string()],
                                "{claim}, internal={internal}, staging={staging}"
                            );
                        } else {
                            assert!(values.is_empty(), "non-PutObject action {action:?} must have no creation condition");
                        }
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn object_creation_operation_controls_identity_and_bucket_policies() {
    for effect in ["Allow", "Deny"] {
        for expected in [false, true] {
            let mut statements = vec![serde_json::json!({
                "Effect": effect, "Action": "s3:PutObject", "Resource": "arn:aws:s3:::bucket/*",
                "Condition": { "Bool": { "s3:ObjectCreationOperation": expected.to_string() } }
            })];
            if effect == "Deny" {
                statements.push(serde_json::json!({
                    "Effect": "Allow", "Action": "s3:PutObject", "Resource": "arn:aws:s3:::bucket/*"
                }));
            }
            let identity: Policy = serde_json::from_str(
                &serde_json::json!({
                    "Version": "2012-10-17", "Statement": statements
                })
                .to_string(),
            )
            .expect("identity policy should accept the AWS Bool key");
            for statement in &mut statements {
                statement["Principal"] = serde_json::json!("*");
            }
            let bucket: BucketPolicy = serde_json::from_str(
                &serde_json::json!({
                    "Version": "2012-10-17", "Statement": statements
                })
                .to_string(),
            )
            .expect("bucket policy should accept the AWS Bool key");
            for actual in [None, Some(false), Some(true)] {
                let conditions = actual
                    .map(|value| HashMap::from([("ObjectCreationOperation".to_string(), vec![value.to_string()])]))
                    .unwrap_or_default();
                let allowed = if effect == "Allow" {
                    actual == Some(expected)
                } else {
                    actual != Some(expected)
                };
                assert_eq!(
                    identity
                        .is_allowed(&Args {
                            account: "writer",
                            groups: &None,
                            action: Action::S3Action(S3Action::PutObjectAction),
                            bucket: "bucket",
                            object: "object",
                            conditions: &conditions,
                            is_owner: false,
                            claims: &HashMap::new(),
                            deny_only: false,
                        })
                        .await,
                    allowed,
                    "identity {effect}, expected={expected}, actual={actual:?}"
                );
                assert_eq!(
                    bucket
                        .is_allowed(&BucketPolicyArgs {
                            account: "writer",
                            groups: &None,
                            action: Action::S3Action(S3Action::PutObjectAction),
                            bucket: "bucket",
                            object: "object",
                            conditions: &conditions,
                            is_owner: false,
                        })
                        .await,
                    allowed,
                    "bucket {effect}, expected={expected}, actual={actual:?}"
                );
            }
        }
    }
}

#[test]
fn legal_hold_policy_checks_body_instead_of_conflicting_headers() {
    let mut req = request(Method::PUT).header("x-amz-object-lock-legal-hold", "ON").build();
    let action = Action::S3Action(S3Action::PutObjectLegalHoldAction);
    // Without the applied body value the header leaks into the decision.
    let conditions = remaining_retention_days_conditions(&req, action);
    assert_eq!(conditions.get("object-lock-legal-hold"), Some(&vec!["ON".to_string()]));
    // The XML body `<LegalHold><Status>OFF</Status></LegalHold>`, as the edge decodes it.
    req.install_requested_legal_hold(RequestedObjectLegalHold(Some("OFF".to_string())));
    let conditions = remaining_retention_days_conditions(&req, action);
    assert_eq!(conditions.get("object-lock-legal-hold"), Some(&vec!["OFF".to_string()]));
    // A body without a status stores no hold change; the header must not fill it in.
    req.install_requested_legal_hold(RequestedObjectLegalHold::default());
    let conditions = remaining_retention_days_conditions(&req, action);
    assert!(!conditions.contains_key("object-lock-legal-hold"));
}

#[test]
fn tagging_policy_checks_body_instead_of_conflicting_headers() {
    let mut req = request(Method::PUT).header("x-amz-tagging", "classification=public").build();
    let action = Action::S3Action(S3Action::PutObjectTaggingAction);
    // The XML tag set `classification=confidential`, as the edge decodes it.
    req.install_requested_object_tags(RequestedObjectTags(vec![("classification".to_string(), "confidential".to_string())]));
    let conditions = remaining_retention_days_conditions(&req, action);
    assert_eq!(conditions.get("RequestObjectTag/classification"), Some(&vec!["confidential".to_string()]));
    assert_eq!(conditions.get("RequestObjectTagKeys"), Some(&vec!["classification".to_string()]));
    req.install_requested_object_tags(RequestedObjectTags::default());
    let conditions = remaining_retention_days_conditions(&req, action);
    assert!(!conditions.contains_key("RequestObjectTag/classification"));
    assert_eq!(conditions.get("RequestObjectTagKeys"), Some(&Vec::new()));
}

#[test]
fn post_object_policy_checks_form_fields_instead_of_headers() {
    let mut req = request(Method::POST)
        .header("x-amz-object-lock-legal-hold", "OFF")
        .header("x-amz-object-lock-mode", "GOVERNANCE")
        .header("x-amz-object-lock-retain-until-date", "2029-01-01T00:00:00Z")
        .header("x-amz-tagging", "classification=public")
        .build();
    PostObjectFormPolicy::new(PostObjectPolicyFields {
        legal_hold: Some("ON"),
        lock_mode: Some("COMPLIANCE"),
        tagging: Some("classification=confidential"),
        ..Default::default()
    })
    .expect("form fields project onto policy headers")
    .install(&mut req);
    let action = Action::S3Action(S3Action::PutObjectAction);
    let conditions = remaining_retention_days_conditions(&req, action);
    assert_eq!(conditions.get("object-lock-legal-hold"), Some(&vec!["ON".to_string()]));
    assert_eq!(conditions.get("object-lock-mode"), Some(&vec!["COMPLIANCE".to_string()]));
    // The form sets no retain-until date, so the header must not stand in for one.
    assert!(!conditions.contains_key("object-lock-retain-until-date"));
    assert_eq!(conditions.get("RequestObjectTag/classification"), Some(&vec!["confidential".to_string()]));
    // A PUT installs no form projection and keeps reading its headers.
    let put = request(Method::PUT).header("x-amz-object-lock-legal-hold", "ON").build();
    let conditions = remaining_retention_days_conditions(&put, action);
    assert_eq!(conditions.get("object-lock-legal-hold"), Some(&vec!["ON".to_string()]));
}

#[test]
fn gateway_post_policy_projects_effective_write_conditions() {
    let mut req = request(Method::POST)
        .header("if-none-match", "*")
        .header("if-none-match", "\"second\"")
        .header("x-amz-server-side-encryption", "wire")
        .header("x-amz-server-side-encryption-customer-algorithm", "AES256")
        .build();
    PostObjectFormPolicy::new(PostObjectPolicyFields {
        if_match: Some(HeaderValue::from_static("\"form-only\"")),
        if_none_match: Some(HeaderValue::from_static("\"form-only\"")),
        encryption: Some("form"),
        ..Default::default()
    })
    .expect("valid form policy")
    .install_gateway(&mut req);
    let projected = &req.post_object_policy_headers().expect("projected headers").0;
    assert!(!projected.contains_key("if-match"));
    assert_eq!(projected.get("if-none-match"), Some(&HeaderValue::from_static("*")));
    assert_eq!(projected.get("x-amz-server-side-encryption"), Some(&HeaderValue::from_static("form")));
    assert_eq!(
        projected.get("x-amz-server-side-encryption-customer-algorithm"),
        Some(&HeaderValue::from_static("AES256"))
    );
    assert_eq!(req.headers().get_all("if-none-match").iter().count(), 2);
    assert_eq!(req.headers().get("x-amz-server-side-encryption"), Some(&HeaderValue::from_static("wire")));
}

#[tokio::test]
async fn copy_source_conditions_use_the_decoded_source_for_both_copy_operations() {
    let mut document = serde_json::json!({"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*"},
        {"Effect":"Deny","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*",
         "Condition":{"StringLike":{"s3:x-amz-copy-source":"secret/*"}}}
    ]});
    let identity = Policy::parse_config(&serde_json::to_vec(&document).expect("encode identity policy")).expect("parse policy");
    for statement in document["Statement"].as_array_mut().expect("statements") {
        statement["Principal"] = serde_json::json!("*");
    }
    let bucket: BucketPolicy =
        serde_json::from_slice(&serde_json::to_vec(&document).expect("encode bucket policy")).expect("parse bucket policy");
    // Each header spelling with the source the request parser decodes from it.
    for (spelling, source_bucket, source_key, source_version) in [
        ("secret/report.txt", "secret", "report.txt", None),
        ("%73ecret/report.txt", "secret", "report.txt", None),
        ("secret%2Freport.txt", "secret", "report.txt", None),
        ("/secret/report.txt", "secret", "report.txt", None),
        (
            "%2Fsecret%2Freport.txt?versionId=00000000-0000-0000-0000-000000000000",
            "secret",
            "report.txt",
            Some("00000000-0000-0000-0000-000000000000"),
        ),
        ("public/report.txt", "public", "report.txt", None),
        ("/public%2Freport.txt", "public", "report.txt", None),
    ] {
        for multipart in [false, true] {
            let source = CopySourceObject {
                bucket: source_bucket,
                key: source_key,
                version_id: source_version,
            };
            let server = ServerContextSlot::new();
            let mut req = request(Method::PUT).req_info().build();
            if multipart {
                let _ = authorize_upload_part_copy(&mut req, &server, "bucket", "copy", Some(source)).await;
            } else {
                let facts = CopyObjectAuthorization {
                    bucket: "bucket",
                    key: "copy",
                    source: Some(source),
                    query_version: None,
                    tagging: None,
                    tagging_directive: None,
                    retain_until: None,
                    legal_hold: false,
                    retention: false,
                };
                let _ = authorize_copy_object(&mut req, &server, facts).await;
            }
            req.headers_mut()
                .insert("x-amz-copy-source", HeaderValue::from_str(spelling).expect("source header"));
            let action = Action::S3Action(S3Action::PutObjectAction);
            let conditions = remaining_retention_days_conditions(&req, action);
            let canonical = &conditions["x-amz-copy-source"];
            assert_eq!(canonical.len(), 1, "one semantic source, independent of header spelling");
            let expected = if spelling.contains("public") {
                "public/report.txt"
            } else if spelling.contains('?') {
                "secret/report.txt?versionId=null"
            } else {
                "secret/report.txt"
            };
            assert_eq!(canonical[0], expected, "{spelling}, multipart={multipart}");
            let allowed = expected.starts_with("public/");
            assert_eq!(
                identity
                    .is_allowed(&Args {
                        account: "user",
                        groups: &None,
                        action,
                        bucket: "bucket",
                        object: "copy",
                        conditions: &conditions,
                        claims: &HashMap::new(),
                        is_owner: false,
                        deny_only: false
                    })
                    .await,
                allowed
            );
            assert_eq!(
                bucket
                    .is_allowed(&BucketPolicyArgs {
                        account: "user",
                        groups: &None,
                        action,
                        bucket: "bucket",
                        object: "copy",
                        conditions: &conditions,
                        is_owner: false
                    })
                    .await,
                allowed
            );
        }
    }
}

#[tokio::test]
async fn legal_hold_policy_checks_xml_body() {
    let action = Action::S3Action(S3Action::PutObjectLegalHoldAction);
    for header in [None, Some("ON")] {
        let mut builder = request(Method::PUT).req_info();
        if let Some(header) = header {
            builder = builder.header("x-amz-object-lock-legal-hold", header);
        }
        let mut req = builder.build();
        let server = ServerContextSlot::new();
        // The XML body `<LegalHold><Status>OFF</Status></LegalHold>`, as the edge decodes it.
        let _ = authorize_put_object_legal_hold(
            &mut req,
            &server,
            "bucket",
            "object",
            None,
            RequestedObjectLegalHold(Some("OFF".to_string())),
        )
        .await;
        let conditions = remaining_retention_days_conditions(&req, action);
        assert_eq!(conditions.get("object-lock-legal-hold"), Some(&vec!["OFF".to_string()]));
        assert_requested_value_denied(action, "s3:object-lock-legal-hold", "OFF", &conditions).await;
    }
}

#[tokio::test]
async fn tagging_policy_checks_xml_body() {
    for version in [None, Some("version")] {
        let mut req = request(Method::PUT)
            .req_info()
            .header("x-amz-tagging", "classification=secret&spoofed=yes")
            .build();
        // The XML tag set `classification=public`, as the edge decodes it.
        let server = ServerContextSlot::new();
        let _ = authorize_put_object_tagging(
            &mut req,
            &server,
            "bucket",
            "object",
            version,
            RequestedObjectTags(vec![("classification".to_string(), "public".to_string())]),
        )
        .await;
        let action = versioned_tagging_action(S3Action::PutObjectTaggingAction, version);
        let conditions = remaining_retention_days_conditions(&req, action);
        assert_eq!(conditions.get("RequestObjectTag/classification"), Some(&vec!["public".to_string()]));
        assert_eq!(conditions.get("RequestObjectTagKeys"), Some(&vec!["classification".to_string()]));
        assert!(!conditions.contains_key("RequestObjectTag/spoofed"));
        assert_requested_value_denied(action, "s3:RequestObjectTag/classification", "public", &conditions).await;
    }
}

#[tokio::test]
async fn post_object_policy_checks_form_fields() {
    let mut req = request(Method::POST)
        .req_info()
        .header("x-amz-object-lock-mode", "GOVERNANCE")
        .header("x-amz-object-lock-legal-hold", "OFF")
        .header("x-amz-object-lock-retain-until-date", "2029-01-01T00:00:00Z")
        .header("x-amz-storage-class", "STANDARD")
        .header("x-amz-tagging", "classification=secret&spoofed=yes")
        .header("x-amz-acl", "public-read")
        .build();
    let retain_until = OffsetDateTime::parse("2035-01-01T00:00:00Z", &Rfc3339).unwrap();
    let server = ServerContextSlot::new();
    // The form fields as the POST edge decodes them; the form names no ACL.
    let input = PutObjectAuthorization {
        bucket: "bucket",
        key: "object",
        version_id: None,
        retain_until: Some(retain_until),
        specific_etag: false,
        legal_hold: true,
        retention: true,
        post: true,
    };
    let _ = authorize_put_object(&mut req, &server, input, |req| {
        PostObjectFormPolicy::new(PostObjectPolicyFields {
            lock_mode: Some("COMPLIANCE"),
            legal_hold: Some("ON"),
            retain_until: Some(retain_until),
            storage_class: Some("STANDARD_IA"),
            tagging: Some("classification=public"),
            ..Default::default()
        })?
        .install(req);
        Ok(())
    })
    .await;
    let action = Action::S3Action(S3Action::PutObjectAction);
    let conditions = remaining_retention_days_conditions(&req, action);
    for (key, value) in [
        ("object-lock-mode", "COMPLIANCE"),
        ("object-lock-legal-hold", "ON"),
        ("object-lock-retain-until-date", "2035-01-01T00:00:00Z"),
        ("x-amz-storage-class", "STANDARD_IA"),
        ("RequestObjectTag/classification", "public"),
    ] {
        assert_eq!(conditions.get(key), Some(&vec![value.to_string()]));
    }
    assert!(!conditions.contains_key("x-amz-acl"));
    assert!(!conditions.contains_key("RequestObjectTag/spoofed"));
    assert_requested_value_denied(action, "s3:object-lock-mode", "COMPLIANCE", &conditions).await;
    assert_requested_value_denied(action, "s3:RequestObjectTag/classification", "public", &conditions).await;
    // The final PostObject check reads the same request, so it sees the same form values.
    let post_conditions = remaining_retention_days_conditions(&req, action);
    assert_eq!(post_conditions.get("object-lock-mode"), conditions.get("object-lock-mode"));
    assert_eq!(
        post_conditions.get("RequestObjectTag/classification"),
        conditions.get("RequestObjectTag/classification")
    );
}

#[tokio::test]
async fn retention_policy_checks_body_instead_of_conflicting_headers() {
    let mut req = request(Method::PUT)
        .header("x-amz-object-lock-mode", "GOVERNANCE")
        .header("x-amz-object-lock-retain-until-date", "2029-01-01T00:00:00Z")
        .build();
    // The XML body's retention, as the edge decodes it.
    let applied = OffsetDateTime::parse("2035-01-02T00:00:00+23:59", &Rfc3339).expect("body date should parse");
    let retention = RequestedObjectLockRetention {
        mode: Some("COMPLIANCE".to_string()),
        retain_until: Some(applied),
    };
    set_requested_object_lock_retain_until(&mut req, retention.retain_until);
    req.install_requested_retention(retention);
    let action = Action::S3Action(S3Action::PutObjectRetentionAction);
    let conditions = remaining_retention_days_conditions(&req, action);
    assert_eq!(conditions.get("object-lock-mode"), Some(&vec!["COMPLIANCE".to_string()]));
    let authorized =
        OffsetDateTime::parse(&conditions["object-lock-retain-until-date"][0], &Rfc3339).expect("condition date should parse");
    assert_eq!(authorized.unix_timestamp(), applied.unix_timestamp());
    let policy = Policy::parse_config(
        br#"{
        "Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObjectRetention","Resource":"arn:aws:s3:::bucket/*",
        "Condition":{"DateLessThan":{"s3:object-lock-retain-until-date":"2030-01-01T00:00:00Z"}}}]
    }"#,
    )
    .expect("bounded retention policy should parse");
    let args = Args {
        account: "writer",
        groups: &None,
        action,
        bucket: "bucket",
        conditions: &conditions,
        is_owner: false,
        object: "object",
        claims: &HashMap::new(),
        deny_only: false,
    };
    assert!(
        !policy.is_allowed(&args).await,
        "an in-range header cannot authorize an out-of-range body"
    );
    // A body without retention applies none; the headers must not fill it in.
    let retention = RequestedObjectLockRetention::default();
    set_requested_object_lock_retain_until(&mut req, retention.retain_until);
    req.install_requested_retention(retention);
    let conditions = remaining_retention_days_conditions(&req, action);
    assert!(!conditions.contains_key("object-lock-mode"));
    assert!(!conditions.contains_key("object-lock-retain-until-date"));
}

#[tokio::test]
async fn bucket_policy_limits_retention_to_thirty_days() {
    let policy: BucketPolicy = serde_json::from_str(
        r#"{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Principal":{"AWS":"*"},
      "Action":["s3:PutObjectRetention"],
      "Resource":["arn:aws:s3:::bucket/*"]
    },
    {
      "Effect":"Deny",
      "Principal":{"AWS":"*"},
      "Action":["s3:PutObjectRetention"],
      "Resource":["arn:aws:s3:::bucket/*"],
      "Condition":{"NumericGreaterThan":{"s3:object-lock-remaining-retention-days":"30"}}
    }
  ]
}"#,
    )
    .expect("bucket policy should parse");
    let no_groups: Option<Vec<String>> = None;
    let action = Action::S3Action(S3Action::PutObjectRetentionAction);
    let spoofed_header = |mut req: RequestEnvelope| {
        req.headers_mut()
            .insert(OBJECT_LOCK_REMAINING_RETENTION_DAYS_CONDITION, HeaderValue::from_static("1"));
        req
    };

    let cases = [
        ("29 days", request_retaining_for(time::Duration::days(29)), true),
        ("30 days", request_retaining_for(time::Duration::days(30)), true),
        ("31 days", request_retaining_for(time::Duration::days(31)), false),
        (
            "30 days and a minute",
            request_retaining_for(time::Duration::days(30) + time::Duration::MINUTE),
            false,
        ),
        ("no retention", request(Method::PUT).build(), true),
        (
            "31 days with spoofed header",
            spoofed_header(request_retaining_for(time::Duration::days(31))),
            false,
        ),
    ];

    for (name, req, allowed) in cases {
        let conditions = remaining_retention_days_conditions(&req, action);
        let args = BucketPolicyArgs {
            bucket: "bucket",
            action,
            is_owner: false,
            account: "",
            groups: &no_groups,
            conditions: &conditions,
            object: "obj",
        };

        assert_eq!(policy.is_allowed(&args).await, allowed, "{name}");
    }
}

#[test]
fn replication_proxy_requires_authorization_independent_of_local_tags() {
    let mut req = request(Method::PUT).build();
    assert!(proxy_allowed(&req));
    // A local miss produces an empty tag map, but still establishes that the
    // policy needs tags. Neither anonymous nor authenticated callers may proxy.
    req.install_object_tag_conditions(ObjectTagConditions::new("bucket", "missing", None, HashMap::new()));
    assert!(!proxy_allowed(&req));
    req.install_object_tag_conditions(ObjectTagConditions::new(
        "bucket",
        "missing",
        Some("version"),
        HashMap::from([("ExistingObjectTag/protected".to_string(), vec!["yes".to_string()])]),
    ));
    assert!(!proxy_allowed(&req));
}
