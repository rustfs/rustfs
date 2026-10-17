// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0
use rustfs_policy::policy::{Args, Policy, action::Action};
use serde_json::{Value, json};
use std::collections::HashMap;

fn allowed(statements: Value, action: &str, bucket: &str, object: &str) -> bool {
    let policy: Policy =
        serde_json::from_value(json!({"Version":"2012-10-17","Statement":statements})).expect("valid artifact IAM policy");
    pollster::block_on(policy.is_allowed(&Args {
        account: "controller",
        groups: &None,
        action: Action::try_from(action).expect("registered artifact action"),
        bucket,
        object,
        conditions: &HashMap::new(),
        claims: &HashMap::new(),
        is_owner: false,
        deny_only: false,
    }))
}

#[test]
fn artifact_resource_allow_is_scoped_to_bucket_and_attempt() {
    for action in ["admin:SealPluginArtifactAttempt", "admin:RetirePluginArtifactVersion"] {
        let policy = json!([{"Effect":"Allow","Action":action,"Resource":["arn:aws:s3:::artifacts/out/attempt-a", "arn:aws:s3:::artifacts/out/attempt-a/*"]}]);
        assert!(allowed(policy.clone(), action, "artifacts", "out/attempt-a/"));
        assert!(allowed(policy.clone(), action, "artifacts", "out/attempt-a/image.png"));
        assert!(
            !allowed(policy.clone(), action, "other", "out/attempt-a/image.png"),
            "ARTIFACT_RESOURCE_BUCKET"
        );
        assert!(!allowed(policy, action, "artifacts", "out/attempt-b/"), "ARTIFACT_RESOURCE_ATTEMPT");
    }
}

#[test]
fn artifact_resource_deny_and_not_resource_are_enforced() {
    for action in ["admin:SealPluginArtifactAttempt", "admin:RetirePluginArtifactVersion"] {
        let scoped_allow = json!({"Effect":"Allow","Action":action,"Resource":"arn:aws:s3:::artifacts/out/*"});
        let explicit_deny = json!({"Effect":"Deny","Action":action,"Resource":"arn:aws:s3:::artifacts/out/private/*"});
        assert!(!allowed(
            json!([scoped_allow.clone(), explicit_deny.clone()]),
            action,
            "artifacts",
            "out/private/image"
        ));
        assert!(allowed(
            json!([scoped_allow.clone(), explicit_deny]),
            action,
            "artifacts",
            "out/public/image"
        ));
        let except_public = json!({"Effect":"Deny","Action":action,"NotResource":"arn:aws:s3:::artifacts/out/public/*"});
        assert!(allowed(
            json!([scoped_allow.clone(), except_public.clone()]),
            action,
            "artifacts",
            "out/public/image"
        ));
        assert!(!allowed(
            json!([scoped_allow.clone(), except_public]),
            action,
            "artifacts",
            "out/private/image"
        ));
        let allow_except_private =
            json!([{"Effect":"Allow","Action":action,"NotResource":"arn:aws:s3:::artifacts/out/private/*"}]);
        assert!(!allowed(allow_except_private.clone(), action, "artifacts", "out/private/image"));
        assert!(allowed(allow_except_private, action, "artifacts", "out/public/image"));
        assert!(
            !allowed(
                json!([scoped_allow, {"Effect":"Deny","Action":"admin:*"}]),
                action,
                "artifacts",
                "out/public/image"
            ),
            "ARTIFACT_GLOBAL_DENY"
        );
    }
}

#[test]
fn artifact_resource_empty_allow_is_rejected_without_changing_old_admin() {
    for action in ["admin:SealPluginArtifactAttempt", "admin:RetirePluginArtifactVersion"] {
        for granted in [action, "admin:*"] {
            assert!(
                !allowed(json!([{"Effect":"Allow","Action":granted}]), action, "artifacts", "out/"),
                "ARTIFACT_RESOURCE_REQUIRED"
            );
        }
    }
    assert!(allowed(
        json!([{"Effect":"Allow","Action":"admin:IssuePluginAttempt"}]),
        "admin:IssuePluginAttempt",
        "",
        ""
    ));
    assert!(allowed(
        json!([{"Effect":"Allow","Action":"admin:IssuePluginAttempt","Resource":"arn:aws:s3:::unrelated/*"}]),
        "admin:IssuePluginAttempt",
        "",
        ""
    ));
}

#[test]
fn artifact_resource_single_object_does_not_authorize_attempt_seal() {
    let action = "admin:SealPluginArtifactAttempt";
    assert!(!allowed(
        json!([{"Effect":"Allow","Action":action,"Resource":"arn:aws:s3:::artifacts/out/attempt-a/image.png"}]),
        action,
        "artifacts",
        "out/attempt-a/"
    ));
    assert!(allowed(
        json!([{"Effect":"Allow","Action":action,"Resource":"arn:aws:s3:::artifacts/out/attempt-a"}]),
        action,
        "artifacts",
        "out/attempt-a/"
    ));
}

#[test]
fn artifact_bucket_create_resource_requires_exact_bucket_authority() {
    let action = "admin:CreatePluginArtifactBucket";
    let scoped = json!({"Effect":"Allow","Action":action,"Resource":"arn:aws:s3:::artifacts"});
    assert!(allowed(json!([scoped.clone()]), action, "artifacts", ""));
    assert!(!allowed(json!([scoped.clone()]), action, "another", ""), "ARTIFACT_CREATE_RESOURCE");
    assert!(!allowed(
        json!([{"Effect":"Allow","Action":action,"Resource":"arn:aws:s3:::artifacts/*"}]),
        action,
        "artifacts",
        ""
    ));
    assert!(!allowed(json!([{"Effect":"Allow","Action":action}]), action, "artifacts", ""));
    assert!(!allowed(
        json!([scoped.clone(), {"Effect":"Deny","Action":"admin:*"}]),
        action,
        "artifacts",
        ""
    ));
    assert!(!allowed(
        json!([scoped, {"Effect":"Deny","Action":action,"Resource":"arn:aws:s3:::artifacts"}]),
        action,
        "artifacts",
        ""
    ));
    let not_resource = json!([{"Effect":"Allow","Action":action,"NotResource":"arn:aws:s3:::artifacts"}]);
    assert!(!allowed(not_resource.clone(), action, "artifacts", ""));
    assert!(allowed(not_resource, action, "another", ""));
}
