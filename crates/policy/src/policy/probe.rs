// Copyright 2024 RustFS Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! Read-only permission-query facts. Potential is an entry-point hint, never authorization.

use super::function::is_server_derived_condition_key;
use super::resource::Resource;
use super::statement::variable_resolver_for_policy_args;
use super::utils::wildcard;
use super::{Args, BucketPolicy, BucketPolicyArgs, Effect, Functions, Policy, ResourceSet};

pub const PROBE_EXISTING_TAGS_RESOLVED: &str = "rustfs-probe-existing-tags-resolved";
pub const PROBE_RETENTION_FACTS_KNOWN: &str = "rustfs-probe-retention-facts-known";
pub const PROBE_COPY_SOURCE_KNOWN: &str = "rustfs-probe-copy-source-known";
pub const PROBE_IF_MATCH_KNOWN: &str = "rustfs-probe-if-match-known";
pub const PROBE_IF_NONE_MATCH_KNOWN: &str = "rustfs-probe-if-none-match-known";

/// Whether all facts referenced by a condition are available to a permission query.
/// Request-body/header conditions are deliberately unknown because the query is not the mutation.
pub fn conditions_known_for_probe(functions: &Functions, conditions: &std::collections::HashMap<String, Vec<String>>) -> bool {
    functions.key_names().all(|name| {
        if name.starts_with("ExistingObjectTag/") {
            return conditions.contains_key(PROBE_EXISTING_TAGS_RESOLVED)
                || conditions.keys().any(|key| key.starts_with("ExistingObjectTag/"));
        }
        if matches!(name.as_str(), "prefix" | "versionid") {
            return conditions.contains_key(&name);
        }
        if matches!(
            name.as_str(),
            "object-lock-mode" | "object-lock-retain-until-date" | "object-lock-remaining-retention-days"
        ) {
            return conditions.contains_key(&name) || conditions.contains_key(PROBE_RETENTION_FACTS_KNOWN);
        }
        if name == "object-lock-legal-hold" {
            return conditions.contains_key(&name);
        }
        if name == "RequestObjectTagKeys" || name.starts_with("RequestObjectTag/") {
            return conditions.contains_key("RequestObjectTagKeys");
        }
        if name == "x-amz-copy-source" {
            return conditions.contains_key(PROBE_COPY_SOURCE_KNOWN) || conditions.contains_key(&name);
        }
        if name == "if-match" {
            return conditions.contains_key(PROBE_IF_MATCH_KNOWN) || conditions.contains_key(&name);
        }
        if name == "if-none-match" {
            return conditions.contains_key(PROBE_IF_NONE_MATCH_KNOWN) || conditions.contains_key(&name);
        }
        is_server_derived_condition_key(&name)
            && !matches!(name.as_str(), "Referer" | "UserAgent" | "SignatureAge" | "LocationConstraint")
            && !name.starts_with("x-amz-")
    })
}

/// The decision when every relevant request condition is known, or when unknown
/// conditions cannot change the result. None means more operation facts are required.
pub async fn policy_probe_decision(policy: &Policy, args: &Args<'_>) -> Option<bool> {
    let resolver = variable_resolver_for_policy_args(args);
    let mut allow = false;
    let mut unknown_allow = false;
    let mut unknown_deny = false;
    for statement in &policy.statements {
        if !statement.request_reaches_condition_eval(args, &resolver).await {
            continue;
        }
        let deny = matches!(statement.effect, Effect::Deny);
        if conditions_known_for_probe(&statement.conditions, args.conditions) {
            if !statement
                .conditions
                .evaluate_with_resolver(args.conditions, Some(&resolver))
                .await
            {
                continue;
            }
            if deny {
                return Some(false);
            }
            allow = true;
        } else if deny {
            unknown_deny = true;
        } else {
            unknown_allow = true;
        }
    }
    if unknown_deny {
        None
    } else if allow || args.is_owner || args.deny_only {
        Some(true)
    } else if unknown_allow {
        None
    } else {
        Some(false)
    }
}

/// Bucket-policy counterpart of policy_probe_decision. Owner queries inspect Deny only.
pub async fn bucket_policy_probe_decision(policy: &BucketPolicy, args: &BucketPolicyArgs<'_>) -> Option<bool> {
    let mut allow = false;
    let mut unknown_allow = false;
    let mut unknown_deny = false;
    for statement in &policy.statements {
        if !statement.request_reaches_condition_eval(args).await {
            continue;
        }
        let deny = matches!(statement.effect, Effect::Deny);
        if conditions_known_for_probe(&statement.conditions, args.conditions) {
            if !statement.conditions.evaluate(args.conditions).await {
                continue;
            }
            if deny {
                return Some(false);
            }
            allow = true;
        } else if deny {
            unknown_deny = true;
        } else {
            unknown_allow = true;
        }
    }
    if unknown_deny {
        None
    } else if allow || args.is_owner {
        Some(true)
    } else if unknown_allow {
        None
    } else {
        Some(false)
    }
}

/// True only when unknown conditions can change this exact policy decision.
pub async fn policy_has_unknown_probe_conditions(policy: &Policy, args: &Args<'_>) -> bool {
    policy_probe_decision(policy, args).await.is_none()
}

/// Bucket-policy counterpart of policy_has_unknown_probe_conditions.
pub async fn bucket_policy_has_unknown_probe_conditions(policy: &BucketPolicy, args: &BucketPolicyArgs<'_>) -> bool {
    bucket_policy_probe_decision(policy, args).await.is_none()
}

fn resource_may_name_bucket(resource: &Resource, bucket: &str) -> bool {
    match resource {
        Resource::All => true,
        Resource::S3(pattern) => {
            if bucket.is_empty() || pattern.contains("${") {
                return true;
            }
            let bucket_pattern = pattern.split_once('/').map_or(pattern.as_str(), |(bucket, _)| bucket);
            wildcard::is_match(bucket_pattern, bucket)
        }
        Resource::Kms(_) => bucket.is_empty(),
    }
}

fn resource_covers_bucket(resource: &Resource, bucket: &str) -> bool {
    match resource {
        Resource::All => true,
        Resource::S3(pattern) if !pattern.contains("${") => {
            pattern == "*"
                || pattern.split_once('/').is_some_and(|(bucket_pattern, object_pattern)| {
                    object_pattern == "*" && !bucket.is_empty() && wildcard::is_match(bucket_pattern, bucket)
                })
        }
        _ => false,
    }
}

fn possible_resources(resources: &ResourceSet, not_resources: &ResourceSet, bucket: &str) -> bool {
    (resources.is_empty() || resources.iter().any(|resource| resource_may_name_bucket(resource, bucket)))
        && !not_resources.iter().any(|resource| resource_covers_bucket(resource, bucket))
}

/// A deny whose known conditions and resources cover the whole bucket blocks discoverability.
pub async fn bucket_policy_blocks_action_potential(policy: &BucketPolicy, args: &BucketPolicyArgs<'_>) -> bool {
    for statement in &policy.statements {
        if matches!(statement.effect, Effect::Deny)
            && statement.principal.is_match(args.account)
            && statement.actions.statement_covers(&statement.not_actions, &args.action, true)
            && statement.not_resources.is_empty()
            && statement
                .resources
                .iter()
                .any(|resource| resource_covers_bucket(resource, args.bucket))
            && conditions_known_for_probe(&statement.conditions, args.conditions)
            && statement.conditions.evaluate(args.conditions).await
        {
            return true;
        }
    }
    false
}

/// Conservatively reports whether this action might be available somewhere in the known bucket.
/// Disjoint object grants, partial denies and unknown request conditions remain possible; callers
/// must authorize the exact target before executing any operation.
pub async fn policy_action_potential(policy: &Policy, args: &Args<'_>) -> bool {
    let resolver = variable_resolver_for_policy_args(args);
    let mut potential = false;
    for statement in &policy.statements {
        let deny = matches!(statement.effect, Effect::Deny);
        if !statement.actions.statement_covers(&statement.not_actions, &args.action, deny)
            || !possible_resources(&statement.resources, &statement.not_resources, args.bucket)
        {
            continue;
        }
        let known = conditions_known_for_probe(&statement.conditions, args.conditions);
        if known
            && !statement
                .conditions
                .evaluate_with_resolver(args.conditions, Some(&resolver))
                .await
        {
            continue;
        }
        if deny {
            if known
                && statement.not_resources.is_empty()
                && statement
                    .resources
                    .iter()
                    .any(|resource| resource_covers_bucket(resource, args.bucket))
            {
                return false;
            }
        } else {
            potential = true;
        }
    }
    potential
}

/// Entry-point hint for a bucket policy, including its Principal and known identity conditions.
pub async fn bucket_policy_action_potential(policy: &BucketPolicy, args: &BucketPolicyArgs<'_>) -> bool {
    let mut potential = false;
    for statement in &policy.statements {
        let deny = matches!(statement.effect, Effect::Deny);
        if !statement.principal.is_match(args.account)
            || !statement.actions.statement_covers(&statement.not_actions, &args.action, deny)
            || !possible_resources(&statement.resources, &statement.not_resources, args.bucket)
        {
            continue;
        }
        let known = conditions_known_for_probe(&statement.conditions, args.conditions);
        if known && !statement.conditions.evaluate(args.conditions).await {
            continue;
        }
        if deny {
            if known
                && statement.not_resources.is_empty()
                && statement
                    .resources
                    .iter()
                    .any(|resource| resource_covers_bucket(resource, args.bucket))
            {
                return false;
            }
        } else {
            potential = true;
        }
    }
    potential
}

#[cfg(test)]
mod tests {
    use super::super::action::{Action, S3Action};
    use super::*;
    use std::collections::HashMap;

    fn decode<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> Result<T, serde_json::Error> {
        serde_json::from_str(&value.to_string())
    }

    fn args<'a>(conditions: &'a HashMap<String, Vec<String>>, claims: &'a HashMap<String, serde_json::Value>) -> Args<'a> {
        Args {
            account: "alice",
            groups: &None,
            action: Action::S3Action(S3Action::PutObjectAction),
            bucket: "alice-data",
            object: "",
            conditions,
            is_owner: false,
            claims,
            deny_only: false,
        }
    }

    #[tokio::test]
    async fn action_potential_distinguishes_readonly_and_scoped_write_without_a_fake_object() {
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let args = args(&conditions, &claims);
        for (action, expected) in [("s3:GetObject", false), ("s3:PutObject", true)] {
            let policy: Policy = decode(serde_json::json!({"Version":"2012-10-17", "Statement":[{
                "Effect":"Allow", "Action":action, "Resource":"arn:aws:s3:::alice-data/private/*"
            }]}))
            .expect("probe policy parses");
            assert_eq!(policy_action_potential(&policy, &args).await, expected);
        }
    }

    #[tokio::test]
    async fn bucket_potential_evaluates_verified_identity_conditions() {
        let policy: BucketPolicy = decode(serde_json::json!({"Version":"2012-10-17", "Statement":[{
            "Effect":"Allow", "Principal":"*", "Action":"s3:PutObject", "Resource":"arn:aws:s3:::alice-data/*",
            "Condition":{"StringEquals":{"jwt:preferred_username":"alice"}}
        }]}))
        .expect("bucket probe policy parses");
        for (username, expected) in [("alice", true), ("bob", false)] {
            let conditions = HashMap::from([("preferred_username".into(), vec![username.into()])]);
            let args = BucketPolicyArgs {
                account: "alice",
                groups: &None,
                action: super::super::action::Action::S3Action(S3Action::PutObjectAction),
                bucket: "alice-data",
                object: "",
                conditions: &conditions,
                is_owner: false,
            };
            assert_eq!(bucket_policy_action_potential(&policy, &args).await, expected);
        }
    }

    #[tokio::test]
    async fn exact_probe_marks_request_header_conditions_unknown() {
        let policy: Policy = decode(serde_json::json!({"Version":"2012-10-17", "Statement":[{
            "Effect":"Allow", "Action":"s3:PutObject", "Resource":"arn:aws:s3:::alice-data/*",
            "Condition":{"Null":{"s3:x-amz-server-side-encryption":"true"}}
        }]}))
        .expect("conditional probe policy parses");
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let mut args = args(&conditions, &claims);
        args.object = "object";
        assert!(policy_has_unknown_probe_conditions(&policy, &args).await);
    }

    #[tokio::test]
    async fn irrelevant_unknown_allows_do_not_override_known_decisions() {
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let mut args = args(&conditions, &claims);
        args.object = "object";
        for (effect, expected) in [("Allow", Some(true)), ("Deny", None)] {
            let policy: Policy = decode(serde_json::json!({"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::alice-data/*"},
                {"Effect":effect,"Action":"s3:PutObject","Resource":"arn:aws:s3:::alice-data/*",
                 "Condition":{"Null":{"s3:x-amz-server-side-encryption":"true"}}}
            ]}))
            .expect("mixed known and unknown policy");
            assert_eq!(policy_probe_decision(&policy, &args).await, expected);
        }
        let deny: Policy = decode(serde_json::json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Deny","Action":"s3:PutObject","Resource":"arn:aws:s3:::alice-data/*"},
            {"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::alice-data/*",
             "Condition":{"Null":{"s3:x-amz-server-side-encryption":"true"}}}
        ]}))
        .expect("known denial policy");
        assert_eq!(policy_probe_decision(&deny, &args).await, Some(false));
    }

    #[tokio::test]
    async fn bucket_deny_only_queries_ignore_unknown_allow_statements() {
        let policy: BucketPolicy = decode(serde_json::json!({"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::alice-data/*",
             "Condition":{"Null":{"s3:x-amz-server-side-encryption":"true"}}}
        ]}))
        .expect("unknown bucket grant");
        let conditions = HashMap::new();
        let args = BucketPolicyArgs {
            account: "alice",
            groups: &None,
            action: Action::S3Action(S3Action::PutObjectAction),
            bucket: "alice-data",
            object: "object",
            conditions: &conditions,
            is_owner: true,
        };
        assert_eq!(bucket_policy_probe_decision(&policy, &args).await, Some(true));
        assert_eq!(
            bucket_policy_probe_decision(&policy, &BucketPolicyArgs { is_owner: false, ..args }).await,
            None
        );
    }
}
