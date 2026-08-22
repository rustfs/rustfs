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

use super::{
    ClaimLookup, Effect, Error as IamError, Functions, ID, Statement, action::Action, get_claim_case_insensitive,
    statement::BPStatement, statement::variable_resolver_for_policy_args,
};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
/// DEFAULT_VERSION is the default version.
/// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_version.html
pub const DEFAULT_VERSION: &str = "2012-10-17";

/// check the data is Validator
pub trait Validator {
    type Error;
    fn is_valid(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args<'a> {
    pub account: &'a str,
    pub groups: &'a Option<Vec<String>>,
    pub action: Action,
    pub bucket: &'a str,
    pub conditions: &'a HashMap<String, Vec<String>>,
    pub is_owner: bool,
    pub object: &'a str,
    pub claims: &'a HashMap<String, Value>,
    pub deny_only: bool,
}

impl Args<'_> {
    pub fn get_role_arn(&self) -> Option<&str> {
        self.claims.get("roleArn").and_then(|x| x.as_str())
    }
    pub fn get_policies(&self, policy_claim_name: &str) -> (HashSet<String>, bool) {
        get_policies_from_claims(self.claims, policy_claim_name)
    }
}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    #[serde(default, rename = "ID", skip_serializing_if = "ID::is_empty")]
    pub id: ID,
    #[serde(default, rename = "Version")]
    pub version: String,
    #[serde(default, rename = "Statement")]
    pub statements: Vec<Statement>,
}

impl Policy {
    pub async fn is_allowed(&self, args: &Args<'_>) -> bool {
        // First, check all Deny statements - if any Deny matches, deny the request
        for statement in self.statements.iter().filter(|s| matches!(s.effect, Effect::Deny)) {
            if !statement.is_allowed(args).await {
                return false;
            }
        }

        // DenyOnly mode: only validate explicit Deny statements.
        // If no Deny matched above, allow the request.
        if args.deny_only {
            return true;
        }

        // Owner has all permissions
        if args.is_owner {
            return true;
        }

        // Check Allow statements
        for statement in self.statements.iter().filter(|s| matches!(s.effect, Effect::Allow)) {
            if statement.is_allowed(args).await {
                return true;
            }
        }

        false
    }

    pub async fn match_resource(&self, resource: &str) -> bool {
        for statement in self.statements.iter() {
            if statement.resources.match_resource(resource).await {
                return true;
            }
        }
        false
    }

    fn drop_duplicate_statements(&mut self) {
        let mut dups = HashSet::new();
        for i in 0..self.statements.len() {
            if dups.contains(&i) {
                // i is already a duplicate of some statement, so we do not need to
                // compare with it.
                continue;
            }
            for j in (i + 1)..self.statements.len() {
                if !self.statements[i].eq(&self.statements[j]) {
                    continue;
                }

                // save duplicate statement index for removal.
                dups.insert(j);
            }
        }

        // remove duplicate items from the slice.
        let mut c = 0;
        for i in 0..self.statements.len() {
            if dups.contains(&i) {
                continue;
            }
            self.statements[c] = self.statements[i].clone();
            c += 1;
        }
        self.statements.truncate(c);
    }
    pub fn merge_policies(inputs: Vec<Policy>) -> Policy {
        let mut merged = Policy::default();

        for p in inputs {
            if merged.version.is_empty() {
                merged.version = p.version.clone();
            }
            for st in p.statements {
                merged.statements.push(st.clone());
            }
        }
        merged.drop_duplicate_statements();
        merged
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    pub fn validate(&self) -> Result<()> {
        self.is_valid()
    }

    pub fn parse_config(data: &[u8]) -> Result<Policy> {
        let policy: Policy = serde_json::from_slice(data)?;
        policy.validate()?;
        Ok(policy)
    }
}

impl Validator for Policy {
    type Error = Error;

    fn is_valid(&self) -> Result<()> {
        if !self.version.is_empty() && !self.version.eq(DEFAULT_VERSION) {
            return Err(IamError::InvalidVersion(self.version.clone()).into());
        }

        for statement in self.statements.iter() {
            statement.is_valid()?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketPolicyArgs<'a> {
    pub account: &'a str,
    pub groups: &'a Option<Vec<String>>,
    pub action: Action,
    pub bucket: &'a str,
    pub conditions: &'a HashMap<String, Vec<String>>,
    pub is_owner: bool,
    pub object: &'a str,
}

/// Bucket Policy with AWS S3-compatible JSON serialization.
/// Empty optional fields are omitted from output to match AWS format.
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
#[serde(deny_unknown_fields)]
pub struct BucketPolicy {
    // RUSTFS_COMPAT_TODO(rustfs-6339): accept bucket policies persisted with the legacy "ID" key. Remove after migration tooling rewrites every retained legacy bucket policy.
    #[serde(default, rename = "Id", alias = "ID", skip_serializing_if = "ID::is_empty")]
    pub id: ID,
    #[serde(rename = "Version")]
    pub version: String,
    #[serde(rename = "Statement")]
    pub statements: Vec<BPStatement>,
}

impl BucketPolicy {
    pub async fn is_allowed(&self, args: &BucketPolicyArgs<'_>) -> bool {
        for statement in self.statements.iter().filter(|s| matches!(s.effect, Effect::Deny)) {
            if !statement.is_allowed(args).await {
                return false;
            }
        }

        if args.is_owner {
            return true;
        }

        for statement in self.statements.iter().filter(|s| matches!(s.effect, Effect::Allow)) {
            if statement.is_allowed(args).await {
                return true;
            }
        }

        false
    }
}

impl Validator for BucketPolicy {
    type Error = Error;

    fn is_valid(&self) -> Result<()> {
        if !self.version.is_empty() && !self.version.eq(DEFAULT_VERSION) {
            return Err(IamError::InvalidVersion(self.version.clone()).into());
        }

        for statement in self.statements.iter() {
            statement.is_valid()?;
        }

        Ok(())
    }
}

fn get_values_from_claims(claims: &HashMap<String, Value>, claim_name: &str) -> (HashSet<String>, bool) {
    let mut s = HashSet::new();
    match get_claim_case_insensitive(claims, claim_name) {
        ClaimLookup::Found(pname) => {
            if let Some(pnames) = pname.as_array() {
                for pname in pnames {
                    if let Some(pname_str) = pname.as_str() {
                        for pname in pname_str.split(',') {
                            let pname = pname.trim();
                            if !pname.is_empty() {
                                s.insert(pname.to_string());
                            }
                        }
                    }
                }
                return (s, true);
            }

            if let Some(pname_str) = pname.as_str() {
                for pname in pname_str.split(',') {
                    let pname = pname.trim();
                    if !pname.is_empty() {
                        s.insert(pname.to_string());
                    }
                }
                return (s, true);
            }
        }
        ClaimLookup::Missing | ClaimLookup::Ambiguous => {}
    }

    (s, false)
}

pub fn get_policies_from_claims(claims: &HashMap<String, Value>, policy_claim_name: &str) -> (HashSet<String>, bool) {
    get_values_from_claims(claims, policy_claim_name)
}

pub fn iam_policy_claim_name_sa() -> String {
    rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA.to_string()
}

#[inline]
pub fn is_existing_object_tag_condition_key(key: &str) -> bool {
    matches!(key, "ExistingObjectTag" | "s3:ExistingObjectTag")
        || key.starts_with("ExistingObjectTag/")
        || key.starts_with("s3:ExistingObjectTag/")
}

pub fn value_uses_existing_object_tag_condition_key(value: &Value) -> bool {
    match value {
        Value::Object(obj) => obj
            .iter()
            .any(|(key, value)| is_existing_object_tag_condition_key(key) || value_uses_existing_object_tag_condition_key(value)),
        Value::Array(items) => items.iter().any(value_uses_existing_object_tag_condition_key),
        _ => false,
    }
}

/// True if `conditions` JSON references `s3:ExistingObjectTag` / `ExistingObjectTag/...` keys.
pub fn functions_use_existing_object_tag(conditions: &Functions) -> bool {
    serde_json::to_value(conditions)
        .map(|v| value_uses_existing_object_tag_condition_key(&v))
        .unwrap_or(false)
}

pub fn policy_uses_existing_object_tag_conditions(policy: &Policy) -> bool {
    policy
        .statements
        .iter()
        .any(|statement| functions_use_existing_object_tag(&statement.conditions))
}

pub fn bucket_policy_uses_existing_object_tag_conditions(policy: &BucketPolicy) -> bool {
    policy
        .statements
        .iter()
        .any(|statement| functions_use_existing_object_tag(&statement.conditions))
}

/// True when at least one statement that applies to `args` may evaluate ExistingObjectTag conditions.
pub async fn policy_needs_existing_object_tag_for_args(policy: &Policy, args: &Args<'_>) -> bool {
    if !policy_uses_existing_object_tag_conditions(policy) {
        return false;
    }
    let resolver = variable_resolver_for_policy_args(args);
    for statement in &policy.statements {
        if !functions_use_existing_object_tag(&statement.conditions) {
            continue;
        }
        if statement.request_reaches_condition_eval(args, &resolver).await {
            return true;
        }
    }
    false
}

/// True when at least one bucket-policy statement that applies to `args` may evaluate ExistingObjectTag conditions.
pub async fn bucket_policy_needs_existing_object_tag_for_args(policy: &BucketPolicy, args: &BucketPolicyArgs<'_>) -> bool {
    if !bucket_policy_uses_existing_object_tag_conditions(policy) {
        return false;
    }
    for statement in &policy.statements {
        if !functions_use_existing_object_tag(&statement.conditions) {
            continue;
        }
        if statement.request_reaches_condition_eval(args).await {
            return true;
        }
    }
    false
}

pub mod default {
    use std::sync::LazyLock;

    use crate::policy::{
        ActionSet, DEFAULT_VERSION, Effect, Functions, ResourceSet, Statement,
        action::{Action, AdminAction, KmsAction, S3Action, StsAction},
        resource::Resource,
    };

    use super::Policy;

    /// Name of the built-in policy granting KMS key lifecycle management.
    pub const KMS_KEY_ADMINISTRATOR: &str = "KMSKeyAdministrator";
    /// Name of the built-in policy granting cryptographic use of KMS keys.
    pub const KMS_KEY_USER: &str = "KMSKeyUser";
    /// Name of the built-in policy granting read-only visibility into KMS keys.
    pub const KMS_AUDITOR: &str = "KMSAuditor";

    /// Every KMS key, in the resource grammar identity policies use.
    ///
    /// The built-in KMS policies ship unscoped so they behave like the other canned
    /// policies; an operator narrows a copy to `arn:aws:kms:::key/<key_id>` per workload.
    const ALL_KMS_KEYS: &str = "*";

    /// A KMS statement allowing `actions` on every key.
    fn kms_allow(actions: Vec<Action>) -> Statement {
        Statement {
            sid: "".into(),
            effect: Effect::Allow,
            actions: ActionSet(actions),
            not_actions: ActionSet(Default::default()),
            resources: ResourceSet(vec![Resource::Kms(ALL_KMS_KEYS.into())]),
            conditions: Functions::default(),
            ..Default::default()
        }
    }

    /// The `sts:AssumeRole` grant every canned policy carries so an STS session may
    /// assume it.
    fn assume_role_allow() -> Statement {
        Statement {
            sid: "".into(),
            effect: Effect::Allow,
            actions: ActionSet(vec![Action::StsAction(StsAction::AssumeRoleAction)]),
            not_actions: ActionSet(Default::default()),
            resources: ResourceSet(Default::default()),
            conditions: Functions::default(),
            ..Default::default()
        }
    }

    #[allow(clippy::incompatible_msrv)]
    pub static DEFAULT_POLICIES: LazyLock<[(&'static str, Policy); 8]> = LazyLock::new(|| {
        [
            (
                "readwrite",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet(vec![Action::S3Action(S3Action::AllActions)]),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(vec![Resource::S3("*".into())]),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet(vec![Action::StsAction(StsAction::AssumeRoleAction)]),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(Default::default()),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
                    ],
                },
            ),
            (
                "readonly",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet(vec![
                                Action::S3Action(S3Action::GetBucketLocationAction),
                                Action::S3Action(S3Action::GetObjectAction),
                                Action::S3Action(S3Action::GetBucketQuotaAction),
                            ]),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(vec![Resource::S3("*".into())]),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet(vec![Action::StsAction(StsAction::AssumeRoleAction)]),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(Default::default()),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
                    ],
                },
            ),
            (
                "writeonly",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet(vec![Action::S3Action(S3Action::PutObjectAction)]),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(vec![Resource::S3("*".into())]),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet(vec![Action::StsAction(StsAction::AssumeRoleAction)]),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(Default::default()),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
                    ],
                },
            ),
            (
                "diagnostics",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet(vec![
                                Action::AdminAction(AdminAction::ProfilingAdminAction),
                                Action::AdminAction(AdminAction::TraceAdminAction),
                                Action::AdminAction(AdminAction::ConsoleLogAdminAction),
                                Action::AdminAction(AdminAction::ServerInfoAdminAction),
                                Action::AdminAction(AdminAction::TopLocksAdminAction),
                                Action::AdminAction(AdminAction::HealthInfoAdminAction),
                                Action::AdminAction(AdminAction::PrometheusAdminAction),
                                Action::AdminAction(AdminAction::BandwidthMonitorAction),
                            ]),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(vec![Resource::S3("*".into())]),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet(vec![Action::StsAction(StsAction::AssumeRoleAction)]),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(Default::default()),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
                    ],
                },
            ),
            (
                "consoleAdmin",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet(vec![Action::AdminAction(AdminAction::AllAdminActions)]),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(Vec::new()),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet(vec![Action::KmsAction(KmsAction::AllActions)]),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(Vec::new()),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet(vec![Action::S3Action(S3Action::AllActions)]),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(vec![Resource::S3("*".into())]),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet(vec![Action::StsAction(StsAction::AssumeRoleAction)]),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(Vec::new()),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
                    ],
                },
            ),
            // KMS role templates. They deliberately carry no S3 or admin grants, so an
            // operator combines one with a data-plane policy ("readwrite,KMSKeyUser").
            //
            // None of them grants kms:Configure, kms:ServiceControl, kms:ClearCache,
            // kms:Backup or kms:Restore: those act on the KMS service or on the material
            // of every key at once, which is a cluster-administration power rather than a
            // key-management one, and they stay with consoleAdmin. Key creation currently
            // shares kms:Configure with backend configuration, so it stays there too.
            (
                KMS_KEY_ADMINISTRATOR,
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![
                        // Separation of duties: a key administrator governs a key's
                        // lifecycle but is never able to encrypt or decrypt with it.
                        kms_allow(vec![
                            Action::KmsAction(KmsAction::DescribeKeyAction),
                            Action::KmsAction(KmsAction::ListKeysAction),
                            Action::KmsAction(KmsAction::EnableKeyAction),
                            Action::KmsAction(KmsAction::DisableKeyAction),
                            Action::KmsAction(KmsAction::RotateKeyAction),
                            Action::KmsAction(KmsAction::DeleteKeyAction),
                        ]),
                        assume_role_allow(),
                    ],
                },
            ),
            (
                KMS_KEY_USER,
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![
                        // The two actions the SSE-KMS data path evaluates, plus the
                        // metadata read a client needs to tell which key it is using.
                        kms_allow(vec![
                            Action::KmsAction(KmsAction::GenerateDataKeyAction),
                            Action::KmsAction(KmsAction::DecryptAction),
                            Action::KmsAction(KmsAction::DescribeKeyAction),
                        ]),
                        assume_role_allow(),
                    ],
                },
            ),
            (
                KMS_AUDITOR,
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![
                        kms_allow(vec![
                            Action::KmsAction(KmsAction::DescribeKeyAction),
                            Action::KmsAction(KmsAction::ListKeysAction),
                        ]),
                        assume_role_allow(),
                    ],
                },
            ),
        ]
    });
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::error::Result;
    use crate::policy::action::{AdminAction, KmsAction, S3Action};

    #[tokio::test]
    async fn test_parse_policy() -> Result<()> {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::dada/*"],
      "Condition": {
        "StringEquals": {
          "s3:ExistingObjectTag/security": "public"
        }
      }
    },
    {
      "Effect": "Allow",
      "Action": ["s3:DeleteObjectTagging"],
      "Resource": ["arn:aws:s3:::dada/*"],
      "Condition": {
        "StringEquals": {
          "s3:ExistingObjectTag/security": "public"
        }
      }
    },
    {
      "Effect": "Allow",
      "Action": ["s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::dada/*"]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::dada/*"
      ],
      "Condition": {
        "ForAllValues:StringLike": {
          "s3:RequestObjectTagKeys": [
            "security",
            "virus"
          ]
        }
      }
    }
  ]
}
"#;

        let p = Policy::parse_config(data.as_bytes())?;

        let str = serde_json::to_string(&p)?;

        let _p2 = Policy::parse_config(str.as_bytes())?;

        // assert_eq!(p, p2);
        Ok(())
    }

    #[tokio::test]
    async fn test_parse_policy_with_single_string_action_and_resource() -> Result<()> {
        // Test policy with single string Action and Resource (AWS IAM allows both formats)
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::test/analytics/customers/*"
    }
  ]
}
"#;

        let p = Policy::parse_config(data.as_bytes())?;
        assert!(!p.statements.is_empty());
        assert!(!p.statements[0].actions.is_empty());
        assert!(!p.statements[0].resources.is_empty());

        // Test with array format (should still work)
        let data_array = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::test/analytics/customers/*"]
    }
  ]
}
"#;

        let p2 = Policy::parse_config(data_array.as_bytes())?;
        assert!(!p2.statements.is_empty());
        assert!(!p2.statements[0].actions.is_empty());
        assert!(!p2.statements[0].resources.is_empty());

        // Verify that both formats produce equivalent results
        assert_eq!(
            p.statements.len(),
            p2.statements.len(),
            "Both policies should have the same number of statements"
        );
        assert_eq!(
            p.statements[0].actions, p2.statements[0].actions,
            "ActionSet from string format should equal ActionSet from array format"
        );
        assert_eq!(
            p.statements[0].resources, p2.statements[0].resources,
            "ResourceSet from string format should equal ResourceSet from array format"
        );
        assert_eq!(
            p.statements[0].effect, p2.statements[0].effect,
            "Effect should be the same in both formats"
        );

        // Verify specific content
        assert_eq!(p.statements[0].actions.len(), 1, "ActionSet should contain exactly one action");
        assert_eq!(p.statements[0].resources.len(), 1, "ResourceSet should contain exactly one resource");

        Ok(())
    }

    #[tokio::test]
    async fn test_default_policies_allow_sts_assume_role() {
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let args = Args {
            account: "testuser",
            groups: &None,
            action: Action::StsAction(crate::policy::action::StsAction::AssumeRoleAction),
            bucket: "",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };

        for (name, policy) in default::DEFAULT_POLICIES.iter() {
            assert!(policy.is_allowed(&args).await, "default policy {name} should allow sts:AssumeRole");
        }
    }

    #[test]
    fn test_default_policy_names_are_unique() {
        let mut names = HashSet::new();
        for (name, _) in default::DEFAULT_POLICIES.iter() {
            assert!(names.insert(*name), "duplicate default policy name: {name}");
        }
    }

    #[test]
    fn test_default_policies_validate() {
        for (name, policy) in default::DEFAULT_POLICIES.iter() {
            policy
                .validate()
                .unwrap_or_else(|err| panic!("default policy {name} should validate: {err}"));
        }
    }

    // ------------------------------------------------------------------------
    // Built-in KMS role templates
    // ------------------------------------------------------------------------

    fn default_policy(name: &str) -> &'static Policy {
        default::DEFAULT_POLICIES
            .iter()
            .find_map(|(candidate, policy)| (*candidate == name).then_some(policy))
            .unwrap_or_else(|| panic!("built-in policy {name} should exist"))
    }

    /// Evaluate `policy` for `account` against `action` on `key_id`.
    ///
    /// Mirrors the admin and SSE call sites: the requested key identifier travels in
    /// `object` with `bucket` left empty. An empty `key_id` is the unscoped call.
    async fn kms_allows(policy: &Policy, account: &str, action: KmsAction, key_id: &str) -> bool {
        let conditions = HashMap::new();
        let claims = HashMap::new();
        policy
            .is_allowed(&Args {
                account,
                groups: &None,
                action: Action::KmsAction(action),
                bucket: "",
                conditions: &conditions,
                is_owner: false,
                object: key_id,
                claims: &claims,
                deny_only: false,
            })
            .await
    }

    const KMS_LIFECYCLE_ACTIONS: [KmsAction; 4] = [
        KmsAction::EnableKeyAction,
        KmsAction::DisableKeyAction,
        KmsAction::RotateKeyAction,
        KmsAction::DeleteKeyAction,
    ];

    const KMS_CRYPTO_ACTIONS: [KmsAction; 2] = [KmsAction::GenerateDataKeyAction, KmsAction::DecryptAction];

    /// Actions that act on the service or on every key's material at once. No role
    /// template may confer them.
    const KMS_CLUSTER_ADMIN_ACTIONS: [KmsAction; 6] = [
        KmsAction::AllActions,
        KmsAction::ConfigureAction,
        KmsAction::ServiceControlAction,
        KmsAction::ClearCacheAction,
        KmsAction::BackupAction,
        KmsAction::RestoreAction,
    ];

    const KMS_ROLE_TEMPLATES: [&str; 3] = [default::KMS_KEY_ADMINISTRATOR, default::KMS_KEY_USER, default::KMS_AUDITOR];

    #[tokio::test]
    async fn kms_key_administrator_manages_keys_but_cannot_use_them() {
        let policy = default_policy(default::KMS_KEY_ADMINISTRATOR);

        for action in KMS_LIFECYCLE_ACTIONS {
            assert!(
                kms_allows(policy, "keyadmin", action, "app-key").await,
                "KMSKeyAdministrator should allow {action:?}"
            );
        }
        assert!(kms_allows(policy, "keyadmin", KmsAction::DescribeKeyAction, "app-key").await);
        assert!(kms_allows(policy, "keyadmin", KmsAction::ListKeysAction, "").await);

        for action in KMS_CRYPTO_ACTIONS {
            assert!(
                !kms_allows(policy, "keyadmin", action, "app-key").await,
                "KMSKeyAdministrator must not allow {action:?}"
            );
        }
    }

    #[tokio::test]
    async fn kms_key_user_uses_keys_but_cannot_manage_them() {
        let policy = default_policy(default::KMS_KEY_USER);

        for action in KMS_CRYPTO_ACTIONS {
            assert!(
                kms_allows(policy, "appuser", action, "app-key").await,
                "KMSKeyUser should allow {action:?}"
            );
        }
        assert!(kms_allows(policy, "appuser", KmsAction::DescribeKeyAction, "app-key").await);

        for action in KMS_LIFECYCLE_ACTIONS {
            assert!(
                !kms_allows(policy, "appuser", action, "app-key").await,
                "KMSKeyUser must not allow {action:?}"
            );
        }
        assert!(!kms_allows(policy, "appuser", KmsAction::ListKeysAction, "").await);
    }

    #[tokio::test]
    async fn kms_auditor_only_reads_key_metadata() {
        let policy = default_policy(default::KMS_AUDITOR);

        assert!(kms_allows(policy, "auditor", KmsAction::DescribeKeyAction, "app-key").await);
        assert!(kms_allows(policy, "auditor", KmsAction::ListKeysAction, "").await);

        for action in KMS_LIFECYCLE_ACTIONS.iter().chain(KMS_CRYPTO_ACTIONS.iter()) {
            assert!(
                !kms_allows(policy, "auditor", *action, "app-key").await,
                "KMSAuditor must not allow {action:?}"
            );
        }
    }

    #[tokio::test]
    async fn kms_role_templates_withhold_service_and_bundle_actions() {
        for name in KMS_ROLE_TEMPLATES {
            let policy = default_policy(name);
            for action in KMS_CLUSTER_ADMIN_ACTIONS {
                assert!(
                    !kms_allows(policy, "someone", action, "app-key").await,
                    "{name} must not allow {action:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn kms_role_templates_grant_nothing_outside_kms() {
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let foreign_actions = [
            Action::S3Action(S3Action::GetObjectAction),
            Action::S3Action(S3Action::PutObjectAction),
            Action::AdminAction(AdminAction::ServerInfoAdminAction),
        ];

        for name in KMS_ROLE_TEMPLATES {
            let policy = default_policy(name);
            for action in &foreign_actions {
                let allowed = policy
                    .is_allowed(&Args {
                        account: "someone",
                        groups: &None,
                        action: *action,
                        bucket: "any-bucket",
                        conditions: &conditions,
                        is_owner: false,
                        object: "any-object",
                        claims: &claims,
                        deny_only: false,
                    })
                    .await;
                assert!(!allowed, "{name} must not allow {action:?}");
            }
        }
    }

    /// The narrowing an operator is told to apply must actually deny the other keys.
    #[tokio::test]
    async fn narrowed_kms_role_template_denies_other_keys() -> Result<()> {
        let narrowed = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:GenerateDataKey", "kms:Decrypt", "kms:DescribeKey"],
      "Resource": ["arn:aws:kms:::key/reports-*"]
    }
  ]
}"#,
        )?;

        assert!(kms_allows(&narrowed, "appuser", KmsAction::GenerateDataKeyAction, "reports-2026").await);
        assert!(kms_allows(&narrowed, "appuser", KmsAction::DecryptAction, "reports-2026").await);
        assert!(!kms_allows(&narrowed, "appuser", KmsAction::DecryptAction, "payroll-2026").await);
        assert!(!kms_allows(&narrowed, "appuser", KmsAction::DisableKeyAction, "reports-2026").await);

        Ok(())
    }

    #[tokio::test]
    async fn test_deny_only_checks_only_deny_statements() -> Result<()> {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::bucket1/*"]
    }
  ]
}
"#;

        let policy = Policy::parse_config(data.as_bytes())?;
        let conditions = HashMap::new();
        let claims = HashMap::new();

        // deny_only=true should allow if no Deny statement matches,
        // even when no Allow statement matches.
        let args_deny_only = Args {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::PutObjectAction),
            bucket: "bucket2",
            conditions: &conditions,
            is_owner: false,
            object: "test.txt",
            claims: &claims,
            deny_only: true,
        };

        assert!(
            policy.is_allowed(&args_deny_only).await,
            "deny_only should allow when no Deny statement matches"
        );

        // deny_only=true should also allow when action matches Allow statement.
        let args_deny_only_allowed = Args {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::GetObjectAction),
            bucket: "bucket1",
            conditions: &conditions,
            is_owner: false,
            object: "test.txt",
            claims: &claims,
            deny_only: true,
        };

        assert!(
            policy.is_allowed(&args_deny_only_allowed).await,
            "deny_only should allow when no Deny statement matches, even if Allow exists"
        );

        // Normal policy evaluation remains unchanged when deny_only=false.
        let args_normal = Args {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::GetObjectAction),
            bucket: "bucket1",
            conditions: &conditions,
            is_owner: false,
            object: "test.txt",
            claims: &claims,
            deny_only: false,
        };

        // Should return true because there's an Allow statement
        assert!(
            policy.is_allowed(&args_normal).await,
            "normal policy evaluation should allow with matching Allow statement"
        );

        // Explicit Deny must still block request in deny_only mode.
        let deny_policy_data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Deny",
      "Action": ["s3:PutObject"],
      "Resource": ["arn:aws:s3:::bucket2/*"]
    }
  ]
}
"#;
        let deny_policy = Policy::parse_config(deny_policy_data.as_bytes())?;
        assert!(
            !deny_policy.is_allowed(&args_deny_only).await,
            "deny_only should reject request when an explicit Deny matches"
        );

        let args_owner_deny_only = Args {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::PutObjectAction),
            bucket: "bucket2",
            conditions: &conditions,
            is_owner: true,
            object: "test.txt",
            claims: &claims,
            deny_only: true,
        };

        assert!(
            policy.is_allowed(&args_owner_deny_only).await,
            "deny_only should allow when no Deny statement matches, including owner requests"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_list_bucket_prefix_condition_uses_bucket_resource() -> Result<()> {
        let policy = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::polaris-test-bucket"],
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "polaris_test/snowflake_catalog/db1/schema/iceberg_table/*"
          ]
        }
      }
    }
  ]
}"#,
        )?;

        let mut conditions = HashMap::new();
        conditions.insert(
            "prefix".to_string(),
            vec!["polaris_test/snowflake_catalog/db1/schema/iceberg_table/metadata/".to_string()],
        );
        let claims = HashMap::new();
        let args = Args {
            account: "polaris-session",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketAction),
            bucket: "polaris-test-bucket",
            conditions: &conditions,
            is_owner: false,
            object: "polaris_test/snowflake_catalog/db1/schema/iceberg_table/metadata/",
            claims: &claims,
            deny_only: false,
        };

        assert!(
            policy.is_allowed(&args).await,
            "ListBucket should match the bucket resource and apply the prefix through the condition, not by converting the prefix into an object resource"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_list_bucket_versions_prefix_condition_uses_bucket_resource() -> Result<()> {
        let policy = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucketVersions"],
      "Resource": ["arn:aws:s3:::polaris-test-bucket"],
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "polaris_test/snowflake_catalog/db1/schema/iceberg_table/*"
          ]
        }
      }
    }
  ]
}"#,
        )?;

        let mut conditions = HashMap::new();
        conditions.insert(
            "prefix".to_string(),
            vec!["polaris_test/snowflake_catalog/db1/schema/iceberg_table/metadata/".to_string()],
        );
        let claims = HashMap::new();
        let args = Args {
            account: "polaris-session",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketVersionsAction),
            bucket: "polaris-test-bucket",
            conditions: &conditions,
            is_owner: false,
            object: "polaris_test/snowflake_catalog/db1/schema/iceberg_table/metadata/",
            claims: &claims,
            deny_only: false,
        };

        assert!(
            policy.is_allowed(&args).await,
            "ListBucketVersions should match the bucket resource and apply the prefix through the condition"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_list_bucket_gateway_prefix_uses_object_resource_when_condition_missing() -> Result<()> {
        let policy = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::polaris-test-bucket/home/alice/*"]
    }
  ]
}"#,
        )?;

        let mut conditions = HashMap::new();
        conditions.insert("prefix".to_string(), vec!["home/alice/projects/".to_string()]);
        let claims = HashMap::new();
        let args = Args {
            account: "polaris-session",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketAction),
            bucket: "polaris-test-bucket",
            conditions: &conditions,
            is_owner: false,
            object: "home/alice/projects/",
            claims: &claims,
            deny_only: false,
        };

        assert!(
            policy.is_allowed(&args).await,
            "Gateway ListBucket auth without an s3:prefix condition should continue matching prefix-scoped resources via args.object"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_bucket_policy_gateway_prefix_uses_object_resource_when_condition_missing() -> Result<()> {
        let bucket_policy: BucketPolicy = serde_json::from_str(
            r#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": "*"},
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::polaris-test-bucket/home/alice/*"]
    }
  ]
}"#,
        )?;

        let mut conditions = HashMap::new();
        conditions.insert("prefix".to_string(), vec!["home/alice/projects/".to_string()]);
        let args = BucketPolicyArgs {
            account: "polaris-session",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketAction),
            bucket: "polaris-test-bucket",
            conditions: &conditions,
            is_owner: false,
            object: "home/alice/projects/",
        };

        assert!(
            bucket_policy.is_allowed(&args).await,
            "Bucket policy ListBucket without an s3:prefix condition should continue matching prefix-scoped resources via args.object"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_aws_username_policy_variable() -> Result<()> {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::${aws:username}-*"]
    }
  ]
}
"#;

        let policy = Policy::parse_config(data.as_bytes())?;

        let conditions = HashMap::new();

        // Test allowed case - user testuser accessing testuser-bucket
        let mut claims1 = HashMap::new();
        claims1.insert("username".to_string(), Value::String("testuser".to_string()));

        let args1 = Args {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketAction),
            bucket: "testuser-bucket",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims1,
            deny_only: false,
        };

        // Test denied case - user otheruser accessing testuser-bucket
        let mut claims2 = HashMap::new();
        claims2.insert("username".to_string(), Value::String("otheruser".to_string()));

        let args2 = Args {
            account: "otheruser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketAction),
            bucket: "testuser-bucket",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims2,
            deny_only: false,
        };

        assert!(pollster::block_on(policy.is_allowed(&args1)));
        assert!(!pollster::block_on(policy.is_allowed(&args2)));

        Ok(())
    }

    #[tokio::test]
    async fn test_aws_userid_policy_variable() -> Result<()> {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::${aws:userid}-bucket"]
    }
  ]
}
"#;

        let policy = Policy::parse_config(data.as_bytes())?;

        let mut claims = HashMap::new();
        claims.insert("sub".to_string(), Value::String("AIDACKCEVSQ6C2EXAMPLE".to_string()));

        let conditions = HashMap::new();

        // Test allowed case
        let args1 = Args {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketAction),
            bucket: "AIDACKCEVSQ6C2EXAMPLE-bucket",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };

        // Test denied case
        let args2 = Args {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketAction),
            bucket: "OTHERUSER-bucket",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };

        assert!(pollster::block_on(policy.is_allowed(&args1)));
        assert!(!pollster::block_on(policy.is_allowed(&args2)));

        Ok(())
    }

    #[tokio::test]
    async fn test_aws_policy_variables_concatenation() -> Result<()> {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::${aws:username}-${aws:userid}-bucket"]
    }
  ]
}
"#;

        let policy = Policy::parse_config(data.as_bytes())?;

        let mut claims = HashMap::new();
        claims.insert("username".to_string(), Value::String("testuser".to_string()));
        claims.insert("sub".to_string(), Value::String("AIDACKCEVSQ6C2EXAMPLE".to_string()));

        let conditions = HashMap::new();

        // Test allowed case
        let args1 = Args {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketAction),
            bucket: "testuser-AIDACKCEVSQ6C2EXAMPLE-bucket",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };

        // Test denied case
        let args2 = Args {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketAction),
            bucket: "otheruser-AIDACKCEVSQ6C2EXAMPLE-bucket",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };

        assert!(pollster::block_on(policy.is_allowed(&args1)));
        assert!(!pollster::block_on(policy.is_allowed(&args2)));

        Ok(())
    }

    #[tokio::test]
    async fn test_aws_policy_variables_nested() -> Result<()> {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::${${aws:PrincipalType}-${aws:userid}}"]
    }
  ]
}
"#;

        let policy = Policy::parse_config(data.as_bytes())?;

        let mut claims = HashMap::new();
        claims.insert("sub".to_string(), Value::String("AIDACKCEVSQ6C2EXAMPLE".to_string()));
        // For PrincipalType, it will default to "User" when not explicitly set

        let conditions = HashMap::new();

        // Test allowed case
        let args1 = Args {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketAction),
            bucket: "User-AIDACKCEVSQ6C2EXAMPLE",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };

        // Test denied case
        let args2 = Args {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketAction),
            bucket: "User-OTHERUSER",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };

        assert!(pollster::block_on(policy.is_allowed(&args1)));
        assert!(!pollster::block_on(policy.is_allowed(&args2)));

        Ok(())
    }

    #[tokio::test]
    async fn test_aws_policy_variables_multi_value() -> Result<()> {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::${aws:username}-bucket"]
    }
  ]
}
"#;

        let policy = Policy::parse_config(data.as_bytes())?;

        let mut claims = HashMap::new();
        // Test with array value for username
        claims.insert(
            "username".to_string(),
            Value::Array(vec![Value::String("user1".to_string()), Value::String("user2".to_string())]),
        );

        let conditions = HashMap::new();

        let args1 = Args {
            account: "user1",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketAction),
            bucket: "user1-bucket",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };

        let args2 = Args {
            account: "user2",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketAction),
            bucket: "user2-bucket",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };

        // Either user1 or user2 should be allowed
        assert!(pollster::block_on(policy.is_allowed(&args1)) || pollster::block_on(policy.is_allowed(&args2)));

        Ok(())
    }

    #[test]
    fn test_statement_with_only_notresource_is_valid() {
        // Test: A statement with only NotResource (and no Resource) is valid
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "NotResource": ["arn:aws:s3:::mybucket/private/*"]
    }
  ]
}
"#;

        let result = Policy::parse_config(data.as_bytes());
        assert!(result.is_ok(), "Statement with only NotResource should be valid");
    }

    #[test]
    fn test_statement_with_both_resource_and_notresource_is_invalid() {
        // Test: A statement with both Resource and NotResource returns BothResourceAndNotResource error
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::mybucket/public/*"],
      "NotResource": ["arn:aws:s3:::mybucket/private/*"]
    }
  ]
}
"#;

        let result = Policy::parse_config(data.as_bytes());
        assert!(result.is_err(), "Statement with both Resource and NotResource should be invalid");

        assert!(
            matches!(result.as_ref().unwrap_err(), Error::PolicyError(IamError::BothResourceAndNotResource)),
            "Error should be BothResourceAndNotResource, got: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_statement_with_only_notaction_is_valid() {
        // IAM allows a statement with only NotAction (no Action). Deserialization should accept
        // missing "Action" (default to empty) and validate when exactly one of Action/NotAction is set.
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "NotAction": ["s3:DeleteBucket", "s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::mybucket/*"]
    }
  ]
}
"#;

        let result = Policy::parse_config(data.as_bytes());
        assert!(result.is_ok(), "Statement with only NotAction should be valid, got: {:?}", result.err());
        let policy = result.unwrap();
        assert_eq!(policy.statements.len(), 1);
        assert!(policy.statements[0].actions.is_empty());
        assert!(!policy.statements[0].not_actions.is_empty());

        // Round-trip: serialization must omit empty Action so re-parse does not violate both-Action-and-NotAction
        let json = serde_json::to_string(&policy).expect("Should serialize");
        let round_tripped = Policy::parse_config(json.as_bytes());
        assert!(
            round_tripped.is_ok(),
            "NotAction-only statement must round-trip without gaining Action: {}",
            round_tripped.unwrap_err()
        );
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("JSON valid");
        let stmt = &parsed["Statement"][0];
        assert!(
            !stmt
                .get("Action")
                .is_some_and(|v| v.as_array().map(|a| a.is_empty()).unwrap_or(false)),
            "Serialized JSON must not contain empty Action for NotAction-only statement"
        );
    }

    #[test]
    fn test_parse_empty_policy_object_as_implied_policy() {
        let policy = Policy::parse_config(b"{}").expect("empty JSON object should parse");

        assert!(policy.version.is_empty());
        assert!(policy.statements.is_empty());
    }

    #[test]
    fn test_statement_with_both_action_and_notaction_is_invalid() {
        // Test: A statement with both Action and NotAction returns BothActionAndNotAction error
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "NotAction": ["s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::mybucket/*"]
    }
  ]
}
"#;

        let result = Policy::parse_config(data.as_bytes());
        assert!(result.is_err(), "Statement with both Action and NotAction should be invalid");

        assert!(
            matches!(result.as_ref().unwrap_err(), Error::PolicyError(IamError::BothActionAndNotAction)),
            "Error should be BothActionAndNotAction, got: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_statement_with_neither_action_nor_notaction_is_invalid() {
        // Statement with both Action and NotAction omitted (both default to empty) fails validation.
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Resource": ["arn:aws:s3:::mybucket/*"]
    }
  ]
}
"#;

        let result = Policy::parse_config(data.as_bytes());
        assert!(result.is_err(), "Statement with neither Action nor NotAction should be invalid");

        assert!(
            matches!(result.as_ref().unwrap_err(), Error::PolicyError(IamError::NonAction)),
            "Error should be NonAction, got: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_statement_with_neither_resource_nor_notresource_is_invalid() {
        // Test: A statement with neither Resource nor NotResource returns NonResource error
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"]
    }
  ]
}
"#;

        let result = Policy::parse_config(data.as_bytes());
        assert!(result.is_err(), "Statement with neither Resource nor NotResource should be invalid");

        assert!(
            matches!(result.as_ref().unwrap_err(), Error::PolicyError(IamError::NonResource)),
            "Error should be NonResource, got: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_admin_statement_without_resource_is_valid() {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["admin:ServerInfo"]
    }
  ]
}
"#;

        let result = Policy::parse_config(data.as_bytes());
        assert!(
            result.is_ok(),
            "Admin-only Action statement without Resource should be valid, got: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_table_admin_action_with_resource_is_limited_to_bucket() -> Result<()> {
        use crate::policy::action::{Action, AdminAction};

        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["admin:GetTableMetadata"],
      "Resource": ["arn:aws:s3:::warehouse-a"]
    }
  ]
}
"#;

        let policy = Policy::parse_config(data.as_bytes())?;
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let groups = None;

        let matching_args = Args {
            account: "testuser",
            groups: &groups,
            action: Action::AdminAction(AdminAction::GetTableMetadataAction),
            bucket: "warehouse-a",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };
        assert!(
            policy.is_allowed(&matching_args).await,
            "table admin action should allow the explicitly granted warehouse bucket"
        );

        let mismatched_args = Args {
            account: "testuser",
            groups: &groups,
            action: Action::AdminAction(AdminAction::GetTableMetadataAction),
            bucket: "warehouse-b",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };
        assert!(
            !policy.is_allowed(&mismatched_args).await,
            "table admin action must not ignore Resource when the request targets a different warehouse bucket"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_table_admin_action_with_resource_is_limited_to_table_object_scope() -> Result<()> {
        use crate::policy::action::{Action, AdminAction};

        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["admin:GetTableMetadata"],
      "Resource": ["arn:aws:s3:::warehouse-a/namespaces/analytics/tables/events"]
    }
  ]
}
"#;

        let policy = Policy::parse_config(data.as_bytes())?;
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let groups = None;

        let matching_args = Args {
            account: "testuser",
            groups: &groups,
            action: Action::AdminAction(AdminAction::GetTableMetadataAction),
            bucket: "warehouse-a",
            conditions: &conditions,
            is_owner: false,
            object: "namespaces/analytics/tables/events",
            claims: &claims,
            deny_only: false,
        };
        assert!(
            policy.is_allowed(&matching_args).await,
            "table admin action should allow the explicitly granted table resource"
        );

        let namespace_only_args = Args {
            account: "testuser",
            groups: &groups,
            action: Action::AdminAction(AdminAction::GetTableMetadataAction),
            bucket: "warehouse-a",
            conditions: &conditions,
            is_owner: false,
            object: "namespaces/analytics",
            claims: &claims,
            deny_only: false,
        };
        assert!(
            !policy.is_allowed(&namespace_only_args).await,
            "table admin action must not match a namespace-only resource when a table resource is required"
        );

        let other_table_args = Args {
            account: "testuser",
            groups: &groups,
            action: Action::AdminAction(AdminAction::GetTableMetadataAction),
            bucket: "warehouse-a",
            conditions: &conditions,
            is_owner: false,
            object: "namespaces/analytics/tables/orders",
            claims: &claims,
            deny_only: false,
        };
        assert!(
            !policy.is_allowed(&other_table_args).await,
            "table admin action must not match a different table resource"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_table_admin_action_with_not_resource_excludes_bucket() -> Result<()> {
        use crate::policy::action::{Action, AdminAction};

        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["admin:GetTableMetadata"],
      "NotResource": ["arn:aws:s3:::warehouse-b"]
    }
  ]
}
"#;

        let policy = Policy::parse_config(data.as_bytes())?;
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let groups = None;

        let allowed_args = Args {
            account: "testuser",
            groups: &groups,
            action: Action::AdminAction(AdminAction::GetTableMetadataAction),
            bucket: "warehouse-a",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };
        assert!(
            policy.is_allowed(&allowed_args).await,
            "table admin NotResource should allow a warehouse outside the excluded bucket"
        );

        let excluded_args = Args {
            account: "testuser",
            groups: &groups,
            action: Action::AdminAction(AdminAction::GetTableMetadataAction),
            bucket: "warehouse-b",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };
        assert!(
            !policy.is_allowed(&excluded_args).await,
            "table admin NotResource should deny the excluded warehouse bucket"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_non_table_admin_action_keeps_unscoped_resource_behavior() -> Result<()> {
        use crate::policy::action::{Action, AdminAction};

        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["admin:ServerInfo"],
      "Resource": ["arn:aws:s3:::warehouse-a"]
    }
  ]
}
"#;

        let policy = Policy::parse_config(data.as_bytes())?;
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let groups = None;

        let args = Args {
            account: "testuser",
            groups: &groups,
            action: Action::AdminAction(AdminAction::ServerInfoAdminAction),
            bucket: "warehouse-b",
            conditions: &conditions,
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };
        assert!(
            policy.is_allowed(&args).await,
            "existing non-table admin actions should preserve resource-independent evaluation"
        );

        Ok(())
    }

    #[test]
    fn test_sts_statement_without_resource_is_valid() {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["sts:AssumeRole"]
    }
  ]
}
"#;

        let result = Policy::parse_config(data.as_bytes());
        assert!(
            result.is_ok(),
            "STS-only Action statement without Resource should be valid, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_kms_statement_without_resource_is_valid() {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:*"]
    }
  ]
}
"#;

        let result = Policy::parse_config(data.as_bytes());
        assert!(
            result.is_ok(),
            "KMS-only Action statement without Resource should be valid, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_dedicated_kms_statement_without_resource_is_valid() {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:GenerateDataKey"]
    }
  ]
}
"#;

        let result = Policy::parse_config(data.as_bytes());
        assert!(
            result.is_ok(),
            "KMS-only dedicated Action statement without Resource should be valid, got: {:?}",
            result.err()
        );
    }

    fn kms_args<'a>(
        action: Action,
        key_id: &'a str,
        conditions: &'a HashMap<String, Vec<String>>,
        claims: &'a HashMap<String, Value>,
    ) -> Args<'a> {
        Args {
            account: "testuser",
            groups: &None,
            action,
            bucket: "",
            conditions,
            is_owner: false,
            object: key_id,
            claims,
            deny_only: false,
        }
    }

    #[tokio::test]
    async fn test_kms_statement_with_key_resource_scopes_by_key() -> Result<()> {
        use crate::policy::action::{Action, KmsAction};

        let policy = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:DisableKey"],
      "Resource": ["arn:aws:kms:::key/key-a"]
    }
  ]
}"#,
        )?;
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let action = Action::KmsAction(KmsAction::DisableKeyAction);

        assert!(
            policy.is_allowed(&kms_args(action, "key-a", &conditions, &claims)).await,
            "the granted key must be allowed"
        );
        assert!(
            !policy.is_allowed(&kms_args(action, "key-b", &conditions, &claims)).await,
            "a key outside the granted resource must be denied"
        );
        assert!(
            !policy
                .is_allowed(&kms_args(Action::KmsAction(KmsAction::EnableKeyAction), "key-a", &conditions, &claims))
                .await,
            "an action outside the grant must stay denied even for the granted key"
        );
        assert!(
            policy.is_allowed(&kms_args(action, "", &conditions, &claims)).await,
            "call sites that do not pass a key resource keep the legacy match-every-key behaviour"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_kms_statement_with_wildcard_key_resource() -> Result<()> {
        use crate::policy::action::{Action, KmsAction};

        let policy = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:GenerateDataKey"],
      "Resource": ["arn:aws:kms:::key/app-*"]
    }
  ]
}"#,
        )?;
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let action = Action::KmsAction(KmsAction::GenerateDataKeyAction);

        assert!(
            policy
                .is_allowed(&kms_args(action, "app-primary", &conditions, &claims))
                .await
        );
        assert!(
            !policy
                .is_allowed(&kms_args(action, "backup-primary", &conditions, &claims))
                .await
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_kms_bundle_actions_require_an_unscoped_statement() -> Result<()> {
        use crate::policy::action::{Action, KmsAction};

        let scoped = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:*"],
      "Resource": ["arn:aws:kms:::key/key-a"]
    }
  ]
}"#,
        )?;
        let unscoped = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:Backup", "kms:Restore"]
    }
  ]
}"#,
        )?;
        let partially_scoped = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:Backup", "kms:Restore"],
      "NotResource": ["arn:aws:kms:::key/protected"]
    }
  ]
}"#,
        )?;
        let scoped_deny = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:Backup", "kms:Restore"]
    },
    {
      "Effect": "Deny",
      "Action": ["kms:Backup", "kms:Restore"],
      "Resource": ["arn:aws:kms:::key/protected"]
    }
  ]
}"#,
        )?;
        let conditions = HashMap::new();
        let claims = HashMap::new();

        for action in [KmsAction::BackupAction, KmsAction::RestoreAction] {
            let args = kms_args(Action::KmsAction(action), "", &conditions, &claims);
            assert!(
                !scoped.is_allowed(&args).await,
                "a single-key grant must not authorize the all-key {action:?} operation"
            );
            assert!(
                !partially_scoped.is_allowed(&args).await,
                "a grant excluding one key must not authorize the all-key {action:?} operation"
            );
            assert!(
                unscoped.is_allowed(&args).await,
                "an action-only grant must continue authorizing {action:?}"
            );
            assert!(
                !scoped_deny.is_allowed(&args).await,
                "a deny for included key material must block the all-key {action:?} operation"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_kms_statement_without_resource_matches_every_key() -> Result<()> {
        use crate::policy::action::{Action, KmsAction};

        let policy = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:*"]
    }
  ]
}"#,
        )?;
        let conditions = HashMap::new();
        let claims = HashMap::new();

        for key_id in ["", "key-a", "any-other-key"] {
            assert!(
                policy
                    .is_allowed(&kms_args(Action::KmsAction(KmsAction::DisableKeyAction), key_id, &conditions, &claims))
                    .await,
                "resource-less KMS statement must keep matching every key (key_id: {key_id:?})"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_kms_deny_with_wildcard_resource_overrides_allow() -> Result<()> {
        use crate::policy::action::{Action, KmsAction};

        let policy = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:*"],
      "Resource": ["arn:aws:kms:::key/key-a"]
    },
    {
      "Effect": "Deny",
      "Action": ["kms:DisableKey"],
      "Resource": ["arn:aws:kms:::key/*"]
    }
  ]
}"#,
        )?;
        let conditions = HashMap::new();
        let claims = HashMap::new();

        assert!(
            !policy
                .is_allowed(&kms_args(Action::KmsAction(KmsAction::DisableKeyAction), "key-a", &conditions, &claims))
                .await,
            "a wildcard Deny must override the narrower Allow"
        );
        assert!(
            policy
                .is_allowed(&kms_args(Action::KmsAction(KmsAction::RotateKeyAction), "key-a", &conditions, &claims))
                .await,
            "actions outside the Deny keep the Allow"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_kms_statement_with_not_resource_excludes_keys() -> Result<()> {
        use crate::policy::action::{Action, KmsAction};

        let policy = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:DescribeKey"],
      "NotResource": ["arn:aws:kms:::key/prod-*"]
    }
  ]
}"#,
        )?;
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let action = Action::KmsAction(KmsAction::DescribeKeyAction);

        assert!(policy.is_allowed(&kms_args(action, "dev-key", &conditions, &claims)).await);
        assert!(!policy.is_allowed(&kms_args(action, "prod-key", &conditions, &claims)).await);

        Ok(())
    }

    #[tokio::test]
    async fn test_kms_statement_with_s3_resource_is_treated_as_unscoped() -> Result<()> {
        use crate::policy::action::{Action, KmsAction};

        // Statements combining KMS actions with S3 resources predate KMS resource
        // support and were always evaluated as if unscoped. Pin that they still
        // match every key (a warning is logged during evaluation).
        let policy = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:DisableKey"],
      "Resource": ["arn:aws:s3:::somebucket/*"]
    }
  ]
}"#,
        )?;
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let action = Action::KmsAction(KmsAction::DisableKeyAction);

        for key_id in ["", "key-a"] {
            assert!(
                policy.is_allowed(&kms_args(action, key_id, &conditions, &claims)).await,
                "malformed KMS statement with S3 resources must keep matching every key (key_id: {key_id:?})"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_kms_alias_resource_parses_but_matches_no_key() -> Result<()> {
        use crate::policy::action::{Action, KmsAction};

        let policy = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:DescribeKey"],
      "Resource": ["arn:aws:kms:::alias/app-alias"]
    }
  ]
}"#,
        )?;
        let conditions = HashMap::new();
        let claims = HashMap::new();
        let action = Action::KmsAction(KmsAction::DescribeKeyAction);

        assert!(
            !policy.is_allowed(&kms_args(action, "app-alias", &conditions, &claims)).await,
            "alias patterns are parse-only until alias resolution lands and must not match key requests"
        );
        assert!(
            policy.is_allowed(&kms_args(action, "", &conditions, &claims)).await,
            "call sites without a key resource keep the legacy match-every-key behaviour"
        );

        Ok(())
    }

    #[test]
    fn test_kms_resource_policy_round_trips() {
        let policy = Policy::parse_config(
            br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kms:GenerateDataKey", "kms:Decrypt"],
      "Resource": ["arn:aws:kms:::key/app-*", "arn:aws:kms:::alias/app-alias"]
    }
  ]
}"#,
        )
        .expect("KMS resource policy should parse");

        let json = serde_json::to_string(&policy).expect("policy should serialize");
        let round_trip = Policy::parse_config(json.as_bytes()).expect("serialized KMS resource policy should re-parse");
        assert_eq!(round_trip.statements[0].resources, policy.statements[0].resources);
        assert_eq!(round_trip.statements[0].actions, policy.statements[0].actions);

        let value: serde_json::Value = serde_json::from_str(&json).expect("JSON valid");
        let resources: Vec<_> = value["Statement"][0]["Resource"]
            .as_array()
            .expect("Resource should serialize as an array")
            .iter()
            .map(|resource| resource.as_str().expect("Resource entries should be strings"))
            .collect();
        assert_eq!(resources, vec!["arn:aws:kms:::key/app-*", "arn:aws:kms:::alias/app-alias"]);
    }

    #[test]
    fn test_kms_resource_with_non_kms_action_is_invalid() {
        for action in ["s3:GetObject", "admin:ServerInfo", "sts:AssumeRole"] {
            let data = format!(
                r#"{{
  "Version": "2012-10-17",
  "Statement": [
    {{
      "Effect": "Allow",
      "Action": ["{action}"],
      "Resource": ["arn:aws:kms:::key/key-a"]
    }}
  ]
}}"#
            );

            let result = Policy::parse_config(data.as_bytes());
            assert!(
                matches!(result.as_ref().unwrap_err(), Error::PolicyError(IamError::KmsResourceWithNonKmsAction)),
                "{action} with a KMS resource should fail with KmsResourceWithNonKmsAction, got: {result:?}"
            );
        }
    }

    #[test]
    fn test_bucket_policy_with_kms_statement_is_invalid() {
        for statement in [
            r#"{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["kms:GenerateDataKey"],"Resource":["arn:aws:s3:::bucket/*"]}"#,
            r#"{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:GetObject"],"Resource":["arn:aws:kms:::key/key-a"]}"#,
            r#"{"Effect":"Allow","Principal":{"AWS":"*"},"NotAction":["kms:*"],"Resource":["arn:aws:s3:::bucket/*"]}"#,
        ] {
            let data = format!(r#"{{"Version":"2012-10-17","Statement":[{statement}]}}"#);
            let policy: BucketPolicy =
                serde_json::from_str(&data).expect("bucket policy with KMS content should still deserialize");
            let result = policy.is_valid();
            assert!(
                matches!(result.as_ref().unwrap_err(), Error::PolicyError(IamError::KmsUnsupportedInBucketPolicy)),
                "bucket policy statement {statement} should fail with KmsUnsupportedInBucketPolicy, got: {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_stored_bucket_policy_with_kms_statement_loads_and_is_ignored() -> Result<()> {
        // Policies stored before KMS statements were rejected at validation must keep
        // deserializing, and their KMS statements must not affect bucket traffic.
        let bucket_policy: BucketPolicy = serde_json::from_str(
            r#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": "*"},
      "Action": ["kms:GenerateDataKey"],
      "Resource": ["arn:aws:s3:::bucket/*"]
    },
    {
      "Effect": "Allow",
      "Principal": {"AWS": "*"},
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::bucket/*"]
    }
  ]
}"#,
        )?;

        let conditions = HashMap::new();
        let args = BucketPolicyArgs {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::GetObjectAction),
            bucket: "bucket",
            conditions: &conditions,
            is_owner: false,
            object: "a.txt",
        };
        assert!(
            bucket_policy.is_allowed(&args).await,
            "the S3 statement must keep working alongside an ignored KMS statement"
        );

        let put_args = BucketPolicyArgs {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::PutObjectAction),
            bucket: "bucket",
            conditions: &conditions,
            is_owner: false,
            object: "a.txt",
        };
        assert!(
            !bucket_policy.is_allowed(&put_args).await,
            "the ignored KMS statement must not grant anything"
        );

        Ok(())
    }

    #[test]
    fn test_mixed_action_families_are_invalid_even_with_resource() {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["admin:*", "sts:AssumeRole"],
      "Resource": ["arn:aws:s3:::*"]
    }
  ]
}
"#;

        let result = Policy::parse_config(data.as_bytes());
        assert!(result.is_err(), "Mixed action families should be rejected");
        assert!(
            matches!(result.as_ref().unwrap_err(), Error::PolicyError(IamError::MixedActionFamilies)),
            "Error should be MixedActionFamilies, got: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_mixed_action_families_are_invalid_even_without_resource() {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["admin:*", "s3:GetObject"]
    }
  ]
}
"#;

        let result = Policy::parse_config(data.as_bytes());
        assert!(result.is_err(), "Mixed action families should be rejected even when Resource is missing");
        assert!(
            matches!(result.as_ref().unwrap_err(), Error::PolicyError(IamError::MixedActionFamilies)),
            "Error should be MixedActionFamilies, got: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_mixed_action_families_with_wildcard_variants_are_invalid() {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:*", "admin:*", "sts:AssumeRole"],
      "Resource": ["arn:aws:s3:::*"]
    }
  ]
}
"#;

        let result = Policy::parse_config(data.as_bytes());
        assert!(result.is_err(), "Mixed action families with wildcard variants should be rejected");
        assert!(
            matches!(result.as_ref().unwrap_err(), Error::PolicyError(IamError::MixedActionFamilies)),
            "Error should be MixedActionFamilies, got: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_notaction_without_resource_remains_invalid() {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "NotAction": ["s3:DeleteObject"]
    }
  ]
}
"#;

        let result = Policy::parse_config(data.as_bytes());
        assert!(result.is_err(), "NotAction statement without Resource should remain invalid");
        assert!(
            matches!(result.as_ref().unwrap_err(), Error::PolicyError(IamError::NonResource)),
            "Error should be NonResource, got: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn test_iam_policy_serialize_omits_empty_fields() {
        let policy = Policy::parse_config(
            br#"{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Action":["s3:GetObject","s3:PutObject"],
    "Resource":["arn:aws:s3:::example-bucket/*"]
  }]
}"#,
        )
        .expect("policy without optional fields should parse");

        let value = serde_json::to_value(&policy).expect("policy should serialize");
        let policy_object = value.as_object().expect("policy should serialize as an object");
        assert!(!policy_object.contains_key("ID"), "empty ID should be omitted");

        let statements = value["Statement"].as_array().expect("Statement should serialize as an array");
        let statement = statements
            .first()
            .expect("serialized policy should include the statement")
            .as_object()
            .expect("statement should serialize as an object");
        assert!(!statement.contains_key("Sid"), "empty Sid should be omitted");
        assert!(!statement.contains_key("Condition"), "empty Condition should be omitted");
    }

    #[test]
    fn test_policy_action_and_resource_serialization_preserves_input_order() {
        let policy = Policy::parse_config(
            br#"{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Action":["s3:PutObject","s3:DeleteObject","s3:ListBucket","s3:GetObject"],
    "Resource":["arn:aws:s3:::example-bucket/*","arn:aws:s3:::example-bucket"]
  }]
}"#,
        )
        .expect("policy with multiple actions and resources should parse");

        let first = serde_json::to_value(&policy).expect("policy should serialize");
        let second = serde_json::to_value(&policy).expect("policy should serialize consistently");
        assert_eq!(first, second, "policy serialization should be deterministic");

        let statement = first["Statement"][0]
            .as_object()
            .expect("statement should serialize as an object");
        let actions: Vec<_> = statement["Action"]
            .as_array()
            .expect("Action should serialize as an array")
            .iter()
            .map(|action| action.as_str().expect("Action entries should be strings"))
            .collect();
        assert_eq!(
            actions,
            vec!["s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetObject"],
            "Action serialization should preserve input order"
        );

        let resources: Vec<_> = statement["Resource"]
            .as_array()
            .expect("Resource should serialize as an array")
            .iter()
            .map(|resource| resource.as_str().expect("Resource entries should be strings"))
            .collect();
        assert_eq!(
            resources,
            vec!["arn:aws:s3:::example-bucket/*", "arn:aws:s3:::example-bucket"],
            "Resource serialization should preserve input order"
        );
    }

    #[test]
    fn test_iam_policy_serialize_preserves_non_empty_optional_fields() {
        let policy = Policy::parse_config(
            br#"{
  "Version":"2012-10-17",
  "Statement":[{
    "Sid":"keep-me",
    "Effect":"Allow",
    "NotAction":["s3:DeleteObject","s3:PutObject"],
    "NotResource":["arn:aws:s3:::example-bucket/private/*","arn:aws:s3:::example-bucket/tmp/*"],
    "Condition":{"StringEquals":{"s3:prefix":"home/"}}
  }]
}"#,
        )
        .expect("policy with non-empty optional fields should parse");

        let value = serde_json::to_value(&policy).expect("policy should serialize");
        let statement = value["Statement"][0]
            .as_object()
            .expect("statement should serialize as an object");
        assert_eq!(statement.get("Sid").and_then(|sid| sid.as_str()), Some("keep-me"));
        assert!(statement.contains_key("Condition"), "non-empty Condition should be preserved");
        assert!(!statement.contains_key("Action"), "empty Action should be omitted for NotAction policy");
        assert!(
            !statement.contains_key("Resource"),
            "empty Resource should be omitted for NotResource policy"
        );

        let not_actions: Vec<_> = statement["NotAction"]
            .as_array()
            .expect("NotAction should serialize as an array")
            .iter()
            .map(|action| action.as_str().expect("NotAction entries should be strings"))
            .collect();
        assert_eq!(not_actions, vec!["s3:DeleteObject", "s3:PutObject"]);

        let not_resources: Vec<_> = statement["NotResource"]
            .as_array()
            .expect("NotResource should serialize as an array")
            .iter()
            .map(|resource| resource.as_str().expect("NotResource entries should be strings"))
            .collect();
        assert_eq!(
            not_resources,
            vec!["arn:aws:s3:::example-bucket/private/*", "arn:aws:s3:::example-bucket/tmp/*"]
        );
    }

    #[test]
    fn test_iam_policy_serialize_keeps_empty_resource_omitted_for_action_families() {
        let policy = Policy::parse_config(
            br#"{
  "Version":"2012-10-17",
  "Statement":[
    {"Effect":"Allow","Action":["sts:AssumeRole"]},
    {"Effect":"Allow","Action":["admin:*"]},
    {"Effect":"Allow","Action":["kms:*"]}
  ]
}"#,
        )
        .expect("STS/Admin/KMS statements without resources should parse");

        let value = serde_json::to_value(&policy).expect("policy should serialize");
        let statements = value["Statement"].as_array().expect("Statement should serialize as an array");
        assert_eq!(statements.len(), 3);

        for statement in statements {
            let statement = statement.as_object().expect("statement should serialize as an object");
            assert!(!statement.contains_key("Resource"), "empty Resource should be omitted");
            assert!(!statement.contains_key("NotResource"), "empty NotResource should be omitted");
        }
    }

    #[test]
    fn test_bucket_policy_serialize_omits_empty_fields() {
        use crate::policy::action::{Action, ActionSet, S3Action};
        use crate::policy::resource::{Resource, ResourceSet};
        use crate::policy::{Effect, Functions, Principal};

        // Create a BucketPolicy with empty optional fields
        // Use JSON deserialization to create Principal (since aws field is private)
        let principal: Principal = serde_json::from_str(r#"{"AWS": "*"}"#).expect("Should parse principal");

        let mut policy = BucketPolicy {
            id: ID::default(), // Empty ID
            version: "2012-10-17".to_string(),
            statements: vec![BPStatement {
                sid: ID::default(), // Empty Sid
                effect: Effect::Allow,
                principal,
                actions: ActionSet::default(),
                not_actions: ActionSet::default(), // Empty NotAction
                resources: ResourceSet::default(),
                not_resources: ResourceSet::default(), // Empty NotResource
                conditions: Functions::default(),      // Empty Condition
            }],
        };

        // Set actions and resources (required fields)
        policy.statements[0]
            .actions
            .0
            .push(Action::S3Action(S3Action::ListBucketAction));
        policy.statements[0]
            .resources
            .0
            .push(Resource::try_from("arn:aws:s3:::test/*").expect("test resource should parse"));

        let json = serde_json::to_string(&policy).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");

        // Verify empty fields are omitted
        assert!(parsed.get("Id").is_none(), "Empty ID should be omitted");

        let statement = &parsed["Statement"][0];
        assert!(!statement.as_object().unwrap().contains_key("Sid"), "Empty Sid should be omitted");
        assert!(
            !statement.as_object().unwrap().contains_key("NotAction"),
            "Empty NotAction should be omitted"
        );
        assert!(
            !statement.as_object().unwrap().contains_key("NotResource"),
            "Empty NotResource should be omitted"
        );
        assert!(
            !statement.as_object().unwrap().contains_key("Condition"),
            "Empty Condition should be omitted"
        );

        // Verify required fields are present
        assert_eq!(parsed["Version"], "2012-10-17");
        assert_eq!(statement["Effect"], "Allow");
        assert_eq!(statement["Principal"]["AWS"], "*");
    }

    #[test]
    fn test_bucket_policy_deserializes_legacy_id() {
        let legacy_policy = br#"{"ID":"","Version":"2012-10-17","Statement":[{"Sid":"","Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"NotAction":[],"Resource":["arn:aws:s3:::bucket/*"],"NotResource":[],"Condition":{}}]}"#;

        let policy: BucketPolicy =
            serde_json::from_slice(legacy_policy).expect("bucket policy with legacy ID should deserialize");
        assert!(policy.id.is_empty());
        policy.is_valid().expect("legacy bucket policy should remain valid");

        let policy: BucketPolicy = serde_json::from_str(r#"{"ID":"legacy-policy","Version":"2012-10-17","Statement":[]}"#)
            .expect("non-empty legacy ID should deserialize");
        assert_eq!(policy.id.0, "legacy-policy");

        let serialized = serde_json::to_value(&policy).expect("bucket policy should serialize");
        assert_eq!(serialized["Id"], "legacy-policy");
        assert!(serialized.get("ID").is_none(), "legacy ID spelling should not be serialized");
    }

    #[test]
    fn test_bucket_policy_legacy_id_alias_remains_strict() {
        let unknown_field = r#"{"Version":"2012-10-17","Statement":[],"Unexpected":true}"#;
        let error =
            serde_json::from_str::<BucketPolicy>(unknown_field).expect_err("unrelated unknown fields should remain rejected");
        assert!(
            error.to_string().contains("unknown field `Unexpected`"),
            "unexpected deserialization error: {error}"
        );

        let duplicate_id = r#"{"Id":"current-policy","ID":"legacy-policy","Version":"2012-10-17","Statement":[]}"#;
        let error = serde_json::from_str::<BucketPolicy>(duplicate_id)
            .expect_err("canonical and legacy ID fields should not be accepted together");
        assert!(
            error.to_string().contains("duplicate field `Id`"),
            "unexpected deserialization error: {error}"
        );
    }

    #[test]
    fn test_existing_object_tag_condition_helpers() {
        let identity_policy = Policy::parse_config(
            br#"{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Action":["s3:GetObject"],
    "Resource":["arn:aws:s3:::bucket/*"],
    "Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}
  }]
}"#,
        )
        .expect("identity policy with ExistingObjectTag key should parse");
        assert!(
            policy_uses_existing_object_tag_conditions(&identity_policy),
            "identity policy ExistingObjectTag key should be detected"
        );

        let identity_value_only = Policy::parse_config(
            br#"{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Action":["s3:GetObject"],
    "Resource":["arn:aws:s3:::bucket/*"],
    "Condition":{"StringEquals":{"s3:prefix":"ExistingObjectTag/security"}}
  }]
}"#,
        )
        .expect("identity policy with value-only marker should parse");
        assert!(
            !policy_uses_existing_object_tag_conditions(&identity_value_only),
            "value-only marker must not be treated as ExistingObjectTag condition key"
        );

        let bucket_policy: BucketPolicy = serde_json::from_str(
            r#"{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Principal":"*",
    "Action":["s3:GetObject"],
    "Resource":["arn:aws:s3:::bucket/*"],
    "Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}
  }]
}"#,
        )
        .expect("bucket policy with ExistingObjectTag key should parse");
        assert!(
            bucket_policy_uses_existing_object_tag_conditions(&bucket_policy),
            "bucket policy ExistingObjectTag key should be detected"
        );
    }

    #[test]
    fn test_bucket_policy_serialize_single_action_as_array() {
        use crate::policy::action::{Action, ActionSet, S3Action};
        use crate::policy::resource::{Resource, ResourceSet};
        use crate::policy::{Effect, Principal};

        // Use JSON deserialization to create Principal (since aws field is private)
        let principal: Principal = serde_json::from_str(r#"{"AWS": "*"}"#).expect("Should parse principal");

        let mut policy = BucketPolicy {
            version: "2012-10-17".to_string(),
            statements: vec![BPStatement {
                effect: Effect::Allow,
                principal,
                actions: ActionSet::default(),
                resources: ResourceSet::default(),
                ..Default::default()
            }],
            ..Default::default()
        };

        // Single action
        policy.statements[0]
            .actions
            .0
            .push(Action::S3Action(S3Action::ListBucketAction));
        policy.statements[0]
            .resources
            .0
            .push(Resource::try_from("arn:aws:s3:::test/*").expect("test resource should parse"));

        let json = serde_json::to_string(&policy).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
        let action = &parsed["Statement"][0]["Action"];

        // Single action should be serialized as array for S3 specification compliance
        assert!(action.is_array(), "Single action should serialize as array");
        let arr = action.as_array().expect("Should be array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].as_str().unwrap(), "s3:ListBucket");
    }

    #[tokio::test]
    async fn test_bucket_policy_list_bucket_prefix_condition_uses_bucket_resource() -> Result<()> {
        let bucket_policy: BucketPolicy = serde_json::from_str(
            r#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": "*"},
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::polaris-test-bucket"],
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "polaris_test/snowflake_catalog/db1/schema/iceberg_table/*"
          ]
        }
      }
    }
  ]
}"#,
        )?;

        let mut conditions = HashMap::new();
        conditions.insert(
            "prefix".to_string(),
            vec!["polaris_test/snowflake_catalog/db1/schema/iceberg_table/metadata/".to_string()],
        );
        let args = BucketPolicyArgs {
            account: "polaris-session",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::ListBucketAction),
            bucket: "polaris-test-bucket",
            conditions: &conditions,
            is_owner: false,
            object: "polaris_test/snowflake_catalog/db1/schema/iceberg_table/metadata/",
        };

        assert!(
            bucket_policy.is_allowed(&args).await,
            "Bucket policy ListBucket should match the bucket resource and apply the prefix through the condition"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_bucket_policy_deny_with_string_not_equals() -> Result<()> {
        let data = r#"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Deny",
      "Action": "s3:PutObject",
      "Principal": {"AWS": "*"},
      "Resource": "arn:aws:s3:::mybucket/*",
      "Condition": {
        "StringNotEquals": {
          "s3:x-amz-server-side-encryption": "aws:kms"
        }
      }
    },
    {
      "Effect": "Deny",
      "Action": "s3:PutObject",
      "Principal": {"AWS": "*"},
      "Resource": "arn:aws:s3:::mybucket/*",
      "Condition": {
        "Null": {
          "s3:x-amz-server-side-encryption": "true"
        }
      }
    }
  ]
}
"#;

        let bp: BucketPolicy = serde_json::from_slice(data.as_bytes())?;

        // Request with wrong encryption → should be DENIED (StringNotEquals matches)
        let mut cond_wrong_enc = HashMap::new();
        cond_wrong_enc.insert("x-amz-server-side-encryption".to_string(), vec!["AES256".to_string()]);

        let args_wrong = BucketPolicyArgs {
            account: "testowner",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::PutObjectAction),
            bucket: "mybucket",
            conditions: &cond_wrong_enc,
            is_owner: true,
            object: "testobj",
        };
        assert!(
            !bp.is_allowed(&args_wrong).await,
            "Should deny PutObject with AES256 when policy requires aws:kms"
        );

        // Request with correct encryption → should be ALLOWED
        let mut cond_correct_enc = HashMap::new();
        cond_correct_enc.insert("x-amz-server-side-encryption".to_string(), vec!["aws:kms".to_string()]);

        let args_correct = BucketPolicyArgs {
            account: "testowner",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::PutObjectAction),
            bucket: "mybucket",
            conditions: &cond_correct_enc,
            is_owner: true,
            object: "testobj",
        };
        assert!(
            bp.is_allowed(&args_correct).await,
            "Should allow PutObject with aws:kms matching the policy"
        );

        // Request with no encryption header → should be DENIED (Null condition matches)
        let cond_no_enc = HashMap::new();

        let args_no_enc = BucketPolicyArgs {
            account: "testowner",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::PutObjectAction),
            bucket: "mybucket",
            conditions: &cond_no_enc,
            is_owner: true,
            object: "testobj",
        };
        assert!(!bp.is_allowed(&args_no_enc).await, "Should deny PutObject with no encryption header");

        Ok(())
    }

    #[tokio::test]
    async fn test_policy_needs_existing_object_tag_narrows_by_action() {
        use crate::policy::Args;
        use crate::policy::action::{Action, S3Action};
        use std::collections::HashMap;

        let split_policy = Policy::parse_config(
            br#"{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Action":["s3:DeleteObject"],
      "Resource":["arn:aws:s3:::bucket/*"]
    },
    {
      "Effect":"Allow",
      "Action":["s3:DeleteObjectVersion"],
      "Resource":["arn:aws:s3:::bucket/*"],
      "Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}
    }
  ]
}"#,
        )
        .expect("split-action policy should parse");

        let groups: Option<Vec<String>> = None;
        let cond = HashMap::new();
        let claims = HashMap::new();

        let args_get = Args {
            account: "user",
            groups: &groups,
            action: Action::S3Action(S3Action::GetObjectAction),
            bucket: "bucket",
            conditions: &cond,
            is_owner: false,
            object: "k",
            claims: &claims,
            deny_only: false,
        };
        assert!(
            !policy_needs_existing_object_tag_for_args(&split_policy, &args_get).await,
            "GetObject should not match statements with DeleteObject/DeleteObjectVersion"
        );

        let args_del = Args {
            account: "user",
            groups: &groups,
            action: Action::S3Action(S3Action::DeleteObjectAction),
            bucket: "bucket",
            conditions: &cond,
            is_owner: false,
            object: "k",
            claims: &claims,
            deny_only: false,
        };
        assert!(
            !policy_needs_existing_object_tag_for_args(&split_policy, &args_del).await,
            "DeleteObject matches only the statement without ExistingObjectTag"
        );

        let args_delv = Args {
            account: "user",
            groups: &groups,
            action: Action::S3Action(S3Action::DeleteObjectVersionAction),
            bucket: "bucket",
            conditions: &cond,
            is_owner: false,
            object: "k",
            claims: &claims,
            deny_only: false,
        };
        assert!(
            policy_needs_existing_object_tag_for_args(&split_policy, &args_delv).await,
            "DeleteObjectVersion matches the statement with ExistingObjectTag"
        );
    }

    #[tokio::test]
    async fn test_policy_needs_existing_object_tag_narrows_by_resource() {
        use crate::policy::Args;
        use crate::policy::action::{Action, S3Action};
        use std::collections::HashMap;

        let policy = Policy::parse_config(
            br#"{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Action":["s3:GetObject"],
      "Resource":["arn:aws:s3:::bucket/private/*"],
      "Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}
    }
  ]
}"#,
        )
        .expect("policy should parse");

        let groups: Option<Vec<String>> = None;
        let cond = HashMap::new();
        let claims = HashMap::new();

        let args_public = Args {
            account: "user",
            groups: &groups,
            action: Action::S3Action(S3Action::GetObjectAction),
            bucket: "bucket",
            conditions: &cond,
            is_owner: false,
            object: "public/a.txt",
            claims: &claims,
            deny_only: false,
        };
        assert!(
            !policy_needs_existing_object_tag_for_args(&policy, &args_public).await,
            "resource mismatch should skip ExistingObjectTag fetch hint"
        );

        let args_private = Args {
            account: "user",
            groups: &groups,
            action: Action::S3Action(S3Action::GetObjectAction),
            bucket: "bucket",
            conditions: &cond,
            is_owner: false,
            object: "private/a.txt",
            claims: &claims,
            deny_only: false,
        };
        assert!(
            policy_needs_existing_object_tag_for_args(&policy, &args_private).await,
            "resource match should keep ExistingObjectTag fetch hint"
        );
    }

    #[tokio::test]
    async fn test_bucket_policy_needs_existing_object_tag_narrows_by_principal() {
        use crate::policy::BucketPolicyArgs;
        use crate::policy::action::{Action, S3Action};
        use std::collections::HashMap;

        let bucket_policy: BucketPolicy = serde_json::from_str(
            r#"{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Principal":{"AWS":["alice"]},
      "Action":["s3:GetObject"],
      "Resource":["arn:aws:s3:::bucket/private/*"],
      "Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}
    }
  ]
}"#,
        )
        .expect("bucket policy should parse");

        let groups: Option<Vec<String>> = None;
        let cond = HashMap::new();

        let args_bob = BucketPolicyArgs {
            bucket: "bucket",
            action: Action::S3Action(S3Action::GetObjectAction),
            is_owner: false,
            account: "bob",
            groups: &groups,
            conditions: &cond,
            object: "private/a.txt",
        };
        assert!(
            !bucket_policy_needs_existing_object_tag_for_args(&bucket_policy, &args_bob).await,
            "principal mismatch should skip ExistingObjectTag fetch hint"
        );

        let args_alice_public = BucketPolicyArgs {
            bucket: "bucket",
            action: Action::S3Action(S3Action::GetObjectAction),
            is_owner: false,
            account: "alice",
            groups: &groups,
            conditions: &cond,
            object: "public/a.txt",
        };
        assert!(
            !bucket_policy_needs_existing_object_tag_for_args(&bucket_policy, &args_alice_public).await,
            "resource mismatch should skip ExistingObjectTag fetch hint"
        );

        let args_alice_private = BucketPolicyArgs {
            bucket: "bucket",
            action: Action::S3Action(S3Action::GetObjectAction),
            is_owner: false,
            account: "alice",
            groups: &groups,
            conditions: &cond,
            object: "private/a.txt",
        };
        assert!(
            bucket_policy_needs_existing_object_tag_for_args(&bucket_policy, &args_alice_private).await,
            "principal and resource match should keep ExistingObjectTag fetch hint"
        );
    }

    #[test]
    fn test_get_values_from_claims_case_insensitive() {
        let mut claims = HashMap::new();
        claims.insert("policyminio".to_string(), Value::Array(vec![Value::String("consoleAdmin".to_string())]));

        let (policies, found) = get_values_from_claims(&claims, "policyMinio");
        assert!(found);
        assert!(policies.contains("consoleAdmin"));

        let (policies, found) = get_values_from_claims(&claims, "POLICYMINIO");
        assert!(found);
        assert!(policies.contains("consoleAdmin"));

        let (policies, found) = get_values_from_claims(&claims, "policyminio");
        assert!(found);
        assert!(policies.contains("consoleAdmin"));
    }

    #[test]
    fn test_get_values_from_claims_exact_match_preferred() {
        let mut claims = HashMap::new();
        claims.insert("Policy".to_string(), Value::Array(vec![Value::String("exact_match".to_string())]));
        claims.insert("policy".to_string(), Value::Array(vec![Value::String("lowercase".to_string())]));

        let (policies, _) = get_values_from_claims(&claims, "Policy");
        assert!(policies.contains("exact_match"));
        assert!(!policies.contains("lowercase"));
    }

    #[test]
    fn test_get_policies_from_claims_case_insensitive_string() {
        let mut claims = HashMap::new();
        claims.insert("policyminio".to_string(), Value::String("consoleAdmin,readwrite".to_string()));

        let (policies, found) = get_policies_from_claims(&claims, "policyMinio");
        assert!(found);
        assert!(policies.contains("consoleAdmin"));
        assert!(policies.contains("readwrite"));
    }

    #[test]
    fn test_get_values_from_claims_ambiguous_case_insensitive_match_returns_missing() {
        let mut claims = HashMap::new();
        claims.insert("Policy".to_string(), Value::Array(vec![Value::String("exact_match".to_string())]));
        claims.insert("policy".to_string(), Value::Array(vec![Value::String("lowercase".to_string())]));

        let (policies, found) = get_values_from_claims(&claims, "POLICY");
        assert!(!found);
        assert!(policies.is_empty());
    }

    #[test]
    fn test_get_policies_from_claims_ambiguous_case_insensitive_match_returns_missing() {
        let mut claims = HashMap::new();
        claims.insert("Policy".to_string(), Value::String("consoleAdmin".to_string()));
        claims.insert("policy".to_string(), Value::String("readwrite".to_string()));

        let (policies, found) = get_policies_from_claims(&claims, "POLICY");
        assert!(!found);
        assert!(policies.is_empty());
    }

    #[test]
    fn test_policy_round_trips_through_json_value() {
        let policy = Policy::parse_config(
            br#"{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Action":["s3:GetObject"],
      "Resource":["arn:aws:s3:::bucket/*"]
    }
  ]
}"#,
        )
        .expect("policy should parse");

        let value = serde_json::to_value(&policy).expect("policy should serialize");
        let round_trip: Policy = serde_json::from_value(value).expect("policy should deserialize from serde_json::Value");

        assert_eq!(round_trip.version, policy.version);
        assert_eq!(round_trip.statements.len(), policy.statements.len());
        assert_eq!(round_trip.statements[0].effect, policy.statements[0].effect);
    }
}
