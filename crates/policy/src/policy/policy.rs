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

use super::{Effect, Error as IamError, ID, Statement, action::Action, statement::BPStatement};
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
pub struct Policy {
    #[serde(default, rename = "ID")]
    pub id: ID,
    #[serde(rename = "Version")]
    pub version: String,
    #[serde(rename = "Statement")]
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

        // Owner has all permissions
        if args.is_owner {
            return true;
        }

        if args.deny_only {
            return false;
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
pub struct BucketPolicy {
    #[serde(default, rename = "Id", skip_serializing_if = "ID::is_empty")]
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
    if let Some(pname) = claims.get(claim_name) {
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
        } else if let Some(pname_str) = pname.as_str() {
            for pname in pname_str.split(',') {
                let pname = pname.trim();
                if !pname.is_empty() {
                    s.insert(pname.to_string());
                }
            }
            return (s, true);
        }
    }
    (s, false)
}

pub fn get_policies_from_claims(claims: &HashMap<String, Value>, policy_claim_name: &str) -> (HashSet<String>, bool) {
    get_values_from_claims(claims, policy_claim_name)
}

pub fn iam_policy_claim_name_sa() -> String {
    rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA.to_string()
}

pub mod default {
    use std::{collections::HashSet, sync::LazyLock};

    use crate::policy::{
        ActionSet, DEFAULT_VERSION, Effect, Functions, ResourceSet, Statement,
        action::{Action, AdminAction, KmsAction, S3Action},
        resource::Resource,
    };

    use super::Policy;

    #[allow(clippy::incompatible_msrv)]
    pub static DEFAULT_POLICIES: LazyLock<[(&'static str, Policy); 6]> = LazyLock::new(|| {
        [
            (
                "readwrite",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![Statement {
                        sid: "".into(),
                        effect: Effect::Allow,
                        actions: ActionSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Action::S3Action(S3Action::AllActions));
                            hash_set
                        }),
                        not_actions: ActionSet(Default::default()),
                        resources: ResourceSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Resource::S3("*".into()));
                            hash_set
                        }),
                        conditions: Functions::default(),
                        ..Default::default()
                    }],
                },
            ),
            (
                "readonly",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![Statement {
                        sid: "".into(),
                        effect: Effect::Allow,
                        actions: ActionSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Action::S3Action(S3Action::GetBucketLocationAction));
                            hash_set.insert(Action::S3Action(S3Action::GetObjectAction));
                            hash_set.insert(Action::S3Action(S3Action::GetBucketQuotaAction));
                            hash_set
                        }),
                        not_actions: ActionSet(Default::default()),
                        resources: ResourceSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Resource::S3("*".into()));
                            hash_set
                        }),
                        conditions: Functions::default(),
                        ..Default::default()
                    }],
                },
            ),
            (
                "writeonly",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![Statement {
                        sid: "".into(),
                        effect: Effect::Allow,
                        actions: ActionSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Action::S3Action(S3Action::PutObjectAction));
                            hash_set
                        }),
                        not_actions: ActionSet(Default::default()),
                        resources: ResourceSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Resource::S3("*".into()));
                            hash_set
                        }),
                        conditions: Functions::default(),
                        ..Default::default()
                    }],
                },
            ),
            (
                "writeonly",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![Statement {
                        sid: "".into(),
                        effect: Effect::Allow,
                        actions: ActionSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Action::S3Action(S3Action::PutObjectAction));
                            hash_set
                        }),
                        not_actions: ActionSet(Default::default()),
                        resources: ResourceSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Resource::S3("*".into()));
                            hash_set
                        }),
                        conditions: Functions::default(),
                        ..Default::default()
                    }],
                },
            ),
            (
                "diagnostics",
                Policy {
                    id: "".into(),
                    version: DEFAULT_VERSION.into(),
                    statements: vec![Statement {
                        sid: "".into(),
                        effect: Effect::Allow,
                        actions: ActionSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Action::AdminAction(AdminAction::ProfilingAdminAction));
                            hash_set.insert(Action::AdminAction(AdminAction::TraceAdminAction));
                            hash_set.insert(Action::AdminAction(AdminAction::ConsoleLogAdminAction));
                            hash_set.insert(Action::AdminAction(AdminAction::ServerInfoAdminAction));
                            hash_set.insert(Action::AdminAction(AdminAction::TopLocksAdminAction));
                            hash_set.insert(Action::AdminAction(AdminAction::HealthInfoAdminAction));
                            hash_set.insert(Action::AdminAction(AdminAction::PrometheusAdminAction));
                            hash_set.insert(Action::AdminAction(AdminAction::BandwidthMonitorAction));
                            hash_set
                        }),
                        not_actions: ActionSet(Default::default()),
                        resources: ResourceSet({
                            let mut hash_set = HashSet::new();
                            hash_set.insert(Resource::S3("*".into()));
                            hash_set
                        }),
                        conditions: Functions::default(),
                        ..Default::default()
                    }],
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
                            actions: ActionSet({
                                let mut hash_set = HashSet::new();
                                hash_set.insert(Action::AdminAction(AdminAction::AllAdminActions));
                                hash_set
                            }),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(HashSet::new()),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet({
                                let mut hash_set = HashSet::new();
                                hash_set.insert(Action::KmsAction(KmsAction::AllActions));
                                hash_set
                            }),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet(HashSet::new()),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
                        Statement {
                            sid: "".into(),
                            effect: Effect::Allow,
                            actions: ActionSet({
                                let mut hash_set = HashSet::new();
                                hash_set.insert(Action::S3Action(S3Action::AllActions));
                                hash_set
                            }),
                            not_actions: ActionSet(Default::default()),
                            resources: ResourceSet({
                                let mut hash_set = HashSet::new();
                                hash_set.insert(Resource::S3("*".into()));
                                hash_set
                            }),
                            conditions: Functions::default(),
                            ..Default::default()
                        },
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
    async fn test_deny_only_security_fix() -> Result<()> {
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

        // Test with deny_only=true but no matching Allow statement
        let args_deny_only = Args {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::PutObjectAction),
            bucket: "bucket2",
            conditions: &conditions,
            is_owner: false,
            object: "test.txt",
            claims: &claims,
            deny_only: true, // Should NOT automatically allow
        };

        // Should return false because deny_only=true, regardless of whether there's a matching Allow statement
        assert!(
            !policy.is_allowed(&args_deny_only).await,
            "deny_only should return false when deny_only=true, regardless of Allow statements"
        );

        // Test with deny_only=true and matching Allow statement
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

        // Should return false because deny_only=true prevents checking Allow statements (unless is_owner=true)
        assert!(
            !policy.is_allowed(&args_deny_only_allowed).await,
            "deny_only should return false even with matching Allow statement"
        );

        // Test with deny_only=false (normal case)
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

        let args_owner_deny_only = Args {
            account: "testuser",
            groups: &None,
            action: Action::S3Action(crate::policy::action::S3Action::PutObjectAction),
            bucket: "bucket2",
            conditions: &conditions,
            is_owner: true, // Owner has all permissions
            object: "test.txt",
            claims: &claims,
            deny_only: true, // Even with deny_only=true, owner should be allowed
        };

        assert!(
            policy.is_allowed(&args_owner_deny_only).await,
            "owner should retain all permissions even when deny_only=true"
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

        // Verify the specific error type
        if let Err(e) = result {
            let error_msg = format!("{}", e);
            assert!(
                error_msg.contains("Resource")
                    && error_msg.contains("NotResource")
                    && error_msg.contains("cannot both be specified"),
                "Error should be BothResourceAndNotResource, got: {}",
                error_msg
            );
        }
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

        if let Err(e) = result {
            let error_msg = format!("{}", e);
            assert!(
                error_msg.contains("Action") && error_msg.contains("NotAction") && error_msg.contains("cannot both be specified"),
                "Error should be BothActionAndNotAction, got: {}",
                error_msg
            );
        }
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
        if let Err(e) = result {
            let error_msg = format!("{}", e);
            assert!(
                error_msg.contains("Action") && error_msg.contains("NotAction") && error_msg.contains("empty"),
                "Error should be NonAction, got: {}",
                error_msg
            );
        }
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

        // Verify the specific error type
        if let Err(e) = result {
            let error_msg = format!("{}", e);
            assert!(
                error_msg.contains("Resource") && error_msg.contains("empty"),
                "Error should be NonResource, got: {}",
                error_msg
            );
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
            .insert(Action::S3Action(S3Action::ListBucketAction));
        policy.statements[0]
            .resources
            .0
            .insert(Resource::try_from("arn:aws:s3:::test/*").unwrap());

        let json = serde_json::to_string(&policy).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");

        // Verify empty fields are omitted
        assert!(!parsed.as_object().unwrap().contains_key("ID"), "Empty ID should be omitted");

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
            .insert(Action::S3Action(S3Action::ListBucketAction));
        policy.statements[0]
            .resources
            .0
            .insert(Resource::try_from("arn:aws:s3:::test/*").unwrap());

        let json = serde_json::to_string(&policy).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
        let action = &parsed["Statement"][0]["Action"];

        // Single action should be serialized as array for S3 specification compliance
        assert!(action.is_array(), "Single action should serialize as array");
        let arr = action.as_array().expect("Should be array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].as_str().unwrap(), "s3:ListBucket");
    }
}
