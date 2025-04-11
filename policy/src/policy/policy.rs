use super::{action::Action, statement::BPStatement, Effect, Error as IamError, Statement, ID};
use common::error::{Error, Result};
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
    pub fn is_allowed(&self, args: &Args) -> bool {
        for statement in self.statements.iter().filter(|s| matches!(s.effect, Effect::Deny)) {
            if !statement.is_allowed(args) {
                return false;
            }
        }

        if args.deny_only || args.is_owner {
            return true;
        }

        for statement in self.statements.iter().filter(|s| matches!(s.effect, Effect::Allow)) {
            if statement.is_allowed(args) {
                return true;
            }
        }

        false
    }

    pub fn match_resource(&self, resource: &str) -> bool {
        for statement in self.statements.iter() {
            if statement.resources.match_resource(resource) {
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
        if !self.id.is_empty() && !self.id.eq(DEFAULT_VERSION) {
            return Err(IamError::InvalidVersion(self.id.0.clone()).into());
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

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct BucketPolicy {
    #[serde(default, rename = "ID")]
    pub id: ID,
    #[serde(rename = "Version")]
    pub version: String,
    #[serde(rename = "Statement")]
    pub statements: Vec<BPStatement>,
}

impl BucketPolicy {
    pub fn is_allowed(&self, args: &BucketPolicyArgs) -> bool {
        for statement in self.statements.iter().filter(|s| matches!(s.effect, Effect::Deny)) {
            if !statement.is_allowed(args) {
                return false;
            }
        }

        if args.is_owner {
            return true;
        }

        for statement in self.statements.iter().filter(|s| matches!(s.effect, Effect::Allow)) {
            if statement.is_allowed(args) {
                return true;
            }
        }

        false
    }
}

impl Validator for BucketPolicy {
    type Error = Error;

    fn is_valid(&self) -> Result<()> {
        if !self.id.is_empty() && !self.id.eq(DEFAULT_VERSION) {
            return Err(IamError::InvalidVersion(self.id.0.clone()).into());
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
    "sa-policy".to_string()
}

pub mod default {
    use std::{collections::HashSet, sync::LazyLock};

    use crate::policy::{
        action::{Action, AdminAction, KmsAction, S3Action},
        resource::Resource,
        ActionSet, Effect, Functions, ResourceSet, Statement, DEFAULT_VERSION,
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
    use common::error::Result;

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

        // println!("{:?}", p);

        let str = serde_json::to_string(&p)?;

        // println!("----- {}", str);

        let _p2 = Policy::parse_config(str.as_bytes())?;
        // println!("33{:?}", p2);

        // assert_eq!(p, p2);
        Ok(())
    }
}
