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

use rustfs_policy::policy::Effect::*;
use rustfs_policy::policy::action::S3Action::*;
use rustfs_policy::policy::*;
use serde_json::Value;
use std::collections::HashMap;
use test_case::test_case;

#[derive(Default)]
struct ArgsBuilder {
    pub account: String,
    pub groups: Vec<String>,
    pub action: String,
    pub bucket: String,
    pub conditions: HashMap<String, Vec<String>>,
    pub is_owner: bool,
    pub object: String,
    pub claims: HashMap<String, Value>,
    pub deny_only: bool,
}

#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            rustfs_policy::policy::Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(PutObjectAction), rustfs_policy::policy::action::Action::S3Action(GetBucketLocationAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::*".try_into().unwrap()].into_iter().collect()),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetBucketLocation".into(),
        bucket: "mybucket".into(),
        ..Default::default()
    } => true;
    "1"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(PutObjectAction), rustfs_policy::policy::action::Action::S3Action(GetBucketLocationAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::*".try_into().unwrap()].into_iter().collect()),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:PutObject".into(),
        bucket: "mybucket".into(),
        conditions: {
           let mut h = HashMap::new();
           h.insert("x-amz-copy-source".into(), vec!["mybucket/myobject".into()]);
           h.insert("SourceIp".into(), vec!["192.168.1.10".into()]);
           h
        },
        ..Default::default()
    } => true;
    "2"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(PutObjectAction), rustfs_policy::policy::action::Action::S3Action(GetBucketLocationAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::*".try_into().unwrap()].into_iter().collect()),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        ..Default::default()
    } => false;
    "3"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(PutObjectAction), rustfs_policy::policy::action::Action::S3Action(GetBucketLocationAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::*".try_into().unwrap()].into_iter().collect()),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetBucketLocation".into(),
        bucket: "mybucket".into(),
        ..Default::default()
    } => true;
    "4"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(PutObjectAction), rustfs_policy::policy::action::Action::S3Action(GetBucketLocationAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::*".try_into().unwrap()].into_iter().collect()),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:PutObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        conditions: {
           let mut h = HashMap::new();
           h.insert("x-amz-copy-source".into(), vec!["mybucket/myobject".into()]);
           h.insert("SourceIp".into(), vec!["192.168.1.10".into()]);
           h
        },
        ..Default::default()
    } => true;
    "5"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(PutObjectAction), rustfs_policy::policy::action::Action::S3Action(GetBucketLocationAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::*".try_into().unwrap()].into_iter().collect()),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        ..Default::default()
    } => false;
    "6"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetBucketLocation".into(),
        bucket: "mybucket".into(),
        ..Default::default()
    } => false;
    "7"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:PutObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        conditions: {
           let mut h = HashMap::new();
           h.insert("x-amz-copy-source".into(), vec!["mybucket/myobject".into()]);
           h.insert("SourceIp".into(), vec!["192.168.1.10".into()]);
           h
        },
        ..Default::default()
    } => true;
    "8"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        ..Default::default()
    } => true;
    "9"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetBucketLocation".into(),
        bucket: "mybucket".into(),
        ..Default::default()
    } => false;
    "10"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:PutObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        conditions: {
           let mut h = HashMap::new();
           h.insert("x-amz-copy-source".into(), vec!["mybucket/myobject".into()]);
           h.insert("SourceIp".into(), vec!["192.168.1.10".into()]);
           h
        },
        ..Default::default()
    } => true;
    "11"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        ..Default::default()
    } => true;
    "12"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    conditions: serde_json::from_str(r#"{"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}}"#).unwrap(),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetBucketLocation".into(),
        bucket: "mybucket".into(),
        ..Default::default()
    } => false;
    "13"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    conditions: serde_json::from_str(r#"{"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}}"#).unwrap(),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:PutObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        conditions: {
           let mut h = HashMap::new();
           h.insert("x-amz-copy-source".into(), vec!["mybucket/myobject".into()]);
           h.insert("SourceIp".into(), vec!["192.168.1.10".into()]);
           h
        },
        ..Default::default()
    } => true;
    "14"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    conditions: serde_json::from_str(r#"{"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}}"#).unwrap(),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        ..Default::default()
    } => false;
    "15"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    conditions: serde_json::from_str(r#"{"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}}"#).unwrap(),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetBucketLocation".into(),
        bucket: "mybucket".into(),
        ..Default::default()
    } => false;
    "16"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    conditions: serde_json::from_str(r#"{"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}}"#).unwrap(),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:PutObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        conditions: {
           let mut h = HashMap::new();
           h.insert("x-amz-copy-source".into(), vec!["mybucket/myobject".into()]);
           h.insert("SourceIp".into(), vec!["192.168.1.10".into()]);
           h
        },
        ..Default::default()
    } => true;
    "17"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    conditions: serde_json::from_str(r#"{"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}}"#).unwrap(),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        ..Default::default()
    } => false;
    "18"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Deny,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    conditions: serde_json::from_str(r#"{"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}}"#).unwrap(),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetBucketLocation".into(),
        bucket: "mybucket".into(),
        ..Default::default()
    } => false;
    "19"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Deny,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    conditions: serde_json::from_str(r#"{"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}}"#).unwrap(),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:PutObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        conditions: {
           let mut h = HashMap::new();
           h.insert("x-amz-copy-source".into(), vec!["mybucket/myobject".into()]);
           h.insert("SourceIp".into(), vec!["192.168.1.10".into()]);
           h
        },
        ..Default::default()
    } => false;
    "20"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Deny,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    conditions: serde_json::from_str(r#"{"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}}"#).unwrap(),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        ..Default::default()
    } => false;
    "21"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Deny,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    conditions: serde_json::from_str(r#"{"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}}"#).unwrap(),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetBucketLocation".into(),
        bucket: "mybucket".into(),
        ..Default::default()
    } => false;
    "22"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Deny,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    conditions: serde_json::from_str(r#"{"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}}"#).unwrap(),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:PutObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        conditions: {
           let mut h = HashMap::new();
           h.insert("x-amz-copy-source".into(), vec!["mybucket/myobject".into()]);
           h.insert("SourceIp".into(), vec!["192.168.1.10".into()]);
           h
        },
        ..Default::default()
    } => false;
    "23"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                    effect: Deny,
                    actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction), rustfs_policy::policy::action::Action::S3Action(PutObjectAction)].into_iter().collect()),
                    resources: ResourceSet(vec!["arn:aws:s3:::mybucket/myobject*".try_into().unwrap()].into_iter().collect()),
                    conditions: serde_json::from_str(r#"{"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}}"#).unwrap(),
                    ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetObject".into(),
        bucket: "mybucket".into(),
        object: "myobject".into(),
        ..Default::default()
    } => false;
    "24"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                effect: Allow,
                actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction)].into_iter().collect()),
                resources: ResourceSet::default(), // Empty Resource
                not_resources: ResourceSet(vec!["arn:aws:s3:::mybucket/private/*".try_into().unwrap()].into_iter().collect()),
                ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetObject".into(),
        bucket: "mybucket".into(),
        object: "public/file.txt".into(),
        ..Default::default()
    } => true;
    "notresource_allows_access_outside_blacklist"
)]
#[test_case(
    Policy{
        version: DEFAULT_VERSION.into(),
        statements: vec![
            Statement{
                effect: Allow,
                actions: ActionSet(vec![rustfs_policy::policy::action::Action::S3Action(GetObjectAction)].into_iter().collect()),
                resources: ResourceSet::default(), // Empty Resource
                not_resources: ResourceSet(vec!["arn:aws:s3:::mybucket/private/*".try_into().unwrap()].into_iter().collect()),
                ..Default::default()
            }
        ],
        ..Default::default()
    },
    ArgsBuilder{
        account: "Q3AM3UQ867SPQQA43P2F".into(),
        action: "s3:GetObject".into(),
        bucket: "mybucket".into(),
        object: "private/secret.txt".into(),
        ..Default::default()
    } => false;
    "notresource_denies_access_in_blacklist"
)]
fn policy_is_allowed(policy: Policy, args: ArgsBuilder) -> bool {
    pollster::block_on(policy.is_allowed(&Args {
        account: &args.account,
        groups: &{
            if args.groups.is_empty() {
                None
            } else {
                Some(args.groups.clone())
            }
        },
        action: args.action.as_str().try_into().unwrap(),
        bucket: &args.bucket,
        conditions: &args.conditions,
        is_owner: args.is_owner,
        object: &args.object,
        claims: &args.claims,
        deny_only: args.deny_only,
    }))
}

// Regression coverage for rustfs/backlog#1028: `Statement` equality must account for
// `NotResource`. When it does not, `Policy::merge_policies`/`drop_duplicate_statements`
// treat statements that differ only in `NotResource` as duplicates and drop one of them,
// which can shrink Deny coverage and escalate privileges.

fn get_object() -> rustfs_policy::policy::action::Action {
    rustfs_policy::policy::action::Action::S3Action(GetObjectAction)
}

fn is_allowed_get_object(policy: &Policy, bucket: &str, object: &str) -> bool {
    let conditions: HashMap<String, Vec<String>> = HashMap::new();
    let claims: HashMap<String, Value> = HashMap::new();
    let groups: Option<Vec<String>> = None;
    pollster::block_on(policy.is_allowed(&Args {
        account: "Q3AM3UQ867SPQQA43P2F",
        groups: &groups,
        action: "s3:GetObject".try_into().unwrap(),
        bucket,
        conditions: &conditions,
        is_owner: false,
        object,
        claims: &claims,
        deny_only: false,
    }))
}

#[test]
fn statements_differing_only_in_not_resource_are_not_equal() {
    let base = Statement {
        effect: Deny,
        actions: ActionSet(vec![get_object()].into_iter().collect()),
        not_resources: ResourceSet(
            vec!["arn:aws:s3:::mybucket/public/*".try_into().unwrap()]
                .into_iter()
                .collect(),
        ),
        ..Default::default()
    };
    let other = Statement {
        not_resources: ResourceSet(vec!["arn:aws:s3:::mybucket/logs/*".try_into().unwrap()].into_iter().collect()),
        ..base.clone()
    };
    assert_ne!(base, other, "statements differing only in NotResource must not compare equal");
}

#[test]
fn merge_keeps_deny_statements_differing_only_in_not_resource() {
    let allow_all = Policy {
        version: DEFAULT_VERSION.into(),
        statements: vec![Statement {
            effect: Allow,
            actions: ActionSet(vec![get_object()].into_iter().collect()),
            resources: ResourceSet(vec!["arn:aws:s3:::mybucket/*".try_into().unwrap()].into_iter().collect()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let deny_except_public = Policy {
        version: DEFAULT_VERSION.into(),
        statements: vec![Statement {
            effect: Deny,
            actions: ActionSet(vec![get_object()].into_iter().collect()),
            not_resources: ResourceSet(
                vec!["arn:aws:s3:::mybucket/public/*".try_into().unwrap()]
                    .into_iter()
                    .collect(),
            ),
            ..Default::default()
        }],
        ..Default::default()
    };
    let deny_except_logs = Policy {
        version: DEFAULT_VERSION.into(),
        statements: vec![Statement {
            effect: Deny,
            actions: ActionSet(vec![get_object()].into_iter().collect()),
            not_resources: ResourceSet(vec!["arn:aws:s3:::mybucket/logs/*".try_into().unwrap()].into_iter().collect()),
            ..Default::default()
        }],
        ..Default::default()
    };

    let merged = Policy::merge_policies(vec![allow_all, deny_except_public, deny_except_logs]);
    assert_eq!(
        merged.statements.len(),
        3,
        "both Deny statements must survive the merge; dropping either shrinks Deny coverage"
    );

    // A request into public/* is denied only by the NotResource=[logs/*] statement. If that
    // statement were dropped as a duplicate, the broad Allow would leak access.
    assert!(
        !is_allowed_get_object(&merged, "mybucket", "public/file.txt"),
        "request into public/* must remain denied"
    );
}

#[test]
fn merge_keeps_allow_statements_differing_only_in_not_resource() {
    let allow_except_public = Policy {
        version: DEFAULT_VERSION.into(),
        statements: vec![Statement {
            effect: Allow,
            actions: ActionSet(vec![get_object()].into_iter().collect()),
            not_resources: ResourceSet(
                vec!["arn:aws:s3:::mybucket/public/*".try_into().unwrap()]
                    .into_iter()
                    .collect(),
            ),
            ..Default::default()
        }],
        ..Default::default()
    };
    let allow_except_logs = Policy {
        version: DEFAULT_VERSION.into(),
        statements: vec![Statement {
            effect: Allow,
            actions: ActionSet(vec![get_object()].into_iter().collect()),
            not_resources: ResourceSet(vec!["arn:aws:s3:::mybucket/logs/*".try_into().unwrap()].into_iter().collect()),
            ..Default::default()
        }],
        ..Default::default()
    };

    let merged = Policy::merge_policies(vec![allow_except_public, allow_except_logs]);
    assert_eq!(
        merged.statements.len(),
        2,
        "both Allow statements must survive the merge; neither may be dropped as a duplicate"
    );
}

#[test]
fn wildcard_deny_only_session_grants_no_storage_access() {
    let policy = Policy::parse_config(
        br#"{"Statement":[{"Action":["s3:GetObject"],"Effect":"Deny","Resource":"*","Sid":"1"}],"Version":"2012-10-17"}"#,
    )
    .expect("deny-only session policy should parse");
    let encoded = serde_json::to_vec(&policy).expect("session policy should serialize into claims");
    let decoded = Policy::parse_config(&encoded).expect("session policy should parse from claims");
    for action in ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"] {
        for bucket in ["bucket", "other-bucket"] {
            assert!(
                !policy_is_allowed(
                    decoded.clone(),
                    ArgsBuilder {
                        action: action.into(),
                        bucket: bucket.into(),
                        object: "data/object".into(),
                        ..Default::default()
                    }
                ),
                "deny-only session must not grant {action} on {bucket}"
            );
        }
    }
}

#[test_case("s3:GetObject", "bucket", "object" => true; "matching object action")]
#[test_case("s3:PutObject", "bucket", "object" => false; "ungranted action")]
#[test_case("s3:GetObject", "other-bucket", "object" => true; "other bucket")]
fn wildcard_resource_allow_preserves_action_scope(action: &str, bucket: &str, object: &str) -> bool {
    let policy = Policy::parse_config(
        br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":["*"]}]}"#,
    )
    .expect("wildcard allow policy should parse");
    policy_is_allowed(
        policy,
        ArgsBuilder {
            action: action.into(),
            bucket: bucket.into(),
            object: object.into(),
            ..Default::default()
        },
    )
}

#[test_case("s3:GetObject", "bucket", "object"; "object")]
#[test_case("s3:ListBucket", "bucket", ""; "bucket")]
#[test_case("s3:ListAllMyBuckets", "", ""; "all buckets")]
#[test_case("kms:Decrypt", "", "mykey"; "kms key")]
#[test_case("kms:Backup", "", ""; "kms backup")]
#[test_case("kms:Restore", "", ""; "kms restore")]
fn wildcard_resource_allow_deny_and_exclusion(action: &str, bucket: &str, object: &str) {
    let other_resource = if action.starts_with("kms:") {
        "arn:aws:kms:::key/other-key"
    } else {
        "arn:aws:s3:::other-bucket/*"
    };
    for (field, resource) in [
        ("Resource", serde_json::json!("*")),
        ("Resource", serde_json::json!(["*"])),
        ("Resource", serde_json::json!([other_resource, "*"])),
        ("NotResource", serde_json::json!("*")),
        ("NotResource", serde_json::json!([other_resource, "*"])),
    ] {
        let mut json = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": action, field: resource}]
        });
        let allow = Policy::parse_config(&serde_json::to_vec(&json).expect("test policy should serialize"))
            .expect("wildcard policy should parse");
        let args = ArgsBuilder {
            action: action.into(),
            bucket: bucket.into(),
            object: object.into(),
            ..Default::default()
        };
        assert_eq!(policy_is_allowed(allow, args), field == "Resource", "{action} with {field}");

        json["Statement"][0]["Effect"] = serde_json::json!("Deny");
        json["Statement"]
            .as_array_mut()
            .expect("statements are an array")
            .push(serde_json::json!({
                "Effect": "Allow", "Action": action, "Resource": "*"
            }));
        let deny = Policy::parse_config(&serde_json::to_vec(&json).expect("test policy should serialize"))
            .expect("wildcard deny policy should parse");
        assert_eq!(
            policy_is_allowed(
                deny,
                ArgsBuilder {
                    action: action.into(),
                    bucket: bucket.into(),
                    object: object.into(),
                    ..Default::default()
                }
            ),
            field == "NotResource",
            "explicit deny must win unless {field} excludes the resource for {action}"
        );
    }
}

#[test_case(Some("allowed/data/") => true; "matching prefix")]
#[test_case(Some("other/data/") => false; "outside allowed prefix")]
#[test_case(None => false; "missing prefix")]
fn wildcard_resource_preserves_conditions(prefix: Option<&str>) -> bool {
    let policy = Policy::parse_config(
        br#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow","Action":"s3:ListBucket","Resource":"*",
            "Condition":{"StringLike":{"s3:prefix":"allowed/*"}}
        }]}"#,
    )
    .expect("conditional wildcard policy should parse");
    let mut args = ArgsBuilder {
        action: "s3:ListBucket".into(),
        bucket: "bucket".into(),
        ..Default::default()
    };
    if let Some(prefix) = prefix {
        args.conditions.insert("prefix".into(), vec![prefix.into()]);
    }
    policy_is_allowed(policy, args)
}

// Conditional write enforcement (`s3:if-none-match` / `s3:if-match`). Condition values
// carry request headers under their lowercase header name; a missing header leaves
// the key absent, which `Null: true` matches and `StringEquals` never satisfies.

fn header_conditions(headers: &[(&str, &str)]) -> HashMap<String, Vec<String>> {
    headers
        .iter()
        .map(|(name, value)| ((*name).to_string(), vec![(*value).to_string()]))
        .collect()
}

fn is_allowed_put_object(policy: &Policy, conditions: &HashMap<String, Vec<String>>) -> bool {
    let claims: HashMap<String, Value> = HashMap::new();
    let groups: Option<Vec<String>> = None;
    pollster::block_on(policy.is_allowed(&Args {
        account: "Q3AM3UQ867SPQQA43P2F",
        groups: &groups,
        action: rustfs_policy::policy::action::Action::S3Action(PutObjectAction),
        bucket: "mybucket",
        conditions,
        is_owner: false,
        object: "uploads/report.csv",
        claims: &claims,
        deny_only: false,
    }))
}

fn put_object_policy_with_condition(condition: &str) -> Policy {
    let json = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":["s3:PutObject"],"Resource":["arn:aws:s3:::mybucket/*"],"Condition":{condition}}}]}}"#
    );
    Policy::parse_config(json.as_bytes()).expect("policy with conditional write key should parse")
}

#[test_case(r#"{"StringEquals":{"s3:if-none-match":"*"}}"#, &[("if-none-match", "*")] => true ; "if_none_match_string_equals_header_present")]
#[test_case(r#"{"StringEquals":{"s3:if-none-match":"*"}}"#, &[] => false ; "if_none_match_string_equals_header_absent")]
#[test_case(r#"{"StringEquals":{"s3:if-none-match":"*"}}"#, &[("if-match", "\"etag\"")] => false ; "if_none_match_string_equals_other_header")]
#[test_case(r#"{"Null":{"s3:if-none-match":"false"}}"#, &[("if-none-match", "*")] => true ; "if_none_match_not_null_header_present")]
#[test_case(r#"{"Null":{"s3:if-none-match":"false"}}"#, &[] => false ; "if_none_match_not_null_header_absent")]
#[test_case(r#"{"Null":{"s3:if-match":"false"}}"#, &[("if-match", "\"etag\"")] => true ; "if_match_not_null_header_present")]
#[test_case(r#"{"Null":{"s3:if-match":"false"}}"#, &[] => false ; "if_match_not_null_header_absent")]
#[test_case(r#"{"StringEquals":{"s3:if-match":"\"etag\""}}"#, &[("if-match", "\"etag\"")] => true ; "if_match_string_equals_matching_value")]
#[test_case(r#"{"StringEquals":{"s3:if-match":"\"etag\""}}"#, &[("if-match", "\"other\"")] => false ; "if_match_string_equals_different_value")]
fn conditional_write_condition_keys(condition: &str, headers: &[(&str, &str)]) -> bool {
    let policy = put_object_policy_with_condition(condition);
    is_allowed_put_object(&policy, &header_conditions(headers))
}

/// Mirrors the AWS deny-based enforcement example: allow PutObject, but deny it
/// when the `If-Match` header is missing.
#[test_case(&[("if-match", "\"etag\"")] => true ; "header_present")]
#[test_case(&[] => false ; "header_absent")]
fn bucket_policy_denies_put_object_without_if_match(headers: &[(&str, &str)]) -> bool {
    let policy: BucketPolicy = serde_json::from_str(
        r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:PutObject"],
                    "Resource": ["arn:aws:s3:::mybucket/*"]
                },
                {
                    "Effect": "Deny",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:PutObject"],
                    "Resource": ["arn:aws:s3:::mybucket/*"],
                    "Condition": {"Null": {"s3:if-match": "true"}}
                }
            ]
        }"#,
    )
    .expect("bucket policy with conditional write key should parse");
    let conditions = header_conditions(headers);
    let groups: Option<Vec<String>> = None;
    pollster::block_on(policy.is_allowed(&BucketPolicyArgs {
        account: "Q3AM3UQ867SPQQA43P2F",
        groups: &groups,
        action: rustfs_policy::policy::action::Action::S3Action(PutObjectAction),
        bucket: "mybucket",
        conditions: &conditions,
        is_owner: false,
        object: "uploads/report.csv",
    }))
}
