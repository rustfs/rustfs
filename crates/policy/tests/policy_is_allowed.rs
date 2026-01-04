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
