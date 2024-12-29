use iam::policy::action::Action;
use iam::policy::action::ActionSet;
use iam::policy::action::S3Action::*;
use iam::policy::resource::ResourceSet;
use iam::policy::Effect::*;
use iam::policy::{Args, Policy, Statement, DEFAULT_VERSION};
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
            Statement{
                    effect: Allow,
                    actions: ActionSet(vec![Action::S3Action(PutObjectAction), Action::S3Action(GetBucketLocationAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(PutObjectAction), Action::S3Action(GetBucketLocationAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(PutObjectAction), Action::S3Action(GetBucketLocationAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(PutObjectAction), Action::S3Action(GetBucketLocationAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(PutObjectAction), Action::S3Action(GetBucketLocationAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(PutObjectAction), Action::S3Action(GetBucketLocationAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
                    actions: ActionSet(vec![Action::S3Action(GetObjectAction), Action::S3Action(PutObjectAction)].into_iter().collect()),
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
fn policy_is_allowed(policy: Policy, args: ArgsBuilder) -> bool {
    policy.is_allowed(&Args {
        account: &args.account,
        groups: &args.groups,
        action: args.action.as_str().try_into().unwrap(),
        bucket: &args.bucket,
        conditions: &args.conditions,
        is_owner: args.is_owner,
        object: &args.object,
        claims: &args.claims,
        deny_only: args.deny_only,
    })
}
