use std::collections::HashMap;

use super::{
    action::{Action, ActionSet},
    condition::function::Functions,
    effect::Effect,
    principal::Principal,
    resource::ResourceSet,
};

#[derive(Debug, Default)]
pub struct BucketPolicyArgs {
    account_name: String,
    groups: Vec<String>,
    action: Action,
    bucket_name: String,
    condition_values: HashMap<String, Vec<String>>,
    is_owner: bool,
    object_name: String,
}

#[derive(Debug, Default)]
pub struct BPStatement {
    sid: String,
    effect: Effect,
    principal: Principal,
    actions: ActionSet,

    not_actions: Option<ActionSet>,
    resources: ResourceSet,
    conditions: Option<Functions>,
}

#[derive(Debug, Default)]
pub struct BucketPolicy {
    pub id: String,
    pub version: String,
    pub statements: Vec<BPStatement>,
}
