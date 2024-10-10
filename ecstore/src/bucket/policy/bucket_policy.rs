use crate::error::Result;
use rmp_serde::Serializer as rmpSerializer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{
    action::{Action, ActionSet},
    condition::function::Functions,
    effect::Effect,
    principal::Principal,
    resource::ResourceSet,
};

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct BucketPolicyArgs {
    account_name: String,
    groups: Vec<String>,
    action: Action,
    bucket_name: String,
    condition_values: HashMap<String, Vec<String>>,
    is_owner: bool,
    object_name: String,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct BPStatement {
    sid: String,
    effect: Effect,
    principal: Principal,
    actions: ActionSet,

    not_actions: Option<ActionSet>,
    resources: ResourceSet,
    conditions: Option<Functions>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct BucketPolicy {
    pub id: String,
    pub version: String,
    pub statements: Vec<BPStatement>,
}

impl BucketPolicy {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: BucketPolicy = rmp_serde::from_slice(buf)?;
        Ok(t)
    }
}
