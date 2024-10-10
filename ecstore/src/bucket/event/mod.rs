mod name;

use crate::error::Result;
use name::Name;
use rmp_serde::Serializer as rmpSerializer;
use serde::{Deserialize, Serialize};

// 定义common结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
struct Common {
    pub id: String,
    pub filter: S3Key,
    pub events: Vec<Name>,
}

// 定义Queue结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
struct Queue {
    pub common: Common,
    pub arn: ARN,
}

// 定义ARN结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct ARN {
    pub target_id: TargetID,
    pub region: String,
}

// 定义TargetID结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct TargetID {
    pub id: String,
    pub name: String,
}

// 定义FilterRule结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct FilterRule {
    pub name: String,
    pub value: String,
}

// 定义FilterRuleList结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct FilterRuleList {
    pub rules: Vec<FilterRule>,
}

// 定义S3Key结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct S3Key {
    pub rule_list: FilterRuleList,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Lambda {
    arn: String,
}

// 定义Topic结构体
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Topic {
    arn: String,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct Config {
    queue_list: Vec<Queue>,
    lambda_list: Vec<Lambda>,
    topic_list: Vec<Topic>,
}

impl Config {
    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: Config = rmp_serde::from_slice(buf)?;
        Ok(t)
    }
}
