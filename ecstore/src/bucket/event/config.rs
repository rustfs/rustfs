use serde::{Deserialize, Serialize};

use super::name::Name;

// 定义common结构体
#[derive(Serialize, Deserialize, Debug)]
struct Common {
    pub id: String,
    pub filter: S3Key,
    pub events: Vec<Name>,
}

// 定义Queue结构体
#[derive(Serialize, Deserialize, Debug)]
struct Queue {
    pub common: Common,
    pub arn: ARN,
}

// 定义ARN结构体
#[derive(Serialize, Deserialize, Debug)]
pub struct ARN {
    pub target_id: TargetID,
    pub region: String,
}

// 定义TargetID结构体
#[derive(Serialize, Deserialize, Debug)]
pub struct TargetID {
    pub id: String,
    pub name: String,
}

// 定义FilterRule结构体
#[derive(Serialize, Deserialize, Debug)]
pub struct FilterRule {
    pub name: String,
    pub value: String,
}

// 定义FilterRuleList结构体
#[derive(Serialize, Deserialize, Debug)]
pub struct FilterRuleList {
    pub rules: Vec<FilterRule>,
}

// 定义S3Key结构体
#[derive(Serialize, Deserialize, Debug)]
pub struct S3Key {
    pub rule_list: FilterRuleList,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Lambda {
    arn: String,
}

// 定义Topic结构体
#[derive(Serialize, Deserialize, Debug)]
pub struct Topic {
    arn: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    queue_list: Vec<Queue>,
    lambda_list: Vec<Lambda>,
    topic_list: Vec<Topic>,
}
