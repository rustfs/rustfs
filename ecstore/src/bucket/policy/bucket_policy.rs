use common::error::{Error, Result};
// use rmp_serde::Serializer as rmpSerializer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{
    action::{Action, ActionSet, IAMActionConditionKeyMap},
    condition::function::Functions,
    effect::Effect,
    principal::Principal,
    resource::ResourceSet,
};

const DEFAULT_VERSION: &str = "2012-10-17";

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct BucketPolicyArgs {
    pub account_name: String,
    pub groups: Vec<String>,
    pub action: Action,
    pub bucket_name: String,
    pub condition_values: HashMap<String, Vec<String>>,
    pub is_owner: bool,
    pub object_name: String,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone, PartialEq, Eq)]
#[serde(rename_all = "PascalCase", default)]
pub struct BPStatement {
    #[serde(rename = "Sid")]
    pub sid: String,
    #[serde(rename = "Effect")]
    pub effect: Effect,
    #[serde(rename = "Principal")]
    pub principal: Principal,
    #[serde(rename = "Action")]
    pub actions: ActionSet,
    #[serde(rename = "NotAction", skip_serializing_if = "ActionSet::is_empty")]
    pub not_actions: ActionSet,
    #[serde(rename = "Resource", skip_serializing_if = "ResourceSet::is_empty")]
    pub resources: ResourceSet,
    #[serde(rename = "Condition", skip_serializing_if = "Functions::is_empty")]
    pub conditions: Functions,
}

impl BPStatement {
    // pub fn equals(&self, other: &BPStatement) -> bool {
    //     if self.effect != other.effect {
    //         return false;
    //     }

    //     if !self.principal.equals(other.principal) {
    //         return false;
    //     }

    //     if !self.actions.equals(other.actions) {
    //         return false;
    //     }
    //     if !self.not_actions.equals(other.not_actions) {
    //         return false;
    //     }
    //     if !self.resources.equals(other.resources) {
    //         return false;
    //     }
    //     if !self.conditions.equals(other.conditions) {
    //         return false;
    //     }

    //     true
    // }
    pub fn validate(&self, bucket: &str) -> Result<()> {
        self.is_valid()?;
        self.resources.validate_bucket(bucket)
    }
    pub fn is_valid(&self) -> Result<()> {
        if !self.effect.is_valid() {
            return Err(Error::msg(format!("invalid Effect {:?}", self.effect)));
        }

        if !self.principal.is_valid() {
            return Err(Error::msg(format!("invalid Principal {:?}", self.principal)));
        }

        if self.actions.is_empty() && self.not_actions.is_empty() {
            return Err(Error::msg("Action must not be empty"));
        }

        if self.resources.as_ref().is_empty() {
            return Err(Error::msg("Resource must not be empty"));
        }

        for act in self.actions.as_ref() {
            if act.is_object_action() {
                if !self.resources.object_resource_exists() {
                    return Err(Error::msg(format!(
                        "unsupported object Resource found {:?} for action {:?}",
                        self.resources, act
                    )));
                }
            } else if !self.resources.bucket_resource_exists() {
                return Err(Error::msg(format!(
                    "unsupported bucket Resource found {:?} for action {:?}",
                    self.resources, act
                )));
            }

            let key_diff = self.conditions.keys().difference(&IAMActionConditionKeyMap.lookup(act));
            if !key_diff.is_empty() {
                return Err(Error::msg(format!(
                    "unsupported condition keys '{:?}' used for action '{:?}'",
                    key_diff, act
                )));
            }
        }
        Ok(())
    }
    fn is_allowed(&self, args: &BucketPolicyArgs) -> bool {
        let check = || -> bool {
            if !self.principal.is_match(&args.account_name) {
                return false;
            }

            if (!self.actions.is_match(&args.action) && !self.actions.is_empty()) || self.not_actions.is_match(&args.action) {
                return false;
            }

            let mut resource = args.bucket_name.clone();
            if !args.object_name.is_empty() {
                if !args.object_name.starts_with('/') {
                    resource.push('/');
                }

                resource.push_str(&args.object_name);
            }

            if !self.resources.is_match(&resource, &args.condition_values) {
                return false;
            }

            self.conditions.evaluate(&args.condition_values)
        };

        self.effect.is_allowed(check())
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
// #[serde(rename_all = "PascalCase", default)]
pub struct BucketPolicy {
    #[serde(rename = "ID", default)]
    pub id: String,
    #[serde(rename = "Version")]
    pub version: String,
    #[serde(rename = "Statement")]
    pub statements: Vec<BPStatement>,
}

impl BucketPolicy {
    pub fn is_allowed(&self, args: &BucketPolicyArgs) -> bool {
        for statement in self.statements.iter() {
            if statement.effect == Effect::Deny && !statement.is_allowed(args) {
                return false;
            }
        }

        if args.is_owner {
            return true;
        }

        for statement in self.statements.iter() {
            if statement.effect == Effect::Allow && statement.is_allowed(args) {
                return true;
            }
        }

        false
    }

    pub fn validate(&self, bucket: &str) -> Result<()> {
        self.is_valid()?;
        for statement in self.statements.iter() {
            statement.validate(bucket)?;
        }
        Ok(())
    }

    pub fn is_valid(&self) -> Result<()> {
        if self.version.as_str() != DEFAULT_VERSION && self.version.is_empty() {
            return Err(Error::msg(format!("invalid version {}", self.version)));
        }

        for statement in self.statements.iter() {
            statement.is_valid()?;
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    pub fn marshal_msg(&self) -> Result<String> {
        let buf = serde_json::to_string(self)?;

        Ok(buf)

        // let mut buf = Vec::new();
        // self.serialize(&mut rmpSerializer::new(&mut buf).with_struct_map())?;

        // Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let mut p = serde_json::from_slice::<BucketPolicy>(buf)?;
        p.drop_duplicate_statements();
        Ok(p)

        // let t: BucketPolicy = rmp_serde::from_slice(buf)?;
        // Ok(t)
    }

    fn drop_duplicate_statements(&mut self) {
        let mut dups = HashMap::new();

        for v in self.statements.iter() {
            if let Ok(data) = serde_json::to_string(self) {
                dups.insert(data, v);
            }
        }

        let mut news = Vec::new();

        for (_, v) in dups {
            news.push(v.clone());
        }

        self.statements = news;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_policy() {
        let json = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Action\":[\"s3:GetBucketLocation\",\"s3:ListBucket\",\"s3:ListBucketMultipartUploads\"],\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"*\"]},\"Resource\":[\"arn:aws:s3:::dada\"],\"Sid\":\"\"},{\"Action\":[\"s3:AbortMultipartUpload\",\"s3:DeleteObject\",\"s3:GetObject\",\"s3:ListMultipartUploadParts\",\"s3:PutObject\"],\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"*\"]},\"Resource\":[\"arn:aws:s3:::dada/*\"],\"Sid\":\"sdf\"}]}";

        let a = BucketPolicy::unmarshal(json.to_string().as_bytes()).unwrap();

        println!("{:?}", a);

        let j = a.marshal_msg();

        println!("{:?}", j);

        println!("{:?}", json);
    }
}
