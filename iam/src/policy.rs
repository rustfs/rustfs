pub mod action;
mod doc;
mod effect;
mod function;
mod id;
mod policy;
mod resource;
mod statement;
pub(crate) mod utils;

use action::Action;
pub use action::ActionSet;
pub use doc::PolicyDoc;
pub use effect::Effect;
pub use function::Functions;
pub use id::ID;
pub use policy::{default::DEFAULT_POLICIES, Policy};
pub use resource::ResourceSet;
use serde::{Deserialize, Serialize};
use serde_json::{to_string, Value};
pub use statement::Statement;
use std::collections::HashMap;
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, Clone)]
pub struct MappedPolicy {
    pub version: i64,
    pub policies: String,
    pub update_at: OffsetDateTime,
}

impl MappedPolicy {
    pub fn new(policy: &str) -> Self {
        Self {
            version: 1,
            policies: policy.to_owned(),
            update_at: OffsetDateTime::now_utc(),
        }
    }

    pub fn to_slice(&self) -> Vec<String> {
        self.policies
            .split(",")
            .filter(|v| !v.trim().is_empty())
            .map(|v| v.to_string())
            .collect()
    }
}

pub struct GroupInfo {
    version: i64,
    status: String,
    members: Vec<String>,
    update_at: OffsetDateTime,
}

#[derive(thiserror::Error, Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum Error {
    #[error("invalid Version '{0}'")]
    InvalidVersion(String),

    #[error("invalid Effect '{0}'")]
    InvalidEffect(String),

    #[error("both 'Action' and 'NotAction' are empty")]
    NonAction,

    #[error("'Resource' is empty")]
    NonResource,

    #[error("invalid key name: '{0}'")]
    InvalidKeyName(String),

    #[error("invalid key: '{0}'")]
    InvalidKey(String),

    #[error("invalid action: '{0}'")]
    InvalidAction(String),

    #[error("invalid resource, type: '{0}', pattern: '{1}'")]
    InvalidResource(String, String),
}

/// DEFAULT_VERSION is the default version.
/// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_version.html
pub const DEFAULT_VERSION: &str = "2012-10-17";

/// check the data is Validator
pub trait Validator {
    fn is_valid(&self) -> Result<(), Error> {
        Ok(())
    }
}

pub enum UserType {
    Svc,
    Sts,
    Reg,
}

impl UserType {
    pub fn prefix(&self) -> &'static str {
        match self {
            UserType::Svc => "service-accounts/",
            UserType::Sts => "sts/",
            UserType::Reg => "users/",
        }
    }
}

pub struct Args<'a> {
    pub account: &'a str,
    pub groups: &'a [String],
    pub action: Action,
    pub bucket: &'a str,
    pub conditions: &'a HashMap<String, Vec<String>>,
    pub is_owner: bool,
    pub object: &'a str,
    pub claims: &'a HashMap<String, Value>,
    pub deny_only: bool,
}

impl<'a> Args<'a> {
    pub fn get_role_arn(&self) -> Option<&str> {
        self.claims.get("roleArn").and_then(|x| x.as_str())
    }
}
