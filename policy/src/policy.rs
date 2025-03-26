pub mod action;
mod doc;
mod effect;
mod function;
mod id;
#[allow(clippy::module_inception)]
mod policy;
pub mod resource;
pub mod statement;
pub(crate) mod utils;

use std::collections::{HashMap, HashSet};

use action::Action;
pub use action::ActionSet;
pub use doc::PolicyDoc;

pub use effect::Effect;
pub use function::Functions;
pub use id::ID;
pub use policy::{default::DEFAULT_POLICIES, Policy};
pub use resource::ResourceSet;

use serde_json::Value;
pub use statement::Statement;

use common::error::Result;

pub const EMBEDDED_POLICY_TYPE: &str = "embedded-policy";
pub const INHERITED_POLICY_TYPE: &str = "inherited-policy";

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

fn get_policies_from_claims(claims: &HashMap<String, Value>, policy_claim_name: &str) -> (HashSet<String>, bool) {
    get_values_from_claims(claims, policy_claim_name)
}

pub fn iam_policy_claim_name_sa() -> String {
    "sa-policy".to_string()
}
