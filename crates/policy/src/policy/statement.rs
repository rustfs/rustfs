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

use super::{
    ActionSet, Args, BucketPolicyArgs, Effect, Error as IamError, Functions, ID, Principal, ResourceSet, Validator,
    action::Action,
};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Statement {
    #[serde(rename = "Sid", default)]
    pub sid: ID,
    #[serde(rename = "Effect")]
    pub effect: Effect,
    #[serde(rename = "Action")]
    pub actions: ActionSet,
    #[serde(rename = "NotAction", default)]
    pub not_actions: ActionSet,
    #[serde(rename = "Resource", default)]
    pub resources: ResourceSet,
    #[serde(rename = "NotResource", default)]
    pub not_resources: ResourceSet,
    #[serde(rename = "Condition", default)]
    pub conditions: Functions,
}

impl Statement {
    fn is_kms(&self) -> bool {
        for act in self.actions.iter() {
            if matches!(act, Action::KmsAction(_)) {
                return true;
            }
        }

        false
    }

    fn is_admin(&self) -> bool {
        for act in self.actions.iter() {
            if matches!(act, Action::AdminAction(_)) {
                return true;
            }
        }

        false
    }

    fn is_sts(&self) -> bool {
        for act in self.actions.iter() {
            if matches!(act, Action::StsAction(_)) {
                return true;
            }
        }

        false
    }

    pub fn is_allowed(&self, args: &Args) -> bool {
        let check = 'c: {
            if (!self.actions.is_match(&args.action) && !self.actions.is_empty()) || self.not_actions.is_match(&args.action) {
                break 'c false;
            }

            let mut resource = String::from(args.bucket);
            if !args.object.is_empty() {
                if !args.object.starts_with('/') {
                    resource.push('/');
                }

                resource.push_str(args.object);
            } else {
                resource.push('/');
            }

            if self.is_kms() && (resource == "/" || self.resources.is_empty()) {
                break 'c self.conditions.evaluate(args.conditions);
            }

            if !self.resources.is_match(&resource, args.conditions) && !self.is_admin() && !self.is_sts() {
                break 'c false;
            }

            self.conditions.evaluate(args.conditions)
        };

        self.effect.is_allowed(check)
    }
}

impl Validator for Statement {
    type Error = Error;
    fn is_valid(&self) -> Result<()> {
        self.effect.is_valid()?;
        // check sid
        self.sid.is_valid()?;

        if self.actions.is_empty() && self.not_actions.is_empty() {
            return Err(IamError::NonAction.into());
        }

        if self.resources.is_empty() {
            return Err(IamError::NonResource.into());
        }

        self.actions.is_valid()?;
        self.not_actions.is_valid()?;
        self.resources.is_valid()?;

        Ok(())
    }
}

impl PartialEq for Statement {
    fn eq(&self, other: &Self) -> bool {
        self.effect == other.effect
            && self.actions == other.actions
            && self.not_actions == other.not_actions
            && self.resources == other.resources
            && self.conditions == other.conditions
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
#[serde(rename_all = "PascalCase", default)]
pub struct BPStatement {
    #[serde(rename = "Sid", default)]
    pub sid: ID,
    #[serde(rename = "Effect")]
    pub effect: Effect,
    #[serde(rename = "Principal")]
    pub principal: Principal,
    #[serde(rename = "Action")]
    pub actions: ActionSet,
    #[serde(rename = "NotAction", default)]
    pub not_actions: ActionSet,
    #[serde(rename = "Resource", default)]
    pub resources: ResourceSet,
    #[serde(rename = "NotResource", default)]
    pub not_resources: ResourceSet,
    #[serde(rename = "Condition", default)]
    pub conditions: Functions,
}

impl BPStatement {
    pub fn is_allowed(&self, args: &BucketPolicyArgs) -> bool {
        let check = 'c: {
            if !self.principal.is_match(args.account) {
                break 'c false;
            }

            if (!self.actions.is_match(&args.action) && !self.actions.is_empty()) || self.not_actions.is_match(&args.action) {
                break 'c false;
            }

            let mut resource = String::from(args.bucket);
            if !args.object.is_empty() {
                if !args.object.starts_with('/') {
                    resource.push('/');
                }

                resource.push_str(args.object);
            } else {
                resource.push('/');
            }

            if !self.resources.is_empty() && !self.resources.is_match(&resource, args.conditions) {
                break 'c false;
            }

            if !self.not_resources.is_empty() && self.not_resources.is_match(&resource, args.conditions) {
                break 'c false;
            }

            self.conditions.evaluate(args.conditions)
        };

        self.effect.is_allowed(check)
    }
}

impl Validator for BPStatement {
    type Error = Error;
    fn is_valid(&self) -> Result<()> {
        self.effect.is_valid()?;
        // check sid
        self.sid.is_valid()?;

        self.principal.is_valid()?;

        if self.actions.is_empty() && self.not_actions.is_empty() {
            return Err(IamError::NonAction.into());
        }

        if self.resources.is_empty() {
            return Err(IamError::NonResource.into());
        }

        self.actions.is_valid()?;
        self.not_actions.is_valid()?;
        self.resources.is_valid()?;

        Ok(())
    }
}
