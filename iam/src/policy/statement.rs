use crate::sys::{Args, Validator};

use super::{action::Action, ActionSet, Effect, Error as IamError, Functions, ResourceSet, ID};
use ecstore::error::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Statement {
    pub sid: ID,
    pub effect: Effect,
    pub actions: ActionSet,
    pub not_actions: ActionSet,
    pub resources: ResourceSet,
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

        if self.actions.is_empty() || self.not_actions.is_empty() {
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
