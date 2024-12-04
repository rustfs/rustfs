use serde::{Deserialize, Serialize};

use super::{action::Action, ActionSet, Args, Effect, Error, Functions, ResourceSet, Validator, ID};

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Statement {
    pub sid: ID,
    pub effect: Effect,
    pub actions: ActionSet,
    pub not_actions: ActionSet,
    pub resoures: ResourceSet,
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

            if self.is_kms() {
                if resource == "/" || self.resoures.is_empty() {
                    break 'c self.conditions.evaluate(&args.conditions);
                }
            }

            if !self.resoures.is_match(&resource, &args.conditions) && !self.is_admin() && !self.is_sts() {
                break 'c false;
            }

            self.conditions.evaluate(&args.conditions)
        };

        self.effect.is_allowed(check)
    }
}

impl Validator for Statement {
    fn is_valid(&self) -> Result<(), Error> {
        self.effect.is_valid()?;
        // check sid
        self.sid.is_valid()?;

        if self.actions.is_empty() || self.not_actions.is_empty() {
            return Err(Error::NonAction);
        }

        if self.resoures.is_empty() {
            return Err(Error::NonResource);
        }

        self.actions.is_valid()?;
        self.not_actions.is_valid()?;
        self.resoures.is_valid()?;

        Ok(())
    }
}
