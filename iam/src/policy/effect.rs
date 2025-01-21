use ecstore::error::{Error, Result};
use serde::{Deserialize, Serialize};
use strum::{EnumString, IntoStaticStr};

use crate::sys::Validator;

#[derive(Serialize, Clone, Deserialize, EnumString, IntoStaticStr, Default, Debug, PartialEq)]
#[serde(try_from = "&str", into = "&str")]
pub enum Effect {
    #[default]
    #[strum(serialize = "Allow")]
    Allow,
    #[strum(serialize = "Deny")]
    Deny,
}

impl Effect {
    pub fn is_allowed(&self, allowed: bool) -> bool {
        if matches!(self, Self::Allow) {
            return allowed;
        }

        !allowed
    }
}

impl Validator for Effect {
    type Error = Error;
    fn is_valid(&self) -> Result<()> {
        Ok(())
    }
}
