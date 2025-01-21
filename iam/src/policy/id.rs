use ecstore::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

use crate::sys::Validator;

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct ID(pub String);

impl Validator for ID {
    type Error = Error;
    /// if id is a valid utf string, then it is valid.
    fn is_valid(&self) -> Result<()> {
        Ok(())
    }
}

impl<T: ToString> From<T> for ID {
    fn from(value: T) -> Self {
        Self(value.to_string())
    }
}

impl Deref for ID {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
