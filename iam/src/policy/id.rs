use std::ops::Deref;

use serde::{Deserialize, Serialize};

use super::{Error, Validator};

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct ID(pub String);

impl Validator for ID {
    /// if id is a valid utf string, then it is valid.
    fn is_valid(&self) -> Result<(), Error> {
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
