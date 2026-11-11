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

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

const MAX_NAME_LEN: usize = 32;

/// Why a name was refused.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NameError {
    #[error("name must be 1 to {MAX_NAME_LEN} bytes long")]
    Length,
    #[error("name must start with a lowercase letter and contain only lowercase letters, digits and '-'")]
    Characters,
}

fn validate(name: &str) -> Result<(), NameError> {
    if name.is_empty() || name.len() > MAX_NAME_LEN {
        return Err(NameError::Length);
    }
    let mut chars = name.chars();
    let starts_with_letter = chars.next().is_some_and(|c| c.is_ascii_lowercase());
    if !starts_with_letter || !chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-') {
        return Err(NameError::Characters);
    }
    Ok(())
}

macro_rules! validated_name {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(name: impl Into<String>) -> Result<Self, NameError> {
                let name = name.into();
                validate(&name)?;
                Ok(Self(name))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = NameError;

            fn try_from(name: String) -> Result<Self, Self::Error> {
                Self::new(name)
            }
        }

        impl From<$name> for String {
            fn from(name: $name) -> Self {
                name.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

validated_name!(
    /// Registry name of a bucket engine, such as `object` or `kv-cache`.
    EngineName
);

validated_name!(
    /// Name of an engine custom operation, such as `prefix-query`.
    OperationName
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_lowercase_kebab_names() {
        assert_eq!(EngineName::new("kv-cache").expect("valid engine name").as_str(), "kv-cache");
        assert!(OperationName::new("prefix-query2").is_ok());
    }

    #[test]
    fn rejects_malformed_names() {
        assert_eq!(EngineName::new(""), Err(NameError::Length));
        assert_eq!(EngineName::new("a".repeat(MAX_NAME_LEN + 1)), Err(NameError::Length));
        for bad in ["Kv-cache", "kv_cache", "-kv", "9kv", "kv cache", "kv/../x"] {
            assert_eq!(EngineName::new(bad), Err(NameError::Characters), "{bad:?} must be refused");
        }
    }

    #[test]
    fn deserialization_validates() {
        let parsed: Result<EngineName, _> = serde_json::from_str("\"Bad Name\"");
        assert!(parsed.is_err(), "a malformed name must not deserialize");
    }
}
