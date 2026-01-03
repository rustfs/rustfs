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

use super::{Validator, utils::wildcard};
use crate::error::Error;
use serde::Serialize;
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "PascalCase", default)]
pub struct Principal {
    #[serde(rename = "AWS")]
    aws: HashSet<String>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum PrincipalFormat {
    Wildcard(String),
    AwsObject(PrincipalAwsObject),
}

#[derive(serde::Deserialize)]
struct PrincipalAwsObject {
    #[serde(rename = "AWS")]
    aws: AwsValues,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum AwsValues {
    Single(String),
    Multiple(HashSet<String>),
}

impl From<PrincipalFormat> for Principal {
    fn from(format: PrincipalFormat) -> Self {
        match format {
            PrincipalFormat::Wildcard(s) if s == "*" => Principal {
                aws: vec!["*".to_string()].into_iter().collect(),
            },
            PrincipalFormat::AwsObject(obj) => {
                let aws = match obj.aws {
                    AwsValues::Single(s) => vec![s].into_iter().collect(),
                    AwsValues::Multiple(set) => set,
                };
                Principal { aws }
            }
            _ => Principal::default(),
        }
    }
}

impl<'de> serde::Deserialize<'de> for Principal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let format = PrincipalFormat::deserialize(deserializer)?;
        Ok(format.into())
    }
}

impl Principal {
    pub fn is_match(&self, parincipal: &str) -> bool {
        for pattern in self.aws.iter() {
            if wildcard::is_simple_match(pattern, parincipal) {
                return true;
            }
        }
        false
    }
}

impl Validator for Principal {
    type Error = Error;
    fn is_valid(&self) -> Result<(), Error> {
        if self.aws.is_empty() {
            return Err(Error::other("Principal is empty"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json;

    #[test_case::test_case(r#""*""#, true ; "wildcard_string")]
    #[test_case::test_case(r#"{"AWS": "*"}"#, true ; "aws_object_single_string")]
    #[test_case::test_case(r#"{"AWS": ["*"]}"#, true ; "aws_object_array")]
    fn test_principal_parsing(json: &str, should_succeed: bool) {
        let result = match serde_json::from_str::<Principal>(json) {
            Ok(principal) => {
                assert!(principal.aws.contains("*"));
                should_succeed
            }
            Err(_) => !should_succeed,
        };
        assert!(result);
    }
}
