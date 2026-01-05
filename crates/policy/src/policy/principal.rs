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

/// Principal that serializes AWS field as single string when containing only "*",
/// or as an array otherwise (matching AWS S3 API format).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Principal {
    aws: HashSet<String>,
}

impl Serialize for Principal {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(1))?;

        // If single element, serialize as string; otherwise as array
        if self.aws.len() == 1 {
            if let Some(val) = self.aws.iter().next() {
                map.serialize_entry("AWS", val)?;
            }
        } else {
            map.serialize_entry("AWS", &self.aws)?;
        }

        map.end()
    }
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

impl<'de> serde::Deserialize<'de> for Principal {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let format = PrincipalFormat::deserialize(deserializer)?;
        match format {
            PrincipalFormat::Wildcard(s) => {
                if s == "*" {
                    Ok(Principal {
                        aws: HashSet::from(["*".to_string()]),
                    })
                } else {
                    Err(serde::de::Error::custom(format!(
                        "invalid wildcard principal value: expected \"*\", got \"{}\"",
                        s
                    )))
                }
            }
            PrincipalFormat::AwsObject(obj) => {
                let aws = match obj.aws {
                    AwsValues::Single(s) => HashSet::from([s]),
                    AwsValues::Multiple(set) => set,
                };
                Ok(Principal { aws })
            }
        }
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
    use test_case::test_case;

    #[test_case(r#""*""#, true ; "wildcard_string")]
    #[test_case(r#"{"AWS": "*"}"#, true ; "aws_object_single_string")]
    #[test_case(r#"{"AWS": ["*"]}"#, true ; "aws_object_array")]
    #[test_case(r#""invalid""#, false ; "invalid_string")]
    #[test_case(r#""""#, false ; "empty_string")]
    #[test_case(r#"{"Other": "*"}"#, false ; "wrong_field")]
    #[test_case(r#"{}"#, false ; "empty_object")]
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

    #[test]
    fn test_principal_serialize_single_element() {
        // Single element should serialize as string (AWS format)
        let principal = Principal {
            aws: HashSet::from(["*".to_string()]),
        };

        let json = serde_json::to_string(&principal).expect("Should serialize");
        assert_eq!(json, r#"{"AWS":"*"}"#);
    }

    #[test]
    fn test_principal_serialize_multiple_elements() {
        // Multiple elements should serialize as array
        let principal = Principal {
            aws: HashSet::from(["*".to_string(), "arn:aws:iam::123456789012:root".to_string()]),
        };

        let json = serde_json::to_string(&principal).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
        let aws_value = parsed.get("AWS").expect("Should have AWS field");
        assert!(aws_value.is_array());
        let arr = aws_value.as_array().expect("Should be array");
        assert_eq!(arr.len(), 2);
    }
}
