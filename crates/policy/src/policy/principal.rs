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
/// Also supports Service principals (e.g., "logging.s3.amazonaws.com").
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Principal {
    aws: HashSet<String>,
    service: HashSet<String>,
}

impl Serialize for Principal {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let field_count = usize::from(!self.aws.is_empty()) + usize::from(!self.service.is_empty());
        let mut map = serializer.serialize_map(Some(field_count))?;

        if !self.aws.is_empty() {
            if self.aws.len() == 1 {
                if let Some(val) = self.aws.iter().next() {
                    map.serialize_entry("AWS", val)?;
                }
            } else {
                map.serialize_entry("AWS", &self.aws)?;
            }
        }

        if !self.service.is_empty() {
            if self.service.len() == 1 {
                if let Some(val) = self.service.iter().next() {
                    map.serialize_entry("Service", val)?;
                }
            } else {
                map.serialize_entry("Service", &self.service)?;
            }
        }

        map.end()
    }
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum PrincipalFormat {
    Wildcard(String),
    Object(PrincipalObject),
}

#[derive(serde::Deserialize)]
struct PrincipalObject {
    #[serde(rename = "AWS", default)]
    aws: Option<PrincipalValues>,
    #[serde(rename = "Service", default)]
    service: Option<PrincipalValues>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum PrincipalValues {
    Single(String),
    Multiple(HashSet<String>),
}

impl PrincipalValues {
    fn into_set(self) -> HashSet<String> {
        match self {
            PrincipalValues::Single(s) => HashSet::from([s]),
            PrincipalValues::Multiple(set) => set,
        }
    }
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
                        service: HashSet::new(),
                    })
                } else {
                    Err(serde::de::Error::custom(format!(
                        "invalid wildcard principal value: expected \"*\", got \"{}\"",
                        s
                    )))
                }
            }
            PrincipalFormat::Object(obj) => {
                let aws = obj.aws.map(PrincipalValues::into_set).unwrap_or_default();
                let service = obj.service.map(PrincipalValues::into_set).unwrap_or_default();
                if aws.is_empty() && service.is_empty() {
                    return Err(serde::de::Error::custom("Principal must have at least one of AWS or Service"));
                }
                Ok(Principal { aws, service })
            }
        }
    }
}

impl Principal {
    pub fn is_match(&self, principal: &str) -> bool {
        for pattern in self.aws.iter() {
            if wildcard::is_simple_match(pattern, principal) {
                return true;
            }
        }
        // Service principals (e.g., logging.s3.amazonaws.com) allow internal
        // AWS services. Treat them as non-matching for user requests — they
        // only apply to service-initiated actions.
        false
    }
}

impl Validator for Principal {
    type Error = Error;
    fn is_valid(&self) -> Result<(), Error> {
        if self.aws.is_empty() && self.service.is_empty() {
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
    #[test_case(r#"{"Service": "logging.s3.amazonaws.com"}"#, true ; "service_single")]
    #[test_case(r#"{"Service": ["logging.s3.amazonaws.com"]}"#, true ; "service_array")]
    #[test_case(r#"{"AWS": "*", "Service": "logging.s3.amazonaws.com"}"#, true ; "aws_and_service")]
    #[test_case(r#""invalid""#, false ; "invalid_string")]
    #[test_case(r#""""#, false ; "empty_string")]
    #[test_case(r#"{}"#, false ; "empty_object")]
    fn test_principal_parsing(json: &str, should_succeed: bool) {
        let result = serde_json::from_str::<Principal>(json);
        assert_eq!(result.is_ok(), should_succeed, "input: {json}, result: {result:?}");
    }

    #[test]
    fn test_principal_serialize_single_element() {
        let principal = Principal {
            aws: HashSet::from(["*".to_string()]),
            service: HashSet::new(),
        };

        let json = serde_json::to_string(&principal).expect("Should serialize");
        assert_eq!(json, r#"{"AWS":"*"}"#);
    }

    #[test]
    fn test_principal_serialize_multiple_elements() {
        let principal = Principal {
            aws: HashSet::from(["*".to_string(), "arn:aws:iam::123456789012:root".to_string()]),
            service: HashSet::new(),
        };

        let json = serde_json::to_string(&principal).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
        let aws_value = parsed.get("AWS").expect("Should have AWS field");
        assert!(aws_value.is_array());
        assert_eq!(aws_value.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_principal_serialize_service() {
        let principal = Principal {
            aws: HashSet::new(),
            service: HashSet::from(["logging.s3.amazonaws.com".to_string()]),
        };

        let json = serde_json::to_string(&principal).expect("Should serialize");
        assert_eq!(json, r#"{"Service":"logging.s3.amazonaws.com"}"#);
    }

    #[test]
    fn test_principal_roundtrip_service() {
        let json = r#"{"Service": "logging.s3.amazonaws.com"}"#;
        let principal: Principal = serde_json::from_str(json).expect("Should parse");
        assert!(principal.service.contains("logging.s3.amazonaws.com"));
        assert!(principal.aws.is_empty());

        let serialized = serde_json::to_string(&principal).expect("Should serialize");
        let reparsed: Principal = serde_json::from_str(&serialized).expect("Should re-parse");
        assert_eq!(principal, reparsed);
    }
}
