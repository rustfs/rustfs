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
use std::collections::HashSet;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Principal {
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
            PrincipalFormat::Wildcard(s) if s == "*" => {
                Principal { aws: vec!["*".to_string()].into_iter().collect() }
            }
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
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let format = PrincipalFormat::deserialize(deserializer)?;
        Ok(format.into())
    }
}

impl serde::Serialize for Principal {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Principal", 1)?;
        state.serialize_field("AWS", &self.aws)?;
        state.end()
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
    fn is_valid(&self) -> std::result::Result<(), Error> {
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

    #[test]
    fn test_principal_parsing() {
        println!("=== Testing Principal Parsing Fix ===");

        // Test case 1: "*" (should now work!)
        let principal_json1 = r#""*""#;
        println!("Testing: {}", principal_json1);
        match serde_json::from_str::<Principal>(principal_json1) {
            Ok(p) => {
                println!("✓ Success: {:?}", p);
                assert!(p.aws.contains("*"));
            }
            Err(e) => {
                println!("✗ Error: {}", e);
                panic!("Should support '*' format");
            }
        }

        // Test case 2: {"AWS": "*"} (should now work!)
        let principal_json2 = r#"{"AWS": "*"}"#;
        println!("\nTesting: {}", principal_json2);
        match serde_json::from_str::<Principal>(principal_json2) {
            Ok(p) => {
                println!("✓ Success: {:?}", p);
                assert!(p.aws.contains("*"));
            }
            Err(e) => {
                println!("✗ Error: {}", e);
                panic!("Should support {{'AWS': '*'}} format");
            }
        }

        // Test case 3: {"AWS": ["*"]} (should still work)
        let principal_json3 = r#"{"AWS": ["*"]}"#;
        println!("\nTesting: {}", principal_json3);
        match serde_json::from_str::<Principal>(principal_json3) {
            Ok(p) => {
                println!("✓ Success: {:?}", p);
                assert!(p.aws.contains("*"));
            }
            Err(e) => {
                println!("✗ Error: {}", e);
                panic!("Should still support {{'AWS': ['*']}} format");
            }
        }

        // Test matching functionality
        let principal = serde_json::from_str::<Principal>(r#""*""#).unwrap();
        assert!(principal.is_match("any-user"));
        assert!(principal.is_match("*"));

        println!("\n=== All tests passed! ===");
    }
}
