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

use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use super::func::InnerFunc;

pub type BinaryFunc = InnerFunc<BinaryFuncValue>;

/// Policy value for the AWS IAM `BinaryEquals` condition.
///
/// Policies store the value as a base64-encoded string. During deserialization
/// the value is validated and the raw bytes are cached, so evaluation is a
/// plain byte comparison and malformed policies are rejected at parse time.
#[derive(Clone, Debug)]
pub struct BinaryFuncValue {
    /// Original base64 form, preserved for serialization round-trips.
    encoded: String,
    /// Decoded bytes used for comparison during `evaluate`.
    decoded: Vec<u8>,
}

impl BinaryFuncValue {
    /// Construct from a base64-encoded string, validating the encoding.
    pub fn new(encoded: impl Into<String>) -> Result<Self, base64_simd::Error> {
        let encoded = encoded.into();
        let decoded = base64_simd::STANDARD.decode_to_vec(encoded.as_bytes())?;
        Ok(Self { encoded, decoded })
    }
}

// Equality is defined over decoded bytes so that semantically equal values
// compare equal regardless of incidental base64 formatting differences.
impl PartialEq for BinaryFuncValue {
    fn eq(&self, other: &Self) -> bool {
        self.decoded == other.decoded
    }
}

impl Eq for BinaryFuncValue {}

impl Serialize for BinaryFuncValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.encoded)
    }
}

impl<'de> Deserialize<'de> for BinaryFuncValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        Self::new(encoded).map_err(|e| de::Error::custom(format!("invalid base64 for BinaryEquals: {e}")))
    }
}

impl BinaryFunc {
    /// Evaluate an AWS IAM `BinaryEquals` condition.
    ///
    /// The policy value's decoded bytes are compared for equality against the
    /// request context values (treated as raw bytes). All key/value pairs in
    /// the function must match (logical AND); for a given key, any request
    /// value that matches satisfies that pair. A missing request key causes
    /// the condition to evaluate to false.
    pub fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        for inner in self.0.iter() {
            let Some(rvalues) = values.get(inner.key.name().as_str()) else {
                return false;
            };

            let expected = inner.values.decoded.as_slice();
            if !rvalues.iter().any(|v| v.as_bytes() == expected) {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::{BinaryFunc, BinaryFuncValue};
    use crate::policy::function::func::FuncKeyValue;
    use crate::policy::function::{
        key::Key,
        key_name::AwsKeyName::*,
        key_name::KeyName::{self, *},
    };
    use std::collections::HashMap;

    fn new_func(name: KeyName, variable: Option<String>, value: &str) -> BinaryFunc {
        BinaryFunc {
            0: vec![FuncKeyValue {
                key: Key { name, variable },
                values: BinaryFuncValue::new(value).expect("valid base64 in test"),
            }],
        }
    }

    #[test]
    fn evaluate_matches_decoded_bytes() {
        // base64("hello") = "aGVsbG8="
        let f = new_func(Aws(AWSUsername), None, "aGVsbG8=");
        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec!["hello".to_string()]);
        assert!(f.evaluate(&ctx));
    }

    #[test]
    fn evaluate_rejects_non_matching_value() {
        let f = new_func(Aws(AWSUsername), None, "aGVsbG8="); // "hello"
        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec!["world".to_string()]);
        assert!(!f.evaluate(&ctx));
    }

    #[test]
    fn evaluate_matches_any_request_value() {
        let f = new_func(Aws(AWSUsername), None, "aGVsbG8="); // "hello"
        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec!["world".to_string(), "hello".to_string()]);
        assert!(f.evaluate(&ctx));
    }

    #[test]
    fn evaluate_missing_key_is_false() {
        let f = new_func(Aws(AWSUsername), None, "aGVsbG8=");
        let ctx = HashMap::new();
        assert!(!f.evaluate(&ctx));
    }

    #[test]
    fn evaluate_empty_request_values_is_false() {
        let f = new_func(Aws(AWSUsername), None, "aGVsbG8=");
        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec![]);
        assert!(!f.evaluate(&ctx));
    }

    #[test]
    fn evaluate_matches_multibyte_utf8() {
        // base64("café") = "Y2Fmw6k=" — exercises multi-byte UTF-8 round trip.
        let f = new_func(Aws(AWSUsername), None, "Y2Fmw6k=");
        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec!["café".to_string()]);
        assert!(f.evaluate(&ctx));
    }

    #[test]
    fn evaluate_all_key_values_must_match() {
        // Two key/value pairs — both must be satisfied.
        let f = BinaryFunc {
            0: vec![
                FuncKeyValue {
                    key: Key {
                        name: Aws(AWSUsername),
                        variable: None,
                    },
                    values: BinaryFuncValue::new("aGVsbG8=").unwrap(), // "hello"
                },
                FuncKeyValue {
                    key: Key {
                        name: Aws(AWSPrincipalType),
                        variable: None,
                    },
                    values: BinaryFuncValue::new("d29ybGQ=").unwrap(), // "world"
                },
            ],
        };

        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec!["hello".to_string()]);
        ctx.insert("principaltype".to_string(), vec!["world".to_string()]);
        assert!(f.evaluate(&ctx));

        // Second key missing — must fail.
        let mut ctx2 = HashMap::new();
        ctx2.insert("username".to_string(), vec!["hello".to_string()]);
        assert!(!f.evaluate(&ctx2));
    }

    #[test]
    fn deserializes_from_policy_json() {
        let json = r#"{"aws:username": "aGVsbG8="}"#;
        let f: BinaryFunc = serde_json::from_str(json).unwrap();
        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec!["hello".to_string()]);
        assert!(f.evaluate(&ctx));
    }

    #[test]
    fn deserialize_rejects_invalid_base64_at_parse_time() {
        // Malformed policies must be rejected eagerly, not silently fail at eval.
        let json = r#"{"aws:username": "!!!not-base64!!!"}"#;
        let err = serde_json::from_str::<BinaryFunc>(json).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid base64"), "unexpected error message: {msg}");
    }

    #[test]
    fn serialize_round_trip_preserves_encoded_form() {
        let json = r#"{"aws:username":"aGVsbG8="}"#;
        let f: BinaryFunc = serde_json::from_str(json).unwrap();
        let out = serde_json::to_string(&f).unwrap();
        assert_eq!(out, json);
    }
}
