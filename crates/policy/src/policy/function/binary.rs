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

#[derive(thiserror::Error, Clone, Debug, Eq, PartialEq)]
pub enum BinaryFuncValueError {
    #[error("invalid base64 for BinaryEquals")]
    InvalidBase64,
}

/// Policy value for the AWS IAM `BinaryEquals` condition.
///
/// Policies store the value as a base64-encoded string or array of strings.
/// During deserialization the values are validated and the raw bytes are
/// cached, so evaluation is a plain byte comparison and malformed policies
/// are rejected at parse time.
#[derive(Clone, Debug)]
pub struct BinaryFuncValue {
    /// Original base64 forms, preserved for serialization round-trips.
    encoded: Vec<String>,
    /// Decoded bytes used for comparison during `evaluate`.
    decoded: Vec<Vec<u8>>,
}

impl BinaryFuncValue {
    /// Construct from a base64-encoded string, validating the encoding.
    pub fn new(encoded: impl Into<String>) -> Result<Self, BinaryFuncValueError> {
        Self::from_encoded_values(vec![encoded.into()])
    }

    fn from_encoded_values(encoded: Vec<String>) -> Result<Self, BinaryFuncValueError> {
        let decoded = encoded
            .iter()
            .map(|value| {
                base64_simd::STANDARD
                    .decode_to_vec(value.as_bytes())
                    .map_err(|_| BinaryFuncValueError::InvalidBase64)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { encoded, decoded })
    }
}

impl TryFrom<String> for BinaryFuncValue {
    type Error = BinaryFuncValueError;

    fn try_from(encoded: String) -> Result<Self, Self::Error> {
        Self::new(encoded)
    }
}

impl TryFrom<&str> for BinaryFuncValue {
    type Error = BinaryFuncValueError;

    fn try_from(encoded: &str) -> Result<Self, Self::Error> {
        Self::new(encoded)
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
        if self.encoded.len() == 1 {
            serializer.serialize_str(&self.encoded[0])
        } else {
            self.encoded.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for BinaryFuncValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StringOrVecVisitor;

        impl<'de> de::Visitor<'de> for StringOrVecVisitor {
            type Value = BinaryFuncValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a base64 string or an array of base64 strings")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                BinaryFuncValue::new(value).map_err(E::custom)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut values = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(value) = seq.next_element::<String>()? {
                    values.push(value);
                }
                if values.is_empty() {
                    return Err(de::Error::custom("empty"));
                }

                BinaryFuncValue::from_encoded_values(values).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(StringOrVecVisitor)
    }
}

impl BinaryFunc {
    /// Evaluate an AWS IAM `BinaryEquals` condition.
    ///
    /// AWS semantics compare the base64-decoded bytes of the policy value
    /// against the base64-decoded bytes of the request context value. In this
    /// codebase request context values come directly from HTTP header strings
    /// (see `rustfs::auth::get_condition_values_with_query`), so for real
    /// binary condition keys (e.g. SSE-C customer-key headers) the request
    /// value is itself base64. Decoding both sides is therefore required for
    /// the comparison to ever succeed.
    ///
    /// All key/value pairs in the function must match (logical AND); for a
    /// given key, any decoded request value that equals any expected decoded
    /// policy value satisfies that pair (OR across request values and policy
    /// values). A missing request key, or *any* request value that is not
    /// valid base64, causes the condition to evaluate to false (fail-closed).
    pub fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        for inner in self.0.iter() {
            let Some(rvalues) = values.get(inner.key.name().as_str()) else {
                return false;
            };

            let mut matched = false;
            for v in rvalues {
                let Ok(decoded) = base64_simd::STANDARD.decode_to_vec(v.as_bytes()) else {
                    return false;
                };
                if inner
                    .values
                    .decoded
                    .iter()
                    .any(|expected| decoded.as_slice() == expected.as_slice())
                {
                    matched = true;
                }
            }
            if !matched {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::{BinaryFunc, BinaryFuncValue, BinaryFuncValueError};
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

    fn new_multi_func(name: KeyName, variable: Option<String>, values: &[&str]) -> BinaryFunc {
        BinaryFunc {
            0: vec![FuncKeyValue {
                key: Key { name, variable },
                values: BinaryFuncValue::from_encoded_values(values.iter().map(|value| (*value).to_string()).collect())
                    .expect("valid binary array in test"),
            }],
        }
    }

    #[test]
    fn evaluate_matches_decoded_bytes() {
        // base64("hello") = "aGVsbG8="
        let f = new_func(Aws(AWSUsername), None, "aGVsbG8=");
        let mut ctx = HashMap::new();
        // Request value is itself base64 — BinaryEquals decodes both sides.
        ctx.insert("username".to_string(), vec!["aGVsbG8=".to_string()]);
        assert!(f.evaluate(&ctx));
    }

    #[test]
    fn evaluate_rejects_non_matching_value() {
        let f = new_func(Aws(AWSUsername), None, "aGVsbG8="); // "hello"
        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec!["d29ybGQ=".to_string()]); // "world"
        assert!(!f.evaluate(&ctx));
    }

    #[test]
    fn evaluate_matches_any_request_value() {
        let f = new_func(Aws(AWSUsername), None, "aGVsbG8="); // "hello"
        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec!["d29ybGQ=".to_string(), "aGVsbG8=".to_string()]);
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
        ctx.insert("username".to_string(), vec!["Y2Fmw6k=".to_string()]);
        assert!(f.evaluate(&ctx));
    }

    #[test]
    fn evaluate_matches_any_policy_value() {
        let f = new_multi_func(Aws(AWSUsername), None, &["aGVsbG8=", "d29ybGQ="]);
        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec!["d29ybGQ=".to_string()]);
        assert!(f.evaluate(&ctx));
    }

    #[test]
    fn evaluate_invalid_base64_request_value_fails_closed() {
        // Malformed base64 in the request must never match, regardless of policy value.
        let f = new_func(Aws(AWSUsername), None, "aGVsbG8=");
        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec!["!!!not-base64!!!".to_string()]);
        assert!(!f.evaluate(&ctx));
    }

    #[test]
    fn evaluate_mixed_valid_and_invalid_request_values_fails_closed() {
        // A valid matching value alongside an invalid base64 value must still
        // fail closed — for BinaryEquals, any unparsable request value causes
        // evaluation to return false even if another request value matches.
        let f = new_func(Aws(AWSUsername), None, "aGVsbG8="); // "hello"
        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec!["aGVsbG8=".to_string(), "!!!not-base64!!!".to_string()]);
        assert!(!f.evaluate(&ctx));

        // Order-independent: invalid first, valid second — still false.
        let mut ctx2 = HashMap::new();
        ctx2.insert("username".to_string(), vec!["!!!not-base64!!!".to_string(), "aGVsbG8=".to_string()]);
        assert!(!f.evaluate(&ctx2));
    }

    #[test]
    fn evaluate_raw_request_value_does_not_match() {
        // A raw (non-base64) request value that happens to equal the decoded
        // policy bytes must NOT match — both sides are decoded first. "hello"
        // is not valid standard base64 (length 5, not a multiple of 4), so
        // decoding fails and the evaluation fails closed.
        let f = new_func(Aws(AWSUsername), None, "aGVsbG8="); // decodes to "hello"
        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec!["hello".to_string()]);
        assert!(!f.evaluate(&ctx));
    }

    #[test]
    fn try_from_constructs_binary_func_value() {
        // Ergonomic alternatives to BinaryFuncValue::new — parity with the
        // prior public-struct API and idiomatic Rust conversion.
        let from_str: BinaryFuncValue = "aGVsbG8=".try_into().unwrap();
        let from_string: BinaryFuncValue = String::from("aGVsbG8=").try_into().unwrap();
        assert_eq!(from_str, from_string);
        assert_eq!(BinaryFuncValue::try_from("!!!bad!!!").unwrap_err(), BinaryFuncValueError::InvalidBase64,);
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
        ctx.insert("username".to_string(), vec!["aGVsbG8=".to_string()]);
        ctx.insert("principaltype".to_string(), vec!["d29ybGQ=".to_string()]);
        assert!(f.evaluate(&ctx));

        // Second key missing — must fail.
        let mut ctx2 = HashMap::new();
        ctx2.insert("username".to_string(), vec!["aGVsbG8=".to_string()]);
        assert!(!f.evaluate(&ctx2));
    }

    #[test]
    fn deserializes_from_policy_json() {
        let json = r#"{"aws:username": "aGVsbG8="}"#;
        let f: BinaryFunc = serde_json::from_str(json).unwrap();
        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec!["aGVsbG8=".to_string()]);
        assert!(f.evaluate(&ctx));
    }

    #[test]
    fn deserializes_array_from_policy_json() {
        let json = r#"{"aws:username": ["aGVsbG8=", "d29ybGQ="]}"#;
        let f: BinaryFunc = serde_json::from_str(json).unwrap();
        let mut ctx = HashMap::new();
        ctx.insert("username".to_string(), vec!["d29ybGQ=".to_string()]);
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
    fn deserialize_rejects_invalid_base64_in_array_at_parse_time() {
        let json = r#"{"aws:username": ["aGVsbG8=", "!!!not-base64!!!"]}"#;
        let err = serde_json::from_str::<BinaryFunc>(json).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid base64"), "unexpected error message: {msg}");
    }

    #[test]
    fn deserialize_rejects_empty_array() {
        let json = r#"{"aws:username": []}"#;
        let err = serde_json::from_str::<BinaryFunc>(json).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("empty"), "unexpected error message: {msg}");
    }

    #[test]
    fn serialize_round_trip_preserves_encoded_form() {
        let json = r#"{"aws:username":"aGVsbG8="}"#;
        let f: BinaryFunc = serde_json::from_str(json).unwrap();
        let out = serde_json::to_string(&f).unwrap();
        assert_eq!(out, json);
    }

    #[test]
    fn serialize_round_trip_preserves_encoded_array_form() {
        let json = r#"{"aws:username":["aGVsbG8=","d29ybGQ="]}"#;
        let f: BinaryFunc = serde_json::from_str(json).unwrap();
        let out = serde_json::to_string(&f).unwrap();
        assert_eq!(out, json);
    }
}
