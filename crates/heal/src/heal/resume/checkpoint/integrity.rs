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

use crate::{Error, Result};
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::fmt;

// Checkpoint-local ordering preserves historical digests without affecting
// the server's serde_json feature set.
#[derive(Debug)]
pub(super) enum CheckpointJson {
    Object(Vec<(String, Self)>),
    Array(Vec<Self>),
    Scalar(Value),
}

impl CheckpointJson {
    fn as_str(&self) -> Option<&str> {
        match self {
            Self::Scalar(Value::String(value)) => Some(value),
            _ => None,
        }
    }

    pub(super) fn from_value(value: Value) -> Result<Self> {
        fn convert(value: Value, depth: usize) -> Result<CheckpointJson> {
            if depth >= 128 {
                return Err(Error::InvalidCheckpoint("Checkpoint JSON exceeds the nesting limit".to_string()));
            }
            match value {
                Value::Object(fields) => fields
                    .into_iter()
                    .map(|(key, value)| convert(value, depth + 1).map(|value| (key, value)))
                    .collect::<Result<_>>()
                    .map(CheckpointJson::Object),
                Value::Array(values) => values
                    .into_iter()
                    .map(|value| convert(value, depth + 1))
                    .collect::<Result<_>>()
                    .map(CheckpointJson::Array),
                scalar => Ok(CheckpointJson::Scalar(scalar)),
            }
        }
        convert(value, 0)
    }

    pub(super) fn sort_all_objects(&mut self) {
        let mut pending = vec![self];
        while let Some(value) = pending.pop() {
            match value {
                Self::Object(fields) => {
                    fields.sort_unstable_by(|left, right| left.0.cmp(&right.0));
                    pending.extend(fields.iter_mut().map(|(_, value)| value));
                }
                Self::Array(values) => pending.extend(values),
                Self::Scalar(_) => {}
            }
        }
    }

    pub(super) fn set_digest(&mut self, digest: Option<String>) -> Result<()> {
        let Self::Object(fields) = self else {
            return Err(Error::InvalidCheckpoint("Checkpoint must be a JSON object".to_string()));
        };
        let (_, value) = fields
            .iter_mut()
            .find(|(key, _)| key == "integrity_digest")
            .ok_or_else(|| Error::InvalidCheckpoint("Signed checkpoint is missing its integrity digest field".to_string()))?;
        *value = Self::Scalar(digest.map_or(Value::Null, Value::String));
        Ok(())
    }

    pub(super) fn unsigned_bytes(&mut self) -> Result<Vec<u8>> {
        self.set_digest(None)?;
        let Self::Object(fields) = self else {
            return Err(Error::InvalidCheckpoint("Checkpoint must be a JSON object".to_string()));
        };
        for field in ["processed_objects", "failed_objects", "skipped_objects"] {
            let Some((_, Self::Array(values))) = fields.iter_mut().find(|(key, _)| key == field) else {
                return Err(Error::InvalidCheckpoint(format!("Checkpoint set is missing or invalid: {field}")));
            };
            values.sort_unstable_by(|left, right| left.as_str().cmp(&right.as_str()));
        }
        serde_json::to_vec(self).map_err(|error| Error::InvalidCheckpoint(format!("Failed to encode checkpoint digest: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_json_preserves_nested_order_and_scalar_values() {
        let wire = r#"{"z":{"z":18446744073709551615,"a":-9223372036854775808,"f":1.5,"s":"hello\nworld","b":true,"n":null},"other":[2,1],"processed_objects":["b","a"],"failed_objects":[],"skipped_objects":[],"integrity_digest":"signed"}"#;
        let mut value: CheckpointJson = serde_json::from_str(wire).expect("parse ordered checkpoint JSON");
        assert_eq!(serde_json::to_string(&value).expect("encode ordered checkpoint JSON"), wire);
        let unsigned = value.unsigned_bytes().expect("normalize only checkpoint sets and digest");
        assert_eq!(
            String::from_utf8(unsigned).expect("unsigned checkpoint must be UTF-8"),
            wire.replace("[\"b\",\"a\"]", "[\"a\",\"b\"]").replace("\"signed\"", "null")
        );
    }

    #[test]
    fn checkpoint_json_sorts_nested_objects_without_reordering_other_arrays() {
        let mut value: CheckpointJson =
            serde_json::from_str(r#"{"z":[{"z":2,"a":1},3,1],"a":false}"#).expect("parse nested checkpoint JSON");
        value.sort_all_objects();
        assert_eq!(
            serde_json::to_string(&value).expect("encode sorted nested checkpoint JSON"),
            r#"{"a":false,"z":[{"a":1,"z":2},3,1]}"#
        );
    }

    #[test]
    fn checkpoint_json_bounds_both_input_and_value_conversion_depth() {
        for depth in [127, 128] {
            let wire = format!("{}null{}", "[".repeat(depth), "]".repeat(depth));
            assert_eq!(serde_json::from_str::<CheckpointJson>(&wire).is_ok(), depth == 127);
            let mut value = Value::Null;
            for _ in 0..depth {
                value = Value::Array(vec![value]);
            }
            assert_eq!(CheckpointJson::from_value(value).is_ok(), depth == 127);
        }
    }
}

impl Serialize for CheckpointJson {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Object(fields) => {
                let mut map = serializer.serialize_map(Some(fields.len()))?;
                for (key, value) in fields {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
            Self::Array(values) => values.serialize(serializer),
            Self::Scalar(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for CheckpointJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct JsonVisitor;

        impl<'de> Visitor<'de> for JsonVisitor {
            type Value = CheckpointJson;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("checkpoint JSON")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Self::Value, A::Error> {
                let mut fields = Vec::new();
                while let Some(field) = map.next_entry()? {
                    fields.push(field);
                }
                Ok(CheckpointJson::Object(fields))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element()? {
                    values.push(value);
                }
                Ok(CheckpointJson::Array(values))
            }

            fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
                Ok(CheckpointJson::Scalar(Value::Bool(value)))
            }

            fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
                Ok(CheckpointJson::Scalar(Value::Number(value.into())))
            }

            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
                Ok(CheckpointJson::Scalar(Value::Number(value.into())))
            }

            fn visit_f64<E: serde::de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
                serde_json::Number::from_f64(value)
                    .map(|value| CheckpointJson::Scalar(Value::Number(value)))
                    .ok_or_else(|| E::custom("non-finite checkpoint number"))
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                self.visit_string(value.to_owned())
            }

            fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
                Ok(CheckpointJson::Scalar(Value::String(value)))
            }

            fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
                Ok(CheckpointJson::Scalar(Value::Null))
            }
        }

        deserializer.deserialize_any(JsonVisitor)
    }
}
