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

use super::func::InnerFunc;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{Error, Visitor},
};

pub type NumberFunc = InnerFunc<NumberFuncValue>;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NumberFuncValue(i64);

impl NumberFunc {
    pub fn evaluate(&self, op: impl Fn(&i64, &i64) -> bool, if_exists: bool, values: &HashMap<String, Vec<String>>) -> bool {
        for inner in self.0.iter() {
            let v = match values.get(inner.key.name().as_str()).and_then(|x| x.first()) {
                Some(x) => x,
                None => return if_exists,
            };

            let Ok(rv) = v.parse::<i64>() else {
                return false;
            };

            if !op(&rv, &inner.values.0) {
                return false;
            }
        }

        true
    }
}

impl Serialize for NumberFuncValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.0.to_string().as_str())
    }
}

impl<'de> Deserialize<'de> for NumberFuncValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct NumberVisitor;

        impl Visitor<'_> for NumberVisitor {
            type Value = NumberFuncValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a number or a string that can be represented as a number.")
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(NumberFuncValue(value))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(NumberFuncValue(value as i64))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(NumberFuncValue(value.parse().map_err(|e| E::custom(format!("{e:?}")))?))
            }
        }

        deserializer.deserialize_any(NumberVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::{NumberFunc, NumberFuncValue};
    use crate::policy::function::func::FuncKeyValue;
    use crate::policy::function::{
        key::Key,
        key_name::KeyName::{self, *},
        key_name::S3KeyName::*,
    };
    use test_case::test_case;

    fn new_func(name: KeyName, variable: Option<String>, value: i64) -> NumberFunc {
        NumberFunc {
            0: vec![FuncKeyValue {
                key: Key { name, variable },
                values: NumberFuncValue(value),
            }],
        }
    }

    #[test_case(r#"{"s3:max-keys": 1}"#, new_func(S3(S3MaxKeys), None, 1); "1")]
    #[test_case(r#"{"s3:max-keys/a": 1}"#, new_func(S3(S3MaxKeys), Some("a".into()), 1); "2")]
    #[test_case(r#"{"s3:max-keys": "1"}"#, new_func(S3(S3MaxKeys), None, 1); "3")]
    #[test_case(r#"{"s3:max-keys/a": "1"}"#, new_func(S3(S3MaxKeys), Some("a".into()), 1); "4")]
    fn test_deser(input: &str, expect: NumberFunc) -> Result<(), serde_json::Error> {
        let v: NumberFunc = serde_json::from_str(input)?;
        assert_eq!(v, expect);
        Ok(())
    }

    #[test_case(r#"{"s3:max-keys":"1"}"#, new_func(S3(S3MaxKeys), None, 1); "1")]
    #[test_case(r#"{"s3:max-keys/a":"1"}"#, new_func(S3(S3MaxKeys), Some("a".into()), 1); "2")]
    fn test_ser(expect: &str, input: NumberFunc) -> Result<(), serde_json::Error> {
        let v = serde_json::to_string(&input)?;
        assert_eq!(v, expect);
        Ok(())
    }
}
