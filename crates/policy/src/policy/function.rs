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

use crate::policy::function::condition::Condition;
use crate::policy::variables::PolicyVariableResolver;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer, de};
use std::collections::HashMap;
use std::collections::HashSet;

pub mod addr;
pub mod binary;
pub mod bool_null;
pub mod condition;
pub mod date;
pub mod func;
pub mod key;
pub mod key_name;
pub mod number;
pub mod string;

#[derive(Clone, Default, Debug)]
pub struct Functions {
    for_any_value: Vec<Condition>,
    for_all_values: Vec<Condition>,
    for_normal: Vec<Condition>,
}

impl Functions {
    pub async fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        self.evaluate_with_resolver(values, None).await
    }

    pub async fn evaluate_with_resolver(
        &self,
        values: &HashMap<String, Vec<String>>,
        resolver: Option<&dyn PolicyVariableResolver>,
    ) -> bool {
        for c in self.for_any_value.iter() {
            if !c.evaluate_with_resolver(false, values, resolver).await {
                return false;
            }
        }

        for c in self.for_all_values.iter() {
            if !c.evaluate_with_resolver(true, values, resolver).await {
                return false;
            }
        }

        for c in self.for_normal.iter() {
            if !c.evaluate_with_resolver(false, values, resolver).await {
                return false;
            }
        }

        true
    }

    pub fn is_empty(&self) -> bool {
        self.for_all_values.is_empty() && self.for_any_value.is_empty() && self.for_normal.is_empty()
    }
}

impl Serialize for Functions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut se =
            serializer.serialize_map(Some(self.for_any_value.len() + self.for_all_values.len() + self.for_normal.len()))?;

        for conditions in self.for_all_values.iter() {
            se.serialize_key(format!("ForAllValues:{}", conditions.to_key()).as_str())?;
            conditions.serialize_map(&mut se)?;
        }

        for conditions in self.for_any_value.iter() {
            se.serialize_key(format!("ForAnyValue:{}", conditions.to_key()).as_str())?;
            conditions.serialize_map(&mut se)?;
        }

        for conditions in self.for_normal.iter() {
            se.serialize_key(conditions.to_key())?;
            conditions.serialize_map(&mut se)?;
        }

        se.end()
    }
}

impl<'de> Deserialize<'de> for Functions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FuncVisitor;
        use serde::de::Visitor;

        impl<'de> Visitor<'de> for FuncVisitor {
            type Value = Functions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("Functions")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                use serde::de::Error;

                let mut hash = HashSet::with_capacity(map.size_hint().unwrap_or_default());

                let mut inner_data = Functions::default();
                while let Some(key) = map.next_key::<&str>()? {
                    if hash.contains(&key) {
                        return Err(Error::custom(format!("duplicate condition operator `{key}`")));
                    }

                    hash.insert(key);

                    let mut tokens = key.split(":");
                    let mut qualifier = tokens.next();
                    let mut name = tokens.next();
                    if name.is_none() {
                        name = qualifier;
                        qualifier = None;
                    }

                    if tokens.next().is_some() {
                        return Err(Error::custom("invalid condition operator"));
                    }

                    let Some(name) = name else { return Err(Error::custom("has no condition operator")) };

                    let condition = Condition::from_deserializer(name, &mut map)?;
                    match qualifier {
                        Some("ForAnyValue") => inner_data.for_any_value.push(condition),
                        Some("ForAllValues") => inner_data.for_all_values.push(condition),
                        Some(q) => return Err(Error::custom(format!("invalid qualifier `{q}`"))),
                        None => inner_data.for_normal.push(condition),
                    }
                }

                /* if inner_data.is_empty() {
                    return Err(Error::custom("has no condition element"));
                } */

                Ok(inner_data)
            }
        }

        deserializer.deserialize_map(FuncVisitor)
    }
}

impl PartialEq for Functions {
    fn eq(&self, other: &Self) -> bool {
        if !(self.for_all_values.len() == other.for_all_values.len()
            && self.for_any_value.len() == other.for_any_value.len()
            && self.for_normal.len() == other.for_normal.len())
        {
            return false;
        }

        self.for_any_value.iter().all(|x| other.for_any_value.contains(x))
            && self.for_all_values.iter().all(|x| other.for_all_values.contains(x))
            && self.for_normal.iter().all(|x| other.for_normal.contains(x))
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct Value;

#[cfg(test)]
mod tests {
    use crate::policy::Functions;
    use crate::policy::function::condition::Condition::*;
    use crate::policy::function::func::FuncKeyValue;
    use crate::policy::function::key::Key;
    use crate::policy::function::string::StringFunc;
    use crate::policy::function::string::StringFuncValue;
    use test_case::test_case;

    #[test_case(
        r#"{
            "Null": {
                "s3:x-amz-server-side-encryption-customer-algorithm": true
            },
            "Null": {
                "s3:x-amz-server-side-encryption-customer-algorithm": "true"
            }
        }"# => false; "1")]
    #[test_case(r#"{}"# => true; "2")]
    #[test_case(
        r#"{
            "StringLike": {
                "s3:x-amz-metadata-directive": "REPL*"
            },
            "StringEquals": {
                "s3:x-amz-copy-source": "mybucket/myobject"
            },
            "StringNotEquals": {
                "s3:x-amz-server-side-encryption": "AES256"
            },
            "NotIpAddress": {
                "aws:SourceIp": [
                    "10.1.10.0/24",
                    "10.10.1.0/24"
                ]
            },
            "StringNotLike": {
                "s3:x-amz-storage-class": "STANDARD",
                "s3:x-amz-server-side-encryption": "AES256"
            },
            "Null": {
                "s3:x-amz-server-side-encryption-customer-algorithm": true
            },
            "IpAddress": {
                "aws:SourceIp": [
                    "192.168.1.0/24",
                    "192.168.2.0/24"
                ]
            }
        }"# => true; "3"
    )]
    #[test_case(
        r#"{
            "StringLike": {
                "s3:x-amz-metadata-directive": "REPL*"
            },
            "StringEquals": {
                "s3:x-amz-copy-source": "mybucket/myobject",
                "s3:prefix": [
                   "",
                   "home/"
                ],
                "s3:delimiter": [
                   "/"
                ]
            },
            "StringNotEquals": {
                "s3:x-amz-server-side-encryption": "AES256"
            },
            "NotIpAddress": {
                "aws:SourceIp": [
                    "10.1.10.0/24",
                    "10.10.1.0/24"
                ]
            },
            "StringNotLike": {
                "s3:x-amz-storage-class": "STANDARD"
            },
            "Null": {
                "s3:x-amz-server-side-encryption-customer-algorithm": true
            },
            "IpAddress": {
                "aws:SourceIp": [
                    "192.168.1.0/24",
                    "192.168.2.0/24"
                ]
            }
        }"# => true; "4"
    )]
    #[test_case(
        r#"{
            "IpAddress": {
                "aws:SourceIp": [
                    "192.168.1.0/24"
                ]
            },
            "NotIpAddress": {
                "aws:SourceIp": [
                    "10.1.10.0/24"
                ]
            },
            "Null": {
                "s3:x-amz-server-side-encryption-customer-algorithm": [
                    true
                ]
            },
            "StringEquals": {
                "s3:x-amz-copy-source": [
                    "mybucket/myobject"
                ]
            },
            "StringLike": {
                "s3:x-amz-metadata-directive": [
                    "REPL*"
                ]
            },
            "StringNotEquals": {
                "s3:x-amz-server-side-encryption": [
                    "AES256"
                ]
            },
            "StringNotLike": {
                "s3:x-amz-storage-class": [
                    "STANDARD"
                ]
            }
        }"# => true;
        "5"
    )]
    #[test_case(
        r#"{
            "IpAddress": {
                "aws:SourceIp": [
                    "192.168.1.0/24"
                ]
            },
            "NotIpAddress": {
                "aws:SourceIp": [
                    "10.1.10.0/24"
                ]
            },
            "Null": {
                "s3:x-amz-server-side-encryption-customer-algorithm": [
                    true
                ]
            },
            "StringEquals": {
                "s3:x-amz-copy-source": [
                    "mybucket/myobject"
                ]
            },
            "StringLike": {
                "s3:x-amz-metadata-directive": [
                    "REPL*"
                ]
            },
            "StringNotEquals": {
                "s3:x-amz-server-side-encryption": [
                    "aws:kms"
                ]
            },
            "StringNotLike": {
                "s3:x-amz-storage-class": [
                    "STANDARD"
                ]
            }
        }"# => true;
        "6"
    )]
    fn test_de(input: &str) -> bool {
        serde_json::from_str::<Functions>(input)
            .map_err(|e| eprintln!("{e:?}"))
            .is_ok()
    }

    #[test_case(
        Functions {
            for_normal: vec![StringNotLike(StringFunc {
                0: vec![FuncKeyValue {
                    key: Key::try_from("s3:LocationConstraint").unwrap(),
                    values: StringFuncValue(vec!["us-east-1"].into_iter().map(ToOwned::to_owned).collect()),
                }],
            })],
            ..Default::default()
        },
        r#"{"StringNotLike":{"s3:LocationConstraint":"us-east-1"}}"#;
        "1"
    )]
    #[test_case(
        Functions {
            for_all_values: vec![StringNotLike(StringFunc {
                0: vec![FuncKeyValue {
                    key: Key::try_from("s3:LocationConstraint").unwrap(),
                    values: StringFuncValue(vec!["us-east-1"].into_iter().map(ToOwned::to_owned).collect()),
                }],
            })],
            ..Default::default()
        },
        r#"{"ForAllValues:StringNotLike":{"s3:LocationConstraint":"us-east-1"}}"#;
        "2"
    )]
    #[test_case(
        Functions {
            for_any_value: vec![StringNotLike(StringFunc {
                0: vec![FuncKeyValue {
                    key: Key::try_from("s3:LocationConstraint").unwrap(),
                    values: StringFuncValue(vec!["us-east-1", "us-east-2"].into_iter().map(ToOwned::to_owned).collect()),
                }],
            })],
            for_all_values: vec![StringNotLike(StringFunc {
                0: vec![FuncKeyValue {
                    key: Key::try_from("s3:LocationConstraint").unwrap(),
                    values: StringFuncValue(vec!["us-east-1"].into_iter().map(ToOwned::to_owned).collect()),
                }],
            })],
            for_normal: vec![StringNotLike(StringFunc {
                0: vec![FuncKeyValue {
                    key: Key::try_from("s3:LocationConstraint").unwrap(),
                    values: StringFuncValue(vec!["us-east-1"].into_iter().map(ToOwned::to_owned).collect()),
                }],
            })],
        },
        r#"{"ForAllValues:StringNotLike":{"s3:LocationConstraint":"us-east-1"},"ForAnyValue:StringNotLike":{"s3:LocationConstraint":["us-east-1","us-east-2"]},"StringNotLike":{"s3:LocationConstraint":"us-east-1"}}"#;
        "3"
    )]
    fn test_ser(input: Functions, expect: &str) {
        assert_eq!(serde_json::to_string(&input).unwrap(), expect);
    }
}
