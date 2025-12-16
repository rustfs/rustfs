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

#[cfg(test)]
use std::collections::BTreeSet as Set;
#[cfg(not(test))]
use std::collections::HashSet as Set;
use std::fmt;
use std::{borrow::Cow, collections::HashMap};

use crate::policy::function::func::FuncKeyValue;
use crate::policy::utils::wildcard;
use futures::future;
use serde::{Deserialize, Deserializer, Serialize, de, ser::SerializeSeq};

use super::{func::InnerFunc, key_name::KeyName};
use crate::policy::variables::PolicyVariableResolver;

pub type StringFunc = InnerFunc<StringFuncValue>;

impl StringFunc {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn evaluate_with_resolver(
        &self,
        for_all: bool,
        ignore_case: bool,
        like: bool,
        negate: bool,
        values: &HashMap<String, Vec<String>>,
        resolver: Option<&dyn PolicyVariableResolver>,
    ) -> bool {
        for inner in self.0.iter() {
            let result = if like {
                inner.eval_like(for_all, values, resolver).await ^ negate
            } else {
                inner.eval(for_all, ignore_case, values, resolver).await ^ negate
            };

            if !result {
                return false;
            }
        }

        true
    }
}

impl FuncKeyValue<StringFuncValue> {
    async fn eval(
        &self,
        for_all: bool,
        ignore_case: bool,
        values: &HashMap<String, Vec<String>>,
        resolver: Option<&dyn PolicyVariableResolver>,
    ) -> bool {
        let rvalues = values
            // http.CanonicalHeaderKey ?
            .get(self.key.name().as_str())
            .map(|t| {
                t.iter()
                    .map(|x| {
                        if ignore_case {
                            Cow::Owned(x.to_lowercase())
                        } else {
                            Cow::from(x)
                        }
                    })
                    .collect::<Set<_>>()
            })
            .unwrap_or_default();

        let resolved_values: Vec<Vec<String>> = futures::future::join_all(self.values.0.iter().map(|c| async {
            if let Some(res) = resolver {
                super::super::variables::resolve_aws_variables(c, res).await
            } else {
                vec![c.to_string()]
            }
        }))
        .await;

        let fvalues = resolved_values
            .into_iter()
            .flatten()
            .map(|resolved_c| {
                let mut c = Cow::from(resolved_c);
                for key in KeyName::COMMON_KEYS {
                    match values.get(key.name()).and_then(|x| x.first()) {
                        Some(v) if !v.is_empty() => return Cow::Owned(c.to_mut().replace(&key.var_name(), v)),
                        _ => continue,
                    };
                }

                c
            })
            .map(|x| if ignore_case { Cow::Owned(x.to_lowercase()) } else { x })
            .collect::<Set<_>>();

        let ivalues = rvalues.intersection(&fvalues);

        if for_all {
            rvalues.is_empty() || rvalues.len() == ivalues.count()
        } else {
            ivalues.count() > 0
        }
    }

    async fn eval_like(
        &self,
        for_all: bool,
        values: &HashMap<String, Vec<String>>,
        resolver: Option<&dyn PolicyVariableResolver>,
    ) -> bool {
        if let Some(rvalues) = values.get(self.key.name().as_str()) {
            for v in rvalues.iter() {
                let resolved_futures: Vec<_> = self
                    .values
                    .0
                    .iter()
                    .map(|c| async {
                        if let Some(res) = resolver {
                            super::super::variables::resolve_aws_variables(c, res).await
                        } else {
                            vec![c.to_string()]
                        }
                    })
                    .collect();
                let resolved_values = future::join_all(resolved_futures).await;
                let matched = resolved_values
                    .into_iter()
                    .flatten()
                    .map(|resolved_c| {
                        let mut c = Cow::from(resolved_c);
                        for key in KeyName::COMMON_KEYS {
                            match values.get(key.name()).and_then(|x| x.first()) {
                                Some(v) if !v.is_empty() => return Cow::Owned(c.to_mut().replace(&key.var_name(), v)),
                                _ => continue,
                            };
                        }

                        c
                    })
                    .any(|x| wildcard::is_match(x, v));

                if for_all {
                    if !matched {
                        return false;
                    }
                } else if matched {
                    return true;
                }
            }
        }

        for_all
    }
}

/// Parse values field
#[derive(Clone, PartialEq, Eq, Debug)]

pub struct StringFuncValue(pub Set<String>);

impl Serialize for StringFuncValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if self.0.len() == 1 {
            serializer.serialize_some(&self.0.iter().next())
        } else {
            let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
            for element in &self.0 {
                seq.serialize_element(element)?;
            }
            seq.end()
        }
    }
}

impl<'d> Deserialize<'d> for StringFuncValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'d>,
    {
        struct StringOrVecVisitor;

        impl<'de> de::Visitor<'de> for StringOrVecVisitor {
            type Value = StringFuncValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or an array of strings")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok({
                    let mut hash = Set::new();
                    hash.insert(value.to_string());
                    StringFuncValue(hash)
                })
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                #[cfg(test)]
                let mut values = Set::new();
                #[cfg(not(test))]
                let mut values = Set::with_capacity(seq.size_hint().unwrap_or(0));

                while let Some(value) = seq.next_element::<String>()? {
                    values.insert(value);
                }
                Ok(StringFuncValue(values))
            }
        }

        let result = deserializer.deserialize_any(StringOrVecVisitor)?;
        if result.0.is_empty() {
            use serde::de::Error;

            return Err(Error::custom("empty"));
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::{StringFunc, StringFuncValue};
    use crate::policy::function::func::FuncKeyValue;
    use crate::policy::function::{
        key::Key,
        key_name::AwsKeyName::*,
        key_name::KeyName::{self, *},
    };
    use std::collections::HashMap;

    use crate::policy::function::key_name::S3KeyName::S3LocationConstraint;
    use test_case::test_case;

    fn new_func(name: KeyName, variable: Option<String>, values: Vec<&str>) -> StringFunc {
        StringFunc {
            0: vec![FuncKeyValue {
                key: Key { name, variable },
                values: StringFuncValue(values.into_iter().map(|x| x.to_owned()).collect()),
            }],
        }
    }

    #[test_case(r#"{"aws:username": "johndoe"}"#,
        new_func(Aws(AWSUsername), None, vec!["johndoe"])
    )]
    #[test_case(r#"{"aws:username": ["johndoe", "aaa"]}"#, new_func(Aws(AWSUsername), None, vec!["johndoe", "aaa"]
    ))]
    #[test_case(r#"{"aws:username/value": "johndoe"}"#, new_func(Aws(AWSUsername), Some("value".into()), vec!["johndoe"]
    ))]
    #[test_case(r#"{"aws:username/value": ["johndoe", "aaa"]}"#, new_func(Aws(AWSUsername), Some("value".into()), vec!["johndoe", "aaa"]
    ))]
    fn test_deser(input: &str, expect: StringFunc) -> Result<(), serde_json::Error> {
        let v: StringFunc = serde_json::from_str(input)?;
        assert_eq!(v, expect);
        Ok(())
    }

    #[test_case(r#"{"aws:usernamea":"johndoe"}"#)]
    #[test_case(r#"{"aws:username":[]}"#)] // Empty
    #[test_case(r#"{"aws:usernamea/value":"johndoe"}"#)]
    #[test_case(r#"{"aws:usernamea/value":["johndoe", "aaa"]}"#)]
    #[test_case(r#""aaa""#)]
    fn test_deser_failed(input: &str) {
        assert!(serde_json::from_str::<StringFunc>(input).is_err());
    }

    #[test_case(r#"{"aws:username":"johndoe"}"#, new_func(Aws(AWSUsername), None, vec!["johndoe"]))]
    #[test_case(r#"{"aws:username":["aaa","johndoe"]}"#, new_func(Aws(AWSUsername), None, vec!["johndoe", "aaa"]))]
    #[test_case(r#"{"aws:username/value":"johndoe"}"#, new_func(Aws(AWSUsername), Some("value".into()), vec!["johndoe"]))]
    #[test_case(r#"{"aws:username/value":["aaa","johndoe"]}"#, new_func(Aws(AWSUsername), Some("value".into()), vec!["johndoe", "aaa"]))]
    fn test_ser(expect: &str, input: StringFunc) -> Result<(), serde_json::Error> {
        let v = serde_json::to_string(&input)?;
        assert_eq!(v.as_str(), expect);
        Ok(())
    }

    fn new_fkv(name: &str, values: Vec<&str>) -> FuncKeyValue<StringFuncValue> {
        FuncKeyValue {
            key: name.try_into().unwrap(),
            values: StringFuncValue(values.into_iter().map(ToOwned::to_owned).collect()),
        }
    }

    fn test_eval(
        s: FuncKeyValue<StringFuncValue>,
        for_all: bool,
        ignore_case: bool,
        negate: bool,
        values: Vec<(&str, Vec<&str>)>,
    ) -> bool {
        let map: HashMap<String, Vec<String>> = values
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.into_iter().map(ToOwned::to_owned).collect::<Vec<String>>()))
            .collect();
        let result = s.eval(for_all, ignore_case, &map, None);

        pollster::block_on(result) ^ negate
    }

    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![("x-amz-copy-source", vec!["mybucket/myobject"])] => true ; "1")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![("x-amz-copy-source", vec!["yourbucket/myobject"])] => false ; "2")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![] => false ; "3")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![("delimiter", vec!["/"])] => false ; "4")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-1", "ap-southeast-1"]), false, vec![("LocationConstraint", vec!["eu-west-1"])] => true ; "5")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-1", "ap-southeast-1"]), false, vec![("LocationConstraint", vec!["ap-southeast-1"])] => true ; "6")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-1", "ap-southeast-1"]), false, vec![("LocationConstraint", vec!["us-east-1"])] => false ; "7")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-1", "ap-southeast-1"]), false, vec![] => false ; "8")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-1", "ap-southeast-1"]), false, vec![("delimiter", vec!["/"])] => false ; "9")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), true, vec![("groups", vec!["prod", "art"])] => true ; "10")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), true, vec![("groups", vec!["art"])] => true ; "11")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), true, vec![] => true ; "12")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), true, vec![("delimiter", vec!["/"])] => true ; "13")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), false, vec![("groups", vec!["prod", "art"])] => true ; "14")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), false, vec![("groups", vec!["art"])] => true ; "15")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), false, vec![] => false ; "16")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), false, vec![("delimiter", vec!["/"])] => false ; "17")]
    #[test_case(new_fkv("s3:LocationConstraint", vec![KeyName::S3(S3LocationConstraint).var_name().as_str()]), false, vec![("LocationConstraint", vec!["us-west-1"])] => true ; "18")]
    #[test_case(new_fkv("s3:ExistingObjectTag/security", vec!["public"]), false, vec![("ExistingObjectTag/security", vec!["public"])] => true ; "19")]
    #[test_case(new_fkv("s3:ExistingObjectTag/security", vec!["public"]), false, vec![("ExistingObjectTag/security", vec!["private"])] => false ; "20")]
    #[test_case(new_fkv("s3:ExistingObjectTag/security", vec!["public"]), false, vec![("ExistingObjectTag/project", vec!["webapp"])] => false ; "21")]
    fn test_string_equals(s: FuncKeyValue<StringFuncValue>, for_all: bool, values: Vec<(&str, Vec<&str>)>) -> bool {
        test_eval(s, for_all, false, false, values)
    }

    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![("x-amz-copy-source", vec!["mybucket/myobject"])] => false ; "1")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![("x-amz-copy-source", vec!["yourbucket/myobject"])] => true ; "2")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![] => true ; "3")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![("delimiter", vec!["/"])] => true ; "4")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-1", "ap-southeast-1"]), false, vec![("LocationConstraint", vec!["eu-west-1"])] => false ; "5")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-1", "ap-southeast-1"]), false, vec![("LocationConstraint", vec!["ap-southeast-1"])] => false ; "6")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-1", "ap-southeast-1"]), false, vec![("LocationConstraint", vec!["us-east-1"])] => true ; "7")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-1", "ap-southeast-1"]), false, vec![] => true ; "8")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-1", "ap-southeast-1"]), false, vec![("delimiter", vec!["/"])] => true ; "9")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), true, vec![("groups", vec!["prod", "art"])] => false ; "10")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), true, vec![("groups", vec!["art"])] => false ; "11")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), true, vec![] => false ; "12")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), true, vec![("delimiter", vec!["/"])] => false ; "13")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), false, vec![("groups", vec!["prod", "art"])] => false ; "14")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), false, vec![("groups", vec!["art"])] => false ; "15")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), false, vec![] => true ; "16")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art"]), false, vec![("delimiter", vec!["/"])] => true ; "17")]
    fn test_string_not_equals(s: FuncKeyValue<StringFuncValue>, for_all: bool, values: Vec<(&str, Vec<&str>)>) -> bool {
        test_eval(s, for_all, false, true, values)
    }

    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/MYOBJECT"]), false, vec![("x-amz-copy-source", vec!["mybucket/myobject"])] => true ; "1")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/MYOBJECT"]), false, vec![("x-amz-copy-source", vec!["yourbucket/myobject"])] => false ; "2")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/MYOBJECT"]), false, vec![] => false ; "3")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/MYOBJECT"]), false, vec![("delimiter", vec!["/"])] => false ; "4")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["EU-WEST-1", "AP-southeast-1"]), false, vec![("LocationConstraint", vec!["eu-west-1"])] => true ; "5")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["EU-WEST-1", "AP-southeast-1"]), false, vec![("LocationConstraint", vec!["ap-southeast-1"])] => true ; "6")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["EU-WEST-1", "AP-southeast-1"]), false, vec![("LocationConstraint", vec!["us-east-1"])] => false ; "7")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["EU-WEST-1", "AP-southeast-1"]), false, vec![] => false ; "8")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["EU-WEST-1", "AP-southeast-1"]), false, vec![("delimiter", vec!["/"])] => false ; "9")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), true, vec![("groups", vec!["prod", "art"])] => true ; "10")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), true, vec![("groups", vec!["art"])] => true ; "11")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), true, vec![] => true ; "12")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), true, vec![("delimiter", vec!["/"])] => true ; "13")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), false, vec![("groups", vec!["prod", "art"])] => true ; "14")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), false, vec![("groups", vec!["art"])] => true ; "15")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), false, vec![] => false ; "16")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), false, vec![("delimiter", vec!["/"])] => false ; "17")]
    fn test_string_equals_ignore_case(s: FuncKeyValue<StringFuncValue>, for_all: bool, values: Vec<(&str, Vec<&str>)>) -> bool {
        test_eval(s, for_all, true, false, values)
    }

    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/MYOBJECT"]), false, vec![("x-amz-copy-source", vec!["mybucket/myobject"])] => false ; "1")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/MYOBJECT"]), false, vec![("x-amz-copy-source", vec!["yourbucket/myobject"])] => true ; "2")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/MYOBJECT"]), false, vec![] => true ; "3")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/MYOBJECT"]), false, vec![("delimiter", vec!["/"])] => true ; "4")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["EU-WEST-1", "AP-southeast-1"]), false, vec![("LocationConstraint", vec!["eu-west-1"])] => false ; "5")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["EU-WEST-1", "AP-southeast-1"]), false, vec![("LocationConstraint", vec!["ap-southeast-1"])] => false ; "6")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["EU-WEST-1", "AP-southeast-1"]), false, vec![("LocationConstraint", vec!["us-east-1"])] => true ; "7")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["EU-WEST-1", "AP-southeast-1"]), false, vec![] => true ; "8")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["EU-WEST-1", "AP-southeast-1"]), false, vec![("delimiter", vec!["/"])] => true ; "9")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), true, vec![("groups", vec!["prod", "art"])] => false ; "10")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), true, vec![("groups", vec!["art"])] => false ; "11")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), true, vec![] => false ; "12")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), true, vec![("delimiter", vec!["/"])] => false ; "13")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), false, vec![("groups", vec!["prod", "art"])] => false ; "14")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), false, vec![("groups", vec!["art"])] => false ; "15")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), false, vec![] => true ; "16")]
    #[test_case(new_fkv("jwt:groups", vec!["Prod", "Art"]), false, vec![("delimiter", vec!["/"])] => true ; "17")]
    fn test_string_not_equals_ignore_case(
        s: FuncKeyValue<StringFuncValue>,
        for_all: bool,
        values: Vec<(&str, Vec<&str>)>,
    ) -> bool {
        test_eval(s, for_all, true, true, values)
    }

    fn test_eval_like(s: FuncKeyValue<StringFuncValue>, for_all: bool, negate: bool, values: Vec<(&str, Vec<&str>)>) -> bool {
        let map: HashMap<String, Vec<String>> = values
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.into_iter().map(ToOwned::to_owned).collect::<Vec<String>>()))
            .collect();
        let result = s.eval_like(for_all, &map, None);

        pollster::block_on(result) ^ negate
    }

    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![("x-amz-copy-source", vec!["mybucket/myobject"])] => true ; "1")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![("x-amz-copy-source", vec!["yourbucket/myobject"])] => false ; "2")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![] => false ; "3")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![("delimiter", vec!["/"])] => false ; "4")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-*", "ap-southeast-1"]), false, vec![("LocationConstraint", vec!["eu-west-1"])] => true ; "5")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-*", "ap-southeast-1"]), false, vec![("LocationConstraint", vec!["ap-southeast-1"])] => true ; "6")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-*", "ap-southeast-1"]), false, vec![("LocationConstraint", vec!["us-east-1"])] => false ; "7")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-*", "ap-southeast-1"]), false, vec![] => false ; "8")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-*", "ap-southeast-1"]), false, vec![("delimiter", vec!["/"])] => false ; "9")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-*", "ap-southeast-1"]), false, vec![("LocationConstraint", vec!["eu-west-2"])] => true ; "10")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art*"]), true, vec![("groups", vec!["prod", "art"])] => true ; "11")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art*"]), true, vec![("groups", vec!["art"])] => true ; "12")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art*"]), true, vec![] => true ; "13")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art*"]), true, vec![("delimiter", vec!["/"])] => true ; "14")]
    #[test_case(new_fkv("jwt:groups", vec!["prod*", "art"]), false, vec![("groups", vec!["prod", "art"])] => true ; "15")]
    #[test_case(new_fkv("jwt:groups", vec!["prod*", "art"]), false, vec![("groups", vec!["art"])] => true ; "16")]
    #[test_case(new_fkv("jwt:groups", vec!["prod*", "art"]), false, vec![] => false ; "17")]
    #[test_case(new_fkv("jwt:groups", vec!["prod*", "art"]), false, vec![("delimiter", vec!["/"])] => false ; "18")]
    fn test_string_like(s: FuncKeyValue<StringFuncValue>, for_all: bool, values: Vec<(&str, Vec<&str>)>) -> bool {
        test_eval_like(s, for_all, false, values)
    }

    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![("x-amz-copy-source", vec!["mybucket/myobject"])] => false ; "1")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![("x-amz-copy-source", vec!["yourbucket/myobject"])] => true ; "2")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![] => true ; "3")]
    #[test_case(new_fkv("s3:x-amz-copy-source", vec!["mybucket/myobject"]), false, vec![("delimiter", vec!["/"])] => true ; "4")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-*", "ap-southeast-1"]), false, vec![("LocationConstraint", vec!["eu-west-1"])] => false ; "5")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-*", "ap-southeast-1"]), false, vec![("LocationConstraint", vec!["ap-southeast-1"])] => false ; "6")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-*", "ap-southeast-1"]), false, vec![("LocationConstraint", vec!["us-east-1"])] => true ; "7")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-*", "ap-southeast-1"]), false, vec![] => true ; "8")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-*", "ap-southeast-1"]), false, vec![("delimiter", vec!["/"])] => true ; "9")]
    #[test_case(new_fkv("s3:LocationConstraint", vec!["eu-west-*", "ap-southeast-1"]), false, vec![("LocationConstraint", vec!["eu-west-2"])] => false ; "10")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art*"]), true, vec![("groups", vec!["prod", "art"])] => false ; "11")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art*"]), true, vec![("groups", vec!["art"])] => false ; "12")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art*"]), true, vec![] => false ; "13")]
    #[test_case(new_fkv("jwt:groups", vec!["prod", "art*"]), true, vec![("delimiter", vec!["/"])] => false ; "14")]
    #[test_case(new_fkv("jwt:groups", vec!["prod*", "art"]), false, vec![("groups", vec!["prod", "art"])] => false ; "15")]
    #[test_case(new_fkv("jwt:groups", vec!["prod*", "art"]), false, vec![("groups", vec!["art"])] => false ; "16")]
    #[test_case(new_fkv("jwt:groups", vec!["prod*", "art"]), false, vec![] => true ; "17")]
    #[test_case(new_fkv("jwt:groups", vec!["prod*", "art"]), false, vec![("delimiter", vec!["/"])] => true ; "18")]
    fn test_string_not_like(s: FuncKeyValue<StringFuncValue>, for_all: bool, values: Vec<(&str, Vec<&str>)>) -> bool {
        test_eval_like(s, for_all, true, values)
    }
}
