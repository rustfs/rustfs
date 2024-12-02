#[cfg(test)]
use std::collections::BTreeSet as Set;
#[cfg(not(test))]
use std::collections::HashSet as Set;
use std::fmt;
use std::{borrow::Cow, collections::HashMap};

use serde::{de, ser::SerializeSeq, Deserialize, Deserializer, Serialize};

use crate::policy::utils::wildcard;

use super::{func::InnerFunc, key_name::KeyName};

pub type StringFunc = InnerFunc<StringFuncValue>;

impl StringFunc {
    fn eval(&self, for_all: bool, ignore_case: bool, values: &HashMap<String, Vec<String>>) -> bool {
        let rvalues = values
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

        let fvalues = self
            .values
            .0
            .iter()
            .map(|c| {
                let mut c = Cow::from(c);
                for key in KeyName::COMMON_KEYS {
                    match values.get(key.name()).and_then(|x| x.get(0)) {
                        Some(v) if !v.is_empty() => return Cow::Owned(c.to_mut().replace(key.name(), v)),
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

    fn eval_like(&self, for_all: bool, values: &HashMap<String, Vec<String>>) -> bool {
        if let Some(rvalues) = values.get(self.key.name().as_str()) {
            for v in rvalues.iter() {
                let matched = self
                    .values
                    .0
                    .iter()
                    .map(|c| {
                        let mut c = Cow::from(c);
                        for key in KeyName::COMMON_KEYS {
                            match values.get(key.name()).and_then(|x| x.get(0)) {
                                Some(v) if !v.is_empty() => return Cow::Owned(c.to_mut().replace(key.name(), v)),
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

    pub(crate) fn evaluate(&self, for_all: bool, ignore_case: bool, like: bool, values: &HashMap<String, Vec<String>>) -> bool {
        if like {
            self.eval_like(for_all, values)
        } else {
            self.eval(for_all, ignore_case, values)
        }
    }
}

/// 解析values字段
#[derive(Clone)]
#[cfg_attr(test, derive(PartialEq, Eq, Debug))]
pub struct StringFuncValue(Set<String>);

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

            return Err(D::Error::custom("empty"));
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::{StringFunc, StringFuncValue};
    use crate::policy::function::{
        key::Key,
        key_name::AwsKeyName::*,
        key_name::KeyName::{self, *},
    };
    use test_case::test_case;

    fn new_func(name: KeyName, variable: Option<String>, values: Vec<&str>) -> StringFunc {
        StringFunc {
            key: Key { name, variable },
            values: StringFuncValue(values.into_iter().map(|x| x.to_owned()).collect()),
        }
    }

    #[test_case(r#"{"aws:username": "johndoe"}"#, new_func(Aws(AWSUsername), None, vec!["johndoe"]))]
    #[test_case(r#"{"aws:username": ["johndoe", "aaa"]}"#, new_func(Aws(AWSUsername), None, vec!["johndoe", "aaa"]))]
    #[test_case(r#"{"aws:username/value": "johndoe"}"#, new_func(Aws(AWSUsername), Some("value".into()), vec!["johndoe"]))]
    #[test_case(r#"{"aws:username/value": ["johndoe", "aaa"]}"#, new_func(Aws(AWSUsername), Some("value".into()), vec!["johndoe", "aaa"]))]
    fn test_deser(input: &str, expect: StringFunc) -> Result<(), serde_json::Error> {
        let v: StringFunc = serde_json::from_str(input)?;
        assert_eq!(v, expect);
        Ok(())
    }

    #[test_case(r#"{"aws:usernamea":"johndoe"}"#)]
    #[test_case(r#"{"aws:username":[]}"#)] // 空
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
}
