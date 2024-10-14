use super::func::InnerFunc;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::{collections::HashMap, fmt};

pub type BoolFunc = InnerFunc<BoolFuncValue>;
impl BoolFunc {
    pub fn evaluate_bool(&self, values: &HashMap<String, Vec<String>>) -> bool {
        match values.get(self.key.name().as_str()).and_then(|x| x.get(0)) {
            Some(x) => self.values.0.to_string().as_str() == x,
            None => false,
        }
    }

    pub fn evaluate_null(&self, values: &HashMap<String, Vec<String>>) -> bool {
        let len = values.get(self.key.name().as_str()).map(Vec::len).unwrap_or(0);
        if self.values.0 {
            return len == 0;
        }

        len != 0
    }
}

#[derive(Clone)]
#[cfg_attr(test, derive(PartialEq, Eq, Debug))]
pub struct BoolFuncValue(bool);

impl Serialize for BoolFuncValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for BoolFuncValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BoolOrStringVisitor;

        impl<'de> de::Visitor<'de> for BoolOrStringVisitor {
            type Value = BoolFuncValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a boolean or a string representing 'true' or 'false'")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(BoolFuncValue(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(BoolFuncValue(value.parse::<bool>().map_err(|e| E::custom(format!("{e:?}")))?))
            }
        }

        deserializer.deserialize_any(BoolOrStringVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::{BoolFunc, BoolFuncValue};
    use crate::policy::function::{
        key::Key,
        key_name::AwsKeyName::*,
        key_name::KeyName::{self, *},
    };
    use test_case::test_case;

    fn new_func(name: KeyName, variable: Option<String>, value: bool) -> BoolFunc {
        BoolFunc {
            key: Key { name, variable },
            values: BoolFuncValue(value),
        }
    }

    #[test_case(r#"{"aws:SecureTransport": "true"}"#, new_func(Aws(AWSSecureTransport), None, true); "1")]
    #[test_case(r#"{"aws:SecureTransport": "false"}"#, new_func(Aws(AWSSecureTransport), None, false); "2")]
    #[test_case(r#"{"aws:SecureTransport": true}"#, new_func(Aws(AWSSecureTransport), None, true); "3")]
    #[test_case(r#"{"aws:SecureTransport": false}"#, new_func(Aws(AWSSecureTransport), None, false); "4")]
    #[test_case(r#"{"aws:SecureTransport/a": "true"}"#, new_func(Aws(AWSSecureTransport), Some("a".into()), true); "9")]
    #[test_case(r#"{"aws:SecureTransport/a": "false"}"#, new_func(Aws(AWSSecureTransport), Some("a".into()), false); "10")]
    #[test_case(r#"{"aws:SecureTransport/a": true}"#, new_func(Aws(AWSSecureTransport), Some("a".into()), true); "11")]
    #[test_case(r#"{"aws:SecureTransport/a": false}"#, new_func(Aws(AWSSecureTransport), Some("a".into()), false); "12")]
    fn test_deser(input: &str, expect: BoolFunc) -> Result<(), serde_json::Error> {
        let v: BoolFunc = serde_json::from_str(input)?;
        assert_eq!(v, expect);
        Ok(())
    }

    #[test_case(r#"{"aws:usernamea":"johndoe"}"#)]
    #[test_case(r#"{"aws:username":[]}"#)] // 空
    #[test_case(r#"{"aws:usernamea/value":"johndoe"}"#)]
    #[test_case(r#"{"aws:usernamea/value":["johndoe", "aaa"]}"#)]
    #[test_case(r#""aaa""#)]
    fn test_deser_failed(input: &str) {
        assert!(serde_json::from_str::<BoolFunc>(input).is_err());
    }

    #[test_case(r#"{"aws:SecureTransport":"true"}"#, new_func(Aws(AWSSecureTransport), None, true); "1")]
    #[test_case(r#"{"aws:SecureTransport":"false"}"#, new_func(Aws(AWSSecureTransport), None, false);"2")]
    #[test_case(r#"{"aws:SecureTransport/aa":"true"}"#, new_func(Aws(AWSSecureTransport),Some("aa".into()), true);"3")]
    #[test_case(r#"{"aws:SecureTransport/aa":"false"}"#, new_func(Aws(AWSSecureTransport), Some("aa".into()), false);"4")]
    fn test_ser(expect: &str, input: BoolFunc) -> Result<(), serde_json::Error> {
        let v = serde_json::to_string(&input)?;
        assert_eq!(v.as_str(), expect);
        Ok(())
    }
}
