use super::func::InnerFunc;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::{collections::HashMap, fmt};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

pub type DateFunc = InnerFunc<DateFuncValue>;

impl DateFunc {
    pub fn evaluate(&self, op: impl Fn(&OffsetDateTime, &OffsetDateTime) -> bool, values: &HashMap<String, Vec<String>>) -> bool {
        for inner in self.0.iter() {
            let v = match values.get(inner.key.name().as_str()).and_then(|x| x.first()) {
                Some(x) => x,
                None => return false,
            };

            let Ok(rv) = OffsetDateTime::parse(v, &Rfc3339) else {
                return false;
            };

            if !op(&inner.values.0, &rv) {
                return false;
            }
        }

        true
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DateFuncValue(OffsetDateTime);

impl Serialize for DateFuncValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;
        serializer.serialize_str(
            &self
                .0
                .format(&Rfc3339)
                .map_err(|e| Error::custom(format!("format datetime failed: {e:?}")))?,
        )
    }
}

impl<'de> Deserialize<'de> for DateFuncValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DateVisitor;

        impl de::Visitor<'_> for DateVisitor {
            type Value = DateFuncValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a data string that is representable in RFC 3339 format.")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(DateFuncValue(
                    OffsetDateTime::parse(value, &Rfc3339).map_err(|e| E::custom(format!("{e:?}")))?,
                ))
            }
        }

        deserializer.deserialize_str(DateVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::{DateFunc, DateFuncValue};
    use crate::policy::function::func::FuncKeyValue;
    use crate::policy::function::{
        key::Key,
        key_name::KeyName::{self, *},
        key_name::S3KeyName::*,
    };
    use test_case::test_case;
    use time::{format_description::well_known::Rfc3339, OffsetDateTime};

    fn new_func(name: KeyName, variable: Option<String>, value: &str) -> DateFunc {
        DateFunc {
            0: vec![FuncKeyValue {
                key: Key { name, variable },
                values: DateFuncValue(OffsetDateTime::parse(value, &Rfc3339).unwrap()),
            }],
        }
    }

    #[test_case(r#"{"s3:object-lock-retain-until-date": "2009-11-10T15:00:00Z"}"#, new_func(S3(S3ObjectLockRetainUntilDate), None, "2009-11-10T15:00:00Z"); "1")]
    #[test_case(r#"{"s3:object-lock-retain-until-date/a": "2009-11-10T15:00:00Z"}"#, new_func(S3(S3ObjectLockRetainUntilDate), Some("a".into()), "2009-11-10T15:00:00Z"); "2")]
    fn test_deser(input: &str, expect: DateFunc) -> Result<(), serde_json::Error> {
        let v: DateFunc = serde_json::from_str(input)?;
        assert_eq!(v, expect);
        Ok(())
    }

    #[test_case(r#"{"s3:object-lock-retain-until-date":"2009-11-10T15:00:00Z"}"#, new_func(S3(S3ObjectLockRetainUntilDate), None, "2009-11-10T15:00:00Z"); "1")]
    #[test_case(r#"{"s3:object-lock-retain-until-date/a":"2009-11-10T15:00:00Z"}"#, new_func(S3(S3ObjectLockRetainUntilDate), Some("a".into()), "2009-11-10T15:00:00Z"); "2")]
    fn test_ser(expect: &str, input: DateFunc) -> Result<(), serde_json::Error> {
        let v = serde_json::to_string(&input)?;
        assert_eq!(v, expect);
        Ok(())
    }
}
