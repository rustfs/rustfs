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

use super::func::InnerFunc;
use super::key_name::{AwsKeyName, KeyName};
use serde::{Deserialize, Deserializer, Serialize, de};
use std::{collections::HashMap, fmt};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub type DateFunc = InnerFunc<DateFuncValue>;

impl DateFunc {
    pub fn evaluate(
        &self,
        op: impl Fn(&OffsetDateTime, &OffsetDateTime) -> bool,
        absent: bool,
        values: &HashMap<String, Vec<String>>,
    ) -> bool {
        for inner in self.0.iter() {
            let v = match values.get(inner.key.name().as_str()).and_then(|x| x.first()) {
                Some(x) => x,
                None if absent => continue,
                None => return false,
            };

            let parsed = if inner.key.is(&KeyName::Aws(AwsKeyName::AWSEpochTime)) {
                v.parse::<i64>()
                    .ok()
                    .and_then(|seconds| OffsetDateTime::from_unix_timestamp(seconds).ok())
            } else {
                OffsetDateTime::parse(v, &Rfc3339).ok()
            };
            let Some(rv) = parsed else {
                return false;
            };

            if !op(&rv, &inner.values.0) {
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
    use super::{AwsKeyName, DateFunc, DateFuncValue};
    use crate::policy::function::func::FuncKeyValue;
    use crate::policy::function::{
        key::Key,
        key_name::KeyName::{self, *},
        key_name::S3KeyName::*,
    };
    use std::collections::HashMap;
    use test_case::test_case;
    use time::{OffsetDateTime, format_description::well_known::Rfc3339};

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

    #[test]
    fn date_conditions_compare_epoch_seconds_only_for_epoch_time() {
        let function = new_func(KeyName::Aws(AwsKeyName::AWSEpochTime), None, "2020-01-01T00:00:00Z");
        for (value, equal, later) in [
            ("1577836799", false, false),
            ("1577836800", true, false),
            ("1577836801", false, true),
        ] {
            let values = HashMap::from([("EpochTime".to_string(), vec![value.to_string()])]);
            assert_eq!(function.evaluate(OffsetDateTime::eq, false, &values), equal);
            assert_eq!(function.evaluate(OffsetDateTime::gt, false, &values), later);
            assert_eq!(function.evaluate(OffsetDateTime::lt, false, &values), !equal && !later);
        }
        for invalid in ["2020-01-01T00:00:00Z", "invalid", "9223372036854775807"] {
            let values = HashMap::from([("EpochTime".to_string(), vec![invalid.to_string()])]);
            assert!(!function.evaluate(OffsetDateTime::ne, true, &values));
        }
        let header_date = new_func(S3(S3ObjectLockRetainUntilDate), None, "2020-01-01T00:00:00Z");
        let values = HashMap::from([("object-lock-retain-until-date".to_string(), vec!["1577836800".to_string()])]);
        assert!(!header_date.evaluate(OffsetDateTime::eq, false, &values));
    }

    #[test]
    fn absent_date_key_does_not_skip_other_keys() {
        let mut function = new_func(S3(S3ObjectLockRetainUntilDate), None, "2030-01-01T00:00:00Z");
        function
            .0
            .extend(new_func(KeyName::Aws(AwsKeyName::AWSCurrentTime), None, "2030-01-01T00:00:00Z").0);
        let values = HashMap::from([("CurrentTime".to_string(), vec!["2030-01-01T00:00:00Z".to_string()])]);
        assert!(!function.evaluate(OffsetDateTime::ne, true, &values));
        assert!(function.evaluate(OffsetDateTime::eq, true, &values));
        function.0.reverse();
        assert!(!function.evaluate(OffsetDateTime::ne, true, &values));
    }

    #[test]
    fn evaluate_compares_request_date_to_policy_date() {
        let function = new_func(S3(S3ObjectLockRetainUntilDate), None, "2030-01-01T00:00:00Z");
        let later = HashMap::from([("object-lock-retain-until-date".to_string(), vec!["2099-01-01T00:00:00Z".to_string()])]);
        let earlier = HashMap::from([("object-lock-retain-until-date".to_string(), vec!["2029-01-01T00:00:00Z".to_string()])]);

        assert!(function.evaluate(OffsetDateTime::gt, false, &later));
        assert!(!function.evaluate(OffsetDateTime::gt, false, &earlier));
        assert!(function.evaluate(OffsetDateTime::lt, false, &earlier));
        assert!(!function.evaluate(OffsetDateTime::lt, false, &later));
    }
}
