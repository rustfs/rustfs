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

use crate::policy::variables::PolicyVariableResolver;
use serde::Deserialize;
use serde::de::{Error, MapAccess};
use serde::ser::SerializeMap;
use std::collections::HashMap;
use time::OffsetDateTime;

use super::{addr::AddrFunc, binary::BinaryFunc, bool_null::BoolFunc, date::DateFunc, number::NumberFunc, string::StringFunc};

#[derive(Clone, Deserialize, Debug)]
pub enum Condition {
    StringEquals(StringFunc),
    StringNotEquals(StringFunc),
    StringEqualsIgnoreCase(StringFunc),
    StringNotEqualsIgnoreCase(StringFunc),
    StringLike(StringFunc),
    StringNotLike(StringFunc),
    ArnLike(StringFunc),
    ArnNotLike(StringFunc),
    ArnEquals(StringFunc),
    ArnNotEquals(StringFunc),
    BinaryEquals(BinaryFunc),
    IpAddress(AddrFunc),
    NotIpAddress(AddrFunc),
    Null(BoolFunc),
    Bool(BoolFunc),
    NumericEquals(NumberFunc),
    NumericNotEquals(NumberFunc),
    NumericLessThan(NumberFunc),
    NumericLessThanEquals(NumberFunc),
    NumericGreaterThan(NumberFunc),
    NumericGreaterThanIfExists(NumberFunc),
    NumericGreaterThanEquals(NumberFunc),
    DateEquals(DateFunc),
    DateNotEquals(DateFunc),
    DateLessThan(DateFunc),
    DateLessThanEquals(DateFunc),
    DateGreaterThan(DateFunc),
    DateGreaterThanEquals(DateFunc),
    /// Wraps any condition with IfExists semantics: if none of the
    /// referenced keys are present in the request context, the
    /// condition evaluates to true.
    IfExists(Box<Condition>),
}

impl Condition {
    pub fn from_deserializer<'a, D: MapAccess<'a>>(key: &str, d: &mut D) -> Result<Self, D::Error> {
        Ok(match key {
            "StringEquals" => Self::StringEquals(d.next_value()?),
            "StringNotEquals" => Self::StringNotEquals(d.next_value()?),
            "StringEqualsIgnoreCase" => Self::StringEqualsIgnoreCase(d.next_value()?),
            "StringNotEqualsIgnoreCase" => Self::StringNotEqualsIgnoreCase(d.next_value()?),
            "StringLike" => Self::StringLike(d.next_value()?),
            "StringNotLike" => Self::StringNotLike(d.next_value()?),
            "ArnLike" => Self::ArnLike(d.next_value()?),
            "ArnNotLike" => Self::ArnNotLike(d.next_value()?),
            "ArnEquals" => Self::ArnEquals(d.next_value()?),
            "ArnNotEquals" => Self::ArnNotEquals(d.next_value()?),
            "BinaryEquals" => Self::BinaryEquals(d.next_value()?),
            "IpAddress" => Self::IpAddress(d.next_value()?),
            "NotIpAddress" => Self::NotIpAddress(d.next_value()?),
            "Null" => Self::Null(d.next_value()?),
            "Bool" => Self::Bool(d.next_value()?),
            "NumericEquals" => Self::NumericEquals(d.next_value()?),
            "NumericNotEquals" => Self::NumericNotEquals(d.next_value()?),
            "NumericLessThan" => Self::NumericLessThan(d.next_value()?),
            "NumericLessThanEquals" => Self::NumericLessThanEquals(d.next_value()?),
            "NumericGreaterThan" => Self::NumericGreaterThan(d.next_value()?),
            "NumericGreaterThanIfExists" => Self::NumericGreaterThanIfExists(d.next_value()?),
            "NumericGreaterThanEquals" => Self::NumericGreaterThanEquals(d.next_value()?),
            "DateEquals" => Self::DateEquals(d.next_value()?),
            "DateNotEquals" => Self::DateNotEquals(d.next_value()?),
            "DateLessThan" => Self::DateLessThan(d.next_value()?),
            "DateLessThanEquals" => Self::DateLessThanEquals(d.next_value()?),
            "DateGreaterThan" => Self::DateGreaterThan(d.next_value()?),
            "DateGreaterThanEquals" => Self::DateGreaterThanEquals(d.next_value()?),
            _ if key.ends_with("IfExists") => {
                let base = key.strip_suffix("IfExists").unwrap_or(key);
                let inner = Self::from_deserializer(base, d)?;
                Self::IfExists(Box::new(inner))
            }
            _ => Err(Error::custom(format!("unknown key: {key}")))?,
        })
    }

    pub fn to_key(&self) -> &'static str {
        match self {
            Condition::StringEquals(_) => "StringEquals",
            Condition::StringNotEquals(_) => "StringNotEquals",
            Condition::StringEqualsIgnoreCase(_) => "StringEqualsIgnoreCase",
            Condition::StringNotEqualsIgnoreCase(_) => "StringNotEqualsIgnoreCase",
            Condition::StringLike(_) => "StringLike",
            Condition::StringNotLike(_) => "StringNotLike",
            Condition::ArnLike(_) => "ArnLike",
            Condition::ArnNotLike(_) => "ArnNotLike",
            Condition::ArnEquals(_) => "ArnEquals",
            Condition::ArnNotEquals(_) => "ArnNotEquals",
            Condition::BinaryEquals(_) => "BinaryEquals",
            Condition::IpAddress(_) => "IpAddress",
            Condition::NotIpAddress(_) => "NotIpAddress",
            Condition::Null(_) => "Null",
            Condition::Bool(_) => "Bool",
            Condition::NumericEquals(_) => "NumericEquals",
            Condition::NumericNotEquals(_) => "NumericNotEquals",
            Condition::NumericLessThan(_) => "NumericLessThan",
            Condition::NumericLessThanEquals(_) => "NumericLessThanEquals",
            Condition::NumericGreaterThan(_) => "NumericGreaterThan",
            Condition::NumericGreaterThanIfExists(_) => "NumericGreaterThanIfExists",
            Condition::NumericGreaterThanEquals(_) => "NumericGreaterThanEquals",
            Condition::DateEquals(_) => "DateEquals",
            Condition::DateNotEquals(_) => "DateNotEquals",
            Condition::DateLessThan(_) => "DateLessThan",
            Condition::DateLessThanEquals(_) => "DateLessThanEquals",
            Condition::DateGreaterThan(_) => "DateGreaterThan",
            Condition::DateGreaterThanEquals(_) => "DateGreaterThanEquals",
            Condition::IfExists(_) => "IfExists",
        }
    }

    pub fn to_key_with_suffix(&self) -> String {
        match self {
            Condition::IfExists(inner) => format!("{}IfExists", inner.to_key_with_suffix()),
            _ => self.to_key().to_owned(),
        }
    }

    pub fn has_any_key_in(&self, values: &HashMap<String, Vec<String>>) -> bool {
        use Condition::*;
        match self {
            StringEquals(s)
            | StringNotEquals(s)
            | StringEqualsIgnoreCase(s)
            | StringNotEqualsIgnoreCase(s)
            | StringLike(s)
            | StringNotLike(s)
            | ArnLike(s)
            | ArnNotLike(s)
            | ArnEquals(s)
            | ArnNotEquals(s) => s.key_names().any(|k| values.contains_key(k.as_str())),
            BinaryEquals(s) => s.key_names().any(|k| values.contains_key(k.as_str())),
            IpAddress(s) | NotIpAddress(s) => s.key_names().any(|k| values.contains_key(k.as_str())),
            Null(s) | Bool(s) => s.key_names().any(|k| values.contains_key(k.as_str())),
            NumericEquals(s)
            | NumericNotEquals(s)
            | NumericLessThan(s)
            | NumericLessThanEquals(s)
            | NumericGreaterThan(s)
            | NumericGreaterThanIfExists(s)
            | NumericGreaterThanEquals(s) => s.key_names().any(|k| values.contains_key(k.as_str())),
            DateEquals(s)
            | DateNotEquals(s)
            | DateLessThan(s)
            | DateLessThanEquals(s)
            | DateGreaterThan(s)
            | DateGreaterThanEquals(s) => s.key_names().any(|k| values.contains_key(k.as_str())),
            IfExists(inner) => inner.has_any_key_in(values),
        }
    }

    pub fn evaluate_with_resolver<'a>(
        &'a self,
        for_all: bool,
        values: &'a HashMap<String, Vec<String>>,
        resolver: Option<&'a dyn PolicyVariableResolver>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            use Condition::*;

            let r = match self {
                StringEquals(s) => s.evaluate_with_resolver(for_all, false, false, false, values, resolver).await,
                StringNotEquals(s) => s.evaluate_with_resolver(for_all, false, false, true, values, resolver).await,
                StringEqualsIgnoreCase(s) => s.evaluate_with_resolver(for_all, true, false, false, values, resolver).await,
                StringNotEqualsIgnoreCase(s) => s.evaluate_with_resolver(for_all, true, false, true, values, resolver).await,
                StringLike(s) => s.evaluate_with_resolver(for_all, false, true, false, values, resolver).await,
                StringNotLike(s) => s.evaluate_with_resolver(for_all, false, true, true, values, resolver).await,
                ArnLike(s) => s.evaluate_with_resolver(for_all, false, true, false, values, resolver).await,
                ArnNotLike(s) => s.evaluate_with_resolver(for_all, false, true, true, values, resolver).await,
                ArnEquals(s) => s.evaluate_with_resolver(for_all, false, false, false, values, resolver).await,
                ArnNotEquals(s) => s.evaluate_with_resolver(for_all, false, false, true, values, resolver).await,
                BinaryEquals(s) => s.evaluate(values),
                IpAddress(s) => s.evaluate(values),
                NotIpAddress(s) => s.evaluate(values),
                Null(s) => s.evaluate_null(values),
                Bool(s) => s.evaluate_bool(values),
                NumericEquals(s) => s.evaluate(i64::eq, false, values),
                NumericNotEquals(s) => s.evaluate(i64::ne, false, values),
                NumericLessThan(s) => s.evaluate(i64::lt, false, values),
                NumericLessThanEquals(s) => s.evaluate(i64::le, false, values),
                NumericGreaterThan(s) => s.evaluate(i64::gt, false, values),
                NumericGreaterThanIfExists(s) => s.evaluate(i64::ge, true, values),
                NumericGreaterThanEquals(s) => s.evaluate(i64::ge, false, values),
                DateEquals(s) => s.evaluate(OffsetDateTime::eq, values),
                DateNotEquals(s) => s.evaluate(OffsetDateTime::ne, values),
                DateLessThan(s) => s.evaluate(OffsetDateTime::lt, values),
                DateLessThanEquals(s) => s.evaluate(OffsetDateTime::le, values),
                DateGreaterThan(s) => s.evaluate(OffsetDateTime::gt, values),
                DateGreaterThanEquals(s) => s.evaluate(OffsetDateTime::ge, values),
                IfExists(inner) => {
                    if !inner.has_any_key_in(values) {
                        return true;
                    }
                    return inner.evaluate_with_resolver(for_all, values, resolver).await;
                }
            };

            if self.is_negate() { !r } else { r }
        })
    }

    #[inline]
    pub fn is_negate(&self) -> bool {
        use Condition::*;
        // StringNotEquals/StringNotEqualsIgnoreCase handle negation via the
        // `negate` parameter in `evaluate_with_resolver`; do NOT negate again here.
        matches!(self, NotIpAddress(_))
    }

    pub fn serialize_map<T: SerializeMap>(&self, se: &mut T) -> Result<(), T::Error> {
        match self {
            Condition::StringEquals(s) => se.serialize_value(s),
            Condition::StringNotEquals(s) => se.serialize_value(s),
            Condition::StringEqualsIgnoreCase(s) => se.serialize_value(s),
            Condition::StringNotEqualsIgnoreCase(s) => se.serialize_value(s),
            Condition::StringLike(s) => se.serialize_value(s),
            Condition::StringNotLike(s) => se.serialize_value(s),
            Condition::ArnLike(s) => se.serialize_value(s),
            Condition::ArnNotLike(s) => se.serialize_value(s),
            Condition::ArnEquals(s) => se.serialize_value(s),
            Condition::ArnNotEquals(s) => se.serialize_value(s),
            Condition::BinaryEquals(s) => se.serialize_value(s),
            Condition::IpAddress(s) => se.serialize_value(s),
            Condition::NotIpAddress(s) => se.serialize_value(s),
            Condition::Null(s) => se.serialize_value(s),
            Condition::Bool(s) => se.serialize_value(s),
            Condition::NumericEquals(s) => se.serialize_value(s),
            Condition::NumericNotEquals(s) => se.serialize_value(s),
            Condition::NumericLessThan(s) => se.serialize_value(s),
            Condition::NumericLessThanEquals(s) => se.serialize_value(s),
            Condition::NumericGreaterThan(s) => se.serialize_value(s),
            Condition::NumericGreaterThanIfExists(s) => se.serialize_value(s),
            Condition::NumericGreaterThanEquals(s) => se.serialize_value(s),
            Condition::DateEquals(s) => se.serialize_value(s),
            Condition::DateNotEquals(s) => se.serialize_value(s),
            Condition::DateLessThan(s) => se.serialize_value(s),
            Condition::DateLessThanEquals(s) => se.serialize_value(s),
            Condition::DateGreaterThan(s) => se.serialize_value(s),
            Condition::DateGreaterThanEquals(s) => se.serialize_value(s),
            Condition::IfExists(inner) => inner.serialize_map(se),
        }
    }
}

impl PartialEq for Condition {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::StringEquals(l0), Self::StringEquals(r0)) => l0 == r0,
            (Self::StringNotEquals(l0), Self::StringNotEquals(r0)) => l0 == r0,
            (Self::StringEqualsIgnoreCase(l0), Self::StringEqualsIgnoreCase(r0)) => l0 == r0,
            (Self::StringNotEqualsIgnoreCase(l0), Self::StringNotEqualsIgnoreCase(r0)) => l0 == r0,
            (Self::StringLike(l0), Self::StringLike(r0)) => l0 == r0,
            (Self::StringNotLike(l0), Self::StringNotLike(r0)) => l0 == r0,
            (Self::BinaryEquals(l0), Self::BinaryEquals(r0)) => l0 == r0,
            (Self::IpAddress(l0), Self::IpAddress(r0)) => l0 == r0,
            (Self::NotIpAddress(l0), Self::NotIpAddress(r0)) => l0 == r0,
            (Self::Null(l0), Self::Null(r0)) => l0 == r0,
            (Self::Bool(l0), Self::Bool(r0)) => l0 == r0,
            (Self::NumericEquals(l0), Self::NumericEquals(r0)) => l0 == r0,
            (Self::NumericNotEquals(l0), Self::NumericNotEquals(r0)) => l0 == r0,
            (Self::NumericLessThan(l0), Self::NumericLessThan(r0)) => l0 == r0,
            (Self::NumericLessThanEquals(l0), Self::NumericLessThanEquals(r0)) => l0 == r0,
            (Self::NumericGreaterThan(l0), Self::NumericGreaterThan(r0)) => l0 == r0,
            (Self::NumericGreaterThanIfExists(l0), Self::NumericGreaterThanIfExists(r0)) => l0 == r0,
            (Self::NumericGreaterThanEquals(l0), Self::NumericGreaterThanEquals(r0)) => l0 == r0,
            (Self::DateEquals(l0), Self::DateEquals(r0)) => l0 == r0,
            (Self::DateNotEquals(l0), Self::DateNotEquals(r0)) => l0 == r0,
            (Self::DateLessThan(l0), Self::DateLessThan(r0)) => l0 == r0,
            (Self::DateLessThanEquals(l0), Self::DateLessThanEquals(r0)) => l0 == r0,
            (Self::DateGreaterThan(l0), Self::DateGreaterThan(r0)) => l0 == r0,
            (Self::DateGreaterThanEquals(l0), Self::DateGreaterThanEquals(r0)) => l0 == r0,
            (Self::ArnLike(l0), Self::ArnLike(r0)) => l0 == r0,
            (Self::ArnNotLike(l0), Self::ArnNotLike(r0)) => l0 == r0,
            (Self::ArnEquals(l0), Self::ArnEquals(r0)) => l0 == r0,
            (Self::ArnNotEquals(l0), Self::ArnNotEquals(r0)) => l0 == r0,
            (Self::IfExists(l0), Self::IfExists(r0)) => l0 == r0,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::function::{
        func::{FuncKeyValue, InnerFunc},
        key::Key,
        string::StringFuncValue,
    };
    use std::collections::{BTreeSet, HashMap};

    fn make_string_condition(condition_type: &str, key: &str, value: &str) -> Condition {
        let func: StringFunc = InnerFunc(vec![FuncKeyValue {
            key: Key {
                name: key.try_into().unwrap(),
                variable: None,
            },
            values: StringFuncValue({
                let mut s = BTreeSet::new();
                s.insert(value.to_string());
                s
            }),
        }]);
        match condition_type {
            "StringEquals" => Condition::StringEquals(func),
            "StringNotEquals" => Condition::StringNotEquals(func),
            "StringNotEqualsIgnoreCase" => Condition::StringNotEqualsIgnoreCase(func),
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_string_not_equals_no_double_negation() {
        let cond = make_string_condition("StringNotEquals", "s3:x-amz-server-side-encryption", "aws:kms");

        let mut values = HashMap::new();
        values.insert("x-amz-server-side-encryption".to_string(), vec!["AES256".to_string()]);

        // "AES256" != "aws:kms" is true, so StringNotEquals should evaluate to true
        assert!(
            cond.evaluate_with_resolver(false, &values, None).await,
            "StringNotEquals should be true when values differ"
        );

        values.insert("x-amz-server-side-encryption".to_string(), vec!["aws:kms".to_string()]);

        // "aws:kms" != "aws:kms" is false, so StringNotEquals should evaluate to false
        assert!(
            !cond.evaluate_with_resolver(false, &values, None).await,
            "StringNotEquals should be false when values match"
        );
    }

    #[tokio::test]
    async fn test_string_equals_condition() {
        let cond = make_string_condition("StringEquals", "s3:x-amz-server-side-encryption", "aws:kms");

        let mut values = HashMap::new();
        values.insert("x-amz-server-side-encryption".to_string(), vec!["aws:kms".to_string()]);

        assert!(
            cond.evaluate_with_resolver(false, &values, None).await,
            "StringEquals should be true when values match"
        );

        values.insert("x-amz-server-side-encryption".to_string(), vec!["AES256".to_string()]);

        assert!(
            !cond.evaluate_with_resolver(false, &values, None).await,
            "StringEquals should be false when values differ"
        );
    }

    #[tokio::test]
    async fn test_string_not_equals_absent_key() {
        let cond = make_string_condition("StringNotEquals", "s3:x-amz-server-side-encryption", "aws:kms");

        let values = HashMap::new();

        // Key absent: rvalues is empty, intersection is empty.
        // for_all=false: ivalues.count() > 0 → false. Negated → true.
        assert!(
            cond.evaluate_with_resolver(false, &values, None).await,
            "StringNotEquals should be true when key is absent"
        );
    }
}
