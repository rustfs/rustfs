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
            "BinaryEquals" => Self::BinaryEquals(d.next_value()?),
            "IpAddress" => Self::IpAddress(d.next_value()?),
            "NotIpAddress" => Self::NotIpAddress(d.next_value()?),
            "Null" => Self::Null(d.next_value()?),
            "Bool" => Self::Bool(d.next_value()?),
            "NumericEquals" => Self::NumericEquals(d.next_value()?),
            "NumericNotEquals" => Self::NumericNotEquals(d.next_value()?),
            "NumericLessThan" => Self::NumericLessThan(d.next_value()?),
            "NumericGreaterThan" => Self::NumericGreaterThan(d.next_value()?),
            "NumericGreaterThanIfExists" => Self::NumericGreaterThanIfExists(d.next_value()?),
            "NumericGreaterThanEquals" => Self::NumericGreaterThanEquals(d.next_value()?),
            "DateEquals" => Self::DateEquals(d.next_value()?),
            "DateNotEquals" => Self::DateNotEquals(d.next_value()?),
            "DateLessThanEquals" => Self::DateLessThanEquals(d.next_value()?),
            "DateGreaterThan" => Self::DateGreaterThan(d.next_value()?),
            "DateGreaterThanEquals" => Self::DateGreaterThanEquals(d.next_value()?),
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
        }
    }

    pub async fn evaluate_with_resolver(
        &self,
        for_all: bool,
        values: &HashMap<String, Vec<String>>,
        resolver: Option<&dyn PolicyVariableResolver>,
    ) -> bool {
        use Condition::*;

        let r = match self {
            StringEquals(s) => s.evaluate_with_resolver(for_all, false, false, false, values, resolver).await,
            StringNotEquals(s) => s.evaluate_with_resolver(for_all, false, false, true, values, resolver).await,
            StringEqualsIgnoreCase(s) => s.evaluate_with_resolver(for_all, true, false, false, values, resolver).await,
            StringNotEqualsIgnoreCase(s) => s.evaluate_with_resolver(for_all, true, false, true, values, resolver).await,
            StringLike(s) => s.evaluate_with_resolver(for_all, false, true, false, values, resolver).await,
            StringNotLike(s) => s.evaluate_with_resolver(for_all, false, true, true, values, resolver).await,
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
        };

        if self.is_negate() { !r } else { r }
    }

    #[inline]
    pub fn is_negate(&self) -> bool {
        use Condition::*;
        matches!(self, StringNotEquals(_) | StringNotEqualsIgnoreCase(_) | NotIpAddress(_))
    }

    pub fn serialize_map<T: SerializeMap>(&self, se: &mut T) -> Result<(), T::Error> {
        match self {
            Condition::StringEquals(s) => se.serialize_value(s),
            Condition::StringNotEquals(s) => se.serialize_value(s),
            Condition::StringEqualsIgnoreCase(s) => se.serialize_value(s),
            Condition::StringNotEqualsIgnoreCase(s) => se.serialize_value(s),
            Condition::StringLike(s) => se.serialize_value(s),
            Condition::StringNotLike(s) => se.serialize_value(s),
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
            _ => false,
        }
    }
}
