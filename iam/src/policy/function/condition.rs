use serde::de::{Error, MapAccess};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use time::OffsetDateTime;

use super::{addr::AddrFunc, binary::BinaryFunc, bool_null::BoolFunc, date::DateFunc, number::NumberFunc, string::StringFunc};

#[derive(Clone, Deserialize)]
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

    pub fn evaluate(&self, for_all: bool, values: &HashMap<String, Vec<String>>) -> bool {
        use Condition::*;

        let r = match self {
            StringEquals(s) => s.evaluate(for_all, false, false, false, values),
            StringNotEquals(s) => s.evaluate(for_all, false, false, true, values),
            StringEqualsIgnoreCase(s) => s.evaluate(for_all, true, false, false, values),
            StringNotEqualsIgnoreCase(s) => s.evaluate(for_all, true, false, true, values),
            StringLike(s) => s.evaluate(for_all, false, true, false, values),
            StringNotLike(s) => s.evaluate(for_all, false, true, true, values),
            BinaryEquals(s) => todo!(),
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

        if self.is_negate() {
            !r
        } else {
            r
        }
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
