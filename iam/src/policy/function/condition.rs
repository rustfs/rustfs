use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::{addr::AddrFunc, binary::BinaryFunc, bool_null::BoolFunc, date::DateFunc, number::NumberFunc, string::StringFunc};

#[derive(Clone, Serialize, Deserialize)]
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
    pub fn evaluate(&self, for_all: bool, values: &HashMap<String, Vec<String>>) -> bool {
        use Condition::*;

        let r = match self {
            StringEquals(s) => s.evaluate(for_all, false, false, values),
            StringNotEquals(s) => s.evaluate(for_all, false, false, values),
            StringEqualsIgnoreCase(s) => s.evaluate(for_all, true, false, values),
            StringNotEqualsIgnoreCase(s) => s.evaluate(for_all, true, false, values),
            StringLike(s) => s.evaluate(for_all, false, true, values),
            StringNotLike(s) => s.evaluate(for_all, false, true, values),
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

    pub fn is_negate(&self) -> bool {
        use Condition::*;
        matches!(self, StringNotEquals(_) | StringNotEqualsIgnoreCase(_) | NotIpAddress(_))
    }
}
