// Copyright 2026 RustFS Team
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

use std::collections::BTreeSet;

use serde_json::{Map, Value};

const MAX_FILTER_CONSTRAINTS: usize = 100;

#[derive(Debug, Clone)]
pub(crate) struct ValidatedFilter {
    instructions: Vec<Instruction>,
}

#[derive(Debug, Clone)]
enum Instruction {
    Predicate { field: String, condition: Condition },
    All(usize),
    Any(usize),
}

#[derive(Debug, Clone)]
enum Condition {
    Equal(Value),
    NotEqual(Value),
    Greater(Value),
    GreaterEqual(Value),
    Less(Value),
    LessEqual(Value),
    In(Vec<Value>),
    NotIn(Vec<Value>),
    Exists(bool),
    StartsWith(String),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum FilterError {
    #[error("filter must be a non-empty JSON object")]
    InvalidFilter,
    #[error("unsupported filter operator: {0}")]
    InvalidOperator(String),
    #[error("invalid operand for filter operator: {0}")]
    InvalidOperand(String),
    #[error("metadata key cannot be filtered: {0}")]
    NonFilterableKey(String),
    #[error("filter exceeds 100 constraints")]
    TooManyConstraints,
}

enum ParseTask<'a> {
    Filter(&'a Value),
    Field(&'a str, &'a Value),
    All(usize),
    Any(usize),
}

impl ValidatedFilter {
    pub(crate) fn parse(value: &Value, non_filterable: &BTreeSet<String>) -> Result<Self, FilterError> {
        let mut tasks = vec![ParseTask::Filter(value)];
        let mut instructions = Vec::new();
        let mut constraints = 0_usize;
        while let Some(task) = tasks.pop() {
            match task {
                ParseTask::Filter(value) => {
                    let fields = value
                        .as_object()
                        .filter(|fields| !fields.is_empty())
                        .ok_or(FilterError::InvalidFilter)?;
                    if fields.len() > MAX_FILTER_CONSTRAINTS {
                        return Err(FilterError::TooManyConstraints);
                    }
                    tasks.push(ParseTask::All(fields.len()));
                    tasks.extend(fields.iter().rev().map(|(field, value)| ParseTask::Field(field, value)));
                }
                ParseTask::Field(field, value) if field == "$and" || field == "$or" => {
                    let filters = value
                        .as_array()
                        .filter(|filters| !filters.is_empty())
                        .ok_or_else(|| FilterError::InvalidOperand(field.to_string()))?;
                    if filters.len() > MAX_FILTER_CONSTRAINTS {
                        return Err(FilterError::TooManyConstraints);
                    }
                    tasks.push(if field == "$and" {
                        ParseTask::All(filters.len())
                    } else {
                        ParseTask::Any(filters.len())
                    });
                    tasks.extend(filters.iter().rev().map(ParseTask::Filter));
                }
                ParseTask::Field(field, value) => {
                    if field.starts_with('$') {
                        return Err(FilterError::InvalidOperator(field.to_string()));
                    }
                    if non_filterable.contains(field) {
                        return Err(FilterError::NonFilterableKey(field.to_string()));
                    }
                    if let Some(operators) = value.as_object() {
                        if operators.is_empty() {
                            return Err(FilterError::InvalidFilter);
                        }
                        if operators.len() > MAX_FILTER_CONSTRAINTS {
                            return Err(FilterError::TooManyConstraints);
                        }
                        for (operator, operand) in operators {
                            let condition = parse_condition(operator, operand, &mut constraints)?;
                            instructions.push(Instruction::Predicate {
                                field: field.to_string(),
                                condition,
                            });
                        }
                        instructions.push(Instruction::All(operators.len()));
                    } else {
                        let condition = parse_condition("$eq", value, &mut constraints)?;
                        instructions.push(Instruction::Predicate {
                            field: field.to_string(),
                            condition,
                        });
                    }
                }
                ParseTask::All(count) => instructions.push(Instruction::All(count)),
                ParseTask::Any(count) => instructions.push(Instruction::Any(count)),
            }
        }
        Ok(Self { instructions })
    }

    pub(crate) fn matches(&self, metadata: &Map<String, Value>) -> bool {
        let mut values = [false; MAX_FILTER_CONSTRAINTS];
        let mut used = 0_usize;
        for instruction in &self.instructions {
            match instruction {
                Instruction::Predicate { field, condition } => {
                    values[used] = condition.matches(metadata.get(field));
                    used += 1;
                }
                Instruction::All(count) | Instruction::Any(count) => {
                    let start = used - count;
                    let result = if matches!(instruction, Instruction::All(_)) {
                        values[start..used].iter().all(|value| *value)
                    } else {
                        values[start..used].iter().any(|value| *value)
                    };
                    values[start] = result;
                    used = start + 1;
                }
            }
        }
        debug_assert_eq!(used, 1);
        values[0]
    }
}

fn primitive(value: &Value) -> bool {
    matches!(value, Value::String(_) | Value::Bool(_)) || value.as_f64().is_some_and(f64::is_finite)
}

fn parse_condition(operator: &str, value: &Value, constraints: &mut usize) -> Result<Condition, FilterError> {
    let invalid = || FilterError::InvalidOperand(operator.to_string());
    let condition = match operator {
        "$eq" | "$ne" if primitive(value) => {
            if operator == "$eq" {
                Condition::Equal(value.clone())
            } else {
                Condition::NotEqual(value.clone())
            }
        }
        "$gt" | "$gte" | "$lt" | "$lte" if value.as_f64().is_some_and(f64::is_finite) => match operator {
            "$gt" => Condition::Greater(value.clone()),
            "$gte" => Condition::GreaterEqual(value.clone()),
            "$lt" => Condition::Less(value.clone()),
            _ => Condition::LessEqual(value.clone()),
        },
        "$in" | "$nin" => {
            let values = value.as_array().filter(|values| !values.is_empty()).ok_or_else(invalid)?;
            if values.len() > MAX_FILTER_CONSTRAINTS.saturating_sub(*constraints) {
                return Err(FilterError::TooManyConstraints);
            }
            if !values.iter().all(primitive) {
                return Err(invalid());
            }
            *constraints += values.len();
            return Ok(if operator == "$in" {
                Condition::In(values.clone())
            } else {
                Condition::NotIn(values.clone())
            });
        }
        "$exists" => Condition::Exists(value.as_bool().ok_or_else(invalid)?),
        "$startsWith" => Condition::StartsWith(value.as_str().ok_or_else(invalid)?.to_string()),
        "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" => return Err(invalid()),
        _ => return Err(FilterError::InvalidOperator(operator.to_string())),
    };
    if *constraints == MAX_FILTER_CONSTRAINTS {
        return Err(FilterError::TooManyConstraints);
    }
    *constraints += 1;
    Ok(condition)
}

impl Condition {
    fn matches(&self, actual: Option<&Value>) -> bool {
        if let Self::Exists(expected) = self {
            return actual.is_some() == *expected;
        }
        let Some(actual) = actual.filter(|value| !value.is_null()) else {
            return false;
        };
        match self {
            Self::Equal(expected) => equal(actual, expected),
            Self::NotEqual(expected) => !equal(actual, expected),
            Self::In(expected) => expected.iter().any(|expected| equal(actual, expected)),
            Self::NotIn(expected) => !expected.iter().any(|expected| equal(actual, expected)),
            Self::Greater(expected) => number_order(actual, expected).is_some_and(std::cmp::Ordering::is_gt),
            Self::GreaterEqual(expected) => number_order(actual, expected).is_some_and(std::cmp::Ordering::is_ge),
            Self::Less(expected) => number_order(actual, expected).is_some_and(std::cmp::Ordering::is_lt),
            Self::LessEqual(expected) => number_order(actual, expected).is_some_and(std::cmp::Ordering::is_le),
            Self::StartsWith(prefix) => actual.as_str().is_some_and(|text| text.starts_with(prefix)),
            Self::Exists(_) => unreachable!("existence predicates are handled before reading the value"),
        }
    }
}

fn equal(actual: &Value, expected: &Value) -> bool {
    if let Value::Array(values) = actual {
        return values.iter().any(|value| scalar_equal(value, expected));
    }
    scalar_equal(actual, expected)
}

fn scalar_equal(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Number(_), Value::Number(_)) => number_order(actual, expected).is_some_and(std::cmp::Ordering::is_eq),
        (Value::String(actual), Value::String(expected)) => actual == expected,
        (Value::Bool(actual), Value::Bool(expected)) => actual == expected,
        _ => false,
    }
}

fn number_order(actual: &Value, expected: &Value) -> Option<std::cmp::Ordering> {
    let (Value::Number(actual), Value::Number(expected)) = (actual, expected) else {
        return None;
    };
    let integer = |value: &serde_json::Number| value.as_i64().map(i128::from).or_else(|| value.as_u64().map(i128::from));
    match (integer(actual), integer(expected)) {
        (Some(actual), Some(expected)) => Some(actual.cmp(&expected)),
        (Some(actual), None) => Some(integer_float_order(actual, expected.as_f64()?)),
        (None, Some(expected)) => Some(integer_float_order(expected, actual.as_f64()?).reverse()),
        (None, None) => actual.as_f64()?.partial_cmp(&expected.as_f64()?),
    }
}

fn integer_float_order(integer: i128, float: f64) -> std::cmp::Ordering {
    if float >= u64::MAX as f64 {
        return std::cmp::Ordering::Less;
    }
    if float < i64::MIN as f64 {
        return std::cmp::Ordering::Greater;
    }
    // The range checks bound this conversion to values representable by i128.
    let truncated = float.trunc() as i128;
    integer.cmp(&truncated).then_with(|| {
        if float.fract() > 0.0 {
            std::cmp::Ordering::Less
        } else if float.fract() < 0.0 {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accepts_documented_operators_and_literal_field_names() {
        for filter in [
            json!({"genre": "documentary"}),
            json!({"genre": {"$eq": "documentary"}}),
            json!({"rating": {"$ne": 4}}),
            json!({"price": {"$gt": 10, "$lte": 50}}),
            json!({"price": {"$gte": 10, "$lt": 50}}),
            json!({"flag": {"$eq": true}}),
            json!({"genre": {"$in": ["comedy", "drama"]}}),
            json!({"genre": {"$nin": ["comedy", "drama"]}}),
            json!({"genre": {"$exists": false}}),
            json!({"path": {"$startsWith": "/marketing/"}}),
            json!({"$and": [{"flag": true}, {"$or": [{"price": 2}, {"price": 3}]}]}),
            json!({"a.b['x'] OR true --": {"$eq": "' OR true --"}}),
        ] {
            assert!(ValidatedFilter::parse(&filter, &BTreeSet::new()).is_ok(), "{filter}");
        }
    }

    #[test]
    fn rejects_invalid_filter_shapes_and_operand_types() {
        for filter in [
            json!(null),
            json!([]),
            json!({}),
            json!({"genre": {}}),
            json!({"$where": "true"}),
            json!({"genre": {"$unknown": 1}}),
            json!({"genre": {"$eq": null}}),
            json!({"genre": {"$eq": []}}),
            json!({"genre": {"$eq": {"nested": 1}}}),
            json!({"price": {"$gt": "10"}}),
            json!({"price": {"$lte": true}}),
            json!({"genre": {"$exists": "true"}}),
            json!({"path": {"$startsWith": 1}}),
            json!({"genre": {"$in": []}}),
            json!({"genre": {"$nin": []}}),
            json!({"genre": {"$in": [null]}}),
            json!({"genre": {"$in": [["drama"]]}}),
            json!({"$and": []}),
            json!({"$or": []}),
            json!({"$and": true}),
            json!({"$or": [{"genre": "drama"}, null]}),
        ] {
            assert!(ValidatedFilter::parse(&filter, &BTreeSet::new()).is_err(), "{filter}");
        }
    }

    #[test]
    fn in_values_count_toward_the_constraint_limit() {
        let allowed = json!({"category": {"$in": (0..100).collect::<Vec<_>>()}});
        let rejected = json!({"category": {"$in": (0..101).collect::<Vec<_>>()}});
        assert!(ValidatedFilter::parse(&allowed, &BTreeSet::new()).is_ok());
        assert!(ValidatedFilter::parse(&rejected, &BTreeSet::new()).is_err());
    }

    #[test]
    fn combined_constraints_share_one_budget() {
        let allowed = json!({"$and": (0..100).map(|value| json!({"category": value})).collect::<Vec<_>>()});
        let rejected = json!({"$and": [allowed, {"flag": true}]});
        assert!(ValidatedFilter::parse(&allowed, &BTreeSet::new()).is_ok());
        assert!(ValidatedFilter::parse(&rejected, &BTreeSet::new()).is_err());
    }

    #[test]
    fn non_filterable_keys_are_rejected_inside_logical_groups() {
        let fields = BTreeSet::from(["private.note".to_string()]);
        let denied = json!({"$or": [{"genre": "drama"}, {"private.note": {"$exists": true}}]});
        assert!(ValidatedFilter::parse(&denied, &fields).is_err());
        assert!(ValidatedFilter::parse(&json!({"private": "public"}), &fields).is_ok());
    }

    fn selected_keys(filter: Value, metadata: Vec<Value>) -> Vec<String> {
        let filter = ValidatedFilter::parse(&filter, &BTreeSet::new()).expect("validated filter");
        metadata
            .iter()
            .enumerate()
            .filter(|(_, value)| filter.matches(value.as_object().expect("metadata object")))
            .map(|(index, _)| index.to_string())
            .collect()
    }

    #[test]
    fn controlled_expr_evaluates_documented_filters() {
        let metadata = vec![
            json!({"genre":["documentary","romance"],"rating":4,"year":2020,"flag":true,"path":"/marketing/a"}),
            json!({"genre":"drama","rating":5,"year":2021,"flag":false,"path":"/sales/b"}),
            json!({"genre":"comedy","rating":3,"year":2019,"flag":true,"path":"/support/c"}),
        ];
        for (filter, expected) in [
            (json!({"genre":"documentary"}), vec!["0"]),
            (json!({"genre":{"$eq":"documentary"}}), vec!["0"]),
            (json!({"genre":{"$ne":"drama"}}), vec!["0", "2"]),
            (json!({"year":{"$gt":2019}}), vec!["0", "1"]),
            (json!({"year":{"$gte":2020}}), vec!["0", "1"]),
            (json!({"year":{"$lt":2020}}), vec!["2"]),
            (json!({"year":{"$lte":2020}}), vec!["0", "2"]),
            (json!({"genre":{"$in":["documentary","drama"]}}), vec!["0", "1"]),
            (json!({"genre":{"$nin":["documentary","drama"]}}), vec!["2"]),
            (json!({"rating":{"$exists":true}}), vec!["0", "1", "2"]),
            (json!({"missing":{"$exists":false}}), vec!["0", "1", "2"]),
            (json!({"path":{"$startsWith":"/marketing/"}}), vec!["0"]),
            (json!({"$and":[{"flag":true},{"year":{"$gte":2020}}]}), vec!["0"]),
            (json!({"$or":[{"year":2019},{"genre":"drama"}]}), vec!["1", "2"]),
            (json!({"rating":{"$gte":4,"$lt":5}}), vec!["0"]),
        ] {
            assert_eq!(selected_keys(filter.clone(), metadata.clone()), expected, "{filter}");
        }
    }

    #[test]
    fn expression_characters_remain_literal_data() {
        let filter = json!({"a.b['x'] OR true --":{"$eq":"' OR true --"}});
        let metadata = vec![
            json!({"a.b['x'] OR true --":"' OR true --"}),
            json!({"a.b['x'] OR true --":"different"}),
            json!({"a":{"b":{"x":"' OR true --"}}}),
        ];
        assert_eq!(selected_keys(filter, metadata), ["0"]);
    }

    #[test]
    fn primitive_types_and_integer_precision_are_preserved() {
        let metadata = vec![
            json!({"value":1}),
            json!({"value":"1"}),
            json!({"value":true}),
            json!({"value":1.0}),
            json!({"value":9007199254740992_u64}),
            json!({"value":9007199254740993_u64}),
        ];
        assert_eq!(selected_keys(json!({"value":1}), metadata.clone()), ["0", "3"]);
        assert_eq!(selected_keys(json!({"value":true}), metadata.clone()), ["2"]);
        assert_eq!(selected_keys(json!({"value":"1"}), metadata.clone()), ["1"]);
        assert_eq!(selected_keys(json!({"value":9007199254740993_u64}), metadata), ["5"]);
    }
}
