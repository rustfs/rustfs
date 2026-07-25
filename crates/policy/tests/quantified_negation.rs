//! Regression tests for quantified negated string conditions.
//!
//! AWS semantics:
//!   ForAllValues:StringNotEquals -> true iff EVERY request value is absent from the policy set.
//!   ForAnyValue:StringNotEquals  -> true iff AT LEAST ONE request value is absent from the policy set.
//!
//! Negating the aggregate result of the positive quantified match instead of the
//! per-value predicate yields NOT(all match) and NOT(any match) respectively,
//! which transposes the two quantifiers. The discriminating case is a request
//! whose values partially overlap the policy set; a set that is fully contained
//! or fully disjoint cannot tell the two apart.

use std::collections::HashMap;

use rustfs_policy::policy::Functions;

fn ctx(key: &str, vals: &[&str]) -> HashMap<String, Vec<String>> {
    let mut m = HashMap::new();
    m.insert(key.to_owned(), vals.iter().map(|s| (*s).to_owned()).collect());
    m
}

fn eval(json: &str, values: &HashMap<String, Vec<String>>) -> bool {
    let f: Functions = serde_json::from_str(json).expect("parse condition");
    pollster::block_on(f.evaluate(values))
}

#[test]
fn for_all_values_string_not_equals_partial_overlap() {
    // groups = {art, hr}; policy set = {prod, art}. "art" IS in the set,
    // so not every value is "not equal" -> AWS says false.
    let v = ctx("groups", &["art", "hr"]);
    let got = eval(r#"{"ForAllValues:StringNotEquals":{"jwt:groups":["prod","art"]}}"#, &v);
    assert!(
        !got,
        "ForAllValues:StringNotEquals must be false when a request value matches the policy set"
    );
}

#[test]
fn for_any_value_string_not_equals_partial_overlap() {
    // groups = {art, hr}; policy set = {prod, art}. "hr" is NOT in the set,
    // so at least one value is "not equal" -> AWS says true.
    let v = ctx("groups", &["art", "hr"]);
    let got = eval(r#"{"ForAnyValue:StringNotEquals":{"jwt:groups":["prod","art"]}}"#, &v);
    assert!(
        got,
        "ForAnyValue:StringNotEquals must be true when some request value is outside the policy set"
    );
}

#[test]
fn for_all_values_string_not_equals_missing_key() {
    // ForAllValues is vacuously true when the key is absent from the request.
    let v = HashMap::new();
    let got = eval(r#"{"ForAllValues:StringNotEquals":{"jwt:groups":["prod","art"]}}"#, &v);
    assert!(got, "ForAllValues:* is vacuously true when the context key is absent");
}

#[test]
fn for_any_value_string_not_equals_missing_key() {
    // ForAnyValue is false when the key is absent -- there is no value to satisfy it.
    let v = HashMap::new();
    let got = eval(r#"{"ForAnyValue:StringNotEquals":{"jwt:groups":["prod","art"]}}"#, &v);
    assert!(!got, "ForAnyValue:* is false when the context key is absent");
}

#[test]
fn for_all_values_string_not_like_partial_overlap() {
    // Same inversion for the wildcard family (R03-CAN-042).
    let v = ctx("groups", &["art-team", "hr"]);
    let got = eval(r#"{"ForAllValues:StringNotLike":{"jwt:groups":["art-*"]}}"#, &v);
    assert!(!got, "ForAllValues:StringNotLike must be false when a request value matches the pattern");
}

#[test]
fn for_any_value_string_not_like_partial_overlap() {
    let v = ctx("groups", &["art-team", "hr"]);
    let got = eval(r#"{"ForAnyValue:StringNotLike":{"jwt:groups":["art-*"]}}"#, &v);
    assert!(got, "ForAnyValue:StringNotLike must be true when some request value does not match");
}

/// Positive (non-negated) quantifiers must keep working -- guards against a fix
/// that flips these instead.
#[test]
fn positive_quantifiers_unchanged() {
    let partial = ctx("groups", &["art", "hr"]);
    let contained = ctx("groups", &["art"]);
    let empty = HashMap::new();

    let all_eq = r#"{"ForAllValues:StringEquals":{"jwt:groups":["prod","art"]}}"#;
    let any_eq = r#"{"ForAnyValue:StringEquals":{"jwt:groups":["prod","art"]}}"#;

    assert!(!eval(all_eq, &partial), "ForAllValues:StringEquals false when a value is outside the set");
    assert!(eval(all_eq, &contained), "ForAllValues:StringEquals true when all values are in the set");
    assert!(eval(all_eq, &empty), "ForAllValues:StringEquals vacuously true on absent key");

    assert!(eval(any_eq, &partial), "ForAnyValue:StringEquals true when some value is in the set");
    assert!(!eval(any_eq, &empty), "ForAnyValue:StringEquals false on absent key");
}
