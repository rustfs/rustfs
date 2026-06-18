#![no_main]

use libfuzzer_sys::fuzz_target;
use rustfs_policy::policy::{BucketPolicy, Policy, PolicyDoc};
use rustfs_security_governance::{SerdePolicy, SerdePolicyKind, UnknownFieldPolicy, validate_serde_policies};
use serde_json::{Map, Value, json};

fn add_unknown_top_level_field(data: &[u8]) -> Option<Vec<u8>> {
    let mut value: Value = serde_json::from_slice(data).ok()?;
    let object = value.as_object_mut()?;
    object.insert("UnexpectedField".to_string(), json!(true));
    serde_json::to_vec(&value).ok()
}

fn looks_like_policy_doc(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|object| object.contains_key("Policy") || object.contains_key("policy"))
}

fn exercise_governance_contracts() {
    let ok = [
        SerdePolicy::new("BucketPolicy", SerdePolicyKind::StrictIngress, UnknownFieldPolicy::Deny),
        SerdePolicy::new("PolicyDoc", SerdePolicyKind::TolerantCompat, UnknownFieldPolicy::Warn),
    ];
    assert!(validate_serde_policies(&ok).is_ok());

    let bad = [SerdePolicy::new(
        "BucketPolicy",
        SerdePolicyKind::StrictIngress,
        UnknownFieldPolicy::Warn,
    )];
    assert!(validate_serde_policies(&bad).is_err());
}

fn exercise_policy_doc(raw: &[u8]) {
    let _ = serde_json::from_slice::<PolicyDoc>(raw);
    let _ = serde_json::from_slice::<Policy>(raw);
    let parsed = PolicyDoc::try_from(raw.to_vec());

    if parsed.is_ok()
        && let Ok(value) = serde_json::from_slice::<Value>(raw)
        && looks_like_policy_doc(&value)
    {
        let mutated = add_unknown_top_level_field(raw).expect("policy doc mutation should stay serializable");
        assert!(
            PolicyDoc::try_from(mutated).is_ok(),
            "policy doc ingress should tolerate unknown top-level fields for compat JSON"
        );
    }
}

fuzz_target!(|data: &[u8]| {
    exercise_governance_contracts();

    let bucket_policy = serde_json::from_slice::<BucketPolicy>(data);
    if bucket_policy.is_ok() {
        if let Some(mutated) = add_unknown_top_level_field(data) {
            assert!(
                serde_json::from_slice::<BucketPolicy>(&mutated).is_err(),
                "bucket policy strict ingress should reject unknown top-level fields"
            );
        }
    }

    exercise_policy_doc(data);

    if let Ok(value) = serde_json::from_slice::<Value>(data)
        && let Some(object) = value.as_object()
    {
        let mut legacy_doc = Map::new();
        if let Some(policy) = object.get("Policy").or_else(|| object.get("policy")) {
            legacy_doc.insert("version".to_string(), json!(1));
            legacy_doc.insert("policy".to_string(), policy.clone());
            legacy_doc.insert("create_date".to_string(), json!("2025-03-07T12:00:00Z"));
            legacy_doc.insert("update_date".to_string(), json!("2025-03-07T12:00:00Z"));
            let legacy_doc = serde_json::to_vec(&Value::Object(legacy_doc)).expect("legacy policy doc should serialize");
            assert!(
                PolicyDoc::try_from(legacy_doc).is_ok(),
                "legacy policy doc aliases should remain parseable"
            );
        }
    }
});
