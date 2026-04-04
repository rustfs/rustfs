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

use std::collections::HashMap;

use serde_json::Value;

pub mod path;
pub mod wildcard;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClaimLookup<'a> {
    Missing,
    Found(&'a Value),
    Ambiguous,
}

fn case_insensitive_eq(left: &str, right: &str) -> bool {
    left.chars()
        .flat_map(char::to_lowercase)
        .eq(right.chars().flat_map(char::to_lowercase))
}

pub fn get_claim_case_insensitive<'a>(claims: &'a HashMap<String, Value>, claim_name: &str) -> ClaimLookup<'a> {
    if let Some(value) = claims.get(claim_name) {
        return ClaimLookup::Found(value);
    }

    let mut matched = None;

    for (candidate, value) in claims {
        if case_insensitive_eq(candidate, claim_name) {
            if matched.is_some() {
                return ClaimLookup::Ambiguous;
            }
            matched = Some(value);
        }
    }

    match matched {
        Some(value) => ClaimLookup::Found(value),
        None => ClaimLookup::Missing,
    }
}

pub fn _get_values_from_claims(claim: &HashMap<String, Value>, chaim_name: &str) -> (Vec<String>, bool) {
    let mut result = vec![];
    let Some(pname) = claim.get(chaim_name) else {
        return (result, false);
    };

    let mut func = |pname_str: &str| {
        for s in pname_str.split(',').map(str::trim) {
            if s.is_empty() {
                continue;
            }
            result.push(s.to_owned());
        }
    };

    if let Some(arrays) = pname.as_array() {
        for array in arrays {
            let Some(pname_str) = array.as_str() else {
                continue;
            };

            func(pname_str);
        }
    } else {
        let Some(pname_str) = pname.as_str() else {
            return (result, false);
        };

        func(pname_str);
    }

    (result, true)
}

pub fn _split_path(path: &str, second_index: bool) -> (&str, &str) {
    let index = if second_index {
        let Some(first) = path.find('/') else {
            return (path, "");
        };

        let Some(second) = &(path[first + 1..]).find('/') else {
            return (path, "");
        };

        Some(first + second + 1)
    } else {
        path.find('/')
    };

    let Some(index) = index else {
        return (path, "");
    };

    (&path[..index + 1], &path[index + 1..])
}

#[cfg(test)]
mod tests {
    use super::{_split_path, ClaimLookup, get_claim_case_insensitive};
    use serde_json::{Value, json};
    use std::collections::HashMap;

    #[test_case::test_case("format.json", false => ("format.json", ""))]
    #[test_case::test_case("users/tester.json", false => ("users/", "tester.json"))]
    #[test_case::test_case("groups/test/group.json", false => ("groups/", "test/group.json"))]
    #[test_case::test_case("policydb/groups/testgroup.json", true => ("policydb/groups/", "testgroup.json"))]
    #[test_case::test_case(
        "policydb/sts-users/uid=slash/user,ou=people,ou=swengg,dc=min,dc=io.json", true =>
        ("policydb/sts-users/", "uid=slash/user,ou=people,ou=swengg,dc=min,dc=io.json"))
    ]
    #[test_case::test_case(
        "policydb/sts-users/uid=slash/user/twice,ou=people,ou=swengg,dc=min,dc=io.json", true =>
        ("policydb/sts-users/", "uid=slash/user/twice,ou=people,ou=swengg,dc=min,dc=io.json"))
    ]
    #[test_case::test_case(
        "policydb/groups/cn=project/d,ou=groups,ou=swengg,dc=min,dc=io.json", true =>
        ("policydb/groups/", "cn=project/d,ou=groups,ou=swengg,dc=min,dc=io.json"))
    ]
    fn test_split_path(path: &str, second_index: bool) -> (&str, &str) {
        _split_path(path, second_index)
    }

    #[test]
    fn test_get_claim_case_insensitive_prefers_exact_match() {
        let mut claims = HashMap::new();
        claims.insert("Policy".to_string(), json!("exact_match"));
        claims.insert("policy".to_string(), json!("lowercase"));

        assert_eq!(
            get_claim_case_insensitive(&claims, "Policy"),
            ClaimLookup::Found(&Value::String("exact_match".to_string()))
        );
    }

    #[test]
    fn test_get_claim_case_insensitive_returns_ambiguous_for_multiple_folded_matches() {
        let mut claims = HashMap::new();
        claims.insert("Policy".to_string(), json!("exact_match"));
        claims.insert("policy".to_string(), json!("lowercase"));

        assert_eq!(get_claim_case_insensitive(&claims, "POLICY"), ClaimLookup::Ambiguous);
    }

    #[test]
    fn test_get_claim_case_insensitive_matches_unicode_without_allocation() {
        let mut claims = HashMap::new();
        claims.insert("Straße".to_string(), json!("value"));

        assert_eq!(
            get_claim_case_insensitive(&claims, "straße"),
            ClaimLookup::Found(&Value::String("value".to_string()))
        );
    }
}
