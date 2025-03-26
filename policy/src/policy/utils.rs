use std::collections::HashMap;

use serde_json::Value;

pub mod path;
pub mod wildcard;

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
    use super::_split_path;

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
}
