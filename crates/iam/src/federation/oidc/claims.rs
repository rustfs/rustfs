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

use super::config::roles_claim_key;
use crate::{
    federation::{FederatedAuthorization, FederatedClaims},
    oidc::{OidcClaims, OidcSys},
};
use rustfs_policy::policy::{ClaimLookup, get_claim_case_insensitive};

fn string_list_claim(claims: &OidcClaims, claim_name: &str) -> Vec<String> {
    match get_claim_case_insensitive(&claims.raw, claim_name) {
        ClaimLookup::Found(serde_json::Value::Array(values)) => values
            .iter()
            .filter_map(|value| value.as_str().map(ToOwned::to_owned))
            .collect(),
        ClaimLookup::Found(serde_json::Value::String(value)) => value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        ClaimLookup::Missing | ClaimLookup::Ambiguous | ClaimLookup::Found(_) => Vec::new(),
    }
}

pub(super) fn authorization(oidc: &OidcSys, provider_id: String, claims: OidcClaims) -> FederatedAuthorization {
    let (policies, groups) = oidc.map_claims_to_policies(&provider_id, &claims);
    let roles_claim_key = roles_claim_key(oidc, &provider_id);
    let roles = roles_claim_key
        .as_deref()
        .map(|claim_name| string_list_claim(&claims, claim_name))
        .unwrap_or_default();

    FederatedAuthorization {
        provider_id,
        claims: FederatedClaims {
            sub: claims.sub,
            email: claims.email,
            username: claims.username,
            groups: claims.groups,
            raw: claims.raw,
        },
        policies,
        groups,
        roles_claim_key,
        roles,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn string_list_claim_matches_existing_array_and_csv_behavior() {
        let claims = OidcClaims {
            raw: HashMap::from([
                ("array_roles".to_string(), json!(["reader", 7, "writer"])),
                ("csv_roles".to_string(), json!("reader, writer, ,auditor")),
            ]),
            ..Default::default()
        };

        assert_eq!(string_list_claim(&claims, "array_roles"), ["reader", "writer"]);
        assert_eq!(string_list_claim(&claims, "csv_roles"), ["reader", "writer", "auditor"]);
    }

    #[test]
    fn string_list_claim_preserves_exact_and_ambiguous_match_behavior() {
        let exact = OidcClaims {
            raw: HashMap::from([
                ("Roles".to_string(), json!(["mixed-case"])),
                ("roles".to_string(), json!(["exact-match"])),
            ]),
            ..Default::default()
        };
        assert_eq!(string_list_claim(&exact, "roles"), ["exact-match"]);

        let ambiguous = OidcClaims {
            raw: HashMap::from([
                ("Roles".to_string(), json!(["mixed-case"])),
                ("ROLES".to_string(), json!(["upper-case"])),
            ]),
            ..Default::default()
        };
        assert!(string_list_claim(&ambiguous, "roles").is_empty());
    }
}
