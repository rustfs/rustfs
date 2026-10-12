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

use crate::{
    federation::{FederatedClaims, FederatedProviderRef, VerifiedFederatedIdentity},
    oidc::OidcClaims,
};

pub(super) fn verified_identity(provider: FederatedProviderRef, claims: OidcClaims) -> VerifiedFederatedIdentity {
    VerifiedFederatedIdentity::from_claims(
        provider,
        FederatedClaims {
            sub: claims.sub,
            email: claims.email,
            username: claims.username,
            groups: claims.groups,
            raw: claims.raw,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn verified_identity_preserves_provider_and_claim_values() {
        let raw = HashMap::from([
            ("iss".to_string(), json!("https://corp.example.test")),
            ("department".to_string(), json!("engineering")),
            ("roles".to_string(), json!(["reader", "admin"])),
        ]);
        let identity = verified_identity(
            FederatedProviderRef::new("corp".to_string()),
            OidcClaims {
                sub: " subject-123 ".to_string(),
                email: " user@example.test ".to_string(),
                username: " user ".to_string(),
                groups: vec![
                    "source-ops".to_string(),
                    "source-developers".to_string(),
                    "source-ops".to_string(),
                ],
                raw: raw.clone(),
            },
        );

        assert_eq!(identity.provider().as_str(), "corp");
        assert_eq!(identity.subject(), " subject-123 ");
        assert_eq!(identity.email(), " user@example.test ");
        assert_eq!(identity.username(), " user ");
        assert_eq!(identity.source_groups(), ["source-ops", "source-developers", "source-ops"]);
        assert_eq!(identity.attributes(), &raw);
    }
}
