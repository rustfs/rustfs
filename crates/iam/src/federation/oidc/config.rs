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

use crate::oidc::{OidcProviderConfig, OidcSys};

pub(super) fn provider_config<'a>(oidc: &'a OidcSys, id: &str) -> Option<&'a OidcProviderConfig> {
    oidc.get_provider_config(id)
}

pub(super) fn roles_claim_key(oidc: &OidcSys, provider_id: &str) -> Option<String> {
    provider_config(oidc, provider_id)
        .map(|config| config.roles_claim.trim().to_string())
        .filter(|claim| !claim.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_claim_key_requires_explicit_provider_config() {
        let oidc = OidcSys::empty().expect("empty OIDC configuration should be valid");
        assert_eq!(roles_claim_key(&oidc, "default"), None);
    }
}
