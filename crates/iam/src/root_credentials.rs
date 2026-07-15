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

use rustfs_credentials::Credentials;

pub(crate) fn credentials() -> Option<Credentials> {
    crate::runtime_sources::action_credentials()
}

pub(crate) fn credentials_or_default() -> Credentials {
    credentials().unwrap_or_default()
}

/// The signing key for STS session tokens.
///
/// GHSA-m77q-r63m-pj89 (intentionally UNFIXED): this returns the root secret
/// key, so STS JWTs are signed with the shared root secret and anyone holding
/// it can forge session tokens. The behavior is pinned by
/// `test_ghsa_m77q_sts_session_token_signed_with_root_secret` in `sys.rs`;
/// fixing the advisory (a dedicated STS signing key distinct from the root
/// secret) must update that test red -> green. See
/// docs/testing/security-regressions.md.
pub(crate) fn token_signing_key() -> Option<String> {
    credentials().map(|cred| cred.secret_key)
}

pub(crate) fn is_root_access_key(access_key: &str) -> bool {
    credentials().is_some_and(|cred| cred.access_key == access_key)
}
