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

use crate::config::{KV, KVS};
use rustfs_config::{
    ENABLE_KEY, EnableState,
    oidc::{
        OIDC_CLAIM_NAME, OIDC_CLAIM_PREFIX, OIDC_CLIENT_ID, OIDC_CLIENT_SECRET, OIDC_CONFIG_URL, OIDC_DEFAULT_CLAIM_NAME,
        OIDC_DEFAULT_EMAIL_CLAIM, OIDC_DEFAULT_GROUPS_CLAIM, OIDC_DEFAULT_SCOPES, OIDC_DEFAULT_USERNAME_CLAIM, OIDC_DISPLAY_NAME,
        OIDC_EMAIL_CLAIM, OIDC_GROUPS_CLAIM, OIDC_REDIRECT_URI, OIDC_REDIRECT_URI_DYNAMIC, OIDC_ROLE_POLICY, OIDC_SCOPES,
        OIDC_USERNAME_CLAIM,
    },
};
use std::sync::LazyLock;

/// Default KVS for OIDC identity provider settings.
pub static DEFAULT_IDENTITY_OPENID_KVS: LazyLock<KVS> = LazyLock::new(|| {
    KVS(vec![
        KV {
            key: ENABLE_KEY.to_owned(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: OIDC_CONFIG_URL.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: OIDC_CLIENT_ID.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: OIDC_CLIENT_SECRET.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: true,
        },
        KV {
            key: OIDC_SCOPES.to_owned(),
            value: OIDC_DEFAULT_SCOPES.to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: OIDC_REDIRECT_URI.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: OIDC_REDIRECT_URI_DYNAMIC.to_owned(),
            value: EnableState::On.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: OIDC_CLAIM_NAME.to_owned(),
            value: OIDC_DEFAULT_CLAIM_NAME.to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: OIDC_CLAIM_PREFIX.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: OIDC_ROLE_POLICY.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: OIDC_DISPLAY_NAME.to_owned(),
            value: "".to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: OIDC_GROUPS_CLAIM.to_owned(),
            value: OIDC_DEFAULT_GROUPS_CLAIM.to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: OIDC_EMAIL_CLAIM.to_owned(),
            value: OIDC_DEFAULT_EMAIL_CLAIM.to_owned(),
            hidden_if_empty: false,
        },
        KV {
            key: OIDC_USERNAME_CLAIM.to_owned(),
            value: OIDC_DEFAULT_USERNAME_CLAIM.to_owned(),
            hidden_if_empty: false,
        },
    ])
});
