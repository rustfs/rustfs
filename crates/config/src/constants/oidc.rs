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

// OIDC configuration field keys (used in KVS)
pub const OIDC_CONFIG_URL: &str = "config_url";
pub const OIDC_CLIENT_ID: &str = "client_id";
pub const OIDC_CLIENT_SECRET: &str = "client_secret";
pub const OIDC_SCOPES: &str = "scopes";
pub const OIDC_REDIRECT_URI: &str = "redirect_uri";
pub const OIDC_REDIRECT_URI_DYNAMIC: &str = "redirect_uri_dynamic";
pub const OIDC_CLAIM_NAME: &str = "claim_name";
pub const OIDC_CLAIM_PREFIX: &str = "claim_prefix";
pub const OIDC_ROLE_POLICY: &str = "role_policy";
pub const OIDC_DISPLAY_NAME: &str = "display_name";
pub const OIDC_GROUPS_CLAIM: &str = "groups_claim";
pub const OIDC_EMAIL_CLAIM: &str = "email_claim";
pub const OIDC_USERNAME_CLAIM: &str = "username_claim";

// Environment variable names for OIDC
pub const ENV_IDENTITY_OPENID_ENABLE: &str = "RUSTFS_IDENTITY_OPENID_ENABLE";
pub const ENV_IDENTITY_OPENID_CONFIG_URL: &str = "RUSTFS_IDENTITY_OPENID_CONFIG_URL";
pub const ENV_IDENTITY_OPENID_CLIENT_ID: &str = "RUSTFS_IDENTITY_OPENID_CLIENT_ID";
pub const ENV_IDENTITY_OPENID_CLIENT_SECRET: &str = "RUSTFS_IDENTITY_OPENID_CLIENT_SECRET";
pub const ENV_IDENTITY_OPENID_SCOPES: &str = "RUSTFS_IDENTITY_OPENID_SCOPES";
pub const ENV_IDENTITY_OPENID_REDIRECT_URI: &str = "RUSTFS_IDENTITY_OPENID_REDIRECT_URI";
pub const ENV_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC: &str = "RUSTFS_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC";
pub const ENV_IDENTITY_OPENID_CLAIM_NAME: &str = "RUSTFS_IDENTITY_OPENID_CLAIM_NAME";
pub const ENV_IDENTITY_OPENID_CLAIM_PREFIX: &str = "RUSTFS_IDENTITY_OPENID_CLAIM_PREFIX";
pub const ENV_IDENTITY_OPENID_ROLE_POLICY: &str = "RUSTFS_IDENTITY_OPENID_ROLE_POLICY";
pub const ENV_IDENTITY_OPENID_DISPLAY_NAME: &str = "RUSTFS_IDENTITY_OPENID_DISPLAY_NAME";
pub const ENV_IDENTITY_OPENID_GROUPS_CLAIM: &str = "RUSTFS_IDENTITY_OPENID_GROUPS_CLAIM";
pub const ENV_IDENTITY_OPENID_EMAIL_CLAIM: &str = "RUSTFS_IDENTITY_OPENID_EMAIL_CLAIM";
pub const ENV_IDENTITY_OPENID_USERNAME_CLAIM: &str = "RUSTFS_IDENTITY_OPENID_USERNAME_CLAIM";

/// List of all environment variable keys for an OIDC provider.
pub const ENV_IDENTITY_OPENID_KEYS: &[&str; 14] = &[
    ENV_IDENTITY_OPENID_ENABLE,
    ENV_IDENTITY_OPENID_CONFIG_URL,
    ENV_IDENTITY_OPENID_CLIENT_ID,
    ENV_IDENTITY_OPENID_CLIENT_SECRET,
    ENV_IDENTITY_OPENID_SCOPES,
    ENV_IDENTITY_OPENID_REDIRECT_URI,
    ENV_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC,
    ENV_IDENTITY_OPENID_CLAIM_NAME,
    ENV_IDENTITY_OPENID_CLAIM_PREFIX,
    ENV_IDENTITY_OPENID_ROLE_POLICY,
    ENV_IDENTITY_OPENID_DISPLAY_NAME,
    ENV_IDENTITY_OPENID_GROUPS_CLAIM,
    ENV_IDENTITY_OPENID_EMAIL_CLAIM,
    ENV_IDENTITY_OPENID_USERNAME_CLAIM,
];

/// A list of all valid configuration keys for an OIDC provider.
pub const IDENTITY_OPENID_KEYS: &[&str] = &[
    crate::ENABLE_KEY,
    OIDC_CONFIG_URL,
    OIDC_CLIENT_ID,
    OIDC_CLIENT_SECRET,
    OIDC_SCOPES,
    OIDC_REDIRECT_URI,
    OIDC_REDIRECT_URI_DYNAMIC,
    OIDC_CLAIM_NAME,
    OIDC_CLAIM_PREFIX,
    OIDC_ROLE_POLICY,
    OIDC_DISPLAY_NAME,
    OIDC_GROUPS_CLAIM,
    OIDC_EMAIL_CLAIM,
    OIDC_USERNAME_CLAIM,
    crate::COMMENT_KEY,
];

// Default values
pub const OIDC_DEFAULT_SCOPES: &str = "openid,profile,email";
pub const OIDC_DEFAULT_CLAIM_NAME: &str = "groups";
pub const OIDC_DEFAULT_GROUPS_CLAIM: &str = "groups";
pub const OIDC_DEFAULT_EMAIL_CLAIM: &str = "email";
pub const OIDC_DEFAULT_USERNAME_CLAIM: &str = "preferred_username";

// Subsystem identifier
pub const IDENTITY_OPENID_SUB_SYS: &str = "identity_openid";
