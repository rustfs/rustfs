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

//! Display-only profile metadata of a federated identity.
//!
//! The profile is never an authorization input. It is kept apart from the raw
//! ID token claims that drive policy mapping, and sessions store it as one
//! nested JSON object under [`OIDC_PROFILE_CLAIM`]. Policy condition expansion
//! only reads string-valued session claims, so no profile value can reach a
//! policy condition or variable.

use rustfs_policy::policy::{ClaimLookup, get_claim_case_insensitive};
use serde_json::{Map, Value};
use std::{collections::HashMap, fmt};
use url::Url;

/// Session claim holding the display profile as a JSON object.
pub const OIDC_PROFILE_CLAIM: &str = "rustfs_profile";

const MAX_PROFILE_TEXT_CHARS: usize = 256;
// The profile travels inside every session token, so keep it small.
const MAX_PROFILE_PICTURE_URL_BYTES: usize = 1024;

const FIELD_USERNAME: &str = "username";
const FIELD_EMAIL: &str = "email";
const FIELD_NAME: &str = "name";
const FIELD_PICTURE: &str = "picture";

/// Normalized display metadata (username, email, full name, avatar URL).
///
/// Every constructor sanitizes its input: text is trimmed, bounded and free of
/// control characters, and the picture is an absolute `https` URL without
/// embedded credentials. Values that fail these checks are dropped, not repaired.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct FederatedProfile {
    username: Option<String>,
    email: Option<String>,
    name: Option<String>,
    picture: Option<String>,
}

impl fmt::Debug for FederatedProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FederatedProfile")
            .field("username_present", &self.username.is_some())
            .field("email_present", &self.email.is_some())
            .field("name_present", &self.name.is_some())
            .field("picture_present", &self.picture.is_some())
            .finish()
    }
}

impl FederatedProfile {
    /// Read the profile from provider claims (ID token or UserInfo response),
    /// using the provider's configured username and email claim names.
    pub(crate) fn from_provider_claims(claims: &HashMap<String, Value>, username_claim: &str, email_claim: &str) -> Self {
        let string_claim = |key: &str| match get_claim_case_insensitive(claims, key) {
            ClaimLookup::Found(value) => value.as_str(),
            ClaimLookup::Missing | ClaimLookup::Ambiguous => None,
        };
        Self {
            username: string_claim(username_claim).and_then(sanitize_text),
            email: string_claim(email_claim).and_then(sanitize_text),
            name: string_claim(FIELD_NAME).and_then(sanitize_text),
            picture: string_claim(FIELD_PICTURE).and_then(sanitize_picture),
        }
    }

    /// Read a profile previously written by [`Self::to_claim_value`]. Input is
    /// re-sanitized because session claims are not trusted to be well formed.
    pub fn from_claim_value(value: &Value) -> Self {
        let Some(object) = value.as_object() else {
            return Self::default();
        };
        let field = |key: &str| object.get(key).and_then(Value::as_str);
        Self {
            username: field(FIELD_USERNAME).and_then(sanitize_text),
            email: field(FIELD_EMAIL).and_then(sanitize_text),
            name: field(FIELD_NAME).and_then(sanitize_text),
            picture: field(FIELD_PICTURE).and_then(sanitize_picture),
        }
    }

    /// The nested-object session claim value, or `None` when the profile is empty.
    pub fn to_claim_value(&self) -> Option<Value> {
        let mut object = Map::new();
        for (key, value) in [
            (FIELD_USERNAME, &self.username),
            (FIELD_EMAIL, &self.email),
            (FIELD_NAME, &self.name),
            (FIELD_PICTURE, &self.picture),
        ] {
            if let Some(value) = value {
                object.insert(key.to_string(), Value::String(value.clone()));
            }
        }
        (!object.is_empty()).then_some(Value::Object(object))
    }

    /// Keep present values and take missing ones from `other`.
    pub(crate) fn fill_missing_from(&mut self, other: Self) {
        self.username = self.username.take().or(other.username);
        self.email = self.email.take().or(other.email);
        self.name = self.name.take().or(other.name);
        self.picture = self.picture.take().or(other.picture);
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.username.is_some() && self.email.is_some() && self.name.is_some() && self.picture.is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.username.is_none() && self.email.is_none() && self.name.is_none() && self.picture.is_none()
    }

    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    pub fn email(&self) -> Option<&str> {
        self.email.as_deref()
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn picture(&self) -> Option<&str> {
        self.picture.as_deref()
    }
}

fn sanitize_text(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty() && value.chars().count() <= MAX_PROFILE_TEXT_CHARS && !value.chars().any(char::is_control))
        .then(|| value.to_string())
}

fn sanitize_picture(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value.len() > MAX_PROFILE_PICTURE_URL_BYTES {
        return None;
    }
    let url = Url::parse(value).ok()?;
    (url.scheme() == "https"
        && url.host_str().is_some_and(|host| !host.is_empty())
        && url.username().is_empty()
        && url.password().is_none())
    .then(|| url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn claims(value: Value) -> HashMap<String, Value> {
        serde_json::from_value(value).expect("test claims are an object")
    }

    #[test]
    fn reads_configured_claims_and_standard_display_claims() {
        let profile = FederatedProfile::from_provider_claims(
            &claims(json!({
                "upn": " j.doe ",
                "mail": "j.doe@example.test",
                "preferred_username": "ignored-because-not-configured",
                "name": "Jane Doe",
                "picture": "https://idp.example.test/avatar/1.png",
            })),
            "upn",
            "mail",
        );

        assert_eq!(profile.username(), Some("j.doe"));
        assert_eq!(profile.email(), Some("j.doe@example.test"));
        assert_eq!(profile.name(), Some("Jane Doe"));
        assert_eq!(profile.picture(), Some("https://idp.example.test/avatar/1.png"));
        assert!(profile.is_complete());
    }

    #[test]
    fn drops_values_that_fail_sanitization() {
        let profile = FederatedProfile::from_provider_claims(
            &claims(json!({
                "preferred_username": "   ",
                "email": ["array@example.test"],
                "name": "line\nbreak",
                "picture": "javascript:alert(1)",
            })),
            "preferred_username",
            "email",
        );
        assert!(profile.is_empty());

        for picture in [
            "http://idp.example.test/a.png".to_string(),
            "data:image/png;base64,AAAA".to_string(),
            "https://user:secret@idp.example.test/a.png".to_string(),
            "/relative/a.png".to_string(),
            format!("https://idp.example.test/{}", "a".repeat(MAX_PROFILE_PICTURE_URL_BYTES)),
        ] {
            let profile =
                FederatedProfile::from_provider_claims(&claims(json!({ "picture": picture })), "preferred_username", "email");
            assert_eq!(profile.picture(), None, "{picture} must be rejected");
        }

        let long_name = "n".repeat(MAX_PROFILE_TEXT_CHARS + 1);
        let profile =
            FederatedProfile::from_provider_claims(&claims(json!({ "name": long_name })), "preferred_username", "email");
        assert_eq!(profile.name(), None);
    }

    #[test]
    fn fill_missing_keeps_present_values() {
        let mut primary = FederatedProfile::from_provider_claims(
            &claims(json!({ "email": "token@example.test" })),
            "preferred_username",
            "email",
        );
        let secondary = FederatedProfile::from_provider_claims(
            &claims(json!({ "email": "userinfo@example.test", "preferred_username": "jane", "name": "Jane" })),
            "preferred_username",
            "email",
        );

        primary.fill_missing_from(secondary);

        assert_eq!(primary.email(), Some("token@example.test"));
        assert_eq!(primary.username(), Some("jane"));
        assert_eq!(primary.name(), Some("Jane"));
        assert_eq!(primary.picture(), None);
        assert!(!primary.is_complete());
    }

    #[test]
    fn claim_value_round_trips_and_is_never_a_string() {
        let profile = FederatedProfile::from_provider_claims(
            &claims(json!({ "preferred_username": "jane", "picture": "https://idp.example.test/a.png" })),
            "preferred_username",
            "email",
        );
        let value = profile.to_claim_value().expect("non-empty profile");

        assert!(value.is_object(), "policy condition expansion only reads string claims");
        assert_eq!(FederatedProfile::from_claim_value(&value), profile);
        assert_eq!(FederatedProfile::default().to_claim_value(), None);
        assert!(FederatedProfile::from_claim_value(&json!("jane")).is_empty());
        assert!(FederatedProfile::from_claim_value(&json!({ "picture": "http://insecure.example.test/a.png" })).is_empty());
    }

    #[test]
    fn debug_output_redacts_values() {
        let profile = FederatedProfile::from_provider_claims(
            &claims(json!({ "preferred_username": "secret-name", "email": "secret@example.test" })),
            "preferred_username",
            "email",
        );
        let debug = format!("{profile:?}");
        assert!(!debug.contains("secret"));
        assert!(debug.contains("username_present: true"));
    }
}
