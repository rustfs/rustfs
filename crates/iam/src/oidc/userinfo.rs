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

//! OpenID Connect UserInfo support (`claim_userinfo`).
//!
//! Some identity providers, community AAI proxies among them, release the
//! authorization claims (`eduperson_entitlement`) only through the UserInfo endpoint and
//! keep the ID token small. When a provider enables `claim_userinfo`, RustFS fetches
//! UserInfo with the access token after the ID token was verified and merges the claims
//! the ID token does not carry. The ID token always wins on a key present in both.

use super::transport::{ReqwestHttpClient, oidc_http_error_diagnostics};
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use openidconnect::AsyncHttpClient;
use std::collections::HashMap;

/// `GET userinfo_endpoint` with the bearer `access_token`, through the shared OIDC HTTP
/// adapter (egress allowlist, timeouts, bounded body). Only `application/json` bodies are
/// accepted; a signed (`application/jwt`) UserInfo response is rejected. Every error names its
/// `stage=` so operators can tell apart egress, HTTP status, content type and parsing failures.
pub(super) async fn fetch_userinfo_claims(
    http_client: &ReqwestHttpClient,
    userinfo_endpoint: &str,
    access_token: &str,
) -> Result<HashMap<String, serde_json::Value>, String> {
    if access_token.is_empty() || !access_token.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err("userinfo access token contains invalid characters: stage=userinfo_access_token".to_string());
    }
    let authorization = http::HeaderValue::from_str(&format!("Bearer {access_token}"))
        .map_err(|_| "userinfo access token contains invalid characters: stage=userinfo_access_token".to_string())?;
    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri(userinfo_endpoint)
        .header(AUTHORIZATION, authorization)
        .header(ACCEPT, "application/json")
        .body(Vec::new())
        .map_err(|err| format!("userinfo request could not be built: {err}: stage=userinfo_request_build"))?;

    let response = http_client.call(request).await.map_err(|err| {
        let (kind, detail) = oidc_http_error_diagnostics(&err);
        format!(
            "userinfo request failed: {err}: stage=userinfo_request_failed, request_error_kind={kind}, request_error_detail={detail}, userinfo_endpoint={userinfo_endpoint}"
        )
    })?;

    let status = response.status();
    if !status.is_success() {
        return Err(format!(
            "userinfo request failed with status {}: stage=userinfo_status, userinfo_endpoint={userinfo_endpoint}",
            status.as_u16()
        ));
    }
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let media_type = content_type.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    if media_type != "application/json" {
        return Err(format!(
            "userinfo response is not JSON (content-type {content_type}): stage=userinfo_content_type, userinfo_endpoint={userinfo_endpoint}"
        ));
    }
    match serde_json::from_slice::<serde_json::Value>(response.body()) {
        Ok(serde_json::Value::Object(claims)) => Ok(claims.into_iter().collect()),
        Ok(_) => Err(format!(
            "userinfo response is not a JSON object: stage=userinfo_parse, userinfo_endpoint={userinfo_endpoint}"
        )),
        Err(err) => Err(format!(
            "userinfo response is not valid JSON: {err}: stage=userinfo_parse, userinfo_endpoint={userinfo_endpoint}"
        )),
    }
}

/// Merge UserInfo claims into the verified ID token claims. UserInfo must carry a string `sub`
/// equal to the ID token `sub` (OpenID Connect Core 5.3.2); keys already present in the ID
/// token are kept. Returns the number of claims added.
pub(super) fn merge_userinfo_claims(
    claims: &mut HashMap<String, serde_json::Value>,
    userinfo: HashMap<String, serde_json::Value>,
) -> Result<usize, String> {
    let Some(userinfo_sub) = userinfo.get("sub").and_then(serde_json::Value::as_str) else {
        return Err("userinfo sub is missing or not a string: stage=userinfo_sub_invalid".to_string());
    };
    let id_token_sub = claims.get("sub").and_then(serde_json::Value::as_str).unwrap_or_default();
    if userinfo_sub != id_token_sub {
        return Err("userinfo sub does not match the ID token sub: stage=userinfo_sub_mismatch".to_string());
    }

    let mut added = 0;
    for (key, value) in userinfo {
        if let std::collections::hash_map::Entry::Vacant(slot) = claims.entry(key) {
            slot.insert(value);
            added += 1;
        }
    }
    Ok(added)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn claims(pairs: &[(&str, serde_json::Value)]) -> HashMap<String, serde_json::Value> {
        pairs.iter().map(|(key, value)| (key.to_string(), value.clone())).collect()
    }

    #[test]
    fn merge_keeps_id_token_values_and_adds_missing() {
        let mut id_token = claims(&[("sub", json!("user-1")), ("groups", json!(["from-id-token"]))]);
        let userinfo = claims(&[
            ("sub", json!("user-1")),
            ("groups", json!(["from-userinfo"])),
            ("eduperson_entitlement", json!(["urn:a"])),
        ]);

        let added = merge_userinfo_claims(&mut id_token, userinfo).expect("same subject merges");

        assert_eq!(added, 1);
        assert_eq!(id_token["groups"], json!(["from-id-token"]));
        assert_eq!(id_token["eduperson_entitlement"], json!(["urn:a"]));
    }

    #[test]
    fn merge_rejects_missing_sub() {
        let mut id_token = claims(&[("sub", json!("user-1"))]);
        let err = merge_userinfo_claims(&mut id_token, claims(&[("groups", json!(["x"]))])).unwrap_err();
        assert!(err.contains("stage=userinfo_sub_invalid"), "{err}");

        let mut id_token = claims(&[("sub", json!("user-1"))]);
        let err = merge_userinfo_claims(&mut id_token, claims(&[("sub", json!(42))])).unwrap_err();
        assert!(err.contains("stage=userinfo_sub_invalid"), "{err}");
        assert_eq!(id_token.len(), 1, "a rejected merge leaves the ID token claims untouched");
    }

    #[test]
    fn merge_rejects_sub_mismatch() {
        let mut id_token = claims(&[("sub", json!("user-1"))]);
        let err = merge_userinfo_claims(&mut id_token, claims(&[("sub", json!("user-2"))])).unwrap_err();
        assert!(err.contains("stage=userinfo_sub_mismatch"), "{err}");
    }
}
