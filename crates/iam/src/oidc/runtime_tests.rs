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

use super::*;
use crate::oidc::{config::*, provider::*, test_support::*};
use jsonwebtoken::{Algorithm, Header};
use openidconnect::ProviderMetadataWithLogout;
use rustfs_utils::egress::{ENV_OUTBOUND_ALLOW_ORIGINS, OutboundPolicy};
use std::time::{Duration as StdDuration, Instant};
use url::Url;

#[test]
fn test_extract_string_claim() {
    let mut claims = HashMap::new();
    claims.insert("email".to_string(), serde_json::json!("user@example.com"));
    claims.insert("sub".to_string(), serde_json::json!("12345"));

    assert_eq!(extract_string_claim(&claims, "email"), "user@example.com");
    assert_eq!(extract_string_claim(&claims, "sub"), "12345");
    assert_eq!(extract_string_claim(&claims, "missing"), "");
}
#[test]
fn test_extract_groups_claim_array() {
    let mut claims = HashMap::new();
    claims.insert("groups".to_string(), serde_json::json!(["admin", "developers", "readonly"]));

    let groups = extract_groups_claim(&claims, "groups");
    assert_eq!(groups, vec!["admin", "developers", "readonly"]);
}
#[test]
fn test_extract_groups_claim_string() {
    let mut claims = HashMap::new();
    claims.insert("groups".to_string(), serde_json::json!("admin,developers"));

    let groups = extract_groups_claim(&claims, "groups");
    assert_eq!(groups, vec!["admin", "developers"]);
}
#[test]
fn test_extract_groups_claim_missing() {
    let claims = HashMap::new();
    let groups = extract_groups_claim(&claims, "groups");
    assert!(groups.is_empty());
}
#[test]
fn test_extract_groups_claim_number() {
    let mut claims = HashMap::new();
    claims.insert("groups".to_string(), serde_json::json!(42));
    let groups = extract_groups_claim(&claims, "groups");
    assert!(groups.is_empty());
}
#[test]
fn test_extract_canonical_group_values_merges_groups_and_roles() {
    let mut claims = HashMap::new();
    claims.insert("groups".to_string(), serde_json::json!(["devs", "admins"]));
    claims.insert("roles".to_string(), serde_json::json!(["admins", "consoleAdmin"]));

    let merged = extract_canonical_group_values(&claims, "groups", "roles");
    assert_eq!(merged, vec!["admins", "consoleAdmin", "devs"]);
}
#[test]
fn test_extract_canonical_group_values_skips_duplicate_claim_name() {
    let mut claims = HashMap::new();
    claims.insert("roles".to_string(), serde_json::json!(["consoleAdmin"]));

    let merged = extract_canonical_group_values(&claims, "roles", "roles");
    assert_eq!(merged, vec!["consoleAdmin"]);
}
#[test]
fn test_extract_canonical_group_values_roles_only() {
    let mut claims = HashMap::new();
    claims.insert("roles".to_string(), serde_json::json!(["consoleAdmin", "bucket-reader"]));

    let merged = extract_canonical_group_values(&claims, "groups", "roles");
    assert_eq!(merged, vec!["bucket-reader", "consoleAdmin"]);
}
#[test]
fn test_extract_string_claim_case_insensitive() {
    let mut claims = HashMap::new();
    claims.insert("policyminio".to_string(), serde_json::json!("consoleAdmin"));

    assert_eq!(extract_string_claim(&claims, "policyMinio"), "consoleAdmin");
    assert_eq!(extract_string_claim(&claims, "POLICYMINIO"), "consoleAdmin");
    assert_eq!(extract_string_claim(&claims, "policyminio"), "consoleAdmin");
}
#[test]
fn test_extract_groups_claim_case_insensitive() {
    let mut claims = HashMap::new();
    claims.insert("policyminio".to_string(), serde_json::json!(["consoleAdmin", "readwrite"]));

    let groups = extract_groups_claim(&claims, "policyMinio");
    assert_eq!(groups, vec!["consoleAdmin", "readwrite"]);

    let groups = extract_groups_claim(&claims, "POLICYMINIO");
    assert_eq!(groups, vec!["consoleAdmin", "readwrite"]);

    let groups = extract_groups_claim(&claims, "policyminio");
    assert_eq!(groups, vec!["consoleAdmin", "readwrite"]);
}
#[test]
fn test_extract_groups_claim_exact_match_preferred() {
    let mut claims = HashMap::new();
    claims.insert("Policy".to_string(), serde_json::json!(["exact_match"]));
    claims.insert("policy".to_string(), serde_json::json!(["lowercase"]));

    let groups = extract_groups_claim(&claims, "Policy");
    assert_eq!(groups, vec!["exact_match"]);
}
#[test]
fn test_extract_string_claim_ambiguous_case_insensitive_match_returns_empty() {
    let mut claims = HashMap::new();
    claims.insert("Policy".to_string(), serde_json::json!("exact_match"));
    claims.insert("policy".to_string(), serde_json::json!("lowercase"));

    assert_eq!(extract_string_claim(&claims, "POLICY"), "");
}
#[test]
fn test_extract_groups_claim_ambiguous_case_insensitive_match_returns_empty() {
    let mut claims = HashMap::new();
    claims.insert("Policy".to_string(), serde_json::json!(["exact_match"]));
    claims.insert("policy".to_string(), serde_json::json!(["lowercase"]));

    let groups = extract_groups_claim(&claims, "POLICY");
    assert!(groups.is_empty());
}
#[test]
fn test_decode_jwt_payload() {
    let payload = r#"{"sub":"user123","email":"user@example.com"}"#;
    let payload_b64 = base64_simd::URL_SAFE_NO_PAD.encode_to_string(payload.as_bytes());
    let token = format!("eyJhbGciOiJSUzI1NiJ9.{payload_b64}.signature");

    let claims = decode_jwt_payload(&token);
    assert_eq!(claims.get("sub").and_then(|v| v.as_str()), Some("user123"));
    assert_eq!(claims.get("email").and_then(|v| v.as_str()), Some("user@example.com"));
}
#[tokio::test]
async fn web_identity_verification_refreshes_rotated_jwks() {
    let (_, initial_jwk) = oidc_es256_key_and_jwk("initial");
    let (rotated_key, rotated_jwk) = oidc_es256_key_and_jwk("rotated");
    let initial_jwks = serde_json::json!({ "keys": [initial_jwk] }).to_string();
    let rotated_jwks = serde_json::json!({ "keys": [rotated_jwk] }).to_string();
    let Some((base, handle)) = start_mock_oidc_discovery_server_with_jwks(
        |base| (base.to_string(), format!("{base}/jwks"), "/jwks".to_string()),
        4,
        "ES256",
        false,
        move |fetch| {
            if fetch == 0 {
                initial_jwks.clone()
            } else {
                rotated_jwks.clone()
            }
        },
    ) else {
        return;
    };

    let config = build_mocked_oidc_provider_config("rotating", &base);
    let policy = OutboundPolicy::from_allowed_origins(&base).expect("loopback origin should be allowed");
    let http_client = ReqwestHttpClient::with_policy(policy);
    let state = discover_provider(&config, &http_client)
        .await
        .expect("initial OIDC discovery should succeed");
    let sys = OidcSys {
        configs: HashMap::from([(config.id.clone(), test_sourced_config(config.clone()))]),
        provider_runtime: ProviderRuntime::new(http_client, HashMap::from([(config.id.clone(), state)])),
        state_store: OidcStateStore::new(),
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after Unix epoch")
        .as_secs();
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some("rotated".to_string());
    let token = jsonwebtoken::encode(
        &header,
        &serde_json::json!({
            "iss": base,
            "sub": "rotated-user",
            "aud": config.client_id,
            "iat": now,
            "exp": now + 300,
            "groups": ["readwrite"],
        }),
        &rotated_key,
    )
    .expect("rotated OIDC token should sign");

    let (claims, provider_id) = sys
        .verify_web_identity_token(&token)
        .await
        .expect("verification should refresh JWKS and accept the rotated key");
    assert_eq!(provider_id, "rotating");
    assert_eq!(claims.sub, "rotated-user");
    assert_eq!(claims.groups, vec!["readwrite"]);
    handle.join().expect("rotating JWKS mock server should exit cleanly");
}
#[tokio::test]
async fn complete_provider_console_login_preserves_hidden_and_issuer_modes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    for hidden in [false, true] {
        for explicit in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let issuer = if explicit {
                "https://issuer.example.com".to_string()
            } else {
                base.clone()
            };
            let mut config = build_mocked_oidc_provider_config("console", &format!("{base}/.well-known/openid-configuration"));
            config.hide_from_ui = hidden;
            config.issuer = explicit.then(|| issuer.clone());
            config.client_secret = Some(Nonce::new_random().secret().clone());
            let server_config = config.clone();
            let server_base = base.clone();
            let redirect = "https://console.example.com/oauth_callback";
            let (key, jwk) = oidc_es256_key_and_jwk("console");
            let (auth_tx, auth_rx) = tokio::sync::oneshot::channel::<HashMap<String, String>>();
            let server = tokio::spawn(async move {
                let mut auth_rx = Some(auth_rx);
                for expected_path in ["/.well-known/openid-configuration", "/jwks", "/token"] {
                    let (mut stream, _) = tokio::time::timeout(StdDuration::from_secs(10), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                    let mut bytes = Vec::new();
                    let header_end = loop {
                        bytes.push(stream.read_u8().await.unwrap());
                        assert!(bytes.len() < 8192);
                        if bytes.ends_with(b"\r\n\r\n") {
                            break bytes.len();
                        }
                    };
                    let headers = String::from_utf8(bytes).unwrap();
                    let request_line = headers.lines().next().unwrap();
                    assert_eq!(request_line.split_whitespace().nth(1), Some(expected_path));
                    let headers_map: HashMap<_, _> = headers
                        .lines()
                        .skip(1)
                        .filter_map(|line| line.split_once(':'))
                        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
                        .collect();
                    let body = match expected_path {
                        "/.well-known/openid-configuration" => {
                            assert!(request_line.starts_with("GET "));
                            assert_eq!(headers_map["accept"], "application/json");
                            serde_json::json!({
                                "issuer": issuer, "authorization_endpoint": format!("{server_base}/authorize"),
                                "token_endpoint": format!("{server_base}/token"), "jwks_uri": format!("{server_base}/jwks"),
                                "response_types_supported": ["code"], "subject_types_supported": ["public"],
                                "id_token_signing_alg_values_supported": ["ES256"]
                            })
                        }
                        "/jwks" => {
                            assert!(request_line.starts_with("GET "));
                            assert!(headers_map["accept"].contains("application/json"));
                            serde_json::json!({"keys": [jwk]})
                        }
                        "/token" => {
                            assert!(request_line.starts_with("POST "));
                            assert_eq!(headers_map["accept"], "application/json");
                            assert!(headers_map["content-type"].starts_with("application/x-www-form-urlencoded"));
                            let length: usize = headers_map["content-length"].parse().unwrap();
                            assert!(header_end + length < 16384);
                            let mut body = vec![0; length];
                            stream.read_exact(&mut body).await.unwrap();
                            let form: HashMap<String, String> = url::form_urlencoded::parse(&body).into_owned().collect();
                            assert_eq!(form["grant_type"], "authorization_code");
                            assert_eq!(form["code"], "test-authorization-code");
                            assert_eq!(form["client_id"], server_config.client_id);
                            assert_eq!(Some(&form["client_secret"]), server_config.client_secret.as_ref());
                            assert_eq!(form["redirect_uri"], redirect);
                            let auth = auth_rx.take().unwrap().await.unwrap();
                            let challenge = PkceCodeChallenge::from_code_verifier_sha256(&PkceCodeVerifier::new(
                                form["code_verifier"].clone(),
                            ));
                            assert_eq!(challenge.as_str(), auth["code_challenge"]);
                            let now = time::OffsetDateTime::now_utc().unix_timestamp();
                            let mut header = Header::new(Algorithm::ES256);
                            header.kid = Some("console".into());
                            let token = jsonwebtoken::encode(
                                &header,
                                &serde_json::json!({
                                    "iss": issuer, "sub": "existing-user", "aud": server_config.client_id,
                                    "iat": now, "exp": now + 300, "nonce": auth["nonce"],
                                    "email": "user@example.com", "groups": ["readwrite"]
                                }),
                                &key,
                            )
                            .unwrap();
                            serde_json::json!({"access_token": "test-access-token", "token_type": "Bearer", "id_token": token})
                        }
                        _ => unreachable!(),
                    }
                    .to_string();
                    stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                }
            });
            let http_client = ReqwestHttpClient::with_policy(OutboundPolicy::from_allowed_origins(&base).unwrap());
            let discovered = discover_provider(&config, &http_client).await.unwrap();
            let sys = OidcSys {
                configs: HashMap::from([(config.id.clone(), test_sourced_config(config))]),
                provider_runtime: ProviderRuntime::new(http_client, HashMap::from([("console".into(), discovered)])),
                state_store: OidcStateStore::new(),
            };
            let auth_url = sys.authorize_url("console", redirect, Some("/buckets".into())).await.unwrap();
            let auth_url = Url::parse(&auth_url).unwrap();
            assert_eq!(auth_url.as_str().split('?').next(), Some(format!("{base}/authorize").as_str()));
            let auth: HashMap<String, String> = auth_url.query_pairs().into_owned().collect();
            assert_eq!(auth["response_type"], "code");
            assert_eq!(auth["client_id"], "rustfs-oidc-test");
            assert!(!auth["nonce"].is_empty());
            assert!(!auth["state"].is_empty());
            assert_eq!(auth["redirect_uri"], redirect);
            assert_eq!(auth["code_challenge_method"], "S256");
            assert!(auth["scope"].split_whitespace().any(|scope| scope == "openid"));
            let state = auth["state"].clone();
            auth_tx.send(auth).unwrap();
            let (claims, provider, session, _) = sys
                .exchange_code(&state, "test-authorization-code", redirect)
                .await
                .unwrap_or_else(|err| panic!("hidden={hidden}, explicit={explicit}: {err}"));
            assert_eq!(provider, "console");
            assert_eq!(claims.sub, "existing-user");
            assert_eq!(claims.email, "user@example.com");
            assert_eq!(claims.groups, vec!["readwrite"]);
            assert_eq!(session.redirect_after.as_deref(), Some("/buckets"));
            assert!(matches!(sys.exchange_code(&state, "test-authorization-code", redirect).await,
                Err(error) if error == "invalid or expired OIDC state"));
            server.await.unwrap();
        }
    }
}
#[test]
fn test_decode_jwt_payload_invalid() {
    assert!(decode_jwt_payload("not-a-jwt").is_empty());
    assert!(decode_jwt_payload("").is_empty());
}
#[test]
fn test_core_token_response_accepts_rfc3339_updated_at() {
    // Signature verification happens later; this test covers token response deserialization.
    let id_token = "eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguZXhhbXBsZS5jb20vb2lkYyIsInN1YiI6InVzZXItMSIsImF1ZCI6InJ1c3RmcyIsImV4cCI6MTc4NDQzMjc2OSwiaWF0IjoxNzgzMjIzMTY5LCJ1cGRhdGVkX2F0IjoiMjAyNi0wNy0wM1QwNDo1MDo1MC44MTFaIn0.c2ln";
    let body = serde_json::json!({
        "scope": "openid roles profile email",
        "token_type": "Bearer",
        "access_token": "access-token",
        "expires_in": 1209600,
        "id_token": id_token,
    })
    .to_string();

    let response: openidconnect::core::CoreTokenResponse =
        serde_json::from_str(&body).expect("RFC3339 updated_at should parse in token response");

    assert!(response.extra_fields().id_token().is_some());
}
#[test]
fn test_map_claims_to_policies_no_provider() {
    let sys = OidcSys::empty().expect("failed to initialize empty OIDC system");

    let claims = OidcClaims {
        sub: "user123".to_string(),
        email: "user@example.com".to_string(),
        username: "user".to_string(),
        groups: vec!["admin".to_string(), "devs".to_string()],
        raw: HashMap::new(),
    };

    let (policies, groups) = sys.map_claims_to_policies("nonexistent", &claims);
    assert!(policies.is_empty());
    assert!(groups.is_empty());
}
#[test]
fn test_oidc_claims_default() {
    let claims = OidcClaims::default();
    assert!(claims.sub.is_empty());
    assert!(claims.email.is_empty());
    assert!(claims.username.is_empty());
    assert!(claims.groups.is_empty());
    assert!(claims.raw.is_empty());
}
#[test]
fn test_oidc_claims_serde_roundtrip() {
    let claims = OidcClaims {
        sub: "user123".to_string(),
        email: "user@example.com".to_string(),
        username: "testuser".to_string(),
        groups: vec!["admin".to_string(), "devs".to_string()],
        raw: {
            let mut m = HashMap::new();
            m.insert("custom".to_string(), serde_json::json!("value"));
            m
        },
    };

    let json = serde_json::to_string(&claims).unwrap();
    let deserialized: OidcClaims = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.sub, "user123");
    assert_eq!(deserialized.email, "user@example.com");
    assert_eq!(deserialized.groups.len(), 2);
}
#[test]
fn test_oidc_provider_summary_serde() {
    let summary = OidcProviderSummary {
        provider_id: "okta".to_string(),
        display_name: "Okta SSO".to_string(),
    };

    let json = serde_json::to_string(&summary).unwrap();
    let deserialized: OidcProviderSummary = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.provider_id, "okta");
    assert_eq!(deserialized.display_name, "Okta SSO");
}
#[test]
fn test_oidc_sys_empty() {
    let sys = OidcSys::empty().expect("failed to initialize empty OIDC system");
    assert!(!sys.has_providers());
    assert!(sys.config_snapshot().providers().is_empty());
}
#[tokio::test]
async fn oidc_token_exchange_reports_forbidden_token_endpoint() {
    let provider_id = "default";
    let token_endpoint = "http://192.168.65.254:8080/realms/rustfs/protocol/openid-connect/token";
    let metadata = serde_json::from_value::<ProviderMetadataWithLogout>(serde_json::json!({
        "issuer": "https://idp.example.com/realms/rustfs",
        "authorization_endpoint": "https://idp.example.com/realms/rustfs/protocol/openid-connect/auth",
        "token_endpoint": token_endpoint,
        "jwks_uri": "https://idp.example.com/realms/rustfs/protocol/openid-connect/certs",
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"]
    }))
    .expect("provider metadata should parse");
    let config =
        build_mocked_oidc_provider_config(provider_id, "https://idp.example.com/realms/rustfs/.well-known/openid-configuration");
    let state_store = OidcStateStore::new();
    state_store
        .insert(
            "test-state".to_string(),
            OidcAuthSession {
                provider_id: provider_id.to_string(),
                pkce_verifier: "test-pkce-verifier".to_string(),
                nonce: "test-nonce".to_string(),
                redirect_after: None,
            },
        )
        .await;
    let sys = OidcSys {
        configs: HashMap::from([(provider_id.to_string(), test_sourced_config(config))]),
        provider_runtime: ProviderRuntime::new(
            ReqwestHttpClient::with_policy(OutboundPolicy::default()),
            HashMap::from([(
                provider_id.to_string(),
                ProviderState {
                    metadata: DiscoveredProviderMetadata::Console(Box::new(metadata)),
                    discovered_at: Instant::now(),
                },
            )]),
        ),
        state_store,
    };

    let error = match sys
        .exchange_code("test-state", "test-code", "https://console.example.com/oauth_callback")
        .await
    {
        Ok(_) => panic!("private token endpoint should require an explicit allowlist origin"),
        Err(error) => error,
    };

    assert!(error.contains("request_error_kind=forbidden_outbound"));
    assert!(error.contains(&format!("add http://192.168.65.254:8080 to {ENV_OUTBOUND_ALLOW_ORIGINS}")));
}
#[test]
fn test_list_visible_providers_hides_hidden_provider() {
    let visible = test_config("dex");
    let mut hidden = test_config("kubernetes");
    hidden.hide_from_ui = true;

    let sys = make_test_sys(vec![visible, hidden]);
    let listed = sys.list_visible_providers();

    assert_eq!(listed.len(), 1);
    assert!(listed.iter().any(|provider| provider.provider_id == "dex"));
    assert!(!listed.iter().any(|provider| provider.provider_id == "kubernetes"));
}
#[test]
fn test_hidden_provider_still_resolvable_for_sts() {
    let visible = test_config("dex");
    let mut hidden = test_config("kubernetes");
    hidden.hide_from_ui = true;

    let sys = make_test_sys(vec![visible, hidden]);

    assert!(sys.get_provider_config("kubernetes").is_some());
    assert!(sys.get_provider_config("dex").is_some());
}
#[test]
fn test_config_snapshot_includes_hidden_for_replication() {
    let visible = test_config("dex");
    let mut hidden = test_config("kubernetes");
    hidden.hide_from_ui = true;

    let sys = make_test_sys(vec![visible, hidden]);

    assert_eq!(sys.config_snapshot().providers().len(), 2);
}
#[test]
fn test_list_providers_includes_hidden_for_compatibility() {
    let visible = test_config("dex");
    let mut hidden = test_config("kubernetes");
    hidden.hide_from_ui = true;

    let sys = make_test_sys(vec![visible, hidden]);

    assert_eq!(sys.list_providers().len(), 2);
    assert_eq!(sys.list_visible_providers().len(), 1);
}
#[test]
fn role_policy_does_not_map_groups_as_policies() {
    let mut config = test_config("authentik");
    config.role_policy = "consoleAdmin".to_string();
    config.claim_name = "policy".to_string();

    let sys = make_test_sys(vec![config]);

    let claims = OidcClaims {
        groups: vec!["authentik Admins".to_string(), "users".to_string()],
        raw: HashMap::from([("policy".to_string(), serde_json::json!(["readonly"]))]),
        ..Default::default()
    };

    let (policies, groups) = sys.map_claims_to_policies("authentik", &claims);
    assert_eq!(groups, vec!["authentik Admins", "users"]);
    assert_eq!(policies, vec!["consoleAdmin"]);
}
#[test]
fn group_claim_policy_names_cover_only_prefixed_group_values() {
    let mut config = test_config("ad");
    config.claim_prefix = "oidc-".to_string();
    let sys = make_test_sys(vec![config]);
    let claims = OidcClaims {
        groups: vec![
            "EXAMPLE\\Domain Users".to_string(),
            "admins".to_string(),
            "admins".to_string(),
        ],
        ..Default::default()
    };

    assert_eq!(
        sys.group_claim_policy_names("ad", &claims),
        vec!["oidc-EXAMPLE\\Domain Users", "oidc-admins"]
    );
    assert!(sys.group_claim_policy_names("missing-provider", &claims).is_empty());
}
#[test]
fn group_claim_policy_names_include_merged_roles_claim_values() {
    let mut config = test_config("keycloak");
    config.roles_claim = "roles".to_string();
    let raw = HashMap::from([
        ("groups".to_string(), serde_json::json!(["readonly"])),
        ("roles".to_string(), serde_json::json!(["offline_access", "default-roles-corp"])),
    ]);
    let claims = OidcClaims {
        groups: extract_canonical_group_values(&raw, &config.groups_claim, &config.roles_claim),
        raw,
        ..Default::default()
    };
    let sys = make_test_sys(vec![config]);

    assert_eq!(
        sys.group_claim_policy_names("keycloak", &claims),
        vec!["default-roles-corp", "offline_access", "readonly"]
    );
}
#[test]
fn group_claim_policy_names_exclude_role_policy_and_dedicated_policy_claim() {
    let mut role_config = test_config("role");
    role_config.role_policy = "consoleAdmin".to_string();
    let mut claim_config = test_config("claim");
    claim_config.claim_name = "policy".to_string();
    let sys = make_test_sys(vec![role_config, claim_config]);
    let claims = OidcClaims {
        groups: vec!["readonly".to_string(), "unmapped".to_string()],
        raw: HashMap::from([("policy".to_string(), serde_json::json!(["readonly", "writeonly"]))]),
        ..Default::default()
    };

    assert!(sys.group_claim_policy_names("role", &claims).is_empty());
    // `readonly` is also requested explicitly through the policy claim, so it must resolve.
    assert_eq!(sys.group_claim_policy_names("claim", &claims), vec!["unmapped"]);
}
#[test]
fn test_map_claims_to_policies_with_prefix() {
    let mut config = test_config("azure");
    config.claim_prefix = "oidc-".to_string();
    config.display_name = "Azure AD".to_string();

    let sys = make_test_sys(vec![config]);

    let claims = OidcClaims {
        sub: "user456".to_string(),
        email: "user@corp.com".to_string(),
        username: "user".to_string(),
        groups: vec!["engineers".to_string()],
        raw: HashMap::new(),
    };

    let (policies, groups) = sys.map_claims_to_policies("azure", &claims);
    assert_eq!(groups, vec!["engineers"]);
    assert!(policies.contains(&"oidc-engineers".to_string()));
    assert_eq!(policies.len(), 1);
}
#[test]
fn blank_role_policy_uses_claim_mapping() {
    let mut config = test_config("keycloak");
    config.role_policy = "   ".to_string();

    let sys = make_test_sys(vec![config]);
    let claims = OidcClaims {
        groups: vec!["readonly".to_string()],
        ..Default::default()
    };

    let (policies, groups) = sys.map_claims_to_policies("keycloak", &claims);
    assert_eq!(groups, vec!["readonly"]);
    assert_eq!(policies, vec!["readonly"]);
}
#[test]
fn claim_mapping_keeps_groups_with_distinct_primary_claim() {
    let mut config = test_config("keycloak");
    config.claim_name = "policy".to_string();

    let sys = make_test_sys(vec![config]);
    let claims = OidcClaims {
        groups: vec!["developers".to_string()],
        raw: HashMap::from([("policy".to_string(), serde_json::json!(["readonly"]))]),
        ..Default::default()
    };

    let (policies, groups) = sys.map_claims_to_policies("keycloak", &claims);
    assert_eq!(groups, vec!["developers"]);
    assert_eq!(policies, vec!["developers", "readonly"]);
}
#[test]
fn test_config_snapshot_lists_providers() {
    let mut config = test_config("keycloak");
    config.display_name = "Keycloak SSO".to_string();

    let sys = make_test_sys(vec![config]);

    assert!(sys.has_providers());
    let snapshot = sys.config_snapshot();
    assert_eq!(snapshot.providers().len(), 1);
    assert_eq!(snapshot.providers()[0].config.id, "keycloak");
    assert_eq!(snapshot.providers()[0].config.display_name, "Keycloak SSO");
    assert_eq!(snapshot.providers()[0].source, OidcProviderConfigSource::Persisted);
}
#[test]
fn test_get_provider_config() {
    let mut config = test_config("test");
    config.client_id = "my-client".to_string();
    config.client_secret = Some("secret".to_string());

    let sys = make_test_sys(vec![config]);

    assert!(sys.get_provider_config("test").is_some());
    assert_eq!(sys.get_provider_config("test").unwrap().client_id, "my-client");
    assert!(sys.get_provider_config("nonexistent").is_none());
}
#[test]
fn verified_identity_preserves_provider_and_claim_values() {
    let raw = HashMap::from([
        ("iss".to_string(), serde_json::json!("https://corp.example.test")),
        ("department".to_string(), serde_json::json!("engineering")),
        ("roles".to_string(), serde_json::json!(["reader", "admin"])),
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
#[test]
fn adapter_preserves_provider_views_and_redirect_policy() {
    let mut visible = test_config("default");
    visible.display_name = "Company SSO".to_string();
    visible.redirect_uri = Some("https://console.example.test/callback".to_string());
    visible.redirect_uri_dynamic = false;
    let mut hidden = test_config("workload");
    hidden.display_name = "Workload Identity".to_string();
    hidden.hide_from_ui = true;
    let adapter = StandardOidcAdapter::new(Arc::new(make_test_sys(vec![visible, hidden])));

    let mut providers = adapter.list_providers();
    providers.sort_by(|left, right| left.provider_id.cmp(&right.provider_id));
    assert_eq!(
        providers,
        vec![
            FederatedProviderView {
                provider_id: "default".to_string(),
                display_name: "Company SSO".to_string(),
            },
            FederatedProviderView {
                provider_id: "workload".to_string(),
                display_name: "Workload Identity".to_string(),
            },
        ]
    );
    assert_eq!(
        adapter.list_visible_providers(),
        vec![FederatedProviderView {
            provider_id: "default".to_string(),
            display_name: "Company SSO".to_string(),
        }]
    );
    assert_eq!(
        adapter.redirect_policy("default"),
        Some(FederatedRedirectPolicy {
            redirect_uri: Some("https://console.example.test/callback".to_string()),
            allow_request_origin: false,
        })
    );
    assert_eq!(adapter.redirect_policy("missing"), None);
}
