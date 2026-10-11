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
use crate::oidc::{runtime::*, state::*, test_support::*, transport::*};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use openidconnect::core::CoreIdToken;
use openidconnect::{IssuerUrl, JsonWebKeySetUrl, Nonce};
use rustfs_utils::egress::{ENV_OUTBOUND_ALLOW_ORIGINS, OutboundPolicy};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use url::Url;

#[test]
fn test_normalize_issuer_matches() {
    let lhs = normalize_issuer("https://idp.example.com/.well-known/openid-configuration/").unwrap();
    let rhs = normalize_issuer("https://idp.example.com/.well-known/openid-configuration").unwrap();
    assert_eq!(lhs, rhs);
    assert_eq!(
        lhs,
        (
            "https".to_string(),
            "idp.example.com".to_string(),
            443,
            "/.well-known/openid-configuration".to_string()
        )
    );
}
#[test]
fn test_normalize_config_url() {
    // --- Well-known suffix stripping ---
    // Bare well-known URL → stripped to just the host
    assert_eq!(
        normalize_config_url("https://idp.example.com/.well-known/openid-configuration").unwrap(),
        "https://idp.example.com"
    );
    // Trailing slash after well-known suffix is also stripped
    assert_eq!(
        normalize_config_url("https://idp.example.com/.well-known/openid-configuration/").unwrap(),
        "https://idp.example.com"
    );
    // Well-known under a sub-path (Keycloak realms)
    assert_eq!(
        normalize_config_url("https://keycloak.example.com/realms/myrealm/.well-known/openid-configuration").unwrap(),
        "https://keycloak.example.com/realms/myrealm"
    );

    // --- Providers WITHOUT trailing slash (Keycloak, Auth0, Okta, Google) ---
    assert_eq!(
        normalize_config_url("https://keycloak.example.com/realms/myrealm").unwrap(),
        "https://keycloak.example.com/realms/myrealm"
    );
    assert_eq!(
        normalize_config_url("https://idp.example.com/custom/realm").unwrap(),
        "https://idp.example.com/custom/realm"
    );

    // --- Providers WITH trailing slash (Authentik) ---
    assert_eq!(
        normalize_config_url("https://auth.example.com/application/o/myapp/").unwrap(),
        "https://auth.example.com/application/o/myapp/"
    );

    // --- Root-level issuer (bare host) ---
    assert_eq!(normalize_config_url("https://idp.example.com").unwrap(), "https://idp.example.com");
    assert_eq!(normalize_config_url("https://idp.example.com/").unwrap(), "https://idp.example.com");

    // --- Custom port ---
    assert_eq!(
        normalize_config_url("https://idp.example.com:8443/auth/realms/test").unwrap(),
        "https://idp.example.com:8443/auth/realms/test"
    );
    assert_eq!(
        normalize_config_url("http://localhost:8080/application/o/app/").unwrap(),
        "http://localhost:8080/application/o/app/"
    );

    // --- Error cases ---
    assert!(normalize_config_url("https://idp.example.com/.well-known/invalid").is_err());
    assert!(normalize_config_url("gopher://idp.example.com").is_err());
    assert!(normalize_config_url("not-a-url").is_err());
}
#[test]
fn test_discovery_url_from_config_url() {
    assert_eq!(
        discovery_url_from_config_url("https://idp.example.com/.well-known/openid-configuration")
            .expect("config URL should parse")
            .as_str(),
        "https://idp.example.com/.well-known/openid-configuration"
    );
    assert_eq!(
        discovery_url_from_config_url("https://idp.example.com/realms/app")
            .expect("issuer URL should derive discovery URL")
            .as_str(),
        "https://idp.example.com/realms/app/.well-known/openid-configuration"
    );
    assert!(discovery_url_from_config_url("https://idp.example.com/.well-known/not-openid").is_err());
}
#[test]
fn test_issuer_candidates() {
    assert_eq!(
        issuer_candidates("https://idp.example.com/realm"),
        vec![
            "https://idp.example.com/realm".to_string(),
            "https://idp.example.com/realm/".to_string()
        ]
    );
    assert_eq!(
        issuer_candidates("https://idp.example.com/realm/"),
        vec![
            "https://idp.example.com/realm/".to_string(),
            "https://idp.example.com/realm".to_string()
        ]
    );
    assert_eq!(
        issuer_candidates("https://idp.example.com"),
        vec!["https://idp.example.com".to_string(), "https://idp.example.com/".to_string()]
    );
}
#[test]
fn workload_metadata_requires_hidden_provider_and_valid_verification_fields() {
    let document = serde_json::json!({
        "issuer": "https://issuer.example.com",
        "jwks_uri": "https://issuer.example.com/jwks",
        "response_types_supported": ["id_token"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["ES256"],
    });
    let parse =
        |value: &serde_json::Value, hidden| DiscoveredProviderMetadata::parse(&serde_json::to_vec(value).unwrap(), hidden);
    let metadata = parse(&document, true).expect("hidden workload metadata should parse");
    assert!(metadata.authorization_endpoint().is_none());
    assert!(metadata.console().err().unwrap().contains("only web identity"));
    assert!(parse(&document, false).err().unwrap().contains("authorization_endpoint"));
    for field in ["issuer", "jwks_uri", "id_token_signing_alg_values_supported"] {
        let mut invalid = document.clone();
        invalid.as_object_mut().unwrap().remove(field);
        assert!(parse(&invalid, true).err().unwrap().contains(field), "missing {field}");
    }
    for endpoint in [serde_json::Value::Null, serde_json::json!(""), serde_json::json!("not a URL")] {
        let mut invalid = document.clone();
        invalid["authorization_endpoint"] = endpoint;
        assert!(parse(&invalid, true).is_err(), "invalid endpoint must not select workload metadata");
    }
    let mut complete = document;
    complete["authorization_endpoint"] = serde_json::json!("https://issuer.example.com/authorize");
    for hidden in [true, false] {
        let metadata = parse(&complete, hidden).expect("full providers keep the existing parser");
        assert!(metadata.console().is_ok());
        assert!(metadata.token_endpoint().is_none(), "token endpoint remains optional");
    }
}
#[test]
fn workload_metadata_rejects_duplicate_fields() {
    for hidden in [false, true] {
        let document = format!(
            r#"{{"issuer":"https://wrong.example.com","issuer":"https://issuer.example.com",{}"jwks_uri":"https://issuer.example.com/jwks","response_types_supported":["id_token"],"subject_types_supported":["public"],"id_token_signing_alg_values_supported":["ES256"]}}"#,
            if hidden {
                ""
            } else {
                r#""authorization_endpoint":"https://issuer.example.com/authorize","#
            },
        );
        let error = DiscoveredProviderMetadata::parse(document.as_bytes(), hidden)
            .err()
            .expect("duplicate issuer must fail");
        assert!(error.contains("duplicate field"), "{error}");
    }
}
#[test]
fn workload_verifier_preserves_algorithm_secret_and_audience_policy() {
    let secret = Nonce::new_random().secret().to_string();
    let mut config = build_mocked_oidc_provider_config("workload", "https://issuer.example.com");
    config.client_secret = Some(secret.clone());
    config.other_audiences = vec!["additional-audience".into()];
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let payload = serde_json::json!({
        "iss": config.config_url, "sub": "repo:example/project:ref:refs/heads/main",
        "aud": [config.client_id, "additional-audience"], "iat": now, "exp": now + 300,
    });
    let signed =
        jsonwebtoken::encode(&Header::new(Algorithm::HS256), &payload, &EncodingKey::from_secret(secret.as_bytes())).unwrap();
    let token: CoreIdToken = signed.parse().unwrap();
    for workload in [false, true] {
        for (algorithms, accepted) in [
            (serde_json::json!(["HS256", "unsupported-future-algorithm"]), true),
            (serde_json::json!(["ES256"]), false),
            (serde_json::json!(["unsupported-future-algorithm"]), false),
        ] {
            let mut document = serde_json::json!({
                "issuer": config.config_url, "jwks_uri": "https://issuer.example.com/jwks",
                "response_types_supported": ["id_token"], "subject_types_supported": ["public"],
                "id_token_signing_alg_values_supported": algorithms,
            });
            if !workload {
                document["authorization_endpoint"] = serde_json::json!("https://issuer.example.com/authorize");
            }
            let metadata = DiscoveredProviderMetadata::parse(&serde_json::to_vec(&document).unwrap(), true).unwrap();
            let verifier = metadata
                .verifier(&config)
                .set_other_audience_verifier_fn(|aud| trusted_aud(&config.other_audiences, aud));
            assert_eq!(
                token.claims(&verifier, |_: Option<&Nonce>| Ok(())).is_ok(),
                accepted,
                "workload={workload}, algorithms={algorithms}"
            );
            if accepted {
                assert!(
                    token.claims(&metadata.verifier(&config), |_: Option<&Nonce>| Ok(())).is_err(),
                    "additional audiences require explicit trust"
                );
                let mut wrong_secret = config.clone();
                wrong_secret.client_secret = Some(Nonce::new_random().secret().to_string());
                let verifier = metadata
                    .verifier(&wrong_secret)
                    .set_other_audience_verifier_fn(|aud| trusted_aud(&config.other_audiences, aud));
                assert!(
                    token.claims(&verifier, |_: Option<&Nonce>| Ok(())).is_err(),
                    "incorrect client secret must fail"
                );
            }
        }
    }
}
#[tokio::test]
async fn workload_discovery_stops_after_forbidden_jwks() {
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = Arc::clone(&requests);
    let (base, handle) = start_mock_oidc_discovery_server_with_jwks(
        |base| (base.to_string(), "http://192.168.65.254:8080/jwks".into(), "/jwks".into()),
        2,
        "ES256",
        true,
        move |_| {
            seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            serde_json::json!({"keys": []}).to_string()
        },
    )
    .expect("workload discovery mock must bind");
    let mut config = build_mocked_oidc_provider_config("workload", &base);
    config.hide_from_ui = true;
    let client = ReqwestHttpClient::with_policy(OutboundPolicy::from_allowed_origins(&base).unwrap());
    let error = discover_provider(&config, &client)
        .await
        .err()
        .expect("private JWKS must be blocked");
    assert!(error.starts_with(OIDC_JWKS_BLOCKED_BY_OUTBOUND_POLICY), "{error}");
    handle.join().unwrap();
    assert_eq!(
        requests.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "a policy denial must not retry discovery"
    );
}
#[tokio::test]
async fn workload_discovery_verification_and_rotation() {
    for explicit_issuer in [false, true] {
        let (_, initial_jwk) = oidc_es256_key_and_jwk("initial");
        let (key, rotated_jwk) = oidc_es256_key_and_jwk("rotated");
        let initial_jwks = serde_json::json!({"keys": [initial_jwk]}).to_string();
        let rotated_jwks = serde_json::json!({"keys": [rotated_jwk]}).to_string();
        let (base, handle) = start_mock_oidc_discovery_server_with_jwks(
            |base| (base.to_string(), format!("{base}/jwks"), "/jwks".into()),
            4,
            "ES256",
            true,
            move |fetch| {
                if fetch == 0 {
                    initial_jwks.clone()
                } else {
                    rotated_jwks.clone()
                }
            },
        )
        .expect("workload discovery mock must bind");
        let mut config = build_mocked_oidc_provider_config("workload", &base);
        config.hide_from_ui = true;
        if explicit_issuer {
            config.issuer = Some(base.clone());
        }
        let http_client = ReqwestHttpClient::with_policy(OutboundPolicy::from_allowed_origins(&base).unwrap());
        let state = discover_provider(&config, &http_client)
            .await
            .expect("workload discovery must succeed");
        assert!(state.metadata.authorization_endpoint().is_none());
        let sys = OidcSys {
            configs: HashMap::from([(config.id.clone(), test_sourced_config(config.clone()))]),
            provider_runtime: ProviderRuntime::new(http_client, HashMap::from([(config.id.clone(), state)])),
            state_store: OidcStateStore::new(),
        };
        assert!(sys.has_providers());
        assert!(sys.provider_configs().all(|provider| provider.hide_from_ui));
        let error = sys
            .authorize_url(&config.id, "https://console.example.com/callback", None)
            .await
            .unwrap_err();
        assert!(error.contains("only web identity"));
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let payload = serde_json::json!({
            "iss": base, "sub": "system:serviceaccount:default:reader", "aud": [config.client_id],
            "iat": now, "exp": now + 300, "groups": ["readonly"],
            "kubernetes.io": {"namespace": "default", "serviceaccount": {"name": "reader"}},
        });
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some("rotated".into());
        let token = jsonwebtoken::encode(&header, &payload, &key).unwrap();
        let (claims, provider) = sys
            .verify_web_identity_token(&token)
            .await
            .expect("rotation must retain workload discovery support");
        assert_eq!(provider, config.id);
        assert_eq!(claims.sub, "system:serviceaccount:default:reader");
        assert_eq!(claims.groups, ["readonly"]);
        // Repeat after the mock exits: the verified snapshot must be cached.
        handle.join().unwrap();
        assert!(sys.verify_web_identity_token(&token).await.is_ok());
        for (field, value, expected) in [
            ("iss", serde_json::json!("https://wrong.example.com"), "issuer"),
            ("aud", serde_json::json!("wrong-audience"), "audience"),
            ("exp", serde_json::json!(now - 60), "expired"),
        ] {
            let mut invalid = payload.clone();
            invalid[field] = value;
            let token = jsonwebtoken::encode(&header, &invalid, &key).unwrap();
            let error = sys
                .verify_web_identity_token(&token)
                .await
                .expect_err("invalid workload token must fail");
            assert!(error.to_lowercase().contains(expected), "{field}: {error}");
        }
        let (wrong_key, _) = oidc_es256_key_and_jwk("wrong");
        let invalid = jsonwebtoken::encode(&header, &payload, &wrong_key).unwrap();
        let error = sys
            .verify_web_identity_token(&invalid)
            .await
            .expect_err("wrong signature must fail");
        assert!(error.to_lowercase().contains("signature"), "{error}");
        assert!(
            sys.verify_web_identity_token(&token).await.is_ok(),
            "failed refresh must preserve the cached keys"
        );
    }
}
#[tokio::test]
async fn oidc_discovery_accepts_extra_root_ca_for_https_provider() {
    let Some((base, ca_pem, handle)) = start_mock_oidc_tls_discovery_server(
        |base| (format!("{base}/application/o/rustfs"), format!("{base}/jwks"), "/jwks".to_string()),
        4,
    ) else {
        return;
    };
    let config_url = format!("{base}/application/o/rustfs");
    let config = build_mocked_oidc_provider_config("default", &config_url);
    let origin = Url::parse(&config.config_url)
        .expect("mock config_url should parse")
        .origin()
        .ascii_serialization();
    let policy = OutboundPolicy::from_allowed_origins(&origin).expect("loopback TLS origin should be allowed");
    let extra_root_certs =
        parse_oidc_extra_root_certs("test OIDC TLS CA", ca_pem.as_bytes()).expect("test CA bundle should parse");
    let http_client = ReqwestHttpClient::with_policy_and_extra_root_certs(policy, extra_root_certs);

    let state = discover_provider(&config, &http_client)
        .await
        .expect("OIDC discovery should trust the extra root CA");

    assert_eq!(state.metadata.issuer().to_string(), format!("{base}/application/o/rustfs"));
    assert!(handle.join().is_ok());
}
#[tokio::test]
async fn oidc_discovery_refreshes_extra_root_ca_when_generation_changes() {
    let Some((base_a, ca_pem_a, handle_a)) = start_mock_oidc_tls_discovery_server(
        |base| (format!("{base}/application/o/rustfs-a"), format!("{base}/jwks"), "/jwks".to_string()),
        4,
    ) else {
        return;
    };
    let Some((base_b, ca_pem_b, handle_b)) = start_mock_oidc_tls_discovery_server(
        |base| (format!("{base}/application/o/rustfs-b"), format!("{base}/jwks"), "/jwks".to_string()),
        4,
    ) else {
        assert!(handle_a.join().is_ok());
        return;
    };

    let origin_a = Url::parse(&base_a)
        .expect("mock base A should parse")
        .origin()
        .ascii_serialization();
    let origin_b = Url::parse(&base_b)
        .expect("mock base B should parse")
        .origin()
        .ascii_serialization();
    let allowed_origins = format!("{origin_a},{origin_b}");
    let policy = OutboundPolicy::from_allowed_origins(&allowed_origins).expect("loopback TLS origins should be allowed");
    let material = Arc::new(Mutex::new(OidcExtraRootCaMaterial {
        generation: 1,
        root_ca_pem: Some(ca_pem_a.into_bytes()),
    }));
    let provider = OidcExtraRootCaProvider::new({
        let material = material.clone();
        move || {
            let material = material.clone();
            async move {
                material
                    .lock()
                    .map(|material| material.clone())
                    .map_err(|e| format!("test OIDC extra CA material lock poisoned: {e}"))
            }
        }
    });
    let http_client = ReqwestHttpClient::with_policy_and_extra_root_ca_provider(policy, provider);

    let config_a = build_mocked_oidc_provider_config("a", &format!("{base_a}/application/o/rustfs-a"));
    let state_a = discover_provider(&config_a, &http_client)
        .await
        .expect("OIDC discovery should trust initial extra root CA");
    assert_eq!(state_a.metadata.issuer().to_string(), format!("{base_a}/application/o/rustfs-a"));

    {
        let mut material = material
            .lock()
            .expect("test OIDC extra CA material lock should not be poisoned");
        material.generation = 2;
        material.root_ca_pem = Some(ca_pem_b.into_bytes());
    }
    let config_b = build_mocked_oidc_provider_config("b", &format!("{base_b}/application/o/rustfs-b"));
    let state_b = discover_provider(&config_b, &http_client)
        .await
        .expect("OIDC discovery should refresh extra root CA after generation change");

    assert_eq!(state_b.metadata.issuer().to_string(), format!("{base_b}/application/o/rustfs-b"));
    assert!(handle_a.join().is_ok());
    assert!(handle_b.join().is_ok());
}
#[test]
fn test_jwks_url_from_config_url_preserves_unrelated_urls() {
    let issuer = IssuerUrl::new("https://public.example.com/realms/app".to_string()).expect("issuer URL should parse");
    for raw_jwks_url in [
        "https://keys.example.com/jwks",
        "http://public.example.com/realms/app/jwks",
        "https://public.example.com:8443/realms/app/jwks",
        "https://public.example.com/keys/jwks",
        "https://public.example.com/realms/application/jwks",
    ] {
        let jwks_url = JsonWebKeySetUrl::new(raw_jwks_url.to_string()).expect("JWKS URL should parse");

        let resolved =
            jwks_url_from_config_url("http://keycloak.internal/realms/app/.well-known/openid-configuration", &issuer, &jwks_url)
                .expect("JWKS URL should resolve");

        assert_eq!(resolved.as_str(), raw_jwks_url);
    }

    let issuer_root_jwks = JsonWebKeySetUrl::new(issuer.as_str().to_string()).expect("JWKS URL should parse");
    let resolved = jwks_url_from_config_url(
        "http://keycloak.internal/realms/app/.well-known/openid-configuration",
        &issuer,
        &issuer_root_jwks,
    )
    .expect("issuer-root JWKS URL should resolve");
    assert_eq!(resolved.as_str(), "http://keycloak.internal/realms/app");

    let issuer_with_slash =
        IssuerUrl::new("https://public.example.com/realms/app/".to_string()).expect("issuer URL should parse");
    let jwks_url =
        JsonWebKeySetUrl::new("https://public.example.com/realms/app/jwks".to_string()).expect("JWKS URL should parse");
    let resolved = jwks_url_from_config_url(
        "http://keycloak.internal/realms/app/.well-known/openid-configuration",
        &issuer_with_slash,
        &jwks_url,
    )
    .expect("issuer-relative JWKS URL should resolve");
    assert_eq!(resolved.as_str(), "http://keycloak.internal/realms/app/jwks");
}
#[tokio::test]
async fn oidc_discovery_reports_forbidden_outbound_without_retrying() {
    let config_url = "http://192.168.65.254:8080/realms/rustfs/.well-known/openid-configuration";
    let config = build_mocked_oidc_provider_config("default", config_url);
    let http_client = ReqwestHttpClient::with_policy(OutboundPolicy::default());

    let error = match discover_provider(&config, &http_client).await {
        Ok(_) => panic!("private OIDC provider should require an explicit allowlist origin"),
        Err(error) => error,
    };

    assert!(error.contains("OIDC provider discovery blocked by outbound policy"));
    assert!(error.contains(&format!("add http://192.168.65.254:8080 to {ENV_OUTBOUND_ALLOW_ORIGINS}")));
    assert!(!error.contains("discovery failed for all issuer variants"));
}
#[tokio::test]
async fn oidc_explicit_issuer_reports_forbidden_discovery_endpoint() {
    let config_url = "http://192.168.65.254:8080/realms/rustfs/.well-known/openid-configuration";
    let mut config = build_mocked_oidc_provider_config("default", config_url);
    config.issuer = Some("https://idp.example.com/realms/rustfs".to_string());
    let http_client = ReqwestHttpClient::with_policy(OutboundPolicy::default());

    let error = match discover_provider(&config, &http_client).await {
        Ok(_) => panic!("private OIDC provider should require an explicit allowlist origin"),
        Err(error) => error,
    };

    assert!(error.contains("OIDC provider discovery blocked by outbound policy"));
    assert!(error.contains(&format!("add http://192.168.65.254:8080 to {ENV_OUTBOUND_ALLOW_ORIGINS}")));
}
#[tokio::test]
async fn oidc_explicit_issuer_reports_forbidden_jwks_endpoint() {
    let Some((base, handle)) = start_mock_oidc_discovery_server(
        |base| {
            (
                format!("{base}/realms/rustfs"),
                "http://192.168.65.254:8080/realms/rustfs/protocol/openid-connect/certs".to_string(),
                "/unused".to_string(),
            )
        },
        1,
    ) else {
        return;
    };
    let mut config =
        build_mocked_oidc_provider_config("default", &format!("{base}/realms/rustfs/.well-known/openid-configuration"));
    config.issuer = Some(format!("{base}/realms/rustfs"));
    let policy = OutboundPolicy::from_allowed_origins(&base).expect("loopback discovery origin should be allowed");
    let http_client = ReqwestHttpClient::with_policy(policy);

    let error = match discover_provider(&config, &http_client).await {
        Ok(_) => panic!("private JWKS endpoint should require an explicit allowlist origin"),
        Err(error) => error,
    };

    assert!(error.contains("JWKS request blocked by outbound policy"));
    assert!(error.contains(&format!("add http://192.168.65.254:8080 to {ENV_OUTBOUND_ALLOW_ORIGINS}")));
    assert!(handle.join().is_ok());
}
#[tokio::test]
async fn oidc_reqwest_dns_policy_rejection_stays_typed() {
    let calls = Arc::new(AtomicUsize::new(0));
    let config = build_mocked_oidc_provider_config(
        "default",
        "http://keycloak.internal:8080/realms/rustfs/.well-known/openid-configuration",
    );
    let http_client = ReqwestHttpClient::with_policy_and_dns_resolver(
        OutboundPolicy::default(),
        Arc::new(RejectingDnsResolver {
            allow_origin_can_recover: true,
            calls: Some(calls.clone()),
        }),
    );

    let error = match discover_provider(&config, &http_client).await {
        Ok(_) => panic!("private DNS answer should fail discovery"),
        Err(error) => error,
    };

    assert!(error.contains("OIDC provider discovery blocked by outbound policy"));
    assert!(error.contains(&format!("add http://keycloak.internal:8080 to {ENV_OUTBOUND_ALLOW_ORIGINS}")));
    assert_eq!(calls.load(Ordering::Relaxed), 1, "policy rejection must not be retried");
}
#[tokio::test]
async fn oidc_explicit_issuer_preserves_dns_policy_rejection() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = build_mocked_oidc_provider_config(
        "default",
        "http://keycloak.internal:8080/realms/rustfs/.well-known/openid-configuration",
    );
    config.issuer = Some("https://idp.example.com/realms/rustfs".to_string());
    let http_client = ReqwestHttpClient::with_policy_and_dns_resolver(
        OutboundPolicy::default(),
        Arc::new(RejectingDnsResolver {
            allow_origin_can_recover: true,
            calls: Some(calls.clone()),
        }),
    );

    let error = match discover_provider(&config, &http_client).await {
        Ok(_) => panic!("private DNS answer should fail discovery"),
        Err(error) => error,
    };

    assert!(error.contains("OIDC provider discovery blocked by outbound policy"));
    assert!(error.contains(&format!("add http://keycloak.internal:8080 to {ENV_OUTBOUND_ALLOW_ORIGINS}")));
    assert_eq!(calls.load(Ordering::Relaxed), 1, "policy rejection must not be retried");
}

#[tokio::test]
async fn workload_config_validation_reports_absent_console_endpoints() {
    let (base, handle) = start_mock_oidc_discovery_server_with_jwks(
        |base| (base.to_string(), format!("{base}/jwks"), "/jwks".into()),
        2,
        "ES256",
        true,
        |_| serde_json::json!({"keys": []}).to_string(),
    )
    .expect("workload discovery mock must bind");
    let mut config = build_mocked_oidc_provider_config("workload", &base);
    config.hide_from_ui = true;
    // Inferred issuers keep the library's discovery URL construction.
    config.config_url = format!("{base}/.well-known/openid-configuration?ignored=1#ignored");
    let result = validate_mocked_oidc_provider_config(&config)
        .await
        .expect("hidden workload configuration should validate");
    assert_eq!(result.issuer, base);
    assert!(result.authorization_endpoint.is_none());
    assert!(result.token_endpoint.is_none());
    handle.join().unwrap();
}

#[tokio::test]
async fn test_validate_oidc_provider_config_retries_with_issuer_candidates() {
    // Discovery document must advertise the canonical issuer path. The first candidate has no
    // trailing slash; openidconnect rejects issuer mismatch, then the second variant succeeds.
    let Some((base, handle)) = start_mock_oidc_discovery_server(
        |base| (format!("{base}/application/o/rustfs/"), format!("{base}/jwks"), "/jwks".to_string()),
        8,
    ) else {
        return;
    };
    let config_url = format!("{base}/application/o/rustfs");
    let config = build_mocked_oidc_provider_config("default", &config_url);

    let result = validate_mocked_oidc_provider_config(&config).await;

    let validation_result = result.expect("OIDC provider validation should succeed");
    assert_eq!(validation_result.issuer, format!("{base}/application/o/rustfs/"));
    assert!(handle.join().is_ok());
}

#[tokio::test]
async fn test_validate_oidc_provider_config_fetches_issuer_relative_jwks_from_config_url() {
    let Some((base, handle)) = start_mock_oidc_discovery_server(
        |_| {
            (
                "http://127.0.0.1:1/public/realms/app".to_string(),
                "http://127.0.0.1:1/public/realms/app/jwks?version=1".to_string(),
                "/internal/realms/app/jwks?version=1".to_string(),
            )
        },
        2,
    ) else {
        return;
    };
    let mut config =
        build_mocked_oidc_provider_config("default", &format!("{base}/internal/realms/app/.well-known/openid-configuration"));
    config.issuer = Some("http://127.0.0.1:1/public/realms/app".to_string());

    let validation_result = validate_mocked_oidc_provider_config(&config)
        .await
        .expect("OIDC provider validation should succeed");

    assert_eq!(validation_result.issuer, "http://127.0.0.1:1/public/realms/app");
    assert!(handle.join().is_ok());
}

#[tokio::test]
async fn test_validate_oidc_provider_config_rejects_separate_issuer_mismatch() {
    let Some((base, handle)) = start_mock_oidc_discovery_server(
        |base| {
            (
                "https://public.example.com/realms/other".to_string(),
                format!("{base}/jwks"),
                "/jwks".to_string(),
            )
        },
        1,
    ) else {
        return;
    };
    let mut config =
        build_mocked_oidc_provider_config("default", &format!("{base}/internal/realms/app/.well-known/openid-configuration"));
    config.issuer = Some("https://public.example.com/realms/app".to_string());

    let err = validate_mocked_oidc_provider_config(&config)
        .await
        .expect_err("OIDC provider validation should fail");

    assert!(err.contains("unexpected issuer URI"));
    assert!(err.contains("https://public.example.com/realms/app"));
    assert!(handle.join().is_ok());
}

#[tokio::test]
async fn test_validate_oidc_provider_config_returns_detailed_errors() {
    let Some((base, handle)) = start_mock_oidc_discovery_server(
        |base| (format!("{base}/application/o/other"), format!("{base}/jwks"), "/jwks".to_string()),
        8,
    ) else {
        return;
    };
    let config_url = format!("{base}/application/o/rustfs");
    let config = build_mocked_oidc_provider_config("default", &config_url);

    let err = validate_mocked_oidc_provider_config(&config)
        .await
        .expect_err("OIDC provider validation should fail");
    assert!(discovery_error_contains_all_variants(&err, &base));
    assert!(err.contains("issuer '"));
    assert!(err.contains(&format!("issuer '{base}/application/o/rustfs'")));
    assert!(err.contains(&format!("issuer '{base}/application/o/rustfs/'")));
    assert!(handle.join().is_ok());
}
