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
use crate::oidc::test_support::*;
use rustfs_config::server_config::{Config as ServerConfig, KVS};
use rustfs_config::{DEFAULT_DELIMITER, ENABLE_KEY};

#[test]
fn test_parse_single_provider_no_config_url() {
    let config = parse_single_provider("_TEST_EMPTY", "test_empty");
    assert!(config.is_none());
}
#[test]
fn test_parse_single_provider_reads_issuer() {
    temp_env::with_vars(
        [
            (
                ENV_IDENTITY_OPENID_CONFIG_URL,
                Some("http://keycloak.ns.svc.cluster.local:8080/realms/app/.well-known/openid-configuration"),
            ),
            (ENV_IDENTITY_OPENID_ISSUER, Some("https://app.local/realms/app")),
            (ENV_IDENTITY_OPENID_CLIENT_ID, Some("console")),
        ],
        || {
            let config = parse_single_provider("", "default").expect("provider config should parse");

            assert_eq!(config.issuer.as_deref(), Some("https://app.local/realms/app"));
        },
    );
}
#[test]
fn test_parse_persisted_provider_config() {
    let mut cfg = ServerConfig::new();
    let mut kvs = KVS(vec![
        rustfs_config::server_config::KV {
            key: ENABLE_KEY.to_string(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        rustfs_config::server_config::KV {
            key: OIDC_CONFIG_URL.to_string(),
            value: String::new(),
            hidden_if_empty: false,
        },
        rustfs_config::server_config::KV {
            key: OIDC_CLIENT_ID.to_string(),
            value: String::new(),
            hidden_if_empty: false,
        },
    ]);
    kvs.insert(
        OIDC_CONFIG_URL.to_string(),
        "https://example.com/.well-known/openid-configuration".to_string(),
    );
    kvs.insert(OIDC_CLIENT_ID.to_string(), "console".to_string());
    kvs.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());
    kvs.insert(OIDC_ISSUER.to_string(), "https://issuer.example".to_string());
    kvs.insert(OIDC_ROLES_CLAIM.to_string(), "app_roles".to_string());

    cfg.0
        .entry(IDENTITY_OPENID_SUB_SYS.to_string())
        .or_default()
        .insert(DEFAULT_DELIMITER.to_string(), kvs);

    let parsed = parse_persisted_configs(&cfg);
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].id, "default");
    assert_eq!(parsed[0].client_id, "console");
    assert_eq!(parsed[0].issuer.as_deref(), Some("https://issuer.example"));
    assert!(parsed[0].enabled);
    assert_eq!(parsed[0].roles_claim, "app_roles");
}
#[test]
fn test_parse_persisted_provider_config_omitted_roles_claim_is_empty() {
    let mut cfg = ServerConfig::new();
    let mut kvs = KVS(vec![
        rustfs_config::server_config::KV {
            key: ENABLE_KEY.to_string(),
            value: EnableState::Off.to_string(),
            hidden_if_empty: false,
        },
        rustfs_config::server_config::KV {
            key: OIDC_CONFIG_URL.to_string(),
            value: String::new(),
            hidden_if_empty: false,
        },
        rustfs_config::server_config::KV {
            key: OIDC_CLIENT_ID.to_string(),
            value: String::new(),
            hidden_if_empty: false,
        },
    ]);
    kvs.insert(
        OIDC_CONFIG_URL.to_string(),
        "https://example.com/.well-known/openid-configuration".to_string(),
    );
    kvs.insert(OIDC_CLIENT_ID.to_string(), "console".to_string());
    kvs.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());

    cfg.0
        .entry(IDENTITY_OPENID_SUB_SYS.to_string())
        .or_default()
        .insert(DEFAULT_DELIMITER.to_string(), kvs);

    let parsed = parse_persisted_configs(&cfg);
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].roles_claim, "");
}
#[test]
fn test_merge_oidc_provider_configs_prefers_env() {
    let mut persisted = test_config("default");
    persisted.display_name = "Persisted".to_string();

    let mut env = test_config("default");
    env.display_name = "Environment".to_string();

    let merged = merge_oidc_provider_configs(vec![env], vec![persisted]);
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].config.display_name, "Environment");
    assert_eq!(merged[0].source, OidcProviderConfigSource::Env);
}
#[test]
fn test_oidc_provider_config_debug_redacts_client_secret() {
    let config = OidcProviderConfig {
        client_secret: Some("oidc-client-secret".to_string()),
        ..test_config("default")
    };
    let sourced = SourcedOidcProviderConfig {
        config,
        source: OidcProviderConfigSource::Persisted,
    };

    let rendered = format!("{sourced:?}");

    assert!(!rendered.contains("oidc-client-secret"));
    assert!(rendered.contains(REDACTED_SECRET));
    assert!(rendered.contains("client-id"));
}
#[test]
fn test_parse_enable_state_on() {
    assert!(parse_enable_state("on", false, false));
}
#[test]
fn test_parse_enable_state_off() {
    assert!(!parse_enable_state("off", true, true));
}
#[test]
fn test_parse_enable_state_empty_returns_default() {
    assert!(parse_enable_state("", true, false));
    assert!(!parse_enable_state("", false, true));
}
#[test]
fn test_parse_enable_state_invalid_returns_error_default() {
    assert!(!parse_enable_state("garbage", true, false));
    assert!(parse_enable_state("garbage", false, true));
}
#[test]
fn test_hide_from_ui_default_is_false() {
    let config = test_config("default");
    assert!(!config.hide_from_ui);
}
#[test]
fn test_parse_persisted_hide_from_ui_off_is_false() {
    let mut cfg = ServerConfig::new();
    let mut kvs = KVS(vec![
        rustfs_config::server_config::KV {
            key: ENABLE_KEY.to_string(),
            value: EnableState::On.to_string(),
            hidden_if_empty: false,
        },
        rustfs_config::server_config::KV {
            key: OIDC_CONFIG_URL.to_string(),
            value: "https://example.com/.well-known/openid-configuration".to_string(),
            hidden_if_empty: false,
        },
        rustfs_config::server_config::KV {
            key: OIDC_CLIENT_ID.to_string(),
            value: "console".to_string(),
            hidden_if_empty: false,
        },
    ]);
    kvs.insert(OIDC_HIDE_FROM_UI.to_string(), EnableState::Off.to_string());

    cfg.0
        .entry(IDENTITY_OPENID_SUB_SYS.to_string())
        .or_default()
        .insert(DEFAULT_DELIMITER.to_string(), kvs);

    let parsed = parse_persisted_configs(&cfg);
    assert_eq!(parsed.len(), 1);
    assert!(!parsed[0].hide_from_ui);
}
#[test]
fn test_parse_persisted_hide_from_ui_missing_defaults_false() {
    let mut cfg = ServerConfig::new();
    let kvs = KVS(vec![
        rustfs_config::server_config::KV {
            key: ENABLE_KEY.to_string(),
            value: EnableState::On.to_string(),
            hidden_if_empty: false,
        },
        rustfs_config::server_config::KV {
            key: OIDC_CONFIG_URL.to_string(),
            value: "https://example.com/.well-known/openid-configuration".to_string(),
            hidden_if_empty: false,
        },
        rustfs_config::server_config::KV {
            key: OIDC_CLIENT_ID.to_string(),
            value: "console".to_string(),
            hidden_if_empty: false,
        },
    ]);

    cfg.0
        .entry(IDENTITY_OPENID_SUB_SYS.to_string())
        .or_default()
        .insert(DEFAULT_DELIMITER.to_string(), kvs);

    let parsed = parse_persisted_configs(&cfg);
    assert_eq!(parsed.len(), 1);
    assert!(!parsed[0].hide_from_ui);
}
#[test]
fn test_parse_persisted_hide_from_ui() {
    let mut cfg = ServerConfig::new();
    let mut kvs = KVS(vec![
        rustfs_config::server_config::KV {
            key: ENABLE_KEY.to_string(),
            value: EnableState::On.to_string(),
            hidden_if_empty: false,
        },
        rustfs_config::server_config::KV {
            key: OIDC_CONFIG_URL.to_string(),
            value: "https://example.com/.well-known/openid-configuration".to_string(),
            hidden_if_empty: false,
        },
        rustfs_config::server_config::KV {
            key: OIDC_CLIENT_ID.to_string(),
            value: "console".to_string(),
            hidden_if_empty: false,
        },
    ]);
    kvs.insert(OIDC_HIDE_FROM_UI.to_string(), EnableState::On.to_string());

    cfg.0
        .entry(IDENTITY_OPENID_SUB_SYS.to_string())
        .or_default()
        .insert(DEFAULT_DELIMITER.to_string(), kvs);

    let parsed = parse_persisted_configs(&cfg);
    assert_eq!(parsed.len(), 1);
    assert!(parsed[0].hide_from_ui);
}
#[test]
fn test_active_config_snapshot_puts_default_first() {
    let sys = make_test_sys(vec![test_config("zeta"), test_config("default"), test_config("alpha")]);

    assert_eq!(
        sys.config_snapshot()
            .providers()
            .iter()
            .map(|provider| provider.config.id.as_str())
            .collect::<Vec<_>>(),
        ["default", "alpha", "zeta"]
    );
}
#[test]
fn test_oidc_provider_config_defaults() {
    let config = OidcProviderConfig {
        id: "test".to_string(),
        enabled: true,
        config_url: "https://example.com/.well-known/openid-configuration".to_string(),
        issuer: None,
        client_id: "my-client".to_string(),
        client_secret: Some("secret".to_string()),
        scopes: vec!["openid".to_string(), "profile".to_string(), "email".to_string()],
        other_audiences: vec![],
        redirect_uri: None,
        redirect_uri_dynamic: true,
        claim_name: "groups".to_string(),
        claim_prefix: "".to_string(),
        role_policy: "readwrite".to_string(),
        display_name: "Test Provider".to_string(),
        groups_claim: "groups".to_string(),
        roles_claim: String::new(),
        email_claim: "email".to_string(),
        username_claim: "preferred_username".to_string(),
        hide_from_ui: false,
    };

    assert_eq!(config.id, "test");
    assert!(config.enabled);
    assert_eq!(config.scopes.len(), 3);
    assert!(config.redirect_uri_dynamic);
}

#[test]
fn config_input_preserves_secret_and_defaults() {
    let input = OidcProviderConfigInput {
        config_url: " https://example.com/.well-known/openid-configuration ".to_string(),
        client_id: " client ".to_string(),
        client_secret: Some(String::new()),
        scopes: vec![" openid ".to_string(), " profile ".to_string()],
        roles_claim: "app_roles".to_string(),
        ..Default::default()
    };
    let config = build_upsert_provider_config("default", input, Some("existing-secret".to_string()))
        .expect("valid config should preserve existing secret");

    assert_eq!(config.client_secret.as_deref(), Some("existing-secret"));
    assert_eq!(config.config_url, "https://example.com/.well-known/openid-configuration");
    assert_eq!(config.client_id, "client");
    assert_eq!(config.scopes, ["openid", "profile"]);
    assert_eq!(config.roles_claim, "app_roles");
    assert_eq!(config.claim_name, OIDC_DEFAULT_CLAIM_NAME);

    let raw_secret = build_upsert_provider_config(
        "default",
        OidcProviderConfigInput {
            config_url: config.config_url,
            client_id: config.client_id,
            client_secret: Some(" raw secret ".to_string()),
            ..Default::default()
        },
        None,
    )
    .expect("a nonempty client secret should be accepted");
    assert_eq!(raw_secret.client_secret.as_deref(), Some(" raw secret "));
}

#[test]
fn config_input_rejects_invalid_fields() {
    let input = OidcProviderConfigInput {
        config_url: "https://example.com/.well-known/openid-configuration".to_string(),
        client_id: "client".to_string(),
        scopes: vec!["profile".to_string()],
        ..Default::default()
    };
    assert!(matches!(
        build_upsert_provider_config("default", input, None),
        Err(OidcConfigError::OpenidScopeRequired)
    ));

    let input = OidcProviderConfigInput {
        config_url: "https://127.0.0.1/.well-known/openid-configuration".to_string(),
        client_id: "client".to_string(),
        ..Default::default()
    };
    assert!(matches!(
        build_upsert_provider_config("default", input, None),
        Err(OidcConfigError::ForbiddenOutbound { field: "config_url", .. })
    ));

    assert!(serde_json::from_str::<OidcProviderConfigInput>(r#"{"unexpected_field":true}"#).is_err());
    assert!(serde_json::from_str::<OidcProviderValidationInput>(r#"{"unexpected_field":true}"#).is_err());
    assert!(matches!(
        validate_mutable_provider_id("../escape"),
        Err(OidcConfigError::InvalidProviderId)
    ));
}

#[test]
fn validation_input_defaults_to_default_provider() {
    let input = OidcProviderValidationInput {
        provider_id: "  ".to_string(),
        config_url: "https://example.com/.well-known/openid-configuration".to_string(),
        client_id: "client".to_string(),
        ..Default::default()
    };
    let config = build_validation_provider_config(input).expect("validation input should use default provider");
    assert_eq!(config.id, "default");
}

#[test]
fn persisted_config_codec_keeps_instance_keys_and_fields() {
    let mut config = ServerConfig::new();
    let mut provider = build_upsert_provider_config(
        "default",
        OidcProviderConfigInput {
            config_url: "https://example.com/.well-known/openid-configuration".to_string(),
            issuer: Some("https://issuer.example.com".to_string()),
            client_id: "client".to_string(),
            client_secret: Some("secret".to_string()),
            hide_from_ui: true,
            ..Default::default()
        },
        None,
    )
    .expect("provider should be valid");
    upsert_persisted_provider_config(&mut config, &provider);

    let default = config
        .0
        .get(IDENTITY_OPENID_SUB_SYS)
        .and_then(|subsystem| subsystem.get(DEFAULT_DELIMITER))
        .expect("default provider KVS should exist");
    assert_eq!(default.get(OIDC_ISSUER), "https://issuer.example.com");
    assert_eq!(default.get(OIDC_HIDE_FROM_UI), EnableState::On.to_string());
    assert_eq!(persisted_provider_secret(&config, "default").as_deref(), Some("secret"));

    provider.hide_from_ui = false;
    upsert_persisted_provider_config(&mut config, &provider);
    let default = config
        .0
        .get(IDENTITY_OPENID_SUB_SYS)
        .and_then(|subsystem| subsystem.get(DEFAULT_DELIMITER))
        .expect("updated default provider KVS should exist");
    assert_eq!(default.get(OIDC_HIDE_FROM_UI), EnableState::Off.to_string());

    delete_persisted_provider_config(&mut config, "default").expect("default provider should be deleted");
    assert!(matches!(
        delete_persisted_provider_config(&mut config, "default"),
        Err(OidcConfigError::ProviderNotFound)
    ));
}

#[test]
fn site_replication_snapshot_hashes_secret_without_exposing_config() {
    let provider = build_upsert_provider_config(
        "default",
        OidcProviderConfigInput {
            config_url: "https://example.com/.well-known/openid-configuration".to_string(),
            client_id: "client".to_string(),
            client_secret: Some("secret".to_string()),
            ..Default::default()
        },
        None,
    )
    .expect("provider should be valid");
    let snapshot = OidcSiteReplicationSnapshot::from_config_snapshot(&OidcConfigSnapshot::new(vec![SourcedOidcProviderConfig {
        config: provider,
        source: OidcProviderConfigSource::Persisted,
    }]));
    assert_eq!(
        snapshot.providers()[0].hashed_client_secret,
        "K7gNU3sdo-OL0wNhqoVWhr3g6s1xYv72ol_pe_Unols"
    );
    assert!(!format!("{snapshot:?}").contains("\"secret\""));
    assert_eq!(hash_client_secret(None), "");
    assert_eq!(hash_client_secret(Some("")), "");
}
