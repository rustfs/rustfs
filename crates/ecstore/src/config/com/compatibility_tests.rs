// Copyright 2026 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

const MINIO_CONFIG: &[u8] = include_bytes!("../../../tests/fixtures/server-config/minio-2025-09-07.json");

fn roundtrip(cfg: &Config, seed: &[u8]) -> (Config, Value) {
    let encoded = encode_server_config_blob(cfg, Some(seed)).expect("encode config with seed");
    let decoded = decode_server_config_blob(&encoded).expect("reload saved config");
    let repeated = encode_server_config_blob(&decoded, Some(&encoded)).expect("encode reloaded config");
    let encoded_value: Value = serde_json::from_slice(&encoded).unwrap();
    let repeated_value: Value = serde_json::from_slice(&repeated).unwrap();
    assert_eq!(encoded_value, repeated_value, "a second save must preserve JSON values");
    (decoded, encoded_value)
}

#[test]
fn pinned_minio_config_preserves_unsupported_heal_settings_without_applying_them() {
    assert!(
        Config::unmarshal(MINIO_CONFIG).is_err(),
        "the complete fixture exercises the external fallback"
    );
    let cfg = decode_server_config_blob(MINIO_CONFIG).expect("decode real MinIO config");
    let heal = cfg.get_value(HEAL_SUB_SYS, DEFAULT_DELIMITER).unwrap_or_default();
    for key in ["bitrotscan", "drive_workers", "max_io", "max_sleep"] {
        assert!(heal.lookup(key).is_none(), "MinIO heal controls must not be mapped by name");
    }
    assert_eq!(
        cfg.get_value(STORAGE_CLASS_SUB_SYS, DEFAULT_DELIMITER)
            .unwrap()
            .get("optimize"),
        "availability"
    );
    let (_, saved) = roundtrip(&cfg, MINIO_CONFIG);
    let original: Value = serde_json::from_slice(MINIO_CONFIG).unwrap();
    assert_eq!(saved["heal"], original["heal"], "unsupported KVS entries survive unchanged");
    assert_eq!(saved["cache"], original["cache"]);
}

#[test]
fn scalar_default_alias_reset_cannot_expose_shadow_overrides() {
    for (subsystem, key, old, shadow, defaults) in [
        (SCANNER_SUB_SYS, rustfs_config::SCANNER_CYCLE, "61", "59", &*scanner::DEFAULT_KVS),
        (HEAL_SUB_SYS, rustfs_config::HEAL_BITROT_CYCLE, "off", "48h", &*heal::DEFAULT_KVS),
    ] {
        let seed = serde_json::to_vec(&serde_json::json!({"version":"33", subsystem: {
            "default": {key: old, "future":"preferred"},
            "_": [{"key":key,"value":shadow,"future_attribute":"keep"}, {"key":"future","value":"shadow"}]
        }}))
        .unwrap();
        let mut cfg = decode_server_config_blob(&seed).unwrap();
        cfg.0.remove(subsystem);
        let (decoded, saved) = roundtrip(&cfg, &seed);
        assert_eq!(
            decoded
                .get_value(subsystem, DEFAULT_DELIMITER)
                .unwrap_or_default()
                .lookup(key)
                .unwrap_or_else(|| defaults.get(key)),
            defaults.get(key)
        );
        assert_eq!(saved[subsystem]["default"]["future"], "preferred");
        assert_eq!(saved[subsystem]["_"][0]["value"], defaults.get(key));
        assert_eq!(saved[subsystem]["_"][0]["future_attribute"], "keep");
        assert_eq!(saved[subsystem]["_"][1]["value"], "shadow");
    }
}

#[test]
fn root_aliases_keep_unknown_data_and_sync_known_values() {
    let seed = br#"{"version":"33",
      "storageclass":{"standard":"EC:2","future":"canonical"},
      "storage_class":{"_":[{"key":"standard","value":"EC:4","extra":"keep"},{"key":"future","value":"native"}]},
      "openid":{"default":{"config_url":"https://id.example/new","client_id":"new","future":"canonical"}},
      "identity_openid":{"_":{"config_url":"https://id.example/old","client_id":"old","future":"alias"}},
      "audit":{"webhook":{"default":{"enable":true,"endpoint":"https://audit.example/new","future":"audit"}}},
      "logger":{"audit_webhook":{"_":{"enable":true,"endpoint":"https://audit.example/old","future":"logger"}}}
    }"#;
    let cfg = decode_server_config_blob(seed).unwrap();
    let (decoded, saved) = roundtrip(&cfg, seed);
    assert!(configs_semantically_equal(&cfg, &decoded));
    assert_eq!(saved["storageclass"]["future"], "canonical");
    assert_eq!(saved["storage_class"]["_"][0]["value"], "EC:2");
    assert_eq!(saved["storage_class"]["_"][0]["extra"], "keep");
    assert_eq!(saved["storage_class"]["_"][1]["value"], "native");
    assert_eq!(saved["openid"]["default"]["future"], "canonical");
    assert_eq!(saved["identity_openid"]["_"]["future"], "alias");
    assert_eq!(saved["identity_openid"]["_"]["client_id"], "new");
    assert_eq!(saved["audit"]["webhook"]["default"]["future"], "audit");
    assert_eq!(saved["logger"]["audit_webhook"]["_"]["future"], "logger");
    assert_eq!(saved["logger"]["audit_webhook"]["_"]["endpoint"], "https://audit.example/new");
}

#[test]
fn storageclass_nested_alias_reset_clears_inline_override_and_retains_attributes() {
    let seed = br#"{"version":"33","storageclass":{"standard":"EC:2","_": [
      {"key":"inline_block","value":"64KiB","extra":{"owner":"future"}},
      {"key":"future","value":"keep"}]},
      "storage_class":{"default":{"standard":"EC:4","future":"alias"}}}"#;
    let (decoded, saved) = roundtrip(&Config::new(), seed);
    assert_eq!(
        decoded
            .get_value(STORAGE_CLASS_SUB_SYS, DEFAULT_DELIMITER)
            .unwrap()
            .get("inline_block"),
        storageclass::DEFAULT_KVS.get("inline_block")
    );
    assert_eq!(saved["storageclass"]["_"][0]["extra"]["owner"], "future");
    assert_eq!(saved["storageclass"]["_"][1]["value"], "keep");
    assert_eq!(saved["storage_class"]["default"]["future"], "alias");
}

#[test]
fn duplicate_known_kvs_entries_are_updated_consistently() {
    let seed = br#"{"version":"33","scanner":[
      {"key":"cycle","value":"61","extra":"first"},
      {"key":"cycle","value":"62","extra":"last"}]}"#;
    let mut cfg = decode_server_config_blob(seed).unwrap();
    cfg.0
        .get_mut(SCANNER_SUB_SYS)
        .unwrap()
        .get_mut(DEFAULT_DELIMITER)
        .unwrap()
        .insert("cycle".to_string(), "73".to_string());
    let (decoded, saved) = roundtrip(&cfg, seed);
    assert_eq!(decoded.get_value(SCANNER_SUB_SYS, DEFAULT_DELIMITER).unwrap().get("cycle"), "73");
    for entry in saved["scanner"].as_array().unwrap() {
        assert_eq!(entry["value"], "73");
    }
}

#[test]
fn removed_target_with_kvs_attributes_stays_disabled_under_enabled_default() {
    let seed = br#"{"version":"33","notify":{"webhook":{
      "default":{"enable":true,"endpoint":"https://notify.example/default"},
      "removed":[{"key":"endpoint","value":"https://notify.example/old","extra":"keep"}]
    }}}"#;
    let mut cfg = decode_server_config_blob(seed).unwrap();
    cfg.0
        .get_mut(rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS)
        .unwrap()
        .remove("removed");
    let (decoded, saved) = roundtrip(&cfg, seed);
    let removed = decoded
        .get_value(rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS, "removed")
        .unwrap();
    assert_eq!(removed.get(ENABLE_KEY), "off");
    assert_eq!(saved["notify"]["webhook"]["removed"][0]["extra"], "keep");
    assert_eq!(saved["notify"]["webhook"]["removed"][0]["value"], "");
}

#[test]
fn empty_preferred_aliases_keep_presence_precedence_but_accept_new_settings() {
    let seed = br#"{"version":"33","openid":null,"identity_openid":{"default":{"config_url":"https://old.example"}},
      "audit":null,"logger":{"webhook":{"default":{"enable":true,"endpoint":"https://old.example"}}},
      "scanner":{"default":null,"_":{"cycle":"59","future":"keep"}}}"#;
    let cfg = decode_server_config_blob(seed).unwrap();
    assert!(build_oidc_object(&cfg).is_empty());
    assert!(build_audit_object(&cfg).is_empty());
    assert!(build_scalar_config_object(&cfg, scanner_config_descriptor()).is_empty());
    let (_, reset) = roundtrip(&cfg, seed);
    assert!(reset["openid"].is_null(), "the preferred null alias must keep shadow config masked");
    assert!(reset["identity_openid"]["default"].get("config_url").is_none());
    let reset_cfg = decode_server_config_blob(&serde_json::to_vec(&reset).unwrap()).unwrap();
    assert!(
        build_oidc_object(&reset_cfg).is_empty(),
        "the old shadow provider must stay unapplied after save"
    );
    let updated = decode_server_config_blob(br#"{"version":"33","openid":{"default":{"config_url":"https://new.example"}},"audit":{"webhook":{"default":{"enable":true,"endpoint":"https://new.example"}}},"scanner":{"cycle":"73"}}"#).unwrap();
    let (decoded, _) = roundtrip(&updated, seed);
    assert!(configs_semantically_equal(&updated, &decoded));
}

#[test]
fn storageclass_null_precedence_is_materialized_before_alias_resave() {
    let seed = br#"{"version":"33","storageclass":null,"storage_class":{"standard":"EC:4","rrs":"EC:2","optimize":"capacity"}}"#;
    let cfg = decode_server_config_blob(seed).unwrap();
    let (_, saved) = roundtrip(&cfg, seed);
    assert_eq!(saved["storageclass"]["standard"], "");
    assert_eq!(saved["storageclass"]["rrs"], "");
    assert_eq!(saved["storage_class"]["standard"], "");
    assert_eq!(saved["storage_class"]["rrs"], "");
    assert_eq!(saved["storage_class"]["optimize"], "availability");
}

#[derive(Clone, Default)]
struct LogCapture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogCapture {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}

fn captured_logs(run: impl FnOnce(&tracing::Dispatch)) -> String {
    let capture = LogCapture::default();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(capture.clone())
            .finish(),
    );
    tracing::dispatcher::with_default(&dispatch, || run(&dispatch));
    String::from_utf8(capture.0.lock().unwrap().clone()).unwrap()
}

#[test]
fn compatibility_reports_are_bounded_summaries_and_never_log_untrusted_payloads() {
    let make_blob = |prefix: &str, keys: usize| {
        let mut settings = Map::new();
        for i in 0..keys {
            settings.insert(format!("SECRET_KEY_{prefix}_{i}"), Value::String("SECRET_VALUE".to_string()));
        }
        serde_json::to_vec(&serde_json::json!({"version":"33", "heal":settings, "scanner":"SECRET_LEGACY_PAYLOAD"})).unwrap()
    };
    let a = make_blob("a", 1000);
    let b = make_blob("b", 65);
    let logs = captured_logs(|dispatch| {
        for _ in 0..100 {
            decode_server_config_blob(&a).unwrap();
        }
        decode_server_config_blob(&b).unwrap();
        decode_server_config_blob(&a).unwrap();
        let barrier = std::sync::Barrier::new(12);
        let concurrent = make_blob("concurrent", 80);
        std::thread::scope(|scope| {
            for _ in 0..12 {
                let (barrier, concurrent) = (&barrier, &concurrent);
                scope.spawn(move || {
                    tracing::dispatcher::with_default(dispatch, || {
                        barrier.wait();
                        decode_server_config_blob(concurrent).unwrap();
                    })
                });
            }
        });
    });
    assert_eq!(
        logs.matches(EVENT_SERVER_CONFIG_COMPATIBILITY_IGNORED).count(),
        3,
        "one summary per diagnostic set, including concurrent readers: {logs}"
    );
    assert!(logs.contains("unknown_key_count=1000"));
    assert!(!logs.contains("SECRET"));
    assert!(logs.len() < 2000, "log volume must not grow with unknown-key count");
}

#[test]
fn malformed_known_values_still_fail_without_a_successful_compatibility_warning() {
    let logs = captured_logs(|_| {
        for seed in [
            br#"{"version":"33","heal":{"future":"secret","bitrot_cycle":null}}"#.as_slice(),
            br#"{"version":"33","scanner":[{"key":"cycle","value":[]}]}"#.as_slice(),
            br#"{"version":"33","scanner":[false]}"#.as_slice(),
            br#"{"scanner":null,"heal":"SECRET"}"#.as_slice(),
        ] {
            assert!(decode_server_config_blob(seed).is_err());
        }
    });
    assert!(!logs.contains(EVENT_SERVER_CONFIG_COMPATIBILITY_IGNORED));
    assert!(!logs.contains("secret"));
}

#[test]
#[serial_test::serial]
fn final_decode_failures_are_deduplicated_without_changing_error_classification() {
    let previous = replace_server_config_decrypt_fn_for_test(None);
    let logs = captured_logs(|_| {
        for _ in 0..100 {
            let err = decode_persisted_server_config(br#"{SECRET_CORRUPT_A"#).unwrap_err();
            assert!(is_server_config_corrupt_error(&err));
        }
        assert!(decode_persisted_server_config(br#"{SECRET_CORRUPT_B"#).is_err());
        assert!(decode_persisted_server_config(br#"{SECRET_CORRUPT_A"#).is_err());
        replace_server_config_decrypt_fn_for_test(Some(std::sync::Arc::new(|_| None)));
        for _ in 0..100 {
            let err = decode_persisted_server_config(b"SECRET_CIPHERTEXT").unwrap_err();
            assert!(is_server_config_decrypt_error(&err));
        }
    });
    replace_server_config_decrypt_fn_for_test(previous);
    assert_eq!(logs.matches(EVENT_SERVER_CONFIG_DECODE_FAILED).count(), 2);
    assert_eq!(logs.matches(EVENT_SERVER_CONFIG_DECRYPT_FAILED).count(), 1);
    assert!(!logs.contains("SECRET"));
}

#[test]
#[serial_test::serial]
fn pinned_minio_plaintext_exercises_the_legacy_decrypt_entry_without_false_errors() {
    // The fixture is plaintext from MinIO; this envelope is produced by RustFS.
    let encrypted = rustfs_crypto::encrypt_data(b"fixture-test-key", MINIO_CONFIG).unwrap();
    let previous = replace_server_config_decrypt_fn_for_test(Some(std::sync::Arc::new(|data| {
        rustfs_crypto::decrypt_data(b"fixture-test-key", data).ok()
    })));
    let logs = captured_logs(|_| {
        let (cfg, seed) = decode_persisted_server_config_with_seed(&encrypted).unwrap();
        assert_eq!(seed, MINIO_CONFIG);
        roundtrip(&cfg, &seed);
    });
    replace_server_config_decrypt_fn_for_test(previous);
    assert!(!logs.contains(EVENT_SERVER_CONFIG_DECODE_FAILED));
    assert!(!logs.contains(EVENT_SERVER_CONFIG_DECRYPT_FAILED));
}
