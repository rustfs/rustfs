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

use crate::config::{Config, GLOBAL_STORAGE_CLASS, KVS, audit, notify, oidc, storageclass};
use crate::disk::{MIGRATING_META_BUCKET, RUSTFS_META_BUCKET};
use crate::error::{Error, Result};
use crate::global::is_first_cluster_node_local;
use crate::store_api::{ObjectInfo, ObjectOptions, PutObjReader, StorageAPI};
use http::HeaderMap;
use rustfs_config::audit::{
    AUDIT_KAFKA_KEYS, AUDIT_KAFKA_SUB_SYS, AUDIT_MQTT_KEYS, AUDIT_MQTT_SUB_SYS, AUDIT_MYSQL_KEYS, AUDIT_MYSQL_SUB_SYS,
    AUDIT_NATS_KEYS, AUDIT_NATS_SUB_SYS, AUDIT_POSTGRES_KEYS, AUDIT_POSTGRES_SUB_SYS, AUDIT_PULSAR_KEYS, AUDIT_PULSAR_SUB_SYS,
    AUDIT_REDIS_KEYS, AUDIT_REDIS_SUB_SYS, AUDIT_WEBHOOK_KEYS, AUDIT_WEBHOOK_SUB_SYS,
};
use rustfs_config::notify::{
    NOTIFY_AMQP_KEYS, NOTIFY_AMQP_SUB_SYS, NOTIFY_KAFKA_KEYS, NOTIFY_KAFKA_SUB_SYS, NOTIFY_MQTT_KEYS, NOTIFY_MQTT_SUB_SYS,
    NOTIFY_MYSQL_KEYS, NOTIFY_MYSQL_SUB_SYS, NOTIFY_NATS_KEYS, NOTIFY_NATS_SUB_SYS, NOTIFY_POSTGRES_KEYS,
    NOTIFY_POSTGRES_SUB_SYS, NOTIFY_PULSAR_KEYS, NOTIFY_PULSAR_SUB_SYS, NOTIFY_REDIS_KEYS, NOTIFY_REDIS_SUB_SYS,
    NOTIFY_WEBHOOK_KEYS, NOTIFY_WEBHOOK_SUB_SYS,
};
use rustfs_config::oidc::{IDENTITY_OPENID_KEYS, IDENTITY_OPENID_SUB_SYS, OIDC_REDIRECT_URI_DYNAMIC};
use rustfs_config::{COMMENT_KEY, DEFAULT_DELIMITER, ENABLE_KEY, EnableState, RUSTFS_REGION};
use rustfs_utils::path::SLASH_SEPARATOR;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::LazyLock;
use tracing::{debug, error, info, instrument, warn};

pub const CONFIG_PREFIX: &str = "config";
const CONFIG_FILE: &str = "config.json";

pub const STORAGE_CLASS_SUB_SYS: &str = "storage_class";

pub const COMMA_SEPARATED_LISTS: &[&str] = &[rustfs_config::oidc::OIDC_SCOPES, rustfs_config::oidc::OIDC_OTHER_AUDIENCES];

static CONFIG_BUCKET: LazyLock<String> = LazyLock::new(|| format!("{RUSTFS_META_BUCKET}{SLASH_SEPARATOR}{CONFIG_PREFIX}"));

static SUB_SYSTEMS_DYNAMIC: LazyLock<HashSet<String>> = LazyLock::new(|| {
    let mut h = HashSet::new();
    h.insert(STORAGE_CLASS_SUB_SYS.to_owned());
    h
});

#[derive(Clone, Copy)]
struct TargetConfigDescriptor {
    external_key: &'static str,
    subsystem_key: &'static str,
    default_kvs: &'static LazyLock<KVS>,
    valid_keys: &'static [&'static str],
}

fn notify_target_descriptors() -> [TargetConfigDescriptor; 9] {
    [
        TargetConfigDescriptor {
            external_key: "webhook",
            subsystem_key: NOTIFY_WEBHOOK_SUB_SYS,
            default_kvs: &notify::DEFAULT_NOTIFY_WEBHOOK_KVS,
            valid_keys: NOTIFY_WEBHOOK_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "amqp",
            subsystem_key: NOTIFY_AMQP_SUB_SYS,
            default_kvs: &notify::DEFAULT_NOTIFY_AMQP_KVS,
            valid_keys: NOTIFY_AMQP_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "kafka",
            subsystem_key: NOTIFY_KAFKA_SUB_SYS,
            default_kvs: &notify::DEFAULT_NOTIFY_KAFKA_KVS,
            valid_keys: NOTIFY_KAFKA_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "mqtt",
            subsystem_key: NOTIFY_MQTT_SUB_SYS,
            default_kvs: &notify::DEFAULT_NOTIFY_MQTT_KVS,
            valid_keys: NOTIFY_MQTT_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "mysql",
            subsystem_key: NOTIFY_MYSQL_SUB_SYS,
            default_kvs: &notify::DEFAULT_NOTIFY_MYSQL_KVS,
            valid_keys: NOTIFY_MYSQL_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "nats",
            subsystem_key: NOTIFY_NATS_SUB_SYS,
            default_kvs: &notify::DEFAULT_NOTIFY_NATS_KVS,
            valid_keys: NOTIFY_NATS_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "postgres",
            subsystem_key: NOTIFY_POSTGRES_SUB_SYS,
            default_kvs: &notify::DEFAULT_NOTIFY_POSTGRES_KVS,
            valid_keys: NOTIFY_POSTGRES_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "redis",
            subsystem_key: NOTIFY_REDIS_SUB_SYS,
            default_kvs: &notify::DEFAULT_NOTIFY_REDIS_KVS,
            valid_keys: NOTIFY_REDIS_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "pulsar",
            subsystem_key: NOTIFY_PULSAR_SUB_SYS,
            default_kvs: &notify::DEFAULT_NOTIFY_PULSAR_KVS,
            valid_keys: NOTIFY_PULSAR_KEYS,
        },
    ]
}

fn audit_target_descriptors() -> [TargetConfigDescriptor; 8] {
    [
        TargetConfigDescriptor {
            external_key: "webhook",
            subsystem_key: AUDIT_WEBHOOK_SUB_SYS,
            default_kvs: &audit::DEFAULT_AUDIT_WEBHOOK_KVS,
            valid_keys: AUDIT_WEBHOOK_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "kafka",
            subsystem_key: AUDIT_KAFKA_SUB_SYS,
            default_kvs: &audit::DEFAULT_AUDIT_KAFKA_KVS,
            valid_keys: AUDIT_KAFKA_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "mqtt",
            subsystem_key: AUDIT_MQTT_SUB_SYS,
            default_kvs: &audit::DEFAULT_AUDIT_MQTT_KVS,
            valid_keys: AUDIT_MQTT_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "mysql",
            subsystem_key: AUDIT_MYSQL_SUB_SYS,
            default_kvs: &audit::DEFAULT_AUDIT_MYSQL_KVS,
            valid_keys: AUDIT_MYSQL_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "nats",
            subsystem_key: AUDIT_NATS_SUB_SYS,
            default_kvs: &audit::DEFAULT_AUDIT_NATS_KVS,
            valid_keys: AUDIT_NATS_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "postgres",
            subsystem_key: AUDIT_POSTGRES_SUB_SYS,
            default_kvs: &audit::DEFAULT_AUDIT_POSTGRES_KVS,
            valid_keys: AUDIT_POSTGRES_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "pulsar",
            subsystem_key: AUDIT_PULSAR_SUB_SYS,
            default_kvs: &audit::DEFAULT_AUDIT_PULSAR_KVS,
            valid_keys: AUDIT_PULSAR_KEYS,
        },
        TargetConfigDescriptor {
            external_key: "redis",
            subsystem_key: AUDIT_REDIS_SUB_SYS,
            default_kvs: &audit::DEFAULT_AUDIT_REDIS_KVS,
            valid_keys: AUDIT_REDIS_KEYS,
        },
    ]
}

#[instrument(skip(api))]
pub async fn read_config<S: StorageAPI>(api: Arc<S>, file: &str) -> Result<Vec<u8>> {
    let (data, _obj) = read_config_with_metadata(api, file, &ObjectOptions::default()).await?;
    Ok(data)
}

pub async fn read_config_no_lock<S: StorageAPI>(api: Arc<S>, file: &str) -> Result<Vec<u8>> {
    let (data, _obj) = read_config_with_metadata(
        api,
        file,
        &ObjectOptions {
            no_lock: true,
            ..Default::default()
        },
    )
    .await?;
    Ok(data)
}

pub async fn read_config_with_metadata<S: StorageAPI>(
    api: Arc<S>,
    file: &str,
    opts: &ObjectOptions,
) -> Result<(Vec<u8>, ObjectInfo)> {
    let h = HeaderMap::new();
    let mut rd = api
        .get_object_reader(RUSTFS_META_BUCKET, file, None, h, opts)
        .await
        .map_err(|err| {
            if err == Error::FileNotFound || matches!(err, Error::ObjectNotFound(_, _)) {
                Error::ConfigNotFound
            } else {
                warn!("read_config_with_metadata: err: {:?}, file: {}", err, file);
                err
            }
        })?;

    let data = rd.read_all().await?;

    if data.is_empty() {
        return Err(Error::ConfigNotFound);
    }

    Ok((data, rd.object_info))
}

#[instrument(skip(api, data))]
pub async fn save_config<S: StorageAPI>(api: Arc<S>, file: &str, data: Vec<u8>) -> Result<()> {
    save_config_with_opts(
        api,
        file,
        data,
        &ObjectOptions {
            max_parity: true,
            ..Default::default()
        },
    )
    .await
}

#[instrument(skip(api))]
pub async fn delete_config<S: StorageAPI>(api: Arc<S>, file: &str) -> Result<()> {
    match api
        .delete_object(
            RUSTFS_META_BUCKET,
            file,
            ObjectOptions {
                delete_prefix: true,
                delete_prefix_object: true,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            if err == Error::FileNotFound || matches!(err, Error::ObjectNotFound(_, _)) {
                Err(Error::ConfigNotFound)
            } else {
                Err(err)
            }
        }
    }
}

pub async fn save_config_with_opts<S: StorageAPI>(api: Arc<S>, file: &str, data: Vec<u8>, opts: &ObjectOptions) -> Result<()> {
    let mut put_data = PutObjReader::from_vec(data);
    if let Err(err) = api.put_object(RUSTFS_META_BUCKET, file, &mut put_data, opts).await {
        error!("save_config_with_opts: err: {:?}, file: {}", err, file);
        return Err(err);
    }
    Ok(())
}

fn new_server_config() -> Config {
    Config::new()
}

async fn new_and_save_server_config<S: StorageAPI>(api: Arc<S>) -> Result<Config> {
    let mut cfg = new_server_config();
    lookup_configs(&mut cfg, api.clone()).await;
    save_server_config(api, &cfg).await?;

    Ok(cfg)
}

fn get_config_file() -> String {
    format!("{CONFIG_PREFIX}{SLASH_SEPARATOR}{CONFIG_FILE}")
}

fn storage_class_kvs_mut(cfg: &mut Config) -> &mut crate::config::KVS {
    let sub_cfg = cfg.0.entry(STORAGE_CLASS_SUB_SYS.to_string()).or_insert_with(|| {
        let mut section = HashMap::new();
        section.insert(DEFAULT_DELIMITER.to_string(), storageclass::DEFAULT_KVS.clone());
        section
    });
    sub_cfg
        .entry(DEFAULT_DELIMITER.to_string())
        .or_insert_with(|| storageclass::DEFAULT_KVS.clone())
}

fn parse_storage_class_value(value: &Value) -> Option<String> {
    match value {
        Value::String(v) => Some(v.trim().to_string()),
        Value::Object(m) => m
            .get("parity")
            .and_then(Value::as_u64)
            .map(|parity| if parity == 0 { String::new() } else { format!("EC:{parity}") }),
        _ => None,
    }
}

fn parse_inline_block_value(value: &Value) -> Option<String> {
    match value {
        Value::String(v) if !v.trim().is_empty() => Some(v.trim().to_string()),
        Value::Number(v) => Some(v.to_string()),
        _ => None,
    }
}

fn parse_oidc_scalar_value(key: &str, value: &Value) -> Option<String> {
    match value {
        Value::String(v) => Some(v.trim().to_string()),
        Value::Bool(v) if key == ENABLE_KEY || key == OIDC_REDIRECT_URI_DYNAMIC => Some(if *v {
            EnableState::On.to_string()
        } else {
            EnableState::Off.to_string()
        }),
        Value::Bool(v) => Some(v.to_string()),
        Value::Number(v) => Some(v.to_string()),
        Value::Array(values) if COMMA_SEPARATED_LISTS.contains(&key) => {
            let values_str = values
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|val| !val.is_empty())
                .collect::<Vec<_>>()
                .join(",");
            Some(values_str)
        }
        Value::Null => None,
        _ => None,
    }
}

fn decode_oidc_provider_object(provider: &Map<String, Value>) -> KVS {
    let mut kvs = oidc::DEFAULT_IDENTITY_OPENID_KVS.clone();

    for (key, value) in provider {
        if !IDENTITY_OPENID_KEYS.contains(&key.as_str()) || key == COMMENT_KEY {
            continue;
        }

        if let Some(parsed) = parse_oidc_scalar_value(key, value) {
            kvs.insert(key.clone(), parsed);
        }
    }

    kvs
}

fn apply_external_oidc_map(cfg: &mut Config, root: &Map<String, Value>) -> bool {
    let oidc_root = root.get("openid").or_else(|| root.get(IDENTITY_OPENID_SUB_SYS));
    let Some(Value::Object(oidc_obj)) = oidc_root else {
        return false;
    };

    if oidc_obj.is_empty() {
        return false;
    }

    let subsystem = cfg.0.entry(IDENTITY_OPENID_SUB_SYS.to_string()).or_default();
    let mut applied = false;

    for (raw_instance, provider) in oidc_obj {
        let instance_key = if raw_instance == "default" {
            DEFAULT_DELIMITER.to_string()
        } else {
            raw_instance.to_string()
        };

        match provider {
            Value::Object(provider_obj) => {
                subsystem.insert(instance_key, decode_oidc_provider_object(provider_obj));
                applied = true;
            }
            Value::Array(_) => {
                if let Ok(kvs) = serde_json::from_value::<KVS>(provider.clone()) {
                    subsystem.insert(instance_key, kvs);
                    applied = true;
                }
            }
            _ => {}
        }
    }

    applied
}

fn parse_target_scalar_value(key: &str, value: &Value) -> Option<String> {
    match value {
        Value::String(v) => Some(v.trim().to_string()),
        Value::Bool(v) if key == ENABLE_KEY || key == rustfs_config::WEBHOOK_SKIP_TLS_VERIFY => Some(if *v {
            EnableState::On.to_string()
        } else {
            EnableState::Off.to_string()
        }),
        Value::Bool(v) => Some(v.to_string()),
        Value::Number(v) => Some(v.to_string()),
        Value::Null => None,
        _ => None,
    }
}

fn decode_target_instance_object(instance: &Map<String, Value>, valid_keys: &[&str]) -> KVS {
    let mut kvs = KVS::new();

    for (key, value) in instance {
        if !valid_keys.contains(&key.as_str()) || key == COMMENT_KEY {
            continue;
        }

        if let Some(parsed) = parse_target_scalar_value(key, value) {
            kvs.insert(key.clone(), parsed);
        }
    }

    kvs
}

fn decode_target_instance_value(value: &Value, valid_keys: &[&str]) -> Option<KVS> {
    match value {
        Value::Object(instance) => Some(decode_target_instance_object(instance, valid_keys)),
        Value::Array(_) => serde_json::from_value::<KVS>(value.clone()).ok(),
        _ => None,
    }
}

fn is_target_instance_shorthand(section: &Map<String, Value>, valid_keys: &[&str]) -> bool {
    section
        .iter()
        .any(|(key, value)| valid_keys.contains(&key.as_str()) && parse_target_scalar_value(key, value).is_some())
}

fn apply_external_target_section(
    cfg: &mut Config,
    notify_obj: &Map<String, Value>,
    external_key: &str,
    subsystem_key: &str,
    default_kvs: &KVS,
    valid_keys: &[&str],
) -> bool {
    let Some(Value::Object(section_obj)) = notify_obj.get(external_key).or_else(|| notify_obj.get(subsystem_key)) else {
        return false;
    };

    if section_obj.is_empty() {
        return false;
    }

    let subsystem = cfg.0.entry(subsystem_key.to_string()).or_default();
    let mut applied = false;

    if is_target_instance_shorthand(section_obj, valid_keys) {
        let kvs = decode_target_instance_object(section_obj, valid_keys);
        if !kvs.is_empty() {
            let mut merged = default_kvs.clone();
            merged.extend(kvs);
            subsystem.insert(DEFAULT_DELIMITER.to_string(), merged);
            applied = true;
        }
        return applied;
    }

    for (raw_instance, value) in section_obj {
        let Some(mut kvs) = decode_target_instance_value(value, valid_keys) else {
            continue;
        };
        if kvs.is_empty() {
            continue;
        }

        let instance_key = if raw_instance == "default" {
            DEFAULT_DELIMITER.to_string()
        } else {
            raw_instance.to_string()
        };

        if instance_key == DEFAULT_DELIMITER {
            let mut merged = default_kvs.clone();
            merged.extend(kvs);
            kvs = merged;
        }

        subsystem.insert(instance_key, kvs);
        applied = true;
    }

    applied
}

fn apply_external_target_descriptors(
    cfg: &mut Config,
    section_obj: &Map<String, Value>,
    descriptors: &[TargetConfigDescriptor],
) -> bool {
    let mut applied = false;
    for descriptor in descriptors {
        applied |= apply_external_target_section(
            cfg,
            section_obj,
            descriptor.external_key,
            descriptor.subsystem_key,
            descriptor.default_kvs,
            descriptor.valid_keys,
        );
    }
    applied
}

fn apply_external_notify_map(cfg: &mut Config, root: &Map<String, Value>) -> bool {
    let Some(Value::Object(notify_obj)) = root.get("notify") else {
        return false;
    };

    apply_external_target_descriptors(cfg, notify_obj, &notify_target_descriptors())
}

fn apply_external_audit_map(cfg: &mut Config, root: &Map<String, Value>) -> bool {
    let audit_root = root.get("audit").or_else(|| root.get("logger")).and_then(Value::as_object);
    let Some(audit_obj) = audit_root else {
        return false;
    };

    apply_external_target_descriptors(cfg, audit_obj, &audit_target_descriptors())
}

fn apply_external_storage_class_map(cfg: &mut Config, root: &Map<String, Value>) -> bool {
    let sc = root.get("storageclass").or_else(|| root.get("storage_class"));
    let Some(Value::Object(sc_obj)) = sc else {
        return false;
    };

    let mut applied = false;
    let kvs = storage_class_kvs_mut(cfg);

    if let Some(v) = sc_obj.get("standard").and_then(parse_storage_class_value) {
        kvs.insert(storageclass::CLASS_STANDARD.to_string(), v);
        applied = true;
    }
    if let Some(v) = sc_obj.get("rrs").and_then(parse_storage_class_value) {
        kvs.insert(storageclass::CLASS_RRS.to_string(), v);
        applied = true;
    }
    if let Some(Value::String(v)) = sc_obj.get("optimize")
        && !v.trim().is_empty()
    {
        kvs.insert(storageclass::OPTIMIZE.to_string(), v.clone());
        applied = true;
    }
    if let Some(v) = sc_obj.get("inline_block").and_then(parse_inline_block_value) {
        kvs.insert(storageclass::INLINE_BLOCK.to_string(), v);
        applied = true;
    }

    applied
}

fn decode_server_config_blob(data: &[u8]) -> Result<Config> {
    if let Ok(cfg) = Config::unmarshal(data) {
        return Ok(cfg);
    }

    let value: Value = serde_json::from_slice(data)?;
    let Value::Object(root) = value else {
        return Err(Error::other("unrecognized external server config shape"));
    };

    let mut cfg = Config::new();
    let has_storage = apply_external_storage_class_map(&mut cfg, &root);
    let has_oidc = apply_external_oidc_map(&mut cfg, &root);
    let has_notify = apply_external_notify_map(&mut cfg, &root);
    let has_audit = apply_external_audit_map(&mut cfg, &root);
    let has_header = root.contains_key("version") || root.contains_key("region") || root.contains_key("credential");
    if !has_storage && !has_oidc && !has_notify && !has_audit && !has_header {
        return Err(Error::other("unrecognized external server config shape"));
    }
    Ok(cfg)
}

fn parse_object_seed(data: &[u8]) -> Option<Map<String, Value>> {
    let value: Value = serde_json::from_slice(data).ok()?;
    value.as_object().cloned()
}

fn build_storageclass_object(cfg: &Config) -> Map<String, Value> {
    let kvs = cfg.get_value(STORAGE_CLASS_SUB_SYS, DEFAULT_DELIMITER).unwrap_or_default();
    let mut sc_obj = Map::new();
    sc_obj.insert(
        "standard".to_string(),
        Value::String(kvs.lookup(storageclass::CLASS_STANDARD).unwrap_or_default()),
    );
    sc_obj.insert("rrs".to_string(), Value::String(kvs.lookup(storageclass::CLASS_RRS).unwrap_or_default()));
    let optimize = kvs
        .lookup(storageclass::OPTIMIZE)
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "availability".to_string());
    sc_obj.insert("optimize".to_string(), Value::String(optimize));
    if let Some(v) = kvs.lookup(storageclass::INLINE_BLOCK).filter(|v| !v.trim().is_empty()) {
        sc_obj.insert("inline_block".to_string(), Value::String(v));
    }
    sc_obj
}

fn build_oidc_provider_object(kvs: &KVS) -> Map<String, Value> {
    let mut provider = Map::new();

    for kv in &kvs.0 {
        if kv.key == COMMENT_KEY || (kv.hidden_if_empty && kv.value.trim().is_empty()) {
            continue;
        }

        if kv.value.trim().is_empty() {
            continue;
        }

        if kv.key == ENABLE_KEY || kv.key == OIDC_REDIRECT_URI_DYNAMIC {
            let enabled = kv
                .value
                .parse::<EnableState>()
                .map(|state| state.is_enabled())
                .unwrap_or(false);
            provider.insert(kv.key.clone(), Value::Bool(enabled));
            continue;
        }

        if COMMA_SEPARATED_LISTS.contains(&kv.key.as_str()) {
            let values = kv
                .value
                .split(',')
                .map(str::trim)
                .filter(|val| !val.is_empty())
                .map(|val| Value::String(val.to_string()))
                .collect::<Vec<_>>();
            provider.insert(kv.key.clone(), Value::Array(values));
            continue;
        }

        provider.insert(kv.key.clone(), Value::String(kv.value.clone()));
    }

    provider
}

fn build_oidc_object(cfg: &Config) -> Map<String, Value> {
    let Some(subsystem) = cfg.0.get(IDENTITY_OPENID_SUB_SYS) else {
        return Map::new();
    };

    let mut providers = subsystem.iter().collect::<Vec<_>>();
    providers.sort_by_key(|(lhs, _)| *lhs);

    let mut oidc_obj = Map::new();
    for (instance_key, kvs) in providers {
        if kvs
            .lookup(rustfs_config::oidc::OIDC_CONFIG_URL)
            .unwrap_or_default()
            .trim()
            .is_empty()
        {
            continue;
        }

        let provider = build_oidc_provider_object(kvs);
        if provider.is_empty() {
            continue;
        }

        let external_key = if instance_key == DEFAULT_DELIMITER {
            "default".to_string()
        } else {
            instance_key.clone()
        };
        oidc_obj.insert(external_key, Value::Object(provider));
    }

    oidc_obj
}

fn build_semantic_oidc_object(cfg: &Config) -> Map<String, Value> {
    let Some(subsystem) = cfg.0.get(IDENTITY_OPENID_SUB_SYS) else {
        return Map::new();
    };

    let mut providers = subsystem.iter().collect::<Vec<_>>();
    providers.sort_by_key(|(lhs, _)| *lhs);

    let mut oidc_obj = Map::new();
    for (instance_key, kvs) in providers {
        let mut normalized = oidc::DEFAULT_IDENTITY_OPENID_KVS.clone();
        normalized.extend(kvs.clone());

        if normalized
            .lookup(rustfs_config::oidc::OIDC_CONFIG_URL)
            .unwrap_or_default()
            .trim()
            .is_empty()
        {
            continue;
        }

        let provider = build_oidc_provider_object(&normalized);
        if provider.is_empty() {
            continue;
        }

        let external_key = if instance_key == DEFAULT_DELIMITER {
            "default".to_string()
        } else {
            instance_key.clone()
        };
        oidc_obj.insert(external_key, Value::Object(provider));
    }

    oidc_obj
}

fn is_target_bool_key(key: &str) -> bool {
    matches!(
        key,
        ENABLE_KEY
            | rustfs_config::WEBHOOK_SKIP_TLS_VERIFY
            | rustfs_config::KAFKA_TLS_ENABLE
            | rustfs_config::MQTT_TLS_TRUST_LEAF_AS_CA
            | rustfs_config::NATS_TLS_REQUIRED
            | rustfs_config::PULSAR_TLS_ALLOW_INSECURE
            | rustfs_config::PULSAR_TLS_HOSTNAME_VERIFICATION
    )
}

fn encode_target_scalar_value(key: &str, value: &str) -> Value {
    if is_target_bool_key(key) {
        if let Ok(state) = value.parse::<EnableState>() {
            return Value::Bool(state.is_enabled());
        }
        if let Ok(boolean) = value.parse::<bool>() {
            return Value::Bool(boolean);
        }
    }

    Value::String(value.to_string())
}

fn is_hidden_if_empty(default_kvs: &KVS, key: &str) -> bool {
    default_kvs
        .0
        .iter()
        .find(|kv| kv.key == key)
        .map(|kv| kv.hidden_if_empty)
        .unwrap_or(false)
}

fn build_target_instance_diff_object(kvs: &KVS, baseline: &KVS, valid_keys: &[&str], default_kvs: &KVS) -> Map<String, Value> {
    let mut instance = Map::new();

    for key in valid_keys {
        if *key == COMMENT_KEY {
            continue;
        }

        let baseline_value = baseline.lookup(key).unwrap_or_default();
        let effective_value = kvs.lookup(key).unwrap_or_else(|| baseline_value.clone());

        if effective_value == baseline_value {
            continue;
        }

        if effective_value.trim().is_empty() && baseline_value.trim().is_empty() {
            continue;
        }

        if is_hidden_if_empty(default_kvs, key) && effective_value.trim().is_empty() && baseline_value.trim().is_empty() {
            continue;
        }

        instance.insert((*key).to_string(), encode_target_scalar_value(key, &effective_value));
    }

    instance
}

fn merged_target_default_kvs(subsystem: &HashMap<String, KVS>, default_kvs: &KVS) -> KVS {
    let mut merged = default_kvs.clone();
    if let Some(kvs) = subsystem.get(DEFAULT_DELIMITER) {
        merged.extend(kvs.clone());
    }
    merged
}

fn build_target_subsystem_object(
    cfg: &Config,
    subsystem_key: &str,
    default_kvs: &KVS,
    valid_keys: &[&str],
) -> Map<String, Value> {
    let Some(subsystem) = cfg.0.get(subsystem_key) else {
        return Map::new();
    };

    let effective_default = merged_target_default_kvs(subsystem, default_kvs);
    let mut subsystem_obj = Map::new();

    if let Some(default_instance) = subsystem.get(DEFAULT_DELIMITER) {
        let default_obj = build_target_instance_diff_object(default_instance, default_kvs, valid_keys, default_kvs);
        if !default_obj.is_empty() {
            subsystem_obj.insert("default".to_string(), Value::Object(default_obj));
        }
    }

    let mut instances = subsystem
        .iter()
        .filter(|(instance_key, _)| instance_key.as_str() != DEFAULT_DELIMITER)
        .collect::<Vec<_>>();
    instances.sort_by_key(|(lhs, _)| *lhs);

    for (instance_key, kvs) in instances {
        let instance_obj = build_target_instance_diff_object(kvs, &effective_default, valid_keys, default_kvs);
        if !instance_obj.is_empty() {
            subsystem_obj.insert(instance_key.clone(), Value::Object(instance_obj));
        }
    }

    subsystem_obj
}

fn build_target_object(cfg: &Config, descriptors: &[TargetConfigDescriptor]) -> Map<String, Value> {
    let mut target_obj = Map::new();
    for descriptor in descriptors {
        let subsystem_obj =
            build_target_subsystem_object(cfg, descriptor.subsystem_key, descriptor.default_kvs, descriptor.valid_keys);
        if !subsystem_obj.is_empty() {
            target_obj.insert(descriptor.external_key.to_string(), Value::Object(subsystem_obj));
        }
    }
    target_obj
}

fn build_notify_object(cfg: &Config) -> Map<String, Value> {
    build_target_object(cfg, &notify_target_descriptors())
}

fn build_audit_object(cfg: &Config) -> Map<String, Value> {
    build_target_object(cfg, &audit_target_descriptors())
}

fn sync_rendered_target_object(
    target_obj: &mut Map<String, Value>,
    rendered_target: &Map<String, Value>,
    descriptors: &[TargetConfigDescriptor],
) {
    for descriptor in descriptors {
        match rendered_target.get(descriptor.external_key) {
            Some(Value::Object(v)) => {
                target_obj.insert(descriptor.external_key.to_string(), Value::Object(v.clone()));
                target_obj.remove(descriptor.subsystem_key);
            }
            _ => {
                target_obj.remove(descriptor.external_key);
                target_obj.remove(descriptor.subsystem_key);
            }
        }
    }
}

fn encode_server_config_blob(cfg: &Config, seed: Option<&[u8]>) -> Result<Vec<u8>> {
    let mut root = seed.and_then(parse_object_seed).unwrap_or_default();

    if !matches!(root.get("version"), Some(Value::String(v)) if !v.trim().is_empty()) {
        root.insert("version".to_string(), Value::String("33".to_string()));
    }
    if !matches!(root.get("region"), Some(Value::String(v)) if !v.trim().is_empty()) {
        root.insert("region".to_string(), Value::String(RUSTFS_REGION.to_string()));
    }

    let mut sc_obj = match root.remove("storageclass") {
        Some(Value::Object(v)) => v,
        _ => Map::new(),
    };
    for (k, v) in build_storageclass_object(cfg) {
        sc_obj.insert(k, v);
    }
    root.insert("storageclass".to_string(), Value::Object(sc_obj));
    root.remove("storage_class");

    let oidc_obj = build_oidc_object(cfg);
    if oidc_obj.is_empty() {
        root.remove("openid");
        root.remove(IDENTITY_OPENID_SUB_SYS);
    } else {
        root.insert("openid".to_string(), Value::Object(oidc_obj));
        root.remove(IDENTITY_OPENID_SUB_SYS);
    }

    let mut notify_obj = match root.remove("notify") {
        Some(Value::Object(v)) => v,
        _ => Map::new(),
    };
    let rendered_notify = build_notify_object(cfg);
    sync_rendered_target_object(&mut notify_obj, &rendered_notify, &notify_target_descriptors());
    if notify_obj.is_empty() {
        root.remove("notify");
    } else {
        root.insert("notify".to_string(), Value::Object(notify_obj));
    }
    for descriptor in notify_target_descriptors() {
        root.remove(descriptor.subsystem_key);
    }

    let mut logger_obj = match root.remove("logger") {
        Some(Value::Object(v)) => v,
        _ => Map::new(),
    };
    let rendered_audit = build_audit_object(cfg);
    sync_rendered_target_object(&mut logger_obj, &rendered_audit, &audit_target_descriptors());
    if logger_obj.is_empty() {
        root.remove("logger");
    } else {
        root.insert("logger".to_string(), Value::Object(logger_obj));
    }
    root.remove("audit");
    for descriptor in audit_target_descriptors() {
        root.remove(descriptor.subsystem_key);
    }

    Ok(serde_json::to_vec(&Value::Object(root))?)
}

fn is_standard_object_server_config(data: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(data) else {
        return false;
    };
    let Value::Object(root) = value else {
        return false;
    };
    matches!(root.get("version"), Some(Value::String(v)) if !v.trim().is_empty())
        && matches!(root.get("storageclass"), Some(Value::Object(_)))
        && !root.contains_key("storage_class")
}

fn configs_semantically_equal(lhs: &Config, rhs: &Config) -> bool {
    build_storageclass_object(lhs) == build_storageclass_object(rhs)
        && build_semantic_oidc_object(lhs) == build_semantic_oidc_object(rhs)
        && build_notify_object(lhs) == build_notify_object(rhs)
        && build_audit_object(lhs) == build_audit_object(rhs)
}

fn is_object_not_found(err: &Error) -> bool {
    *err == Error::FileNotFound || matches!(err, Error::ObjectNotFound(_, _) | Error::BucketNotFound(_))
}

pub async fn try_migrate_server_config<S: StorageAPI>(api: Arc<S>) {
    let config_file = get_config_file();
    match api
        .get_object_info(
            RUSTFS_META_BUCKET,
            &config_file,
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => {
            debug!("server config already exists in RustFS metadata bucket, skip migration");
            return;
        }
        Err(err) if is_object_not_found(&err) => {}
        Err(err) => {
            warn!("check target server config failed, skip migration: {:?}", err);
            return;
        }
    }

    let opts = ObjectOptions {
        max_parity: true,
        no_lock: true,
        ..Default::default()
    };

    let mut rd = match api
        .get_object_reader(MIGRATING_META_BUCKET, &config_file, None, HeaderMap::new(), &opts)
        .await
    {
        Ok(v) => v,
        Err(err) => {
            if !is_object_not_found(&err) {
                warn!("read legacy server config failed: {:?}", err);
            }
            return;
        }
    };

    let data = match rd.read_all().await {
        Ok(v) if !v.is_empty() => v,
        Ok(_) => {
            debug!("legacy server config is empty, skip migration");
            return;
        }
        Err(err) => {
            warn!("read legacy server config body failed: {:?}", err);
            return;
        }
    };

    let cfg = match decode_server_config_blob(&data) {
        Ok(v) => v,
        Err(err) => {
            warn!("legacy server config format is incompatible, skip migration: {:?}", err);
            return;
        }
    };
    let normalized = match encode_server_config_blob(&cfg, Some(&data)) {
        Ok(v) => v,
        Err(err) => {
            warn!("serialize migrated server config failed, skip migration: {:?}", err);
            return;
        }
    };

    match save_config(api, &config_file, normalized).await {
        Ok(()) => {
            info!("Migrated compatible server config from legacy metadata bucket");
        }
        Err(err) => {
            warn!("write migrated server config failed: {:?}", err);
        }
    }
}

/// Handle the situation where the configuration file does not exist, create and save a new configuration
async fn handle_missing_config<S: StorageAPI>(api: Arc<S>, context: &str) -> Result<Config> {
    warn!("Configuration not found ({}): Start initializing new configuration", context);
    let cfg = if is_first_cluster_node_local().await {
        new_and_save_server_config(api.clone()).await?
    } else {
        let mut cfg = new_server_config();
        lookup_configs(&mut cfg, api).await;
        cfg
    };
    warn!("Configuration initialization complete ({})", context);
    Ok(cfg)
}

/// Handle configuration file read errors
fn handle_config_read_error(err: Error, file_path: &str) -> Result<Config> {
    error!("Read configuration failed (path: '{}'): {:?}", file_path, err);
    Err(err)
}

pub async fn read_config_without_migrate<S: StorageAPI>(api: Arc<S>) -> Result<Config> {
    let config_file = get_config_file();

    // Try to read the configuration file
    match read_config_no_lock(api.clone(), &config_file).await {
        Ok(data) => read_server_config(api, &data).await,
        Err(Error::ConfigNotFound) => handle_missing_config(api, "Read the main configuration").await,
        Err(err) => handle_config_read_error(err, &config_file),
    }
}

async fn read_server_config<S: StorageAPI>(api: Arc<S>, data: &[u8]) -> Result<Config> {
    // If the provided data is empty, try to read from the file again
    if data.is_empty() {
        let config_file = get_config_file();
        warn!("Received empty configuration data, try to reread from '{}'", config_file);

        // Try to read the configuration again
        match read_config_no_lock(api.clone(), &config_file).await {
            Ok(cfg_data) => {
                // TODO: decrypt
                let cfg = decode_server_config_blob(&cfg_data)?;
                return Ok(cfg.merge());
            }
            Err(Error::ConfigNotFound) => return handle_missing_config(api, "Read alternate configuration").await,
            Err(err) => return handle_config_read_error(err, &config_file),
        }
    }

    // Process non-empty configuration data
    let cfg = decode_server_config_blob(data)?;
    Ok(cfg.merge())
}

pub async fn save_server_config<S: StorageAPI>(api: Arc<S>, cfg: &Config) -> Result<()> {
    let config_file = get_config_file();
    let existing = match read_config(api.clone(), &config_file).await {
        Ok(v) => Some(v),
        Err(Error::ConfigNotFound) => None,
        Err(err) => {
            warn!("read existing server config before save failed, continue with clean output: {:?}", err);
            None
        }
    };

    if let Some(current) = existing.as_deref()
        && is_standard_object_server_config(current)
        && let Ok(decoded_current) = decode_server_config_blob(current)
        && configs_semantically_equal(&decoded_current, cfg)
    {
        debug!("server config unchanged and already in standard object shape, skip write");
        return Ok(());
    }

    let data = encode_server_config_blob(cfg, existing.as_deref())?;
    if existing.as_deref().is_some_and(|current| current == data.as_slice()) {
        debug!("server config bytes unchanged after encode, skip write");
        return Ok(());
    }

    save_config(api, &config_file, data).await
}

pub async fn lookup_configs<S: StorageAPI>(cfg: &mut Config, api: Arc<S>) {
    // TODO: from etcd
    if let Err(err) = apply_dynamic_config(cfg, api).await {
        error!("apply_dynamic_config err {:?}", &err);
    }
}

async fn apply_dynamic_config<S: StorageAPI>(cfg: &mut Config, api: Arc<S>) -> Result<()> {
    for key in SUB_SYSTEMS_DYNAMIC.iter() {
        apply_dynamic_config_for_sub_sys(cfg, api.clone(), key).await?;
    }

    Ok(())
}

async fn apply_dynamic_config_for_sub_sys<S: StorageAPI>(cfg: &mut Config, api: Arc<S>, subsys: &str) -> Result<()> {
    let set_drive_counts = api.set_drive_counts();
    if subsys == STORAGE_CLASS_SUB_SYS {
        let kvs = cfg.get_value(STORAGE_CLASS_SUB_SYS, DEFAULT_DELIMITER).unwrap_or_default();

        for (i, count) in set_drive_counts.iter().enumerate() {
            match storageclass::lookup_config(&kvs, *count) {
                Ok(res) => {
                    if i == 0
                        && GLOBAL_STORAGE_CLASS.get().is_none()
                        && let Err(r) = GLOBAL_STORAGE_CLASS.set(res)
                    {
                        error!("GLOBAL_STORAGE_CLASS.set failed {:?}", r);
                    }
                }
                Err(err) => {
                    error!("init storage class err:{:?}", &err);
                    break;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        configs_semantically_equal, decode_server_config_blob, encode_server_config_blob, is_standard_object_server_config,
        read_config_with_metadata, storage_class_kvs_mut,
    };
    use crate::config::{Config, audit, notify, oidc};
    use crate::disk::endpoint::Endpoint;
    use crate::endpoints::SetupType;
    use crate::error::{Error, Result};
    use crate::global::{is_dist_erasure, is_erasure, is_erasure_sd, update_erasure_type};
    use crate::set_disk::SetDisks;
    use crate::store_api::{
        BucketInfo, BucketOperations, BucketOptions, CompletePart, DeleteBucketOptions, DeletedObject, GetObjectReader,
        HTTPRangeSpec, HealOperations, ListMultipartsInfo, ListObjectVersionsInfo, ListObjectsV2Info, ListOperations,
        MakeBucketOptions, MultipartInfo, MultipartOperations, MultipartUploadResult, ObjectIO, ObjectInfo, ObjectOperations,
        ObjectOptions, ObjectToDelete, PartInfo, PutObjReader, StorageAPI, WalkOptions,
    };
    use http::HeaderMap;
    use rustfs_config::audit::{AUDIT_KAFKA_SUB_SYS, AUDIT_MQTT_SUB_SYS, AUDIT_WEBHOOK_SUB_SYS};
    use rustfs_config::notify::{
        NOTIFY_AMQP_SUB_SYS, NOTIFY_KAFKA_SUB_SYS, NOTIFY_MQTT_SUB_SYS, NOTIFY_MYSQL_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS,
    };
    use rustfs_config::oidc::IDENTITY_OPENID_SUB_SYS;
    use rustfs_config::{
        DEFAULT_DELIMITER, ENABLE_KEY, EnableState, MYSQL_DSN_STRING, MYSQL_MAX_OPEN_CONNECTIONS, MYSQL_QUEUE_DIR, MYSQL_TABLE,
    };
    use rustfs_filemeta::FileInfo;
    use rustfs_lock::client::LockClient;
    use rustfs_lock::client::local::LocalClient;
    use rustfs_lock::{LockError, LockInfo, LockResponse, LockStats};
    use serde_json::Value;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::fmt::{Debug, Formatter};
    use std::io::Cursor;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use time::OffsetDateTime;
    use tokio::io::{AsyncRead, ReadBuf};
    use tokio::sync::RwLock;
    use tokio_util::sync::CancellationToken;

    #[derive(Debug, Default)]
    struct FailingClient;

    #[async_trait::async_trait]
    impl LockClient for FailingClient {
        async fn acquire_lock(&self, _request: &rustfs_lock::LockRequest) -> rustfs_lock::Result<LockResponse> {
            Err(LockError::internal("simulated offline client"))
        }

        async fn release(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            Ok(false)
        }

        async fn refresh(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            Ok(false)
        }

        async fn force_release(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            Ok(false)
        }

        async fn check_status(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<Option<LockInfo>> {
            Ok(None)
        }

        async fn get_stats(&self) -> rustfs_lock::Result<LockStats> {
            Ok(LockStats::default())
        }

        async fn close(&self) -> rustfs_lock::Result<()> {
            Ok(())
        }

        async fn is_online(&self) -> bool {
            false
        }

        async fn is_local(&self) -> bool {
            false
        }
    }

    struct GuardedCursor {
        inner: Cursor<Vec<u8>>,
        _guard: Option<rustfs_lock::NamespaceLockGuard>,
    }

    impl AsyncRead for GuardedCursor {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    struct LockingConfigStorage {
        set_disks: Arc<SetDisks>,
        data: Vec<u8>,
    }

    impl Debug for LockingConfigStorage {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("LockingConfigStorage").finish()
        }
    }

    struct SetupTypeGuard {
        previous: SetupType,
    }

    impl SetupTypeGuard {
        async fn switch_to(next: SetupType) -> Self {
            let previous = current_setup_type().await;
            update_erasure_type(next).await;
            Self { previous }
        }
    }

    impl Drop for SetupTypeGuard {
        fn drop(&mut self) {
            let previous = self.previous.clone();
            let handle = tokio::runtime::Handle::current();
            tokio::task::block_in_place(|| {
                handle.block_on(async move {
                    update_erasure_type(previous).await;
                });
            });
        }
    }

    async fn current_setup_type() -> SetupType {
        if is_dist_erasure().await {
            SetupType::DistErasure
        } else if is_erasure_sd().await {
            SetupType::ErasureSD
        } else if is_erasure().await {
            SetupType::Erasure
        } else {
            SetupType::Unknown
        }
    }

    impl LockingConfigStorage {
        async fn new(lockers: Vec<Arc<dyn LockClient>>, data: Vec<u8>) -> Self {
            let endpoints = vec![
                Endpoint::try_from("http://127.0.0.1:9000/data").expect("first endpoint should parse"),
                Endpoint::try_from("http://127.0.0.1:9001/data").expect("second endpoint should parse"),
            ];

            let set_disks = SetDisks::new(
                "config-test-owner".to_string(),
                Arc::new(RwLock::new(vec![None, None])),
                2,
                1,
                0,
                0,
                endpoints,
                crate::disk::format::FormatV3::new(1, 2),
                lockers,
            )
            .await;

            Self { set_disks, data }
        }

        fn object_info(&self, bucket: &str, object: &str) -> ObjectInfo {
            ObjectInfo {
                bucket: bucket.to_string(),
                name: object.to_string(),
                storage_class: None,
                mod_time: Some(OffsetDateTime::now_utc()),
                size: self.data.len() as i64,
                actual_size: self.data.len() as i64,
                is_dir: false,
                user_defined: HashMap::new(),
                parity_blocks: 0,
                data_blocks: 0,
                version_id: None,
                delete_marker: false,
                transitioned_object: Default::default(),
                restore_ongoing: false,
                restore_expires: None,
                user_tags: String::new(),
                parts: Vec::new(),
                is_latest: true,
                content_type: Some("application/json".to_string()),
                content_encoding: None,
                expires: None,
                num_versions: 1,
                successor_mod_time: None,
                put_object_reader: None,
                etag: None,
                inlined: false,
                metadata_only: false,
                version_only: false,
                replication_status_internal: None,
                replication_status: Default::default(),
                version_purge_status_internal: None,
                version_purge_status: Default::default(),
                replication_decision: String::new(),
                checksum: None,
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectIO for LockingConfigStorage {
        async fn get_object_reader(
            &self,
            bucket: &str,
            object: &str,
            _range: Option<HTTPRangeSpec>,
            _h: HeaderMap,
            opts: &ObjectOptions,
        ) -> Result<GetObjectReader> {
            let guard = if opts.no_lock {
                None
            } else {
                Some(
                    self.set_disks
                        .new_ns_lock(bucket, object)
                        .await?
                        .get_read_lock(std::time::Duration::from_millis(100))
                        .await
                        .map_err(|err| Error::other(format!("lock failed: {err}")))?,
                )
            };

            Ok(GetObjectReader {
                stream: Box::new(GuardedCursor {
                    inner: Cursor::new(self.data.clone()),
                    _guard: guard,
                }),
                object_info: self.object_info(bucket, object),
            })
        }

        async fn put_object(
            &self,
            _bucket: &str,
            _object: &str,
            _data: &mut PutObjReader,
            _opts: &ObjectOptions,
        ) -> Result<ObjectInfo> {
            panic!("unused in test")
        }
    }

    #[async_trait::async_trait]
    impl BucketOperations for LockingConfigStorage {
        async fn make_bucket(&self, _bucket: &str, _opts: &MakeBucketOptions) -> Result<()> {
            panic!("unused in test")
        }

        async fn get_bucket_info(&self, _bucket: &str, _opts: &BucketOptions) -> Result<BucketInfo> {
            panic!("unused in test")
        }

        async fn list_bucket(&self, _opts: &BucketOptions) -> Result<Vec<BucketInfo>> {
            panic!("unused in test")
        }

        async fn delete_bucket(&self, _bucket: &str, _opts: &DeleteBucketOptions) -> Result<()> {
            panic!("unused in test")
        }
    }

    #[async_trait::async_trait]
    impl ObjectOperations for LockingConfigStorage {
        async fn get_object_info(&self, _bucket: &str, _object: &str, _opts: &ObjectOptions) -> Result<ObjectInfo> {
            panic!("unused in test")
        }

        async fn verify_object_integrity(&self, _bucket: &str, _object: &str, _opts: &ObjectOptions) -> Result<()> {
            panic!("unused in test")
        }

        async fn copy_object(
            &self,
            _src_bucket: &str,
            _src_object: &str,
            _dst_bucket: &str,
            _dst_object: &str,
            _src_info: &mut ObjectInfo,
            _src_opts: &ObjectOptions,
            _dst_opts: &ObjectOptions,
        ) -> Result<ObjectInfo> {
            panic!("unused in test")
        }

        async fn delete_object_version(
            &self,
            _bucket: &str,
            _object: &str,
            _fi: &FileInfo,
            _force_del_marker: bool,
        ) -> Result<()> {
            panic!("unused in test")
        }

        async fn delete_object(&self, _bucket: &str, _object: &str, _opts: ObjectOptions) -> Result<ObjectInfo> {
            panic!("unused in test")
        }

        async fn delete_objects(
            &self,
            _bucket: &str,
            _objects: Vec<ObjectToDelete>,
            _opts: ObjectOptions,
        ) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
            panic!("unused in test")
        }

        async fn put_object_metadata(&self, _bucket: &str, _object: &str, _opts: &ObjectOptions) -> Result<ObjectInfo> {
            panic!("unused in test")
        }

        async fn get_object_tags(&self, _bucket: &str, _object: &str, _opts: &ObjectOptions) -> Result<String> {
            panic!("unused in test")
        }

        async fn put_object_tags(&self, _bucket: &str, _object: &str, _tags: &str, _opts: &ObjectOptions) -> Result<ObjectInfo> {
            panic!("unused in test")
        }

        async fn delete_object_tags(&self, _bucket: &str, _object: &str, _opts: &ObjectOptions) -> Result<ObjectInfo> {
            panic!("unused in test")
        }

        async fn add_partial(&self, _bucket: &str, _object: &str, _version_id: &str) -> Result<()> {
            panic!("unused in test")
        }

        async fn transition_object(&self, _bucket: &str, _object: &str, _opts: &ObjectOptions) -> Result<()> {
            panic!("unused in test")
        }

        async fn restore_transitioned_object(self: Arc<Self>, _bucket: &str, _object: &str, _opts: &ObjectOptions) -> Result<()> {
            panic!("unused in test")
        }
    }

    #[async_trait::async_trait]
    impl ListOperations for LockingConfigStorage {
        async fn list_objects_v2(
            self: Arc<Self>,
            _bucket: &str,
            _prefix: &str,
            _continuation_token: Option<String>,
            _delimiter: Option<String>,
            _max_keys: i32,
            _fetch_owner: bool,
            _start_after: Option<String>,
            _incl_deleted: bool,
        ) -> Result<ListObjectsV2Info> {
            panic!("unused in test")
        }

        async fn list_object_versions(
            self: Arc<Self>,
            _bucket: &str,
            _prefix: &str,
            _marker: Option<String>,
            _version_marker: Option<String>,
            _delimiter: Option<String>,
            _max_keys: i32,
        ) -> Result<ListObjectVersionsInfo> {
            panic!("unused in test")
        }

        async fn walk(
            self: Arc<Self>,
            _rx: CancellationToken,
            _bucket: &str,
            _prefix: &str,
            _result: tokio::sync::mpsc::Sender<crate::store_api::ObjectInfoOrErr>,
            _opts: WalkOptions,
        ) -> Result<()> {
            panic!("unused in test")
        }
    }

    #[async_trait::async_trait]
    impl MultipartOperations for LockingConfigStorage {
        async fn list_multipart_uploads(
            &self,
            _bucket: &str,
            _prefix: &str,
            _key_marker: Option<String>,
            _upload_id_marker: Option<String>,
            _delimiter: Option<String>,
            _max_uploads: usize,
        ) -> Result<ListMultipartsInfo> {
            panic!("unused in test")
        }

        async fn new_multipart_upload(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &ObjectOptions,
        ) -> Result<MultipartUploadResult> {
            panic!("unused in test")
        }

        async fn copy_object_part(
            &self,
            _src_bucket: &str,
            _src_object: &str,
            _dst_bucket: &str,
            _dst_object: &str,
            _upload_id: &str,
            _part_id: usize,
            _start_offset: i64,
            _length: i64,
            _src_info: &ObjectInfo,
            _src_opts: &ObjectOptions,
            _dst_opts: &ObjectOptions,
        ) -> Result<()> {
            panic!("unused in test")
        }

        async fn put_object_part(
            &self,
            _bucket: &str,
            _object: &str,
            _upload_id: &str,
            _part_id: usize,
            _data: &mut PutObjReader,
            _opts: &ObjectOptions,
        ) -> Result<PartInfo> {
            panic!("unused in test")
        }

        async fn get_multipart_info(
            &self,
            _bucket: &str,
            _object: &str,
            _upload_id: &str,
            _opts: &ObjectOptions,
        ) -> Result<MultipartInfo> {
            panic!("unused in test")
        }

        async fn list_object_parts(
            &self,
            _bucket: &str,
            _object: &str,
            _upload_id: &str,
            _part_number_marker: Option<usize>,
            _max_parts: usize,
            _opts: &ObjectOptions,
        ) -> Result<crate::store_api::ListPartsInfo> {
            panic!("unused in test")
        }

        async fn abort_multipart_upload(
            &self,
            _bucket: &str,
            _object: &str,
            _upload_id: &str,
            _opts: &ObjectOptions,
        ) -> Result<()> {
            panic!("unused in test")
        }

        async fn complete_multipart_upload(
            self: Arc<Self>,
            _bucket: &str,
            _object: &str,
            _upload_id: &str,
            _uploaded_parts: Vec<CompletePart>,
            _opts: &ObjectOptions,
        ) -> Result<ObjectInfo> {
            panic!("unused in test")
        }
    }

    #[async_trait::async_trait]
    impl HealOperations for LockingConfigStorage {
        async fn heal_format(&self, _dry_run: bool) -> Result<(rustfs_madmin::heal_commands::HealResultItem, Option<Error>)> {
            panic!("unused in test")
        }

        async fn heal_bucket(
            &self,
            _bucket: &str,
            _opts: &rustfs_common::heal_channel::HealOpts,
        ) -> Result<rustfs_madmin::heal_commands::HealResultItem> {
            panic!("unused in test")
        }

        async fn heal_object(
            &self,
            _bucket: &str,
            _object: &str,
            _version_id: &str,
            _opts: &rustfs_common::heal_channel::HealOpts,
        ) -> Result<(rustfs_madmin::heal_commands::HealResultItem, Option<Error>)> {
            panic!("unused in test")
        }

        async fn get_pool_and_set(&self, _id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)> {
            panic!("unused in test")
        }

        async fn check_abandoned_parts(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &rustfs_common::heal_channel::HealOpts,
        ) -> Result<()> {
            panic!("unused in test")
        }
    }

    #[async_trait::async_trait]
    impl StorageAPI for LockingConfigStorage {
        async fn new_ns_lock(&self, bucket: &str, object: &str) -> Result<rustfs_lock::NamespaceLockWrapper> {
            self.set_disks.new_ns_lock(bucket, object).await
        }

        async fn backend_info(&self) -> rustfs_madmin::BackendInfo {
            panic!("unused in test")
        }

        async fn storage_info(&self) -> rustfs_madmin::StorageInfo {
            panic!("unused in test")
        }

        async fn local_storage_info(&self) -> rustfs_madmin::StorageInfo {
            panic!("unused in test")
        }

        async fn get_disks(&self, _pool_idx: usize, _set_idx: usize) -> Result<Vec<Option<crate::disk::DiskStore>>> {
            panic!("unused in test")
        }

        fn set_drive_counts(&self) -> Vec<usize> {
            panic!("unused in test")
        }
    }

    #[test]
    fn test_decode_server_config_accepts_legacy_hidden_if_empty_alias() {
        let input = r#"{"storage_class":{"_":[{"key":"standard","value":"EC:2","hiddenIfEmpty":true}]}}"#;
        let cfg = decode_server_config_blob(input.as_bytes()).expect("decode should succeed");
        let kvs = cfg.get_value("storage_class", "_").expect("storage_class should exist");
        assert!(kvs.0[0].hidden_if_empty);
    }

    #[test]
    fn test_decode_server_config_accepts_missing_hidden_if_empty() {
        let input = r#"{"storage_class":{"_":[{"key":"standard","value":"EC:2"}]}}"#;
        let cfg = decode_server_config_blob(input.as_bytes()).expect("decode should succeed");
        let kvs = cfg.get_value("storage_class", "_").expect("storage_class should exist");
        assert!(!kvs.0[0].hidden_if_empty);
    }

    #[test]
    fn test_decode_server_config_accepts_v33_object_shape() {
        let input = r#"{
          "version":"33",
          "credential":{"accessKey":"test","secretKey":"testtesttest"},
          "region":"us-east-1",
          "worm":"off",
          "storageclass":{"standard":"EC:2","rrs":"EC:1"},
          "notify":{},
          "logger":{},
          "compress":{"enabled":false},
          "openid":{},
          "policy":{"opa":{}},
          "ldapserverconfig":{}
        }"#;

        let cfg = decode_server_config_blob(input.as_bytes()).expect("decode should succeed");
        let kvs = cfg.get_value("storage_class", "_").expect("storage_class should exist");
        assert_eq!(kvs.get("standard"), "EC:2");
        assert_eq!(kvs.get("rrs"), "EC:1");
        assert_eq!(kvs.get("optimize"), "availability");
    }

    #[test]
    fn test_decode_server_config_reads_openid_providers() {
        let input = r#"{
          "version":"33",
          "storageclass":{"standard":"EC:2","rrs":"EC:1"},
          "openid":{
            "default":{
              "enable":true,
              "config_url":"https://example.com/.well-known/openid-configuration",
              "client_id":"console",
              "client_secret":"secret-value",
              "scopes":["openid","profile","email"],
              "other_audiences":["aud1", "aud2"],
              "redirect_uri_dynamic":true,
              "display_name":"Default Provider"
            },
            "smoke":{
              "enable":false,
              "config_url":"https://issuer.example.com/.well-known/openid-configuration",
              "client_id":"smoke-client",
              "scopes":["openid"],
              "redirect_uri_dynamic":false
            }
          }
        }"#;

        let cfg = decode_server_config_blob(input.as_bytes()).expect("decode should succeed");

        let default_kvs = cfg
            .get_value(IDENTITY_OPENID_SUB_SYS, DEFAULT_DELIMITER)
            .expect("default oidc provider should exist");
        assert_eq!(
            default_kvs.get(rustfs_config::oidc::OIDC_CONFIG_URL),
            "https://example.com/.well-known/openid-configuration"
        );
        assert_eq!(default_kvs.get(rustfs_config::oidc::OIDC_CLIENT_ID), "console");
        assert_eq!(default_kvs.get(rustfs_config::oidc::OIDC_SCOPES), "openid,profile,email");
        assert_eq!(default_kvs.get(rustfs_config::oidc::OIDC_OTHER_AUDIENCES), "aud1,aud2");
        assert_eq!(default_kvs.get(ENABLE_KEY), EnableState::On.to_string());

        let smoke_kvs = cfg
            .get_value(IDENTITY_OPENID_SUB_SYS, "smoke")
            .expect("named oidc provider should exist");
        assert_eq!(smoke_kvs.get(rustfs_config::oidc::OIDC_CLIENT_ID), "smoke-client");
        assert_eq!(
            smoke_kvs.get(rustfs_config::oidc::OIDC_REDIRECT_URI_DYNAMIC),
            EnableState::Off.to_string()
        );
    }

    #[test]
    fn test_decode_server_config_reads_notify_targets() {
        let input = r#"{
          "version":"33",
          "storageclass":{"standard":"EC:2","rrs":"EC:1"},
          "notify":{
            "webhook":{
              "primary":{
                "enable":true,
                "endpoint":"https://example.com/hook",
                "queue_dir":"/tmp/webhook-queue"
              }
            },
            "mqtt":{
              "default":{
                "enable":true,
                "topic":"events"
              },
              "analytics":{
                "enable":true,
                "broker":"tcp://127.0.0.1:1883",
                "topic":"events",
                "queue_dir":""
              }
            },
            "kafka":{
              "streaming":{
                "enable":true,
                "brokers":"127.0.0.1:9092,127.0.0.1:9093",
                "topic":"events-kafka",
                "acks":"all",
                "tls_enable":true
              }
            },
            "amqp":{
              "primary":{
                "enable":true,
                "url":"amqp://127.0.0.1:5672/%2f",
                "exchange":"rustfs.events",
                "routing_key":"objects",
                "persistent":true
              }
            },
            "mysql":{
              "primary":{
                "enable":true,
                "dsn_string":"rustfs:password@tcp(127.0.0.1:3306)/rustfs_events",
                "table":"rustfs_events",
                "queue_dir":"/tmp/mysql-queue",
                "max_open_connections":"2"
              }
            }
          }
        }"#;

        let cfg = decode_server_config_blob(input.as_bytes()).expect("decode should succeed");

        let webhook = cfg
            .get_value(NOTIFY_WEBHOOK_SUB_SYS, "primary")
            .expect("webhook target should be decoded");
        assert_eq!(webhook.get(ENABLE_KEY), EnableState::On.to_string());
        assert_eq!(webhook.get(rustfs_config::WEBHOOK_ENDPOINT), "https://example.com/hook");
        assert_eq!(webhook.get(rustfs_config::WEBHOOK_QUEUE_DIR), "/tmp/webhook-queue");

        let mqtt_default = cfg
            .get_value(NOTIFY_MQTT_SUB_SYS, DEFAULT_DELIMITER)
            .expect("mqtt default should be decoded");
        assert_eq!(mqtt_default.get(ENABLE_KEY), EnableState::On.to_string());
        assert_eq!(mqtt_default.get(rustfs_config::MQTT_TOPIC), "events");
        assert_eq!(
            mqtt_default.get(rustfs_config::MQTT_QUEUE_DIR),
            notify::DEFAULT_NOTIFY_MQTT_KVS.get(rustfs_config::MQTT_QUEUE_DIR)
        );

        let mqtt = cfg
            .get_value(NOTIFY_MQTT_SUB_SYS, "analytics")
            .expect("mqtt target should be decoded");
        assert_eq!(mqtt.get(rustfs_config::MQTT_BROKER), "tcp://127.0.0.1:1883");
        assert_eq!(mqtt.get(rustfs_config::MQTT_QUEUE_DIR), "");

        let kafka = cfg
            .get_value(NOTIFY_KAFKA_SUB_SYS, "streaming")
            .expect("kafka target should be decoded");
        assert_eq!(kafka.get(rustfs_config::KAFKA_BROKERS), "127.0.0.1:9092,127.0.0.1:9093");
        assert_eq!(kafka.get(rustfs_config::KAFKA_TOPIC), "events-kafka");
        assert_eq!(kafka.get(rustfs_config::KAFKA_ACKS), "all");
        assert_eq!(kafka.get(rustfs_config::KAFKA_TLS_ENABLE), "true");

        let amqp = cfg
            .get_value(NOTIFY_AMQP_SUB_SYS, "primary")
            .expect("amqp target should be decoded");
        assert_eq!(amqp.get(ENABLE_KEY), EnableState::On.to_string());
        assert_eq!(amqp.get(rustfs_config::AMQP_URL), "amqp://127.0.0.1:5672/%2f");
        assert_eq!(amqp.get(rustfs_config::AMQP_EXCHANGE), "rustfs.events");
        assert_eq!(amqp.get(rustfs_config::AMQP_ROUTING_KEY), "objects");
        assert_eq!(amqp.get(rustfs_config::AMQP_PERSISTENT), "true");

        let mysql = cfg
            .get_value(NOTIFY_MYSQL_SUB_SYS, "primary")
            .expect("mysql target should be decoded");
        assert_eq!(mysql.get(ENABLE_KEY), EnableState::On.to_string());
        assert_eq!(mysql.get(MYSQL_DSN_STRING), "rustfs:password@tcp(127.0.0.1:3306)/rustfs_events");
        assert_eq!(mysql.get(MYSQL_TABLE), "rustfs_events");
        assert_eq!(mysql.get(MYSQL_QUEUE_DIR), "/tmp/mysql-queue");
        assert_eq!(mysql.get(MYSQL_MAX_OPEN_CONNECTIONS), "2");
    }

    #[test]
    fn test_decode_server_config_reads_notify_shorthand_default() {
        let input = r#"{
          "version":"33",
          "storageclass":{"standard":"EC:2","rrs":"EC:1"},
          "notify":{
            "webhook":{
              "enable":true,
              "endpoint":"https://example.com/shorthand"
            }
          }
        }"#;

        let cfg = decode_server_config_blob(input.as_bytes()).expect("decode should succeed");
        let webhook_default = cfg
            .get_value(NOTIFY_WEBHOOK_SUB_SYS, DEFAULT_DELIMITER)
            .expect("default webhook config should be decoded");
        assert_eq!(webhook_default.get(ENABLE_KEY), EnableState::On.to_string());
        assert_eq!(webhook_default.get(rustfs_config::WEBHOOK_ENDPOINT), "https://example.com/shorthand");
    }

    #[test]
    fn test_decode_server_config_keeps_instance_named_like_field() {
        let input = r#"{
          "version":"33",
          "storageclass":{"standard":"EC:2","rrs":"EC:1"},
          "notify":{
            "webhook":{
              "enable":{
                "enable":true,
                "endpoint":"https://example.com/instance-enable"
              }
            }
          }
        }"#;

        let cfg = decode_server_config_blob(input.as_bytes()).expect("decode should succeed");
        let named = cfg
            .get_value(NOTIFY_WEBHOOK_SUB_SYS, "enable")
            .expect("instance named 'enable' should be decoded");
        assert_eq!(named.get(ENABLE_KEY), EnableState::On.to_string());
        assert_eq!(named.get(rustfs_config::WEBHOOK_ENDPOINT), "https://example.com/instance-enable");
    }

    #[test]
    fn test_decode_server_config_reads_audit_targets() {
        let input = r#"{
          "version":"33",
          "storageclass":{"standard":"EC:2","rrs":"EC:1"},
          "logger":{
            "webhook":{
              "primary":{
                "enable":true,
                "endpoint":"https://example.com/audit-hook",
                "queue_dir":"/tmp/audit-queue"
              }
            },
            "mqtt":{
              "default":{
                "enable":true,
                "topic":"audit-events"
              },
              "analytics":{
                "enable":true,
                "broker":"tcp://127.0.0.1:1883",
                "topic":"audit-events"
              }
            },
            "kafka":{
              "auditlog":{
                "enable":true,
                "brokers":"127.0.0.1:9092",
                "topic":"audit-events-kafka",
                "acks":"1"
              }
            }
          }
        }"#;

        let cfg = decode_server_config_blob(input.as_bytes()).expect("decode should succeed");

        let webhook = cfg
            .get_value(AUDIT_WEBHOOK_SUB_SYS, "primary")
            .expect("audit webhook target should be decoded");
        assert_eq!(webhook.get(ENABLE_KEY), EnableState::On.to_string());
        assert_eq!(webhook.get(rustfs_config::WEBHOOK_ENDPOINT), "https://example.com/audit-hook");
        assert_eq!(webhook.get(rustfs_config::WEBHOOK_QUEUE_DIR), "/tmp/audit-queue");

        let mqtt_default = cfg
            .get_value(AUDIT_MQTT_SUB_SYS, DEFAULT_DELIMITER)
            .expect("audit mqtt default should be decoded");
        assert_eq!(mqtt_default.get(ENABLE_KEY), EnableState::On.to_string());
        assert_eq!(mqtt_default.get(rustfs_config::MQTT_TOPIC), "audit-events");

        let mqtt = cfg
            .get_value(AUDIT_MQTT_SUB_SYS, "analytics")
            .expect("audit mqtt target should be decoded");
        assert_eq!(mqtt.get(rustfs_config::MQTT_BROKER), "tcp://127.0.0.1:1883");

        let kafka = cfg
            .get_value(AUDIT_KAFKA_SUB_SYS, "auditlog")
            .expect("audit kafka target should be decoded");
        assert_eq!(kafka.get(rustfs_config::KAFKA_BROKERS), "127.0.0.1:9092");
        assert_eq!(kafka.get(rustfs_config::KAFKA_TOPIC), "audit-events-kafka");
    }

    #[test]
    fn test_encode_server_config_writes_external_object_shape() {
        let mut cfg = Config::new();
        let kvs = storage_class_kvs_mut(&mut cfg);
        kvs.insert("standard".to_string(), "EC:2".to_string());
        kvs.insert("rrs".to_string(), "EC:1".to_string());

        let out = encode_server_config_blob(&cfg, None).expect("encode should succeed");
        let v: Value = serde_json::from_slice(&out).expect("output should be json");
        assert!(v.get("version").is_some(), "external object should have version");
        assert!(v.get("storageclass").is_some(), "external object should have storageclass");
        assert!(v.get("storage_class").is_none(), "should not write rustfs map shape");
    }

    #[test]
    fn test_encode_server_config_writes_openid_object_shape() {
        let mut cfg = Config::new();
        let mut oidc_section = std::collections::HashMap::new();
        let mut default_provider = oidc::DEFAULT_IDENTITY_OPENID_KVS.clone();
        default_provider.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());
        default_provider.insert(
            rustfs_config::oidc::OIDC_CONFIG_URL.to_string(),
            "https://example.com/.well-known/openid-configuration".to_string(),
        );
        default_provider.insert(rustfs_config::oidc::OIDC_CLIENT_ID.to_string(), "console".to_string());
        default_provider.insert(rustfs_config::oidc::OIDC_SCOPES.to_string(), "openid,profile,email".to_string());
        default_provider.insert(rustfs_config::oidc::OIDC_OTHER_AUDIENCES.to_string(), "aud1,aud2".to_string());
        oidc_section.insert(DEFAULT_DELIMITER.to_string(), default_provider);
        cfg.0.insert(IDENTITY_OPENID_SUB_SYS.to_string(), oidc_section);

        let out = encode_server_config_blob(&cfg, None).expect("encode should succeed");
        let v: Value = serde_json::from_slice(&out).expect("output should be json");
        let openid = v
            .get("openid")
            .and_then(Value::as_object)
            .expect("output should include openid object");
        let default_provider = openid
            .get("default")
            .and_then(Value::as_object)
            .expect("default provider should be encoded");

        assert_eq!(
            default_provider
                .get(rustfs_config::oidc::OIDC_CLIENT_ID)
                .and_then(Value::as_str),
            Some("console")
        );
        assert_eq!(
            default_provider
                .get(rustfs_config::oidc::OIDC_SCOPES)
                .and_then(Value::as_array)
                .map(|values| values.iter().filter_map(Value::as_str).collect::<Vec<_>>()),
            Some(vec!["openid", "profile", "email"])
        );
        assert_eq!(
            default_provider
                .get(rustfs_config::oidc::OIDC_OTHER_AUDIENCES)
                .and_then(Value::as_array)
                .map(|values| values.iter().filter_map(Value::as_str).collect::<Vec<_>>()),
            Some(vec!["aud1", "aud2"])
        );
        assert_eq!(default_provider.get(ENABLE_KEY).and_then(Value::as_bool), Some(true));
    }

    #[test]
    fn test_encode_server_config_writes_notify_object_shape() {
        let mut cfg = Config::new();
        let mut webhook_section = std::collections::HashMap::new();
        webhook_section.insert(DEFAULT_DELIMITER.to_string(), notify::DEFAULT_NOTIFY_WEBHOOK_KVS.clone());
        webhook_section.insert(
            "primary".to_string(),
            crate::config::KVS(vec![
                crate::config::KV {
                    key: ENABLE_KEY.to_string(),
                    value: EnableState::On.to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::WEBHOOK_ENDPOINT.to_string(),
                    value: "https://example.com/hook".to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::WEBHOOK_QUEUE_DIR.to_string(),
                    value: "/tmp/webhook-queue".to_string(),
                    hidden_if_empty: false,
                },
            ]),
        );
        cfg.0.insert(NOTIFY_WEBHOOK_SUB_SYS.to_string(), webhook_section);

        let mut mqtt_default = notify::DEFAULT_NOTIFY_MQTT_KVS.clone();
        mqtt_default.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());
        mqtt_default.insert(rustfs_config::MQTT_TOPIC.to_string(), "events".to_string());
        let mut mqtt_section = std::collections::HashMap::new();
        mqtt_section.insert(DEFAULT_DELIMITER.to_string(), mqtt_default);
        mqtt_section.insert(
            "analytics".to_string(),
            crate::config::KVS(vec![
                crate::config::KV {
                    key: ENABLE_KEY.to_string(),
                    value: EnableState::On.to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::MQTT_BROKER.to_string(),
                    value: "tcp://127.0.0.1:1883".to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::MQTT_QUEUE_DIR.to_string(),
                    value: "".to_string(),
                    hidden_if_empty: false,
                },
            ]),
        );
        cfg.0.insert(NOTIFY_MQTT_SUB_SYS.to_string(), mqtt_section);

        let mut kafka_default = notify::DEFAULT_NOTIFY_KAFKA_KVS.clone();
        kafka_default.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());
        kafka_default.insert(rustfs_config::KAFKA_TOPIC.to_string(), "events-kafka".to_string());
        let mut kafka_section = std::collections::HashMap::new();
        kafka_section.insert(DEFAULT_DELIMITER.to_string(), kafka_default);
        kafka_section.insert(
            "streaming".to_string(),
            crate::config::KVS(vec![
                crate::config::KV {
                    key: ENABLE_KEY.to_string(),
                    value: EnableState::On.to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::KAFKA_BROKERS.to_string(),
                    value: "127.0.0.1:9092,127.0.0.1:9093".to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::KAFKA_ACKS.to_string(),
                    value: "all".to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::KAFKA_TLS_ENABLE.to_string(),
                    value: EnableState::On.to_string(),
                    hidden_if_empty: false,
                },
            ]),
        );
        cfg.0.insert(NOTIFY_KAFKA_SUB_SYS.to_string(), kafka_section);

        let mut amqp_section = std::collections::HashMap::new();
        amqp_section.insert(
            "primary".to_string(),
            crate::config::KVS(vec![
                crate::config::KV {
                    key: ENABLE_KEY.to_string(),
                    value: EnableState::On.to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::AMQP_URL.to_string(),
                    value: "amqp://127.0.0.1:5672/%2f".to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::AMQP_EXCHANGE.to_string(),
                    value: "rustfs.events".to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::AMQP_ROUTING_KEY.to_string(),
                    value: "objects".to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::AMQP_MANDATORY.to_string(),
                    value: EnableState::Off.to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::AMQP_PERSISTENT.to_string(),
                    value: "false".to_string(),
                    hidden_if_empty: false,
                },
            ]),
        );
        cfg.0.insert(NOTIFY_AMQP_SUB_SYS.to_string(), amqp_section);

        let out = encode_server_config_blob(&cfg, None).expect("encode should succeed");
        let v: Value = serde_json::from_slice(&out).expect("output should be json");
        let notify = v
            .get("notify")
            .and_then(Value::as_object)
            .expect("notify object should be present");
        let webhook = notify
            .get("webhook")
            .and_then(Value::as_object)
            .and_then(|targets| targets.get("primary"))
            .and_then(Value::as_object)
            .expect("webhook target should be encoded");
        assert_eq!(
            webhook.get(rustfs_config::WEBHOOK_ENDPOINT).and_then(Value::as_str),
            Some("https://example.com/hook")
        );
        assert_eq!(webhook.get(ENABLE_KEY).and_then(Value::as_bool), Some(true));

        let mqtt_default = notify
            .get("mqtt")
            .and_then(Value::as_object)
            .and_then(|targets| targets.get("default"))
            .and_then(Value::as_object)
            .expect("mqtt default should be encoded");
        assert_eq!(mqtt_default.get(ENABLE_KEY).and_then(Value::as_bool), Some(true));
        assert_eq!(mqtt_default.get(rustfs_config::MQTT_TOPIC).and_then(Value::as_str), Some("events"));

        let mqtt = notify
            .get("mqtt")
            .and_then(Value::as_object)
            .and_then(|targets| targets.get("analytics"))
            .and_then(Value::as_object)
            .expect("mqtt target should be encoded");
        assert_eq!(mqtt.get(rustfs_config::MQTT_BROKER).and_then(Value::as_str), Some("tcp://127.0.0.1:1883"));
        assert_eq!(mqtt.get(rustfs_config::MQTT_QUEUE_DIR).and_then(Value::as_str), Some(""));

        let kafka = notify
            .get("kafka")
            .and_then(Value::as_object)
            .and_then(|targets| targets.get("streaming"))
            .and_then(Value::as_object)
            .expect("kafka target should be encoded");
        assert_eq!(
            kafka.get(rustfs_config::KAFKA_BROKERS).and_then(Value::as_str),
            Some("127.0.0.1:9092,127.0.0.1:9093")
        );
        assert_eq!(kafka.get(rustfs_config::KAFKA_ACKS).and_then(Value::as_str), Some("all"));
        assert_eq!(kafka.get(rustfs_config::KAFKA_TLS_ENABLE).and_then(Value::as_bool), Some(true));

        let amqp = notify
            .get("amqp")
            .and_then(Value::as_object)
            .and_then(|targets| targets.get("primary"))
            .and_then(Value::as_object)
            .expect("amqp target should be encoded");
        assert_eq!(
            amqp.get(rustfs_config::AMQP_URL).and_then(Value::as_str),
            Some("amqp://127.0.0.1:5672/%2f")
        );
        assert_eq!(amqp.get(rustfs_config::AMQP_EXCHANGE).and_then(Value::as_str), Some("rustfs.events"));
        assert_eq!(amqp.get(rustfs_config::AMQP_ROUTING_KEY).and_then(Value::as_str), Some("objects"));
        assert_eq!(amqp.get(rustfs_config::AMQP_MANDATORY).and_then(Value::as_str), Some("off"));
        assert_eq!(amqp.get(rustfs_config::AMQP_PERSISTENT).and_then(Value::as_str), Some("false"));
    }

    #[test]
    fn test_encode_server_config_writes_audit_object_shape() {
        let mut cfg = Config::new();
        let mut webhook_section = std::collections::HashMap::new();
        webhook_section.insert(DEFAULT_DELIMITER.to_string(), audit::DEFAULT_AUDIT_WEBHOOK_KVS.clone());
        webhook_section.insert(
            "primary".to_string(),
            crate::config::KVS(vec![
                crate::config::KV {
                    key: ENABLE_KEY.to_string(),
                    value: EnableState::On.to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::WEBHOOK_ENDPOINT.to_string(),
                    value: "https://example.com/audit-hook".to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::WEBHOOK_QUEUE_DIR.to_string(),
                    value: "/tmp/audit-queue".to_string(),
                    hidden_if_empty: false,
                },
            ]),
        );
        cfg.0.insert(AUDIT_WEBHOOK_SUB_SYS.to_string(), webhook_section);

        let mut mqtt_default = audit::DEFAULT_AUDIT_MQTT_KVS.clone();
        mqtt_default.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());
        mqtt_default.insert(rustfs_config::MQTT_TOPIC.to_string(), "audit-events".to_string());
        let mut mqtt_section = std::collections::HashMap::new();
        mqtt_section.insert(DEFAULT_DELIMITER.to_string(), mqtt_default);
        mqtt_section.insert(
            "analytics".to_string(),
            crate::config::KVS(vec![
                crate::config::KV {
                    key: ENABLE_KEY.to_string(),
                    value: EnableState::On.to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::MQTT_BROKER.to_string(),
                    value: "tcp://127.0.0.1:1883".to_string(),
                    hidden_if_empty: false,
                },
            ]),
        );
        cfg.0.insert(AUDIT_MQTT_SUB_SYS.to_string(), mqtt_section);

        let mut kafka_default = audit::DEFAULT_AUDIT_KAFKA_KVS.clone();
        kafka_default.insert(ENABLE_KEY.to_string(), EnableState::On.to_string());
        kafka_default.insert(rustfs_config::KAFKA_TOPIC.to_string(), "audit-events-kafka".to_string());
        let mut kafka_section = std::collections::HashMap::new();
        kafka_section.insert(DEFAULT_DELIMITER.to_string(), kafka_default);
        kafka_section.insert(
            "auditlog".to_string(),
            crate::config::KVS(vec![
                crate::config::KV {
                    key: ENABLE_KEY.to_string(),
                    value: EnableState::On.to_string(),
                    hidden_if_empty: false,
                },
                crate::config::KV {
                    key: rustfs_config::KAFKA_BROKERS.to_string(),
                    value: "127.0.0.1:9092".to_string(),
                    hidden_if_empty: false,
                },
            ]),
        );
        cfg.0.insert(AUDIT_KAFKA_SUB_SYS.to_string(), kafka_section);

        let out = encode_server_config_blob(&cfg, None).expect("encode should succeed");
        let v: Value = serde_json::from_slice(&out).expect("output should be json");
        let logger = v
            .get("logger")
            .and_then(Value::as_object)
            .expect("logger object should be present");
        let webhook = logger
            .get("webhook")
            .and_then(Value::as_object)
            .and_then(|targets| targets.get("primary"))
            .and_then(Value::as_object)
            .expect("audit webhook target should be encoded");
        assert_eq!(
            webhook.get(rustfs_config::WEBHOOK_ENDPOINT).and_then(Value::as_str),
            Some("https://example.com/audit-hook")
        );
        assert_eq!(webhook.get(ENABLE_KEY).and_then(Value::as_bool), Some(true));

        let mqtt_default = logger
            .get("mqtt")
            .and_then(Value::as_object)
            .and_then(|targets| targets.get("default"))
            .and_then(Value::as_object)
            .expect("audit mqtt default should be encoded");
        assert_eq!(mqtt_default.get(ENABLE_KEY).and_then(Value::as_bool), Some(true));
        assert_eq!(mqtt_default.get(rustfs_config::MQTT_TOPIC).and_then(Value::as_str), Some("audit-events"));

        let kafka = logger
            .get("kafka")
            .and_then(Value::as_object)
            .and_then(|targets| targets.get("auditlog"))
            .and_then(Value::as_object)
            .expect("audit kafka target should be encoded");
        assert_eq!(kafka.get(rustfs_config::KAFKA_BROKERS).and_then(Value::as_str), Some("127.0.0.1:9092"));
    }

    #[test]
    fn test_is_standard_object_server_config_detection() {
        let external = br#"{"version":"33","storageclass":{"standard":"EC:2","rrs":"EC:1"}}"#;
        assert!(is_standard_object_server_config(external));

        let legacy = br#"{"storage_class":{"_":[{"key":"standard","value":"EC:2"}]}}"#;
        assert!(!is_standard_object_server_config(legacy));
    }

    #[test]
    fn test_configs_semantically_equal_for_equivalent_shapes() {
        let external = br#"{"version":"33","storageclass":{"standard":"EC:2","rrs":"EC:1","optimize":"availability"}}"#;
        let legacy = br#"{"storage_class":{"_":[{"key":"standard","value":"EC:2"},{"key":"rrs","value":"EC:1"},{"key":"optimize","value":"availability"}]}}"#;
        let lhs = decode_server_config_blob(external).expect("decode external");
        let rhs = decode_server_config_blob(legacy).expect("decode legacy");
        assert!(configs_semantically_equal(&lhs, &rhs));
    }

    #[test]
    fn test_configs_semantically_equal_accounts_for_openid() {
        let external = br#"{
          "version":"33",
          "storageclass":{"standard":"EC:2","rrs":"EC:1","optimize":"availability"},
          "openid":{
            "default":{
              "enable":true,
              "config_url":"https://example.com/.well-known/openid-configuration",
              "client_id":"console",
              "scopes":["openid","profile","email"],
              "redirect_uri_dynamic":true
            }
          }
        }"#;
        let legacy = br#"{
          "storage_class":{"_":[
            {"key":"standard","value":"EC:2"},
            {"key":"rrs","value":"EC:1"},
            {"key":"optimize","value":"availability"}
          ]},
          "identity_openid":{"_":[
            {"key":"enable","value":"on"},
            {"key":"config_url","value":"https://example.com/.well-known/openid-configuration"},
            {"key":"client_id","value":"console"},
            {"key":"scopes","value":"openid,profile,email"},
            {"key":"redirect_uri_dynamic","value":"on"}
          ]}
        }"#;

        let lhs = decode_server_config_blob(external).expect("decode external");
        let rhs = decode_server_config_blob(legacy).expect("decode legacy");
        assert!(configs_semantically_equal(&lhs, &rhs));
    }

    #[test]
    fn test_configs_semantically_equal_accounts_for_notify() {
        let external = br#"{
          "version":"33",
          "storageclass":{"standard":"EC:2","rrs":"EC:1","optimize":"availability"},
          "notify":{
            "webhook":{
              "primary":{
                "enable":true,
                "endpoint":"https://example.com/hook"
              }
            }
          }
        }"#;
        let legacy = br#"{
          "storage_class":{"_":[
            {"key":"standard","value":"EC:2"},
            {"key":"rrs","value":"EC:1"},
            {"key":"optimize","value":"availability"}
          ]},
          "notify_webhook":{
            "_":[
              {"key":"enable","value":"off"},
              {"key":"endpoint","value":""},
              {"key":"queue_limit","value":"100000"},
              {"key":"queue_dir","value":"/opt/rustfs/events"},
              {"key":"client_cert","value":""},
              {"key":"client_key","value":""},
              {"key":"comment","value":""},
              {"key":"client_ca","value":""},
              {"key":"skip_tls_verify","value":"off"}
            ],
            "primary":[
              {"key":"enable","value":"on"},
              {"key":"endpoint","value":"https://example.com/hook"}
            ]
          }
        }"#;

        let lhs = decode_server_config_blob(external).expect("decode external");
        let rhs = decode_server_config_blob(legacy).expect("decode legacy");
        assert!(configs_semantically_equal(&lhs, &rhs));
    }

    #[test]
    fn test_configs_semantically_equal_detects_notify_changes() {
        let lhs = decode_server_config_blob(
            br#"{"version":"33","storageclass":{"standard":"EC:2","rrs":"EC:1"},"notify":{"webhook":{"primary":{"enable":true,"endpoint":"https://example.com/a"}}}}"#,
        )
        .expect("decode lhs");
        let rhs = decode_server_config_blob(
            br#"{"version":"33","storageclass":{"standard":"EC:2","rrs":"EC:1"},"notify":{"webhook":{"primary":{"enable":true,"endpoint":"https://example.com/b"}}}}"#,
        )
        .expect("decode rhs");

        assert!(!configs_semantically_equal(&lhs, &rhs));
    }

    #[test]
    fn test_configs_semantically_equal_accounts_for_audit() {
        let external = br#"{
          "version":"33",
          "storageclass":{"standard":"EC:2","rrs":"EC:1","optimize":"availability"},
          "logger":{
            "webhook":{
              "primary":{
                "enable":true,
                "endpoint":"https://example.com/audit-hook"
              }
            }
          }
        }"#;
        let legacy = br#"{
          "storage_class":{"_":[
            {"key":"standard","value":"EC:2"},
            {"key":"rrs","value":"EC:1"},
            {"key":"optimize","value":"availability"}
          ]},
          "audit_webhook":{
            "_":[
              {"key":"enable","value":"off"},
              {"key":"endpoint","value":""},
              {"key":"auth_token","value":""},
              {"key":"client_cert","value":""},
              {"key":"client_key","value":""},
              {"key":"client_ca","value":""},
              {"key":"skip_tls_verify","value":"off"},
              {"key":"batch_size","value":"1"},
              {"key":"queue_limit","value":"100000"},
              {"key":"queue_dir","value":"/opt/rustfs/events"},
              {"key":"max_retry","value":"0"},
              {"key":"retry_interval","value":"3s"},
              {"key":"http_timeout","value":"5s"},
              {"key":"comment","value":""}
            ],
            "primary":[
              {"key":"enable","value":"on"},
              {"key":"endpoint","value":"https://example.com/audit-hook"}
            ]
          }
        }"#;

        let lhs = decode_server_config_blob(external).expect("decode external");
        let rhs = decode_server_config_blob(legacy).expect("decode legacy");
        assert!(configs_semantically_equal(&lhs, &rhs));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_read_config_with_metadata_succeeds_with_one_healthy_locker_in_two_node_dist_setup() {
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;

        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let healthy_client: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager));
        let failing_client: Arc<dyn LockClient> = Arc::new(FailingClient);
        let storage = Arc::new(LockingConfigStorage::new(vec![healthy_client, failing_client], br#"{"ok":true}"#.to_vec()).await);

        let (data, object_info) = read_config_with_metadata(storage, "config/test.json", &ObjectOptions::default())
            .await
            .expect("config read should succeed with one healthy locker");

        assert_eq!(data, br#"{"ok":true}"#.to_vec());
        assert_eq!(object_info.bucket, crate::disk::RUSTFS_META_BUCKET);
        assert_eq!(object_info.name, "config/test.json");
    }
}
