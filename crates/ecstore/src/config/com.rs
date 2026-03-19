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

use crate::config::{Config, GLOBAL_STORAGE_CLASS, storageclass};
use crate::disk::{MIGRATING_META_BUCKET, RUSTFS_META_BUCKET};
use crate::error::{Error, Result};
use crate::store_api::{ObjectInfo, ObjectOptions, PutObjReader, StorageAPI};
use http::HeaderMap;
use rustfs_config::{DEFAULT_DELIMITER, RUSTFS_REGION};
use rustfs_utils::path::SLASH_SEPARATOR;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::LazyLock;
use tracing::{debug, error, info, instrument, warn};

pub const CONFIG_PREFIX: &str = "config";
const CONFIG_FILE: &str = "config.json";

pub const STORAGE_CLASS_SUB_SYS: &str = "storage_class";

static CONFIG_BUCKET: LazyLock<String> = LazyLock::new(|| format!("{RUSTFS_META_BUCKET}{SLASH_SEPARATOR}{CONFIG_PREFIX}"));

static SUB_SYSTEMS_DYNAMIC: LazyLock<HashSet<String>> = LazyLock::new(|| {
    let mut h = HashSet::new();
    h.insert(STORAGE_CLASS_SUB_SYS.to_owned());
    h
});

#[instrument(skip(api))]
pub async fn read_config<S: StorageAPI>(api: Arc<S>, file: &str) -> Result<Vec<u8>> {
    let (data, _obj) = read_config_with_metadata(api, file, &ObjectOptions::default()).await?;
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
    if let Err(err) = api
        .put_object(RUSTFS_META_BUCKET, file, &mut PutObjReader::from_vec(data), opts)
        .await
    {
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
    let has_header = root.contains_key("version") || root.contains_key("region") || root.contains_key("credential");
    if !has_storage && !has_header {
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
}

fn is_object_not_found(err: &Error) -> bool {
    *err == Error::FileNotFound || matches!(err, Error::ObjectNotFound(_, _) | Error::BucketNotFound(_))
}

pub async fn try_migrate_server_config<S: StorageAPI>(api: Arc<S>) {
    let config_file = get_config_file();
    match api
        .get_object_info(RUSTFS_META_BUCKET, &config_file, &ObjectOptions::default())
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
    let cfg = new_and_save_server_config(api).await?;
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
    match read_config(api.clone(), &config_file).await {
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
        match read_config(api.clone(), &config_file).await {
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
        storage_class_kvs_mut,
    };
    use crate::config::Config;
    use serde_json::Value;

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
}
