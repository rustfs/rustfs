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

pub mod com;
#[allow(dead_code)]
pub mod heal;
mod notify;
pub mod storageclass;

use crate::error::Result;
use crate::store::ECStore;
use com::{STORAGE_CLASS_SUB_SYS, lookup_configs, read_config_without_migrate};
use lazy_static::lazy_static;
use rustfs_config::DEFAULT_DELIMITER;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

lazy_static! {
    pub static ref GLOBAL_StorageClass: OnceLock<storageclass::Config> = OnceLock::new();
    pub static ref DefaultKVS: OnceLock<HashMap<String, KVS>> = OnceLock::new();
    pub static ref GLOBAL_ServerConfig: OnceLock<Config> = OnceLock::new();
    pub static ref GLOBAL_ConfigSys: ConfigSys = ConfigSys::new();
}

/// Standard config keys and values.
pub const ENABLE_KEY: &str = "enable";
pub const COMMENT_KEY: &str = "comment";

/// Enable values
pub const ENABLE_ON: &str = "on";
pub const ENABLE_OFF: &str = "off";

pub const ENV_ACCESS_KEY: &str = "RUSTFS_ACCESS_KEY";
pub const ENV_SECRET_KEY: &str = "RUSTFS_SECRET_KEY";
pub const ENV_ROOT_USER: &str = "RUSTFS_ROOT_USER";
pub const ENV_ROOT_PASSWORD: &str = "RUSTFS_ROOT_PASSWORD";

pub static RUSTFS_CONFIG_PREFIX: &str = "config";

pub struct ConfigSys {}

impl Default for ConfigSys {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigSys {
    pub fn new() -> Self {
        Self {}
    }
    pub async fn init(&self, api: Arc<ECStore>) -> Result<()> {
        let mut cfg = read_config_without_migrate(api.clone().clone()).await?;

        lookup_configs(&mut cfg, api).await;

        let _ = GLOBAL_ServerConfig.set(cfg);

        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct KV {
    pub key: String,
    pub value: String,
    pub hidden_if_empty: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct KVS(pub Vec<KV>);

impl Default for KVS {
    fn default() -> Self {
        Self::new()
    }
}

impl KVS {
    pub fn new() -> Self {
        KVS(Vec::new())
    }
    pub fn get(&self, key: &str) -> String {
        if let Some(v) = self.lookup(key) { v } else { "".to_owned() }
    }
    pub fn lookup(&self, key: &str) -> Option<String> {
        for kv in self.0.iter() {
            if kv.key.as_str() == key {
                return Some(kv.value.clone());
            }
        }

        None
    }

    ///Check if KVS is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns a list of all keys for the current KVS.
    /// If the "comment" key does not exist, it will be added.
    pub fn keys(&self) -> Vec<String> {
        let mut found_comment = false;
        let mut keys: Vec<String> = self
            .0
            .iter()
            .map(|kv| {
                if kv.key == COMMENT_KEY {
                    found_comment = true;
                }
                kv.key.clone()
            })
            .collect();

        if !found_comment {
            keys.push(COMMENT_KEY.to_owned());
        }

        keys
    }
}

#[derive(Debug, Clone)]
pub struct Config(pub HashMap<String, HashMap<String, KVS>>);

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

impl Config {
    pub fn new() -> Self {
        let mut cfg = Config(HashMap::new());
        cfg.set_defaults();

        cfg
    }

    pub fn get_value(&self, sub_sys: &str, key: &str) -> Option<KVS> {
        if let Some(m) = self.0.get(sub_sys) {
            m.get(key).cloned()
        } else {
            None
        }
    }

    pub fn set_defaults(&mut self) {
        if let Some(defaults) = DefaultKVS.get() {
            for (k, v) in defaults.iter() {
                if !self.0.contains_key(k) {
                    let mut default = HashMap::new();
                    default.insert(DEFAULT_DELIMITER.to_owned(), v.clone());
                    self.0.insert(k.clone(), default);
                } else if !self.0[k].contains_key(DEFAULT_DELIMITER) {
                    if let Some(m) = self.0.get_mut(k) {
                        m.insert(DEFAULT_DELIMITER.to_owned(), v.clone());
                    }
                }
            }
        }
    }

    pub fn unmarshal(data: &[u8]) -> Result<Config> {
        let m: HashMap<String, HashMap<String, KVS>> = serde_json::from_slice(data)?;
        let mut cfg = Config(m);
        cfg.set_defaults();
        Ok(cfg)
    }

    pub fn marshal(&self) -> Result<Vec<u8>> {
        let data = serde_json::to_vec(&self.0)?;
        Ok(data)
    }

    pub fn merge(&self) -> Config {
        // TODO: merge default
        self.clone()
    }
}

pub fn register_default_kvs(kvs: HashMap<String, KVS>) {
    let mut p = HashMap::new();
    for (k, v) in kvs {
        p.insert(k, v);
    }

    let _ = DefaultKVS.set(p);
}

pub fn init() {
    let mut kvs = HashMap::new();
    // Load storageclass default configuration
    kvs.insert(STORAGE_CLASS_SUB_SYS.to_owned(), storageclass::DefaultKVS.clone());
    // New: Loading default configurations for notify_webhook and notify_mqtt
    // Referring subsystem names through constants to improve the readability and maintainability of the code
    kvs.insert(
        rustfs_config::notify::NOTIFY_WEBHOOK_SUB_SYS.to_owned(),
        notify::DefaultWebhookKVS.clone(),
    );
    kvs.insert(rustfs_config::notify::NOTIFY_MQTT_SUB_SYS.to_owned(), notify::DefaultMqttKVS.clone());

    // Register all default configurations
    register_default_kvs(kvs)
}
