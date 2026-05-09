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

mod audit;
pub mod com;
#[allow(dead_code)]
pub mod heal;
mod notify;
mod oidc;
pub mod storageclass;

use crate::error::Result;
use crate::store::ECStore;
use com::{STORAGE_CLASS_SUB_SYS, lookup_configs, read_config_without_migrate};
use rustfs_config::COMMENT_KEY;
use rustfs_config::DEFAULT_DELIMITER;
use rustfs_config::audit::{
    AUDIT_AMQP_SUB_SYS, AUDIT_KAFKA_SUB_SYS, AUDIT_MQTT_SUB_SYS, AUDIT_MYSQL_SUB_SYS, AUDIT_NATS_SUB_SYS, AUDIT_POSTGRES_SUB_SYS,
    AUDIT_PULSAR_SUB_SYS, AUDIT_REDIS_SUB_SYS, AUDIT_WEBHOOK_SUB_SYS,
};
use rustfs_config::notify::{
    NOTIFY_AMQP_SUB_SYS, NOTIFY_KAFKA_SUB_SYS, NOTIFY_MQTT_SUB_SYS, NOTIFY_MYSQL_SUB_SYS, NOTIFY_NATS_SUB_SYS,
    NOTIFY_POSTGRES_SUB_SYS, NOTIFY_PULSAR_SUB_SYS, NOTIFY_REDIS_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS,
};
use rustfs_config::oidc::IDENTITY_OPENID_SUB_SYS;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::{Arc, OnceLock};

pub static GLOBAL_STORAGE_CLASS: LazyLock<OnceLock<storageclass::Config>> = LazyLock::new(OnceLock::new);
pub static DEFAULT_KVS: LazyLock<OnceLock<HashMap<String, KVS>>> = LazyLock::new(OnceLock::new);
pub static GLOBAL_SERVER_CONFIG: LazyLock<OnceLock<Config>> = LazyLock::new(OnceLock::new);
pub static GLOBAL_CONFIG_SYS: LazyLock<ConfigSys> = LazyLock::new(ConfigSys::new);

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

        let _ = GLOBAL_SERVER_CONFIG.set(cfg);

        Ok(())
    }
}

pub fn get_global_server_config() -> Option<Config> {
    GLOBAL_SERVER_CONFIG.get().cloned()
}

pub async fn init_global_config_sys(api: Arc<ECStore>) -> Result<()> {
    GLOBAL_CONFIG_SYS.init(api).await
}

pub async fn try_migrate_server_config(api: Arc<ECStore>) {
    com::try_migrate_server_config(api).await
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct KV {
    pub key: String,
    pub value: String,
    #[serde(default, alias = "hiddenIfEmpty")]
    pub hidden_if_empty: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
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

    /// Insert or update a pair of key/values in KVS
    pub fn insert(&mut self, key: String, value: String) {
        for kv in self.0.iter_mut() {
            if kv.key == key {
                kv.value = value;
                return;
            }
        }
        self.0.push(KV {
            key,
            value,
            hidden_if_empty: false,
        });
    }

    /// Merge all entries from another KVS to the current instance
    pub fn extend(&mut self, other: KVS) {
        for KV { key, value, .. } in other.0.into_iter() {
            self.insert(key, value);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
        if let Some(defaults) = DEFAULT_KVS.get() {
            for (k, v) in defaults.iter() {
                if !self.0.contains_key(k) {
                    let mut default = HashMap::new();
                    default.insert(DEFAULT_DELIMITER.to_owned(), v.clone());
                    self.0.insert(k.clone(), default);
                } else if !self.0[k].contains_key(DEFAULT_DELIMITER)
                    && let Some(m) = self.0.get_mut(k)
                {
                    m.insert(DEFAULT_DELIMITER.to_owned(), v.clone());
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

    let _ = DEFAULT_KVS.set(p);
}

pub fn init() {
    let mut kvs = HashMap::new();
    // Load storageclass default configuration
    kvs.insert(STORAGE_CLASS_SUB_SYS.to_owned(), storageclass::DEFAULT_KVS.clone());
    // New: Loading default configurations for notify_webhook and notify_mqtt
    // Referring subsystem names through constants to improve the readability and maintainability of the code
    kvs.insert(NOTIFY_WEBHOOK_SUB_SYS.to_owned(), notify::DEFAULT_NOTIFY_WEBHOOK_KVS.clone());
    kvs.insert(AUDIT_WEBHOOK_SUB_SYS.to_owned(), audit::DEFAULT_AUDIT_WEBHOOK_KVS.clone());
    kvs.insert(NOTIFY_MQTT_SUB_SYS.to_owned(), notify::DEFAULT_NOTIFY_MQTT_KVS.clone());
    kvs.insert(AUDIT_MQTT_SUB_SYS.to_owned(), audit::DEFAULT_AUDIT_MQTT_KVS.clone());
    kvs.insert(NOTIFY_AMQP_SUB_SYS.to_owned(), notify::DEFAULT_NOTIFY_AMQP_KVS.clone());
    kvs.insert(AUDIT_AMQP_SUB_SYS.to_owned(), audit::DEFAULT_AUDIT_AMQP_KVS.clone());
    kvs.insert(NOTIFY_NATS_SUB_SYS.to_owned(), notify::DEFAULT_NOTIFY_NATS_KVS.clone());
    kvs.insert(AUDIT_NATS_SUB_SYS.to_owned(), audit::DEFAULT_AUDIT_NATS_KVS.clone());
    kvs.insert(NOTIFY_REDIS_SUB_SYS.to_owned(), notify::DEFAULT_NOTIFY_REDIS_KVS.clone());
    kvs.insert(AUDIT_REDIS_SUB_SYS.to_owned(), audit::DEFAULT_AUDIT_REDIS_KVS.clone());
    kvs.insert(NOTIFY_POSTGRES_SUB_SYS.to_owned(), notify::DEFAULT_NOTIFY_POSTGRES_KVS.clone());
    kvs.insert(AUDIT_POSTGRES_SUB_SYS.to_owned(), audit::DEFAULT_AUDIT_POSTGRES_KVS.clone());
    kvs.insert(NOTIFY_PULSAR_SUB_SYS.to_owned(), notify::DEFAULT_NOTIFY_PULSAR_KVS.clone());
    kvs.insert(AUDIT_PULSAR_SUB_SYS.to_owned(), audit::DEFAULT_AUDIT_PULSAR_KVS.clone());
    kvs.insert(NOTIFY_KAFKA_SUB_SYS.to_owned(), notify::DEFAULT_NOTIFY_KAFKA_KVS.clone());
    kvs.insert(AUDIT_KAFKA_SUB_SYS.to_owned(), audit::DEFAULT_AUDIT_KAFKA_KVS.clone());
    kvs.insert(NOTIFY_MYSQL_SUB_SYS.to_owned(), notify::DEFAULT_NOTIFY_MYSQL_KVS.clone());
    kvs.insert(AUDIT_MYSQL_SUB_SYS.to_owned(), audit::DEFAULT_AUDIT_MYSQL_KVS.clone());
    kvs.insert(IDENTITY_OPENID_SUB_SYS.to_owned(), oidc::DEFAULT_IDENTITY_OPENID_KVS.clone());

    // Register all default configurations
    register_default_kvs(kvs)
}
