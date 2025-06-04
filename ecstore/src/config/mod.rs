pub mod com;
#[allow(dead_code)]
pub mod heal;
pub mod storageclass;

use crate::error::Result;
use crate::store::ECStore;
use com::{lookup_configs, read_config_without_migrate, STORAGE_CLASS_SUB_SYS};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

lazy_static! {
    pub static ref GLOBAL_StorageClass: OnceLock<storageclass::Config> = OnceLock::new();
    pub static ref DefaultKVS: OnceLock<HashMap<String, KVS>> = OnceLock::new();
    pub static ref GLOBAL_ServerConfig: OnceLock<Config> = OnceLock::new();
    pub static ref GLOBAL_ConfigSys: ConfigSys = ConfigSys::new();
}

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
pub struct KVS(Vec<KV>);

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
        if let Some(v) = self.lookup(key) {
            v
        } else {
            "".to_owned()
        }
    }
    pub fn lookup(&self, key: &str) -> Option<String> {
        for kv in self.0.iter() {
            if kv.key.as_str() == key {
                return Some(kv.value.clone());
            }
        }

        None
    }
}

#[derive(Debug, Clone)]
pub struct Config(HashMap<String, HashMap<String, KVS>>);

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

    pub fn get_value(&self, subsys: &str, key: &str) -> Option<KVS> {
        if let Some(m) = self.0.get(subsys) {
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
                    default.insert("_".to_owned(), v.clone());
                    self.0.insert(k.clone(), default);
                } else if !self.0[k].contains_key("_") {
                    if let Some(m) = self.0.get_mut(k) {
                        m.insert("_".to_owned(), v.clone());
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
    kvs.insert(STORAGE_CLASS_SUB_SYS.to_owned(), storageclass::DefaultKVS.clone());
    // TODO: other default
    register_default_kvs(kvs)
}
