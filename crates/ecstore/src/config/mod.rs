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
mod scanner;
pub mod storageclass;

use crate::error::Result;
use crate::store::ECStore;
use com::{STORAGE_CLASS_SUB_SYS, lookup_configs, read_config_without_migrate};
use rustfs_config::HEAL_SUB_SYS;
use rustfs_config::audit::{
    AUDIT_AMQP_SUB_SYS, AUDIT_KAFKA_SUB_SYS, AUDIT_MQTT_SUB_SYS, AUDIT_MYSQL_SUB_SYS, AUDIT_NATS_SUB_SYS, AUDIT_POSTGRES_SUB_SYS,
    AUDIT_PULSAR_SUB_SYS, AUDIT_REDIS_SUB_SYS, AUDIT_WEBHOOK_SUB_SYS,
};
use rustfs_config::notify::{
    NOTIFY_AMQP_SUB_SYS, NOTIFY_KAFKA_SUB_SYS, NOTIFY_MQTT_SUB_SYS, NOTIFY_MYSQL_SUB_SYS, NOTIFY_NATS_SUB_SYS,
    NOTIFY_POSTGRES_SUB_SYS, NOTIFY_PULSAR_SUB_SYS, NOTIFY_REDIS_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS,
};
use rustfs_config::oidc::IDENTITY_OPENID_SUB_SYS;
use rustfs_config::server_config::{register_default_kvs, set_global_server_config};
use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::{Arc, RwLock};

pub static GLOBAL_STORAGE_CLASS: LazyLock<RwLock<storageclass::Config>> =
    LazyLock::new(|| RwLock::new(storageclass::Config::default()));
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

        set_global_server_config(cfg);

        Ok(())
    }
}

pub fn get_global_storage_class() -> Option<storageclass::Config> {
    GLOBAL_STORAGE_CLASS.read().ok().map(|guard| (*guard).clone())
}

pub fn set_global_storage_class(cfg: storageclass::Config) {
    if let Ok(mut guard) = GLOBAL_STORAGE_CLASS.write() {
        *guard = cfg;
    }
}

pub async fn init_global_config_sys(api: Arc<ECStore>) -> Result<()> {
    GLOBAL_CONFIG_SYS.init(api).await
}

pub async fn try_migrate_server_config(api: Arc<ECStore>) {
    com::try_migrate_server_config(api).await
}

pub fn init() {
    let mut kvs = HashMap::new();
    // Load storageclass default configuration
    kvs.insert(STORAGE_CLASS_SUB_SYS.to_owned(), storageclass::DEFAULT_KVS.clone());
    kvs.insert(rustfs_config::SCANNER_SUB_SYS.to_owned(), scanner::DEFAULT_KVS.clone());
    kvs.insert(HEAL_SUB_SYS.to_owned(), heal::DEFAULT_KVS.clone());
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

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_config::server_config::{Config, KVS, get_global_server_config, set_global_server_config};
    use rustfs_config::{
        DEFAULT_DELIMITER, DEFAULT_HEAL_BITROT_CYCLE_SECS, DEFAULT_SCANNER_SPEED, HEAL_BITROT_CYCLE, SCANNER_CYCLE_MAX_OBJECTS,
        SCANNER_DELAY, SCANNER_MAX_WAIT, SCANNER_SPEED, SCANNER_SUB_SYS,
    };

    #[test]
    fn global_server_config_set_and_get_roundtrip() {
        init();
        let mut cfg = Config::new();
        let mut kvs = KVS::new();
        kvs.insert("standard".to_string(), "EC:4".to_string());
        cfg.0
            .insert(STORAGE_CLASS_SUB_SYS.to_string(), HashMap::from([("_".to_string(), kvs)]));

        set_global_server_config(cfg.clone());
        let loaded = get_global_server_config().expect("global config should be set");
        let sc_kvs = loaded
            .get_value(STORAGE_CLASS_SUB_SYS, "_")
            .expect("storage_class should exist");
        assert_eq!(sc_kvs.get("standard"), "EC:4");
    }

    #[test]
    fn scanner_defaults_are_registered_for_admin_config() {
        init();
        let cfg = Config::new();
        let scanner_kvs = cfg
            .get_value(SCANNER_SUB_SYS, DEFAULT_DELIMITER)
            .expect("scanner defaults should exist");

        assert_eq!(scanner_kvs.get(SCANNER_SPEED), DEFAULT_SCANNER_SPEED);
        assert_eq!(scanner_kvs.get(SCANNER_DELAY), "");
        assert_eq!(scanner_kvs.get(SCANNER_MAX_WAIT), "");
        assert_eq!(scanner_kvs.get(SCANNER_CYCLE_MAX_OBJECTS), "0");

        let heal_kvs = cfg
            .get_value(HEAL_SUB_SYS, DEFAULT_DELIMITER)
            .expect("heal defaults should exist");

        assert_eq!(heal_kvs.get(HEAL_BITROT_CYCLE), DEFAULT_HEAL_BITROT_CYCLE_SECS.to_string());
    }
}
