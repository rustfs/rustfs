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

// #730: configuration migration keeps legacy subsystem definitions available behind this module.
#![allow(dead_code)]

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
use arc_swap::ArcSwap;
use com::{STORAGE_CLASS_SUB_SYS, lookup_configs, read_config_without_migrate_with_recovery};
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
use std::sync::Arc;
use std::sync::LazyLock;
use tracing::warn;

pub static GLOBAL_STORAGE_CLASS: LazyLock<ArcSwap<storageclass::Config>> =
    LazyLock::new(|| ArcSwap::from_pointee(storageclass::Config::default()));
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
        let mut cfg = read_config_without_migrate_with_recovery(api.clone()).await?;

        lookup_configs(&mut cfg, api).await?;

        set_global_server_config(cfg);

        Ok(())
    }
}

pub fn get_global_storage_class() -> Option<storageclass::Config> {
    Some(GLOBAL_STORAGE_CLASS.load().as_ref().clone())
}

pub(crate) fn get_global_storage_class_snapshot() -> Arc<storageclass::Config> {
    GLOBAL_STORAGE_CLASS.load_full()
}

fn publish_storage_class_config(target: &ArcSwap<storageclass::Config>, cfg: storageclass::Config) {
    let cfg = Arc::new(cfg);
    target.store(cfg.clone());

    for (pool_index, drives_per_set) in cfg.automatic_zero_parity_pools() {
        warn!(
            event = "storage_class_zero_redundancy",
            component = "ecstore",
            subsystem = "storage_class",
            state = "degraded",
            pool_index,
            drives_per_set,
            parity = 0,
            "automatic storage class has no parity"
        );
    }
}

pub fn set_global_storage_class(cfg: storageclass::Config) {
    publish_storage_class_config(&GLOBAL_STORAGE_CLASS, cfg);
}

#[cfg(test)]
mod storage_class_publish_tests {
    use super::publish_storage_class_config;
    use crate::config::storageclass::{self, CLASS_STANDARD, STANDARD, lookup_config_for_pools_without_env};
    use arc_swap::ArcSwap;
    use rustfs_config::server_config::KVS;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    struct CapturedLogs {
        output: Arc<Mutex<Vec<u8>>>,
        published: Option<Arc<ArcSwap<storageclass::Config>>>,
    }

    struct CapturedWriter(CapturedLogs);

    impl Write for CapturedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if let Some(published) = &self.0.published {
                assert_eq!(
                    published.load_full().parities_for_sc(STANDARD),
                    Some(vec![2, 0]),
                    "warning must be emitted after the new snapshot is visible"
                );
            }
            self.0.output.lock().expect("log buffer lock poisoned").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedWriter(self.clone())
        }
    }

    impl CapturedLogs {
        fn observing(published: Arc<ArcSwap<storageclass::Config>>) -> Self {
            Self {
                published: Some(published),
                ..Default::default()
            }
        }

        fn output(&self) -> String {
            String::from_utf8(self.output.lock().expect("log buffer lock poisoned").clone()).expect("logs should be UTF-8")
        }
    }

    #[test]
    fn automatic_single_disk_publish_emits_structured_warning() {
        let cfg = lookup_config_for_pools_without_env(&KVS::new(), &[4, 1]).expect("automatic config should resolve");
        let target = Arc::new(ArcSwap::from_pointee(Default::default()));
        let logs = CapturedLogs::observing(target.clone());
        let subscriber = tracing_subscriber::fmt().json().with_writer(logs.clone()).finish();

        tracing::subscriber::with_default(subscriber, || publish_storage_class_config(&target, cfg));

        assert_eq!(target.load_full().parities_for_sc(STANDARD), Some(vec![2, 0]));
        let output = logs.output();
        assert_eq!(
            output.matches("storage_class_zero_redundancy").count(),
            1,
            "unexpected warning count: {output}"
        );
        assert!(output.contains("\"level\":\"WARN\""), "unexpected warning level: {output}");
        assert!(output.contains("\"pool_index\":1"), "missing pool index: {output}");
        assert!(output.contains("\"drives_per_set\":1"), "missing drive count: {output}");
        assert!(output.contains("\"parity\":0"), "missing parity: {output}");
    }

    #[test]
    fn explicit_zero_parity_publish_does_not_emit_automatic_warning() {
        let mut kvs = KVS::new();
        kvs.insert(CLASS_STANDARD.to_string(), "EC:0".to_string());
        let cfg = lookup_config_for_pools_without_env(&kvs, &[1]).expect("explicit EC:0 should resolve");
        let target = ArcSwap::from_pointee(Default::default());
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt().json().with_writer(logs.clone()).finish();

        tracing::subscriber::with_default(subscriber, || publish_storage_class_config(&target, cfg));

        assert_eq!(target.load_full().parities_for_sc(STANDARD), Some(vec![0]));
        assert!(!logs.output().contains("storage_class_zero_redundancy"));
    }

    #[test]
    fn storage_class_arc_swap_never_exposes_mixed_pool_state() {
        let automatic = lookup_config_for_pools_without_env(&KVS::new(), &[4, 6]).expect("automatic config should resolve");
        let mut kvs = KVS::new();
        kvs.insert(CLASS_STANDARD.to_string(), "EC:1".to_string());
        let explicit = lookup_config_for_pools_without_env(&kvs, &[4, 6]).expect("explicit config should resolve");
        let target = Arc::new(ArcSwap::from_pointee(automatic.clone()));
        let old_snapshot = target.load_full();

        let writer_target = target.clone();
        let writer = std::thread::spawn(move || {
            for index in 0..2_000 {
                let next = if index % 2 == 0 { automatic.clone() } else { explicit.clone() };
                writer_target.store(Arc::new(next));
            }
        });

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let reader_target = target.clone();
                std::thread::spawn(move || {
                    for _ in 0..2_000 {
                        let pair = reader_target
                            .load_full()
                            .parities_for_sc(STANDARD)
                            .expect("published config must have pool topology");
                        assert!(pair == [2, 3] || pair == [1, 1], "mixed pool snapshot: {pair:?}");
                    }
                })
            })
            .collect();

        writer.join().expect("writer thread should complete");
        for reader in readers {
            reader.join().expect("reader thread should complete");
        }
        assert_eq!(old_snapshot.parities_for_sc(STANDARD), Some(vec![2, 3]));
    }
}

pub async fn init_global_config_sys(api: Arc<ECStore>) -> Result<()> {
    GLOBAL_CONFIG_SYS.init(api).await
}

pub async fn try_migrate_server_config(api: Arc<ECStore>, decrypt_fn: Option<crate::bucket::migration::LegacyBlobDecryptFn>) {
    com::try_migrate_server_config(api, decrypt_fn).await
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
